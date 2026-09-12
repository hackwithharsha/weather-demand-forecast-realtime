COMPOSE := docker compose

# Profile flag helpers
P_CORE   := --profile core
P_STREAM := --profile stream
P_TOOLS  := --profile tools
P_ML     := --profile ml
P_OBS    := --profile obs
P_UI     := --profile ui
P_ALL    := $(P_CORE) $(P_STREAM) $(P_TOOLS) $(P_ML) $(P_OBS) $(P_UI)

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
.PHONY: up
up: ## Start core services (postgres, redis)
	$(COMPOSE) $(P_CORE) up -d

.PHONY: up-stream
up-stream: ## Start core + streaming services (redpanda, producers, ingestor)
	$(COMPOSE) $(P_CORE) $(P_STREAM) up -d

.PHONY: up-ml
up-ml: ## Start ML services (clickhouse, minio, mlflow)
	$(COMPOSE) $(P_ML) up -d

.PHONY: up-obs
up-obs: ## Start observability services (prometheus, grafana)
	$(COMPOSE) $(P_OBS) up -d

.PHONY: up-ui
up-ui: ## Start application services (api, ingestion, forecaster, frontend)
	$(COMPOSE) $(P_UI) up -d

.PHONY: up-all
up-all: ## Start all services
	$(COMPOSE) $(P_ALL) up -d

# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------
.PHONY: down
down: ## Stop and remove containers (volumes are preserved)
	$(COMPOSE) $(P_ALL) down

.PHONY: destroy
destroy: ## Stop and remove containers AND volumes (destructive)
	$(COMPOSE) $(P_ALL) down -v

# ---------------------------------------------------------------------------
# Observe
# ---------------------------------------------------------------------------
.PHONY: ps
ps: ## Show container status
	$(COMPOSE) $(P_ALL) ps

.PHONY: logs
logs: ## Tail logs for all running containers
	$(COMPOSE) logs -f

.PHONY: logs-%
logs-%: ## Tail logs for a specific service  (e.g. make logs-postgres)
	$(COMPOSE) logs -f $*

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
.PHONY: build-base
build-base: ## Build the shared Python base image (forecast-base:latest)
	docker build -f libs/Dockerfile.base -t forecast-base:latest .

.PHONY: build
build: build-base ## Build all application images (rebuilds base first)
	$(COMPOSE) $(P_UI) build

.PHONY: build-%
build-%: ## Build a specific service image  (e.g. make build-api)
	$(COMPOSE) build $*

# ---------------------------------------------------------------------------
# Shells
# ---------------------------------------------------------------------------
.PHONY: shell
shell: ## Open sh in a running container  (SERVICE=name, e.g. make shell SERVICE=api)
	$(COMPOSE) exec $${SERVICE:?usage: make shell SERVICE=<name>} sh

.PHONY: shell-db
shell-db: ## Open psql inside the postgres container
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-forecast} $${POSTGRES_DB:-forecast}

.PHONY: shell-redis
shell-redis: ## Open redis-cli inside the redis container
	$(COMPOSE) exec redis redis-cli -a $${REDIS_PASSWORD}

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------
.PHONY: test-common
test-common: ## Run libs/common unit tests (pure Python, no services needed)
	docker run --rm \
		-v "$(PWD)/libs/common:/app" \
		-w /app \
		python:3.12-slim \
		sh -c "pip install -q uv && uv pip install --system -q '.[test]' && python -m pytest tests/ -v"

.PHONY: test
test: test-common ## Run all tests

.PHONY: test-%
test-%: ## Run tests for a compose service  (e.g. make test-api)
	$(COMPOSE) run --rm $* pytest -v

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
.PHONY: verify
verify: ## Assert infra healthy; validate mock-weather schema and chaos toggle
	@ok=1; \
	W=http://localhost:8001; \
	_pass() { printf "  %-42s OK  %s\n"   "$$1" "$$2"; }; \
	_fail() { printf "  %-42s FAIL  %s\n" "$$1" "$$2"; ok=0; }; \
	\
	printf "\n--- infrastructure ---\n"; \
	for svc in postgres redis; do \
		printf "  %-42s " "$$svc"; \
		id=$$($(COMPOSE) $(P_CORE) ps -q $$svc 2>/dev/null); \
		if [ -z "$$id" ]; then \
			echo "FAIL  (not running)"; ok=0; \
		else \
			st=$$(docker inspect --format='{{.State.Health.Status}}' "$$id" 2>/dev/null); \
			[ "$$st" = "healthy" ] && echo "OK" || { echo "FAIL  ($$st)"; ok=0; }; \
		fi; \
	done; \
	\
	printf "\n--- mock-weather ---\n"; \
	mw_id=$$($(COMPOSE) $(P_CORE) ps -q mock-weather 2>/dev/null); \
	if [ -z "$$mw_id" ]; then \
		echo "  mock-weather not running — skipping endpoint checks"; \
	else \
		printf "  %-42s " "health"; \
		mw_st=$$(docker inspect --format='{{.State.Health.Status}}' "$$mw_id" 2>/dev/null); \
		if [ "$$mw_st" != "healthy" ]; then \
			echo "FAIL  ($$mw_st)"; ok=0; \
		else \
			echo "OK"; \
			\
			printf "  %-42s " "GET /v1/current schema"; \
			body=$$(curl -sf "$$W/v1/current?lat=51.5&lon=-0.1" 2>/dev/null); \
			echo "$$body" | python3 -c 'import sys,json; cc=json.load(sys.stdin)["currentConditions"]; assert {"temperature","feelsLike","humidity","wind","precipitation","weatherCondition","uvIndex","cloudCover"}.issubset(cc)' 2>/dev/null \
			&& echo "OK" || { echo "FAIL  (unexpected body)"; ok=0; }; \
			\
			printf "  %-42s " "GET /v1/forecast schema (3 h)"; \
			body=$$(curl -sf "$$W/v1/forecast?lat=51.5&lon=-0.1&hours=3" 2>/dev/null); \
			echo "$$body" | python3 -c 'import sys,json; fh=json.load(sys.stdin)["forecastHours"]; assert len(fh)==3 and "interval" in fh[0] and "temperature" in fh[0]' 2>/dev/null \
			&& echo "OK  (3 h)" || { echo "FAIL  (unexpected body)"; ok=0; }; \
			\
			printf "  %-42s " "POST /admin/chaos error_rate=1.0"; \
			curl -sf -X POST "$$W/admin/chaos" \
				-H 'Content-Type: application/json' \
				-d '{"error_rate":1.0}' >/dev/null 2>&1 \
			&& echo "OK" || { echo "FAIL  (request failed)"; ok=0; }; \
			\
			printf "  %-42s " "GET /v1/current → expect 5xx"; \
			code=$$(curl -s -o /dev/null -w '%{http_code}' "$$W/v1/current?lat=51.5&lon=-0.1"); \
			case "$$code" in \
				5*) echo "OK  (HTTP $$code)" ;; \
				*)  echo "FAIL  (expected 5xx, got $$code)"; ok=0 ;; \
			esac; \
			\
			printf "  %-42s " "POST /admin/chaos error_rate=0.0 (reset)"; \
			curl -sf -X POST "$$W/admin/chaos" \
				-H 'Content-Type: application/json' \
				-d '{"error_rate":0.0}' >/dev/null 2>&1 \
			&& echo "OK" || { echo "FAIL  (request failed)"; ok=0; }; \
			\
			printf "  %-42s " "GET /v1/current → expect 200"; \
			code=$$(curl -s -o /dev/null -w '%{http_code}' "$$W/v1/current?lat=51.5&lon=-0.1"); \
			[ "$$code" = "200" ] \
				&& echo "OK  (HTTP 200)" \
				|| { echo "FAIL  (expected 200, got $$code)"; ok=0; }; \
		fi; \
	fi; \
	printf "\n"; \
	[ "$$ok" = "1" ] || exit 1

# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------
.PHONY: reset
reset: ## Wipe all volumes and restart core services from scratch
	$(COMPOSE) $(P_ALL) down -v --remove-orphans
	$(COMPOSE) $(P_CORE) up -d

# ---------------------------------------------------------------------------
# Migrations (Alembic)
# ---------------------------------------------------------------------------

# Default downgrade step; override on the command line: make migrate-down REV=base
REV ?= -1

.PHONY: migrate
migrate: ## Apply all pending migrations (upgrade head)
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic upgrade head

.PHONY: migrate-down
migrate-down: ## Downgrade one step (REV=-1 default; REV=base to wipe all)
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic downgrade $(REV)

.PHONY: migrate-new
migrate-new: ## Scaffold an empty migration file  (MSG="short description required")
	@test -n "$(MSG)" \
		|| (printf 'Usage: make migrate-new MSG="short description"\n' >&2; exit 1)
	$(COMPOSE) $(P_TOOLS) run --rm --no-deps alembic revision -m "$(MSG)"

.PHONY: migrate-history
migrate-history: ## Show full migration history
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic history --verbose

.PHONY: migrate-current
migrate-current: ## Show the current applied revision
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic current --verbose

.PHONY: migrate-check
migrate-check: ## Roundtrip test: downgrade base → upgrade head  (DESTRUCTIVE — dev only)
	@printf '\nmigrate-check: downgrade base (drops all schemas)...\n'
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic downgrade base
	@printf '\nmigrate-check: upgrade head (recreates all schemas)...\n'
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic upgrade head
	@printf '\nmigrate-check: current revision\n'
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm alembic current --verbose
	@printf '\nmigrate-check: OK\n\n'

# ---------------------------------------------------------------------------
# Lake CLI
# ---------------------------------------------------------------------------

.PHONY: lake
lake: ## Run lake CLI (e.g. make lake CMD="list")
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm lake $(CMD)

.PHONY: lake-counts
lake-counts: ## City event counts for the last 3 hours  (TABLE=demand_events)
	$(COMPOSE) $(P_CORE) $(P_TOOLS) run --rm lake counts ${TABLE:-demand_events} --last 3

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
.PHONY: env
env: ## Copy .env.example to .env if .env does not exist
	@test -f .env && echo ".env already exists, skipping" || (cp .env.example .env && echo "Created .env from .env.example")
