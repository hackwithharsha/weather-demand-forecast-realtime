COMPOSE := docker compose

# Profile flag helpers
P_CORE   := --profile core
P_STREAM := --profile stream
P_ML     := --profile ml
P_OBS    := --profile obs
P_UI     := --profile ui
P_ALL    := $(P_CORE) $(P_STREAM) $(P_ML) $(P_OBS) $(P_UI)

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
up-stream: ## Start streaming services (redpanda)
	$(COMPOSE) $(P_STREAM) up -d

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
verify: ## Assert postgres and redis are healthy (exit 1 if not)
	@ok=1; \
	for svc in postgres redis; do \
		printf "  %-10s " "$$svc"; \
		id=$$($(COMPOSE) $(P_CORE) ps -q $$svc 2>/dev/null); \
		if [ -z "$$id" ]; then \
			echo "NOT RUNNING"; ok=0; \
		else \
			status=$$(docker inspect --format='{{.State.Health.Status}}' $$id 2>/dev/null); \
			if [ "$$status" = "healthy" ]; then \
				echo "OK  (healthy)"; \
			else \
				echo "FAIL  ($$status)"; ok=0; \
			fi; \
		fi; \
	done; \
	[ $$ok -eq 1 ] || exit 1

# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------
.PHONY: reset
reset: ## Wipe all volumes and restart core services from scratch
	$(COMPOSE) $(P_ALL) down -v --remove-orphans
	$(COMPOSE) $(P_CORE) up -d

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
.PHONY: env
env: ## Copy .env.example to .env if .env does not exist
	@test -f .env && echo ".env already exists, skipping" || (cp .env.example .env && echo "Created .env from .env.example")
