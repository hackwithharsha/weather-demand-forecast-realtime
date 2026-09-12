COMPOSE := docker compose

# Profile flag helpers
P_CORE        := --profile core
P_STREAMING   := --profile streaming
P_STORAGE     := --profile storage
P_ML          := --profile ml
P_OBS         := --profile observability
P_APP         := --profile app
P_ALL         := $(P_CORE) $(P_STREAMING) $(P_STORAGE) $(P_ML) $(P_OBS) $(P_APP)

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

.PHONY: up-streaming
up-streaming: ## Start streaming services (redpanda)
	$(COMPOSE) $(P_STREAMING) up -d

.PHONY: up-storage
up-storage: ## Start storage services (minio, clickhouse)
	$(COMPOSE) $(P_STORAGE) up -d

.PHONY: up-ml
up-ml: ## Start ML services (mlflow)
	$(COMPOSE) $(P_ML) up -d

.PHONY: up-obs
up-obs: ## Start observability services (prometheus, grafana)
	$(COMPOSE) $(P_OBS) up -d

.PHONY: up-app
up-app: ## Start application services (api, ingestion, forecaster, frontend)
	$(COMPOSE) $(P_APP) up -d

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
.PHONY: build
build: ## Build all application images
	$(COMPOSE) $(P_APP) build

.PHONY: build-%
build-%: ## Build a specific service image  (e.g. make build-api)
	$(COMPOSE) build $*

# ---------------------------------------------------------------------------
# Shells
# ---------------------------------------------------------------------------
.PHONY: shell-db
shell-db: ## Open psql inside the postgres container
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-forecast} $${POSTGRES_DB:-forecast}

.PHONY: shell-redis
shell-redis: ## Open redis-cli inside the redis container
	$(COMPOSE) exec redis redis-cli -a $${REDIS_PASSWORD}

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
.PHONY: env
env: ## Copy .env.example to .env if .env does not exist
	@test -f .env && echo ".env already exists, skipping" || (cp .env.example .env && echo "Created .env from .env.example")
