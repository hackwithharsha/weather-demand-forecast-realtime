# Weather-Driven Demand Forecasting Platform

Ingests weather signals and historical demand data, trains sklearn-based
forecasting models, and serves predictions via a FastAPI backend with a React
frontend. The full pipeline runs on Redpanda (streaming), Postgres + ClickHouse
(storage), MinIO (artifacts), and MLflow (experiment tracking).

**Current stage: 0**

---

## Hard Rule

Everything runs in Docker Compose. No application code, test runner, or tool
is executed directly on the host. The only host dependencies are Docker 25+,
Docker Compose v2.24+, and GNU Make 3.81+.

---

## Services

| Service | Image / Source | Port | Profile |
|---|---|---|---|
| postgres | postgres:16-alpine | 5432 | core |
| redis | redis:7-alpine | 6379 | core |
| redpanda | redpandadata/redpanda | 9092 / 9644 | streaming |
| clickhouse | clickhouse/clickhouse-server | 8123 / 9000 | storage |
| minio | minio/minio | 9001 / 9002 | storage |
| mlflow | infra/mlflow | 5000 | ml |
| prometheus | prom/prometheus | 9090 | observability |
| grafana | grafana/grafana | 3001 | observability |
| api | services/api | 8000 | app |
| ingestion | services/ingestion | — | app |
| forecaster | services/forecaster | 8001 | app |
| frontend | services/frontend | 5173 | app |

---

## Quick Start

```bash
make env        # copies .env.example → .env
# edit .env: set POSTGRES_PASSWORD and REDIS_PASSWORD
make up         # starts core profile (postgres, redis)
make ps         # verify healthy
```

---

## Make Targets

```
make up              start core profile (postgres, redis)
make up-streaming    start redpanda
make up-storage      start minio, clickhouse
make up-ml           start mlflow
make up-obs          start prometheus, grafana
make up-app          start api, ingestion, forecaster, frontend
make up-all          start everything
make down            stop containers, keep volumes
make destroy         stop containers, delete volumes
make ps              container status
make logs            tail all logs
make logs-<svc>      tail one service  (e.g. make logs-api)
make shell-db        psql inside postgres
make shell-redis     redis-cli inside redis
make test-<svc>      run tests for a service (see below)
```

---

## Running Tests

Tests run inside the service container. No test runner on the host.

```bash
make test-api          # docker compose run --rm api pytest
make test-ingestion    # docker compose run --rm ingestion pytest
make test-forecaster   # docker compose run --rm forecaster pytest
```

Each service's `Dockerfile` must have a stage or target that includes dev
dependencies (pytest, etc.). The `make test-<svc>` targets will be wired up
as services are implemented.

---

## Python Conventions

| Topic | Rule |
|---|---|
| Dependency management | `uv` — never pip directly |
| Linting / formatting | `ruff` (check + format); enforced in CI |
| Data models / validation | Pydantic v2 (`model_config`, no v1 compat shims) |
| Structured logging | `structlog` with JSON renderer; no `print()` in production paths |
| Exception handling | No bare `except:`; always catch a specific exception type |
| Python version | 3.12-slim base image |

---

## Health Endpoints

Every owned service (`api`, `ingestion`, `forecaster`) must expose:

```
GET /health  →  200 {"status": "ok"}
```

This endpoint is used by Docker Compose `healthcheck` and by any future
orchestrator. It must not require authentication.
