# Weather-Driven Demand Forecasting Platform

## Overview

End-to-end platform that ingests weather signals and historical demand data,
trains sklearn-based forecasting models, and serves predictions via a FastAPI
backend with a React frontend.

Everything runs in Docker Compose. **Nothing is installed on the host except
Docker and Docker Compose.**

---

## Prerequisites

| Tool | Minimum version |
|---|---|
| Docker | 25.x |
| Docker Compose | v2.24 |
| GNU Make | 3.81 |

---

## Quick Start

```bash
cp .env.example .env          # fill in secrets
make up                       # starts core profile (postgres, redis)
make ps                       # verify containers are healthy
```

---

## Docker Compose Profiles

Services are grouped by profile. Start only what you need.

| Profile | Services | Make target |
|---|---|---|
| `core` | postgres, redis | `make up` |
| `streaming` | redpanda | `make up-streaming` |
| `storage` | minio, clickhouse | `make up-storage` |
| `ml` | mlflow | `make up-ml` |
| `observability` | prometheus, grafana | `make up-obs` |
| `app` | api, ingestion, forecaster, frontend | `make up-app` |

To start everything:

```bash
make up-all
```

---

## Repository Layout

```
.
├── CLAUDE.md               # this file
├── Makefile                # all dev workflows
├── docker-compose.yml      # all service definitions
├── .env.example            # required env vars with safe defaults
│
├── services/               # code you own
│   ├── api/                # FastAPI application
│   │   ├── Dockerfile
│   │   └── app/            # Python package root
│   ├── ingestion/          # weather + demand data ingest workers
│   │   └── Dockerfile
│   ├── forecaster/         # sklearn training + inference service
│   │   └── Dockerfile
│   └── frontend/           # React application
│       └── Dockerfile
│
├── infra/                  # third-party service configuration
│   ├── postgres/init/      # *.sql files run at first boot
│   ├── clickhouse/init/    # *.sql files run at first boot
│   ├── redpanda/config/    # redpanda.yaml overrides
│   ├── minio/buckets.sh    # idempotent bucket creation
│   ├── mlflow/Dockerfile   # thin wrapper over mlflow image
│   ├── prometheus/         # prometheus.yml scrape config
│   └── grafana/            # provisioned datasources + dashboards
│
└── scripts/                # one-off shell helpers (seed, topic creation, etc.)
```

---

## Environment Variables

All secrets and config live in `.env` (git-ignored). See `.env.example` for
the full list with descriptions.

Never commit `.env`.

---

## Common Make Targets

```
make help          list all targets with descriptions
make up            start core services
make up-all        start every profile
make down          stop and remove containers (keeps volumes)
make destroy       stop and remove containers AND volumes
make logs          tail all running service logs
make ps            show container status
make shell-db      open psql inside the postgres container
make shell-redis   open redis-cli inside the redis container
```

---

## Adding a New Service

1. Add the service definition to `docker-compose.yml` under the appropriate profile.
2. If it needs config files, create `infra/<service>/` and mount it.
3. If it is a service you own, add `services/<service>/Dockerfile`.
4. Add a `make up-<profile>` target if a new profile is introduced.
5. Document the service in the table above.

---

## Conventions

- Python services target **Python 3.12-slim**.
- Frontend targets **Node 22-alpine**.
- All inter-service communication uses the Docker Compose service name as hostname.
- Secrets are never hardcoded; always read from environment variables.
- Healthchecks are required on every stateful service.
- Volumes are named (not bind-mounted) for stateful data.
- Bind mounts are used only for config files and source code hot-reload in dev.
