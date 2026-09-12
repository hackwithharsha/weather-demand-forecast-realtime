# Build context: repo root  (needed to COPY libs/common)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 app

WORKDIR /app

# 1. Install the shared common library and its deps first.
#    This layer is invalidated only when libs/common changes.
COPY libs/common ./libs/common
RUN uv pip install --system --no-cache ./libs/common

# 2. Install service-level deps.
#    Dummy app/__init__.py satisfies hatchling during the dep-only install.
COPY services/mock-weather/pyproject.toml .
RUN mkdir -p app && touch app/__init__.py \
    && uv pip install --system --no-cache .

# 3. Copy real source last — code changes don't bust the dep layers.
COPY services/mock-weather/app/ ./app/

USER app

EXPOSE 8001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
