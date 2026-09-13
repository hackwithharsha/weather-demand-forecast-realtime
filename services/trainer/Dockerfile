# Build context: repo root  (needed to COPY libs/common)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GIT_PYTHON_REFRESH=quiet

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd -m -u 1000 app

WORKDIR /app

# Layer 1 — shared library
COPY libs/common ./libs/common
RUN uv pip install --system --no-cache ./libs/common

# Layer 2 — service deps incl. test extras (cached as long as pyproject.toml is unchanged)
COPY services/trainer/pyproject.toml .
RUN mkdir -p app && touch app/__init__.py \
    && uv pip install --system --no-cache ".[test]"

# Layer 3 — source + tests
COPY services/trainer/app/ ./app/
COPY services/trainer/tests/ ./tests/

USER app

CMD ["python", "-m", "app.main"]
