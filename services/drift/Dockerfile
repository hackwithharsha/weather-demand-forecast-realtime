# Build context: services/drift (no shared libs needed)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd -m -u 1000 app

WORKDIR /app

COPY pyproject.toml .
RUN mkdir -p app && touch app/__init__.py \
    && uv pip install --system --no-cache .

COPY app/ ./app/

USER app

CMD ["python", "-m", "app.main"]
