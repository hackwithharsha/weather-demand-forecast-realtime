"""
Fixtures for integration tests.

These tests require live services:
  - Postgres (always needed)
  - API   (needed for HTTP tests — skipped if unreachable)

Run from the worker container after `make up`:

    docker compose --profile core run --rm worker \
        pytest tests/integration/ -v
"""

from __future__ import annotations

import os

import httpx
import psycopg2
import pytest


# ---------------------------------------------------------------------------
# Postgres fixture (same pattern as worker/tests/conftest.py)
# ---------------------------------------------------------------------------


def _build_dsn() -> str | None:
    if dsn := os.environ.get("POSTGRES_DSN"):
        return dsn
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        return None
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "forecast")
    user = os.environ.get("POSTGRES_USER", "forecast")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture
def pg_conn():
    """
    psycopg2 connection inside a transaction; rolled back on teardown.

    Skipped when no Postgres DSN is configured.
    """
    dsn = _build_dsn()
    if not dsn:
        pytest.skip(
            "No Postgres connection available. "
            "Set POSTGRES_DSN or POSTGRES_PASSWORD to run integration tests."
        )
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def pg_conn_commit():
    """
    psycopg2 connection that commits writes (for pipeline-visible inserts).

    Data is NOT rolled back — tests using this fixture must clean up after
    themselves or accept persistent test rows with synthetic event_ids.
    """
    dsn = _build_dsn()
    if not dsn:
        pytest.skip(
            "No Postgres connection available. "
            "Set POSTGRES_DSN or POSTGRES_PASSWORD to run integration tests."
        )
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# API client fixture
# ---------------------------------------------------------------------------


def _api_url() -> str:
    return os.environ.get("API_URL", "http://api:8080")


@pytest.fixture
def api_client():
    """
    httpx.Client pointed at the API service.

    Skipped when the API is not reachable (e.g. unit-test environment without
    the app profile running).
    """
    url = _api_url()
    try:
        with httpx.Client(base_url=url, timeout=5.0) as client:
            resp = client.get("/health")
            if resp.status_code != 200:
                pytest.skip(f"API health check returned {resp.status_code}")
    except Exception as exc:
        pytest.skip(f"API not reachable at {url}: {exc}")

    with httpx.Client(base_url=url, timeout=30.0) as client:
        yield client
