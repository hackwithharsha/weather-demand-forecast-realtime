"""
Shared pytest fixtures for the worker test suite.
"""

from __future__ import annotations

import os

import psycopg2
import pytest


def _build_dsn() -> str | None:
    """Return a Postgres DSN from environment variables, or None if unavailable."""
    if dsn := os.environ.get("POSTGRES_DSN"):
        return dsn
    # Compose individual vars — matches what docker-compose.yml injects into
    # the worker service at runtime.
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        return None
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "forecast")
    user = os.environ.get("POSTGRES_USER", "forecast")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture
def pg_conn():
    """Open a psycopg2 connection inside a transaction; roll back on teardown.

    Skipped when no Postgres DSN is configured, or when the server is not
    reachable (e.g. unit-test run with --no-deps).
    All data written by the test is undone automatically — nothing persists.
    """
    dsn = _build_dsn()
    if not dsn:
        pytest.skip(
            "No Postgres connection available. "
            "Set POSTGRES_DSN or POSTGRES_PASSWORD to run integration tests."
        )
    try:
        conn = psycopg2.connect(dsn, connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not reachable (start services with make up): {exc}")
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()
