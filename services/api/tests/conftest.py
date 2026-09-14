"""
Shared fixtures for the API test suite.

app_client
    An httpx.AsyncClient wired to the FastAPI app with the real lifespan
    replaced by a no-op.  _state is reset to a fresh _AppState before each
    test so tests can set exactly the attributes they need
    (e.g. state.prod_model = MagicMock()).

The real lifespan tries to connect to Postgres, Redis, and MLflow.  None of
those are available when running unit tests with --no-deps.  Replacing
app.router.lifespan_context before each test ensures the ASGI app starts
cleanly without any I/O.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest_asyncio


@asynccontextmanager
async def _null_lifespan(application):
    """No-op lifespan: skip all startup I/O for unit tests."""
    yield


@pytest_asyncio.fixture
async def app_client(monkeypatch):
    """
    Async httpx client pointing at the FastAPI app.

    Yields (client, state) where *state* is the fresh _AppState instance
    the test can mutate before issuing requests.
    """
    import app.main as main_mod
    from httpx import ASGITransport, AsyncClient

    fresh_state = main_mod._AppState()
    monkeypatch.setattr(main_mod, "_state", fresh_state)
    monkeypatch.setattr(main_mod.app.router, "lifespan_context", _null_lifespan)

    async with AsyncClient(
        transport=ASGITransport(app=main_mod.app),
        base_url="http://test",
    ) as client:
        yield client, fresh_state
