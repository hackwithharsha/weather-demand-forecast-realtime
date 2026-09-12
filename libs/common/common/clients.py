"""
Lazy, retry-wrapped async clients for Postgres and Redis.

Both clients are module-level singletons created on first call.
Services should call the ``close_*`` coroutines during shutdown.

Usage
-----
    from common.clients import get_pg_pool, get_redis, close_pg_pool, close_redis
    from common.settings import settings

    pool = await get_pg_pool(settings)
    redis = await get_redis(settings)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import asyncpg
import redis.asyncio as aioredis
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from .settings import Settings

_log = logging.getLogger(__name__)

_pg_pool: asyncpg.Pool | None = None
_redis: aioredis.Redis | None = None


@retry(
    retry=retry_if_exception_type((OSError, asyncpg.PostgresConnectionError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(_log, logging.WARNING),
    reraise=True,
)
async def get_pg_pool(settings: Settings) -> asyncpg.Pool:
    """Return the shared asyncpg pool, creating it on the first call."""
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(
            dsn=settings.postgres_dsn,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
    return _pg_pool


async def close_pg_pool() -> None:
    """Drain and close the Postgres pool. Call during service shutdown."""
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()
        _pg_pool = None


@retry(
    retry=retry_if_exception_type((OSError, aioredis.RedisError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(_log, logging.WARNING),
    reraise=True,
)
async def get_redis(settings: Settings) -> aioredis.Redis:
    """Return the shared Redis client, creating and pinging it on first call."""
    global _redis
    if _redis is None:
        client: aioredis.Redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        await client.ping()
        _redis = client
    return _redis


async def close_redis() -> None:
    """Close the Redis connection. Call during service shutdown."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
