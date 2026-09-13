"""
Feature staleness debug API.

Endpoints
---------
GET /health
    Liveness probe.

GET /features/{route}/debug
    Returns every registered feature for *route* with its current value,
    data source, age in seconds, freshness SLA, and whether the SLA is met.

    ``overall_status`` is ``"ok"`` only when every feature's SLA is satisfied.
    Any breach (including a missing timestamp) sets it to ``"stale"``.
"""

from __future__ import annotations

import functools
from datetime import datetime, timezone
from typing import Any

import redis
import structlog
from fastapi import FastAPI, HTTPException

from common.features.registry import (
    BATCH_COMPUTED_AT_FIELD,
    ROUTE_FEATURES,
    ROUTE_REDIS_KEY_PATTERN,
    STREAM_COMPUTED_AT_FIELD,
    SYNC_COMPLETED_AT_KEY,
)

from .settings import Settings

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# App + Redis client
# ---------------------------------------------------------------------------

app = FastAPI(title="Forecast Feature API", version="0.1.0")

@functools.lru_cache(maxsize=1)
def _settings() -> Settings:
    return Settings()


@functools.lru_cache(maxsize=1)
def _redis() -> redis.Redis:
    s = _settings()
    return redis.Redis(
        host=s.redis_host,
        port=s.redis_port,
        db=s.redis_db,
        password=s.redis_password or None,
        decode_responses=True,
        socket_connect_timeout=2,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso(ts: str | None) -> float | None:
    """Parse an ISO-8601 UTC string to a Unix epoch float.  Returns None on failure."""
    if ts is None:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


def _cast_value(value_str: str, dtype: str) -> Any:
    """Cast a Redis string to the feature's declared dtype."""
    if dtype == "str":
        return value_str
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return value_str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/features/{route}/debug")
def features_debug(route: str) -> dict:
    """Return per-feature staleness diagnostics for *route*."""
    r = _redis()
    key = ROUTE_REDIS_KEY_PATTERN.format(route_id=route)

    try:
        raw: dict[str, str] = r.hgetall(key)
    except redis.RedisError as exc:
        log.error("redis_error", route=route, error=str(exc))
        raise HTTPException(status_code=503, detail="Redis unavailable") from exc

    if not raw:
        raise HTTPException(
            status_code=404,
            detail=f"No features found for route {route!r}. "
                   "Has the stream worker processed any events for this route?",
        )

    now_ts = datetime.now(timezone.utc)
    now_epoch = now_ts.timestamp()

    batch_computed_at_str  = raw.get(BATCH_COMPUTED_AT_FIELD)
    stream_computed_at_str = raw.get(STREAM_COMPUTED_AT_FIELD)
    sync_completed_at_str  = None
    try:
        sync_completed_at_str = r.get(SYNC_COMPLETED_AT_KEY)
    except redis.RedisError:
        pass

    batch_epoch  = _parse_iso(batch_computed_at_str)
    stream_epoch = _parse_iso(stream_computed_at_str)

    features_out: list[dict] = []
    for feat in ROUTE_FEATURES:
        value_str = raw.get(feat.name)
        missing   = value_str is None

        # Age is measured from the source's last-written timestamp.
        ts_epoch = batch_epoch if feat.source == "batch" else stream_epoch

        age_seconds: float | None
        if ts_epoch is None:
            age_seconds = None
        else:
            age_seconds = now_epoch - ts_epoch

        # SLA is satisfied only when age is known and within the budget.
        sla_ok = (age_seconds is not None) and (
            age_seconds <= feat.freshness_sla_seconds
        )

        # Resolve display value.
        if not missing:
            value = _cast_value(value_str, feat.dtype)
        elif feat.default_on_missing is not None:
            value = feat.default_on_missing
        else:
            value = None

        features_out.append(
            {
                "name":                  feat.name,
                "source":                feat.source,
                "value":                 value,
                "age_seconds":           round(age_seconds, 1) if age_seconds is not None else None,
                "freshness_sla_seconds": feat.freshness_sla_seconds,
                "sla_ok":                sla_ok,
                "missing":               missing,
                "description":           feat.description,
            }
        )

    overall_status = "ok" if all(f["sla_ok"] for f in features_out) else "stale"

    return {
        "route":                route,
        "as_of":                now_ts.isoformat(),
        "overall_status":       overall_status,
        "batch_computed_at":    batch_computed_at_str,
        "stream_computed_at":   stream_computed_at_str,
        "sync_completed_at":    sync_completed_at_str,
        "features":             features_out,
    }
