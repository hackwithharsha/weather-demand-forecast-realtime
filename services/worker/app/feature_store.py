"""
Offline feature store: nightly mart computation + Redis batch sync.

Two responsibilities, one entry point
--------------------------------------
``run_feature_store(settings)`` is the single function called by APScheduler.
It does exactly two things in sequence:

  1. ``_run_route_mart``  — execute sql/feature_store/001_route_features_daily.sql
                            against Postgres to populate/refresh
                            marts.route_features_daily for yesterday.

  2. ``_sync_to_redis``   — read the most recent row per route from that table
                            and write it to Redis as a hash.

Redis write strategy: HSET, never SET
--------------------------------------
Each route's features are stored as a Redis *hash* at key
``feat:route:{route_id}``.  The sync uses ``HSET key field value ...``
(the multi-field variant) rather than ``SET key <json-blob>``.

Why field-level writes matter — see docs/decisions.md §"HSET vs SET for
the feature store".  In brief:

  - A future online pipeline can update individual fields (e.g.
    ``lead_time_p50`` computed from a real-time booking stream) without
    touching the offline-computed fields written here.  SET would silently
    overwrite those concurrent writes.

  - The API can fetch a single field with ``HGET feat:route:london
    avg_bookings_90d`` without parsing a JSON blob or reading all fields.

  - Pipelines that update disjoint field sets never race: each HSET call
    is an atomic multi-field write that leaves all other fields untouched.

Feature name discipline
------------------------
All field names in the Redis hash, all column names in the SQL SELECT, and
all column names in marts.route_features_daily are taken exclusively from
``common.feature_registry.ROUTE_FEATURES``.  No feature name string literals
appear in this module.  If a feature is added or renamed, change the registry
and the migration — this file does not need editing.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import redis
import structlog

from common.features.registry import (
    BATCH_COMPUTED_AT_FIELD,
    ROUTE_FEATURES,
    ROUTE_REDIS_KEY_PATTERN,
)

from .settings import Settings
from .sql_runner import SqlRunner

log = structlog.get_logger()

_SQL_DIR = Path(__file__).parent.parent / "sql" / "feature_store"

#: Number of HSET commands queued in one pipeline before flushing to Redis.
#: 500 keys × ~13 fields × ~50 bytes ≈ 325 KB per round-trip — well inside
#: Redis's default max-inline-command buffer (64 MB) while keeping round-trip
#: count low for realistic route counts.  Pass ``batch_size`` explicitly to
#: _sync_to_redis() to override in tests.
_PIPELINE_BATCH_SIZE: int = 500

# Query to fetch the most recent feature row per route.
# DISTINCT ON (route_id) with ORDER BY route_id, feature_date DESC returns
# exactly one row per route — the row with the latest feature_date.
_LATEST_ROUTE_FEATURES_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id,
        feature_date,
        {fields},
        feature_computed_at
    FROM marts.route_features_daily
    ORDER BY route_id, feature_date DESC
""".format(fields=", ".join(ROUTE_FEATURES.names()))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_feature_store(settings: Settings) -> None:
    """Compute route mart features then sync them to Redis.

    Called nightly by APScheduler.  Errors are logged and re-raised so the
    scheduler can record the misfire without silently swallowing failures.
    """
    log.info("feature_store_sync_started")
    try:
        _run_route_mart(settings)
        _sync_to_redis(settings)
        log.info("feature_store_sync_completed")
    except Exception:
        log.exception("feature_store_sync_failed")
        raise


# ---------------------------------------------------------------------------
# Step 1: populate marts.route_features_daily
# ---------------------------------------------------------------------------

def _run_route_mart(settings: Settings) -> None:
    """Execute sql/feature_store/001_route_features_daily.sql.

    Computes features for yesterday (date.today() - 1 day) and upserts into
    marts.route_features_daily.  Running it twice on the same day is safe
    (ON CONFLICT DO UPDATE overwrites with identical values).

    The feature_date parameter anchors all lookback windows so that the SQL
    is point-in-time correct and safe to re-run for any past date.
    """
    feature_date = date.today() - timedelta(days=1)
    log.info("route_mart_started", feature_date=feature_date.isoformat())
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        results = SqlRunner(_SQL_DIR).run(
            conn, params={"feature_date": feature_date.isoformat()}
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    result = results[0] if results else {}
    log.info(
        "route_mart_done",
        feature_date=feature_date.isoformat(),
        routes_assembled=int(result.get("routes_assembled", 0) or 0),
        routes_upserted=int(result.get("routes_upserted", 0) or 0),
    )


# ---------------------------------------------------------------------------
# Step 2: sync latest features to Redis
# ---------------------------------------------------------------------------

def _sync_to_redis(
    settings: Settings,
    *,
    batch_size: int = _PIPELINE_BATCH_SIZE,
) -> None:
    """Read the latest row per route and write it to Redis as a hash.

    For each route:
      - Key   : ``feat:route:{route_id}``  (e.g. ``feat:route:london``)
      - Fields: one field per feature in ROUTE_FEATURES, plus
                ``batch_computed_at`` (ISO-8601 UTC timestamp)

    Field names are taken from ``ROUTE_FEATURES.names()``; no string literals
    appear in this function.

    Null feature values are omitted from the mapping so the hash never grows
    stale "null" strings.  The API must treat a missing hash field as "not yet
    computed" and fall back to a default or emit a feature-freshness alert.

    HSET calls are batched into pipelines of *batch_size* keys per round-trip.
    Each pipeline is flushed immediately after filling, bounding both the
    in-process buffer size and the latency of any single round-trip.
    """
    # ── Fetch from Postgres ───────────────────────────────────────────────
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_LATEST_ROUTE_FEATURES_SQL)
            rows: list = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        log.warning(
            "feature_store_sync_skipped",
            reason="marts.route_features_daily is empty — run the mart first",
        )
        return

    batch_computed_at = datetime.now(timezone.utc).isoformat()
    t_start = time.monotonic()

    r = _make_redis_client(settings)

    routes_synced = 0
    fields_written = 0
    batches_flushed = 0

    # ── Write to Redis in batches ─────────────────────────────────────────
    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start : batch_start + batch_size]
        t_batch = time.monotonic()

        pipe = r.pipeline(transaction=False)
        batch_fields = 0

        for row in batch:
            route_id = row["route_id"]
            key = ROUTE_REDIS_KEY_PATTERN.format(route_id=route_id)

            # Build the field mapping from the registry — no feature-name
            # strings here.  Null values are excluded; see docstring above.
            mapping: dict[str, str] = {}
            for feat in ROUTE_FEATURES:
                value = row.get(feat.name)
                if value is not None:
                    mapping[feat.name] = str(float(value))

            # Always write the freshness timestamp even if all features are null.
            mapping[BATCH_COMPUTED_AT_FIELD] = batch_computed_at

            # HSET writes individual fields atomically.
            # It does NOT replace the entire key — other fields (e.g. online
            # features written by a separate pipeline) remain untouched.
            # See docs/decisions.md §"HSET vs SET for the feature store".
            pipe.hset(key, mapping=mapping)
            batch_fields += len(mapping)

        pipe.execute()
        batches_flushed += 1

        batch_elapsed = time.monotonic() - t_batch
        log.debug(
            "feature_store_batch_flushed",
            batch_num=batches_flushed,
            keys=len(batch),
            fields=batch_fields,
            elapsed_ms=round(batch_elapsed * 1000),
            keys_per_s=round(len(batch) / batch_elapsed) if batch_elapsed else None,
        )

        routes_synced += len(batch)
        fields_written += batch_fields

    r.close()

    elapsed_s = time.monotonic() - t_start
    log.info(
        "feature_store_redis_sync_done",
        routes_synced=routes_synced,
        fields_written=fields_written,
        batches_flushed=batches_flushed,
        elapsed_s=round(elapsed_s, 3),
        keys_per_s=round(routes_synced / elapsed_s) if elapsed_s else None,
        fields_per_s=round(fields_written / elapsed_s) if elapsed_s else None,
        batch_computed_at=batch_computed_at,
    )


# ---------------------------------------------------------------------------
# Redis client factory
# ---------------------------------------------------------------------------

def _make_redis_client(settings: Settings) -> redis.Redis:
    """Return a synchronous redis.Redis client using Settings credentials."""
    return redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )
