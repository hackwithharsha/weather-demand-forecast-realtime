"""
raw → staging

Executes the versioned SQL files in sql/staging/ against the Postgres raw
tables and commits the results.  All transform logic (deduplication, null
validation, reject logging, aggregation) lives in the SQL files; this module
is a thin executor.

Data source: raw.demand_events and raw.weather_readings (Postgres)
-----------------------------------------------------------------
Staging reads directly from the raw Postgres tables, not from the Parquet
lake.  This is the correct boundary for a SQL-based staging layer:

  raw.*  ──SQL transform──►  staging.*  ──Python pandas──►  marts.*

The Parquet lake on MinIO is a parallel analytical export written by the
ingestor.  It is the read path for large-scale ML training queries (DuckDB /
Spark), not the input to the staging pipeline.  Reading staging from Postgres
avoids the 1-hour Parquet lag, keeps the staging step independent of MinIO
availability, and lets us use full Postgres SQL expressiveness (window
functions, CTEs, ON CONFLICT) without an embedded in-process query engine.

Window parameter: ingested_at, not sim_ts
-----------------------------------------
The window filter uses ingested_at (wall-clock) because:
  - ingested_at is always aligned with real time regardless of SIM_SPEED
  - It matches the ParquetWriter's partitioning convention
  - The SQL still groups by sim_ts / polled_at so staging rows carry
    business-time keys (hour_ts = date_trunc('hour', sim_ts))
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import structlog

from .settings import Settings
from .sql_runner import SqlRunner

log = structlog.get_logger()

_SQL_DIR = Path(__file__).parent.parent / "sql" / "staging"


def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def run_staging(settings: Settings) -> tuple[int, int]:
    """
    Execute sql/staging/001_demand.sql and sql/staging/002_weather.sql
    for the configured ``training_lookback_days`` window.

    Each SQL file deduplicates on event_id, classifies null violations,
    writes rejects to staging.rejects, then aggregates clean rows into
    the staging table — all in a single Postgres round-trip per file.

    The total reject and staging-write counts are logged at INFO level.
    Re-running is fully idempotent (ON CONFLICT in every SQL file).
    """
    window_end   = _floor_hour(datetime.now(timezone.utc))
    window_start = window_end - timedelta(days=settings.training_lookback_days)

    log.info(
        "staging_started",
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        lookback_days=settings.training_lookback_days,
    )

    params  = {"window_start": window_start, "window_end": window_end}
    runner  = SqlRunner(_SQL_DIR)

    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        results = runner.run(conn, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    total_rejects = sum(int(r.get("rejects_written", 0) or 0) for r in results)
    total_staging = sum(int(r.get("staging_written", 0) or 0) for r in results)
    log.info(
        "staging_done",
        files_run=len(results),
        total_rejects=total_rejects,
        total_staging_upserts=total_staging,
    )
    return total_staging, total_rejects
