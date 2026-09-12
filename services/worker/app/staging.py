"""
raw → staging

Reads from the Parquet lake (MinIO via DuckDB httpfs), aggregates raw
demand events and weather readings to one row per (city, business hour),
and upserts the results into the Postgres staging tables.

Why business time (sim_ts / polled_at), not ingested_at?
---------------------------------------------------------
The feature table must align to business time so the ML model learns genuine
temporal patterns (hour-of-day demand peaks, weekday/weekend seasonality,
etc.).  ``ingested_at`` is the wall-clock write time; it reflects Kafka lag
and ParquetWriter trigger timing, not business reality.  ``sim_ts`` and
``polled_at`` are the actual event times and are always preserved in Parquet.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
import psycopg2
import psycopg2.extras
import structlog

from .settings import Settings

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _esc(s: str) -> str:
    """Escape single quotes for embedding in a DuckDB SQL string literal."""
    return s.replace("'", "''")


def _nan_to_none(val: object) -> object:
    try:
        return None if math.isnan(val) else val  # type: ignore[arg-type]
    except TypeError:
        return val


def _duck_con(settings: Settings) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with S3/MinIO credentials loaded."""
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    endpoint = (
        settings.minio_endpoint_url
        .removeprefix("http://")
        .removeprefix("https://")
    )
    use_ssl = "true" if settings.minio_endpoint_url.startswith("https://") else "false"
    con.execute(f"""
        CREATE SECRET lake_minio (
            TYPE      s3,
            KEY_ID    '{_esc(settings.minio_access_key)}',
            SECRET    '{_esc(settings.minio_secret_key)}',
            ENDPOINT  '{_esc(endpoint)}',
            URL_STYLE 'path',
            USE_SSL   {use_ssl}
        )
    """)
    return con


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_staging(settings: Settings) -> None:
    """
    Populate staging.demand_hourly and staging.weather_hourly for the
    configured ``training_lookback_days`` window and upsert into Postgres.

    The window is ``[now_hour - lookback_days, now_hour)``.  Re-running is
    idempotent: every upsert uses ``ON CONFLICT DO UPDATE``.
    """
    window_end = _floor_hour(datetime.now(timezone.utc))
    window_start = window_end - timedelta(days=settings.training_lookback_days)

    log.info(
        "staging_started",
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        lookback_days=settings.training_lookback_days,
    )

    con = _duck_con(settings)
    demand_df = _read_demand(con, settings, window_start, window_end)
    weather_df = _read_weather(con, settings, window_start, window_end)

    log.info(
        "staging_read",
        demand_rows=len(demand_df),
        weather_rows=len(weather_df),
    )

    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        _upsert_demand_hourly(conn, demand_df)
        _upsert_weather_hourly(conn, weather_df)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("staging_done")


# ---------------------------------------------------------------------------
# DuckDB reads
# ---------------------------------------------------------------------------

def _read_demand(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
) -> pd.DataFrame:
    glob = (
        f"s3://{_esc(settings.lake_bucket)}"
        "/raw/demand_events/dt=*/hour=*/data.parquet"
    )
    try:
        return con.execute(
            f"""
            SELECT
                city,
                date_trunc('hour', sim_ts)::TIMESTAMPTZ  AS hour_ts,
                SUM(quantity::DOUBLE)                     AS total_demand,
                COUNT(*)                                  AS event_count,
                AVG(temperature_c::DOUBLE)                AS avg_temp_c
            FROM read_parquet('{glob}', hive_partitioning = true)
            WHERE sim_ts >= ? AND sim_ts < ?
            GROUP BY city, date_trunc('hour', sim_ts)
            ORDER BY city, hour_ts
            """,
            [window_start, window_end],
        ).df()
    except duckdb.IOException as exc:
        log.warning("staging_demand_read_failed", error=str(exc))
        return pd.DataFrame(
            columns=["city", "hour_ts", "total_demand", "event_count", "avg_temp_c"]
        )


def _read_weather(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
) -> pd.DataFrame:
    glob = (
        f"s3://{_esc(settings.lake_bucket)}"
        "/raw/weather_readings/dt=*/hour=*/data.parquet"
    )
    try:
        return con.execute(
            f"""
            SELECT
                city,
                date_trunc('hour', polled_at)::TIMESTAMPTZ  AS hour_ts,
                AVG(temperature_c::DOUBLE)                   AS temperature_c,
                AVG(humidity_pct::DOUBLE)                    AS humidity_pct,
                SUM(precip_mm::DOUBLE)                       AS precip_mm
            FROM read_parquet('{glob}', hive_partitioning = true)
            WHERE polled_at >= ? AND polled_at < ?
            GROUP BY city, date_trunc('hour', polled_at)
            ORDER BY city, hour_ts
            """,
            [window_start, window_end],
        ).df()
    except duckdb.IOException as exc:
        log.warning("staging_weather_read_failed", error=str(exc))
        return pd.DataFrame(
            columns=["city", "hour_ts", "temperature_c", "humidity_pct", "precip_mm"]
        )


# ---------------------------------------------------------------------------
# Postgres upserts
# ---------------------------------------------------------------------------

def _upsert_demand_hourly(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
) -> None:
    if df.empty:
        return
    rows = [
        (
            row.city,
            row.hour_ts,
            _nan_to_none(row.total_demand),
            int(row.event_count),
            _nan_to_none(row.avg_temp_c),
        )
        for row in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO staging.demand_hourly
                (city, hour_ts, total_demand, event_count, avg_temp_c)
            VALUES %s
            ON CONFLICT (city, hour_ts) DO UPDATE SET
                total_demand = EXCLUDED.total_demand,
                event_count  = EXCLUDED.event_count,
                avg_temp_c   = EXCLUDED.avg_temp_c
            """,
            rows,
        )
    log.info("staging_demand_upserted", rows=len(rows))


def _upsert_weather_hourly(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
) -> None:
    if df.empty:
        return
    rows = [
        (
            row.city,
            row.hour_ts,
            _nan_to_none(row.temperature_c),
            _nan_to_none(row.humidity_pct),
            _nan_to_none(row.precip_mm),
        )
        for row in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO staging.weather_hourly
                (city, hour_ts, temperature_c, humidity_pct, precip_mm)
            VALUES %s
            ON CONFLICT (city, hour_ts) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                humidity_pct  = EXCLUDED.humidity_pct,
                precip_mm     = EXCLUDED.precip_mm
            """,
            rows,
        )
    log.info("staging_weather_upserted", rows=len(rows))
