"""
backfill — replay Parquet lake partitions → raw.* Postgres tables.

Usage
-----
  python -m tools.backfill --date-start YYYY-MM-DD --date-end YYYY-MM-DD
  python -m tools.backfill --date-start 2026-09-01 --date-end 2026-09-07 \\
      --tables demand_events
  python -m tools.backfill --date-start 2026-09-01 --date-end 2026-09-07 \\
      --dry-run

Each Parquet partition (one per table/day/hour) is downloaded from
s3://lake/raw/{table}/dt=.../hour=.../data.parquet and bulk-inserted into the
matching raw.* Postgres table with ON CONFLICT (event_id) DO NOTHING — so
re-running is always safe.

After a successful backfill, run the batch pipeline to propagate changes
through staging → marts:

    make worker-pipeline
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from datetime import date, timedelta

import boto3
import botocore.exceptions
import pandas as pd
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Column lists (must match db.py INSERT statements exactly)
# ---------------------------------------------------------------------------

_DEMAND_COLS: list[str] = [
    "city", "event_type", "sim_ts", "quantity",
    "temperature_c", "condition",
    "kafka_partition", "kafka_offset", "schema_version", "event_id",
]

_WEATHER_COLS: list[str] = [
    "city", "polled_at",
    "temperature_c", "feels_like_c", "dew_point_c",
    "humidity_pct", "wind_kph", "wind_direction_deg", "cloud_cover_pct",
    "precip_probability_pct", "precip_mm", "condition",
    "kafka_partition", "kafka_offset", "schema_version", "event_id",
]

_INSERT_SQL: dict[str, str] = {
    "demand_events": (
        "INSERT INTO raw.demand_events "
        "(city, event_type, sim_ts, quantity, temperature_c, condition, "
        "kafka_partition, kafka_offset, schema_version, event_id) "
        "VALUES %s ON CONFLICT (event_id) DO NOTHING"
    ),
    "weather_readings": (
        "INSERT INTO raw.weather_readings "
        "(city, polled_at, temperature_c, feels_like_c, dew_point_c, "
        "humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct, "
        "precip_probability_pct, precip_mm, condition, "
        "kafka_partition, kafka_offset, schema_version, event_id) "
        "VALUES %s ON CONFLICT (event_id) DO NOTHING"
    ),
}

_TABLE_COLS: dict[str, list[str]] = {
    "demand_events": _DEMAND_COLS,
    "weather_readings": _WEATHER_COLS,
}

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def _pg_dsn() -> str:
    if dsn := os.environ.get("POSTGRES_DSN"):
        return dsn
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "forecast")
    user = os.environ.get("POSTGRES_USER", "forecast")
    pw   = os.environ.get("POSTGRES_PASSWORD", "")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# MinIO download
# ---------------------------------------------------------------------------


def _download_parquet(s3, bucket: str, key: str) -> pd.DataFrame | None:
    """Download a single Parquet object from MinIO; return None if absent."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return None
        print(f"  warn: s3 error for {key}: {exc}", file=sys.stderr)
        return None
    except Exception as exc:
        print(f"  warn: could not read {key}: {exc}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Postgres insert
# ---------------------------------------------------------------------------


def _to_row(df_row, cols: list[str]) -> tuple:
    """Convert a DataFrame row to a tuple, mapping NaN → None."""
    return tuple(
        None if (v is None or (isinstance(v, float) and v != v)) else v
        for v in (df_row.get(c) for c in cols)
    )


def _insert_partition(
    conn: "psycopg2.connection",
    table: str,
    df: pd.DataFrame,
    dry_run: bool,
) -> tuple[int, int]:
    """Insert rows from df into raw.<table>.

    Returns (attempted, inserted) where:
      attempted = rows in the Parquet file
      inserted  = rows actually written (0 when all event_ids already exist)
    """
    cols = _TABLE_COLS[table]
    rows = [_to_row(row, cols) for _, row in df.iterrows()]
    if not rows:
        return 0, 0
    if dry_run:
        return len(rows), len(rows)
    with conn.cursor() as cur:
        # page_size=len(rows) issues a single INSERT so cur.rowcount reflects
        # the total number of rows actually inserted (ON CONFLICT skips = 0).
        psycopg2.extras.execute_values(
            cur, _INSERT_SQL[table], rows, page_size=len(rows)
        )
        inserted = cur.rowcount if cur.rowcount >= 0 else 0
    conn.commit()
    return len(rows), inserted


# ---------------------------------------------------------------------------
# Date range generator
# ---------------------------------------------------------------------------


def _date_range(start: str, end: str):
    d    = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while d <= stop:
        yield d
        d += timedelta(days=1)


# ---------------------------------------------------------------------------
# Main backfill logic
# ---------------------------------------------------------------------------


def backfill(
    date_start: str,
    date_end: str,
    tables: list[str],
    bucket: str,
    dry_run: bool,
) -> None:
    s3   = _s3_client()
    conn = None if dry_run else psycopg2.connect(_pg_dsn())

    total_rows  = 0
    total_files = 0
    tag = "[dry-run] " if dry_run else ""

    try:
        for dt in _date_range(date_start, date_end):
            dt_str = dt.isoformat()
            for table in tables:
                for hour in range(24):
                    key = f"raw/{table}/dt={dt_str}/hour={hour:02d}/data.parquet"
                    df  = _download_parquet(s3, bucket, key)
                    if df is None or df.empty:
                        continue
                    attempted, inserted = _insert_partition(conn, table, df, dry_run)
                    if attempted:
                        total_files += 1
                        total_rows  += inserted
                        label = f"{inserted} new / {attempted} attempted"
                        print(f"  {tag}{key}: {label} rows")
    finally:
        if conn is not None:
            conn.close()

    print(
        f"\n{tag}Backfill complete: "
        f"{total_rows} row(s) from {total_files} file(s)."
    )
    if not dry_run and total_rows > 0:
        print(
            "Run `make worker-pipeline` to propagate changes "
            "through staging → marts."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay Parquet lake partitions into raw.* Postgres tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date-start", required=True, metavar="YYYY-MM-DD",
        help="First date to backfill (inclusive)",
    )
    parser.add_argument(
        "--date-end", required=True, metavar="YYYY-MM-DD",
        help="Last date to backfill (inclusive)",
    )
    parser.add_argument(
        "--tables", nargs="+",
        choices=["demand_events", "weather_readings"],
        default=["demand_events", "weather_readings"],
        help="Tables to backfill (default: both)",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("LAKE_BUCKET", "lake"),
        help="MinIO bucket name (default: lake)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be inserted without writing to Postgres",
    )
    args = parser.parse_args()

    backfill(
        date_start=args.date_start,
        date_end=args.date_end,
        tables=args.tables,
        bucket=args.bucket,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
