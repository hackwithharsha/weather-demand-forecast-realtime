"""
lake — DuckDB-powered CLI for the Parquet data lake on MinIO.

Commands
--------
  list    [PREFIX]               List objects in the lake bucket.
  preview GLOB [--rows N]        Print first N rows from a file or glob.
  query   SQL                    Run arbitrary DuckDB SQL (S3 pre-configured).
  counts  TABLE                  Count rows grouped by city across hourly
          [--dt DATE]            partitions.  TABLE is demand_events or
          [--hours H [H ...]]    weather_readings.
          [--last N]

Examples
--------
  # What's in the lake?
  lake list
  lake list raw/demand_events/dt=2026-09-12/

  # Peek at a single partition file
  lake preview raw/demand_events/dt=2026-09-12/hour=14/data.parquet --rows 5

  # Arbitrary DuckDB SQL (all s3://lake/... paths work)
  lake query "SELECT city, count(*) FROM read_parquet( \\
      's3://lake/raw/demand_events/dt=*/hour=*/data.parquet',   \\
      hive_partitioning=true) GROUP BY 1 ORDER BY 2 DESC"

  # City-level event counts for the last 3 completed hours
  lake counts demand_events --last 3

  # Specific hours on a given date
  lake counts demand_events --dt 2026-09-12 --hours 14 15 16
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
import duckdb
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _endpoint_url() -> str:
    return _env("MINIO_ENDPOINT_URL", "http://minio:9000")


def _endpoint_host() -> str:
    """Strip URL scheme — DuckDB's ENDPOINT param expects host[:port] only."""
    url = _endpoint_url()
    return url.removeprefix("http://").removeprefix("https://")


def _access_key() -> str:
    return _env("MINIO_ACCESS_KEY", "minioadmin")


def _secret_key() -> str:
    return _env("MINIO_SECRET_KEY", "minioadmin")


def _bucket() -> str:
    return _env("LAKE_BUCKET", "lake")


def _esc(s: str) -> str:
    """Escape single quotes for embedding in a DuckDB string literal."""
    return s.replace("'", "''")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_endpoint_url(),
        aws_access_key_id=_access_key(),
        aws_secret_access_key=_secret_key(),
    )


def _s3_url(path: str) -> str:
    """Expand a bare relative path to a full s3:// URL."""
    if path.startswith("s3://"):
        return path
    return f"s3://{_bucket()}/{path.lstrip('/')}"


# ---------------------------------------------------------------------------
# DuckDB session
# ---------------------------------------------------------------------------

def _duck_con() -> duckdb.DuckDBPyConnection:
    """
    Return a fresh in-memory DuckDB connection with httpfs loaded and MinIO
    credentials registered via CREATE SECRET.

    Notes
    -----
    * ``LOAD httpfs`` (not ``INSTALL``): the httpfs extension is bundled with
      the duckdb Python package; INSTALL would attempt an internet download.
    * ``CREATE SECRET`` (DuckDB ≥ 0.10): the idiomatic 1.x credential API.
      Secrets in an in-memory connection are session-scoped and not written to
      ``~/.duckdb/stored_secrets/``.
    * Single-quote escaping via ``_esc()`` covers credentials that contain
      apostrophes (operator-controlled env vars, not user input).
    """
    con = duckdb.connect()
    con.execute("LOAD httpfs")

    use_ssl = "true" if _endpoint_url().startswith("https://") else "false"

    con.execute(f"""
        CREATE SECRET lake_minio (
            TYPE      s3,
            KEY_ID    '{_esc(_access_key())}',
            SECRET    '{_esc(_secret_key())}',
            ENDPOINT  '{_esc(_endpoint_host())}',
            URL_STYLE 'path',
            USE_SSL   {use_ssl}
        )
    """)
    return con


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _print_table(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Execute *sql* and render the result as a plain ASCII box table."""
    rel = con.execute(sql)
    col_names = [d[0] for d in rel.description]
    rows = rel.fetchall()

    if not rows:
        print("(no rows)")
        return

    # Stringify every cell, compute column widths.
    str_rows = [
        [("NULL" if v is None else str(v)) for v in row]
        for row in rows
    ]
    widths = [len(name) for name in col_names]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    fmt = "|" + "|".join(f" {{:<{w}}} " for w in widths) + "|"

    print(sep)
    print(fmt.format(*col_names))
    print(sep)
    for row in str_rows:
        print(fmt.format(*row))
    print(sep)
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


# ---------------------------------------------------------------------------
# Slot helpers (date + hour pairs)
# ---------------------------------------------------------------------------

def _last_n_slots(n: int) -> list[tuple[str, int]]:
    """
    Return the (dt_str, hour) pairs for the last *n* completed UTC hours,
    ordered oldest-first.  Correctly spans midnight boundaries.
    """
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    slots = []
    for i in range(n, 0, -1):          # oldest → newest
        h = now - timedelta(hours=i)
        slots.append((h.strftime("%Y-%m-%d"), h.hour))
    return slots


def _slots_for_date(dt: str, hours: list[int]) -> list[tuple[str, int]]:
    return [(dt, h) for h in sorted(hours)]


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_list(prefix: str) -> None:
    """List Parquet objects under *prefix* and print with sizes."""
    s3 = _s3_client()
    bucket = _bucket()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            size_kb = obj["Size"] / 1024
            print(f"  {obj['Key']}  ({size_kb:.1f} KB)")
            count += 1
            total_bytes += obj["Size"]
    if count == 0:
        print(
            f"(no objects found under s3://{bucket}/{prefix})",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"\n{count} object(s)  —  {total_bytes / 1024:.1f} KB total")


def cmd_preview(glob: str, rows: int) -> None:
    """Print the first *rows* rows from a Parquet file or glob pattern."""
    url = _s3_url(glob)
    con = _duck_con()
    _print_table(
        con,
        f"SELECT * FROM read_parquet('{_esc(url)}', hive_partitioning = true)"
        f" LIMIT {rows}",
    )


def cmd_query(sql: str) -> None:
    """
    Execute arbitrary DuckDB SQL with the S3 session pre-configured.

    All ``s3://lake/...`` paths are directly accessible.  Use
    ``read_parquet('...', hive_partitioning=true)`` to get ``dt`` and
    ``hour`` as filterable columns from the path.
    """
    con = _duck_con()
    _print_table(con, sql)


def cmd_counts(table: str, slots: list[tuple[str, int]]) -> None:
    """
    Count rows grouped by city for *table* across the requested hourly
    partitions.

    Uses an explicit list of S3 paths (one per slot) so the query works
    correctly whether the slots span one date or several.  Slots that
    have not yet been written to S3 are reported but skipped gracefully.
    """
    bucket = _bucket()
    s3 = _s3_client()

    # ── Resolve which partition files actually exist ────────────────────
    found: list[str] = []
    missing: list[str] = []
    for dt_str, hour in slots:
        key = f"raw/{table}/dt={dt_str}/hour={hour:02d}/data.parquet"
        try:
            s3.head_object(Bucket=bucket, Key=key)
            found.append(f"s3://{bucket}/{key}")
        except ClientError:
            missing.append(f"dt={dt_str}/hour={hour:02d}")

    # ── Print summary header ────────────────────────────────────────────
    slot_labels = [f"dt={d}/h={h:02d}" for d, h in slots]
    print(f"table    : {table}")
    print(f"slots    : {', '.join(slot_labels)}")
    print(f"partitions: {len(found)}/{len(slots)} found", end="")
    if missing:
        print(f"  (missing: {', '.join(missing)})", end="")
    print("\n")

    if not found:
        print(
            "No partitions found.  Run  lake list  to see what is available.",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Build and run the DuckDB query ──────────────────────────────────
    # read_parquet accepts a list literal; union_by_name handles any minor
    # schema drift between hourly files (e.g. all-NULL columns inferred
    # differently by PyArrow on different runs).
    paths_literal = "[" + ", ".join(f"'{p}'" for p in found) + "]"

    sql = f"""
        SELECT
            city,
            count(*)                        AS events,
            min(ingested_at)::VARCHAR        AS first_ingested,
            max(ingested_at)::VARCHAR        AS last_ingested
        FROM read_parquet({paths_literal}, union_by_name = true)
        GROUP BY city
        ORDER BY events DESC
    """
    con = _duck_con()
    _print_table(con, sql)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lake",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="command", required=True)

    # list
    p_list = sub.add_parser("list", help="list objects in the lake bucket")
    p_list.add_argument(
        "prefix",
        nargs="?",
        default="raw/",
        help="S3 prefix (default: raw/)",
    )

    # preview
    p_prev = sub.add_parser(
        "preview", help="print first N rows from a file or glob"
    )
    p_prev.add_argument("glob", help="Parquet path or glob (raw/... or s3://...)")
    p_prev.add_argument(
        "--rows", type=int, default=10, metavar="N",
        help="rows to display (default: 10)",
    )

    # query
    p_qry = sub.add_parser(
        "query", help="run arbitrary DuckDB SQL (S3 pre-configured)"
    )
    p_qry.add_argument("sql", help="SQL statement to execute")

    # counts
    p_cnt = sub.add_parser(
        "counts",
        help="count rows grouped by city across hourly partitions",
    )
    p_cnt.add_argument(
        "table",
        choices=["demand_events", "weather_readings"],
        help="table to query",
    )
    when = p_cnt.add_mutually_exclusive_group()
    when.add_argument(
        "--last", type=int, metavar="N",
        help="last N completed UTC hours (default: 3)",
    )
    when.add_argument(
        "--hours", type=int, nargs="+", metavar="H",
        help="explicit hours 0-23 on --dt date",
    )
    p_cnt.add_argument(
        "--dt",
        metavar="DATE",
        help="partition date YYYY-MM-DD (required with --hours; "
             "default today UTC with --last)",
    )

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "list":
        cmd_list(args.prefix)

    elif args.command == "preview":
        cmd_preview(args.glob, args.rows)

    elif args.command == "query":
        cmd_query(args.sql)

    elif args.command == "counts":
        if args.hours is not None:
            if not args.dt:
                parser.error("--hours requires --dt DATE")
            slots = _slots_for_date(args.dt, args.hours)
        else:
            n = args.last if args.last else 3
            slots = _last_n_slots(n)
            if args.dt:
                # --dt with --last: filter slots to the specified date
                slots = [(d, h) for d, h in slots if d == args.dt]
                if not slots:
                    parser.error(
                        f"--dt {args.dt} produced no slots "
                        f"within the last {n} hours"
                    )
        cmd_counts(args.table, slots)


if __name__ == "__main__":
    main()
