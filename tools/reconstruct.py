#!/usr/bin/env python3
"""
reconstruct.py — Replay the exact sliding-window computation for any
(route, timestamp) from the Parquet lake, reproducing the values that Redis
holds at that moment.

The tool reads from  stream/demand_events/  — the append-only, sim_ts-
partitioned Parquet files written by the worker's stream-feature consumers.
Because the files are partitioned by *business event time* (not wall-clock
ingestion time), an exact 1-hour window scan requires loading at most two
hourly partition prefixes.

Algorithm (mirrors update_demand_features in redis_ops.py exactly)
------------------------------------------------------------------
Given target time T (unix epoch seconds) and city/route R:

  searches_5m  = COUNT(DISTINCT event_id)
                   WHERE city=R AND event_type='search'
                     AND sim_epoch ∈ [T−300, T]

  bookings_15m = COUNT(DISTINCT event_id)
                   WHERE city=R AND event_type='booking'
                     AND sim_epoch ∈ [T−900, T]

  searches_1h  = COUNT(DISTINCT event_id) ... sim_epoch ∈ [T−3600, T]
  bookings_1h  = COUNT(DISTINCT event_id) ... sim_epoch ∈ [T−3600, T]

  look_to_book_1h = bookings_1h / max(1, searches_1h)

Usage
-----
  # Point-in-time reconstruction for london at a given sim_ts
  python3 tools/reconstruct.py \\
      --route london \\
      --at 2028-10-29T14:30:00+00:00

  # Compare to live Redis values for the route
  python3 tools/reconstruct.py \\
      --route london \\
      --at 2028-10-29T14:30:00+00:00 \\
      --verify

  # Auto-pick the timestamp from Redis stream_computed_at
  python3 tools/reconstruct.py --route london --verify-latest

Environment (all optional — flags take precedence)
---------
  MINIO_ENDPOINT_URL   default http://localhost:9000
  MINIO_ACCESS_KEY     default svc-lake
  MINIO_SECRET_KEY     default changeme-svc-lake
  LAKE_BUCKET          default lake
  REDIS_HOST           default localhost
  REDIS_PORT           default 6379
  REDIS_PASSWORD       (required for --verify / --verify-latest)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import boto3
import duckdb
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Window constants (must mirror redis_ops.py exactly)
# ---------------------------------------------------------------------------

_W_5M  =   300   # searches window
_W_15M =   900   # bookings window
_W_1H  = 3_600   # longest window (used for trimming and ratio denominator)

_SEARCH_TYPES  = frozenset({"search"})
_BOOKING_TYPES = frozenset({"booking"})


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _endpoint_url() -> str:
    return _env("MINIO_ENDPOINT_URL", "http://localhost:9000")


def _endpoint_host() -> str:
    url = _endpoint_url()
    return url.removeprefix("http://").removeprefix("https://")


def _access_key() -> str:
    return _env("MINIO_ACCESS_KEY", "svc-lake")


def _secret_key() -> str:
    return _env("MINIO_SECRET_KEY", "changeme-svc-lake")


def _bucket() -> str:
    return _env("LAKE_BUCKET", "lake")


def _esc(s: str) -> str:
    return s.replace("'", "''")


# ---------------------------------------------------------------------------
# DuckDB session — identical pattern to lake.py
# ---------------------------------------------------------------------------

def _duck_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
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
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_endpoint_url(),
        aws_access_key_id=_access_key(),
        aws_secret_access_key=_secret_key(),
    )


def _window_slots(t_epoch: float, window_s: float = _W_1H) -> list[tuple[str, int]]:
    """Return (dt_str, hour) pairs spanning [t_epoch − window_s, t_epoch]."""
    lo_dt = datetime.fromtimestamp(t_epoch - window_s, tz=timezone.utc)
    hi_dt = datetime.fromtimestamp(t_epoch, tz=timezone.utc)
    # Floor to hour boundary.
    lo_h = lo_dt.replace(minute=0, second=0, microsecond=0)
    hi_h = hi_dt.replace(minute=0, second=0, microsecond=0)
    slots: list[tuple[str, int]] = []
    cur = lo_h
    while cur <= hi_h:
        slots.append((cur.strftime("%Y-%m-%d"), cur.hour))
        cur += timedelta(hours=1)
    return slots


def _list_stream_paths(s3, bucket: str, slots: list[tuple[str, int]]) -> list[str]:
    """Return all part_*.parquet keys under stream/demand_events/ for given slots."""
    found: list[str] = []
    for dt_str, hour in slots:
        prefix = f"stream/demand_events/dt={dt_str}/hour={hour:02d}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    found.append(f"s3://{bucket}/{key}")
    return found


# ---------------------------------------------------------------------------
# Core reconstruction
# ---------------------------------------------------------------------------

class WindowResult(NamedTuple):
    searches_5m:     float
    bookings_15m:    float
    look_to_book_1h: float
    searches_1h:     float
    bookings_1h:     float
    files_scanned:   int
    rows_scanned:    int


def reconstruct(
    route: str,
    t_epoch: float,
    con: duckdb.DuckDBPyConnection,
    s3,
    bucket: str,
) -> WindowResult | None:
    """
    Replay sliding-window aggregates for *route* at epoch *t_epoch*.

    Returns None if no Parquet files exist for the required time window.
    """
    slots = _window_slots(t_epoch)
    paths = _list_stream_paths(s3, bucket, slots)

    if not paths:
        return None

    # Build a DuckDB list literal from the S3 paths.
    prefix = f"s3://{bucket}/"
    paths_literal = "[" + ", ".join(
        f"'{prefix}{_esc(p[len(prefix):])}'" for p in paths
    ) + "]"

    lo_5m  = t_epoch - _W_5M
    lo_15m = t_epoch - _W_15M
    lo_1h  = t_epoch - _W_1H

    # Mirror the exact ZCOUNT semantics from redis_ops.py:
    #   - ZADD is idempotent via event_id (member uniqueness)
    #   - COUNT(DISTINCT event_id) replicates that deduplication from Parquet
    #   - epoch(sim_ts::TIMESTAMPTZ) converts ISO-8601 string → Unix seconds
    sql = f"""
        SELECT
            COUNT(DISTINCT
                CASE WHEN event_type = 'search'
                          AND epoch(sim_ts::TIMESTAMPTZ) >= {lo_5m}
                     THEN event_id END
            )                                        AS searches_5m,
            COUNT(DISTINCT
                CASE WHEN event_type = 'booking'
                          AND epoch(sim_ts::TIMESTAMPTZ) >= {lo_15m}
                     THEN event_id END
            )                                        AS bookings_15m,
            COUNT(DISTINCT
                CASE WHEN event_type = 'search'
                          AND epoch(sim_ts::TIMESTAMPTZ) >= {lo_1h}
                     THEN event_id END
            )                                        AS searches_1h,
            COUNT(DISTINCT
                CASE WHEN event_type = 'booking'
                          AND epoch(sim_ts::TIMESTAMPTZ) >= {lo_1h}
                     THEN event_id END
            )                                        AS bookings_1h,
            COUNT(*)                                 AS rows_scanned
        FROM read_parquet({paths_literal}, union_by_name = true)
        WHERE city = '{_esc(route)}'
          AND epoch(sim_ts::TIMESTAMPTZ) BETWEEN {lo_1h} AND {t_epoch}
    """

    row = con.execute(sql).fetchone()
    if row is None:
        return WindowResult(0.0, 0.0, 0.0, 0.0, 0.0, len(paths), 0)

    searches_5m, bookings_15m, searches_1h, bookings_1h, rows_scanned = row
    searches_5m   = float(searches_5m  or 0)
    bookings_15m  = float(bookings_15m or 0)
    searches_1h   = float(searches_1h  or 0)
    bookings_1h   = float(bookings_1h  or 0)
    rows_scanned  = int(rows_scanned   or 0)

    look_to_book_1h = bookings_1h / max(1.0, searches_1h)

    return WindowResult(
        searches_5m     = searches_5m,
        bookings_15m    = bookings_15m,
        look_to_book_1h = round(look_to_book_1h, 6),
        searches_1h     = searches_1h,
        bookings_1h     = bookings_1h,
        files_scanned   = len(paths),
        rows_scanned    = rows_scanned,
    )


# ---------------------------------------------------------------------------
# Redis helpers (--verify / --verify-latest)
# ---------------------------------------------------------------------------

def _redis_connect() -> "redis.Redis":
    try:
        import redis
    except ImportError:
        print("ERROR: 'redis' package not installed. pip install redis", file=sys.stderr)
        sys.exit(1)

    host     = _env("REDIS_HOST", "localhost")
    port     = int(_env("REDIS_PORT", "6379"))
    password = _env("REDIS_PASSWORD", "") or None

    r = redis.Redis(
        host=host, port=port, password=password,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    r.ping()
    return r


def _redis_get_features(route: str) -> dict[str, str]:
    r = _redis_connect()
    key = f"feat:route:{route}"
    data = r.hgetall(key)
    if not data:
        print(f"WARNING: no Redis key found for {key!r}", file=sys.stderr)
    return data


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

_W = 72

def _hdr() -> None:
    print("─" * _W)


def _banner(title: str) -> None:
    print("═" * _W)
    print(f"  {title}")
    print("═" * _W)


def _print_result(res: WindowResult, label: str = "Parquet reconstruction") -> None:
    print(f"\n  {label}")
    print(f"    searches_5m     : {res.searches_5m}")
    print(f"    bookings_15m    : {res.bookings_15m}")
    print(f"    look_to_book_1h : {res.look_to_book_1h}")
    print(f"    searches_1h     : {res.searches_1h}")
    print(f"    bookings_1h     : {res.bookings_1h}")
    print(f"    files scanned   : {res.files_scanned}")
    print(f"    rows scanned    : {res.rows_scanned:,}")


def _print_comparison(reconstructed: WindowResult, redis_data: dict[str, str]) -> None:
    _hdr()
    print()
    print("  VERIFY — Parquet reconstruction vs. live Redis")
    print()

    checks: list[tuple[str, float, str]] = [
        ("searches_5m",     reconstructed.searches_5m,
                            redis_data.get("searches_5m",     "missing")),
        ("bookings_15m",    reconstructed.bookings_15m,
                            redis_data.get("bookings_15m",    "missing")),
        ("look_to_book_1h", reconstructed.look_to_book_1h,
                            redis_data.get("look_to_book_1h", "missing")),
    ]

    all_match = True
    for field, parquet_val, redis_raw in checks:
        try:
            redis_val = round(float(redis_raw), 6)
        except (ValueError, TypeError):
            redis_val = None

        match = (redis_val is not None) and abs(parquet_val - redis_val) < 1e-3
        if not match:
            all_match = False
        status = "MATCH" if match else "DIFF "
        print(
            f"  {status}  {field:<20s}"
            f"  parquet={parquet_val:<12}  redis={redis_val}"
        )

    print()
    redis_computed_at = redis_data.get("stream_computed_at", "N/A")
    print(f"  Redis stream_computed_at : {redis_computed_at}")
    print()

    if all_match:
        print("  RESULT : all fields match ✓")
    else:
        print(
            "  RESULT : mismatch detected — this is expected when the\n"
            "           --at timestamp differs from Redis stream_computed_at.\n"
            "           For an exact match, use --verify-latest which auto-\n"
            "           picks the timestamp recorded in Redis."
        )
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--route", required=True,
        help="City / route identifier (e.g. london, new_york)",
    )

    when = ap.add_mutually_exclusive_group(required=True)
    when.add_argument(
        "--at", metavar="ISO_TIMESTAMP",
        help="sim_ts at which to reconstruct features (ISO-8601 with timezone)",
    )
    when.add_argument(
        "--verify-latest", action="store_true",
        help=(
            "Read stream_computed_at from Redis for the route and reconstruct "
            "at that exact timestamp, then compare to current Redis values."
        ),
    )

    ap.add_argument(
        "--verify", action="store_true",
        help=(
            "After reconstruction, compare values to current Redis "
            "feat:route:<route> hash."
        ),
    )

    args = ap.parse_args()

    route = args.route

    # ── Resolve the target timestamp ───────────────────────────────────────
    if args.verify_latest:
        redis_data = _redis_get_features(route)
        computed_at_str = redis_data.get("stream_computed_at")
        if not computed_at_str:
            print(
                f"ERROR: Redis key feat:route:{route} has no stream_computed_at field.\n"
                "       Is the worker's stream consumer running?",
                file=sys.stderr,
            )
            sys.exit(1)
        # stream_computed_at is wall-clock time, not sim_ts.
        # We need to find the sim_ts that was last processed.
        # The best proxy: use the most recent sim_ts in the lake for this route.
        print(f"\n  Redis stream_computed_at : {computed_at_str}")
        print("  Locating most recent sim_ts for this route in the lake …")

        s3 = _s3_client()
        bucket = _bucket()
        # List all stream/demand_events/ partitions in reverse order.
        paginator = s3.get_paginator("list_objects_v2")
        all_keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix="stream/demand_events/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    all_keys.append(key)

        if not all_keys:
            print(
                "ERROR: no stream/demand_events/ Parquet files found in the lake.",
                file=sys.stderr,
            )
            sys.exit(1)

        # The most recent partition by path sort (dt=YYYY-MM-DD/hour=HH sorts lexicographically).
        all_keys.sort(reverse=True)
        latest_key = all_keys[0]
        print(f"  Latest partition file    : {latest_key}")

        # Query the latest sim_ts for the given route from that file.
        con = _duck_con()
        max_ts_sql = f"""
            SELECT MAX(sim_ts) AS max_sim_ts
            FROM read_parquet('s3://{_esc(bucket)}/{_esc(latest_key)}', union_by_name=true)
            WHERE city = '{_esc(route)}'
        """
        row = con.execute(max_ts_sql).fetchone()
        if row is None or row[0] is None:
            # Try a few more partitions.
            for key in all_keys[1:10]:
                row = con.execute(
                    f"SELECT MAX(sim_ts) FROM read_parquet('s3://{_esc(bucket)}/{_esc(key)}') "
                    f"WHERE city = '{_esc(route)}'"
                ).fetchone()
                if row and row[0]:
                    break

        if row is None or row[0] is None:
            print(
                f"ERROR: no records found for route {route!r} in the latest Parquet files.",
                file=sys.stderr,
            )
            sys.exit(1)

        max_sim_ts_str = str(row[0])
        print(f"  Latest sim_ts for route  : {max_sim_ts_str}")
        t_epoch = datetime.fromisoformat(max_sim_ts_str).timestamp()
        verify_after = True
        redis_data_for_verify = redis_data

    else:
        try:
            t_dt = datetime.fromisoformat(args.at)
            if t_dt.tzinfo is None:
                print(
                    "WARNING: --at timestamp has no timezone; interpreting as UTC.",
                    file=sys.stderr,
                )
                t_dt = t_dt.replace(tzinfo=timezone.utc)
            t_epoch = t_dt.timestamp()
        except ValueError as exc:
            print(f"ERROR: cannot parse --at timestamp: {exc}", file=sys.stderr)
            sys.exit(1)

        verify_after = args.verify
        redis_data_for_verify = None

    # ── Reconstruct ───────────────────────────────────────────────────────
    t_dt_str = datetime.fromtimestamp(t_epoch, tz=timezone.utc).isoformat()
    slots = _window_slots(t_epoch)

    _banner(f"reconstruct — route={route}  at={t_dt_str}")
    print()
    print(f"  Window            : [{datetime.fromtimestamp(t_epoch - _W_1H, tz=timezone.utc).isoformat()},")
    print(f"                       {t_dt_str}]")
    print(f"  Slots to scan     : {', '.join(f'dt={d}/h={h:02d}' for d, h in slots)}")
    print()

    if not args.verify_latest:
        s3 = _s3_client()
    bucket = _bucket()

    print("  Listing Parquet files …", end=" ", flush=True)
    try:
        result = reconstruct(route, t_epoch, _duck_con(), s3, bucket)
    except Exception as exc:
        print(f"\nERROR during reconstruction: {exc}", file=sys.stderr)
        raise

    if result is None:
        print()
        print(
            f"\n  No Parquet files found for route {route!r} in the window.\n"
            f"  Slots checked: {slots}\n"
            f"\n  The worker's stream consumer may still be catching up, or no\n"
            f"  events exist for this route in the target time range.\n"
            f"\n  Run  make lake CMD=\"list stream/\"  to see what is available.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"OK ({result.files_scanned} file(s))")
    _print_result(result)

    # ── Optional Redis comparison ─────────────────────────────────────────
    if verify_after:
        if redis_data_for_verify is None:
            redis_data_for_verify = _redis_get_features(route)
        _print_comparison(result, redis_data_for_verify)
    else:
        print()

    _hdr()
    print()


if __name__ == "__main__":
    main()
