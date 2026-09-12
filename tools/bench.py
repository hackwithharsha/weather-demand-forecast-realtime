"""
bench.py  –  Postgres vs ClickHouse: wide-aggregation benchmark.

Generates N synthetic feature rows (default 10 M), bulk-loads them into both
databases, then times a GROUP-BY aggregation representative of ML feature
diagnostics: ten aggregate statistics grouped by city and hour-of-day, full
table scan, no indexes useful.

Usage (via Makefile):
    make bench

Optional flags:
    --rows N        Rows to generate (default 10_000_000)
    --runs R        Timed repetitions per database (default 3)
    --skip-load     Skip data generation; only re-run the query timing

Required environment variables:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    CLICKHOUSE_HOST (default clickhouse), CLICKHOUSE_PORT (default 8123)
    CLICKHOUSE_USER (default forecast),   CLICKHOUSE_PASSWORD (default clickhouse)
"""

from __future__ import annotations

import argparse
import io
import os
import time
from collections.abc import Iterator

import clickhouse_connect
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extensions

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CITIES = ("london", "new_york", "tokyo", "sydney", "dubai")
CHUNK  = 500_000     # rows per generation / insert chunk

# Canonical column order; both CREATE TABLE statements and the DataFrame
# always follow this ordering so COPY-from-CSV is unambiguous.
COLUMNS: tuple[str, ...] = (
    "city", "hour_ts",
    "total_demand", "event_count",
    "demand_lag_1h", "demand_lag_24h", "demand_lag_168h",
    "demand_roll_3h", "demand_roll_24h",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "temperature_c", "humidity_pct", "precip_mm",
    "is_holiday", "feature_computed_at",
)

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

# Postgres uses DOUBLE PRECISION (not NUMERIC) so arithmetic is native float
# and COPY doesn't pay numeric-to-float conversion on every row.
_PG_SETUP_STMTS = [
    "CREATE SCHEMA IF NOT EXISTS bench",
    "DROP TABLE IF EXISTS bench.city_hour_features",
    """
    CREATE TABLE bench.city_hour_features (
        city                TEXT             NOT NULL,
        hour_ts             TIMESTAMPTZ      NOT NULL,
        total_demand        DOUBLE PRECISION,
        event_count         INTEGER,
        demand_lag_1h       DOUBLE PRECISION,
        demand_lag_24h      DOUBLE PRECISION,
        demand_lag_168h     DOUBLE PRECISION,
        demand_roll_3h      DOUBLE PRECISION,
        demand_roll_24h     DOUBLE PRECISION,
        hour_sin            DOUBLE PRECISION,
        hour_cos            DOUBLE PRECISION,
        dow_sin             DOUBLE PRECISION,
        dow_cos             DOUBLE PRECISION,
        temperature_c       DOUBLE PRECISION,
        humidity_pct        DOUBLE PRECISION,
        precip_mm           DOUBLE PRECISION,
        is_holiday          BOOLEAN          NOT NULL DEFAULT FALSE,
        feature_computed_at TIMESTAMPTZ      NOT NULL DEFAULT now()
    )
    """,
]

# ClickHouse: columnar MergeTree; LowCardinality(String) for the 5-value
# city column; DateTime64(0,'UTC') = second-precision UTC timestamp.
_CH_SETUP_STMTS = [
    "CREATE DATABASE IF NOT EXISTS bench",
    "DROP TABLE IF EXISTS bench.city_hour_features",
    """
    CREATE TABLE bench.city_hour_features (
        city                LowCardinality(String),
        hour_ts             DateTime64(0, 'UTC'),
        total_demand        Nullable(Float64),
        event_count         Nullable(Int32),
        demand_lag_1h       Nullable(Float64),
        demand_lag_24h      Nullable(Float64),
        demand_lag_168h     Nullable(Float64),
        demand_roll_3h      Nullable(Float64),
        demand_roll_24h     Nullable(Float64),
        hour_sin            Nullable(Float64),
        hour_cos            Nullable(Float64),
        dow_sin             Nullable(Float64),
        dow_cos             Nullable(Float64),
        temperature_c       Nullable(Float64),
        humidity_pct        Nullable(Float64),
        precip_mm           Nullable(Float64),
        is_holiday          UInt8,
        feature_computed_at DateTime64(0, 'UTC')
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(hour_ts)
    ORDER BY (city, hour_ts)
    """,
]

# ---------------------------------------------------------------------------
# Benchmark queries  (logically identical; syntax adapted to each dialect)
# ---------------------------------------------------------------------------

# Groups by city × hour-of-day (120 output rows for 5 cities × 24 hours).
# Ten aggregate statistics per group → forces the engine to materialise and
# combine all non-null values in every column it touches.
BENCH_PG = """
SELECT
    city,
    EXTRACT(HOUR FROM hour_ts)::integer             AS hour_of_day,
    AVG(total_demand)                               AS avg_demand,
    STDDEV(total_demand)                            AS std_demand,
    AVG(demand_lag_1h)                              AS avg_lag_1h,
    AVG(demand_lag_24h)                             AS avg_lag_24h,
    AVG(demand_roll_24h)                            AS avg_roll_24h,
    AVG(temperature_c)                              AS avg_temp_c,
    AVG(humidity_pct)                               AS avg_humidity,
    SUM(CASE WHEN is_holiday THEN 1 ELSE 0 END)     AS holiday_hours,
    COUNT(*)                                        AS n_rows
FROM bench.city_hour_features
GROUP BY city, EXTRACT(HOUR FROM hour_ts)
ORDER BY city, hour_of_day
"""

BENCH_CH = """
SELECT
    city,
    toHour(hour_ts)         AS hour_of_day,
    avg(total_demand)       AS avg_demand,
    stddevPop(total_demand) AS std_demand,
    avg(demand_lag_1h)      AS avg_lag_1h,
    avg(demand_lag_24h)     AS avg_lag_24h,
    avg(demand_roll_24h)    AS avg_roll_24h,
    avg(temperature_c)      AS avg_temp_c,
    avg(humidity_pct)       AS avg_humidity,
    sum(is_holiday)         AS holiday_hours,
    count()                 AS n_rows
FROM bench.city_hour_features
GROUP BY city, toHour(hour_ts)
ORDER BY city, hour_of_day
"""

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def _make_chunk(
    start: int,
    n: int,
    rng: np.random.Generator,
    fca_epoch_s: int,
) -> pd.DataFrame:
    """Return n synthetic rows starting at global offset *start*.

    Cities cycle in round-robin; within each city timestamps advance by one
    hour so each city gets a consecutive series starting 2010-01-01 UTC.
    """
    city_idx  = np.arange(start, start + n, dtype=np.int64) % len(CITIES)
    row_h     = np.arange(start, start + n, dtype=np.int64) // len(CITIES)

    # hour_ts: UTC DatetimeIndex at second precision
    base_s  = np.datetime64("2010-01-01T00:00:00", "s").astype(np.int64)
    ts_arr  = (base_s + row_h * 3600).astype("datetime64[s]")
    hour_ts = pd.DatetimeIndex(ts_arr).tz_localize("UTC")

    hour_of_day = row_h % 24
    day_of_week = (row_h // 24) % 7

    # feature_computed_at: same for all rows in the chunk
    fca_arr = np.full(n, fca_epoch_s, dtype="datetime64[s]")
    feature_computed_at = pd.DatetimeIndex(fca_arr).tz_localize("UTC")

    def _null(arr: np.ndarray, p: float) -> np.ndarray:
        out = arr.astype(np.float64)
        out[rng.random(n) < p] = np.nan
        return out

    return pd.DataFrame(
        {
            "city":             [CITIES[i] for i in city_idx],
            "hour_ts":          hour_ts,
            "total_demand":     rng.uniform(50.0,  2000.0, n),
            "event_count":      rng.integers(0, 200, n, dtype=np.int32),
            "demand_lag_1h":    _null(rng.uniform(50, 2000, n), 0.01),
            "demand_lag_24h":   _null(rng.uniform(50, 2000, n), 0.01),
            "demand_lag_168h":  _null(rng.uniform(50, 2000, n), 0.01),
            "demand_roll_3h":   _null(rng.uniform(50, 2000, n), 0.01),
            "demand_roll_24h":  _null(rng.uniform(50, 2000, n), 0.01),
            "hour_sin":         np.sin(2 * np.pi * hour_of_day / 24.0),
            "hour_cos":         np.cos(2 * np.pi * hour_of_day / 24.0),
            "dow_sin":          np.sin(2 * np.pi * day_of_week  / 7.0),
            "dow_cos":          np.cos(2 * np.pi * day_of_week  / 7.0),
            "temperature_c":    _null(rng.uniform(-15, 42, n), 0.05),
            "humidity_pct":     _null(rng.uniform(10,  99, n), 0.05),
            "precip_mm":        _null(rng.uniform(0,   30, n), 0.05),
            "is_holiday":       rng.integers(0, 2, n, dtype=np.uint8),
            "feature_computed_at": feature_computed_at,
        },
        columns=list(COLUMNS),
    )


def _chunks(n_rows: int, seed: int = 42) -> Iterator[pd.DataFrame]:
    """Yield chunks of *n_rows* total from a reproducible RNG."""
    rng        = np.random.default_rng(seed)
    fca_epoch  = int(pd.Timestamp.now("UTC").floor("s").timestamp())
    offset     = 0
    while offset < n_rows:
        n = min(CHUNK, n_rows - offset)
        yield _make_chunk(offset, n, rng, fca_epoch)
        offset += n


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def _pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host     = os.environ["POSTGRES_HOST"],
        port     = int(os.getenv("POSTGRES_PORT", "5432")),
        dbname   = os.environ["POSTGRES_DB"],
        user     = os.environ["POSTGRES_USER"],
        password = os.environ["POSTGRES_PASSWORD"],
    )


def _pg_setup(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        for stmt in _PG_SETUP_STMTS:
            cur.execute(stmt)
    conn.commit()


def _pg_load(conn: psycopg2.extensions.connection, n_rows: int) -> float:
    """Bulk-load n_rows via COPY CSV; return elapsed seconds."""
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        for i, df in enumerate(_chunks(n_rows)):
            # Prepare CSV-safe representations.
            buf = io.StringIO()
            tmp = pd.DataFrame(index=df.index)
            tmp["city"]               = df["city"]
            tmp["hour_ts"]            = df["hour_ts"].dt.strftime("%Y-%m-%d %H:%M:%S+00")
            tmp["total_demand"]       = df["total_demand"]
            tmp["event_count"]        = df["event_count"]
            tmp["demand_lag_1h"]      = df["demand_lag_1h"]
            tmp["demand_lag_24h"]     = df["demand_lag_24h"]
            tmp["demand_lag_168h"]    = df["demand_lag_168h"]
            tmp["demand_roll_3h"]     = df["demand_roll_3h"]
            tmp["demand_roll_24h"]    = df["demand_roll_24h"]
            tmp["hour_sin"]           = df["hour_sin"]
            tmp["hour_cos"]           = df["hour_cos"]
            tmp["dow_sin"]            = df["dow_sin"]
            tmp["dow_cos"]            = df["dow_cos"]
            tmp["temperature_c"]      = df["temperature_c"]
            tmp["humidity_pct"]       = df["humidity_pct"]
            tmp["precip_mm"]          = df["precip_mm"]
            tmp["is_holiday"]         = df["is_holiday"].astype(np.int8)
            tmp["feature_computed_at"] = df["feature_computed_at"].strftime(
                "%Y-%m-%d %H:%M:%S+00"
            )
            tmp.to_csv(buf, index=False, header=False, na_rep=r"\N")
            buf.seek(0)
            cur.copy_expert(
                "COPY bench.city_hour_features ("
                + ", ".join(COLUMNS)
                + r") FROM STDIN WITH (FORMAT CSV, NULL '\N')",
                buf,
            )
            _progress("  pg", (i + 1) * CHUNK, n_rows)
    conn.commit()
    # Refresh planner statistics before timing the query.
    with conn.cursor() as cur:
        cur.execute("ANALYZE bench.city_hour_features")
    conn.commit()
    return time.perf_counter() - t0


def _pg_run_query(conn: psycopg2.extensions.connection) -> float:
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(BENCH_PG)
        cur.fetchall()
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def _ch_connect() -> clickhouse_connect.driver.client.Client:
    return clickhouse_connect.get_client(
        host            = os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port            = int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username        = os.getenv("CLICKHOUSE_USER", "forecast"),
        password        = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        connect_timeout = 30,
        # No database: all statements reference bench.* explicitly.
    )


def _ch_setup(client: clickhouse_connect.driver.client.Client) -> None:
    for stmt in _CH_SETUP_STMTS:
        client.command(stmt.strip())


def _ch_load(
    client: clickhouse_connect.driver.client.Client,
    n_rows: int,
) -> float:
    t0 = time.perf_counter()
    for i, df in enumerate(_chunks(n_rows)):
        # clickhouse-connect maps:
        #   datetime64[s, UTC] → DateTime64(0,'UTC')
        #   np.uint8           → UInt8
        #   np.int32           → Nullable(Int32)  (no NaN, so always non-null)
        #   np.float64 w/ NaN  → Nullable(Float64)
        client.insert_df("bench.city_hour_features", df[list(COLUMNS)])
        _progress("  ch", (i + 1) * CHUNK, n_rows)
    return time.perf_counter() - t0


def _ch_run_query(client: clickhouse_connect.driver.client.Client) -> float:
    t0 = time.perf_counter()
    client.query(BENCH_CH)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _progress(label: str, done: int, total: int) -> None:
    pct = min(done, total) / total * 100
    print(
        f"\r{label}  {min(done, total):>12,} / {total:,}  ({pct:.0f}%)",
        end="",
        flush=True,
    )


def _print_results(
    pg_times:  list[float],
    ch_times:  list[float],
    pg_load_s: float,
    ch_load_s: float,
    n_rows:    int,
) -> None:
    pg_min  = min(pg_times)
    ch_min  = min(ch_times)
    speedup = pg_min / ch_min if ch_min > 0 else float("inf")

    w = 64
    sep = "─" * w

    def _run_str(times: list[float]) -> str:
        return "  ".join(f"{t:.2f}s" for t in times)

    print(f"\n{sep}")
    print(f"  Benchmark: wide GROUP-BY on {n_rows:,} rows")
    print(sep)
    print(f"  {'Database':<12}  {'Load':>7}  {'Query runs':<32}  {'Min':>6}")
    print(f"  {'─'*12}  {'─'*7}  {'─'*32}  {'─'*6}")
    print(f"  {'Postgres':<12}  {pg_load_s:>6.1f}s  {_run_str(pg_times):<32}  {pg_min:>5.2f}s")
    print(f"  {'ClickHouse':<12}  {ch_load_s:>6.1f}s  {_run_str(ch_times):<32}  {ch_min:>5.2f}s")
    print(sep)
    print(f"  ClickHouse speedup (query, min-of-{len(pg_times)}): {speedup:.1f}×")
    print(f"{sep}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows",      type=int,  default=10_000_000,
                    help="Number of synthetic rows (default 10_000_000)")
    ap.add_argument("--runs",      type=int,  default=3,
                    help="Timed query repetitions per database (default 3)")
    ap.add_argument("--skip-load", action="store_true",
                    help="Skip data loading; only re-run the query timing")
    args = ap.parse_args()

    print(f"\nConnecting to Postgres    ({os.environ.get('POSTGRES_HOST', '?')}) …")
    pg = _pg_connect()
    print(f"Connecting to ClickHouse  ({os.getenv('CLICKHOUSE_HOST', 'clickhouse')}) …")
    ch = _ch_connect()

    pg_load_s = ch_load_s = 0.0

    if not args.skip_load:
        n_chunks = (args.rows + CHUNK - 1) // CHUNK
        print(f"\nGenerating {args.rows:,} rows in {n_chunks} chunks of {CHUNK:,} …")

        print("\nLoading Postgres …")
        _pg_setup(pg)
        pg_load_s = _pg_load(pg, args.rows)
        print(f"\n  ✓ {args.rows / pg_load_s:,.0f} rows/s")

        print("\nLoading ClickHouse …")
        _ch_setup(ch)
        ch_load_s = _ch_load(ch, args.rows)
        print(f"\n  ✓ {args.rows / ch_load_s:,.0f} rows/s")

    print(f"\nRunning query benchmark ({args.runs} runs each) …")
    pg_times: list[float] = []
    for i in range(args.runs):
        t = _pg_run_query(pg)
        pg_times.append(t)
        print(f"  Postgres    run {i + 1}: {t:.2f}s")

    ch_times: list[float] = []
    for i in range(args.runs):
        t = _ch_run_query(ch)
        ch_times.append(t)
        print(f"  ClickHouse  run {i + 1}: {t:.2f}s")

    _print_results(pg_times, ch_times, pg_load_s, ch_load_s, args.rows)
    pg.close()


if __name__ == "__main__":
    main()
