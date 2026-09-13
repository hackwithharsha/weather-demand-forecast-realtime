"""
staging → marts

Pipeline
--------
1. SQL transform   sql/marts/001_city_hour_features.sql runs all features
                   that are expressible in relational SQL: lags, rolling means,
                   cyclical encodings, and the weather left-join.  A single
                   data-modifying CTE completes the work in one round-trip.

2. Holiday update  Python-only step.  SQL cannot look up public-holiday
                   calendars; the 'holidays' library is the only clean way to
                   do this.  Bulk UPDATE via a temp table (one round-trip).

3. Quality checks  Assertions fire after both writes commit.  Every broken
                   invariant is collected before raising so a single run
                   reveals all problems at once.

Why no pandas here?
-------------------
The previous implementation loaded all staging rows into DataFrames, computed
window functions in Python, and serialised the result back to Postgres.
PostgreSQL window functions (LAG, AVG OVER ROWS BETWEEN) execute the same
logic server-side, eliminating the Python↔DB round-trips and the need for
NumPy/pandas in this module entirely.  pandas remains in scaler.py where it
genuinely simplifies the fit/transform code.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path

import holidays as hol
import psycopg2
import psycopg2.extras
import structlog

from .settings import Settings
from .sql_runner import SqlRunner

log = structlog.get_logger()

_SQL_DIR = Path(__file__).parent.parent / "sql" / "marts"

# ---------------------------------------------------------------------------
# City → ISO-3166 country code
# ---------------------------------------------------------------------------

_CITY_COUNTRY: dict[str, str | None] = {
    "london":   "GB",
    "new_york": "US",
    "tokyo":    "JP",
    "sydney":   "AU",
    "dubai":    None,   # no public-holiday support; always False
}


@lru_cache(maxsize=64)
def _holidays_for(city: str, year: int) -> frozenset[date]:
    """Public holiday dates for *city* in *year*.  Cached per (city, year)."""
    country = _CITY_COUNTRY.get(city.lower())
    if not country:
        return frozenset()
    return frozenset(hol.country_holidays(country, years=year).keys())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_marts(settings: Settings) -> int:
    """
    Populate marts.city_hour_features from staging, apply holiday flags,
    then assert quality.  Raises RuntimeError (listing all failures) if any
    assertion fails.
    """
    # ── Step 1: SQL transform ─────────────────────────────────────────────
    log.info("marts_sql_started")
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        results = SqlRunner(_SQL_DIR).run(conn, params={})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # The SQL returns exactly one result row from the single .sql file.
    result       = results[0] if results else {}
    staging_rows = int(result.get("staging_rows",  0) or 0)
    rows_upserted = int(result.get("rows_upserted", 0) or 0)
    log.info("marts_sql_done", staging_rows=staging_rows, rows_upserted=rows_upserted)

    if rows_upserted == 0:
        log.warning("marts_no_rows_upserted", reason="staging.demand_hourly is empty")
        return 0

    # ── Step 2: holiday flags (Python; SQL cannot do this) ────────────────
    log.info("marts_holidays_started")
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        n_updated = _apply_holidays(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("marts_holidays_done", rows_updated=n_updated)

    # ── Step 3: quality assertions ────────────────────────────────────────
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        _assert_quality(conn)
    finally:
        conn.close()

    log.info("marts_done", rows=rows_upserted)
    return rows_upserted


# ---------------------------------------------------------------------------
# Holiday update (Python-only step)
# ---------------------------------------------------------------------------

def _apply_holidays(conn: psycopg2.extensions.connection) -> int:
    """
    Compute is_holiday for every row in marts.city_hour_features and UPDATE.

    Uses a TEMP TABLE (dropped on commit) to issue one bulk UPDATE instead of
    N individual statements.  The lru_cache on _holidays_for ensures the
    holidays library is invoked at most once per (city, year) pair.
    """
    # Load all keys — not just the latest window — so re-runs are fully
    # idempotent and rows from earlier runs keep correct holiday values.
    with conn.cursor() as cur:
        cur.execute("SELECT city, hour_ts FROM marts.city_hour_features")
        pairs: list[tuple] = cur.fetchall()

    if not pairs:
        return 0

    update_rows = [
        (d in _holidays_for(city, d.year), city, hour_ts)
        for city, hour_ts in pairs
        for d in [hour_ts.date()]   # single-element loop to avoid repeating hour_ts.date()
    ]

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _holiday_flags (
                flag     BOOLEAN     NOT NULL,
                city     TEXT        NOT NULL,
                hour_ts  TIMESTAMPTZ NOT NULL
            ) ON COMMIT DROP
        """)
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO _holiday_flags (flag, city, hour_ts) VALUES %s",
            update_rows,
        )
        cur.execute("""
            UPDATE marts.city_hour_features m
               SET is_holiday = h.flag
              FROM _holiday_flags h
             WHERE m.city    = h.city
               AND m.hour_ts = h.hour_ts
        """)
        return cur.rowcount


# ---------------------------------------------------------------------------
# Quality assertions
# ---------------------------------------------------------------------------

# Hard limits: null rate must be exactly 0 for these columns.
_MUST_BE_COMPLETE: tuple[str, ...] = (
    "total_demand",
    "event_count",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "is_holiday",
)

# Soft limits: log a warning (not an error) when null rate exceeds the threshold.
_WARN_IF_NULL_ABOVE: dict[str, float] = {
    "demand_lag_1h":   0.5,    # expect nulls only for the first row per city
    "temperature_c":   0.9,    # >90% missing → weather join has essentially failed
}


def _assert_quality(conn: psycopg2.extensions.connection) -> None:
    """
    Run post-commit quality checks on marts.city_hour_features.

    Hard assertions
    ---------------
    - mart row count > 0
    - mart row count == staging.demand_hourly row count  (no rows silently dropped)
    - null rate == 0 for columns that can never be null: total_demand, event_count,
      cyclical features (computed from hour_ts), is_holiday (Python-filled)

    Soft warnings  (logged but do not fail the pipeline)
    ---------------
    - demand_lag_1h null rate > 50%: history too shallow for 1h lag
    - temperature_c null rate > 90%: weather join has essentially failed

    All hard failures are collected before raising so a single failing run
    reveals every broken invariant at once.
    """
    null_selects = "\n".join(
        f"    COUNT(*) FILTER (WHERE {col} IS NULL) AS null_{col},"
        for col in (*_MUST_BE_COMPLETE, *_WARN_IF_NULL_ABOVE)
    ).rstrip(",")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                (SELECT COUNT(*) FROM staging.demand_hourly) AS staging_rows,
                COUNT(*)                                      AS mart_rows,
            {null_selects}
            FROM marts.city_hour_features
        """)
        s = dict(cur.fetchone())

    mart_rows    = int(s["mart_rows"])
    staging_rows = int(s["staging_rows"])
    safe_n       = mart_rows or 1   # avoid division-by-zero in rate expressions

    # ── Collect hard failures ─────────────────────────────────────────────
    errors: list[str] = []

    if mart_rows == 0:
        errors.append("mart_rows == 0: nothing was written")

    if mart_rows != staging_rows:
        errors.append(
            f"row count mismatch: mart={mart_rows} != staging={staging_rows} "
            f"(every staging demand row must produce exactly one mart row)"
        )

    for col in _MUST_BE_COMPLETE:
        n_null = int(s[f"null_{col}"])
        if n_null > 0:
            rate = n_null / safe_n
            errors.append(
                f"{col}: {n_null} null{'s' if n_null > 1 else ''} ({rate:.1%}) — "
                f"must be zero"
            )

    if errors:
        detail = "\n".join(f"  - {e}" for e in errors)
        raise RuntimeError(f"marts quality assertions failed:\n{detail}")

    # ── Soft warnings ─────────────────────────────────────────────────────
    for col, threshold in _WARN_IF_NULL_ABOVE.items():
        n_null = int(s[f"null_{col}"])
        rate   = n_null / safe_n
        if rate > threshold:
            log.warning(
                "mart_quality_warning",
                column=col,
                null_rate=round(rate, 4),
                threshold=threshold,
                mart_rows=mart_rows,
            )

    # ── Structured summary ────────────────────────────────────────────────
    log.info(
        "mart_quality_ok",
        mart_rows=mart_rows,
        null_rate_lag_1h=round(int(s["null_demand_lag_1h"]) / safe_n, 4),
        null_rate_temperature_c=round(int(s["null_temperature_c"]) / safe_n, 4),
    )
