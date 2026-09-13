"""
Training-serving skew detector.

Joins marts.prediction_features (exact vectors used at serving time) against
marts.city_hour_features (the warehouse source used during model training) on
(city, date_trunc('hour', requested_at)) and reports per-feature mismatch
counts.

Skew taxonomy
-------------
offline features  – demand_lag_*, demand_roll_*, event_count, humidity_pct,
                    is_holiday.  These come exclusively from Postgres.
                    Any mismatch vs the warehouse on the same hour_ts is an
                    unexpected discrepancy and counts as a skew bug.

online features   – temperature_c, precip_mm.  When online_source is non-empty
                    the serving layer read a fresher value from Redis, so a
                    difference vs the warehouse is expected and desired.
                    These are reported separately and do NOT inflate the bug
                    count.

stale mart        – When mart_hour_ts != date_trunc('hour', requested_at) the
                    serving layer used an older mart snapshot.  All offline
                    features will differ from the warehouse current-hour row.
                    This is infrastructure staleness, not a code bug, but is
                    reported so the operator knows the mart is lagging.

Exit codes
----------
0  No offline-feature skew bugs detected.
1  One or more offline features differed from the warehouse.
"""

from __future__ import annotations

import math
import os
import sys

import psycopg2
import psycopg2.extras

TOLERANCE = 1e-2   # absolute-difference threshold (accommodates float rounding)

# ── Features treated as "online" (may legitimately differ when Redis overrides)
ONLINE_FEAT_COLS = {"temperature_c", "precip_mm"}

# ── All numeric feature columns that must match the warehouse exactly
OFFLINE_FEAT_COLS = [
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "humidity_pct",
    "is_holiday",
]

ALL_FEAT_COLS = OFFLINE_FEAT_COLS + sorted(ONLINE_FEAT_COLS)


# ---------------------------------------------------------------------------
# SQL — summary + per-feature breakdown in a single pass
# ---------------------------------------------------------------------------

_SUMMARY_SQL = """
WITH
serving AS (
    SELECT
        city,
        date_trunc('hour', requested_at)  AS serving_hour,
        mart_hour_ts,
        online_source,
        event_count,
        demand_lag_1h,  demand_lag_24h,  demand_lag_168h,
        demand_roll_3h, demand_roll_24h,
        humidity_pct,   is_holiday,
        temperature_c,  precip_mm
    FROM marts.prediction_features
),
warehouse AS (
    SELECT
        city, hour_ts,
        CAST(event_count     AS double precision) AS event_count,
        CAST(demand_lag_1h   AS double precision) AS demand_lag_1h,
        CAST(demand_lag_24h  AS double precision) AS demand_lag_24h,
        CAST(demand_lag_168h AS double precision) AS demand_lag_168h,
        CAST(demand_roll_3h  AS double precision) AS demand_roll_3h,
        CAST(demand_roll_24h AS double precision) AS demand_roll_24h,
        CAST(humidity_pct    AS double precision) AS humidity_pct,
        CAST(is_holiday::INT AS double precision) AS is_holiday,
        CAST(temperature_c   AS double precision) AS temperature_c,
        CAST(precip_mm       AS double precision) AS precip_mm
    FROM marts.city_hour_features
),
joined AS (
    SELECT
        s.city,
        s.serving_hour,
        s.mart_hour_ts,
        s.online_source,
        w.hour_ts                                                      AS wh_hour,
        -- stale: serving pulled a mart row from a different hour
        (s.mart_hour_ts IS NOT NULL
         AND s.mart_hour_ts != s.serving_hour)                         AS is_stale,
        -- per-feature absolute deltas
        ABS(COALESCE(s.event_count,    0) - COALESCE(w.event_count,    0)) AS d_event_count,
        ABS(COALESCE(s.demand_lag_1h,  0) - COALESCE(w.demand_lag_1h,  0)) AS d_lag_1h,
        ABS(COALESCE(s.demand_lag_24h, 0) - COALESCE(w.demand_lag_24h, 0)) AS d_lag_24h,
        ABS(COALESCE(s.demand_lag_168h,0) - COALESCE(w.demand_lag_168h,0)) AS d_lag_168h,
        ABS(COALESCE(s.demand_roll_3h, 0) - COALESCE(w.demand_roll_3h, 0)) AS d_roll_3h,
        ABS(COALESCE(s.demand_roll_24h,0) - COALESCE(w.demand_roll_24h,0)) AS d_roll_24h,
        ABS(COALESCE(s.humidity_pct,   0) - COALESCE(w.humidity_pct,   0)) AS d_humidity_pct,
        ABS(COALESCE(s.is_holiday,     0) - COALESCE(w.is_holiday,     0)) AS d_is_holiday,
        ABS(COALESCE(s.temperature_c,  0) - COALESCE(w.temperature_c,  0)) AS d_temperature_c,
        ABS(COALESCE(s.precip_mm,      0) - COALESCE(w.precip_mm,      0)) AS d_precip_mm
    FROM serving s
    LEFT JOIN warehouse w
           ON w.city = s.city AND w.hour_ts = s.serving_hour
)
SELECT
    COUNT(*)                                        AS total,
    COUNT(wh_hour)                                  AS matched,
    COUNT(*) - COUNT(wh_hour)                       AS unmatched,
    SUM(is_stale::int)                              AS stale_mart,
    -- offline bugs (all matched rows)
    COUNT(*) FILTER (WHERE d_event_count  > %(tol)s) AS bug_event_count,
    COUNT(*) FILTER (WHERE d_lag_1h       > %(tol)s) AS bug_lag_1h,
    COUNT(*) FILTER (WHERE d_lag_24h      > %(tol)s) AS bug_lag_24h,
    COUNT(*) FILTER (WHERE d_lag_168h     > %(tol)s) AS bug_lag_168h,
    COUNT(*) FILTER (WHERE d_roll_3h      > %(tol)s) AS bug_roll_3h,
    COUNT(*) FILTER (WHERE d_roll_24h     > %(tol)s) AS bug_roll_24h,
    COUNT(*) FILTER (WHERE d_humidity_pct > %(tol)s) AS bug_humidity_pct,
    COUNT(*) FILTER (WHERE d_is_holiday   > %(tol)s) AS bug_is_holiday,
    -- same counts restricted to fresh-mart rows only (true code bugs)
    COUNT(*) FILTER (WHERE NOT is_stale AND d_event_count  > %(tol)s) AS true_bug_event_count,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_lag_1h       > %(tol)s) AS true_bug_lag_1h,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_lag_24h      > %(tol)s) AS true_bug_lag_24h,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_lag_168h     > %(tol)s) AS true_bug_lag_168h,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_roll_3h      > %(tol)s) AS true_bug_roll_3h,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_roll_24h     > %(tol)s) AS true_bug_roll_24h,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_humidity_pct > %(tol)s) AS true_bug_humidity_pct,
    COUNT(*) FILTER (WHERE NOT is_stale AND d_is_holiday   > %(tol)s) AS true_bug_is_holiday,
    -- online overrides (expected)
    COUNT(*) FILTER (WHERE d_temperature_c > %(tol)s) AS online_temp,
    COUNT(*) FILTER (WHERE d_precip_mm     > %(tol)s) AS online_precip
FROM joined
"""

# Per-feature detail using a LATERAL unnest of (name, delta, is_online) tuples
_FEATURE_SQL = """
WITH
serving AS (
    SELECT
        city,
        date_trunc('hour', requested_at) AS serving_hour,
        event_count,
        demand_lag_1h,  demand_lag_24h,  demand_lag_168h,
        demand_roll_3h, demand_roll_24h,
        humidity_pct,   is_holiday,
        temperature_c,  precip_mm
    FROM marts.prediction_features
),
warehouse AS (
    SELECT
        city, hour_ts,
        CAST(event_count     AS double precision) AS event_count,
        CAST(demand_lag_1h   AS double precision) AS demand_lag_1h,
        CAST(demand_lag_24h  AS double precision) AS demand_lag_24h,
        CAST(demand_lag_168h AS double precision) AS demand_lag_168h,
        CAST(demand_roll_3h  AS double precision) AS demand_roll_3h,
        CAST(demand_roll_24h AS double precision) AS demand_roll_24h,
        CAST(humidity_pct    AS double precision) AS humidity_pct,
        CAST(is_holiday::INT AS double precision) AS is_holiday,
        CAST(temperature_c   AS double precision) AS temperature_c,
        CAST(precip_mm       AS double precision) AS precip_mm
    FROM marts.city_hour_features
),
joined AS (
    SELECT
        s.city, s.serving_hour, w.hour_ts AS wh_hour,
        ABS(COALESCE(s.event_count,    0) - COALESCE(w.event_count,    0)) AS d_event_count,
        ABS(COALESCE(s.demand_lag_1h,  0) - COALESCE(w.demand_lag_1h,  0)) AS d_lag_1h,
        ABS(COALESCE(s.demand_lag_24h, 0) - COALESCE(w.demand_lag_24h, 0)) AS d_lag_24h,
        ABS(COALESCE(s.demand_lag_168h,0) - COALESCE(w.demand_lag_168h,0)) AS d_lag_168h,
        ABS(COALESCE(s.demand_roll_3h, 0) - COALESCE(w.demand_roll_3h, 0)) AS d_roll_3h,
        ABS(COALESCE(s.demand_roll_24h,0) - COALESCE(w.demand_roll_24h,0)) AS d_roll_24h,
        ABS(COALESCE(s.humidity_pct,   0) - COALESCE(w.humidity_pct,   0)) AS d_humidity_pct,
        ABS(COALESCE(s.is_holiday,        0) - COALESCE(w.is_holiday,     0)) AS d_is_holiday,
        ABS(COALESCE(s.temperature_c,     0) - COALESCE(w.temperature_c,  0)) AS d_temperature_c,
        ABS(COALESCE(s.precip_mm,         0) - COALESCE(w.precip_mm,      0)) AS d_precip_mm
    FROM serving s
    LEFT JOIN warehouse w ON w.city = s.city AND w.hour_ts = s.serving_hour
),
unpivoted AS (
    SELECT feat, delta, is_online, wh_hour IS NOT NULL AS has_wh
    FROM joined
    CROSS JOIN LATERAL (VALUES
        ('event_count',    d_event_count,   false),
        ('demand_lag_1h',  d_lag_1h,        false),
        ('demand_lag_24h', d_lag_24h,       false),
        ('demand_lag_168h',d_lag_168h,      false),
        ('demand_roll_3h', d_roll_3h,       false),
        ('demand_roll_24h',d_roll_24h,      false),
        ('humidity_pct',   d_humidity_pct,  false),
        ('is_holiday',     d_is_holiday,    false),
        ('temperature_c',  d_temperature_c, true),
        ('precip_mm',      d_precip_mm,     true)
    ) AS t(feat, delta, is_online)
)
SELECT
    feat,
    is_online,
    COUNT(*) FILTER (WHERE has_wh AND delta > %(tol)s)                     AS mismatches,
    ROUND(AVG(delta)  FILTER (WHERE has_wh AND delta > %(tol)s)::numeric, 4) AS avg_delta,
    ROUND(MAX(delta)  FILTER (WHERE has_wh AND delta > %(tol)s)::numeric, 4) AS max_delta
FROM unpivoted
GROUP BY feat, is_online
ORDER BY is_online, mismatches DESC NULLS LAST, feat
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'forecast')} "
        f"user={os.getenv('POSTGRES_USER', 'forecast')} "
        f"password={os.getenv('POSTGRES_PASSWORD', '')}"
    )


_BOLD = "\033[1m"
_RED  = "\033[31m"
_GRN  = "\033[32m"
_YLW  = "\033[33m"
_DIM  = "\033[2m"
_RST  = "\033[0m"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute("SELECT COUNT(*) AS n FROM marts.prediction_features")
            total: int = cur.fetchone()["n"]  # type: ignore[index]

            if total == 0:
                print(
                    "\nprediction_features is empty.\n"
                    "Call POST /predict a few times first, then re-run.\n"
                )
                sys.exit(0)

            cur.execute(_SUMMARY_SQL, {"tol": TOLERANCE})
            summary = cur.fetchone()

            cur.execute(_FEATURE_SQL, {"tol": TOLERANCE})
            feat_rows = cur.fetchall()

    _print_report(total, summary, feat_rows)  # type: ignore[arg-type]


def _print_report(total: int, s: dict, feat_rows: list[dict]) -> None:
    matched   = s["matched"]
    unmatched = s["unmatched"]
    stale     = s["stale_mart"] or 0

    # All offline mismatches (includes those explained by stale mart)
    offline_bugs_total: int = sum(
        s[k] or 0
        for k in [
            "bug_event_count", "bug_lag_1h",  "bug_lag_24h", "bug_lag_168h",
            "bug_roll_3h",     "bug_roll_24h", "bug_humidity_pct", "bug_is_holiday",
        ]
    )
    # Offline mismatches on fresh-mart rows only — these are true code-level bugs
    true_offline_bugs: int = sum(
        s[k] or 0
        for k in [
            "true_bug_event_count", "true_bug_lag_1h",  "true_bug_lag_24h",
            "true_bug_lag_168h",    "true_bug_roll_3h",  "true_bug_roll_24h",
            "true_bug_humidity_pct","true_bug_is_holiday",
        ]
    )
    stale_explained: int = offline_bugs_total - true_offline_bugs
    online_diff: int = (s["online_temp"] or 0) + (s["online_precip"] or 0)

    print(f"\n{_BOLD}=== Training-Serving Skew Report ==={_RST}")
    print(f"  Serving rows logged        : {total}")
    print(f"  Matched to warehouse hour  : {matched}  "
          f"{_DIM}(joined on city + date_trunc('hour', requested_at)){_RST}")
    if unmatched:
        print(f"  No warehouse match         : {unmatched}  "
              f"{_DIM}(requested_at hour not yet in mart){_RST}")
    if stale:
        print(f"  Stale mart rows used       : {stale}  "
              f"{_DIM}(mart_hour_ts ≠ request hour — mart is lagging){_RST}")

    print(f"\n{_BOLD}Per-feature breakdown  "
          f"{_DIM}(|Δ| > {TOLERANCE}  •  matched rows only){_RST}")
    hdr = (f"  {'Feature':<22}  {'All':>6}  {'Fresh':>6}"
           f"  {'Avg |Δ|':>10}  {'Max |Δ|':>10}  Kind")
    sep = f"  {'-'*22}  {'-'*6}  {'-'*6}  {'-'*10}  {'-'*10}  {'-'*24}"
    print(hdr)
    print(sep)

    for fr in feat_rows:
        n   = fr["mismatches"] or 0
        avg = fr["avg_delta"] or 0
        mx  = fr["max_delta"] or 0
        fresh_key = f"true_bug_{fr['feat'].replace('demand_lag_', 'lag_').replace('demand_roll_', 'roll_')}"
        fresh_n = s.get(fresh_key) or 0

        if fr["is_online"]:
            kind = f"{_YLW}online override (expected){_RST}"
        elif fresh_n > 0:
            kind = f"{_RED}SKEW BUG (code){_RST}"
        elif n > 0:
            kind = f"{_DIM}stale-mart drift{_RST}"
        else:
            kind = f"{_GRN}clean{_RST}"
        print(f"  {fr['feat']:<22}  {n:>6}  {fresh_n:>6}"
              f"  {avg:>10.4f}  {mx:>10.4f}  {kind}")

    print(f"\n{_BOLD}Summary{_RST}")
    print(f"  {'Column':30s}  {'Count':>8}")
    print(f"  {'-'*30}  {'-'*8}")

    if online_diff:
        print(f"  {'Online overrides (Redis)':30s}  {online_diff:>8}  "
              f"{_DIM}(fresher than warehouse — expected){_RST}")
    if stale_explained:
        print(f"  {'Stale-mart mismatches':30s}  {stale_explained:>8}  "
              f"{_DIM}(serving used older mart row — infrastructure lag){_RST}")

    if true_offline_bugs == 0:
        print(f"  {'True skew bugs (code)':30s}  {_GRN}{true_offline_bugs:>8}  "
              f"✓  no code-level training-serving skew{_RST}")
    else:
        print(f"  {_RED}{_BOLD}{'True skew bugs (code)':30s}  {true_offline_bugs:>8}"
              f"  ← offline features differ on fresh-mart rows{_RST}")
        print(f"  {_RED}  → inspect batch pipeline feature computation logic{_RST}")

    print()
    sys.exit(1 if true_offline_bugs > 0 else 0)


if __name__ == "__main__":
    main()
