"""
staging → marts

Reads staging.demand_hourly and staging.weather_hourly, engineers the full
feature set, and upserts into marts.city_hour_features.

Feature engineering
-------------------
lags            demand_lag_1h / 24h / 168h  — per-city shift by N hours
rolling means   demand_roll_3h / 24h        — past-only (shift-1 before window)
cyclical time   hour_sin/cos, dow_sin/cos   — sin/cos of hour-of-day (24) and
                                              day-of-week (7); range [-1, 1]
weather join    temperature_c, humidity_pct, precip_mm — left-join from staging
holiday flag    is_holiday                  — public calendar per city

Idempotency
-----------
Every write uses ON CONFLICT (city, hour_ts) DO UPDATE, so the job can be
re-run without producing duplicate rows.

Holiday coverage
----------------
london    → GB (England & Wales)
new_york  → US (federal + NYSE)
tokyo     → JP
sydney    → AU (New South Wales)
dubai     → None (no public-holiday data; is_holiday always False)
"""

from __future__ import annotations

import math
from datetime import date
from functools import lru_cache

import holidays as hol
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import structlog

from .settings import Settings

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Holiday helpers
# ---------------------------------------------------------------------------

_CITY_COUNTRY: dict[str, str | None] = {
    "london":   "GB",
    "new_york": "US",
    "tokyo":    "JP",
    "sydney":   "AU",
    "dubai":    None,
}


@lru_cache(maxsize=64)
def _holidays_for(city: str, year: int) -> frozenset[date]:
    """Return the set of public holiday dates for *city* in *year*."""
    country = _CITY_COUNTRY.get(city.lower())
    if not country:
        return frozenset()
    return frozenset(hol.country_holidays(country, years=year).keys())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_marts(settings: Settings) -> None:
    """
    Build mart features from the full staging history and upsert into
    marts.city_hour_features.

    Reading the entire staging table (not a rolling window) ensures that lag
    columns at the start of any window are computed from genuine historical
    rows rather than being left as NaN due to an artificial cutoff.
    """
    log.info("marts_started")

    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        demand_df = pd.read_sql(
            "SELECT city, hour_ts, total_demand, event_count "
            "FROM staging.demand_hourly ORDER BY city, hour_ts",
            conn,
        )
        weather_df = pd.read_sql(
            "SELECT city, hour_ts, temperature_c, humidity_pct, precip_mm "
            "FROM staging.weather_hourly ORDER BY city, hour_ts",
            conn,
        )

        if demand_df.empty:
            log.warning("marts_skipped", reason="staging.demand_hourly is empty")
            return

        features_df = _build_features(demand_df, weather_df)
        log.info("marts_features_computed", rows=len(features_df))

        _upsert_features(conn, features_df)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("marts_done", rows=len(features_df))


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def _build_features(
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    df = demand_df.merge(weather_df, on=["city", "hour_ts"], how="left")
    df = df.sort_values(["city", "hour_ts"]).reset_index(drop=True)

    df = _add_lags(df)
    df = _add_rolling(df)
    df = _add_cyclical(df)
    df = _add_holidays(df)

    df["feature_computed_at"] = pd.Timestamp.now(tz="UTC")
    return df


def _add_lags(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("city")["total_demand"]
    for lag_h in (1, 24, 168):
        df[f"demand_lag_{lag_h}h"] = grp.shift(lag_h)
    return df


def _add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    # shift(1) before rolling excludes the current row so every mean is
    # computed solely from past values (no data leakage at train time).
    for window in (3, 24):
        df[f"demand_roll_{window}h"] = (
            df.groupby("city")["total_demand"]
            .transform(
                lambda x, w=window: x.shift(1).rolling(w, min_periods=1).mean()
            )
        )
    return df


def _add_cyclical(df: pd.DataFrame) -> pd.DataFrame:
    hour = df["hour_ts"].dt.hour.astype(float)
    dow  = df["hour_ts"].dt.dayofweek.astype(float)
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["dow_sin"]  = np.sin(2 * np.pi * dow  / 7)
    df["dow_cos"]  = np.cos(2 * np.pi * dow  / 7)
    return df


def _add_holidays(df: pd.DataFrame) -> pd.DataFrame:
    cities = df["city"].tolist()
    dates  = df["hour_ts"].dt.date.tolist()
    df["is_holiday"] = [
        d in _holidays_for(city, d.year)
        for city, d in zip(cities, dates)
    ]
    return df


# ---------------------------------------------------------------------------
# Postgres upsert
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
INSERT INTO marts.city_hour_features (
    city, hour_ts,
    total_demand, event_count,
    demand_lag_1h, demand_lag_24h, demand_lag_168h,
    demand_roll_3h, demand_roll_24h,
    hour_sin, hour_cos, dow_sin, dow_cos,
    temperature_c, humidity_pct, precip_mm,
    is_holiday, feature_computed_at
) VALUES %s
ON CONFLICT (city, hour_ts) DO UPDATE SET
    total_demand        = EXCLUDED.total_demand,
    event_count         = EXCLUDED.event_count,
    demand_lag_1h       = EXCLUDED.demand_lag_1h,
    demand_lag_24h      = EXCLUDED.demand_lag_24h,
    demand_lag_168h     = EXCLUDED.demand_lag_168h,
    demand_roll_3h      = EXCLUDED.demand_roll_3h,
    demand_roll_24h     = EXCLUDED.demand_roll_24h,
    hour_sin            = EXCLUDED.hour_sin,
    hour_cos            = EXCLUDED.hour_cos,
    dow_sin             = EXCLUDED.dow_sin,
    dow_cos             = EXCLUDED.dow_cos,
    temperature_c       = EXCLUDED.temperature_c,
    humidity_pct        = EXCLUDED.humidity_pct,
    precip_mm           = EXCLUDED.precip_mm,
    is_holiday          = EXCLUDED.is_holiday,
    feature_computed_at = EXCLUDED.feature_computed_at
"""


def _nan_to_none(val: object) -> object:
    try:
        return None if math.isnan(val) else val  # type: ignore[arg-type]
    except TypeError:
        return val


def _upsert_features(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
) -> None:
    rows = [
        (
            row.city,
            row.hour_ts,
            _nan_to_none(row.total_demand),
            int(row.event_count) if not math.isnan(float(row.event_count)) else None,
            _nan_to_none(row.demand_lag_1h),
            _nan_to_none(row.demand_lag_24h),
            _nan_to_none(row.demand_lag_168h),
            _nan_to_none(row.demand_roll_3h),
            _nan_to_none(row.demand_roll_24h),
            _nan_to_none(row.hour_sin),
            _nan_to_none(row.hour_cos),
            _nan_to_none(row.dow_sin),
            _nan_to_none(row.dow_cos),
            _nan_to_none(row.temperature_c),
            _nan_to_none(row.humidity_pct),
            _nan_to_none(row.precip_mm),
            bool(row.is_holiday),
            row.feature_computed_at,
        )
        for row in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _UPSERT_SQL, rows)
    log.info("marts_upserted", rows=len(rows))
