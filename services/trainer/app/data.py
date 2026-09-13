"""
Load marts.city_hour_features from Postgres and produce a deterministic
snapshot hash for reproducibility tracking in MLflow.

Feature columns
---------------
FEATURE_COLS defines the canonical ordered list of model inputs.  The order
matters: ColumnTransformer in train.py references columns by name, but the
order governs the columns written into the MLflow model signature, which is
used for validation during serving.

Target column
-------------
TARGET_COL = "total_demand" — sum of event quantities in the hour window.

Null behaviour
--------------
Lag and rolling columns (demand_lag_*, demand_roll_*) are NULL for the first
few rows per city (not enough history yet).  Weather columns are NULL when no
weather reading has been received.  Both Ridge and HistGradientBoosting handle
these NULLs via their respective preprocessing pipelines.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
import structlog

from .settings import Settings

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

#: Categorical features — handled by encoder, not imputer.
CAT_FEATURES: list[str] = ["city"]

#: Numeric features — may contain NaN (lag/rolling/weather columns).
NUM_FEATURES: list[str] = [
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
    "is_holiday",
]

#: Full ordered feature list (categorical first for ColumnTransformer clarity).
FEATURE_COLS: list[str] = CAT_FEATURES + NUM_FEATURES

TARGET_COL: str = "total_demand"

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

_QUERY = """
SELECT
    city,
    hour_ts,
    total_demand,
    event_count,
    demand_lag_1h,
    demand_lag_24h,
    demand_lag_168h,
    demand_roll_3h,
    demand_roll_24h,
    hour_sin,
    hour_cos,
    dow_sin,
    dow_cos,
    temperature_c,
    humidity_pct,
    precip_mm,
    is_holiday::INT AS is_holiday
FROM marts.city_hour_features
WHERE hour_ts >= %s
  AND total_demand IS NOT NULL
ORDER BY hour_ts, city
"""


def load_features(settings: Settings) -> pd.DataFrame:
    """Load mart rows for the configured lookback window.

    Rows with NULL total_demand are excluded (they indicate hours where the
    mart was populated from weather data only, with no demand events).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.training_lookback_days)

    with psycopg2.connect(settings.postgres_dsn) as conn:
        df = pd.read_sql(_QUERY, conn, params=(cutoff,), parse_dates=["hour_ts"])

    log.info(
        "mart_data_loaded",
        rows=len(df),
        cities=int(df["city"].nunique()) if not df.empty else 0,
        lookback_days=settings.training_lookback_days,
        cutoff=cutoff.isoformat(),
    )
    return df


# ---------------------------------------------------------------------------
# Snapshot hash
# ---------------------------------------------------------------------------

def snapshot_hash(df: pd.DataFrame) -> str:
    """16-hex-char SHA-256 of (city, hour_ts, total_demand) for each row.

    Identical datasets produce identical hashes; any change in the underlying
    mart data produces a different hash.  Logged as an MLflow param so that
    training runs referencing the same data snapshot can be identified.
    """
    digest = hashlib.sha256()
    for city, hour_ts, demand in zip(
        df["city"], df["hour_ts"], df["total_demand"], strict=True
    ):
        digest.update(f"{city}|{hour_ts}|{demand}\n".encode())
    return digest.hexdigest()[:16]
