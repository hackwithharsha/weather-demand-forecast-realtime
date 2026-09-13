"""create marts.prediction_features

Stores the exact feature vector used for every /predict call so that
training-serving skew can be detected by diffing this table against
marts.city_hour_features (the warehouse source used during training).

Design decisions
----------------
One row per /predict request (not per forecast horizon step)
    The feature vector is constant across all horizon steps (lag/rolling and
    weather are held fixed; only the cyclic time encodings change).  Logging
    row-0 of the inference DataFrame captures the base features that drove
    every step of the prediction.

DOUBLE PRECISION for all feature columns
    Avoids Decimal round-trips and matches the dtype the sklearn pipeline
    actually operates on.

Staleness provenance columns
    mart_hour_ts   – the hour_ts of the mart row that was pulled.  If this
                     differs from date_trunc('hour', requested_at), the serving
                     layer used a stale mart snapshot.
    online_age_s   – seconds since the Redis hash's stream_computed_at
                     timestamp; NULL when the hash was absent.
    online_source  – Redis field names that were present and used (not missing
                     / fallen back to Postgres).
    online_degraded – field names whose SLA was breached (value still served).
    online_missing  – field names absent from the hash (Postgres fallback used).

Skew interpretation
-------------------
  Offline features (lags, rolling, event_count, humidity_pct, is_holiday):
    If online_source is NULL / empty these came from Postgres only.
    Difference vs warehouse on the same hour_ts → genuine skew bug.

  Online features (temperature_c, precip_mm):
    When present in online_source their value came from Redis and is expected
    to differ from the warehouse value for that hour.

Revision ID: 0008
Revises: 0007
Create Date: 2024-01-18
"""

from __future__ import annotations

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS marts.prediction_features (
            id              BIGSERIAL       PRIMARY KEY,

            -- Request context
            requested_at    TIMESTAMPTZ     NOT NULL,
            city            TEXT            NOT NULL,
            model_version   INTEGER,

            -- Exact feature vector used (mirrors FEATURE_COLS; row 0 of the
            -- inference DataFrame, converted to DOUBLE PRECISION)
            event_count      DOUBLE PRECISION,
            demand_lag_1h    DOUBLE PRECISION,
            demand_lag_24h   DOUBLE PRECISION,
            demand_lag_168h  DOUBLE PRECISION,
            demand_roll_3h   DOUBLE PRECISION,
            demand_roll_24h  DOUBLE PRECISION,
            hour_sin         DOUBLE PRECISION,
            hour_cos         DOUBLE PRECISION,
            dow_sin          DOUBLE PRECISION,
            dow_cos          DOUBLE PRECISION,
            temperature_c    DOUBLE PRECISION,
            humidity_pct     DOUBLE PRECISION,
            precip_mm        DOUBLE PRECISION,
            is_holiday       DOUBLE PRECISION,

            -- Staleness provenance
            mart_hour_ts    TIMESTAMPTZ,        -- which mart row was used
            online_age_s    DOUBLE PRECISION,   -- seconds since stream_computed_at
            online_source   TEXT[],             -- redis fields present + used
            online_degraded TEXT[],             -- redis fields that breached SLA
            online_missing  TEXT[],             -- redis fields absent (Postgres fallback)

            logged_at       TIMESTAMPTZ     NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS marts_prediction_features_city_requested_at
            ON marts.prediction_features (city, requested_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS marts.prediction_features")
