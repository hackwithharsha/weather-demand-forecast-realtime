"""create marts.city_hour_features

The feature table for demand forecasting.  One row per (city, hour_ts);
keyed by the business hour the demand was observed (sim_ts truncated to hour).

Column groups
-------------
demand aggregates  : total_demand, event_count
lagged demand      : 1 h, 24 h, 168 h (one week) lookbacks
rolling means      : 3 h and 24 h windows (non-overlapping with current hour)
cyclical encodings : hour-of-day and day-of-week sin/cos pairs
weather join       : temperature_c, humidity_pct, precip_mm
holiday flag       : is_holiday per city using public-holiday calendars

Revision ID: 0004
Revises: 0003
Create Date: 2024-01-16

"""

from __future__ import annotations

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS marts.city_hour_features (
            city                TEXT              NOT NULL,
            hour_ts             TIMESTAMPTZ       NOT NULL,

            -- demand aggregates (from staging.demand_hourly)
            total_demand        NUMERIC(14,4),
            event_count         INTEGER,

            -- lagged demand
            demand_lag_1h       NUMERIC(14,4),
            demand_lag_24h      NUMERIC(14,4),
            demand_lag_168h     NUMERIC(14,4),

            -- rolling means (past values only; current hour excluded)
            demand_roll_3h      NUMERIC(14,4),
            demand_roll_24h     NUMERIC(14,4),

            -- cyclical time encodings (range [-1, 1]; no scaling needed)
            hour_sin            DOUBLE PRECISION,
            hour_cos            DOUBLE PRECISION,
            dow_sin             DOUBLE PRECISION,
            dow_cos             DOUBLE PRECISION,

            -- weather (from staging.weather_hourly; NULL when no reading exists)
            temperature_c       NUMERIC(6,2),
            humidity_pct        NUMERIC(5,2),
            precip_mm           NUMERIC(8,3),

            -- public holiday flag per city
            is_holiday          BOOLEAN           NOT NULL DEFAULT FALSE,

            -- bookkeeping
            feature_computed_at TIMESTAMPTZ       NOT NULL DEFAULT now(),

            PRIMARY KEY (city, hour_ts)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS marts_city_hour_features_hour_ts
            ON marts.city_hour_features (hour_ts)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS marts.city_hour_features")
