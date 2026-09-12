"""create marts.route_features_daily

Offline feature store table for route-level demand signals.

Key design decisions
--------------------
keyed by (route_id, feature_date)
    One row per route per calendar day.  The batch job computes features for
    CURRENT_DATE - 1 (the most recently completed day) and upserts here.
    Historical rows are kept so the batch sync can always read the latest row
    and so training pipelines can join on feature_date.

route_id TEXT, not a foreign key
    Route IDs come from the demand pipeline (city names in v1; will become
    origin/destination pairs in v2).  A FK would require a routes dimension
    table that does not yet exist.  TEXT keeps the migration self-contained.

DOUBLE PRECISION for all feature columns
    Avoids Decimal serialisation in psycopg2 (NUMERIC returns decimal.Decimal
    which requires explicit float() calls everywhere).  The features are
    statistical estimates; eight-byte float64 precision is sufficient.

Seasonality stored as 7 columns (one per ISO weekday)
    Storing a single ``seasonality_index`` would force the API to know which
    row to read for a given forecast day.  Storing all seven values in the
    same row allows the API to fetch the whole vector with one HGET call and
    pick the right DOW field at serving time.

Revision ID: 0006
Revises: 0005
Create Date: 2024-01-17
"""

from __future__ import annotations

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS marts.route_features_daily (
            route_id        TEXT             NOT NULL,
            feature_date    DATE             NOT NULL,

            -- 90-day rolling average of daily bookings
            avg_bookings_90d        DOUBLE PRECISION,

            -- Seasonality index per ISO weekday (1=Mon … 7=Sun).
            -- Value = avg demand on that DOW / route overall avg demand.
            seasonality_mon         DOUBLE PRECISION,
            seasonality_tue         DOUBLE PRECISION,
            seasonality_wed         DOUBLE PRECISION,
            seasonality_thu         DOUBLE PRECISION,
            seasonality_fri         DOUBLE PRECISION,
            seasonality_sat         DOUBLE PRECISION,
            seasonality_sun         DOUBLE PRECISION,

            -- Booking lead time percentiles (hours)
            lead_time_p50           DOUBLE PRECISION,
            lead_time_p90           DOUBLE PRECISION,

            -- 180-day cancellation rate [0, 1]
            cancellation_rate_180d  DOUBLE PRECISION,

            -- Pearson correlation: temperature_c vs total_demand
            elasticity_estimate     DOUBLE PRECISION,

            -- Bookkeeping
            feature_computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

            PRIMARY KEY (route_id, feature_date)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS marts_route_features_daily_feature_date
            ON marts.route_features_daily (feature_date DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS marts.route_features_daily")
