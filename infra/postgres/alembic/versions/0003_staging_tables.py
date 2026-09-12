"""create staging.demand_hourly and staging.weather_hourly

Revision ID: 0003
Revises: 0002
Create Date: 2024-01-16

"""

from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.demand_hourly (
            city         TEXT            NOT NULL,
            hour_ts      TIMESTAMPTZ     NOT NULL,
            total_demand NUMERIC(14,4),
            event_count  INTEGER,
            avg_temp_c   NUMERIC(6,2),
            PRIMARY KEY (city, hour_ts)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS staging_demand_hourly_hour_ts
            ON staging.demand_hourly (hour_ts)
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.weather_hourly (
            city          TEXT        NOT NULL,
            hour_ts       TIMESTAMPTZ NOT NULL,
            temperature_c NUMERIC(6,2),
            humidity_pct  NUMERIC(5,2),
            precip_mm     NUMERIC(8,3),
            PRIMARY KEY (city, hour_ts)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS staging_weather_hourly_hour_ts
            ON staging.weather_hourly (hour_ts)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS staging.weather_hourly")
    op.execute("DROP TABLE IF EXISTS staging.demand_hourly")
