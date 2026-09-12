"""create raw schema and tables

Revision ID: 0001
Revises:
Create Date: 2024-01-16

"""

from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS raw")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.demand_events (
            id               BIGSERIAL     PRIMARY KEY,
            city             TEXT          NOT NULL,
            event_type       TEXT          NOT NULL,
            sim_ts           TIMESTAMPTZ   NOT NULL,
            quantity         NUMERIC(12,4) NOT NULL,
            temperature_c    NUMERIC(6,2),
            condition        TEXT,
            kafka_partition  SMALLINT,
            kafka_offset     BIGINT,
            schema_version   SMALLINT,
            event_id         TEXT          UNIQUE,
            ingested_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS raw_demand_events_city_sim_ts
            ON raw.demand_events (city, sim_ts)
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.weather_readings (
            id                     BIGSERIAL     PRIMARY KEY,
            city                   TEXT          NOT NULL,
            polled_at              TIMESTAMPTZ   NOT NULL,
            temperature_c          NUMERIC(6,2),
            feels_like_c           NUMERIC(6,2),
            dew_point_c            NUMERIC(6,2),
            humidity_pct           SMALLINT,
            wind_kph               NUMERIC(6,2),
            wind_direction_deg     NUMERIC(6,2),
            cloud_cover_pct        SMALLINT,
            precip_probability_pct SMALLINT,
            precip_mm              NUMERIC(8,3),
            condition              TEXT,
            kafka_partition        SMALLINT,
            kafka_offset           BIGINT,
            schema_version         SMALLINT,
            event_id               TEXT          UNIQUE,
            ingested_at            TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS raw_weather_readings_city_polled_at
            ON raw.weather_readings (city, polled_at)
        """
    )


def downgrade() -> None:
    # CASCADE removes tables, indexes, sequences, and any other objects
    # owned by the schema in one shot, avoiding dependency-order issues.
    op.execute("DROP SCHEMA IF EXISTS raw CASCADE")
