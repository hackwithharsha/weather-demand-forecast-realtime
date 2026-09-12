"""
Synchronous Postgres helpers for the ingestor (psycopg2).

Each consumer thread owns one connection.  These functions operate on that
connection without managing transactions — callers must commit/rollback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extras
import structlog

log = structlog.get_logger()


@dataclass
class _Buffered:
    """A validated, decoded Kafka message ready for batch insertion."""

    partition:      int
    offset:         int
    key:            bytes | None
    raw_value:      bytes
    schema_version: int
    event_id:       str
    payload:        dict[str, Any]


def connect(dsn: str) -> "psycopg2.connection":
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def batch_insert_demand_events(
    conn: "psycopg2.connection",
    items: list[_Buffered],
) -> None:
    """
    Bulk-insert demand events.  Duplicate event_ids are silently skipped.

    Raises psycopg2.Error on failure; caller must rollback and not commit.
    """
    rows = [
        (
            item.payload["city"],
            item.payload["event_type"],
            item.payload["sim_ts"],
            item.payload["quantity"],
            item.payload.get("temperature_c"),
            item.payload.get("condition"),
            item.partition,
            item.offset,
            item.schema_version,
            item.event_id,
        )
        for item in items
    ]
    sql = """
        INSERT INTO raw.demand_events
            (city, event_type, sim_ts, quantity, temperature_c, condition,
             kafka_partition, kafka_offset, schema_version, event_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()


def batch_insert_weather_readings(
    conn: "psycopg2.connection",
    items: list[_Buffered],
) -> None:
    """
    Bulk-insert weather readings.  Duplicate event_ids are silently skipped.

    Raises psycopg2.Error on failure; caller must rollback and not commit.
    """
    rows = [
        (
            item.payload["city"],
            item.payload["polled_at"],
            item.payload.get("temperature_c"),
            item.payload.get("feels_like_c"),
            item.payload.get("dew_point_c"),
            item.payload.get("humidity_pct"),
            item.payload.get("wind_kph"),
            item.payload.get("wind_direction_deg"),
            item.payload.get("cloud_cover_pct"),
            item.payload.get("precip_probability_pct"),
            item.payload.get("precip_mm"),
            item.payload.get("condition"),
            item.partition,
            item.offset,
            item.schema_version,
            item.event_id,
        )
        for item in items
    ]
    sql = """
        INSERT INTO raw.weather_readings
            (city, polled_at, temperature_c, feels_like_c, dew_point_c,
             humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct,
             precip_probability_pct, precip_mm, condition,
             kafka_partition, kafka_offset, schema_version, event_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
