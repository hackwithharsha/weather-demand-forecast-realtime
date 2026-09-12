"""
Synchronous Postgres helpers for the ingestor (psycopg2).

Each consumer thread owns one connection; these functions operate on that
connection and do not manage transactions — callers must commit/rollback.
"""

from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras
import structlog

log = structlog.get_logger()


def connect(dsn: str) -> "psycopg2.connection":
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def insert_demand_event(
    conn: "psycopg2.connection",
    data: dict[str, Any],
    partition: int,
    offset: int,
) -> None:
    sql = """
        INSERT INTO raw.demand_events
            (city, event_type, sim_ts, quantity, temperature_c, condition,
             kafka_partition, kafka_offset)
        VALUES
            (%(city)s, %(event_type)s, %(sim_ts)s, %(quantity)s,
             %(temperature_c)s, %(condition)s, %(partition)s, %(offset)s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "city":          data["city"],
            "event_type":    data["event_type"],
            "sim_ts":        data["sim_ts"],
            "quantity":      data["quantity"],
            "temperature_c": data.get("temperature_c"),
            "condition":     data.get("condition"),
            "partition":     partition,
            "offset":        offset,
        })
    conn.commit()


def insert_weather_reading(
    conn: "psycopg2.connection",
    data: dict[str, Any],
    partition: int,
    offset: int,
) -> None:
    sql = """
        INSERT INTO raw.weather_readings
            (city, polled_at, temperature_c, feels_like_c, dew_point_c,
             humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct,
             precip_probability_pct, precip_mm, condition,
             kafka_partition, kafka_offset)
        VALUES
            (%(city)s, %(polled_at)s, %(temperature_c)s, %(feels_like_c)s,
             %(dew_point_c)s, %(humidity_pct)s, %(wind_kph)s,
             %(wind_direction_deg)s, %(cloud_cover_pct)s,
             %(precip_probability_pct)s, %(precip_mm)s, %(condition)s,
             %(partition)s, %(offset)s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, {**data, "partition": partition, "offset": offset})
    conn.commit()
