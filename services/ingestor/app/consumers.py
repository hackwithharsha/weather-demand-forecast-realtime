"""
Consumer threads — one per topic.

Each thread:
  1. Polls its topic in a tight loop.
  2. Parses the JSON envelope (schema_version, event_id, produced_at, payload).
  3. Validates the schema_version is one it knows how to handle.
  4. Writes the payload to the corresponding Postgres raw table.
  5. Commits the Kafka offset only after a successful DB write.
  6. On any processing failure: sends the raw bytes to the DLQ with a
     reason header, then commits the offset (no stuck partitions).

Threading model
---------------
Two worker threads are started by main.py.  Each owns its own:
  - confluent_kafka.Consumer instance
  - psycopg2 connection

The DLQ producer is shared between threads; confluent_kafka.Producer is
thread-safe for produce() calls.
"""

from __future__ import annotations

import json
import threading
from typing import Any

import psycopg2
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from common.producer import EventProducer

from .db import connect, insert_demand_event, insert_weather_reading
from .settings import Settings

log = structlog.get_logger()

# Schema versions this ingestor knows how to handle
_KNOWN_DEMAND_VERSIONS  = frozenset({EventProducer.DEMAND_EVENTS_SCHEMA_VERSION})
_KNOWN_WEATHER_VERSIONS = frozenset({EventProducer.WEATHER_READINGS_SCHEMA_VERSION})


def _make_consumer(settings: Settings, group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers":  settings.kafka_bootstrap_servers,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })


def _make_dlq_producer(settings: Settings) -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "1",
    })


def _send_to_dlq(
    producer: Producer,
    dlq_topic: str,
    key: bytes | None,
    value: bytes | None,
    reason: str,
) -> None:
    try:
        producer.produce(
            topic=dlq_topic,
            key=key,
            value=value,
            headers={"dlq-reason": reason.encode()},
        )
        producer.poll(0)
    except KafkaException as exc:
        log.error("dlq_produce_failed", error=str(exc))


def _unwrap(raw_value: bytes) -> tuple[int, str, dict[str, Any]]:
    """
    Decode and unwrap a message envelope.

    Returns (schema_version, event_id, payload).
    Raises KeyError if required envelope fields are missing.
    """
    envelope: dict[str, Any] = json.loads(raw_value)
    return (
        envelope["schema_version"],
        envelope["event_id"],
        envelope["payload"],
    )


# ---------------------------------------------------------------------------
# Demand consumer
# ---------------------------------------------------------------------------

def demand_consumer_loop(
    settings: Settings,
    dlq_producer: Producer,
    stop_event: threading.Event,
) -> None:
    consumer = _make_consumer(settings, settings.demand_group_id)
    consumer.subscribe([settings.demand_topic])
    conn: psycopg2.connection | None = None

    try:
        conn = connect(settings.postgres_dsn)
        log.info("demand_consumer_started", topic=settings.demand_topic)

        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("demand_consumer_error", error=str(msg.error()))
                continue

            raw_value = msg.value()
            try:
                schema_version, event_id, payload = _unwrap(raw_value)

                if schema_version not in _KNOWN_DEMAND_VERSIONS:
                    raise ValueError(
                        f"unknown demand schema_version={schema_version}"
                    )

                insert_demand_event(
                    conn, payload, msg.partition(), msg.offset(),
                    schema_version=schema_version, event_id=event_id,
                )
                log.debug(
                    "demand_inserted",
                    city=payload.get("city"),
                    event_type=payload.get("event_type"),
                    schema_version=schema_version,
                    event_id=event_id,
                    offset=msg.offset(),
                    partition=msg.partition(),
                )

            except (json.JSONDecodeError, KeyError, ValueError, psycopg2.Error) as exc:
                if isinstance(exc, psycopg2.Error):
                    try:
                        conn.rollback()
                    except psycopg2.Error:
                        pass
                log.warning(
                    "demand_processing_failed",
                    offset=msg.offset(),
                    partition=msg.partition(),
                    error=str(exc),
                )
                _send_to_dlq(
                    dlq_producer, settings.dlq_topic,
                    msg.key(), raw_value, str(exc),
                )

            consumer.commit(message=msg, asynchronous=False)

    except psycopg2.OperationalError as exc:
        log.error("demand_db_connect_failed", error=str(exc))
    finally:
        consumer.close()
        if conn is not None:
            conn.close()
        log.info("demand_consumer_stopped")


# ---------------------------------------------------------------------------
# Weather consumer
# ---------------------------------------------------------------------------

def weather_consumer_loop(
    settings: Settings,
    dlq_producer: Producer,
    stop_event: threading.Event,
) -> None:
    consumer = _make_consumer(settings, settings.weather_group_id)
    consumer.subscribe([settings.weather_topic])
    conn: psycopg2.connection | None = None

    try:
        conn = connect(settings.postgres_dsn)
        log.info("weather_consumer_started", topic=settings.weather_topic)

        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("weather_consumer_error", error=str(msg.error()))
                continue

            raw_value = msg.value()
            try:
                schema_version, event_id, payload = _unwrap(raw_value)

                if schema_version not in _KNOWN_WEATHER_VERSIONS:
                    raise ValueError(
                        f"unknown weather schema_version={schema_version}"
                    )

                insert_weather_reading(
                    conn, payload, msg.partition(), msg.offset(),
                    schema_version=schema_version, event_id=event_id,
                )
                log.debug(
                    "weather_inserted",
                    city=payload.get("city"),
                    schema_version=schema_version,
                    event_id=event_id,
                    offset=msg.offset(),
                    partition=msg.partition(),
                )

            except (json.JSONDecodeError, KeyError, ValueError, psycopg2.Error) as exc:
                if isinstance(exc, psycopg2.Error):
                    try:
                        conn.rollback()
                    except psycopg2.Error:
                        pass
                log.warning(
                    "weather_processing_failed",
                    offset=msg.offset(),
                    partition=msg.partition(),
                    error=str(exc),
                )
                _send_to_dlq(
                    dlq_producer, settings.dlq_topic,
                    msg.key(), raw_value, str(exc),
                )

            consumer.commit(message=msg, asynchronous=False)

    except psycopg2.OperationalError as exc:
        log.error("weather_db_connect_failed", error=str(exc))
    finally:
        consumer.close()
        if conn is not None:
            conn.close()
        log.info("weather_consumer_stopped")
