"""
Consumer threads — one per topic.

Each thread:
  1. Polls its topic (0.5 s timeout for batch-timing precision).
  2. Parses + validates the JSON envelope; invalid messages → DLQ immediately.
  3. Accumulates valid messages into a batch buffer.
  4. Flushes when the batch reaches BATCH_SIZE messages or BATCH_TIMEOUT seconds.
  5. On flush: bulk-inserts with ON CONFLICT (event_id) DO NOTHING, then commits
     per-partition max-offsets for the whole cycle (valid + DLQ'd).
  6. On DB failure during flush: rolls back, sends every batch item to DLQ, then
     commits offsets (no stuck partitions).

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
import time
from typing import Any

import psycopg2
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from common.producer import EventProducer

from .db import _Buffered, batch_insert_demand_events, batch_insert_weather_readings, connect
from .metrics import (
    batch_write_duration_seconds,
    batch_write_rows_total,
    messages_consumed_total,
    messages_dlq_total,
    messages_validation_failed_total,
)
from .settings import Settings

log = structlog.get_logger()

BATCH_SIZE    = 500
BATCH_TIMEOUT = 2.0  # seconds

# Schema versions this ingestor knows how to handle
_KNOWN_DEMAND_VERSIONS  = frozenset({EventProducer.DEMAND_EVENTS_SCHEMA_VERSION})
_KNOWN_WEATHER_VERSIONS = frozenset({EventProducer.WEATHER_READINGS_SCHEMA_VERSION})


class ValidationError(Exception):
    pass


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

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


def _validate_demand(schema_version: int, payload: dict[str, Any]) -> None:
    if schema_version not in _KNOWN_DEMAND_VERSIONS:
        raise ValidationError(f"unknown demand schema_version={schema_version}")
    for field in ("city", "event_type", "sim_ts", "quantity"):
        if field not in payload:
            raise ValidationError(f"demand payload missing required field: {field!r}")


def _validate_weather(schema_version: int, payload: dict[str, Any]) -> None:
    if schema_version not in _KNOWN_WEATHER_VERSIONS:
        raise ValidationError(f"unknown weather schema_version={schema_version}")
    for field in ("city", "polled_at"):
        if field not in payload:
            raise ValidationError(f"weather payload missing required field: {field!r}")


def _update_max_offset(offsets: dict[int, int], partition: int, offset: int) -> None:
    if partition not in offsets or offset > offsets[partition]:
        offsets[partition] = offset


def _commit_offsets(consumer: Consumer, topic: str, offsets: dict[int, int]) -> None:
    tps = [TopicPartition(topic, p, o + 1) for p, o in offsets.items()]
    consumer.commit(offsets=tps, asynchronous=False)


# ---------------------------------------------------------------------------
# Demand consumer
# ---------------------------------------------------------------------------

def _flush_demand_batch(
    conn: "psycopg2.connection",
    consumer: Consumer,
    dlq_producer: Producer,
    settings: Settings,
    batch: list[_Buffered],
    pending_offsets: dict[int, int],
) -> None:
    if batch:
        t0 = time.monotonic()
        try:
            batch_insert_demand_events(conn, batch)
            elapsed = time.monotonic() - t0
            batch_write_duration_seconds.labels(topic="demand").observe(elapsed)
            batch_write_rows_total.labels(topic="demand").inc(len(batch))
            log.info("demand_batch_flushed", count=len(batch))
        except psycopg2.Error as exc:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
            log.error("demand_batch_insert_failed", count=len(batch), error=str(exc))
            for item in batch:
                _send_to_dlq(
                    dlq_producer, settings.dlq_topic, item.key, item.raw_value, str(exc)
                )
                messages_dlq_total.labels(topic="demand").inc()
    _commit_offsets(consumer, settings.demand_topic, pending_offsets)


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

        batch: list[_Buffered] = []
        pending_offsets: dict[int, int] = {}
        batch_started = time.monotonic()

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("demand_consumer_error", error=str(msg.error()))
                else:
                    raw_value = msg.value()
                    try:
                        schema_version, event_id, payload = _unwrap(raw_value)
                        _validate_demand(schema_version, payload)
                        batch.append(_Buffered(
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            raw_value=raw_value,
                            schema_version=schema_version,
                            event_id=event_id,
                            payload=payload,
                        ))
                        messages_consumed_total.labels(topic="demand").inc()
                        log.debug(
                            "demand_buffered",
                            city=payload.get("city"),
                            event_type=payload.get("event_type"),
                            event_id=event_id,
                            offset=msg.offset(),
                            partition=msg.partition(),
                        )
                    except (json.JSONDecodeError, KeyError, ValidationError) as exc:
                        messages_validation_failed_total.labels(topic="demand").inc()
                        messages_dlq_total.labels(topic="demand").inc()
                        log.warning(
                            "demand_validation_failed",
                            offset=msg.offset(),
                            partition=msg.partition(),
                            error=str(exc),
                        )
                        _send_to_dlq(
                            dlq_producer, settings.dlq_topic,
                            msg.key(), raw_value, str(exc),
                        )

                    _update_max_offset(pending_offsets, msg.partition(), msg.offset())

            now = time.monotonic()
            if pending_offsets and (
                len(batch) >= BATCH_SIZE
                or now - batch_started >= BATCH_TIMEOUT
            ):
                _flush_demand_batch(
                    conn, consumer, dlq_producer, settings, batch, pending_offsets
                )
                batch = []
                pending_offsets = {}
                batch_started = now

    except psycopg2.OperationalError as exc:
        log.error("demand_db_connect_failed", error=str(exc))
    finally:
        if pending_offsets and conn is not None:
            try:
                _flush_demand_batch(
                    conn, consumer, dlq_producer, settings, batch, pending_offsets
                )
            except Exception:
                pass
        consumer.close()
        if conn is not None:
            conn.close()
        log.info("demand_consumer_stopped")


# ---------------------------------------------------------------------------
# Weather consumer
# ---------------------------------------------------------------------------

def _flush_weather_batch(
    conn: "psycopg2.connection",
    consumer: Consumer,
    dlq_producer: Producer,
    settings: Settings,
    batch: list[_Buffered],
    pending_offsets: dict[int, int],
) -> None:
    if batch:
        t0 = time.monotonic()
        try:
            batch_insert_weather_readings(conn, batch)
            elapsed = time.monotonic() - t0
            batch_write_duration_seconds.labels(topic="weather").observe(elapsed)
            batch_write_rows_total.labels(topic="weather").inc(len(batch))
            log.info("weather_batch_flushed", count=len(batch))
        except psycopg2.Error as exc:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
            log.error("weather_batch_insert_failed", count=len(batch), error=str(exc))
            for item in batch:
                _send_to_dlq(
                    dlq_producer, settings.dlq_topic, item.key, item.raw_value, str(exc)
                )
                messages_dlq_total.labels(topic="weather").inc()
    _commit_offsets(consumer, settings.weather_topic, pending_offsets)


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

        batch: list[_Buffered] = []
        pending_offsets: dict[int, int] = {}
        batch_started = time.monotonic()

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("weather_consumer_error", error=str(msg.error()))
                else:
                    raw_value = msg.value()
                    try:
                        schema_version, event_id, payload = _unwrap(raw_value)
                        _validate_weather(schema_version, payload)
                        batch.append(_Buffered(
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            raw_value=raw_value,
                            schema_version=schema_version,
                            event_id=event_id,
                            payload=payload,
                        ))
                        messages_consumed_total.labels(topic="weather").inc()
                        log.debug(
                            "weather_buffered",
                            city=payload.get("city"),
                            event_id=event_id,
                            offset=msg.offset(),
                            partition=msg.partition(),
                        )
                    except (json.JSONDecodeError, KeyError, ValidationError) as exc:
                        messages_validation_failed_total.labels(topic="weather").inc()
                        messages_dlq_total.labels(topic="weather").inc()
                        log.warning(
                            "weather_validation_failed",
                            offset=msg.offset(),
                            partition=msg.partition(),
                            error=str(exc),
                        )
                        _send_to_dlq(
                            dlq_producer, settings.dlq_topic,
                            msg.key(), raw_value, str(exc),
                        )

                    _update_max_offset(pending_offsets, msg.partition(), msg.offset())

            now = time.monotonic()
            if pending_offsets and (
                len(batch) >= BATCH_SIZE
                or now - batch_started >= BATCH_TIMEOUT
            ):
                _flush_weather_batch(
                    conn, consumer, dlq_producer, settings, batch, pending_offsets
                )
                batch = []
                pending_offsets = {}
                batch_started = now

    except psycopg2.OperationalError as exc:
        log.error("weather_db_connect_failed", error=str(exc))
    finally:
        if pending_offsets and conn is not None:
            try:
                _flush_weather_batch(
                    conn, consumer, dlq_producer, settings, batch, pending_offsets
                )
            except Exception:
                pass
        consumer.close()
        if conn is not None:
            conn.close()
        log.info("weather_consumer_stopped")
