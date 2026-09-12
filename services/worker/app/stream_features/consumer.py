"""
Stream feature consumer loops — one per topic.

demand_stream_consumer_loop
  Reads demand.events.v1.  For each message:
    1. Updates Redis sliding-window aggregates (searches_5m, bookings_15m,
       look_to_book_1h, stream_computed_at) via update_demand_features().
    2. Appends the event payload to a ParquetBuffer so every event is
       preserved in the lake for future ML training.

weather_stream_consumer_loop
  Reads weather.readings.v1.  For each message:
    1. Writes latest weather values to Redis via update_weather_features().
    2. Appends the reading to a ParquetBuffer.

Both loops:
  - Use separate confluent_kafka.Consumer instances with distinct group IDs
    (worker-stream-demand / worker-stream-weather) that are independent of
    the ingestor's consumer groups.
  - Commit Kafka offsets in micro-batches (COMMIT_SIZE messages or
    COMMIT_TIMEOUT seconds), whichever comes first.
  - Flush the ParquetBuffer on a longer, independent timer
    (stream_parquet_flush_interval_s, default 60 s).
  - On parse / validation error: log a warning and commit the offset so the
    consumer does not stall on a permanently invalid message.
  - On Redis error: log an error but do NOT commit the offset — the message
    will be re-delivered on restart and the Redis operations are idempotent
    (ZADD member uniqueness / HSET field overwrite), so the retry is safe.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

import redis as _redis_module
import structlog
from confluent_kafka import Consumer, KafkaError, TopicPartition

from common.producer import EventProducer

from ..settings import Settings
from .parquet_buffer import ParquetBuffer
from .redis_ops import update_demand_features, update_weather_features

log = structlog.get_logger()

# Micro-batch commit thresholds — same as the ingestor.
_COMMIT_SIZE    = 500
_COMMIT_TIMEOUT = 2.0  # seconds

# Schema versions this consumer understands.
_KNOWN_DEMAND_VERSIONS  = frozenset({EventProducer.DEMAND_EVENTS_SCHEMA_VERSION})
_KNOWN_WEATHER_VERSIONS = frozenset({EventProducer.WEATHER_READINGS_SCHEMA_VERSION})


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


def _make_redis(settings: Settings) -> _redis_module.Redis:
    return _redis_module.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )


def _unwrap(raw_value: bytes) -> tuple[int, str, dict[str, Any]]:
    """Decode envelope → (schema_version, event_id, payload)."""
    envelope: dict[str, Any] = json.loads(raw_value)
    return envelope["schema_version"], envelope["event_id"], envelope["payload"]


def _update_max_offset(offsets: dict[int, int], partition: int, offset: int) -> None:
    if partition not in offsets or offset > offsets[partition]:
        offsets[partition] = offset


def _commit(consumer: Consumer, topic: str, offsets: dict[int, int]) -> None:
    tps = [TopicPartition(topic, p, o + 1) for p, o in offsets.items()]
    consumer.commit(offsets=tps, asynchronous=False)


# ---------------------------------------------------------------------------
# Demand consumer
# ---------------------------------------------------------------------------

def demand_stream_consumer_loop(
    settings: Settings,
    stop_event: threading.Event,
) -> None:
    """Read demand.events.v1 → update Redis windows → buffer Parquet.

    Runs until *stop_event* is set.  Designed to be started as a daemon-False
    background thread so the process waits for a clean shutdown flush.
    """
    consumer = _make_consumer(settings, settings.stream_demand_group_id)
    consumer.subscribe([settings.demand_topic])
    r = _make_redis(settings)
    buf = ParquetBuffer(settings, "demand_events")

    pending_offsets: dict[int, int] = {}
    commit_started  = time.monotonic()
    parquet_flushed = time.monotonic()

    try:
        log.info("stream_demand_consumer_started", topic=settings.demand_topic)

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error(
                            "stream_demand_kafka_error", error=str(msg.error())
                        )
                else:
                    raw_value = msg.value()
                    try:
                        schema_version, event_id, payload = _unwrap(raw_value)

                        if schema_version not in _KNOWN_DEMAND_VERSIONS:
                            raise ValueError(
                                f"unknown demand schema_version={schema_version}"
                            )
                        for field in ("city", "event_type", "sim_ts", "quantity"):
                            if field not in payload:
                                raise ValueError(
                                    f"demand payload missing required field: {field!r}"
                                )

                        route_id   = payload["city"]
                        event_type = payload["event_type"]
                        sim_ts_str = payload["sim_ts"]
                        sim_epoch  = datetime.fromisoformat(sim_ts_str).timestamp()

                        # ── Redis update (idempotent) ──────────────────────
                        update_demand_features(
                            r,
                            route_id=route_id,
                            event_id=event_id,
                            event_type=event_type,
                            sim_epoch=sim_epoch,
                        )

                        # ── Parquet buffer ────────────────────────────────
                        sim_dt = datetime.fromtimestamp(sim_epoch, tz=timezone.utc)
                        record = dict(payload)
                        record["event_id"]       = event_id
                        record["schema_version"] = schema_version
                        record["_dt"]            = sim_dt.strftime("%Y-%m-%d")
                        record["_hour"]          = sim_dt.hour
                        buf.append(record)

                        _update_max_offset(
                            pending_offsets, msg.partition(), msg.offset()
                        )

                    except (json.JSONDecodeError, KeyError, ValueError) as exc:
                        # Invalid message: log, skip, still commit offset.
                        log.warning(
                            "stream_demand_parse_failed",
                            offset=msg.offset(),
                            partition=msg.partition(),
                            error=str(exc),
                        )
                        _update_max_offset(
                            pending_offsets, msg.partition(), msg.offset()
                        )

                    except _redis_module.RedisError as exc:
                        # Redis failure: log, do NOT commit — will retry.
                        log.error(
                            "stream_demand_redis_error",
                            offset=msg.offset(),
                            partition=msg.partition(),
                            error=str(exc),
                        )

            now = time.monotonic()

            # Micro-batch offset commit.
            if pending_offsets and (
                len(pending_offsets) >= _COMMIT_SIZE
                or now - commit_started >= _COMMIT_TIMEOUT
            ):
                _commit(consumer, settings.demand_topic, pending_offsets)
                pending_offsets = {}
                commit_started  = now

            # Independent Parquet flush timer.
            if now - parquet_flushed >= settings.stream_parquet_flush_interval_s:
                buf.flush()
                parquet_flushed = now

    finally:
        # Drain: commit remaining offsets, flush buffer, close resources.
        if pending_offsets:
            try:
                _commit(consumer, settings.demand_topic, pending_offsets)
            except Exception:
                log.exception("stream_demand_final_commit_failed")
        buf.flush()
        consumer.close()
        r.close()
        log.info("stream_demand_consumer_stopped")


# ---------------------------------------------------------------------------
# Weather consumer
# ---------------------------------------------------------------------------

def weather_stream_consumer_loop(
    settings: Settings,
    stop_event: threading.Event,
) -> None:
    """Read weather.readings.v1 → update Redis latest values → buffer Parquet.

    Runs until *stop_event* is set.
    """
    consumer = _make_consumer(settings, settings.stream_weather_group_id)
    consumer.subscribe([settings.weather_topic])
    r = _make_redis(settings)
    buf = ParquetBuffer(settings, "weather_readings")

    pending_offsets: dict[int, int] = {}
    commit_started  = time.monotonic()
    parquet_flushed = time.monotonic()

    try:
        log.info("stream_weather_consumer_started", topic=settings.weather_topic)

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error(
                            "stream_weather_kafka_error", error=str(msg.error())
                        )
                else:
                    raw_value = msg.value()
                    try:
                        schema_version, event_id, payload = _unwrap(raw_value)

                        if schema_version not in _KNOWN_WEATHER_VERSIONS:
                            raise ValueError(
                                f"unknown weather schema_version={schema_version}"
                            )
                        for field in ("city", "polled_at"):
                            if field not in payload:
                                raise ValueError(
                                    f"weather payload missing required field: {field!r}"
                                )

                        route_id      = payload["city"]
                        polled_at_str = payload["polled_at"]
                        temperature_c = payload.get("temperature_c")
                        precip_mm     = payload.get("precip_mm")
                        condition     = payload.get("condition")

                        # ── Redis update (latest-value write) ─────────────
                        update_weather_features(
                            r,
                            route_id=route_id,
                            temperature_c=temperature_c,
                            precip_mm=precip_mm,
                            condition=condition,
                        )

                        # ── Parquet buffer ────────────────────────────────
                        polled_dt = datetime.fromisoformat(polled_at_str).astimezone(
                            timezone.utc
                        )
                        record = dict(payload)
                        record["event_id"]       = event_id
                        record["schema_version"] = schema_version
                        record["_dt"]            = polled_dt.strftime("%Y-%m-%d")
                        record["_hour"]          = polled_dt.hour
                        buf.append(record)

                        _update_max_offset(
                            pending_offsets, msg.partition(), msg.offset()
                        )

                    except (json.JSONDecodeError, KeyError, ValueError) as exc:
                        log.warning(
                            "stream_weather_parse_failed",
                            offset=msg.offset(),
                            partition=msg.partition(),
                            error=str(exc),
                        )
                        _update_max_offset(
                            pending_offsets, msg.partition(), msg.offset()
                        )

                    except _redis_module.RedisError as exc:
                        log.error(
                            "stream_weather_redis_error",
                            offset=msg.offset(),
                            partition=msg.partition(),
                            error=str(exc),
                        )

            now = time.monotonic()

            if pending_offsets and (
                len(pending_offsets) >= _COMMIT_SIZE
                or now - commit_started >= _COMMIT_TIMEOUT
            ):
                _commit(consumer, settings.weather_topic, pending_offsets)
                pending_offsets = {}
                commit_started  = now

            if now - parquet_flushed >= settings.stream_parquet_flush_interval_s:
                buf.flush()
                parquet_flushed = now

    finally:
        if pending_offsets:
            try:
                _commit(consumer, settings.weather_topic, pending_offsets)
            except Exception:
                log.exception("stream_weather_final_commit_failed")
        buf.flush()
        consumer.close()
        r.close()
        log.info("stream_weather_consumer_stopped")
