"""
Shared Kafka producer with versioned message envelope.

Every message produced through EventProducer is a JSON object with this shape:

  {
    "schema_version": <int>,          # payload schema version; bump on breaking change
    "event_id":       "<uuid4>",      # unique per message; useful for deduplication
    "produced_at":    "<iso-utc>",    # UTC wall-clock at time of produce() call
    "payload":        { ... }         # domain-specific data (schema-versioned)
  }

Key:   city name, UTF-8 bytes — routes to the city's consistent partition.
Value: JSON-encoded envelope, UTF-8 bytes.

Schema versioning strategy
--------------------------
Schema versions are plain integers.  The authoritative values live as class
attributes on EventProducer so that producers and consumers import the same
constant:

    from common.producer import EventProducer
    EventProducer.DEMAND_EVENTS_SCHEMA_VERSION    # → 1
    EventProducer.WEATHER_READINGS_SCHEMA_VERSION # → 1

Bump the constant and update the corresponding payload dict whenever a
breaking change is introduced.  Consumers should reject or route to a
dead-letter queue any message whose schema_version they do not recognise.

Thread safety
-------------
confluent_kafka.Producer is thread-safe for produce() calls.
EventProducer inherits that guarantee.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import structlog
from confluent_kafka import Producer

log = structlog.get_logger()

# Type alias for the confluent-kafka delivery callback signature
DeliveryCallback = Callable[[Exception | None, Any], None]


def _log_delivery(err: Exception | None, msg: Any) -> None:
    """Default delivery callback.  Logs errors at ERROR, successes at DEBUG."""
    if err is not None:
        log.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic(),
            partition=msg.partition(),
        )
    else:
        log.debug(
            "kafka_delivery_ok",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


class EventProducer:
    """
    Thin, thread-safe wrapper around confluent_kafka.Producer.

    Responsibilities
    ----------------
    - Build the message envelope (schema_version, event_id, produced_at, payload)
    - Key every message by city name (UTF-8 bytes)
    - Attach a default delivery callback that logs failures
    - Expose flush() and context-manager support for clean shutdown

    Usage
    -----
        producer = EventProducer("redpanda:9092")
        producer.produce(
            topic="demand.events.v1",
            city="london",
            schema_version=EventProducer.DEMAND_EVENTS_SCHEMA_VERSION,
            payload={"city": "london", "event_type": "ELECTRICITY_KWH", ...},
        )
        producer.flush()

    Or as a context manager (flushes on exit):

        with EventProducer("redpanda:9092") as p:
            p.produce(...)
    """

    # ---------------------------------------------------------------------------
    # Schema version constants — bump on any breaking payload change
    # ---------------------------------------------------------------------------

    DEMAND_EVENTS_SCHEMA_VERSION:    int = 1
    WEATHER_READINGS_SCHEMA_VERSION: int = 1

    # ---------------------------------------------------------------------------
    # Constructor
    # ---------------------------------------------------------------------------

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        cfg: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "acks":              "all",
            "retries":           5,
            "retry.backoff.ms":  500,
        }
        if extra_config:
            cfg.update(extra_config)
        self._producer = Producer(cfg)

    # ---------------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------------

    def produce(
        self,
        *,
        topic: str,
        city: str,
        schema_version: int,
        payload: dict[str, Any],
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        """
        Wrap *payload* in an envelope and produce to *topic*, keyed by *city*.

        Calls producer.poll(0) after produce() to serve any pending delivery
        callbacks without blocking.
        """
        envelope: dict[str, Any] = {
            "schema_version": schema_version,
            "event_id":       str(uuid.uuid4()),
            "produced_at":    datetime.now(timezone.utc).isoformat(),
            "payload":        payload,
        }
        self._producer.produce(
            topic=topic,
            key=city.encode(),
            value=json.dumps(envelope, default=str).encode(),
            on_delivery=on_delivery if on_delivery is not None else _log_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """
        Block until all in-flight messages are delivered or *timeout* expires.
        Returns the number of messages still enqueued (0 on clean flush).
        """
        return self._producer.flush(timeout=timeout)

    # ---------------------------------------------------------------------------
    # Context manager
    # ---------------------------------------------------------------------------

    def __enter__(self) -> EventProducer:
        return self

    def __exit__(self, *_: Any) -> None:
        self.flush()
