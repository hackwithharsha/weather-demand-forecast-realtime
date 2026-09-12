"""
Event-generator main loop.

Tick loop
---------
Each real-second tick (tick_interval_s) advances the simulation clock by
  sim_delta = tick_interval_s * sim_speed  (simulated seconds)

For every tick, for every configured city, for every EventType:
  1. Call common.generator.generate_reading(city, sim_ts)
  2. Call demand.generate_quantity(event_type, reading, city_key, sim_ts)
  3. Produce a JSON message to demand.events.v1  (key = city name)
  4. Emit a structlog JSON line to stdout

Kafka message value schema:
  {
    "city":          "london",
    "event_type":    "ELECTRICITY_KWH",
    "sim_ts":        "2024-01-15T14:00:00+00:00",
    "quantity":      742.18,
    "temperature_c": 3.2,
    "condition":     "MOSTLY_CLEAR"
  }
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from datetime import timedelta, timezone
from typing import Any

import structlog
from confluent_kafka import KafkaException, Producer

from common.generator import CITIES, generate_reading
from common.log import configure_logging

from .demand import EventType, generate_quantity
from .settings import Settings

log = structlog.get_logger()

_SHUTDOWN = False


def _install_signal_handlers() -> None:
    def _handler(signum, frame):  # noqa: ANN001
        global _SHUTDOWN
        _SHUTDOWN = True
        log.info("shutdown_signal_received", signal=signum)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT,  _handler)


def _on_delivery(err: Exception | None, msg: Any) -> None:
    if err is not None:
        log.error("kafka_delivery_failed", error=str(err), topic=msg.topic())


def _make_producer(settings: Settings) -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    })


async def run(settings: Settings) -> None:
    city_keys = settings.city_list()
    unknown   = [k for k in city_keys if k not in CITIES]
    if unknown:
        log.warning("unknown_cities_skipped", cities=unknown)
        city_keys = [k for k in city_keys if k in CITIES]

    if not city_keys:
        log.error("no_valid_cities_configured")
        sys.exit(1)

    cities    = {k: CITIES[k] for k in city_keys}
    sim_ts    = settings.start_time().replace(tzinfo=timezone.utc) \
                if settings.start_time().tzinfo is None \
                else settings.start_time()
    sim_delta = timedelta(seconds=settings.tick_interval_s * settings.sim_speed)

    producer = _make_producer(settings)

    log.info(
        "generator_started",
        cities=city_keys,
        tick_interval_s=settings.tick_interval_s,
        sim_speed=settings.sim_speed,
        sim_start=sim_ts.isoformat(),
        events_per_tick=len(city_keys) * len(EventType),
        topic=settings.demand_topic,
    )

    tick = 0
    while not _SHUTDOWN:
        tick += 1
        for city_key, city in cities.items():
            reading = generate_reading(city, sim_ts)

            for event_type in EventType:
                quantity = generate_quantity(
                    event_type=event_type,
                    reading=reading,
                    city_key=city_key,
                    sim_ts=sim_ts,
                )
                payload = {
                    "city":          city_key,
                    "event_type":    event_type.value,
                    "sim_ts":        sim_ts.isoformat(),
                    "quantity":      quantity,
                    "temperature_c": reading.temperature_c,
                    "condition":     reading.condition,
                }
                producer.produce(
                    topic=settings.demand_topic,
                    key=city_key.encode(),
                    value=json.dumps(payload).encode(),
                    on_delivery=_on_delivery,
                )
                producer.poll(0)

                log.info(
                    "demand",
                    city=city_key,
                    event_type=event_type.value,
                    sim_ts=sim_ts.isoformat(),
                    quantity=quantity,
                    temperature_c=reading.temperature_c,
                    condition=reading.condition,
                    tick=tick,
                )

        sim_ts += sim_delta
        await asyncio.sleep(settings.tick_interval_s)

    log.info("flushing_producer")
    producer.flush(timeout=10)
    log.info("generator_stopped", ticks_completed=tick)


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    _install_signal_handlers()
    try:
        asyncio.run(run(settings))
    except KafkaException as exc:
        log.error("fatal_kafka_error", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
