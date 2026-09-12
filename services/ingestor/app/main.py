"""
Ingestor: consumes demand.events.v1 and weather.readings.v1,
writes rows to raw.demand_events and raw.weather_readings in Postgres.

Two consumer threads run concurrently. The main thread waits for SIGTERM/SIGINT,
sets a stop event, and joins both threads before exiting.
"""

from __future__ import annotations

import signal
import sys
import threading

import structlog

from common.log import configure_logging

from .consumers import (
    _make_dlq_producer,
    demand_consumer_loop,
    weather_consumer_loop,
)
from .parquet_writer import ParquetWriter
from .settings import Settings

log = structlog.get_logger()


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    stop_event = threading.Event()

    def _shutdown(signum, frame):  # noqa: ANN001
        log.info("shutdown_signal_received", signal=signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    dlq_producer = _make_dlq_producer(settings)

    demand_thread = threading.Thread(
        target=demand_consumer_loop,
        args=(settings, dlq_producer, stop_event),
        name="demand-consumer",
        daemon=False,
    )
    weather_thread = threading.Thread(
        target=weather_consumer_loop,
        args=(settings, dlq_producer, stop_event),
        name="weather-consumer",
        daemon=False,
    )
    parquet_thread = ParquetWriter(settings, stop_event)

    log.info(
        "ingestor_started",
        demand_topic=settings.demand_topic,
        weather_topic=settings.weather_topic,
        dlq_topic=settings.dlq_topic,
        parquet_flush_interval_s=settings.parquet_flush_interval_s,
    )

    demand_thread.start()
    weather_thread.start()
    parquet_thread.start()

    # Block until a signal is received, then wait for threads to finish
    stop_event.wait()
    log.info("stopping_consumers")
    demand_thread.join(timeout=15)
    weather_thread.join(timeout=15)
    parquet_thread.join(timeout=30)

    dlq_producer.flush(timeout=10)
    log.info("ingestor_stopped")


if __name__ == "__main__":
    main()
