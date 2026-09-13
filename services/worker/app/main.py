"""
Worker: APScheduler-driven batch pipeline, feature store sync, and stream
feature consumers.

Schedule
--------
Hourly pipeline  HH:05 UTC  — raw → staging → marts → scaler fit (if needed).
                               The 5-minute offset lets the ingestor write the
                               tail of the previous hour's events before staging
                               reads them.

Nightly sync     02:30 UTC  — route mart computation + Redis feature store sync.
                               Runs at 02:30 so the 02:05 mart run has finished
                               before features are pushed to Redis.
                               Hour and minute are configurable via
                               FEATURE_STORE_CRON_HOUR / FEATURE_STORE_CRON_MINUTE.

Stream consumers (always-on background threads)
-----------------------------------------------
demand_stream_consumer_loop   Reads demand.events.v1, updates Redis sliding-
                               window aggregates (searches_5m, bookings_15m,
                               look_to_book_1h) and writes events to the lake.

weather_stream_consumer_loop  Reads weather.readings.v1, writes latest weather
                               values to Redis and events to the lake.

Startup behaviour
-----------------
The hourly pipeline is also executed once immediately on startup so there is no
silent gap between deployment and the first scheduled run.  APScheduler's
``coalesce=True`` then collapses any additional missed runs into one catch-up
execution.  The feature store sync is NOT run at startup to avoid hammering
Redis and Postgres during a rolling deploy.

Shutdown
--------
SIGTERM / SIGINT sets *stop_event*, which signals the stream consumer threads
to drain their Parquet buffers and exit cleanly.  The scheduler is told to stop
(without waiting for running jobs), causing ``scheduler.start()`` to return.
``main()`` then joins the consumer threads before exiting.
"""

from __future__ import annotations

import signal
import threading

import structlog
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from common.log import configure_logging
from common.metrics import start_metrics_server

from .drift import run_drift_check
from .feature_store import run_feature_store
from .pipeline import run_pipeline
from .settings import Settings
from .stream_features import demand_stream_consumer_loop, weather_stream_consumer_loop

log = structlog.get_logger()


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    start_metrics_server(9101)

    # Shared stop signal for the stream consumer threads.
    stop_event = threading.Event()

    # ── Stream feature consumers ──────────────────────────────────────────
    # Started before the scheduler so they begin consuming immediately.
    # daemon=False ensures the process waits for a clean drain on exit.
    demand_thread = threading.Thread(
        target=demand_stream_consumer_loop,
        args=[settings, stop_event],
        name="stream-demand",
        daemon=False,
    )
    weather_thread = threading.Thread(
        target=weather_stream_consumer_loop,
        args=[settings, stop_event],
        name="stream-weather",
        daemon=False,
    )
    demand_thread.start()
    weather_thread.start()

    # ── APScheduler ───────────────────────────────────────────────────────
    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        run_pipeline,
        CronTrigger(minute=settings.pipeline_cron_minute, timezone="UTC"),
        args=[settings],
        id="batch_pipeline",
        name="raw → staging → marts → scaler",
        misfire_grace_time=300,
        coalesce=True,
    )

    scheduler.add_job(
        run_feature_store,
        CronTrigger(
            hour=settings.feature_store_cron_hour,
            minute=settings.feature_store_cron_minute,
            timezone="UTC",
        ),
        args=[settings],
        id="feature_store_sync",
        name="route mart → Redis feature store",
        misfire_grace_time=600,
        coalesce=True,
    )

    scheduler.add_job(
        run_drift_check,
        "interval",
        minutes=settings.drift_check_interval_minutes,
        args=[settings],
        id="drift_check",
        name=f"Evidently drift check (every {settings.drift_check_interval_minutes} min)",
        coalesce=True,
    )

    def _shutdown(signum: int, frame: object) -> None:
        log.info("worker_shutdown_signal", signal=signum)
        # Signal consumers to drain and exit.
        stop_event.set()
        # Stop the scheduler (non-blocking; running jobs continue to completion).
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info(
        "worker_started",
        pipeline_cron_minute=settings.pipeline_cron_minute,
        feature_store_cron=f"{settings.feature_store_cron_hour:02d}:{settings.feature_store_cron_minute:02d} UTC",
        lookback_days=settings.training_lookback_days,
    )

    # Immediate first run of the hourly pipeline only.
    try:
        run_pipeline(settings)
    except Exception:
        log.warning("worker_startup_pipeline_failed_continuing")

    # Blocks until _shutdown() calls scheduler.shutdown().
    scheduler.start()

    # Wait for stream consumer threads to drain their Parquet buffers.
    demand_thread.join()
    weather_thread.join()
    log.info("worker_stopped")


if __name__ == "__main__":
    main()
