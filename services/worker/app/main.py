"""
Worker: APScheduler-driven batch pipeline and feature store sync.

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

Startup behaviour
-----------------
The hourly pipeline is also executed once immediately on startup so there is no
silent gap between deployment and the first scheduled run.  APScheduler's
``coalesce=True`` then collapses any additional missed runs into one catch-up
execution.  The feature store sync is NOT run at startup to avoid hammering
Redis and Postgres during a rolling deploy.
"""

from __future__ import annotations

import signal
import sys

import structlog
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from common.log import configure_logging

from .feature_store import run_feature_store
from .pipeline import run_pipeline
from .settings import Settings

log = structlog.get_logger()


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    scheduler = BlockingScheduler(timezone="UTC")

    # ── Hourly batch pipeline ─────────────────────────────────────────────
    scheduler.add_job(
        run_pipeline,
        CronTrigger(minute=settings.pipeline_cron_minute, timezone="UTC"),
        args=[settings],
        id="batch_pipeline",
        name="raw → staging → marts → scaler",
        misfire_grace_time=300,
        coalesce=True,
    )

    # ── Nightly feature store sync ────────────────────────────────────────
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
        # Allow up to 10 minutes late start (Redis + Postgres warm-up).
        misfire_grace_time=600,
        coalesce=True,
    )

    def _shutdown(signum: int, frame: object) -> None:
        log.info("worker_shutdown_signal", signal=signum)
        scheduler.shutdown(wait=False)
        sys.exit(0)

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

    scheduler.start()


if __name__ == "__main__":
    main()
