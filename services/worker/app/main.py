"""
Worker: APScheduler-driven batch pipeline.

Schedule
--------
The pipeline fires at HH:05 UTC on every wall-clock hour.  The 5-minute
offset ensures the ingestor has had time to write the tail of the previous
hour's events into raw.* before staging reads them.

Startup behaviour
-----------------
The pipeline is also executed once immediately on startup so there is no
silent gap between deployment and the first scheduled run.  APScheduler's
``coalesce=True`` then collapses any additional missed runs into one catch-up
execution.
"""

from __future__ import annotations

import signal
import sys

import structlog
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from common.log import configure_logging

from .pipeline import run_pipeline
from .settings import Settings

log = structlog.get_logger()


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_pipeline,
        CronTrigger(minute=settings.pipeline_cron_minute, timezone="UTC"),
        args=[settings],
        id="batch_pipeline",
        name="raw → staging → marts → scaler",
        # Tolerate up to 5 min late start (e.g. slow container boot).
        misfire_grace_time=300,
        # If several runs were missed (crash + restart), execute once to
        # catch up rather than firing a burst of back-to-back runs.
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
        cron_minute=settings.pipeline_cron_minute,
        lookback_days=settings.training_lookback_days,
        scaler_key=settings.scaler_s3_key,
    )

    # Immediate first run — populates staging/marts and fits the scaler if
    # this is a cold start.  Failures are logged but do not abort the scheduler.
    try:
        run_pipeline(settings)
    except Exception:
        log.warning("worker_startup_pipeline_failed_continuing")

    scheduler.start()


if __name__ == "__main__":
    main()
