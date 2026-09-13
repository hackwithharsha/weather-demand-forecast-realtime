"""
Drift detection service.

Runs an hourly Evidently drift check comparing:
  • reference: last ``reference_days`` days of marts.city_hour_features
               (the feature distribution the model was trained on)
  • current:   last ``current_hours`` hours of marts.prediction_features
               (the feature vectors that were actually served)

Results are written to marts.drift_reports and exposed as Prometheus
gauges so Grafana can visualise feature drift over time.

APScheduler drives the schedule.  An immediate check fires on startup so
the first scrape by Prometheus already has values.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
import psycopg2.extras
import structlog
from apscheduler.schedulers.blocking import BlockingScheduler
from evidently.metrics import ColumnDriftMetric
from evidently.report import Report
from prometheus_client import start_http_server

from .metrics import (
    drift_detected_gauge,
    drift_job_current_count,
    drift_job_duration_seconds,
    drift_job_last_run_timestamp,
    drift_job_reference_count,
    drift_score_gauge,
)
from .settings import DRIFT_FEATURES, Settings

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Core drift check
# ---------------------------------------------------------------------------

def _run_drift_check(settings: Settings) -> None:
    """
    Query reference + current data, run Evidently, write results.

    Never raises — errors are caught and logged so the scheduler keeps
    firing on the next interval.
    """
    t0 = time.monotonic()
    checked_at = datetime.now(timezone.utc)

    log.info("drift_check_started", checked_at=checked_at.isoformat())

    conn: psycopg2.extensions.connection | None = None
    try:
        conn = psycopg2.connect(settings.postgres_dsn)

        # ── Reference: last N days of warehouse features ──────────────────
        ref_cutoff = checked_at - timedelta(days=settings.reference_days)
        cols = ", ".join(DRIFT_FEATURES)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT {cols} FROM marts.city_hour_features WHERE hour_ts >= %s",
                (ref_cutoff,),
            )
            ref_rows = cur.fetchall()

        # ── Current: last N hours of serving features ─────────────────────
        cur_cutoff = checked_at - timedelta(hours=settings.current_hours)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT {cols} FROM marts.prediction_features WHERE requested_at >= %s",
                (cur_cutoff,),
            )
            cur_rows = cur.fetchall()

        n_ref = len(ref_rows)
        n_cur = len(cur_rows)
        drift_job_reference_count.set(n_ref)
        drift_job_current_count.set(n_cur)

        if n_cur < settings.min_current_rows:
            log.warning(
                "drift_check_skipped",
                reason="insufficient current rows",
                current_rows=n_cur,
                min_required=settings.min_current_rows,
            )
            return

        if n_ref < settings.min_current_rows:
            log.warning(
                "drift_check_skipped",
                reason="insufficient reference rows",
                reference_rows=n_ref,
            )
            return

        ref_df = pd.DataFrame(ref_rows).astype(float, errors="ignore")
        cur_df = pd.DataFrame(cur_rows).astype(float, errors="ignore")

        # Only compare features that exist in both DataFrames
        available = [f for f in DRIFT_FEATURES if f in ref_df.columns and f in cur_df.columns]
        if not available:
            log.warning("drift_check_skipped", reason="no comparable columns")
            return

        # ── Evidently drift report ────────────────────────────────────────
        report = Report(metrics=[ColumnDriftMetric(column_name=f) for f in available])
        report.run(
            reference_data=ref_df[available],
            current_data=cur_df[available],
        )
        report_dict = report.as_dict()

        # ── Parse + write results ─────────────────────────────────────────
        rows_to_insert: list[tuple] = []
        for item in report_dict.get("metrics", []):
            res = item.get("result", {})
            feature_name: str | None = res.get("column_name")
            if not feature_name:
                continue

            drift_score: float | None = res.get("drift_score")
            drift_det: bool = bool(res.get("drift_detected", False))
            stat_test: str | None = res.get("stattest_name")
            p_value: float | None = res.get("p_value")

            # Update Prometheus gauges (persisted until next check)
            drift_score_gauge.labels(feature=feature_name).set(
                drift_score if drift_score is not None else 0.0
            )
            drift_detected_gauge.labels(feature=feature_name).set(1.0 if drift_det else 0.0)

            rows_to_insert.append((
                checked_at, feature_name, drift_score, drift_det,
                stat_test, p_value, n_ref, n_cur,
            ))
            log.info(
                "drift_result",
                feature=feature_name,
                drift_score=round(drift_score, 4) if drift_score is not None else None,
                drift_detected=drift_det,
                stat_test=stat_test,
            )

        if rows_to_insert:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO marts.drift_reports
                        (checked_at, feature_name, drift_score, drift_detected,
                         stat_test, p_value, reference_count, current_count)
                    VALUES %s
                    """,
                    rows_to_insert,
                )
            conn.commit()
            log.info("drift_reports_written", count=len(rows_to_insert), checked_at=checked_at.isoformat())

    except Exception:
        log.exception("drift_check_failed")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn is not None:
            conn.close()

    elapsed = time.monotonic() - t0
    drift_job_duration_seconds.set(elapsed)
    drift_job_last_run_timestamp.set_to_current_time()
    log.info("drift_check_completed", duration_s=round(elapsed, 2))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
    )

    settings = Settings()

    start_http_server(settings.metrics_port)
    log.info("drift_metrics_server_started", port=settings.metrics_port)

    # Immediate check on startup so Prometheus gets values before the first
    # scheduled run fires.
    try:
        _run_drift_check(settings)
    except Exception:
        log.warning("drift_check_startup_failed_continuing")

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        _run_drift_check,
        "interval",
        minutes=settings.check_interval_minutes,
        args=[settings],
        id="drift_check",
        name=f"Evidently drift check (every {settings.check_interval_minutes} min)",
        coalesce=True,
    )

    log.info(
        "drift_scheduler_starting",
        interval_minutes=settings.check_interval_minutes,
        reference_days=settings.reference_days,
        current_hours=settings.current_hours,
        features=DRIFT_FEATURES,
    )
    scheduler.start()


if __name__ == "__main__":
    main()
