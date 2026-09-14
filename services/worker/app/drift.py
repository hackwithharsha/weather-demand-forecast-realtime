"""
Evidently feature-drift check for the worker service.

Reference dataset
-----------------
X_train (all FEATURE_COLS) logged as ``reference/reference_data.parquet``
to the MLflow run that produced the current Production model.  This means
the reference always reflects the exact data the model was trained on —
not a rolling window of the warehouse that may have shifted.

Current dataset
---------------
Last ``drift_current_hours`` hours from ``marts.prediction_features`` —
the feature vectors that were actually served to end users.

Results
-------
Written to ``marts.drift_reports`` and exposed as Prometheus Gauges (shared
with the rest of the worker metrics, served on port 9101).
"""

from __future__ import annotations

import os
import tempfile
import time
from datetime import datetime, timedelta, timezone

import mlflow.artifacts
import mlflow.tracking
import pandas as pd
import psycopg2
import psycopg2.extras
import structlog

from .auto_retrain import maybe_trigger_retrain
from .metrics import (
    drift_detected_gauge,
    drift_job_current_count,
    drift_job_duration_seconds,
    drift_job_last_run_timestamp,
    drift_job_reference_count,
    drift_score_gauge,
)
from .settings import Settings

log = structlog.get_logger()

# Features compared between the MLflow reference and the serving distribution.
# Cyclic time encodings (hour_sin/cos, dow_sin/cos) are excluded: a short
# current window always looks different in time-of-day distribution vs. a
# multi-month training set — that is by design, not a pipeline anomaly.
DRIFT_FEATURES: list[str] = [
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
    "is_holiday",
]


# ---------------------------------------------------------------------------
# Reference dataset loader
# ---------------------------------------------------------------------------

def _load_reference(settings: Settings) -> pd.DataFrame | None:
    """
    Resolve the Production model in the MLflow registry, find its training
    run, and download the ``reference/reference_data.parquet`` artifact that
    was logged at training time.

    Returns None when:
    - MLflow is unreachable
    - No Production version exists in the registry
    - The run has no reference artifact (model trained before this feature)
    """
    # Point boto3 / MLflow SDK at the MinIO instance.
    os.environ["MLFLOW_TRACKING_URI"] = settings.mlflow_tracking_uri
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = settings.mlflow_s3_endpoint_url
    os.environ["AWS_ACCESS_KEY_ID"] = settings.mlflow_s3_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.mlflow_s3_secret_key

    try:
        client = mlflow.tracking.MlflowClient(
            tracking_uri=settings.mlflow_tracking_uri
        )
        versions = client.get_latest_versions(
            settings.mlflow_registered_model_name, stages=["Production"]
        )
    except Exception:
        log.warning("drift_mlflow_unavailable", tracking_uri=settings.mlflow_tracking_uri)
        return None

    if not versions:
        log.info("drift_no_production_model", model=settings.mlflow_registered_model_name)
        return None

    prod_version = versions[0]
    run_id = prod_version.run_id
    artifact_uri = f"runs:/{run_id}/reference/reference_data.parquet"

    log.info(
        "drift_fetching_reference",
        run_id=run_id,
        model_version=prod_version.version,
        artifact_uri=artifact_uri,
    )

    try:
        with tempfile.TemporaryDirectory() as tmp:
            local_path = mlflow.artifacts.download_artifacts(
                artifact_uri=artifact_uri,
                dst_path=tmp,
                tracking_uri=settings.mlflow_tracking_uri,
            )
            ref_df = pd.read_parquet(local_path)
    except Exception:
        log.warning(
            "drift_reference_artifact_missing",
            run_id=run_id,
            artifact_uri=artifact_uri,
            hint="Re-train the Production model to generate the reference artifact.",
        )
        return None

    log.info(
        "drift_reference_loaded",
        run_id=run_id,
        rows=len(ref_df),
        model_version=prod_version.version,
    )
    return ref_df


# ---------------------------------------------------------------------------
# Drift check
# ---------------------------------------------------------------------------

def run_drift_check(settings: Settings) -> None:
    """
    Full drift check: reference from MLflow, current from Postgres.

    Never raises — errors are caught and logged so the scheduler keeps firing
    on the next interval.
    """
    from evidently.metrics import ColumnDriftMetric
    from evidently.report import Report

    t0 = time.monotonic()
    checked_at = datetime.now(timezone.utc)
    log.info("drift_check_started", checked_at=checked_at.isoformat())

    # ── 1. Reference: X_train from the Production MLflow run ─────────────
    ref_df = _load_reference(settings)
    if ref_df is None:
        log.warning("drift_check_skipped", reason="no reference dataset from MLflow")
        return

    conn: psycopg2.extensions.connection | None = None
    try:
        conn = psycopg2.connect(settings.postgres_dsn)

        # ── 2. Current: recent serving features ───────────────────────────
        cur_cutoff = checked_at - timedelta(hours=settings.drift_current_hours)
        available = [f for f in DRIFT_FEATURES if f in ref_df.columns]
        if not available:
            log.warning("drift_check_skipped", reason="no overlapping columns")
            return

        cols_sql = ", ".join(available)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT {cols_sql} FROM marts.prediction_features "
                f"WHERE requested_at >= %s",
                (cur_cutoff,),
            )
            cur_rows = cur.fetchall()

        n_ref = len(ref_df)
        n_cur = len(cur_rows)
        drift_job_reference_count.set(n_ref)
        drift_job_current_count.set(n_cur)

        if n_cur < settings.drift_min_current_rows:
            log.warning(
                "drift_check_skipped",
                reason="insufficient current rows",
                current_rows=n_cur,
                min_required=settings.drift_min_current_rows,
            )
            return

        ref_subset = ref_df[available].astype(float, errors="ignore")
        cur_df = pd.DataFrame(cur_rows)[available].astype(float, errors="ignore")

        # ── 3. Evidently drift report ──────────────────────────────────────
        report = Report(
            metrics=[ColumnDriftMetric(column_name=f) for f in available]
        )
        report.run(reference_data=ref_subset, current_data=cur_df)
        report_dict = report.as_dict()

        # ── 4. Parse + persist results ────────────────────────────────────
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

            drift_score_gauge.labels(feature=feature_name).set(
                drift_score if drift_score is not None else 0.0
            )
            drift_detected_gauge.labels(feature=feature_name).set(
                1.0 if drift_det else 0.0
            )

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
            log.info(
                "drift_reports_written",
                count=len(rows_to_insert),
                checked_at=checked_at.isoformat(),
            )

            # Trigger auto-retrain when any feature exceeds the threshold.
            drift_summary = [
                {
                    "feature":  feature_name,
                    "score":    drift_score if drift_score is not None else 0.0,
                    "detected": drift_det,
                }
                for (_, feature_name, drift_score, drift_det, *_rest)
                in rows_to_insert
            ]
            maybe_trigger_retrain(settings, drift_summary)

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
