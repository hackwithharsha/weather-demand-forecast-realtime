"""
Batch pipeline orchestrator: raw → staging → marts → scaler (once).

Steps
-----
1. staging   Execute versioned SQL files in sql/staging/ against raw.* Postgres
             tables.  Each file deduplicates, validates, rejects bad rows to
             staging.rejects, and aggregates clean rows into staging.demand_hourly
             / staging.weather_hourly (one Postgres round-trip per file).

2. marts     Execute sql/marts/001_city_hour_features.sql (window functions,
             cyclical encoding, weather join), then Python-fills is_holiday via
             a bulk UPDATE, then asserts row counts and null rates.

3. pipeline  On the very first run (no artifact in S3), fit the feature-
             engineering Pipeline (imputer + scaler + OHE) on the mart table
             and save it to MinIO.  Every subsequent run skips this step.

Pipeline re-fit
---------------
To force a re-fit after a distribution shift (e.g. new cities, seasonality
change), delete the S3 artifact and run the pipeline once manually:

    make worker-pipeline

or inside the container:

    python -m app.pipeline
"""

from __future__ import annotations

import pandas as pd
import psycopg2
import structlog
from botocore.exceptions import ClientError

from common.s3 import make_s3_client

from .features.pipeline import ALL_FEATURE_COLS, fit_pipeline, save_pipeline
from .marts import run_marts
from .settings import Settings
from .staging import run_staging

log = structlog.get_logger()


def run_pipeline(settings: Settings) -> None:
    """
    Execute the full batch pipeline.

    Errors are logged and re-raised.  When called from APScheduler, the
    exception is caught by the scheduler so subsequent jobs still fire.
    """
    log.info("pipeline_run_started")
    try:
        run_staging(settings)
        run_marts(settings)
        _maybe_fit_features_pipeline(settings)
        log.info("pipeline_run_completed")
    except Exception:
        log.exception("pipeline_run_failed")
        raise


# ---------------------------------------------------------------------------
# Feature pipeline bootstrap (first-run only)
# ---------------------------------------------------------------------------

def _maybe_fit_features_pipeline(settings: Settings) -> None:
    """Fit the feature Pipeline on the mart table and persist to MinIO.

    Skips if the artifact already exists — the S3 object acts as a
    distributed lock so the fitted statistics never drift on incremental runs.

    To force a re-fit after a distribution shift, delete the artifact and
    run once manually (``make worker-pipeline``).
    """
    s3     = make_s3_client(settings)
    bucket = settings.lake_bucket
    key    = settings.features_pipeline_s3_key

    try:
        s3.head_object(Bucket=bucket, Key=key)
        log.debug("features_pipeline_artifact_exists_skipping", key=key)
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise   # unexpected error (permissions, network) — propagate

    log.info("features_pipeline_fitting_started", bucket=bucket, key=key)

    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        df = pd.read_sql(
            f"SELECT {', '.join(ALL_FEATURE_COLS)} FROM marts.city_hour_features",
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        log.warning("features_pipeline_skipped", reason="marts.city_hour_features is empty")
        return

    fitted = fit_pipeline(df)
    save_pipeline(fitted, s3, bucket, key)
    log.info("features_pipeline_artifact_saved", key=key, n_rows=len(df))


# ---------------------------------------------------------------------------
# Manual entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from common.log import configure_logging
    from .settings import Settings as _Settings

    _s = _Settings()
    configure_logging(_s.log_level)
    run_pipeline(_s)
