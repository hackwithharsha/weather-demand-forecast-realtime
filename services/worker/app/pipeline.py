"""
Batch pipeline orchestrator: raw → staging → marts → scaler (once).

Steps
-----
1. staging   Read from the Parquet lake (MinIO) via DuckDB, aggregate by
             business hour, and upsert into staging.demand_hourly and
             staging.weather_hourly.

2. marts     Read from staging, compute lag, rolling, cyclical, weather,
             and holiday features, then upsert into marts.city_hour_features.

3. scaler    On the very first run (no artifact in S3), fit a StandardScaler
             on the mart table and save it to MinIO.  Every subsequent run
             skips this step — the scaler is never re-fit automatically.

Scaler re-fit
-------------
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

from .marts import run_marts
from .scaler import SCALE_COLS, ScalerTrainer
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
        _maybe_fit_scaler(settings)
        log.info("pipeline_run_completed")
    except Exception:
        log.exception("pipeline_run_failed")
        raise


# ---------------------------------------------------------------------------
# Scaler bootstrap (first-run only)
# ---------------------------------------------------------------------------

def _maybe_fit_scaler(settings: Settings) -> None:
    """
    Fit a StandardScaler on the mart table and persist to MinIO — but only
    if the artifact does not already exist.

    The S3 object acts as a distributed lock: once present, the scaler is
    never replaced by normal pipeline runs.  This guarantees that the scaler
    statistics always reflect the original training distribution rather than
    drifting silently with each incremental run.
    """
    s3 = make_s3_client(settings)
    bucket = settings.lake_bucket
    key    = settings.scaler_s3_key

    try:
        s3.head_object(Bucket=bucket, Key=key)
        log.debug("scaler_artifact_exists_skipping", key=key)
        return
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in ("404", "NoSuchKey"):
            raise   # unexpected error (permissions, network) — propagate

    # Artifact absent → fit on the current mart table.
    log.info("scaler_fitting_started", bucket=bucket, key=key)

    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        features_df = pd.read_sql(
            f"SELECT {', '.join(SCALE_COLS)} FROM marts.city_hour_features",
            conn,
        )
    finally:
        conn.close()

    if features_df.empty:
        log.warning("scaler_skipped", reason="marts.city_hour_features is empty")
        return

    trainer = ScalerTrainer()
    fitted  = trainer.fit(features_df, feature_cols=SCALE_COLS)
    trainer.save(fitted, s3, bucket, key)

    log.info(
        "scaler_saved",
        key=key,
        n_train_rows=len(features_df.dropna()),
        means={k: round(float(v), 4) for k, v in fitted.feature_means().items()},
    )


# ---------------------------------------------------------------------------
# Manual entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from common.log import configure_logging
    from .settings import Settings as _Settings

    _s = _Settings()
    configure_logging(_s.log_level)
    run_pipeline(_s)
