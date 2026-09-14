"""
Automated model retraining for the worker service.

Trigger conditions
------------------
drift     When any Evidently drift score from the latest check exceeds
          settings.retrain_drift_threshold, a training run is kicked off
          immediately.  This prevents the model from silently serving stale
          predictions after a distribution shift.

weekly    A weekly APScheduler job fires at the configured day/hour (default:
          Sunday 04:00 UTC) regardless of drift state.  This keeps the model
          fresh even when no explicit drift is detected.

Training pipeline (inline, mirrors services/trainer)
------------------------------------------------------
1. Load features from marts.city_hour_features (last retrain_lookback_days).
2. Time-based train / validation split (last retrain_val_days withheld).
3. Fit HistGradientBoosting pipeline (NaN-native; consistently better than
   Ridge on this dataset).
4. Log run to MLflow.
5. Register model version.
6. Gate: compare holdout MAE against the current Production model.
   - Beats production (or no production exists) → promote to Staging.
   - Otherwise → attach rejection tags; leave stage as None.

The MLflow experiment and model name match the trainer service so all runs
appear in the same experiment view.

Cooldown
--------
A module-level monotonic timestamp records when the last retrain was
*started* (under the lock, so both drift and weekly runs update it).
``maybe_trigger_retrain`` compares the elapsed time against
``settings.retrain_cooldown_minutes`` (default 360 min / 6 h) before
spawning a thread.  This stops a sustained-drift event from re-launching
training on every drift-check cycle while still allowing the weekly
maintenance run, which is governed by its own cron schedule.

Idempotency / safety
---------------------
- The function is wrapped in a broad try/except so scheduler failures never
  crash the worker process.
- A module-level threading.Lock prevents concurrent retrains when both the
  drift trigger and the weekly trigger fire close together.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import psycopg2
import structlog
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from .settings import Settings

log = structlog.get_logger()

# Prevents simultaneous retrains from drift trigger + weekly job.
_retrain_lock = threading.Lock()

# Monotonic timestamp of when the last retrain was *started*.
# Written under _retrain_lock; read lock-free in maybe_trigger_retrain for a
# fast-path rejection (a concurrent run would fail at lock.acquire anyway).
_last_retrain_triggered_at: float | None = None

# ---------------------------------------------------------------------------
# Feature schema (mirrors trainer/app/data.py)
# ---------------------------------------------------------------------------

_CAT_FEATURES: list[str] = ["city"]

_NUM_FEATURES: list[str] = [
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
    "is_holiday",
]

_FEATURE_COLS: list[str] = _CAT_FEATURES + _NUM_FEATURES
_TARGET_COL:   str        = "total_demand"

_LOAD_QUERY = """
SELECT
    city, hour_ts, total_demand,
    event_count,
    demand_lag_1h, demand_lag_24h, demand_lag_168h,
    demand_roll_3h, demand_roll_24h,
    hour_sin, hour_cos, dow_sin, dow_cos,
    temperature_c, humidity_pct, precip_mm,
    is_holiday::INT AS is_holiday
FROM marts.city_hour_features
WHERE hour_ts >= %s
  AND total_demand IS NOT NULL
ORDER BY hour_ts, city
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_mlflow_env(settings: Settings) -> None:
    os.environ["MLFLOW_TRACKING_URI"]     = settings.mlflow_tracking_uri
    os.environ["MLFLOW_S3_ENDPOINT_URL"]  = settings.mlflow_s3_endpoint_url
    os.environ["AWS_ACCESS_KEY_ID"]       = settings.mlflow_s3_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"]   = settings.mlflow_s3_secret_key


def _load_features(settings: Settings) -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(
        days=settings.retrain_lookback_days
    )
    with psycopg2.connect(settings.postgres_dsn) as conn:
        df = pd.read_sql(_LOAD_QUERY, conn, params=(cutoff,), parse_dates=["hour_ts"])
    log.info(
        "retrain_features_loaded",
        rows=len(df),
        cities=int(df["city"].nunique()) if not df.empty else 0,
        lookback_days=settings.retrain_lookback_days,
    )
    return df


def _time_split(
    df: pd.DataFrame, val_days: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    split_ts = df["hour_ts"].max() - pd.Timedelta(days=val_days)
    return (
        df[df["hour_ts"] < split_ts].copy(),
        df[df["hour_ts"] >= split_ts].copy(),
    )


def _build_pipeline() -> Pipeline:
    """HistGradientBoosting with OrdinalEncoder for the city column."""
    prep = ColumnTransformer(
        transformers=[
            ("num", "passthrough", _NUM_FEATURES),
            (
                "cat",
                OrdinalEncoder(
                    handle_unknown="use_encoded_value", unknown_value=-1
                ),
                _CAT_FEATURES,
            ),
        ],
        remainder="drop",
    )
    return Pipeline([
        ("prep", prep),
        ("model", HistGradientBoostingRegressor(
            max_iter=300,
            learning_rate=0.05,
            max_leaf_nodes=31,
            min_samples_leaf=20,
            random_state=42,
        )),
    ])


# ---------------------------------------------------------------------------
# MLflow run + registration
# ---------------------------------------------------------------------------


def _train_and_register(
    settings: Settings,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    trigger: str,
) -> tuple[str, float]:
    """
    Fit HGB on train_df, evaluate on val_df, log to MLflow, register.

    Returns (run_id, val_mae).
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment_name)

    X_train = train_df[_FEATURE_COLS]
    y_train = train_df[_TARGET_COL]
    X_val   = val_df[_FEATURE_COLS]
    y_val   = val_df[_TARGET_COL]

    pipeline = _build_pipeline()
    ts       = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    params: dict[str, Any] = {
        "model_type":        "HistGradientBoosting",
        "trigger":           trigger,
        "train_rows":        len(train_df),
        "val_rows":          len(val_df),
        "lookback_days":     settings.retrain_lookback_days,
        "val_days":          settings.retrain_val_days,
        "feature_list":      json.dumps(_FEATURE_COLS),
        "auto_retrain":      "true",
    }

    with mlflow.start_run(run_name=f"AutoHGB-{trigger}-{ts}",
                          tags={"model_type": "HistGradientBoosting",
                                "auto_retrain": "true",
                                "trigger": trigger}) as run:
        mlflow.log_params(params)
        pipeline.fit(X_train, y_train)

        y_pred   = pipeline.predict(X_val)
        val_mae  = float(mean_absolute_error(y_val, y_pred))
        val_rmse = float(np.sqrt(np.mean((y_val.to_numpy() - y_pred) ** 2)))
        mlflow.log_metrics({"val_mae": val_mae, "val_rmse": val_rmse})

        mlflow.sklearn.log_model(
            pipeline,
            artifact_path="model",
            serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_PICKLE,
        )

        # Log X_train as reference artifact for the drift job.
        with tempfile.TemporaryDirectory() as tmp:
            ref_path = os.path.join(tmp, "reference_data.parquet")
            X_train.to_parquet(ref_path, index=False)
            mlflow.log_artifact(ref_path, artifact_path="reference")

        run_id = run.info.run_id

    log.info(
        "retrain_run_logged",
        run_id=run_id,
        val_mae=round(val_mae, 4),
        val_rmse=round(val_rmse, 4),
        trigger=trigger,
    )
    return run_id, val_mae


def _maybe_promote(
    settings: Settings,
    run_id: str,
    new_val_mae: float,
    X_val: pd.DataFrame,
    y_val: pd.Series,
) -> None:
    """Register the new run and promote to Staging if it beats Production."""
    client     = mlflow.tracking.MlflowClient()
    model_name = settings.mlflow_registered_model_name
    model_uri  = f"runs:/{run_id}/model"

    mv          = mlflow.register_model(model_uri, model_name)
    new_version = mv.version
    log.info("retrain_model_registered", model_name=model_name, version=new_version)

    # Check for existing Production model.
    try:
        prod_versions = client.get_latest_versions(
            model_name, stages=["Production"]
        )
    except mlflow.exceptions.MlflowException as exc:
        log.warning("retrain_registry_query_failed", error=str(exc))
        prod_versions = []

    if not prod_versions:
        client.transition_model_version_stage(
            name=model_name, version=new_version,
            stage="Staging", archive_existing_versions=False,
        )
        log.info("retrain_promoted_staging",
                 version=new_version,
                 reason="no_production_model",
                 val_mae=round(new_val_mae, 4))
        return

    # Gate: holdout MAE comparison against Production.
    prod_version  = prod_versions[0]
    prod_model_uri = f"models:/{model_name}/Production"
    try:
        prod_pipeline = mlflow.sklearn.load_model(prod_model_uri)
        prod_pred     = prod_pipeline.predict(X_val)
        prod_val_mae  = float(mean_absolute_error(y_val, prod_pred))
    except Exception as exc:
        # Broken Production model should not block future retraining.
        log.warning(
            "retrain_prod_eval_failed",
            error=str(exc),
            fallback="promoting_anyway",
        )
        client.transition_model_version_stage(
            name=model_name, version=new_version,
            stage="Staging", archive_existing_versions=False,
        )
        log.info("retrain_promoted_staging",
                 version=new_version,
                 reason="prod_eval_failed",
                 val_mae=round(new_val_mae, 4))
        return

    client.set_model_version_tag(
        name=model_name, version=new_version,
        key="prod_val_mae", value=f"{prod_val_mae:.6f}",
    )
    client.set_model_version_tag(
        name=model_name, version=new_version,
        key="compared_against_prod_version", value=str(prod_version.version),
    )

    if new_val_mae < prod_val_mae:
        client.transition_model_version_stage(
            name=model_name, version=new_version,
            stage="Staging", archive_existing_versions=False,
        )
        log.info(
            "retrain_promoted_staging",
            version=new_version,
            reason="beats_production",
            new_val_mae=round(new_val_mae, 4),
            prod_val_mae=round(prod_val_mae, 4),
            improvement=round(prod_val_mae - new_val_mae, 4),
        )
    else:
        rejection = (
            f"holdout_mae: new={new_val_mae:.6f} >= prod={prod_val_mae:.6f}"
        )
        client.set_model_version_tag(
            name=model_name, version=new_version,
            key="rejection_reason", value=rejection,
        )
        log.warning(
            "retrain_rejected",
            version=new_version,
            new_val_mae=round(new_val_mae, 4),
            prod_val_mae=round(prod_val_mae, 4),
        )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run_auto_retrain(settings: Settings, trigger: str = "weekly") -> None:
    """
    Run a full auto-retrain cycle.

    Acquires _retrain_lock so concurrent drift + weekly triggers do not
    start two simultaneous training runs.  Skips silently when the lock
    is held.

    Also updates _last_retrain_triggered_at under the lock so the cooldown
    clock is reset regardless of whether the trigger was drift or weekly.
    """
    global _last_retrain_triggered_at

    if not _retrain_lock.acquire(blocking=False):
        log.info("retrain_skipped", reason="retrain_already_in_progress")
        return

    # Stamp the trigger time now, while we hold the lock, so any concurrent
    # drift check that reads it gets an accurate value.
    _last_retrain_triggered_at = time.monotonic()

    t0 = time.monotonic()
    log.info("retrain_started", trigger=trigger)
    try:
        _set_mlflow_env(settings)
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)

        df = _load_features(settings)
        if len(df) < 100:
            log.warning(
                "retrain_skipped",
                reason="insufficient_data",
                rows=len(df),
            )
            return

        train_df, val_df = _time_split(df, settings.retrain_val_days)
        if len(train_df) == 0 or len(val_df) == 0:
            log.warning(
                "retrain_skipped",
                reason="empty_split",
                train_rows=len(train_df),
                val_rows=len(val_df),
            )
            return

        run_id, val_mae = _train_and_register(settings, train_df, val_df, trigger)
        _maybe_promote(
            settings,
            run_id,
            val_mae,
            val_df[_FEATURE_COLS],
            val_df[_TARGET_COL],
        )

    except Exception:
        log.exception("retrain_failed", trigger=trigger)
    finally:
        _retrain_lock.release()
        elapsed = time.monotonic() - t0
        log.info("retrain_finished", trigger=trigger, duration_s=round(elapsed, 1))


def maybe_trigger_retrain(
    settings: Settings,
    drift_scores: list[dict],
) -> None:
    """
    Called by drift.py after a completed drift check.

    Triggers a background retrain when:
    - settings.auto_retrain_on_drift is True, AND
    - any score in drift_scores exceeds settings.retrain_drift_threshold.

    The retrain runs in a daemon thread so it does not block the scheduler.
    """
    if not settings.auto_retrain_on_drift:
        return

    exceeding = [
        s["feature"]
        for s in drift_scores
        if s.get("score", 0.0) > settings.retrain_drift_threshold
        or s.get("detected", False)
    ]
    if not exceeding:
        return

    # Cooldown guard — prevent thrashing when drift stays elevated across
    # consecutive check cycles.  The timestamp is set under _retrain_lock
    # by run_auto_retrain, so this read is a best-effort fast-path rejection;
    # any genuine race is caught by _retrain_lock.acquire(blocking=False).
    if _last_retrain_triggered_at is not None:
        elapsed_s = time.monotonic() - _last_retrain_triggered_at
        cooldown_s = settings.retrain_cooldown_minutes * 60
        if elapsed_s < cooldown_s:
            remaining_m = int((cooldown_s - elapsed_s) / 60)
            log.info(
                "retrain_cooldown_active",
                trigger="drift",
                features=exceeding,
                cooldown_minutes=settings.retrain_cooldown_minutes,
                remaining_minutes=remaining_m,
            )
            return

    log.info(
        "retrain_drift_trigger",
        features=exceeding,
        threshold=settings.retrain_drift_threshold,
    )
    t = threading.Thread(
        target=run_auto_retrain,
        args=[settings],
        kwargs={"trigger": "drift"},
        name="auto-retrain",
        daemon=True,
    )
    t.start()
