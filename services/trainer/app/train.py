"""
Train a naive baseline plus Ridge and HistGradientBoosting regressors.

Run order
---------
Run 0  NaiveLastDay      — demand_lag_24h as prediction (no fitting, no
                           artifact).  Logged first so every subsequent run
                           appears beneath it in the MLflow UI.
Run 1  Ridge             — imputed + scaled linear model.
Run 2  HistGradientBoosting — gradient-boosted trees, NaN-native.

All three runs share the same train/val split so their validation metrics
are directly comparable.  ``run_training`` returns the best *fitted* result
(lowest val_mae among Ridge and HGB); the naive baseline is never a
candidate for registration.

Time-based split guarantee
--------------------------
The validation set always consists of the chronologically latest ``val_days``
days of data.  No random shuffling is ever performed.  This ensures:

  * No future leakage into training features (lags, rolling windows).
  * The holdout is representative of the freshest data the model will see
    in production.
  * The train/val boundary is deterministic and logged to MLflow.

Model pipelines
---------------
NaiveLastDay:
    y_pred = demand_lag_24h  (yesterday's same hour)
    NaN rows → imputed with median(demand_lag_24h) from the training set.
    No sklearn Pipeline; no artifact logged.

Ridge:
    ColumnTransformer
      numeric  → SimpleImputer(median) → StandardScaler
      city     → OneHotEncoder(handle_unknown="ignore")
    Ridge(alpha=1.0)

HistGradientBoosting:
    ColumnTransformer
      numeric  → passthrough  (HGB handles NaN natively via surrogate splits)
      city     → OrdinalEncoder(handle_unknown="use_encoded_value")
    HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05)

The entire Pipeline (preprocessor + model) is serialised and logged as an
MLflow sklearn model artifact so predictions can be made by loading the
registered model without re-applying any separate preprocessing step.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import structlog
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

from .data import CAT_FEATURES, FEATURE_COLS, NUM_FEATURES, TARGET_COL, snapshot_hash
from .settings import Settings

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class TrainResult:
    """Everything the promotion step needs from a training run."""

    run_id: str
    model_type: str
    val_mae: float
    val_rmse: float
    val_mape: float
    pipeline: Pipeline           # fitted sklearn Pipeline (preprocessor + model)
    X_val: pd.DataFrame          # raw holdout features — for re-evaluating Production
    y_val: pd.Series             # holdout targets
    split_date: datetime         # first timestamp in the validation set
    feature_cols: list[str]      # ordered list of model input columns
    params: dict[str, Any]       # all params logged to MLflow


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def _mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Percentage Error; guarded against zero-denominator."""
    denom = np.maximum(np.abs(y_true), 1e-8)
    return float(np.mean(np.abs(y_true - y_pred) / denom))


# ---------------------------------------------------------------------------
# Naive baseline (run 0)
# ---------------------------------------------------------------------------

def _naive_baseline_run(
    val_df: pd.DataFrame,
    train_df: pd.DataFrame,
    shared_params: dict[str, Any],
) -> tuple[str, float, float, float]:
    """Log the 'yesterday same hour' naive baseline as MLflow run 0.

    Prediction is ``demand_lag_24h`` — the value observed exactly 24 h before
    each validation hour.  Rows where that lag is NULL (insufficient history)
    are imputed with the median of ``demand_lag_24h`` across the training set.

    No model artifact is logged; this run exists purely as a comparison floor
    so that every other run in the experiment can be evaluated relative to
    "doing nothing clever".

    Returns
    -------
    (run_id, val_mae, val_rmse, val_mape)
    """
    lag_raw = train_df["demand_lag_24h"].median()
    lag_median = float(lag_raw) if pd.notna(lag_raw) else 0.0

    y_pred = val_df["demand_lag_24h"].fillna(lag_median).to_numpy()
    y_true = val_df[TARGET_COL].to_numpy()

    val_mae  = float(mean_absolute_error(y_true, y_pred))
    val_rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    val_mape = _mape(y_true, y_pred)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    params: dict[str, Any] = {
        **shared_params,
        "model_type":  "NaiveLastDay",
        "prediction":  "demand_lag_24h; nan→train_median",
        "lag_median":  round(lag_median, 4),
    }

    with mlflow.start_run(
        run_name=f"NaiveLastDay-{ts}",
        tags={"model_type": "NaiveLastDay"},
    ) as run:
        mlflow.log_params(params)
        mlflow.log_metrics({
            "val_mae":  val_mae,
            "val_rmse": val_rmse,
            "val_mape": val_mape,
        })
        run_id = run.info.run_id

    log.info(
        "baseline_run_logged",
        model_type="NaiveLastDay",
        run_id=run_id,
        val_mae=round(val_mae, 4),
        val_rmse=round(val_rmse, 4),
        val_mape=round(val_mape, 4),
        note="lag_24h → train median for nulls",
    )
    return run_id, val_mae, val_rmse, val_mape


# ---------------------------------------------------------------------------
# Model pipeline factories
# ---------------------------------------------------------------------------

def _ridge_pipeline(seed: int) -> Pipeline:  # noqa: ARG001  (seed unused; uniform interface)
    """Ridge regression with imputation + standardisation.

    Ridge is analytically deterministic regardless of seed, but the parameter
    is accepted so every factory in ``_PIPELINE_FACTORIES`` has the same
    signature.
    """
    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUM_FEATURES),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CAT_FEATURES),
        ],
        remainder="drop",
    )
    return Pipeline([("prep", preprocessor), ("model", Ridge(alpha=1.0))])


def _hgb_pipeline(seed: int) -> Pipeline:
    """HistGradientBoosting — handles NaN natively via surrogate splits."""
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", NUM_FEATURES),
            (
                "cat",
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                CAT_FEATURES,
            ),
        ],
        remainder="drop",
    )
    return Pipeline([
        ("prep", preprocessor),
        ("model", HistGradientBoostingRegressor(
            max_iter=300,
            learning_rate=0.05,
            max_leaf_nodes=31,
            min_samples_leaf=20,
            random_state=seed,
        )),
    ])


_PIPELINE_FACTORIES: dict[str, Callable[[int], Pipeline]] = {
    "Ridge": _ridge_pipeline,
    "HistGradientBoosting": _hgb_pipeline,
}


# ---------------------------------------------------------------------------
# Time-based split
# ---------------------------------------------------------------------------

def _time_split(
    df: pd.DataFrame, val_days: int
) -> tuple[pd.DataFrame, pd.DataFrame, datetime]:
    """Chronological split.

    Returns (train_df, val_df, split_date) where *split_date* is the first
    timestamp in the validation window.  train_df contains all rows strictly
    before split_date; val_df contains the rest.
    """
    split_ts = df["hour_ts"].max() - pd.Timedelta(days=val_days)
    train = df[df["hour_ts"] < split_ts].copy()
    val   = df[df["hour_ts"] >= split_ts].copy()
    split_date_dt = val["hour_ts"].min().to_pydatetime()
    return train, val, split_date_dt


# ---------------------------------------------------------------------------
# Single-model training loop
# ---------------------------------------------------------------------------

def _train_one(
    model_type: str,
    pipeline: Pipeline,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    shared_params: dict[str, Any],
    settings: Settings,
) -> TrainResult:
    """Fit one model, log everything to MLflow, return a TrainResult."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    params: dict[str, Any] = {
        **shared_params,
        "model_type":    model_type,
        "num_features":  len(NUM_FEATURES),
        "cat_features":  len(CAT_FEATURES),
        "feature_list":  json.dumps(FEATURE_COLS),
    }

    with mlflow.start_run(
        run_name=f"{model_type}-{ts}",
        tags={"model_type": model_type},
    ) as run:
        mlflow.log_params(params)

        # ── Fit ──────────────────────────────────────────────────────────
        pipeline.fit(X_train[FEATURE_COLS], y_train)

        # ── Evaluate ─────────────────────────────────────────────────────
        y_pred   = pipeline.predict(X_val[FEATURE_COLS])
        val_mae  = float(mean_absolute_error(y_val, y_pred))
        val_rmse = float(np.sqrt(mean_squared_error(y_val, y_pred)))
        val_mape = _mape(y_val.to_numpy(), y_pred)

        mlflow.log_metrics({
            "val_mae":  val_mae,
            "val_rmse": val_rmse,
            "val_mape": val_mape,
        })

        # ── Log fitted pipeline as MLflow sklearn model ───────────────────
        signature = infer_signature(
            X_val[FEATURE_COLS].head(10),
            pipeline.predict(X_val[FEATURE_COLS].head(10)),
        )
        mlflow.sklearn.log_model(
            pipeline,
            artifact_path="model",
            signature=signature,
            input_example=X_val[FEATURE_COLS].head(5),
            serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_PICKLE,
        )

        # ── Log feature list as standalone artifact ───────────────────────
        mlflow.log_text(json.dumps(FEATURE_COLS, indent=2), "feature_list.json")

        log.info(
            "model_trained",
            model_type=model_type,
            run_id=run.info.run_id,
            val_mae=round(val_mae, 4),
            val_rmse=round(val_rmse, 4),
            val_mape=round(val_mape, 4),
            train_rows=len(X_train),
            val_rows=len(X_val),
        )

        return TrainResult(
            run_id=run.info.run_id,
            model_type=model_type,
            val_mae=val_mae,
            val_rmse=val_rmse,
            val_mape=val_mape,
            pipeline=pipeline,
            X_val=X_val[FEATURE_COLS].copy(),
            y_val=y_val.copy(),
            split_date=shared_params["split_date_dt"],
            feature_cols=FEATURE_COLS,
            params=params,
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_training(df: pd.DataFrame, settings: Settings) -> TrainResult:
    """
    Train all registered model types on *df*, log each to MLflow.

    Returns the best ``TrainResult`` (lowest val_mae).

    Raises
    ------
    ValueError
        When the dataset is too small to produce a non-empty split.
    """
    train_df, val_df, split_date_dt = _time_split(df, settings.val_days)

    if len(train_df) == 0 or len(val_df) == 0:
        raise ValueError(
            f"Insufficient data for a {settings.val_days}-day time split "
            f"(train={len(train_df)} rows, val={len(val_df)} rows). "
            f"Increase training_lookback_days (currently {settings.training_lookback_days})."
        )

    data_hash = snapshot_hash(df)
    feature_list_hash = hashlib.sha256(
        json.dumps(FEATURE_COLS).encode()
    ).hexdigest()[:16]

    shared_params: dict[str, Any] = {
        "lookback_days":        settings.training_lookback_days,
        "val_days":             settings.val_days,
        "train_rows":           len(train_df),
        "val_rows":             len(val_df),
        "split_date":           split_date_dt.isoformat(),
        "split_date_dt":        split_date_dt,     # removed before log_params
        "data_snapshot_hash":   data_hash,
        "num_cities":           int(df["city"].nunique()),
        # --- reproducibility fingerprint ---
        "random_seed":          settings.random_seed,
        "git_sha":              settings.git_sha,
        "feature_list_hash":    feature_list_hash,
    }

    log.info(
        "training_started",
        train_rows=len(train_df),
        val_rows=len(val_df),
        split_date=split_date_dt.isoformat(),
        data_snapshot_hash=data_hash,
        models=["NaiveLastDay"] + list(_PIPELINE_FACTORIES.keys()),
    )

    X_train, y_train = train_df[FEATURE_COLS], train_df[TARGET_COL]
    X_val,   y_val   = val_df[FEATURE_COLS],   val_df[TARGET_COL]

    # Remove non-serialisable helper key before passing to MLflow log_params.
    loggable_shared = {k: v for k, v in shared_params.items() if k != "split_date_dt"}

    # ── Run 0: naive baseline ─────────────────────────────────────────────
    _baseline_run_id, baseline_mae, _baseline_rmse, _baseline_mape = (
        _naive_baseline_run(val_df, train_df, loggable_shared)
    )

    # ── Runs 1-N: fitted models ───────────────────────────────────────────
    results: list[TrainResult] = []
    for model_type, factory in _PIPELINE_FACTORIES.items():
        result = _train_one(
            model_type,
            factory(settings.random_seed),
            X_train, y_train,
            X_val,   y_val,
            {**loggable_shared, "split_date_dt": split_date_dt},
            settings,
        )
        results.append(result)

    best = min(results, key=lambda r: r.val_mae)
    log.info(
        "best_model_selected",
        model_type=best.model_type,
        run_id=best.run_id,
        val_mae=round(best.val_mae, 4),
        baseline_mae=round(baseline_mae, 4),
        improvement_vs_baseline=round(baseline_mae - best.val_mae, 4),
        runner_up={r.model_type: round(r.val_mae, 4) for r in results if r is not best},
    )
    return best
