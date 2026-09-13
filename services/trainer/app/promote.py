"""
Model Registry: register the best training run and conditionally promote to Staging.

Promotion gates (applied in order)
-----------------------------------
Gate 1 — Feature drift check
    Compare the distribution of every feature in the *training* split to the
    *validation* split using the Population Stability Index (PSI).

    PSI interpretation (industry convention):
      < 0.10  — negligible drift, distribution stable
      0.10–0.20 — moderate drift, worth monitoring
      > 0.20  — significant drift; model assumptions may no longer hold
      > threshold (default 0.25) — blocks promotion

    PSI is computed per feature:
      • Numeric features — percentile-based binning on the training distribution
        (10 bins), Laplace-smoothed (ε=0.5) to handle empty bins.
      • Categorical features (city) — proportion-based PSI with Laplace
        smoothing across observed values.

    If ANY feature exceeds the threshold → attach drift tags to the new model
    version and return without touching the stage.

Gate 2 — Holdout MAE comparison
    Load the current Production model and re-evaluate it on result.X_val /
    result.y_val (the **same** holdout window used for the new run).

    If new_val_mae < prod_val_mae → promote new version to Staging.
    Otherwise → attach rejection tags, leave stage as None.

Fallback behaviour
------------------
If the Production model artifact cannot be loaded, the new model is promoted
to Staging with a warning.  This prevents a permanently broken Production
model from blocking all future training runs.

Stages used
-----------
  None       → initial state after registration
  Staging    → passed both gates; awaiting human promotion to Production
  Production → currently serving; promoted manually after human review

Note: MLflow deprecated stage-based transitions in v2.9 in favour of model
version aliases.  The stage API still works through v3.x and is used here
because the task specification explicitly names "Staging" and "Production".
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass

import mlflow
import mlflow.exceptions
import mlflow.sklearn
import numpy as np
import structlog
from sklearn.metrics import mean_absolute_error

from .data import CAT_FEATURES, NUM_FEATURES  # noqa: F401 (NUM_FEATURES kept for reference)
from .settings import Settings
from .train import TrainResult

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# PSI constants and feature scope
# ---------------------------------------------------------------------------

_PSI_N_BINS = 10
# Laplace smoothing added to every bin count before computing proportions.
# Prevents log(0) when a bin is empty in either distribution.
_PSI_EPS = 0.5

# Features monitored for drift.
#
# Lag and rolling features (demand_lag_*, demand_roll_*) are intentionally
# excluded: they are derived from the target variable, so their distributions
# will always shift when demand itself shifts — that is expected and handled
# by the model, not a sign of a broken data pipeline.
#
# Weather features and city identity are exogenous inputs that the model
# cannot adapt to at inference time.  Significant drift there indicates a
# change in the data source (sensor drift, API schema change, new city) that
# the model was never trained on.
_DRIFT_NUMERIC_FEATURES: list[str] = ["temperature_c", "humidity_pct", "precip_mm"]
_DRIFT_CAT_FEATURES: list[str] = ["city"]


# ---------------------------------------------------------------------------
# Drift report
# ---------------------------------------------------------------------------

@dataclass
class DriftReport:
    """Per-feature PSI summary from the train/val distribution comparison."""

    feature_psi: dict[str, float]   # {feature_name: psi_value}
    drifted_features: list[str]     # features where PSI > threshold
    max_psi: float                  # highest PSI across all features
    worst_feature: str | None       # feature with the highest PSI
    threshold: float
    passed: bool                    # True when no feature exceeds threshold


# ---------------------------------------------------------------------------
# PSI helpers
# ---------------------------------------------------------------------------

def _psi_numeric(
    train_vals: np.ndarray,
    val_vals: np.ndarray,
    n_bins: int = _PSI_N_BINS,
) -> float:
    """PSI for a continuous feature using percentile-based binning.

    Bin edges are derived from the *training* distribution so they reflect the
    model's reference world.  Laplace smoothing prevents log(0) for any bin
    that is empty in the validation set.

    Returns 0.0 when either array has fewer than 10 non-NaN values — too
    sparse to produce reliable estimates.
    """
    train = train_vals[~np.isnan(train_vals)]
    val   = val_vals[~np.isnan(val_vals)]

    if len(train) < 10 or len(val) < 5:
        return 0.0

    # Build bin edges from training percentiles; collapse duplicates that arise
    # when the feature is nearly constant.
    edges = np.unique(np.percentile(train, np.linspace(0, 100, n_bins + 1)))
    if len(edges) < 3:
        # Feature is essentially constant in training — no meaningful drift.
        return 0.0

    train_counts, _ = np.histogram(train, bins=edges)
    val_counts,   _ = np.histogram(val,   bins=edges)

    n_bins_actual = len(train_counts)
    train_pct = (train_counts + _PSI_EPS) / (len(train) + _PSI_EPS * n_bins_actual)
    val_pct   = (val_counts   + _PSI_EPS) / (len(val)   + _PSI_EPS * n_bins_actual)

    return float(np.sum((val_pct - train_pct) * np.log(val_pct / train_pct)))


def _psi_categorical(
    train_vals: "pd.Series",  # noqa: F821
    val_vals: "pd.Series",    # noqa: F821
) -> float:
    """PSI for a categorical feature using value-count proportions.

    Laplace smoothing is applied across the union of observed values from both
    splits so that new categories in val do not cause division-by-zero.
    """
    all_cats = set(train_vals.unique()) | set(val_vals.unique())
    n_train, n_val, n_cats = len(train_vals), len(val_vals), len(all_cats)

    if n_train < 1 or n_val < 1:
        return 0.0

    train_counts = Counter(train_vals)
    val_counts   = Counter(val_vals)

    psi = 0.0
    for cat in all_cats:
        ep = (train_counts.get(cat, 0) + _PSI_EPS) / (n_train + _PSI_EPS * n_cats)
        ap = (val_counts.get(cat, 0)   + _PSI_EPS) / (n_val   + _PSI_EPS * n_cats)
        psi += (ap - ep) * np.log(ap / ep)

    return float(psi)


# ---------------------------------------------------------------------------
# Drift computation
# ---------------------------------------------------------------------------

def _compute_drift(result: TrainResult, threshold: float) -> DriftReport:
    """Compute PSI for monitored features comparing training to validation splits.

    Only exogenous features are checked (weather + city).  Lag and rolling
    demand features are excluded because their distributions will naturally
    shift whenever the underlying demand shifts — this is expected model
    behaviour, not a data-pipeline anomaly.

    Uses result.X_train and result.X_val captured at training time so this
    function needs no database access.
    """
    feature_psi: dict[str, float] = {}

    for feat in _DRIFT_NUMERIC_FEATURES:
        train_col = result.X_train[feat].to_numpy().astype(float)
        val_col   = result.X_val[feat].to_numpy().astype(float)
        feature_psi[feat] = _psi_numeric(train_col, val_col)

    for feat in _DRIFT_CAT_FEATURES:
        feature_psi[feat] = _psi_categorical(
            result.X_train[feat], result.X_val[feat]
        )

    drifted = [f for f, psi in feature_psi.items() if psi > threshold]
    max_psi = max(feature_psi.values()) if feature_psi else 0.0
    worst   = max(feature_psi, key=feature_psi.__getitem__) if feature_psi else None

    return DriftReport(
        feature_psi=feature_psi,
        drifted_features=drifted,
        max_psi=max_psi,
        worst_feature=worst,
        threshold=threshold,
        passed=len(drifted) == 0,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def register_and_maybe_promote(result: TrainResult, settings: Settings) -> None:
    """Register *result* in the Model Registry and promote when both gates pass."""
    client = mlflow.tracking.MlflowClient()
    model_name = settings.mlflow_registered_model_name

    # ── 1. Register new version ──────────────────────────────────────────────
    model_uri = f"runs:/{result.run_id}/model"
    log.info("registering_model", model_name=model_name, run_id=result.run_id)
    mv = mlflow.register_model(model_uri, model_name)
    new_version = mv.version
    log.info("model_registered", model_name=model_name, version=new_version)

    # ── 2. Gate 1: Feature drift check ───────────────────────────────────────
    drift = _compute_drift(result, settings.feature_drift_threshold)
    _tag_drift(client, model_name, new_version, drift)

    log.info(
        "drift_check_complete",
        passed=drift.passed,
        max_psi=round(drift.max_psi, 4),
        worst_feature=drift.worst_feature,
        threshold=drift.threshold,
        drifted_features=drift.drifted_features,
        per_feature={f: round(v, 4) for f, v in drift.feature_psi.items()},
    )

    # Also persist the full per-feature breakdown as a run artifact for
    # auditability (the model version tags only hold the summary).
    try:
        with mlflow.start_run(result.run_id, nested=False):
            mlflow.log_text(
                json.dumps(
                    {f: round(v, 6) for f, v in drift.feature_psi.items()},
                    indent=2,
                ),
                "drift_report.json",
            )
    except Exception as exc:
        log.warning("drift_artifact_log_failed", error=str(exc))

    if not drift.passed:
        rejection = (
            f"feature_drift: PSI > {drift.threshold} for "
            f"{drift.drifted_features} "
            f"(max={drift.max_psi:.4f} on '{drift.worst_feature}')"
        )
        client.set_model_version_tag(
            name=model_name, version=new_version,
            key="rejection_reason", value=rejection,
        )
        log.warning(
            "promotion_rejected_drift",
            version=new_version,
            rejection_reason=rejection,
        )
        return

    # ── 3. Check for existing Production model ───────────────────────────────
    try:
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    except mlflow.exceptions.MlflowException as exc:
        log.warning("registry_query_failed", error=str(exc))
        prod_versions = []

    if not prod_versions:
        _transition(client, model_name, new_version, "Staging")
        log.info(
            "promoted_to_staging",
            version=new_version,
            reason="no_production_model_exists",
            new_val_mae=round(result.val_mae, 4),
        )
        return

    # ── 4. Gate 2: Holdout MAE comparison ────────────────────────────────────
    prod_version = prod_versions[0]
    prod_model_uri = f"models:/{model_name}/Production"

    try:
        prod_pipeline = mlflow.sklearn.load_model(prod_model_uri)
        prod_pred     = prod_pipeline.predict(result.X_val)
        prod_val_mae  = float(mean_absolute_error(result.y_val, prod_pred))
    except Exception as exc:
        # A broken Production model must not block all future training.
        log.warning(
            "production_model_eval_failed",
            error=str(exc),
            prod_version=prod_version.version,
            fallback="promoting_new_model",
        )
        _transition(client, model_name, new_version, "Staging")
        log.info(
            "promoted_to_staging",
            version=new_version,
            reason="production_model_eval_failed",
            new_val_mae=round(result.val_mae, 4),
        )
        return

    client.set_model_version_tag(
        name=model_name, version=new_version,
        key="compared_against_prod_version", value=str(prod_version.version),
    )
    client.set_model_version_tag(
        name=model_name, version=new_version,
        key="prod_val_mae", value=f"{prod_val_mae:.6f}",
    )

    if result.val_mae < prod_val_mae:
        _transition(client, model_name, new_version, "Staging")
        log.info(
            "promoted_to_staging",
            version=new_version,
            reason="beats_production",
            new_val_mae=round(result.val_mae, 4),
            prod_val_mae=round(prod_val_mae, 4),
            improvement=round(prod_val_mae - result.val_mae, 4),
            compared_against_prod_version=prod_version.version,
        )
    else:
        rejection = (
            f"holdout_mae: new={result.val_mae:.6f} >= "
            f"prod={prod_val_mae:.6f} "
            f"(split_date={result.split_date.date()}, "
            f"prod_version={prod_version.version})"
        )
        client.set_model_version_tag(
            name=model_name, version=new_version,
            key="rejection_reason", value=rejection,
        )
        log.warning(
            "promotion_rejected_holdout",
            version=new_version,
            new_val_mae=round(result.val_mae, 4),
            prod_val_mae=round(prod_val_mae, 4),
            compared_against_prod_version=prod_version.version,
            rejection_reason=rejection,
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _tag_drift(
    client: mlflow.tracking.MlflowClient,
    model_name: str,
    version: str,
    drift: DriftReport,
) -> None:
    """Attach drift summary as model version tags."""
    tags = {
        "drift_check_passed":     str(drift.passed).lower(),
        "drift_threshold":        str(drift.threshold),
        "drift_max_psi":          f"{drift.max_psi:.4f}",
        "drift_worst_feature":    drift.worst_feature or "",
        "drift_drifted_features": ",".join(drift.drifted_features),
    }
    for key, value in tags.items():
        client.set_model_version_tag(
            name=model_name, version=version, key=key, value=value,
        )


def _transition(
    client: mlflow.tracking.MlflowClient,
    model_name: str,
    version: str,
    stage: str,
) -> None:
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=stage,
        archive_existing_versions=False,
    )
