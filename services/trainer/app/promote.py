"""
Model Registry: register the best training run and conditionally promote to Staging.

Promotion logic
---------------
1.  Register the new model version from the best training run.

2.  If no Production model exists → promote to Staging unconditionally.
    (There is no baseline to beat; human review is required before promoting
    to Production.)

3.  If a Production model exists → load it and re-evaluate on the **same
    holdout** used in the current training run (result.X_val / result.y_val).
    This gives an apples-to-apples MAE comparison on identical data.

    a.  If new_val_mae < prod_val_mae → promote to Staging.
    b.  Otherwise → register the version but do NOT promote.  Attach
        ``rejection_reason`` and ``compared_against_prod_version`` tags to
        the new version for audit traceability.

Fallback behaviour
------------------
If the Production model artifact cannot be loaded (e.g., feature mismatch,
corrupted artifact), the new model is promoted to Staging with a warning
logged.  This prevents a broken Production model from blocking all future
training runs.

Stages used
-----------
  None     → initial state after registration
  Staging  → validated by this job; awaiting human promotion to Production
  Production → currently serving; set manually after human review

Note: MLflow deprecated stage-based transitions in v2.9 in favour of model
version aliases.  The stage API still works in all 2.x releases and is used
here because the task specification explicitly names "Staging" and
"Production".
"""

from __future__ import annotations

import mlflow
import mlflow.exceptions
import mlflow.sklearn
import structlog
from sklearn.metrics import mean_absolute_error

from .settings import Settings
from .train import TrainResult

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def register_and_maybe_promote(result: TrainResult, settings: Settings) -> None:
    """Register *result* and promote to Staging when it beats Production."""
    client = mlflow.tracking.MlflowClient()
    model_name = settings.mlflow_registered_model_name

    # ── 1. Register new version ──────────────────────────────────────────────
    model_uri = f"runs:/{result.run_id}/model"
    log.info("registering_model", model_name=model_name, run_id=result.run_id)
    mv = mlflow.register_model(model_uri, model_name)
    new_version = mv.version
    log.info("model_registered", model_name=model_name, version=new_version)

    # ── 2. Check for existing Production model ───────────────────────────────
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

    # ── 3. Re-evaluate Production model on the same holdout ──────────────────
    prod_version = prod_versions[0]
    prod_model_uri = f"models:/{model_name}/Production"

    try:
        prod_pipeline = mlflow.sklearn.load_model(prod_model_uri)
        prod_pred = prod_pipeline.predict(result.X_val)
        prod_val_mae = float(mean_absolute_error(result.y_val, prod_pred))
    except Exception as exc:
        # Treat a broken Production model as "no baseline" and promote anyway.
        log.warning(
            "production_model_eval_failed",
            error=str(exc),
            prod_version=prod_version.version,
            fallback="promoting_new_model",
        )
        _transition(client, model_name, new_version, "Staging")
        return

    # ── 4. Promote or reject ─────────────────────────────────────────────────
    if result.val_mae < prod_val_mae:
        _transition(client, model_name, new_version, "Staging")
        log.info(
            "promoted_to_staging",
            version=new_version,
            new_val_mae=round(result.val_mae, 4),
            prod_val_mae=round(prod_val_mae, 4),
            improvement=round(prod_val_mae - result.val_mae, 4),
            compared_against_prod_version=prod_version.version,
        )
    else:
        rejection_reason = (
            f"new_val_mae={result.val_mae:.6f} \u2265 "
            f"prod_val_mae={prod_val_mae:.6f} on holdout "
            f"(split_date={result.split_date.date()})"
        )
        client.set_model_version_tag(
            name=model_name,
            version=new_version,
            key="rejection_reason",
            value=rejection_reason,
        )
        client.set_model_version_tag(
            name=model_name,
            version=new_version,
            key="compared_against_prod_version",
            value=str(prod_version.version),
        )
        log.warning(
            "promotion_rejected",
            version=new_version,
            new_val_mae=round(result.val_mae, 4),
            prod_val_mae=round(prod_val_mae, 4),
            reason=rejection_reason,
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

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
