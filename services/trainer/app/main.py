"""
Trainer entrypoint.

Steps
-----
1.  Configure structlog and read settings from environment.
2.  Load marts.city_hour_features for the configured lookback window.
3.  Set up the MLflow experiment.
4.  Train Ridge + HistGradientBoosting; each run is logged independently.
5.  Register the best run (lowest val_mae) to the Model Registry.
6.  Promote to Staging when the new model beats the current Production model
    on the same holdout, otherwise log why it was rejected.

Usage
-----
Via Make (preferred):

    make train          # requires core + ml profiles to be running

Via Compose directly:

    docker compose --profile core --profile ml run --rm trainer

Environment variables
---------------------
All configuration is via environment; see app/settings.py for defaults.
The only required secret is POSTGRES_PASSWORD.
"""

from __future__ import annotations

import sys
import time as _time

import mlflow
import structlog

from .data import load_features
from .promote import register_and_maybe_promote
from .settings import Settings
from .train import run_training


def main() -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
    )
    log = structlog.get_logger()

    settings = Settings()
    log.info(
        "trainer_started",
        **settings.model_dump(exclude={"postgres_password"}),
    )

    # ── Configure MLflow ──────────────────────────────────────────────────────
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    experiment = mlflow.set_experiment(settings.mlflow_experiment_name)
    log.info(
        "mlflow_experiment_set",
        experiment_name=settings.mlflow_experiment_name,
        experiment_id=experiment.experiment_id,
        tracking_uri=settings.mlflow_tracking_uri,
    )

    # ── Load training data ────────────────────────────────────────────────────
    df = load_features(settings)
    if df.empty:
        log.error(
            "no_training_data",
            lookback_days=settings.training_lookback_days,
            hint=(
                "Run 'make worker-pipeline' to populate marts.city_hour_features, "
                "then retry."
            ),
        )
        sys.exit(1)

    # ── Train ─────────────────────────────────────────────────────────────────
    best = run_training(df, settings)

    # ── Register + conditionally promote ─────────────────────────────────────
    register_and_maybe_promote(best, settings)

    # ── Push metrics to Prometheus Pushgateway (best-effort) ──────────────────
    if settings.pushgateway_url:
        try:
            from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
            registry = CollectorRegistry()
            g_ts   = Gauge("model_last_trained_timestamp", "Unix timestamp of last training run", registry=registry)
            g_mae  = Gauge("model_val_mae",  "Validation MAE",  ["model_type"], registry=registry)
            g_rmse = Gauge("model_val_rmse", "Validation RMSE", ["model_type"], registry=registry)
            g_mape = Gauge("model_val_mape", "Validation MAPE", ["model_type"], registry=registry)
            g_ts.set(_time.time())
            g_mae.labels(model_type=best.model_type).set(best.val_mae)
            g_rmse.labels(model_type=best.model_type).set(best.val_rmse)
            g_mape.labels(model_type=best.model_type).set(best.val_mape)
            push_to_gateway(settings.pushgateway_url, job="trainer", registry=registry)
            log.info("trainer_metrics_pushed", pushgateway_url=settings.pushgateway_url)
        except Exception as exc:
            log.warning("trainer_metrics_push_failed", error=str(exc))

    log.info(
        "trainer_finished",
        best_model=best.model_type,
        val_mae=round(best.val_mae, 4),
        val_rmse=round(best.val_rmse, 4),
        val_mape=round(best.val_mape, 4),
        mlflow_run_id=best.run_id,
    )


if __name__ == "__main__":
    main()
