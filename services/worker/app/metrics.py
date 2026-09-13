"""Prometheus metrics for the worker service.

Exposed on port 9101 via prometheus_client.start_http_server.
"""
from prometheus_client import Counter, Gauge, Histogram

pipeline_run_duration_seconds = Histogram(
    "worker_pipeline_run_duration_seconds",
    "Duration of each pipeline stage in seconds",
    ["stage"],
    buckets=[0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0],
)

pipeline_runs_total = Counter(
    "worker_pipeline_runs_total",
    "Total pipeline runs by outcome",
    ["status"],
)

pipeline_last_run_timestamp = Gauge(
    "worker_pipeline_last_run_timestamp",
    "Unix timestamp of the last successful pipeline run",
)

pipeline_rows_processed_total = Counter(
    "worker_pipeline_rows_processed_total",
    "Rows produced by each pipeline stage",
    ["stage"],
)

pipeline_validation_rejects_total = Counter(
    "worker_pipeline_validation_rejects_total",
    "Rows rejected by the staging validation step",
)

# ---------------------------------------------------------------------------
# Drift check metrics
# ---------------------------------------------------------------------------

drift_score_gauge = Gauge(
    "drift_score",
    "Evidently drift score for each feature (value from the last check run)",
    ["feature"],
)

drift_detected_gauge = Gauge(
    "drift_detected",
    "Whether Evidently detected drift for a feature (1 = yes, 0 = no)",
    ["feature"],
)

drift_job_last_run_timestamp = Gauge(
    "drift_job_last_run_timestamp",
    "Unix timestamp of the last completed drift check",
)

drift_job_duration_seconds = Gauge(
    "drift_job_duration_seconds",
    "Wall-clock seconds taken by the last drift check run",
)

drift_job_reference_count = Gauge(
    "drift_job_reference_count",
    "Number of reference rows (from MLflow run) used in the last check",
)

drift_job_current_count = Gauge(
    "drift_job_current_count",
    "Number of current rows (prediction_features) used in the last check",
)
