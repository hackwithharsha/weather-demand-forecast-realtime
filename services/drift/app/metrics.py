"""Prometheus metrics for the drift detection service."""
from prometheus_client import Gauge

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
    "Number of reference rows (city_hour_features) used in the last check",
)

drift_job_current_count = Gauge(
    "drift_job_current_count",
    "Number of current rows (prediction_features) used in the last check",
)
