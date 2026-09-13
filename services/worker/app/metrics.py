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
