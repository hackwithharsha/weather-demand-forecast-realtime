"""Prometheus metrics for the API service.

Generic HTTP request metrics (http_requests_total, http_request_duration_seconds)
live in common.metrics and are recorded by PrometheusMiddleware.
"""
from prometheus_client import Counter, Histogram

predictions_total = Counter(
    "predictions_total",
    "Total /predict requests served by the Production model",
    ["city", "model_version"],
)

feature_cache_hits_total = Counter(
    "feature_cache_hits_total",
    "Redis feature hash present (online override available)",
)

feature_cache_misses_total = Counter(
    "feature_cache_misses_total",
    "Redis feature hash absent (Postgres fallback used)",
)

shadow_delta_absolute = Histogram(
    "shadow_delta_absolute",
    "Mean absolute difference between Staging and Production predictions",
    ["city"],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
)
