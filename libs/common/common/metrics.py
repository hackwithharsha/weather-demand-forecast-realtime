"""Shared Prometheus instrumentation helpers.

FastAPI / Starlette services
----------------------------
    from common.metrics import PrometheusMiddleware, mount_metrics
    app.add_middleware(PrometheusMiddleware)
    mount_metrics(app)

Background-process services (ingestor, worker)
----------------------------------------------
    from common.metrics import start_metrics_server
    start_metrics_server(port=9100)

Timing utility
--------------
    from common.metrics import timed_block
    with timed_block(my_histogram, stage="preprocessing"):
        do_work()
"""

from __future__ import annotations

import contextlib
import time as _time

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from prometheus_client import start_http_server as _start_http_server
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# Shared HTTP metrics — one set, registered once, used by all HTTP services
# ---------------------------------------------------------------------------

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests handled",
    ["method", "path", "status_code"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# ---------------------------------------------------------------------------
# Paths that bypass instrumentation
# ---------------------------------------------------------------------------

_PASSTHROUGH_PATHS: frozenset[str] = frozenset({"/health", "/ready", "/metrics"})


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

class PrometheusMiddleware(BaseHTTPMiddleware):
    """Record request count + latency for every route except infra paths."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in _PASSTHROUGH_PATHS:
            return await call_next(request)
        method = request.method
        t0 = _time.perf_counter()
        response = await call_next(request)
        elapsed = _time.perf_counter() - t0
        http_requests_total.labels(
            method=method, path=path, status_code=str(response.status_code)
        ).inc()
        http_request_duration_seconds.labels(method=method, path=path).observe(elapsed)
        return response


# ---------------------------------------------------------------------------
# FastAPI helper
# ---------------------------------------------------------------------------

def mount_metrics(app) -> None:
    """Add GET /metrics scrape endpoint to a FastAPI app."""

    @app.get("/metrics", include_in_schema=False)
    async def _metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Background-process helper
# ---------------------------------------------------------------------------

def start_metrics_server(port: int) -> None:
    """Start a Prometheus metrics HTTP server on *port* (daemon thread)."""
    import structlog

    _start_http_server(port)
    structlog.get_logger().info("metrics_server_started", port=port)


# ---------------------------------------------------------------------------
# Timing context manager
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def timed_block(histogram, **label_values):
    """Observe wall-clock duration of the enclosed block into *histogram*.

    Example::

        with timed_block(pipeline_run_duration_seconds, stage="staging"):
            run_staging(settings)
    """
    t0 = _time.perf_counter()
    try:
        yield
    finally:
        elapsed = _time.perf_counter() - t0
        if label_values:
            histogram.labels(**label_values).observe(elapsed)
        else:
            histogram.observe(elapsed)
