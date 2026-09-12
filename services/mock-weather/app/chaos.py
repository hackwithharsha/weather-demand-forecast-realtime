"""
Chaos controls for the mock-weather service.

Two layers
----------
ChaosMiddleware   request-level: latency injection and HTTP 503 errors.
apply_chaos()     response-body-level: null fields and schema drift.

Both layers read from a shared ChaosConfig instance stored on app.state.
That instance is mutated at runtime by POST /admin/chaos, so chaos can be
toggled without restarting the service.

/health and /admin/* are always exempt.
"""

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


@dataclass
class ChaosConfig:
    """Mutable chaos state.  Updated in-place by the admin endpoint."""

    latency_ms: int = 0           # ms of artificial delay on every response
    error_rate: float = 0.0       # 0.0–1.0 probability of returning HTTP 503
    null_field_rate: float = 0.0  # 0.0–1.0 probability of nulling any response field
    schema_drift: bool = False    # rename "temperature" → "temp" in every payload

    @classmethod
    def from_settings(cls, s: Any) -> "ChaosConfig":
        return cls(
            latency_ms=s.fault_latency_ms,
            error_rate=s.fault_error_rate,
            null_field_rate=s.fault_null_field_rate,
            schema_drift=s.fault_schema_drift,
        )


_EXEMPT_PREFIXES = ("/health", "/admin/")


class ChaosMiddleware(BaseHTTPMiddleware):
    """Injects latency and errors before the route handler runs."""

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        path = request.url.path
        if any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            return await call_next(request)

        cfg: ChaosConfig = request.app.state.chaos

        if cfg.latency_ms > 0:
            await asyncio.sleep(cfg.latency_ms / 1000.0)

        if cfg.error_rate > 0.0 and random.random() < cfg.error_rate:
            return Response(
                content='{"error":"injected_fault","code":503}',
                status_code=503,
                media_type="application/json",
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# Response-body transforms
# ---------------------------------------------------------------------------

def _nullify(data: Any, rate: float, rng: random.Random) -> Any:
    if isinstance(data, dict):
        return {
            k: None if rng.random() < rate else _nullify(v, rate, rng)
            for k, v in data.items()
        }
    if isinstance(data, list):
        return [_nullify(item, rate, rng) for item in data]
    return data


def _drift(data: Any) -> Any:
    """Rename every 'temperature' key to 'temp' to simulate schema breakage."""
    if isinstance(data, dict):
        return {("temp" if k == "temperature" else k): _drift(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_drift(item) for item in data]
    return data


def apply_chaos(data: dict, cfg: ChaosConfig) -> dict:
    """Apply response-body chaos transforms.  Called after the route handler."""
    rng = random.Random()  # intentionally unseeded — chaos must vary between calls

    if cfg.null_field_rate > 0.0:
        data = _nullify(data, cfg.null_field_rate, rng)

    if cfg.schema_drift:
        data = _drift(data)

    return data
