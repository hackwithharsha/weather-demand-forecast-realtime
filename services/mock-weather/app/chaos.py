"""
Chaos controls injected around every non-health request.

Middleware layer  →  latency + error-rate  (request-level)
apply_chaos()    →  null-field + schema-drift  (response-body-level)

Chaos is intentionally non-deterministic (uses an unseeded RNG) so that
successive calls to the same endpoint can produce different fault patterns.
"""

import asyncio
import random
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class ChaosMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings) -> None:
        super().__init__(app)
        self._settings = settings

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        # Health checks are exempt from all chaos.
        if request.url.path == "/health":
            return await call_next(request)

        if self._settings.fault_latency_ms > 0:
            await asyncio.sleep(self._settings.fault_latency_ms / 1000.0)

        if self._settings.fault_error_rate > 0 and random.random() < self._settings.fault_error_rate:
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
    """Recursively replace values with None at the given probability."""
    if isinstance(data, dict):
        return {k: None if rng.random() < rate else _nullify(v, rate, rng) for k, v in data.items()}
    if isinstance(data, list):
        return [_nullify(item, rate, rng) for item in data]
    return data


def _drift(data: Any) -> Any:
    """Rename the 'temperature' key to 'temp' everywhere in the payload."""
    if isinstance(data, dict):
        return {("temp" if k == "temperature" else k): _drift(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_drift(item) for item in data]
    return data


def apply_chaos(data: dict, settings) -> dict:
    rng = random.Random()  # unseeded — chaos should vary between calls

    if settings.fault_null_field_rate > 0:
        data = _nullify(data, settings.fault_null_field_rate, rng)

    if settings.fault_schema_drift:
        data = _drift(data)

    return data
