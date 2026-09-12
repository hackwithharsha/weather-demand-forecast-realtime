import logging
import sys
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Query, Request

from common.generator import City

from .adapter import current_payload, forecast_payload
from .chaos import ChaosConfig, ChaosMiddleware, apply_chaos
from .schemas import ChaosStatus, ChaosUpdate, CurrentWeatherResponse, ForecastResponse
from .settings import Settings

settings = Settings()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _configure_logging(level: str) -> None:
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging(settings.log_level)
    app.state.chaos = ChaosConfig.from_settings(settings)
    log = structlog.get_logger()
    log.info("mock_weather_started", port=settings.port, chaos=vars(app.state.chaos))
    yield
    structlog.get_logger().info("mock_weather_stopped")


app = FastAPI(title="Mock Weather API", version="1.0.0", lifespan=lifespan)
app.add_middleware(ChaosMiddleware)

log = structlog.get_logger(__name__)


def _city(lat: float, lon: float) -> City:
    """Anonymous city from raw coordinates."""
    return City(name=f"{lat},{lon}", lat=lat, lon=lon)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
async def health() -> dict:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Weather endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/v1/current",
    tags=["weather"],
    responses={200: {"model": CurrentWeatherResponse}},
)
async def current_weather(
    request: Request,
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
) -> dict:
    log.debug("current_request", lat=lat, lon=lon)
    return apply_chaos(current_payload(_city(lat, lon)), request.app.state.chaos)


@app.get(
    "/v1/forecast",
    tags=["weather"],
    responses={200: {"model": ForecastResponse}},
)
async def forecast(
    request: Request,
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    hours: int = Query(24, ge=1, le=168, description="Number of forecast hours (max 168)"),
) -> dict:
    log.debug("forecast_request", lat=lat, lon=lon, hours=hours)
    return apply_chaos(forecast_payload(_city(lat, lon), hours), request.app.state.chaos)


# ---------------------------------------------------------------------------
# Admin — chaos controls (always exempt from chaos effects)
# ---------------------------------------------------------------------------

@app.get("/admin/chaos", response_model=ChaosStatus, tags=["admin"])
async def get_chaos(request: Request) -> ChaosStatus:
    """Return the current chaos configuration."""
    return ChaosStatus(**vars(request.app.state.chaos))


@app.post("/admin/chaos", response_model=ChaosStatus, tags=["admin"])
async def set_chaos(update: ChaosUpdate, request: Request) -> ChaosStatus:
    """
    Partially update chaos settings at runtime.

    Omit any field to leave it unchanged.  Send `{}` to inspect without
    modifying.  Changes take effect immediately on the next request.
    """
    cfg: ChaosConfig = request.app.state.chaos

    if update.latency_ms is not None:
        cfg.latency_ms = update.latency_ms
    if update.error_rate is not None:
        cfg.error_rate = update.error_rate
    if update.null_field_rate is not None:
        cfg.null_field_rate = update.null_field_rate
    if update.schema_drift is not None:
        cfg.schema_drift = update.schema_drift

    log.info("chaos_updated", **vars(cfg))
    return ChaosStatus(**vars(cfg))
