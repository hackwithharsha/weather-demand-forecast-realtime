import logging
import sys
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Query

from .chaos import ChaosMiddleware, apply_chaos
from .settings import Settings
from .weather import generate_current, generate_forecast

settings = Settings()


# ---------------------------------------------------------------------------
# Logging — inline rather than via libs/common so this service is standalone
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
    log = structlog.get_logger()
    log.info(
        "mock_weather_started",
        port=settings.port,
        fault_latency_ms=settings.fault_latency_ms,
        fault_error_rate=settings.fault_error_rate,
        fault_null_field_rate=settings.fault_null_field_rate,
        fault_schema_drift=settings.fault_schema_drift,
    )
    yield
    structlog.get_logger().info("mock_weather_stopped")


app = FastAPI(title="Mock Weather API", version="1.0.0", lifespan=lifespan)
app.add_middleware(ChaosMiddleware, settings=settings)

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/v1/current")
async def current_weather(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
) -> dict:
    log.debug("current_request", lat=lat, lon=lon)
    return apply_chaos(generate_current(lat, lon), settings)


@app.get("/v1/forecast")
async def forecast(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    hours: int = Query(24, ge=1, le=168, description="Number of forecast hours (max 168)"),
) -> dict:
    log.debug("forecast_request", lat=lat, lon=lon, hours=hours)
    return apply_chaos(generate_forecast(lat, lon, hours), settings)
