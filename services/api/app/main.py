"""
Demand-forecast serving API.

Endpoints
---------
GET  /health            Liveness probe — always 200.
GET  /ready             Readiness probe — 503 until Production model is loaded.
GET  /model/info        Production + Staging metadata and feature-miss-rate stats.

GET  /cities            Distinct cities in the feature mart.
GET  /history/{city}    Last N hours of actual demand (default 168 = 7 days).

POST /predict           Multi-step demand forecast.
                        Body: { "city": str, "horizon_hours": int (1-168) }
                        Returns only the Production model prediction.

WS   /ws/live           Server-push stream: every /predict call (and the 60-second
                        background refresh) broadcasts a JSON message to all clients.

POST /admin/reload      Hot-swap Production + Staging from the MLflow registry
                        without restarting.  Uses asyncio.Lock to serialise concurrent
                        reload calls.

Feature-loading strategy
------------------------
1. Base features (lag/rolling/event_count/humidity_pct) are read from
   ``marts.city_hour_features`` (latest row per city) via asyncpg.

2. Real-time weather override: the stream worker publishes current weather to
   ``feat:route:{city}`` in Redis (fields: weather_temp_c, weather_precip_mm).
   If both fields are present the Postgres weather values are overridden;
   otherwise the Postgres values are kept.  Redis hits/misses are counted and
   logged at every 200th request.

Shadow scoring
--------------
After every Production prediction an ``asyncio.create_task`` fires off a
background coroutine that runs the Staging model on the same feature matrix
and logs the comparison.  The result is never returned to the caller.

Model loading
-------------
Models are loaded once at startup inside a thread-pool executor
(``mlflow.sklearn.load_model`` is blocking I/O).  If MLflow is unavailable at
startup the API starts without models and a background retry loop retries every
30 s until successful.  ``POST /admin/reload`` re-runs the same load path
under the reload lock.
"""

from __future__ import annotations

import asyncio
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from .settings import Settings

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Feature column order — must exactly match trainer's FEATURE_COLS
# ---------------------------------------------------------------------------

FEATURE_COLS: list[str] = [
    "city",
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
    "is_holiday",
]

# Redis key where the stream worker stores current weather per city.
# Fields used: weather_temp_c, weather_precip_mm.
_CITY_REDIS_KEY = "feat:route:{city}"


# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------

class _AppState:
    """Mutable application state; all writes happen from the asyncio event loop."""

    def __init__(self) -> None:
        self.settings: Settings | None = None
        self.redis: aioredis.Redis | None = None
        self.pg: asyncpg.Pool | None = None
        # sklearn Pipelines (or None before first successful load)
        self.prod_model: Any = None
        self.staging_model: Any = None
        self.prod_info: dict = {}
        self.staging_info: dict = {}
        # asyncio primitives — created once inside the event loop
        self.reload_lock: asyncio.Lock = asyncio.Lock()
        self.ws_subscribers: set[asyncio.Queue] = set()
        # Feature-miss counters
        self.redis_hits: int = 0
        self.redis_misses: int = 0
        # Background task handles
        self.broadcast_task: asyncio.Task | None = None
        self.retry_task: asyncio.Task | None = None


_state = _AppState()


# ---------------------------------------------------------------------------
# Model loading (synchronous; runs in thread executor)
# ---------------------------------------------------------------------------

def _do_load_models(settings: Settings) -> tuple[Any, dict, Any, dict]:
    """Load Production and Staging sklearn pipelines from the MLflow registry.

    Models are always fetched by *stage URI* (``models:/<name>/Production`` and
    ``models:/<name>/Staging``), never by run ID or version number.  This
    guarantees the caller always gets whatever the registry currently calls
    "Production", even if a new version was promoted since the last load.

    Version metadata is obtained with ``search_model_versions`` (the
    non-deprecated replacement for ``get_latest_versions``).  A single search
    call retrieves all versions; they are filtered client-side by
    ``current_stage`` and sorted descending so the highest version number wins
    when multiple versions share the same stage.

    Raises RuntimeError if no Production version is registered or if the
    registry is unreachable — the caller is responsible for retry logic.
    Staging absence is tolerated (returns None, {}).
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    if settings.aws_access_key_id:
        os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
    if settings.aws_secret_access_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
    if settings.mlflow_s3_endpoint_url:
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = settings.mlflow_s3_endpoint_url

    client = mlflow.tracking.MlflowClient()
    model_name = settings.mlflow_registered_model_name

    # One registry round-trip retrieves all versions; we filter client-side.
    # max_results=1000 is generous — real deployments rarely exceed a few dozen.
    all_vs = client.search_model_versions(
        f"name='{model_name}'", max_results=1000
    )

    def _latest_in_stage(stage: str):
        """Return the highest-version ModelVersion in *stage*, or None."""
        vs = [v for v in all_vs if v.current_stage == stage]
        return max(vs, key=lambda v: int(v.version)) if vs else None

    # ── Production (required) ────────────────────────────────────────────────
    pv = _latest_in_stage("Production")
    if pv is None:
        raise RuntimeError(
            f"No Production model registered for {model_name!r}. "
            "Run `make train` then promote a version to Production."
        )
    # Load by stage URI — not by version number or run ID
    prod_m = mlflow.sklearn.load_model(f"models:/{model_name}/Production")
    try:
        val_mae = client.get_run(pv.run_id).data.metrics.get("val_mae")
    except Exception:
        val_mae = None
    prod_info: dict = {
        "model_name": model_name,
        "version": int(pv.version),
        "stage": "Production",
        "run_id": pv.run_id,
        "val_mae": round(val_mae, 6) if val_mae is not None else None,
        "tags": {k: v for k, v in pv.tags.items() if not k.startswith("mlflow.")},
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    log.info("prod_model_loaded", version=pv.version, run_id=pv.run_id)

    # ── Staging (optional) ───────────────────────────────────────────────────
    sv = _latest_in_stage("Staging")
    staging_m: Any = None
    staging_info: dict = {}
    if sv is not None:
        try:
            # Load by stage URI — not by version number or run ID
            staging_m = mlflow.sklearn.load_model(f"models:/{model_name}/Staging")
            try:
                sv_mae = client.get_run(sv.run_id).data.metrics.get("val_mae")
            except Exception:
                sv_mae = None
            staging_info = {
                "model_name": model_name,
                "version": int(sv.version),
                "stage": "Staging",
                "run_id": sv.run_id,
                "val_mae": round(sv_mae, 6) if sv_mae is not None else None,
                "tags": {k: v for k, v in sv.tags.items() if not k.startswith("mlflow.")},
                "loaded_at": datetime.now(timezone.utc).isoformat(),
            }
            log.info("staging_model_loaded", version=sv.version, run_id=sv.run_id)
        except Exception as exc:
            log.warning("staging_model_not_loaded", error=str(exc))

    return prod_m, prod_info, staging_m, staging_info


async def _load_models_async(settings: Settings) -> None:
    """Async wrapper: load models in executor, then update shared state."""
    loop = asyncio.get_running_loop()
    prod_m, prod_info, staging_m, staging_info = await loop.run_in_executor(
        None, _do_load_models, settings
    )
    async with _state.reload_lock:
        _state.prod_model = prod_m
        _state.prod_info = prod_info
        _state.staging_model = staging_m
        _state.staging_info = staging_info


async def _model_load_retry_loop(settings: Settings, interval_s: int = 30) -> None:
    """Retry model loading every *interval_s* seconds until it succeeds.

    The loop runs until the task is cancelled (shutdown) or a load attempt
    completes without raising.  It does not exit early based on the current
    value of ``_state.prod_model`` — that avoids a race where a concurrent
    ``/admin/reload`` sets the model between the sleep and the attempt.
    """
    attempt = 0
    while True:
        await asyncio.sleep(interval_s)
        attempt += 1
        try:
            await _load_models_async(settings)
            log.info("model_load_retry_succeeded", attempts=attempt)
            return          # success — exit the loop
        except Exception as exc:
            log.warning(
                "model_load_retry_failed",
                attempt=attempt,
                retry_in_s=interval_s,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# WebSocket broadcast helpers
# ---------------------------------------------------------------------------

async def _broadcast(msg: dict) -> None:
    """Put *msg* into every subscriber queue; drop slow/full queues."""
    dead: list[asyncio.Queue] = []
    for q in list(_state.ws_subscribers):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _state.ws_subscribers.discard(q)


async def _periodic_broadcast_loop(interval_s: int = 60) -> None:
    """Every *interval_s* seconds, refresh predictions for all cities and broadcast."""
    while True:
        await asyncio.sleep(interval_s)
        if not _state.ws_subscribers or _state.prod_model is None or _state.pg is None:
            continue
        try:
            cities = await _fetch_cities()
        except Exception:
            continue
        for city in cities:
            try:
                result = await _do_predict(city, 1)
                await _broadcast({"event": "forecast_refresh", "data": result})
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    s = Settings()
    _state.settings = s

    # Redis pool
    _state.redis = aioredis.Redis(
        host=s.redis_host,
        port=s.redis_port,
        db=s.redis_db,
        password=s.redis_password or None,
        decode_responses=True,
        socket_connect_timeout=2,
    )

    # Postgres pool
    try:
        _state.pg = await asyncpg.create_pool(
            host=s.postgres_host,
            port=s.postgres_port,
            database=s.postgres_db,
            user=s.postgres_user,
            password=s.postgres_password,
            min_size=1,
            max_size=5,
            command_timeout=10,
        )
        log.info("postgres_pool_ready")
    except Exception as exc:
        log.warning("postgres_pool_failed", error=str(exc))
        _state.pg = None

    # Model loading — best-effort at startup.
    # Two distinct failure modes are handled identically here:
    #   • MLflow/MinIO unreachable (network, wrong profile, not yet up)
    #   • Registry has no Production version yet (need `make train` first)
    # In both cases the API starts, /health returns 200, /ready returns 503,
    # and a background task retries every 30 s until it succeeds.
    # Once MLflow is reachable (or after `make train`), call `make reload`
    # or wait for the retry loop to pick it up automatically.
    try:
        await _load_models_async(s)
    except Exception as exc:
        log.warning(
            "model_load_startup_failed",
            error=str(exc),
            ready_probe="503 until models loaded",
            recovery="background retry every 30 s; or POST /admin/reload",
        )
        _state.retry_task = asyncio.create_task(_model_load_retry_loop(s))

    # Background broadcast
    _state.broadcast_task = asyncio.create_task(_periodic_broadcast_loop())

    yield  # ── serve ──

    if _state.broadcast_task:
        _state.broadcast_task.cancel()
    if _state.retry_task:
        _state.retry_task.cancel()
    await _state.redis.aclose()
    if _state.pg:
        await _state.pg.close()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Demand Forecast Serving API",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    city: str
    horizon_hours: int = Field(default=24, ge=1, le=168)


class HourForecast(BaseModel):
    target_hour: str        # ISO-8601 UTC
    predicted_demand: float


class PredictResponse(BaseModel):
    city: str
    as_of: str              # ISO-8601 UTC when the prediction was made
    horizon_hours: int
    model_version: str
    source: str             # always "production"
    feature_source: str     # "redis+postgres" | "postgres"
    forecasts: list[HourForecast]


# ---------------------------------------------------------------------------
# Feature helpers
# ---------------------------------------------------------------------------

def _to_f(v: Any) -> float | None:
    """Cast asyncpg Decimal / int / None to float | None."""
    return None if v is None else float(v)


async def _pg_latest_features(city: str) -> dict | None:
    """Return the most recent mart row for *city*, or None if absent."""
    row = await _state.pg.fetchrow(
        """
        SELECT
            event_count,
            demand_lag_1h,
            demand_lag_24h,
            demand_lag_168h,
            demand_roll_3h,
            demand_roll_24h,
            temperature_c,
            humidity_pct,
            precip_mm,
            is_holiday::INT AS is_holiday
        FROM marts.city_hour_features
        WHERE city = $1
        ORDER BY hour_ts DESC
        LIMIT 1
        """,
        city,
    )
    return dict(row) if row else None


async def _redis_weather(city: str) -> dict | None:
    """Try to fetch real-time weather from the stream worker's Redis hash.

    Returns {'temperature_c': float, 'precip_mm': float} or None.
    Fields are written by the stream worker as weather_temp_c / weather_precip_mm.
    """
    key = _CITY_REDIS_KEY.format(city=city)
    try:
        vals = await _state.redis.hmget(key, "weather_temp_c", "weather_precip_mm")
        if vals[0] is not None and vals[1] is not None:
            return {"temperature_c": float(vals[0]), "precip_mm": float(vals[1])}
    except Exception as exc:
        log.debug("redis_weather_miss", city=city, error=str(exc))
    return None


def _time_feats(ts: datetime) -> dict:
    """Cyclic hour-of-day and day-of-week encodings for *ts*."""
    return {
        "hour_sin": math.sin(2 * math.pi * ts.hour / 24),
        "hour_cos": math.cos(2 * math.pi * ts.hour / 24),
        "dow_sin":  math.sin(2 * math.pi * ts.weekday() / 7),
        "dow_cos":  math.cos(2 * math.pi * ts.weekday() / 7),
    }


def _build_rows(
    city: str,
    horizon: int,
    base: dict,
    as_of: datetime,
) -> pd.DataFrame:
    """Build a (horizon × len(FEATURE_COLS)) DataFrame for the sklearn pipeline.

    Time features are recomputed for each target hour.
    Lag, rolling, weather, and event_count are held constant at the last known
    values (static multi-step forecast — no recursive feedback).
    """
    base_hour = as_of.replace(minute=0, second=0, microsecond=0)
    rows = []
    for h in range(horizon):
        t = base_hour + timedelta(hours=h)
        rows.append({
            "city":             city,
            "event_count":      float(base.get("event_count") or 0),
            "demand_lag_1h":    _to_f(base.get("demand_lag_1h")),
            "demand_lag_24h":   _to_f(base.get("demand_lag_24h")),
            "demand_lag_168h":  _to_f(base.get("demand_lag_168h")),
            "demand_roll_3h":   _to_f(base.get("demand_roll_3h")),
            "demand_roll_24h":  _to_f(base.get("demand_roll_24h")),
            **_time_feats(t),
            "temperature_c":    _to_f(base.get("temperature_c")),
            "humidity_pct":     _to_f(base.get("humidity_pct")),
            "precip_mm":        _to_f(base.get("precip_mm")),
            "is_holiday":       float(base.get("is_holiday") or 0),
        })
    return pd.DataFrame(rows, columns=FEATURE_COLS)


# ---------------------------------------------------------------------------
# Core prediction logic
# ---------------------------------------------------------------------------

async def _do_predict(city: str, horizon_hours: int) -> dict:
    """Fetch features, run Production model, schedule shadow scoring.

    Returns the serialisable dict matching PredictResponse.
    """
    as_of = datetime.now(timezone.utc)

    # 1. Base features from Postgres mart
    base = await _pg_latest_features(city)
    if base is None:
        raise HTTPException(404, f"No feature data for city {city!r}. "
                                 "Has the mart pipeline run yet?")

    # 2. Real-time weather override from Redis
    rw = await _redis_weather(city)
    if rw is not None:
        _state.redis_hits += 1
        base["temperature_c"] = rw["temperature_c"]
        base["precip_mm"] = rw["precip_mm"]
        feat_src = "redis+postgres"
    else:
        _state.redis_misses += 1
        feat_src = "postgres"

    total = _state.redis_hits + _state.redis_misses
    if total > 0 and total % 200 == 0:
        log.info(
            "feature_store_miss_rate",
            redis_hits=_state.redis_hits,
            redis_misses=_state.redis_misses,
            miss_pct=round(_state.redis_misses / total * 100, 1),
        )

    # 3. Build feature matrix
    df = _build_rows(city, horizon_hours, base, as_of)

    # 4. Production inference (blocking sklearn call in thread executor)
    loop = asyncio.get_running_loop()
    prod_model = _state.prod_model
    preds: np.ndarray = await loop.run_in_executor(
        None, prod_model.predict, df
    )
    prod_ver = str(_state.prod_info.get("version", "unknown"))

    # 5. Shadow-score Staging in background (fire-and-forget)
    if _state.staging_model is not None:
        asyncio.create_task(_shadow_task(df.copy(), preds.copy(), city, prod_ver))

    # 6. Assemble response
    base_hour = as_of.replace(minute=0, second=0, microsecond=0)
    return {
        "city": city,
        "as_of": as_of.isoformat(),
        "horizon_hours": horizon_hours,
        "model_version": prod_ver,
        "source": "production",
        "feature_source": feat_src,
        "forecasts": [
            {
                "target_hour": (base_hour + timedelta(hours=h)).isoformat(),
                "predicted_demand": round(float(p), 4),
            }
            for h, p in enumerate(preds)
        ],
    }


async def _shadow_task(
    df: pd.DataFrame,
    prod_preds: np.ndarray,
    city: str,
    prod_ver: str,
) -> None:
    """Run Staging model on the same features and log the comparison.

    Never raises — errors are logged as warnings.
    """
    try:
        staging_m = _state.staging_model
        if staging_m is None:
            return
        loop = asyncio.get_running_loop()
        staging_preds: np.ndarray = await loop.run_in_executor(
            None, staging_m.predict, df
        )
        staging_ver = str(_state.staging_info.get("version", "unknown"))
        log.info(
            "shadow_score",
            city=city,
            prod_version=prod_ver,
            staging_version=staging_ver,
            prod_mean=round(float(np.mean(prod_preds)), 4),
            staging_mean=round(float(np.mean(staging_preds)), 4),
            delta=round(float(np.mean(staging_preds - prod_preds)), 4),
            n_hours=len(prod_preds),
        )
    except Exception as exc:
        log.warning("shadow_score_failed", city=city, error=str(exc))


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _fetch_cities() -> list[str]:
    rows = await _state.pg.fetch(
        "SELECT DISTINCT city FROM marts.city_hour_features ORDER BY city"
    )
    return [r["city"] for r in rows]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    """Liveness probe — always 200."""
    return {"status": "ok"}


@app.get("/ready")
async def ready() -> dict:
    """Readiness probe — 503 until the Production model is loaded."""
    if _state.prod_model is None:
        raise HTTPException(503, "Production model not yet loaded")
    return {
        "status": "ready",
        "model_version": str(_state.prod_info.get("version")),
    }


@app.get("/model/info")
async def model_info() -> dict:
    """Metadata for the loaded Production and Staging models plus miss-rate stats."""
    if _state.prod_model is None:
        raise HTTPException(503, "Production model not yet loaded")
    total = _state.redis_hits + _state.redis_misses
    return {
        "production": _state.prod_info,
        "staging": _state.staging_info or None,
        "feature_store": {
            "redis_hits": _state.redis_hits,
            "redis_misses": _state.redis_misses,
            "miss_rate_pct": (
                round(_state.redis_misses / total * 100, 2) if total else None
            ),
            "description": (
                "A 'miss' means Redis had no live weather for the city; "
                "Postgres weather was used instead."
            ),
        },
    }


@app.get("/cities")
async def cities() -> dict:
    """List all cities present in the feature mart."""
    if _state.pg is None:
        raise HTTPException(503, "Database unavailable")
    try:
        city_list = await _fetch_cities()
    except Exception as exc:
        log.error("cities_query_failed", error=str(exc))
        raise HTTPException(503, "Database unavailable") from exc
    return {"cities": city_list}


@app.get("/history/{city}")
async def history(city: str, hours: int = 168) -> dict:
    """Return the last *hours* hours of actual demand for *city*.

    Query param ``hours`` is clamped to [1, 8760] (1 h – 1 year).
    """
    if _state.pg is None:
        raise HTTPException(503, "Database unavailable")
    hours = max(1, min(hours, 8760))
    try:
        rows = await _state.pg.fetch(
            """
            SELECT hour_ts, total_demand
            FROM   marts.city_hour_features
            WHERE  city = $1 AND total_demand IS NOT NULL
            ORDER  BY hour_ts DESC
            LIMIT  $2
            """,
            city,
            hours,
        )
    except Exception as exc:
        log.error("history_query_failed", city=city, error=str(exc))
        raise HTTPException(503, "Database unavailable") from exc
    if not rows:
        raise HTTPException(404, f"No demand history for city {city!r}")
    return {
        "city": city,
        "count": len(rows),
        "history": [
            {
                "hour_ts": r["hour_ts"].isoformat(),
                "total_demand": float(r["total_demand"]),
            }
            for r in rows
        ],
    }


@app.post("/predict", response_model=PredictResponse)
async def predict(req: PredictRequest) -> dict:
    """Generate a multi-step demand forecast for *city*.

    Only Production model predictions are returned.  The Staging model is
    evaluated in the background for comparison logging.
    """
    if _state.prod_model is None:
        raise HTTPException(503, "Production model not yet loaded")
    if _state.pg is None:
        raise HTTPException(503, "Database unavailable")
    result = await _do_predict(req.city, req.horizon_hours)
    await _broadcast({"event": "predict", "data": result})
    return result


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket) -> None:
    """Stream forecasts to the client in real time.

    Every call to POST /predict and the 60-second background refresh pushes
    a JSON object:  { "event": "predict" | "forecast_refresh", "data": {...} }
    """
    await ws.accept()
    q: asyncio.Queue[dict] = asyncio.Queue(maxsize=50)
    _state.ws_subscribers.add(q)
    log.info("ws_connected", subscribers=len(_state.ws_subscribers))
    try:
        while True:
            msg = await q.get()
            await ws.send_json(msg)
    except WebSocketDisconnect:
        pass
    finally:
        _state.ws_subscribers.discard(q)
        log.info("ws_disconnected", subscribers=len(_state.ws_subscribers))


@app.post("/admin/reload")
async def admin_reload() -> dict:
    """Hot-swap Production and Staging models from the MLflow registry.

    Holds the reload lock for the duration of the MLflow download; concurrent
    calls queue behind it rather than double-loading.
    """
    async with _state.reload_lock:
        try:
            loop = asyncio.get_running_loop()
            prod_m, prod_info, staging_m, staging_info = await loop.run_in_executor(
                None, _do_load_models, _state.settings
            )
            _state.prod_model = prod_m
            _state.prod_info = prod_info
            _state.staging_model = staging_m
            _state.staging_info = staging_info
        except Exception as exc:
            log.error("admin_reload_failed", error=str(exc))
            raise HTTPException(500, f"Reload failed: {exc}") from exc

    log.info(
        "admin_reload_complete",
        prod_version=_state.prod_info.get("version"),
        staging_version=_state.staging_info.get("version"),
    )
    return {
        "status": "reloaded",
        "production_version": str(_state.prod_info.get("version")),
        "staging_version": (
            str(_state.staging_info["version"]) if _state.staging_info else None
        ),
    }
