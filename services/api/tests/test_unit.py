"""
Unit tests for services/api.

All tests run without live services: no Postgres, no Redis, no MLflow.
_state is patched per-test via the app_client fixture (see conftest.py).

Coverage
--------
TestHealth
    /health always returns 200 {"status": "ok"} regardless of state.

TestReady
    /ready returns 503 when no Production model is loaded, 200 once one is set.

TestCities
    /cities returns 503 when _state.pg is None; returns city list when pg mock
    is wired up.

TestPredictRequestSchema
    PredictRequest rejects horizon_hours outside [1, 168] at the Pydantic level.

TestTimeFeats
    _time_feats returns the correct sin/cos encodings and all four keys.

TestBuildRows
    _build_rows returns a DataFrame with the right shape, column names, and
    constant lag/weather values across every horizon step.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import pytest_asyncio
from pydantic import ValidationError

from app.main import FEATURE_COLS, PredictRequest, _build_rows, _time_feats


# ---------------------------------------------------------------------------
# TestHealth
# ---------------------------------------------------------------------------

class TestHealth:
    @pytest.mark.asyncio
    async def test_health_always_200(self, app_client):
        client, state = app_client
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_body(self, app_client):
        client, state = app_client
        resp = await client.get("/health")
        assert resp.json() == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_health_with_no_model(self, app_client):
        """Health must return 200 even when no Production model is loaded."""
        client, state = app_client
        assert state.prod_model is None
        resp = await client.get("/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# TestReady
# ---------------------------------------------------------------------------

class TestReady:
    @pytest.mark.asyncio
    async def test_ready_503_when_no_model(self, app_client):
        client, state = app_client
        assert state.prod_model is None
        resp = await client.get("/ready")
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_ready_200_when_model_loaded(self, app_client):
        client, state = app_client
        state.prod_model = MagicMock()
        state.prod_info = {"version": 3}
        resp = await client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        assert body["model_version"] == "3"


# ---------------------------------------------------------------------------
# TestCities
# ---------------------------------------------------------------------------

class TestCities:
    @pytest.mark.asyncio
    async def test_cities_503_when_no_pg(self, app_client):
        client, state = app_client
        assert state.pg is None
        resp = await client.get("/cities")
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_cities_returns_list(self, app_client):
        client, state = app_client
        pg_mock = AsyncMock()
        pg_mock.fetch = AsyncMock(
            return_value=[{"city": "london"}, {"city": "tokyo"}]
        )
        state.pg = pg_mock
        resp = await client.get("/cities")
        assert resp.status_code == 200
        body = resp.json()
        assert "cities" in body
        assert body["cities"] == ["london", "tokyo"]

    @pytest.mark.asyncio
    async def test_cities_empty_mart(self, app_client):
        client, state = app_client
        pg_mock = AsyncMock()
        pg_mock.fetch = AsyncMock(return_value=[])
        state.pg = pg_mock
        resp = await client.get("/cities")
        assert resp.status_code == 200
        assert resp.json()["cities"] == []


# ---------------------------------------------------------------------------
# TestPredictRequestSchema
# ---------------------------------------------------------------------------

class TestPredictRequestSchema:
    def test_valid_request(self):
        req = PredictRequest(city="london", horizon_hours=24)
        assert req.city == "london"
        assert req.horizon_hours == 24

    def test_default_horizon(self):
        req = PredictRequest(city="tokyo")
        assert req.horizon_hours == 24

    def test_horizon_too_low(self):
        with pytest.raises(ValidationError):
            PredictRequest(city="london", horizon_hours=0)

    def test_horizon_too_high(self):
        with pytest.raises(ValidationError):
            PredictRequest(city="london", horizon_hours=169)

    def test_horizon_boundary_1(self):
        req = PredictRequest(city="london", horizon_hours=1)
        assert req.horizon_hours == 1

    def test_horizon_boundary_168(self):
        req = PredictRequest(city="london", horizon_hours=168)
        assert req.horizon_hours == 168


# ---------------------------------------------------------------------------
# TestTimeFeats
# ---------------------------------------------------------------------------

class TestTimeFeats:
    def test_returns_four_keys(self):
        ts = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
        result = _time_feats(ts)
        assert set(result.keys()) == {"hour_sin", "hour_cos", "dow_sin", "dow_cos"}

    def test_midnight_hour_sin_zero(self):
        """sin(2π × 0/24) = 0."""
        ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        feats = _time_feats(ts)
        assert abs(feats["hour_sin"]) < 1e-10

    def test_midnight_hour_cos_one(self):
        """cos(2π × 0/24) = 1."""
        ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        feats = _time_feats(ts)
        assert abs(feats["hour_cos"] - 1.0) < 1e-10

    def test_noon_hour_sin_zero(self):
        """sin(2π × 12/24) = sin(π) ≈ 0."""
        ts = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        feats = _time_feats(ts)
        assert abs(feats["hour_sin"]) < 1e-10

    def test_noon_hour_cos_minus_one(self):
        """cos(2π × 12/24) = cos(π) = -1."""
        ts = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        feats = _time_feats(ts)
        assert abs(feats["hour_cos"] - (-1.0)) < 1e-10

    def test_sin_cos_identity(self):
        """sin²+cos² = 1 for any timestamp."""
        ts = datetime(2026, 6, 15, 17, 30, tzinfo=timezone.utc)
        feats = _time_feats(ts)
        assert abs(feats["hour_sin"] ** 2 + feats["hour_cos"] ** 2 - 1.0) < 1e-10
        assert abs(feats["dow_sin"] ** 2 + feats["dow_cos"] ** 2 - 1.0) < 1e-10

    def test_monday_weekday_0(self):
        """Monday = weekday 0; sin(2π×0/7) = 0, cos = 1."""
        # 2026-09-14 is a Monday
        ts = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)
        assert ts.weekday() == 0
        feats = _time_feats(ts)
        assert abs(feats["dow_sin"]) < 1e-10
        assert abs(feats["dow_cos"] - 1.0) < 1e-10


# ---------------------------------------------------------------------------
# TestBuildRows
# ---------------------------------------------------------------------------

class TestBuildRows:
    _BASE = {
        "event_count": 50.0,
        "demand_lag_1h": 100.0,
        "demand_lag_24h": 95.0,
        "demand_lag_168h": 90.0,
        "demand_roll_3h": 98.0,
        "demand_roll_24h": 97.0,
        "temperature_c": 18.5,
        "humidity_pct": 65.0,
        "precip_mm": 0.0,
        "is_holiday": 0,
    }

    def test_shape(self):
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 6, self._BASE, as_of)
        assert df.shape == (6, len(FEATURE_COLS))

    def test_column_names(self):
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 6, self._BASE, as_of)
        assert list(df.columns) == FEATURE_COLS

    def test_city_constant(self):
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("tokyo", 3, self._BASE, as_of)
        assert (df["city"] == "tokyo").all()

    def test_lag_features_constant_across_horizon(self):
        """Lag/rolling features are held constant (static multi-step)."""
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 12, self._BASE, as_of)
        for col in ["demand_lag_1h", "demand_lag_24h", "demand_roll_3h"]:
            assert df[col].nunique() == 1, f"{col} should be constant across horizon"

    def test_time_features_vary_across_horizon(self):
        """hour_sin must differ across rows because the target hour changes."""
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 6, self._BASE, as_of)
        assert df["hour_sin"].nunique() > 1

    def test_horizon_1_single_row(self):
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 1, self._BASE, as_of)
        assert len(df) == 1

    def test_null_base_values_propagate(self):
        """None in base dict must produce NaN in the DataFrame (not crash)."""
        base = {**self._BASE, "demand_lag_1h": None, "temperature_c": None}
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 3, base, as_of)
        assert pd.isna(df["demand_lag_1h"]).all()
        assert pd.isna(df["temperature_c"]).all()

    def test_is_holiday_cast_to_float(self):
        """is_holiday is forced to float (0.0 or 1.0), never bool/int."""
        as_of = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)
        df = _build_rows("london", 1, {**self._BASE, "is_holiday": True}, as_of)
        assert df["is_holiday"].iloc[0] == 1.0
        assert isinstance(df["is_holiday"].iloc[0], float)
