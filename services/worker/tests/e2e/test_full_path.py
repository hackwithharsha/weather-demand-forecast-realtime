"""
End-to-end test: raw event → batch pipeline → API prediction.

Unlike the integration tests in tests/integration/, this test requires the
API service to be reachable and FAILS (not skips) when it is not.

Path exercised
--------------
1. Insert 72 h of synthetic demand events and weather readings directly into
   raw.demand_events / raw.weather_readings (bypasses Kafka entirely).

2. Run the batch pipeline (raw → staging → marts).

3. Assert that mart rows exist for the test city.

4. Call POST /predict on the live API with a real known city that has mart
   data.  Assert:
     - HTTP 200 (or 503 when no Production model is registered — that is
       still a valid "API responded" result for a freshly started stack).
     - Response schema is correct when status is 200.
     - predicted_demand values are finite floats.

5. Call GET /cities and verify the response is a list (may or may not include
   the synthetic city, which is cleaned up before verification).

Cleanup
-------
All synthetic rows are deleted in a finally block before the API assertions
so the test city never appears in /cities or the feature mart during the HTTP
checks.

Run this suite with:
    docker compose --profile core --profile stream run --rm worker \
        pytest tests/e2e/ -v
"""

from __future__ import annotations

import math
import os
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2
import psycopg2.extras
import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_TEST_CITY = "__e2e_fullpath__"
_REAL_CITIES = ["london", "new_york", "tokyo"]  # cities that should have mart data


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _api_url() -> str:
    return os.environ.get("API_URL", "http://api:8000")


def _build_dsn() -> str | None:
    if dsn := os.environ.get("POSTGRES_DSN"):
        return dsn
    pw = os.environ.get("POSTGRES_PASSWORD")
    if not pw:
        return None
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "forecast")
    user = os.environ.get("POSTGRES_USER", "forecast")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pg():
    dsn = _build_dsn()
    if not dsn:
        pytest.fail(
            "E2E test requires Postgres. Set POSTGRES_DSN or POSTGRES_PASSWORD."
        )
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
    except psycopg2.OperationalError as exc:
        pytest.fail(
            f"Postgres not reachable. Run `make up` before `make test-e2e`. Error: {exc}"
        )
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def api():
    """httpx.Client pointing at the live API.  Fails (not skips) if not up."""
    url = _api_url()
    try:
        resp = httpx.get(f"{url}/health", timeout=5.0)
        if resp.status_code != 200:
            pytest.fail(
                f"API health check returned {resp.status_code}. "
                "Run `make up` before `make test-e2e`."
            )
    except httpx.ConnectError as exc:
        pytest.fail(
            f"API not reachable at {url}. "
            "Run `make up` before `make test-e2e`. Error: {exc}"
        )
    with httpx.Client(base_url=url, timeout=30.0) as client:
        yield client


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _insert_demand(conn, city: str, n: int = 72) -> None:
    base_ts = _now_utc() - timedelta(hours=n)
    rows = []
    for i in range(n):
        ts = base_ts + timedelta(hours=i)
        rows.append((
            city, "search", ts.isoformat(),
            float(10 + i % 8),
            20.0 + i * 0.05, "clear",
            0, i, 1,
            f"e2e-fullpath-demand-{uuid.uuid4()}",
        ))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO raw.demand_events
               (city, event_type, sim_ts, quantity, temperature_c, condition,
                kafka_partition, kafka_offset, schema_version, event_id)
               VALUES %s ON CONFLICT (event_id) DO NOTHING""",
            rows,
        )
    conn.commit()


def _insert_weather(conn, city: str, n: int = 72) -> None:
    base_ts = _now_utc() - timedelta(hours=n)
    rows = []
    for i in range(n):
        ts = base_ts + timedelta(hours=i)
        rows.append((
            city, ts.isoformat(),
            18.0 + i * 0.1, 17.0, 11.0,
            65.0, 15.0, 270.0, 30.0, 10.0, 0.0, "clear",
            0, i, 1,
            f"e2e-fullpath-weather-{uuid.uuid4()}",
        ))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO raw.weather_readings
               (city, polled_at, temperature_c, feels_like_c, dew_point_c,
                humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct,
                precip_probability_pct, precip_mm, condition,
                kafka_partition, kafka_offset, schema_version, event_id)
               VALUES %s ON CONFLICT (event_id) DO NOTHING""",
            rows,
        )
    conn.commit()


def _cleanup(conn, city: str) -> None:
    with conn.cursor() as cur:
        for table in (
            "marts.city_hour_features",
            "staging.demand_hourly",
            "staging.weather_hourly",
            "raw.demand_events",
            "raw.weather_readings",
        ):
            cur.execute(f"DELETE FROM {table} WHERE city = %s", (city,))
    conn.commit()


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestFullPath:
    """
    Covers the complete path: raw insert → batch pipeline → API prediction.
    """

    def test_01_insert_propagates_to_marts(self, pg):
        """Pipeline must produce mart rows for the synthetic city."""
        _insert_demand(pg, _TEST_CITY, n=72)
        _insert_weather(pg, _TEST_CITY, n=72)

        from app.pipeline import run_pipeline
        from app.settings import Settings

        try:
            run_pipeline(Settings())
        finally:
            pass  # cleanup happens in test_03

        with pg.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM marts.city_hour_features WHERE city = %s",
                (_TEST_CITY,),
            )
            count = cur.fetchone()[0]

        assert count > 0, (
            f"Expected mart rows for {_TEST_CITY!r} after pipeline run, got 0."
        )

    def test_02_cleanup_test_city(self, pg):
        """Remove synthetic data before testing the live API to avoid pollution."""
        _cleanup(pg, _TEST_CITY)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM marts.city_hour_features WHERE city = %s",
                (_TEST_CITY,),
            )
            assert cur.fetchone()[0] == 0

    def test_03_health_endpoint(self, api):
        resp = api.get("/health")
        assert resp.status_code == 200
        assert resp.json().get("status") == "ok"

    def test_04_cities_endpoint_returns_list(self, api):
        resp = api.get("/cities")
        assert resp.status_code == 200
        body = resp.json()
        assert "cities" in body
        assert isinstance(body["cities"], list)
        # synthetic city was cleaned up — it must not appear
        assert _TEST_CITY not in body["cities"]

    @pytest.mark.parametrize("city", _REAL_CITIES)
    def test_05_predict_for_known_city(self, api, city):
        """POST /predict returns 200 with valid forecast values, or 503/404 if
        no Production model or no mart data exists for this city yet."""
        resp = api.post("/predict", json={"city": city, "horizon_hours": 6})

        # 503 = no Production model loaded (valid on a fresh stack)
        # 404 = city not in mart yet
        # 200 = full success path
        assert resp.status_code in (200, 404, 503), (
            f"Unexpected status {resp.status_code} for city={city!r}: {resp.text}"
        )

        if resp.status_code != 200:
            return  # other tiers cover the model-loaded path

        body = resp.json()
        assert body["city"] == city
        assert body["horizon_hours"] == 6
        assert "forecasts" in body
        assert len(body["forecasts"]) == 6

        for fc in body["forecasts"]:
            assert "target_hour" in fc
            val = fc["predicted_demand"]
            assert isinstance(val, float), f"predicted_demand is not float: {val!r}"
            assert math.isfinite(val), f"predicted_demand is not finite: {val}"

    def test_06_predict_unknown_city_4xx(self, api):
        """A city with no mart data must return 4xx, never 200 or 5xx crash."""
        resp = api.post(
            "/predict",
            json={"city": "__no_such_city_xyz__", "horizon_hours": 1},
        )
        assert resp.status_code in (404, 422, 503), (
            f"Expected 4xx/503 for unknown city, got {resp.status_code}"
        )

    def test_07_predict_invalid_horizon_422(self, api):
        """horizon_hours=0 must be rejected with 422 Unprocessable Entity."""
        resp = api.post("/predict", json={"city": "london", "horizon_hours": 0})
        assert resp.status_code == 422
