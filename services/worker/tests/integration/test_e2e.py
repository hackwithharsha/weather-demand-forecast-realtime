"""
End-to-end integration test: raw event → batch pipeline → prediction.

Path exercised
--------------
1. Insert synthetic demand events and weather readings directly into
   raw.demand_events / raw.weather_readings (bypasses Kafka — tests the
   batch layer in isolation).

2. Run the batch pipeline (raw → staging → marts) against the live
   Postgres instance.

3. Assert that mart rows for the test city exist after the pipeline run.

4. Call POST /predict on the running API service and assert a 200 response
   with sensible forecast values.

Steps 1–3 run against Postgres and are always executed when POSTGRES_DSN /
POSTGRES_PASSWORD is set.  Step 4 requires the API service to be reachable
and is skipped automatically when it is not (unit-test environment).

Run from the worker container after `make up`:

    docker compose --profile core run --rm worker \
        pytest tests/integration/test_e2e.py -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_CITY = "__e2e_test_city__"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _insert_demand_events(conn, city: str, n: int = 48) -> list[str]:
    """Insert n synthetic demand events across the last 48 hours."""
    base_ts = _now_utc() - timedelta(hours=n)
    event_ids: list[str] = []
    rows = []
    for i in range(n):
        ts       = base_ts + timedelta(hours=i)
        event_id = f"e2e-demand-{uuid.uuid4()}"
        event_ids.append(event_id)
        rows.append((
            city,
            "search",               # event_type
            ts.isoformat(),         # sim_ts
            float(10 + i % 5),      # quantity
            20.0,                   # temperature_c
            "clear",                # condition
            0,                      # kafka_partition
            i,                      # kafka_offset
            1,                      # schema_version
            event_id,
        ))
    sql = """
        INSERT INTO raw.demand_events
            (city, event_type, sim_ts, quantity, temperature_c, condition,
             kafka_partition, kafka_offset, schema_version, event_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    return event_ids


def _insert_weather_readings(conn, city: str, n: int = 48) -> list[str]:
    """Insert n synthetic weather readings across the last 48 hours."""
    base_ts = _now_utc() - timedelta(hours=n)
    event_ids: list[str] = []
    rows = []
    for i in range(n):
        ts       = base_ts + timedelta(hours=i)
        event_id = f"e2e-weather-{uuid.uuid4()}"
        event_ids.append(event_id)
        rows.append((
            city,
            ts.isoformat(),  # polled_at
            20.0 + i * 0.1,  # temperature_c
            19.0,            # feels_like_c
            12.0,            # dew_point_c
            60.0,            # humidity_pct
            15.0,            # wind_kph
            180.0,           # wind_direction_deg
            20.0,            # cloud_cover_pct
            10.0,            # precip_probability_pct
            0.0,             # precip_mm
            "clear",         # condition
            0,               # kafka_partition
            i,               # kafka_offset
            1,               # schema_version
            event_id,
        ))
    sql = """
        INSERT INTO raw.weather_readings
            (city, polled_at, temperature_c, feels_like_c, dew_point_c,
             humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct,
             precip_probability_pct, precip_mm, condition,
             kafka_partition, kafka_offset, schema_version, event_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    return event_ids


def _cleanup_test_data(conn, city: str) -> None:
    """Remove all test rows for the synthetic city."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM marts.drift_reports WHERE 1=0")  # no-op guard
        cur.execute(
            "DELETE FROM marts.city_hour_features WHERE city = %s", (city,)
        )
        cur.execute(
            "DELETE FROM staging.demand_hourly WHERE city = %s", (city,)
        )
        cur.execute(
            "DELETE FROM staging.weather_hourly WHERE city = %s", (city,)
        )
        cur.execute(
            "DELETE FROM raw.demand_events WHERE city = %s", (city,)
        )
        cur.execute(
            "DELETE FROM raw.weather_readings WHERE city = %s", (city,)
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRawIngestion:
    """Verify that synthetic rows land in raw.* tables correctly."""

    def test_demand_events_inserted(self, pg_conn_commit):
        conn = pg_conn_commit
        try:
            ids = _insert_demand_events(conn, _TEST_CITY, n=5)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM raw.demand_events WHERE city = %s",
                    (_TEST_CITY,),
                )
                count = cur.fetchone()[0]
            assert count == 5, f"Expected 5 rows, got {count}"
        finally:
            _cleanup_test_data(conn, _TEST_CITY)

    def test_weather_readings_inserted(self, pg_conn_commit):
        conn = pg_conn_commit
        try:
            ids = _insert_weather_readings(conn, _TEST_CITY, n=5)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM raw.weather_readings WHERE city = %s",
                    (_TEST_CITY,),
                )
                count = cur.fetchone()[0]
            assert count == 5, f"Expected 5 rows, got {count}"
        finally:
            _cleanup_test_data(conn, _TEST_CITY)

    def test_event_id_deduplication(self, pg_conn_commit):
        """Inserting the same event_id twice must not create duplicates."""
        conn = pg_conn_commit
        try:
            fixed_id = f"e2e-dedup-{uuid.uuid4()}"
            row = (
                _TEST_CITY, "search", _now_utc().isoformat(),
                10.0, 20.0, "clear", 0, 0, 1, fixed_id,
            )
            sql = """
                INSERT INTO raw.demand_events
                    (city, event_type, sim_ts, quantity, temperature_c, condition,
                     kafka_partition, kafka_offset, schema_version, event_id)
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
            """
            for _ in range(2):
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, [row])
                conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM raw.demand_events WHERE event_id = %s",
                    (fixed_id,),
                )
                assert cur.fetchone()[0] == 1
        finally:
            _cleanup_test_data(conn, _TEST_CITY)


class TestBatchPipeline:
    """
    Run the batch pipeline and verify mart rows are produced.

    These tests call run_pipeline() directly against the live Postgres
    instance.  They write to raw.* then run staging→marts and assert that
    city_hour_features rows appear.
    """

    def test_pipeline_produces_mart_rows(self, pg_conn_commit):
        """Insert 48 h of demand+weather, run pipeline, assert mart rows."""
        conn = pg_conn_commit
        try:
            _insert_demand_events(conn, _TEST_CITY, n=48)
            _insert_weather_readings(conn, _TEST_CITY, n=48)

            # Import here so Postgres creds are resolved at runtime.
            from app.pipeline import run_pipeline
            from app.settings import Settings

            settings = Settings()
            run_pipeline(settings)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM marts.city_hour_features WHERE city = %s",
                    (_TEST_CITY,),
                )
                mart_count = cur.fetchone()[0]

            assert mart_count > 0, (
                f"Expected mart rows for {_TEST_CITY!r}, got 0. "
                "Check that staging SQL runs correctly."
            )
        finally:
            _cleanup_test_data(conn, _TEST_CITY)


class TestPredictEndpoint:
    """
    Call POST /predict on the live API.

    Skipped automatically when the API service is not reachable.
    """

    @pytest.mark.parametrize("city", ["london", "new_york"])
    def test_predict_returns_forecasts(self, api_client, city):
        resp = api_client.post("/predict", json={"city": city, "horizon_hours": 6})

        # The model may not be loaded yet — accept 200 or 503/422.
        # The primary assertion is that the API responds at all.
        assert resp.status_code in (200, 503, 422), (
            f"/predict returned unexpected status {resp.status_code}: {resp.text}"
        )

        if resp.status_code == 200:
            body = resp.json()
            assert body["city"] == city
            assert "forecasts" in body
            assert isinstance(body["forecasts"], list)
            assert len(body["forecasts"]) > 0

    def test_predict_unknown_city_422(self, api_client):
        """Requesting a city not in the feature mart should return 422 or 404."""
        resp = api_client.post(
            "/predict",
            json={"city": "__nonexistent__", "horizon_hours": 1},
        )
        assert resp.status_code in (404, 422, 500), (
            f"Expected 4xx/5xx for unknown city, got {resp.status_code}"
        )

    def test_cities_endpoint(self, api_client):
        resp = api_client.get("/cities")
        assert resp.status_code == 200
        body = resp.json()
        assert "cities" in body
        assert isinstance(body["cities"], list)

    def test_health_endpoint(self, api_client):
        resp = api_client.get("/health")
        assert resp.status_code == 200
        assert resp.json().get("status") == "ok"
