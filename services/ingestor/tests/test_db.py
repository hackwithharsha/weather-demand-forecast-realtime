"""
Unit tests for app.db — row-building logic, column ordering, null handling.

All tests are pure-Python: no Postgres connection required.  The actual
INSERT SQL is verified structurally (column list matches tuple order) rather
than executed against a live database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from app.db import _Buffered, batch_insert_demand_events, batch_insert_weather_readings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc).isoformat()


def _demand_item(
    event_id: str = "evt-001",
    city: str = "london",
    *,
    partition: int = 0,
    offset: int = 0,
    temperature_c: float | None = 20.0,
    condition: str | None = "clear",
) -> _Buffered:
    return _Buffered(
        partition=partition,
        offset=offset,
        key=None,
        raw_value=b"{}",
        schema_version=1,
        event_id=event_id,
        payload={
            "city": city,
            "event_type": "search",
            "sim_ts": _TS,
            "quantity": 5.0,
            "temperature_c": temperature_c,
            "condition": condition,
        },
    )


def _weather_item(
    event_id: str = "w-001",
    city: str = "london",
    *,
    partition: int = 0,
    offset: int = 0,
) -> _Buffered:
    return _Buffered(
        partition=partition,
        offset=offset,
        key=None,
        raw_value=b"{}",
        schema_version=1,
        event_id=event_id,
        payload={
            "city": city,
            "polled_at": _TS,
            "temperature_c": 18.5,
            "feels_like_c": 17.0,
            "dew_point_c": 12.0,
            "humidity_pct": 65.0,
            "wind_kph": 20.0,
            "wind_direction_deg": 270.0,
            "cloud_cover_pct": 40.0,
            "precip_probability_pct": 10.0,
            "precip_mm": 0.0,
            "condition": "clear",
        },
    )


def _fake_conn():
    """Return a MagicMock that records execute_values calls."""
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


# ---------------------------------------------------------------------------
# _Buffered dataclass
# ---------------------------------------------------------------------------

class TestBuffered:
    def test_fields_accessible(self):
        item = _demand_item()
        assert item.event_id == "evt-001"
        assert item.schema_version == 1
        assert item.partition == 0
        assert item.offset == 0

    def test_payload_is_dict(self):
        item = _demand_item()
        assert isinstance(item.payload, dict)
        assert "city" in item.payload


# ---------------------------------------------------------------------------
# batch_insert_demand_events — column ordering and null handling
# ---------------------------------------------------------------------------

class TestBatchInsertDemandEvents:
    def _capture_rows(self, items: list[_Buffered]) -> list[tuple]:
        conn, cur = _fake_conn()
        with patch("app.db.psycopg2.extras.execute_values") as mock_ev:
            batch_insert_demand_events(conn, items)
            assert mock_ev.called
            _, _, rows = mock_ev.call_args[0]
            return rows

    def test_column_order(self):
        """Tuple order must match the INSERT column list in batch_insert_demand_events."""
        # INSERT: city, event_type, sim_ts, quantity, temperature_c, condition,
        #         kafka_partition, kafka_offset, schema_version, event_id
        item = _demand_item("evt-1", "tokyo", partition=3, offset=99)
        rows = self._capture_rows([item])

        assert len(rows) == 1
        (city, event_type, sim_ts, quantity,
         temperature_c, condition,
         kafka_partition, kafka_offset, schema_version, event_id) = rows[0]

        assert city == "tokyo"
        assert event_type == "search"
        assert sim_ts == _TS
        assert quantity == 5.0
        assert temperature_c == 20.0
        assert condition == "clear"
        assert kafka_partition == 3
        assert kafka_offset == 99
        assert schema_version == 1
        assert event_id == "evt-1"

    def test_null_optional_fields_pass_through(self):
        """temperature_c and condition are optional; None must reach the tuple."""
        item = _demand_item(temperature_c=None, condition=None)
        rows = self._capture_rows([item])
        row = rows[0]
        assert row[4] is None  # temperature_c
        assert row[5] is None  # condition

    def test_multiple_items_produce_multiple_rows(self):
        items = [_demand_item(f"evt-{i}") for i in range(5)]
        rows = self._capture_rows(items)
        assert len(rows) == 5

    def test_commit_called_once(self):
        conn, _ = _fake_conn()
        with patch("app.db.psycopg2.extras.execute_values"):
            batch_insert_demand_events(conn, [_demand_item()])
        conn.commit.assert_called_once()

    def test_event_ids_preserved_in_order(self):
        ids = ["a", "b", "c"]
        items = [_demand_item(eid) for eid in ids]
        rows = self._capture_rows(items)
        assert [r[-1] for r in rows] == ids  # event_id is last column


# ---------------------------------------------------------------------------
# batch_insert_weather_readings — column ordering
# ---------------------------------------------------------------------------

class TestBatchInsertWeatherReadings:
    def _capture_rows(self, items: list[_Buffered]) -> list[tuple]:
        conn, cur = _fake_conn()
        with patch("app.db.psycopg2.extras.execute_values") as mock_ev:
            batch_insert_weather_readings(conn, items)
            assert mock_ev.called
            _, _, rows = mock_ev.call_args[0]
            return rows

    def test_column_order(self):
        """Tuple order must match INSERT: city, polled_at, temperature_c,
        feels_like_c, dew_point_c, humidity_pct, wind_kph, wind_direction_deg,
        cloud_cover_pct, precip_probability_pct, precip_mm, condition,
        kafka_partition, kafka_offset, schema_version, event_id
        """
        item = _weather_item("w-1", "berlin", partition=1, offset=42)
        rows = self._capture_rows([item])

        assert len(rows) == 1
        (city, polled_at, temperature_c, feels_like_c, dew_point_c,
         humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct,
         precip_probability_pct, precip_mm, condition,
         kafka_partition, kafka_offset, schema_version, event_id) = rows[0]

        assert city == "berlin"
        assert polled_at == _TS
        assert temperature_c == 18.5
        assert feels_like_c == 17.0
        assert dew_point_c == 12.0
        assert humidity_pct == 65.0
        assert wind_kph == 20.0
        assert wind_direction_deg == 270.0
        assert cloud_cover_pct == 40.0
        assert precip_probability_pct == 10.0
        assert precip_mm == 0.0
        assert condition == "clear"
        assert kafka_partition == 1
        assert kafka_offset == 42
        assert schema_version == 1
        assert event_id == "w-1"

    def test_missing_optional_field_is_none(self):
        """Fields accessed via .get() must be None when absent from payload."""
        item = _weather_item()
        item.payload.pop("feels_like_c")
        rows = self._capture_rows([item])
        assert rows[0][3] is None  # feels_like_c

    def test_commit_called_once(self):
        conn, _ = _fake_conn()
        with patch("app.db.psycopg2.extras.execute_values"):
            batch_insert_weather_readings(conn, [_weather_item()])
        conn.commit.assert_called_once()

    def test_event_ids_preserved_in_order(self):
        ids = ["w-1", "w-2", "w-3"]
        items = [_weather_item(eid) for eid in ids]
        rows = self._capture_rows(items)
        assert [r[-1] for r in rows] == ids
