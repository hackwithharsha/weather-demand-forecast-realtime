"""
Tests for app.stream_features.redis_ops.

Strategy
--------
Both public functions are tested with a fake Redis client that records every
call made to it and implements the minimal sorted-set and hash protocol needed
to exercise the real logic.

FakeRedis
  Implements ZADD, ZREMRANGEBYSCORE, ZCOUNT, HSET, and a minimal Pipeline.
  ZCOUNT returns the exact count from the in-memory sorted set so window
  boundary arithmetic can be verified precisely.

TestUpdateDemandFeatures
  - Phase 1: search / booking events ZADD to the correct sorted set; other
    event types (e.g. "cancellation") add no member.
  - Phase 2: both sets are trimmed to the 1-hour window on every call.
  - Phase 3: ZCOUNT uses the correct (lo, sim_epoch) bounds for each window.
  - Phase 4: HSET writes the three computed aggregates plus stream_computed_at.
  - Deduplication: re-delivering the same event_id leaves counts unchanged.
  - Look-to-book ratio: bookings / max(1, searches) with zero-search guard.

TestUpdateWeatherFeatures
  - Non-null fields written to the hash.
  - Null fields omitted from the mapping.
  - stream_computed_at always present.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from app.stream_features.redis_ops import update_demand_features, update_weather_features
from common.features.registry import (
    ROUTE_FEATURES,
    STREAM_BOOKINGS_KEY_PATTERN,
    STREAM_COMPUTED_AT_FIELD,
    STREAM_SEARCHES_KEY_PATTERN,
)

# ---------------------------------------------------------------------------
# Minimal fake Redis / Pipeline
# ---------------------------------------------------------------------------

class _FakeSortedSet:
    """In-memory sorted set: member → score."""

    def __init__(self) -> None:
        self._data: dict[str, float] = {}

    def zadd(self, mapping: dict[str, float]) -> None:
        for member, score in mapping.items():
            self._data[member] = score

    def zremrangebyscore(self, lo: float | str, hi: float | str) -> None:
        lo_f = float("-inf") if lo == "-inf" else float(lo)
        hi_f = float("inf")  if hi == "+inf" else float(hi)
        self._data = {
            m: s for m, s in self._data.items()
            if not (s >= lo_f and s <= hi_f)
        }

    def zcount(self, lo: float, hi: float) -> int:
        return sum(1 for s in self._data.values() if lo <= s <= hi)


class _FakePipeline:
    """Records commands and executes them against in-memory state."""

    def __init__(self, sets: dict[str, _FakeSortedSet]) -> None:
        self._sets = sets
        self._queue: list[tuple[str, Any]] = []

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self._queue.append(("zadd", (key, mapping)))

    def zremrangebyscore(self, key: str, lo: Any, hi: Any) -> None:
        self._queue.append(("zremrangebyscore", (key, lo, hi)))

    def zcount(self, key: str, lo: float, hi: float) -> None:
        self._queue.append(("zcount", (key, lo, hi)))

    def execute(self) -> list:
        results = []
        for op, args in self._queue:
            if op == "zadd":
                key, mapping = args
                self._sets.setdefault(key, _FakeSortedSet()).zadd(mapping)
                results.append(None)
            elif op == "zremrangebyscore":
                key, lo, hi = args
                self._sets.setdefault(key, _FakeSortedSet()).zremrangebyscore(lo, hi)
                results.append(None)
            elif op == "zcount":
                key, lo, hi = args
                results.append(self._sets.setdefault(key, _FakeSortedSet()).zcount(lo, hi))
        self._queue = []
        return results


class FakeRedis:
    """Thread-unsafe fake Redis for single-threaded tests."""

    def __init__(self) -> None:
        self._sets: dict[str, _FakeSortedSet] = {}
        self._hashes: dict[str, dict[str, str]] = {}

    def pipeline(self, *, transaction: bool = True) -> _FakePipeline:
        return _FakePipeline(self._sets)

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self._hashes.setdefault(key, {}).update(mapping)

    # Convenience helpers for assertions.
    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))

    def zcount(self, key: str, lo: float, hi: float) -> int:
        return self._sets.get(key, _FakeSortedSet()).zcount(lo, hi)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ROUTE_ID   = "london"
SIM_EPOCH  = 1_700_000_000.0  # arbitrary fixed epoch

_SEARCHES_KEY = STREAM_SEARCHES_KEY_PATTERN.format(route_id=ROUTE_ID)
_BOOKINGS_KEY = STREAM_BOOKINGS_KEY_PATTERN.format(route_id=ROUTE_ID)
_ROUTE_KEY    = f"feat:route:{ROUTE_ID}"

_F_SEARCHES_5M  = ROUTE_FEATURES["searches_5m"].name
_F_BOOKINGS_15M = ROUTE_FEATURES["bookings_15m"].name
_F_LOOK_TO_BOOK = ROUTE_FEATURES["look_to_book_1h"].name
_F_TEMP         = ROUTE_FEATURES["weather_temp_c"].name
_F_PRECIP       = ROUTE_FEATURES["weather_precip_mm"].name
_F_COND         = ROUTE_FEATURES["weather_condition"].name


def _demand(
    r: FakeRedis,
    *,
    event_type: str = "search",
    event_id: str = "evt-1",
    sim_epoch: float = SIM_EPOCH,
) -> None:
    update_demand_features(
        r,
        route_id=ROUTE_ID,
        event_id=event_id,
        event_type=event_type,
        sim_epoch=sim_epoch,
    )


# ---------------------------------------------------------------------------
# TestUpdateDemandFeatures
# ---------------------------------------------------------------------------

class TestUpdateDemandFeatures:

    # -- Phase 1: ZADD routing ------------------------------------------

    def test_search_event_added_to_searches_set(self):
        r = FakeRedis()
        _demand(r, event_type="search", sim_epoch=SIM_EPOCH)
        assert r.zcount(_SEARCHES_KEY, SIM_EPOCH - 300, SIM_EPOCH) == 1

    def test_booking_event_added_to_bookings_set(self):
        r = FakeRedis()
        _demand(r, event_type="booking", sim_epoch=SIM_EPOCH)
        assert r.zcount(_BOOKINGS_KEY, SIM_EPOCH - 900, SIM_EPOCH) == 1

    def test_other_event_types_not_added_to_either_set(self):
        r = FakeRedis()
        _demand(r, event_type="cancellation", sim_epoch=SIM_EPOCH)
        assert r.zcount(_SEARCHES_KEY, SIM_EPOCH - 3600, SIM_EPOCH) == 0
        assert r.zcount(_BOOKINGS_KEY, SIM_EPOCH - 3600, SIM_EPOCH) == 0

    def test_search_not_in_bookings_set(self):
        r = FakeRedis()
        _demand(r, event_type="search", sim_epoch=SIM_EPOCH)
        assert r.zcount(_BOOKINGS_KEY, SIM_EPOCH - 3600, SIM_EPOCH) == 0

    # -- Phase 1: ZADD idempotency (deduplication) ----------------------

    def test_same_event_id_does_not_double_count(self):
        r = FakeRedis()
        _demand(r, event_type="search", event_id="evt-dup", sim_epoch=SIM_EPOCH)
        _demand(r, event_type="search", event_id="evt-dup", sim_epoch=SIM_EPOCH)
        assert r.zcount(_SEARCHES_KEY, SIM_EPOCH - 300, SIM_EPOCH) == 1

    def test_different_event_ids_accumulate(self):
        r = FakeRedis()
        _demand(r, event_type="search", event_id="a", sim_epoch=SIM_EPOCH)
        _demand(r, event_type="search", event_id="b", sim_epoch=SIM_EPOCH)
        assert r.zcount(_SEARCHES_KEY, SIM_EPOCH - 300, SIM_EPOCH) == 2

    # -- Phase 2: trim (ZREMRANGEBYSCORE) --------------------------------

    def test_old_events_trimmed_outside_1h_window(self):
        r = FakeRedis()
        # Insert an event older than 1 hour.
        old_epoch = SIM_EPOCH - 3700  # 3700 s ago — outside 1-h window
        _demand(r, event_type="search", event_id="old", sim_epoch=old_epoch)
        # Now process a new event at SIM_EPOCH — triggers trim.
        _demand(r, event_type="search", event_id="new", sim_epoch=SIM_EPOCH)
        # Only the new event should remain.
        assert r.zcount(_SEARCHES_KEY, old_epoch, SIM_EPOCH) == 1

    # -- Phase 3: ZCOUNT window bounds -----------------------------------

    def test_searches_5m_counts_only_events_within_5_minutes(self):
        r = FakeRedis()
        # Event at t-400 (outside 5-min window) and t-100 (inside).
        update_demand_features(
            r, route_id=ROUTE_ID, event_id="a", event_type="search",
            sim_epoch=SIM_EPOCH - 400,
        )
        update_demand_features(
            r, route_id=ROUTE_ID, event_id="b", event_type="search",
            sim_epoch=SIM_EPOCH,
        )
        mapping = r.hgetall(_ROUTE_KEY)
        assert float(mapping[_F_SEARCHES_5M]) == 1.0

    def test_bookings_15m_counts_only_events_within_15_minutes(self):
        r = FakeRedis()
        update_demand_features(
            r, route_id=ROUTE_ID, event_id="old", event_type="booking",
            sim_epoch=SIM_EPOCH - 1000,  # outside 15-min window
        )
        update_demand_features(
            r, route_id=ROUTE_ID, event_id="new", event_type="booking",
            sim_epoch=SIM_EPOCH,
        )
        mapping = r.hgetall(_ROUTE_KEY)
        assert float(mapping[_F_BOOKINGS_15M]) == 1.0

    # -- Phase 4: HSET output --------------------------------------------

    def test_hset_writes_searches_5m_field(self):
        r = FakeRedis()
        _demand(r, event_type="search", sim_epoch=SIM_EPOCH)
        mapping = r.hgetall(_ROUTE_KEY)
        assert _F_SEARCHES_5M in mapping
        assert float(mapping[_F_SEARCHES_5M]) == 1.0

    def test_hset_writes_bookings_15m_field(self):
        r = FakeRedis()
        _demand(r, event_type="booking", sim_epoch=SIM_EPOCH)
        mapping = r.hgetall(_ROUTE_KEY)
        assert _F_BOOKINGS_15M in mapping
        assert float(mapping[_F_BOOKINGS_15M]) == 1.0

    def test_hset_writes_stream_computed_at(self):
        r = FakeRedis()
        _demand(r, sim_epoch=SIM_EPOCH)
        mapping = r.hgetall(_ROUTE_KEY)
        assert STREAM_COMPUTED_AT_FIELD in mapping
        ts = mapping[STREAM_COMPUTED_AT_FIELD]
        # Must be a parseable UTC ISO-8601 string.
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None

    # -- Look-to-book ratio ----------------------------------------------

    def test_look_to_book_ratio_calculated_correctly(self):
        """3 bookings / 6 searches = 0.5."""
        r = FakeRedis()
        for i in range(6):
            update_demand_features(
                r, route_id=ROUTE_ID, event_id=f"s{i}", event_type="search",
                sim_epoch=SIM_EPOCH,
            )
        for i in range(3):
            update_demand_features(
                r, route_id=ROUTE_ID, event_id=f"b{i}", event_type="booking",
                sim_epoch=SIM_EPOCH,
            )
        mapping = r.hgetall(_ROUTE_KEY)
        assert abs(float(mapping[_F_LOOK_TO_BOOK]) - 0.5) < 1e-6

    def test_look_to_book_zero_searches_uses_denominator_of_1(self):
        """With 0 searches, denominator is max(1, 0) = 1; ratio = bookings."""
        r = FakeRedis()
        for i in range(2):
            update_demand_features(
                r, route_id=ROUTE_ID, event_id=f"b{i}", event_type="booking",
                sim_epoch=SIM_EPOCH,
            )
        mapping = r.hgetall(_ROUTE_KEY)
        # 2 bookings / max(1, 0 searches) = 2.0
        assert abs(float(mapping[_F_LOOK_TO_BOOK]) - 2.0) < 1e-6

    def test_look_to_book_zero_bookings_is_zero(self):
        r = FakeRedis()
        _demand(r, event_type="search", sim_epoch=SIM_EPOCH)
        mapping = r.hgetall(_ROUTE_KEY)
        assert float(mapping[_F_LOOK_TO_BOOK]) == 0.0


# ---------------------------------------------------------------------------
# TestUpdateWeatherFeatures
# ---------------------------------------------------------------------------

class TestUpdateWeatherFeatures:

    def _weather(
        self,
        r: FakeRedis,
        *,
        temperature_c: float | None = 20.0,
        precip_mm: float | None = 0.5,
        condition: str | None = "Clear",
    ) -> None:
        update_weather_features(
            r,
            route_id=ROUTE_ID,
            temperature_c=temperature_c,
            precip_mm=precip_mm,
            condition=condition,
        )

    def test_temperature_written_when_non_null(self):
        r = FakeRedis()
        self._weather(r, temperature_c=18.5)
        assert r.hgetall(_ROUTE_KEY)[_F_TEMP] == "18.5"

    def test_precip_written_when_non_null(self):
        r = FakeRedis()
        self._weather(r, precip_mm=2.3)
        assert r.hgetall(_ROUTE_KEY)[_F_PRECIP] == "2.3"

    def test_condition_written_when_non_null(self):
        r = FakeRedis()
        self._weather(r, condition="Rain")
        assert r.hgetall(_ROUTE_KEY)[_F_COND] == "Rain"

    def test_null_temperature_omitted(self):
        r = FakeRedis()
        self._weather(r, temperature_c=None)
        assert _F_TEMP not in r.hgetall(_ROUTE_KEY)

    def test_null_precip_omitted(self):
        r = FakeRedis()
        self._weather(r, precip_mm=None)
        assert _F_PRECIP not in r.hgetall(_ROUTE_KEY)

    def test_null_condition_omitted(self):
        r = FakeRedis()
        self._weather(r, condition=None)
        assert _F_COND not in r.hgetall(_ROUTE_KEY)

    def test_stream_computed_at_always_written(self):
        r = FakeRedis()
        self._weather(r, temperature_c=None, precip_mm=None, condition=None)
        mapping = r.hgetall(_ROUTE_KEY)
        assert STREAM_COMPUTED_AT_FIELD in mapping
        assert mapping[STREAM_COMPUTED_AT_FIELD]

    def test_all_non_null_fields_written_together(self):
        r = FakeRedis()
        self._weather(r, temperature_c=22.0, precip_mm=1.0, condition="Cloudy")
        mapping = r.hgetall(_ROUTE_KEY)
        assert mapping[_F_TEMP]   == "22.0"
        assert mapping[_F_PRECIP] == "1.0"
        assert mapping[_F_COND]   == "Cloudy"
        assert STREAM_COMPUTED_AT_FIELD in mapping

    def test_latest_value_overwrites_previous(self):
        r = FakeRedis()
        self._weather(r, temperature_c=10.0)
        self._weather(r, temperature_c=25.0)
        assert r.hgetall(_ROUTE_KEY)[_F_TEMP] == "25.0"

    def test_null_field_does_not_overwrite_previous_value(self):
        """Writing a null field must not erase a previously written value."""
        r = FakeRedis()
        self._weather(r, temperature_c=15.0, precip_mm=0.0, condition="Clear")
        # Second update omits temperature_c — must not erase the previous value.
        self._weather(r, temperature_c=None, precip_mm=0.5, condition="Rain")
        assert r.hgetall(_ROUTE_KEY)[_F_TEMP] == "15.0"
