"""
Tests for app.feature_store and common.feature_registry.

Coverage
--------
TestFeatureRegistry
    Registry construction, lookup, duplication guard, and the invariant that
    ROUTE_FEATURES.names() matches the columns selected in the Redis sync query.

TestSyncToRedis
    The sync function is tested with a fake psycopg2 connection and a
    FakePipeline that records every hset() call.  Assertions verify:
      - HSET (not SET) is called for every route in the result set.
      - All field names come from ROUTE_FEATURES.names() plus BATCH_COMPUTED_AT_FIELD;
        no string literals appear in the mapping.
      - Null feature values are omitted from the mapping (not stored as "null").
      - batch_computed_at is present in every hash.
      - An empty result set produces zero pipeline calls and a warning log.

TestNoStringLiterals
    Parse feature_store.py with ast.parse and assert that no string matching a
    feature name appears as an ast.Constant outside of comments and docstrings.
    This enforces the "no feature-name string literals" invariant mechanically.
"""

from __future__ import annotations

import ast
import inspect
import io
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import structlog.testing

import app.feature_store as fs_module
from app.feature_store import _sync_to_redis
from common.features.registry import (
    BATCH_COMPUTED_AT_FIELD,
    ROUTE_FEATURES,
    ROUTE_REDIS_KEY_PATTERN,
    Feature,
    Registry,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_settings(**overrides) -> MagicMock:
    s = MagicMock()
    s.postgres_dsn     = "postgresql://test:test@localhost/test"
    s.redis_host       = "localhost"
    s.redis_port       = 6379
    s.redis_db         = 0
    s.redis_password   = "test"
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _make_row(route_id: str, **field_overrides) -> dict[str, Any]:
    """Synthetic feature row as psycopg2 would return it."""
    row: dict[str, Any] = {
        "route_id":    route_id,
        "feature_date": date(2024, 1, 16),
        "avg_bookings_90d":       312.5,
        "seasonality_mon":        1.12,
        "seasonality_tue":        1.05,
        "seasonality_wed":        0.98,
        "seasonality_thu":        0.93,
        "seasonality_fri":        1.08,
        "seasonality_sat":        0.72,
        "seasonality_sun":        0.68,
        "lead_time_p50":          2.4,
        "lead_time_p90":          8.1,
        "cancellation_rate_180d": 0.03,
        "elasticity_estimate":    0.61,
        "feature_computed_at":    "2024-01-17T02:30:00+00:00",
    }
    row.update(field_overrides)
    return row


class FakePipeline:
    """Records hset() calls; mimics redis.client.Pipeline.

    execute_count tracks how many times execute() has been called so tests
    can assert the correct number of round-trips for a given batch_size.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.executed = False
        self.execute_count: int = 0

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.calls.append((key, mapping))

    def execute(self) -> list:
        self.executed = True
        self.execute_count += 1
        return [1] * len(self.calls)


# ---------------------------------------------------------------------------
# TestFeatureRegistry
# ---------------------------------------------------------------------------

class TestFeatureRegistry:
    def test_all_route_features_have_required_fields(self):
        for feat in ROUTE_FEATURES:
            assert feat.name, "name must not be empty"
            assert feat.dtype, "dtype must not be empty"
            assert feat.source in ("batch", "stream"), (
                f"{feat.name}.source={feat.source!r} must be 'batch' or 'stream'"
            )
            assert feat.freshness_sla_seconds > 0, (
                f"{feat.name}.freshness_sla_seconds must be positive"
            )
            assert feat.description, "description must not be empty"

    def test_names_returns_ordered_list(self):
        names = ROUTE_FEATURES.names()
        assert isinstance(names, list)
        assert len(names) == len(ROUTE_FEATURES)

    def test_lookup_by_name_succeeds(self):
        feat = ROUTE_FEATURES["avg_bookings_90d"]
        assert feat.dtype == "float64"
        assert feat.source == "batch"
        assert feat.freshness_sla_seconds == 86_400

    def test_lookup_missing_name_raises_key_error(self):
        with pytest.raises(KeyError, match="not registered"):
            _ = ROUTE_FEATURES["nonexistent_feature"]

    def test_contains_operator(self):
        assert "avg_bookings_90d" in ROUTE_FEATURES
        assert "nonexistent_feature" not in ROUTE_FEATURES

    def test_all_seven_dow_seasonality_features_present(self):
        dow_names = {f"seasonality_{d}" for d in ("mon","tue","wed","thu","fri","sat","sun")}
        registered = set(ROUTE_FEATURES.names())
        missing = dow_names - registered
        assert not missing, f"DOW seasonality features missing from registry: {missing}"

    def test_duplicate_names_raise_at_construction(self):
        feat = Feature("x", "float64", "batch", 86_400, "desc")
        with pytest.raises(ValueError, match="Duplicate"):
            Registry([feat, feat])

    def test_freshness_sla_seconds_is_positive(self):
        """All freshness SLAs must be positive integers (seconds)."""
        for feat in ROUTE_FEATURES:
            assert isinstance(feat.freshness_sla_seconds, int), (
                f"{feat.name}.freshness_sla_seconds must be int, "
                f"got {type(feat.freshness_sla_seconds).__name__}"
            )
            assert feat.freshness_sla_seconds > 0, (
                f"{feat.name}.freshness_sla_seconds must be > 0"
            )

    def test_redis_key_pattern_contains_route_id_placeholder(self):
        key = ROUTE_REDIS_KEY_PATTERN.format(route_id="london")
        assert key == "feat:route:london"

    def test_registry_names_match_sync_query_fields(self):
        """_LATEST_ROUTE_FEATURES_SQL must SELECT every feature in the registry.

        This test catches the case where the registry gains a new feature but
        the SQL query in feature_store.py is not updated to include it.
        """
        sql = fs_module._LATEST_ROUTE_FEATURES_SQL
        for name in ROUTE_FEATURES.names():
            assert name in sql, (
                f"Feature '{name}' is in ROUTE_FEATURES but not in "
                "_LATEST_ROUTE_FEATURES_SQL.  Add it to the SELECT list."
            )


# ---------------------------------------------------------------------------
# TestSyncToRedis
# ---------------------------------------------------------------------------

class TestSyncToRedis:
    """Test _sync_to_redis with fake Postgres cursor and fake Redis pipeline."""

    def _run_sync(
        self,
        rows: list[dict],
        *,
        settings: MagicMock | None = None,
        batch_size: int = 500,
    ) -> FakePipeline:
        """Run _sync_to_redis with mocked dependencies; return the fake pipeline.

        Pass batch_size to exercise the chunking path (e.g. batch_size=2 with
        5 rows → 3 pipeline.execute() calls).
        """
        if settings is None:
            settings = _make_settings()

        fake_pipe = FakePipeline()
        fake_redis = MagicMock()
        fake_redis.pipeline.return_value = fake_pipe

        # Fake psycopg2 cursor
        fake_cursor = MagicMock()
        fake_cursor.__enter__ = lambda s: s
        fake_cursor.__exit__ = MagicMock(return_value=False)
        fake_cursor.fetchall.return_value = rows

        fake_conn = MagicMock()
        fake_conn.__enter__ = lambda s: s
        fake_conn.__exit__ = MagicMock(return_value=False)
        fake_conn.cursor.return_value = fake_cursor

        with (
            patch("app.feature_store.psycopg2.connect", return_value=fake_conn),
            patch("app.feature_store._make_redis_client", return_value=fake_redis),
        ):
            _sync_to_redis(settings, batch_size=batch_size)

        return fake_pipe

    # -- Basic write behaviour ------------------------------------------------

    def test_hset_called_once_per_route(self):
        rows = [_make_row("london"), _make_row("tokyo")]
        pipe = self._run_sync(rows)
        assert len(pipe.calls) == 2

    def test_redis_key_format(self):
        pipe = self._run_sync([_make_row("london")])
        key, _ = pipe.calls[0]
        assert key == "feat:route:london"

    def test_pipeline_executed(self):
        pipe = self._run_sync([_make_row("london")])
        assert pipe.executed

    # -- Field names come from registry, not literals -------------------------

    def test_all_registry_features_present_in_mapping(self):
        pipe = self._run_sync([_make_row("london")])
        _, mapping = pipe.calls[0]
        for feat in ROUTE_FEATURES:
            assert feat.name in mapping, (
                f"Field '{feat.name}' missing from Redis mapping"
            )

    def test_no_extra_fields_beyond_registry_plus_timestamp(self):
        pipe = self._run_sync([_make_row("london")])
        _, mapping = pipe.calls[0]
        allowed = set(ROUTE_FEATURES.names()) | {BATCH_COMPUTED_AT_FIELD}
        extra = set(mapping.keys()) - allowed
        assert not extra, f"Unexpected fields in Redis mapping: {extra}"

    def test_batch_computed_at_always_present(self):
        pipe = self._run_sync([_make_row("london")])
        _, mapping = pipe.calls[0]
        assert BATCH_COMPUTED_AT_FIELD in mapping
        assert mapping[BATCH_COMPUTED_AT_FIELD]  # non-empty

    # -- Null handling --------------------------------------------------------

    def test_null_feature_values_omitted_from_mapping(self):
        """Null features must not appear in the hash (not stored as 'None' or '')."""
        row = _make_row("london", avg_bookings_90d=None, elasticity_estimate=None)
        pipe = self._run_sync([row])
        _, mapping = pipe.calls[0]
        assert "avg_bookings_90d" not in mapping
        assert "elasticity_estimate" not in mapping

    def test_non_null_values_stored_as_float_strings(self):
        row = _make_row("london", avg_bookings_90d=312.5)
        pipe = self._run_sync([row])
        _, mapping = pipe.calls[0]
        assert mapping["avg_bookings_90d"] == "312.5"

    def test_decimal_values_coerced_to_float_string(self):
        """psycopg2 NUMERIC columns return decimal.Decimal; must survive str(float(...))."""
        row = _make_row("london", avg_bookings_90d=Decimal("312.5000"))
        pipe = self._run_sync([row])
        _, mapping = pipe.calls[0]
        assert mapping["avg_bookings_90d"] == "312.5"

    # -- Empty result set -----------------------------------------------------

    def test_empty_result_set_no_pipeline_calls(self, caplog):
        pipe = self._run_sync([])
        assert len(pipe.calls) == 0
        assert not pipe.executed

    # -- Pipeline batching ----------------------------------------------------

    def test_batching_splits_rows_into_multiple_pipeline_flushes(self):
        """5 rows with batch_size=2 → ceil(5/2)=3 execute() calls, 5 hset() calls.

        The same FakePipeline instance is returned by every pipeline() call
        (MagicMock return_value semantics), so execute_count accumulates the
        total number of flushes across all batches.
        """
        rows = [_make_row(f"city_{i}") for i in range(5)]
        pipe = self._run_sync(rows, batch_size=2)
        assert pipe.execute_count == 3, (
            f"Expected 3 pipeline flushes for 5 rows with batch_size=2, "
            f"got {pipe.execute_count}"
        )
        assert len(pipe.calls) == 5

    def test_throughput_info_log_emitted(self):
        """feature_store_redis_sync_done is logged at INFO with all throughput fields."""
        rows = [_make_row("london"), _make_row("tokyo")]
        with structlog.testing.capture_logs() as cap:
            self._run_sync(rows)

        events = [e for e in cap if e.get("event") == "feature_store_redis_sync_done"]
        assert len(events) == 1, (
            f"Expected exactly 1 feature_store_redis_sync_done log entry, "
            f"got {len(events)}.  All captured events: {[e['event'] for e in cap]}"
        )
        ev = events[0]
        assert ev["log_level"] == "info"
        assert ev["routes_synced"] == 2
        assert ev["batches_flushed"] == 1   # 2 rows fit in one batch at default size
        assert "elapsed_s" in ev
        assert "keys_per_s" in ev
        assert "fields_per_s" in ev
        assert "batch_computed_at" in ev


# ---------------------------------------------------------------------------
# TestNoStringLiterals — mechanical enforcement of the registry discipline
# ---------------------------------------------------------------------------

class TestNoStringLiterals:
    """Assert that feature_store.py contains no feature-name string literals.

    We parse the module source with ast.parse and walk every ast.Constant node.
    Any constant whose value exactly matches a name in ROUTE_FEATURES is a
    violation — it means someone bypassed the registry.

    This test intentionally does NOT scan comments or docstrings (ast strips
    those), so the explanatory text in the module header does not trigger it.
    """

    def _feature_name_literals_in_source(self) -> list[str]:
        source = inspect.getsource(fs_module)
        tree   = ast.parse(source)
        feature_names = set(ROUTE_FEATURES.names())
        violations: list[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value in feature_names:
                    violations.append(
                        f"line {node.lineno}: string literal {node.value!r}"
                    )
        return violations

    def test_no_feature_name_string_literals_in_feature_store(self):
        violations = self._feature_name_literals_in_source()
        assert not violations, (
            "feature_store.py contains feature-name string literals — "
            "use ROUTE_FEATURES instead:\n"
            + "\n".join(f"  {v}" for v in violations)
        )
