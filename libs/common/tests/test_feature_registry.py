"""
Tests for common.features.registry.

Coverage
--------
TestFeature
    Feature dataclass: construction, frozen invariant, default_on_missing.

TestRegistry
    Registry: lookup, iteration, duplication guard, bulk accessors.

TestRouteFeatures
    Structural assertions against the live ROUTE_FEATURES constant.

TestNoUnregisteredReferences
    AST-scan all production Python files for ROUTE_FEATURES["<name>"] subscript
    accesses.  Fails if any name accessed this way is not in the registry.

    Intent: a developer writing ROUTE_FEATURES["new_feat"] before adding the
    entry to ROUTE_FEATURES will see this test fail, prompting them to register
    the feature first.

    Scope: only production code directories are scanned (libs/common/common/ and
    services/*/app/).  Test files are excluded to avoid false positives from
    deliberate negative-path tests such as:

        with pytest.raises(KeyError):
            _ = ROUTE_FEATURES["nonexistent_feature"]
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from common.features.registry import (
    BATCH_COMPUTED_AT_FIELD,
    ROUTE_FEATURES,
    ROUTE_REDIS_KEY_PATTERN,
    Feature,
    Registry,
)

# ---------------------------------------------------------------------------
# Repo layout — resolved relative to this file so the test is portable.
# File path: libs/common/tests/test_feature_registry.py
#   parent  → libs/common/tests/
#   ..      → libs/common/
#   ../..   → libs/
#   ../../..→ project root
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent.parent

# Production source directories to scan for unregistered references.
# Test files and the registry definition itself are excluded automatically by
# the scan logic (see _collect_registry_subscript_literals).
_PROD_DIRS = [
    _REPO_ROOT / "libs" / "common" / "common",
    _REPO_ROOT / "services" / "worker" / "app",
]

# Registry variable names recognised as "the feature registry" in source code.
_REGISTRY_VAR_NAMES = {"ROUTE_FEATURES"}


# ---------------------------------------------------------------------------
# TestFeature
# ---------------------------------------------------------------------------

class TestFeature:
    def test_construction_with_all_fields(self):
        feat = Feature(
            name="test_feat",
            dtype="float64",
            source="batch",
            freshness_sla_seconds=3600,
            description="A test feature.",
            default_on_missing=0.0,
        )
        assert feat.name == "test_feat"
        assert feat.dtype == "float64"
        assert feat.source == "batch"
        assert feat.freshness_sla_seconds == 3600
        assert feat.description == "A test feature."
        assert feat.default_on_missing == 0.0

    def test_default_on_missing_defaults_to_none(self):
        feat = Feature("f", "float64", "stream", 60, "desc")
        assert feat.default_on_missing is None

    def test_frozen_prevents_mutation(self):
        feat = Feature("f", "float64", "batch", 3600, "desc")
        with pytest.raises((AttributeError, TypeError)):
            feat.name = "changed"  # type: ignore[misc]

    def test_source_batch(self):
        feat = Feature("f", "float64", "batch", 86400, "desc")
        assert feat.source == "batch"

    def test_source_stream(self):
        feat = Feature("f", "float64", "stream", 300, "desc")
        assert feat.source == "stream"

    def test_equality_ignores_default_on_missing(self):
        """default_on_missing has compare=False so it is excluded from __eq__."""
        f1 = Feature("f", "float64", "batch", 3600, "desc", default_on_missing=0.0)
        f2 = Feature("f", "float64", "batch", 3600, "desc", default_on_missing=None)
        assert f1 == f2


# ---------------------------------------------------------------------------
# TestRegistry
# ---------------------------------------------------------------------------

class TestRegistry:
    def _make_reg(self) -> Registry:
        return Registry([
            Feature("a", "float64", "batch", 3600, "Feature A"),
            Feature("b", "float64", "stream", 60, "Feature B", default_on_missing=0.0),
        ])

    def test_lookup_by_name(self):
        reg = self._make_reg()
        assert reg["a"].description == "Feature A"
        assert reg["b"].default_on_missing == 0.0

    def test_missing_name_raises_key_error(self):
        reg = self._make_reg()
        with pytest.raises(KeyError, match="not registered"):
            _ = reg["nonexistent"]

    def test_contains_operator(self):
        reg = self._make_reg()
        assert "a" in reg
        assert "nonexistent" not in reg

    def test_len(self):
        reg = self._make_reg()
        assert len(reg) == 2

    def test_iter_order(self):
        reg = self._make_reg()
        names = [f.name for f in reg]
        assert names == ["a", "b"]

    def test_names_returns_ordered_list(self):
        reg = self._make_reg()
        assert reg.names() == ["a", "b"]

    def test_by_source_batch(self):
        reg = self._make_reg()
        batch = reg.by_source("batch")
        assert [f.name for f in batch] == ["a"]

    def test_by_source_stream(self):
        reg = self._make_reg()
        stream = reg.by_source("stream")
        assert [f.name for f in stream] == ["b"]

    def test_duplicate_names_raise_at_construction(self):
        feat = Feature("dup", "float64", "batch", 3600, "desc")
        with pytest.raises(ValueError, match="Duplicate"):
            Registry([feat, feat])

    def test_empty_registry(self):
        reg = Registry([])
        assert len(reg) == 0
        assert reg.names() == []


# ---------------------------------------------------------------------------
# TestRouteFeatures
# ---------------------------------------------------------------------------

class TestRouteFeatures:
    def test_all_features_have_required_fields(self):
        for feat in ROUTE_FEATURES:
            assert feat.name, f"name empty on {feat}"
            assert feat.dtype, f"dtype empty on {feat.name}"
            assert feat.source in ("batch", "stream"), (
                f"{feat.name}.source={feat.source!r} must be 'batch' or 'stream'"
            )
            assert feat.freshness_sla_seconds > 0, (
                f"{feat.name}.freshness_sla_seconds must be > 0"
            )
            assert feat.description, f"description empty on {feat.name}"

    def test_all_seven_dow_seasonality_features_present(self):
        dow_names = {f"seasonality_{d}" for d in ("mon","tue","wed","thu","fri","sat","sun")}
        missing = dow_names - set(ROUTE_FEATURES.names())
        assert not missing, f"Missing DOW seasonality features: {missing}"

    def test_avg_bookings_90d_present_and_typed(self):
        feat = ROUTE_FEATURES["avg_bookings_90d"]
        assert feat.dtype == "float64"
        assert feat.source == "batch"
        assert feat.freshness_sla_seconds == 86_400

    def test_cancellation_rate_has_non_none_default(self):
        """cancellation_rate_180d should default to 0.0 when absent from Redis."""
        feat = ROUTE_FEATURES["cancellation_rate_180d"]
        assert feat.default_on_missing == 0.0

    def test_redis_key_pattern(self):
        key = ROUTE_REDIS_KEY_PATTERN.format(route_id="london")
        assert key == "feat:route:london"

    def test_batch_computed_at_field_constant(self):
        assert BATCH_COMPUTED_AT_FIELD == "batch_computed_at"

    def test_stream_features_present(self):
        """Registry must contain at least the six expected stream features."""
        expected = {
            "searches_5m", "bookings_15m", "look_to_book_1h",
            "weather_temp_c", "weather_precip_mm", "weather_condition",
        }
        registered_stream = {f.name for f in ROUTE_FEATURES.by_source("stream")}
        missing = expected - registered_stream
        assert not missing, f"Missing stream features in ROUTE_FEATURES: {missing}"

    def test_stream_features_have_correct_source(self):
        for feat in ROUTE_FEATURES.by_source("stream"):
            assert feat.source == "stream"


# ---------------------------------------------------------------------------
# TestNoUnregisteredReferences
# ---------------------------------------------------------------------------

def _is_registry_name(node: ast.expr) -> bool:
    """Return True if *node* is a Name or dotted Attribute in _REGISTRY_VAR_NAMES."""
    if isinstance(node, ast.Name):
        return node.id in _REGISTRY_VAR_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in _REGISTRY_VAR_NAMES
    return False


def _collect_registry_subscript_literals(
    dirs: list[Path],
) -> list[tuple[Path, int, str]]:
    """Walk *dirs* for ROUTE_FEATURES["<literal>"] and return (file, line, name).

    Files named ``test_*.py`` and the registry definition file itself are
    excluded from the scan.
    """
    found: list[tuple[Path, int, str]] = []

    for root in dirs:
        for py_file in sorted(root.rglob("*.py")):
            # Exclude test files and the registry source itself.
            if py_file.name.startswith("test_"):
                continue
            if py_file.name in ("registry.py", "feature_registry.py"):
                continue

            source = py_file.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(py_file))
            except SyntaxError:
                continue

            for node in ast.walk(tree):
                if not isinstance(node, ast.Subscript):
                    continue
                if not _is_registry_name(node.value):
                    continue
                # ast.Subscript.slice is an ast.Constant in Python 3.9+.
                if not isinstance(node.slice, ast.Constant):
                    continue
                if not isinstance(node.slice.value, str):
                    continue
                found.append((py_file, node.lineno, node.slice.value))

    return found


class TestNoUnregisteredReferences:
    """Fail if any production file accesses ROUTE_FEATURES with an unregistered name.

    This is the complement of TestNoStringLiterals in test_feature_store.py:
    - TestNoStringLiterals: registered names must NOT appear as literals (use the registry)
    - TestNoUnregisteredReferences: subscript names that DO appear must BE registered
    """

    def test_all_registry_subscript_accesses_are_registered(self):
        registered = set(ROUTE_FEATURES.names())
        accesses = _collect_registry_subscript_literals(_PROD_DIRS)

        violations = [
            f"{path.relative_to(_REPO_ROOT)}:{line}  ROUTE_FEATURES[{name!r}]"
            for path, line, name in accesses
            if name not in registered
        ]

        assert not violations, (
            "The following ROUTE_FEATURES subscript accesses use names that are "
            "not in the registry.  Add the feature to ROUTE_FEATURES first:\n"
            + "\n".join(f"  {v}" for v in violations)
        )
