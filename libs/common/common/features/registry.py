"""
Feature registry: single source of truth for every feature definition.

Both the batch sync job (services/worker) and the serving API (services/api)
import from this module.  No service writes feature-name strings as literals.

Adding a new feature
--------------------
1. Add a ``Feature`` entry to ``ROUTE_FEATURES`` below.
2. Add the corresponding column to the Postgres mart table (Alembic migration).
3. Add the column to the SQL file that populates the mart table.

The batch sync job reads ``registry.names()`` to build the Redis HSET mapping.
The API reads the same list to know which hash fields to fetch and how to cast
the stored strings back to typed values.  Neither service owns a hard-coded
list of feature names.

Field conventions
-----------------
``name``                Snake-case Python identifier.  Used verbatim as the
                        Redis hash field name AND the Postgres column name, so
                        changing a name requires a coordinated migration + deploy.
``dtype``               NumPy / Arrow canonical dtype string.  Governs how the
                        API casts the Redis string value back to a Python type.
``source``              Either ``"batch"`` (computed offline by the worker and
                        written to Redis on a nightly schedule) or ``"stream"``
                        (computed online from live events and written in near
                        real-time).
``freshness_sla_seconds``  Maximum acceptable staleness in seconds at serving
                        time.  The API should emit an alert if the age of
                        ``batch_computed_at`` exceeds this value.
``description``         Human-readable purpose; surfaced in documentation and
                        monitoring dashboards.
``default_on_missing``  Value returned by the API when the feature's hash field
                        is absent from Redis.  ``None`` means "no default —
                        treat a missing field as an error / stale-feature alert".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = [
    "Feature",
    "Registry",
    "ROUTE_FEATURES",
    "ROUTE_REDIS_KEY_PATTERN",
    "BATCH_COMPUTED_AT_FIELD",
    "SYNC_COMPLETED_AT_KEY",
    "STREAM_COMPUTED_AT_FIELD",
    "STREAM_SEARCHES_KEY_PATTERN",
    "STREAM_BOOKINGS_KEY_PATTERN",
]

Source = Literal["batch", "stream"]


@dataclass(frozen=True)
class Feature:
    """Immutable metadata descriptor for a single feature."""

    name: str
    dtype: str
    source: Source
    freshness_sla_seconds: int
    description: str
    default_on_missing: Any = field(default=None, compare=False)


class Registry:
    """Ordered, name-indexed collection of :class:`Feature` objects.

    Examples
    --------
    >>> reg = Registry([Feature("x", "float64", "batch", 86400, "desc")])
    >>> reg["x"].dtype
    'float64'
    >>> reg.names()
    ['x']
    >>> list(reg)
    [Feature(name='x', ...)]
    """

    def __init__(self, features: list[Feature]) -> None:
        self._features = list(features)
        self._by_name: dict[str, Feature] = {f.name: f for f in self._features}
        if len(self._by_name) != len(self._features):
            seen: set[str] = set()
            dupes = [
                f.name
                for f in self._features
                if f.name in seen or seen.add(f.name)  # type: ignore[func-returns-value]
            ]
            raise ValueError(f"Duplicate feature names in registry: {dupes}")

    # ------------------------------------------------------------------
    # Mapping-like access
    # ------------------------------------------------------------------

    def __getitem__(self, name: str) -> Feature:
        try:
            return self._by_name[name]
        except KeyError:
            raise KeyError(
                f"Feature {name!r} is not registered.  "
                f"Known features: {self.names()}"
            ) from None

    def __contains__(self, name: object) -> bool:
        return name in self._by_name

    def __iter__(self):
        return iter(self._features)

    def __len__(self) -> int:
        return len(self._features)

    # ------------------------------------------------------------------
    # Bulk accessors used by the batch job and API
    # ------------------------------------------------------------------

    def names(self) -> list[str]:
        """Ordered list of feature names; mirrors the mart column order.

        The batch job iterates this list to build the Redis HSET mapping.
        The API iterates this list to build the HMGET field list.
        """
        return [f.name for f in self._features]

    def by_source(self, source: Source) -> list[Feature]:
        """Return all features whose ``source`` matches *source*."""
        return [f for f in self._features if f.source == source]


# ---------------------------------------------------------------------------
# Route feature registry
# ---------------------------------------------------------------------------

#: Redis key template for route features.
#: Format: ``ROUTE_REDIS_KEY_PATTERN.format(route_id="london")``
ROUTE_REDIS_KEY_PATTERN: str = "feat:route:{route_id}"

#: Hash field written alongside feature values that records when the batch ran.
#: Used by the API to enforce ``freshness_sla_seconds`` and emit staleness alerts.
BATCH_COMPUTED_AT_FIELD: str = "batch_computed_at"

#: Plain-string Redis key written as the LAST operation of a fully-successful
#: ``_sync_to_redis()`` run.  Its value equals the ``batch_computed_at``
#: timestamp shared by every route hash written in that run.
#:
#: If this key's value matches a route's ``batch_computed_at`` field, that route
#: was confirmed complete.  If a route's ``batch_computed_at`` is *newer* than
#: this sentinel, the route was written in a run that did not finish.
#:
#: See docs/decisions.md §"Partial-failure handling for the nightly Redis sync".
SYNC_COMPLETED_AT_KEY: str = "feat:sync:completed_at"

# ---------------------------------------------------------------------------
# Stream feature Redis key patterns
# ---------------------------------------------------------------------------

#: Hash field written by the stream consumer alongside stream feature values.
#: Records when the most recent stream event for this route was processed.
STREAM_COMPUTED_AT_FIELD: str = "stream_computed_at"

#: Sorted set key for search events.  score = sim_ts epoch (float seconds).
#: member = event_id (deduplicates at-least-once Kafka delivery).
#: Trimmed to 1 hour (the longest sliding window) on every write.
#: See docs/decisions.md §"Sliding-window counters".
STREAM_SEARCHES_KEY_PATTERN: str = "feat:stream:{route_id}:searches"

#: Sorted set key for booking events.  Same score/member semantics as above.
STREAM_BOOKINGS_KEY_PATTERN: str = "feat:stream:{route_id}:bookings"

# Day-of-week order: 1=Mon … 7=Sun (ISO 8601 / PostgreSQL EXTRACT(ISODOW)).
_DOW_ABBRS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

# 24 hours expressed in seconds — the SLA for all nightly-batch features.
_NIGHTLY_SLA_S = 86_400

ROUTE_FEATURES: Registry = Registry([
    Feature(
        name="avg_bookings_90d",
        dtype="float64",
        source="batch",
        freshness_sla_seconds=_NIGHTLY_SLA_S,
        description=(
            "Rolling 90-day average of daily bookings for the route, "
            "computed over the 90 calendar days that precede feature_date. "
            "Null when fewer than one full day of history exists."
        ),
    ),
    # Seven seasonality columns — one per ISO day-of-week.
    # seasonality_{dow} = (avg demand on that DOW) / (route overall avg demand).
    # > 1 means above-average demand on that day; < 1 means below-average.
    *[
        Feature(
            name=f"seasonality_{dow}",
            dtype="float64",
            source="batch",
            freshness_sla_seconds=_NIGHTLY_SLA_S,
            description=(
                f"Seasonality index for {dow.capitalize()}s: ratio of the "
                f"route's average {dow.capitalize()} demand to its overall "
                "daily average across all history. "
                "Null when fewer than one occurrence of this weekday is recorded."
            ),
        )
        for dow in _DOW_ABBRS
    ],
    Feature(
        name="lead_time_p50",
        dtype="float64",
        source="batch",
        freshness_sla_seconds=_NIGHTLY_SLA_S,
        description=(
            "Median booking lead time in hours (P50 of ingested_at − sim_ts) "
            "over the 90 days preceding feature_date. "
            "Positive values indicate advance-booking patterns."
        ),
    ),
    Feature(
        name="lead_time_p90",
        dtype="float64",
        source="batch",
        freshness_sla_seconds=_NIGHTLY_SLA_S,
        description=(
            "90th-percentile booking lead time in hours over the 90 days "
            "preceding feature_date. High P90 signals a long-tail of "
            "far-in-advance purchases that can inflate demand forecasts."
        ),
    ),
    Feature(
        name="cancellation_rate_180d",
        dtype="float64",
        source="batch",
        freshness_sla_seconds=_NIGHTLY_SLA_S,
        description=(
            "Fraction of demand events with event_type='cancellation' over "
            "the 180 days preceding feature_date.  Range [0, 1]; 0.0 when "
            "no cancellation events are recorded."
        ),
        default_on_missing=0.0,
    ),
    Feature(
        name="elasticity_estimate",
        dtype="float64",
        source="batch",
        freshness_sla_seconds=_NIGHTLY_SLA_S,
        description=(
            "Pearson correlation between temperature_c and total_demand over "
            "all available history as of feature_date.  Positive = demand "
            "rises with temperature (summer-peak routes); negative = inverse "
            "(cold-weather routes).  Null when temperature data is missing."
        ),
    ),

    # ── Stream features (online, written by the demand/weather consumers) ──
    # These occupy disjoint HSET fields in the same feat:route:{id} hash.
    # The sliding-window state is maintained in separate sorted sets;
    # only the computed aggregate values land in the hash.
    # See docs/decisions.md §"Sliding-window counters".

    Feature(
        name="searches_5m",
        dtype="float64",
        source="stream",
        freshness_sla_seconds=300,  # 5 minutes
        description=(
            "Count of demand events with event_type='search' in the "
            "5-minute sliding window ending at the last event's sim_ts.  "
            "Updated on every demand event for this route."
        ),
        default_on_missing=0.0,
    ),
    Feature(
        name="bookings_15m",
        dtype="float64",
        source="stream",
        freshness_sla_seconds=900,  # 15 minutes
        description=(
            "Count of demand events with event_type='booking' in the "
            "15-minute sliding window ending at the last event's sim_ts.  "
            "Updated on every demand event for this route."
        ),
        default_on_missing=0.0,
    ),
    Feature(
        name="look_to_book_1h",
        dtype="float64",
        source="stream",
        freshness_sla_seconds=3_600,  # 1 hour
        description=(
            "Ratio of bookings to searches over the 1-hour sliding window: "
            "bookings_1h / max(1, searches_1h).  Range [0, ∞); values > 1 "
            "indicate more bookings than searches (unusual — may signal a "
            "data quality issue).  Updated on every demand event."
        ),
        default_on_missing=0.0,
    ),
    Feature(
        name="weather_temp_c",
        dtype="float64",
        source="stream",
        freshness_sla_seconds=3_600,  # weather polling interval
        description=(
            "Latest temperature in Celsius for this route, taken from "
            "the most recent weather.readings.v1 message.  Null when no "
            "weather reading has been received yet."
        ),
    ),
    Feature(
        name="weather_precip_mm",
        dtype="float64",
        source="stream",
        freshness_sla_seconds=3_600,
        description=(
            "Latest precipitation in millimetres for this route, taken "
            "from the most recent weather.readings.v1 message.  Null when "
            "no weather reading has been received yet."
        ),
        default_on_missing=0.0,
    ),
    Feature(
        name="weather_condition",
        dtype="str",
        source="stream",
        freshness_sla_seconds=3_600,
        description=(
            "Latest weather condition string (e.g. 'Clear', 'Rain') for "
            "this route.  Stored as-is from the weather.readings.v1 payload.  "
            "Null when no weather reading has been received yet."
        ),
    ),
])
