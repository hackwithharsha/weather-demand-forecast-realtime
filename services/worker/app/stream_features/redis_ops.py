"""
Redis operations for the stream feature consumer.

Demand events — sliding-window aggregates
-----------------------------------------
On each demand event:

  1. ZADD the event_id to the appropriate sorted set (searches or bookings),
     scored by sim_ts epoch.  ZADD is idempotent: re-delivering the same
     event_id leaves the set unchanged.

  2. ZREMRANGEBYSCORE both sets to drop entries older than 1 hour (the
     longest window), keeping memory O(events/hour/route).

  3. ZCOUNT all four window/direction combinations in the same pipeline.

  4. Derive look_to_book_1h = bookings_1h / max(1, searches_1h).

  5. HSET the computed aggregates into feat:route:{route_id}.

Steps 1-3 execute in one pipelined round-trip.  Step 5 is a second call.
All field name strings come from registry lookups, not literals.

Weather events — latest-value write
-------------------------------------
Weather readings do not need windowing.  ``update_weather_features`` does a
single HSET with the three latest-value fields.

See docs/decisions.md §"Sliding-window counters: Redis sorted sets vs
counter-per-minute buckets" for the full design rationale.
"""

from __future__ import annotations

from datetime import datetime, timezone

import redis as _redis_module
import structlog

from common.features.registry import (
    ROUTE_FEATURES,
    ROUTE_REDIS_KEY_PATTERN,
    STREAM_BOOKINGS_KEY_PATTERN,
    STREAM_COMPUTED_AT_FIELD,
    STREAM_SEARCHES_KEY_PATTERN,
)

log = structlog.get_logger()

# Window lengths in seconds.  Trim boundary = longest window.
_W_5M  =   300
_W_15M =   900
_W_1H  = 3_600

# Event types that map to each sorted set.
_SEARCH_TYPES  = frozenset({"search"})
_BOOKING_TYPES = frozenset({"booking"})

# Stream feature field names resolved from the registry — no string literals.
_F_SEARCHES_5M    = ROUTE_FEATURES["searches_5m"].name
_F_BOOKINGS_15M   = ROUTE_FEATURES["bookings_15m"].name
_F_LOOK_TO_BOOK   = ROUTE_FEATURES["look_to_book_1h"].name
_F_WEATHER_TEMP   = ROUTE_FEATURES["weather_temp_c"].name
_F_WEATHER_PRECIP = ROUTE_FEATURES["weather_precip_mm"].name
_F_WEATHER_COND   = ROUTE_FEATURES["weather_condition"].name


def update_demand_features(
    r: _redis_module.Redis,
    *,
    route_id: str,
    event_id: str,
    event_type: str,
    sim_epoch: float,
) -> None:
    """Update sliding-window demand aggregates for one event.

    Args:
        r:          Synchronous Redis client (``decode_responses=True``).
        route_id:   City / route identifier, e.g. ``"london"``.
        event_id:   Unique event ID from the Kafka envelope.  Used as the
                    sorted-set member for at-least-once deduplication.
        event_type: Raw event_type string from the demand payload.
        sim_epoch:  ``sim_ts`` converted to Unix epoch (float seconds).
                    Used as the sorted-set score so window queries are
                    point-in-time correct even during backfill replays.
    """
    searches_key = STREAM_SEARCHES_KEY_PATTERN.format(route_id=route_id)
    bookings_key = STREAM_BOOKINGS_KEY_PATTERN.format(route_id=route_id)
    route_key    = ROUTE_REDIS_KEY_PATTERN.format(route_id=route_id)

    # Window lower bounds (inclusive).
    lo_5m  = sim_epoch - _W_5M
    lo_15m = sim_epoch - _W_15M
    lo_1h  = sim_epoch - _W_1H

    pipe = r.pipeline(transaction=False)

    # ── Phase 1: add current event to the appropriate sorted set ──────────
    # Conditional: only search and booking events go into a sorted set.
    # Cancellations and other types still trigger a window recount so that
    # stream_computed_at stays fresh.
    if event_type in _SEARCH_TYPES:
        pipe.zadd(searches_key, {event_id: sim_epoch})
    elif event_type in _BOOKING_TYPES:
        pipe.zadd(bookings_key, {event_id: sim_epoch})

    # ── Phase 2: trim both sets to the longest window ─────────────────────
    # "0" / "-inf" for the lower bound keeps the trim idiomatic.
    # Upper bound is 1 second inside lo_1h to exclude the exact boundary.
    trim_upper = lo_1h - 1
    pipe.zremrangebyscore(searches_key, "-inf", trim_upper)
    pipe.zremrangebyscore(bookings_key, "-inf", trim_upper)

    # ── Phase 3: count in each window ─────────────────────────────────────
    # Upper bound is sim_epoch (not "now") so the window is PIT-correct.
    pipe.zcount(searches_key, lo_5m,  sim_epoch)  # searches_5m
    pipe.zcount(bookings_key, lo_15m, sim_epoch)  # bookings_15m
    pipe.zcount(searches_key, lo_1h,  sim_epoch)  # searches_1h  (ratio)
    pipe.zcount(bookings_key, lo_1h,  sim_epoch)  # bookings_1h  (ratio)

    results = pipe.execute()

    # The last 4 results are always the four ZCOUNT calls, regardless of
    # whether a ZADD was prepended (which shifts earlier indices).
    searches_5m, bookings_15m, searches_1h, bookings_1h = results[-4:]

    look_to_book_1h = float(bookings_1h) / max(1.0, float(searches_1h))

    # ── Phase 4: write aggregates to the route hash ───────────────────────
    # HSET only touches these fields; batch-written fields are untouched.
    r.hset(
        route_key,
        mapping={
            _F_SEARCHES_5M:  str(float(searches_5m)),
            _F_BOOKINGS_15M: str(float(bookings_15m)),
            _F_LOOK_TO_BOOK: str(round(look_to_book_1h, 6)),
            STREAM_COMPUTED_AT_FIELD: datetime.now(timezone.utc).isoformat(),
        },
    )

    log.debug(
        "stream_demand_features_updated",
        route_id=route_id,
        event_type=event_type,
        searches_5m=searches_5m,
        bookings_15m=bookings_15m,
        look_to_book_1h=round(look_to_book_1h, 4),
    )


def update_weather_features(
    r: _redis_module.Redis,
    *,
    route_id: str,
    temperature_c: float | None,
    precip_mm: float | None,
    condition: str | None,
) -> None:
    """Write the latest weather values to the route hash.

    Args:
        r:              Synchronous Redis client.
        route_id:       City / route identifier.
        temperature_c:  Latest temperature in Celsius (nullable).
        precip_mm:      Latest precipitation in mm (nullable).
        condition:      Latest weather condition string (nullable).

    Null fields are omitted from the HSET mapping so the hash never grows
    stale ``"None"`` strings.  A previously written value is preserved until
    a non-null reading arrives.
    """
    route_key = ROUTE_REDIS_KEY_PATTERN.format(route_id=route_id)

    mapping: dict[str, str] = {
        STREAM_COMPUTED_AT_FIELD: datetime.now(timezone.utc).isoformat(),
    }
    if temperature_c is not None:
        mapping[_F_WEATHER_TEMP] = str(float(temperature_c))
    if precip_mm is not None:
        mapping[_F_WEATHER_PRECIP] = str(float(precip_mm))
    if condition is not None:
        mapping[_F_WEATHER_COND] = str(condition)

    r.hset(route_key, mapping=mapping)

    log.debug(
        "stream_weather_features_updated",
        route_id=route_id,
        temperature_c=temperature_c,
        precip_mm=precip_mm,
        condition=condition,
    )
