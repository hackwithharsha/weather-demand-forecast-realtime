"""
Online feature retrieval for the forecast serving layer.

Design
------
Every feature the demand model can read from the live Redis feature hash is
described in ``ONLINE_FEATURES``.  Each entry wraps a ``Feature`` object from
the shared common registry (which owns the SLA and default values) and adds
the ``model_col`` name that the sklearn pipeline expects.

On every prediction request ``fetch_online_features`` is called once:

1. A single ``HGETALL`` is issued against the city's feature hash.
   (``feat:route:{city}``, written by the stream worker.)

2. The hash's ``stream_computed_at`` timestamp is parsed.  Its age relative
   to *as_of* is used for all SLA checks in the same call.

3. For every registered online feature:
   • Field present in hash → cast to float.
       – Age ≤ SLA          : value used, no flag.
       – Age > SLA          : value used, ``model_col`` added to
                              ``OnlineResult.degraded``.  The feature IS served
                              because withholding stale-but-present data
                              is almost always worse than using it.
   • Field absent from hash → ``Feature.default_on_missing`` applied.
       – If default is not None : value used.
       – If default is None     : ``None`` returned for that column;
                                  the caller falls back to the Postgres value.
       In both cases the redis_field name is added to
       ``OnlineResult.missing`` and the per-field miss counter is incremented.

4. Callers overlay only non-``None`` values from ``OnlineResult.values`` onto
   their Postgres base row, so a ``None`` default means "use Postgres".

Metrics
-------
Module-level counters (``_miss_counts``) accumulate the total number of times
each Redis field was absent since process start.  ``get_miss_counts()`` returns
a snapshot for surfacing in ``/model/info``.  Each individual miss is also
emitted as a structured ``info`` log line for stream ingestion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog

from common.features.registry import (
    ROUTE_REDIS_KEY_PATTERN,
    ROUTE_FEATURES,
    STREAM_COMPUTED_AT_FIELD,
)

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Online feature descriptor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OnlineFeature:
    """Mapping from one Redis hash field to one sklearn pipeline column.

    ``feature`` is the shared ``Feature`` object from the common registry;
    it owns the SLA (``freshness_sla_seconds``) and the default
    (``default_on_missing``).  ``model_col`` is the column name inside
    ``FEATURE_COLS`` that the demand pipeline expects.
    """

    feature: Any    # common.features.registry.Feature
    model_col: str  # the column name the sklearn pipeline reads


# ---------------------------------------------------------------------------
# Registry — single source of truth for what the serving layer reads online
# ---------------------------------------------------------------------------

# Each entry maps a Redis hash field (``feature.name``) to the corresponding
# sklearn column (``model_col``).  SLA values and defaults come directly from
# the shared common registry so they never need to be duplicated here.
ONLINE_FEATURES: list[OnlineFeature] = [
    OnlineFeature(
        feature=ROUTE_FEATURES["weather_temp_c"],
        model_col="temperature_c",
    ),
    OnlineFeature(
        feature=ROUTE_FEATURES["weather_precip_mm"],
        model_col="precip_mm",
    ),
    # weather_humidity_pct is not yet written by the stream worker.
    # When the worker starts publishing it, add an entry here:
    # OnlineFeature(
    #     feature=Feature(
    #         name="weather_humidity_pct",
    #         dtype="float64",
    #         source="stream",
    #         freshness_sla_seconds=3_600,
    #         description="Relative humidity from the weather poller.",
    #     ),
    #     model_col="humidity_pct",
    # ),
]

# O(1) lookup by redis_field name
_BY_REDIS_FIELD: dict[str, OnlineFeature] = {
    of.feature.name: of for of in ONLINE_FEATURES
}


# ---------------------------------------------------------------------------
# Per-field miss counters (module-level; reset only on process restart)
# ---------------------------------------------------------------------------

_miss_counts: dict[str, int] = {of.feature.name: 0 for of in ONLINE_FEATURES}


def get_miss_counts() -> dict[str, int]:
    """Return a snapshot of per-field miss counts since process start."""
    return dict(_miss_counts)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class OnlineResult:
    """Outcome of a single ``HGETALL``-based online feature fetch.

    Attributes
    ----------
    values:
        ``{model_col: float | None}`` for every registered online feature.
        ``None`` means "no Redis value and no numeric default — caller should
        keep the Postgres base value for this column".
    missing:
        Redis field names that were absent from the hash (or unparseable).
        Defaults were applied; callers receive these for response metadata.
    degraded:
        Model column names whose hash value was present but whose age
        exceeded ``Feature.freshness_sla_seconds``.  Served, not withheld.
    age_s:
        Seconds elapsed since ``stream_computed_at`` at call time.
        ``None`` when the timestamp field is absent or unparseable.
    hash_present:
        ``False`` when ``HGETALL`` returned an empty mapping (key never
        written, city unknown, or Redis unavailable).
    """

    values: dict[str, float | None]
    missing: list[str]
    degraded: list[str]
    age_s: float | None
    hash_present: bool


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def fetch_online_features(
    city: str,
    redis: Any,     # redis.asyncio.Redis
    as_of: datetime,
) -> OnlineResult:
    """Fetch all registered online features for *city* in one ``HGETALL``.

    Parameters
    ----------
    city:
        City identifier — used to construct the Redis key.
    redis:
        An active ``redis.asyncio.Redis`` client with ``decode_responses=True``.
    as_of:
        Request timestamp used to compute feature age for SLA checks.
    """
    key = ROUTE_REDIS_KEY_PATTERN.format(route_id=city)

    try:
        raw: dict[str, str] = await redis.hgetall(key)
    except Exception as exc:
        log.warning("online_hgetall_error", city=city, key=key, error=str(exc))
        raw = {}

    # ── Hash absent (key never written or Redis unavailable) ─────────────────
    if not raw:
        for of in ONLINE_FEATURES:
            _miss_counts[of.feature.name] += 1
            log.info(
                "online_feature_missing",
                city=city,
                redis_field=of.feature.name,
                model_col=of.model_col,
                default=of.feature.default_on_missing,
                reason="hash_absent",
                total_misses=_miss_counts[of.feature.name],
            )
        return OnlineResult(
            values={of.model_col: of.feature.default_on_missing for of in ONLINE_FEATURES},
            missing=[of.feature.name for of in ONLINE_FEATURES],
            degraded=[],
            age_s=None,
            hash_present=False,
        )

    # ── Parse hash age from stream_computed_at ────────────────────────────────
    age_s: float | None = None
    ts_str = raw.get(STREAM_COMPUTED_AT_FIELD)
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_s = (as_of - ts).total_seconds()
        except ValueError:
            log.warning(
                "online_timestamp_parse_error",
                city=city,
                field=STREAM_COMPUTED_AT_FIELD,
                raw=ts_str,
            )

    # ── Validate every registered field ──────────────────────────────────────
    values: dict[str, float | None] = {}
    missing: list[str] = []
    degraded: list[str] = []

    for of in ONLINE_FEATURES:
        redis_field = of.feature.name
        raw_val = raw.get(redis_field)

        if raw_val is None:
            # Field absent from the hash — apply default and record miss
            _miss_counts[redis_field] += 1
            log.info(
                "online_feature_missing",
                city=city,
                redis_field=redis_field,
                model_col=of.model_col,
                default=of.feature.default_on_missing,
                reason="field_absent",
                total_misses=_miss_counts[redis_field],
            )
            missing.append(redis_field)
            values[of.model_col] = of.feature.default_on_missing
            continue

        # Parse to float; treat parse failure as a miss
        try:
            parsed = float(raw_val)
        except (ValueError, TypeError):
            _miss_counts[redis_field] += 1
            log.warning(
                "online_feature_parse_error",
                city=city,
                redis_field=redis_field,
                raw_val=raw_val,
                total_misses=_miss_counts[redis_field],
            )
            missing.append(redis_field)
            values[of.model_col] = of.feature.default_on_missing
            continue

        values[of.model_col] = parsed

        # SLA check — breach is flagged but the value is still served
        if age_s is not None and age_s > of.feature.freshness_sla_seconds:
            degraded.append(of.model_col)
            log.warning(
                "online_feature_sla_breach",
                city=city,
                redis_field=redis_field,
                model_col=of.model_col,
                age_s=round(age_s),
                sla_s=of.feature.freshness_sla_seconds,
            )

    log.debug(
        "online_features_fetched",
        city=city,
        age_s=round(age_s, 1) if age_s is not None else None,
        missing=missing,
        degraded=degraded,
    )

    return OnlineResult(
        values=values,
        missing=missing,
        degraded=degraded,
        age_s=round(age_s, 1) if age_s is not None else None,
        hash_present=True,
    )
