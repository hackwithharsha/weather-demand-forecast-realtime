"""
Backward-compatibility shim — new code should import from
``common.features.registry``.

Aliases provided for existing callers:

  FeatureDef     → common.features.registry.Feature
  FeatureRegistry → common.features.registry.Registry

These aliases will be removed in a future release once all services have
been updated to import from ``common.features.registry`` directly.
"""

from common.features.registry import (  # noqa: F401
    BATCH_COMPUTED_AT_FIELD,
    ROUTE_FEATURES,
    ROUTE_REDIS_KEY_PATTERN,
    Feature,
    Feature as FeatureDef,
    Registry,
    Registry as FeatureRegistry,
)

__all__ = [
    "Feature",
    "FeatureDef",
    "Registry",
    "FeatureRegistry",
    "ROUTE_FEATURES",
    "ROUTE_REDIS_KEY_PATTERN",
    "BATCH_COMPUTED_AT_FIELD",
]
