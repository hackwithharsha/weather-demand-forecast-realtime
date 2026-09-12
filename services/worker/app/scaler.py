"""
fit/transform split for StandardScaler.

Design
------
FittedScaler
    Wraps a fitted sklearn StandardScaler together with the ordered list of
    columns it was fit on.  Exposes only .transform() — there is no .fit()
    method on this class.  Passing a FittedScaler to serving/prediction code
    makes it structurally impossible to trigger a re-fit at inference time.

ScalerTrainer
    Fits a StandardScaler on a training DataFrame and persists/loads the
    resulting FittedScaler to/from MinIO.  Only imported by the batch
    pipeline; never imported by serving code.

SCALE_COLS
    Canonical tuple of continuous feature column names to normalise.
    Shared between trainer and serving code so both always agree on which
    columns are scaled and in what order.

Artifact format
---------------
The fitted scaler is serialised with pickle and stored as a single object
at ``s3://<lake_bucket>/<scaler_s3_key>``.  Pickle is acceptable here because:
  - The artifact is written and read by the same trusted codebase.
  - The S3 bucket is internal; no external input reaches this path.
  - The sklearn StandardScaler has no code-execution surface beyond
    array arithmetic.
"""

from __future__ import annotations

import io
import pickle
from typing import Any

import pandas as pd
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Canonical column list
# ---------------------------------------------------------------------------

SCALE_COLS: tuple[str, ...] = (
    "total_demand",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
)


# ---------------------------------------------------------------------------
# FittedScaler — transform-only, no fit()
# ---------------------------------------------------------------------------

class FittedScaler:
    """
    Immutable wrapper around a fitted StandardScaler.

    The class intentionally exposes only `transform`.  The absence of `fit`
    is the structural guarantee: code that receives a FittedScaler cannot
    re-fit it, regardless of the caller's intent.

    Usage
    -----
    Serving code::

        from app.scaler import ScalerTrainer, FittedScaler
        fitted: FittedScaler = ScalerTrainer.load(s3_client, bucket, key)
        scaled_df = fitted.transform(features_df)

    Note: only `ScalerTrainer.load` and the batch pipeline's `ScalerTrainer`
    instance should ever create FittedScaler objects.
    """

    def __init__(
        self,
        scaler: StandardScaler,
        feature_cols: tuple[str, ...],
    ) -> None:
        self._scaler = scaler
        self.feature_cols: tuple[str, ...] = feature_cols

    # ------------------------------------------------------------------
    # Public interface (safe for serving code)
    # ------------------------------------------------------------------

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return a copy of *df* with every column in ``feature_cols`` replaced
        by its z-score (mean 0, std 1).

        Columns absent from *df* are silently skipped so that new feature
        columns added after the scaler was fit do not break serving code.

        NaN values pass through unchanged; imputation is the caller's
        responsibility.
        """
        present = [c for c in self.feature_cols if c in df.columns]
        if not present:
            return df.copy()
        out = df.copy()
        # Only scale rows where all present columns are non-null.
        mask = out[present].notna().all(axis=1)
        if mask.any():
            out.loc[mask, present] = self._scaler.transform(out.loc[mask, present])
        return out

    # ------------------------------------------------------------------
    # Introspection (safe for logging / debugging)
    # ------------------------------------------------------------------

    def feature_means(self) -> dict[str, float]:
        return dict(zip(self.feature_cols, self._scaler.mean_, strict=False))

    def feature_scales(self) -> dict[str, float]:
        return dict(zip(self.feature_cols, self._scaler.scale_, strict=False))

    def __repr__(self) -> str:
        return (
            f"FittedScaler(cols={self.feature_cols!r}, "
            f"n_features={len(self.feature_cols)})"
        )


# ---------------------------------------------------------------------------
# ScalerTrainer — fit + persist
# ---------------------------------------------------------------------------

class ScalerTrainer:
    """
    Fits a StandardScaler on a DataFrame and persists / loads the result.

    This class must only be imported by the batch pipeline.  Serving code
    should import only `FittedScaler` (obtained via `ScalerTrainer.load`).
    """

    def fit(
        self,
        df: pd.DataFrame,
        feature_cols: tuple[str, ...] = SCALE_COLS,
    ) -> FittedScaler:
        """
        Fit a StandardScaler on *df[feature_cols]*, dropping any row that
        has a NaN in at least one feature column so the statistics reflect
        only clean, complete observations.

        Raises ValueError if the cleaned training set is empty.
        """
        train = df[list(feature_cols)].dropna()
        if train.empty:
            raise ValueError(
                f"Cannot fit scaler: no rows without NaN in {feature_cols!r}. "
                "Ensure staging and marts tables are populated first."
            )
        scaler = StandardScaler()
        scaler.fit(train)
        return FittedScaler(scaler, feature_cols)

    def save(
        self,
        fitted: FittedScaler,
        s3_client: Any,
        bucket: str,
        key: str,
    ) -> None:
        """Pickle *fitted* and PUT it to S3/MinIO at *bucket*/*key*."""
        body = pickle.dumps(fitted)
        s3_client.put_object(Bucket=bucket, Key=key, Body=body)

    @staticmethod
    def load(s3_client: Any, bucket: str, key: str) -> FittedScaler:
        """
        Download and unpickle a FittedScaler from S3/MinIO.

        Raises ``botocore.exceptions.ClientError`` (NoSuchKey / 404) when the
        artifact does not exist — callers should catch this and decide whether
        to fit or abort.
        """
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return pickle.loads(response["Body"].read())  # noqa: S301
