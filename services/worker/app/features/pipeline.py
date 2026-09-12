"""
Feature engineering pipeline: impute → scale → encode.

Design
------
A single ColumnTransformer handles four column groups:

  numeric     (10 cols)  SimpleImputer(median)       → StandardScaler
  cyclical     (4 cols)  passthrough                 — already bounded [-1,1]
  categorical  (1 col)   SimpleImputer(most_frequent) → OneHotEncoder(city)
  boolean      (1 col)   cast to float64             — is_holiday, guaranteed non-null

Column order in the output array mirrors the transformer order above:
numeric columns first, then cyclical, then OHE-expanded city, then boolean.

fit / transform split
---------------------
``fit_pipeline`` returns a plain sklearn Pipeline.
``transform`` is a standalone function that calls ``pipeline.transform()`` —
never ``fit_transform()`` — so the fitted state (scaler mean/scale, OHE
vocabulary) cannot change during inference.

To force a re-fit after a distribution shift, delete the S3 artifact and
run the pipeline once manually (``make worker-pipeline``).
"""

from __future__ import annotations

import pickle
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Column groups
# ---------------------------------------------------------------------------

#: Continuous numeric features: imputed with median then z-scored.
NUMERIC_COLS: tuple[str, ...] = (
    "total_demand",
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
)

#: Sin/cos cyclical encodings already in [-1, 1]; no further transform.
CYCLICAL_COLS: tuple[str, ...] = (
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
)

#: City identifier; one-hot encoded at fit time.
CATEGORICAL_COLS: tuple[str, ...] = ("city",)

#: Boolean flag guaranteed non-null by mart quality assertions.
BOOLEAN_COLS: tuple[str, ...] = ("is_holiday",)

#: Complete ordered feature set.  Both fit_pipeline and transform select by
#: this tuple so caller column order never matters.
ALL_FEATURE_COLS: tuple[str, ...] = (
    *NUMERIC_COLS,
    *CYCLICAL_COLS,
    *CATEGORICAL_COLS,
    *BOOLEAN_COLS,
)

#: Default S3 key.  Override via ``Settings.features_pipeline_s3_key``.
DEFAULT_S3_KEY: str = "artifacts/pipelines/features_v1.pkl"


# ---------------------------------------------------------------------------
# Dtype helper (module-level so pickle can resolve it)
# ---------------------------------------------------------------------------

def _to_float64(X: np.ndarray) -> np.ndarray:
    """Cast a boolean/integer column array to float64.

    Applied to BOOLEAN_COLS so the ColumnTransformer output is uniformly
    float64 regardless of the pandas column dtype stored in Postgres.
    """
    return np.asarray(X, dtype=np.float64)


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------

def _build_preprocessor() -> ColumnTransformer:
    """Construct and return an *unfitted* ColumnTransformer."""
    numeric_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])

    categorical_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    return ColumnTransformer(
        transformers=[
            ("numeric",     numeric_pipe,                      list(NUMERIC_COLS)),
            ("cyclical",    "passthrough",                     list(CYCLICAL_COLS)),
            ("categorical", categorical_pipe,                  list(CATEGORICAL_COLS)),
            ("boolean",     FunctionTransformer(_to_float64),  list(BOOLEAN_COLS)),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fit_pipeline(df: pd.DataFrame) -> Pipeline:
    """Fit the preprocessing Pipeline on *df* and return it.

    Parameters
    ----------
    df:
        Training DataFrame.  Must contain every column in ALL_FEATURE_COLS.
        Rows with NaN values are included; the median imputer handles them.

    Returns
    -------
    Pipeline
        Fitted sklearn Pipeline.  Pass to :func:`transform` to apply.

    Raises
    ------
    ValueError
        If *df* is empty or missing required columns.
    """
    missing = set(ALL_FEATURE_COLS) - set(df.columns)
    if missing:
        raise ValueError(
            f"fit_pipeline: DataFrame missing columns: {sorted(missing)}"
        )
    if df.empty:
        raise ValueError("fit_pipeline: DataFrame is empty")

    pipeline = Pipeline([("preprocessor", _build_preprocessor())])
    pipeline.fit(df[list(ALL_FEATURE_COLS)])

    ct     = pipeline.named_steps["preprocessor"]
    scaler = ct.named_transformers_["numeric"].named_steps["scaler"]
    ohe    = ct.named_transformers_["categorical"].named_steps["encoder"]

    log.info(
        "features_pipeline_fitted",
        n_rows=len(df),
        ohe_categories=list(ohe.categories_[0]),
        numeric_means={
            col: round(float(m), 4)
            for col, m in zip(NUMERIC_COLS, scaler.mean_)
        },
    )
    return pipeline


def transform(pipeline: Pipeline, df: pd.DataFrame) -> np.ndarray:
    """Apply the fitted *pipeline* to *df*; return a float64 ndarray.

    Calls ``pipeline.transform()`` — never ``fit_transform()`` — so the
    fitted state (scaler mean/scale, OHE vocabulary) cannot change.

    Parameters
    ----------
    pipeline:
        A Pipeline returned by :func:`fit_pipeline`.
    df:
        Input DataFrame.  Must contain every column in ALL_FEATURE_COLS;
        extra columns are silently ignored.

    Returns
    -------
    np.ndarray, shape (n_rows, n_output_features), dtype float64.
        Column order: numeric → cyclical → OHE-expanded city → boolean.
    """
    missing = set(ALL_FEATURE_COLS) - set(df.columns)
    if missing:
        raise ValueError(
            f"transform: DataFrame missing columns: {sorted(missing)}"
        )
    return np.asarray(
        pipeline.transform(df[list(ALL_FEATURE_COLS)]),
        dtype=np.float64,
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_pipeline(
    pipeline: Pipeline,
    s3_client: Any,
    bucket: str,
    key: str = DEFAULT_S3_KEY,
) -> None:
    """Pickle *pipeline* and PUT it to S3/MinIO at *bucket*/*key*."""
    body = pickle.dumps(pipeline)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    log.info("features_pipeline_saved", bucket=bucket, key=key, size_bytes=len(body))


def load_pipeline(
    s3_client: Any,
    bucket: str,
    key: str = DEFAULT_S3_KEY,
) -> Pipeline:
    """Download and unpickle a Pipeline from S3/MinIO.

    Raises
    ------
    botocore.exceptions.ClientError
        With code ``"404"`` or ``"NoSuchKey"`` if the artifact is absent.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    pipeline = pickle.loads(response["Body"].read())  # noqa: S301
    log.info("features_pipeline_loaded", bucket=bucket, key=key)
    return pipeline
