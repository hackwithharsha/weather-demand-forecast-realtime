"""
Tests for app.features.pipeline.

Key invariant under test
------------------------
transform() must call pipeline.transform() — never fit_transform() — so the
fitted state (scaler mean/scale, OHE vocabulary) is identical before and after
any call to transform().  The immutability tests assert this directly by
comparing fitted parameters before and after one or more transform() calls.
"""

from __future__ import annotations

import io
import pickle
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from app.features.pipeline import (
    ALL_FEATURE_COLS,
    BOOLEAN_COLS,
    CATEGORICAL_COLS,
    CYCLICAL_COLS,
    NUMERIC_COLS,
    fit_pipeline,
    load_pipeline,
    save_pipeline,
    transform,
)


# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------

def _make_df(n: int = 200, *, seed: int = 0) -> pd.DataFrame:
    """Synthetic mart-shaped DataFrame with all feature columns.

    Uses np.resize for the city column to guarantee all three cities appear
    even at small n, keeping the OHE vocabulary stable across test runs.
    """
    rng = np.random.default_rng(seed)

    # 5 % null rate on lag/rolling cols; 10 % on weather — mirrors production.
    def _with_nulls(arr: np.ndarray, p: float) -> np.ndarray:
        mask = rng.random(n) < p
        out = arr.astype(float)
        out[mask] = np.nan
        return out

    cities = np.resize(["london", "new_york", "tokyo"], n)

    return pd.DataFrame({
        "total_demand":    rng.uniform(100, 1000, n),
        "event_count":     rng.integers(1, 100, n).astype(float),
        "demand_lag_1h":   _with_nulls(rng.uniform(100, 1000, n), 0.05),
        "demand_lag_24h":  _with_nulls(rng.uniform(100, 1000, n), 0.05),
        "demand_lag_168h": _with_nulls(rng.uniform(100, 1000, n), 0.05),
        "demand_roll_3h":  _with_nulls(rng.uniform(100, 1000, n), 0.05),
        "demand_roll_24h": _with_nulls(rng.uniform(100, 1000, n), 0.05),
        "temperature_c":   _with_nulls(rng.uniform(-10, 40, n), 0.10),
        "humidity_pct":    _with_nulls(rng.uniform(20, 95, n),  0.10),
        "precip_mm":       _with_nulls(rng.uniform(0, 20, n),   0.10),
        "hour_sin":        np.sin(2 * np.pi * rng.integers(0, 24, n) / 24.0),
        "hour_cos":        np.cos(2 * np.pi * rng.integers(0, 24, n) / 24.0),
        "dow_sin":         np.sin(2 * np.pi * rng.integers(0, 7,  n) / 7.0),
        "dow_cos":         np.cos(2 * np.pi * rng.integers(0, 7,  n) / 7.0),
        "city":            cities,
        "is_holiday":      rng.choice([True, False], n),
    })


def _get_scaler(pipeline):
    """Navigate to the StandardScaler inside the nested pipeline."""
    return (
        pipeline.named_steps["preprocessor"]
        .named_transformers_["numeric"]
        .named_steps["scaler"]
    )


def _get_ohe(pipeline):
    """Navigate to the OneHotEncoder inside the nested pipeline."""
    return (
        pipeline.named_steps["preprocessor"]
        .named_transformers_["categorical"]
        .named_steps["encoder"]
    )


def _n_output_cols(pipeline) -> int:
    """Total output columns: numeric + cyclical + OHE + boolean."""
    n_ohe = sum(len(cats) for cats in _get_ohe(pipeline).categories_)
    return len(NUMERIC_COLS) + len(CYCLICAL_COLS) + n_ohe + len(BOOLEAN_COLS)


# ---------------------------------------------------------------------------
# fit_pipeline
# ---------------------------------------------------------------------------

class TestFitPipeline:
    def test_returns_sklearn_pipeline(self):
        pipeline = fit_pipeline(_make_df())
        assert hasattr(pipeline, "steps")
        assert pipeline.steps[0][0] == "preprocessor"

    def test_raises_on_empty_dataframe(self):
        empty = pd.DataFrame(columns=list(ALL_FEATURE_COLS))
        with pytest.raises(ValueError, match="empty"):
            fit_pipeline(empty)

    def test_raises_on_missing_columns(self):
        df = _make_df().drop(columns=["city", "is_holiday"])
        with pytest.raises(ValueError, match="missing columns"):
            fit_pipeline(df)

    def test_extra_columns_in_training_data_are_ignored(self):
        df = _make_df()
        df["extra"] = 999
        pipeline = fit_pipeline(df)          # must not raise
        arr = transform(pipeline, df)
        assert arr.shape[0] == len(df)

    def test_ohe_learns_city_vocabulary(self):
        pipeline = fit_pipeline(_make_df())
        cats = list(_get_ohe(pipeline).categories_[0])
        assert sorted(cats) == ["london", "new_york", "tokyo"]

    def test_scaler_mean_is_finite(self):
        pipeline = fit_pipeline(_make_df(500))
        assert np.all(np.isfinite(_get_scaler(pipeline).mean_))

    def test_scaler_n_features_equals_numeric_cols(self):
        pipeline = fit_pipeline(_make_df())
        assert len(_get_scaler(pipeline).mean_) == len(NUMERIC_COLS)


# ---------------------------------------------------------------------------
# transform — output properties
# ---------------------------------------------------------------------------

class TestTransformOutput:
    def test_output_shape(self):
        df = _make_df(80)
        pipeline = fit_pipeline(df)
        arr = transform(pipeline, df)
        assert arr.shape == (80, _n_output_cols(pipeline))

    def test_output_dtype_is_float64(self):
        df = _make_df()
        pipeline = fit_pipeline(df)
        assert transform(pipeline, df).dtype == np.float64

    def test_raises_on_missing_columns(self):
        df = _make_df()
        pipeline = fit_pipeline(df)
        with pytest.raises(ValueError, match="missing columns"):
            transform(pipeline, df.drop(columns=["total_demand"]))

    def test_extra_columns_silently_ignored(self):
        df = _make_df(60)
        pipeline = fit_pipeline(df)
        df_extra = df.copy()
        df_extra["noise"] = -1
        np.testing.assert_array_equal(
            transform(pipeline, df),
            transform(pipeline, df_extra),
        )

    def test_column_order_of_input_df_does_not_matter(self):
        df = _make_df(60)
        pipeline = fit_pipeline(df)
        df_shuffled = df[list(reversed(df.columns))]
        np.testing.assert_array_equal(
            transform(pipeline, df),
            transform(pipeline, df_shuffled),
        )

    def test_boolean_col_values_are_0_or_1(self):
        df = _make_df(100)
        pipeline = fit_pipeline(df)
        arr = transform(pipeline, df)
        bool_col = arr[:, -1]          # boolean is the last output column
        assert set(bool_col.tolist()).issubset({0.0, 1.0})

    def test_cyclical_cols_pass_through_unchanged(self):
        """Cyclical columns must emerge from transform with identical values."""
        df = _make_df(100)
        pipeline = fit_pipeline(df)
        arr = transform(pipeline, df)

        cyc_start = len(NUMERIC_COLS)
        cyc_end   = cyc_start + len(CYCLICAL_COLS)
        cyclical_out = arr[:, cyc_start:cyc_end]

        expected = df[list(CYCLICAL_COLS)].to_numpy(dtype=np.float64)
        np.testing.assert_array_equal(cyclical_out, expected)


# ---------------------------------------------------------------------------
# transform — numeric correctness
# ---------------------------------------------------------------------------

class TestNumericCorrectness:
    def test_numeric_cols_are_approximately_standardised(self):
        """Fit+transform on the same data → numeric output ≈ N(0, 1)."""
        df = _make_df(1000)
        pipeline = fit_pipeline(df)
        arr = transform(pipeline, df)

        numeric_out = arr[:, : len(NUMERIC_COLS)]

        # Rows with any NaN in numeric source cols are imputed to the median,
        # which maps to near-zero after scaling.  Check per-column statistics.
        np.testing.assert_allclose(
            numeric_out.mean(axis=0), 0.0, atol=0.05,
            err_msg="Numeric columns should be approximately mean-zero after scaling",
        )
        np.testing.assert_allclose(
            numeric_out.std(axis=0), 1.0, atol=0.05,
            err_msg="Numeric columns should have approximately unit variance after scaling",
        )

    def test_nans_are_imputed_not_propagated(self):
        """NaN values in numeric columns must not appear in transform output."""
        df = _make_df(200)
        assert df[list(NUMERIC_COLS)].isna().any().any(), "fixture has no NaNs"
        pipeline = fit_pipeline(df)
        arr = transform(pipeline, df)
        assert not np.any(np.isnan(arr[:, : len(NUMERIC_COLS)])), (
            "NaN propagated through imputer — imputation is not working"
        )


# ---------------------------------------------------------------------------
# transform — immutability (primary invariant)
# ---------------------------------------------------------------------------

class TestTransformDoesNotMutateState:
    """
    transform() calls pipeline.transform(), never fit_transform().
    These tests verify that no fitted parameter changes after a transform call.

    If someone accidentally replaces ``pipeline.transform(X)`` with
    ``pipeline.fit_transform(X)`` inside ``transform()``, the scaler's
    mean_ and scale_ would be recomputed on the new data and these tests
    would fail immediately.
    """

    def test_scaler_mean_unchanged_after_single_transform(self):
        """scaler.mean_ must be identical before and after one transform()."""
        pipeline = fit_pipeline(_make_df(200, seed=0))
        scaler   = _get_scaler(pipeline)

        mean_before  = scaler.mean_.copy()
        scale_before = scaler.scale_.copy()

        transform(pipeline, _make_df(50, seed=1))   # different data

        np.testing.assert_array_equal(
            scaler.mean_, mean_before,
            err_msg="transform() mutated scaler.mean_",
        )
        np.testing.assert_array_equal(
            scaler.scale_, scale_before,
            err_msg="transform() mutated scaler.scale_",
        )

    def test_scaler_state_unchanged_after_repeated_transforms(self):
        """Five consecutive transform() calls must not accumulate changes."""
        pipeline = fit_pipeline(_make_df(200, seed=0))
        scaler   = _get_scaler(pipeline)

        mean_before  = scaler.mean_.copy()
        scale_before = scaler.scale_.copy()

        for i in range(5):
            transform(pipeline, _make_df(40, seed=i + 10))

        np.testing.assert_array_equal(scaler.mean_,  mean_before)
        np.testing.assert_array_equal(scaler.scale_, scale_before)

    def test_ohe_categories_unchanged_after_transform(self):
        """OHE vocabulary must not change when transform() sees known cities."""
        pipeline = fit_pipeline(_make_df(200, seed=0))
        ohe      = _get_ohe(pipeline)

        categories_before = [cats.copy() for cats in ohe.categories_]

        transform(pipeline, _make_df(60, seed=99))

        for before, after in zip(categories_before, ohe.categories_):
            np.testing.assert_array_equal(
                before, after,
                err_msg="transform() mutated OHE categories_",
            )

    def test_transform_result_is_deterministic(self):
        """Calling transform() twice on the same df yields identical arrays."""
        df = _make_df(100)
        pipeline = fit_pipeline(df)

        arr1 = transform(pipeline, df)
        arr2 = transform(pipeline, df)

        np.testing.assert_array_equal(arr1, arr2)


# ---------------------------------------------------------------------------
# Persistence round-trip
# ---------------------------------------------------------------------------

class TestPersistence:
    def _mock_s3(self) -> tuple[MagicMock, io.BytesIO]:
        """Return (mock_s3_client, shared_buffer) for save/load tests."""
        buf = io.BytesIO()
        mock = MagicMock()
        mock.put_object.side_effect = (
            lambda **kw: buf.write(kw["Body"]) and None
        )
        mock.get_object.return_value = {"Body": io.BytesIO(buf.getvalue())}
        return mock, buf

    def test_save_load_roundtrip_produces_identical_output(self):
        """A pipeline saved to mock S3 and loaded back gives the same array."""
        df = _make_df(100)
        pipeline_orig = fit_pipeline(df)

        buf = io.BytesIO()
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = lambda **kw: buf.write(kw["Body"])
        mock_s3.get_object.return_value = {"Body": io.BytesIO(buf.getvalue())}

        save_pipeline(pipeline_orig, mock_s3, "lake", "test/pipeline.pkl")
        # Rewind so get_object returns the full bytes.
        mock_s3.get_object.return_value = {"Body": io.BytesIO(buf.getvalue())}

        pipeline_loaded = load_pipeline(mock_s3, "lake", "test/pipeline.pkl")

        np.testing.assert_array_equal(
            transform(pipeline_orig,  df),
            transform(pipeline_loaded, df),
        )

    def test_loaded_pipeline_scaler_state_is_unchanged_by_transform(self):
        """The immutability invariant holds for a deserialized pipeline too."""
        df_train = _make_df(200, seed=0)
        df_new   = _make_df(50,  seed=7)

        buf = io.BytesIO()
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = lambda **kw: buf.write(kw["Body"])

        save_pipeline(fit_pipeline(df_train), mock_s3, "lake", "k")

        mock_s3.get_object.return_value = {"Body": io.BytesIO(buf.getvalue())}
        pipeline = load_pipeline(mock_s3, "lake", "k")

        scaler       = _get_scaler(pipeline)
        mean_before  = scaler.mean_.copy()
        scale_before = scaler.scale_.copy()

        transform(pipeline, df_new)

        np.testing.assert_array_equal(scaler.mean_,  mean_before)
        np.testing.assert_array_equal(scaler.scale_, scale_before)
