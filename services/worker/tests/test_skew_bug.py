"""
Training-serving skew bug: demonstration and regression guard.

Run with:
    pytest services/worker/tests/test_skew_bug.py -v -s

Three tests, two concerns:

  test_skew_bug_demo
      Quantifies the error when a buggy serve path re-fits the scaler on the
      live inference batch instead of using the training-time statistics.
      Prints a side-by-side table and asserts the magnitude exceeds safe
      thresholds.

  test_real_code_path_cannot_refit
      Plants tripwires on pipeline.fit and pipeline.fit_transform, then calls
      our transform() function.  If transform() ever touches either tripwired
      method the test fails immediately with the exact skew-bug message.

  test_tripwires_fire_when_fit_transform_is_called_directly
      Calls fit_transform() directly on the armed pipeline to confirm the
      tripwires actually fire — proving test_real_code_path_cannot_refit is
      not vacuously passing.

Scenario
--------
The pipeline is trained on steady-state demand (total_demand ∈ [100, 500],
mean ≈ 300, std ≈ 116).  At serve time a post-holiday spike arrives
(total_demand ∈ [2 000, 5 000], mean ≈ 3 500, std ≈ 866).

  demand observation = 4 000

  persisted scaler → (4000 - 300) / 116  ≈ +32 σ  (correctly extreme)
  re-fitted scaler → (4000 - 3500) / 866 ≈ +0.6 σ  (signal erased)

The 31+ σ gap is what makes training-serving skew so dangerous: the model
receives a near-normal-looking input when it should see a massive outlier.
"""

from __future__ import annotations

import copy

import numpy as np
import pandas as pd
import pytest

from app.features.pipeline import (
    ALL_FEATURE_COLS,
    NUMERIC_COLS,
    fit_pipeline,
    transform,
)

# ---------------------------------------------------------------------------
# Data factories
# ---------------------------------------------------------------------------

_CITIES = ["london", "new_york", "tokyo"]


def _make_df_normal(n: int = 500, *, seed: int = 0) -> pd.DataFrame:
    """Steady-state distribution used for training (total_demand ∈ [100, 500])."""
    rng = np.random.default_rng(seed)
    cities = np.resize(_CITIES, n)
    return pd.DataFrame({
        "total_demand":    rng.uniform(100, 500, n),
        "event_count":     rng.integers(1, 50, n).astype(float),
        "demand_lag_1h":   rng.uniform(100, 500, n),
        "demand_lag_24h":  rng.uniform(100, 500, n),
        "demand_lag_168h": rng.uniform(100, 500, n),
        "demand_roll_3h":  rng.uniform(100, 500, n),
        "demand_roll_24h": rng.uniform(100, 500, n),
        "temperature_c":   rng.uniform(-10, 40, n),
        "humidity_pct":    rng.uniform(20, 95, n),
        "precip_mm":       rng.uniform(0, 20, n),
        "hour_sin":        np.sin(2 * np.pi * rng.integers(0, 24, n) / 24.0),
        "hour_cos":        np.cos(2 * np.pi * rng.integers(0, 24, n) / 24.0),
        "dow_sin":         np.sin(2 * np.pi * rng.integers(0, 7,  n) / 7.0),
        "dow_cos":         np.cos(2 * np.pi * rng.integers(0, 7,  n) / 7.0),
        "city":            cities,
        "is_holiday":      rng.choice([True, False], n),
    })


def _make_df_spike(n: int = 100, *, seed: int = 42) -> pd.DataFrame:
    """Post-holiday spike distribution used at serve time (total_demand ∈ [2 000, 5 000]).

    Short-lag columns also reflect the spike; the 168 h lag predates it —
    making demand_lag_168h the one numeric column whose range is unchanged,
    which cleanly isolates which columns drive the stat divergence.
    """
    rng = np.random.default_rng(seed)
    cities = np.resize(_CITIES, n)
    return pd.DataFrame({
        "total_demand":    rng.uniform(2_000, 5_000, n),
        "event_count":     rng.integers(1, 50, n).astype(float),
        "demand_lag_1h":   rng.uniform(2_000, 5_000, n),
        "demand_lag_24h":  rng.uniform(2_000, 5_000, n),
        "demand_lag_168h": rng.uniform(100, 500, n),        # predates the spike
        "demand_roll_3h":  rng.uniform(2_000, 5_000, n),
        "demand_roll_24h": rng.uniform(2_000, 5_000, n),
        "temperature_c":   rng.uniform(-10, 40, n),
        "humidity_pct":    rng.uniform(20, 95, n),
        "precip_mm":       rng.uniform(0, 20, n),
        "hour_sin":        np.sin(2 * np.pi * rng.integers(0, 24, n) / 24.0),
        "hour_cos":        np.cos(2 * np.pi * rng.integers(0, 24, n) / 24.0),
        "dow_sin":         np.sin(2 * np.pi * rng.integers(0, 7,  n) / 7.0),
        "dow_cos":         np.cos(2 * np.pi * rng.integers(0, 7,  n) / 7.0),
        "city":            cities,
        "is_holiday":      rng.choice([True, False], n),
    })


# ---------------------------------------------------------------------------
# Tripwire functions (module-level so the assertion messages are informative)
# ---------------------------------------------------------------------------

def _raise_fit(*args, **kwargs) -> None:
    raise AssertionError(
        "pipeline.fit() was called at serve time — training-serving skew bug!"
    )


def _raise_fit_transform(*args, **kwargs) -> None:
    raise AssertionError(
        "pipeline.fit_transform() was called at serve time — training-serving skew bug!"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_scaler(pipeline):
    return (
        pipeline.named_steps["preprocessor"]
        .named_transformers_["numeric"]
        .named_steps["scaler"]
    )


# ---------------------------------------------------------------------------
# Test 1 — demonstrate the bug and measure its magnitude
# ---------------------------------------------------------------------------

def test_skew_bug_demo():
    """Show exactly how much error re-fitting the scaler at serve time introduces.

    The test passes only when:
      - The mean of total_demand diverges by > 2 000 units between training
        and the re-fitted serve scaler.
      - A single demand observation of 4 000 is mis-scaled by > 20 σ.
      - The max absolute error across the numeric block of 100 serve rows
        exceeds 10 (in standardised units).
    """
    # --- training ---
    train_df  = _make_df_normal(n=500, seed=0)
    persisted = fit_pipeline(train_df)

    train_scaler = _get_scaler(persisted)
    train_mean   = train_scaler.mean_[0]    # total_demand fitted mean
    train_std    = train_scaler.scale_[0]   # total_demand fitted std

    # --- serve time: spike batch ---
    serve_df = _make_df_spike(n=100, seed=42)

    # Correct path: persisted training-time statistics
    correct_out = transform(persisted, serve_df)

    # Buggy path: deep-copy the fitted pipeline and re-fit on the spike batch.
    # This is the mistake a naive implementation makes when it calls
    # fit_transform(serve_batch) instead of transform(serve_batch).
    buggy      = copy.deepcopy(persisted)
    buggy_out  = np.asarray(
        buggy.fit_transform(serve_df[list(ALL_FEATURE_COLS)]),
        dtype=np.float64,
    )
    buggy_mean = _get_scaler(buggy).mean_[0]
    buggy_std  = _get_scaler(buggy).scale_[0]

    # --- single-observation spotlight ---
    example   = 4_000.0
    correct_z = (example - train_mean) / train_std
    buggy_z   = (example - buggy_mean) / buggy_std
    z_diff    = abs(correct_z - buggy_z)

    # --- array-level diff on the numeric block ---
    n_num     = len(NUMERIC_COLS)
    abs_diff  = np.abs(correct_out[:, :n_num] - buggy_out[:, :n_num])
    max_diff  = float(abs_diff.max())
    mean_diff = float(abs_diff.mean())

    # Per-column breakdown (training mean vs buggy re-fitted mean)
    col_deltas = [
        (col, train_scaler.mean_[i], _get_scaler(buggy).mean_[i])
        for i, col in enumerate(NUMERIC_COLS)
    ]

    # --- print the comparison table ---
    print("\n" + "=" * 68)
    print("  TRAINING-SERVING SKEW BUG  —  DEMONSTRATION")
    print("=" * 68)
    print(
        f"\n  Scenario: post-holiday demand spike\n"
        f"    training  total_demand ∈ [100, 500]    mean≈300,  std≈116\n"
        f"    serve     total_demand ∈ [2000, 5000]  mean≈3500, std≈866\n"
    )

    print(f"  Scaler means: persisted (correct) vs re-fitted (buggy)")
    print(f"  {'column':24s}  {'persisted mean':>14}  {'buggy mean':>10}  {'Δ mean':>10}")
    print(f"  {'-'*24}  {'-'*14}  {'-'*10}  {'-'*10}")
    for col, pm, bm in col_deltas:
        marker = "  ←" if abs(pm - bm) > 500 else ""
        print(f"  {col:24s}  {pm:14.1f}  {bm:10.1f}  {abs(pm-bm):10.1f}{marker}")

    print(f"\n  Single observation: demand = {example:,.0f}")
    print(f"  {'persisted scaler':28s}  {correct_z:+.2f} σ   ← extreme outlier, correctly flagged")
    print(f"  {'re-fitted scaler':28s}  {buggy_z:+.2f} σ   ← spike signal erased, looks normal")
    print(f"  {'difference':28s}  {z_diff:.1f} σ")

    print(f"\n  Numeric block ({n_num} cols × {len(serve_df)} serve rows)")
    print(f"  max  |correct − buggy|  =  {max_diff:.2f}  (standardised units)")
    print(f"  mean |correct − buggy|  =  {mean_diff:.2f}  (standardised units)")
    print("=" * 68 + "\n")

    # Scaler mean for total_demand must diverge by > 2 000 units
    assert abs(train_mean - buggy_mean) > 2_000, (
        f"Mean divergence for total_demand expected > 2000; "
        f"got {abs(train_mean - buggy_mean):.1f}"
    )

    # The z-score difference for demand=4000 must exceed 20 σ
    assert z_diff > 20, (
        f"Expected >20 σ difference for demand={example}; "
        f"got {z_diff:.2f} σ"
    )

    # Max absolute error across the numeric block must exceed 10
    assert max_diff > 10, (
        f"Expected max diff > 10 (standardised units); got {max_diff:.2f}"
    )


# ---------------------------------------------------------------------------
# Test 2 — assert the real code path is structurally incapable of re-fitting
# ---------------------------------------------------------------------------

def test_real_code_path_cannot_refit():
    """transform() must only call pipeline.transform() — never fit or fit_transform.

    Method
    ------
    We set pipeline.fit and pipeline.fit_transform to functions that raise
    AssertionError.  Setting these as instance attributes shadows the class
    methods for this object only, leaving the Pipeline class itself unchanged.

    If transform() calls pipeline.fit() or pipeline.fit_transform(), the test
    fails immediately with the skew-bug message from the tripwire function.
    If transform() calls only pipeline.transform(), the tripwires are never
    reached and the test passes.

    The assertions at the end verify we received real, non-trivial output.
    """
    pipeline = fit_pipeline(_make_df_normal(n=200, seed=0))
    serve_df = _make_df_spike(n=50, seed=1)

    # Arm the tripwires on this pipeline instance.
    # Instance-level attribute lookup shadows Pipeline.fit / Pipeline.fit_transform.
    pipeline.fit           = _raise_fit           # type: ignore[method-assign]
    pipeline.fit_transform = _raise_fit_transform  # type: ignore[method-assign]

    # Must complete without raising — transform() calls pipeline.transform() only.
    result = transform(pipeline, serve_df)

    assert result.shape[0] == len(serve_df), (
        f"Expected {len(serve_df)} output rows; got {result.shape[0]}"
    )
    assert result.dtype == np.float64, (
        f"Expected float64 output; got {result.dtype}"
    )
    # The spike values should produce large positive z-scores in the correct
    # (persisted) scaler — confirming we are using training-time statistics.
    n_num = len(NUMERIC_COLS)
    numeric_mean_z = float(result[:, :n_num].mean())
    assert numeric_mean_z > 5, (
        f"Serve-time z-scores should be large positive (spike vs training mean); "
        f"mean z = {numeric_mean_z:.2f}.  If this is near 0 the scaler was re-fitted."
    )


# ---------------------------------------------------------------------------
# Test 3 — verify the tripwires themselves actually fire (guard is non-vacuous)
# ---------------------------------------------------------------------------

def test_tripwires_fire_when_fit_transform_is_called_directly():
    """Calling fit_transform() on an armed pipeline must hit the tripwire.

    This test exists to prevent a false sense of security: if the tripwires
    somehow failed to activate, test_real_code_path_cannot_refit would pass
    vacuously even if transform() did call fit_transform().  This test proves
    the tripwires are live.
    """
    pipeline = fit_pipeline(_make_df_normal(n=200, seed=0))
    serve_df = _make_df_spike(n=50, seed=1)

    pipeline.fit           = _raise_fit           # type: ignore[method-assign]
    pipeline.fit_transform = _raise_fit_transform  # type: ignore[method-assign]

    with pytest.raises(AssertionError, match="fit_transform.*called at serve time"):
        # Direct call to the buggy path — must hit the tripwire.
        pipeline.fit_transform(serve_df[list(ALL_FEATURE_COLS)])
