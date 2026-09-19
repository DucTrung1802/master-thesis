"""The window reduction has TWO implementations. This is what stops them drifting.

`common.features.window_statistics` (numpy) is what `baseline_ridge_stats` and `gbt`
eat; `mlp.model.window_statistics_torch` is the same six reductions inside an
`nn.Module`, because the numpy version cannot run on CUDA inside a forward pass.

⚠️ **One definition in two places is issue TGT-1's shape** — that was
`final_features._stored_target` duplicated "in a second place that could drift from
it". The duplication here is unavoidable (numpy cannot autograd, torch cannot be
handed to xgboost), so it is pinned instead. If these two disagree, a ridge coefficient
and an MLP weight stop describing the same feature and the capacity ladder in
`.claude/context/model.md` §14 compares models on different inputs.

    python -m pytest model/common/test_features.py -q
"""

from __future__ import annotations

import numpy as np
import pytest
import torch

from model.common.features import WINDOW_STATS, stat_names, window_statistics
from model.mlp.model import window_statistics_torch


@pytest.mark.parametrize("shape", [(37, 20, 4), (5, 1, 3), (128, 20, 1)])
def test_the_two_implementations_agree(shape):
    X = np.random.default_rng(0).normal(size=shape)
    numpy_out = window_statistics(X)
    torch_out = window_statistics_torch(torch.tensor(X)).numpy()
    assert numpy_out.shape == torch_out.shape
    assert np.abs(numpy_out - torch_out).max() < 1e-10


def test_the_design_is_stat_major_not_channel_major():
    """⚠️ Column order is `[stat][channel]`, and a reader mapping a coefficient back
    must know which. The first `f` columns are every channel's `last`."""
    X = np.zeros((2, 20, 3))
    X[:, -1, :] = [1.0, 2.0, 3.0]        # only the LAST row is non-zero
    out = window_statistics(X)
    assert out.shape == (2, 18)
    np.testing.assert_allclose(out[0, :3], [1.0, 2.0, 3.0])   # the three `last` values
    assert stat_names(["a", "b", "c"])[:3] == ["last__a", "last__b", "last__c"]


def test_slope_is_the_least_squares_gradient():
    """A perfectly linear ramp of slope 2 must reduce to `slope == 2`."""
    d = 20
    ramp = (np.arange(d, dtype=float) * 2.0).reshape(1, d, 1)
    out = window_statistics(ramp)
    slope_index = WINDOW_STATS.index("slope")
    assert out[0, slope_index] == pytest.approx(2.0)


def test_last_makes_lookback_one_the_unwindowed_case():
    """⚠️ At `d=1`, `last` IS the raw value — the property that makes an un-windowed
    run a special case of this rather than a different pipeline."""
    X = np.array([[[3.0, -1.0]]])
    out = window_statistics(X)
    np.testing.assert_allclose(out[0, :2], [3.0, -1.0])


def test_a_one_row_window_is_the_row_itself_on_both_implementations():
    """⚠️ `WST-1`: at `d = 1` the six statistics are one, and emitting six was a defect.

    `last`/`mean`/`min`/`max` are the same number, `sd` is 0 and `slope` divides by 0, so a
    151-channel panel used to become 906 columns holding 152 distinct ones — six times the
    fit, and `max_features`/`colsample` drawing their decorrelation from duplicates.
    """
    import numpy as np
    import torch

    from model.common.features import window_statistics
    from model.mlp.model import window_statistics_torch

    rng = np.random.default_rng(0)
    X = rng.normal(size=(64, 1, 7))
    out = window_statistics(X)
    assert out.shape == (64, 7)                       # not (64, 42)
    assert np.array_equal(out, X[:, -1, :])
    assert np.allclose(window_statistics_torch(torch.tensor(X)).numpy(), out)

    # and a real window is untouched: six statistics, in the documented order
    wide = window_statistics(rng.normal(size=(64, 5, 7)))
    assert wide.shape == (64, 42)


def test_window_statistics_is_a_pure_function_of_the_row():
    """⚠️ The `tensordot` version summed in BLAS order, which depends on the MATRIX SIZE.

    Measured 2026-09-19: a 16,384-row block and the whole array disagreed in the last bits
    of `slope` (up to 5.6e-17) — so a row scored inside `train` and again inside
    `train + val` (the report's refit) was not guaranteed the same design. The fixed-order
    sum makes chunk size and neighbours irrelevant, bit for bit.
    """
    rng = np.random.default_rng(0)
    X = rng.normal(size=(5_003, 10, 7)).astype(np.float32)
    whole = window_statistics(X, chunk_rows=5_003)
    for rows in (1, 7, 1024):
        assert np.array_equal(window_statistics(X, chunk_rows=rows), whole)
    assert np.array_equal(window_statistics(X[100:300]), whole[100:300])


def test_float32_design_is_the_float64_design_rounded():
    """A tree asks for float32 — which is what XGBoost and sklearn round its input to."""
    rng = np.random.default_rng(1)
    X = rng.normal(size=(2_000, 10, 5)).astype(np.float32)
    assert np.array_equal(window_statistics(X, dtype=np.float32),
                          window_statistics(X).astype(np.float32))
    assert window_statistics(X, dtype=np.float32).dtype == np.float32


def test_float32_input_computes_what_a_float64_cast_of_it_did():
    """Each row block is upcast before any arithmetic, so the engine's copy bought nothing."""
    rng = np.random.default_rng(2)
    X = rng.normal(size=(3_000, 10, 6)).astype(np.float32)
    assert np.array_equal(window_statistics(X), window_statistics(X.astype(np.float64)))
