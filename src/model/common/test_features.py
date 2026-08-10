"""The window reduction has TWO implementations. This is what stops them drifting.

`common.features.window_statistics` (numpy) is what `baseline_ridge_stats` and `gbt`
eat; `mlp.model.window_statistics_torch` is the same six reductions inside an
`nn.Module`, because the numpy version cannot run on CUDA inside a forward pass.

⚠️ **One definition in two places is issue TGT-1's shape** — that was
`final_features._stored_target` duplicated "in a second place that could drift from
it". The duplication here is unavoidable (numpy cannot autograd, torch cannot be
handed to xgboost), so it is pinned instead. If these two disagree, a ridge coefficient
and an MLP weight stop describing the same feature and the capacity ladder in
`model/CONTEXT.md` §14 compares models on different inputs.

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
