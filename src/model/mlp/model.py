"""Shallow MLP over the window statistics — Tier 2.

A non-linear model actually sized for this sample: `(n, d, f)` → 6 statistics per
channel → one hidden layer → scalar. At `f=4` and `hidden=8` that is **209 parameters**,
between the 25-parameter ridge and the 3,745-parameter CNN, which is the gap the
capacity ladder in `model/CONTEXT.md` §14 has.

⚠️ **It eats the SAME design matrix as `baseline_ridge_stats` and `gbt`** — six
statistics per channel, `model/common/features.window_statistics`. So the comparison
between the three is purely "linear vs boosted trees vs one hidden layer" on identical
inputs, with the window reduction held fixed. The CNN and the LSTM are the models that
see the raw sequence; these three do not, deliberately.

⚠️ **The reduction happens inside `forward`, not in the dataset.** Every model on this
leaderboard takes `(batch, d, n_features)` — that is the input half of the RUN STANDARD
(§1a) and a package that needed a differently-shaped tensor would break the shared
engine. So the statistics are computed on the GPU, per batch, from the same windows the
LSTM reads.
"""

from __future__ import annotations

import torch
import torch.nn as nn

# Must match `model.common.features.WINDOW_STATS` — this is the torch implementation of
# the same six reductions, so a coefficient here is comparable with a ridge coefficient
# there. Kept as a count because the torch path builds them inline.
N_WINDOW_STATS = 6


def window_statistics_torch(x: torch.Tensor) -> torch.Tensor:
    """`(b, d, f)` → `(b, f*6)`, matching `common.features.window_statistics` exactly.

    ⚠️ Same `[stat][channel]` column order and the same closed-form `slope`. A second
    implementation of one definition is a drift risk (issue **TGT-1**'s shape), and it
    exists only because the numpy version cannot run inside an `nn.Module` on CUDA.
    `test_mlp.py` pins the two to agree.
    """
    b, d, f = x.shape
    t = torch.arange(d, dtype=x.dtype, device=x.device)
    t_centred = t - t.mean()
    denom = (t_centred ** 2).sum().clamp_min(1e-12)
    centred = x - x.mean(dim=1, keepdim=True)
    slope = torch.einsum("bdf,d->bf", centred, t_centred) / denom
    return torch.cat(
        [
            x[:, -1, :],
            x.mean(dim=1),
            slope,
            # ⚠️ `unbiased=False` to match numpy's `ndarray.std()` default (ddof=0).
            x.std(dim=1, unbiased=False),
            x.min(dim=1).values,
            x.max(dim=1).values,
        ],
        dim=1,
    )


class MLPRegressor(nn.Module):
    """One hidden layer over the six window statistics per channel."""

    def __init__(self, n_features: int, hidden_size: int = 8, dropout: float = 0.1):
        super().__init__()
        width = n_features * N_WINDOW_STATS
        self.net = nn.Sequential(
            # BatchNorm over the design, because the six statistics are on wildly
            # different scales (a `sd` and a `last` of a standardised channel are not
            # comparable magnitudes) and a 209-parameter model cannot learn around that.
            nn.BatchNorm1d(width),
            nn.Linear(width, hidden_size),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x):
        return self.net(window_statistics_torch(x)).squeeze(-1)


def build_model(n_features: int, hidden_size: int = 8, dropout: float = 0.1):
    return MLPRegressor(n_features, hidden_size, dropout)


def arch_dict(n_features: int, hidden_size: int = 8, dropout: float = 0.1) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "MLPRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "hidden_size": int(hidden_size),
            "dropout": float(dropout),
        },
    }
