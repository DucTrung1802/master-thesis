# src\model\common\features.py
"""Window → vector reductions, shared by every model that cannot eat a sequence.

A tree and an MLP take a vector, so a `(n, d, f)` window has to be reduced first. This
is that reduction, and there is one of it because **the feature selection ranked these
channels under exactly this design** — `feature_selection.windows.window_design` uses
the same six statistics, and `feature_importance.csv`'s `best_stat__*` columns name
them. A model reducing the window differently would be fed something the ranking never
scored.

⚠️ `last` is the raw value at day `N`, so `lookback=1` reduces to the un-windowed case
exactly. That property is what makes `d=1` a special case of this rather than a
different pipeline.
"""

from __future__ import annotations

import numpy as np

# ⚠️ Order and definitions are fixed: a coefficient or an importance computed on this
# design is read against `feature_importance.csv`'s `best_stat` column, which names the
# same six in the same order.
WINDOW_STATS = ("last", "mean", "slope", "sd", "min", "max")


def window_statistics(X: np.ndarray) -> np.ndarray:
    """`(n, d, f)` → `(n, f*6)`: last, mean, slope, sd, min, max per channel — and
    `(n, f)` at `d = 1`, where the six collapse to one.

    ⚠️ **AT `d = 1` THE SIX STATISTICS ARE ONE** (`WST-1`, fixed 2026-09-19). A one-row
    window has no dispersion and no trend: `last`, `mean`, `min` and `max` are the same
    number, `sd` is 0, and `slope` divides by 0. Emitting all six turned a 151-channel
    panel into **906 columns of which 152 were distinct** — four identical copies plus 302
    constant zeros. ⚠️ **The cost was not only the 6x fit**: `max_features` / `colsample`
    exist to DECORRELATE the trees, and drawing 30 % of 906 mostly-duplicate columns draws
    nearly every channel, so they decorrelated far less than their values claimed. The
    degenerate case now returns the row itself, `(n, f)`.

    `slope` is the least-squares gradient over the window in closed form against a fixed
    time index — no per-sample `polyfit`, which on 2,939 × 4 would dominate the fit.

    ⚠️ Column ORDER is `[stat][channel]`: the first `f` columns are every channel's
    `last`, the next `f` its `mean`, and so on. A reader mapping a coefficient back to a
    (channel, stat) pair must use `divmod(i, f)` → `(stat_index, channel_index)`.
    """
    n, d, f = X.shape
    if d == 1:
        # see the docstring: the other five statistics are this column, 0, or undefined
        return X[:, -1, :]
    t = np.arange(d, dtype=float)
    t_centred = t - t.mean()
    denom = float((t_centred ** 2).sum()) or 1.0
    slope = np.tensordot(
        X - X.mean(axis=1, keepdims=True), t_centred, axes=([1], [0])
    ) / denom
    return np.concatenate(
        [
            X[:, -1, :],
            X.mean(axis=1),
            slope,
            X.std(axis=1),
            X.min(axis=1),
            X.max(axis=1),
        ],
        axis=1,
    )


def stat_names(feature_columns) -> list:
    """`['last__close_adjust', 'last__…', 'mean__close_adjust', …]` for a design matrix.

    Matches `window_statistics`'s `[stat][channel]` column order, so a coefficient
    vector can be labelled without re-deriving the layout.
    """
    return [f"{stat}__{name}" for stat in WINDOW_STATS for name in feature_columns]
