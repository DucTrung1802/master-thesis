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


def window_statistics(X: np.ndarray, dtype=None, chunk_rows: int = 16384) -> np.ndarray:
    """`(n, d, f)` → `(n, f*6)`: last, mean, slope, sd, min, max per channel — and
    `(n, f)` at `d = 1`, where the six collapse to one.

    `dtype` is the OUTPUT's; `None` keeps float64 (every caller before 2026-09-19).
    ⚠️ **A TREE ASKS FOR `np.float32`, AND THAT CHANGES NO NUMBER IT SEES**: XGBoost stores
    features as float32 and sklearn's trees cast `X` to float32 before splitting, so a
    float64 design handed to either was rounded to float32 on the way in. Rounding here
    instead — numpy and both libraries round to nearest-even — hands the trees the same
    values at half the memory, and `test_features` pins that the two designs agree bit for
    bit after the cast. A LINEAR model does not cast, so it keeps the float64 default.

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
        last = X[:, -1, :]
        return last if dtype is None else np.asarray(last, dtype=dtype)
    t = np.arange(d, dtype=float)
    t_centred = t - t.mean()
    denom = float((t_centred ** 2).sum()) or 1.0
    # ⚠️ **ROW BLOCKS INTO A PREALLOCATED OUTPUT, AND THE ARITHMETIC IS STILL float64.**
    # The one-shot version held FOUR full-size copies at once — the caller's float64 `X`,
    # `X - mean` (a second), `tensordot`'s transposed reshape (a third) and the six stats
    # before `concatenate` joined them into a fourth — and on the d=10 basket panel
    # (416,135 x 10 x 151) that is `Unable to allocate 4.68 GiB` on a 15.6 GB machine
    # (2026-09-19, the forest's fit). Every statistic here is computed PER ROW, so a block
    # of rows gives the same numbers as the whole array; each block is upcast to float64
    # first, so float32 input computes exactly what a float64 cast of it did.
    out_dtype = np.float64 if dtype is None else dtype
    out = np.empty((n, f * len(WINDOW_STATS)), dtype=out_dtype)
    for start in range(0, n, chunk_rows):
        stop = min(start + chunk_rows, n)
        block = np.asarray(X[start:stop], dtype=np.float64)
        mean = block.mean(axis=1)
        # ⚠️ **AN EXPLICIT SUM IN A FIXED ORDER, NOT `tensordot`** (2026-09-19). `tensordot`
        # hands the sum to BLAS, whose accumulation order depends on the MATRIX SIZE: the
        # same row's slope came out different in the last bits on a 16,384-row block and
        # on the whole array (up to 5.6e-17 absolute, where the slope nearly cancels). So
        # the old one-shot version was never a pure function of the row either — a row
        # scored inside `train` and again inside `train + val` (the report's refit) could
        # differ. Summing `t_centred[i] * (x_i - mean)` over `i` in index order makes it one.
        slope = np.zeros_like(mean)
        for i in range(d):
            slope += t_centred[i] * (block[:, i, :] - mean)
        slope /= denom
        for k, stat in enumerate((block[:, -1, :], mean, slope, block.std(axis=1),
                                  block.min(axis=1), block.max(axis=1))):
            out[start:stop, k * f:(k + 1) * f] = stat
    return out


def stat_names(feature_columns) -> list:
    """`['last__close_adjust', 'last__…', 'mean__close_adjust', …]` for a design matrix.

    Matches `window_statistics`'s `[stat][channel]` column order, so a coefficient
    vector can be labelled without re-deriving the layout.
    """
    return [f"{stat}__{name}" for stat in WINDOW_STATS for name in feature_columns]
