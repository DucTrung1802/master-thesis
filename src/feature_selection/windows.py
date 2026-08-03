# src\feature_selection\windows.py
"""Turn a daily panel into windowed training samples, and score CHANNELS not columns.

The model this exists for takes one sample as a `(d, n)` matrix — days `N-d+1 … N`
of `n` features — and predicts `y_N`, the return over `[N, N+h]`. A tree takes a
vector, so a feature selector for that model has to reduce the window to something
flat without changing the question being asked.

## The unit of selection is a CHANNEL

The model consumes all `d` days of a feature or none of them. So:

* a feature is scored by its **whole window**, never by its value on day `N`;
* the redundancy prune drops **whole channels** — dropping `close` at lag 3 while
  keeping it at lag 4 would be meaningless to a model that reads the column;
* every per-column score is aggregated back to one number per channel before
  anything ranks or prunes.

## Two ways to flatten, and why this one

| | columns at n=27, d=20 | at n=900 (`pool__ta`) |
|---|---|---|
| full flatten — one column per (feature, lag) | 540 | **18,000** |
| **summary stats** — one column per (feature, stat) | **162** | 5,400 |

The full flatten is what `unified_schema_creator.ipynb` did (its `lookback`
parameter), and it is what cost ~6.6 h on `pool__ta`. It also answers a question
nobody asked — *which lag matters* — when the model will read every lag anyway.

The six stats answer the question that is actually useful: does this channel
matter through its **level** (`last`, `mean`), its **direction** (`slope`), or its
**dispersion** (`sd`, `min`, `max`)? That is diagnostic output a lag-importance
profile does not give you, at a third of the width.

⚠️ **`last` is the raw value at day `N`** — so `lookback=1` with stats reduced to
`("last",)` is exactly the un-windowed selector, and the two are comparable.

## ⚠️ The rows this costs

A window ending at `N` needs `N >= d-1`, and `y_N` needs `N <= L-1-h`. So usable
samples are `L - d - h + 1`: on VCB's 4,235 sessions at `d=20`, that is **4,211**
for `h=5` and **4,206** for `h=10`. The two horizons therefore do NOT have the
same sample count, which is why each target's own tail is dropped rather than a
shared one.
"""

import warnings
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

# The six window summaries, in the order they appear in a design matrix. `last` is
# first because it is the identity — the un-windowed feature.
WINDOW_STATS: Tuple[str, ...] = ("last", "mean", "slope", "sd", "min", "max")

# `<channel>` and `<stat>` are joined by this. Two underscores, matching the repo's
# convention (`pool__basic`, `<target>__lb20__ta__12`), so a channel name that
# itself contains a single underscore — `close_adjust`, `foreign_net_value`, all of
# them — can still be recovered by splitting on the LAST occurrence.
STAT_SEPARATOR = "__"


def design_column(channel: str, stat: str) -> str:
    return f"{channel}{STAT_SEPARATOR}{stat}"


def split_design_column(column: str) -> Tuple[str, str]:
    """`"close_adjust__slope"` → `("close_adjust", "slope")`."""
    channel, _, stat = column.rpartition(STAT_SEPARATOR)
    return channel, stat


def window_design(
    frame: pd.DataFrame,
    lookback: int,
    stats: Sequence[str] = WINDOW_STATS,
) -> pd.DataFrame:
    """Summarise each column over its trailing `lookback`-day window.

    Returns a frame indexed by the ORIGINAL row positions that have a full window
    (`lookback-1 …`), with one column per `(channel, stat)`.

    ⚠️ **Every statistic is computed over `[N-lookback+1, N]` inclusive — it ends
    ON day `N` and never reaches past it.** That is the whole look-ahead contract
    of this function; `sliding_window_view` makes it structural rather than a
    convention, because the view simply has no elements after `N`.

    ⚠️ **`slope` is the OLS slope in units of "feature per day", not a correlation.**
    Over a fixed time index `0 … d-1` the denominator `Σ(t-t̄)²` is a constant, so
    the whole thing is one dot product against a fixed centred ramp — which is why
    this is a reduction over a strided view and not a `rolling().apply()`.

    Args:
        frame: the daily panel, one row per session, already numeric.
        lookback: `d`, in ROWS. `pool__basic` is one row per session, so a row
            offset is a trading-day offset.
        stats: which summaries to compute; a subset of `WINDOW_STATS`.

    Raises:
        ValueError: unknown stat, `lookback < 1`, or a frame shorter than the
            window.
    """
    unknown = [s for s in stats if s not in WINDOW_STATS]
    if unknown:
        raise ValueError(f"unknown window stat(s) {unknown}; pick from {WINDOW_STATS}")
    if lookback < 1:
        raise ValueError(f"lookback must be >= 1, got {lookback}")
    if len(frame) < lookback:
        raise ValueError(
            f"{len(frame)} rows cannot form a single {lookback}-day window."
        )
    if lookback == 1 and tuple(stats) == ("last",):
        # The degenerate case is the identity, and saying so keeps the un-windowed
        # path free of a pointless copy.
        return frame.rename(columns={c: design_column(c, "last") for c in frame})

    values = frame.to_numpy(np.float64)
    # (n_windows, lookback, n_channels) — a VIEW, so nothing is copied here. Each
    # reduction below streams over it and materialises only its own (n_windows,
    # n_channels) result.
    windows = np.lib.stride_tricks.sliding_window_view(
        values, window_shape=lookback, axis=0
    ).transpose(0, 2, 1)

    ramp = np.arange(lookback, dtype=np.float64)
    centred = ramp - ramp.mean()
    denominator = float((centred**2).sum()) or np.nan

    # How many observations each window actually has. `prop_buy_vol` and friends are
    # NULL for years, so an ALL-NaN window is a legitimate state, not an error — the
    # right answer for every statistic over it is NaN, and the imputer downstream
    # fills it from the training median. numpy says so with a RuntimeWarning per
    # reduction, which would bury the notebook in noise; the expected case is
    # declared here instead of shouted about eight times.
    finite = np.count_nonzero(np.isfinite(windows), axis=1)
    empty = finite == 0

    computed: Dict[str, np.ndarray] = {}
    with np.errstate(invalid="ignore"), warnings.catch_warnings():
        warnings.filterwarnings("ignore", r"(Mean|All-NaN|Degrees of freedom)", RuntimeWarning)
        for stat in stats:
            if stat == "last":
                computed[stat] = windows[:, -1, :]
            elif stat == "mean":
                computed[stat] = np.nanmean(windows, axis=1)
            elif stat == "sd":
                computed[stat] = np.nanstd(windows, axis=1)
            elif stat == "min":
                computed[stat] = np.nanmin(windows, axis=1)
            elif stat == "max":
                computed[stat] = np.nanmax(windows, axis=1)
            elif stat == "slope":
                # ⚠️ NaN wherever the window holds fewer than two observations. The
                # `nan_to_num` below is what makes the dot product ignore missing
                # days, but on its own it would report a FLAT TREND (0.0) for a
                # window with no data at all — a claim the data cannot support, and
                # one that would rank an empty channel above a genuinely flat one.
                centred_values = windows - np.nanmean(windows, axis=1, keepdims=True)
                slope = (
                    np.einsum("k,ikj->ij", centred, np.nan_to_num(centred_values))
                    / denominator
                )
                computed[stat] = np.where(finite >= 2, slope, np.nan)
            # Every other statistic already yields NaN on an empty window; this
            # keeps them consistent with `slope` rather than relying on it.
            if stat != "slope":
                computed[stat] = np.where(empty, np.nan, computed[stat])

    # Grouped by CHANNEL, so a channel's stats sit together in the design matrix and
    # a per-channel slice is contiguous.
    columns, blocks = [], []
    for position, channel in enumerate(frame.columns):
        for stat in stats:
            columns.append(design_column(channel, stat))
            blocks.append(computed[stat][:, position])

    return pd.DataFrame(
        np.column_stack(blocks),
        columns=columns,
        index=frame.index[lookback - 1 :],
    )


def aggregate_to_channels(
    scores: pd.DataFrame, channels: Sequence[str]
) -> pd.DataFrame:
    """Per-`(channel, stat)` scores → one row per channel, by MAX over its stats.

    ⚠️ **Max, not sum, and the difference matters.** `mean` and `last` are near
    duplicates of each other on a slow-moving series, so a sum would score a
    channel twice for saying one thing and rank it above a channel that is
    genuinely informative through a single statistic. Max asks "is there ANY view
    of this window that carries signal", which is the question a model that sees
    the whole window will answer for itself.
    """
    owner = pd.Series(
        [split_design_column(c)[0] for c in scores.index], index=scores.index
    )
    aggregated = scores.groupby(owner, sort=False).max()
    return aggregated.reindex(channels)


def best_stat_per_channel(
    scores: pd.DataFrame, channels: Sequence[str], method: str
) -> pd.Series:
    """Which window statistic carried each channel, for one ranking method.

    The interpretive payoff of the whole design: a channel that wins on `slope` is
    a trend, one that wins on `sd` is a volatility signal, and one that wins on
    `last` did not need a window at all.
    """
    owner = pd.Series(
        [split_design_column(c)[0] for c in scores.index], index=scores.index
    )
    winners = scores[method].groupby(owner, sort=False).idxmax()
    return (
        winners.map(lambda c: split_design_column(c)[1] if isinstance(c, str) else None)
        .reindex(channels)
        .rename(f"best_stat__{method}")
    )


def stat_matrix(
    scores: pd.DataFrame, channels: Sequence[str], method: str, stats: Sequence[str]
) -> pd.DataFrame:
    """`channel × stat` scores for one method — the input of the stat heatmap."""
    rows = {}
    for channel in channels:
        rows[channel] = {
            stat: scores[method].get(design_column(channel, stat), np.nan)
            for stat in stats
        }
    return pd.DataFrame.from_dict(rows, orient="index", columns=list(stats))


def usable_sample_count(n_rows: int, lookback: int, horizon: int) -> int:
    """`L - d - h + 1` — how many `(window, label)` pairs a panel of `L` rows gives."""
    return max(0, n_rows - lookback - horizon + 1)
