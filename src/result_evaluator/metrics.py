# src\result_evaluator\metrics.py
"""One metric block for every model, plus the task-specific extras.

## The one-fits-all set, and why these three

Every model in this project — a return regressor, a direction classifier, a
cross-sectional ranker — emits **one score per sample**, and every target is derived
from **one realised forward return per sample**. That is the whole overlap, and it is
enough:

| | question | invariant to |
|---|---|---|
| `ic` (Spearman) | does a higher score mean a higher return? | any monotone rescaling of the score |
| `dir_auc` | does a higher score mean *up* more often? | ditto, and to the base rate |
| `long_short` | what does trading the top vs bottom quintile pay? | ditto, and it is in return units |

Ranking skill, directional skill, economic value. All three read a *score*, never a
*prediction in the target's units*, so a regressor's return, a classifier's `P(up)`
and a ranker's rank are scored on one axis and land in one leaderboard. Nothing else
is shared: RMSE is meaningless for a classifier and `log_loss` is meaningless for a
regressor, so those stay in the per-task extras.

⚠️ **Each of the three is reported with a BAR, not alone.** `feature_selection/
CONTEXT.md` §10 and `final_features/CONTEXT.md` §6 make the same point about
selections: a number without a null is descriptive, not evidence. `dir_auc = 0.54` on
635 overlapping 5-day samples is roughly what noise pays. So every core metric carries
`p_<metric>` and `bar_<metric>` from a block-shuffled null, and `clears_<metric>`.

## ⚠️ The null here is WEAKER than the one in `feature_selection`

`feature_selection.evaluation.null_distribution` re-runs the whole selection on each
draw, because selection is the step that inflates. This module cannot: by the time it
sees a prediction vector the features were chosen, the architecture was chosen and the
epoch was early-stopped on val. So this null answers only:

> given THIS score vector, could its agreement with the target be produced by a target
> with the same autocorrelation and no relation to it?

It prices in the overlap of a 5-day label across consecutive days — the thing that
makes 635 samples behave like ~127 — and nothing about model search. **A run that
fails this bar is dead; a run that clears it is not yet alive.**

⚠️ **The shuffle is by BLOCKS**, reusing `feature_selection.evaluation.block_shuffle`.
A row-wise permutation destroys the label's own autocorrelation, the null comes out far
tighter than reality, and a worthless run clears a bar that was never there.

## The five blocks, added 2026-08-16 — one question each

The three-metric core above answers *ranking*, and for three years of this project that
was the only question asked. It is not the only question a time-series forecast has to
answer, and the two that were missing are the two the literature chapter says nobody
reports (`experiment/experiment_10`: **not one of 23 papers reports a naive baseline**).

| block | question | metrics | fails when |
|---|---|---|---|
| **A. ranking** | does a higher score mean a higher return? | `ic`, `dir_auc`, `hit_rate`, `long_short`, each with a null bar; `ic_se`, `ic_t` | `ic_clears` False |
| **B. vs naive** | is it better than doing nothing? | `mase`, `rmsse`, `skill_score`, `beats_naive`, `naive_kind` | `mase ≥ 1` |
| **C. calibration** | are the magnitudes right, not just the order? | `r2`, `RMSE`, `MAE`, `calibration_slope`, `calibration_intercept`, `pred_sd_ratio` | slope ≠ 1 |
| **D. task extras** | probability quality / error size | `log_loss`, `brier`, `pr_auc`… or `RMSE`… | task-specific |
| **E. degeneracy** | which of the above CANNOT fail on this target? | `target_single_signed`, and the withdrawals it forces | — |

### ⚠️ B is the block that was missing, and `RMSE_zero_baseline` is not it

The zero baseline predicts a return of **0**. On a return target that IS the naive
forecast and the two agree. On a **price-LEVEL** target it predicts *zero dong*, which
nothing can lose to: on 2026-08-16 an LSTM with **R² = −85.6** reported
`beats_zero_baseline = True`. That is CLAUDE.md §5 rule 21 — a metric that cannot fail
is not a pass.

The honest baseline for an h-step forecast is **persistence**: the last value actually
observable at prediction time. This module derives it from `y_true` itself, because for
a level target `y_true[i - h]` *is* the level at the sample's own date — no extra data
needed, and `naive_kind` always says which baseline was used:

| target | `naive_kind` | naive forecast |
|---|---|---|
| single-signed (a LEVEL) | `lag_h` | `y_true[i - h]` — the random walk |
| two-signed (a RETURN) | `zero` | `0.0` — no change |

`mase` is the standard scaled error (**MAE ÷ naive MAE**); `< 1` beats the naive, `≥ 1`
does not. `rmsse` is the same ratio on squared error, and `skill_score = 1 − MSE/MSE_naive`
puts it on an R²-like scale where 0 = "exactly the naive" and negative = worse.

### ⚠️ C separates ranking from calibration, which this project has measured apart

`model/CONTEXT.md` §14 and CLAUDE.md §5c both record the same thing: **the models that
rank best are the ones whose magnitudes are most wrong** (`GBT` cleared its IC bar at
R² = −2.11). `calibration_slope` is the OLS slope of realised on predicted — 1.0 is
perfect, 0 means the prediction carries no magnitude information, and a slope far from 1
beside a good `ic` is the signature of a model that orders well and predicts badly.

### ⚠️ E withdraws rather than reports, and that is deliberate

On a single-signed target every label is positive, so `sign_accuracy` and `hit_rate_pos`
are **1.0 by construction**, core `hit_rate` is pinned at the fraction of scores above
their own median (≈ 0.5 whatever the model does), and `beats_zero_baseline` cannot be
False. All four become `NaN` — CLAUDE.md §5 rule 21, finally shipped: the rule has been
written in the hub since 2026-08-14 while the code still printed `+1.0000`.
"""

from __future__ import annotations

from typing import Dict, Optional, Sequence

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)

from feature_selection.evaluation import block_shuffle, effective_sample

# The tasks a run can declare. `TASKS[task]` names the extras block that runs on top
# of the shared core.
REGRESSION = "regression"
CLASSIFICATION = "classification"
RANKING = "ranking"
TASKS = (REGRESSION, CLASSIFICATION, RANKING)

# The metrics every task reports, in leaderboard order. ⚠️ Anything added here has to
# be computable from `(score, realised return)` alone — that is the only thing three
# different model types have in common.
# ⚠️ `hit_rate`, not `dir_accuracy` (renamed 2026-08-09, issue NAM-1). The legacy
# shim `model/common/metrics.py` reports a `dir_accuracy` measured at a threshold
# of ZERO; this one is at the SCORE'S OWN MEDIAN, because a classifier's score is
# a probability and a ranker's is a rank, so "positive score" is not a shared
# notion. Two quantities under one name in one leaderboard is how a column stops
# meaning anything.
CORE_METRICS = ("ic", "dir_auc", "hit_rate", "long_short")

# Metrics the null is computed for. A subset of CORE_METRICS: `dir_accuracy` is a
# thresholded `dir_auc` and `long_short` is a coarsened `ic`, so bootstrapping all
# four would quadruple the cost to report two numbers twice.
NULLED_METRICS = ("ic", "dir_auc")

# Quantile cut for the long/short spread. A fifth each side — narrow enough that the
# tails are where a real signal lives, wide enough that 635 test samples leave 127 per
# leg rather than a dozen.
LONG_SHORT_Q = 0.2

# Draws in the block bootstrap. 200 gives p-value resolution ~0.005, and each draw is
# a shuffle plus two rank correlations, so the whole null costs well under a second.
NULL_DRAWS = 200

# The percentile of the null a metric must exceed. Same convention as
# `feature_selection.evaluation.NullResult.bar`.
BAR_PERCENTILE = 95


def _clean(y_true, score) -> tuple:
    y_true = np.asarray(y_true, dtype=float).ravel()
    score = np.asarray(score, dtype=float).ravel()
    if len(y_true) != len(score):
        raise ValueError(
            f"{len(y_true)} outcomes and {len(score)} scores — a metric on mismatched "
            f"vectors would silently score a shifted series."
        )
    keep = np.isfinite(y_true) & np.isfinite(score)
    return y_true[keep], score[keep], int((~keep).sum())


def ic(y_true: np.ndarray, score: np.ndarray) -> float:
    """Spearman rank correlation of the score against the realised return."""
    if len(y_true) < 3 or np.unique(score).size < 2:
        return float("nan")
    value, _ = spearmanr(score, y_true)
    return float(value) if value == value else float("nan")


def dir_auc(y_true: np.ndarray, score: np.ndarray) -> float:
    """ROC-AUC of the score against the up/down label. 0.5 = no directional skill."""
    up = (y_true > 0).astype(int)
    if not 0 < up.sum() < len(up):
        return float("nan")
    return float(roc_auc_score(up, score))


def long_short(y_true: np.ndarray, score: np.ndarray, q: float = LONG_SHORT_Q) -> float:
    """Mean realised return of the top `q` of scores minus the bottom `q`.

    ⚠️ In RETURN units, and per sample rather than per period — with overlapping 5-day
    labels this is not a portfolio return and must not be annualised. It answers
    "does acting on the extremes of this score pay", which `ic` does not: an IC can be
    carried entirely by the middle of the distribution, where nothing is traded.
    """
    n = len(y_true)
    k = int(n * q)
    if k < 2:
        return float("nan")
    order = np.argsort(score)
    return float(y_true[order[-k:]].mean() - y_true[order[:k]].mean())


def _ic_uncertainty(value, n_eff) -> Dict[str, float]:
    """`se(ic) ≈ 1/√(n_eff − 1)` and the t-stat that follows from it.

    ⚠️ `n_eff`, never `n`. Consecutive samples share `h − 1` of their `h` label days, so
    640 test rows carry ~128 independent observations and the SE computed on `n` is
    **2.2× too small** at h=5 — which is the difference between a t of 1.4 and a t of
    3.2 on the same number. Matches `feature_selection.evaluation.ic_summary`'s
    `se_ic_per_fold`, so the selection stage and this one can be read side by side.
    """
    try:
        value, n_eff = float(value), float(n_eff)
    except (TypeError, ValueError):
        return {"ic_se": float("nan"), "ic_t": float("nan")}
    if not (value == value) or n_eff <= 1:
        return {"ic_se": float("nan"), "ic_t": float("nan")}
    se = 1.0 / np.sqrt(n_eff - 1.0)
    return {"ic_se": round(float(se), 4), "ic_t": round(float(value / se), 2)}


def single_signed(y_true: np.ndarray) -> bool:
    """Is every non-zero label the same sign? Then the direction metrics cannot fail.

    ⚠️ This is the guard CLAUDE.md §5 rule 21 has asked for since 2026-08-14. It is a
    property of the TARGET, measured, not of the target's NAME — `close_adjust_5day`
    happens to be a level, but so is anything else someone points this at, and a name
    check would miss it. Zeros are ignored so a return series with an unchanged day is
    still two-signed.
    """
    y_true = np.asarray(y_true, dtype=float).ravel()
    y_true = y_true[np.isfinite(y_true) & (y_true != 0)]
    if len(y_true) == 0:
        return True
    return bool(np.all(y_true > 0) or np.all(y_true < 0))


def core_metrics(y_true: np.ndarray, score: np.ndarray) -> Dict[str, float]:
    """The three-plus-one block every task reports. `y_true` is the realised return."""
    y_true, score, dropped = _clean(y_true, score)
    degenerate = single_signed(y_true)
    return {
        "n": int(len(y_true)),
        "n_dropped_nonfinite": dropped,
        "ic": ic(y_true, score),
        "dir_auc": dir_auc(y_true, score),
        # At the score's own median, not at 0 — see CORE_METRICS.
        # `regression_extras.sign_accuracy` is the 0-threshold version.
        # ⚠️ WITHDRAWN when every label shares a sign: `(y_true > 0)` is then a constant
        # and this collapses to "what fraction of scores exceed their own median" ≈ 0.5
        # for every model ever scored. A number that cannot move is not a measurement.
        "hit_rate": (
            float("nan")
            if degenerate
            else float(np.mean((score > np.median(score)) == (y_true > 0)))
        ),
        # ⚠️ NOT withdrawn, and it is the one direction-ish metric that survives: a
        # spread of top minus bottom quintile can still come out negative on a
        # single-signed target. But it is then in the TARGET'S units (dong, not
        # return), so it must not be read as a tradeable spread — `target_single_signed`
        # beside it is what says so.
        "long_short": long_short(y_true, score),
        "target_single_signed": bool(degenerate),
    }


def null_metrics(
    y_true: np.ndarray,
    score: np.ndarray,
    block: int,
    draws: int = NULL_DRAWS,
    seed: int = 0,
    metrics: Sequence[str] = NULLED_METRICS,
) -> Dict[str, float]:
    """Block-shuffle the outcomes `draws` times and report the bar each metric clears.

    Args:
        block: the shuffle block in rows. Pass `lookback + horizon` — one sample's
            whole footprint — the same block `feature_selection` uses.

    ⚠️ Read the module docstring before reading a p-value off this. It nulls the
    SCORE-vs-TARGET agreement, not the search that produced the score.
    """
    y_true, score, _ = _clean(y_true, score)
    if len(y_true) < 3 * block:
        return {f"{m}_p": float("nan") for m in metrics}

    functions = {"ic": ic, "dir_auc": dir_auc, "long_short": long_short}
    observed = {m: functions[m](y_true, score) for m in metrics}
    rng = np.random.default_rng(seed)
    series = pd.Series(y_true)
    drawn: Dict[str, list] = {m: [] for m in metrics}
    for _ in range(draws):
        shuffled, kept = _shuffled_pair(series, block, rng)
        for name in metrics:
            drawn[name].append(functions[name](shuffled, score[kept]))

    out: Dict[str, float] = {"null_draws": draws, "null_block": int(block)}
    for name in metrics:
        values = np.array([v for v in drawn[name] if np.isfinite(v)])
        out[f"{name}_draws_used"] = int(len(values))
        if not len(values) or not np.isfinite(observed[name]):
            out[f"{name}_p"] = float("nan")
            continue
        bar = float(np.percentile(values, BAR_PERCENTILE))
        # ⚠️ Divided by the number of USABLE draws, not by `draws`. An earlier version
        # divided by `draws + 1` while silently discarding the draws that came back
        # NaN — see `_shuffled_pair` — which understated every p-value by the discard
        # ratio and turned worthless runs into "clears the bar".
        out[f"{name}_p"] = max(int((values >= observed[name]).sum()), 1) / (len(values) + 1)
        out[f"{name}_bar"] = bar
        out[f"{name}_null_mean"] = float(values.mean())
        out[f"{name}_clears"] = bool(observed[name] > bar)
    return out


def _shuffled_pair(series: pd.Series, block: int, rng) -> tuple:
    """One block-shuffled outcome vector and the score positions that align with it.

    ⚠️ **`block_shuffle` pads to a whole number of blocks with NaN**, and when the
    padded partial block is permuted forward, those NaNs land inside the retained
    slice. On 635 rows at `block=25` that is 15 NaN in most draws — enough that a
    rank correlation returns NaN and the draw is unusable. Dropping the pair rather
    than the draw keeps ~97% of the rows and all of the draws; the block structure the
    null exists to preserve is untouched, because whole blocks still move together.
    """
    shuffled = block_shuffle(series, block, rng).to_numpy()
    kept = np.isfinite(shuffled)
    return shuffled[kept], kept


def null_draws(
    y_true: np.ndarray,
    score: np.ndarray,
    block: int,
    metric: str = "ic",
    draws: int = NULL_DRAWS,
    seed: int = 0,
) -> Dict:
    """The raw null draws for ONE metric, for plotting.

    `null_metrics` returns the summary; this returns the distribution behind it, with
    the same seed and the same block, so a chart and a p-value cannot disagree.
    """
    functions = {"ic": ic, "dir_auc": dir_auc, "long_short": long_short}
    if metric not in functions:
        raise ValueError(f"metric must be one of {list(functions)}, got {metric!r}")
    y_true, score, _ = _clean(y_true, score)
    observed = functions[metric](y_true, score)
    rng = np.random.default_rng(seed)
    series = pd.Series(y_true)
    drawn = []
    for _ in range(draws):
        shuffled, kept = _shuffled_pair(series, block, rng)
        drawn.append(functions[metric](shuffled, score[kept]))
    values = np.array(drawn)
    values = values[np.isfinite(values)]
    return {
        "metric": metric,
        "observed": observed,
        "draws": values,
        "bar": float(np.percentile(values, BAR_PERCENTILE)) if len(values) else np.nan,
        "p": max(int((values >= observed).sum()), 1) / (len(values) + 1)
        if len(values)
        else np.nan,
    }


# --------------------------------------------------------------- the panel case
#
# ⚠️ Everything above treats the samples as one series. On an N-ticker panel that is
# wrong in three separate ways, each of which flatters the model:
#
#   1. `n_eff = n/h` counts 20 stocks on one date as 20 observations. They are one
#      observation of the market — `feature_selection.evaluation.ic_summary` says so
#      in a comment and `cross_sectional.DailyICSummary` uses `n_dates/h` instead.
#      On the bank panel the row-wise figure overstates the evidence 20×.
#   2. A POOLED Spearman mixes "which stock beats which today" with "is today a good
#      day", and the second is not what a market-neutral book trades. The
#      cross-sectional IC is the per-date rank correlation, averaged over dates.
#   3. `block_shuffle` on a date-sorted panel permutes blocks of ~1.25 dates, tearing
#      each date's cross-section apart. The panel null has to move whole DATES, so
#      each stock keeps its own labels and a day's labels stay one real day's —
#      `cross_sectional.shuffle_dates`, §4 of that module.
#
# The matrix form below is what makes the null affordable: one `(dates × tickers)`
# array, and a draw is a block permutation of its ROWS.

# Minimum names on a date for that date's cross-sectional metric to count. An IC over
# 3 names takes a handful of discrete values in [-1, 1] and would dominate the mean's
# variance while carrying almost nothing — the same floor `cross_sectional.daily_ic`
# applies.
MIN_PANEL_WIDTH = 5


def panel_matrices(
    dates: Sequence, tickers: Sequence, y_true: np.ndarray, score: np.ndarray
) -> tuple:
    """`(Y, S, valid)` as `(n_dates, n_tickers)` arrays, NaN where a name is absent."""
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(pd.Series(dates)),
            "ticker": pd.Series(tickers).astype(str),
            "y": np.asarray(y_true, float).ravel(),
            "s": np.asarray(score, float).ravel(),
        }
    )
    y_wide = frame.pivot_table(index="date", columns="ticker", values="y", aggfunc="first")
    s_wide = frame.pivot_table(index="date", columns="ticker", values="s", aggfunc="first")
    s_wide = s_wide.reindex(columns=y_wide.columns)
    Y, S = y_wide.to_numpy(float), s_wide.to_numpy(float)
    return Y, S, np.isfinite(Y) & np.isfinite(S)


def _row_ranks(values: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Average ranks within each row, NaN where invalid. Ties get the mean rank."""
    out = np.full(values.shape, np.nan)
    for i in range(values.shape[0]):
        keep = valid[i]
        if keep.sum() < 2:
            continue
        row = values[i, keep]
        order = np.argsort(row, kind="stable")
        ranks = np.empty(len(row), float)
        ranks[order] = np.arange(1, len(row) + 1, dtype=float)
        # Average-rank tie correction — without it this disagrees with
        # `scipy.stats.spearmanr` on any column with repeated values.
        unique, inverse, counts = np.unique(row, return_inverse=True, return_counts=True)
        if (counts > 1).any():
            sums = np.zeros(len(unique))
            np.add.at(sums, inverse, ranks)
            ranks = (sums / counts)[inverse]
        out[i, keep] = ranks
    return out


def panel_core_metrics(
    Y: np.ndarray,
    S: np.ndarray,
    valid: np.ndarray,
    q: float = LONG_SHORT_Q,
    min_width: int = MIN_PANEL_WIDTH,
    horizon: int = 1,
) -> Dict[str, float]:
    """The core block computed PER DATE and averaged — the cross-sectional reading.

    ⚠️ `horizon` is not decoration: it is the denominator of `ic_t`. Consecutive daily
    ICs share `h − 1` of their `h` label days, so the independent count is
    `n_dates / h`, never `n_dates` — see `_ic_uncertainty`, which has said the same
    thing on the single-series side since 2026-08-16. The default of 1 means "no label
    overlap", which is honest for a caller that does not know its horizon and is why
    `panel_null_metrics` may leave it alone: that path reads only `CORE_METRICS`.
    """
    width = valid.sum(axis=1)
    usable = width >= min_width
    if not usable.any():
        return {k: float("nan") for k in CORE_METRICS}

    ry = _row_ranks(Y, valid)
    rs = _row_ranks(S, valid)
    ics, aucs, spreads, hits = [], [], [], []
    for i in np.flatnonzero(usable):
        keep = valid[i]
        a, b = ry[i, keep], rs[i, keep]
        if np.std(a) > 0 and np.std(b) > 0:
            ics.append(float(np.corrcoef(a, b)[0, 1]))

        y_row, s_row = Y[i, keep], S[i, keep]
        up = y_row > 0
        n_pos, n_neg = int(up.sum()), int((~up).sum())
        if n_pos and n_neg:
            # AUC as the normalised Mann-Whitney U from the score ranks — exact, and
            # cheap enough to run inside a 200-draw null.
            aucs.append((b[up].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))

        k = max(1, int(round(keep.sum() * q)))
        order = np.argsort(s_row, kind="stable")
        spreads.append(float(y_row[order[-k:]].mean() - y_row[order[:k]].mean()))
        hits.append(float(np.mean((s_row > np.median(s_row)) == up)))

    # ⚠️ **ISSUE NUL-3: ON A PANEL, THIS t IS THE VERDICT AND `ic_clears` IS NOT.**
    # The evaluator's panel null is not label-neutral — its centre moved with the MODEL
    # across three runs (−0.0171 / +0.0076 / +0.0109) and it got both ends wrong,
    # manufacturing a clear for the weakest model and failing the strongest
    # (`model/CONTEXT.md` §16). The daily IC series needs no null: each date is one
    # cross-sectional observation, so its own spread IS the error bar. It was computed
    # inside this function all along and thrown away at the `np.mean` — reported since
    # 2026-08-16.
    # ⚠️ **ISSUE ICT-1, fixed 2026-08-18.** This divided by `sqrt(len(daily))` — the
    # RAW date count — which overstates `ic_t` by exactly `√h`. Measured on
    # `lstm__all__rank_20day__final__d20_h20`: 15.50 reported against +3.47 honest at
    # h=20, a ratio of 4.472 = √20. `evaluate_panel` computed `n_eff = n_dates / h`
    # correctly on the line below and nothing consumed it.
    daily = np.array(ics, dtype=float)
    n_eff = len(daily) / max(1, int(horizon))
    sd = float(daily.std(ddof=1)) if len(daily) > 1 else float("nan")
    se = sd / np.sqrt(n_eff) if n_eff > 1 and sd == sd else float("nan")
    return {
        "ic": float(np.mean(ics)) if ics else float("nan"),
        "dir_auc": float(np.mean(aucs)) if aucs else float("nan"),
        "hit_rate": float(np.mean(hits)) if hits else float("nan"),
        "long_short": float(np.mean(spreads)) if spreads else float("nan"),
        "n": int(valid.sum()),
        "n_dates": int(usable.sum()),
        "n_tickers": int(Y.shape[1]),
        "ic_days_positive": float(np.mean(np.array(ics) > 0)) if ics else float("nan"),
        "ic_daily_sd": round(sd, 4) if sd == sd else float("nan"),
        "ic_se": round(float(se), 4) if se == se else float("nan"),
        "ic_t": (
            round(float(np.mean(daily) / se), 2) if se == se and se > 0 else float("nan")
        ),
    }


def panel_null_metrics(
    Y: np.ndarray,
    S: np.ndarray,
    valid: np.ndarray,
    block: int,
    draws: int = NULL_DRAWS,
    seed: int = 0,
    metrics: Sequence[str] = NULLED_METRICS,
) -> Dict[str, float]:
    """Permute whole DATE BLOCKS of the label matrix, `draws` times.

    ⚠️ Each stock keeps its own labels, moved to a different fortnight, and a day's
    labels stay one real day's — so cross-sectional dispersion, each name's own
    volatility and the label's autocorrelation all survive; only the pairing of a
    day's FEATURES with a day's LABELS is destroyed. This is
    `cross_sectional.shuffle_dates(mode="date_block")` in matrix form.
    """
    observed = panel_core_metrics(Y, S, valid)
    n_dates = Y.shape[0]
    if n_dates < 3 * block:
        return {f"{m}_p": float("nan") for m in metrics}

    rng = np.random.default_rng(seed)
    n_blocks = int(np.ceil(n_dates / block))
    padded = np.concatenate([np.arange(n_dates), np.full(n_blocks * block - n_dates, -1)])
    drawn: Dict[str, list] = {m: [] for m in metrics}
    for _ in range(draws):
        order = padded.reshape(n_blocks, block)[rng.permutation(n_blocks)].ravel()
        order = order[order >= 0]
        shuffled = Y[order]
        # ⚠️ The validity mask moves WITH the labels: a cell whose donor date had no
        # listing for that name is not labelled, exactly as `shuffle_dates` documents.
        drawn_valid = np.isfinite(shuffled) & np.isfinite(S)
        values = panel_core_metrics(shuffled, S, drawn_valid)
        for name in metrics:
            drawn[name].append(values[name])

    out: Dict[str, float] = {"null_draws": draws, "null_block": int(block)}
    for name in metrics:
        values = np.array([v for v in drawn[name] if np.isfinite(v)])
        out[f"{name}_draws_used"] = int(len(values))
        if not len(values) or not np.isfinite(observed[name]):
            out[f"{name}_p"] = float("nan")
            continue
        out[f"{name}_bar"] = float(np.percentile(values, BAR_PERCENTILE))
        out[f"{name}_p"] = max(int((values >= observed[name]).sum()), 1) / (len(values) + 1)
        out[f"{name}_null_mean"] = float(values.mean())
        out[f"{name}_clears"] = bool(observed[name] > out[f"{name}_bar"])
    return out


def evaluate_panel(
    dates: Sequence,
    tickers: Sequence,
    y_true: np.ndarray,
    score: np.ndarray,
    task: str = REGRESSION,
    horizon: int = 5,
    lookback: int = 1,
    draws: int = NULL_DRAWS,
    seed: int = 0,
) -> Dict[str, float]:
    """`evaluate`, for an N-ticker panel. Same keys, cross-sectional meaning.

    ⚠️ `n_eff` is `n_dates / h`, NOT `n / h`. Twenty banks on one date are one
    observation of the market, not twenty — `feature_selection/cross_sectional.py` §2
    is the argument, and on this panel the row-wise figure would overstate the
    evidence twentyfold.
    """
    Y, S, valid = panel_matrices(dates, tickers, y_true, score)
    out = panel_core_metrics(Y, S, valid, horizon=horizon)
    out["task"] = task
    out["grain"] = "panel"
    out["n_eff"] = round(max(1.0, out["n_dates"] / max(1, horizon)), 1)
    out.update(
        panel_null_metrics(Y, S, valid, block=lookback + horizon, draws=draws, seed=seed)
    )
    flat_true = np.asarray(y_true, float).ravel()
    flat_score = np.asarray(score, float).ravel()
    if task == REGRESSION:
        out.update(regression_extras(flat_true, flat_score))
        # ⚠️ **Block B, `P4-12` — panel-aware and not the flat function.** See
        # `panel_accuracy_vs_naive`: the `lag_h` baseline must step back along DATES, and
        # on a flattened panel `i - h` lands on another company.
        out.update(
            panel_accuracy_vs_naive(Y, S, valid, horizon=horizon, dates=sorted(set(
                pd.to_datetime(pd.Series(dates))
            )))
        )
    elif task == CLASSIFICATION:
        out.update(classification_extras(flat_true > 0, flat_score))
    return out


NAIVE_ZERO = "zero"
NAIVE_LAG_H = "lag_h"


def naive_forecast(
    y_true: np.ndarray, horizon: int, kind: Optional[str] = None
) -> tuple:
    """The forecast a person makes with no model, and the name of which one it is.

    Returns `(naive, kind)` where `naive` is NaN wherever the baseline is undefined —
    the first `h` samples of a `lag_h` baseline have nothing behind them.

    ⚠️ **`lag_h` is `y_true[i - h]`, and that is the value observable at sample `i`'s
    own date, not a peek forward.** For a level target `y_true[i]` is the price `h`
    steps AFTER date `i`, so `y_true[i - h]` is the price at date `i` itself. This
    identity is why the naive baseline needs no extra column — but it holds only while
    the rows are consecutive samples in date order, which is how every
    `predictions_<split>.csv` in this repo is written (`evaluator._read_predictions`
    sorts by date). Hand it a shuffled frame and the baseline is wrong without saying
    so, so callers pass `dates` and `accuracy_vs_naive` checks them.
    """
    y_true = np.asarray(y_true, dtype=float).ravel()
    if kind is None:
        kind = NAIVE_LAG_H if single_signed(y_true) else NAIVE_ZERO
    if kind == NAIVE_ZERO:
        return np.zeros_like(y_true), NAIVE_ZERO
    if kind != NAIVE_LAG_H:
        raise ValueError(f"naive kind must be {NAIVE_ZERO!r} or {NAIVE_LAG_H!r}, got {kind!r}")
    naive = np.full_like(y_true, np.nan)
    if horizon < len(y_true):
        naive[horizon:] = y_true[:-horizon]
    return naive, NAIVE_LAG_H


def accuracy_vs_naive(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    horizon: int,
    kind: Optional[str] = None,
    dates: Optional[Sequence] = None,
) -> Dict[str, float]:
    """⚠️ **Block B — is the forecast better than doing nothing?**

    `mase < 1` beats the naive; `skill_score > 0` is the same statement on an R²-like
    scale. This is the block `experiment_10` found missing from **all 23 papers** it
    reviewed, and the one `RMSE_zero_baseline` only pretends to be on a level target.

    ⚠️ `dates` is validated, not trusted: a `lag_h` baseline read off non-consecutive
    rows silently compares against the wrong day. If the gaps are irregular the block
    still computes but `naive_contiguous` is False, and that flag is the thing to read
    before quoting `mase`.
    """
    y_true = np.asarray(y_true, dtype=float).ravel()
    y_pred = np.asarray(y_pred, dtype=float).ravel()
    naive, kind = naive_forecast(y_true, horizon, kind)

    usable = np.isfinite(y_true) & np.isfinite(y_pred) & np.isfinite(naive)
    out: Dict[str, float] = {
        "naive_kind": kind,
        "n_vs_naive": int(usable.sum()),
        # ⚠️ Only meaningful for `lag_h`; a zero baseline needs no ordering at all.
        "naive_contiguous": _contiguous(dates) if kind == NAIVE_LAG_H else True,
    }
    if usable.sum() < 3:
        return {**out, "mase": float("nan"), "rmsse": float("nan"),
                "skill_score": float("nan"), "beats_naive": False}

    err = np.abs(y_pred[usable] - y_true[usable])
    naive_err = np.abs(naive[usable] - y_true[usable])
    sq, naive_sq = err ** 2, naive_err ** 2
    mae_naive, mse_naive = float(naive_err.mean()), float(naive_sq.mean())

    # ⚠️ A zero-error naive is not a division to guess at. It happens when the target
    # is a constant, and the honest answer is that the comparison is undefined.
    mase = float(err.mean() / mae_naive) if mae_naive > 0 else float("nan")
    rmsse = float(np.sqrt(sq.mean() / mse_naive)) if mse_naive > 0 else float("nan")
    return {
        **out,
        "mase": mase,
        "rmsse": rmsse,
        "skill_score": float(1.0 - sq.mean() / mse_naive) if mse_naive > 0 else float("nan"),
        "beats_naive": bool(mase == mase and mase < 1.0),
    }


def panel_accuracy_vs_naive(
    Y: np.ndarray,
    S: np.ndarray,
    valid: np.ndarray,
    horizon: int,
    kind: Optional[str] = None,
    dates: Optional[Sequence] = None,
) -> Dict[str, float]:
    """⚠️ **Block B on a PANEL — `P4-12`, shipped 2026-08-19.**

    `accuracy_vs_naive` was called from `evaluate` only, so every cross-sectional run in
    this repo carried `test_mase = NaN` while the two VCB runs carried a number. §5 rule 2
    says an absent measurement is absent, never a pass — and `mase >= 1` is the column
    `P2-3` calls *the line to quote*, the one that showed the `return_5day` model losing to
    "predict no change" while its `ic` looked respectable.

    ⚠️ **IT IS NOT A COPY-PASTE, AND THIS FUNCTION EXISTS BECAUSE OF WHERE IT BREAKS.**
    The flat version's `lag_h` naive is `y_true[i - h]`, which is only the same date's
    observable value while **rows are consecutive samples in date order**. On a panel each
    date holds N tickers, so `i - h` steps back a fraction of a session and lands on
    another COMPANY: at 150 names and h=20 it reads a different ticker's label from 7
    sessions ago. The baseline would not be wrong by a little, it would be unrelated.

    Here the shift is along the DATE axis of the `(n_dates, n_tickers)` matrix, which is
    per-ticker by construction — `Y[i - h, j]` is name `j`'s own value `h` sessions back.

    ⚠️ On a two-signed label (`kind` resolves to `zero`) the ordering never mattered and
    this returns what the flat version would. The panel path matters for a LEVEL target,
    which is exactly the case §6-0 records as unreadable for every other reason too.
    """
    Y = np.asarray(Y, float)
    S = np.asarray(S, float)
    flat_true = Y[valid]
    if kind is None:
        kind = NAIVE_LAG_H if single_signed(flat_true) else NAIVE_ZERO

    if kind == NAIVE_ZERO:
        naive = np.zeros_like(Y)
    elif kind == NAIVE_LAG_H:
        naive = np.full_like(Y, np.nan)
        if horizon < Y.shape[0]:
            # ⚠️ Axis 0 is DATE and axis 1 is TICKER. Shifting axis 0 is the whole fix.
            naive[horizon:, :] = Y[:-horizon, :]
    else:
        raise ValueError(
            f"naive kind must be {NAIVE_ZERO!r} or {NAIVE_LAG_H!r}, got {kind!r}"
        )

    usable = valid & np.isfinite(naive) & np.isfinite(S)
    out: Dict[str, float] = {
        "naive_kind": kind,
        "n_vs_naive": int(usable.sum()),
        "naive_contiguous": _contiguous(dates) if kind == NAIVE_LAG_H else True,
    }
    if usable.sum() < 3:
        return {**out, "mase": float("nan"), "rmsse": float("nan"),
                "skill_score": float("nan"), "beats_naive": False}

    err = np.abs(S[usable] - Y[usable])
    naive_err = np.abs(naive[usable] - Y[usable])
    mae_naive = float(naive_err.mean())
    mse_naive = float((naive_err ** 2).mean())

    mase = float(err.mean() / mae_naive) if mae_naive > 0 else float("nan")
    rmsse = (
        float(np.sqrt((err ** 2).mean() / mse_naive)) if mse_naive > 0 else float("nan")
    )
    skill = float(1.0 - (err ** 2).mean() / mse_naive) if mse_naive > 0 else float("nan")
    return {**out, "mase": mase, "rmsse": rmsse, "skill_score": skill,
            "beats_naive": bool(mase == mase and mase < 1.0)}


def _contiguous(dates: Optional[Sequence]) -> bool:
    """Are these dates a single run of consecutive observations, evenly spaced?"""
    if dates is None:
        return True
    try:
        stamps = pd.to_datetime(pd.Series(list(dates))).sort_values()
    except (TypeError, ValueError):
        return True
    gaps = stamps.diff().dropna()
    if gaps.empty:
        return True
    # Daily bars skip weekends and holidays, so "consecutive" cannot mean a constant
    # gap. It means no gap large enough to be a missing block — 5 sessions.
    return bool(gaps.max() <= pd.Timedelta(days=7))


def calibration_extras(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """⚠️ **Block C — are the magnitudes right, or only the order?**

    `calibration_slope` is the OLS slope of realised on predicted. 1.0 is perfect; 0
    means the prediction's SIZE carries nothing even if its ORDER does; > 1 means the
    model under-shoots the spread and < 1 that it over-shoots.

    This exists because this project has repeatedly measured the two apart: CLAUDE.md
    §5c's `GBT` had the board's best IC (+0.1263, and it cleared its bar) with
    R² = −2.11, and `ridge_stats` ranked second at R² = −5.19. A model can be worth
    trading on rank and worthless as a forecast, and one column cannot say both.
    """
    y_true, y_pred, _ = _clean(y_true, y_pred)
    if len(y_true) < 3 or np.std(y_pred) == 0:
        return {
            "calibration_slope": float("nan"),
            "calibration_intercept": float("nan"),
            "pred_sd_ratio": float("nan"),
        }
    slope, intercept = np.polyfit(y_pred, y_true, 1)
    sd_true = float(np.std(y_true))
    return {
        "calibration_slope": float(slope),
        "calibration_intercept": float(intercept),
        # sd(prediction) / sd(outcome). A shrunk forecast — the usual outcome of
        # training on noise — sits far below 1 while its `ic` is unaffected.
        "pred_sd_ratio": float(np.std(y_pred) / sd_true) if sd_true > 0 else float("nan"),
    }


def regression_extras(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Error magnitude, and the zero-return baseline it has to beat.

    ⚠️ **`RMSE_zero_baseline` is the WEAKER of the two bars, not the stronger one.**
    `RMSE_zero² = var(y) + mean(y)²`, so it is always ≥ the target's sd, which is the
    RMSE of the mean-predicting model. Therefore `r2 > 0` implies
    `beats_zero_baseline`, but not the reverse: on a target with a non-zero mean a
    model can beat the zero baseline while still being worse than predicting the
    training mean. Read `r2` for that. Both are reported because on `return_5day` the
    mean is small enough that the two bars nearly coincide — and "nearly" is not a
    reason to report only one.
    """
    y_true, y_pred, _ = _clean(y_true, y_pred)
    err = y_pred - y_true
    sse = float(np.sum(err ** 2))
    sst = float(np.sum((y_true - y_true.mean()) ** 2))
    rmse = float(np.sqrt(np.mean(err ** 2)))
    zero = float(np.sqrt(np.mean(y_true ** 2)))
    positive = y_pred > 0
    # ⚠️ Block E. On a single-signed target these three are 1.0 / 1.0 / True whatever
    # the model does — measured 2026-08-16 on a run with R² = −85.6 that reported all
    # three as perfect. `RMSE_zero_baseline` itself stays: it is a NUMBER, and a number
    # that is merely weak is not the same as a verdict that cannot be False.
    degenerate = single_signed(y_true)
    return {
        "RMSE": rmse,
        "MAE": float(np.mean(np.abs(err))),
        "RMSE_zero_baseline": zero,
        "beats_zero_baseline": float("nan") if degenerate else bool(rmse < zero),
        "r2": (1.0 - sse / sst) if sst > 0 else float("nan"),
        # The 0-threshold sign hit rate, kept beside the core's median-threshold one:
        # for a return regressor 0 IS the natural threshold and the two differ
        # whenever the predictions are biased.
        "sign_accuracy": (
            float("nan") if degenerate else float(np.mean(np.sign(y_pred) == np.sign(y_true)))
        ),
        "hit_rate_pos": (
            float("nan")
            if degenerate or not positive.any()
            else float(np.mean(y_true[positive] > 0))
        ),
    }


def classification_extras(
    y_label: np.ndarray, y_prob: np.ndarray, threshold: float = 0.5
) -> Dict[str, float]:
    """Probability quality, and the majority-class baseline accuracy has to beat.

    ⚠️ On an imbalanced target accuracy is uninformative and `beats_majority` is
    False for every model worth having — `probability_gain_5pct_5day` has a 0.071 test
    base rate, so predicting "never" scores 0.929. Read `pr_auc` against `base_rate`.
    """
    y_label, y_prob, _ = _clean(y_label, y_prob)
    label = (y_label >= 0.5).astype(int)
    predicted = (y_prob >= threshold).astype(int)
    base_rate = float(np.mean(label))
    majority = max(base_rate, 1.0 - base_rate)
    accuracy = float(np.mean(predicted == label))
    both = 0 < label.sum() < len(label)
    return {
        "base_rate": base_rate,
        "accuracy": accuracy,
        "majority_baseline_acc": float(majority),
        "beats_majority": bool(accuracy > majority),
        "pr_auc": float(average_precision_score(label, y_prob)) if both else float("nan"),
        "pr_auc_lift": float(average_precision_score(label, y_prob) / base_rate)
        if both and base_rate > 0
        else float("nan"),
        "precision": float(precision_score(label, predicted, zero_division=0)),
        "recall": float(recall_score(label, predicted, zero_division=0)),
        "f1": float(f1_score(label, predicted, zero_division=0)),
        "log_loss": float(
            log_loss(label, np.clip(y_prob, 1e-7, 1 - 1e-7), labels=[0, 1])
        ),
        "brier": float(np.mean((y_prob - label) ** 2)),
    }


def evaluate(
    y_true: np.ndarray,
    score: np.ndarray,
    task: str = REGRESSION,
    horizon: int = 5,
    lookback: int = 1,
    y_pred: Optional[np.ndarray] = None,
    y_label: Optional[np.ndarray] = None,
    draws: int = NULL_DRAWS,
    seed: int = 0,
    dates: Optional[Sequence] = None,
) -> Dict[str, float]:
    """The full metric block for one split of one run.

    Args:
        y_true: the REALISED FORWARD RETURN per sample. Every core metric is measured
            against this, whatever the model was trained on — that is what lets a
            classifier and a regressor share a leaderboard.
        score: the model's per-sample score. Higher must mean "more likely to rise".
        task: `regression`, `classification` or `ranking`; picks the extras block.
        horizon, lookback: set the null's block size, `lookback + horizon`.
        y_pred: predictions in RETURN units, for the regression extras. Defaults to
            `score`, which is the same thing for a return regressor.
        y_label: the 0/1 label, for the classification extras. Defaults to
            `y_true > 0`.

    ⚠️ `n_eff` is reported because `n` overstates the evidence: consecutive samples
    share `h-1` of their `h` label days, so 635 test samples carry about 127
    independent observations. It is still optimistic — see
    `feature_selection.evaluation.effective_sample`.
    """
    if task not in TASKS:
        raise ValueError(f"task must be one of {TASKS}, got {task!r}")

    out = core_metrics(y_true, score)
    out["task"] = task
    out["grain"] = "series"
    out["n_eff"] = round(effective_sample(out["n"], horizon), 1)
    # ⚠️ `n_eff` prices in the LABEL overlap only (`n/h`), matching
    # `feature_selection.evaluation.effective_sample` so the two stages agree.
    # A windowed sample also overlaps its neighbours through its INPUT, and two
    # samples share nothing only when separated by `d + h - 1` rows — the purge
    # gap. That stricter count is reported beside it rather than replacing it,
    # because diverging from upstream silently would be worse than being
    # optimistic openly. On 635 test samples at d=20,h=5: 127.0 vs 26.5.
    out["n_eff_windowed"] = round(
        max(1.0, out["n"] / max(1, lookback + horizon - 1)), 1
    )
    out.update(
        null_metrics(y_true, score, block=lookback + horizon, draws=draws, seed=seed)
    )
    # ⚠️ The error bar on `ic`, reported rather than left to be recomputed by hand.
    # CLAUDE.md §5c had to state "the whole spread is one error bar" in prose because
    # no column carried it: 9 models spanned IC −0.100…+0.126 against SE 0.197, and the
    # largest |t| on that board was +1.42. `n_eff`, not `n` — see below.
    out.update(_ic_uncertainty(out.get("ic"), out.get("n_eff")))

    if task == REGRESSION:
        predicted = score if y_pred is None else y_pred
        out.update(regression_extras(y_true, predicted))
        # Blocks B and C. Both need predictions in the TARGET'S units, so neither runs
        # for a classifier — `P(up)` is not a forecast of anything RMSE can read.
        out.update(accuracy_vs_naive(y_true, predicted, horizon, dates=dates))
        out.update(calibration_extras(y_true, predicted))
    elif task == CLASSIFICATION:
        label = (np.asarray(y_true, float).ravel() > 0) if y_label is None else y_label
        out.update(classification_extras(label, score))
    return out


def summarise(metrics: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    """`{split: metrics}` → one row per split, core columns first."""
    frame = pd.DataFrame(metrics).T
    lead = [c for c in ("task", "n", "n_eff", *CORE_METRICS) if c in frame.columns]
    nulls = [c for c in frame.columns if c.endswith(("_p", "_bar", "_clears"))]
    rest = [c for c in frame.columns if c not in lead + nulls]
    return frame[lead + nulls + rest]


def verdict(metrics: Dict[str, float]) -> str:
    """One sentence saying whether the split cleared its own null.

    ⚠️ Deliberately blunt, and deliberately not a score. The repo's failure mode is a
    plausible-looking number read as a result; this makes the only defensible reading
    the printed one.
    """
    cleared = [m for m in NULLED_METRICS if metrics.get(f"{m}_clears")]
    if not cleared:
        return (
            f"NO SKILL DEMONSTRATED — ic={metrics.get('ic', float('nan')):+.4f} "
            f"(bar {metrics.get('ic_bar', float('nan')):+.4f}), "
            f"dir_auc={metrics.get('dir_auc', float('nan')):.4f} "
            f"(bar {metrics.get('dir_auc_bar', float('nan')):.4f}). "
            f"Both inside what block-shuffled labels produce."
        )
    return (
        f"clears the shuffled-label bar on {', '.join(cleared)} "
        f"(n_eff={metrics.get('n_eff')}). ⚠️ That null does NOT price in feature "
        f"selection, architecture search or early stopping — it is a floor, not a "
        f"result."
    )
