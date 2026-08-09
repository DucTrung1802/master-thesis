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


def core_metrics(y_true: np.ndarray, score: np.ndarray) -> Dict[str, float]:
    """The three-plus-one block every task reports. `y_true` is the realised return."""
    y_true, score, dropped = _clean(y_true, score)
    return {
        "n": int(len(y_true)),
        "n_dropped_nonfinite": dropped,
        "ic": ic(y_true, score),
        "dir_auc": dir_auc(y_true, score),
        # At the score's own median, not at 0 — see CORE_METRICS.
        # `regression_extras.sign_accuracy` is the 0-threshold version.
        "hit_rate": float(np.mean((score > np.median(score)) == (y_true > 0))),
        "long_short": long_short(y_true, score),
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
) -> Dict[str, float]:
    """The core block computed PER DATE and averaged — the cross-sectional reading."""
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

    return {
        "ic": float(np.mean(ics)) if ics else float("nan"),
        "dir_auc": float(np.mean(aucs)) if aucs else float("nan"),
        "hit_rate": float(np.mean(hits)) if hits else float("nan"),
        "long_short": float(np.mean(spreads)) if spreads else float("nan"),
        "n": int(valid.sum()),
        "n_dates": int(usable.sum()),
        "n_tickers": int(Y.shape[1]),
        "ic_days_positive": float(np.mean(np.array(ics) > 0)) if ics else float("nan"),
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
    out = panel_core_metrics(Y, S, valid)
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
    elif task == CLASSIFICATION:
        out.update(classification_extras(flat_true > 0, flat_score))
    return out


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
    return {
        "RMSE": rmse,
        "MAE": float(np.mean(np.abs(err))),
        "RMSE_zero_baseline": zero,
        "beats_zero_baseline": bool(rmse < zero),
        "r2": (1.0 - sse / sst) if sst > 0 else float("nan"),
        # The 0-threshold sign hit rate, kept beside the core's median-threshold one:
        # for a return regressor 0 IS the natural threshold and the two differ
        # whenever the predictions are biased.
        "sign_accuracy": float(np.mean(np.sign(y_pred) == np.sign(y_true))),
        "hit_rate_pos": float(np.mean(y_true[positive] > 0))
        if positive.any()
        else float("nan"),
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

    if task == REGRESSION:
        out.update(regression_extras(y_true, score if y_pred is None else y_pred))
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
