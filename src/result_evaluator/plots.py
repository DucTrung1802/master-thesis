# src\result_evaluator\plots.py
"""The run-level figures — on the theme `feature_selection.plots` already defines.

⚠️ **No second palette.** `feature_selection/plots.py` is the one theme in this repo
("one theme, one palette, no per-chart styling"), so this module imports its surface,
ink, gridline and `SERIES` colours and its `use_theme` / `_titles` / `_legend`
helpers rather than restating them. A figure from a selection report and a figure from
a run must be readable side by side in one document; two palettes would make the same
blue mean two things.

Every chart here obeys the same rules the rest of the repo's charts do:

* **at most two series**, in `SERIES` order, never cycled;
* **a legend whenever there are two**, plus a differing line style, so identity is
  never carried by colour alone;
* **one y-axis** — never a second scale;
* recessive grid and axes, thin marks;
* the caveat lives in the subtitle, where it cannot be cropped off.

## The four figures, and what each is for

| | answers |
|---|---|
| `plot_loss_history` | did it learn, or did it stop at epoch 1? |
| `plot_score_vs_return` | is the relationship in the tails, where it would be traded? |
| `plot_null` | is the observed metric outside what shuffled labels produce? |
| `plot_predictions` | what does the prediction series actually look like over time? |

⚠️ `plot_score_vs_return` shows the **quintile means**, not a fitted line. A least-
squares line through 635 noisy points always slopes somewhere and reads as a finding;
the quintile means are the same quantity `long_short` reports, so the chart and the
metric cannot disagree.
"""

from __future__ import annotations

from typing import Dict, Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from feature_selection.plots import (
    GRIDLINE,
    INK_MUTED,
    INK_SECONDARY,
    MUTED_FILL,
    SERIES,
    _legend,
    _titles,
    use_theme,
)
from result_evaluator import metrics as M

__all__ = [
    "use_theme",
    "plot_loss_history",
    "plot_null",
    "plot_predictions",
    "plot_score_vs_return",
    "run_figures",
]


def plot_loss_history(history: pd.DataFrame, best_epoch: Optional[int] = None, ax=None):
    """Train and val loss per epoch — two series, one axis.

    ⚠️ The single most diagnostic chart for this project's runs: `model/CONTEXT.md`
    §10 records that most classification runs stop at best epoch 1, train loss falling
    while val rises. That is visible here and in no metric.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(7.5, 3.8))
    epochs = np.arange(1, len(history) + 1)
    ax.plot(epochs, history["train"], color=SERIES[0], label="train", lw=2.0)
    ax.plot(epochs, history["val"], color=SERIES[1], label="val", lw=2.0, ls="--")
    subtitle = "loss on the standardised target"
    if best_epoch:
        ax.axvline(best_epoch, color=INK_SECONDARY, lw=1.2, zorder=1)
        ax.annotate(
            f"best epoch {best_epoch}",
            xy=(best_epoch, ax.get_ylim()[1]),
            xytext=(5, -12),
            textcoords="offset points",
            fontsize=9,
            color=INK_SECONDARY,
        )
        subtitle += (
            f" · restored from epoch {best_epoch} of {len(history)}"
            + (" — it never improved on its initialisation" if best_epoch == 1 else "")
        )
    ax.set_xlabel("epoch")
    ax.set_ylabel("loss")
    _titles(ax, "Did it learn?", subtitle)
    _legend(ax, ax.get_lines()[:2], pad=0.20)
    return ax


def plot_score_vs_return(
    y_true: np.ndarray, score: np.ndarray, quantiles: int = 5, ax=None
):
    """The score against the realised return, with the quintile means on top.

    ⚠️ The quintile means ARE `long_short`: the rightmost minus the leftmost is the
    number the metric reports. Reading the cloud and reading the metric cannot come
    apart, which is the point of drawing them together.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(7.5, 3.8))
    y_true = np.asarray(y_true, float).ravel()
    score = np.asarray(score, float).ravel()

    ax.axhline(0, color=GRIDLINE, lw=1.0, zorder=1)
    ax.scatter(score, y_true * 100, s=9, color=MUTED_FILL, edgecolor="none", zorder=2)

    bins = pd.qcut(pd.Series(score).rank(method="first"), quantiles, labels=False)
    means = pd.Series(y_true * 100).groupby(bins).mean()
    centres = pd.Series(score).groupby(bins).mean()
    ax.plot(
        centres, means, color=SERIES[0], lw=2.0, marker="o", ms=8, zorder=3,
        label=f"mean return per {quantiles}-tile",
    )

    spread = M.long_short(y_true, score)
    ax.set_xlabel("model score")
    ax.set_ylabel("realised forward return (%)")
    _titles(
        ax,
        "Does the score pay in the tails?",
        f"{len(y_true)} samples · long-short spread {spread * 100:+.3f}% per sample · "
        f"IC {M.ic(y_true, score):+.4f} — overlapping labels, so this is not a "
        f"portfolio return",
    )
    _legend(ax, ax.get_lines()[-1:], pad=0.20)
    return ax


def plot_null(
    observed: float,
    draws: Sequence[float],
    bar: float,
    label: str = "",
    metric: str = "ic",
    p_value: Optional[float] = None,
    ax=None,
):
    """The block-shuffled null, with the observed metric placed inside it.

    Deliberately the same chart as `feature_selection.plots.plot_null`, because it
    answers the same question one stage later — and a reader who has learned to read
    one should not have to learn the other.
    """
    draws = np.asarray([d for d in draws if np.isfinite(d)], dtype=float)
    if not len(draws):
        print("no null draws")
        return None
    if ax is None:
        _, ax = plt.subplots(figsize=(7.5, 3.4))

    ax.hist(draws, bins=18, color=MUTED_FILL, edgecolor="none", zorder=2)
    head = ax.get_ylim()[1]
    ax.axvline(bar, color=INK_SECONDARY, lw=1.4, zorder=3)
    ax.annotate(
        "null p95 — the bar",
        xy=(bar, head * 0.72), xytext=(5, 0), textcoords="offset points",
        fontsize=9, color=INK_SECONDARY,
    )
    ax.axvline(observed, color=SERIES[1], lw=2.4, zorder=4)
    ax.annotate(
        f"observed {observed:+.4f}",
        xy=(observed, head * 0.92), xytext=(5, 0), textcoords="offset points",
        fontsize=9, color=INK_SECONDARY, fontweight="600",
    )
    ax.set_xlabel(f"{metric} on block-shuffled outcomes")
    ax.set_ylabel("draws")
    ax.grid(axis="x", visible=False)
    clears = observed > bar
    _titles(
        ax,
        f"{metric} vs shuffled labels — {'CLEARS the bar' if clears else 'inside the null'}",
        f"{label} · {len(draws)} draws · bar {bar:+.4f}"
        + (f" · p = {p_value:.3f}" if p_value is not None else "")
        + " · this null does NOT price in feature selection or model search",
    )
    return ax


def plot_predictions(frame: pd.DataFrame, title: str = "Test", ax=None):
    """Realised and predicted return over time — two series, one axis.

    ⚠️ The useful reading is usually the SPREAD, not the tracking: a model that has
    learned nothing predicts a nearly flat line near the target's train mean, and that
    is obvious here long before any metric says so.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(9.5, 3.6))
    x = pd.to_datetime(frame["date"]) if "date" in frame else np.arange(len(frame))
    ax.axhline(0, color=GRIDLINE, lw=1.0, zorder=1)
    ax.plot(x, frame["y_true"] * 100, color=MUTED_FILL, lw=1.2, label="realised", zorder=2)
    column = "y_pred" if "y_pred" in frame else "y_prob"
    ax.plot(x, frame[column] * 100, color=SERIES[0], lw=1.6, label="predicted", zorder=3)
    ax.set_ylabel("return (%)" if column == "y_pred" else "P(up) ×100")
    spread = float(frame[column].std() / max(frame["y_true"].std(), 1e-12))
    _titles(
        ax,
        f"{title} — realised vs predicted",
        f"predicted sd is {spread:.2f}× the realised sd"
        + (" — the model is barely moving" if spread < 0.25 else ""),
    )
    _legend(ax, ax.get_lines()[-2:], pad=0.20)
    return ax


def run_figures(
    run_dir: str,
    split: str = "test",
    draws: Optional[Dict] = None,
    save: bool = True,
):
    """The four figures for one run folder, saved into its `results/`.

    Returns the matplotlib figure. Reads only files the run already wrote, so it can
    be called on a run trained in another session.
    """
    import os

    use_theme()
    results = os.path.join(run_dir, "results")
    predictions = pd.read_csv(os.path.join(results, f"predictions_{split}.csv"))
    score = predictions["y_pred" if "y_pred" in predictions else "y_prob"].to_numpy()
    y_true = predictions["y_true"].to_numpy(dtype=float)

    figure, axes = plt.subplots(2, 2, figsize=(15.5, 8.0))
    history_path = os.path.join(results, "loss_history.csv")
    if os.path.exists(history_path):
        history = pd.read_csv(history_path, index_col=0)
        best = int(history["val"].idxmin()) + 1
        plot_loss_history(history, best_epoch=best, ax=axes[0][0])
    else:
        axes[0][0].axis("off")

    plot_score_vs_return(y_true, score, ax=axes[0][1])
    plot_predictions(predictions, title=split.title(), ax=axes[1][0])

    if draws is not None:
        plot_null(
            observed=draws["observed"], draws=draws["draws"], bar=draws["bar"],
            label=os.path.basename(run_dir), metric=draws.get("metric", "ic"),
            p_value=draws.get("p"), ax=axes[1][1],
        )
    else:
        axes[1][1].axis("off")

    figure.tight_layout(h_pad=3.0, w_pad=2.5)
    if save:
        figure.savefig(os.path.join(results, f"figures_{split}.png"), dpi=130)
    return figure
