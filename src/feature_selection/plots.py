# src\feature_selection\plots.py
"""The figures — one theme, one palette, applied by role rather than by taste.

Every colour below comes from one validated reference palette, and each is used
for the JOB it does, which is the only decision that matters here:

* **sequential** (one hue, light→dark) for magnitude — importance, coverage,
  rank. `SEQUENTIAL_BLUE`.
* **diverging** (two opposite hues either side of a neutral gray) for polarity —
  anything on a −1…+1 correlation scale, where the midpoint must read as
  "nothing". `DIVERGING_BLUE_RED`.
* **categorical** (fixed hue order, never cycled) for identity — at most two
  series appear on one chart in this module, so it never leaves the palette's
  all-pairs-validated first three slots.

⚠️ **Never a rainbow for magnitude and never a hue at a diverging midpoint.** A
correlation heatmap on `viridis` (matplotlib's default for `imshow`) puts its
brightest colour at +1 and a mid-hue at 0, so "uncorrelated" reads as a signal.
`DIVERGING_BLUE_RED` is symmetric about a gray zero and every function that uses
it forces `vmin=-vmax`, because a diverging scale whose midpoint is not zero is
just a misleading sequential one.

Charts are light-mode: they render inline in Jupyter on a light surface and are
saved as static PNG, so there is no viewer theme to respond to. `SURFACE`/`INK`
below are the one place to change that.
"""

from typing import Optional, Sequence

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

# ------------------------------------------------------------------- the tokens

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
AXIS = "#c3c2b7"

# Categorical slots, in the palette's fixed order. Never cycled, never reordered.
SERIES = ("#2a78d6", "#eb6834", "#1baf7a")  # blue, orange, aqua

# The sequential ramp, steps 100→700 of one hue.
_BLUE_STEPS = [
    "#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec",
    "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab",
    "#184f95", "#104281", "#0d366b",
]
SEQUENTIAL_BLUE = LinearSegmentedColormap.from_list("seq_blue", _BLUE_STEPS)

# The diverging pair: blue ↔ red, warm/cool so the poles read as opposite, with a
# NEUTRAL gray midpoint so zero reads as nothing.
#
# ⚠️ **Equal step count AND matched lightness per arm.** The two arms must climb
# from the midpoint at the same rate, or the scale lies: an arm that starts at a
# mid-tone while the other starts near-white makes −0.2 look stronger than +0.2 on
# the same chart. The blue arm is five documented steps (100/200/400/550/700) and
# the red arm is five steps of matching lightness around the palette's red pole.
_NEUTRAL = "#f0efec"
_BLUE_ARM = ["#cde2fb", "#9ec5f4", "#3987e5", "#1c5cab", "#0d366b"]
_RED_ARM = ["#f6d4d3", "#eda9a8", "#e34948", "#a83535", "#7d2828"]
DIVERGING_BLUE_RED = LinearSegmentedColormap.from_list(
    "div_blue_red",
    list(reversed(_BLUE_ARM)) + [_NEUTRAL] + _RED_ARM,
)

_FONT_STACK = ["Segoe UI", "DejaVu Sans", "sans-serif"]


def use_theme() -> None:
    """Apply the theme globally. Call once, at the top of the notebook.

    Recessive chrome is the rule: hairline SOLID gridlines one shade off the
    surface (dashed grid reads as "threshold"), no top/right spines, muted tick
    labels, and generous padding.
    """
    mpl.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "figure.dpi": 120,
            "savefig.facecolor": SURFACE,
            "savefig.bbox": "tight",
            "axes.facecolor": SURFACE,
            "axes.edgecolor": AXIS,
            "axes.linewidth": 0.8,
            "axes.labelcolor": INK_SECONDARY,
            "axes.titlecolor": INK,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": GRIDLINE,
            "grid.linewidth": 0.8,
            "grid.linestyle": "-",
            "xtick.color": INK_MUTED,
            "ytick.color": INK_MUTED,
            "xtick.labelcolor": INK_SECONDARY,
            "ytick.labelcolor": INK_SECONDARY,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "text.color": INK,
            "font.family": "sans-serif",
            "font.sans-serif": _FONT_STACK,
            "font.size": 10,
            "legend.frameon": False,
            "legend.fontsize": 9,
            "lines.linewidth": 2.0,
            "figure.autolayout": False,
        }
    )


def _titles(ax, title: str, subtitle: str = "") -> None:
    """Title in primary ink; the subtitle carries the caveat, in secondary ink."""
    ax.set_title(title, loc="left", fontsize=12, fontweight="600", pad=18 if subtitle else 10)
    if subtitle:
        ax.text(
            0.0,
            1.02,
            subtitle,
            transform=ax.transAxes,
            fontsize=9,
            color=INK_SECONDARY,
            va="bottom",
        )


def _heatmap(
    ax,
    data: pd.DataFrame,
    cmap,
    vmin: float,
    vmax: float,
    annotate: bool,
    fmt: str,
    cbar_label: str,
):
    """Shared heatmap body: cells, hairline separators, optional value labels."""
    mesh = ax.imshow(data.values, cmap=cmap, vmin=vmin, vmax=vmax, aspect="auto")
    ax.set_xticks(range(data.shape[1]))
    ax.set_xticklabels(data.columns, rotation=45, ha="right")
    ax.set_yticks(range(data.shape[0]))
    ax.set_yticklabels(data.index)
    # A 2px surface gap between cells, not a border drawn around each mark.
    ax.set_xticks(np.arange(-0.5, data.shape[1], 1), minor=True)
    ax.set_yticks(np.arange(-0.5, data.shape[0], 1), minor=True)
    ax.grid(which="minor", color=SURFACE, linewidth=2)
    ax.grid(which="major", visible=False)
    ax.tick_params(which="minor", length=0)

    if annotate:
        span = max(abs(vmin), abs(vmax)) or 1.0
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                value = data.values[i, j]
                if not np.isfinite(value):
                    continue
                # Ink flips on dark cells so the label stays legible; the value is
                # also in the returned DataFrame, which is the table view.
                colour = "#ffffff" if abs(value) / span > 0.55 else INK_SECONDARY
                ax.text(
                    j, i, format(value, fmt), ha="center", va="center",
                    fontsize=7.5, color=colour,
                )

    bar = ax.figure.colorbar(mesh, ax=ax, fraction=0.025, pad=0.02)
    bar.set_label(cbar_label, color=INK_SECONDARY, fontsize=9)
    bar.outline.set_visible(False)
    bar.ax.tick_params(colors=INK_MUTED, labelsize=8)
    return mesh


# ------------------------------------------------------------------ the figures


def plot_coverage(coverage: pd.Series, threshold: float = 1.0, ax=None):
    """Non-null share per feature — a magnitude, so one hue, one series.

    Only columns below `threshold` are drawn: a bar chart of forty features that
    are all 100% complete says nothing that one sentence does not.
    """
    incomplete = coverage[coverage < threshold].sort_values()
    if incomplete.empty:
        print(f"Every feature is {threshold:.0%} complete — nothing to plot.")
        return None
    if ax is None:
        _, ax = plt.subplots(figsize=(8, max(2.2, 0.28 * len(incomplete))))
    ax.barh(incomplete.index, incomplete.values, color=SERIES[0], height=0.7)
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(mpl.ticker.PercentFormatter(xmax=1))
    ax.grid(axis="y", visible=False)
    # Direct-label the worst one only; the axis carries the rest.
    worst = incomplete.index[0]
    ax.text(
        incomplete.iloc[0] + 0.01, 0, f"{incomplete.iloc[0]:.0%}",
        va="center", fontsize=9, color=INK_SECONDARY,
    )
    _titles(
        ax,
        "Feature coverage",
        f"non-null share of labelled rows · {len(incomplete)} incomplete "
        f"column(s) · worst: {worst}",
    )
    return ax


def plot_target_distribution(y: pd.Series, target: str, ax=None):
    """The label's distribution — one series, with zero marked.

    Zero is the only reference that matters on a forward return: the mass either
    side of it is what a directional model is trying to separate.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 3.4))
    values = y.dropna()
    ax.hist(values, bins=80, color=SERIES[0], edgecolor=SURFACE, linewidth=0.4)
    ax.axvline(0, color=AXIS, linewidth=1.2)
    ax.xaxis.set_major_formatter(mpl.ticker.PercentFormatter(xmax=1))
    ax.grid(axis="x", visible=False)
    ax.set_xlabel(target)
    ax.set_ylabel("sessions")
    _titles(
        ax,
        f"Distribution of {target}",
        f"n = {len(values):,} labelled sessions · mean {values.mean():+.2%} · "
        f"sd {values.std():.2%} · {(values > 0).mean():.1%} positive",
    )
    return ax


def plot_target_correlation(target_corr: pd.Series, target: str, top: int = 25, ax=None):
    """Signed rank correlation of each feature with the target.

    Polarity, not magnitude — so a diverging encoding, blue for a positive
    association and red for a negative one, symmetric about zero.
    """
    ordered = target_corr.reindex(
        target_corr.abs().sort_values(ascending=False).index
    ).head(top).iloc[::-1]
    if ax is None:
        _, ax = plt.subplots(figsize=(8, max(2.5, 0.3 * len(ordered))))
    limit = float(np.abs(ordered).max())
    colours = [
        DIVERGING_BLUE_RED((-v / limit + 1) / 2) for v in ordered.values
    ]
    ax.barh(ordered.index, ordered.values, color=colours, height=0.7)
    ax.axvline(0, color=AXIS, linewidth=1.0)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel(f"Spearman ρ vs {target}")
    handles = [
        Patch(facecolor=DIVERGING_BLUE_RED(0.15), label="positive"),
        Patch(facecolor=DIVERGING_BLUE_RED(0.85), label="negative"),
    ]
    ax.legend(handles=handles, loc="lower right")
    _titles(
        ax,
        f"Association with {target}",
        f"top {len(ordered)} by |ρ| · a forward return is mostly noise, so read "
        f"the SCALE before the ranking",
    )
    return ax


def plot_method_heatmap(scores: pd.DataFrame, top: int = 25, ax=None):
    """Feature × ranking-method importance — magnitude, so a sequential ramp.

    The disagreement between columns is the point of the chart: a row that is
    dark all the way across is a feature every method likes, and a row dark in one
    column only is one method's inductive bias.
    """
    ordered = scores.head(top)
    if ax is None:
        _, ax = plt.subplots(
            figsize=(1.15 * ordered.shape[1] + 4.5, max(3, 0.32 * len(ordered)))
        )
    _heatmap(
        ax, ordered, SEQUENTIAL_BLUE, 0.0, 1.0,
        annotate=len(ordered) <= 25, fmt=".2f",
        cbar_label="normalised importance",
    )
    _titles(
        ax,
        "Importance by method",
        f"top {len(ordered)} features by ensemble rank · each column min-max "
        f"normalised to 0-1, so read ACROSS a row, not down a column",
    )
    return ax


def plot_correlation_heatmap(corr: pd.DataFrame, top: int = 25, ax=None):
    """Feature-feature Spearman — polarity on −1…+1, so diverging about gray.

    Ordered by ensemble rank rather than clustered, so the blocks that appear are
    the redundancy the prune is about to act on.
    """
    subset = corr.iloc[:top, :top]
    if ax is None:
        size = max(4.5, 0.36 * len(subset))
        _, ax = plt.subplots(figsize=(size + 2, size))
    _heatmap(
        ax, subset, DIVERGING_BLUE_RED, -1.0, 1.0,
        annotate=len(subset) <= 18, fmt=".1f",
        cbar_label="Spearman ρ",
    )
    _titles(
        ax,
        "Feature redundancy",
        f"top {len(subset)} features, ordered by ensemble rank · dark blocks off "
        f"the diagonal are what the correlation prune removes",
    )
    return ax


def plot_stability_heatmap(stability: pd.DataFrame, top: int = 25, ax=None):
    """Per-fold importance RANK — is a feature useful in every era or in one?

    Rank is a magnitude, so one hue; it is inverted so that rank 1 is the darkest
    cell and the eye reads dark as "important" in both this chart and the method
    heatmap.
    """
    subset = stability.head(top)
    if subset.empty:
        print("No stability matrix — the selector was run with stability=False.")
        return None
    if ax is None:
        _, ax = plt.subplots(
            figsize=(1.3 * subset.shape[1] + 4.5, max(3, 0.32 * len(subset)))
        )
    inverted = subset.max().max() + 1 - subset
    mesh = ax.imshow(
        inverted.values, cmap=SEQUENTIAL_BLUE, aspect="auto",
        vmin=1, vmax=inverted.values.max(),
    )
    ax.set_xticks(range(subset.shape[1]))
    ax.set_xticklabels(subset.columns, rotation=0)
    ax.set_yticks(range(subset.shape[0]))
    ax.set_yticklabels(subset.index)
    ax.set_xticks(np.arange(-0.5, subset.shape[1], 1), minor=True)
    ax.set_yticks(np.arange(-0.5, subset.shape[0], 1), minor=True)
    ax.grid(which="minor", color=SURFACE, linewidth=2)
    ax.grid(which="major", visible=False)
    ax.tick_params(which="minor", length=0)
    span = inverted.values.max()
    for i in range(subset.shape[0]):
        for j in range(subset.shape[1]):
            rank = int(subset.values[i, j])
            colour = "#ffffff" if inverted.values[i, j] / span > 0.55 else INK_SECONDARY
            ax.text(j, i, rank, ha="center", va="center", fontsize=7.5, color=colour)
    bar = ax.figure.colorbar(mesh, ax=ax, fraction=0.025, pad=0.02)
    bar.set_label("importance (dark = better rank)", color=INK_SECONDARY, fontsize=9)
    bar.outline.set_visible(False)
    bar.ax.set_yticks([])
    _titles(
        ax,
        "Rank stability across walk-forward folds",
        "SHAP rank within each expanding training window · a row that jumps is a "
        "regime, not a feature",
    )
    return ax


def plot_ensemble_ranking(result, top: int = 25, ax=None):
    """The headline ranking, with the pruned features shown rather than hidden.

    Two classes, so a legend: kept in the identity hue, pruned in muted ink. The
    pruned bars stay on the chart because "why is `value_matched` missing" is the
    first question anyone asks of a selection.
    """
    # Mean rank is better-when-lower; inverted against the FULL candidate count so
    # the bar lengths do not change when `top` does.
    n = len(result.features)
    scores = (n + 1 - result.ranks["ensemble"].head(top)).iloc[::-1]
    kept = set(result.kept)
    if ax is None:
        _, ax = plt.subplots(figsize=(8.5, max(2.5, 0.3 * len(scores))))
    colours = [SERIES[0] if f in kept else INK_MUTED for f in scores.index]
    ax.barh(scores.index, scores.values, color=colours, height=0.7)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("ensemble score (higher = better mean rank across 6 methods)")
    ax.legend(
        handles=[
            Patch(facecolor=SERIES[0], label=f"kept ({len(result.kept)})"),
            Patch(facecolor=INK_MUTED, label="pruned — redundant or below the cap"),
        ],
        loc="lower right",
    )
    _titles(
        ax,
        f"Ensemble feature ranking for {result.target}",
        f"{len(result.features)} candidates · {len(result.dropped_correlated)} "
        f"dropped as redundant at |ρ| ≥ {result.corr_threshold} · "
        f"{len(result.dropped_constant)} constant columns never scored",
    )
    return ax


def plot_stat_profile(result, method: str = "xgb_shap", top: int = 20, ax=None):
    """`channel × window-stat` importance — HOW a channel carries its signal.

    Magnitude, so one hue. This is the chart the windowed design exists to
    produce: a channel bright on `slope` is a trend, one bright on `sd` is a
    volatility signal, and one bright only on `last` never needed a window.
    """
    profile = result.stat_profile(method=method, top=top)
    if profile.empty or profile.shape[1] <= 1:
        print("No window statistics to profile — the run used lookback=1.")
        return None
    if ax is None:
        _, ax = plt.subplots(
            figsize=(1.25 * profile.shape[1] + 4.5, max(3, 0.32 * len(profile)))
        )
    _heatmap(
        ax, profile, SEQUENTIAL_BLUE, 0.0, 1.0,
        annotate=len(profile) <= 25, fmt=".2f",
        cbar_label=f"normalised {method}",
    )
    _titles(
        ax,
        f"How each channel carries its signal (d = {result.lookback})",
        f"top {len(profile)} channels by ensemble rank · {method} per "
        f"(channel, window statistic), normalised across the whole design matrix",
    )
    return ax


def plot_horizon_comparison(results: dict, ax=None):
    """Out-of-sample IC per fold, one series per horizon.

    Two horizons → two categorical slots and a legend. The question it answers is
    not "which is bigger" but **"do they agree about when the signal was there"** —
    two horizons whose ICs rise and fall together are reading one slow effect;
    two that disagree fold by fold are reading noise.
    """
    series = {
        label: (
            result.validation[result.validation["feature_set"] == "selected"]
            .set_index("fold")["ic"]
        )
        for label, result in results.items()
    }
    wide = pd.DataFrame(series)
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 3.6))
    x = np.arange(len(wide))
    width = 0.8 / max(1, wide.shape[1])
    for i, column in enumerate(wide.columns):
        offset = (i - (wide.shape[1] - 1) / 2) * width
        ax.bar(x + offset, wide[column].values, width * 0.94,
               color=SERIES[i % len(SERIES)], label=column)
    ax.axhline(0, color=AXIS, linewidth=1.0)
    ax.set_xticks(x)
    ax.set_xticklabels(wide.index)
    ax.set_ylabel("Spearman IC (out of sample)")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="best")
    _titles(
        ax,
        "Selected channels, by horizon",
        " · ".join(f"{k}: mean IC {v:+.3f}" for k, v in wide.mean().items()),
    )
    return ax


def plot_null(null, ax=None):
    """The shuffled-label null, with the observed result placed inside it.

    One distribution, one series. The two reference lines are the whole chart:
    the **p95 of the null** is the bar, and where the observed value falls
    relative to it is the result. A bar chart of ICs without this is a chart of
    numbers whose scale nobody knows.
    """
    if not len(null.draws):
        print("No null draws.")
        return None
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 3.4))
    ax.hist(null.draws, bins=12, color=SERIES[0], edgecolor=SURFACE, linewidth=0.6)
    ax.axvline(0, color=GRIDLINE, linewidth=1.0)
    ax.axvline(null.bar, color=INK_MUTED, linewidth=1.6, linestyle="-")
    ax.axvline(null.observed, color=SERIES[1], linewidth=2.4)
    top = ax.get_ylim()[1]
    ax.text(null.bar, top * 0.98, "  null p95 = the bar", color=INK_SECONDARY,
            fontsize=9, va="top")
    ax.text(null.observed, top * 0.80, "  observed", color=SERIES[1], fontsize=9,
            va="top", fontweight="600")
    ax.set_xlabel("mean out-of-sample IC")
    ax.set_ylabel("shuffled-label runs")
    ax.grid(axis="x", visible=False)
    verdict = "CLEARS the bar" if null.clears else "inside the null"
    _titles(
        ax,
        f"Does it beat shuffled labels? — {verdict}",
        f"{null.label or 'run'} · {len(null.draws)} block-shuffled reruns of the whole "
        f"pipeline · observed {null.observed:+.4f} vs bar {null.bar:+.4f} · "
        f"p = {null.p_value:.3f}",
    )
    return ax


def plot_validation(validation: pd.DataFrame, ax=None):
    """Walk-forward out-of-sample IC per fold, selected vs all features.

    Two series → two categorical slots and a legend. Per fold, never averaged
    into one number: with a 5-day overlapping label the fold-to-fold spread IS
    the uncertainty, and a mean would present noise as a result.
    """
    if validation.empty:
        print("No validation frame — the selector was run with validate=False.")
        return None
    wide = validation.pivot(index="fold", columns="feature_set", values="ic")
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 3.6))
    x = np.arange(len(wide))
    width = 0.36
    for i, column in enumerate(list(wide.columns)[:2]):
        # A 2px surface gap between adjacent bars, not a stroke around them.
        ax.bar(
            x + (i - 0.5) * width, wide[column].values, width * 0.94,
            color=SERIES[i], label=column,
        )
    ax.axhline(0, color=AXIS, linewidth=1.0)
    ax.set_xticks(x)
    ax.set_xticklabels(wide.index)
    ax.set_ylabel("Spearman IC (out of sample)")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="best")
    means = wide.mean()
    _titles(
        ax,
        "Does the selection generalise?",
        " · ".join(f"{k}: mean IC {v:+.3f}" for k, v in means.items())
        + " · purged expanding-window folds",
    )
    return ax
