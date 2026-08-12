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
  first three slots.
* **de-emphasis** — muted ink for "present but not the subject" (pruned
  channels). Highlight-one-gray-the-rest is an emphasis pattern, not a fourth
  categorical slot.

⚠️ **The palette is COMPUTED, not eyeballed.** `SERIES` was run through the
six-checks validator (OKLCH lightness band, chroma floor, Machado-Oliveira-
Fernandes CVD ΔE, normal-vision floor, WCAG contrast) at all pairs on this
surface: worst CVD ΔE **9.2** (deutan, orange vs aqua) against a target of 8.0,
worst normal-vision ΔE **24.0** against a floor of 15.0. `#1baf7a` contrasts
2.74:1 against the surface, below the 3:1 relief band — which is why **every
chart using slot 3 also carries a legend and a table twin in the report folder**,
never colour alone.

⚠️ **Never a rainbow for magnitude and never a hue at a diverging midpoint.** A
correlation heatmap on `viridis` (matplotlib's default for `imshow`) puts its
brightest colour at +1 and a mid-hue at 0, so "uncorrelated" reads as a signal.
`DIVERGING_BLUE_RED` is symmetric about a gray zero and every function that uses
it forces `vmin=-vmax`, because a diverging scale whose midpoint is not zero is
just a misleading sequential one.

## ⚠️ The mark rules, and why the first version broke them

The figures this replaces were legible but loud, and three faults did the damage:

1. **Bars filled their slot.** `height=0.7`-and-up leaves no air, so twenty-seven
   channels read as one solid block. Bars are now capped by `_BAR_FRACTION` and
   carry a **rounded data-end, square at the baseline** — the end that means
   something is the one that gets the radius.
2. **A ranking of mean RANKS was drawn as bars from zero.** Every value sat
   between 11 and 19 on a 0-20 axis, so the bars were all the same length and the
   chart said nothing. A mean rank has no meaningful zero, so it is now a **dot
   plot**, where a non-zero baseline is honest rather than a lie.
3. **Legends sat on top of the data**, and on a tall heatmap the subtitle
   collided with the title — because the subtitle was positioned in AXES
   fractions while the title pad was in POINTS, so the two drifted apart as the
   figure grew. Both are now in points, and legends are placed outside the axes.

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
from matplotlib.patches import PathPatch, Patch
from matplotlib.path import Path

# ------------------------------------------------------------------- the tokens

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
AXIS = "#c3c2b7"

# De-emphasis fill: present, but not the subject. Lighter than INK_MUTED so a
# gray bar never competes with a coloured one for attention.
MUTED_FILL = "#cfcdc5"

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

# The two poles as FLAT fills, for when bar LENGTH already carries the magnitude
# and colour only has to carry the sign. Using the ramp there would double-encode
# one number on two channels and burn the only free one.
POS_FILL, NEG_FILL = "#3987e5", "#e34948"

_FONT_STACK = ["Segoe UI", "DejaVu Sans", "sans-serif"]

# Share of a categorical slot a bar may occupy. The rest is air — the spec is
# "cap it, never fill the slot", and 0.58 is what keeps 27 rows readable.
_BAR_FRACTION = 0.58
# Corner radius of a data-end, as a share of bar thickness. ~4px at these sizes.
_BAR_ROUND = 0.45


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
    """Title in primary ink; the subtitle carries the caveat, in secondary ink.

    ⚠️ **Both are positioned in POINTS, not axes fractions.** The subtitle used to
    sit at `y=1.02` in axes coordinates while the title used a fixed point pad —
    so on a tall figure (a 27-row heatmap) 0.02 of the axes height is many more
    points than the title's pad and the two collided. Offsetting both from the
    axes' top edge in points makes the spacing identical at every figure size.
    """
    ax.set_title(title, loc="left", fontsize=12.5, fontweight="600",
                 pad=24 if subtitle else 10)
    if subtitle:
        ax.annotate(
            subtitle,
            xy=(0.0, 1.0), xycoords="axes fraction",
            xytext=(0, 7), textcoords="offset points",
            fontsize=9, color=INK_SECONDARY, va="bottom", ha="left",
        )


def _legend(ax, handles, where: str = "below", ncol: Optional[int] = None,
            pad: float = 0.09):
    """A legend OUTSIDE the axes, centred BELOW the plot.

    ⚠️ **Always below, never above.** The first version placed short wide charts'
    legends on the subtitle line at the top right, and on `plot_validation` — whose
    subtitle carries two mean ICs and a caveat — the two collided. The subtitle is
    the one piece of text on these charts that cannot be shortened without losing a
    caveat, so the legend is what moves. Below the x-axis is free space on every
    chart in this module.

    `where` is kept for callers that want extra clearance on a chart with tick
    labels and an axis label beneath it.
    """
    return ax.legend(
        handles=handles, loc="upper center",
        bbox_to_anchor=(0.5, -(pad if where == "below" else pad * 1.9)),
        ncol=ncol or len(handles), frameon=False,
        handlelength=1.1, handleheight=1.1, columnspacing=1.8, borderpad=0.0,
    )


def _colorbar(ax, mesh, label: str):
    """A thin, short colourbar pinned to the axes.

    ⚠️ `shrink` is what keeps it from becoming a second visual element on a tall
    heatmap — an unshrunk bar beside 27 rows is taller than most of the figures in
    this module and reads as a chart of its own.

    ⚠️ **It is ANCHORED TO THE TOP, not centred, and that is a layout fix.** A
    shrunk colourbar defaults to `anchor=(0.5, 0.5)`, which floats it at the
    vertical middle of a tall heatmap with a large gap above and below — it read as
    a detached third element rather than a legend for the cells. Pinning it to the
    top aligns its cap with the first row of the matrix and with the title block,
    so the eye meets the scale before the data instead of hunting for it.

    ⚠️ `shrink` is also FLOORED. On a 30-row heatmap the old expression returned
    ~0.15, a stub about six cells tall with its ticks crowded into a thumbnail.
    """
    height_in = max(1e-9, ax.bbox.height / 72)
    bar = ax.figure.colorbar(
        mesh, ax=ax, fraction=0.022, pad=0.015,
        shrink=float(np.clip(6.0 / height_in, 0.34, 1.0)),
        aspect=22, anchor=(0.0, 1.0), panchor=(0.0, 1.0),
    )
    bar.set_label(label, color=INK_SECONDARY, fontsize=9)
    bar.outline.set_visible(False)
    bar.ax.tick_params(colors=INK_MUTED, labelsize=8, length=0)
    return bar


# --------------------------------------------------------------- the bar marks


def _radius_in_data_units(ax, points: float):
    """A radius of `points` typographic points, expressed in x and y DATA units.

    ⚠️ **This is the whole reason bars are drawn as paths and not with `ax.bar`.**
    A corner radius is a property of the rendered picture, not of the data: on a
    `ρ` axis one data unit is ~1,000 px and on the category axis it is ~30 px, so a
    single radius expressed in data units produces a corner that is invisible along
    one axis and swallows the bar along the other. The first version of this module
    did exactly that and drew tombstone-shaped IC bars. Converting one PIXEL radius
    into each axis's own units keeps the corner circular on screen.

    ⚠️ Requires the axes limits to be final — the transform is what is being read.
    """
    pixels = points * ax.figure.dpi / 72.0
    inverse = ax.transData.inverted()
    (x0, y0), (x1, y1) = inverse.transform([(0.0, 0.0), (pixels, pixels)])
    return abs(x1 - x0), abs(y1 - y0)


def _rounded_bar_path(lo: float, hi: float, centre: float, thickness: float,
                      rx: float, ry: float, horizontal: bool) -> Path:
    """A bar whose DATA end is rounded and whose baseline end is square.

    `lo`/`hi` are the bar's extent along the value axis, `hi` being the data end.
    `rx`/`ry` are the corner radii in each axis's data units — equal on screen.
    Each is clipped so a near-zero bar degenerates to a square nub rather than
    inverting its own path.
    """
    a, b = centre - thickness / 2.0, centre + thickness / 2.0
    sign = 1.0 if hi >= lo else -1.0
    if horizontal:
        r_along = max(0.0, min(rx, abs(hi - lo) / 2.0))
        r_across = max(0.0, min(ry, thickness / 2.0))
        inner = hi - sign * r_along
        pts = [
            (lo, a), (inner, a),
            (hi, a), (hi, a + r_across),
            (hi, b - r_across),
            (hi, b), (inner, b),
            (lo, b),
        ]
    else:
        r_along = max(0.0, min(ry, abs(hi - lo) / 2.0))
        r_across = max(0.0, min(rx, thickness / 2.0))
        inner = hi - sign * r_along
        pts = [
            (a, lo), (a, inner),
            (a, hi), (a + r_across, hi),
            (b - r_across, hi),
            (b, hi), (b, inner),
            (b, lo),
        ]
    codes = [Path.MOVETO, Path.LINETO,
             Path.CURVE3, Path.CURVE3,
             Path.LINETO,
             Path.CURVE3, Path.CURVE3,
             Path.LINETO]
    return Path(pts + [pts[0]], codes + [Path.CLOSEPOLY])


def _bars(ax, positions, values, colours, horizontal=True, baseline=0.0,
          thickness=_BAR_FRACTION, zorder=2, radius_pt=3.0):
    """Draw rounded-end bars.

    ⚠️ **Call this AFTER setting the axes limits.** The corner radius is derived
    from the data transform (see `_radius_in_data_units`), and `PathPatch` does not
    grow the data limits the way `ax.bar` does — so limits first, marks second.
    """
    rx, ry = _radius_in_data_units(ax, radius_pt)
    for pos, value, colour in zip(positions, values, colours):
        if not np.isfinite(value):
            continue
        ax.add_patch(PathPatch(
            _rounded_bar_path(baseline, value, pos, thickness, rx, ry, horizontal),
            facecolor=colour, edgecolor="none", zorder=zorder,
        ))


def _span(values, baseline=0.0):
    finite = [v for v in np.asarray(values, dtype=float) if np.isfinite(v)]
    return (min(finite + [baseline]), max(finite + [baseline])) if finite else (0.0, 1.0)


# ------------------------------------------------------- channel-name shortening


def _plus(subtitle: str, note: str) -> str:
    """Append a `_shorten` note to a subtitle, or leave it alone when there is none."""
    return f"{subtitle} · {note}" if note else subtitle


def _shorten(labels, min_saved: int = 6):
    """`(short_labels, note)` — strip the `__`-token prefix/suffix EVERY label shares.

    ⚠️ **The shared part of a name carries no information ON THIS CHART.** A
    `pool__economy_vietnam` run labels 30 rows
    `vietnam__economy__labor__economics__vnwag`, of which `vietnam__economy__` is
    identical on every row — it spent 18 of 41 characters, and the axis it forced
    was wider than the plot. What distinguishes the rows is the tail. The stripped
    affix is returned so the caller can put it in the SUBTITLE once, which is where
    a constant belongs.

    ⚠️ **Tokens, never characters.** A character-wise common prefix would cut
    `vietnam__economy__gdp…` and `vietnam__economy__government…` at
    `vietnam__economy__g`, inventing names that match nothing in the CSV. Splitting
    on `__` first means every shortened label is still a suffix of the real channel
    name, so it can be pasted back into a lookup.

    ⚠️ **No-ops rather than mangles.** A `pool__basic` panel (`close_adjust`,
    `close_raw`, …) shares no leading token, and a set of one label shares
    everything with itself — both return the labels untouched. `min_saved` stops it
    firing for a saving too small to be worth a subtitle clause.
    """
    original = [str(x) for x in labels]
    if len(original) < 2:
        return original, ""
    split = [name.split("__") for name in original]
    # Never consume a whole label: a name reduced to "" is worse than a long one.
    limit = min(len(parts) for parts in split) - 1
    if limit < 1:
        return original, ""

    head = 0
    while head < limit and len({parts[head] for parts in split}) == 1:
        head += 1
    tail = 0
    while tail < limit - head and len({parts[-1 - tail] for parts in split}) == 1:
        tail += 1

    if not head and not tail:
        return original, ""
    # `head + tail <= limit` guarantees at least one token survives on the
    # SHORTEST label, so this slice is never empty.
    short = ["__".join(parts[head: len(parts) - tail]) for parts in split]
    saved = len(original[0]) - len(short[0])
    if saved < min_saved:
        return original, ""

    note = ""
    if head:
        note = "__".join(split[0][:head]) + "__*"
    if tail:
        suffix = "*__" + "__".join(split[0][len(split[0]) - tail:])
        note = f"{note} and {suffix}" if note else suffix
    return short, f"names trimmed of {note}"


def _pad_limits(lo: float, hi: float, pad: float = 0.06):
    span = (hi - lo) or 1.0
    return lo - span * pad, hi + span * pad


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
        _, ax = plt.subplots(figsize=(8, max(2.4, 0.34 * len(incomplete))))
    y = np.arange(len(incomplete))
    labels, note = _shorten(incomplete.index)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_ylim(-0.6, len(incomplete) - 0.4)
    ax.set_xlim(0, 1)
    _bars(ax, y, incomplete.values, [SERIES[0]] * len(incomplete))
    ax.xaxis.set_major_formatter(mpl.ticker.PercentFormatter(xmax=1))
    ax.grid(axis="y", visible=False)
    # Direct-label the worst one only; the axis carries the rest.
    worst = incomplete.index[0]
    ax.text(incomplete.iloc[0] + 0.012, 0, f"{incomplete.iloc[0]:.0%}",
            va="center", fontsize=9, color=INK_SECONDARY)
    _titles(
        ax,
        "Feature coverage",
        _plus(
            f"non-null share of labelled rows · {len(incomplete)} incomplete "
            f"column(s) · worst: {worst}",
            note,
        ),
    )
    return ax


def _value_fmt(values) -> str:
    """A `format()` spec for one label column, chosen from its MAGNITUDE.

    ⚠️ **Never a percent.** This axis carries the target in the target's OWN units,
    and the module cannot know what those are — `return_5day` is a ratio, but
    `close_adjust_5day` is a price in VND. The old code hard-coded
    `PercentFormatter`, which rendered VCB's mean forward close of 27,692 VND as
    **"+2769229.05%"** and the x-axis as `1000000%` … `7000000%`. A number that is
    wrong by a factor of 100 and labelled with the wrong unit is worse than an
    unformatted one, so the unit annotation is gone and only the DIGITS adapt.
    """
    scale = float(np.nanmax(np.abs(np.asarray(values, dtype=float)))) if len(values) else 0.0
    if scale >= 1000:
        return ",.0f"          # prices, volumes — separators, no false precision
    if scale >= 1:
        return ",.2f"
    return ".4f"               # returns and ranks — 0.0312, not 3.12%


def plot_target_distribution(y: pd.Series, target: str, ax=None):
    """The label's distribution — one series, with zero marked.

    Zero is the only reference that matters on a forward return: the mass either
    side of it is what a directional model is trying to separate.

    ⚠️ **Zero is marked only when it is INSIDE the data.** On a price-level target
    every value is ~10,000-70,000 and a zero line would sit far off the left edge,
    dragging the x-limits out and squashing the entire histogram into the right
    margin to reference a value the series never takes.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(8.5, 3.6))
    values = y.dropna()
    counts, edges = np.histogram(values, bins=60)
    centres = (edges[:-1] + edges[1:]) / 2
    width = (edges[1] - edges[0])
    ax.set_xlim(*_pad_limits(edges[0], edges[-1], 0.02))
    ax.set_ylim(0, counts.max() * 1.22)
    # A 2px surface GAP between neighbouring bars, not a stroke around each.
    _bars(ax, centres, counts, [SERIES[0]] * len(counts), horizontal=False,
          thickness=width * 0.82, radius_pt=1.5)
    fmt = _value_fmt(values)
    # ⚠️ The two references sit at almost the same x on a near-zero-mean return,
    # so they are staggered vertically and kept clear of the top spine.
    if edges[0] <= 0 <= edges[-1]:
        ax.axvline(0, color=AXIS, linewidth=1.2, zorder=1)
        ax.annotate("zero", xy=(0, counts.max() * 0.94), xytext=(5, 0),
                    textcoords="offset points", fontsize=9, color=INK_SECONDARY)
    mean = float(values.mean())
    ax.axvline(mean, color=SERIES[1], linewidth=1.6, zorder=3)
    ax.annotate(f"mean {format(mean, fmt)}", xy=(mean, counts.max() * 1.08),
                xytext=(5, 0), textcoords="offset points",
                fontsize=9, color=INK_SECONDARY)
    # ⚠️ The TICKS trim trailing zeros; the SUMMARY above does not. One spec has to
    # serve both, and they want opposite things: the summary reports `sd 0.0430`,
    # where the last digit is information, while the axis put `-0.1000` and
    # `0.0000` under a chart whose ticks land on tenths. Trimming is safe because
    # it only ever removes zeros — `,.0f` has no decimal point to touch.
    ax.xaxis.set_major_formatter(
        mpl.ticker.FuncFormatter(
            lambda v, _pos: (
                format(v, fmt).rstrip("0").rstrip(".") if "." in format(v, fmt)
                else format(v, fmt)
            )
        )
    )
    ax.grid(axis="x", visible=False)
    ax.set_xlabel(target)
    ax.set_ylabel("sessions")
    _titles(
        ax,
        f"Distribution of {target}",
        f"n = {len(values):,} labelled sessions · mean {format(mean, fmt)} · "
        f"sd {format(float(values.std()), fmt)} · min {format(float(values.min()), fmt)} "
        f"· max {format(float(values.max()), fmt)}",
    )
    return ax


def plot_target_correlation(target_corr: pd.Series, target: str, top: int = 25, ax=None):
    """Signed rank correlation of each feature with the target.

    Polarity, not magnitude — so blue for a positive association and red for a
    negative one, symmetric about zero.

    ⚠️ **Two FLAT fills, not the diverging ramp.** Bar length already encodes |ρ|;
    shading the bars by the same number would double-encode it and, worse, made
    the small values so pale they vanished against the surface. Colour here does
    one job — the sign.
    """
    ordered = target_corr.reindex(
        target_corr.abs().sort_values(ascending=False).index
    ).head(top).iloc[::-1]
    if ax is None:
        _, ax = plt.subplots(figsize=(8.5, max(2.6, 0.34 * len(ordered))))
    y = np.arange(len(ordered))
    colours = [POS_FILL if v >= 0 else NEG_FILL for v in ordered.values]
    lo, hi = _span(ordered.values)
    labels, note = _shorten(ordered.index)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_ylim(-0.7, len(ordered) - 0.3)
    limit = max(abs(lo), abs(hi))
    ax.set_xlim(-limit * 1.25, limit * 1.25)
    _bars(ax, y, ordered.values, colours)
    ax.axvline(0, color=AXIS, linewidth=1.0, zorder=1)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel(f"Spearman ρ vs {target}")

    # Direct-label the two POLES — the strongest positive and the strongest
    # negative. The axis carries the rest; a number on every bar goes unread.
    for position in {int(np.argmax(ordered.values)), int(np.argmin(ordered.values))}:
        value = ordered.values[position]
        offset = 7 if value >= 0 else -7
        ax.annotate(
            f"{value:+.3f}", xy=(value, position), xytext=(offset, 0),
            textcoords="offset points", fontsize=8.5, color=INK_SECONDARY,
            va="center", ha="left" if value >= 0 else "right",
        )
    _legend(
        ax,
        [Patch(facecolor=POS_FILL, label="positive"),
         Patch(facecolor=NEG_FILL, label="negative")],
        where="below",
    )
    _titles(
        ax,
        f"Association with {target}",
        # ⚠️ The caveat used to read "a forward return is mostly noise". It is
        # printed under whatever target the run chose, and on `close_adjust_5day`
        # — a forward PRICE — it named the wrong quantity while every bar sat at
        # ρ ≈ 0.95. The warning is worth keeping; asserting the target's type is
        # not, since this function is handed a name and never learns its units.
        _plus(
            f"top {len(ordered)} by |ρ| · read the SCALE before the order — a "
            f"column of near-equal bars is one effect, not a ranking",
            note,
        ),
    )
    return ax


def _heatmap(ax, data: pd.DataFrame, cmap, vmin: float, vmax: float,
             annotate: bool, fmt: str, cbar_label: str, mask=None):
    """Shared heatmap body: cells, hairline separators, optional value labels.

    Returns the `_shorten` note for the ROW labels, for the caller's subtitle.
    """
    values = np.array(data.values, dtype=float)
    if mask is not None:
        values = np.where(mask, np.nan, values)
    mesh = ax.imshow(values, cmap=cmap, vmin=vmin, vmax=vmax, aspect="auto")
    rows, note = _shorten(data.index)
    # ⚠️ The COLUMNS are shortened against their own set, not the rows'. On the
    # correlation heatmap both axes are the same channels and share one affix; on
    # the method heatmap the columns are `spearman`/`lasso`/… and share nothing,
    # and folding them into one call would find no common affix and shorten
    # neither axis.
    columns, _ = _shorten(data.columns)
    ax.set_xticks(range(data.shape[1]))
    ax.set_xticklabels(columns, rotation=45, ha="right")
    ax.set_yticks(range(data.shape[0]))
    ax.set_yticklabels(rows)
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
                value = values[i, j]
                if not np.isfinite(value):
                    continue
                # ⚠️ Ink flips to white at 0.45 of the ramp, not 0.55. The mid
                # steps of a blue ramp are dark enough that secondary ink on them
                # lands near 3:1 — legible-ish, and below what a value label
                # should be. The value is also in the CSV, which is the table
                # view, so nothing is gated on reading the cell.
                colour = "#ffffff" if abs(value) / span > 0.45 else INK_SECONDARY
                ax.text(j, i, format(value, fmt), ha="center", va="center",
                        fontsize=7.5, color=colour)

    _colorbar(ax, mesh, cbar_label)
    return note


def plot_method_heatmap(scores: pd.DataFrame, top: int = 25, ax=None):
    """Feature × ranking-method importance — magnitude, so a sequential ramp.

    The disagreement between columns is the point of the chart: a row that is
    dark all the way across is a feature every method likes, and a row dark in one
    column only is one method's inductive bias.
    """
    ordered = scores.head(top)
    if ax is None:
        _, ax = plt.subplots(
            figsize=(1.0 * ordered.shape[1] + 4.8, max(3, 0.34 * len(ordered)))
        )
    note = _heatmap(
        ax, ordered, SEQUENTIAL_BLUE, 0.0, 1.0,
        annotate=len(ordered) <= 30, fmt=".2f",
        cbar_label="normalised importance",
    )
    _titles(
        ax,
        "Importance by method",
        _plus(
            f"top {len(ordered)} features by ensemble rank · each column min-max "
            f"normalised to 0-1, so read ACROSS a row, not down a column",
            note,
        ),
    )
    return ax


def plot_correlation_heatmap(corr: pd.DataFrame, top: int = 25, ax=None):
    """Feature-feature Spearman — polarity on −1…+1, so diverging about gray.

    Ordered by ensemble rank rather than clustered, so the blocks that appear are
    the redundancy the prune is about to act on.

    ⚠️ **The upper triangle is masked.** A correlation matrix is symmetric, so
    drawing both halves doubles the ink to say the same thing twice and makes the
    diagonal hard to find. Half a matrix is not half the information.
    """
    subset = corr.iloc[:top, :top]
    if ax is None:
        size = max(4.8, 0.36 * len(subset))
        _, ax = plt.subplots(figsize=(size + 2.2, size))
    mask = np.triu(np.ones(subset.shape, dtype=bool), k=1)
    note = _heatmap(
        ax, subset, DIVERGING_BLUE_RED, -1.0, 1.0,
        annotate=len(subset) <= 18, fmt=".1f",
        cbar_label="Spearman ρ", mask=mask,
    )
    _titles(
        ax,
        "Feature redundancy",
        _plus(
            f"top {len(subset)} features, ordered by ensemble rank · lower triangle "
            f"only (the matrix is symmetric) · dark blocks are what the prune removes",
            note,
        ),
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
            figsize=(1.05 * subset.shape[1] + 4.8, max(3, 0.34 * len(subset)))
        )
    inverted = subset.max().max() + 1 - subset
    mesh = ax.imshow(
        inverted.values, cmap=SEQUENTIAL_BLUE, aspect="auto",
        vmin=1, vmax=inverted.values.max(),
    )
    rows, note = _shorten(subset.index)
    ax.set_xticks(range(subset.shape[1]))
    ax.set_xticklabels(subset.columns, rotation=0)
    ax.set_yticks(range(subset.shape[0]))
    ax.set_yticklabels(rows)
    ax.set_xticks(np.arange(-0.5, subset.shape[1], 1), minor=True)
    ax.set_yticks(np.arange(-0.5, subset.shape[0], 1), minor=True)
    ax.grid(which="minor", color=SURFACE, linewidth=2)
    ax.grid(which="major", visible=False)
    ax.tick_params(which="minor", length=0)
    span = inverted.values.max()
    for i in range(subset.shape[0]):
        for j in range(subset.shape[1]):
            rank = int(subset.values[i, j])
            colour = "#ffffff" if inverted.values[i, j] / span > 0.45 else INK_SECONDARY
            ax.text(j, i, rank, ha="center", va="center", fontsize=7.5, color=colour)
    bar = _colorbar(ax, mesh, "importance (dark = better rank)")
    bar.ax.set_yticks([])
    _titles(
        ax,
        "Rank stability across walk-forward folds",
        _plus(
            "SHAP rank within each expanding training window · a row that jumps is "
            "a regime, not a feature",
            note,
        ),
    )
    return ax


def plot_ensemble_ranking(result, top: int = 25, ax=None):
    """The headline ranking, with the pruned features shown rather than hidden.

    ⚠️ **A DOT PLOT, not bars, and the reason is the quantity.** The value is a
    mean RANK across six methods — an ordinal position with no meaningful zero.
    Drawn as bars from zero, every channel here sat between 11 and 19 on a 0-20
    axis and the bars were visually identical; the chart showed a ranking while
    hiding every difference in it. A dot plot may start its axis at the data, so
    the spread that decides the selection is the thing you see. The thin connector
    to the axis is a leader, not a magnitude.

    Two classes, so a legend: kept in the identity hue, pruned in muted fill. The
    pruned features stay on the chart because "why is `value_matched` missing" is
    the first question anyone asks of a selection.
    """
    ranks = result.ranks["ensemble"].head(top)
    ordered = ranks.iloc[::-1]           # best at the top of a bottom-up axis
    kept = set(result.kept)
    if ax is None:
        _, ax = plt.subplots(figsize=(8.5, max(2.6, 0.34 * len(ordered))))

    y = np.arange(len(ordered))
    colours = [SERIES[0] if f in kept else MUTED_FILL for f in ordered.index]
    best, worst = float(ordered.min()), float(ordered.max())
    span = (worst - best) or 1.0
    # ⚠️ The axis reads left-to-right ascending, as an axis must. Lower is better,
    # so the best channels sit LEFT and their leader lines are the shortest — the
    # chart reads "shorter is better" without inverting anything. An earlier
    # version flipped the axis to put the best on the right and produced a scale
    # that counted downwards, which reads as a rendering fault.
    left = best - span * 0.10
    labels, note = _shorten(ordered.index)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_ylim(-0.8, len(ordered) - 0.2)
    ax.set_xlim(left, worst + span * 0.16)

    ax.hlines(y, left, ordered.values, color=GRIDLINE, linewidth=1.2, zorder=1)
    ax.scatter(ordered.values, y, s=74, c=colours, zorder=3,
               edgecolors=SURFACE, linewidths=2.0)  # 2px surface ring
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("mean rank across the 6 methods (lower is better)")

    # Direct-label the best three only, BESIDE the dot — never a number on every
    # point, and never stacked above it, where two near-equal ranks collide.
    for position in range(len(ordered) - 1, max(-1, len(ordered) - 4), -1):
        ax.annotate(
            f"{ordered.values[position]:.1f}",
            xy=(ordered.values[position], position), xytext=(9, 0),
            textcoords="offset points", fontsize=8.5, color=INK_SECONDARY,
            va="center", ha="left",
        )
    _legend(
        ax,
        [Patch(facecolor=SERIES[0], label=f"kept ({len(result.kept)})"),
         Patch(facecolor=MUTED_FILL, label="pruned — redundant or below the cap")],
        where="below",
    )
    _titles(
        ax,
        f"Ensemble feature ranking for {result.target}",
        _plus(
            f"{len(result.features)} candidates · {len(result.dropped_correlated)} "
            f"dropped as redundant at |ρ| ≥ {result.corr_threshold} · "
            f"{len(result.dropped_constant)} constant columns never scored",
            note,
        ),
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
            figsize=(1.0 * profile.shape[1] + 4.8, max(3, 0.34 * len(profile)))
        )
    note = _heatmap(
        ax, profile, SEQUENTIAL_BLUE, 0.0, 1.0,
        annotate=len(profile) <= 25, fmt=".2f",
        cbar_label=f"normalised {method}",
    )
    _titles(
        ax,
        f"How each channel carries its signal (d = {result.lookback})",
        _plus(
            f"top {len(profile)} channels by ensemble rank · {method} per "
            f"(channel, window statistic), normalised across the whole design matrix",
            note,
        ),
    )
    return ax


def _grouped_ic_bars(ax, wide: pd.DataFrame, ylabel: str):
    """Shared body of the two per-fold IC charts — grouped, rounded, gapped."""
    x = np.arange(len(wide))
    columns = list(wide.columns)
    slot = 0.72 / max(1, len(columns))
    lo = min(0.0, float(np.nanmin(wide.values)))
    hi = max(0.0, float(np.nanmax(wide.values)))
    ax.set_ylim(*_pad_limits(lo, hi, 0.16))
    ax.set_xlim(-0.6, len(wide) - 0.4)
    ax.set_xticks(x)
    ax.set_xticklabels(wide.index)
    ax.set_ylabel(ylabel)
    ax.grid(axis="x", visible=False)
    for i, column in enumerate(columns):
        offset = (i - (len(columns) - 1) / 2) * slot
        _bars(ax, x + offset, wide[column].values, [SERIES[i % len(SERIES)]] * len(wide),
              horizontal=False, thickness=slot * 0.82)
    ax.axhline(0, color=AXIS, linewidth=1.0, zorder=1)
    _legend(ax, [Patch(facecolor=SERIES[i % len(SERIES)], label=c)
                 for i, c in enumerate(columns)], where="below", pad=0.16)


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
        _, ax = plt.subplots(figsize=(8.5, 3.8))
    _grouped_ic_bars(ax, wide, "Spearman IC (out of sample)")
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
        _, ax = plt.subplots(figsize=(8.5, 3.6))
    counts, edges = np.histogram(null.draws, bins=12)
    centres = (edges[:-1] + edges[1:]) / 2
    width = edges[1] - edges[0]
    span_lo = min(edges[0], null.observed, 0.0)
    span_hi = max(edges[-1], null.observed)
    ax.set_xlim(*_pad_limits(span_lo, span_hi, 0.10))
    ax.set_ylim(0, max(1, counts.max()) * 1.30)
    head = ax.get_ylim()[1]
    _bars(ax, centres, counts, [MUTED_FILL] * len(counts), horizontal=False,
          thickness=width * 0.82, radius_pt=2.0)

    ax.axvline(0, color=GRIDLINE, linewidth=1.0, zorder=1)
    ax.axvline(null.bar, color=INK_SECONDARY, linewidth=1.4, zorder=3)
    ax.annotate("null p95 — the bar", xy=(null.bar, head * 0.72), xytext=(5, 0),
                textcoords="offset points", fontsize=9, color=INK_SECONDARY)
    ax.axvline(null.observed, color=SERIES[1], linewidth=2.4, zorder=4)
    ax.annotate(f"observed {null.observed:+.4f}", xy=(null.observed, head * 0.93),
                xytext=(5, 0), textcoords="offset points", fontsize=9,
                color=INK_SECONDARY, fontweight="600")
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
    # `all channels` first so the reference series is the leftmost bar in a group.
    ordered = [c for c in ("all channels", "selected") if c in wide.columns]
    wide = wide[ordered + [c for c in wide.columns if c not in ordered]]
    if ax is None:
        _, ax = plt.subplots(figsize=(8.5, 3.8))
    _grouped_ic_bars(ax, wide, "Spearman IC (out of sample)")
    means = wide.mean()
    _titles(
        ax,
        "Does the selection generalise?",
        " · ".join(f"{k}: mean IC {v:+.3f}" for k, v in means.items())
        + " · purged expanding-window folds",
    )
    return ax
