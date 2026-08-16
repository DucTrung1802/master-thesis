# src\feature_selection\cross_sectional.py
"""The cross-sectional study — `unified_schema_all`, N stocks × T days, one target.

This is CONTEXT.md §7 built. The single-ticker study ended at a verdict — nothing
cleared its own null, and the binding constraint was `n_eff`, not features — with
one way out: stop asking *will VCB go up* and start asking *which of these stocks
will beat the others*. That question needs a panel, a per-date target, a per-date
metric and a date-grouped CV, and it needs the SAME six rankers and the SAME null
protocol or the two studies cannot be compared.

So nothing here re-implements a ranker. `CrossSectionalSelector` overrides the six
hooks `FeatureSelector` factors out and inherits everything else.

| hook | single ticker | here |
|---|---|---|
| `_design` | one `sliding_window_view` down the frame | **one per ticker**, then reassembled in `(date, ticker)` order |
| `_splits` | expanding row blocks, purge `d+h-1` ROWS | expanding **date** blocks, purge `d+h-1` **DATES** |
| `_ic` | one pooled Spearman | **Spearman per date, then averaged** |
| `_purge_boundary` | drop the last `d+h-1` rows | drop the last `d+h-1` **dates** |
| `_effective_n` | `n / h` | **`n_dates / h`** — the width buys precision, not independence |
| `_ids_of` / `_dates_of` | `None` | the `ticker` and `date` columns |

## ⚠️ 1. THE THREE MISTAKES THAT MANUFACTURE A CROSS-SECTIONAL RESULT

**1. Splitting the CV by row.** A row split lands mid-day, putting some of a day's
stocks in train and the rest in test. They share the same market move, the same
news and — after a per-date target — the same normalisation constant, so the model
reads the answer off its training set. `PurgedWalkForwardByDate` splits on UNIQUE
DATES and purges in dates.

**2. Windowing straight down the stacked frame.** `pool__basic` sorted by
`(date, ticker)` interleaves companies, so a 20-row window ending at AAA's row 500
would be built out of twenty different companies. `_design` groups by ticker first.

**3. Reporting a POOLED IC.** Pooled over `N × T` rows, a rank correlation mostly
measures *which day was good*, not *which stock was good on that day* — and only
the second is tradeable, because the first needs you to time the market. The pooled
number is also enormously more significant-looking, because it counts `N × T`
observations when it has `T / h`.

## ⚠️ 2. What the cross-section actually buys — and what it does not

§6d of CONTEXT.md priced the single-ticker study at ~850 independent observations
and said 1,500 were needed. It is tempting to read "N stocks × T days at the same
`h` multiplies the independent count by N" as `850 × 100`. **It does not.**

The mean IC here is the average of `T` daily cross-sectional ICs, and consecutive
days overlap by `h-1` label days, so the number of INDEPENDENT observations behind
it is `T / h` — the same `T / h` as before. Widening the panel adds no dates.

What it does buy is **precision per observation**, and that is worth a great deal:

| | single ticker | cross-section, N=100 |
|---|---|---|
| one observation is | one day's ±1 sign | one day's IC over 100 stocks |
| sd of that observation | ~1.0 | **~0.1-0.2** |
| observations | `T/h` | `T/h` |
| SE of the mean | ~`1/√(T/h)` | **~`0.15/√(T/h)`** |

The daily IC is an average over `N` stocks, so its sampling noise falls like
`1/√N`. That is why a cross-sectional IC of **0.02** can be decisive where a
time-series IC of 0.05 was not: the bar moved, not the signal. `ic_summary` and the
null both read `n_eff = n_dates / h` so this is priced in rather than assumed.

## ⚠️ 3. The target is a PER-DATE RANK, and the features can be too

`cs_rank_{h}day` is `return_{h}day` ranked within its date and mapped to
`[-0.5, +0.5]`. Three things follow, and each of them fixes a specific finding of
the single-ticker study:

* **The market factor is gone by construction.** §6d's `return_rel_5day` subtracted
  VNINDEX and still carried the residual beta of each stock; a within-date rank
  removes anything common to the whole cross-section, whatever its beta.
* **Outliers cannot dominate.** `unified_schema_all.return_5day` reaches **−781**
  (VNX, whose silver `close_adjust` is negative for 968 sessions — see §5). A rank
  is invariant to it. A regression on the raw return is not, and one such row would
  set the loss.
* **It is stationary.** No era to identify, which is what `EXCLUDE_PRICE_LEVELS`
  and the `zscore` representation were both groping at in §6c.

`feature_normalize="cs_rank"` does the same to every FEATURE, per date, before the
windowing. That is the representation a cross-sectional model actually wants: it
removes the level that acted as a date proxy AND the size that acts as a permanent
stock label, leaving "where does this stock sit in today's cross-section".

⚠️ **The feature normalisation happens BEFORE the window, the target's after the
window is irrelevant** — both are computed from one date's rows and use nothing
from the future, so neither needs train/test fitting. This is the same argument
`windows.window_design` makes for its own normalisations.

## ⚠️ 4. The null shuffles DATE BLOCKS OF THE WHOLE LABEL MATRIX

`evaluation.block_shuffle` permutes blocks of one series. Here the label is a
`date × ticker` matrix, and the null has to break the feature↔label link while
keeping everything else. `shuffle_dates` pivots the target to that matrix and
permutes contiguous **blocks of its ROWS**, so:

* **each stock keeps its own labels** — AAA is given AAA's returns from a different
  fortnight, never BBB's, so cross-sectional dispersion and each name's own
  volatility survive;
* **a day's labels stay one real day's labels** — the market-wide component and the
  co-movement of the cross-section are intact;
* **the block keeps the label's autocorrelation**, at `d + h` dates, so the overlap
  the effective sample is discounted for is not quietly removed from the null;
* only the pairing of a day's FEATURES with a day's LABELS is destroyed.

⚠️ **`date_block` LOSES ROWS ON A RAGGED PANEL, and the loss favours the observed
result.** A cell `(t, ticker)` takes its label from `(donor(t), ticker)`, which is
empty if that name had not listed yet. Every draw therefore trains on less data
than the observed run did — a weaker model, a lower IC, a **lower bar**, which is a
bias toward a false positive. **Measured on VN100 2015-2026: a draw keeps 230,685
of 250,610 labelled rows, or 92.0 %.** Small, but it is a thumb on the scale in the
one direction that matters, so it is measured rather than argued about.

⚠️ `mode="within_date"` — permuting the label across tickers within each date — is
**lossless**: every labelled cell stays labelled, so its draws have exactly the
observed run's row count and the asymmetry above does not exist. It destroys less
(the label's time structure goes with it), but against a **per-date rank** target
the only link the pipeline can exploit IS the within-date one, so this is arguably
the primary null and `date_block` the conservative cross-check. **Report both.**
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from feature_selection import windows
from feature_selection.selector import FeatureSelector, PurgedWalkForward
from feature_selection.unified_reader import _NUMERIC_SQL_TYPES, UnifiedSchemaReader

# The schema `_ingest_unified_pool_basic(DataPreprocessor.UNIFIED_UNIVERSE)` writes.
UNIVERSE_SCHEMA_TICKER = "ALL"

# Columns of `pool__basic` that are identity or taxonomy, never features.
#
# ⚠️ The eight GICS columns were CONSTANT on a single-ticker panel and so were
# dropped automatically as "not a feature". On the universe panel they VARY — a
# tree can now split on `sector_code` — and they are still not features here: they
# are strings, they would need encoding, and an industry effect is a different
# study from a price/flow one. They are excluded by name rather than by accident.
IDENTITY_COLUMNS = (
    "exchange",
    "ticker",
    "sector",
    "sector_code",
    "industry_group",
    "industry_group_code",
    "industry",
    "industry_code",
    "sub_industry",
    "sub_industry_code",
)

TICKER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,20}$")


# ------------------------------------------------------------------ the panel


def cross_sectional_rank(
    values: pd.Series, dates: pd.Series, min_width: int = 2
) -> pd.Series:
    """Rank `values` within each date, mapped to `[-0.5, +0.5]`.

    The uniform-on-`[-0.5, 0.5]` scaling — `(rank - 1) / (n - 1) - 0.5` — is what
    makes days with different cross-section widths comparable: a panel of 30 names
    and a panel of 300 both produce a label with the same mean (0) and nearly the
    same spread, so no era of the sample gets a louder label than another.

    ⚠️ **Dates with fewer than `min_width` labelled names come back NaN**, not 0.
    A "rank" among two stocks is a coin flip, and a run of such days would be pure
    noise wearing the same units as the signal.

    ⚠️ Ties get the AVERAGE rank. VN stocks hit limit-up and limit-down together, so
    ties are common and frequent; `method="first"` would break them by row order,
    which is alphabetical by ticker.
    """
    series = pd.Series(values.to_numpy(float), index=values.index)
    grouped = series.groupby(dates.to_numpy(), sort=False)
    ranked = grouped.rank(method="average")
    width = grouped.transform("count")
    scaled = (ranked - 1.0) / (width - 1.0) - 0.5
    return scaled.where(width >= min_width)


def read_universe_panel(
    tickers: Optional[Sequence[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    horizons: Sequence[int] = (5, 10),
    min_width: int = 20,
    exclude_tickers: Sequence[str] = ("VNX",),
    schema_ticker: str = UNIVERSE_SCHEMA_TICKER,
) -> pd.DataFrame:
    """`unified_schema_all.pool__basic ⋈ pool__targets`, filtered, plus `cs_rank_*`.

    ⚠️ **Filtered in SQL, not in pandas.** The full pool is 2.39 M rows × 42
    columns; reading it to select 100 tickers would cost ~1.5 GB and eight minutes
    to throw most of it away.

    Args:
        tickers: the universe. `None` reads every ticker, which is 2.39 M rows —
            pass a universe unless you mean it.
        start, end: inclusive date bounds, `YYYY-MM-DD`.
        horizons: which `return_{h}day` get a `cs_rank_{h}day` twin.
        min_width: drop dates whose cross-section is narrower than this. A date
            with 3 names contributes an IC computed from 3 points, which is nearly
            uniform on [-1, 1] and would swamp the fold mean with variance.
        exclude_tickers: names removed before anything else. ⚠️ `VNX` is the
            default because its silver `close_adjust` is NEGATIVE for 968 sessions,
            which makes `return_5day` reach −781. That is a data defect, not a
            return, and it is the single worst outlier in the table.

    Returns:
        A frame sorted `(date, ticker)`, with a RangeIndex, carrying every
        `pool__basic` column plus `return_{h}day`, `return_rel_{h}day` and
        `cs_rank_{h}day`.
    """
    for ticker in tuple(tickers or ()) + tuple(exclude_tickers):
        if not TICKER_PATTERN.match(ticker):
            raise ValueError(f"ticker {ticker!r} is not an identifier-safe name.")

    reader = UnifiedSchemaReader(schema_ticker).connect()
    try:
        where, params = [], []
        if tickers:
            where.append(f"b.ticker IN ({', '.join(['%s'] * len(tickers))})")
            params.extend(list(tickers))
        if exclude_tickers:
            where.append(
                f"b.ticker NOT IN ({', '.join(['%s'] * len(exclude_tickers))})"
            )
            params.extend(list(exclude_tickers))
        if start:
            where.append("b.date >= %s")
            params.append(start)
        if end:
            where.append("b.date <= %s")
            params.append(end)

        target_cols = [f"return_{h}day" for h in horizons] + [
            f"return_rel_{h}day" for h in horizons
        ]
        # ⚠️ Joined on all three key columns, the same key `UnifiedSchemaReader.join`
        # would use. On the universe pool `date` alone is not a key at all, so this
        # is not the belt-and-braces it was on a single ticker — a `date`-only join
        # here would be a 780-fold cross product.
        sql = (
            f"SELECT b.*, {', '.join('t.' + c for c in target_cols)} "
            f"FROM {reader.schema}.pool__basic b "
            f"JOIN {reader.schema}.pool__targets t "
            f"  ON t.date = b.date AND t.exchange = b.exchange "
            f" AND t.ticker = b.ticker"
            + (f" WHERE {' AND '.join(where)}" if where else "")
            + " ORDER BY b.date, b.ticker"
        )
        with reader.driver._cursor_ctx() as cur:
            cur.execute(sql, params)
            names = [d[0] for d in cur.description]
            panel = pd.DataFrame(cur.fetchall(), columns=names)
        types = reader.column_types("pool__basic")
    finally:
        reader.close()

    if panel.empty:
        raise ValueError("the universe filter matched no rows.")

    panel["date"] = pd.to_datetime(panel["date"])
    # ⚠️ psycopg2 hands `numeric` back as `Decimal`, which pandas stores as dtype
    # `object` — the same trap `unified_reader.read` documents. Cast by the SQL
    # TYPE, never by inspecting the values: a column of Decimals looks like a
    # string column and every correlation downstream would silently coerce it.
    numeric = {
        c for c, t in types.items() if t in _NUMERIC_SQL_TYPES
    } | set(target_cols)
    for column in panel.columns:
        if column in numeric:
            panel[column] = pd.to_numeric(panel[column], errors="coerce")

    # ⚠️ Width is measured AFTER the ticker filter and BEFORE the rank, because the
    # rank's denominator is the width of the universe actually being modelled. A
    # date kept at width 300 in the raw pool but width 4 in a VN30 run must be
    # dropped by the VN30 run's own count.
    width = panel.groupby("date")["ticker"].transform("size")
    panel = panel[width >= min_width].reset_index(drop=True)
    if panel.empty:
        raise ValueError(
            f"no date has {min_width} or more names in this universe — lower "
            f"min_width or widen the universe."
        )

    # ⚠️ `min_width` is applied a SECOND time, to the LABELLED width. The filter
    # above counts rows; a date can hold 100 rows and 4 labels, because each
    # series' last `h` rows have no future — which is exactly what the ragged end
    # of the panel looks like when some tickers were scraped further than others.
    # Such a date would otherwise contribute a "cross-sectional rank" over 4 names.
    # NaN here means `_prepare` drops those rows as unlabelled, which is what they
    # are.
    for h in horizons:
        panel[f"cs_rank_{h}day"] = cross_sectional_rank(
            panel[f"return_{h}day"], panel["date"], min_width=min_width
        )
    return panel.sort_values(["date", "ticker"]).reset_index(drop=True)


def panel_summary(panel: pd.DataFrame, target: str) -> pd.Series:
    """The shape numbers every claim below rests on — width, dates, `n_eff`."""
    labelled = panel[panel[target].notna()]
    width = labelled.groupby("date").size()
    return pd.Series(
        {
            "rows": len(panel),
            "labelled_rows": len(labelled),
            "tickers": panel["ticker"].nunique(),
            "dates": int(labelled["date"].nunique()),
            "first_date": str(panel["date"].min().date()),
            "last_date": str(panel["date"].max().date()),
            "width_min": int(width.min()),
            "width_median": int(width.median()),
            "width_max": int(width.max()),
            "density": round(len(labelled) / (panel["ticker"].nunique() * len(width)), 3),
        },
        name=target,
    )


# ----------------------------------------------------------------- the CV split


class PurgedWalkForwardByDate(PurgedWalkForward):
    """Expanding walk-forward whose blocks are DATES, not rows.

    Fold boundaries land between sessions, so every stock of a given date is on the
    same side of every split. `gap` keeps its meaning — `lookback + horizon - 1` —
    but is now spent in sessions, which is what it always meant: `d + h - 1` rows of
    a single ticker's history was only ever a proxy for `d + h - 1` sessions.

    ⚠️ `min_train` is also in DATES here, not rows. On a 100-name panel a
    `min_train` of 500 rows would be five sessions.
    """

    def split_dates(
        self, dates: pd.Series
    ) -> List[Tuple[np.ndarray, np.ndarray]]:
        """`dates` is one date per ROW; the folds come back as row positions."""
        ordered = np.array(sorted(pd.unique(dates)))
        n_dates = len(ordered)
        gap = self.gap
        usable = n_dates - self.min_train - gap
        if usable < self.n_splits:
            raise ValueError(
                f"{n_dates} sessions cannot be split into {self.n_splits} purged "
                f"folds with min_train={self.min_train} sessions, "
                f"horizon={self.horizon} and lookback={self.lookback} "
                f"(purge gap {gap} sessions)."
            )
        block = usable // self.n_splits
        # date -> position, so a row's fold is one lookup rather than a comparison
        # against a date per fold.
        position = pd.Series(np.arange(n_dates), index=ordered)
        row_position = position.reindex(dates.to_numpy()).to_numpy()

        folds = []
        for i in range(self.n_splits):
            test_start = self.min_train + gap + i * block
            test_end = test_start + block if i < self.n_splits - 1 else n_dates
            train_end = test_start - gap
            train = np.flatnonzero(row_position < train_end)
            test = np.flatnonzero(
                (row_position >= test_start) & (row_position < test_end)
            )
            folds.append((train, test))
        return folds


# ------------------------------------------------------------- the design matrix


def panel_window_design(
    frame: pd.DataFrame,
    ids: pd.Series,
    lookback: int,
    stats_: Sequence[str],
    normalize: str,
    dtype: np.dtype = np.float64,
) -> pd.DataFrame:
    """`windows.window_design` PER SERIES, reassembled on the original index.

    ⚠️ This is the whole reason the cross-sectional path cannot reuse
    `window_design` directly. That function runs one `sliding_window_view` down the
    frame it is given; over a `(date, ticker)`-sorted panel every window it produced
    would be a mixture of ~20 different companies.

    ⚠️ **A series shorter than `lookback` contributes nothing and is skipped, not
    padded.** Padding would invent a history the company does not have, and every
    statistic over it would be a claim about data that never existed.
    """
    blocks = []
    for _, positions in frame.groupby(ids.to_numpy(), sort=False).groups.items():
        block = frame.loc[positions]
        if len(block) < lookback:
            continue
        blocks.append(
            windows.window_design(block, lookback, stats_, normalize, dtype=dtype)
        )
    if not blocks:
        raise ValueError(
            f"no series has {lookback} rows — every window would be empty."
        )
    # ⚠️ Sorted back into the panel's own order, which is `(date, ticker)`. The
    # walk-forward split and every `.iloc` downstream address rows by position, and
    # a design matrix in ticker-major order would put fold 1 entirely inside AAA.
    return pd.concat(blocks).sort_index()


def cross_sectional_normalize(
    frame: pd.DataFrame, dates: pd.Series
) -> pd.DataFrame:
    """Replace every column by its within-date percentile rank, in `[-0.5, +0.5]`.

    The representation a cross-sectional model wants. It removes, in one step, two
    things the single-ticker study spent §6c fighting:

    * the **level acting as a date** — every date's ranks span the same range, so
      nothing in the matrix tells the tree what year it is;
    * the **size acting as a stock label** — a permanently-large company is no
      longer permanently at the top of the `volume_matched` column unless it is
      large *relative to that day's cross-section*.

    ⚠️ A column that is NaN for a row stays NaN — it is not ranked as "lowest". The
    flow columns are absent for years on most tickers (`prop_buy_vol` covers 3 % of
    the universe), and imputing them as an extreme rank would be a strong, false
    statement about those stocks.
    """
    grouped = frame.groupby(dates.to_numpy(), sort=False)
    ranked = grouped.rank(method="average")
    width = grouped.transform("count")
    scaled = (ranked - 1.0).div((width - 1.0).replace(0.0, np.nan)) - 0.5
    # width 1 with a value present ⇒ (1-1)/(1-1). The honest answer for "the only
    # name that reported today" is 0, the middle of its own cross-section — not an
    # infinity, and not a NaN, which would say the value was missing when it was
    # not.
    return scaled.mask(ranked.notna() & (width == 1), 0.0)[frame.columns]


# ------------------------------------------------------------------- the metric


def daily_ic(
    prediction: np.ndarray, y: pd.Series, dates: pd.Series, min_width: int = 5
) -> pd.Series:
    """One Spearman IC per date — the cross-sectional information coefficient.

    ⚠️ **Dates narrower than `min_width` are dropped, not averaged in.** An IC over
    3 names takes values in a handful of discrete points spanning [-1, 1]; a few
    hundred such days would dominate the mean's variance while carrying almost no
    information. `read_universe_panel(min_width=…)` already removes them from the
    panel; this is the second line of defence, for the last fold of a universe that
    thins out.

    ⚠️ **Vectorised as the Pearson correlation of within-date RANKS, which is what
    Spearman is** — including the average-rank tie correction, so it agrees with
    `scipy.stats.spearmanr` exactly (asserted in `test_cross_sectional.py`). This is
    not a micro-optimisation: `permutation_importance` calls the scorer
    `n_repeats × n_columns` times — 1,620 times on a 162-column design — and a
    `groupby().apply()` with a Python-level `spearmanr` per date turns a two-minute
    run into an overnight one.
    """
    frame = pd.DataFrame(
        {
            "pred": np.asarray(prediction, float),
            "y": y.to_numpy(float),
            "date": dates.to_numpy(),
        }
    )
    frame = frame.dropna(subset=["pred", "y"])
    grouped = frame.groupby("date", sort=True)
    rank_pred = grouped["pred"].rank(method="average")
    rank_y = grouped["y"].rank(method="average")

    parts = pd.DataFrame(
        {
            "date": frame["date"].to_numpy(),
            "rp": rank_pred.to_numpy(),
            "ry": rank_y.to_numpy(),
            "rp2": rank_pred.to_numpy() ** 2,
            "ry2": rank_y.to_numpy() ** 2,
            "rpry": rank_pred.to_numpy() * rank_y.to_numpy(),
        }
    )
    agg = parts.groupby("date", sort=True).agg(
        n=("rp", "size"), rp=("rp", "sum"), ry=("ry", "sum"),
        rp2=("rp2", "sum"), ry2=("ry2", "sum"), rpry=("rpry", "sum"),
    )
    n = agg["n"]
    cov = agg["rpry"] - agg["rp"] * agg["ry"] / n
    var_p = agg["rp2"] - agg["rp"] ** 2 / n
    var_y = agg["ry2"] - agg["ry"] ** 2 / n
    denominator = np.sqrt(var_p * var_y)
    # A date on which every prediction ties (a model that split on nothing) has zero
    # rank variance: the IC is undefined, not 0. NaN says so and `.mean()` skips it.
    ic = cov.div(denominator.where(denominator > 0))
    return ic.where(n >= min_width)


@dataclass
class DailyICSummary:
    """A fold's daily ICs, and the t-statistic that prices their overlap."""

    ic: pd.Series
    horizon: int

    @property
    def mean(self) -> float:
        return float(self.ic.mean())

    @property
    def n_eff(self) -> float:
        """`n_dates / h` — consecutive days' labels share `h-1` of their `h` days."""
        return max(1.0, self.ic.notna().sum() / max(1, self.horizon))

    @property
    def t_stat(self) -> float:
        """`mean / (sd / √n_eff)`. ⚠️ The denominator is the whole argument of §2."""
        sd = float(self.ic.std(ddof=1))
        return self.mean / (sd / np.sqrt(self.n_eff)) if sd else np.nan

    @property
    def hit_rate(self) -> float:
        """Share of DAYS with a positive IC — the cross-sectional hit rate."""
        return float((self.ic > 0).mean())

    def summary(self) -> pd.Series:
        return pd.Series(
            {
                "days": int(self.ic.notna().sum()),
                "ic_mean": round(self.mean, 4),
                "ic_sd_daily": round(float(self.ic.std(ddof=1)), 4),
                "n_eff_days": round(self.n_eff, 1),
                "t_stat": round(self.t_stat, 2),
                "days_positive": round(self.hit_rate, 3),
            }
        )


# ------------------------------------------------------------------ the selector


class CrossSectionalSelector(FeatureSelector):
    """`FeatureSelector` over an `N × T` panel. Six hooks differ; nothing else does.

    Args:
        panel: `read_universe_panel(...)` output — sorted `(date, ticker)`.
        target: normally `cs_rank_5day`. ⚠️ `return_5day` also works and is the
            wrong question: see §3.
        panel_col: the column identifying a series. `ticker` — and `exchange` is
            not needed with it, because no VN ticker is listed twice (checked).
        feature_normalize: `"none"` or `"cs_rank"` (§3). This is applied to the
            channel panel BEFORE `windows`' own `normalize`, which still applies
            within each window afterwards.
        min_ic_width: dates narrower than this contribute no IC (§`daily_ic`).

    ⚠️ `min_train` and `n_splits` count SESSIONS, not rows — see
    `PurgedWalkForwardByDate`.
    """

    def __init__(
        self,
        panel: pd.DataFrame,
        target: str,
        panel_col: str = "ticker",
        feature_normalize: str = "cs_rank",
        min_ic_width: int = 5,
        **kwargs,
    ):
        if feature_normalize not in ("none", "cs_rank"):
            raise ValueError(
                f"feature_normalize must be 'none' or 'cs_rank', got "
                f"{feature_normalize!r}"
            )
        if panel_col not in panel.columns:
            raise ValueError(f"panel_col {panel_col!r} is not a column of the panel.")
        # ⚠️ Sorted `(date, ticker)` HERE, once. `_prepare` sorts by `date` alone,
        # which on a panel leaves the order within a session up to the sort's tie
        # handling — and XGBoost's row subsampling draws from that order, so two
        # runs over the same data could disagree about the kept set. Sorting on
        # both keys first makes the later sort a no-op.
        date_col = kwargs.get("date_col", "date")
        panel = panel.sort_values([date_col, panel_col]).reset_index(drop=True)
        kwargs.setdefault("exclude", ())
        # ⚠️ Every target column is excluded from the candidates, not just the one
        # in use. `cs_rank_5day` predicted from `return_5day` is the answer, and on
        # a universe panel those columns are no longer constant enough to be caught
        # by the constant-column drop.
        kwargs["exclude"] = tuple(kwargs["exclude"]) + IDENTITY_COLUMNS + tuple(
            c
            for c in panel.columns
            if c.startswith(("return_", "cs_rank_")) and c != target
        )
        super().__init__(panel=panel, target=target, **kwargs)
        self.panel_col = panel_col
        self.feature_normalize = feature_normalize
        self.min_ic_width = min_ic_width
        # ⚠️ The CV is REPLACED, not configured. `PurgedWalkForward` splits rows;
        # on this panel that would cut a session in half.
        self.cv = PurgedWalkForwardByDate(
            n_splits=self.cv.n_splits,
            horizon=self.cv.horizon,
            min_train=self.cv.min_train,
            lookback=self.cv.lookback,
        )
        self._dev_dates: Optional[pd.Series] = None
        self._dev_ids: Optional[pd.Series] = None

    # ------------------------------------------------------------------ hooks

    def _on_development(self, frame: pd.DataFrame) -> None:
        self._dev_dates = frame[self.date_col].reset_index(drop=True)
        self._dev_ids = frame[self.panel_col].reset_index(drop=True)

    def _design(
        self, X: pd.DataFrame, source: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        # ⚠️ `source` is the frame X's rows came from. `None` means the development
        # frame; `score_holdout` passes the holdout, whose row positions are its own
        # and share nothing with development's.
        if source is None:
            dates, ids = self._dev_dates, self._dev_ids
        else:
            dates = source[self.date_col].reset_index(drop=True)
            ids = source[self.panel_col].reset_index(drop=True)
        block = X
        if self.feature_normalize == "cs_rank":
            block = cross_sectional_normalize(X, dates.loc[X.index])
        return panel_window_design(
            block,
            ids.loc[X.index],
            self.lookback,
            self.window_stats,
            self.normalize,
            dtype=self.design_dtype,
        )

    def _splits(self, index: pd.Index) -> List[Tuple[np.ndarray, np.ndarray]]:
        # ⚠️ The dates are taken at the DESIGN's index, not the development frame's.
        # Windowing drops the first `lookback-1` rows of EVERY TICKER, so the two
        # differ by `(lookback-1) × N` rows and folds cut at the wrong sessions.
        return self.cv.split_dates(self._dev_dates.loc[index].reset_index(drop=True))

    def _ic(
        self, prediction: np.ndarray, y: pd.Series, dates: Optional[pd.Series] = None
    ) -> float:
        dates = self._dev_dates.loc[y.index] if dates is None else dates
        ic = daily_ic(prediction, y, dates, min_width=self.min_ic_width)
        return 0.0 if ic.isna().all() else float(ic.mean())

    def _purge_boundary(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Drop the last `d + h - 1` SESSIONS, which is many more rows."""
        gap = self.cv.gap
        if not gap:
            return frame
        sessions = np.sort(frame[self.date_col].unique())
        if len(sessions) <= gap:
            return frame
        cutoff = sessions[-gap]
        return frame[frame[self.date_col] < cutoff].reset_index(drop=True)

    def _effective_n(
        self, y: pd.Series, dates: Optional[pd.Series] = None
    ) -> float:
        """`n_dates / h`. ⚠️ NOT `n_rows / h` — see §2 of the module docstring.

        A 100-name fold of 300 sessions has 30,000 rows and **60** independent
        observations at `h=5`, not 6,000. Reporting the row-based number would
        shrink every error bar in the study by a factor of 10.
        """
        dates = self._dev_dates.loc[y.index] if dates is None else dates
        return max(1.0, dates.nunique() / max(1, self.horizon))

    # ------------------------------------------------------------- diagnostics

    def daily_ic_by_fold(
        self, result, feature_set: str = "selected"
    ) -> pd.DataFrame:
        """Re-fit each fold and return its DAILY ICs — the honest error bar.

        `result.validation` reports one mean per fold, which hides that the mean is
        an average of `n_dates` numbers with a spread of ~0.15. This returns the
        distribution those means came from.
        """
        X, y, _, _ = self._prepare()
        channels = result.features
        design = self._design(X[channels])
        y_windowed = y.loc[design.index]
        fold_dates = self._dev_dates.loc[design.index].reset_index(drop=True)
        columns_of = self._columns_of(channels, design.columns)
        chosen = list(result.kept) if feature_set == "selected" else list(channels)
        cols = [c for ch in chosen for c in columns_of[ch]]

        rows = []
        for i, (train_idx, test_idx) in enumerate(self._splits(design.index), start=1):
            X_tr, X_te = self._impute(
                design.iloc[train_idx][cols], design.iloc[test_idx][cols]
            )
            model = self._xgb().fit(X_tr, y_windowed.iloc[train_idx])
            ic = daily_ic(
                model.predict(X_te),
                y_windowed.iloc[test_idx],
                fold_dates.iloc[test_idx],
                min_width=self.min_ic_width,
            )
            summary = DailyICSummary(ic, self.horizon).summary()
            summary["fold"] = f"fold {i}"
            rows.append(summary)
        return pd.DataFrame(rows).set_index("fold")


# --------------------------------------------------------------------- the null


def shuffle_dates(
    panel: pd.DataFrame,
    target: str,
    block: int,
    rng: np.random.Generator,
    date_col: str = "date",
    panel_col: str = "ticker",
    mode: str = "date_block",
) -> pd.DataFrame:
    """A copy of `panel` whose target has been decoupled from its features.

    `mode="date_block"` — the one to quote — pivots the target to a
    `date × ticker` matrix and permutes contiguous blocks of `block` ROWS.
    **Each stock keeps its own labels**, moved to a different fortnight: AAA gets
    AAA's returns from elsewhere in the sample, never BBB's. So the cross-sectional
    dispersion of a day, each name's own volatility, and the label's
    autocorrelation inside a block all survive; the feature↔label pairing does not.

    `mode="within_date"` permutes the label across tickers within each date. It is
    the WEAKER null — it destroys the label's time structure too — and is reported
    for contrast, not for the verdict.

    ⚠️ **Missing cells stay missing.** A stock that had not listed on the donor date
    contributes NaN, which `_prepare` drops as an unlabelled row. The null panel is
    therefore slightly smaller than the real one on a ragged universe; that makes
    the null harder to clear, not easier, and it is why `panel_summary`'s `density`
    is worth reading before quoting a bar.
    """
    if mode not in ("date_block", "within_date"):
        raise ValueError(f"mode must be 'date_block' or 'within_date', got {mode!r}")
    out = panel.copy()
    if mode == "within_date":
        out[target] = out.groupby(date_col)[target].transform(
            lambda s: rng.permutation(s.to_numpy())
        )
        return out

    wide = panel.pivot(index=date_col, columns=panel_col, values=target)
    n_dates = len(wide)
    n_blocks = int(np.ceil(n_dates / block))
    order = np.concatenate(
        [np.arange(n_dates), np.full(n_blocks * block - n_dates, -1)]
    )
    order = order.reshape(n_blocks, block)[rng.permutation(n_blocks)].ravel()
    order = order[order >= 0]

    donor = wide.to_numpy()[order]  # row i of the shuffled matrix ← date `order[i]`
    shuffled = pd.DataFrame(donor, index=wide.index, columns=wide.columns)
    stacked = shuffled.stack(future_stack=True).rename(target)
    stacked.index.names = [date_col, panel_col]

    keys = pd.MultiIndex.from_arrays(
        [panel[date_col].to_numpy(), panel[panel_col].to_numpy()]
    )
    out[target] = stacked.reindex(keys).to_numpy()
    return out


def cross_sectional_null(
    panel: pd.DataFrame,
    target: str,
    factory,
    observed: float,
    lookback: int,
    horizon: int,
    n_draws: int = 20,
    seed: int = 0,
    feature_set: str = "selected",
    label: str = "",
    mode: str = "date_block",
    panel_col: str = "ticker",
    progress: bool = True,
):
    """`evaluation.null_distribution`, with the panel-aware shuffle of §4.

    ⚠️ The SELECTION is inside every draw, because selection is the step that
    inflates. `factory` takes a panel and must return a `SelectionResult`.
    """
    from feature_selection.evaluation import NullResult

    rng = np.random.default_rng(seed)
    block = lookback + horizon
    draws: List[float] = []
    failures = 0
    for i in range(n_draws):
        shuffled = shuffle_dates(panel, target, block, rng, panel_col=panel_col, mode=mode)
        try:
            result = factory(shuffled)
            rows = result.validation
            draws.append(float(rows[rows["feature_set"] == feature_set]["ic"].mean()))
        except Exception as error:  # noqa: BLE001 - counted, never swallowed
            failures += 1
            if progress:
                print(f"  draw {i + 1}/{n_draws}: FAILED — {error}")
            continue
        if progress:
            print(f"  draw {i + 1}/{n_draws}: null mean IC {draws[-1]:+.4f}")
    return NullResult(
        draws=np.array(draws),
        observed=observed,
        block=block,
        failures=failures,
        label=label or f"{target} / {mode}",
    )
