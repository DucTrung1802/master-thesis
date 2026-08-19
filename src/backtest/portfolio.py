# src\backtest\portfolio.py
"""Costed, non-overlapping backtest primitives — the stage that turns a score into money.

⚠️ **THIS STAGE EXISTS BECAUSE `result_evaluator` CANNOT ANSWER "IS IT TRADABLE?"** It
reports `ic`, `dir_auc`, `hit_rate` and `long_short`, and on a `cs_rank_*` run the last
one is a spread of **RANKS, not returns** (CLAUDE.md §6-0 note 2). `predictions_*.csv`
carries `y_true` in rank units — the top-150 h=20 run's spans exactly [-0.5, +0.5] — so
**the realised return has to be joined back from `pool__targets`**; there is no way to
compute a PnL from a run folder alone.

⚠️ **NON-OVERLAPPING, ALWAYS.** At `h=20` every date carries a forward 20-day return, so
trading every date holds 20 overlapping tranches and multiplies the apparent sample by
20 without adding one independent observation — CLAUDE.md §5 rule 7 at the portfolio
level. Positions are taken every `h`-th session and held to the next one, which is also
the assumption the cost arithmetic below makes.

⚠️ **THE ERROR BAR IS THE POINT, NOT A FOOTNOTE.** A 2.6-year test window at `h=20` is
about **33 independent periods**. `SE(Sharpe) ≈ sqrt((1 + S²/2) / n)` is then ~0.18, so
two strategies inside ±0.35 of each other are the same strategy. Every table this module
prints carries `n_periods` and `se_sharpe` beside the Sharpe for that reason.

**Cost convention.** `cost = round_trip × ½ × Σ|Δw|`. One name entered and later exited
pays `½ + ½ = 1` round trip; a portfolio replacing a fraction `τ` of its book pays
`½ × 2τ = τ` round trips. That is the same accounting as the annual-drag table measured
on 2026-08-18: `τ = 0.70` at `h=5` is `50.4 × 0.70 × 0.005 = 17.6 %/yr`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

#: Round-trip transaction cost, as a fraction. VN brokerage ~0.15-0.35% a side plus the
#: 0.1% sell tax. ⚠️ The same constant `sentiment/weekly_xsec.py` uses, deliberately —
#: two costed backtests in one repo disagreeing about the cost is a defect, not a study.
#: ⚠️ `model/CONTEXT.md` §11 measured the cross-sectional strategy **dead at 40 bps**,
#: which is BELOW this. That is a finding, not a parameter to tune away.
ROUND_TRIP_COST = 0.005

#: The sweep every report runs, so the reader sees where a result dies rather than one
#: number at one assumption. 30 bps is `experiment_3`'s 15-a-side; 50 is the line above.
COST_SWEEP = (0.0, 0.0030, 0.0050, 0.0070)

#: Sessions per year, for annualising. VN trades ~250; 252 keeps it comparable with the
#: literature and the difference is far inside any error bar here.
SESSIONS_PER_YEAR = 252.0

#: Hysteresis defaults, from `model/CONTEXT.md` §11's measured turnover control (EWMA
#: span-10 + enter 0.90 / exit 0.75), which flipped net@20bps from Sharpe −1.5 to +0.46.
#: ⚠️ Stated there as percentiles of the SCORE, so they are percentiles here too.
ENTER_PERCENTILE = 0.90
EXIT_PERCENTILE = 0.75


#: Daily price bands, per exchange. A close AT the band has no counterparty on that side:
#: a name at its ceiling cannot be bought, one at its floor cannot be sold.
#: ⚠️ Measured to matter: `backtest/CONTEXT.md` §8f found the 5-day hand screen picking
#: ceiling names **2.14× more often than chance**, and excluding them took that book from
#: +19.3 % to +7.2 % CAGR. On the h=20 MODEL the bias is 1.33× and removing it slightly
#: IMPROVES the result (§8h) — but that is a measurement, not a licence to skip it.
BANDS = {"HOSE": 0.07, "HNX": 0.10, "UPCOM": 0.15}

#: How close to the band counts as "at" it. A close that prints at 0.93× the limit is
#: already illiquid on that side; the exact tick is not knowable from a daily bar.
CEILING_TOLERANCE = 0.93


def mark_ceiling(daily: pd.DataFrame, exchange: str = "exchange",
                 day_return: str = "day_ret") -> pd.Series:
    """Boolean: did this (date, ticker) close at its exchange's daily ceiling?

    ⚠️ **ONE implementation, on purpose.** This rule lived in `walkforward/evaluate.py`
    and in an ad-hoc probe, which is two copies of a number that decides whether a trade
    was executable — the same defect the shared `ROUND_TRIP_COST` constant exists to
    prevent. Both callers use this now.

    ⚠️ An exchange the `BANDS` table does not name yields **NaN → False**, i.e. "not
    known to be at a ceiling". That is the safe direction for an ENTRY screen (it keeps
    the row and lets the backtest trade it), and it is the WRONG direction for a floor /
    exit screen — which is why `PRF-4` lists the sell side as a separate, unbuilt item
    rather than something this function quietly half-covers.
    """
    limit = daily[exchange].map(BANDS) * CEILING_TOLERANCE
    return (daily[day_return] >= limit).fillna(False)


def drop_ceiling(panel: pd.DataFrame, column: str = "at_ceiling") -> tuple:
    """`(buyable panel, n_dropped)`. Missing column is an ERROR, never a silent pass.

    ⚠️ **A backtest that silently skips the exclusion reports a number the market would
    not have given you**, and it looks identical to one that applied it. So a panel that
    cannot answer the question refuses to be traded rather than defaulting to "nothing is
    at a ceiling" — `build_panel` attaches the column, and any other caller must too.
    """
    if column not in panel.columns:
        raise ValueError(
            f"the panel carries no {column!r} column, so the price-band exclusion cannot "
            f"be applied. Build it with `backtest.run.build_panel` (which joins "
            f"`exchange` from pool__basic), or call `mark_ceiling` yourself. Refusing to "
            f"trade a panel that cannot say what was buyable — see PRF-0."
        )
    flag = panel[column].fillna(False).astype(bool)
    return panel[~flag].copy(), int(flag.sum())


def rebalance_dates(dates: Sequence, horizon: int) -> List:
    """Every `horizon`-th distinct date, so the held windows do not overlap.

    ⚠️ Steps forward from the FIRST date rather than picking calendar months: the label
    is `h` SESSIONS forward, and a calendar step would leave gaps and overlaps against it.
    """
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1, got {horizon}")
    unique = sorted(pd.unique(pd.Series(list(dates))))
    return list(unique[::horizon])


def turnover_cost(previous: Dict, current: Dict, round_trip: float) -> float:
    """`round_trip × ½ × Σ|Δw|` over the union of both books."""
    names = set(previous) | set(current)
    delta = sum(abs(current.get(n, 0.0) - previous.get(n, 0.0)) for n in names)
    return round_trip * 0.5 * delta


@dataclass
class Track:
    """One strategy's realised path: what it held, what it paid, what it earned."""

    name: str
    dates: List = field(default_factory=list)
    gross: List[float] = field(default_factory=list)
    cost: List[float] = field(default_factory=list)
    exposure: List[float] = field(default_factory=list)
    n_held: List[int] = field(default_factory=list)

    def frame(self) -> pd.DataFrame:
        out = pd.DataFrame(
            {
                "date": self.dates,
                "gross": self.gross,
                "cost": self.cost,
                "exposure": self.exposure,
                "n_held": self.n_held,
            }
        )
        out["net"] = out["gross"] - out["cost"]
        return out


def long_flat_single(
    panel: pd.DataFrame,
    ticker: str,
    horizon: int,
    return_column: str,
    round_trip: float = ROUND_TRIP_COST,
    enter: float = ENTER_PERCENTILE,
    exit_: float = EXIT_PERCENTILE,
) -> Track:
    """Hold ONE name while its cross-sectional score percentile stays high, else cash.

    ⚠️ **THE PERCENTILE IS CROSS-SECTIONAL, WHICH IS WHY THE WHOLE PANEL IS NEEDED TO
    TRADE ONE STOCK.** The model predicts where a name sits among its peers on a date;
    that number does not exist until every peer is scored. Trading VCB alone still means
    running all 150 names through the model on every rebalance date.

    ⚠️ **Hysteresis, not one threshold.** Entering at `enter` and leaving only below
    `exit_` is what stops a score oscillating around a single line from paying a round
    trip each time. With `enter == exit_` this degrades to a plain threshold rule.
    """
    if not 0.0 <= exit_ <= enter <= 1.0:
        raise ValueError(f"need 0 <= exit_ <= enter <= 1, got exit_={exit_}, enter={enter}")

    track = Track(name=f"{ticker.upper()} long/flat")
    held: Dict[str, float] = {}
    for date in rebalance_dates(panel["date"], horizon):
        day = panel[panel["date"] == date]
        row = day[day["ticker"].str.upper() == ticker.upper()]
        if row.empty or day["y_pred"].notna().sum() < 2:
            continue
        realised = float(row[return_column].iloc[0])
        if realised != realised:  # the forward return runs off the end of the sample
            continue

        pct = float((day["y_pred"] < float(row["y_pred"].iloc[0])).mean())
        want = 1.0 if (pct >= enter or (held and pct >= exit_)) else 0.0
        book = {ticker.upper(): want} if want else {}

        track.dates.append(date)
        track.gross.append(realised * want)
        track.cost.append(turnover_cost(held, book, round_trip))
        track.exposure.append(want)
        track.n_held.append(int(want))
        held = book
    return track


def long_only_top_k(
    panel: pd.DataFrame,
    horizon: int,
    return_column: str,
    k: int,
    round_trip: float = ROUND_TRIP_COST,
) -> Track:
    """Equal-weight the `k` highest-scored names each rebalance. VN-executable: no short.

    ⚠️ **Long-only is not a simplification, it is the market.** `experiment_3` records
    that single-stock shorting is effectively unavailable on HOSE, so a long-short Sharpe
    is a number about a portfolio nobody here can hold.
    """
    if k < 1:
        raise ValueError(f"k must be >= 1, got {k}")

    track = Track(name=f"long-only top-{k}")
    held: Dict[str, float] = {}
    for date in rebalance_dates(panel["date"], horizon):
        day = panel[(panel["date"] == date) & panel["y_pred"].notna()]
        day = day[day[return_column].notna()]
        if len(day) < k:
            continue
        picks = day.nlargest(k, "y_pred")
        weight = 1.0 / len(picks)
        book = {str(t).upper(): weight for t in picks["ticker"]}

        track.dates.append(date)
        track.gross.append(float(picks[return_column].mean()))
        track.cost.append(turnover_cost(held, book, round_trip))
        track.exposure.append(1.0)
        track.n_held.append(len(picks))
        held = book
    return track


def buy_and_hold(
    panel: pd.DataFrame,
    horizon: int,
    return_column: str,
    ticker: Optional[str] = None,
) -> Track:
    """The benchmark that actually competes: hold it and pay nothing after entry.

    ⚠️ **`experiment_3` is why this is the benchmark and zero is not.** VCB timing
    measured Sharpe 0.67 against Buy&Hold's 0.66 — a "positive Sharpe" that bought
    nothing. `experiment_10` reviewed 23 papers and not one reported a naive baseline.
    """
    name = f"{ticker.upper()} buy&hold" if ticker else "equal-weight universe"
    track = Track(name=name)
    for date in rebalance_dates(panel["date"], horizon):
        day = panel[panel["date"] == date]
        if ticker:
            day = day[day["ticker"].str.upper() == ticker.upper()]
        day = day[day[return_column].notna()]
        if day.empty:
            continue
        track.dates.append(date)
        track.gross.append(float(day[return_column].mean()))
        track.cost.append(0.0)
        track.exposure.append(1.0)
        track.n_held.append(len(day))
    return track


def stats(track: Track, horizon: int, sessions: float = SESSIONS_PER_YEAR) -> Dict[str, float]:
    """CAGR / Sharpe / max drawdown, plus the error bar that decides whether to read them.

    ⚠️ **`se_sharpe` is `sqrt((1 + S²/2) / n)` and it is the column to read first.** With
    `n ≈ 33` — a 2.6-year test at `h=20` — it is ~0.18, so a Sharpe of 0.6 and one of 0.9
    are the same measurement. Reporting a Sharpe from 33 periods without it is the error
    `experiment_10` found in all 23 papers.
    """
    frame = track.frame()
    n = len(frame)
    blank = {
        "cagr": float("nan"), "sharpe": float("nan"), "se_sharpe": float("nan"),
        "max_dd": float("nan"), "hit": float("nan"), "cost_drag": float("nan"),
        "exposure": float("nan"),
    }
    if n == 0:
        return {"n_periods": 0, **blank}

    per_year = sessions / horizon
    net = frame["net"].to_numpy(float)
    equity = np.cumprod(1.0 + net)
    years = n / per_year

    sd = float(net.std(ddof=1)) if n > 1 else float("nan")
    sharpe = (
        float(net.mean() / sd * np.sqrt(per_year))
        if sd == sd and sd > 0
        else float("nan")
    )
    out = {"n_periods": n}
    out["cagr"] = (
        float(equity[-1] ** (1.0 / years) - 1.0)
        if years > 0 and equity[-1] > 0
        else float("nan")
    )
    out["sharpe"] = sharpe
    # ⚠️ Lo (2002): the SE of an annualised Sharpe over `n` periods. It does not depend
    # on the annualisation factor, which is why it compares straight across horizons.
    out["se_sharpe"] = (
        float(np.sqrt((1.0 + 0.5 * sharpe**2) / n)) if sharpe == sharpe else float("nan")
    )
    out["max_dd"] = float((equity / np.maximum.accumulate(equity) - 1.0).min())
    out["hit"] = float((net > 0).mean())
    out["cost_drag"] = float(frame["cost"].mean() * per_year)
    out["exposure"] = float(frame["exposure"].mean())
    return out


def shuffle_scores_within_date(panel: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Permute `y_pred` inside each date — the strategy's own null.

    ⚠️ **WITHIN each date, never across.** Shuffling globally would also destroy the
    time structure, so a strategy could "fail" its null for holding the market at the
    wrong moment rather than for picking the wrong names. Permuting inside a date keeps
    every date's score DISTRIBUTION and every realised return exactly where it was, and
    breaks only the pairing between a name's score and that name's outcome — which is
    the one thing the strategy claims to have.

    ⚠️ **What this null does NOT price in**, and it is the same list as `NUL-1`: the
    feature selection, the architecture search, the choice of `k`, and the choice of
    this test window. It is a floor, not a result.
    """
    out = panel.copy()
    out["y_pred"] = (
        out.groupby("date", sort=False)["y_pred"]
        .transform(lambda column: rng.permutation(column.to_numpy()))
    )
    return out


def null_bar(
    panel: pd.DataFrame,
    horizon: int,
    return_column: str,
    k: int,
    round_trip: float = ROUND_TRIP_COST,
    draws: int = 200,
    seed: int = 7,
) -> Dict[str, float]:
    """`long_only_top_k` against `draws` within-date shuffles. Returns the bar and the max.

    ⚠️ CLAUDE.md §5 rule 3: **quote the null MAX beside the p95 bar.** A strategy that
    clears its p95 while one shuffled draw beats it outright has not been shown much.
    """
    rng = np.random.default_rng(seed)
    sharpes, cagrs = [], []
    for _ in range(draws):
        drawn = shuffle_scores_within_date(panel, rng)
        result = stats(long_only_top_k(drawn, horizon, return_column, k, round_trip), horizon)
        sharpes.append(result["sharpe"])
        cagrs.append(result["cagr"])
    sharpes = np.array([s for s in sharpes if s == s], dtype=float)
    cagrs = np.array([c for c in cagrs if c == c], dtype=float)
    if not len(sharpes):
        return {"draws": 0}
    return {
        "draws": int(len(sharpes)),
        "sharpe_null_mean": float(sharpes.mean()),
        "sharpe_null_sd": float(sharpes.std(ddof=1)),
        "sharpe_bar_p95": float(np.percentile(sharpes, 95)),
        "sharpe_null_max": float(sharpes.max()),
        "cagr_null_mean": float(cagrs.mean()),
        "cagr_bar_p95": float(np.percentile(cagrs, 95)),
        "cagr_null_max": float(cagrs.max()),
    }
