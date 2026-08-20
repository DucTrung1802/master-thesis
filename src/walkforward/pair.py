# src\walkforward\pair.py
"""P2-4 — compare two walk-forward tracks at DIFFERENT horizons, paired on the CALENDAR.

    python -m walkforward.pair --top-k 20 --universe all --draws 2000 \
        h10=../results/walkforward_h10:10 \
        h20=../results/walkforward:20

⚠️ **`walkforward.compare` CANNOT DO THIS, AND THE REASON IS STRUCTURAL.** It pairs ARMS
inside one sweep — arms that trade the SAME rebalance dates out of the same panel, so the
difference series `net_A - net_B` is defined period by period. Two HORIZONS produce 236 and
118 periods over different holding intervals: there is no period-wise correspondence, so the
subtraction `compare` performs does not exist here. Measured 2026-08-20 with h=10 at
Sharpe@30 +2.531 and h=20 at +1.991 — a +0.54 gap that no tool in this repo could test.

⚠️ **WHAT THEY DO SHARE IS THE CALENDAR**, and that is the whole idea. Both strategies hold
a book on every session of the same ~2,383 dates, so both have a DAILY net-return series and
those two series pair date by date. The market factor is common to them exactly as it is
common to two arms, and differencing removes it.

⚠️ **THE DAILY SERIES IS VERIFIED AGAINST THE PERIOD ONE, NOT ASSUMED EQUAL TO IT.** A
rebalance holds an equal-weight book for `h` sessions and lets the weights DRIFT, so the
daily series must compound back to `long_only_top_k`'s own period returns. The `verify:` line
prints the worst per-period disagreement; a construction that does not reproduce the tool it
is being compared against is measuring a third strategy. CLAUDE.md §5c is the cautionary case
for believing two numbers are comparable because they carry the same name.

⚠️ **THE CEILING SCREEN APPLIES TO ENTRY ONLY** (`PRF-0`). A name at its ceiling on the
rebalance date has no sellers and cannot be bought; a name that hits its ceiling on day 4 of
a holding period is simply a good day. So picks come from the buyable panel and the daily
returns come from the FULL one — dropping ceiling rows from the return path would silently
delete the best days of the very names the model chose.

⚠️ **TWO TESTS, DELIBERATELY, BECAUSE THEY FAIL DIFFERENTLY.** Newey-West prices the overlap
analytically and leans on the CLT at this sample size; the block bootstrap assumes only
stationarity within a block but needs the block long enough to carry the autocorrelation.
They are reported side by side and are expected to agree — when they do not, the
disagreement is the finding.

⚠️ **WHAT THIS STILL CANNOT ANSWER.** It prices the DIFFERENCE between two realised tracks.
It prices neither the feature selection behind either one (`NUL-1`), nor the choice to
compare these two horizons rather than others, nor survivorship — which protects both `z`
scores and neither CAGR (CLAUDE.md §2c).
"""

from __future__ import annotations

import math
import os
import sys
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from backtest import portfolio as P
from walkforward.evaluate import load_track

COSTS = (20, 30, 50)


def _parse_tracks(argv: Sequence[str]) -> List[Tuple[str, str, int]]:
    """`label=dir:horizon`, exactly two, and the horizons must differ.

    ⚠️ Equal horizons are REFUSED and pointed at `walkforward.compare`: that tool pairs
    period by period, which is strictly more powerful wherever it applies.
    """
    out: List[Tuple[str, str, int]] = []
    for token in argv:
        if token.startswith("-") or "=" not in token:
            continue
        label, _, rest = token.partition("=")
        directory, _, horizon = rest.rpartition(":")
        if not directory or not horizon.isdigit():
            raise ValueError(
                "expected `label=<dir>:<horizon>`, got {!r} — the horizon is not optional, "
                "because a track carries no record of the one it was built at.".format(token)
            )
        out.append((label.strip(), os.path.abspath(directory), int(horizon)))
    if len(out) != 2:
        raise ValueError("pass exactly two tracks, got {}".format(len(out)))
    if out[0][2] == out[1][2]:
        raise ValueError(
            "both tracks are h={}. Same-horizon tracks pair PERIOD BY PERIOD, which is "
            "strictly stronger — use `python -m walkforward.compare`.".format(out[0][2])
        )
    return out


def attach_daily(
    track: pd.DataFrame, universe: str, horizon: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """The scored panel, and SEPARATELY the full daily-return matrix to hold it against.

    ⚠️ **THE RETURN MATRIX IS NOT BUILT FROM THE TRACK, AND THE FIRST VERSION OF THIS
    FUNCTION WAS — measured 2026-08-20, and the reconciliation caught it.** A track holds
    one row per SCORED `(date, ticker)`, and every one of the 150 names is missing some
    (median 2,339 rows against a 2,373-session calendar, minimum 258): **2.21 % of the
    cells.** Pivoting `day_ret` out of the track therefore books a 0 % return on any day
    the model happened not to score a name **we are still holding**, which is not a
    modelling choice, it is a hole. It showed up as `max|diff| 2.48e-02` against
    `return_{h}day` on a corr of 0.9979 — small enough to look like rounding and large
    enough to move a Sharpe.

    So the returns come from `pool__basic` over the FULL calendar, and only the scores and
    the ceiling flag come from the track.
    """
    from feature_selection.unified_reader import UnifiedSchemaReader

    with UnifiedSchemaReader(universe) as reader:
        targets = reader.read(
            "pool__targets", columns=["date", "ticker", "return_{}day".format(horizon)]
        )
        basic = reader.read(
            "pool__basic", columns=["date", "exchange", "ticker", "close_adjust"]
        )
    for frame in (targets, basic):
        frame["date"] = pd.to_datetime(frame["date"])
        frame["ticker"] = frame["ticker"].astype(str).str.upper()

    basic = basic.sort_values(["ticker", "date"])
    basic["day_ret"] = basic.groupby("ticker")["close_adjust"].pct_change()
    basic["at_ceiling"] = P.mark_ceiling(basic)

    out = track.merge(targets, on=["date", "ticker"], how="left", validate="one_to_one")
    out = out.merge(
        basic[["date", "ticker", "exchange", "at_ceiling"]],
        on=["date", "ticker"], how="left", validate="one_to_one",
    )
    out["at_ceiling"] = out["at_ceiling"].fillna(False)

    # ⚠️ Restricted to the track's names and dates, but NOT to its (date, ticker) pairs.
    names = set(out["ticker"].unique())
    lo, hi = out["date"].min(), out["date"].max()
    held = basic[
        basic["ticker"].isin(names) & basic["date"].between(lo, hi)
    ]
    returns = held.pivot(index="date", columns="ticker", values="day_ret")
    return out, returns


def daily_net_series(
    panel: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
    top_k: int,
    round_trip: float,
) -> Tuple[pd.Series, pd.DataFrame]:
    """One net return per SESSION, plus the per-period reconciliation against the period one.

    Equal weight at entry, weights DRIFT for `h` sessions, turnover charged on the first
    session of the period — the daily statement of exactly what `long_only_top_k` does.

    `returns` is the FULL daily-return matrix from `attach_daily`, not a pivot of `panel`;
    see that docstring for the measurement that forced the distinction.
    """
    dates = sorted(pd.unique(panel["date"]))
    wide = returns.reindex(index=pd.DatetimeIndex(dates))
    scores = panel.pivot(index="date", columns="ticker", values="y_pred")
    ceiling = panel.pivot(index="date", columns="ticker", values="at_ceiling").fillna(False)
    target_column = "return_{}day".format(horizon)

    daily = pd.Series(0.0, index=pd.DatetimeIndex(dates), name="net")
    covered = pd.Series(False, index=daily.index)
    rows: List[Dict] = []
    held: Dict[str, float] = {}

    for i in range(0, len(dates) - 1, horizon):
        entry = dates[i]
        window = dates[i + 1: i + 1 + horizon]
        if not window:
            break
        # ⚠️ PICK from the buyable names; take RETURNS from the full panel.
        row = scores.loc[entry]
        eligible = row[row.notna() & ~ceiling.loc[entry].astype(bool)]
        if len(eligible) < top_k:
            continue
        picks = list(eligible.nlargest(top_k).index)

        book = {str(t).upper(): 1.0 / top_k for t in picks}
        cost = P.turnover_cost(held, book, round_trip)
        held = book

        weights = np.full(len(picks), 1.0 / len(picks))
        compounded = 1.0
        for j, session in enumerate(window):
            step_returns = np.nan_to_num(
                wide.loc[session, picks].to_numpy(float), nan=0.0
            )
            step = float((weights * step_returns).sum() / weights.sum())
            daily.loc[session] = step - (cost if j == 0 else 0.0)
            covered.loc[session] = True
            weights = weights * (1.0 + step_returns)
            compounded *= 1.0 + step

        # ⚠️ RECONCILIATION, per pick, split by whether that name traded every session.
        # `return_{h}day` steps h ROWS of the ticker's own series (verified 2026-08-20 to
        # 8.9e-16 over 2.37 M rows); the book is held for h SESSIONS of the calendar. For a
        # name with a gap those are different windows, so only the complete names can be
        # compared like for like — and on those the agreement must be exact.
        block = wide.loc[window, picks]
        per_name = (1.0 + block.fillna(0.0)).prod() - 1.0
        complete = block.notna().all()
        realised = panel.loc[
            (panel["date"] == entry) & panel["ticker"].isin(picks)
        ].set_index("ticker")[target_column].reindex(per_name.index)
        gap = (per_name - realised).abs()
        rows.append({
            "entry": entry,
            "compounded_daily": compounded - 1.0,
            "period_gross": float(realised.mean()) if realised.notna().any() else float("nan"),
            "cost": cost,
            "n_picks": len(picks),
            "n_complete": int(complete.sum()),
            "max_diff_complete": float(gap[complete].max()) if complete.any() else 0.0,
            "max_diff_all": float(gap.max()) if len(gap) else 0.0,
        })

    recon = pd.DataFrame(rows)
    if not recon.empty:
        recon["abs_diff"] = (recon["compounded_daily"] - recon["period_gross"]).abs()
    return daily[covered.to_numpy()], recon


def newey_west_se(x: np.ndarray, lag: int) -> float:
    """Bartlett-kernel HAC standard error of the MEAN. `lag` must cover the overlap."""
    x = np.asarray(x, float)
    n = len(x)
    if n < 2:
        return float("nan")
    e = x - x.mean()
    total = float(e @ e) / n
    for step in range(1, min(lag, n - 1) + 1):
        gamma = float(e[step:] @ e[:-step]) / n
        total += 2.0 * (1.0 - step / (lag + 1.0)) * gamma
    return math.sqrt(max(total, 0.0) / n)


def sharpe(x: np.ndarray, sessions: float = P.SESSIONS_PER_YEAR) -> float:
    sd = float(np.std(x, ddof=1))
    return float(np.mean(x) / sd * math.sqrt(sessions)) if sd > 0 else float("nan")


def block_bootstrap_diff(
    a: np.ndarray, b: np.ndarray, block: int, draws: int, seed: int = 7
) -> Dict[str, float]:
    """Circular block bootstrap of `sharpe(a) - sharpe(b)`, resampling the SAME blocks.

    ⚠️ Both series are indexed by ONE block draw, which is what keeps the pairing.
    Resampling them independently would destroy the very correlation this exists to price.
    """
    rng = np.random.default_rng(seed)
    n = len(a)
    n_blocks = int(math.ceil(n / block))
    offsets = np.arange(block)
    d_sharpe = np.empty(draws)
    d_mean = np.empty(draws)
    for draw in range(draws):
        starts = rng.integers(0, n, size=n_blocks)
        idx = ((starts[:, None] + offsets[None, :]).ravel() % n)[:n]
        d_sharpe[draw] = sharpe(a[idx]) - sharpe(b[idx])
        d_mean[draw] = float(np.mean(a[idx] - b[idx]))

    def summarise(sample: np.ndarray, observed: float) -> Dict[str, float]:
        return {
            "observed": observed,
            "sd": float(sample.std(ddof=1)),
            "ci_lo": float(np.percentile(sample, 2.5)),
            "ci_hi": float(np.percentile(sample, 97.5)),
            "p": float(2.0 * min((sample <= 0).mean(), (sample >= 0).mean())),
        }

    # ⚠️ BOTH ESTIMANDS, because they are not the same question and the first version of
    # this module compared a Newey-West test of the MEAN against a bootstrap of the
    # SHARPE and read the disagreement as a method disagreement. It was not: a mean is a
    # linear functional and a Sharpe is a ratio whose denominator also moves.
    return {
        "sharpe": summarise(d_sharpe, sharpe(a) - sharpe(b)),
        "mean": summarise(d_mean, float(np.mean(a - b))),
    }


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    top_k = int(option("--top-k", 20))
    universe = str(option("--universe", "all"))
    draws = int(option("--draws", 2000))
    tracks = _parse_tracks(argv)

    from utils import runtime

    banner = "walkforward.pair  {}  k={}".format(
        " vs ".join("{}(h={})".format(n, h) for n, _, h in tracks), top_k
    )
    with runtime.RunTimer(banner, show_gpu=False):
        series: Dict[str, Dict[int, pd.Series]] = {}
        labels = [t[0] for t in tracks]
        horizons = {t[0]: t[2] for t in tracks}

        for label, directory, horizon in tracks:
            panel, returns = attach_daily(load_track(directory), universe, horizon)
            print("\n{}  h={}  {}".format(label, horizon, directory))
            print("  panel {:,} rows, {} dates, {} tickers   "
                  "return matrix {} x {}, {:.2%} NaN".format(
                      len(panel), panel["date"].nunique(), panel["ticker"].nunique(),
                      returns.shape[0], returns.shape[1],
                      float(returns.isna().to_numpy().mean())))
            series[label] = {}
            for bps in COSTS:
                daily, recon = daily_net_series(
                    panel, returns, horizon, top_k, bps / 10_000)
                series[label][bps] = daily
                if bps == COSTS[0]:
                    exact = float(recon["max_diff_complete"].max())
                    loose = float(recon["max_diff_all"].max())
                    picks_n = int(recon["n_picks"].sum())
                    gapped = picks_n - int(recon["n_complete"].sum())
                    print("  verify: {} periods x {} picks, daily-compounded vs "
                          "return_{}day".format(len(recon), top_k, horizon))
                    print("     names trading EVERY session : max|diff| {:.2e}  "
                          "<- the like-for-like check".format(exact))
                    print("     names with a gap            : {} of {} ({:.1%}), "
                          "max|diff| {:.2e}".format(
                              gapped, picks_n, gapped / max(picks_n, 1), loose))
                    if exact > 1e-9:
                        print("  ⚠️  the daily construction does NOT reproduce the period "
                              "return on names that traded every session — that is a BUG "
                              "in this module, and every number below is void")
                    elif gapped:
                        print("     (the gap is DEFINITIONAL, not an error: `return_{}day` "
                              "steps {} ROWS of the\n      ticker while the book is held {} "
                              "SESSIONS. Priced at -0.015 Sharpe at h=20 and\n      -0.038 at "
                              "h=10 — see BKT-1.)".format(horizon, horizon, horizon))

        a, b = labels
        common = series[a][COSTS[0]].index.intersection(series[b][COSTS[0]].index)
        print("\ncommon calendar: {:,} sessions  {} -> {}".format(
            len(common), common.min().date(), common.max().date()))
        print("  {} covers {:,}, {} covers {:,}".format(
            a, len(series[a][COSTS[0]]), b, len(series[b][COSTS[0]])))

        lag = max(horizons.values())
        print("\n" + "=" * 104)
        print("PAIRED ON THE CALENDAR — daily net returns, Newey-West lag {} (= max "
              "horizon), block bootstrap {} draws".format(lag, draws))
        print("=" * 104)
        print("⚠️  TWO ESTIMANDS. `mean` is the average daily return gap (annualised for "
              "reading);\n    `Sharpe` is the risk-adjusted gap. They can disagree, and if "
              "they do that IS the answer.\n")
        print("{:>4} {:>7} | {:>10} {:>8} {:>9} {:>19} {:>8} | {:>9} {:>19} {:>8}".format(
            "bps", "corr", "d mean/yr", "NW t", "NW p", "boot 95% CI", "boot p",
            "d Sharpe", "boot 95% CI", "boot p"))
        for bps in COSTS:
            x = series[a][bps].reindex(common).to_numpy(float)
            y = series[b][bps].reindex(common).to_numpy(float)
            d = x - y
            se = newey_west_se(d, lag)
            t = float(d.mean() / se) if se > 0 else float("nan")
            p = math.erfc(abs(t) / math.sqrt(2.0))  # two-sided normal; n is thousands
            boot = block_bootstrap_diff(x, y, block=2 * lag, draws=draws)
            year = P.SESSIONS_PER_YEAR
            print("{:>4} {:>7.3f} | {:>+10.4f} {:>+8.2f} {:>9.4f} "
                  "{:>19} {:>8.4f} | {:>+9.4f} {:>19} {:>8.4f}".format(
                      bps, float(np.corrcoef(x, y)[0, 1]),
                      boot["mean"]["observed"] * year, t, p,
                      "[{:+.3f}, {:+.3f}]".format(boot["mean"]["ci_lo"] * year,
                                                  boot["mean"]["ci_hi"] * year),
                      boot["mean"]["p"],
                      boot["sharpe"]["observed"],
                      "[{:+.3f}, {:+.3f}]".format(boot["sharpe"]["ci_lo"],
                                                  boot["sharpe"]["ci_hi"]),
                      boot["sharpe"]["p"]))

        print("\n⚠️  `d Sharpe` differences ANNUALISED Sharpes computed from the DAILY "
              "series, so it is\n    not identical to the period-based Sharpe either track "
              "reports — the same strategy,\n    sampled differently. The paired tests are "
              "the answer; the levels are context.")
    return series


if __name__ == "__main__":
    main()
