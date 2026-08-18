# src\walkforward\evaluate.py
"""Score the concatenated OOS track — per fold, and as one walk-forward backtest.

    python -m walkforward.evaluate [--out DIR] [--top-k 20] [--draws 200]

⚠️ **THE PER-FOLD TABLE IS THE POINT, NOT THE AVERAGE.** PRF-1 exists because a single
number over 2017-2026 cannot tell *"the edge decayed"* from *"one split was lucky"* — and
`backtest/CONTEXT.md` §8g already shows the two halves disagreeing. An average over a
regime that worked and one that does not would hide exactly the thing being measured, so
the fold series is printed first and the pooled figure second.

⚠️ **The pooled track is a real walk-forward backtest**: every prediction in it was made
by a model that saw only earlier data, and no `(date, ticker)` appears twice (asserted in
`run.collect`). It is therefore the honest version of `backtest/CONTEXT.md` §4, whose
single test window happened to be a +20.2 %/yr bull market.

⚠️ **The price band is applied here** (`PRF-0`): a name at its exchange ceiling on the
entry date is dropped, because it has no sellers. Measured to bite the h=20 model only
mildly (1.33× over-selection, and removing it slightly IMPROVES the Sharpe), but it is
applied rather than assumed.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from backtest import portfolio as P

DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "..", "..", "results", "walkforward")

#: Daily price bands. A close at the band has no counterparty on that side.
BANDS = {"HOSE": 0.07, "HNX": 0.10, "UPCOM": 0.15}


def load_track(out_dir: str) -> pd.DataFrame:
    path = os.path.join(out_dir, "predictions_oos.csv")
    frame = pd.read_csv(path)
    frame["date"] = pd.to_datetime(frame["date"])
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    return frame


def attach_returns(track: pd.DataFrame, universe: str, horizon: int) -> pd.DataFrame:
    """Realised forward returns + the exchange + a ceiling flag, from the database.

    ⚠️ Same reason as `backtest.run`: `y_true` here is a RANK and has no PnL.
    """
    from feature_selection.unified_reader import UnifiedSchemaReader

    with UnifiedSchemaReader(universe) as reader:
        targets = reader.read(
            "pool__targets",
            columns=["date", "ticker", f"return_{horizon}day", f"return_rel_{horizon}day"],
        )
        basic = reader.read("pool__basic", columns=["date", "exchange", "ticker", "close_adjust"])
    for frame in (targets, basic):
        frame["date"] = pd.to_datetime(frame["date"])
        frame["ticker"] = frame["ticker"].astype(str).str.upper()

    basic = basic.sort_values(["ticker", "date"])
    basic["day_ret"] = basic.groupby("ticker")["close_adjust"].pct_change()
    basic["at_ceiling"] = basic["day_ret"] >= basic["exchange"].map(BANDS) * 0.93

    out = track.merge(targets, on=["date", "ticker"], how="left", validate="one_to_one")
    out = out.merge(basic[["date", "ticker", "exchange", "at_ceiling"]],
                    on=["date", "ticker"], how="left", validate="one_to_one")
    out["at_ceiling"] = out["at_ceiling"].fillna(False)
    return out


def daily_ic(frame: pd.DataFrame, column: str) -> Dict[str, float]:
    """Spearman per date, and the t-stat on `n_eff = n_dates / h`.

    ⚠️ `n_eff`, never `n_dates` — that was `ICT-1`, fixed 2026-08-18 and worth exactly
    `√h`. The horizon is read from the caller, not assumed.
    """
    ics = []
    for _, day in frame.groupby("date"):
        day = day.dropna(subset=["y_pred", column])
        if len(day) < 5:
            continue
        a = day["y_pred"].rank()
        b = day[column].rank()
        if a.std() > 0 and b.std() > 0:
            ics.append(float(np.corrcoef(a, b)[0, 1]))
    return {"ic": float(np.mean(ics)) if ics else float("nan"),
            "n_dates": len(ics),
            "days_positive": float(np.mean(np.array(ics) > 0)) if ics else float("nan"),
            "_ics": ics}


def score(frame: pd.DataFrame, horizon: int, column: str, top_k: int,
          bps: Sequence[int]) -> Dict[str, float]:
    ic = daily_ic(frame, column)
    n_eff = max(1.0, ic["n_dates"] / horizon)
    ics = np.array(ic["_ics"], dtype=float)
    sd = float(ics.std(ddof=1)) if len(ics) > 1 else float("nan")
    t = float(ic["ic"] / (sd / np.sqrt(n_eff))) if sd == sd and sd > 0 else float("nan")

    out = {"ic": ic["ic"], "ic_t": t, "n_dates": ic["n_dates"],
           "days_pos": ic["days_positive"]}
    market = P.stats(P.buy_and_hold(frame, horizon, column), horizon)
    out["mkt_sharpe"] = market["sharpe"]
    out["mkt_cagr"] = market["cagr"]
    for cost in bps:
        st = P.stats(P.long_only_top_k(frame, horizon, column, top_k, cost / 10_000), horizon)
        out[f"sharpe@{cost}"] = st["sharpe"]
        out[f"cagr@{cost}"] = st["cagr"]
    out["n_periods"] = st["n_periods"]
    out["se_sharpe"] = st["se_sharpe"]
    return out


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    out_dir = os.path.abspath(str(option("--out", DEFAULT_OUT)))
    top_k = int(option("--top-k", 20))
    draws = int(option("--draws", 200))
    horizon = int(option("--horizon", 20))
    universe = str(option("--universe", "all"))
    costs = [20, 30, 50]

    from utils import runtime

    with runtime.RunTimer(f"walkforward.evaluate  k={top_k}", show_gpu=False):
        track = load_track(out_dir)
        panel = attach_returns(track, universe, horizon)
        column = f"return_{horizon}day"
        buyable = panel[~panel["at_ceiling"]].copy()

        print(f"\nOOS track {len(panel):,} rows, {panel['date'].nunique()} dates, "
              f"{panel['date'].min().date()} -> {panel['date'].max().date()}")
        print(f"dropped {int(panel['at_ceiling'].sum()):,} ceiling rows "
              f"({panel['at_ceiling'].mean():.4f}) — PRF-0\n")

        print("=" * 104)
        print(f"PER FOLD — long-only top-{top_k}, held {horizon} sessions, buyable only")
        print("=" * 104)
        rows = []
        for tag, part in buyable.groupby("fold"):
            rows.append({"fold": tag, **score(part, horizon, column, top_k, costs)})
        per_fold = pd.DataFrame(rows).sort_values("fold")
        print(per_fold.to_string(index=False, float_format=lambda v: f"{v:,.4f}"))
        per_fold.to_csv(os.path.join(out_dir, "per_fold.csv"), index=False)

        # ⚠️ The trend across folds IS the answer to PRF-1. Quoted with its own slope
        # rather than eyeballed, since "it decays" was the recorded prediction.
        y = per_fold[f"sharpe@30"].to_numpy(float)
        ok = np.isfinite(y)
        if ok.sum() > 2:
            slope = float(np.polyfit(np.arange(ok.sum()), y[ok], 1)[0])
            first = float(np.nanmean(y[ok][: ok.sum() // 2]))
            second = float(np.nanmean(y[ok][ok.sum() // 2:]))
            print(f"\nSharpe@30bps across folds: slope {slope:+.4f}/fold  "
                  f"first half {first:+.3f}  second half {second:+.3f}")

        print()
        print("=" * 104)
        print(f"POOLED — the whole walk-forward as ONE track")
        print("=" * 104)
        pooled = score(buyable, horizon, column, top_k, costs)
        print(pd.DataFrame([pooled]).to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        if draws > 0:
            print(f"\nthe NULL — y_pred shuffled within date, {draws} draws, top-{top_k}")
            for cost in costs:
                obs = P.stats(P.long_only_top_k(buyable, horizon, column, top_k,
                                                cost / 10_000), horizon)
                bar = P.null_bar(buyable, horizon, column, top_k, cost / 10_000,
                                 draws=draws, seed=7)
                z = (obs["sharpe"] - bar["sharpe_null_mean"]) / bar["sharpe_null_sd"]
                print(f"  {cost:3d} bps  observed {obs['sharpe']:+.3f}  null mean "
                      f"{bar['sharpe_null_mean']:+.3f}  bar {bar['sharpe_bar_p95']:+.3f}  "
                      f"MAX {bar['sharpe_null_max']:+.3f}  z={z:+.2f}  "
                      f"{'CLEARS' if obs['sharpe'] > bar['sharpe_bar_p95'] else 'FAILS'}")
        print("\n⚠️ The 13 channels were selected on the WHOLE sample, so every fold's "
              "LEVEL is optimistic. The SHAPE across folds is the honest part.")
    return per_fold


if __name__ == "__main__":
    main()
