# src\walkforward\compare.py
"""PRF-8 — score several walk-forward tracks IDENTICALLY and compare them paired.

    python -m walkforward.compare --top-k 20 --horizon 20 --universe all \
        lstm=../results/walkforward \
        lstm_small=../results/walkforward/prf8/lstm_small \
        gbt=../results/walkforward/prf8/gbt

⚠️ **THE COMPARISON IS PAIRED, AND IT HAS TO BE.** Every arm trades the SAME rebalance
dates out of the same panel, so its per-period returns are strongly correlated with every
other arm's — the market factor is common to all of them. An unpaired comparison of two
Sharpes each carrying `se_sharpe` ~ 0.16 would call a real 0.3 difference noise, and would
call a spurious one signal. The difference series `net_A - net_B` removes the common
factor, and its t-stat is what this module reports. CLAUDE.md §5c is the cautionary case:
eleven architectures spanning a 0.227 IC range, all inside ONE error bar.

⚠️ **THE ARMS MUST SHARE A `(date, ticker)` INDEX OR THE COMPARISON IS NOT ONE.** Asserted,
not assumed: two tracks built from different folds, a different universe or a different
ceiling screen would still produce two Sharpes that LOOK comparable. If the index differs
this raises instead of quietly scoring two different experiments against each other.

⚠️ **The price band is applied here too** (`PRF-0`), by the same rule as
`walkforward.evaluate`: a name at its exchange ceiling on the entry date has no sellers.
Applied to every arm identically, so it cannot favour one.

⚠️ **What this CANNOT answer.** A tie says the two models extract the same thing from these
13 channels; it does not say either extracts everything there is. And `NUL-1` still holds —
no null here prices in the feature selection that chose the channels both arms read.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from backtest import portfolio as P
from walkforward.evaluate import attach_returns, load_track, score

COSTS = (20, 30, 50)


def _parse_arms(argv: Sequence[str]) -> List[Tuple[str, str]]:
    """`label=DIR` positionals, in the order given. The first is the REFERENCE arm."""
    arms = []
    for token in argv:
        if token.startswith("-") or "=" not in token:
            continue
        label, _, directory = token.partition("=")
        arms.append((label.strip(), os.path.abspath(directory.strip())))
    if len(arms) < 2:
        raise ValueError(
            "give at least two arms as `label=DIR` — e.g. "
            "`lstm=../results/walkforward "
            "lstm_small=../results/walkforward/prf8/lstm_small`"
        )
    return arms


def load_arms(arms: Sequence[Tuple[str, str]]) -> Dict[str, pd.DataFrame]:
    """Load each arm's OOS track and assert they cover the same `(date, ticker)` rows."""
    tracks: Dict[str, pd.DataFrame] = {}
    index = None
    for label, directory in arms:
        track = load_track(directory)
        ordered = track[["date", "ticker"]].sort_values(["date", "ticker"])
        key = pd.MultiIndex.from_frame(ordered)
        if index is None:
            index = key
        elif not key.equals(index):
            raise AssertionError(
                f"arm {label!r} covers {len(key):,} (date, ticker) rows against the "
                f"reference arm's {len(index):,}, or the same count over a different "
                f"set. These are two different experiments and their Sharpes are not "
                f"comparable — check the folds, the universe and the source table."
            )
        tracks[label] = track
    return tracks


def net_series(panel: pd.DataFrame, horizon: int, column: str, top_k: int,
               bps: int) -> pd.Series:
    """The arm's per-rebalance NET return, indexed by date — the paired unit."""
    track = P.long_only_top_k(panel, horizon, column, top_k, bps / 10_000)
    return track.frame().set_index("date")["net"]


def paired(a: pd.Series, b: pd.Series, horizon: int,
           sessions: float = P.SESSIONS_PER_YEAR) -> Dict[str, float]:
    """Paired difference `a - b` over the common rebalance dates.

    Returns the mean difference annualised, its t-stat, and the correlation between the
    two arms' period returns — which is the number that says how much the pairing bought.
    """
    joined = pd.concat({"a": a, "b": b}, axis=1).dropna()
    if len(joined) < 3:
        return {"n": len(joined), "corr": float("nan"),
                "mean_diff": float("nan"), "t": float("nan")}
    diff = joined["a"].to_numpy(float) - joined["b"].to_numpy(float)
    n = len(diff)
    sd = float(diff.std(ddof=1))
    per_year = sessions / horizon
    return {
        "n": n,
        "corr": float(np.corrcoef(joined["a"], joined["b"])[0, 1]),
        # annualised, so it reads beside CAGR rather than as a 20-session number
        "mean_diff": float(diff.mean() * per_year),
        "t": float(diff.mean() / (sd / np.sqrt(n))) if sd > 0 else float("nan"),
    }


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    top_k = int(option("--top-k", 20))
    horizon = int(option("--horizon", 20))
    universe = str(option("--universe", "all"))
    draws = int(option("--draws", 0))
    spec = _parse_arms(argv)

    from utils import runtime

    with runtime.RunTimer(f"walkforward.compare  k={top_k}  arms={len(spec)}",
                          show_gpu=False):
        tracks = load_arms(spec)
        column = f"return_{horizon}day"
        reference = spec[0][0]

        panels: Dict[str, pd.DataFrame] = {}
        nets: Dict[str, Dict[int, pd.Series]] = {}
        for label, _ in spec:
            panel = attach_returns(tracks[label], universe, horizon)
            buyable, _ = P.drop_ceiling(panel)
            panels[label] = buyable
            nets[label] = {c: net_series(buyable, horizon, column, top_k, c)
                           for c in COSTS}

        first = panels[reference]
        print(f"\n{len(first):,} rows, {first['date'].nunique()} dates, "
              f"{first['date'].min().date()} -> {first['date'].max().date()}, "
              f"ceiling rows dropped (PRF-0)")

        print("\n" + "=" * 108)
        print(f"POOLED — long-only top-{top_k}, held {horizon} sessions, buyable only")
        print("=" * 108)
        rows = []
        for label, _ in spec:
            rows.append({"arm": label,
                         **score(panels[label], horizon, column, top_k, COSTS)})
        pooled = pd.DataFrame(rows)
        print(pooled.to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        print("\n" + "=" * 108)
        print(f"PAIRED vs {reference} — the difference removes the common market factor")
        print("=" * 108)
        pair_rows = []
        for label, _ in spec[1:]:
            for cost in COSTS:
                stat = paired(nets[label][cost], nets[reference][cost], horizon)
                a = float(pooled.loc[pooled["arm"] == label, f"sharpe@{cost}"].iloc[0])
                b = float(pooled.loc[pooled["arm"] == reference,
                                     f"sharpe@{cost}"].iloc[0])
                pair_rows.append({
                    "arm": label, "bps": cost, "sharpe": a,
                    f"sharpe_{reference}": b, "d_sharpe": a - b,
                    "corr": stat["corr"], "d_cagr_ann": stat["mean_diff"],
                    "t_paired": stat["t"], "n": stat["n"],
                })
        pairs = pd.DataFrame(pair_rows)
        print(pairs.to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        print("\n" + "=" * 108)
        print("PER FOLD — sharpe@30bps, one column per arm")
        print("=" * 108)
        per_fold = {}
        for label, _ in spec:
            per_fold[label] = {
                tag: score(part, horizon, column, top_k, (30,))["sharpe@30"]
                for tag, part in panels[label].groupby("fold")
            }
        folds = pd.DataFrame(per_fold).sort_index()
        folds.loc["MEAN"] = folds.mean()
        print(folds.to_string(float_format=lambda v: f"{v:,.3f}"))

        print("\n" + "=" * 108)
        print("IC per fold — the model's ranking, before any portfolio rule")
        print("=" * 108)
        ic = {}
        for label, _ in spec:
            ic[label] = {
                tag: score(part, horizon, column, top_k, ())["ic"]
                for tag, part in panels[label].groupby("fold")
            }
        ics = pd.DataFrame(ic).sort_index()
        ics.loc["MEAN"] = ics.mean()
        print(ics.to_string(float_format=lambda v: f"{v:+,.4f}"))

        if draws > 0:
            print("\n" + "=" * 108)
            print(f"THE NULL — y_pred shuffled within date, {draws} draws, "
                  f"top-{top_k}, 30 bps")
            print("=" * 108)
            for label, _ in spec:
                obs = P.stats(P.long_only_top_k(panels[label], horizon, column,
                                                top_k, 30 / 10_000), horizon)
                bar = P.null_bar(panels[label], horizon, column, top_k, 30 / 10_000,
                                 draws=draws, seed=7)
                z = (obs["sharpe"] - bar["sharpe_null_mean"]) / bar["sharpe_null_sd"]
                verdict = "CLEARS" if obs["sharpe"] > bar["sharpe_bar_p95"] else "FAILS"
                print(f"  {label:<12s} observed {obs['sharpe']:+.3f}  null mean "
                      f"{bar['sharpe_null_mean']:+.3f}  bar "
                      f"{bar['sharpe_bar_p95']:+.3f}  MAX "
                      f"{bar['sharpe_null_max']:+.3f}  z={z:+.2f}  {verdict}")

        print("\n⚠️ Read `t_paired`, not the Sharpe gap. The arms share the market "
              "factor, so an\n   unpaired comparison of two ~0.16-SE Sharpes cannot "
              "resolve a difference this size.")
        print("⚠️ A TIE IS A FINDING: it says the result lives in the 13 CHANNELS and "
              "not in the\n   architecture — which makes PRF-7's selection look-ahead "
              "the whole story.")
    return pooled, pairs, folds


if __name__ == "__main__":
    main()
