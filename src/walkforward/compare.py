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
from walkforward import manifest
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


def _shared_horizon(arms: Sequence[Tuple[str, str]],
                    requested: Optional[int] = None) -> int:
    """The one horizon every arm was built at, or a refusal naming the disagreement."""
    per_arm = {label: manifest.horizon_for(directory, requested)
               for label, directory in arms}
    distinct = set(per_arm.values())
    if len(distinct) > 1:
        raise SystemExit(
            "\n⚠️  the arms were built at different horizons and cannot be compared "
            "arm-for-arm:\n"
            + "".join(f"    {label:<14s} h={h}\n" for label, h in per_arm.items())
            + "    `compare` pairs arms INSIDE one sweep — same dates, same panel. Two "
              "horizons produce different period counts over different holding "
              "intervals, so use `python -m walkforward.pair` (P2-4), which pairs on "
              "the CALENDAR.\n"
        )
    return distinct.pop()


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


#: ⚠️ **TWO PERIODS, AND THE CHOICE IS ABOUT THE DESIGN RATHER THAN ABOUT TASTE.**
#: `pair` bootstraps DAILY returns and must use `2 × horizon` because consecutive days
#: share a holding window. Here the unit is a *non-overlapping* period — `rebalance_dates`
#: takes every `h`-th date — so the mechanical overlap is one period and the analogue of
#: `pair`'s `2 × lag` is 2. `paired` reports the difference series' lag-1 autocorrelation
#: beside the result so the choice is auditable rather than asserted.
DEFAULT_BLOCK = 2


def paired(a: pd.Series, b: pd.Series, horizon: int,
           sessions: float = P.SESSIONS_PER_YEAR, draws: int = 2000,
           block: int = DEFAULT_BLOCK, seed: int = 7) -> Dict[str, float]:
    """Paired difference `a - b` over the common rebalance dates — BOTH estimands.

    ⚠️ **`P1-9`: THIS USED TO RETURN A `t` ON THE MEAN RETURN ONLY, WHILE `main` PRINTED
    `d_sharpe` IN THE SAME ROW.** They are different questions and they can disagree in
    SIGN — the h=10 arm sweep's `gbt` shows `d_sharpe` **+0.36** beside `t` **−1.02**, a
    lower mean return at lower volatility, which is not a contradiction and reads like
    one. `PRF-8`'s *"paired |t| < 1 at every cost level"* and §6-0-ter-2's ties were both
    read off that `t`, so both are statements about MEAN RETURN; on the Sharpe difference
    the architecture question had never been tested at either horizon.

    A mean is a linear functional; a Sharpe is a ratio whose denominator also moves, so
    the second estimand needs its own interval and cannot be inferred from the first.
    `pair.block_bootstrap_diff` already makes exactly this distinction (P2-4, after its
    own first version conflated them) and is REUSED here rather than rewritten.
    """
    from walkforward.pair import block_bootstrap_diff, sharpe

    joined = pd.concat({"a": a, "b": b}, axis=1).dropna()
    blank = {"n": len(joined), "corr": float("nan"), "mean_diff": float("nan"),
             "t": float("nan"), "ac1": float("nan"), "d_sharpe": float("nan"),
             "sharpe_lo": float("nan"), "sharpe_hi": float("nan"),
             "p_sharpe": float("nan")}
    if len(joined) < 3:
        return blank

    x = joined["a"].to_numpy(float)
    y = joined["b"].to_numpy(float)
    diff = x - y
    n = len(diff)
    sd = float(diff.std(ddof=1))
    # ⚠️ The annualisation factor of ONE observation. A period is `horizon` sessions, so
    # it is `252/h` and not 252 — the same constant `stats` uses, which is what makes the
    # `d_sharpe` below reproduce the pooled table's difference exactly (asserted in
    # `test_compare_sharpe.py`) instead of being a second, quietly different Sharpe.
    per_year = sessions / horizon

    # The lag-1 autocorrelation of the DIFFERENCE, which is what the block length has to
    # cover. Reported, not assumed away.
    ac1 = (float(np.corrcoef(diff[:-1], diff[1:])[0, 1])
           if n > 3 and diff[:-1].std() > 0 and diff[1:].std() > 0 else float("nan"))

    boot = (block_bootstrap_diff(x, y, block=block, draws=draws, seed=seed,
                                 sessions=per_year)
            if draws > 0 else None)
    return {
        "n": n,
        "corr": float(np.corrcoef(x, y)[0, 1]),
        # annualised, so it reads beside CAGR rather than as a 20-session number
        "mean_diff": float(diff.mean() * per_year),
        "t": float(diff.mean() / (sd / np.sqrt(n))) if sd > 0 else float("nan"),
        "ac1": ac1,
        "d_sharpe": sharpe(x, per_year) - sharpe(y, per_year),
        "sharpe_lo": boot["sharpe"]["ci_lo"] if boot else float("nan"),
        "sharpe_hi": boot["sharpe"]["ci_hi"] if boot else float("nan"),
        "p_sharpe": boot["sharpe"]["p"] if boot else float("nan"),
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
    draws = int(option("--draws", 0))
    spec = _parse_arms(argv)
    # ⚠️ `WFO-1`, scoring half. This was `int(option("--horizon", 20))`, so an h=10 arm
    # sweep compared without the flag scored `return_20day` against h=10 predictions.
    # Derived PER ARM and asserted equal — which is the check `load_arms` cannot make,
    # since two tracks at two horizons can cover the same `(date, ticker)` index and
    # still be two different experiments.
    horizon = _shared_horizon(spec, int(option("--horizon")) if "--horizon" in argv
                              else None)
    # ⚠️ `--draws` already means the WITHIN-DATE shuffle null on each arm's own track and
    # is expensive; the paired bootstrap is a different question and cheap (it resamples
    # ~100-240 period returns), so it gets its own flag and is ON by default. P1-9 exists
    # because the Sharpe gap had no interval at all.
    boot_draws = int(option("--boot-draws", 2000))
    block = int(option("--block", DEFAULT_BLOCK))

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
        print("⚠️  TWO ESTIMANDS, and they can disagree in SIGN (P1-9). `t_ret` tests the "
              "MEAN period\n    RETURN difference; `d_sharpe` is risk-adjusted and "
              "carries its OWN bootstrap interval.\n    Reading a Sharpe gap off `t_ret` "
              "is what PRF-8 and §6-0-ter-2 did.\n")
        pair_rows = []
        for label, _ in spec[1:]:
            for cost in COSTS:
                stat = paired(nets[label][cost], nets[reference][cost], horizon,
                              draws=boot_draws, block=block)
                a = float(pooled.loc[pooled["arm"] == label, f"sharpe@{cost}"].iloc[0])
                b = float(pooled.loc[pooled["arm"] == reference,
                                     f"sharpe@{cost}"].iloc[0])
                # ⚠️ The paired Sharpe difference is computed from the SAME `net` series
                # the pooled table scores, so the two must agree. Asserted rather than
                # trusted: a silent disagreement here is precisely the defect P1-9 is.
                if np.isfinite(stat["d_sharpe"]) and abs((a - b) - stat["d_sharpe"]) > 1e-9:
                    raise AssertionError(
                        f"arm {label} @{cost}bps: the pooled Sharpe difference "
                        f"{a - b:+.6f} does not reproduce the paired one "
                        f"{stat['d_sharpe']:+.6f} — the two are scoring different rows"
                    )
                pair_rows.append({
                    "arm": label, "bps": cost, "sharpe": a,
                    f"sharpe_{reference}": b,
                    "corr": stat["corr"], "n": stat["n"], "ac1": stat["ac1"],
                    "d_cagr_ann": stat["mean_diff"], "t_ret": stat["t"],
                    "d_sharpe": stat["d_sharpe"],
                    "sh_ci_lo": stat["sharpe_lo"], "sh_ci_hi": stat["sharpe_hi"],
                    "p_sharpe": stat["p_sharpe"],
                })
        pairs = pd.DataFrame(pair_rows)
        print(pairs.to_string(index=False, float_format=lambda v: f"{v:,.4f}"))
        print(f"\n  bootstrap: {boot_draws} circular block draws, block={block} "
              f"period(s), the SAME blocks drawn from both arms so the pairing survives."
              f"\n  `ac1` is the lag-1 autocorrelation of the DIFFERENCE — the thing the "
              f"block length has to cover.")

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

        print("\n⚠️ BOTH columns are paired and NEITHER is a summary of the other "
              "(P1-9). `t_ret` tests\n   the mean period-RETURN gap; `d_sharpe` is "
              "risk-adjusted and is read against its OWN\n   bootstrap CI. An arm can "
              "lose on one and tie on the other — that is not a\n   contradiction, it "
              "is a lower mean return at lower volatility.")
        print("⚠️ A TIE ON BOTH is a finding: it says the result lives in the CHANNELS "
              "and not in the\n   architecture — which makes PRF-7's selection "
              "look-ahead the whole story. A tie on\n   ONE is not that finding.")
    return pooled, pairs, folds


if __name__ == "__main__":
    main()
