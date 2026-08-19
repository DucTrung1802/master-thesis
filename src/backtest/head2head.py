# src\backtest\head2head.py
"""Two scored runs, ONE panel, a PAIRED verdict — `PRF-9`'s downstream test.

    python -m backtest.head2head --a <run_id> --b <run_id> --split test --top-k 15

⚠️ **WHY A SEPARATE MODULE AND NOT TWO `python -m backtest` CALLS.** Reading two runs'
`backtest_test.csv` side by side compares two Sharpes computed over **different rows** and
with **no pairing**. Both defects push the same way — toward calling a difference real:

1. **Different rows.** `pool__ta` stops 2026-06-26 (`STA-1`), so a chain that joins it
   splits on a shorter panel — 2023-11-03 → 2026-06-26 against the narrow chain's
   2023-11-15 → 2026-07-10. Two Sharpes over two windows are two measurements of two
   things. This prices both on the **INTERSECTION**, asserted rather than assumed.
2. **No pairing.** Both books trade the same names on the same dates out of the same
   universe, so their period returns are strongly correlated and `se_sharpe` ≈ 0.25 is
   the error bar on the wrong quantity. `walkforward.compare.paired` is reused here for
   exactly the reason it exists — CLAUDE.md §5c's eleven architectures inside one error
   bar is what an unpaired reading of numbers this close produces.

⚠️ **THE PRICE BAND IS APPLIED TO BOTH** (`PRF-0`), by `build_panel`'s default. A name at
its exchange ceiling had no sellers, so buying it is fiction on either side.

⚠️ **What a tie means here.** `PRF-9` asks whether offering `pool__ta` alongside
`pool__basic` buys anything. A tie says the extra channels changed the shortlist without
changing the money — which, given `PRF-8` found the architecture worth nothing, would
leave the 13 original channels as the whole result.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Sequence

import numpy as np
import pandas as pd

from backtest import portfolio as P

COSTS = (20, 30, 50)


def compare_runs(run_a: str, run_b: str, split: str, top_k: int,
                 costs: Sequence[int] = COSTS, draws: int = 0) -> Dict:
    """Price two run folders' predictions on the rows they share."""
    from backtest.run import build_panel
    from walkforward.compare import paired

    built_a = build_panel(run_a, split)
    built_b = build_panel(run_b, split)
    if built_a["horizon"] != built_b["horizon"]:
        raise ValueError(
            f"h={built_a['horizon']} vs h={built_b['horizon']} — two horizons are two "
            f"rebalance schedules and two cost drags, not two models. Compare within one."
        )
    if built_a["universe"] != built_b["universe"]:
        raise ValueError(
            f"universes {built_a['universe']!r} vs {built_b['universe']!r} differ; the "
            f"realised returns come from different pool__targets."
        )

    h, column = built_a["horizon"], built_a["absolute"]
    keys = ["date", "ticker"]
    a = built_a["panel"][keys + ["y_pred", column]].rename(columns={"y_pred": "a"})
    b = built_b["panel"][keys + ["y_pred"]].rename(columns={"y_pred": "b"})
    shared = a.merge(b, on=keys, how="inner", validate="one_to_one")

    out = {
        "a": os.path.basename(run_a), "b": os.path.basename(run_b),
        "split": split, "horizon": h, "top_k": top_k,
        "rows_a": len(built_a["panel"]), "rows_b": len(built_b["panel"]),
        "rows_shared": len(shared), "dates_shared": int(shared["date"].nunique()),
        "first": shared["date"].min(), "last": shared["date"].max(),
    }
    if not len(shared):
        raise ValueError("the two runs share no (date, ticker) rows at all")

    rows, nets = [], {}
    for label in ("a", "b"):
        frame = shared.rename(columns={label: "y_pred"})
        nets[label] = {}
        for cost in costs:
            track = P.long_only_top_k(frame, h, column, top_k, cost / 10_000)
            nets[label][cost] = track.frame().set_index("date")["net"]
            rows.append({"run": out[label], "arm": label, "bps": cost,
                         **P.stats(track, h)})
    market = P.stats(P.buy_and_hold(shared.rename(columns={"a": "y_pred"}), h, column), h)
    rows.append({"run": "equal-weight universe", "arm": "market", "bps": 0, **market})
    out["table"] = pd.DataFrame(rows)

    pair_rows = []
    for cost in costs:
        sa = float(out["table"].query("arm=='a' and bps==@cost")["sharpe"].iloc[0])
        sb = float(out["table"].query("arm=='b' and bps==@cost")["sharpe"].iloc[0])
        pair_rows.append({"bps": cost, "sharpe_a": sa, "sharpe_b": sb,
                          "d_sharpe": sa - sb,
                          **paired(nets["a"][cost], nets["b"][cost], h)})
    out["paired"] = pd.DataFrame(pair_rows)

    # Daily IC on the shared rows, so the ranking is compared on the same footing as
    # the money. ⚠️ n_eff = n_dates / h, never n_rows / h (§5 rule 7).
    ics = {}
    for label in ("a", "b"):
        per_day = []
        for _, day in shared.groupby("date"):
            day = day.dropna(subset=[label, column])
            if len(day) < 5:
                continue
            x, y = day[label].rank(), day[column].rank()
            if x.std() > 0 and y.std() > 0:
                per_day.append(float(np.corrcoef(x, y)[0, 1]))
        arr = np.array(per_day, dtype=float)
        n_eff = max(1.0, len(arr) / h)
        ics[label] = {
            "ic": float(arr.mean()), "days_pos": float((arr > 0).mean()),
            "ic_t": float(arr.mean() / (arr.std(ddof=1) / np.sqrt(n_eff))),
            "n_dates": len(arr), "n_eff": round(n_eff, 1),
        }
    out["ic"] = pd.DataFrame([{"run": out[k], **ics[k]} for k in ("a", "b")])

    if draws > 0:
        bars = []
        for label in ("a", "b"):
            frame = shared.rename(columns={label: "y_pred"})
            observed = P.stats(P.long_only_top_k(frame, h, column, top_k, 30 / 10_000), h)
            bar = P.null_bar(frame, h, column, top_k, 30 / 10_000, draws=draws, seed=7)
            bars.append({
                "run": out[label], "observed": observed["sharpe"],
                "null_mean": bar["sharpe_null_mean"], "bar_p95": bar["sharpe_bar_p95"],
                "null_max": bar["sharpe_null_max"],
                "z": (observed["sharpe"] - bar["sharpe_null_mean"]) / bar["sharpe_null_sd"],
            })
        out["null"] = pd.DataFrame(bars)
    return out


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    from backtest.run import DEFAULT_RUNS_DIR
    from utils import runtime

    def resolve(name: str) -> str:
        return name if os.path.isdir(name) else os.path.join(DEFAULT_RUNS_DIR, name)

    run_a, run_b = option("--a"), option("--b")
    if not (run_a and run_b):
        raise SystemExit("--a <run_id> --b <run_id> are both required")
    split = str(option("--split", "test"))
    top_k = int(option("--top-k", 15))
    draws = int(option("--draws", 200))

    with runtime.RunTimer(f"head2head  k={top_k}  {split}", show_gpu=False):
        out = compare_runs(resolve(run_a), resolve(run_b), split, top_k, draws=draws)
        pd.set_option("display.width", 220)
        pd.set_option("display.max_columns", 30)

        print(f"\nA  {out['a']}   ({out['rows_a']:,} rows)")
        print(f"B  {out['b']}   ({out['rows_b']:,} rows)")
        print(f"\nPRICED ON THE INTERSECTION: {out['rows_shared']:,} rows, "
              f"{out['dates_shared']} dates, "
              f"{out['first'].date()} -> {out['last'].date()}, "
              f"ceiling-screened (PRF-0)")
        dropped_a = out["rows_a"] - out["rows_shared"]
        dropped_b = out["rows_b"] - out["rows_shared"]
        print(f"  dropped {dropped_a:,} rows A had and B did not, "
              f"{dropped_b:,} the other way\n")

        print("=" * 110)
        print(f"long-only top-{top_k}, held {out['horizon']} sessions")
        print("=" * 110)
        print(out["table"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        print("\n" + "=" * 110)
        print("DAILY IC on the same rows")
        print("=" * 110)
        print(out["ic"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        print("\n" + "=" * 110)
        print("PAIRED A - B (they trade the same dates, so pair the difference)")
        print("=" * 110)
        print(out["paired"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        if "null" in out:
            print("\n" + "=" * 110)
            print(f"THE NULL — y_pred shuffled within date, {draws} draws, 30 bps")
            print("=" * 110)
            print(out["null"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        print("\n⚠️ Read `t_paired` (on the RETURN difference) beside `d_sharpe`. Two "
              "Sharpes\n   at se ~0.25 cannot resolve a difference this size unpaired.")
    return out


if __name__ == "__main__":
    main()
