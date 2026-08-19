# src\backtest\handscreen.py
"""The 3-channel hand-built rank, as a `y_pred` the normal backtest can price — `PRF-2`.

    python -m backtest.handscreen --run <run_id> --split test --top-k 20

⚠️ **WHY THIS EXISTS.** `backtest/CONTEXT.md` §8g measured a hand-built rank at h=10 —
Sharpe **+0.652** at 30 bps, z = +4.72 — and §4 measured a fitted LSTM at h=20. **Nobody
has ever put the two on one panel**, so *how much a fitted model adds over three ranked
columns* is unknown at EVERY horizon. That is the question `PRF-2` exists to answer, and
CLAUDE.md §5c is the reason it is worth asking: eleven architectures spanning 0 to 276 k
parameters landed inside one error bar, and a 25-parameter ridge was among the best.

**The rule, quoted from §8g so it can be refuted:** universe = top liquidity quintile by
that date's `value_matched`; drop any name at its exchange's ceiling; score = **mean
within-date percentile rank** of `drv_order_vol_imb_5`, the trailing 5-day return, and
`drv_dist_from_high_63`; equal-weight the top `k`; hold `h` sessions.

⚠️ **TWO DEPARTURES FROM §8g, BOTH DELIBERATE, BOTH BECAUSE THE COMPARISON IS THE POINT.**

1. **The candidate set is the MODEL'S, not the quintile.** §8g screened the whole market to
   its top liquidity quintile; the model picks from a fixed 150. Running the hand rule on
   its own universe and the model on another would compare two UNIVERSES and call it a
   comparison of two methods. `--quintile` restores §8g's screen for anyone who wants the
   faithful reproduction instead, and it is off by default.
2. **Same-close, not t+1.** §8g's own figure lags the signal one session. The model's
   `y_pred` at date `t` is paired with the forward return from `t`, so pricing the hand
   score at `t+1` while the model trades at `t` would hand the model a free session.
   ⚠️ **Neither convention is the tradable one** — `backtest/CONTEXT.md` records the 5-day
   signal decaying inside ONE session (+24.4 % same-close against +5.6 % at t+1), which is
   `PRF-6`'s intraday-data argument. What matters here is only that both sides use one.

⚠️ **THE TRAILING RETURN IS DERIVED, NOT A STORED CHANNEL.** `pool__basic` has `drv_ret_1d`
but no 5-day trailing return, so it is computed here as `close_adjust / close_adjust.shift(5)
- 1` per ticker over the FULL history and then filtered to the scored dates. Computing it
after the filter would compare each date with the previous SCORED one, which spans the split
boundary and every gap.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from backtest import portfolio as P

#: The three channels, in §8g's order. ⚠️ `trailing_ret_5` is derived here; the other two
#: are `pool__basic` columns of the same names.
CHANNELS = ("drv_order_vol_imb_5", "trailing_ret_5", "drv_dist_from_high_63")

#: §8g's own universe screen, kept behind a flag. `0.8` = the top quintile by that date's
#: matched turnover.
QUINTILE = 0.8


def hand_panel(universe: str, tickers, dates, quintile: bool = False) -> pd.DataFrame:
    """`(date, ticker, y_pred, in_quintile)` for the hand rule, over the given rows.

    The score is the **mean of three within-date percentile ranks**, so it needs no
    scaling and is invariant to each channel's units — the same reason
    `cross_sectional.py` §3 gives for ranking features within a date.
    """
    from feature_selection.unified_reader import UnifiedSchemaReader

    columns = ["date", "ticker", "close_adjust", "value_matched",
               "drv_order_vol_imb_5", "drv_dist_from_high_63"]
    with UnifiedSchemaReader(universe) as reader:
        basic = reader.read("pool__basic", columns=columns)
    basic["date"] = pd.to_datetime(basic["date"])
    basic["ticker"] = basic["ticker"].astype(str).str.upper()
    return score_frame(basic, tickers, dates, quintile=quintile)


def score_frame(basic: pd.DataFrame, tickers, dates,
                quintile: bool = False) -> pd.DataFrame:
    """The scoring half of `hand_panel`, with the database read taken out.

    Separated so the two traps below can be pinned by a test rather than by a comment.
    """
    basic = basic.sort_values(["ticker", "date"]).copy()

    # ⚠️ **OVER THE FULL HISTORY, THEN FILTERED.** Computing this after the filter would
    # make `shift(5)` step back five SCORED rows, which spans the split boundary and every
    # gap in the sample — the same defect `exchange_and_ceiling` guards against for the
    # daily return.
    basic["trailing_ret_5"] = basic.groupby("ticker")["close_adjust"].pct_change(5)

    keep = basic["ticker"].isin(set(tickers)) & basic["date"].isin(set(dates))
    frame = basic.loc[keep].copy()

    ranks = []
    for channel in CHANNELS:
        # `pct=True` is the within-date percentile; NaN stays NaN so a name missing one
        # channel still contributes to the others and is dropped only if it has none.
        ranks.append(frame.groupby("date")[channel].rank(pct=True))
    frame["y_pred"] = pd.concat(ranks, axis=1).mean(axis=1, skipna=True)

    frame["in_quintile"] = (
        frame.groupby("date")["value_matched"].rank(pct=True) >= QUINTILE
    )
    if quintile:
        frame = frame[frame["in_quintile"]]
    return frame[["date", "ticker", "y_pred", "in_quintile"]].reset_index(drop=True)


def compare(run_dir: str, split: str, top_k: int, costs: Sequence[int] = (20, 30, 50),
            quintile: bool = False, draws: int = 0) -> Dict:
    """Price the model and the hand rule on ONE panel, and pair the difference."""
    from backtest.run import build_panel
    from walkforward.compare import paired

    built = build_panel(run_dir, split)
    panel, h = built["panel"], built["horizon"]
    column = built["absolute"]

    hand = hand_panel(built["universe"], panel["ticker"], panel["date"], quintile=quintile)
    merged = panel.drop(columns=["y_pred"]).merge(
        hand, on=["date", "ticker"], how="inner", validate="one_to_one"
    )
    # ⚠️ The MODEL is re-priced on the merged frame, not on `panel`. If the hand join drops
    # a row (a name with no trailing return early in the sample), pricing the model on the
    # wider frame would give it dates the baseline never saw.
    model = panel.merge(hand[["date", "ticker"]], on=["date", "ticker"], how="inner")

    out = {"run": os.path.basename(run_dir), "split": split, "horizon": h,
           "top_k": top_k, "quintile": quintile,
           "rows": len(model), "dates": int(model["date"].nunique()),
           "dropped_by_hand_join": len(panel) - len(model)}

    rows, nets = [], {}
    for label, frame in (("model", model), ("hand", merged)):
        nets[label] = {}
        for cost in costs:
            track = P.long_only_top_k(frame, h, column, top_k, cost / 10_000)
            stats = P.stats(track, h)
            nets[label][cost] = track.frame().set_index("date")["net"]
            rows.append({"strategy": label, "bps": cost, **stats})
    market = P.stats(P.buy_and_hold(model, h, column), h)
    rows.append({"strategy": "equal-weight universe", "bps": 0, **market})
    out["table"] = pd.DataFrame(rows)

    out["paired"] = pd.DataFrame([
        {"bps": cost,
         "d_sharpe": float(out["table"].query("strategy=='model' and bps==@cost")["sharpe"].iloc[0]
                           - out["table"].query("strategy=='hand' and bps==@cost")["sharpe"].iloc[0]),
         **paired(nets["model"][cost], nets["hand"][cost], h)}
        for cost in costs
    ])

    if draws > 0:
        bars = []
        for label, frame in (("model", model), ("hand", merged)):
            observed = P.stats(P.long_only_top_k(frame, h, column, top_k, 30 / 10_000), h)
            bar = P.null_bar(frame, h, column, top_k, 30 / 10_000, draws=draws, seed=7)
            bars.append({
                "strategy": label, "observed": observed["sharpe"],
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

    run = str(option("--run", ""))
    if not run:
        raise SystemExit("--run <run_id> is required")
    run_dir = run if os.path.isdir(run) else os.path.join(DEFAULT_RUNS_DIR, run)
    split = str(option("--split", "test"))
    top_k = int(option("--top-k", 20))
    draws = int(option("--draws", 200))
    quintile = "--quintile" in argv

    with runtime.RunTimer(f"handscreen  {os.path.basename(run_dir)}  {split}",
                          show_gpu=False):
        out = compare(run_dir, split, top_k, quintile=quintile, draws=draws)
        pd.set_option("display.width", 200)
        pd.set_option("display.max_columns", 30)

        print(f"\n{out['rows']:,} rows, {out['dates']} dates, h={out['horizon']}, "
              f"top-{top_k}, ceiling-screened (PRF-0)")
        print(f"the hand join dropped {out['dropped_by_hand_join']:,} rows the model had "
              f"(no trailing 5-day return yet); both sides are priced on the REMAINDER")
        print(f"universe: {'§8g top liquidity quintile' if quintile else 'the MODEL''s own — the comparison is method vs method, not universe vs universe'}\n")

        print("=" * 100)
        print(out["table"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))
        print("\n" + "=" * 100)
        print("PAIRED model - hand (the two trade the same dates, so pair the difference)")
        print("=" * 100)
        print(out["paired"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))
        if "null" in out:
            print("\n" + "=" * 100)
            print(f"THE NULL — y_pred shuffled within date, {draws} draws, 30 bps")
            print("=" * 100)
            print(out["null"].to_string(index=False, float_format=lambda v: f"{v:,.4f}"))

        print("\n⚠️ A model that does not beat three ranked columns has not earned its "
              "complexity (§5 rule 4's shape).\n⚠️ `t_paired` is on the RETURN difference; "
              "read it beside `d_sharpe`, which is the Sharpe one.")
    return out


if __name__ == "__main__":
    main()
