# src\backtest\run.py
"""Stage 9 — read a scored run folder, join the realised returns, report money.

⚠️ **THE RUN FOLDER IS NOT ENOUGH AND THAT IS THE WHOLE REASON THIS MODULE TOUCHES THE
DATABASE.** `predictions_*.csv` carries `y_pred` and a `y_true` that, on a `cs_rank_*`
run, is a RANK in [-0.5, +0.5]. A rank has no PnL. The realised forward return lives in
`unified_schema_<universe>.pool__targets`, so the join happens here, keyed on
`(date, ticker)` and validated one-to-one.

⚠️ **`return_{h}day` IS WHAT YOU CAN TRADE; `return_rel_{h}day` IS WHAT THE MODEL
PREDICTS.** Both are reported and they are not interchangeable. The relative one is the
model's own quantity — it subtracts `hose__vnindex__close_adjust` — but realising it
needs a short leg against VNINDEX, and **this database holds no such instrument**:
`silver.stock_market` carries six SPOT indices and no futures, and `experiment_3` records
that single-stock shorting is effectively unavailable on HOSE. So the relative column is
the honest measure of the SIGNAL and the absolute column is the honest measure of the
TRADE, and a reader who quotes the first as money has quoted an unhedgeable number.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, List, Optional

import pandas as pd

from backtest import portfolio as P

DEFAULT_RUNS_DIR = os.path.join(os.path.dirname(__file__), "..", "model", "runs")


def _read_predictions(run_dir: str, split: str) -> pd.DataFrame:
    path = os.path.join(run_dir, "results", f"predictions_{split}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"no {path}")
    frame = pd.read_csv(path)
    missing = [c for c in ("date", "ticker", "y_pred") if c not in frame.columns]
    if missing:
        raise ValueError(f"{path} has no column(s) {missing}; a panel run is required")
    frame["date"] = pd.to_datetime(frame["date"])
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    return frame


def _run_setup(run_dir: str) -> Dict:
    """Horizon, universe and target for a run — read, never guessed twice.

    ⚠️ **`horizon` comes from `result_evaluator.evaluator._setup`, not from a second
    reader here.** That function already knows the three places a run may record it
    (`config.horizon`, `lineage.horizon_h`, the run NAME) and records which ones it had
    to infer; re-implementing it is how two stages come to disagree about `h` — and `h`
    is the rebalance period, so disagreeing means costing the wrong number of trades.

    ⚠️ The universe comes from `lineage.schema` (`unified_schema_all` -> `all`), which
    is the schema the table was actually built in. `dataset.universe` does not exist on
    a run of this vintage and reading it returned `None`.
    """
    from result_evaluator.evaluator import _setup as evaluator_setup

    with open(os.path.join(run_dir, "metadata.json"), encoding="utf-8") as handle:
        meta = json.load(handle)
    resolved = evaluator_setup(meta, run_dir)

    lineage = meta.get("lineage") or {}
    schema = str(lineage.get("schema") or "")
    universe = schema.replace("unified_schema_", "") if schema else ""
    if not universe:
        raise ValueError(
            f"{run_dir}: metadata.json names no lineage.schema, so the universe whose "
            "pool__targets holds the realised returns is unknown. Refusing to guess."
        )
    return {
        "horizon": int(resolved["horizon"]),
        "universe": universe,
        "target": lineage.get("target"),
        "inferred": resolved.get("inferred", []),
        "meta": meta,
    }


def realised_returns(universe: str, horizon: int, tickers, dates) -> pd.DataFrame:
    """`pool__targets` for the names and dates the run actually scored.

    ⚠️ Read through `UnifiedSchemaReader`, not a hand-written SELECT, so the `numeric`
    → `Decimal` → dtype `object` trap (CLAUDE.md §5 rule 15) is handled in one place.
    """
    from feature_selection.unified_reader import UnifiedSchemaReader

    columns = ["date", "ticker", f"return_{horizon}day", f"return_rel_{horizon}day"]
    with UnifiedSchemaReader(universe) as reader:
        frame = reader.read("pool__targets", columns=columns)
    frame["date"] = pd.to_datetime(frame["date"])
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    keep = frame["ticker"].isin(set(tickers)) & frame["date"].isin(set(dates))
    return frame[keep].reset_index(drop=True)


def build_panel(run_dir: str, split: str) -> Dict:
    """`predictions_<split>.csv ⋈ pool__targets`, one-to-one, with the join asserted."""
    setup = _run_setup(run_dir)
    predictions = _read_predictions(run_dir, split)
    truth = realised_returns(
        setup["universe"], setup["horizon"], predictions["ticker"], predictions["date"]
    )

    before = len(predictions)
    panel = predictions.merge(truth, on=["date", "ticker"], how="left", validate="one_to_one")
    if len(panel) != before:
        raise AssertionError(
            f"the join changed the row count {before} -> {len(panel)}; "
            "(date, ticker) is not one-to-one on one side"
        )
    absolute = f"return_{setup['horizon']}day"
    matched = int(panel[absolute].notna().sum())
    return {
        **setup,
        "split": split,
        "panel": panel,
        "absolute": absolute,
        "relative": f"return_rel_{setup['horizon']}day",
        "rows": before,
        "matched": matched,
    }


def null_table(built: Dict, top_k: int, draws: int, round_trip: float) -> pd.DataFrame:
    """The top-k strategy against `draws` within-date shuffles, per target definition.

    ⚠️ **Without this the stage reports a Sharpe with no bar, which is the exact error
    CLAUDE.md §5 rule 1 exists to prevent.** The shuffled draws pick from the SAME 150
    names, so this null also controls for the universe's survivorship (§2c): the
    z-score is protected by it even though the headline CAGR is not.
    """
    panel, h = built["panel"], built["horizon"]
    rows: List[Dict] = []
    for column, kind in ((built["absolute"], "absolute"), (built["relative"], "relative")):
        if column not in panel.columns:
            continue
        observed = P.stats(P.long_only_top_k(panel, h, column, top_k, round_trip), h)
        bar = P.null_bar(panel, h, column, top_k, round_trip, draws=draws)
        if not bar.get("draws"):
            continue
        rows.append({
            "target": kind,
            "observed_sharpe": observed["sharpe"],
            "se_sharpe": observed["se_sharpe"],
            "null_mean": bar["sharpe_null_mean"],
            "null_sd": bar["sharpe_null_sd"],
            "bar_p95": bar["sharpe_bar_p95"],
            "null_max": bar["sharpe_null_max"],
            "z": (observed["sharpe"] - bar["sharpe_null_mean"]) / bar["sharpe_null_sd"],
            "clears": bool(observed["sharpe"] > bar["sharpe_bar_p95"]),
            "draws": bar["draws"],
        })
    return pd.DataFrame(rows)


def sweep(built: Dict, ticker: str, top_k: int, costs=P.COST_SWEEP) -> pd.DataFrame:
    """Every strategy × every cost, one row each. The benchmarks pay no ongoing cost."""
    panel, h = built["panel"], built["horizon"]
    rows: List[Dict] = []
    for column, kind in ((built["absolute"], "absolute"), (built["relative"], "relative")):
        if column not in panel.columns:
            continue
        benchmarks = [
            P.buy_and_hold(panel, h, column, ticker=ticker),
            P.buy_and_hold(panel, h, column),
        ]
        for track in benchmarks:
            rows.append({"target": kind, "bps": 0, "strategy": track.name,
                         **P.stats(track, h)})
        for cost in costs:
            for track in (
                P.long_flat_single(panel, ticker, h, column, round_trip=cost),
                P.long_only_top_k(panel, h, column, k=top_k, round_trip=cost),
            ):
                rows.append({"target": kind, "bps": round(cost * 10_000),
                             "strategy": track.name, **P.stats(track, h)})
    return pd.DataFrame(rows)


def main(argv: Optional[List[str]] = None) -> pd.DataFrame:
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    from utils import runtime

    run = option("--run")
    if not run:
        raise SystemExit(
            "usage: python -m backtest --run <run_id> [--ticker VCB] [--top-k 15] "
            "[--split test] [--draws 200] [--runs DIR]"
        )
    runs_dir = option("--runs", DEFAULT_RUNS_DIR)
    run_dir = run if os.path.isdir(run) else os.path.join(runs_dir, run)
    ticker = str(option("--ticker", "VCB")).upper()
    top_k = int(option("--top-k", 15))
    split = option("--split", "test")

    with runtime.RunTimer(f"backtest  {os.path.basename(run_dir)}  {split}", show_gpu=False):
        built = build_panel(run_dir, split)
        table = sweep(built, ticker, top_k)

        pd.set_option("display.width", 200)
        pd.set_option("display.max_columns", 30)
        print(
            f"\n{built['rows']:,} scored rows, {built['matched']:,} with a realised "
            f"{built['absolute']}  |  h={built['horizon']}  universe={built['universe']}"
        )
        print(f"rebalance every {built['horizon']} sessions -> "
              f"{P.SESSIONS_PER_YEAR / built['horizon']:.1f} per year\n")
        for kind in table["target"].unique():
            block = table[table["target"] == kind]
            print(f"--- {kind} returns " + "-" * 60)
            print(block.drop(columns=["target"]).to_string(index=False,
                  float_format=lambda v: f"{v:,.4f}"))
            print()

        draws = int(option("--draws", 200))
        if draws > 0:
            bars = null_table(built, top_k, draws, P.ROUND_TRIP_COST)
            print(f"--- the NULL: y_pred shuffled WITHIN each date, {draws} draws, "
                  f"top-{top_k} @ {P.ROUND_TRIP_COST * 10_000:.0f} bps " + "-" * 12)
            print(bars.to_string(index=False, float_format=lambda v: f"{v:,.4f}"))
            print()
            bars.to_csv(
                os.path.join(run_dir, "results", f"backtest_null_{split}.csv"), index=False
            )

        out = os.path.join(run_dir, "results", f"backtest_{split}.csv")
        table.to_csv(out, index=False)
        print(f"wrote {out}")
        print(
            "\n⚠️ Read `se_sharpe` before `sharpe`: at h=%d this test window holds ~%d "
            "independent periods, so two strategies inside ~2x that SE are one "
            "measurement.\n⚠️ `relative` returns are the model's own quantity and are "
            "NOT tradable here — no short instrument exists in this database."
            "\n⚠️ The null prices in the universe, the cost, the schedule and "
            "k. It does NOT price in the feature selection, the architecture search, or "
            "the choice of this window (NUL-1). A cleared bar is a floor, not a result."
            % (built["horizon"], int(table["n_periods"].max()))
        )
    return table


if __name__ == "__main__":
    main()
