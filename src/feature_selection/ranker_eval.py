# src\feature_selection\ranker_eval.py
"""What is each ranker WORTH? — advantage, cost, necessity, efficiency, in one table.

    python -m feature_selection.ranker_eval --cost-only     # archive timings, no GPU
    python -m feature_selection.ranker_eval --apply         # the full measurement
    python -m feature_selection.ranker_eval --apply --targets return_5day,return_rel_5day

`CONTEXT.md` §4 has always said what each ranker SEES. That is a claim about its
inductive bias. This module measures a different claim — **does it pick channels that
generalise** — and it is the module behind §19, where the default ensemble went from six
rankers to three on 2026-08-16.

⚠️ **THIS EXISTS BECAUSE THE MEASUREMENT WAS FIRST DONE IN A THROWAWAY SCRIPT, AND THE
SCRIPT GOT THE ARITHMETIC WRONG.** The published `mean` column averaged each row's four
measured cells *and its own `min` column*, biasing every mean low by 2-12 points; one
conclusion had to be withdrawn afterwards. A number that decides which code ships is not
a number to compute in a scratchpad. `scorecard()` is now the only place that arithmetic
lives, and `test_ranker_eval.py` pins it.

## The four questions, and the column that answers each

| question | column | how |
|---|---|---|
| **advantage** — does it beat chance? | `advantage_pct` | its own top-k, scored out of sample, as a percentile of `RANDOM_DRAWS` random-k picks |
| **necessity** — does the blend need it? | `necessity_delta` | the full ensemble's percentile minus the ensemble-without-it's |
| **cost** — what does it charge? | `pct_of_run`, `pct_of_ranking` | mean over every archived run's own `timings_seconds` |
| **efficiency** — advantage per unit cost | `edge_per_pct` | `(advantage - 50) / pct_of_ranking` |

## ⚠️ The null is RANDOM-k, not shuffled labels

Whether the pool predicts at all is a different question and it is settled — CLAUDE.md §2:
it does not. The question here is narrower, *does ranker M choose better than chance*, and
the control for that is chance. A ranker at the 100th percentile here beats **chance at
picking channels** on a pool that does not clear its own label null. **Nothing this module
prints is evidence of predictive skill.**

## ⚠️ A dead ranker's advantage is WITHDRAWN, not reported as low

`lasso` collapses to zero coefficients on a return target, so `sort_values()` returns the
channels in **pool column order** — its "top-k" is the first k columns of the pool, which
is not a selection. Scored anyway it produced the 92.5th percentile in one cell and the
2.5th in another; both are facts about column order. `advantage_pct` is `NaN` for such a
method and `withdrawn` says why. Same rule as CLAUDE.md §5 rule 21, mirrored: a metric
computed on a non-ranking is not a measurement.

⚠️ **AND "COLLAPSED" IS NOT THE SAME AS "IDENTICAL" — THAT COST A SECOND CORRECTION.**
The rule was first `nunique() <= 1`, which caught `return_5day` (byte-identical zeros) and
MISSED `return_rel_5day`, where the coefficients are all <= 1e-12 but differ in their last
bits. `nunique()` was 84, the rule did not fire, and the ranker was scored at the 81.25th
percentile for **sorting floating-point noise** — a degenerate ranking that does not look
degenerate, which is the more dangerous of the two. See `ZERO_TOL`.

## ⚠️ It writes a REPORT FOLDER, never a database table, and NEVER into the run root

`feature_selection` does not write to the database — that boundary is the package split
itself (CLAUDE.md §8), and a scorecard is a result object, not a table. The folder also
goes under `reports/ranker_evaluation/`, **not** `reports/feature_selection/`: the latter
is scanned by `contract.run_folders`, which calls any directory holding a `metadata.json`
a selection run. Writing there would make this evaluation appear in
`final_features.plan_from_reports` as a run whose channels belong in a `__final__` table.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from feature_selection import gpu, report, windows
from feature_selection.selector import ALL_METHODS, METHODS, FeatureSelector
from feature_selection.unified_reader import UnifiedSchemaReader
from utils import runtime

# ⚠️ NOT `report.DEFAULT_REPORT_ROOT`. See the module docstring — that root is scanned
# for selection runs and this is not one.
DEFAULT_ROOT = os.path.join(os.path.dirname(report.DEFAULT_REPORT_ROOT), "ranker_evaluation")

# The control. 40 draws puts the p95 bar at the 38th of 40, which is the coarsest the
# percentile can be and still separate "top of the table" from "typical".
RANDOM_DRAWS = 40
RANDOM_SEED = 7

# ⚠️ **A SCORE THIS SMALL IS NOT A SCORE.** The withdrawal rule below was first written
# as `nunique() <= 1` — "every channel scored identically" — and that is too strict by
# exactly the amount that matters. Measured 2026-08-16: on `return_5day` LassoCV returns
# byte-identical zeros and the rule fired; on `return_rel_5day` it returns values that are
# all <= 1e-12 but differ in the last bits, so `nunique()` is 84, the rule did NOT fire,
# and the ranker was scored at the 81.25th percentile for **sorting floating-point noise**.
# The test is therefore "effectively zero", not "identical".
ZERO_TOL = 1e-12

# ⚠️ TWO widths, because a ranker that wins at one k and loses at the other has not won.
# `+mrmr(shap)` scored 100th at both on one target and 50th at both on the other (§19f) —
# the widths agreed and the TARGETS did not, which is why both are varied.
K_VALUES = (10, 20)

# Every non-key column of `pool__targets`; whichever is not the target is excluded.
# ⚠️ Kept in step with `run.ALL_TARGETS` by `test_ranker_eval.py`, not by hand.
ALL_TARGETS = [
    "return_5day", "return_10day", "return_rel_5day", "return_rel_10day",
    "close_adjust_5day", "close_adjust_10day",
]
IDENTITY = [
    "exchange", "ticker", "sector", "sector_code", "industry_group",
    "industry_group_code", "industry", "industry_code", "sub_industry",
    "sub_industry_code",
]

# `timings_seconds` keys, per method. ⚠️ `xgb_gain` and `xgb_shap` share ONE fit and one
# timer, so their cost is that timer halved — neither can be removed to save all of it.
# ⚠️ `spearman` is absent on purpose: `target_corr` is computed for the report's SIGN
# whether or not it is in the ensemble, so its MARGINAL cost is exactly zero.
TIMER_KEYS = {
    "mutual_info": ("mutual_info",),
    "lasso": ("lasso",),
    "permutation": ("permutation",),
    "xgb_gain": ("xgb gain",),
    "xgb_shap": ("xgb gain",),
}
SHARED_TIMER = ("xgb_gain", "xgb_shap")
FREE_METHODS = ("spearman",)


# ------------------------------------------------------------------------ cost


def runtime_shares(root: str = None) -> pd.DataFrame:
    """Per-method share of wall clock, one row per archived run. No GPU, no database.

    Two denominators, because they answer different questions:

    * `run_total_s` — everything the run timed, including the window design, the
      correlation matrix, stability and the walk-forward. What a member costs the
      OPERATOR.
    * `rank_total_s` — the timed rankers only. What is actually on the table when you
      decide to drop one.
    """
    root = root or report.DEFAULT_REPORT_ROOT
    rows: List[Dict] = []
    for name in sorted(os.listdir(root)) if os.path.isdir(root) else []:
        path = os.path.join(root, name, "metadata.json")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as handle:
            meta = json.load(handle)
        timings = meta.get("results", {}).get("timings_seconds", {})
        if not timings:
            continue
        row = {
            "run": name,
            "target": meta["target"]["name"],
            "channels": meta["results"]["n_channels"],
            "run_total_s": float(sum(timings.values())),
            "rank_total_s": float(sum(
                v for k, v in timings.items()
                if any(x in k for x in ("mutual_info", "lasso", "permutation", "xgb gain"))
            )),
        }
        for method, keys in TIMER_KEYS.items():
            seconds = sum(v for k, v in timings.items() if any(x in k for x in keys))
            row[method] = seconds / 2 if method in SHARED_TIMER else seconds
        for method in FREE_METHODS:
            row[method] = 0.0
        rows.append(row)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    # ⚠️ A LEVEL target and a RETURN target are different cost regimes and must not be
    # averaged together: `lasso` is 95.6 % of the ranking phase on the first and 11.3 %
    # on the second, because a return collapses the solver to zero coefficients at once.
    frame["kind"] = np.where(
        frame["target"].str.startswith("close_adjust"), "level", "return"
    )
    return frame


def cost_table(shares: pd.DataFrame) -> pd.DataFrame:
    """`runtime_shares` → mean % of run and % of ranking, per method, per target kind."""
    if shares.empty:
        return pd.DataFrame()
    out = []
    for method in ALL_METHODS:
        row = {"ranker": method}
        for kind, part in list(shares.groupby("kind")) + [("all", shares)]:
            row[f"pct_of_run__{kind}"] = float(
                (part[method] / part["run_total_s"] * 100).mean()
            )
            row[f"pct_of_ranking__{kind}"] = float(
                (part[method] / part["rank_total_s"] * 100).mean()
            )
        out.append(row)
    return pd.DataFrame(out).set_index("ranker")


# ------------------------------------------------------------------- advantage


def measure_advantage(
    ticker: str = "VCB",
    pools: Sequence[str] = ("pool__basic", "pool__targets"),
    target: str = "return_5day",
    lookback: int = 20,
    horizon: int = 5,
    n_splits: int = 5,
    min_train: int = 500,
    device: str = "cuda",
    random_state: int = 18,
    k_values: Sequence[int] = K_VALUES,
    random_draws: int = RANDOM_DRAWS,
    panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """One selection, then every candidate selector's top-k scored out of sample.

    ⚠️ **The selection runs ONCE and every selector is scored from the same score
    matrix.** Re-running it per method would change the XGBoost draws and make the
    comparison a comparison of seeds. The scoring reuses `_splits`, `_impute`, `_xgb`
    and `_ic` — the selector's own code — so "the same folds, the same model" is an
    identity, not a promise.

    ⚠️ **The selector is pinned to `ALL_METHODS`.** This module measures the members,
    so it must not inherit whichever subset happens to be the default that week.

    Returns one row per (k, selector): `ic_mean`, `ic_sd_folds`, `advantage_pct`,
    `z_vs_random`, and `withdrawn` for a selector whose ranking was degenerate.
    """
    exclude = IDENTITY + [c for c in ALL_TARGETS if c != target]
    if panel is None:
        with UnifiedSchemaReader(ticker) as reader:
            panel = reader.join(list(pools))

    selector = FeatureSelector(
        panel=panel, target=target, exclude=exclude, max_features=None,
        corr_threshold=0.9, horizon=horizon, lookback=lookback,
        window_stats=windows.WINDOW_STATS, normalize="none", n_splits=n_splits,
        min_train=min_train, device=device, random_state=random_state,
        methods=ALL_METHODS,
    )

    X, y, _dropped, _coverage = selector._prepare()
    channels = list(X.columns)
    design = selector._design(X)
    nunique = design.nunique(dropna=True)
    design = design.drop(columns=nunique[nunique <= 1].index.tolist())
    y_w = y.loc[design.index]
    selector.device = gpu.resolve_device(
        selector.device_preference, n_features=design.shape[1]
    )
    design_corr = gpu.spearman_vector(design, y_w, device=selector.device)
    raw = windows.aggregate_to_channels(
        selector._score_methods(design, y_w, design_corr), channels
    )
    scores = selector._normalise(raw)
    ranks = scores.rank(ascending=False, method="min")
    ranks["ensemble"] = ranks[list(ALL_METHODS)].mean(axis=1)
    corr = gpu.spearman_matrix(X, device=selector.device)

    # ⚠️ THE WITHDRAWAL RULE, in TWO parts, because one was not enough (see `ZERO_TOL`).
    # A constant column ranks nothing and `sort_values` returns pool column order; a
    # column that is all-but-zero ranks the last bits of a float, which is worse, because
    # it does not LOOK degenerate — it produced an 81.25th-percentile score.
    dead = {
        m for m in ALL_METHODS
        if raw[m].nunique() <= 1 or bool((raw[m].abs() <= ZERO_TOL).all())
    }

    columns_of = selector._columns_of(channels, design.columns)
    splits = selector._splits(design.index)

    def out_of_sample(chosen: Sequence[str]) -> np.ndarray:
        ics = []
        for train_idx, test_idx in splits:
            cols = [c for ch in chosen for c in columns_of[ch]]
            X_tr, X_te = selector._impute(
                design.iloc[train_idx][cols], design.iloc[test_idx][cols]
            )
            model = selector._xgb().fit(X_tr, y_w.iloc[train_idx])
            ics.append(selector._ic(model.predict(X_te), y_w.iloc[test_idx]))
        return np.asarray(ics, dtype=float)

    def top_k(order_series: pd.Series, k: int) -> List[str]:
        order = order_series.sort_values().index.tolist()
        kept, _ = selector._prune(order, corr.loc[order, order])
        return kept[:k]

    candidates: Dict[str, pd.Series] = {m: ranks[m] for m in ALL_METHODS}
    candidates["ENSEMBLE (all)"] = ranks["ensemble"]
    for dropped in ALL_METHODS:
        rest = [m for m in ALL_METHODS if m != dropped]
        candidates[f"ensemble -{dropped}"] = ranks[rest].mean(axis=1)
    candidates["DEFAULT " + "+".join(METHODS)] = ranks[list(METHODS)].mean(axis=1)

    rng = np.random.default_rng(RANDOM_SEED)
    rows: List[Dict] = []
    for k in k_values:
        draws = np.array([
            out_of_sample(list(rng.choice(channels, size=min(k, len(channels)),
                                          replace=False))).mean()
            for _ in range(random_draws)
        ])
        rows.append({
            "k": k, "selector": "RANDOM control", "n": k, "ic_mean": float(draws.mean()),
            "ic_sd_folds": np.nan, "advantage_pct": 50.0, "z_vs_random": 0.0,
            "withdrawn": "", "random_p95": float(np.percentile(draws, 95)),
        })
        for label, order_series in candidates.items():
            chosen = top_k(order_series, k)
            ics = out_of_sample(chosen)
            rows.append({
                "k": k, "selector": label, "n": len(chosen),
                "ic_mean": float(ics.mean()), "ic_sd_folds": float(ics.std(ddof=1)),
                "advantage_pct": (
                    np.nan if label in dead else float((draws < ics.mean()).mean() * 100)
                ),
                "z_vs_random": float((ics.mean() - draws.mean()) / draws.std(ddof=1)),
                "withdrawn": (
                    "ranked nothing — every score identical or <= ZERO_TOL, so the "
                    "order is pool column order or float noise" if label in dead else ""
                ),
                "random_p95": float(np.percentile(draws, 95)),
            })
    frame = pd.DataFrame(rows)
    frame["target"] = target
    frame["timings_seconds"] = [dict(selector.timings)] * len(frame)
    return frame


# ------------------------------------------------------------------- scorecard


def scorecard(advantage: pd.DataFrame, cost: pd.DataFrame,
              kind: str = "level") -> pd.DataFrame:
    """The four questions in one table, one row per ranker.

    ⚠️ **`mean` IS OVER THE MEASURED CELLS AND NOTHING ELSE.** The first version of this
    computation wrote `min` into the frame before taking `mean(axis=1)`, so every mean
    averaged the cells *and its own minimum* and came out 2-12 points low. That is the
    reason this function exists instead of a scratchpad; `test_ranker_eval.py` pins it.
    """
    cells = advantage[advantage["selector"] != "RANDOM control"].pivot_table(
        index="selector", columns=["target", "k"], values="advantage_pct", dropna=False
    )
    per_cell_mean = cells.mean(axis=1)          # over the CELLS only
    per_cell_min = cells.min(axis=1)
    full = float(per_cell_mean.get("ENSEMBLE (all)", np.nan))

    rows = []
    for method in ALL_METHODS:
        without = f"ensemble -{method}"
        alone = float(per_cell_mean.get(method, np.nan))
        blend_without = float(per_cell_mean.get(without, np.nan))
        pct_ranking = (
            float(cost.loc[method, f"pct_of_ranking__{kind}"]) if len(cost) else np.nan
        )
        rows.append({
            "ranker": method,
            "in_default": method in METHODS,
            "advantage_pct": alone,
            "advantage_min_cell": float(per_cell_min.get(method, np.nan)),
            "blend_without": blend_without,
            # > 0 means the blend is WORSE without it, i.e. it is carrying weight.
            "necessity_delta": full - blend_without,
            "pct_of_run": (
                float(cost.loc[method, f"pct_of_run__{kind}"]) if len(cost) else np.nan
            ),
            "pct_of_ranking": pct_ranking,
            # ⚠️ `NaN`, never `inf`, for a free member: an undefined ratio is undefined.
            "edge_per_pct": (
                np.nan if not pct_ranking or np.isnan(alone)
                else max(alone - 50.0, 0.0) / pct_ranking
            ),
        })
    out = pd.DataFrame(rows).set_index("ranker")
    out.attrs["full_ensemble_pct"] = full
    return out


# ------------------------------------------------------------------------- CLI


def _write(root: str, advantage: pd.DataFrame, shares: pd.DataFrame,
           cost: pd.DataFrame, card: pd.DataFrame, execution: Dict) -> str:
    folder = os.path.join(root, runtime.folder_stamp())
    os.makedirs(folder, exist_ok=True)
    if not advantage.empty:
        advantage.drop(columns=["timings_seconds"], errors="ignore").to_csv(
            os.path.join(folder, "advantage.csv"), index=False
        )
    shares.to_csv(os.path.join(folder, "runtime_shares.csv"), index=False)
    cost.to_csv(os.path.join(folder, "cost.csv"))
    card.to_csv(os.path.join(folder, "scorecard.csv"))
    # ⚠️ `evaluation.json`, NOT `metadata.json` — see the module docstring. A
    # `metadata.json` here would make `contract.run_folders` call this a selection run.
    with open(os.path.join(folder, "evaluation.json"), "w", encoding="utf-8") as handle:
        json.dump({
            "generated_at": runtime.iso(),
            "execution": execution,
            "full_ensemble_pct": card.attrs.get("full_ensemble_pct"),
            "default_methods": list(METHODS),
            "all_methods": list(ALL_METHODS),
            "random_draws": RANDOM_DRAWS,
            "k_values": list(K_VALUES),
        }, handle, indent=2, ensure_ascii=False)
    return folder


def main(argv: Optional[Sequence[str]] = None) -> pd.DataFrame:
    parser = argparse.ArgumentParser(
        prog="python -m feature_selection.ranker_eval",
        description="Measure what each ranker is worth: advantage, cost, necessity.",
    )
    parser.add_argument("--ticker", default="VCB")
    parser.add_argument("--pools", default="pool__basic")
    parser.add_argument("--targets", default="return_5day,return_rel_5day")
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--k", default=",".join(str(k) for k in K_VALUES))
    parser.add_argument("--random-draws", type=int, default=RANDOM_DRAWS)
    parser.add_argument("--root", default=DEFAULT_ROOT)
    parser.add_argument(
        "--kind", default="level", choices=["level", "return", "all"],
        help="which cost regime the scorecard quotes. WARNING: lasso is 95 percent of "
             "the ranking phase on a level target and 11 percent on a return one",
    )
    parser.add_argument(
        "--cost-only", action="store_true",
        help="runtime shares from the archived runs only. No GPU, no database",
    )
    parser.add_argument("--apply", action="store_true", help="run the measurement")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))

    targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    pools = [p.strip() for p in args.pools.split(",") if p.strip()]
    if "pool__targets" not in pools:
        pools.append("pool__targets")

    with runtime.RunTimer(
        f"feature_selection.ranker_eval  {args.ticker} / {'+'.join(pools)} -> "
        f"{','.join(targets)}"
        f"{'  --cost-only' if args.cost_only else ('  --apply' if args.apply else '  (plan only)')}",
        device=None if args.cost_only else args.device,
        show_gpu=not args.cost_only,
    ) as timer:
        shares = runtime_shares()
        cost = cost_table(shares)
        pd.set_option("display.width", 220)
        pd.set_option("display.max_columns", 30)

        print(f"\n{'=' * 78}\nCOST — mean share of wall clock over "
              f"{len(shares)} archived run(s)")
        if len(shares):
            print(cost.round(1).to_string())
            print("\n⚠️ `spearman` is 0.0 by construction: target_corr is computed for "
                  "the report's SIGN whether or not it is in the ensemble.")
            print("⚠️ `xgb_gain` and `xgb_shap` share ONE fit and one timer; each is "
                  "shown as half of it, and neither alone saves all of it.")

        advantage = pd.DataFrame()
        if args.cost_only:
            print("\n--cost-only: the advantage measurement needs a GPU and a database.")
        elif not args.apply:
            print(f"\nplan: {len(targets)} target(s) x {len(args.k.split(','))} width(s), "
                  f"{args.random_draws} random draws each, on "
                  f"{args.ticker}/{'+'.join(pools)}. Pass --apply to run it.")
        else:
            frames = []
            for target in targets:
                print(f"\n{'=' * 78}\nADVANTAGE — {target}")
                frames.append(measure_advantage(
                    ticker=args.ticker, pools=pools, target=target,
                    lookback=args.lookback, horizon=args.horizon, device=args.device,
                    k_values=[int(k) for k in args.k.split(",") if k.strip()],
                    random_draws=args.random_draws,
                ))
            advantage = pd.concat(frames, ignore_index=True)

        card = scorecard(advantage, cost, kind=args.kind) if len(advantage) else pd.DataFrame()
        if len(card):
            print(f"\n{'=' * 78}\nSCORECARD  (full ensemble = "
                  f"{card.attrs['full_ensemble_pct']:.1f}th percentile; chance = 50; "
                  f"cost regime = {args.kind})")
            print(card.round(2).to_string())
            print("\nnecessity_delta > 0  = the blend is WORSE without it")
            print("advantage_pct NaN    = WITHDRAWN, the ranker ranked nothing")
            print("\n⚠️ The control is random-k, so this measures choosing better than "
                  "CHANCE. It is not evidence of predictive skill — see CLAUDE.md §2.")

        if args.apply or args.cost_only:
            folder = _write(args.root, advantage, shares, cost, card, timer.summary())
            print(f"\nwrote {folder}")
    return card


if __name__ == "__main__":
    main()
