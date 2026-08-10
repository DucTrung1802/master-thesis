# src\feature_selection\run.py
"""Run ONE feature selection headlessly and archive it — the notebook, as a command.

    python -m feature_selection.run --pools pool__basic --null-draws 20
    python -m feature_selection.run --ticker VCB --pools pool__basic,pool__ta \
                                    --target return_5day --lookback 20 --horizon 5
    python -m feature_selection.run --pools pool__basic --root reports/feature_selection_basic

`RUN__feature_importance_report.ipynb` remains the interactive entry point and this
module reproduces it exactly — same `FeatureSelector`, same `evaluation.null_distribution`,
same `report.write_report`. Nothing is re-implemented here; this file is the parameter
cell plus an `argparse`.

## ⚠️ Why this exists at all

`pipeline.Stage("selection")` is `manual=True` because a selection is hours of GPU and a
judgement about which pools to join, so `--apply` can only refresh shortlists from runs
that already exist (issue **PIP-1**). That is still true of the wide pools —
`basic+economy_usa` spends 12,255 s on `permutation` per pass at 1,458 channels, and one
20-draw null is ~68 CPU-hours (issue **EVD-1**). It is NOT true of `pool__basic`: 27
channels is minutes, which is why §10b's null was the one affordable measurement in the
archive. A prototype that has to run end to end needs the cheap case to be scriptable.

## ⚠️ `--null-draws 0` writes `"null": null`, and that is a recorded absence

An absent null is recorded as absent, never omitted and never implied to be a pass
(CONTEXT §10). The shortlist then carries `evidence=no_null`, `final_features` copies
that into the table `COMMENT`, `train_test_creator` copies it into `metadata.json` and
`model` copies it into the run's `lineage`. **The default here is 20 draws**, unlike the
notebook's `RUN_NULL = False`, because a scripted run has no human at the keyboard to
read the warning it would otherwise print.

## ⚠️ `--root` is what keeps a run out of an existing table's grouping

`final_features.plan_from_reports` groups EVERY run under one root by
`(schema, target, setup)`. A new `pool__basic` run at `d=20, h=5` shares all three with
the 19 runs behind `unified_schema_vcb.return_5day__final__d20_h5`, so archiving it into
the default root would silently widen that table's channel set, move its fingerprint and
make every dataset and run below it stale (the STL-1 domino). A separate root is what
makes a scoped experiment a separate experiment.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import List, Optional, Sequence

import pandas as pd

from feature_selection import cross_sectional as cs
from feature_selection import evaluation, outstanding, report, windows
from feature_selection.cross_sectional import CrossSectionalSelector, read_universe_panel
from feature_selection.selector import FeatureSelector
from feature_selection.unified_reader import UnifiedSchemaReader, unified_schema_name

# ⚠️ Identity and taxonomy columns are never candidates. On a single-ticker panel they
# are constant and would be dropped anyway; naming them keeps that a decision rather
# than an accident, and keeps this correct for a multi-ticker panel. Copied verbatim
# from the notebook's parameter cell — the two must not drift.
IDENTITY = [
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
]

# Every label in `pool__targets`. Whichever is not the target is excluded — a second
# label as a feature is the most direct leak available.
ALL_TARGETS = ["return_5day", "return_10day", "return_rel_5day", "return_rel_10day"]

# The label table. Always joined, never named in the run id — the target is already the
# last segment of the folder name (`report.default_run_id`).
TARGETS_TABLE = "pool__targets"

# The seed `evaluation.null_distribution` draws its shuffles from. Fixed and separate
# from `--random-state`: the null must not move when the selector's seed does, or the
# bar and the number it judges stop being comparable.
NULL_SEED = 7


def run_selection(
    ticker: str = "VCB",
    pools: Sequence[str] = ("pool__basic",),
    target: str = "return_5day",
    lookback: int = 20,
    horizon: int = 5,
    normalize: str = "none",
    max_features: Optional[int] = None,
    corr_threshold: float = 0.9,
    n_splits: int = 5,
    min_train: int = 500,
    device: str = "cuda",
    random_state: int = 18,
    stability: bool = True,
    null_draws: int = 20,
    holdout_start: Optional[str] = None,
    root: str = report.DEFAULT_REPORT_ROOT,
    notes: str = "",
    top: int = 30,
    feature_normalize: str = "cs_rank",
    min_ic_width: int = 5,
):
    """Read the panel, rank it, measure the bar, and archive one run folder.

    Returns the `report.write_report` result. `max_features=None` is the ONLY default
    now and means the per-run measured cut (`selection_cut`) decides the width. The flat
    `max_features=12` has not determined a shortlist since issue STL-1 and was removed
    from the last live default (`RUN__feature_importance_report.ipynb`) on 2026-08-10.

    ⚠️ **The 20 runs under `reports/feature_selection/` still RECORD `max_features: 12`
    in their `metadata.json` and that number described their `kept` column, not their
    shortlist.** Their `outstanding.csv` was regenerated under the measured cut and holds
    10–236 channels. The two runs under `reports/feature_selection_basic/` (2026-08-10)
    are the first archived uncapped ones and record `null`. Read the width off
    `outstanding.csv`, never off `setup.max_features`.
    """
    pools = list(pools)
    if TARGETS_TABLE not in pools:
        pools = pools + [TARGETS_TABLE]

    # ⚠️ **THE TARGET DECIDES THE PROCEDURE, not a flag.** A `cs_` prefix marks a
    # CROSS-SECTIONAL target — a rank within a date across the schema's universe — and
    # that changes the panel reader, the selector, the CV (sessions, not rows) and the
    # null (whole dates, not rows). Inferring it from the target name is what stops a
    # 20-ticker panel being run through the single-series path, which would pool
    # cross-sectional with time-series variation and count 20 banks on a date as 20
    # observations (issue **PNL-1**, at the selection stage instead of the scoring one).
    cross = target.startswith("cs_")

    started = time.time()
    if cross:
        # `read_universe_panel` joins `pool__basic ⋈ pool__targets` in SQL and adds the
        # `cs_rank_{h}day` columns. ⚠️ It reads those two pools ONLY, so a
        # cross-sectional run is `pool__basic`-scoped by construction; `--pools` is
        # accepted and recorded but cannot widen it.
        if set(pools) - {"pool__basic", TARGETS_TABLE}:
            raise ValueError(
                f"a cross-sectional target reads pool__basic ⋈ {TARGETS_TABLE} only; "
                f"got pools={pools}. Widening needs read_universe_panel to join more."
            )
        panel = read_universe_panel(
            horizons=(horizon,), min_width=min_ic_width, schema_ticker=ticker
        )
        schema_name = unified_schema_name(ticker)
        with UnifiedSchemaReader(ticker) as reader:
            database = reader.database
        join_log = [
            {
                "table": TARGETS_TABLE,
                "join_keys": "date, exchange, ticker",
                "how": "inner",
                "note": "read_universe_panel, server-side; cs_rank_* derived after",
            }
        ]
    else:
        with UnifiedSchemaReader(ticker) as reader:
            panel = reader.join(pools)
            join_log = reader.join_log
            schema_name, database = reader.schema, reader.database

    n_tickers = panel["ticker"].nunique() if "ticker" in panel.columns else 1
    print(
        f"{schema_name}: {panel.shape[0]:,} rows x {panel.shape[1]} columns "
        f"({panel['date'].min():%Y-%m-%d} -> {panel['date'].max():%Y-%m-%d}), "
        f"{n_tickers} ticker(s), grain={'panel' if cross else 'series'}"
    )
    if target not in panel.columns:
        raise ValueError(f"target {target!r} is not in the joined panel.")
    if cross and n_tickers < 2:
        raise ValueError(
            f"target {target!r} is cross-sectional but the panel holds {n_tickers} "
            f"ticker(s). A rank within a date needs a cross-section to rank across."
        )

    exclude = IDENTITY + [c for c in ALL_TARGETS if c != target]

    def build(frame: pd.DataFrame, holdout: Optional[str]) -> FeatureSelector:
        """One selector, built the same way for the run and for every draw.

        ⚠️ The null must be the SAME procedure as the thing it judges, or the bar is
        measuring a different pipeline (CONTEXT §9d). The only differences permitted
        are `stability` (a diagnostic, not part of the score) and the holdout, which
        the draws do not score.

        ⚠️ `CrossSectionalSelector` RE-IMPLEMENTS NO RANKER — it overrides six hooks on
        `FeatureSelector` and inherits the six rankers, the ensemble, the correlation
        prune and the holdout protocol unchanged. That is deliberate: the two studies
        have to be the same procedure on differently-shaped panels, or §9's numbers
        cannot be set beside §6's.
        """
        common = dict(
            panel=frame,
            target=target,
            exclude=exclude,
            max_features=max_features,
            corr_threshold=corr_threshold,
            horizon=horizon,
            lookback=lookback,
            window_stats=windows.WINDOW_STATS,
            normalize=normalize,
            n_splits=n_splits,
            min_train=min_train,
            device=device,
            random_state=random_state,
            holdout_start=holdout,
        )
        if cross:
            # ⚠️ `min_train` and `n_splits` count SESSIONS here, not rows.
            return CrossSectionalSelector(
                feature_normalize=feature_normalize,
                min_ic_width=min_ic_width,
                **common,
            )
        return FeatureSelector(**common)

    selector = build(panel, holdout_start)
    result = selector.run(stability=stability)
    summary = evaluation.ic_summary(result.validation, horizon)
    print(
        f"kept {len(result.kept)} channels; ic_mean {summary['ic_mean']:+.4f}, "
        f"trend {summary['ic_trend_per_fold']:+.4f}"
    )
    if result.dead_methods:
        # ⚠️ `WARNING:` and not `⚠️`. A Windows console and a redirected stdout are both
        # cp1252, which has no code point for U+26A0 — the print itself raises
        # UnicodeEncodeError (CLAUDE.md §5 rule 18). This branch is not hypothetical:
        # on `pool__basic`, `LassoCV` zeroes every coefficient, so `lasso` contributes a
        # constant to the blend and `dead_methods` names it (CONTEXT §4).
        print(f"WARNING: dead methods (separated nothing): {result.dead_methods}")

    null = None
    if null_draws > 0:
        print(f"null: {null_draws} block-shuffled draws, selection re-run inside each...")
        common = dict(
            panel=panel,
            target=target,
            factory=lambda frame: build(frame, None).run(stability=False),
            observed=summary["ic_mean"],
            horizon=horizon,
            lookback=lookback,
            n_draws=null_draws,
            seed=NULL_SEED,
            label=f"{schema_name} / {target}",
        )
        # ⚠️ **A PANEL NEEDS A PANEL-AWARE SHUFFLE.** `evaluation.block_shuffle` permutes
        # ROWS, which on an N × T panel tears each date's cross-section apart and
        # destroys the very structure the target is computed within — the null then
        # measures a different procedure and comes out far too easy.
        # `cross_sectional_null`'s `date_block` mode pivots the label to `date × ticker`
        # and permutes blocks of DATES, so each stock keeps its own labels and the
        # autocorrelation survives (§9d). It is also the conservative choice: on a
        # ragged panel a draw loses rows, which biases toward a false positive.
        try:
            null = (cs.cross_sectional_null if cross else evaluation.null_distribution)(
                **common
            )
        except Exception as error:  # noqa: BLE001 — see below
            # ⚠️ **A FAILED NULL MUST NOT DISCARD THE OBSERVED RUN.** The report is
            # written after the null because it embeds it, so anything raising here
            # threw away a completed selection — measured twice on 2026-08-10, once on
            # a `KeyError` in a summary f-string and once on a wrong function name, each
            # costing the whole run. The selection is the expensive artefact; the null
            # is an addition to it. An absent null is recorded as ABSENT, which is the
            # rule §10 already states — `evidence=no_null`, never an implied pass.
            print(f"WARNING: the null FAILED and was not computed: {error}")
            print("WARNING: the report will record evidence=no_null. Re-run the null.")
            null = None
    else:
        print("WARNING: --null-draws 0: this run will record that NO bar was computed.")

    holdout = selector.score_holdout(result) if holdout_start else None

    # ⚠️ **THE REPORT IS WRITTEN BEFORE ANYTHING IS PRINTED ABOUT IT.** The null is the
    # expensive artefact — 20 draws re-run the whole selection — and the first version of
    # this function formatted a summary line first. It read `null.summary()["bar"]`, the
    # Series key is `null_p95_BAR`, and a `KeyError` in that f-string discarded twenty
    # completed draws. Same class of loss as CLAUDE.md §5 rule 20: finish the unit, put
    # it on disk, and only then describe it.
    written = report.write_report(
        result,
        selector=selector,
        panel=panel,
        schema=schema_name,
        tables=pools,
        database=database,
        join_log=join_log,
        null=null,
        holdout=holdout,
        notes=notes,
        top=top,
        root=root,
    )
    print(f"\nreport: {written.path}  ({time.time() - started:.1f}s)")

    if null is not None:
        # Read off the ATTRIBUTES, not the summary Series. The attributes are the
        # object's interface; the Series is a display format whose key names have
        # already changed once.
        print(
            f"observed {null.observed:+.4f} | null mean {null.draws.mean():+.4f} "
            f"| p95 bar {null.bar:+.4f} | null MAX {null.draws.max():+.4f} "
            f"| z {null.z:+.2f} | p {null.p_value:.4f} "
            f"| {'CLEARS' if null.clears else 'FAILS'}"
        )
        # ⚠️ `clears_bar` is the wrong summary whenever the null MAX exceeds the
        # observed — one shuffled draw beating the real data is the fact that matters
        # and a boolean hides it (CLAUDE.md §5 rule 3).
        if null.draws.max() >= null.observed:
            print(
                f"WARNING: a shuffled draw reached {null.draws.max():+.4f}, at or above "
                f"the observed {null.observed:+.4f} - quote the max beside the bar."
            )

    # The shortlist is what every downstream stage reads, so it is written here rather
    # than left for a second command — a report folder without an `outstanding.csv` is
    # invisible to `final_features.plan_from_reports`.
    shortlists = outstanding.main(root=root)
    for name, table in shortlists.items():
        if name == os.path.basename(written.path):
            print(
                f"shortlist: {len(table)} channels, evidence="
                f"{table['evidence'].iloc[0]}"
            )
    return written


def main(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(
        prog="python -m feature_selection.run",
        description="Run one feature selection headlessly and archive it.",
    )
    parser.add_argument("--ticker", default="VCB", help="unified_schema_<ticker>")
    parser.add_argument(
        "--pools",
        default="pool__basic",
        help="comma-separated pool tables; pool__targets is joined automatically",
    )
    parser.add_argument("--target", default="return_5day")
    parser.add_argument("--lookback", type=int, default=20, help="d, in sessions")
    parser.add_argument("--horizon", type=int, default=5, help="h; must match the target")
    parser.add_argument(
        "--normalize", default="none", choices=["none", "zscore", "window_relative"]
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=None,
        help="cap on kept channels. Default None = the per-run measured cut (STL-1)",
    )
    parser.add_argument("--corr-threshold", type=float, default=0.9)
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--min-train", type=int, default=500)
    parser.add_argument(
        "--device",
        default="cuda",
        help="cuda | cpu | auto. Default cuda = EVERY ranker on the GPU (raises if "
             "there is none). auto keeps narrow pools on the host, which left 14 of "
             "19 country pools on the CPU. Part of the setup - see gpu.py section 1",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=18,
        help="the SELECTOR's seed. Separate from NULL_SEED on purpose - see this "
             "module's NULL_SEED comment",
    )
    parser.add_argument("--no-stability", action="store_true")
    # ⚠️ ASCII only in `help=` — argparse PRINTS these, and a Windows console is cp1252,
    # so a `⚠️` here turns `--help` itself into a UnicodeEncodeError traceback
    # (CLAUDE.md §5 rule 18). The warnings live in the module docstring instead.
    parser.add_argument(
        "--null-draws",
        type=int,
        default=20,
        help="draws in the block-shuffled null. WARNING: 0 records evidence=no_null",
    )
    parser.add_argument("--holdout-start", default=None, help="e.g. 2024-06-01")
    parser.add_argument(
        "--root",
        default=report.DEFAULT_REPORT_ROOT,
        help="report root. WARNING: a run here joins every other run here when "
        "final_features groups by (schema, target, setup)",
    )
    parser.add_argument("--notes", default="")
    parser.add_argument("--top", type=int, default=30, help="channels per figure")
    # Cross-sectional only; ignored when the target has no `cs_` prefix.
    parser.add_argument(
        "--feature-normalize",
        default="cs_rank",
        choices=["none", "cs_rank"],
        help="cross-sectional runs only: rank features within each date, or not",
    )
    parser.add_argument(
        "--min-ic-width",
        type=int,
        default=5,
        help="cross-sectional runs only: dates narrower than this contribute no IC",
    )
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))

    return run_selection(
        ticker=args.ticker,
        pools=[p.strip() for p in args.pools.split(",") if p.strip()],
        target=args.target,
        lookback=args.lookback,
        horizon=args.horizon,
        normalize=args.normalize,
        max_features=args.max_features,
        corr_threshold=args.corr_threshold,
        n_splits=args.n_splits,
        min_train=args.min_train,
        device=args.device,
        random_state=args.random_state,
        stability=not args.no_stability,
        null_draws=args.null_draws,
        holdout_start=args.holdout_start,
        root=args.root,
        notes=args.notes,
        top=args.top,
        feature_normalize=args.feature_normalize,
        min_ic_width=args.min_ic_width,
    )


if __name__ == "__main__":
    main()
