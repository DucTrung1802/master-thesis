"""The event chain's stages, in order, and the CLI that runs them.

    python -m event_chain                              # the plan: what exists, what is stale
    python -m event_chain --apply                      # run every stale stage
    python -m event_chain --apply --stages train,report
    python -m event_chain --apply --stages select --pools pool__basic,pool__ta
    python -m event_chain --setup tabular --apply     # d=1, all channels, linear kinds + ensemble

⚠️ **NOTHING HERE DEFINES A LABEL, A FEATURE, A SPLIT OR A METRIC.** Each stage calls the
package that owns it — `feature_selection.run`, `final_features.builder`,
`train_test_creator.dataset`, `model.<arch>.train` — so the event chain cannot drift from
the regression chain it shares those rules with. What it adds is the ORDER, the holdout
that keeps the selection off the val and test rows, and the event report.

⚠️ **THE TWO LEAKS THIS ORDER EXISTS TO PREVENT.**

| leak | where it would come from | what stops it |
|---|---|---|
| feature selection reads the test period | a selection run over the whole history | `holdout_start` = the dataset's VAL start, and `dataset` REFUSES a table whose val start falls before it |
| a train label reaches into val/test | an un-purged split | `train_test_creator`'s `d + h - 1` purge, which this chain never switches off |
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

import pandas as pd
import yaml

from event_chain import config as C
from utils import event_target

STAGES = ("select", "final", "dataset", "train", "report")


def _utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


@dataclass
class EventChain:
    ticker: str = C.TICKER
    event: event_target.EventTarget = field(default_factory=lambda: event_target.DEFAULT_EVENT)
    lookback: int = C.LOOKBACK
    root: str = C.REPORT_ROOT
    pools: Sequence[str] = C.POOLS
    null_draws: int = C.NULL_DRAWS
    device: str = "cuda"
    # A NARROW table built from part of the root: `scope` names it
    # (`up_5pct_5day__final__d20_h5__<scope>`), `scope_pools` says which pools it unions.
    scope: Optional[str] = None
    scope_pools: Optional[Sequence[str]] = None
    # `final`'s policy on pools whose selection FAILED its null — recorded by every trial.
    keep_failed: bool = False
    # ⚠️ THE SETUP (`config.SETUPS`) decides d, the pools, the model grid, the table's scope,
    # `final_features`' channel mode, the auxiliary targets and the fixed ensembles — one
    # name, so a trial can say which experiment it is. `window` is every trial before
    # 2026-09-17; `tabular` is the d=1 linear chain.
    setup: str = "window"
    channels: str = "shortlist"
    models: Sequence = C.MODELS
    aux_targets: Sequence[str] = ()
    ensembles: Dict = field(default_factory=dict)

    @classmethod
    def from_setup(cls, name: str = "window", **overrides) -> "EventChain":
        if name not in C.SETUPS:
            raise ValueError(f"unknown setup {name!r}; have {sorted(C.SETUPS)}")
        spec = dict(C.SETUPS[name])
        event = overrides.get("event") or event_target.DEFAULT_EVENT
        spec["aux_targets"] = tuple(a.format(h=int(event.horizon)) for a in spec["aux_targets"])
        spec.update({k: v for k, v in overrides.items() if v is not None})
        return cls(setup=name, **spec)

    # ------------------------------------------------------------------ names
    @property
    def horizon(self) -> int:
        return int(self.event.horizon)

    @property
    def schema(self) -> str:
        return f"unified_schema_{self.ticker.lower()}"

    @property
    def table(self) -> str:
        from final_features.builder import table_name

        return table_name(self.event.column, self.lookback, self.horizon, self.scope)

    def creator(self):
        from train_test_creator.dataset import TrainTestCreator

        return TrainTestCreator(
            ticker=self.ticker.lower(), table=self.table, train_ratio=C.TRAIN_RATIO,
            val_ratio=C.VAL_RATIO, report_root=self.root, aux_targets=tuple(self.aux_targets),
        )

    @property
    def output_dir(self) -> str:
        return os.path.join(C.OUTPUT_ROOT, f"{self.ticker.lower()}__{self.table}")

    # ----------------------------------------------------------------- reads
    def labelled_dates(self) -> pd.Series:
        from feature_selection.unified_reader import UnifiedSchemaReader

        with UnifiedSchemaReader(self.ticker) as reader:
            labels = reader.read("pool__targets", order_by=("date",))
        if self.event.column not in labels.columns:
            raise ValueError(
                f"{self.schema}.pool__targets has no {self.event.column!r}. The event is "
                f"defined in utils/event_target.py; re-materialise it with\n  dagster "
                f"asset materialize -f src/orchestration/definitions.py --select "
                f"\"unified/pool__targets\" --partition {self.ticker.upper()}"
            )
        labelled = labels.dropna(subset=[self.event.column])
        return pd.to_datetime(labelled["date"]).reset_index(drop=True), labelled

    def holdout_start(self) -> str:
        """The VAL start `train_test_creator` will cut, from the label's own calendar."""
        dates, _ = self.labelled_dates()
        bounds = self.creator()._bounds(dates)
        return str(bounds.train_end_date.date())

    def runs(self, before: Optional[str] = None) -> pd.DataFrame:
        """Every selection run under the chain's root for THIS target and window.

        `before` (`YYYY-mm-dd HH:MM:SS`) keeps only runs whose folder timestamp is earlier —
        how a trial logged after the fact states the selection it actually used.
        """
        rows = []
        for path in sorted(glob.glob(os.path.join(self.root, "*", "metadata.json"))):
            with open(path, encoding="utf-8") as fh:
                meta = json.load(fh)
            setup = meta.get("setup") or {}
            inputs = meta.get("input") or {}
            null = meta.get("null") or {}
            rows.append(
                {
                    "run_id": os.path.basename(os.path.dirname(path)),
                    "target": setup.get("target"),
                    "lookback_d": setup.get("lookback_d"),
                    "horizon_h": setup.get("horizon_h"),
                    "tables": ",".join(
                        t for t in (inputs.get("tables") or []) if t != "pool__targets"
                    ),
                    "holdout_start": setup.get("holdout_start"),
                    "channels": setup.get("channels"),
                    "kept": setup.get("kept"),
                    "ic": null.get("observed_ic"),
                    "null_bar": null.get("null_p95_BAR"),
                    "null_max": null.get("null_max"),
                    "z": null.get("z_vs_null"),
                    "clears": null.get("clears_bar"),
                    "dir": os.path.dirname(path),
                }
            )
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["started_at"] = pd.to_datetime(
            frame["run_id"].str.slice(0, 17), format="%Y-%m-%d_%H%M%S", errors="coerce"
        )
        frame = frame[
            (frame["target"] == self.event.column)
            & (frame["lookback_d"] == self.lookback)
            & (frame["horizon_h"] == self.horizon)
        ]
        if before is not None:
            frame = frame[frame["started_at"] < pd.Timestamp(before)]
        return frame.reset_index(drop=True)

    # ---------------------------------------------------------------- stages
    def select(self, pools: Optional[Sequence[str]] = None, force: bool = False) -> None:
        from feature_selection.run import run_selection
        from feature_selection.unified_reader import UnifiedSchemaReader

        holdout = self.holdout_start()
        done = set(self.runs().get("tables", pd.Series(dtype=str)))
        with UnifiedSchemaReader(self.ticker) as reader:
            present = set(reader.tables())
        pools = list(pools or self.pools)
        for i, pool in enumerate(pools, 1):
            head = f"[select {i}/{len(pools)}] {pool}"
            if pool not in present:
                print(f"{head}: SKIPPED — {self.schema}.{pool} does not exist")
                continue
            if pool in done and not force:
                print(f"{head}: already run under {self.root}")
                continue
            print(f"\n{head}: holdout from {holdout}, {self.null_draws} null draws")
            run_selection(
                ticker=self.ticker, pools=[pool], target=self.event.column,
                lookback=self.lookback, horizon=self.horizon, null_draws=self.null_draws,
                holdout_start=holdout, root=self.root, device=self.device,
                notes=f"event_chain: {self.event.describe()}",
            )

    def final(self, keep_failed: Optional[bool] = None) -> None:
        from final_features import builder

        if keep_failed is not None:
            self.keep_failed = keep_failed
        keep_failed = self.keep_failed

        # ⚠️ A pool whose selection FAILED its null offers no channel by default: its
        # shortlist is the ranking of noise, recorded in the report and kept out of the model.
        exclude = () if keep_failed else ("failed_null",)
        plans = [
            p for p in builder.plan_from_reports(
                self.root, scope=self.scope, exclude_evidence=exclude,
                include_tables=self.scope_pools,
            )
            if p.schema == self.schema and p.table == self.table
        ]
        # ⚠️ The root also holds the OTHER setup's runs (d=20 beside d=1): they are a separate
        # plan, and `tables=` keeps this build to the chain's own table.
        pools = set(self.pools)
        missing = sorted(pools - set(plans[0].columns_by_table)) if plans else []
        if not plans:
            raise ValueError(f"no selection run under {self.root} plans {self.schema}.{self.table}")
        plan = plans[0]
        print(f"{self.schema}.{self.table}: {len(plan.columns_by_table)} pool(s) "
              f"({self.channels} channels), evidence {plan.evidence}")
        for table in plan.source_tables:
            print(f"    {len(plan.columns_by_table[table]):>4}  {table}  (shortlist)")
        if missing:
            print(f"    not in the table (no run, or its selection failed the null): {missing}")
        result = builder.build_all(
            root=self.root, apply=True, replace=True, scope=self.scope,
            exclude_evidence=exclude, include_tables=self.scope_pools,
            channels=self.channels, tables=[self.table],
        )
        print(result.to_string(index=False))

    def dataset(self) -> str:
        creator = self.creator()
        data = creator.build()
        holdout = pd.Timestamp(self.holdout_start())
        val_start = data.bounds.train_end_date
        # ⚠️ THE CHECK THAT MAKES THE HOLDOUT MEAN ANYTHING. The selection ranked rows
        # strictly before `holdout`; a dataset whose val split starts EARLIER would put
        # rows the ranking read into val.
        if val_start < holdout:
            raise ValueError(
                f"the dataset's val split starts {val_start.date()} but the selection "
                f"read everything before {holdout.date()} — re-run `select` with the "
                f"table's own calendar."
            )
        directory = creator.save(data, replace=True)
        rates = {s: float(data.y[s].mean()) for s in ("train", "val", "test")}
        print(f"dataset {data.name}: features {data.n_features}, samples "
              + ", ".join(f"{s} {len(data.y[s])} (base {rates[s]:.3f})" for s in rates))
        print(f"saved to {directory}")
        return directory

    def _config(self, package: str, variant: str, model: Dict, extra: Dict, meta: Dict) -> Dict:
        from model.common.data import load_dataset

        run_name = f"{package}{variant}__{self.ticker.lower()}__{self.table}"
        name = self.creator().name
        dataset = load_dataset(name)
        model = dict(model)
        if package == "event_linear" and self.aux_targets:
            model.setdefault("aux_target", self.aux_targets[0])
        if model.get("kind") == "logistic_channel":
            columns = (dataset.meta or {}).get("features", {}).get("feature_columns", [])
            chosen = next((c for c in C.BASELINE_CHANNELS if c in columns), None)
            if chosen is None:
                return {}
            model["target_channel"] = chosen
        config = {
            "run_name": run_name,
            "dataset": name,
            "dataset_hash": dataset.hash,
            "task": "classification",
            "lookback": int(dataset.lookback),
            "n_features": int(dataset.n_features),
            "model": model,
            "null_draws": 200,
            "seed": 42,
            "device": "auto" if package not in ("baseline", "gbt", "forest", "event_linear", "event_panel") else "cpu",
            **extra,
        }
        return config

    def train(self, force: bool = False, only: Optional[Sequence[str]] = None) -> None:
        import importlib

        from model.common.data import load_dataset
        from model.common.engine import RUNS_DIR

        dataset = load_dataset(self.creator().name)
        index = os.path.join(RUNS_DIR, "index.csv")
        done = pd.read_csv(index) if os.path.exists(index) else pd.DataFrame()
        for i, (package, variant, model, extra) in enumerate(self.models, 1):
            config = self._config(package, variant, model, extra, {})
            if not config:
                print(f"[train {i}/{len(self.models)}] {package}{variant}: no candidate channel, skipped")
                continue
            if only and not any(o in config["run_name"] for o in only):
                continue
            already = (
                not done.empty
                and ((done["run_id"].astype(str).str.startswith(config["run_name"] + "__"))
                     & (done["dataset_hash"].astype(str) == dataset.hash)).any()
            )
            if already and not force:
                print(f"[train {i}/{len(self.models)}] {config['run_name']}: already trained on {dataset.hash}")
                continue
            path = os.path.join(C.REPO_ROOT, "src", "model", package, "configs", f"{config['run_name']}.yaml")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(
                    f"# Written by `python -m event_chain` for {self.event.column} "
                    f"({self.event.describe()}).\n# The grid is event_chain/config.py SETUPS[{self.setup!r}].\n"
                )
                yaml.safe_dump(config, fh, sort_keys=False, allow_unicode=True)
            print(f"\n[train {i}/{len(self.models)}] {config['run_name']}")
            binding = importlib.import_module(f"model.{package}.train")
            binding.train(config)

    def report(self, walkforward: bool = True, runs_dir: Optional[str] = None) -> Dict:
        """Score, write the report, and return the numbers (see `report.write`)."""
        from event_chain import report

        return report.write(self, walkforward=walkforward, runs_dir=runs_dir)

    # ------------------------------------------------------------------ plan
    def status(self) -> None:
        from feature_selection.unified_reader import UnifiedSchemaReader

        print(f"{'=' * 78}\nevent   {self.event.column} — {self.event.describe()}")
        print(f"setup   {self.setup}   ticker {self.ticker}   d={self.lookback}  h={self.horizon}   "
              f"channels {self.channels}   root {self.root}")
        try:
            dates, labelled = self.labelled_dates()
            print(f"label   {len(dates)} labelled sessions {dates.iloc[0].date()} -> "
                  f"{dates.iloc[-1].date()}, base rate {labelled[self.event.column].mean():.3f}")
            print(f"holdout selection reads rows before {self.holdout_start()}")
        except Exception as error:  # noqa: BLE001 — printed, the plan continues
            print(f"label   MISSING: {error}")
        with UnifiedSchemaReader(self.ticker) as reader:
            present = set(reader.tables())
        runs = self.runs()
        for pool in self.pools:
            ran = (not runs.empty) and (runs["tables"] == pool).any()
            print(f"  {'ran ' if ran else 'TODO'} select  {pool}{'' if pool in present else '  (pool absent)'}")
        print(f"  {'ok  ' if self.table in present else 'TODO'} final   {self.schema}.{self.table}")
        from train_test_creator.dataset import DEFAULT_OUTPUT_ROOT

        exists = os.path.isdir(os.path.join(DEFAULT_OUTPUT_ROOT, self.creator().name))
        print(f"  {'ok  ' if exists else 'TODO'} dataset {self.creator().name}")
        print(f"  ---- train   {len(self.models)} model configs (event_chain/config.py SETUPS[{self.setup!r}])")
        for name, members in (self.ensembles or {}).items():
            print(f"  ---- ensemble {name} = geometric mean of {', '.join(members)}")
        print(f"  ---- report  {self.output_dir}")


def main(argv: Optional[Sequence[str]] = None) -> None:
    _utf8()
    parser = argparse.ArgumentParser(prog="python -m event_chain")
    parser.add_argument("--apply", action="store_true", help="run the stages (default: plan only)")
    parser.add_argument("--stages", default=",".join(STAGES))
    parser.add_argument("--ticker", default=C.TICKER)
    parser.add_argument("--setup", default="window", choices=sorted(C.SETUPS),
                        help="window: d=20 shortlist chain; tabular: d=1, all channels, linear kinds")
    parser.add_argument("--pools", default=None, help="comma-separated subset of config.POOLS")
    parser.add_argument("--null-draws", type=int, default=C.NULL_DRAWS)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--only", default=None, help="train: run names containing any of these")
    parser.add_argument("--force", action="store_true", help="re-run a stage that already ran")
    parser.add_argument("--no-walkforward", action="store_true")
    parser.add_argument("--scope", default=None, help="final..report: a narrow table's name")
    parser.add_argument("--scope-pools", default=None,
                        help="with --scope: the comma-separated pools the narrow table unions")
    parser.add_argument("--keep-failed", action="store_true",
                        help="final: also union the pools whose selection FAILED its null")
    parser.add_argument("--notes", default="", help="report: a sentence logged with the trial")
    parser.add_argument("--no-trial", action="store_true",
                        help="report: do NOT log this report as a trial (debugging only)")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    argv_list = list(sys.argv if argv is None else ["python -m event_chain", *argv])

    scope_pools = [p.strip() for p in args.scope_pools.split(",")] if args.scope_pools else None
    if bool(args.scope) != bool(scope_pools):
        raise SystemExit("--scope and --scope-pools go together")
    chain = EventChain.from_setup(
        args.setup, ticker=args.ticker, null_draws=args.null_draws, device=args.device,
        scope=args.scope, scope_pools=scope_pools, keep_failed=args.keep_failed,
    )
    started = pd.Timestamp.now()
    chain.status()
    if not args.apply:
        print("\nplan only — pass --apply to run")
        return
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = sorted(set(stages) - set(STAGES))
    if unknown:
        raise SystemExit(f"unknown stage(s) {unknown}; have {STAGES}")
    pools = [p.strip() for p in args.pools.split(",")] if args.pools else None
    only = [o.strip() for o in args.only.split(",")] if args.only else None
    stage_log: List[Dict] = []
    for stage in STAGES:
        if stage not in stages:
            continue
        print(f"\n{'#' * 78}\n# {stage}\n{'#' * 78}")
        t0 = pd.Timestamp.now()
        built = None
        if stage == "select":
            chain.select(pools, force=args.force)
        elif stage == "final":
            chain.final()
        elif stage == "dataset":
            chain.dataset()
        elif stage == "train":
            chain.train(force=args.force, only=only)
        elif stage == "report":
            built = chain.report(walkforward=not args.no_walkforward)
            print(built["text"])
        stage_log.append({
            "stage": stage, "started_at": str(t0.round("s")),
            "seconds": round((pd.Timestamp.now() - t0).total_seconds(), 1),
        })
        if built is not None and not args.no_trial:
            # ⚠️ EVERY COMPLETED REPORT IS A TRIAL: its model runs become rows of
            # `trials.csv`, which is how the size of the search stays on disk beside the
            # number it produced (NUL-1).
            from event_chain import trial

            path = trial.record(chain, built, kind="trial", notes=args.notes,
                                stages=stage_log, argv=argv_list, started_at=started)
            print(f"trial logged: {path}")
