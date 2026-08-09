# src\pipeline\stages.py
"""The five stages, in order, and what each one hands the next.

    python -m pipeline                    # print the plan and every stage's state
    python -m pipeline --apply            # run every stage that is not up to date
    python -m pipeline --apply --from model
    python -m pipeline --apply --only train_test_creator

## The chain

```
reports/feature_selection/<run>/         feature_selection   (runs are produced by hand;
    outstanding.csv                                           this stage only refreshes
                                                              the per-run shortlist)
        ↓
unified_schema_<t>.<target>__final__d<d>_h<h>    final_features   ⚠️ writes the DATABASE
        ↓
src/train_test_set/<dataset>/            train_test_creator
        ↓
src/model/runs/<run_id>/                 model.lstm
        ↓
results/metrics.json + runs/index.csv    result_evaluator
```

## ⚠️ Each stage VERIFIES its input; none of them infers it

The seam between two stages used to be a string typed twice — a view name in a
notebook parameter, a dataset folder name in a YAML. Both could be wrong in a way that
still ran:

| seam | used to be | now |
|---|---|---|
| table → dataset | `LOOKBACK_DAY` set by hand | `d`,`h` parsed from the table name |
| dataset → run | `dataset:` string in a config | asserted against `metadata.json` |
| run → score | metrics computed in a notebook | recomputed from `predictions_*.csv` |

So this module does not *pass* anything between stages. It runs each one and checks
that what the next stage will read actually exists and agrees. `status()` is that
check, and it is the whole value here: `python -m pipeline` with no flags answers
"which stage is stale" without touching anything.

## ⚠️ What the pipeline does NOT do

**Run a feature selection.** A selection is hours of GPU time and a judgement about
which pools to join; `RUN__feature_importance_report.ipynb` is the entry point and it
stays manual. This stage refreshes `outstanding.csv` from the runs that already exist.

**Vouch for anything.** Every table, dataset and run downstream of the current archive
carries the same warning: all 19 source runs computed no null
(`feature_selection/CONTEXT.md` §14b). A green pipeline means the stages agree with
each other, not that the result means anything.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

import pandas as pd

_SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO = os.path.dirname(_SRC)

# The one dataset and config the chain is wired for today. Both are arguments rather
# than constants everywhere below, so a second target is a flag and not an edit.
DEFAULT_TICKER = "vcb"
DEFAULT_TABLE = "return_5day__final__d20_h5"
DEFAULT_CONFIG = "vcb__return_5day__final__d20_h5.yaml"


@dataclass
class StageState:
    """What a stage produced, and whether the next stage can read it."""

    name: str
    ready: bool
    detail: str
    output: str = ""
    counts: Dict[str, int] = field(default_factory=dict)

    def row(self) -> Dict:
        return {
            "stage": self.name,
            "ready": self.ready,
            "output": self.output,
            "detail": self.detail,
            **self.counts,
        }


@dataclass
class Stage:
    name: str
    describe: str
    status: Callable[[], StageState]
    apply: Optional[Callable[[], None]] = None
    # ⚠️ `manual` means this stage's INPUT cannot be produced here, even though
    # `apply` exists and does something useful. The selection stage refreshes
    # shortlists from archived runs; it cannot perform a run. Reported in the plan
    # so `--apply` is never mistaken for a cold rebuild (issue PIP-1).
    manual: bool = False


# ------------------------------------------------------------------- 1. selection


def _report_root() -> str:
    from feature_selection.report import DEFAULT_REPORT_ROOT

    return DEFAULT_REPORT_ROOT


def status_selection() -> StageState:
    from feature_selection.outstanding import OUTSTANDING_FILENAME

    root = _report_root()
    if not os.path.isdir(root):
        return StageState("selection", False, f"{root} does not exist", root)
    runs = [
        name
        for name in sorted(os.listdir(root))
        if os.path.exists(os.path.join(root, name, "metadata.json"))
    ]
    shortlisted = [
        name
        for name in runs
        if os.path.exists(os.path.join(root, name, OUTSTANDING_FILENAME))
    ]
    return StageState(
        "selection",
        ready=bool(shortlisted),
        detail=f"{len(shortlisted)}/{len(runs)} runs carry {OUTSTANDING_FILENAME}",
        output=root,
        counts={"runs": len(runs), "shortlists": len(shortlisted)},
    )


def apply_selection() -> None:
    from feature_selection import outstanding

    outstanding.main([])


# --------------------------------------------------------------- 2. final_features


def status_final_features(ticker: str = DEFAULT_TICKER, table: str = DEFAULT_TABLE):
    """Does the table exist — AND does it still match the shortlists it came from?

    ⚠️ **"Exists" was the only check this made until 2026-08-09, and that is issue
    STL-1.** The VCB table drifted 26 columns away from its own shortlists (which had
    been regenerated under a new cut) and the stage kept reporting "ready". A table now
    carries a `fingerprint` of its exact `(source_table, channel)` set in its
    `COMMENT`; this compares that against the set the current reports would produce.
    """
    from final_features.builder import fingerprint_of_comment, plan_from_reports
    from feature_selection.unified_reader import UnifiedSchemaReader

    try:
        with UnifiedSchemaReader(ticker) as reader:
            present = table in set(reader.tables())
            rows = columns = 0
            comment = ""
            if present:
                columns = len(reader.column_types(table))
                with reader.driver._cursor_ctx() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {reader.schema}.{table}")
                    rows = int(cur.fetchone()[0])
                    cur.execute(
                        "SELECT obj_description(%s::regclass)",
                        (f"{reader.schema}.{table}",),
                    )
                    row = cur.fetchone()
                    comment = row[0] if row and row[0] else ""
        wanted = next(
            (p for p in plan_from_reports() if p.schema.endswith(ticker) and p.table == table),
            None,
        )
    except Exception as error:  # noqa: BLE001 — a dead database is a stage state
        return StageState("final_features", False, f"{type(error).__name__}: {error}")

    if not present:
        return StageState(
            "final_features",
            False,
            f"MISSING in unified_schema_{ticker}",
            f"unified_schema_{ticker}.{table}",
        )

    stored = fingerprint_of_comment(comment)
    current = wanted.fingerprint if wanted else None
    if stored is None:
        detail = "STALE — built before fingerprinting; rebuild to make it checkable"
        ready = False
    elif current is None:
        detail = f"orphan — fingerprint {stored}, but no current plan builds this table"
        ready = False
    elif stored != current:
        detail = (
            f"STALE — table {stored} vs shortlists {current} "
            f"({wanted.n_features} channels selected now, {columns - 4} in the table)"
        )
        ready = False
    else:
        detail = f"current — fingerprint {stored} matches the shortlists"
        ready = True

    return StageState(
        "final_features",
        ready=ready,
        detail=detail,
        output=f"unified_schema_{ticker}.{table}",
        counts={"rows": rows, "columns": columns},
    )


def apply_final_features() -> None:
    from final_features.builder import build_all

    # ⚠️ `replace=False`. Rebuilding a table silently would invalidate every dataset
    # hash downstream; `final_features/CONTEXT.md` §7 makes the same argument. Pass
    # --replace to that module directly when the rebuild is intended.
    build_all(apply=True, replace=False)


# ----------------------------------------------------------- 3. train_test_creator


def status_dataset(ticker: str = DEFAULT_TICKER, table: str = DEFAULT_TABLE):
    from train_test_creator.dataset import TrainTestCreator

    creator = TrainTestCreator(ticker=ticker, table=table)
    directory = creator.output_dir()
    tensors = ["X_train.npy", "y_train.npy", "X_test.npy", "y_test.npy"]
    present = os.path.isdir(directory) and all(
        os.path.exists(os.path.join(directory, name)) for name in tensors
    )
    counts: Dict[str, int] = {}
    if present:
        from model.common.data import load_dataset

        dataset = load_dataset(creator.name)
        counts = {
            "train": len(dataset.y_train),
            "val": len(dataset.y_val),
            "test": len(dataset.y_test),
            "features": dataset.n_features,
        }
    return StageState(
        "train_test_creator",
        ready=present,
        detail=f"{'built' if present else 'MISSING'} — {creator.name}",
        output=directory,
        counts=counts,
    )


def apply_dataset(ticker: str = DEFAULT_TICKER, table: str = DEFAULT_TABLE) -> None:
    from train_test_creator.dataset import TrainTestCreator

    creator = TrainTestCreator(ticker=ticker, table=table)
    creator.save(creator.build(), replace=True)


# ----------------------------------------------------------------------- 4. model


def _config_path(config: str = DEFAULT_CONFIG) -> str:
    from model.lstm.train import CONFIG_DIR

    return config if os.path.isabs(config) else os.path.join(CONFIG_DIR, config)


def status_model(config: str = DEFAULT_CONFIG):
    from model.lstm.train import RUNS_DIR, load_config

    path = _config_path(config)
    if not os.path.exists(path):
        return StageState("model", False, f"no config at {path}", path)
    cfg = load_config(path)
    matches = [
        name
        for name in sorted(os.listdir(RUNS_DIR))
        if name.startswith(cfg["run_name"] + "__")
    ] if os.path.isdir(RUNS_DIR) else []
    return StageState(
        "model",
        ready=bool(matches),
        detail=f"{len(matches)} run(s) for {cfg['run_name']}"
        + (f"; latest {matches[-1]}" if matches else ""),
        output=os.path.join(RUNS_DIR, matches[-1]) if matches else RUNS_DIR,
        counts={"runs": len(matches)},
    )


def apply_model(config: str = DEFAULT_CONFIG) -> None:
    from model.lstm.train import load_config, train

    train(load_config(_config_path(config)))


# ------------------------------------------------------------------ 5. evaluation


def status_evaluation():
    from result_evaluator.evaluator import DEFAULT_RUNS_DIR, run_folders

    folders = run_folders()
    scored = [
        f
        for f in folders
        if os.path.exists(os.path.join(f, "results", "metrics.json"))
    ]
    return StageState(
        "result_evaluator",
        ready=bool(folders) and len(scored) == len(folders),
        detail=f"{len(scored)}/{len(folders)} run(s) scored",
        output=DEFAULT_RUNS_DIR,
        counts={"runs": len(folders), "scored": len(scored)},
    )


def apply_evaluation() -> None:
    from result_evaluator.evaluator import leaderboard

    leaderboard(rescore=True)


# ------------------------------------------------------------------------- driver


def stages(
    ticker: str = DEFAULT_TICKER,
    table: str = DEFAULT_TABLE,
    config: str = DEFAULT_CONFIG,
) -> List[Stage]:
    return [
        Stage(
            "selection",
            "refresh outstanding.csv from the archived feature-selection runs",
            status_selection,
            apply_selection,
            # ⚠️ The RUNS themselves are manual — hours of GPU and a judgement about
            # which pools to join. This only rebuilds the shortlists.
            manual=True,
        ),
        Stage(
            "final_features",
            "materialise <target>__final__d<d>_h<h> from every shortlist",
            lambda: status_final_features(ticker, table),
            apply_final_features,
        ),
        Stage(
            "train_test_creator",
            "purge, impute, scale and window the table into tensors",
            lambda: status_dataset(ticker, table),
            lambda: apply_dataset(ticker, table),
        ),
        Stage(
            "model",
            "train one config into an immutable run folder",
            lambda: status_model(config),
            lambda: apply_model(config),
        ),
        Stage(
            "result_evaluator",
            "score every run against its own block-shuffled null",
            status_evaluation,
            apply_evaluation,
        ),
    ]


def status(**kwargs) -> pd.DataFrame:
    """One row per stage: is its output there, and what is in it."""
    return pd.DataFrame([stage.status().row() for stage in stages(**kwargs)])


def run(
    apply: bool = False,
    start: Optional[str] = None,
    only: Optional[str] = None,
    **kwargs,
) -> pd.DataFrame:
    """Print the chain, and with `apply=True` execute the stages that are not ready.

    ⚠️ A stage that is already ready is SKIPPED, not re-run. Re-running
    `final_features` would need `--replace` and would change every dataset hash
    downstream; re-running `model` would append a second run folder. Use `--only` to
    force one stage, which is explicit about which of those you want.
    """
    chain = stages(**kwargs)
    names = [stage.name for stage in chain]
    if only and only not in names:
        raise ValueError(f"--only {only!r} is not a stage; have {names}")
    if start and start not in names:
        raise ValueError(f"--from {start!r} is not a stage; have {names}")

    selected = [s for s in chain if s.name == only] if only else (
        chain[names.index(start):] if start else chain
    )

    rows = []
    for stage in chain:
        state = stage.status()
        chosen = stage in selected
        action = "—"
        if chosen and apply:
            if state.ready and not only:
                action = "skip (ready)"
            elif stage.apply is None:
                action = "MANUAL"
            else:
                print(f"\n{'=' * 78}\n▶ {stage.name}  —  {stage.describe}\n")
                stage.apply()
                state = stage.status()
                # ⚠️ A manual stage's `apply` refreshes what already exists; it
                # cannot create the input from nothing, and saying plain "ran"
                # invites reading `--apply` as a cold rebuild (PIP-1).
                action = "ran (refresh only)" if stage.manual else "ran"
        elif chosen:
            if stage.manual and not state.ready:
                action = "MANUAL — cannot be produced here"
            else:
                action = "would run" if not state.ready else "up to date"
        row = state.row()
        row["action"] = action
        row["manual"] = stage.manual
        rows.append(row)

    frame = pd.DataFrame(rows)
    lead = ["stage", "ready", "manual", "action", "detail", "output"]
    frame = frame[lead + [c for c in frame.columns if c not in lead]]
    return frame
