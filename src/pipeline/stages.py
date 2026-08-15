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
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

import pandas as pd

from utils import runtime

_SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO = os.path.dirname(_SRC)

# The one dataset and config the chain is wired for today. All of these are arguments
# rather than constants everywhere below, so a second target is a flag and not an edit.
DEFAULT_TICKER = "vcb"
DEFAULT_TABLE = "return_5day__final__d20_h5"
DEFAULT_CONFIG = "lstm__vcb__return_5day__final__d20_h5.yaml"
DEFAULT_ROOT = None  # None → feature_selection.report.DEFAULT_REPORT_ROOT
DEFAULT_SCOPE = None  # None → the table name carries no feature-block suffix

# ── The data stage ────────────────────────────────────────────────────────────
# The Dagster assets that stand between the network and `unified_schema_<t>.pool__*`.
# ⚠️ NOT a `+unified/pool__targets` upstream selection: that resolves to the whole
# landing layer, including `raw/trading_view_data@stocks` (777 tickers, ~10 h) and
# `raw/cafef_financials` (~2.4 h per ticker), neither of which `pool__basic` reads.
# These six raw assets are exactly `silver.stocks_basic`'s sources
# (`orchestration/assets/silver.py::STOCKS_BASIC_SOURCES`) and nothing else.
DATA_SCRAPE_ASSETS = (
    "raw/cafef_price",
    "raw/cafef_order_stats",
    "raw/cafef_foreign",
    "raw/cafef_prop_trading",
    "raw/simplize_industry",
    "raw/gics_structure",
)
DATA_INGEST_ASSETS = (
    "bronze/cafef_price",
    "bronze/cafef_order_stats",
    "bronze/cafef_foreign",
    "bronze/cafef_prop_trading",
    "bronze/simplize_industry",
    "bronze/gics",
    "silver/stocks_basic",
)
DATA_UNIFIED_ASSETS = ("unified/pool__basic", "unified/pool__targets")

# The four CafeF tabs take a per-run config; the other two take none. Only these are
# given `skip_existing: false` and a ticker scope.
DATA_SCOPED_ASSETS = DATA_SCRAPE_ASSETS[:4]

DEFINITIONS = os.path.join(_SRC, "orchestration", "definitions.py")


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


# ----------------------------------------------------------------------- 0. data


def status_data(ticker: str = DEFAULT_TICKER) -> StageState:
    """How fresh is the unified layer — measured as a DATE, never as a green asset.

    ⚠️ **A green asset is not evidence of fresh data** (CLAUDE.md §5 rule 10). `landed()`
    answers "is this folder empty?", and with `skip_existing=True` a scrape returns at an
    `os.path.exists` check before a single request goes out — green, fast, and every
    series left at whatever date it had. So this stage reports the only thing that cannot
    be faked: **the max `date` in the pool tables, against the max date on disk in the raw
    CSVs that feed them.**

    ⚠️ It also compares the two, because a scrape and its ingests are separate assets and
    "re-scraped" never implies "re-ingested" (rule 11). Bronze once sat a full day behind
    a completed scrape with nothing raising. A raw file newer than the table is the exact
    signature of that, and it is what makes this stage `ready=False`.
    """
    import csv
    import glob

    from feature_selection.unified_reader import UnifiedSchemaReader
    from utils.constants import CAFEF_RAW_DATA_DIR

    counts: Dict[str, int] = {}
    try:
        with UnifiedSchemaReader(ticker) as reader:
            present = set(reader.tables())
            missing = [t for t in ("pool__basic", "pool__targets") if t not in present]
            if missing:
                return StageState(
                    "data",
                    False,
                    f"MISSING {missing} in {reader.schema}",
                    reader.schema,
                )
            with reader.driver._cursor_ctx() as cur:
                cur.execute(f"SELECT MAX(date), COUNT(*) FROM {reader.schema}.pool__basic")
                table_date, rows = cur.fetchone()
                counts["rows"] = int(rows)
                # ⚠️ THE OTHER POOLS DO NOT MOVE WITH THIS ONE, and nothing else looks.
                # `unified/pool__basic` and `pool__targets` are two assets of five;
                # materialising them alone leaves `pool__ta`, `pool__fa` and the 19
                # `pool__economy_*` on the OLD calendar. Measured 2026-08-10: basic and
                # targets reached 2026-08-07 while the other 21 stayed at 2026-06-25.
                # That is harmless for a `pool__basic`-only build, and it is NOT
                # harmless for a wide one — `final_features` joins INNER across pools,
                # so a rebuild would silently truncate back to the laggard's calendar
                # and the table would look unchanged.
                behind_pools = [
                    name
                    for name in sorted(present)
                    if name.startswith("pool__")
                    and name not in ("pool__basic", "pool__targets")
                ]
                stale_pools = []
                for name in behind_pools:
                    cur.execute(f"SELECT MAX(date) FROM {reader.schema}.{name}")
                    other = cur.fetchone()[0]
                    if other is not None and table_date is not None and other < table_date:
                        stale_pools.append(name)
                counts["pools_behind"] = len(stale_pools)
    except Exception as error:  # noqa: BLE001 — a dead database is a stage state
        return StageState("data", False, f"{type(error).__name__}: {error}")

    # The newest session in the raw price CSV for this ticker. `pool__basic` cannot be
    # fresher than its own source, so this is the bar the table has to meet.
    # ⚠️ Anchored to the REPO, not the CWD: `CAFEF_RAW_DATA_DIR` is a relative path, so
    # calling this from `src/` (which the stage runner does) silently globs nothing and
    # the check reports "no raw CSV to compare against" instead of comparing.
    raw_date = None
    pattern = os.path.join(_REPO, CAFEF_RAW_DATA_DIR, "price", f"*_{ticker.upper()}.csv")
    for path in glob.glob(pattern):
        with open(path, "r", encoding="utf-8") as handle:
            dates = [row["date"] for row in csv.DictReader(handle) if row.get("date")]
        if dates:
            newest = max(dates)
            raw_date = newest if raw_date is None else max(raw_date, newest)

    stamp = table_date.isoformat() if table_date else "none"
    if raw_date is None:
        return StageState(
            "data",
            ready=True,
            detail=f"pool__basic to {stamp}; no raw CSV at {pattern} to check it against",
            output=f"unified_schema_{ticker}.pool__basic",
            counts=counts,
        )
    # ⚠️ An EMPTY table is stale, not current. Comparing `raw_date > stamp` as strings
    # is correct for two ISO dates and silently wrong when `table_date` is None: the
    # placeholder sorts above every date beginning with a digit, so `"2026-08-07" >
    # "none"` is False and a table with no rows would report itself up to date.
    behind = table_date is None or raw_date > stamp
    # ⚠️ A skewed sibling pool does NOT make this stage `not ready`, deliberately. The
    # stage's contract is `pool__basic` + `pool__targets`, which is what everything
    # below it reads on the `--scope basic` path; failing here would make `--apply`
    # re-materialise `pool__ta` (hours, ~11 GB upstream) to satisfy a chain that never
    # reads it. It is REPORTED so a wide build is a decision rather than a surprise.
    skew = (
        f" WARNING: {counts.get('pools_behind', 0)} sibling pool(s) still on an older "
        f"calendar - a wide rebuild would INNER-join back down to theirs"
        if counts.get("pools_behind")
        else ""
    )
    return StageState(
        "data",
        ready=not behind,
        # ⚠️ ASCII only. This prints to a cp1252 console on Windows (§5 rule 18).
        detail=(
            f"STALE - raw CafeF price reaches {raw_date}, pool__basic stops at {stamp}; "
            f"the scrape ran and the ingest did not"
            if behind
            else f"current - pool__basic to {stamp}, matching the raw CSV.{skew}"
        ),
        output=f"unified_schema_{ticker}.pool__basic",
        counts=counts,
    )


def _dagster(select: str, config: Optional[Dict] = None, partition: str = None) -> None:
    """One `dagster asset materialize`, as a subprocess with an absolute DAGSTER_HOME.

    ⚠️ `DAGSTER_HOME` MUST be set and MUST be absolute, or Dagster invents a temporary
    home per run and the four `.tmp_dagster_home_*` directories in the repo root are what
    that looks like when it happens.

    ⚠️ Run config is passed as a FILE, not `--config-json`. PowerShell 5.1 strips the
    double quotes out of a native command's arguments, so a JSON string arrives as
    `{ops:{...}}` and fails to parse — measured, not guessed.
    """
    import json
    import tempfile

    env = dict(os.environ, DAGSTER_HOME=os.path.join(_REPO, ".dagster"))
    command = [
        sys.executable, "-m", "dagster", "asset", "materialize",
        "-f", DEFINITIONS, "--select", select,
    ]
    if partition:
        command += ["--partition", partition]

    handle = None
    try:
        if config:
            handle = tempfile.NamedTemporaryFile(
                "w", suffix=".json", delete=False, encoding="utf-8"
            )
            json.dump(config, handle)
            handle.close()
            command += ["--config", handle.name]
        print(f"  $ dagster asset materialize --select {select}"
              + (f" --partition {partition}" if partition else ""))
        subprocess.run(command, env=env, cwd=_REPO, check=True)
    finally:
        if handle is not None:
            os.unlink(handle.name)


def apply_data(ticker: str = DEFAULT_TICKER, rescrape: bool = False) -> None:
    """Materialise the data layer: optionally re-scrape, then always re-ingest.

    ⚠️ **`rescrape=False` by default and that is not timidity** — it is the same
    reasoning as `--replace` on `final_features`. The scrape hits the network on someone
    else's servers; the ingest is local and idempotent. `--rescrape` asks for the first
    explicitly, and when it is asked for it is asked for HONESTLY: `skip_existing=False`,
    so the fetch actually happens, scoped to `--ticker` so it costs one name rather than
    781 (`orchestration/assets/scrape.py::CafeFTabConfig`).
    """
    if rescrape:
        config = {
            "ops": {
                asset.replace("/", "__"): {
                    "config": {
                        "skip_existing": False,
                        "tickers": [ticker.upper()],
                    }
                }
                for asset in DATA_SCOPED_ASSETS
            }
        }
        _dagster(",".join(DATA_SCRAPE_ASSETS), config=config)
    # ⚠️ Always run, even when the scrape was skipped — a scrape and its ingests are
    # separate assets, so the table can be behind raw_data/ for reasons this run had
    # nothing to do with (CLAUDE.md §5 rule 11).
    _dagster(",".join(DATA_INGEST_ASSETS))
    _dagster(",".join(DATA_UNIFIED_ASSETS), partition=ticker.upper())


# ------------------------------------------------------------------- 1. selection


def _report_root() -> str:
    from feature_selection.report import DEFAULT_REPORT_ROOT

    return DEFAULT_REPORT_ROOT


def status_selection(root: Optional[str] = DEFAULT_ROOT) -> StageState:
    from feature_selection.outstanding import OUTSTANDING_FILENAME

    root = root or _report_root()
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


def apply_selection(root: Optional[str] = DEFAULT_ROOT) -> None:
    from feature_selection import outstanding

    outstanding.main(root=root or _report_root())


# --------------------------------------------------------------- 2. final_features


def status_final_features(
    ticker: str = DEFAULT_TICKER,
    table: str = DEFAULT_TABLE,
    root: Optional[str] = DEFAULT_ROOT,
    scope: Optional[str] = DEFAULT_SCOPE,
):
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
        plans = plan_from_reports(root or _report_root(), scope)
        wanted = next(
            (p for p in plans if p.schema.endswith(ticker) and p.table == table),
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


def apply_final_features(
    root: Optional[str] = DEFAULT_ROOT, scope: Optional[str] = DEFAULT_SCOPE
) -> None:
    from final_features.builder import build_all

    # ⚠️ `replace=False`. Rebuilding a table silently would invalidate every dataset
    # hash downstream; `final_features/CONTEXT.md` §7 makes the same argument. Pass
    # --replace to that module directly when the rebuild is intended.
    build_all(root=root or _report_root(), apply=True, replace=False, scope=scope)


# ----------------------------------------------------------- 3. train_test_creator


def status_dataset(ticker: str = DEFAULT_TICKER, table: str = DEFAULT_TABLE,
                   root: Optional[str] = DEFAULT_ROOT):
    from train_test_creator.dataset import TrainTestCreator

    creator = TrainTestCreator(ticker=ticker, table=table, report_root=root)
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


def apply_dataset(ticker: str = DEFAULT_TICKER, table: str = DEFAULT_TABLE,
                  root: Optional[str] = DEFAULT_ROOT) -> None:
    from train_test_creator.dataset import TrainTestCreator

    creator = TrainTestCreator(ticker=ticker, table=table, report_root=root)
    creator.save(creator.build(), replace=True)


# ----------------------------------------------------------------------- 4. model


def _config_path(config: str = DEFAULT_CONFIG) -> str:
    """Resolve a config NAME against every model package's `configs/`.

    ⚠️ **This used to look only in `model/lstm/configs/`**, which made `--config` a
    flag that silently could not reach a second architecture — `python -m pipeline
    --config cnn__…yaml` reported `no config at …/lstm/configs/cnn__…yaml`. The stage
    is about "train one config into a run folder", and which package owns that config
    is not the pipeline's business. First match wins; an absolute or already-existing
    path is used as given.
    """
    if os.path.isabs(config) or os.path.exists(config):
        return config
    import glob

    matches = sorted(glob.glob(os.path.join(_SRC, "model", "*", "configs", config)))
    # ⚠️ **AMBIGUITY RAISES; it does not resolve alphabetically.** Config directories are
    # per-model, so a bare filename can exist in several of them — and it did: a CNN
    # config named `vcb__return_5day__final__d20_h5__basic.yaml` sat beside an LSTM one
    # of the same name, `sorted()` put `cnn` first, and the LSTM config became
    # unreachable through this function while still resolving through
    # `model.lstm.train.CONFIG_DIR`. Two ways to name one file that disagree is the
    # STL-1 shape. Prefix the config with its model, as `run_name` already is.
    if len(matches) > 1:
        raise ValueError(
            f"config {config!r} exists in {len(matches)} model packages "
            f"({[os.path.relpath(m, _SRC) for m in matches]}) — a bare name cannot "
            f"choose between them. Prefix it with the model, or pass a full path."
        )
    if matches:
        return matches[0]
    # Fall back to the LSTM directory so the error message names a concrete path
    # rather than an empty search.
    from model.lstm.train import CONFIG_DIR

    return os.path.join(CONFIG_DIR, config)


def _model_train(config_path: str):
    """The `train` bound to whichever model package owns `config_path`.

    ⚠️ Dispatched on the config's LOCATION, not on `config["model"]["type"]`. The type
    field is a label a person edits; the directory is where the module that can build
    the architecture actually lives, and `engine.train` takes the model module itself.
    """
    import importlib

    package = os.path.basename(os.path.dirname(os.path.dirname(config_path)))
    try:
        module = importlib.import_module(f"model.{package}.train")
    except ImportError as error:
        raise ValueError(
            f"config {config_path} sits under model/{package}/configs but "
            f"model.{package}.train does not import: {error}"
        ) from error
    return module.train


def status_model(config: str = DEFAULT_CONFIG):
    from model.lstm.train import RUNS_DIR, load_config

    path = _config_path(config)
    if not os.path.exists(path):
        return StageState("model", False, f"no config at {path}", path)
    cfg = load_config(path)
    # ⚠️ **A BARE PREFIX MATCH ATTRIBUTES ANOTHER CONFIG'S RUNS TO THIS ONE.** A run
    # folder is `<run_name>__<YYYYmmdd-HHMMSS>`, and `startswith(run_name + "__")` also
    # matches `<run_name>__<anything>__<stamp>` — so the moment a scoped config named
    # `lstm__vcb__return_5day__final__d20_h5__basic` existed, the WIDE config
    # `lstm__vcb__return_5day__final__d20_h5` reported "2 run(s)" and named the basic
    # run as its own latest. Measured 2026-08-10. Anchoring on the timestamp is what
    # makes the two configs' run sets disjoint.
    stamp = re.compile(re.escape(cfg["run_name"]) + r"__\d{8}-\d{6}$")
    matches = [
        name
        for name in sorted(os.listdir(RUNS_DIR))
        if stamp.match(name)
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
    from model.common.engine import load_config

    path = _config_path(config)
    _model_train(path)(load_config(path))


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
    root: Optional[str] = DEFAULT_ROOT,
    scope: Optional[str] = DEFAULT_SCOPE,
    rescrape: bool = False,
) -> List[Stage]:
    return [
        Stage(
            "data",
            "scrape → bronze → silver → unified_schema_<t>.pool__{basic,targets}",
            lambda: status_data(ticker),
            lambda: apply_data(ticker, rescrape),
        ),
        Stage(
            "selection",
            "refresh outstanding.csv from the archived feature-selection runs",
            lambda: status_selection(root),
            lambda: apply_selection(root),
            # ⚠️ The RUNS themselves are manual — hours of GPU and a judgement about
            # which pools to join for the WIDE pools. This only rebuilds the shortlists.
            # `python -m feature_selection.run` is the scriptable entry point, and the
            # cheap case (`pool__basic`, 27 channels) is minutes rather than the ~68
            # CPU-hours one wide-pool null costs (issue EVD-1).
            manual=True,
        ),
        Stage(
            "final_features",
            "materialise <target>__final__d<d>_h<h> from every shortlist",
            lambda: status_final_features(ticker, table, root, scope),
            lambda: apply_final_features(root, scope),
        ),
        Stage(
            "train_test_creator",
            "purge, impute, scale and window the table into tensors",
            lambda: status_dataset(ticker, table, root),
            lambda: apply_dataset(ticker, table, root),
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
        # ⚠️ **A STAGE THAT DID NOT RUN HAS NO RUNTIME, AND THAT IS NOT `0`.** The
        # column is empty for `skip (ready)` and for a plan, because a zero there reads
        # as "ran instantly" — the same rule §10 applies to an absent null, one level
        # down: an absent measurement is recorded as absent.
        elapsed = ""
        if chosen and apply:
            if state.ready and not only:
                action = "skip (ready)"
            elif stage.apply is None:
                action = "MANUAL"
            else:
                print(f"\n{'=' * 78}\n▶ {stage.name}  —  {stage.describe}")
                print(f"  started {runtime.stamp()}\n")
                # ⚠️ **THE STAGES ARE CALLED IN-PROCESS, NOT AS `python -m <stage>`**
                # (`apply_final_features` imports `build_all`, `apply_model` imports the
                # binding's `train`) — so each module's OWN `runtime.RunTimer` banner
                # lives in its `main()` and never fires here. Without this clock a
                # `--apply` run printed no per-stage timing at all, and the only thing
                # anyone could budget from was a wall clock and a memory of when they
                # pressed enter.
                clock = time.perf_counter()
                try:
                    stage.apply()
                finally:
                    # ⚠️ `finally`: a stage that dies after two hours must still say it
                    # spent two hours. CLAUDE.md §5 rule 20 — finish the unit, write it
                    # down, and only then describe it.
                    elapsed = runtime.format_duration(time.perf_counter() - clock)
                    print(f"\n  {stage.name} finished {runtime.stamp()}   "
                          f"runtime {elapsed}")
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
        row["runtime"] = elapsed
        rows.append(row)

    frame = pd.DataFrame(rows)
    lead = ["stage", "ready", "manual", "action", "runtime", "detail", "output"]
    frame = frame[lead + [c for c in frame.columns if c not in lead]]
    return frame
