# src\walkforward\run.py
"""PRF-1 — train one model per expanding fold, concatenate the OOS predictions, backtest.

    python -m walkforward --ticker all --table rank_20day__final__d20_h20 \
        --config lstm__all__rank_20day__final__d20_h20.yaml --first-test 2017-01-01

⚠️ **THE OUTPUT IS ONE OOS SERIES, NOT N RESULTS.** Each fold contributes only its own
test block, and no date appears twice, so the concatenation is a single walk-forward track
that `backtest.portfolio` can price exactly as it prices a normal split. The per-fold
table is printed beside it because the SHAPE of the fold series — decaying or flat — is
the question PRF-1 exists to answer, and an average over a regime that worked and one that
does not would hide it.

⚠️ **Fold datasets are built, trained on, and DELETED.** At h=20 one fold's `X_train` is
~440 MB; ten kept would be 4 GB of tensors describing nothing a run folder cannot rebuild.
Pass `--keep` to retain them.

⚠️ **A run folder per fold IS kept** — that is where `predictions_test.csv` lives, and it
is the artefact the concatenation is assembled from. They are named `<run_name>__<tag>` so
`result_evaluator`'s leaderboard shows them as the fold set they are.

## ⚠️ ARMS — several architectures over ONE set of folds (PRF-8, 2026-08-19)

    python -m walkforward --out ../results/walkforward/prf8         --arm lstm:lstm_small__all__rank_20day__final__d20_h20.yaml         --arm gbt:gbt__all__rank_20day__final__d20_h20.yaml

⚠️ **THE ARMS SHARE THE FOLD'S TENSORS, AND THAT IS THE WHOLE REASON THE FLAG EXISTS.**
Running `walkforward` twice would rebuild each fold's dataset a second time — and
`TrainTestCreator` refits the scaler, the imputation median and the coverage screen from
the train slice, so two builds are only identical because the code is deterministic, which
is an assumption rather than a measurement. Building once and training N models on it
makes "same data, different model" true by construction, which is what PRF-8 compares.

**`--arm <package>:<config>`**, repeatable. The package is the architecture
(`model.<package>.train`); the config carries the size. Each arm writes
`<out>/<label>/{folds,predictions_oos}.csv`, where the label is the `run_name` up to its
first `__`. ⚠️ The legacy `--model`/`--config` form still writes FLAT into `--out`, so
PRF-1's `results/walkforward/*.csv` are reproduced by the command that made them.
"""

from __future__ import annotations

import contextlib
import copy
import json
import os
import shutil
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from walkforward.folds import Fold, FoldBuilder, make_folds

DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "..", "..", "results", "walkforward")


def _lock_path() -> str:
    """The lock lives in the dataset root, because that is the resource being claimed.

    ⚠️ Imported, never guessed. A `getattr(..., default)` here would silently guard the
    WRONG directory the day `DEFAULT_OUTPUT_ROOT` moves, and a lock over a directory
    nobody writes to protects nothing while looking like it does.
    """
    from train_test_creator.dataset import DEFAULT_OUTPUT_ROOT

    return os.path.abspath(os.path.join(DEFAULT_OUTPUT_ROOT, ".walkforward.lock"))


def _pid_alive(pid: int) -> bool:
    try:
        import psutil

        return psutil.pid_exists(pid)
    except ImportError:
        return True   # ⚠️ cannot tell → assume alive, i.e. refuse rather than clobber


@contextlib.contextmanager
def namespace_lock(out: str = ""):
    """Exclusive claim on the FOLD-DATASET namespace, for the length of a sweep.

    ⚠️ **THIS EXISTS BECAUSE TWO CONCURRENT SWEEPS SILENTLY CORRUPTED EACH OTHER**
    (measured 2026-08-19, and it cost a 25-minute run). Every fold writes
    `train_test_set/<ticker>__<table>__…__<tag>` — a name derived from the DATA, with no
    term for which process built it — and `build_fold` saves with `replace=True` while
    `main` deletes the folder once its arms are done. So a second sweep over the same
    table rebuilds the same directory *while the first is training out of it*, and then
    deletes it underneath.

    ⚠️ **The visible symptom was the harmless half.** The second sweep died with
    `FileNotFoundError: dataset … not found`, which is loud. The dangerous half is silent:
    the surviving sweep read tensors another process was mid-`np.save` on, and would have
    reported a Sharpe with no way to tell. A number whose provenance cannot be
    reconstructed is worth nothing here, so this refuses the second sweep instead.

    The lock is advisory and self-healing: a lock whose pid is dead is taken over, since
    a killed sweep would otherwise block every later one.
    """
    path = _lock_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as handle:
                held = json.load(handle)
        except (ValueError, OSError):
            held = {}
        pid = int(held.get("pid", -1))
        # ⚠️ A lock held by OUR OWN pid is refused too. There is no legitimate
        # re-entry — two sweeps inside one process share the fold-dataset namespace
        # exactly as two processes do, and letting the inner one "take over" would
        # reproduce the very race this guard exists to stop.
        if pid > 0 and _pid_alive(pid):
            raise RuntimeError(
                f"another walkforward sweep (pid {pid}, started {held.get('started')}, "
                f"out={held.get('out')}) holds the fold-dataset namespace. Two sweeps "
                f"over one table overwrite and delete each other's tensors — see "
                f"`namespace_lock`. Wait for it, or kill it and delete {path}."
            )
        print(f"⚠️ taking over a stale lock from dead pid {pid} ({path})")
        os.remove(path)

    # ⚠️ `O_CREAT | O_EXCL`, not `open(path, "w")` — the check above and the create are
    # otherwise two steps, and two sweeps launched in the same second would both pass the
    # check and both claim the lock. This is one atomic step at the filesystem.
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError(
            f"another walkforward sweep claimed {path} between the check and the create. "
            f"Two sweeps started in the same instant; run one."
        ) from None
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump({"pid": os.getpid(), "started": str(pd.Timestamp.now()),
                   "out": out}, handle)
    try:
        yield path
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def resolve_model(package: str):
    """`"lstm"` → the `model.lstm.train` binding. **The PACKAGE is the architecture.**

    ⚠️ Every binding exposes the same three names — `CONFIG_DIR`, `load_config`, `train` —
    and `train(config)` returns `(run_dir, table)` whether the engine underneath is the
    torch loop or `train_estimator`'s single `.fit()`. That is what lets a walk-forward
    run a GBT and an LSTM through one code path instead of two.
    """
    import importlib

    try:
        module = importlib.import_module(f"model.{package}.train")
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"no model package {package!r} — expected `src/model/{package}/train.py`. "
            f"⚠️ The package is the ARCHITECTURE, not the run: a smaller LSTM is "
            f"`--arm lstm:lstm_small__....yaml`, not `--arm lstm_small:...`."
        ) from exc
    for name in ("CONFIG_DIR", "load_config", "train"):
        if not hasattr(module, name):
            raise ValueError(f"model.{package}.train exposes no {name!r}")
    return module


def _load_config(config: str, module) -> Dict:
    path = config if os.path.isabs(config) or os.path.exists(config) else os.path.join(
        module.CONFIG_DIR, config
    )
    return module.load_config(path)


@dataclass(frozen=True)
class Arm:
    """One architecture in a fold sweep: a model package plus the config that sizes it."""

    package: str
    config: Dict
    module: object

    @property
    def run_name(self) -> str:
        return str(self.config["run_name"])

    @property
    def label(self) -> str:
        """`lstm_small__all__rank_20day__final__d20_h20` → `lstm_small`.

        ⚠️ The `run_name` is unique by the RUN STANDARD (the config FILENAME must equal
        it), so the label is unique across arms of one sweep as long as no two arms share
        a first segment — asserted in `parse_arms` rather than left to collide silently
        in a shared output directory.
        """
        return self.run_name.split("__")[0]


def parse_arms(argv: Sequence[str]) -> List[Arm]:
    """Every `--arm <package>:<config>` on the command line, in order."""
    arms: List[Arm] = []
    for index, token in enumerate(argv):
        if token != "--arm":
            continue
        if index + 1 >= len(argv):
            raise ValueError("--arm needs <package>:<config>")
        spec = argv[index + 1]
        if ":" not in spec:
            raise ValueError(
                f"--arm {spec!r} is not <package>:<config> — e.g. "
                f"`--arm lstm:lstm_small__all__rank_20day__final__d20_h20.yaml`"
            )
        package, _, config_name = spec.partition(":")
        module = resolve_model(package.strip())
        arms.append(Arm(package.strip(), _load_config(config_name.strip(), module), module))
    labels = [arm.label for arm in arms]
    clash = {label for label in labels if labels.count(label) > 1}
    if clash:
        raise ValueError(
            f"two arms share the label(s) {sorted(clash)} and would write to one output "
            f"directory. The label is `run_name` up to the first `__`, so rename a run."
        )
    return arms


def build_fold(builder: FoldBuilder, frame: pd.DataFrame, fold: Fold, keep: bool):
    """Slice, build and SAVE one fold's dataset. Returns `(dataset_name, n_features)`."""
    cut = frame[frame["date"] < fold.test_end]
    data = builder.use(fold).build(frame=cut)
    directory = builder.save(data, replace=True)
    meta_path = os.path.join(directory, "metadata.json")
    with open(meta_path, encoding="utf-8") as handle:
        meta = json.load(handle)
    n_features = int(meta["dataset"]["n_features"]) if "dataset" in meta else None
    if n_features is None:
        n_features = int(data.X["train"].shape[2])
    return builder.name, n_features, directory, data


def train_fold(base_config: Dict, dataset_name: str, n_features: int, tag: str,
               module=None):
    """Train one arm on one fold and return its run directory.

    `module` is the model binding from `resolve_model`; it defaults to the LSTM so the
    pre-PRF-8 call signature still works.
    """
    if module is None:
        module = resolve_model("lstm")
    train = module.train

    config = copy.deepcopy(base_config)
    config["run_name"] = f"{base_config['run_name']}__{tag}"
    config["dataset"] = dataset_name
    config["n_features"] = n_features
    # ⚠️ The dataset is rebuilt per fold, so a hash pinned to the single-split dataset
    # would refuse every fold. The fold's own tensors are the authority here.
    config.pop("dataset_hash", None)
    # ⚠️ The model-stage null costs `draws` re-scorings per fold and buys nothing: the
    # verdict here is the BACKTEST against Buy&Hold, not `ic_clears` (NUL-3, and
    # RUNBOOK §8 rule 4). Turned off so ten folds cost ten trainings.
    config["null_draws"] = 0
    run_dir, _ = train(config)
    return run_dir


def collect(run_dirs: Sequence[str], tags: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Concatenate every fold's `predictions_test.csv`, asserting no date is shared.

    ⚠️ **The tag is PASSED, not parsed out of the folder name.** `RunDir.create` appends
    its own `__<timestamp>`, so `basename.split("__")[-1]` returns `20260819-023033` and
    every fold gets labelled by the minute it was trained rather than by the year it is
    out of sample for. Measured 2026-08-19: the first per-fold table was unreadable for
    exactly that reason, and a reader could not tell 2017 from 2026.
    """
    frames = []
    tags = list(tags) if tags is not None else [None] * len(run_dirs)
    for run_dir, tag in zip(run_dirs, tags):
        path = os.path.join(run_dir, "results", "predictions_test.csv")
        piece = pd.read_csv(path)
        piece["date"] = pd.to_datetime(piece["date"])
        piece["fold"] = tag or os.path.basename(run_dir).split("__")[-2]
        frames.append(piece)
    out = pd.concat(frames, ignore_index=True).sort_values(["date", "ticker"])
    overlap = out.groupby(["date", "ticker"]).size().max()
    if overlap > 1:
        raise AssertionError(
            f"a (date, ticker) appears in {overlap} folds — the test blocks overlap, "
            "so the concatenated track double-counts and every statistic on it is wrong"
        )
    return out.reset_index(drop=True)


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    from utils import runtime

    ticker = str(option("--ticker", "all"))
    table = str(option("--table", "rank_20day__final__d20_h20"))
    config_name = str(option("--config", "lstm__all__rank_20day__final__d20_h20.yaml"))
    model_name = str(option("--model", "lstm"))
    first_test = str(option("--first-test", "2017-01-01"))
    step = int(option("--step-months", 12))
    val_months = int(option("--val-months", 12))
    keep = "--keep" in argv
    out_dir = os.path.abspath(str(option("--out", DEFAULT_OUT)))
    os.makedirs(out_dir, exist_ok=True)

    arms = parse_arms(argv)
    # ⚠️ No `--arm` means the pre-PRF-8 single-model form, and it writes FLAT into
    # `--out` so PRF-1's own command still reproduces PRF-1's own file paths.
    flat = not arms
    if flat:
        module = resolve_model(model_name)
        arms = [Arm(model_name, _load_config(config_name, module), module)]

    with namespace_lock(out_dir), runtime.RunTimer(
        f"walkforward  {ticker}.{table}  step={step}m  "
        f"arms={','.join(a.label for a in arms)}", show_gpu=True
    ):
        builder = FoldBuilder(ticker=ticker, table=table)
        print(f"reading {builder.schema_table} ...")
        frame, comment = builder.read()
        print(f"  {len(frame):,} rows x {frame['ticker'].nunique()} tickers  "
              f"{frame['date'].min().date()} -> {frame['date'].max().date()}")
        print(f"  target stored={builder.stored_target}  selected_for={builder.selected_for}")

        folds = make_folds(frame["date"], first_test, step, val_months)
        print(f"\n{len(folds)} folds:")
        for fold in folds:
            print("  " + fold.describe())
        print(f"\n{len(arms)} arm(s) over the SAME folds:")
        for arm in arms:
            print(f"  {arm.label:<12s} package=model.{arm.package:<10s} "
                  f"run_name={arm.run_name}")
        print()

        run_dirs: Dict[str, List[str]] = {arm.label: [] for arm in arms}
        rows: Dict[str, List[Dict]] = {arm.label: [] for arm in arms}
        for fold in folds:
            print(f"\n{'=' * 78}\n{fold.describe()}\n{'=' * 78}")
            name, n_features, directory, data = build_fold(builder, frame, fold, keep)
            shapes = {s: data.X[s].shape[0] for s in ("train", "val", "test")}
            print(f"  dataset {name}  features={n_features}  "
                  f"train {shapes['train']:,} | val {shapes['val']:,} | test {shapes['test']:,}")
            # ⚠️ EVERY ARM TRAINS ON THIS ONE BUILD. Rebuilding per arm would refit the
            # scaler, the median and the coverage screen a second time, so "same data,
            # different model" would rest on the builder being deterministic instead of
            # on there being one dataset.
            for arm in arms:
                if len(arms) > 1:
                    print(f"\n  --- arm {arm.label} ---")
                run_dir = train_fold(arm.config, name, n_features, fold.tag, arm.module)
                run_dirs[arm.label].append(run_dir)
                rows[arm.label].append(
                    {"fold": fold.tag, "train": shapes["train"], "val": shapes["val"],
                     "test": shapes["test"], "n_features": n_features,
                     "run": os.path.basename(run_dir)}
                )
            if not keep:
                shutil.rmtree(directory, ignore_errors=True)

        results = {}
        for arm in arms:
            target_dir = out_dir if flat else os.path.join(out_dir, arm.label)
            os.makedirs(target_dir, exist_ok=True)
            plan = pd.DataFrame(rows[arm.label])
            plan.to_csv(os.path.join(target_dir, "folds.csv"), index=False)
            predictions = collect(run_dirs[arm.label], [f.tag for f in folds])
            path = os.path.join(target_dir, "predictions_oos.csv")
            predictions.to_csv(path, index=False)
            results[arm.label] = predictions
            print(f"\n{'=' * 78}\narm {arm.label}")
            print(plan.to_string(index=False))
            print(f"\nOOS track: {len(predictions):,} rows, "
                  f"{predictions['date'].nunique()} dates, "
                  f"{predictions['date'].min().date()} -> {predictions['date'].max().date()}")
            print(f"wrote {path}")
    return results[arms[0].label] if flat else results


if __name__ == "__main__":
    main()
