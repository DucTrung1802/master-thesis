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
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from walkforward.folds import Fold, FoldBuilder, make_folds

DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "..", "..", "results", "walkforward")


def _load_config(config: str) -> Dict:
    from model.lstm.train import CONFIG_DIR, load_config

    path = config if os.path.isabs(config) or os.path.exists(config) else os.path.join(
        CONFIG_DIR, config
    )
    return load_config(path)


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


def train_fold(base_config: Dict, dataset_name: str, n_features: int, tag: str):
    """Train the model on one fold and return its run directory."""
    from model.lstm.train import train

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
    first_test = str(option("--first-test", "2017-01-01"))
    step = int(option("--step-months", 12))
    val_months = int(option("--val-months", 12))
    keep = "--keep" in argv
    out_dir = os.path.abspath(str(option("--out", DEFAULT_OUT)))
    os.makedirs(out_dir, exist_ok=True)

    with runtime.RunTimer(f"walkforward  {ticker}.{table}  step={step}m", show_gpu=True):
        base_config = _load_config(config_name)
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
        print()

        run_dirs, rows = [], []
        for fold in folds:
            print(f"\n{'=' * 78}\n{fold.describe()}\n{'=' * 78}")
            name, n_features, directory, data = build_fold(builder, frame, fold, keep)
            shapes = {s: data.X[s].shape[0] for s in ("train", "val", "test")}
            print(f"  dataset {name}  features={n_features}  "
                  f"train {shapes['train']:,} | val {shapes['val']:,} | test {shapes['test']:,}")
            run_dir = train_fold(base_config, name, n_features, fold.tag)
            run_dirs.append(run_dir)
            rows.append({"fold": fold.tag, "train": shapes["train"], "val": shapes["val"],
                         "test": shapes["test"], "n_features": n_features,
                         "run": os.path.basename(run_dir)})
            if not keep:
                shutil.rmtree(directory, ignore_errors=True)

        plan = pd.DataFrame(rows)
        plan.to_csv(os.path.join(out_dir, "folds.csv"), index=False)
        predictions = collect(run_dirs, [f.tag for f in folds])
        path = os.path.join(out_dir, "predictions_oos.csv")
        predictions.to_csv(path, index=False)
        print(f"\n{'=' * 78}")
        print(plan.to_string(index=False))
        print(f"\nOOS track: {len(predictions):,} rows, "
              f"{predictions['date'].nunique()} dates, "
              f"{predictions['date'].min().date()} -> {predictions['date'].max().date()}")
        print(f"wrote {path}")
    return predictions


if __name__ == "__main__":
    main()
