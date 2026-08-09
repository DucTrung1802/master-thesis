# src\model\lstm\train.py
"""Config → run folder → trained LSTM → scored result. One command, no notebook.

    python -m model.lstm                                     # default config
    python -m model.lstm --config configs/vcb__return_5day__final__d20_h5.yaml
    python -m model.lstm --config <path> --dry-run           # print the plan only

`RUN__lstm.ipynb` is the same thing with figures. Both call `train()` — the notebook
holds no logic the script does not, so a sweep is a shell loop rather than nine edited
copies of a notebook.

## What this adds over the old notebook

`lstm_return_5day.ipynb` inlined the whole run: config parsing, dataset load, model
build, training, metric computation, plotting and the registry row. Three consequences
it lived with, all removed here:

1. **The config's `lookback` and the dataset's could disagree.** The dataset now
   *declares* its `lookback_d` in `metadata.json` and `_verify` raises on a mismatch
   instead of training a `d=20` model on `d=5` windows.
2. **Metrics were computed in the notebook**, so improving them meant re-running every
   training job. They now come from `result_evaluator`, which reads
   `predictions_*.csv` — `--rescore` fixes a metric across every past run without a
   GPU.
3. **The run recorded its dataset but not where the dataset came from.** `lineage`
   now carries the chain: source table → `COMMENT` → dataset → run. See §Lineage.

## ⚠️ Lineage — what the run is allowed to claim

The dataset's `metadata.json` carries the source table's `COMMENT`, and that comment
records that all 19 feature-selection runs behind the table computed **no null**
(`feature_selection/CONTEXT.md` §14b). That sentence is copied into every run's
metadata, so a run folder read six months from now still says what its features are
and are not. It is not decoration: a run trained on channels no null ever cleared can
still post a good test IC, and the only defence is that the provenance travels with it.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, Optional, Sequence

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import yaml

from model.common.data import load_dataset
from model.common.registry import append_run
from model.common.run_dir import RunDir
from model.common.trainer import (
    TrainConfig,
    Trainer,
    resolve_device,
    set_seed,
    to_loaders,
)
from model.lstm import model as lstm_model
from result_evaluator import metrics as M
from result_evaluator.evaluator import evaluate_run

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.dirname(os.path.dirname(_HERE))

# Every model's runs share one folder and one `index.csv`, so a `<model>__` prefix on
# `run_name` is what keeps them apart. See `model/CONTEXT.md` §2.
RUNS_DIR = os.path.join(_SRC, "model", "runs")
CONFIG_DIR = os.path.join(_HERE, "configs")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "vcb__return_5day__final__d20_h5.yaml")

# The loss each task trains with. ⚠️ A classifier's model emits a raw LOGIT and
# `BCEWithLogitsLoss` applies the sigmoid internally — the head is unchanged, only the
# loss and the eval transform differ.
CRITERIA = {
    M.REGRESSION: nn.MSELoss,
    M.CLASSIFICATION: nn.BCEWithLogitsLoss,
}


def load_config(path: str) -> Dict:
    with open(path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    for key in ("run_name", "dataset", "model"):
        if key not in config:
            raise ValueError(f"{path} has no {key!r} — a run cannot be identified.")
    config.setdefault("task", M.REGRESSION)
    if config["task"] not in M.TASKS:
        raise ValueError(f"task must be one of {M.TASKS}, got {config['task']!r}")
    return config


def _verify(config: Dict, dataset) -> Dict:
    """Check the config against what the dataset actually is, and build the lineage.

    ⚠️ Raises rather than warns. Training a `d=20` architecture on `d=5` windows
    produces a run that looks finished, lands in `index.csv` beside comparable runs
    and is not one of them.
    """
    meta = dataset.meta or {}
    window = meta.get("window", {})
    declared = window.get("lookback_d")
    if declared is not None and int(declared) != int(dataset.lookback):
        raise ValueError(
            f"{dataset.name} declares lookback_d={declared} but its tensors are "
            f"{dataset.lookback} deep — the dataset is inconsistent with itself."
        )
    for key, actual in (("lookback", dataset.lookback), ("n_features", dataset.n_features)):
        stated = config.get(key)
        if stated is not None and int(stated) != int(actual):
            raise ValueError(
                f"config says {key}={stated}, the dataset has {actual}. Fix the "
                f"config — the dataset is the authority."
            )

    task = config["task"]
    if task == M.CLASSIFICATION and dataset.target_scaler is not None:
        raise ValueError(
            f"{dataset.name} has a target scaler but the task is classification — a "
            f"0/1 label must be built with scale_target=False, never standardised."
        )

    source = meta.get("source", {})
    return {
        "schema": source.get("schema"),
        "table": source.get("table"),
        "source_comment": source.get("comment"),
        "dataset_name": dataset.name,
        "dataset_hash": dataset.hash,
        "target": (meta.get("target") or {}).get("column"),
        "horizon_h": (meta.get("target") or {}).get("horizon_h"),
        "lookback_d": declared or dataset.lookback,
        "purge_gap_rows": (meta.get("split") or {}).get("purge_gap_rows"),
        "features_dropped": list((meta.get("features") or {}).get("dropped_columns", {})),
        "evidence": meta.get("evidence"),
    }


def _horizon(lineage: Dict, config: Dict) -> int:
    """The label horizon in rows — the null's block size depends on it."""
    return int(lineage.get("horizon_h") or config.get("horizon") or 5)


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    """Train one config end to end and return `(run_dir, metrics_table)`."""
    dataset = load_dataset(config["dataset"], expected_hash=config.get("dataset_hash"))
    lineage = _verify(config, dataset)
    task = config["task"]
    horizon = _horizon(lineage, config)

    print(f"{'=' * 78}")
    print(f"run       {config['run_name']}   task={task}")
    print(f"dataset   {dataset.name}  ({dataset.hash})")
    print(f"source    {lineage['schema']}.{lineage['table']}")
    print(f"window    d={dataset.lookback}  h={horizon}  features={dataset.n_features}")
    print(f"samples   train {len(dataset.y_train)} | val {len(dataset.y_val)} "
          f"| test {len(dataset.y_test)}")
    if dry_run:
        print("\ndry run — nothing trained, nothing written")
        return None, None

    set_seed(config.get("seed", 42))
    device = resolve_device(config.get("device", "auto"))
    run = RunDir.create(base_dir=runs_dir, run_name=config["run_name"], config=config)
    run.update_metadata(
        dataset=dataset.reference(), device=str(device), lineage=lineage
    )

    spec = config["model"]
    arch = lstm_model.arch_dict(
        n_features=dataset.n_features,
        hidden_size=spec["hidden_size"],
        num_layers=spec["num_layers"],
        dropout=spec["dropout"],
    )
    net = lstm_model.build_model(**arch["kwargs"])
    n_params = sum(p.numel() for p in net.parameters())
    print(f"device    {device}   parameters {n_params:,}")

    train_cfg = TrainConfig.from_dict(config.get("train", {}))
    loaders = dict(
        zip(("train", "val", "test"), to_loaders(dataset, train_cfg.batch_size, device))
    )
    trainer = Trainer(
        net, train_cfg, run, device, arch=arch, criterion=CRITERIA[task]()
    )
    history = trainer.fit(loaders["train"], loaders["val"])
    pd.DataFrame(history).to_csv(
        os.path.join(run.results_dir, "loss_history.csv"), index_label="epoch"
    )

    _write_predictions(trainer, dataset, loaders, run, task)

    run.update_metadata(
        model={"type": "LSTM", **arch["kwargs"], "n_params": int(n_params)},
        training={
            "best_epoch": int(trainer.best_epoch),
            "best_val_loss": float(trainer.best_val),
            "criterion": CRITERIA[task].__name__,
            **{
                k: getattr(train_cfg, k)
                for k in ("batch_size", "lr", "weight_decay", "max_epochs", "patience")
            },
        },
    )

    # ⚠️ Scored by `result_evaluator`, not here. The same call rescoring an old run
    # produces the same columns, which is the only way runs from different sessions
    # are comparable at all.
    table = evaluate_run(run.dir, draws=config.get("null_draws", M.NULL_DRAWS))
    scored = {split: table.loc[split].to_dict() for split in table.index}
    run.update_metadata(metrics=scored)

    trainer.log_hparams(
        {
            "hidden_size": spec["hidden_size"],
            "num_layers": spec["num_layers"],
            "dropout": spec["dropout"],
            "lr": train_cfg.lr,
            "batch_size": train_cfg.batch_size,
            "lookback": dataset.lookback,
        },
        {f"test_{k}": v for k, v in scored.get("test", {}).items()
         if isinstance(v, (int, float)) and not isinstance(v, bool)},
    )
    trainer.close()

    append_run(runs_dir, _registry_row(run, dataset, task, scored))

    print(f"\n{table.to_string()}")
    for split in table.index:
        print(f"\n{split}: {M.verdict(table.loc[split].to_dict())}")
    print(f"\nrun folder {run.dir}")
    return run.dir, table


def _write_predictions(trainer, dataset, loaders, run, task) -> None:
    """`results/predictions_<split>.csv` — the file `result_evaluator` reads.

    ⚠️ Regression predictions are inverse-transformed to the RETURN scale first.
    Scoring on the standardised target would make RMSE depend on the train slice's own
    variance and stop two datasets being comparable.
    """
    for split in ("val", "test"):
        raw = trainer.predict(loaders[split])
        y_scaled = getattr(dataset, f"y_{split}")
        dates = getattr(dataset, f"dates_{split}")
        if task == M.CLASSIFICATION:
            frame = pd.DataFrame(
                {
                    "y_true": np.asarray(y_scaled, dtype=float).ravel(),
                    "y_prob": 1.0 / (1.0 + np.exp(-raw)),
                }
            )
        else:
            frame = pd.DataFrame(
                {
                    "y_true": dataset.inverse_target(y_scaled),
                    "y_pred": dataset.inverse_target(raw),
                }
            )
        if dates is not None:
            frame.insert(0, "date", dates.astype(str))
        # ⚠️ The ticker is what tells the evaluator this is a PANEL. Without it a
        # 20-ticker run is scored as one series: `n_eff` counts 20 banks on a date as
        # 20 observations, the IC pools cross-sectional with time-series variation,
        # and the null tears each date's cross-section apart. See
        # `result_evaluator.metrics.evaluate_panel`.
        tickers = _split_tickers(dataset, split)
        if tickers is not None:
            frame.insert(1 if dates is not None else 0, "ticker", tickers.astype(str))
        frame.to_csv(
            os.path.join(run.results_dir, f"predictions_{split}.csv"), index=False
        )


def _split_tickers(dataset, split: str):
    """`tickers_<split>.npy` from the dataset folder, or None on a one-ticker set.

    `model/common/data.py` does not load these — it predates the panel case — so they
    are read straight from the referenced folder rather than adding a field there that
    every existing run would carry as None.
    """
    path = os.path.join(dataset.dir, f"tickers_{split}.npy")
    if not os.path.exists(path):
        return None
    values = np.load(path, allow_pickle=False)
    return values if len(np.unique(values)) > 1 else None


def _registry_row(run, dataset, task, scored: Dict[str, Dict]) -> Dict:
    """One `index.csv` row. Core metrics fill the same columns for every task."""
    val, test = scored.get("val", {}), scored.get("test", {})

    def number(source: Dict, key: str, digits: int = 4):
        value = source.get(key)
        return round(float(value), digits) if isinstance(value, (int, float)) else ""

    return {
        "run_id": run.run_id,
        "created_at": run.metadata["created_at"],
        "dataset_name": dataset.name,
        "dataset_hash": dataset.hash,
        "model_type": "LSTM",
        "task": task,
        "lookback": dataset.lookback,
        "n_features": dataset.n_features,
        "best_epoch": run.metadata["training"]["best_epoch"],
        "best_val_loss": round(run.metadata["training"]["best_val_loss"], 6),
        "val_ic": number(val, "ic"),
        "val_dir_auc": number(val, "dir_auc"),
        "val_long_short": number(val, "long_short", 6),
        "test_n": test.get("n", ""),
        "test_n_eff": test.get("n_eff", ""),
        "test_ic": number(test, "ic"),
        "test_ic_p": number(test, "ic_p"),
        "test_ic_clears": test.get("ic_clears", ""),
        "test_dir_auc": number(test, "dir_auc"),
        "test_dir_auc_p": number(test, "dir_auc_p"),
        "test_dir_auc_clears": test.get("dir_auc_clears", ""),
        "test_hit_rate": number(test, "hit_rate"),
        "test_long_short": number(test, "long_short", 6),
        "test_RMSE": number(test, "RMSE", 6),
        "test_RMSE_zero_baseline": number(test, "RMSE_zero_baseline", 6),
        "test_beats_zero_baseline": test.get("beats_zero_baseline", ""),
        "test_pr_auc": number(test, "pr_auc"),
        "test_base_rate": number(test, "base_rate"),
        "test_beats_majority": test.get("beats_majority", ""),
        "git_sha": run.metadata["git_sha"],
        "run_dir": os.path.relpath(run.dir, _SRC).replace("\\", "/"),
    }


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)
    path = argv[argv.index("--config") + 1] if "--config" in argv else DEFAULT_CONFIG
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, os.path.basename(path))
    return train(load_config(path), dry_run="--dry-run" in argv)


if __name__ == "__main__":
    main()
