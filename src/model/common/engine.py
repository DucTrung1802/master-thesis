# src\model\common\engine.py
"""Config → run folder → trained model → scored result, for ANY model type.

This is `lstm/train.py` with the four LSTM-specific lines lifted out into arguments.
`model/lstm/train.py` and `model/cnn/train.py` are thin wrappers that bind their own
`model` module and type name; everything else — verification, lineage, prediction
writing, scoring, the registry row — happens here, once.

## ⚠️ Why this is not a copy of `lstm/train.py`

`model/CONTEXT.md` §7 says a new model is "a `train.py` copying `lstm/train.py`". That
recipe is what this module replaces, and the repo's own history is the argument: issue
**TGT-1** was `final_features._stored_target` duplicated "in a second place that could
drift from it", and the fix was to have exactly one authority. `train.py` is 346 lines
of which **eight** are LSTM-specific — the import, the `arch_dict` call, and the string
`"LSTM"` twice. Copying it would mean every future correction to `_verify` (which
raises on a `d` mismatch), to `_write_predictions` (which inverse-transforms the target
and inserts the `ticker` column that tells the evaluator a run is a PANEL), or to
`_registry_row` has to be made in N places or silently apply to one model and not the
others.

⚠️ **`_write_predictions` is the one worth protecting.** Two of its behaviours are
load-bearing and neither is obvious: regression predictions are inverse-transformed to
the RETURN scale (scoring on the standardised target makes RMSE depend on the train
slice's variance, so two datasets stop being comparable), and a missing `ticker` column
makes a 20-ticker panel score as one series — `n_eff` counts 20 banks on a date as 20
observations rather than one (issue **PNL-1**). A copy that drifts on either produces a
run that looks finished and is not comparable to the runs beside it.

## ⚠️ Lineage — what a run is allowed to claim

The dataset's `metadata.json` carries the source table's `COMMENT`, and that comment
records the `evidence` of every feature-selection run behind the table. That sentence is
copied into every run's metadata, so a run folder read six months from now still says
what its features are and are not. It is not decoration: a run trained on channels no
null ever cleared can still post a good test IC, and the only defence is that the
provenance travels with it.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Sequence

import numpy as np
import pandas as pd
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
from result_evaluator import metrics as M
from result_evaluator.evaluator import evaluate_run

_SRC = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Every model's runs share one folder and one `index.csv`, so a `<model>__` prefix on
# `run_name` is what keeps them apart. See `model/CONTEXT.md` §2.
RUNS_DIR = os.path.join(_SRC, "model", "runs")

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


def model_spec(config: Dict) -> Dict:
    """The `model:` block minus `type`, i.e. the kwargs its `arch_dict` takes.

    ⚠️ `type` is the SELECTOR, not a hyper-parameter, and passing it through would make
    every `arch_dict` grow a parameter it ignores. Each model module owns the rest of
    the keys, which is why an LSTM config carries `hidden_size`/`num_layers` and a CNN
    config carries `channels`/`kernel_size` without either needing to know about the
    other.
    """
    return {k: v for k, v in config["model"].items() if k != "type"}


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


def train(
    config: Dict,
    model_module,
    model_type: str,
    runs_dir: str = RUNS_DIR,
    dry_run: bool = False,
):
    """Train one config end to end and return `(run_dir, metrics_table)`.

    Args:
        model_module: a module exposing `build_model(**kwargs)` and
            `arch_dict(n_features, **spec)`. See `model/cnn/model.py`.
        model_type: what lands in `index.csv`'s `model_type` and the run metadata.
            ⚠️ It is passed rather than read from `config["model"]["type"]` so the
            registry cannot be changed by editing a YAML.
    """
    dataset = load_dataset(config["dataset"], expected_hash=config.get("dataset_hash"))
    lineage = _verify(config, dataset)
    task = config["task"]
    horizon = _horizon(lineage, config)

    print(f"{'=' * 78}")
    print(f"run       {config['run_name']}   task={task}   model={model_type}")
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

    spec = model_spec(config)
    arch = model_module.arch_dict(n_features=dataset.n_features, **spec)
    net = model_module.build_model(**arch["kwargs"])
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
        model={"type": model_type, **arch["kwargs"], "n_params": int(n_params)},
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

    # ⚠️ The hparam block is built from `spec`, so a model with different knobs logs
    # its own. Hard-coding `hidden_size`/`num_layers` here — as the LSTM version did —
    # would raise a KeyError on any architecture that does not have them.
    trainer.log_hparams(
        {
            **{k: v for k, v in spec.items() if isinstance(v, (int, float, str, bool))},
            "lr": train_cfg.lr,
            "batch_size": train_cfg.batch_size,
            "lookback": dataset.lookback,
        },
        {f"test_{k}": v for k, v in scored.get("test", {}).items()
         if isinstance(v, (int, float)) and not isinstance(v, bool)},
    )
    trainer.close()

    append_run(runs_dir, _registry_row(run, dataset, task, model_type, scored))

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


def _registry_row(run, dataset, task, model_type: str, scored: Dict[str, Dict]) -> Dict:
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
        "model_type": model_type,
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


def run_cli(
    model_module,
    model_type: str,
    config_dir: str,
    default_config: str,
    argv: Optional[Sequence[str]] = None,
):
    """`python -m model.<name> [--config PATH] [--dry-run]`, for any model package."""
    argv = list(sys.argv[1:] if argv is None else argv)
    path = argv[argv.index("--config") + 1] if "--config" in argv else default_config
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(config_dir, os.path.basename(path))
    return train(
        load_config(path),
        model_module=model_module,
        model_type=model_type,
        dry_run="--dry-run" in argv,
    )
