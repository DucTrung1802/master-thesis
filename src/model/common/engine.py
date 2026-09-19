# src\model\common\engine.py
"""Config → run folder → trained model → scored result, for ANY model type.

This is `lstm/train.py` with the four LSTM-specific lines lifted out into arguments.
`model/lstm/train.py` and `model/cnn/train.py` are thin wrappers that bind their own
`model` module and type name; everything else — verification, lineage, prediction
writing, scoring, the registry row — happens here, once.

## ⚠️ Why this is not a copy of `lstm/train.py`

`.claude/context/model.md` §7 says a new model is "a `train.py` copying `lstm/train.py`". That
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
import time
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
from result_evaluator.index import index_row
from utils import runtime

_SRC = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Every model's runs share one folder and one `index.csv`, so a `<model>__` prefix on
# `run_name` is what keeps them apart. See `.claude/context/model.md` §2.
RUNS_DIR = os.path.join(_SRC, "model", "runs")

# The loss each task trains with. ⚠️ A classifier's model emits a raw LOGIT and
# `BCEWithLogitsLoss` applies the sigmoid internally — the head is unchanged, only the
# loss and the eval transform differ.
CRITERIA = {
    M.REGRESSION: nn.MSELoss,
    M.CLASSIFICATION: nn.BCEWithLogitsLoss,
}


def _console_safe() -> None:
    """Let stdout survive the `⚠️` this package prints.

    ⚠️ **`result_evaluator.metrics.verdict` puts a `⚠️` in its CLEARING branch and
    nowhere else**, and a Windows console is cp1252, which has no code point for U+26A0.
    So the only path that could raise `UnicodeEncodeError` was the one that reports a
    positive result — and it had never fired, because no run had ever cleared a bar.
    The first one that did (`baseline_ar`, `dir_auc_clears=True`, 2026-08-10) crashed
    the process *after* `append_run`, i.e. after the run was already recorded.

    `errors="replace"` degrades the glyph rather than the run. Same fix as
    `train_test_creator.dataset.main` (CLAUDE.md §5 rules 18 and 20).
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")


def load_config(path: str) -> Dict:
    with open(path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    for key in ("run_name", "dataset", "model"):
        if key not in config:
            raise ValueError(f"{path} has no {key!r} — a run cannot be identified.")
    config.setdefault("task", M.REGRESSION)
    if config["task"] not in M.TASKS:
        raise ValueError(f"task must be one of {M.TASKS}, got {config['task']!r}")

    # ⚠️ **THE FILENAME MUST BE THE `run_name`.** Two names for one run is how a config
    # ends up describing a different run than the folder it produces, and it is how
    # issue **CFG-1** happened: config files live in per-model directories, so a bare
    # `--config` name is resolved across `model/*/configs/` — and two packages held the
    # same filename while their `run_name`s differed by the model prefix. Enforcing
    # `basename == run_name` makes the collision impossible rather than detectable,
    # because `run_name` starts with the model by convention (§RUN STANDARD).
    stem = os.path.splitext(os.path.basename(path))[0]
    # `_legacy/` is exempt: those 27 configs predate the convention, their datasets no
    # longer exist (issue RPR-1), and renaming dead files would be churn. They are
    # quarantined by directory, which is the record that they are not current.
    if stem != config["run_name"] and "_legacy" not in path.replace("\\", "/").split(
        "/"
    ):
        raise ValueError(
            f"config filename {stem!r} != run_name {config['run_name']!r}. A run has "
            f"ONE name: rename the file to {config['run_name']}.yaml. See "
            f".claude/context/model.md §RUN STANDARD."
        )
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
    for key, actual in (
        ("lookback", dataset.lookback),
        ("n_features", dataset.n_features),
    ):
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
        "features_dropped": list(
            (meta.get("features") or {}).get("dropped_columns", {})
        ),
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
    _console_safe()
    dataset = load_dataset(config["dataset"], expected_hash=config.get("dataset_hash"))
    lineage = _verify(config, dataset)
    task = config["task"]
    horizon = _horizon(lineage, config)

    print(f"{'=' * 78}")
    print(f"run       {config['run_name']}   task={task}   model={model_type}")
    print(f"dataset   {dataset.name}  ({dataset.hash})")
    print(f"source    {lineage['schema']}.{lineage['table']}")
    print(f"window    d={dataset.lookback}  h={horizon}  features={dataset.n_features}")
    print(
        f"samples   train {len(dataset.y_train)} | val {len(dataset.y_val)} "
        f"| test {len(dataset.y_test)}"
    )
    if dry_run:
        print("\ndry run — nothing trained, nothing written")
        return None, None

    set_seed(config.get("seed", 42))
    # ⚠️ WALL-CLOCK, recorded in the run (2026-09-17): `fit_seconds` is the fit alone,
    # `run_seconds` the whole run including the 200-draw scoring — the trial log reads both.
    run_started = time.perf_counter()
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
    fit_started = time.perf_counter()
    history = trainer.fit(loaders["train"], loaders["val"])
    fit_seconds = time.perf_counter() - fit_started
    # ⚠️ The column names are the SHARED contract, not this path's own: `_write_loss_history`
    # writes the same three columns for an estimator that has no epochs at all, so one
    # plotting function reads any run. This path's `step` is the epoch.
    # ⚠️ `step` counts from 1 HERE because `Trainer` numbers epochs from 1 and writes that
    # number into `training.best_epoch`; the curve and the metadata of one run must name the
    # same point. (A boosted model's `best_iteration` is 0-based, and so is its curve.) The
    # first basket trial to write these files, `20260919-192250`, has 0-based torch curves.
    _write_loss_history(run, [{"step": i, "train_loss": tr, "val_loss": va}
                              for i, (tr, va) in enumerate(zip(history["train"],
                                                               history["val"]), start=1)])

    _write_predictions(lambda s: trainer.predict(loaders[s]), dataset, run, task)
    _write_importances(run, net, dataset)
    if task == M.CLASSIFICATION:
        _write_calibration(run)

    run.update_metadata(
        model={"type": model_type, **arch["kwargs"], "n_params": int(n_params)},
        training={
            "best_epoch": int(trainer.best_epoch),
            "best_val_loss": float(trainer.best_val),
            "criterion": CRITERIA[task].__name__,
            # ⚠️ The LOSS, spelled out, because `criterion` is a class name and
            # `BCEWithLogitsLoss` and `binary:logistic` are the same function under two
            # libraries' spellings. One grid, one loss (`event_chain/config.py`).
            "objective": ("binary cross-entropy" if task == M.CLASSIFICATION
                          else "mean squared error"),
            "early_stopping": f"val loss, patience {getattr(train_cfg, 'patience', None)}",
            "sample_weight": "uniform",
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
        {
            f"test_{k}": v
            for k, v in scored.get("test", {}).items()
            if isinstance(v, (int, float)) and not isinstance(v, bool)
        },
    )
    trainer.close()

    run.update_metadata(timing={
        "fit_seconds": round(fit_seconds, 3),
        "run_seconds": round(time.perf_counter() - run_started, 3),
    })
    append_run(runs_dir, _registry_row(run, dataset, task, model_type, scored))

    print(f"\n{table.to_string()}")
    for split in table.index:
        print(f"\n{split}: {M.verdict(table.loc[split].to_dict())}")
    print(f"\nrun folder {run.dir}")
    return run.dir, table


def train_estimator(
    config: Dict,
    model_module,
    model_type: str,
    runs_dir: str = RUNS_DIR,
    dry_run: bool = False,
):
    """`train()` for a model that has no epochs — a closed form, a fit, or a constant.

    Same contract as `train()`: same `_verify`, the same `predictions_*.csv`, the same
    `evaluate_run`, the same `index.csv` row. **The only thing that differs is that
    there is no training LOOP** — no optimiser, no early stopping, no checkpoints, no
    TensorBoard. A ridge, an AR(p) and a constant predictor are one `.fit()` call.

    ⚠️ **This exists so the baselines are scored by the SAME code as the networks.** A
    baseline computed in a notebook, or with its own metric block, cannot be put beside
    an LSTM row and read — which is the entire reason `result_evaluator` is a separate
    package. The zero-baseline in particular is already reported *inside* every run as
    `RMSE_zero_baseline`, and both networks lose to it; making it a run of its own is
    what lets it be compared on `ic`, `dir_auc` and `hit_rate` too.

    `model_module` must expose `build_model(n_features, lookback, **kwargs)` returning
    an object with `.fit(X, y)` and `.predict(X)` over `(n, lookback, n_features)`
    float arrays, plus `arch_dict(...)`.

    ⚠️ `best_epoch` and `best_val_loss` are recorded as 0 and the TRAIN MSE. They are
    not epochs; the columns exist because the schema is shared, and leaving them blank
    would read as "not measured" rather than "not applicable".
    """
    _console_safe()
    dataset = load_dataset(config["dataset"], expected_hash=config.get("dataset_hash"))
    lineage = _verify(config, dataset)
    task = config["task"]
    horizon = _horizon(lineage, config)

    # ⚠️ **CLASSIFICATION IS SUPPORTED ONLY BY AN ESTIMATOR THAT SAYS SO** (2026-09-16).
    # `_write_predictions` turns the raw output into `y_prob` with a SIGMOID, so a
    # classifier must hand back LOGITS — `predict_logit` — and never a probability, or
    # the sigmoid would be applied twice and every AUC would survive it while every
    # log-loss and Brier score would silently be wrong. An estimator without the method
    # is refused below, after it is built, rather than guessed at.
    if task not in (M.REGRESSION, M.CLASSIFICATION):
        raise ValueError(
            f"{model_type} supports regression and classification; got task={task!r}."
        )

    print(f"{'=' * 78}")
    print(f"run       {config['run_name']}   task={task}   model={model_type}")
    print(f"dataset   {dataset.name}  ({dataset.hash})")
    print(f"source    {lineage['schema']}.{lineage['table']}")
    print(f"window    d={dataset.lookback}  h={horizon}  features={dataset.n_features}")
    print(
        f"samples   train {len(dataset.y_train)} | val {len(dataset.y_val)} "
        f"| test {len(dataset.y_test)}"
    )
    if dry_run:
        print("\ndry run — nothing fitted, nothing written")
        return None, None

    set_seed(config.get("seed", 42))
    # ⚠️ WALL-CLOCK, recorded in the run (2026-09-17): `fit_seconds` is the fit alone,
    # `run_seconds` the whole run including the 200-draw scoring — the trial log reads both.
    run_started = time.perf_counter()
    run = RunDir.create(base_dir=runs_dir, run_name=config["run_name"], config=config)

    spec = model_spec(config)
    arch = model_module.arch_dict(
        n_features=dataset.n_features, lookback=dataset.lookback, **spec
    )
    estimator = model_module.build_model(**arch["kwargs"])
    # ⚠️ THE DEVICE IS THE ESTIMATOR'S, AND THIS LINE USED TO HARD-CODE `"cpu"` (fixed
    # 2026-09-19). Every model on this path is sklearn-shaped, but three of them are
    # XGBoost underneath and take `device="cuda"` from their config, so the run folder and
    # the trial log were recording `cpu` for fits that ran on the card. A device is part of
    # the experimental setup for any SAMPLED XGBoost (`model/gbt/model.py`), so a metadata
    # field that cannot be wrong about it is the point of having one.
    est_device = str(getattr(estimator, "device", None)
                     or (getattr(estimator, "params", {}) or {}).get("device") or "cpu")
    run.update_metadata(dataset=dataset.reference(), device=est_device, lineage=lineage)
    # ⚠️ An estimator that must express a value in the ORIGINAL target units needs the
    # scaler, because everything here works in the scaled space and
    # `_write_predictions` inverts on the way out. `baseline.ZeroPredictor` is the case:
    # emitting 0.0 would inverse-transform to the train MEAN return, not to zero.
    if getattr(estimator, "needs_dataset", False):
        estimator.set_dataset(dataset)
    classify = task == M.CLASSIFICATION
    if classify:
        if not callable(getattr(estimator, "predict_logit", None)):
            raise ValueError(
                f"{model_type} has no `predict_logit`, so it has no classification "
                f"path — see this function's comment on the sigmoid."
            )
        if callable(getattr(estimator, "set_task", None)):
            estimator.set_task(task)

    # ⚠️ **THE WINDOW'S DTYPE IS THE ESTIMATOR'S CALL** (`input_dtype`, default float64 as
    # before). A tree declares float32: it rounds its input to float32 inside XGBoost or
    # sklearn regardless, and the float64 copy made here was 4.68 GiB at d=10 — the first
    # half of the out-of-memory that killed the basket forest on 2026-09-19. A linear
    # model keeps float64 because it does its arithmetic in whatever it is handed.
    in_dtype = getattr(estimator, "input_dtype", float)
    X_train = np.asarray(dataset.X_train, dtype=in_dtype)
    y_train = np.asarray(dataset.y_train, dtype=float).ravel()
    if classify:
        # The dataset holds the raw 0/1 (`_verify` refused a target scaler above).
        y_train = (y_train >= 0.5).astype(int)
    fit_started = time.perf_counter()
    estimator.fit(X_train, y_train)
    fit_seconds = time.perf_counter() - fit_started
    n_params = int(getattr(estimator, "n_params", 0))
    print(f"device    {est_device}   parameters {n_params:,}")

    raw = estimator.predict_logit if classify else estimator.predict

    def predict(split: str) -> np.ndarray:
        X = np.asarray(getattr(dataset, f"X_{split}"), dtype=in_dtype)
        return np.asarray(raw(X), dtype=float).ravel()

    fitted = np.asarray(raw(X_train), dtype=float).ravel()
    if classify:
        prob = np.clip(1.0 / (1.0 + np.exp(-fitted)), 1e-7, 1 - 1e-7)
        train_mse = float(-np.mean(y_train * np.log(prob) + (1 - y_train) * np.log(1 - prob)))
    else:
        train_mse = float(np.mean((fitted - y_train) ** 2))
    _write_predictions(predict, dataset, run, task)
    _write_importances(run, estimator, dataset)
    if classify:
        _write_calibration(run)

    # ⚠️ `loss_history()` is the estimator's own curve when it HAS one — XGBoost's
    # `evals_result` under `eval_set`, one row per boosting round. Everything else is a
    # single fit and writes the one row the contract requires; see `_write_loss_history`.
    curve = getattr(estimator, "loss_history", None)
    rows = curve() if callable(curve) else None
    if not rows:
        y_val = np.asarray(dataset.y_val, dtype=float).ravel()
        val_loss = (log_loss((y_val >= 0.5).astype(int), 1.0 / (1.0 + np.exp(-predict("val"))))
                    if classify else float(np.mean((predict("val") - y_val) ** 2)))
        rows = [{"step": 0, "train_loss": train_mse, "val_loss": val_loss}]
    _write_loss_history(run, rows)

    # ⚠️ An estimator that trains on rows OUTSIDE the dataset (`model.event_panel`'s peers)
    # says what they were here — the dataset hash cannot.
    provenance = getattr(estimator, "provenance", None)
    extra = {"provenance": provenance()} if callable(provenance) else {}
    best_iteration = getattr(estimator, "best_iteration", None)
    run.update_metadata(
        model={"type": model_type, **arch["kwargs"], "n_params": n_params, **extra},
        training={
            # ⚠️ Not epochs. See the docstring — the schema is shared with the torch
            # path and a blank would read as "not measured". A boosted model that
            # early-stopped puts its chosen ROUND here, which is the same quantity.
            "best_epoch": int(best_iteration) if best_iteration is not None else 0,
            "best_val_loss": float(rows[-1]["val_loss"]) if len(rows) > 1 else train_mse,
            "criterion": (
                "train log-loss, single fit (no training loop)" if classify
                else "closed-form / single fit (no training loop)"
            ),
            # ⚠️ THE LOSS THE MODEL ACTUALLY MINIMISED, from the estimator itself. The
            # line above is a SHAPE (one fit vs a loop) and three different objectives
            # used to share it word for word, which is how a squared-error member and a
            # log-loss member read identically in the trial log.
            "objective": str(getattr(estimator, "objective", "unknown")),
            "sample_weight": str(getattr(estimator, "sample_weight_rule", "uniform")),
            "early_stopping": str(getattr(estimator, "early_stopping_rule", "none")),
            "fitted_on": "train split only",
        },
    )

    table = evaluate_run(run.dir, draws=config.get("null_draws", M.NULL_DRAWS))
    scored = {split: table.loc[split].to_dict() for split in table.index}
    run.update_metadata(metrics=scored)
    run.update_metadata(timing={
        "fit_seconds": round(fit_seconds, 3),
        "run_seconds": round(time.perf_counter() - run_started, 3),
    })
    append_run(runs_dir, _registry_row(run, dataset, task, model_type, scored))

    print(f"\n{table.to_string()}")
    for split in table.index:
        print(f"\n{split}: {M.verdict(table.loc[split].to_dict())}")
    print(f"\nrun folder {run.dir}")
    return run.dir, table


def _cli_label(model_type: str, config: Dict, argv: Sequence[str]) -> str:
    """`model.<type>  <run_name>[  --dry-run]` — the one banner label both CLI
    entry points print, so `run_cli` and `run_estimator_cli` say the same thing
    for the same reason a run has ONE name (see `load_config`).
    """
    dry = "  --dry-run" if "--dry-run" in argv else ""
    return f"model.{model_type.lower()}  {config.get('run_name', '?')}{dry}"


def run_estimator_cli(
    model_module,
    model_type: str,
    config_dir: str,
    default_config: str,
    argv: Optional[Sequence[str]] = None,
):
    """`run_cli` for the estimator path.

    ⚠️ Wrapped in the same `runtime.RunTimer` as `run_cli`. A baseline's fit is
    fast enough that the banner might look like overkill, but the whole point of
    `train_estimator` is that a baseline is scored by the same code as a network
    (see its docstring) — exempting its CLI wrapper from the same runtime banner
    would make baseline rows the one kind of run without a `started`/`finished`/
    `gpu` line in the log.
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    path = argv[argv.index("--config") + 1] if "--config" in argv else default_config
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(config_dir, os.path.basename(path))
    config = load_config(path)
    resolved_type = model_type or str(config["model"].get("kind", "")).upper()

    with runtime.RunTimer(_cli_label(resolved_type, config, argv), device="cpu"):
        return train_estimator(
            config,
            model_module=model_module,
            model_type=resolved_type,
            dry_run="--dry-run" in argv,
        )


def _write_predictions(predict, dataset, run, task) -> None:
    """`results/predictions_<split>.csv` — the file `result_evaluator` reads.

    ⚠️ Regression predictions are inverse-transformed to the RETURN scale first.
    Scoring on the standardised target would make RMSE depend on the train slice's own
    variance and stop two datasets being comparable.

    Args:
        predict: `split -> np.ndarray` of raw model output, in the SCALED target space.
            ⚠️ A callable rather than a `Trainer`, so the torch path and the estimator
            path (`train_estimator`) write byte-identical files. This is the function
            whose two behaviours — the inverse transform above, and the `ticker` column
            below — decide whether two runs are comparable at all, so there is exactly
            one of it.
    """
    for split in ("val", "test"):
        raw = predict(split)
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


def feature_columns(dataset) -> list:
    """The dataset's own channel names, or `[]` — `train_test_creator` writes them."""
    return list(((dataset.meta or {}).get("features") or {}).get("feature_columns") or [])


def log_loss(y_true, y_prob) -> float:
    """Binary cross-entropy, the ONE loss this repo's classifiers minimise (§5 rule 25)."""
    p = np.clip(np.asarray(y_prob, dtype=float).ravel(), 1e-7, 1 - 1e-7)
    y = np.asarray(y_true, dtype=float).ravel()
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def _write_loss_history(run, rows) -> None:
    """`results/loss_history.csv` — `step, train_loss, val_loss`, written by EVERY run.

    ⚠️ **`step` IS WHATEVER THE FAMILY ITERATES OVER**, and the families disagree: an
    EPOCH on the torch path, a BOOSTING ROUND under XGBoost, and `0` for a model fitted
    in one shot — a logistic, a ridge, `xgbrf`'s single round of `num_parallel_tree`
    trees, the prior. **A model with no curve writes ONE ROW rather than no file**, so a
    missing file always means a bug and never "this family has no epochs"; the reader
    tells the two apart by the row count, which is the honest distinction.
    """
    frame = pd.DataFrame(rows, columns=["step", "train_loss", "val_loss"])
    frame.to_csv(os.path.join(run.results_dir, "loss_history.csv"), index=False)


def _write_importances(run, estimator, dataset) -> None:
    """`results/feature_importance.csv` — `feature, importance`, when the model has one.

    ⚠️ **NOT COMPARABLE ACROSS FAMILIES.** A tree reports GAIN over its splits and a
    linear model the absolute standardised coefficient; both answer "which channel did
    this model lean on" and neither is on the other's scale. The column is named
    `importance` and the metadata records `importance_kind`, because a chart that puts
    the two on one axis is the mistake this file cannot prevent.
    """
    importances = getattr(estimator, "importances", None)
    if not callable(importances):
        return
    scored = importances(feature_columns(dataset)) or {}
    if not scored:
        return
    frame = pd.DataFrame(sorted(scored.items(), key=lambda kv: -abs(kv[1])),
                         columns=["feature", "importance"])
    frame.to_csv(os.path.join(run.results_dir, "feature_importance.csv"), index=False)


def _write_calibration(run, bins: int = 10) -> None:
    """`results/calibration.csv` — equal-count bins of the predicted probability.

    ⚠️ **THE BASKET'S CUT IS A LEVEL, NOT A RANK** (`event_chain` §7): `min_prob` keeps a
    name only when P(event) clears it, so the model's probability has to MEAN something
    and the AUCs cannot see that. `mean_pred` against `observed` per bin is the check.
    Read back from `predictions_<split>.csv` rather than recomputed, so the file can
    never disagree with the predictions it describes.
    """
    rows = []
    for split in ("val", "test"):
        path = os.path.join(run.results_dir, f"predictions_{split}.csv")
        if not os.path.exists(path):
            continue
        frame = pd.read_csv(path)
        if "y_prob" not in frame:
            return
        # ⚠️ Equal COUNT, not equal width: at a base rate of 0.16 the top width-bins hold
        # a handful of rows and their observed rate is noise wearing a chart's authority.
        order = frame["y_prob"].rank(method="first")
        frame["bin"] = np.minimum((order * bins / (len(frame) + 1)).astype(int), bins - 1)
        for b, part in frame.groupby("bin"):
            rows.append({"split": split, "bin": int(b), "n": int(len(part)),
                         "mean_pred": float(part["y_prob"].mean()),
                         "observed": float(part["y_true"].mean())})
    if rows:
        pd.DataFrame(rows).to_csv(
            os.path.join(run.results_dir, "calibration.csv"), index=False)


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
    """One `index.csv` row, built by the SAME function `--rebuild-index` uses.

    ⚠️ This used to construct the dict here, and `result_evaluator.rebuild_index`
    constructed a different one — two writers, one file, two schemas. Both now go
    through `result_evaluator.index.index_row`, so a rebuilt leaderboard is the same
    shape as an appended one.
    """
    return index_row(
        run_id=run.run_id,
        scored=scored,
        meta=run.metadata,
        run_dir=os.path.relpath(run.dir, _SRC),
        verdict=M.verdict(scored.get("test", {})) if scored.get("test") else "",
    )


def run_cli(
    model_module,
    model_type: str,
    config_dir: str,
    default_config: str,
    argv: Optional[Sequence[str]] = None,
):
    """`python -m model.<name> [--config PATH] [--dry-run]`, for any model package.

    ⚠️ **The banner is here, not in the six bindings.** `model/lstm/train.py` and its
    five siblings are ~30 lines each that name a model module and a config directory
    (`.claude/context/model.md` §7); anything they would all have to repeat belongs in the
    engine, which is the same rule that put `_verify` and `_write_predictions` here.
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    path = argv[argv.index("--config") + 1] if "--config" in argv else default_config
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(config_dir, os.path.basename(path))
    config = load_config(path)

    # ⚠️ The device is `config["device"]` — a PREFERENCE, usually `auto` — and it is
    # printed here as one. `train` resolves it and prints the resolved device on its
    # own `device …  parameters …` line; the finish banner then repeats the request.
    # Both are true and they are different facts (`utils/runtime.gpu_report`).
    with runtime.RunTimer(
        _cli_label(model_type, config, argv),
        device=str(config.get("device", "auto")),
    ):
        return train(
            config,
            model_module=model_module,
            model_type=model_type,
            dry_run="--dry-run" in argv,
        )
