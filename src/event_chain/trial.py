"""The TRIAL LOG — every COMPLETE run of the event chain, one row per model run.

```
reports/event_chain/
    trials.csv                      THE LOG: one row per MODEL RUN of a complete trial —
                                    data, features, target, split, model, hyperparameters,
                                    run time, device/GPU, results. Rebuilt from the folders below.
    trials/<trial_id>/
        trial.json                  THE RECORD: every input and every result of one trial
        leaderboard.csv             event metrics per model, val and test
        selection.csv               the selection runs the table was built from
        walkforward.csv             yearly expanding refits (when computed)
        predictions.csv             every model's val/test `date, y_true, y_prob`
        report.md                   the report exactly as printed
```

    python -m event_chain.trial                 # print the log
    python -m event_chain.trial --rebuild       # regenerate trials.csv from the trial folders
    python -m event_chain.trial --show <id>     # print one trial's headline

⚠️ **A FIXED ENSEMBLE IS A ROW; THE TOP-3 BLEND IS NOT** (2026-09-17). An ensemble named in
`config.SETUPS` is a model whose composition was decided before the chain ran, so it is
logged like a run (its `run_seconds` is the sum of its members'); the blend is picked on val
and stays in `trial.json` only.

⚠️ **A TRIAL IS LOGGED ONLY WHEN IT COMPLETED** (decided 2026-09-17): the `report` stage ran on
a dataset with trained models. Probes, crashed attempts and data refreshes are NOT rows —
they have no data/feature/target/split/model/result to record.

⚠️ **`trials.csv` IS DERIVED, `trial.json` IS THE SOURCE.** The CSV is rebuilt from every
`trials/*/trial.json` on each record, so a row can never disagree with its folder and a
deleted folder removes its rows.

⚠️ **`predictions.csv` IS WHAT MAKES A TRIAL SURVIVE ITS RUN FOLDERS.** Run folders under
`src/model/runs/` are git-ignored and 29 of them were deleted once with no way back
(`RPR-1`). Every metric in `trial.json` can be recomputed from this file alone.

⚠️ **`code.digest` IS THE SHA-256 OF THE CODE THAT PRODUCED THE NUMBERS**, dirty tree
included — a git commit alone cannot name an uncommitted change.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from event_chain import config as C

TRIALS_DIR = os.path.join(C.OUTPUT_ROOT, "trials")
TRIALS_LOG = os.path.join(C.OUTPUT_ROOT, "trials.csv")
SCHEMA_VERSION = 3

# The code whose bytes decide a trial's numbers. Globs, relative to the repo root.
CODE_FILES = (
    "src/utils/event_target.py",
    "src/event_chain/*.py",
    "src/feature_selection/run.py",
    "src/feature_selection/selector.py",
    "src/final_features/builder.py",
    "src/train_test_creator/dataset.py",
    "src/model/common/*.py",
    "src/model/baseline/model.py",
    "src/model/gbt/model.py",
    "src/model/forest/model.py",
    "src/model/event_linear/model.py",
    "src/model/event_panel/model.py",
    "src/model/event_boost/model.py",
    "src/model/lstm/model.py",
    "src/model/gru/model.py",
    "src/model/cnn/model.py",
    "src/model/mlp/model.py",
    "src/model/tcn/model.py",
    "src/result_evaluator/*.py",
    "src/orchestration/preprocessor/preprocessor.py",
    "src/backtest/portfolio.py",
)

# ONE ROW PER MODEL RUN. Grouped: key | data | features | target | split | model | run time
# and hardware | results. Nothing else belongs in the log; the rest is in `trial.json`.
LOG_COLUMNS = [
    # which stock — FIRST, because the log holds more than one ticker since 2026-09-17 (MBB)
    "exchange", "ticker",
    # key
    "run_id", "trial_id", "started_at",
    # data
    "data_table", "data_from", "data_to",
    # features
    "feature_pools", "n_features", "lookback_d", "feature_selection",
    # target
    "target", "target_definition",
    # split
    "split_ratio", "purge_gap", "train_period", "val_period", "test_period",
    "train_samples", "val_samples", "test_samples",
    "train_positive_rate", "val_positive_rate", "test_positive_rate",
    # model
    "model", "model_variant", "hyperparameters", "n_params", "best_epoch", "seed",
    # run time and hardware
    "run_seconds", "fit_seconds", "device", "gpu",
    # results
    "val_auc", "test_auc", "test_auc_null_p95", "test_auc_z", "test_beats_null",
    "test_auc_refit_train_val", "test_auc_refit_null_p95",
    "test_auc_rolling_refit", "test_auc_rolling_refit_null_p95",
    "test_pr_auc", "test_precision_top10pct", "test_brier_skill",
    "test_precision_at_val_threshold", "test_recall_at_val_threshold", "chosen_on_val",
    # ⚠️ BASKET setups only (2026-09-17, `event_chain.basket`) — blank for a single-ticker trial:
    # the share of each session's top-k that hit, its buyable base rate, the random-basket null,
    # the basket's h-session return against the universe's, and the non-overlapping track.
    "top_k", "val_hit_at_k", "test_hit_at_k", "test_base_rate_buyable", "test_hit_null_p95",
    "test_hit_null_max", "test_hit_z", "test_all_hit", "test_basket_return", "test_universe_return",
    # ⚠️ THE WITHIN-SESSION AUC IS THE BASKET'S CHOICE METRIC (2026-09-18,
    # `config.BASKET_CHOOSE_ON = "val_daily_auc"`): the ROC-AUC inside each session, averaged.
    # The pooled `val_auc`/`test_auc` columns above are kept because every earlier trial is
    # logged in them, and they read ~0.02 higher — a pooled AUC is also paid for knowing which
    # SESSIONS are eventful, which no basket trades.
    "val_daily_auc", "test_daily_auc", "test_daily_auc_null_p95", "test_daily_auc_z",
    "test_daily_auc_refit_train_val", "test_daily_auc_rolling_refit",
    "test_sharpe_50bps", "test_cagr_50bps", "test_hit_refit_train_val",
    "test_hit_rolling_refit", "test_sharpe_50bps_rolling_refit",
    # the val-chosen P(event) cut (at most k names, possibly none), on the chosen row only
    "min_prob", "test_cut_active_share", "test_cut_precision", "test_cut_basket_return",
    "test_cut_sharpe_50bps", "test_cut_precision_rolling_refit", "test_cut_sharpe_50bps_rolling_refit",
]


# ------------------------------------------------------------------ helpers
def _rel(path: str) -> str:
    """Repo-relative with forward slashes; absolute when the path is on another drive."""
    try:
        return os.path.relpath(path, C.REPO_ROOT).replace("\\", "/")
    except ValueError:
        return os.path.abspath(path).replace("\\", "/")


def _jsonable(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp,)):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def _clean(obj):
    """Recursively replace NaN/inf with None so the JSON is valid JSON."""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, float) and not np.isfinite(obj):
        return None
    if isinstance(obj, np.floating):
        return None if not np.isfinite(obj) else float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def _row_dict(row: pd.Series) -> Dict:
    return _clean({k: (None if (isinstance(v, float) and not np.isfinite(v)) else v)
                   for k, v in row.to_dict().items()})


def _git() -> Dict:
    def run(*args: str) -> str:
        try:
            return subprocess.run(
                ["git", *args], cwd=C.REPO_ROOT, capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=30,
            ).stdout.strip()
        except Exception:  # noqa: BLE001 — a machine without git still logs a trial
            return ""

    dirty = [line[3:] for line in run("status", "--porcelain").splitlines() if line.strip()]
    return {
        "commit": run("rev-parse", "HEAD"),
        "branch": run("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty_files": len(dirty),
        "dirty": dirty[:200],
    }


def code_digest() -> Dict:
    files = sorted({f for pattern in CODE_FILES
                    for f in glob.glob(os.path.join(C.REPO_ROOT, pattern))})
    digest = hashlib.sha256()
    per_file = {}
    for path in files:
        with open(path, "rb") as fh:
            data = fh.read().replace(b"\r\n", b"\n")  # autocrlf must not move the digest
        rel = _rel(path)
        digest.update(rel.encode() + b"\0" + data)
        per_file[rel] = hashlib.sha256(data).hexdigest()[:12]
    return {"digest": digest.hexdigest(), "files": per_file}


def _environment() -> Dict:
    from importlib import metadata

    packages = {}
    for name in ("numpy", "pandas", "scikit-learn", "xgboost", "torch", "psycopg2-binary",
                 "psycopg2", "dagster", "scipy"):
        try:
            packages[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
    env = {"python": sys.version.split()[0], "platform": platform.platform(),
           "machine": platform.machine(), "cpu_count": os.cpu_count(), "packages": packages}
    try:
        from utils import runtime

        env["runtime"] = runtime.environment()
    except Exception as error:  # noqa: BLE001 — recorded, not fatal
        env["runtime"] = f"unavailable ({type(error).__name__})"
    return env


def _pool_stats(chain, pools: Sequence[str]) -> Dict:
    from feature_selection.unified_reader import UnifiedSchemaReader

    out = {}
    comment = None
    with UnifiedSchemaReader(chain.ticker) as reader:
        present = set(reader.tables())
        with reader.driver._cursor_ctx() as cur:
            for pool in pools:
                if pool not in present:
                    out[pool] = {"exists": False}
                    continue
                cur.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {chain.schema}.{pool}")
                rows, first, last = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s", (chain.schema, pool))
                out[pool] = {"exists": True, "rows": int(rows), "columns": int(cur.fetchone()[0]),
                             "first_date": str(first), "last_date": str(last)}
            if chain.table in present:
                cur.execute("SELECT obj_description(%s::regclass)",
                            (f"{chain.schema}.{chain.table}",))
                comment = cur.fetchone()[0]
    return {"pools": out, "final_table_comment": comment}


def exchange(chain) -> Optional[str]:
    """The exchange(s) the ticker's labelled rows were listed on, oldest first (`HOSE`).

    ⚠️ Read from the rows the label was computed on, not from a listing table: a ticker that
    moved (HNX -> HOSE) carries both, joined by `>`, in the order it traded on them.
    """
    _, labelled = chain.labelled_dates()
    if "exchange" not in labelled.columns:
        return None
    first = labelled.groupby("exchange")["date"].min().sort_values()
    return ">".join(str(e) for e in first.index) or None


def _label_stats(chain) -> Dict:
    dates, labelled = chain.labelled_dates()
    y = labelled[chain.event.column].astype(float)
    by_year = labelled.assign(year=pd.to_datetime(labelled["date"]).dt.year).groupby("year")[
        chain.event.column].agg(["count", "mean"])
    return {
        "column": chain.event.column,
        "labelled_sessions": int(len(y)),
        "positives": int(y.sum()),
        "base_rate": float(y.mean()),
        "first_date": str(dates.iloc[0].date()),
        "last_date": str(dates.iloc[-1].date()),
        "base_rate_by_year": {int(k): {"sessions": int(v["count"]), "base_rate": float(v["mean"])}
                              for k, v in by_year.iterrows()},
    }


def _selection(chain, runs: pd.DataFrame, feature_columns: Sequence[str]) -> List[Dict]:
    features = set(feature_columns)
    out = []
    for _, run in runs.iterrows():
        with open(os.path.join(run["dir"], "metadata.json"), encoding="utf-8") as fh:
            meta = json.load(fh)
        shortlist_path = os.path.join(run["dir"], "outstanding.csv")
        shortlist = pd.read_csv(shortlist_path) if os.path.exists(shortlist_path) else pd.DataFrame()
        channels = list(shortlist["channel"]) if "channel" in shortlist else []
        setup = meta.get("setup") or {}
        out.append(_clean({
            "run_id": run["run_id"],
            "pool": run["tables"],
            "started_at": str(run.get("started_at")),
            "channels_offered": setup.get("channels"),
            "kept": setup.get("kept"),
            "shortlisted": len(channels),
            "shortlist": channels,
            "in_final_table": sorted(features.intersection(channels)),
            # ⚠️ **WHICH OF THE DATASET'S CHANNELS CAME FROM THIS POOL** (added 2026-09-19).
            # `trial.json` carried a FLAT list of 151 feature columns and a per-pool KEPT
            # COUNT, so nothing on disk said which channel belonged to which pool — a chart
            # grouped by feature group had to join three folders to find out, and the
            # selection folder is the one artefact a re-run overwrites. The list is the
            # intersection of the pool's own ranked channels with the built design, so a
            # channel two pools both offer appears under both, which is the truth.
            "columns_in_design": sorted(features.intersection(
                _pool_channels(run["dir"]))),
            "evidence": shortlist["evidence"].iloc[0] if "evidence" in shortlist and len(shortlist) else None,
            "cv_ic": run.get("ic"),
            "null": meta.get("null"),
            "holdout": meta.get("holdout"),
            "setup": {k: setup.get(k) for k in (
                "target", "lookback_d", "horizon_h", "normalize", "purge_gap_rows", "window_stats",
                "dev_samples", "holdout_start", "holdout_rows", "design_columns", "device",
                "methods", "max_features", "corr_threshold", "n_splits", "min_train",
                "random_state", "design_dtype")},
            "execution": meta.get("execution"),
            "git_commit": meta.get("git_commit"),
        }))
    return out


def _timing(folder: str, meta: Dict) -> Dict:
    """The run's own clock when it recorded one; otherwise the folder's file times.

    ⚠️ Runs made before 2026-09-17 carry no `timing` block, so `run_seconds` is then the
    gap between `config.yaml` (written as the folder is created) and `results/metrics.json`
    (written after scoring) — the same span, measured by the filesystem instead.
    """
    if meta.get("timing"):
        return {**meta["timing"], "source": "engine clock"}
    try:
        start = os.path.getmtime(os.path.join(folder, "config.yaml"))
        end = os.path.getmtime(os.path.join(folder, "results", "metrics.json"))
        return {"fit_seconds": None, "run_seconds": round(end - start, 3), "source": "file mtimes"}
    except OSError:
        return {}


def _models(board: pd.DataFrame, runs_dir: str) -> List[Dict]:
    import yaml

    out = []
    for _, row in board.iterrows():
        record = {"event_metrics": _row_dict(row)}
        run_id = row.get("run_id")
        folder = os.path.join(runs_dir, run_id) if run_id else None
        if folder and os.path.isdir(folder):
            cfg_path = os.path.join(folder, "config.yaml")
            if os.path.exists(cfg_path):
                with open(cfg_path, encoding="utf-8") as fh:
                    record["config"] = yaml.safe_load(fh)
            meta = {}
            meta_path = os.path.join(folder, "metadata.json")
            if os.path.exists(meta_path):
                with open(meta_path, encoding="utf-8") as fh:
                    meta = json.load(fh)
            record["model"] = meta.get("model")
            record["training"] = meta.get("training")
            record["device"] = meta.get("device")
            record["env"] = meta.get("env")
            record["created_at"] = meta.get("created_at")
            record["timing"] = _timing(folder, meta)
            metrics_path = os.path.join(folder, "results", "metrics.json")
            if os.path.exists(metrics_path):
                with open(metrics_path, encoding="utf-8") as fh:
                    record["result_evaluator"] = json.load(fh)
        record["run_id"] = run_id
        record["run_name"] = row.get("run_name")
        record["model_type"] = row.get("model_type")
        out.append(_clean(record))
    return out


def _predictions(board: pd.DataFrame, runs_dir: str) -> pd.DataFrame:
    frames = []
    for _, row in board.iterrows():
        run_id = row.get("run_id")
        if not run_id:
            continue
        for split in ("val", "test"):
            path = os.path.join(runs_dir, run_id, "results", f"predictions_{split}.csv")
            if os.path.exists(path):
                frame = pd.read_csv(path)
                frame.insert(0, "split", split)
                frame.insert(0, "run_name", row["run_name"])
                frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _pool_channels(selection_dir: str) -> set:
    """Every channel this selection run ranked — `feature_importance.csv`'s own column."""
    path = os.path.join(selection_dir, "feature_importance.csv")
    if not os.path.exists(path):
        return set()
    frame = pd.read_csv(path)
    return set(frame["channel"].astype(str)) if "channel" in frame else set()


def _loss_block(board: pd.DataFrame, runs_dir: str) -> dict:
    """The grid's loss rule, plus the objective every run actually recorded.

    ⚠️ **THE GRID CARRIED THREE LOSSES UNTIL 2026-09-19 AND THE RUN FOLDERS SAID SO IN ONE
    SENTENCE**: `training.criterion` read `"train log-loss, single fit (no training loop)"`
    for a `binary:logistic` classifier, a `reg:squarederror` regressor and a
    `rank:pairwise` ranker alike, because it describes the SHAPE of the fit and not the
    function. `per_model` below is read from each run's own `training.objective`, so a
    member that drifts off the rule is visible here rather than inferable from a config.
    """
    per_model = {}
    for _, row in board.iterrows():
        run_id = row.get("run_id")
        if not run_id:
            # An ensemble fits nothing; the geometric mean has no loss of its own.
            per_model[row["run_name"].split("__")[0]] = "none (fixed geometric mean)"
            continue
        path = os.path.join(runs_dir, run_id, "metadata.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                training = json.load(fh).get("training") or {}
            per_model[row["run_name"].split("__")[0]] = {
                "objective": training.get("objective"),
                "sample_weight": training.get("sample_weight"),
                "early_stopping": training.get("early_stopping"),
                "best_round": training.get("best_epoch"),
            }
    return {
        "rule": "binary cross-entropy on the 0/1 event label, unweighted, no class weighting",
        "chosen_because": (
            "the decision has two halves and both were measured: 84 % of the edge is "
            "choosing the NAME inside a session (a rank) and 16 % is choosing the SESSION "
            "through the min_prob cut (a LEVEL). Log-loss is a proper scoring rule, so one "
            "number serves both; rank:pairwise is invariant to monotone transforms inside a "
            "query, so its scores cannot be compared across sessions and the cut is lost."
        ),
        "would_flip_if": "the cut is dropped and a fixed basket is traded every session",
        "early_stopping": (
            "val log-loss. ⚠️ VAL NOW CHOOSES THE MODEL, THE CUT AND THE ROUND, so "
            "val_daily_auc is a selection score and TEST is the only honest read. The refit "
            "paths have no val and reuse the frozen fit's round."
        ),
        "families_that_cannot_stop": (
            "a forest is one boosting round, a logistic is convex (capacity is C/alpha), "
            "and the prior does not fit — they write a one-row loss_history.csv"
        ),
        "per_model": per_model,
    }


def _copy_curves(board: pd.DataFrame, runs_dir: str, folder: str) -> int:
    """Copy each run's small per-fit artefacts into the trial folder, which git TRACKS.

    ⚠️ **`src/model/runs/*/` IS GITIGNORED** (only `index.csv` is tracked) and `RPR-1`
    deleted 29 run folders once. A learning curve, an importance table and a calibration
    table are a few KB each and are the inputs to every chart this trial will ever justify,
    so they live where the trial lives. The predictions stay behind: those are megabytes,
    and `predictions_wide.csv.gz` already carries them.
    """
    copied = 0
    curves = os.path.join(folder, "curves")
    for _, row in board.iterrows():
        run_id = row.get("run_id")
        if not run_id:
            continue
        short = row["run_name"].split("__")[0]
        for artefact in ("loss_history.csv", "feature_importance.csv", "calibration.csv"):
            path = os.path.join(runs_dir, run_id, "results", artefact)
            if os.path.exists(path):
                os.makedirs(curves, exist_ok=True)
                shutil.copyfile(path, os.path.join(curves, f"{short}__{artefact}"))
                copied += 1
    return copied


def _with_ensembles(wide: pd.DataFrame, chain) -> pd.DataFrame:
    """Add one column per FIXED ensemble — the geometric mean `event_chain.report` scored.

    ⚠️ **THE CHOSEN MODEL HAS BEEN AN ENSEMBLE ON EVERY BASKET TRIAL, AND IT WAS THE ONE
    ROW THIS FILE DID NOT CARRY** (fixed 2026-09-19). An ensemble has no run folder — its
    `run_id` is empty by construction, because nothing fits it — so `_predictions` skipped
    it and `predictions_wide.csv.gz` held the eight members and not the winner. It is
    REPRODUCIBLE from them (a member listed twice counts twice, so `ensemble_xl` is
    `xgb^(2/3) · evt^(1/3)`), and reproducible is not recorded: every chart of the chosen
    model had to recompute it, from a rule stored in a different file.

    ⚠️ An ensemble whose member is missing from the frame is SKIPPED rather than computed
    from what is there — a geometric mean over a subset is a different model wearing the
    same name.
    """
    for name, members in (getattr(chain, "ensembles", None) or {}).items():
        if any(m not in wide.columns for m in members):
            continue
        product = np.ones(len(wide))
        for member in members:
            product = product * np.clip(wide[member].to_numpy(dtype=float), 1e-9, 1.0)
        wide[name] = product ** (1.0 / len(members))
    return wide


def _trial_id(started_at: pd.Timestamp, chain) -> str:
    return f"{started_at:%Y%m%d-%H%M%S}__{chain.ticker.lower()}__{chain.table}"


# ------------------------------------------------------------------- record
def record(chain, built: Dict, notes: str = "",
           stages: Optional[List[Dict]] = None, argv: Optional[Sequence[str]] = None,
           started_at: Optional[pd.Timestamp] = None, runs_dir: Optional[str] = None,
           kind: str = "trial") -> str:
    """Write `trials/<trial_id>/` for a COMPLETE trial and rebuild `trials.csv`."""
    from model.common import engine
    from model.common.data import load_dataset
    from event_chain import report

    board: pd.DataFrame = built["board"]
    if board.empty:
        raise ValueError("no scored model run — an incomplete trial is not logged")
    runs_dir = runs_dir or engine.RUNS_DIR
    started_at = pd.Timestamp(started_at) if started_at is not None else pd.Timestamp.now()
    trial_id = _trial_id(started_at, chain)
    folder = os.path.join(TRIALS_DIR, trial_id)
    os.makedirs(folder, exist_ok=True)

    dataset = load_dataset(chain.creator().name)
    meta = dataset.meta or {}
    selection_runs: pd.DataFrame = built["selection"]
    wf: pd.DataFrame = built["walkforward"]
    features = (meta.get("features") or {}).get("feature_columns", [])
    selection = _selection(chain, selection_runs, features) if not selection_runs.empty else []
    best = report.best_on_val(board)
    blend = board[board["model_type"] == "BLEND"]

    split_meta = meta.get("split", {})
    splits = {}
    for s in ("train", "val", "test"):
        ys = getattr(dataset, f"y_{s}")
        rng = split_meta.get("date_ranges", {}).get(s, [None, None])
        splits[s] = {"first_label_date": rng[0], "last_label_date": rng[1], "samples": int(len(ys)),
                     "positives": int(ys.sum()), "base_rate": float(ys.mean())}

    significant = None
    verdict = "no eligible model"
    basket = built.get("basket")
    if best is not None and basket:
        significant = bool(best.get("test_hit", np.nan) > best.get("test_hit_bar", np.nan))
        verdict = (
            f"best on val `{best['run_name'].split('__')[0]}`: val within-session AUC "
            f"{best.get('val_daily_auc', float('nan')):.3f} (pooled {best.get('val_auc', float('nan')):.3f}), "
            f"test within-session AUC {best.get('test_daily_auc', float('nan')):.3f} vs a "
            f"within-session shuffle null p95 {best.get('test_daily_auc_bar', float('nan')):.3f}; "
            f"val hit@{basket['top_k']} "
            f"{best['val_hit']:.3f}, test hit {best['test_hit']:.3f} vs buyable base "
            f"{best['test_base']:.3f} and a random-basket null p95 {best['test_hit_bar']:.3f} "
            f"(max {best['test_hit_null_max']:.3f}, z {best['test_hit_z']:+.2f}); basket return "
            f"{best['test_bret']:+.4f} vs universe {best['test_uret']:+.4f} per {chain.horizon} sessions — "
            + ("CLEARS the per-run null (the grid is not priced, NUL-1)." if significant
               else "does NOT clear the null: no demonstrated skill on test.")
        )
    elif best is not None:
        significant = bool(best.get("test_auc", np.nan) > best.get("test_auc_bar", np.nan))
        verdict = (
            f"best on val `{best['run_name'].split('__')[0]}`: val AUC {best['val_auc']:.3f}, "
            f"test AUC {best['test_auc']:.3f} vs block-shuffled null p95 {best['test_auc_bar']:.3f} "
            f"(max {best['test_auc_null_max']:.3f}, z {best['test_auc_z']:+.2f}) on "
            f"{splits['test']['positives']} test positives — "
            + ("CLEARS the per-run null (the grid is not priced, NUL-1)." if significant
               else "does NOT clear the null: no demonstrated skill on test.")
        )

    pools = list(dict.fromkeys(list(chain.pools) + ["pool__targets"]))
    try:
        data = _pool_stats(chain, pools)
    except Exception as error:  # noqa: BLE001 — a read failure must not lose the trial
        data = {"error": f"{type(error).__name__}: {error}"}

    git = _git()
    code = code_digest()
    body = {
        "schema_version": SCHEMA_VERSION,
        "trial": {
            "id": trial_id, "kind": kind, "started_at": str(started_at.round("s")),
            "logged_at": str(pd.Timestamp.now().round("s")), "notes": notes,
            "argv": list(argv or []), "stages": stages or [], "runs_dir": _rel(runs_dir),
            "output_dir": _rel(built["output_dir"]),
        },
        "code": {"git": git, "digest": code["digest"], "files": code["files"]},
        "environment": _environment(),
        "event": {"column": chain.event.column, "gain_pct": chain.event.gain_pct,
                  "horizon": chain.event.horizon, "rule": chain.event.rule,
                  "definition": chain.event.describe(),
                  "defined_in": "src/utils/event_target.py"},
        "universe": {"exchange": exchange(chain), "ticker": chain.ticker, "schema": chain.schema},
        "data": {**data, "label": _label_stats(chain)},
        "split": {"train_ratio": C.TRAIN_RATIO, "val_ratio": C.VAL_RATIO,
                  "holdout_start": chain.holdout_start(),
                  "val_start_date": split_meta.get("val_start_date"),
                  "test_start_date": split_meta.get("test_start_date"),
                  "purge_gap_rows": split_meta.get("purge_gap_rows"),
                  "purge_rule": split_meta.get("purge_rule"), "splits": splits},
        "setup": {"name": getattr(chain, "setup", "window"), "channels": getattr(chain, "channels", "shortlist"),
                  "aux_targets": list(getattr(chain, "aux_targets", ()) or []),
                  "ensembles": {k: list(v) for k, v in (getattr(chain, "ensembles", {}) or {}).items()},
                  "declared_models": list(getattr(chain, "run_names", []) or []),
                  "scored_models": sorted(board["run_name"].tolist()) if not board.empty else []},
        # ⚠️ ONE LOSS FOR THE WHOLE GRID, written down so a later reader does not have to
        # infer it from nine `hyperparameters` blobs. `objective` per model is in
        # `models[].training.objective`; this is the GRID's rule and what it refuses.
        "loss": _loss_block(board, runs_dir),
        "selection": {"root": _rel(chain.root), "lookback": chain.lookback,
                      "null_draws_configured": chain.null_draws,
                      "pools_offered": list(chain.pools), "runs": selection},
        "final_table": {"schema": chain.schema, "table": chain.table, "scope": chain.scope,
                        "scope_pools": list(chain.scope_pools or []),
                        "keep_failed_pools": chain.keep_failed,
                        "pools_in_table": sorted({r["pool"] for r in selection if r["in_final_table"]}),
                        "channels": len(features)},
        "dataset": {"name": dataset.name, "hash": dataset.hash, "n_features": dataset.n_features,
                    "lookback": dataset.lookback, "task": meta.get("task"),
                    "target": meta.get("target"), "features": meta.get("features"),
                    "imputation": meta.get("imputation"), "drift": meta.get("drift"),
                    "shapes": meta.get("shapes"), "created_at": meta.get("created_at_tz")},
        "basket": ({**basket, "null_draws": C.BASKET_NULL_DRAWS,
                    "refit_sessions": C.BASKET_REFIT_SESSIONS, "costs": list(C.BASKET_COSTS),
                    "best_rule": f"max {C.BASKET_CHOOSE_ON} (the other val metrics break a tie), "
                                 f"excluding BASELINE_PRIOR",
                    "entry_screen": ("no name at its exchange ceiling on N (PRF-0)" if C.BASKET_EXCLUDE_CEILING
                                     else "none: a name at its ceiling on N is bought"),
                    "min_prob_rule": (f"max val {C.BASKET_MIN_PROB_ON} over {len(C.BASKET_MIN_PROB_GRID)} cuts, "
                                      f">= {C.BASKET_MIN_ACTIVE} active val sessions")} if basket else None),
        "report_config": {"null_draws": C.REPORT_NULL_DRAWS,
                          "null_block": int(dataset.lookback + chain.horizon),
                          "best_rule": "max VAL ROC-AUC, excluding BASELINE_PRIOR and BLEND "
                                       "(a FIXED ensemble is eligible)",
                          "refit_rule": "the run's own config refitted on train+val, test scored once",
                          "rolling_rule": f"refitted every {C.ROLLING_REFIT_SESSIONS} test sessions on all rows "
                                          "ending d+h-1 samples before the block; blocks pooled",
                          "threshold_rule": "VAL threshold maximising F1, applied once to TEST"},
        "models": _models(board, runs_dir),
        "best": _row_dict(best) if best is not None else None,
        "blend": _row_dict(blend.iloc[0]) if not blend.empty else None,
        "walkforward": wf.to_dict(orient="records") if isinstance(wf, pd.DataFrame) else [],
        "verdict": {"significant_on_test": significant, "text": verdict},
    }
    with open(os.path.join(folder, "trial.json"), "w", encoding="utf-8") as fh:
        json.dump(_clean(body), fh, indent=2, ensure_ascii=False, default=_jsonable)
    board.to_csv(os.path.join(folder, "leaderboard.csv"), index=False)
    if not selection_runs.empty:
        selection_runs.drop(columns=["dir"]).to_csv(os.path.join(folder, "selection.csv"), index=False)
    if isinstance(wf, pd.DataFrame) and not wf.empty:
        wf.to_csv(os.path.join(folder, "walkforward.csv"), index=False)
    predictions = _predictions(board, runs_dir)
    if basket and not predictions.empty:
        # ⚠️ A PANEL'S PREDICTIONS ARE ~2M ROWS LONG: one column per run, gzipped, so the
        # trial still outlives its run folders (`RPR-1`) without a 60 MB file in git.
        wide = predictions.assign(run=predictions["run_name"].str.split("__").str[0]).pivot_table(
            index=["split", "date", "ticker", "y_true"], columns="run", values="y_prob", aggfunc="first")
        wide = _with_ensembles(wide, chain)
        wide.reset_index().to_csv(os.path.join(folder, "predictions_wide.csv.gz"), index=False,
                                  float_format="%.5g", compression="gzip")
    elif not predictions.empty:
        # 6 significant digits: re-scorable to 1e-6, and half the bytes of repr floats.
        predictions.to_csv(os.path.join(folder, "predictions.csv"), index=False,
                           float_format="%.6g")
    _copy_curves(board, runs_dir, folder)
    shutil.copyfile(built["path"], os.path.join(folder, "report.md"))
    rebuild_log()
    return folder


# ---------------------------------------------------------------------- log
def _period(split: Dict) -> str:
    return f"{split.get('first_label_date')} -> {split.get('last_label_date')}"


def _hyperparameters(config: Dict) -> str:
    """The model block minus its label, plus the training block — as compact JSON."""
    model = {k: v for k, v in (config.get("model") or {}).items() if k != "type"}
    out = {"model": model}
    if config.get("train"):
        out["train"] = config["train"]
    return json.dumps(out, sort_keys=True, separators=(",", ":"))


def log_rows(body: Dict) -> List[Dict]:
    """`trials.csv` rows for one `trial.json` — one per MODEL RUN, the blend excluded."""
    split = body["split"]
    splits = split["splits"]
    event = body["event"]
    selection = body.get("selection") or {}
    final = body.get("final_table") or {}
    best = body.get("best") or {}
    runtime_env = (body.get("environment") or {}).get("runtime") or {}
    machine_gpu = (runtime_env.get("gpu") or {}).get("name") if isinstance(runtime_env, dict) else None
    dataset = body.get("dataset") or {}
    test_ratio = 1 - split["train_ratio"] - split["val_ratio"]
    ratio = f"train {split['train_ratio']:.0%} / val {split['val_ratio']:.0%} / test {test_ratio:.0%}"
    by_name = {m.get("run_name"): m for m in body.get("models", [])}
    rows = []
    for m in body.get("models", []):
        ensemble = m.get("model_type") == "ENSEMBLE"
        if m.get("model_type") == "BLEND" or not (m.get("run_id") or ensemble):
            continue
        em = m.get("event_metrics") or {}
        config = m.get("config") or {}
        timing = m.get("timing") or {}
        device = m.get("device") or config.get("device")
        run_id = m.get("run_id")
        if ensemble:
            listed = [n for n in str(em.get("ensemble_members", "")).split(",") if n]
            # ⚠️ A member listed twice is WEIGHTED twice; its time is counted once.
            members = [by_name.get(n) or {} for n in dict.fromkeys(listed)]
            run_id = f"{m.get('run_name')}__{body['trial']['id']}"
            config = {"model": {"combine": "geometric mean of member probabilities (a member listed "
                                           "twice counts twice)",
                                "members": [n.split("__")[0] for n in listed]}}
            member_timing = [x.get("timing") or {} for x in members]
            timing = {k: round(sum((t.get(k) or 0) for t in member_timing), 3)
                      for k in ("run_seconds", "fit_seconds")}
            devices = {str(x.get("device") or (x.get("config") or {}).get("device")) for x in members}
            device = "+".join(sorted(devices)) if devices else "cpu"
        # ⚠️ `startswith` WAS WRONG FOR AN ENSEMBLE, whose `device` is the JOINED set of its
        # members' (`cpu+cuda`): the string starts with `cpu`, so a row whose two XGBoost
        # members fitted on the card logged `gpu: not used`. That is the second half of
        # `DEV-1` — the first was the engine writing a literal `cpu` — and it reads as a
        # different experiment, since a sampled XGBoost draws from another RNG stream on
        # CUDA. Any member on the card makes the row a CUDA row.
        gpu = ((m.get("env") or {}).get("cuda_device") or machine_gpu) if "cuda" in str(device) else ""
        rows.append({
            "exchange": body["universe"].get("exchange"),
            "ticker": body["universe"]["ticker"],
            "run_id": run_id, "trial_id": body["trial"]["id"],
            "started_at": m.get("created_at") or body["trial"]["started_at"],
            "data_table": f"{body['universe']['schema']}.{final.get('table')}",
            "data_from": splits["train"]["first_label_date"],
            "data_to": splits["test"]["last_label_date"],
            "feature_pools": "+".join(p.replace("pool__", "") for p in final.get("pools_in_table", [])),
            "n_features": dataset.get("n_features"),
            "lookback_d": dataset.get("lookback"),
            "feature_selection": (
                f"one selection per pool, {selection.get('null_draws_configured')} null draws, rows before "
                f"{split.get('holdout_start')}; failed pools "
                f"{'kept' if final.get('keep_failed_pools') else 'excluded'}"
            ),
            "target": event["column"], "target_definition": event["definition"],
            "split_ratio": ratio, "purge_gap": split.get("purge_gap_rows"),
            "train_period": _period(splits["train"]), "val_period": _period(splits["val"]),
            "test_period": _period(splits["test"]),
            "train_samples": splits["train"]["samples"], "val_samples": splits["val"]["samples"],
            "test_samples": splits["test"]["samples"],
            "train_positive_rate": round(splits["train"]["base_rate"], 4),
            "val_positive_rate": round(splits["val"]["base_rate"], 4),
            "test_positive_rate": round(splits["test"]["base_rate"], 4),
            "model": m.get("model_type"),
            "model_variant": str(m.get("run_name", "")).split("__")[0],
            "hyperparameters": _hyperparameters(config),
            "n_params": (m.get("model") or {}).get("n_params") if not ensemble else em.get("n_params"),
            "best_epoch": (m.get("training") or {}).get("best_epoch"),
            "seed": config.get("seed") if not ensemble else None,
            "run_seconds": timing.get("run_seconds"), "fit_seconds": timing.get("fit_seconds"),
            "device": device, "gpu": gpu or "not used",
            "val_auc": _r(em.get("val_auc")), "test_auc": _r(em.get("test_auc")),
            "test_auc_null_p95": _r(em.get("test_auc_bar")), "test_auc_z": _r(em.get("test_auc_z"), 2),
            "test_beats_null": (None if em.get("test_auc") is None or em.get("test_auc_bar") is None
                                else bool(em["test_auc"] > em["test_auc_bar"])),
            "test_auc_refit_train_val": _r(em.get("refit_auc")),
            "test_auc_refit_null_p95": _r(em.get("refit_auc_bar")),
            "test_auc_rolling_refit": _r(em.get("rolling_auc")),
            "test_auc_rolling_refit_null_p95": _r(em.get("rolling_auc_bar")),
            "test_pr_auc": _r(em.get("test_pr_auc")),
            "test_precision_top10pct": _r(em.get("test_p_at_10")),
            "test_brier_skill": _r(em.get("test_brier_skill")),
            "test_precision_at_val_threshold": _r(em.get("test_precision_at_thr")),
            "test_recall_at_val_threshold": _r(em.get("test_recall_at_thr")),
            "chosen_on_val": m.get("run_name") == best.get("run_name"),
            **_basket_cells(body, em, m.get("run_name") == best.get("run_name")),
        })
    return rows


def _basket_cells(body: Dict, em: Dict, chosen: bool = False) -> Dict:
    if not body.get("basket"):
        return {}
    summary = body["basket"].get("min_prob_summary") or {}
    frozen, rolling = summary.get("frozen") or {}, summary.get("rolling") or {}
    cut = ({"min_prob": body["basket"].get("min_prob"),
            "test_cut_active_share": _r(frozen.get("active_share")),
            "test_cut_precision": _r(frozen.get("precision")),
            "test_cut_basket_return": _r(frozen.get("bret"), 5),
            "test_cut_sharpe_50bps": _r(frozen.get("sharpe"), 3),
            "test_cut_precision_rolling_refit": _r(rolling.get("precision")),
            "test_cut_sharpe_50bps_rolling_refit": _r(rolling.get("sharpe"), 3)} if chosen and summary else {})
    return {
        **cut,
        "top_k": body["basket"].get("top_k"),
        "val_hit_at_k": _r(em.get("val_hit")), "test_hit_at_k": _r(em.get("test_hit")),
        "test_base_rate_buyable": _r(em.get("test_base")),
        "test_hit_null_p95": _r(em.get("test_hit_bar")), "test_hit_null_max": _r(em.get("test_hit_null_max")),
        "test_hit_z": _r(em.get("test_hit_z"), 2), "test_all_hit": _r(em.get("test_all_hit")),
        "test_basket_return": _r(em.get("test_bret"), 5), "test_universe_return": _r(em.get("test_uret"), 5),
        "val_daily_auc": _r(em.get("val_daily_auc")),
        "test_daily_auc": _r(em.get("test_daily_auc")),
        "test_daily_auc_null_p95": _r(em.get("test_daily_auc_bar")),
        "test_daily_auc_z": _r(em.get("test_daily_auc_z"), 2),
        "test_daily_auc_refit_train_val": _r(em.get("refit_daily_auc")),
        "test_daily_auc_rolling_refit": _r(em.get("rolling_daily_auc")),
        "test_sharpe_50bps": _r(em.get("test_sharpe_50"), 3), "test_cagr_50bps": _r(em.get("test_cagr_50")),
        "test_hit_refit_train_val": _r(em.get("refit_hit")),
        "test_hit_rolling_refit": _r(em.get("rolling_hit")),
        "test_sharpe_50bps_rolling_refit": _r(em.get("rolling_sharpe_50"), 3),
        # a basket's `test_beats_null` is its hit rate against the random-basket null
        "test_beats_null": (None if em.get("test_hit") is None or em.get("test_hit_bar") is None
                            else bool(em["test_hit"] > em["test_hit_bar"])),
    }


def rebuild_log() -> pd.DataFrame:
    """Regenerate `trials.csv` from every complete trial folder, oldest run first."""
    rows = []
    for path in sorted(glob.glob(os.path.join(TRIALS_DIR, "*", "trial.json"))):
        with open(path, encoding="utf-8") as fh:
            body = json.load(fh)
        if (body.get("trial") or {}).get("kind", "trial") != "trial" or not body.get("models"):
            continue
        rows.extend(log_rows(body))
    frame = pd.DataFrame(rows, columns=LOG_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values(["started_at", "run_id"], kind="stable")
    os.makedirs(os.path.dirname(TRIALS_LOG), exist_ok=True)
    frame.to_csv(TRIALS_LOG, index=False, encoding="utf-8")
    return frame


def _r(value, digits: int = 4):
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return round(value, digits) if np.isfinite(value) else None


# ---------------------------------------------------------------------- CLI
def main(argv: Optional[Sequence[str]] = None) -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(prog="python -m event_chain.trial")
    parser.add_argument("--show", default=None, help="a trial_id (or a unique prefix)")
    parser.add_argument("--rebuild", action="store_true", help="regenerate trials.csv")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.rebuild or not os.path.exists(TRIALS_LOG):
        rebuild_log()
    log = pd.read_csv(TRIALS_LOG)
    if args.show:
        paths = glob.glob(os.path.join(TRIALS_DIR, f"{args.show}*", "trial.json"))
        if len(paths) != 1:
            raise SystemExit(f"{len(paths)} trials match {args.show!r}")
        with open(paths[0], encoding="utf-8") as fh:
            body = json.load(fh)
        print(json.dumps({k: body.get(k) for k in ("trial", "event", "split", "final_table",
                                                     "best", "verdict")}, indent=2,
                         ensure_ascii=False)[:20000])
        return
    cols = ["exchange", "ticker", "started_at", "model_variant", "feature_pools", "n_features", "target", "split_ratio",
            "device", "run_seconds", "val_auc", "test_auc", "test_auc_null_p95", "test_beats_null",
            "chosen_on_val"]
    with pd.option_context("display.width", 250, "display.max_colwidth", 50):
        print(log[cols].to_string(index=False))


if __name__ == "__main__":
    main()
