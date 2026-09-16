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
SCHEMA_VERSION = 2

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
    "src/model/lstm/model.py",
    "src/model/gru/model.py",
    "src/model/cnn/model.py",
    "src/model/mlp/model.py",
    "src/model/tcn/model.py",
    "src/result_evaluator/*.py",
    "src/orchestration/preprocessor/preprocessor.py",
)

# ONE ROW PER MODEL RUN. Grouped: key | data | features | target | split | model | run time
# and hardware | results. Nothing else belongs in the log; the rest is in `trial.json`.
LOG_COLUMNS = [
    # key
    "run_id", "trial_id", "started_at",
    # data
    "ticker", "data_table", "data_from", "data_to",
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
    "test_pr_auc", "test_precision_top10pct", "test_brier_skill",
    "test_precision_at_val_threshold", "test_recall_at_val_threshold", "chosen_on_val",
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
    if best is not None:
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
        "universe": {"ticker": chain.ticker, "schema": chain.schema},
        "data": {**data, "label": _label_stats(chain)},
        "split": {"train_ratio": C.TRAIN_RATIO, "val_ratio": C.VAL_RATIO,
                  "holdout_start": chain.holdout_start(),
                  "val_start_date": split_meta.get("val_start_date"),
                  "test_start_date": split_meta.get("test_start_date"),
                  "purge_gap_rows": split_meta.get("purge_gap_rows"),
                  "purge_rule": split_meta.get("purge_rule"), "splits": splits},
        "setup": {"name": getattr(chain, "setup", "window"), "channels": getattr(chain, "channels", "shortlist"),
                  "aux_targets": list(getattr(chain, "aux_targets", ()) or []),
                  "ensembles": {k: list(v) for k, v in (getattr(chain, "ensembles", {}) or {}).items()}},
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
        "report_config": {"null_draws": C.REPORT_NULL_DRAWS,
                          "null_block": int(dataset.lookback + chain.horizon),
                          "best_rule": "max VAL ROC-AUC, excluding BASELINE_PRIOR and BLEND "
                                       "(a FIXED ensemble is eligible)",
                          "refit_rule": "the run's own config refitted on train+val, test scored once",
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
    if not predictions.empty:
        # 6 significant digits: re-scorable to 1e-6, and half the bytes of repr floats.
        predictions.to_csv(os.path.join(folder, "predictions.csv"), index=False,
                           float_format="%.6g")
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
            members = [by_name.get(n) or {} for n in str(em.get("ensemble_members", "")).split(",") if n]
            run_id = f"{m.get('run_name')}__{body['trial']['id']}"
            config = {"model": {"combine": "geometric mean of member probabilities",
                                "members": [str(x.get("run_name", "")).split("__")[0] for x in members]}}
            member_timing = [x.get("timing") or {} for x in members]
            timing = {k: round(sum((t.get(k) or 0) for t in member_timing), 3)
                      for k in ("run_seconds", "fit_seconds")}
            devices = {str(x.get("device") or (x.get("config") or {}).get("device")) for x in members}
            device = "+".join(sorted(devices)) if devices else "cpu"
        gpu = ((m.get("env") or {}).get("cuda_device") or machine_gpu) if str(device).startswith("cuda") else ""
        rows.append({
            "run_id": run_id, "trial_id": body["trial"]["id"],
            "started_at": m.get("created_at") or body["trial"]["started_at"],
            "ticker": body["universe"]["ticker"],
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
            "test_pr_auc": _r(em.get("test_pr_auc")),
            "test_precision_top10pct": _r(em.get("test_p_at_10")),
            "test_brier_skill": _r(em.get("test_brier_skill")),
            "test_precision_at_val_threshold": _r(em.get("test_precision_at_thr")),
            "test_recall_at_val_threshold": _r(em.get("test_recall_at_thr")),
            "chosen_on_val": m.get("run_name") == best.get("run_name"),
        })
    return rows


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
    cols = ["started_at", "model_variant", "feature_pools", "n_features", "target", "split_ratio",
            "device", "run_seconds", "val_auc", "test_auc", "test_auc_null_p95", "test_beats_null",
            "chosen_on_val"]
    with pd.option_context("display.width", 250, "display.max_colwidth", 50):
        print(log[cols].to_string(index=False))


if __name__ == "__main__":
    main()
