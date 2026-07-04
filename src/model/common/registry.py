"""Append-only run registry: one row per run in `<runs_dir>/index.csv` for quick
cross-run comparison (open in Data Wrangler / sort by test RMSE or IC)."""

from __future__ import annotations

import csv
import os

_COLUMNS = [
    "run_id", "created_at", "dataset_name", "dataset_hash", "model_type",
    "lookback", "n_features", "best_epoch", "best_val_mse",
    "val_RMSE", "val_dir_accuracy", "val_dir_auc", "val_spearman_ic",
    "test_RMSE", "test_RMSE_zero_baseline", "test_dir_accuracy", "test_dir_auc",
    "test_spearman_ic", "test_beats_zero_baseline", "git_sha", "run_dir",
]


def append_run(runs_dir: str, row: dict) -> str:
    """Append `row` (any subset of _COLUMNS) to runs_dir/index.csv, creating the
    header on first write. Returns the index path."""
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "index.csv")
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_COLUMNS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in _COLUMNS})
    return path
