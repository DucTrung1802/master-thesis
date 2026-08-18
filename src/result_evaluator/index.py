# src\result_evaluator\index.py
"""THE ONE SCHEMA for `src/model/runs/index.csv`, and the one function that fills it.

⚠️ **Two writers used to produce two different files under one name.**
`model.common.registry.append_run` wrote `model_type` / `val_ic` / `test_ic` / … after
a training run, and `result_evaluator.rebuild_index` wrote `model` / `ic` / `dir_auc` /
… (test split only) when regenerating. Running `--rebuild-index` therefore silently
*replaced the schema* of the leaderboard, and any reader written against one form broke
against the other. Issue **IDX-1** fixed two metric ERAS inside the file; this fixes the
two WRITERS that could recreate them.

The schema lives here, in `result_evaluator`, because that is the direction the imports
already run: `model.common.engine` imports `result_evaluator.metrics`, never the
reverse. `registry.append_run` takes its header from `INDEX_COLUMNS`, and both the
training path and the rebuild path fill it through `index_row`.

## What a row is

One run, both splits. The **core block** (`*_ic`, `*_dir_auc`, `*_hit_rate`,
`*_long_short`) is filled by every task and every architecture, because
`result_evaluator.metrics` computes it from `(score, realised return)` and nothing
else — which is exactly what lets an LSTM, a CNN, a ridge and a constant share one
table. Task-specific columns are blank for the other task.

⚠️ **`*_clears` and `*_p` are the columns that matter.** A run at the top of the board
on `test_ic` whose `test_ic_clears` is False is the best-ranked noise, not the best
model.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

INDEX_COLUMNS: List[str] = [
    # --- identity -------------------------------------------------------------
    "run_id", "created_at", "model_type", "task",
    # --- what it was trained on ----------------------------------------------
    "dataset_name", "dataset_hash", "universe", "target", "scope",
    "lookback", "horizon", "n_features", "grain",
    # --- how it trained -------------------------------------------------------
    "n_params", "best_epoch", "best_val_loss",
    # --- the core block, filled by every task ---------------------------------
    "val_ic", "val_dir_auc", "val_hit_rate", "val_long_short",
    "test_n", "test_n_eff", "test_n_eff_windowed",
    "test_ic", "test_ic_p", "test_ic_bar", "test_ic_clears",
    "test_dir_auc", "test_dir_auc_p", "test_dir_auc_bar", "test_dir_auc_clears",
    "test_hit_rate", "test_long_short",
    # --- the error bar on `ic`, and the panel verdict (added 2026-08-16) --------
    # ⚠️ `test_ic_t` is THE column to read on a PANEL grain: issue NUL-3 says the
    # evaluator's panel null is not label-neutral, so `test_ic_clears` is not
    # trustworthy there while the daily-IC t-stat needs no null at all.
    # ⚠️ **AND THAT COLUMN WAS ITSELF WRONG UNTIL 2026-08-18 (`ICT-1`)** — it divided
    # the daily-IC sd by `sqrt(n_dates)` rather than `sqrt(n_dates / h)`, overstating
    # every panel run by exactly `√h` (15.50 against +3.47 at h=20). Fixed in
    # `metrics.panel_core_metrics`. Any `index.csv` row written before that date and
    # not re-scored still carries the old figure — `--rescore` rewrites it from
    # `predictions_*.csv` and needs no GPU.
    "test_ic_se", "test_ic_t",
    # --- vs a naive forecast, block B (added 2026-08-16) -----------------------
    # ⚠️ `test_mase < 1` is the first column that asks whether the model beats DOING
    # NOTHING. `experiment_10` reviewed 23 papers and not one reported this.
    "test_naive_kind", "test_mase", "test_rmsse", "test_skill_score", "test_beats_naive",
    # --- calibration, block C (added 2026-08-16) -------------------------------
    "test_calibration_slope", "test_pred_sd_ratio",
    # --- degeneracy, block E (added 2026-08-16) --------------------------------
    # ⚠️ True means the direction metrics above were WITHDRAWN to NaN because every
    # label shares a sign — they could not have failed (CLAUDE.md §5 rule 21).
    "test_target_single_signed",
    # --- regression extras -----------------------------------------------------
    "test_RMSE", "test_RMSE_zero_baseline", "test_beats_zero_baseline", "test_r2",
    # --- classification extras -------------------------------------------------
    "test_pr_auc", "test_base_rate", "test_beats_majority",
    # --- provenance ------------------------------------------------------------
    "verdict", "git_sha", "run_dir",
]


def _number(source: Dict, key: str, digits: int = 4):
    value = source.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return ""
    return round(float(value), digits)


def _flag(source: Dict, key: str):
    value = source.get(key)
    return value if isinstance(value, bool) else ""


def index_row(
    run_id: str,
    scored: Dict[str, Dict],
    meta: Optional[Dict] = None,
    run_dir: str = "",
    verdict: str = "",
) -> Dict:
    """One `index.csv` row from a run's metrics and (optionally) its metadata.

    ⚠️ **Tolerates absent metadata by design.** `src/model/runs/*/` is git-ignored
    (issue **RPR-1**), so a fresh checkout can hold a run folder with `results/` and
    nothing else. Such a run is still scoreable from `predictions_*.csv`; the columns
    that need `metadata.json` come back blank rather than making the row unbuildable.

    Args:
        scored: `{split: {metric: value}}` as `evaluate_run` returns it.
        meta: the run's `metadata.json`, or None.
    """
    meta = meta or {}
    val, test = scored.get("val", {}), scored.get("test", {})
    model = meta.get("model") or {}
    training = meta.get("training") or {}
    dataset = meta.get("dataset") or {}
    lineage = meta.get("lineage") or {}

    # ⚠️ The run_id is `<model>__<universe>__<target>__final__d<d>_h<h>[__<scope>]__<stamp>`
    # by convention (model/CONTEXT.md §3), so it is the last-resort source for the
    # identity columns when there is no metadata to read them from.
    parts = run_id.split("__")

    return {
        "run_id": run_id,
        "created_at": meta.get("created_at", ""),
        "model_type": model.get("type") or (parts[0].upper() if parts else ""),
        "task": meta.get("task") or (meta.get("config") or {}).get("task", ""),
        "dataset_name": dataset.get("dataset_name") or lineage.get("dataset_name", ""),
        "dataset_hash": dataset.get("dataset_hash") or lineage.get("dataset_hash", ""),
        "universe": parts[1] if len(parts) > 1 else "",
        "target": lineage.get("target", ""),
        # `scope` is the feature block; absent means the archive's union. It is the
        # segment between `d<d>_h<h>` and the timestamp, when there is one.
        "scope": parts[-2] if len(parts) > 5 and not parts[-2].startswith("d") else "",
        "lookback": lineage.get("lookback_d") or dataset.get("lookback", ""),
        "horizon": lineage.get("horizon_h", ""),
        "n_features": dataset.get("n_features", ""),
        "grain": test.get("grain", ""),
        "n_params": model.get("n_params", ""),
        "best_epoch": training.get("best_epoch", ""),
        "best_val_loss": round(training["best_val_loss"], 6)
        if isinstance(training.get("best_val_loss"), (int, float)) else "",
        "val_ic": _number(val, "ic"),
        "val_dir_auc": _number(val, "dir_auc"),
        "val_hit_rate": _number(val, "hit_rate"),
        "val_long_short": _number(val, "long_short", 6),
        "test_n": test.get("n", ""),
        "test_n_eff": test.get("n_eff", ""),
        "test_n_eff_windowed": test.get("n_eff_windowed", ""),
        "test_ic": _number(test, "ic"),
        "test_ic_p": _number(test, "ic_p"),
        "test_ic_bar": _number(test, "ic_bar"),
        "test_ic_clears": _flag(test, "ic_clears"),
        "test_dir_auc": _number(test, "dir_auc"),
        "test_dir_auc_p": _number(test, "dir_auc_p"),
        "test_dir_auc_bar": _number(test, "dir_auc_bar"),
        "test_dir_auc_clears": _flag(test, "dir_auc_clears"),
        "test_hit_rate": _number(test, "hit_rate"),
        "test_long_short": _number(test, "long_short", 6),
        "test_ic_se": _number(test, "ic_se"),
        "test_ic_t": _number(test, "ic_t", 2),
        # ⚠️ A STRING, not a number — `zero` or `lag_h`. `mase` cannot be read without
        # it: the same 0.87 means "beats no-change" on a return and "beats the random
        # walk" on a level, and those are very different claims.
        "test_naive_kind": test.get("naive_kind", ""),
        "test_mase": _number(test, "mase"),
        "test_rmsse": _number(test, "rmsse"),
        "test_skill_score": _number(test, "skill_score"),
        "test_beats_naive": _flag(test, "beats_naive"),
        "test_calibration_slope": _number(test, "calibration_slope"),
        "test_pred_sd_ratio": _number(test, "pred_sd_ratio"),
        "test_target_single_signed": _flag(test, "target_single_signed"),
        "test_RMSE": _number(test, "RMSE", 6),
        "test_RMSE_zero_baseline": _number(test, "RMSE_zero_baseline", 6),
        "test_beats_zero_baseline": _flag(test, "beats_zero_baseline"),
        "test_r2": _number(test, "r2"),
        "test_pr_auc": _number(test, "pr_auc"),
        "test_base_rate": _number(test, "base_rate"),
        "test_beats_majority": _flag(test, "beats_majority"),
        "verdict": verdict,
        "git_sha": meta.get("git_sha", ""),
        "run_dir": run_dir.replace("\\", "/"),
    }
