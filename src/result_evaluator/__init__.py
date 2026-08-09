# src\result_evaluator\__init__.py
"""Score a finished run, whatever produced it.

    metrics.py     the ONE core block (ic, dir_auc, hit_rate, long_short) + a
                   block-shuffled null for each, plus per-task extras
    evaluator.py   read a run folder's predictions_*.csv; put every run on one board

Every model here emits one score per sample and every target derives from one realised
forward return, so the core block is measured on `(score, return)` and nothing else.
That is what lets a regressor, a classifier and a ranker share a leaderboard.

⚠️ Nothing in this package trains anything or touches a GPU. A run is scoreable the
moment `results/predictions_<split>.csv` exists, which is how `dir_auc` was once
backfilled across every run without retraining one.
"""

from result_evaluator.metrics import (
    CLASSIFICATION,
    CORE_METRICS,
    NULLED_METRICS,
    RANKING,
    REGRESSION,
    TASKS,
    classification_extras,
    core_metrics,
    dir_auc,
    evaluate,
    evaluate_panel,
    ic,
    long_short,
    null_draws,
    null_metrics,
    panel_core_metrics,
    panel_matrices,
    panel_null_metrics,
    regression_extras,
    summarise,
    verdict,
)
from result_evaluator.evaluator import (
    DEFAULT_RUNS_DIR,
    evaluate_run,
    leaderboard,
    run_folders,
    run_metadata,
)

__all__ = [
    "CLASSIFICATION",
    "CORE_METRICS",
    "DEFAULT_RUNS_DIR",
    "NULLED_METRICS",
    "RANKING",
    "REGRESSION",
    "TASKS",
    "classification_extras",
    "core_metrics",
    "dir_auc",
    "evaluate",
    "evaluate_panel",
    "evaluate_run",
    "ic",
    "leaderboard",
    "long_short",
    "null_draws",
    "null_metrics",
    "panel_core_metrics",
    "panel_matrices",
    "panel_null_metrics",
    "regression_extras",
    "run_folders",
    "run_metadata",
    "summarise",
    "verdict",
]
