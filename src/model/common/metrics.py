"""Model-stage metrics — a thin shim over `result_evaluator.metrics`.

⚠️ **There is one definition of every metric in this repo and it is not here.** It
lives in `result_evaluator/metrics.py`, because a metric computed inside the training
stage can only be improved by retraining, while a metric computed from
`results/predictions_<split>.csv` can be backfilled across every past run without a
GPU. `model/CONTEXT.md` §9 records the one time that mattered.

These two functions are kept so the existing notebooks
(`lstm_return_5day.ipynb`, `lstm_direction_5day.ipynb`) keep running unchanged. New
code should call `result_evaluator.metrics.evaluate`, which returns the shared core
block plus the task extras and, unlike these, carries a null.

⚠️ The keys below are the pre-2026-08-09 names. `spearman_ic` is `ic` and
`dir_accuracy` here is the SIGN hit rate (threshold 0), where the core block's
`dir_accuracy` is at the score's median — the two differ whenever predictions are
biased, which for a return regressor trained on a standardised target they are.
"""

from __future__ import annotations

import numpy as np

from result_evaluator.metrics import (
    classification_extras,
    core_metrics,
    dir_auc as _dir_auc,
    regression_extras,
)


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Legacy key set for a return regressor, on the original return scale."""
    core = core_metrics(y_true, y_pred)
    extras = regression_extras(y_true, y_pred)
    return {
        "n": core["n"],
        "RMSE": extras["RMSE"],
        "MAE": extras["MAE"],
        "RMSE_zero_baseline": extras["RMSE_zero_baseline"],
        "r2": extras["r2"],
        "dir_accuracy": extras["sign_accuracy"],
        "dir_auc": core["dir_auc"],
        "spearman_ic": core["ic"],
        "hit_rate_pos": extras["hit_rate_pos"],
        "beats_zero_baseline": extras["beats_zero_baseline"],
    }


def classification_metrics(
    y_true: np.ndarray, y_prob: np.ndarray, threshold: float = 0.5
) -> dict:
    """Legacy key set for a binary classifier, scored on P(class 1)."""
    extras = classification_extras(y_true, y_prob, threshold=threshold)
    return {
        "n": int(len(np.asarray(y_true).ravel())),
        "base_rate": extras["base_rate"],
        "dir_accuracy": extras["accuracy"],
        "majority_baseline_acc": extras["majority_baseline_acc"],
        "dir_auc": _dir_auc(
            (np.asarray(y_true, dtype=float).ravel() - 0.5), np.asarray(y_prob, float)
        ),
        "pr_auc": extras["pr_auc"],
        "precision": extras["precision"],
        "recall": extras["recall"],
        "f1": extras["f1"],
        "log_loss": extras["log_loss"],
        "brier": extras["brier"],
        "beats_majority": extras["beats_majority"],
    }
