"""Evaluation metrics for return-regression models (original return scale)."""

from __future__ import annotations

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Suitable metrics for a 5-day-return regressor, on the original scale.

    - RMSE / MAE            : error magnitude
    - RMSE_zero_baseline    : error of always predicting a 0 return; the model must
                              beat this to have any absolute-return edge
    - r2                    : 1 - SSE/SST (can be negative = worse than mean)
    - dir_accuracy          : sign hit-rate (up/down) at the 0 threshold
    - dir_auc               : ROC-AUC ranking up-days vs down-days using the
                              predicted return as the score (threshold-free
                              direction skill; 0.5 = none)
    - spearman_ic           : rank correlation between predictions and outcomes
    - hit_rate_pos          : precision of the "predict up" calls
    """
    y_true = np.asarray(y_true, dtype=float).ravel()
    y_pred = np.asarray(y_pred, dtype=float).ravel()
    err = y_pred - y_true

    sse = float(np.sum(err ** 2))
    sst = float(np.sum((y_true - y_true.mean()) ** 2))
    ic, _ = spearmanr(y_pred, y_true)

    pos = y_pred > 0
    hit_rate_pos = float(np.mean(y_true[pos] > 0)) if pos.any() else float("nan")

    # Direction ROC-AUC: predicted return as score for the up(1)/down(0) label.
    # Needs both classes present; else undefined.
    y_up = (y_true > 0).astype(int)
    dir_auc = (
        float(roc_auc_score(y_up, y_pred)) if 0 < y_up.sum() < len(y_up)
        else float("nan")
    )

    return {
        "n": int(len(y_true)),
        "RMSE": float(np.sqrt(np.mean(err ** 2))),
        "MAE": float(np.mean(np.abs(err))),
        "RMSE_zero_baseline": float(np.sqrt(np.mean(y_true ** 2))),
        "r2": (1.0 - sse / sst) if sst > 0 else float("nan"),
        "dir_accuracy": float(np.mean(np.sign(y_pred) == np.sign(y_true))),
        "dir_auc": dir_auc,
        "spearman_ic": float(ic) if ic == ic else float("nan"),
        "hit_rate_pos": hit_rate_pos,
        "beats_zero_baseline": bool(
            np.sqrt(np.mean(err ** 2)) < np.sqrt(np.mean(y_true ** 2))
        ),
    }
