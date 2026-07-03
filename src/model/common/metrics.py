"""Evaluation metrics for return-regression models (original return scale)."""

from __future__ import annotations

import numpy as np
from scipy.stats import spearmanr


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Suitable metrics for a 5-day-return regressor, on the original scale.

    - RMSE / MAE            : error magnitude
    - RMSE_zero_baseline    : error of always predicting a 0 return; the model must
                              beat this to have any absolute-return edge
    - r2                    : 1 - SSE/SST (can be negative = worse than mean)
    - dir_accuracy          : sign hit-rate (up/down), the tradable direction
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

    return {
        "n": int(len(y_true)),
        "RMSE": float(np.sqrt(np.mean(err ** 2))),
        "MAE": float(np.mean(np.abs(err))),
        "RMSE_zero_baseline": float(np.sqrt(np.mean(y_true ** 2))),
        "r2": (1.0 - sse / sst) if sst > 0 else float("nan"),
        "dir_accuracy": float(np.mean(np.sign(y_pred) == np.sign(y_true))),
        "spearman_ic": float(ic) if ic == ic else float("nan"),
        "hit_rate_pos": hit_rate_pos,
        "beats_zero_baseline": bool(
            np.sqrt(np.mean(err ** 2)) < np.sqrt(np.mean(y_true ** 2))
        ),
    }
