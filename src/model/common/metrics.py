"""Evaluation metrics for the model stage.

`regression_metrics` scores a continuous target (return regressor, original scale);
`classification_metrics` scores a binary target (e.g. `direction_5day`) from
predicted probabilities. Both feed `results/metrics.json` and `runs/index.csv`.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)


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


def classification_metrics(y_true: np.ndarray, y_prob: np.ndarray,
                           threshold: float = 0.5) -> dict:
    """Metrics for a binary up/down classifier (e.g. `direction_5day`).

    `y_true` is the 0/1 label; `y_prob` is the predicted P(class 1) (post-sigmoid).
    The direction-skill keys are deliberately named `dir_accuracy` / `dir_auc` — the
    SAME columns the regressor fills from the sign of its predicted return — so a
    dedicated classifier and a return regressor are directly comparable in
    `index.csv`.

    - dir_accuracy          : accuracy at `threshold` (default 0.5)
    - majority_baseline_acc : accuracy of always predicting the majority class; the
                              bar to beat (`beats_majority` flag)
    - base_rate             : share of positives (class 1) in `y_true`
    - dir_auc               : ROC-AUC of the probability score (0.5 = none)
    - pr_auc                : average precision (area under precision-recall)
    - precision/recall/f1   : at `threshold`, for the "up" (class 1) call
    - log_loss              : binary cross-entropy on the original label
    - brier                 : mean squared (prob - label)
    """
    y_true = np.asarray(y_true, dtype=float).ravel()
    y_prob = np.asarray(y_prob, dtype=float).ravel()
    y_hat = (y_prob >= threshold).astype(int)
    y_int = (y_true >= 0.5).astype(int)

    base_rate = float(np.mean(y_int))
    majority = max(base_rate, 1.0 - base_rate)
    accuracy = float(np.mean(y_hat == y_int))
    both = 0 < y_int.sum() < len(y_int)  # both classes present

    return {
        "n": int(len(y_true)),
        "base_rate": base_rate,
        "dir_accuracy": accuracy,
        "majority_baseline_acc": float(majority),
        "dir_auc": float(roc_auc_score(y_int, y_prob)) if both else float("nan"),
        "pr_auc": float(average_precision_score(y_int, y_prob)) if both else float("nan"),
        "precision": float(precision_score(y_int, y_hat, zero_division=0)),
        "recall": float(recall_score(y_int, y_hat, zero_division=0)),
        "f1": float(f1_score(y_int, y_hat, zero_division=0)),
        "log_loss": float(log_loss(y_int, np.clip(y_prob, 1e-7, 1 - 1e-7),
                                   labels=[0, 1])),
        "brier": float(np.mean((y_prob - y_int) ** 2)),
        "beats_majority": bool(accuracy > majority),
    }
