"""Model 2 (jump variant) — predict the **probability that the close rises ≥ threshold
within `horizon` days**, from **sentiment-only** features.

Target: `jump_fwd = 1{ close_adjust[t+H]/close_adjust[t] - 1 ≥ jump_threshold }` (default
H=5, threshold=5%). It is a rare-ish event (~11% base rate on VCB/FPT/PNJ), so this is an
**imbalanced binary classification** scored by ranking + calibration metrics, NOT accuracy:

  • **ROC-AUC**   — can the model rank a jump day above a non-jump day? (0.5 = no signal)
  • **PR-AUC**    — precision-recall area; its no-skill floor is the positive base rate.
  • **Brier**     — mean squared error of the predicted probability (lower is better);
                    compared to always predicting the base rate.
  • **lift@decile** — positive rate in the model's top-decile scores ÷ overall base rate;
                    "if I act on the 10% highest-probability days, how many more jumps?"

Two models (per the plan): **Logistic Regression** (interpretable baseline, standardized
inputs) and **Gradient Boosting** (nonlinear main model). Both keep the natural class
balance via `class_weight` / sample weights and output a **calibrated probability**
(sigmoid/Platt on a held-out slice of each training fold). Everything runs on the purged
walk-forward folds from `sentiment_features`, so every number is out-of-sample and
point-in-time.

⚠️ Sentiment-only, by request — no price/TA features. Whether news sentiment alone carries
information about an imminent jump is precisely what this measures; expect a weak signal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np
import pandas as pd

from sentiment.sentiment_features import SENTIMENT_FEATURES, purged_walkforward_folds

DEFAULT_CUTS = [
    "2019-12-31",
    "2020-12-31",
    "2021-12-31",
    "2022-12-31",
    "2023-12-31",
    "2024-12-31",
    "2025-06-30",
]
MIN_TRAIN = 200
MIN_TEST = 30
MIN_TEST_POS = 3  # need a few positives in test for AUC to be defined


@dataclass
class FoldMetrics:
    cut: str
    n_train: int
    n_test: int
    base_rate: float  # positive fraction in test (= PR-AUC no-skill floor)
    roc_auc: float
    pr_auc: float
    brier: float
    brier_baserate: float  # Brier of always predicting the train base rate
    lift_top_decile: float


@dataclass
class ModelResult:
    model_name: str
    folds: List[FoldMetrics] = field(default_factory=list)

    def _avg(self, attr: str) -> float:
        vals = [getattr(f, attr) for f in self.folds]
        return float(np.mean(vals)) if vals else float("nan")

    @property
    def n_folds(self) -> int:
        return len(self.folds)

    @property
    def avg_roc_auc(self) -> float:
        return self._avg("roc_auc")

    @property
    def avg_pr_auc(self) -> float:
        return self._avg("pr_auc")

    @property
    def avg_base_rate(self) -> float:
        return self._avg("base_rate")

    @property
    def avg_brier(self) -> float:
        return self._avg("brier")

    @property
    def avg_brier_baserate(self) -> float:
        return self._avg("brier_baserate")

    @property
    def avg_lift(self) -> float:
        return self._avg("lift_top_decile")

    @property
    def folds_auc_above_half(self) -> int:
        return int(sum(f.roc_auc > 0.5 for f in self.folds))


def _make_models():
    """Return {name: (estimator_factory, needs_scaling)}."""
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression

    return {
        "logistic": (
            lambda: LogisticRegression(max_iter=2000, class_weight="balanced", C=1.0),
            True,
        ),
        "gradient_boosting": (
            lambda: GradientBoostingClassifier(
                n_estimators=300,
                max_depth=3,
                learning_rate=0.05,
                subsample=0.8,
                random_state=0,
            ),
            False,
        ),
    }


def _lift_top_decile(y_true: np.ndarray, proba: np.ndarray) -> float:
    """Positive rate among the top-10% highest-probability rows ÷ overall positive rate."""
    n = len(y_true)
    k = max(1, int(round(0.10 * n)))
    order = np.argsort(-proba)
    top_rate = y_true[order[:k]].mean()
    base = y_true.mean()
    return float(top_rate / base) if base > 0 else float("nan")


def _score_fold(
    panel: pd.DataFrame,
    features: List[str],
    factory,
    needs_scaling: bool,
    train_mask: np.ndarray,
    test_mask: np.ndarray,
    cut: pd.Timestamp,
) -> FoldMetrics | None:
    tr, te = panel[train_mask], panel[test_mask]
    ytr = tr["jump_fwd"].to_numpy()
    yte = te["jump_fwd"].to_numpy()
    if (
        len(tr) < MIN_TRAIN
        or len(te) < MIN_TEST
        or len(np.unique(ytr)) < 2
        or yte.sum() < MIN_TEST_POS
    ):
        return None

    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        roc_auc_score,
    )
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    Xtr, Xte = tr[features].to_numpy(), te[features].to_numpy()

    base_est = factory()
    est = make_pipeline(StandardScaler(), base_est) if needs_scaling else base_est
    # Calibrate probabilities on a held-out slice of the (chronological) training fold.
    # cv=3 internally splits train; sigmoid (Platt) is stable for small positive counts.
    clf = CalibratedClassifierCV(est, method="sigmoid", cv=3)
    clf.fit(Xtr, ytr)
    proba = clf.predict_proba(Xte)[:, 1]

    train_base = float(ytr.mean())
    return FoldMetrics(
        cut=str(cut.date()),
        n_train=len(tr),
        n_test=len(te),
        base_rate=float(yte.mean()),
        roc_auc=float(roc_auc_score(yte, proba)),
        pr_auc=float(average_precision_score(yte, proba)),
        brier=float(brier_score_loss(yte, proba)),
        brier_baserate=float(brier_score_loss(yte, np.full(len(yte), train_base))),
        lift_top_decile=_lift_top_decile(yte, proba),
    )


def evaluate(
    panel: pd.DataFrame,
    cut_dates: List[str] | None = None,
    horizon: int = 5,
    features: List[str] | None = None,
) -> List[ModelResult]:
    """Purged walk-forward for each model on the given (sentiment-only) features."""
    cut_dates = cut_dates or DEFAULT_CUTS
    features = [f for f in (features or SENTIMENT_FEATURES) if f in panel.columns]

    results: List[ModelResult] = []
    for name, (factory, needs_scaling) in _make_models().items():
        mr = ModelResult(model_name=name)
        for cut, tr_mask, te_mask in purged_walkforward_folds(
            panel, cut_dates, horizon=horizon
        ):
            fold = _score_fold(
                panel, features, factory, needs_scaling, tr_mask, te_mask, cut
            )
            if fold is not None:
                mr.folds.append(fold)
        results.append(mr)
    return results


def format_report(results: List[ModelResult], target_desc: str) -> str:
    lines = []
    lines.append(
        f"Jump probability model - {target_desc} (sentiment-only, purged walk-forward)\n"
        "  metric bars: ROC-AUC 0.5 = no signal; PR-AUC floor = base rate; "
        "Brier < Brier(base-rate) = better than guessing the rate; lift>1 = top-decile "
        "beats base rate.\n"
    )
    header = (
        f"{'model':<20} {'folds':>5} {'baseRate':>9} {'ROC_AUC':>8} "
        f"{'PR_AUC':>7} {'Brier':>7} {'Brier_bR':>9} {'lift@10%':>9} {'AUC>0.5':>8}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for mr in results:
        lines.append(
            f"{mr.model_name:<20} {mr.n_folds:>5} {mr.avg_base_rate:>9.3f} "
            f"{mr.avg_roc_auc:>8.3f} {mr.avg_pr_auc:>7.3f} {mr.avg_brier:>7.4f} "
            f"{mr.avg_brier_baserate:>9.4f} {mr.avg_lift:>9.2f} "
            f"{mr.folds_auc_above_half:>5}/{mr.n_folds:<2}"
        )
    return "\n".join(lines)
