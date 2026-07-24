"""Model 2 — predict the 5-day-ahead close price and direction from sentiment (+ price)
features, scored honestly against the baselines that make those targets hard.

Two heads on the same panel:
  • **price** — GradientBoosting regressor on `close_fwd = close_adjust[t+H]`, judged by
    MAE **against the random-walk** `close[t+H] = close[t]`. ⚠️ Absolute-price forecasting
    is a random walk; a model is only useful if MAE < random-walk MAE.
  • **direction** — GradientBoosting classifier on `up_fwd`, judged by accuracy **against
    the majority-class baseline** and the 0.5 coin-flip.

Everything runs through the **purged walk-forward** folds from `sentiment_features`, so
every number is strictly out-of-sample and point-in-time. `evaluate` sweeps feature sets
(sentiment-only / price-only / all) so you can see whether sentiment adds anything over
price/TA alone — the ablation is the actual finding, not a single headline metric.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np
import pandas as pd

from sentiment.sentiment_features import (
    PRICE_FEATURES,
    SENTIMENT_FEATURES,
    purged_walkforward_folds,
)

# Default cut dates for the walk-forward (year-ends + a recent mid-year), overridable.
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


@dataclass
class FoldResult:
    cut: str
    n_train: int
    n_test: int
    # price head
    mae_model: float
    mae_random_walk: float
    # direction head
    dir_acc_model: float
    dir_acc_majority: float


@dataclass
class AblationResult:
    feature_set: str
    n_features: int
    folds: List[FoldResult] = field(default_factory=list)

    # ── aggregates over folds ──
    @property
    def avg_mae_model(self) -> float:
        return float(np.mean([f.mae_model for f in self.folds]))

    @property
    def avg_mae_random_walk(self) -> float:
        return float(np.mean([f.mae_random_walk for f in self.folds]))

    @property
    def price_beats_rw(self) -> int:
        return int(sum(f.mae_model < f.mae_random_walk for f in self.folds))

    @property
    def avg_dir_acc(self) -> float:
        return float(np.mean([f.dir_acc_model for f in self.folds]))

    @property
    def dir_beats_half(self) -> int:
        return int(sum(f.dir_acc_model > 0.5 for f in self.folds))

    @property
    def n_folds(self) -> int:
        return len(self.folds)


def _make_regressor():
    from sklearn.ensemble import GradientBoostingRegressor

    return GradientBoostingRegressor(
        n_estimators=400,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        random_state=0,
    )


def _make_classifier():
    from sklearn.ensemble import GradientBoostingClassifier

    return GradientBoostingClassifier(
        n_estimators=400,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        random_state=0,
    )


def _score_fold(
    panel: pd.DataFrame,
    features: List[str],
    train_mask: np.ndarray,
    test_mask: np.ndarray,
    cut: pd.Timestamp,
) -> FoldResult | None:
    tr, te = panel[train_mask], panel[test_mask]
    if len(tr) < MIN_TRAIN or len(te) < MIN_TEST:
        return None

    from sklearn.metrics import mean_absolute_error

    Xtr, Xte = tr[features], te[features]
    close_now = te["close_adjust"].to_numpy()

    # ── price head ──
    reg = _make_regressor().fit(Xtr, tr["close_fwd"])
    pred_price = reg.predict(Xte)
    y_price = te["close_fwd"].to_numpy()
    mae_model = mean_absolute_error(y_price, pred_price)
    mae_rw = mean_absolute_error(y_price, close_now)  # close[t+H] = close[t]

    # ── direction head ──
    ytr_dir, yte_dir = tr["up_fwd"].astype(int), te["up_fwd"].astype(int)
    if ytr_dir.nunique() < 2:
        dir_acc_model = float((np.zeros(len(yte_dir)) == yte_dir).mean())
    else:
        clf = _make_classifier().fit(Xtr, ytr_dir)
        dir_acc_model = float((clf.predict(Xte) == yte_dir).mean())
    majority = int(ytr_dir.mode().iloc[0])
    dir_acc_majority = float((np.full(len(yte_dir), majority) == yte_dir).mean())

    return FoldResult(
        cut=str(cut.date()),
        n_train=len(tr),
        n_test=len(te),
        mae_model=mae_model,
        mae_random_walk=mae_rw,
        dir_acc_model=dir_acc_model,
        dir_acc_majority=dir_acc_majority,
    )


def evaluate(
    panel: pd.DataFrame,
    cut_dates: List[str] | None = None,
    horizon: int = 5,
    feature_sets: Dict[str, List[str]] | None = None,
) -> List[AblationResult]:
    """Run the purged walk-forward for each feature set → one `AblationResult` each."""
    cut_dates = cut_dates or DEFAULT_CUTS
    feature_sets = feature_sets or {
        "sentiment-only": SENTIMENT_FEATURES,
        "price/TA/foreign-only": PRICE_FEATURES,
        "sentiment+price (all)": SENTIMENT_FEATURES + PRICE_FEATURES,
    }

    results: List[AblationResult] = []
    for name, feats in feature_sets.items():
        feats = [f for f in feats if f in panel.columns]
        ab = AblationResult(feature_set=name, n_features=len(feats))
        for cut, tr_mask, te_mask in purged_walkforward_folds(
            panel, cut_dates, horizon=horizon
        ):
            fold = _score_fold(panel, feats, tr_mask, te_mask, cut)
            if fold is not None:
                ab.folds.append(fold)
        results.append(ab)
    return results


def format_report(results: List[AblationResult]) -> str:
    """Human-readable summary — the honest scoreboard vs the baselines."""
    lines = []
    lines.append(
        "Model 2 - 5-day-ahead close & direction (purged walk-forward, "
        "out-of-sample)\n"
        "  NOTE: price target is a random walk; the bar is beating "
        "close[t+H]=close[t] and 0.5 direction.\n"
    )
    header = (
        f"{'feature set':<24} {'feats':>5} {'folds':>5} "
        f"{'MAE_model':>10} {'MAE_RW':>8} {'beatsRW':>8} "
        f"{'dirAcc':>7} {'dirBase':>7} {'dir>0.5':>8}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for ab in results:
        dir_base = (
            float(np.mean([f.dir_acc_majority for f in ab.folds])) if ab.folds else 0.0
        )
        lines.append(
            f"{ab.feature_set:<24} {ab.n_features:>5} {ab.n_folds:>5} "
            f"{ab.avg_mae_model:>10.0f} {ab.avg_mae_random_walk:>8.0f} "
            f"{ab.price_beats_rw:>5}/{ab.n_folds:<2} "
            f"{ab.avg_dir_acc:>7.3f} {dir_base:>7.3f} "
            f"{ab.dir_beats_half:>5}/{ab.n_folds:<2}"
        )
    return "\n".join(lines)
