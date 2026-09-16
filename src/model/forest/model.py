"""Bagged trees over the window statistics — ExtraTrees and RandomForest.

A second tree family beside `model.gbt`. Boosting fits the residual of the trees before
it, so on a sample this small (`n_eff` ~ train windows / h) it can chase noise the
earlier trees left; bagging averages DEEP-but-decorrelated trees instead, which is the
lower-variance choice for a rare 0/1 event. Both share `model.common.features`' six
window statistics, the design the selection ranked under.

⚠️ `min_samples_leaf` is the capacity knob, not `max_depth`: a leaf of 20 windows at
h=5 is ~4 independent observations, which is the floor below which a leaf's event rate
is noise. The default is 20 for that reason.

⚠️ `class_weight=None` by default. Re-weighting the rare class moves the leaf means and
de-calibrates the probability, so `log_loss`/`brier` stop being readable; ranking
metrics (`dir_auc`, `pr_auc`) are unaffected either way.
"""

from __future__ import annotations

import numpy as np

from model.common.features import window_statistics

KINDS = ("et", "rf")


class ForestWindow:
    """`.fit(X, y)` / `.predict(X)` / `.predict_logit(X)` over `(n, lookback, n_features)`."""

    def __init__(self, n_features: int, lookback: int, kind: str = "et",
                 n_estimators: int = 500, min_samples_leaf: int = 20,
                 max_features: float = 0.3, max_depth=None, class_weight=None,
                 random_state: int = 42):
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
        self.kind = kind
        self.task = "regression"
        self.params = dict(
            n_estimators=int(n_estimators),
            min_samples_leaf=int(min_samples_leaf),
            max_features=max_features,
            max_depth=None if max_depth is None else int(max_depth),
            random_state=int(random_state),
            n_jobs=-1,
        )
        self.class_weight = class_weight
        self.n_params = 0

    def set_task(self, task: str) -> None:
        if task not in ("regression", "classification"):
            raise ValueError(f"unknown task {task!r}")
        self.task = task

    def fit(self, X: np.ndarray, y: np.ndarray) -> "ForestWindow":
        from sklearn.ensemble import (
            ExtraTreesClassifier,
            ExtraTreesRegressor,
            RandomForestClassifier,
            RandomForestRegressor,
        )

        if self.task == "classification":
            cls = ExtraTreesClassifier if self.kind == "et" else RandomForestClassifier
            self.model_ = cls(**self.params, class_weight=self.class_weight)
        else:
            cls = ExtraTreesRegressor if self.kind == "et" else RandomForestRegressor
            self.model_ = cls(**self.params)
        self.model_.fit(window_statistics(X), y)
        # Decision nodes, the same capacity measure `model.gbt` reports.
        self.n_params = int(sum(t.tree_.node_count - t.tree_.n_leaves
                                for t in self.model_.estimators_))
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model_.predict(window_statistics(X))

    def predict_logit(self, X: np.ndarray) -> np.ndarray:
        """Log-odds of the forest's vote share, clipped so a unanimous leaf is finite."""
        if self.task != "classification":
            raise RuntimeError("predict_logit on a regression forest")
        p = np.clip(self.model_.predict_proba(window_statistics(X))[:, 1], 1e-4, 1 - 1e-4)
        return np.log(p / (1.0 - p))


def build_model(n_features: int, lookback: int, **kwargs) -> ForestWindow:
    return ForestWindow(n_features, lookback, **kwargs)


def arch_dict(n_features: int, lookback: int, **kwargs) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "ForestWindow",
        "module": "model",
        "builder": "build_model",
        "kwargs": {"n_features": int(n_features), "lookback": int(lookback), **kwargs},
    }
