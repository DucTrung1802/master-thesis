"""Bagged trees over the window statistics — ExtraTrees, RandomForest, and a GPU one.

⚠️ **`xgbrf` IS A THIRD KIND, AND THE CARD IS NOT WHY IT IS FASTER** (2026-09-19). sklearn
has no CUDA path and cuML is Linux-only, so a bagged-trees member that runs on the card has
to be XGBoost's own random forest — `num_parallel_tree=N, n_estimators=1, learning_rate=1`,
which grows N decorrelated trees in ONE boosting round. **Measured on the basket panel
through THIS CLASS, so `window_statistics` is inside the timing** (419,923 x 151 -> a
906-column design, RTX 3050 4 GB):

| kind | device | fit | val pooled AUC |
|---|---|---|---|
| `et` (ExtraTrees, 200 x leaf 200) | cpu | **1,103.8 s** | — |
| `xgbrf` 200 x depth 12 | **cuda** | **371.6 s** | 0.6481 |
| `xgbrf` 200 x depth 12 | cpu | **372.6 s** | 0.6464 |

⚠️ **THE TWO DEVICES ARE 1.0 s APART ON 372** — the card buys NOTHING here, and the 3x is
the ESTIMATOR (one Newton step with histogram splits against 200 independently grown
sklearn trees). Do not read `device="cuda"` in the config as the reason this is faster.

⚠️ **AND THE FIRST VERSION OF THIS TABLE CLAIMED 20x, MEASURED WRONG.** It fed the raw 151
columns straight to XGBoost and read 55.0 s, while every caller of this class goes through
`window_statistics` first — which at lookback 1 returns **906 columns**. A benchmark that
skips the transform the production path always runs is measuring a different model.

⚠️ **The two are different estimators and their numbers are not interchangeable**:
ExtraTrees splits at random thresholds and averages equal-weight votes; `xgbrf` fits one
Newton step on the logistic loss with histogram splits. ⚠️ **`min_samples_leaf` becomes
`min_child_weight`, which counts HESSIAN and not ROWS** — for the logistic loss the hessian
is `p(1-p) <= 0.25`, so a 200-row floor is at most 50 of hessian and the config says 50.
⚠️ **And `max_depth` is REQUIRED where sklearn allowed None**: XGBoost needs a cap.

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

KINDS = ("et", "rf", "xgbrf")


class ForestWindow:
    """`.fit(X, y)` / `.predict(X)` / `.predict_logit(X)` over `(n, lookback, n_features)`."""

    # ⚠️ The engine hands this model its window in the DATASET's float32 rather than a
    # float64 copy (4.68 GiB at d=10). A tree rounds its input to float32 anyway, and
    # `window_statistics` upcasts each row block to float64 before any arithmetic, so the
    # copy bought nothing but the out-of-memory on 2026-09-19.
    input_dtype = np.float32

    def __init__(self, n_features: int, lookback: int, kind: str = "et",
                 n_estimators: int = 500, min_samples_leaf: int = 20,
                 max_features: float = 0.3, max_depth=None, class_weight=None,
                 random_state: int = 42, device: str = "cpu",
                 min_child_weight: float = 50.0, subsample: float = 0.8):
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
        self.kind = kind
        self.task = "regression"
        self.device = str(device)
        if kind == "xgbrf":
            if max_depth is None:
                raise ValueError("xgbrf needs an explicit max_depth — XGBoost has no unlimited tree")
            # ⚠️ ONE boosting round of `n_estimators` parallel trees at full step size IS a
            # random forest; `colsample_bynode` is `max_features`, drawn per SPLIT as sklearn
            # draws it. See the module docstring for what does NOT carry over.
            self.params = dict(
                n_estimators=1, num_parallel_tree=int(n_estimators), learning_rate=1.0,
                max_depth=int(max_depth), min_child_weight=float(min_child_weight),
                colsample_bynode=float(max_features), subsample=float(subsample),
                tree_method="hist", device=str(device), random_state=int(random_state),
                verbosity=0,
            )
            self.class_weight = class_weight
            self.n_params = 0
            return
        if device != "cpu":
            raise ValueError(f"kind {kind!r} is sklearn and has no CUDA path; use kind='xgbrf'")
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
        if self.kind == "xgbrf":
            return self._fit_xgbrf(X, y)
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
        self.model_.fit(window_statistics(X, dtype=np.float32), y)
        # Decision nodes, the same capacity measure `model.gbt` reports.
        self.n_params = int(sum(t.tree_.node_count - t.tree_.n_leaves
                                for t in self.model_.estimators_))
        return self

    def _fit_xgbrf(self, X: np.ndarray, y: np.ndarray) -> "ForestWindow":
        import xgboost as xgb

        if self.task != "classification":
            raise RuntimeError("kind 'xgbrf' is a classifier; the basket grid is the only caller")
        Z = window_statistics(X, dtype=np.float32)
        self.model_ = xgb.XGBClassifier(objective="binary:logistic", eval_metric="logloss",
                                        **self.params)
        self.model_.fit(Z, np.asarray(y).ravel())
        # ⚠️ PREDICTION MOVES BACK TO THE HOST once the trees exist: scoring a few hundred
        # thousand rows is cheap and the card is the scarce resource (`event_boost` does the
        # same). The fitted trees are identical either way — only the fit is stochastic.
        self.model_.get_booster().set_param({"device": "cpu"})
        df = self.model_.get_booster().trees_to_dataframe()
        self.n_params = int((df["Feature"] != "Leaf").sum())
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model_.predict(window_statistics(X, dtype=np.float32))

    def predict_logit(self, X: np.ndarray) -> np.ndarray:
        """Log-odds of the forest's vote share, clipped so a unanimous leaf is finite."""
        if self.task != "classification":
            raise RuntimeError("predict_logit on a regression forest")
        p = np.clip(self.model_.predict_proba(window_statistics(X, dtype=np.float32))[:, 1], 1e-4, 1 - 1e-4)
        return np.log(p / (1.0 - p))

    def provenance(self) -> dict:
        """What the run folder records about WHERE this was fitted (`device` is part of the
        experimental setup for any sampled XGBoost — `model.gbt`'s docstring says why)."""
        return {"kind": self.kind, "device": self.device,
                "backend": "xgboost" if self.kind == "xgbrf" else "sklearn"}

    @property
    def objective(self) -> str:
        if self.task != "classification":
            return "mean squared error (variance reduction at the split)"
        return ("binary:logistic (binary cross-entropy)" if self.kind == "xgbrf"
                else "Gini impurity at the split (sklearn), not a global loss")

    @property
    def early_stopping_rule(self) -> str:
        """⚠️ **A FOREST HAS NOTHING TO STOP.** `xgbrf` is ONE boosting round of
        `num_parallel_tree` trees at full step size, and a bagged sklearn forest is not
        sequential at all — there is no val curve to watch, so its `loss_history.csv` is
        one row by construction rather than by omission (`engine._write_loss_history`)."""
        return "none: a forest is one round, there is no curve to stop on"

    def importances(self, feature_columns=None) -> dict:
        """`{stat__channel: importance}` — gain under `xgbrf`, impurity drop under sklearn.

        ⚠️ The two are NOT on one scale; `engine._write_importances` records which.
        """
        from model.common.features import stat_names

        names = stat_names(list(feature_columns or []))
        if self.kind == "xgbrf":
            score = self.model_.get_booster().get_score(importance_type="gain")
            return {names[int(k[1:])]: float(v) for k, v in score.items()
                    if int(k[1:]) < len(names)}
        values = getattr(self.model_, "feature_importances_", None)
        if values is None or len(names) != len(values):
            return {}
        return {n: float(v) for n, v in zip(names, values) if v > 0}


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
