"""Gradient-boosted trees over the window statistics — Tier 2.

⚠️ **This is the estimator `feature_selection` has been ranking with all along.**
`xgb_gain`, `xgb_shap` and `permutation` are XGBoost fits on the same `(n, f*6)` design
this builds, under the same purged walk-forward — so the selection's `+0.0783` IC *is*
substantially an XGBoost result. What was missing was the same estimator scored as a
RUN, against `result_evaluator`'s null, in the shared leaderboard, where it can be read
against the LSTM and the ridge. That is all this package adds.

⚠️ **Shallow by default, and that is the whole point of putting it in Tier 2.**
`max_depth=3`, 200 rounds, heavy subsampling. Train carries 2,939 windows but `n_eff` is
588 on label overlap and **122** on window overlap; a deep forest on 122 independent
observations memorises the training period. The capacity ladder measured in §14 —
25-parameter ridge best, 4,961-parameter LSTM negative — is the reason to start small
here too.

⚠️ **`subsample`/`colsample` make the GPU and the CPU disagree.** `feature_selection`
CONTEXT §5 measured it: with sampling on, XGBoost draws from a different RNG stream on
CUDA, 4,189 of 8,280 nodes pick a different feature, and the kept feature set changes.
This runs on **CPU** with a pinned seed so the run is reproducible; the design is 24
columns, which is far too little work per kernel launch for a GPU to help anyway
(measured: 21.2 s CUDA vs 12.3 s host on a comparable narrow pool).
"""

from __future__ import annotations

import numpy as np

from model.common.features import WINDOW_STATS, window_statistics


class GBTRegressor:
    """XGBoost on the six window statistics per channel.

    `.fit(X, y)` / `.predict(X)` over `(n, lookback, n_features)`, matching the
    estimator protocol `engine.train_estimator` expects.
    """

    def __init__(self, n_features: int, lookback: int, max_depth: int = 3,
                 n_estimators: int = 200, learning_rate: float = 0.05,
                 subsample: float = 0.8, colsample_bytree: float = 0.8,
                 min_child_weight: float = 5.0, reg_lambda: float = 1.0,
                 random_state: int = 42, gamma: float = 0.0,
                 scale_pos_weight: float = 1.0):
        self.n_features = int(n_features)
        # ⚠️ `task` is set by the ENGINE (`set_task`), never by the config: the config's
        # own `task:` field is the one authority, and a second copy inside `model:` could
        # disagree with it.
        self.task = "regression"
        self.scale_pos_weight = float(scale_pos_weight)
        self.params = dict(
            max_depth=int(max_depth),
            n_estimators=int(n_estimators),
            learning_rate=float(learning_rate),
            subsample=float(subsample),
            colsample_bytree=float(colsample_bytree),
            min_child_weight=float(min_child_weight),
            reg_lambda=float(reg_lambda),
            gamma=float(gamma),
            random_state=int(random_state),
            # ⚠️ CPU, deliberately. See the module docstring.
            device="cpu",
            tree_method="hist",
            n_jobs=0,
        )
        # A tree ensemble has no weight count; the honest analogue for the capacity
        # ladder is the number of decision NODES, filled in after `fit`.
        self.n_params = 0

    def set_task(self, task: str) -> None:
        """`classification` swaps in `XGBClassifier` (binary:logistic) on the same design."""
        if task not in ("regression", "classification"):
            raise ValueError(f"unknown task {task!r}")
        self.task = task

    def fit(self, X: np.ndarray, y: np.ndarray) -> "GBTRegressor":
        from xgboost import XGBClassifier, XGBRegressor

        if self.task == "classification":
            # ⚠️ `scale_pos_weight` re-weights the rare class and so DE-CALIBRATES the
            # probability; left at 1.0 by default so log-loss and Brier stay readable.
            self.model_ = XGBClassifier(
                **self.params,
                objective="binary:logistic",
                eval_metric="logloss",
                scale_pos_weight=self.scale_pos_weight,
            ).fit(window_statistics(X), y)
        else:
            self.model_ = XGBRegressor(**self.params).fit(window_statistics(X), y)
        # ⚠️ `n_params` in `index.csv` is a CAPACITY column, so a tree model must put
        # something comparable in it or the ladder in §14 has a hole. A boosted ensemble
        # has no weights; its fitted degrees of freedom are the DECISION NODES (every
        # row of the dump that is not a leaf), so that is what goes in the column.
        frame = self.model_.get_booster().trees_to_dataframe()
        self.n_params = int((frame["Feature"] != "Leaf").sum())
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model_.predict(window_statistics(X))

    def predict_logit(self, X: np.ndarray) -> np.ndarray:
        """The MARGIN (log-odds), which `engine._write_predictions` sigmoids once."""
        if self.task != "classification":
            raise RuntimeError("predict_logit on a regression GBT")
        return self.model_.predict(window_statistics(X), output_margin=True)

    def importances(self, feature_columns) -> "dict":
        """`{stat__channel: gain}` — which window statistic of which channel the trees used."""
        from model.common.features import stat_names

        names = stat_names(feature_columns)
        score = self.model_.get_booster().get_score(importance_type="gain")
        return {names[int(k[1:])]: float(v) for k, v in score.items()}


def build_model(n_features: int, lookback: int, **kwargs) -> GBTRegressor:
    return GBTRegressor(n_features, lookback, **kwargs)


def arch_dict(n_features: int, lookback: int, **kwargs) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "GBTRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "lookback": int(lookback),
            **kwargs,
        },
    }
