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
**`device` is therefore part of the experimental setup and is recorded on the run** —
pin one before quoting a number, and never compare a CUDA run to a host one as though
the seed made them the same model.

⚠️ **THE DEFAULT IS `cpu` AND THE REASON IT GIVES EXPIRED** (2026-09-19). It used to read
*"the design is 24 columns, far too little work per kernel launch"*, citing 21.2 s CUDA
against 12.3 s host on a 27-column pool — true there and **false on the basket panel,
which is 151 columns and 420k rows**. The default stays `cpu` because the narrow callers
are still narrow; the basket's own configs pass `device="cuda"` (`event_chain/config.py`),
which is how a width-dependent choice belongs in the CONFIG rather than in this file.
"""

from __future__ import annotations

import numpy as np

from model.common.features import WINDOW_STATS, window_statistics


class GBTRegressor:
    """XGBoost on the six window statistics per channel.

    `.fit(X, y)` / `.predict(X)` over `(n, lookback, n_features)`, matching the
    estimator protocol `engine.train_estimator` expects.
    """

    # ⚠️ For the VAL BLOCK ONLY (early stopping). The fit rows stay the train split, and
    # `set_dataset` says what that costs.
    needs_dataset = True

    def __init__(self, n_features: int, lookback: int, max_depth: int = 3,
                 n_estimators: int = 200, learning_rate: float = 0.05,
                 subsample: float = 0.8, colsample_bytree: float = 0.8,
                 min_child_weight: float = 5.0, reg_lambda: float = 1.0,
                 random_state: int = 42, gamma: float = 0.0,
                 scale_pos_weight: float = 1.0, device: str = "cpu",
                 early_stopping_rounds: int = 0):
        self.n_features = int(n_features)
        # ⚠️ `n_estimators` stops being a hyper-parameter once this is set: it becomes a
        # CAP and the val curve picks the round. Raise the cap when you turn this on.
        self.early_stopping_rounds = int(early_stopping_rounds) or 0
        self._val_X = None
        self._val_y = None
        self._val_design = None
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
            # ⚠️ The CALLER's choice — see the module docstring on what it changes.
            device=str(device),
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

    @property
    def objective(self) -> str:
        return ("binary:logistic (binary cross-entropy)" if self.task == "classification"
                else "reg:squarederror")

    @property
    def sample_weight_rule(self) -> str:
        return ("uniform" if self.scale_pos_weight == 1.0
                else f"scale_pos_weight={self.scale_pos_weight}")

    def set_dataset(self, dataset) -> None:
        """Keep the VAL block for early stopping. ⚠️ The FIT ROWS are still train only.

        ⚠️ **VAL NOW CARRIES THREE JOBS** (decided 2026-09-19 by the user, against the
        alternative of carving the stop block out of train): it chooses the model, it
        chooses the basket's `min_prob` cut, and it stops the boosting. **So `val_daily_auc`
        is no longer an out-of-sample estimate of anything** — it is a selection score, and
        TEST is the only honest read. ⚠️ **And the refit paths have no val at all**
        (`event_chain.report` refits on train+val), so they reuse the round this fit chose
        rather than stopping again — see `best_iteration`.
        """
        # ⚠️ The DESIGN is not built here: `window_statistics` on the val block is a
        # second copy of it in memory, and a run with early stopping off must not pay
        # for a block it will never look at.
        self._val_X = dataset.X_val
        self._val_y = np.asarray(dataset.y_val, dtype=float).ravel()

    @property
    def best_iteration(self):
        """The round the val curve chose, or None when nothing stopped this fit."""
        return getattr(self.model_, "best_iteration", None) if self.early_stopping_rounds \
            else None

    def loss_history(self) -> list:
        """`[{step, train_loss, val_loss}]` per boosting round — `engine`'s contract."""
        result = getattr(self.model_, "evals_result_", None) or {}
        train = list((result.get("validation_0") or {}).values())
        val = list((result.get("validation_1") or {}).values())
        if not train:
            return []
        return [{"step": i, "train_loss": float(t), "val_loss": float(v)}
                for i, (t, v) in enumerate(zip(train[0], val[0] if val else train[0]))]

    def fit(self, X: np.ndarray, y: np.ndarray) -> "GBTRegressor":
        from xgboost import XGBClassifier, XGBRegressor

        design = window_statistics(X)
        fit_kwargs = {}
        if self._val_X is not None and self.early_stopping_rounds:
            self._val_design = window_statistics(np.asarray(self._val_X, dtype=float))
            y_val = (self._val_y >= 0.5).astype(int) if self.task == "classification" \
                else self._val_y
            # ⚠️ TRAIN FIRST, VAL SECOND: `evals_result` keys on position
            # (`validation_0`/`validation_1`) and XGBoost early-stops on the LAST one.
            fit_kwargs = {"eval_set": [(design, y), (self._val_design, y_val)],
                          "verbose": False}
        if self.task == "classification":
            # ⚠️ `scale_pos_weight` re-weights the rare class and so DE-CALIBRATES the
            # probability; left at 1.0 by default so log-loss and Brier stay readable.
            self.model_ = XGBClassifier(
                **self.params,
                objective="binary:logistic",
                eval_metric="logloss",
                early_stopping_rounds=self.early_stopping_rounds if fit_kwargs else None,
                scale_pos_weight=self.scale_pos_weight,
            ).fit(design, y, **fit_kwargs)
        else:
            self.model_ = XGBRegressor(
                **self.params,
                early_stopping_rounds=self.early_stopping_rounds if fit_kwargs else None,
            ).fit(design, y, **fit_kwargs)
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
