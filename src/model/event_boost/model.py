"""Gradient-boosted trees for a binary EVENT label on a PANEL, fitted on the LAST ROW.

`model.gbt` compresses a window into six statistics per channel; at `d = 1` five of them are
copies or zeros, so a 151-channel basket table becomes a 906-column design (3.4 GB as float64
on 420k rows) for no information. This estimator reads the last row as it is — the design
`model.event_linear` and `model.event_panel` read — and runs XGBoost on the GPU.

| kind | objective | `predict_logit` returns |
|---|---|---|
| `xgb` | `binary:logistic` on the 0/1 event | the margin — logit P(event) |
| `rank` | `rank:pairwise`, one query per SESSION (the fit rows' dates) | a within-session score; ⚠️ its scale is not a probability and does not travel across sessions |
| `magnitude` | `reg:squarederror` on `log(abs(log(1 + r_h)) + eps)`, the auxiliary target `return_{h}day` | the regression mapped to the event by a 1-D logistic fitted on the SAME rows — logit P(event); `model.event_linear`'s magnitude ridge with a tree in place of the ridge |

⚠️ **MEASURED BEFORE IT WAS BUILT** (2026-09-17, `.claude/context/event_chain.md` §6): on a
rolling-origin CV over the LIQUID panel's train+val rows (validation years 2016-2023, no test
row read) a depth-8 classifier reached pooled AUC ~0.668 and within-session AUC ~0.666, against
0.652 / 0.650 for the event logit.

⚠️ **`columns` SELECTS CHANNELS BY NAME PREFIX** from the dataset's `feature_columns`, exactly
as `model.event_linear` does, and a prefix list that matches nothing RAISES.

⚠️ **THE DEVICE IS THE MODEL'S, NOT THE ENGINE'S.** The engine records every estimator run as
`cpu`; `device: cuda` moves the XGBoost FIT to the card and `provenance()` says so. Prediction
runs on the CPU (the design arrives as a host array). XGBoost's `hist` on CUDA draws its
subsamples from another stream than the CPU's (`model.gbt`), so a run is reproducible on the
device it names.
"""

from __future__ import annotations

from typing import Optional, Sequence

import numpy as np

KINDS = ("xgb", "rank", "magnitude")


class EventBoost:
    """`.fit(X, y)` / `.predict_logit(X)` over `(n, lookback, n_features)`; last row only."""

    needs_dataset = True

    def __init__(self, n_features: int, lookback: int, kind: str = "xgb",
                 columns: Optional[Sequence[str]] = None,
                 exclude: Optional[Sequence[str]] = None,
                 n_estimators: int = 800, max_depth: int = 8, learning_rate: float = 0.02,
                 subsample: float = 0.8, colsample_bytree: float = 0.4,
                 min_child_weight: float = 500.0, reg_lambda: float = 1.0,
                 max_bin: int = 256, half_life_years: Optional[float] = None,
                 aux_target: str = "return_5day", eps: float = 1e-3,
                 device: str = "cuda", seed: int = 42,
                 early_stopping_rounds: int = 0):
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
        self.kind = kind
        self.columns = tuple(columns) if columns else ()
        self.exclude = tuple(exclude) if exclude else ()
        self.device = str(device)
        self.aux_target = str(aux_target)
        self.eps = float(eps)
        self.half_life_years = None if half_life_years in (None, 0) else float(half_life_years)
        self.params = dict(n_estimators=int(n_estimators), max_depth=int(max_depth),
                           learning_rate=float(learning_rate), subsample=float(subsample),
                           colsample_bytree=float(colsample_bytree),
                           min_child_weight=float(min_child_weight), reg_lambda=float(reg_lambda),
                           max_bin=int(max_bin), random_state=int(seed), tree_method="hist")
        self.n_features = int(n_features)
        self.task = "classification"
        self.index_: list = list(range(self.n_features))
        self.names_: list = []
        self.fit_dates_ = None
        self.fit_aux_ = None
        self.fit_summary_: dict = {}
        self.n_params = 0
        # ⚠️ A CAP once early stopping is on, not a hyper-parameter — the val curve picks
        # the round. `kind='xgb'` only: a ranker's val query set is a different question and
        # the magnitude regressor is off the basket grid (one loss, `event_chain/config.py`).
        self.early_stopping_rounds = int(early_stopping_rounds) or 0
        self._val_X = None
        self._val_y = None

    # ------------------------------------------------------------ reporting
    @property
    def objective(self) -> str:
        return {"xgb": "binary:logistic (binary cross-entropy)",
                "magnitude": "reg:squarederror",
                "rank": "rank:pairwise"}[self.kind]

    @property
    def sample_weight_rule(self) -> str:
        return ("uniform" if self.half_life_years is None
                else f"exponential age decay, half-life {self.half_life_years} years")

    @property
    def early_stopping_rule(self) -> str:
        if self.kind != "xgb" or not self.early_stopping_rounds:
            return "none"
        if self._val_X is None:
            return "none (no val block: this is a refit, rounds frozen)"
        return f"val log-loss, patience {self.early_stopping_rounds} rounds"

    @property
    def best_iteration(self):
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

    def importances(self, feature_columns=None) -> dict:
        """`{channel: gain}` over the channels this model kept (`columns` / `exclude`)."""
        score = self.model_.get_booster().get_score(importance_type="gain")
        names = self.names_ or [f"f{i}" for i in self.index_]
        return {names[int(k[1:])]: float(v) for k, v in score.items()
                if int(k[1:]) < len(names)}

    # ------------------------------------------------------------ context
    def set_task(self, task: str) -> None:
        if task != "classification":
            raise ValueError(f"{type(self).__name__} scores a 0/1 event; task={task!r} has no path")
        self.task = task

    def set_dataset(self, dataset) -> None:
        names = list(((dataset.meta or {}).get("features") or {}).get("feature_columns") or [])
        if len(names) != self.n_features:
            raise ValueError(f"the dataset names {len(names)} feature columns for {self.n_features} channels")
        chosen = [i for i, n in enumerate(names)
                  if (not self.columns or n.startswith(self.columns))
                  and not (self.exclude and n.startswith(self.exclude))]
        if not chosen:
            raise ValueError(f"no feature column starts with {self.columns} outside {self.exclude}")
        self.index_ = chosen
        self.names_ = [names[i] for i in chosen]
        aux = (getattr(dataset, "aux", {}) or {}).get(self.aux_target, {})
        if self.kind == "magnitude" and "train" not in aux:
            raise ValueError(
                f"kind='magnitude' fits on the auxiliary target {self.aux_target!r}, which the dataset "
                f"does not carry — build it with train_test_creator aux_targets=({self.aux_target!r},)")
        # ⚠️ THE VAL BLOCK IS KEPT FOR EARLY STOPPING ONLY and the fit rows stay the train
        # split. Val now chooses the model, the basket's cut AND the boosting round, so
        # `val_daily_auc` is a selection score and TEST is the only honest read.
        self._val_X = dataset.X_val if self.early_stopping_rounds else None
        self._val_y = dataset.y_val if self.early_stopping_rounds else None
        self.set_fit_context(dates=dataset.dates_train, aux=aux.get("train"))

    def set_fit_context(self, dates=None, aux=None) -> None:
        """The label dates (query ids, ages) and auxiliary target of the NEXT `fit`'s rows."""
        self.fit_dates_ = None if dates is None else np.asarray(dates)
        self.fit_aux_ = None if aux is None else np.asarray(aux, dtype=float).ravel()

    # ------------------------------------------------------------- design
    def _design(self, X: np.ndarray) -> np.ndarray:
        return np.ascontiguousarray(np.asarray(X)[:, -1, self.index_], dtype=np.float32)

    def _dates(self, n: int) -> np.ndarray:
        if self.fit_dates_ is None or len(self.fit_dates_) != n:
            raise ValueError(
                f"fit got {n} rows and {None if self.fit_dates_ is None else len(self.fit_dates_)} "
                f"dates — call set_fit_context with the fit rows' dates")
        return np.asarray(self.fit_dates_, dtype="datetime64[D]")

    # ---------------------------------------------------------------- fit
    def fit(self, X: np.ndarray, y: np.ndarray) -> "EventBoost":
        from sklearn.linear_model import LogisticRegression
        from xgboost import XGBClassifier, XGBRanker, XGBRegressor

        Z = self._design(X)
        target = (np.asarray(y, dtype=float).ravel() >= 0.5).astype(int)
        dates = self._dates(len(Z))
        weight = None
        if self.half_life_years is not None:
            age = (dates.max() - dates).astype(float) / 365.25
            weight = np.power(0.5, age / self.half_life_years)
        if self.kind == "xgb":
            # ⚠️ EARLY STOPPING IS ON THE VAL BLOCK, and only when a dataset handed one over
            # (`set_dataset`). A REFIT has no val — `event_chain.report` fits on train+val —
            # so it arrives with `early_stopping_rounds=0` and the round this fit chose
            # frozen into `n_estimators`; stopping on rows inside the fit block would pick
            # the last round every time.
            stop = self.early_stopping_rounds if self._val_X is not None else 0
            eval_set = None
            if stop:
                Zval = self._design(np.asarray(self._val_X))
                yval = (np.asarray(self._val_y, dtype=float).ravel() >= 0.5).astype(int)
                # train first, val second: XGBoost stops on the LAST eval set.
                eval_set = [(Z, target), (Zval, yval)]
            self.model_ = XGBClassifier(**self.params, device=self.device,
                                        objective="binary:logistic", eval_metric="logloss",
                                        early_stopping_rounds=stop or None)
            self.model_.fit(Z, target, sample_weight=weight, eval_set=eval_set, verbose=False)
        elif self.kind == "magnitude":
            if self.fit_aux_ is None or len(self.fit_aux_) != len(Z):
                raise ValueError(
                    f"kind='magnitude' needs {len(Z)} auxiliary targets for its fit rows, has "
                    f"{None if self.fit_aux_ is None else len(self.fit_aux_)} — call set_fit_context")
            size = np.log(np.abs(np.log1p(self.fit_aux_)) + self.eps)
            ok = np.isfinite(size)
            self.model_ = XGBRegressor(**self.params, device=self.device, objective="reg:squarederror")
            self.model_.fit(Z[ok], size[ok], sample_weight=None if weight is None else weight[ok])
            score = self._raw(Z)
            self.score_mean_ = float(score[ok].mean())
            self.score_sd_ = float(score[ok].std()) or 1.0
            # ⚠️ Monotone, so it cannot change this model's own ranking; it exists so the
            # expected SIZE of the move can be combined with the other kinds as a probability.
            self.calibration_ = LogisticRegression(C=100.0, max_iter=5000)
            self.calibration_.fit(self._standard(score)[:, None], target)
        else:
            # ⚠️ A query must be CONTIGUOUS, so the rows are sorted by session (stable). XGBoost
            # weights a ranking objective per QUERY, so a row-level age weight has no place here.
            if weight is not None:
                raise ValueError("kind='rank' takes no half_life_years (weights are per query)")
            codes = np.unique(dates, return_inverse=True)[1]
            order = np.argsort(codes, kind="stable")
            self.model_ = XGBRanker(**self.params, device=self.device, objective="rank:pairwise")
            self.model_.fit(Z[order], target[order], qid=codes[order])
        dump = self.model_.get_booster().get_dump()
        self.n_params = int(sum(1 for tree in dump for node in tree.split("\n")
                                if node.strip() and "leaf=" not in node))
        self.fit_summary_ = {"kind": self.kind, "device": self.device, "rows": int(len(Z)),
                             "channels": len(self.index_), "decision_nodes": self.n_params,
                             "fit_dates": [str(dates.min()), str(dates.max())]}
        return self

    # ------------------------------------------------------------ predict
    def _raw(self, Z: np.ndarray) -> np.ndarray:
        booster = self.model_.get_booster()
        booster.set_param({"device": "cpu"})
        try:
            return np.asarray(self.model_.predict(Z, output_margin=True), dtype=float)
        finally:
            booster.set_param({"device": self.device})

    def _standard(self, score: np.ndarray) -> np.ndarray:
        return (np.asarray(score, dtype=float) - self.score_mean_) / self.score_sd_

    def predict_logit(self, X: np.ndarray) -> np.ndarray:
        raw = self._raw(self._design(X))
        if self.kind == "magnitude":
            return self.calibration_.decision_function(self._standard(raw)[:, None])
        return raw

    predict = predict_logit

    def provenance(self) -> dict:
        """What the last `fit` ran on — written into the run's metadata by the engine."""
        return dict(self.fit_summary_)


def build_model(n_features: int, lookback: int, **kwargs) -> EventBoost:
    return EventBoost(n_features, lookback, **kwargs)


def arch_dict(n_features: int, lookback: int, **kwargs) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "EventBoost",
        "module": "model",
        "builder": "build_model",
        "kwargs": {"n_features": int(n_features), "lookback": int(lookback), **kwargs},
    }
