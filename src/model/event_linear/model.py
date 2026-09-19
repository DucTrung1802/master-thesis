"""Linear estimators for a binary EVENT label, fitted on the LAST ROW of the window.

The event `utils.event_target` defines — *does the close rise by at least g % within h
sessions?* — is two questions stacked: will the price MOVE that far, and will the move be
UP. Each kind here answers one of them, and `event_chain.report` combines them:

| kind | fitted on | `predict_logit` returns |
|---|---|---|
| `event_logit` | the 0/1 event itself | logit P(event) |
| `magnitude_ridge` | `log(abs(log(1 + r_h)) + eps)`, the auxiliary target `return_{h}day` | the ridge's prediction mapped to the event by a 1-D logistic fitted on the SAME training rows — logit P(event) |
| `direction_logit` | `1{r_h > 0}` on the rows where `abs(log(1 + r_h)) >= min_move` | logit P(up, given a move that size) |

⚠️ **MEASURED BEFORE IT WAS BUILT** (2026-09-17, `.claude/context/event_chain.md` §6): on a
10-fold rolling-origin CV over 2014-2023 that never read a test row, the magnitude ridge
and the event logit each scored CV AUC ~0.67 on VCB, the windowed networks and trees of
the first three trials 0.52-0.63, and a geometric mean of the three kinds ~0.69. A
+5 %-in-5-sessions move is a VOLATILITY question first, and `log|r_h|` is close to linear
in log volatility (the HAR model) — which is why a ridge on `har_*` reads what a tree on
`evt_vol_*` levels does not.

⚠️ **`columns` SELECTS CHANNELS BY NAME PREFIX** from the dataset's own
`features.feature_columns`, so one `__final__` table can feed three models three different
blocks. A prefix that matches nothing RAISES — an empty design would fit an intercept and
score a constant, which is an AUC of exactly 0.5 and looks like a result.

⚠️ **THE AUXILIARY TARGET IS READ FROM THE DATASET, NEVER THE DATABASE** (`aux_*.npy`,
`train_test_creator` `aux_targets`), and it is aligned with `y` sample for sample. By
default the fit rows are the TRAIN split; `set_fit_context` points them at another block
(`event_chain.report` refits on train+val and walks forward by year).

⚠️ The design is the dataset's train-standardised last row, CLIPPED to ±`clip` sigmas: a
level that drifted out of the train range (`EVD-1`) saturates at the clip instead of
dominating a linear score.
"""

from __future__ import annotations

from typing import Optional, Sequence

import numpy as np

KINDS = ("event_logit", "magnitude_ridge", "direction_logit")


class EventLinear:
    """`.fit(X, y)` / `.predict_logit(X)` over `(n, lookback, n_features)`; last row only."""

    needs_dataset = True

    def __init__(self, n_features: int, lookback: int, kind: str = "event_logit",
                 columns: Optional[Sequence[str]] = None,
                 exclude: Optional[Sequence[str]] = None, C: float = 0.1,
                 alpha: float = 10.0, half_life_years: Optional[float] = None,
                 min_move: float = 0.03, aux_target: str = "return_5day",
                 eps: float = 1e-3, clip: float = 5.0, max_iter: int = 5000):
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
        self.kind = kind
        self.columns = tuple(columns) if columns else ()
        self.exclude = tuple(exclude) if exclude else ()
        self.C = float(C)
        self.alpha = float(alpha)
        self.half_life_years = None if half_life_years in (None, 0) else float(half_life_years)
        self.min_move = float(min_move)
        self.aux_target = str(aux_target)
        self.eps = float(eps)
        self.clip = float(clip)
        self.max_iter = int(max_iter)
        self.n_features = int(n_features)
        self.task = "classification"
        self.index_ = list(range(self.n_features))
        self.feature_names_: list = []
        self.fit_dates_ = None
        self.fit_aux_ = None
        self.n_params = 0

    # ------------------------------------------------------------ context
    def set_task(self, task: str) -> None:
        if task != "classification":
            raise ValueError(f"{type(self).__name__} scores a 0/1 event; task={task!r} has no path")
        self.task = task

    def set_dataset(self, dataset) -> None:
        names = list(((dataset.meta or {}).get("features") or {}).get("feature_columns") or [])
        if len(names) != self.n_features:
            raise ValueError(
                f"the dataset names {len(names)} feature columns for {self.n_features} channels"
            )
        self.feature_names_ = names
        chosen = range(len(names))
        if self.columns:
            chosen = [i for i in chosen if names[i].startswith(self.columns)]
        if self.exclude:
            chosen = [i for i in chosen if not names[i].startswith(self.exclude)]
        self.index_ = list(chosen)
        if not self.index_:
            raise ValueError(f"no feature column starts with {self.columns} outside {self.exclude}")
        needs_aux = self.kind in ("magnitude_ridge", "direction_logit")
        aux = (getattr(dataset, "aux", {}) or {}).get(self.aux_target, {})
        if needs_aux and "train" not in aux:
            raise ValueError(
                f"{self.kind} fits on the auxiliary target {self.aux_target!r}, which the dataset "
                f"does not carry — build it with train_test_creator aux_targets=({self.aux_target!r},)"
            )
        self.set_fit_context(dates=dataset.dates_train, aux=aux.get("train"))

    def set_fit_context(self, dates=None, aux=None) -> None:
        """The label dates and auxiliary target of the rows the NEXT `fit` receives."""
        self.fit_dates_ = None if dates is None else np.asarray(dates)
        self.fit_aux_ = None if aux is None else np.asarray(aux, dtype=float).ravel()

    # ------------------------------------------------------------- design
    def _design(self, X: np.ndarray) -> np.ndarray:
        Z = np.asarray(X, dtype=float)[:, -1, :][:, self.index_]
        return np.clip(np.nan_to_num(Z, nan=0.0, posinf=self.clip, neginf=-self.clip),
                       -self.clip, self.clip)

    def _weights(self, n: int) -> Optional[np.ndarray]:
        if self.half_life_years is None:
            return None
        if self.fit_dates_ is None or len(self.fit_dates_) != n:
            raise ValueError("half_life_years needs the fit rows' dates (set_fit_context)")
        dates = np.asarray(self.fit_dates_, dtype="datetime64[D]")
        age_years = (dates.max() - dates).astype(float) / 365.25
        return np.power(0.5, age_years / self.half_life_years)

    def _aux(self, n: int) -> np.ndarray:
        if self.fit_aux_ is None or len(self.fit_aux_) != n:
            raise ValueError(
                f"{self.kind} needs {n} auxiliary targets for its fit rows, has "
                f"{None if self.fit_aux_ is None else len(self.fit_aux_)} — call set_fit_context"
            )
        return self.fit_aux_

    # ---------------------------------------------------------------- fit
    def fit(self, X: np.ndarray, y: np.ndarray) -> "EventLinear":
        from sklearn.linear_model import LogisticRegression, Ridge

        Z = self._design(X)
        event = (np.asarray(y, dtype=float).ravel() >= 0.5).astype(int)
        w = self._weights(len(Z))
        if self.kind == "event_logit":
            self.model_ = LogisticRegression(C=self.C, max_iter=self.max_iter)
            self.model_.fit(Z, event, sample_weight=w)
            self.n_params = Z.shape[1] + 1
        elif self.kind == "magnitude_ridge":
            move = np.log1p(self._aux(len(Z)))
            target = np.log(np.abs(move) + self.eps)
            ok = np.isfinite(target)
            self.model_ = Ridge(alpha=self.alpha)
            self.model_.fit(Z[ok], target[ok], sample_weight=None if w is None else w[ok])
            score = self.model_.predict(Z)
            self.score_mean_ = float(score[ok].mean())
            self.score_sd_ = float(score[ok].std()) or 1.0
            # ⚠️ The map from "expected size of the move" to "P(event)" is fitted on the same
            # rows as the ridge. It is monotone, so it cannot change this model's own ranking
            # (its AUC); it exists so the probability can be COMBINED with the other kinds.
            self.calibration_ = LogisticRegression(C=100.0, max_iter=self.max_iter)
            self.calibration_.fit(self._standard(score)[:, None], event)
            self.n_params = Z.shape[1] + 3
        else:
            move = np.log1p(self._aux(len(Z)))
            ok = np.isfinite(move) & (np.abs(move) >= self.min_move)
            if ok.sum() < 20 or len(np.unique(move[ok] > 0)) < 2:
                raise ValueError(f"direction_logit has {int(ok.sum())} rows moving >= {self.min_move}")
            self.model_ = LogisticRegression(C=self.C, max_iter=self.max_iter)
            self.model_.fit(Z[ok], (move[ok] > 0).astype(int),
                            sample_weight=None if w is None else w[ok])
            self.n_params = Z.shape[1] + 1
        return self

    def _standard(self, score: np.ndarray) -> np.ndarray:
        return (np.asarray(score, dtype=float) - self.score_mean_) / self.score_sd_

    # ------------------------------------------------------------ predict
    def predict_logit(self, X: np.ndarray) -> np.ndarray:
        Z = self._design(X)
        if self.kind == "magnitude_ridge":
            return self.calibration_.decision_function(self._standard(self.model_.predict(Z))[:, None])
        return self.model_.decision_function(Z)

    predict = predict_logit

    def coefficients(self) -> dict:
        """`{channel: coefficient}` on the standardised design, largest magnitude first."""
        names = [self.feature_names_[i] for i in self.index_] if self.feature_names_ else self.index_
        coef = np.ravel(self.model_.coef_)
        order = np.argsort(-np.abs(coef))
        return {str(names[i]): float(coef[i]) for i in order}

    # `engine._write_importances` reads this name on every family; the SIGNED coefficient
    # is the honest one for a linear model, and the file is sorted by magnitude either way.
    importances = coefficients

    @property
    def objective(self) -> str:
        return {"event_logit": "binary cross-entropy (L2 logistic, the event label)",
                "magnitude_ridge": "mean squared error (ridge on log|log(1+r_h)|), "
                                   "then a 1-D logistic calibration",
                "direction_logit": "binary cross-entropy on a DIFFERENT label "
                                   "(1{r>0} on rows with |move| >= min_move)"}[self.kind]

    @property
    def sample_weight_rule(self) -> str:
        return ("uniform" if self.half_life_years is None
                else f"exponential age decay, half-life {self.half_life_years} years")

    @property
    def early_stopping_rule(self) -> str:
        """⚠️ **CONVEX: THERE IS NO CURVE TO STOP.** The capacity knob is `C` / `alpha`,
        not a round count, so this family cannot answer an early-stopping requirement —
        `event_chain/config.py` records that as a property of the grid, not an omission."""
        return "none: convex fit, capacity set by C/alpha"


def build_model(n_features: int, lookback: int, **kwargs) -> EventLinear:
    return EventLinear(n_features, lookback, **kwargs)


def arch_dict(n_features: int, lookback: int, **kwargs) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "EventLinear",
        "module": "model",
        "builder": "build_model",
        "kwargs": {"n_features": int(n_features), "lookback": int(lookback), **kwargs},
    }
