"""Tier-1 baselines — the rung of the ladder the study skipped.

Four estimators over the same `(n, lookback, n_features)` windows the networks eat,
selected by `model: {type: BASELINE, kind: …}`:

| `kind` | what it predicts | params |
|---|---|---|
| `zero` | the constant 0 | **0** |
| `mean` | the TRAIN mean of the scaled target | 1 |
| `ridge_stats` | Ridge on 6 window statistics per channel | `6·n + 1` |
| `ridge_flat` | Ridge on the flattened window | `d·n + 1` |
| `ar` | Ridge on the target channel's own last `p` values | `p + 1` |

## ⚠️ Why these exist at all

`src/model/runs/index.csv` held 31 runs and **not one linear model or constant**. The
study went straight to sequence models, and both of them — an LSTM at 4,961 parameters
and a CNN at 3,745 — **lose to a zero predictor on test RMSE** (0.0383 and 0.0373
against 0.0372). That comparison is currently made *inside* a run, as the
`RMSE_zero_baseline` column. Making it a run of its own is what lets it be compared on
`ic`, `dir_auc` and `hit_rate` too, and puts it in the same table under the same null.

⚠️ **The capacity argument is the reason to expect these to do as well as the
networks.** Train carries 2,939 windows but `n_eff` is `n/h` = **588** (label overlap)
and `n/(d+h-1)` = **122** (window overlap). A 25-parameter ridge is the model this
sample size supports; a 4,961-parameter LSTM is not, and `.claude/context/feature_selection.md`
§6d puts the observations needed to separate an IC of 0.05 from zero at ~1,500.

⚠️ **`LassoCV` already zeroed every coefficient on this pool** (§4) — no linear signal
survived cross-validated shrinkage on the 27-channel version. Expect Ridge to land near
zero. **That is the point**: a linear model reaching the same answer as an LSTM is a
far stronger statement about the data than another network reaching it.

## ⚠️ Everything is fitted on the TRAIN SPLIT ONLY

The dataset's features are already standardised with train-slice statistics
(`.claude/context/train_test_creator.md` §6) and its target is already scaled, so these fit on
`X_train`/`y_train` and never look at val or test. `Ridge` uses a fixed `alpha` rather
than `RidgeCV`: a CV inside the train split would be a second selection step that the
evaluator's null does not price in (issue **NUL-1**), and the whole point of a baseline
is that it has no knobs to tune.
"""

from __future__ import annotations

import numpy as np

# ⚠️ Shared with `model/gbt` and `model/mlp`, and matching
# `feature_selection.windows.window_design` — a model reducing the window differently
# would be fed a design the ranking never scored. See `model/common/features.py`.
from model.common.features import WINDOW_STATS, window_statistics

KINDS = ("zero", "mean", "ridge_stats", "ridge_flat", "ar",
         "prior", "logistic_stats", "logistic_channel")

# ⚠️ **THE THREE CLASSIFICATION BASELINES (2026-09-16)** — for a 0/1 event label such as
# `up_5pct_5day` (`utils.event_target`). Each exposes `predict_logit`, which
# `engine.train_estimator` requires of a classifier: the engine sigmoids the raw output
# exactly once, so a probability returned here would be squashed twice.
#
# | `kind` | score | params |
# |---|---|---|
# | `prior` | the TRAIN base rate, as a constant log-odds | 1 |
# | `logistic_stats` | L2 logistic regression on the 6 window statistics per channel | `6·n + 1` |
# | `logistic_channel` | logistic regression on ONE named channel's last value | 2 |
#
# `prior` has no AUC (a constant ranks nothing) and is the reference for `log_loss` and
# `brier`. `logistic_channel` is the one-feature question — does a single volatility
# column already carry what a 1,000-column model finds? — and it resolves its channel by
# NAME for the reason `ARPredictor` does.
CLASSIFICATION_KINDS = ("prior", "logistic_stats", "logistic_channel")


class _Base:
    """Common `.fit`/`.predict` shape. `n_params` is what lands in `index.csv`."""

    n_params = 0

    def fit(self, X: np.ndarray, y: np.ndarray) -> "_Base":
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        raise NotImplementedError


class ZeroPredictor(_Base):
    """Predicts a forward return of **exactly zero**. Zero parameters, nothing fitted.

    ⚠️ **It does NOT emit 0.0, and the difference is the whole point.** Every estimator
    here works in the SCALED target space, and `engine._write_predictions`
    inverse-transforms whatever it returns back to the return scale. Emitting 0.0 there
    would inverse-transform to the TRAIN MEAN return — which is `MeanPredictor`, a
    different and worse model. Measured 2026-08-10: the first version of this class did
    exactly that, and its RMSE came out at 0.03726 against the 0.037212 that
    `RMSE_zero_baseline` reports for a literal zero. Two baselines that should have been
    identical differed in the fourth decimal, which is how the bug was found.

    So it asks the target scaler which scaled value maps to a return of 0, and emits
    that. `RMSE` for this run must now equal `RMSE_zero_baseline` exactly — the
    self-check that keeps the reference point honest.

    ⚠️ This is the incumbent every run already reports itself against, and the one both
    networks lose to on test.
    """

    n_params = 0
    # Asks `engine.train_estimator` for the dataset, so it can invert the target scaler.
    needs_dataset = True

    def __init__(self):
        self.scaled_zero_ = 0.0

    def set_dataset(self, dataset) -> None:
        scaler = getattr(dataset, "target_scaler", None)
        if scaler is None:
            # An unscaled target: 0 in scaled space IS a zero return.
            self.scaled_zero_ = 0.0
            return
        # The scaled value `z` such that inverse_transform(z) == 0.0.
        self.scaled_zero_ = float(
            scaler.transform(np.zeros((1, 1), dtype=float)).ravel()[0]
        )

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.full(len(X), self.scaled_zero_, dtype=float)


class MeanPredictor(_Base):
    """Predicts the train mean. One parameter — the least a fitted model can have."""

    n_params = 1

    def fit(self, X, y):
        self.value_ = float(np.mean(y))
        return self

    def predict(self, X):
        return np.full(len(X), self.value_, dtype=float)


class RidgeWindow(_Base):
    """Ridge on either the 6 window statistics per channel, or the flat window.

    ⚠️ `alpha` is FIXED, not cross-validated. `RidgeCV` inside the train split is a
    second selection step, and the evaluator's null prices in no selection at all
    (**NUL-1**) — a baseline with a tuned hyper-parameter is not a baseline.
    """

    def __init__(self, n_features: int, lookback: int, design: str = "stats",
                 alpha: float = 1.0):
        if design not in ("stats", "flat"):
            raise ValueError(f"design must be 'stats' or 'flat', got {design!r}")
        self.design = design
        self.alpha = float(alpha)
        width = n_features * len(WINDOW_STATS) if design == "stats" else lookback * n_features
        self.n_params = width + 1  # + intercept

    def _design(self, X: np.ndarray) -> np.ndarray:
        if self.design == "stats":
            return window_statistics(X)
        return X.reshape(len(X), -1)

    def fit(self, X, y):
        from sklearn.linear_model import Ridge

        self.model_ = Ridge(alpha=self.alpha).fit(self._design(X), y)
        return self

    def predict(self, X):
        return self.model_.predict(self._design(X))


class ARPredictor(_Base):
    """Ridge on the TARGET CHANNEL's own last `p` values — no other feature.

    ⚠️ The window holds features, not the label's own history, so "AR" here means
    autoregression on `target_channel`'s last `p` observations. `close_adjust` is the
    price level the return is computed from, which makes it the honest stand-in. If it
    matches the feature models, the features are decoration.

    ⚠️ **`target_channel` should be a NAME, not an index.** An index is positional in
    the dataset's `feature_columns`, and those differ per table — `close_adjust` is
    index 0 on the VCB `basic` dataset and need not be on any other. A config that
    pinned `0` would keep running against a different table and silently
    autoregress on whatever column happened to land first. A name is resolved against
    `metadata.json`'s `feature_columns` and **raises** when absent.
    """

    needs_dataset = True

    def __init__(self, n_features: int, lookback: int, order: int = 5,
                 target_channel="close_adjust", alpha: float = 1.0):
        if not 1 <= order <= lookback:
            raise ValueError(f"order must be in 1..{lookback}, got {order}")
        self.n_features = int(n_features)
        self.order = int(order)
        self.target_channel = target_channel
        self.alpha = float(alpha)
        self.n_params = self.order + 1
        self.channel_index_ = None
        if isinstance(target_channel, int):
            if not 0 <= target_channel < n_features:
                raise ValueError(
                    f"target_channel {target_channel} outside 0..{n_features - 1}"
                )
            self.channel_index_ = target_channel

    def set_dataset(self, dataset) -> None:
        if self.channel_index_ is not None:
            return
        columns = list(((dataset.meta or {}).get("features") or {}).get(
            "feature_columns", []
        ))
        if self.target_channel not in columns:
            raise ValueError(
                f"target_channel {self.target_channel!r} is not a feature of "
                f"{dataset.name}; it has {columns}. An AR baseline on the wrong "
                f"column is not a baseline."
            )
        self.channel_index_ = columns.index(self.target_channel)

    def _design(self, X: np.ndarray) -> np.ndarray:
        if self.channel_index_ is None:
            raise RuntimeError(
                "target_channel was never resolved — set_dataset must run before fit."
            )
        return X[:, -self.order:, self.channel_index_]

    def fit(self, X, y):
        from sklearn.linear_model import Ridge

        self.model_ = Ridge(alpha=self.alpha).fit(self._design(X), y)
        return self

    def predict(self, X):
        return self.model_.predict(self._design(X))


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=float), 1e-7, 1 - 1e-7)
    return np.log(p / (1.0 - p))


class PriorClassifier(_Base):
    """The train base rate for every sample. The log-loss/Brier reference."""

    n_params = 1

    def fit(self, X, y):
        self.rate_ = float(np.mean(np.asarray(y, dtype=float) >= 0.5))
        return self

    def predict_logit(self, X):
        return np.full(len(X), float(_logit(np.array([self.rate_]))[0]), dtype=float)

    predict = predict_logit


class LogisticWindow(_Base):
    """L2 logistic regression on the 6 window statistics per channel.

    ⚠️ `C` is FIXED, not cross-validated — `RidgeWindow`'s argument (NUL-1). The design is
    already standardised on the train slice, so one `C` means the same shrinkage on every
    column.
    """

    def __init__(self, n_features: int, lookback: int, C: float = 0.1,
                 class_weight=None, max_iter: int = 5000):
        self.C = float(C)
        self.class_weight = class_weight
        self.max_iter = int(max_iter)
        self.n_params = n_features * len(WINDOW_STATS) + 1

    def fit(self, X, y):
        from sklearn.linear_model import LogisticRegression

        self.model_ = LogisticRegression(
            C=self.C, class_weight=self.class_weight, max_iter=self.max_iter
        ).fit(window_statistics(X), (np.asarray(y) >= 0.5).astype(int))
        return self

    def predict_logit(self, X):
        return self.model_.decision_function(window_statistics(X))

    predict = predict_logit


class LogisticChannel(ARPredictor):
    """Logistic regression on ONE named channel's LAST value — two parameters."""

    def __init__(self, n_features: int, lookback: int, target_channel="drv_realized_vol_10",
                 C: float = 1.0):
        super().__init__(n_features, lookback, order=1, target_channel=target_channel)
        self.C = float(C)
        self.n_params = 2

    def fit(self, X, y):
        from sklearn.linear_model import LogisticRegression

        self.model_ = LogisticRegression(C=self.C, max_iter=5000).fit(
            self._design(X), (np.asarray(y) >= 0.5).astype(int)
        )
        return self

    def predict_logit(self, X):
        return self.model_.decision_function(self._design(X))

    predict = predict_logit


def build_model(n_features: int, lookback: int, kind: str = "zero", **kwargs):
    """One estimator, selected by `kind`. See `KINDS`."""
    if kind == "zero":
        return ZeroPredictor()
    if kind == "mean":
        return MeanPredictor()
    if kind == "ridge_stats":
        return RidgeWindow(n_features, lookback, design="stats",
                           alpha=kwargs.get("alpha", 1.0))
    if kind == "ridge_flat":
        return RidgeWindow(n_features, lookback, design="flat",
                           alpha=kwargs.get("alpha", 1.0))
    if kind == "ar":
        return ARPredictor(n_features, lookback, order=kwargs.get("order", 5),
                           target_channel=kwargs.get("target_channel", 0),
                           alpha=kwargs.get("alpha", 1.0))
    if kind == "prior":
        return PriorClassifier()
    if kind == "logistic_stats":
        return LogisticWindow(n_features, lookback, C=kwargs.get("C", 0.1),
                              class_weight=kwargs.get("class_weight"))
    if kind == "logistic_channel":
        return LogisticChannel(n_features, lookback,
                               target_channel=kwargs.get("target_channel", "drv_realized_vol_10"),
                               C=kwargs.get("C", 1.0))
    raise ValueError(f"unknown baseline kind {kind!r}; have {KINDS}")


def arch_dict(n_features: int, lookback: int, kind: str = "zero", **kwargs) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "baseline",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "lookback": int(lookback),
            "kind": str(kind),
            **{k: v for k, v in kwargs.items()},
        },
    }
