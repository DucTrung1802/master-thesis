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
sample size supports; a 4,961-parameter LSTM is not, and `feature_selection/CONTEXT.md`
§6d puts the observations needed to separate an IC of 0.05 from zero at ~1,500.

⚠️ **`LassoCV` already zeroed every coefficient on this pool** (§4) — no linear signal
survived cross-validated shrinkage on the 27-channel version. Expect Ridge to land near
zero. **That is the point**: a linear model reaching the same answer as an LSTM is a
far stronger statement about the data than another network reaching it.

## ⚠️ Everything is fitted on the TRAIN SPLIT ONLY

The dataset's features are already standardised with train-slice statistics
(`train_test_creator/CONTEXT.md` §6) and its target is already scaled, so these fit on
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

KINDS = ("zero", "mean", "ridge_stats", "ridge_flat", "ar")


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
    autoregression on `target_channel`'s last `p` observations. `close_adjust` is
    index 0 of this dataset's four channels and is the price level the return is
    computed from, which makes it the honest stand-in. If it matches the feature
    models, the features are decoration.
    """

    def __init__(self, n_features: int, lookback: int, order: int = 5,
                 target_channel: int = 0, alpha: float = 1.0):
        if not 1 <= order <= lookback:
            raise ValueError(f"order must be in 1..{lookback}, got {order}")
        if not 0 <= target_channel < n_features:
            raise ValueError(
                f"target_channel {target_channel} outside 0..{n_features - 1}"
            )
        self.order = int(order)
        self.target_channel = int(target_channel)
        self.alpha = float(alpha)
        self.n_params = self.order + 1

    def _design(self, X: np.ndarray) -> np.ndarray:
        return X[:, -self.order:, self.target_channel]

    def fit(self, X, y):
        from sklearn.linear_model import Ridge

        self.model_ = Ridge(alpha=self.alpha).fit(self._design(X), y)
        return self

    def predict(self, X):
        return self.model_.predict(self._design(X))


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
