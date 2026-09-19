"""The three per-run artefacts every chart is drawn from, and the contract they share.

⚠️ **A MISSING `loss_history.csv` MUST ALWAYS MEAN A BUG.** Four of the basket grid's
families fit in one shot — a forest is one boosting round, a logistic is convex, the prior
does not fit at all — so "this model has no epochs" and "the writer failed" would otherwise
look identical on disk. The contract is that the file is ALWAYS there and that a family
with no curve writes ONE ROW; the reader tells them apart by counting rows.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

from model.common import engine as E


class _Run:
    def __init__(self, tmp):
        self.results_dir = str(tmp)


def test_loss_history_is_written_even_for_a_model_with_no_curve(tmp_path):
    E._write_loss_history(_Run(tmp_path), [{"step": 0, "train_loss": 0.4, "val_loss": 0.5}])
    frame = pd.read_csv(os.path.join(tmp_path, "loss_history.csv"))
    assert list(frame.columns) == ["step", "train_loss", "val_loss"]
    assert len(frame) == 1


def test_the_curve_columns_are_the_same_for_epochs_and_boosting_rounds(tmp_path):
    """The torch path counts EPOCHS and XGBoost counts ROUNDS into the same three columns."""
    E._write_loss_history(_Run(tmp_path), [{"step": i, "train_loss": 1.0 / (i + 1),
                                            "val_loss": 1.0 / (i + 1) + 0.01}
                                           for i in range(7)])
    frame = pd.read_csv(os.path.join(tmp_path, "loss_history.csv"))
    assert list(frame.columns) == ["step", "train_loss", "val_loss"] and len(frame) == 7
    assert frame["step"].tolist() == list(range(7))


def test_calibration_bins_hold_equal_counts_and_read_back_the_predictions(tmp_path):
    """⚠️ EQUAL COUNT, NOT EQUAL WIDTH — at a 0.16 base rate the top width-bins are noise."""
    rng = np.random.default_rng(0)
    prob = rng.random(1000)
    pd.DataFrame({"y_true": (rng.random(1000) < prob).astype(int), "y_prob": prob}).to_csv(
        os.path.join(tmp_path, "predictions_test.csv"), index=False)
    E._write_calibration(_Run(tmp_path), bins=10)
    frame = pd.read_csv(os.path.join(tmp_path, "calibration.csv"))
    assert set(frame["split"]) == {"test"} and len(frame) == 10
    assert frame["n"].min() >= 99 and frame["n"].sum() == 1000
    # a well-calibrated generator: observed tracks mean_pred, monotonically enough to rank
    assert frame["observed"].corr(frame["mean_pred"]) > 0.95


def test_importances_are_skipped_when_the_model_has_none(tmp_path):
    class NoImportances:
        pass

    class Dataset:
        meta = {"features": {"feature_columns": ["a", "b"]}}

    E._write_importances(_Run(tmp_path), NoImportances(), Dataset())
    assert not os.path.exists(os.path.join(tmp_path, "feature_importance.csv"))


def test_importances_are_written_sorted_by_magnitude(tmp_path):
    class Linear:
        def importances(self, columns):
            return {"a": -0.9, "b": 0.2, "c": 0.5}

    class Dataset:
        meta = {"features": {"feature_columns": ["a", "b", "c"]}}

    E._write_importances(_Run(tmp_path), Linear(), Dataset())
    frame = pd.read_csv(os.path.join(tmp_path, "feature_importance.csv"))
    # ⚠️ the SIGN is kept (a linear coefficient means something signed) and the ORDER is
    # by magnitude, so the negative channel leads.
    assert frame["feature"].tolist() == ["a", "c", "b"]
    assert frame["importance"].tolist() == [-0.9, 0.5, 0.2]


def test_gbt_early_stops_on_val_and_reports_the_round_it_took():
    """⚠️ Val now chooses the model, the cut AND the round — see `config.EARLY_STOPPING_ROUNDS`."""
    from model.gbt.model import build_model

    rng = np.random.default_rng(1)
    X = rng.normal(size=(400, 1, 6))
    y = (X[:, 0, 0] + rng.normal(scale=0.5, size=400) > 0).astype(int)

    class Dataset:
        X_val = rng.normal(size=(120, 1, 6))
        y_val = (X_val[:, 0, 0] + rng.normal(scale=0.5, size=120) > 0).astype(float)

    model = build_model(6, 1, max_depth=2, n_estimators=200, early_stopping_rounds=5)
    model.set_task("classification")
    model.set_dataset(Dataset())
    model.fit(X, y)
    curve = model.loss_history()
    assert curve and set(curve[0]) == {"step", "train_loss", "val_loss"}
    # it stopped before the cap, which is the whole point of raising the cap
    assert 0 <= model.best_iteration < 200 and len(curve) < 200
    assert model.objective.startswith("binary:logistic")


def test_a_model_without_a_val_block_does_not_early_stop():
    """⚠️ A REFIT fits on train+val, so stopping on val would watch its own training rows."""
    from model.gbt.model import build_model

    rng = np.random.default_rng(2)
    X = rng.normal(size=(200, 1, 5))
    y = (rng.random(200) > 0.5).astype(int)
    model = build_model(5, 1, max_depth=2, n_estimators=12, early_stopping_rounds=0)
    model.set_task("classification")
    model.fit(X, y)
    assert model.best_iteration is None
    assert model.loss_history() == []   # no eval_set, so the engine writes its one row


@pytest.mark.parametrize("kind,expected", [
    ("event_logit", "binary cross-entropy"),
    ("magnitude_ridge", "mean squared error"),
])
def test_every_family_names_the_loss_it_actually_minimises(kind, expected):
    """⚠️ `training.criterion` said the same sentence for three different objectives."""
    from model.event_linear.model import build_model

    assert build_model(4, 1, kind=kind).objective.startswith(expected)
