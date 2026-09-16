"""`train_test_creator` auxiliary targets — windowed beside `y`, never touching `X`. No database."""

import numpy as np
import pandas as pd

from train_test_creator.dataset import SplitBounds, TrainTestCreator


def test_aux_targets_follow_y_and_leave_every_split_of_x_intact():
    n = 60
    dates = pd.date_range("2020-01-01", periods=n, freq="D")
    labelled = pd.DataFrame({"date": dates, "exchange": "HOSE", "ticker": "VCB"})
    features = pd.DataFrame({"a": np.arange(n, dtype=float), "b": -np.arange(n, dtype=float)})
    creator = TrainTestCreator.__new__(TrainTestCreator)
    creator.lookback, creator.horizon, creator.purge = 1, 5, True
    bounds = SplitBounds(train_end_date=dates[30], val_end_date=dates[45], n_dates=n,
                         train_dates=30, val_dates=15, test_dates=15)
    aux_full = {"return_5day": np.arange(n, dtype=float) / 100}
    X, y, _, _, aux = creator._window(features, np.arange(n, dtype=float), labelled, bounds, aux_full)
    for split in ("train", "val", "test"):
        assert X[split].shape[1:] == (1, 2)
        np.testing.assert_allclose(aux["return_5day"][split] * 100, y[split])
        np.testing.assert_allclose(X[split][:, -1, 0], y[split])
