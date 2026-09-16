"""`model.event_linear` — the three linear kinds on synthetic data. No database."""

from types import SimpleNamespace

import numpy as np
import pytest
from sklearn.metrics import roc_auc_score

from model.event_linear.model import EventLinear


def _dataset(n=1500, seed=0):
    rng = np.random.default_rng(seed)
    vol = rng.normal(size=n)                      # "har_" channel: drives the SIZE of the move
    trend = rng.normal(size=n)                    # "drv_" channel: drives its SIGN
    noise = rng.normal(size=n)
    sigma = 0.02 * np.exp(0.5 * vol)
    r = np.expm1(0.004 * trend + sigma * rng.standard_t(5, size=n) + 0.01 * np.tanh(trend))
    X = np.stack([vol, trend, noise], axis=1)[:, None, :]
    y = (r >= 0.05).astype(float)
    dates = np.array([str(d) for d in np.arange("2010-01-01", n, dtype="datetime64[D]")])
    ds = SimpleNamespace(
        meta={"features": {"feature_columns": ["har_lpk_22", "drv_trend", "drv_vwap_raw"]}},
        dates_train=dates, aux={"return_5day": {"train": r}},
    )
    return ds, X, y, r


def _fit(kind, ds, X, y, **kw):
    est = EventLinear(n_features=3, lookback=1, kind=kind, **kw)
    est.set_dataset(ds)
    est.set_task("classification")
    return est.fit(X, y)


def test_columns_select_by_prefix_and_exclude_removes():
    ds, X, y, _ = _dataset()
    est = _fit("event_logit", ds, X, y, columns=["drv_"], exclude=["drv_vwap_raw"])
    assert est.index_ == [1]
    with pytest.raises(ValueError, match="no feature column"):
        _fit("event_logit", ds, X, y, columns=["nothing_"])


def test_magnitude_ridge_ranks_the_event_by_the_size_of_the_move():
    ds, X, y, _ = _dataset()
    est = _fit("magnitude_ridge", ds, X, y, columns=["har_"], alpha=1.0)
    p = est.predict_logit(X)
    assert np.isfinite(p).all() and roc_auc_score(y, p) > 0.65


def test_direction_logit_reads_the_sign_and_needs_the_aux_target():
    ds, X, y, r = _dataset()
    est = _fit("direction_logit", ds, X, y, columns=["drv_trend"], C=1.0, min_move=0.02)
    up = np.log1p(r) > 0
    big = np.abs(np.log1p(r)) >= 0.02
    assert roc_auc_score(up[big], est.predict_logit(X[big])) > 0.6
    ds.aux = {}
    with pytest.raises(ValueError, match="auxiliary target"):
        _fit("direction_logit", ds, X, y)


def test_a_refit_on_other_rows_must_be_given_their_context():
    ds, X, y, r = _dataset()
    est = _fit("magnitude_ridge", ds, X, y, half_life_years=4.0)
    with pytest.raises(ValueError, match="set_fit_context"):
        est.fit(X[:100], y[:100])                 # the TRAIN context is 1,500 rows long
    est.set_fit_context(dates=ds.dates_train[:100], aux=r[:100])
    assert np.isfinite(est.fit(X[:100], y[:100]).predict_logit(X[:5])).all()
