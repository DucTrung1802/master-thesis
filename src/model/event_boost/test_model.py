"""`model.event_boost` on a synthetic panel — CPU, no database."""

from types import SimpleNamespace

import numpy as np
import pytest
from sklearn.metrics import roc_auc_score

from model.event_boost.model import EventBoost, arch_dict, build_model


def _panel(n_dates=120, n_names=30, seed=0):
    rng = np.random.default_rng(seed)
    dates = np.repeat(np.array([str(d) for d in np.arange("2015-01-01", n_dates, dtype="datetime64[D]")]), n_names)
    vol = rng.normal(size=len(dates))
    noise = rng.normal(size=len(dates))
    r = np.expm1(0.03 * np.exp(0.6 * vol) * rng.standard_t(5, size=len(dates)))
    y = (r >= 0.05).astype(float)
    X = np.stack([vol, noise, rng.normal(size=len(dates))], axis=1)[:, None, :].astype(np.float32)
    ds = SimpleNamespace(meta={"features": {"feature_columns": ["har_vol", "evt_noise", "drv_vwap_raw"]}},
                         dates_train=dates, aux={"return_5day": {"train": r}})
    return ds, X, y, dates


def _fit(ds, X, y, **kw):
    est = EventBoost(n_features=3, lookback=1, device="cpu", n_estimators=40, max_depth=3,
                     min_child_weight=5, **kw)
    est.set_dataset(ds)
    est.set_task("classification")
    return est.fit(X, y)


@pytest.mark.parametrize("kind", ["xgb", "rank", "magnitude"])
def test_every_kind_ranks_the_informative_channel(kind):
    ds, X, y, _ = _panel()
    est = _fit(ds, X, y, kind=kind)
    assert roc_auc_score(y, est.predict_logit(X)) > 0.7
    assert est.provenance()["kind"] == kind and est.n_params > 0


def test_columns_select_by_prefix_and_exclude_removes():
    ds, X, y, _ = _panel()
    est = _fit(ds, X, y, columns=["har_", "drv_"], exclude=["drv_vwap_raw"])
    assert est.index_ == [0]
    with pytest.raises(ValueError, match="no feature column"):
        _fit(ds, X, y, columns=["mctx_"])


def test_fit_needs_the_rows_dates():
    ds, X, y, dates = _panel()
    est = EventBoost(n_features=3, lookback=1, device="cpu", n_estimators=5)
    est.set_dataset(ds)
    est.set_fit_context(dates=dates[:10])
    with pytest.raises(ValueError, match="set_fit_context"):
        est.fit(X, y)


def test_a_ranker_takes_no_row_weights_and_a_classifier_does():
    ds, X, y, _ = _panel()
    assert _fit(ds, X, y, kind="xgb", half_life_years=2.0).provenance()["rows"] == len(y)
    with pytest.raises(ValueError, match="per query"):
        _fit(ds, X, y, kind="rank", half_life_years=2.0)


def test_magnitude_needs_the_auxiliary_return():
    ds, X, y, _ = _panel()
    ds.aux = {}
    with pytest.raises(ValueError, match="auxiliary target"):
        _fit(ds, X, y, kind="magnitude")


def test_the_arch_round_trips():
    arch = arch_dict(3, 1, kind="xgb", max_depth=4)
    assert build_model(**arch["kwargs"]).params["max_depth"] == 4
    with pytest.raises(ValueError, match="kind"):
        EventBoost(3, 1, kind="lightgbm")
