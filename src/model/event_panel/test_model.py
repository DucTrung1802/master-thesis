"""`model.event_panel` — the peer panel on synthetic data. No database: the reader is replaced."""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

from model.event_panel import model as M


def _frame(n, seed, shift=0.0):
    rng = np.random.default_rng(seed)
    vol = rng.normal(size=n) + shift
    noise = rng.normal(size=n)
    y = (rng.random(n) < 1 / (1 + np.exp(-(2.0 * (vol - shift) - 2.0)))).astype(float)
    return vol, noise, y


@pytest.fixture
def setup(monkeypatch):
    n = 800
    dates = pd.bdate_range("2012-01-02", periods=n)
    vol, noise, y = _frame(n, 0)
    raw = np.stack([vol * 3 + 10, noise], axis=1)       # the own ticker, in RAW units
    scaler = StandardScaler().fit(raw[:600, :1])
    X = raw.copy()
    X[:, :1] = scaler.transform(raw[:, :1])
    calls = []

    def reader(universe, own, columns, label, pools):
        calls.append((universe, own, tuple(columns), label))
        parts = []
        for k, t in enumerate(("AAA", "BBB", "VCB")):
            v, z, yy = _frame(n, k + 1)
            parts.append(pd.DataFrame({"date": dates, "ticker": t, label: yy,
                                       "har_x": v * 3 + 10, "drv_noise": z}))
        frame = pd.concat(parts, ignore_index=True)
        return frame[frame["ticker"] != own][["date", "ticker", label] + list(columns)]

    monkeypatch.setattr(M, "PEER_READER", reader)
    M._PEERS.clear()
    ds = SimpleNamespace(
        meta={"features": {"feature_columns": ["har_x", "drv_noise"], "scaled_columns": ["har_x"]},
              "source": {"schema": "unified_schema_vcb"},
              "target": {"column": "up_5pct_5day", "stored_target": "up_5pct_5day"}},
        feature_scaler=scaler,
        dates_train=np.array(dates[:600].strftime("%Y-%m-%d")),
    )
    return ds, X[:, None, :], y, dates, calls


def test_peers_exclude_the_own_ticker_and_share_the_datasets_scale(setup):
    ds, X, y, dates, calls = setup
    est = M.EventPanel(2, 1, columns=["har_", "drv_"])
    est.set_dataset(ds)
    assert calls == [("BANK", "VCB", ("har_x", "drv_noise"), "up_5pct_5day")]
    assert set(est.peers_["ticker"]) == {"AAA", "BBB"}
    # the raw peer level 3*v+10 is mapped by the OWN scaler, so its spread matches the own rows'
    assert abs(est.peers_["har_x"].std() - np.std(X[:600, 0, 0])) < 0.15


def test_the_peer_cut_follows_the_fit_rows_dates(setup):
    ds, X, y, dates, _ = setup
    est = M.EventPanel(2, 1, n_estimators=50)
    est.set_dataset(ds)
    est.fit(X[:600], y[:600])
    assert est.provenance()["peer_rows"] == 2 * 600
    assert est.provenance()["peer_dates"][1] == str(dates[599].date())
    est.set_fit_context(dates=np.array(dates[:700].strftime("%Y-%m-%d")))
    est.fit(X[:700], y[:700])
    assert est.provenance()["peer_rows"] == 2 * 700
    with pytest.raises(ValueError, match="set_fit_context"):
        est.fit(X[:650], y[:650])


def test_it_ranks_the_own_rows_and_selects_by_prefix(setup):
    ds, X, y, _, _ = setup
    est = M.EventPanel(2, 1, columns=["har_"], n_estimators=100)
    est.set_dataset(ds)
    assert est.index_ == [0]
    est.fit(X[:600], y[:600])
    assert roc_auc_score(y[600:], est.predict_logit(X[600:])) > 0.7
    with pytest.raises(ValueError, match="no feature column"):
        M.EventPanel(2, 1, columns=["zzz_"]).set_dataset(ds)
