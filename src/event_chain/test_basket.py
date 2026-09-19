"""`event_chain.basket` on synthetic panels — no database.

    python -m pytest event_chain/test_basket.py -q
"""

import numpy as np
import pandas as pd
import pytest

from event_chain import basket as B
from event_chain import config as C
from event_chain.chain import EventChain


def _panel(n_dates=40, n_names=12, seed=0, informative=True):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-01", periods=n_dates)
    rows = []
    for d in dates:
        for j in range(n_names):
            ret = rng.normal(0.0, 0.05)
            y = float(ret >= 0.05)
            score = (ret + rng.normal(0, 0.01)) if informative else rng.random()
            rows.append({"date": d, "ticker": f"T{j:02d}", "y_true": y, "y_prob": score,
                         "ret": ret, "at_ceiling": False, "exchange": "HOSE"})
    return pd.DataFrame(rows)


def test_basket_is_the_top_k_of_each_session():
    frame = _panel()
    picks = B.baskets(frame, 3)
    assert picks.groupby("date").size().eq(3).all()
    for date, group in frame.groupby("date"):
        top = set(group.nlargest(3, "y_prob")["ticker"])
        assert set(picks.loc[picks["date"] == date, "ticker"]) == top


def test_hit_rate_matches_a_direct_count():
    frame = _panel(seed=3)
    per = B.sessions(frame, 4)
    direct = frame.sort_values("y_prob", ascending=False).groupby("date").head(4) \
        .groupby("date")["y_true"].mean()
    assert np.allclose(per.set_index("date")["hit"].to_numpy(), direct.sort_index().to_numpy())
    assert np.allclose(per["base"].to_numpy(), frame.groupby("date")["y_true"].mean().to_numpy())


def test_the_ceiling_screen_is_a_switch():
    frame = _panel(seed=1)
    market = frame[["date", "ticker", "exchange", "at_ceiling", "ret"]].copy()
    market.loc[market.index[:3], "at_ceiling"] = True
    pred = frame[["date", "ticker", "y_true", "y_prob"]]
    on, off = B.attach(pred, market, exclude_ceiling=True), B.attach(pred, market, exclude_ceiling=False)
    assert on["at_ceiling"].sum() == 3 and not off["at_ceiling"].any()
    assert off["at_ceiling_n"].sum() == 3


def test_a_ceiling_name_is_never_bought_when_screened():
    frame = _panel(seed=1)
    best = frame.loc[frame.groupby("date")["y_prob"].idxmax()]
    frame.loc[best.index, "at_ceiling"] = True
    picks = B.baskets(frame, 1)
    merged = picks.merge(best[["date", "ticker"]], on=["date", "ticker"])
    assert merged.empty
    assert B.sessions(frame, 1)["width"].eq(11).all()


def test_short_session_takes_what_it_has():
    frame = _panel(n_names=3)
    per = B.sessions(frame, 5)
    assert per["picked"].eq(3).all()
    assert np.allclose(per["hit"], per["base"])


def test_null_keeps_the_base_rate_and_an_informative_score_beats_it():
    good = B.basket_metrics(_panel(n_dates=60, n_names=30, seed=5), 5, 5, 5.0, draws=100)
    noise = B.basket_metrics(_panel(n_dates=60, n_names=30, seed=5, informative=False), 5, 5, 5.0, draws=100)
    assert abs(good["hit_null_mean"] - good["base"]) < 0.02
    assert good["hit"] > good["hit_bar"] and good["hit_z"] > 5
    assert noise["hit"] < noise["hit_null_max"] + 1e-9
    assert good["daily_auc"] > 0.9 and abs(noise["daily_auc"] - 0.5) < 0.05


def test_daily_auc_matches_sklearn_per_session():
    from sklearn.metrics import roc_auc_score

    frame = _panel(seed=7)
    per = B.sessions(frame, 3).set_index("date")
    for date, group in frame.groupby("date"):
        if 0 < group["y_true"].sum() < len(group):
            assert per.loc[date, "daily_auc"] == pytest.approx(roc_auc_score(group["y_true"], group["y_prob"]))


def test_trading_track_is_non_overlapping_and_charges_the_round_trip():
    per = pd.DataFrame({"date": pd.bdate_range("2024-01-01", periods=20),
                        "bret": [0.01] * 20, "uret": [0.0, 0.001] * 10})
    out = B.trading(per, 5, costs=(0.0, 0.005))
    assert out["periods"] == 4
    assert out["cagr_0"] == pytest.approx(1.01 ** (252 / 5) - 1)
    assert out["cagr_50"] == pytest.approx(1.005 ** (252 / 5) - 1)
    assert out["maxdd_50"] == 0.0


def test_the_basket_setup_is_a_panel_with_a_parameter_x():
    chain = EventChain.from_setup("basket")
    assert chain.is_basket and chain.top_k == C.BASKET_SIZE == 5
    assert chain.ticker == "LIQUID" and chain.lookback == 1
    assert C.BASKET_EXCLUDE_CEILING is False   # the user's decision, 2026-09-17
    assert EventChain.from_setup("basket", top_k=3).top_k == 3
    assert set(C.SETUPS) == {"basket"}   # the close-to-close setups were deleted 2026-09-18
    members = {m for ms in chain.ensembles.values() for m in ms}
    names = {f"{p}{v}" for p, v, _, _ in chain.models}
    assert members <= names


def test_the_auc_nulls_keep_timing_and_break_the_ranking():
    from sklearn.metrics import roc_auc_score

    good = B.attach(_panel(n_dates=60, n_names=30, seed=9)[["date", "ticker", "y_true", "y_prob"]],
                    _panel(n_dates=60, n_names=30, seed=9)[["date", "ticker", "exchange", "at_ceiling", "ret"]])
    out = B.auc_nulls(good, draws=50)
    frame = good.sort_values(["date", "ticker"])
    assert out["auc_z"] > 5 and out["daily_auc_z"] > 5
    assert abs(out["auc_null_mean"] - 0.5) < 0.05
    assert out["auc_bar"] < roc_auc_score(frame["y_true"], frame["y_prob"])


def test_a_cut_basket_holds_at_most_k_names_and_cash_below_the_cut():
    frame = _panel(n_dates=30, n_names=10, seed=5)
    per, top = B.sessions(frame, 4), B.baskets(frame, 4)
    tau = float(top["y_prob"].quantile(0.7))
    cut = B.capped(top, per, tau)
    for date, group in top.groupby("date"):
        kept = group[group["y_prob"] >= tau]
        assert cut.loc[date, "n"] == len(kept) <= 4
        if len(kept):
            assert cut.loc[date, "bret"] == pytest.approx(kept["ret"].mean())
            assert cut.loc[date, "hits"] == (kept["y_true"] >= 0.5).sum()
    m = B.capped_metrics(cut, tau, horizon=5, gain_pct=5.0, cost=0.005)
    net = np.where(cut["n"] > 0, cut["bret"].fillna(0) - 0.005, 0.0)
    assert m["ev"] == pytest.approx(net.mean())
    assert m["active"] == int((cut["n"] > 0).sum()) < len(cut)
    assert m["precision"] == pytest.approx(cut["hits"].sum() / cut["n"].sum())
    # no cut is the plain top-k basket (the synthetic scores go below 0; a probability does not)
    plain = B.capped_metrics(B.capped(top, per, -np.inf), 0.0, 5, 5.0)
    assert plain["active_share"] == 1.0 and plain["precision"] == pytest.approx(per["hit"].mean())


def test_the_cut_null_keeps_the_counts_and_an_informative_score_beats_it():
    frame = _panel(n_dates=60, n_names=20, seed=7)
    per, top = B.sessions(frame, 5), B.baskets(frame, 5)
    cut = B.capped(top, per, float(top["y_prob"].median()))
    null = B.capped_null(frame, cut, 5.0, draws=40)
    m = B.capped_metrics(cut, 0.0, 5, 5.0)
    assert m["precision"] > np.quantile(null["precision"], 0.95)
    noise = _panel(n_dates=60, n_names=20, seed=7, informative=False)
    ncut = B.capped(B.baskets(noise, 5), B.sessions(noise, 5), 0.5)
    nnull = B.capped_null(noise, ncut, 5.0, draws=40)
    assert abs(B.capped_metrics(ncut, 0.5, 5, 5.0)["precision"] - nnull["precision"].mean()) < 0.1


def test_the_cut_is_chosen_on_ev_among_cuts_that_trade(monkeypatch):
    table = pd.DataFrame({"tau": [0.0, 0.2, 0.3, 0.6], "active": [100, 90, 60, 3],
                          "ev": [0.01, 0.02, 0.02, 0.09]})
    monkeypatch.setattr(C, "BASKET_MIN_PROB_ON", "ev")
    monkeypatch.setattr(C, "BASKET_MIN_ACTIVE", 25)
    assert B.choose_min_prob(table) == 0.2   # 0.6 trades 3 sessions; the tie goes to the fuller basket
    assert B.choose_min_prob(table.assign(active=1)) == 0.0


def test_the_board_is_chosen_on_the_within_session_auc(monkeypatch):
    """⚠️ `BASKET_CHOOSE_ON` decides, and the default is the WITHIN-SESSION AUC.

    A pooled AUC also ranks name-sessions ACROSS sessions, which no basket trades, so a row
    that wins on `val_auc` and loses on `val_daily_auc` must NOT be chosen by default.
    """
    board = pd.DataFrame({
        "run_name": ["pooled__a", "daily__b"], "model_type": ["GBT", "GBT"],
        "val_auc": [0.70, 0.60], "val_daily_auc": [0.55, 0.65], "val_hit": [0.30, 0.30]})
    assert C.BASKET_CHOOSE_ON == "val_daily_auc"
    assert B._selection_key(board).iloc[0]["run_name"] == "daily__b"
    assert B.best_on_val(B._selection_key(board))["run_name"] == "daily__b"
    monkeypatch.setattr(C, "BASKET_CHOOSE_ON", "val_auc")
    assert B._selection_key(board).iloc[0]["run_name"] == "pooled__a"


def test_a_pooled_auc_can_be_paid_for_timing_the_within_session_one_cannot():
    """The gap between the two AUCs is the part of the ranking a basket cannot trade."""
    rows = []
    for d in range(40):                       # half the sessions are eventful, half are not
        hot = d % 2 == 0
        for name in range(10):
            y = float(hot and name < 5)
            rows.append({"date": pd.Timestamp("2024-01-01") + pd.Timedelta(days=d),
                         "ticker": f"T{name}", "y_true": y,
                         # the score knows WHICH SESSION is eventful and nothing else
                         "y_prob": 0.9 if hot else 0.1, "ret": 0.0,
                         "at_ceiling": False, "at_ceiling_n": False})
    frame = pd.DataFrame(rows)
    out = B.auc_nulls(frame, draws=20)
    per = B.sessions(frame, 5)
    assert out["auc_null_mean"] > 0.6          # the pooled null itself is paid for timing
    assert float(np.nanmean(per["daily_auc"])) == pytest.approx(0.5, abs=1e-9)
