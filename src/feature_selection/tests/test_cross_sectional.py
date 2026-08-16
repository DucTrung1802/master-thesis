# src\feature_selection\test_cross_sectional.py
"""The cross-sectional path, checked on synthetic data — no database, ~40 s.

Run with `pytest src/feature_selection/test_cross_sectional.py` from the repo root
(`pytest.ini` puts `src` on the path).

**What these tests are for.** `cross_sectional.py` §1 names three mistakes that
manufacture a cross-sectional result — splitting the CV by row, windowing down the
stacked frame, and reporting a pooled IC. Every one of them produces numbers that
look *better* than the honest ones, and none of them raises. So each has a test
here that fails if the mistake is reintroduced, and the last two tests close the
loop from the other end: a planted signal must be found, and a panel with no signal
must fail to clear its own null.
"""

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from feature_selection import cross_sectional as cs
from feature_selection import windows

TICKERS = [f"T{i:02d}" for i in range(8)]
LOOKBACK, HORIZON = 20, 5


@pytest.fixture(scope="module")
def panel() -> pd.DataFrame:
    """8 tickers x 200 sessions, with one channel that genuinely predicts."""
    rng = np.random.default_rng(0)
    dates = pd.bdate_range("2020-01-01", periods=200)
    frame = pd.DataFrame(
        [(d, t) for d in dates for t in TICKERS], columns=["date", "ticker"]
    )
    n = len(frame)
    frame["exchange"] = "HOSE"
    frame["signal"] = rng.normal(size=n)
    frame["noise"] = rng.normal(size=n)
    frame["return_5day"] = 0.4 * frame["signal"] + rng.normal(scale=0.6, size=n)
    frame["cs_rank_5day"] = cs.cross_sectional_rank(
        frame["return_5day"], frame["date"]
    )
    return frame


def build(frame, **kwargs):
    defaults = dict(
        target="cs_rank_5day",
        feature_normalize="cs_rank",
        max_features=2,
        horizon=HORIZON,
        lookback=5,
        n_splits=3,
        min_train=60,
        device="cpu",
        min_ic_width=3,
    )
    return cs.CrossSectionalSelector(panel=frame, **{**defaults, **kwargs})


# ---------------------------------------------------------------- the metric


def test_daily_ic_matches_scipy(panel):
    """The vectorised rank correlation IS Spearman, ties and all."""
    rng = np.random.default_rng(1)
    prediction = panel["signal"].to_numpy() + rng.normal(scale=0.5, size=len(panel))
    fast = cs.daily_ic(prediction, panel["cs_rank_5day"], panel["date"], min_width=2)
    slow = (
        pd.DataFrame(
            {"p": prediction, "y": panel["cs_rank_5day"], "d": panel["date"]}
        )
        .groupby("d")[["p", "y"]]
        .apply(lambda g: stats.spearmanr(g["p"], g["y"]).statistic)
    )
    assert np.nanmax(np.abs(fast.to_numpy() - slow.to_numpy())) < 1e-10


def test_daily_ic_matches_scipy_with_heavy_ties(panel):
    """⚠️ VN stocks hit limit-up together, so ties are the common case."""
    rng = np.random.default_rng(1)
    prediction = panel["signal"].to_numpy() + rng.normal(scale=0.5, size=len(panel))
    tied = np.round(panel["return_5day"], 1)
    fast = cs.daily_ic(prediction, tied, panel["date"], min_width=2)
    slow = (
        pd.DataFrame({"p": prediction, "y": tied, "d": panel["date"]})
        .groupby("d")[["p", "y"]]
        .apply(lambda g: stats.spearmanr(g["p"], g["y"]).statistic)
    )
    assert np.nanmax(np.abs(fast.to_numpy() - slow.to_numpy())) < 1e-10


def test_cross_sectional_rank_is_uniform_per_date(panel):
    by_date = panel.groupby("date")["cs_rank_5day"]
    assert np.allclose(by_date.min(), -0.5)
    assert np.allclose(by_date.max(), +0.5)
    assert np.allclose(by_date.mean(), 0.0, atol=1e-12)


def test_cross_sectional_rank_drops_narrow_dates(panel):
    """A rank among two names is a coin flip, and must come back NaN."""
    narrow = panel[panel["ticker"].isin(TICKERS[:3])]
    ranked = cs.cross_sectional_rank(
        narrow["return_5day"], narrow["date"], min_width=5
    )
    assert ranked.isna().all()


# ------------------------------------------------------------------- the CV


def test_folds_never_split_a_session(panel):
    """⚠️ Mistake 1. A row split puts some of a day's stocks in train and the rest
    in test; they share the day's market move, so the model reads its answer."""
    cv = cs.PurgedWalkForwardByDate(
        n_splits=4, horizon=HORIZON, min_train=60, lookback=LOOKBACK
    )
    for train, test in cv.split_dates(panel["date"]):
        train_dates = set(panel["date"].iloc[train])
        test_dates = set(panel["date"].iloc[test])
        assert not (train_dates & test_dates)
        # and every row of a test date is in the test fold, none left behind
        for date in test_dates:
            assert (panel["date"].iloc[test] == date).sum() == (
                panel["date"] == date
            ).sum()


def test_purge_gap_is_d_plus_h_minus_1_sessions(panel):
    """⚠️ The gap is `d + h - 1`, spent in SESSIONS, so that train sample `M` and
    test sample `N` satisfy `M + h < N - d + 1`."""
    cv = cs.PurgedWalkForwardByDate(
        n_splits=4, horizon=HORIZON, min_train=60, lookback=LOOKBACK
    )
    assert cv.gap == LOOKBACK + HORIZON - 1 == 24
    sessions = sorted(set(panel["date"]))
    for train, test in cv.split_dates(panel["date"]):
        last_train = panel["date"].iloc[train].max()
        first_test = panel["date"].iloc[test].min()
        between = sum(1 for d in sessions if last_train < d < first_test)
        assert between == cv.gap  # so first_test is gap+1 = d+h sessions after


# -------------------------------------------------------------- the windows


def test_window_never_crosses_a_ticker(panel):
    """⚠️ Mistake 2. `window_design` run down a `(date, ticker)`-sorted panel would
    build every window out of ~20 different companies."""
    X = panel[["signal", "noise"]].astype(float)
    design = cs.panel_window_design(
        X, panel["ticker"], LOOKBACK, windows.WINDOW_STATS, "none"
    )
    assert len(design) == len(panel) - (LOOKBACK - 1) * len(TICKERS)
    assert design.index.is_monotonic_increasing  # back in (date, ticker) order

    rows = panel.index[panel["ticker"] == "T03"]
    first_kept = rows[LOOKBACK - 1]
    assert design.loc[first_kept, "signal__mean"] == pytest.approx(
        X["signal"].loc[rows[:LOOKBACK]].mean()
    )
    # `last` is the identity — the un-windowed feature
    assert np.allclose(design["signal__last"], X["signal"].loc[design.index])


def test_cs_normalisation_keeps_missing_values_missing(panel):
    """A NaN is not "the lowest rank" — `prop_buy_vol` is absent for years."""
    X = panel[["signal", "noise"]].astype(float).copy()
    X.iloc[0, 0] = np.nan
    normed = cs.cross_sectional_normalize(X, panel["date"])
    assert np.isnan(normed.iloc[0, 0])
    assert np.allclose(normed.groupby(panel["date"]).mean(), 0.0, atol=1e-12)


# ---------------------------------------------------------------- the null


def test_date_block_null_gives_each_stock_its_own_labels(panel):
    """The point of pivoting: AAA gets AAA's returns from another fortnight, never
    BBB's, so cross-sectional dispersion survives the shuffle."""
    shuffled = cs.shuffle_dates(
        panel, "cs_rank_5day", block=25, rng=np.random.default_rng(1)
    )
    for ticker in TICKERS:
        real = set(np.round(panel.loc[panel["ticker"] == ticker, "cs_rank_5day"].dropna(), 10))
        null = set(np.round(shuffled.loc[shuffled["ticker"] == ticker, "cs_rank_5day"].dropna(), 10))
        assert null <= real
    assert not np.allclose(
        shuffled["cs_rank_5day"].fillna(-9), panel["cs_rank_5day"].fillna(-9)
    )
    assert panel.groupby("date")["cs_rank_5day"].std().mean() == pytest.approx(
        shuffled.groupby("date")["cs_rank_5day"].std().mean()
    )


def test_within_date_null_is_lossless(panel):
    """⚠️ Unlike `date_block`, it keeps every labelled cell — see the module §4."""
    shuffled = cs.shuffle_dates(
        panel, "cs_rank_5day", block=25, rng=np.random.default_rng(1),
        mode="within_date",
    )
    assert shuffled["cs_rank_5day"].notna().sum() == panel["cs_rank_5day"].notna().sum()
    assert np.allclose(
        sorted(shuffled["cs_rank_5day"].dropna()),
        sorted(panel["cs_rank_5day"].dropna()),
    )


# ------------------------------------------------------------- end to end


def test_a_planted_signal_is_found(panel):
    selector = build(panel)
    result = selector.run(stability=False)
    assert result.features[0] == "signal"
    ic = result.validation.query("feature_set == 'selected'")["ic"].mean()
    assert ic > 0.2


def test_n_eff_counts_dates_not_rows(panel):
    """⚠️ Mistake 3's arithmetic twin. `n_rows / h` would be 8x too generous here
    and 100x on VN100, shrinking every error bar in the study."""
    result = build(panel).run(stability=False)
    row = result.validation.iloc[0]
    assert row["n_eff_test"] == pytest.approx(
        row["n_test"] / len(TICKERS) / HORIZON, abs=0.5
    )


def test_a_panel_with_no_signal_is_indistinguishable_from_its_null(panel):
    """The leak detector: a label unrelated to every feature must score inside what
    the same pipeline scores on shuffled labels.

    ⚠️ **Asserted on `z`, not on `clears`.** `NullResult.bar` is the null's 95th
    percentile, and a p95 estimated from a handful of draws over an 8-name panel is
    essentially the maximum of a very noisy sample — `clears` flips on coin
    tosses here and would make this test flaky rather than informative. A real leak
    does not produce `z` of 1.8; it produces 5 to 20. That is what this checks, and
    it is checked in BOTH directions because a systematically NEGATIVE observed
    result is as much a sign of a broken fold as a positive one.
    """
    rng = np.random.default_rng(11)
    flat = panel.copy()
    flat["return_5day"] = rng.normal(size=len(flat))
    flat["cs_rank_5day"] = cs.cross_sectional_rank(flat["return_5day"], flat["date"])

    def factory(frame):
        return build(frame).run(stability=False)

    observed = factory(flat).validation.query("feature_set == 'selected'")["ic"].mean()
    null = cs.cross_sectional_null(
        flat, "cs_rank_5day", factory, observed=observed,
        lookback=5, horizon=HORIZON, n_draws=10, seed=3, progress=False,
    )
    assert abs(observed) < 0.15, f"no-signal panel scored {observed:+.4f}"
    assert abs(null.z) < 3.0, f"z = {null.z:+.2f} against its own null"
