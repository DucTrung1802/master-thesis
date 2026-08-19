# src\backtest\test_portfolio.py
"""What a costed backtest must not get wrong, pinned.

Every test here names a way a backtest flatters itself: overlapping windows, a cost
charged once instead of twice, a benchmark of zero, a Sharpe quoted without its `n`.
"""

import numpy as np
import pandas as pd
import pytest

from backtest import portfolio as P


def _panel(n_dates=200, n_tickers=10, horizon=20, seed=0, skill=0.0):
    """A panel with a known amount of skill in `y_pred`, and real forward returns."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2020-01-01", periods=n_dates)
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    rows = []
    for date in dates:
        truth = rng.normal(0.0, 0.08, n_tickers)
        noise = rng.normal(0.0, 0.08, n_tickers)
        score = skill * truth + (1 - skill) * noise
        for t, r, s in zip(tickers, truth, score):
            rows.append({"date": date, "ticker": t, f"return_{horizon}day": r, "y_pred": s})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ the sampling


def test_rebalance_dates_do_not_overlap_the_holding_window():
    """⚠️ The defect this prevents: trading every date at h=20 holds 20 overlapping
    tranches and reports 20x the independent sample. CLAUDE.md §5 rule 7."""
    dates = pd.bdate_range("2020-01-01", periods=100)
    picked = P.rebalance_dates(dates, 20)
    assert len(picked) == 5
    gaps = np.diff([dates.get_loc(d) for d in picked])
    assert (gaps == 20).all()
    # h=1 is every date, which is the overlapping case and must still be expressible
    assert len(P.rebalance_dates(dates, 1)) == 100


def test_rebalance_dates_dedupe_a_panel_where_every_date_repeats():
    """The panel has 10 rows per date; the schedule is over DATES, not rows."""
    panel = _panel(n_dates=60, n_tickers=10, horizon=20)
    assert len(panel) == 600
    assert len(P.rebalance_dates(panel["date"], 20)) == 3


def test_a_zero_horizon_raises_rather_than_returning_everything():
    with pytest.raises(ValueError):
        P.rebalance_dates(pd.bdate_range("2020-01-01", periods=5), 0)


# ------------------------------------------------------------------ the cost


def test_a_full_round_trip_costs_exactly_one_round_trip():
    """⚠️ `cost = rt x 1/2 x sum|dw|`. Enter pays half, exit pays the other half. A
    backtest that charges `rt` on entry alone is 2x too cheap and every Sharpe in it
    is wrong in the direction that sells the result."""
    rt = 0.005
    enter = P.turnover_cost({}, {"VCB": 1.0}, rt)
    exit_ = P.turnover_cost({"VCB": 1.0}, {}, rt)
    assert enter == pytest.approx(rt / 2)
    assert exit_ == pytest.approx(rt / 2)
    assert enter + exit_ == pytest.approx(rt)


def test_replacing_a_fraction_of_the_book_costs_that_fraction_of_a_round_trip():
    """The identity behind the annual-drag table: tau of the book replaced costs
    `tau x round_trip`, so h=5 at tau=0.70 and 50 bps is 50.4 x 0.70 x 0.005 = 17.6%/yr."""
    rt = 0.005
    old = {f"T{i}": 0.1 for i in range(10)}
    new = {**{f"T{i}": 0.1 for i in range(3)}, **{f"N{i}": 0.1 for i in range(7)}}
    assert P.turnover_cost(old, new, rt) == pytest.approx(0.7 * rt)

    per_year = P.SESSIONS_PER_YEAR / 5
    assert per_year * P.turnover_cost(old, new, rt) == pytest.approx(0.1764, abs=1e-4)


def test_holding_the_same_book_costs_nothing():
    book = {"VCB": 0.5, "ACB": 0.5}
    assert P.turnover_cost(book, dict(book), 0.005) == 0.0


# ------------------------------------------------------------------ the strategies


def test_hysteresis_trades_less_than_a_bare_threshold_and_costs_less():
    """⚠️ §11's measured turnover control. A score oscillating around one line pays a
    round trip each crossing; a band holds through it."""
    panel = _panel(n_dates=400, horizon=20, seed=3)
    band = P.long_flat_single(panel, "T00", 20, "return_20day", enter=0.90, exit_=0.75)
    bare = P.long_flat_single(panel, "T00", 20, "return_20day", enter=0.90, exit_=0.90)
    assert sum(band.cost) <= sum(bare.cost)
    # and the band must actually be holding sometimes, or the comparison is vacuous
    assert 0.0 < np.mean(band.exposure) < 1.0


def test_a_skilful_score_beats_a_random_one_gross_and_the_cost_is_what_decides():
    """The mechanism check: with real skill the top-k beats the universe GROSS. Whether
    it still does NET is exactly the question this stage exists to ask."""
    skilled = _panel(n_dates=600, n_tickers=30, horizon=20, seed=5, skill=0.6)
    picked = P.long_only_top_k(skilled, 20, "return_20day", k=5, round_trip=0.0)
    market = P.buy_and_hold(skilled, 20, "return_20day")
    assert np.mean(picked.gross) > np.mean(market.gross)

    dear = P.long_only_top_k(skilled, 20, "return_20day", k=5, round_trip=0.05)
    assert np.mean(dear.frame()["net"]) < np.mean(picked.frame()["net"])


def test_buy_and_hold_pays_no_ongoing_cost_and_is_always_invested():
    """⚠️ The benchmark that competes. experiment_3: VCB timing Sharpe 0.67 vs B&H 0.66."""
    panel = _panel(n_dates=200, horizon=20)
    bench = P.buy_and_hold(panel, 20, "return_20day", ticker="T00")
    assert sum(bench.cost) == 0.0
    assert set(bench.exposure) == {1.0}


def test_a_flat_position_earns_and_costs_nothing_that_period():
    """Cash is cash: no return, and no cost while it stays flat."""
    panel = _panel(n_dates=300, horizon=20, seed=11)
    # ⚠️ `enter=1.0` is unreachable by construction: the percentile is the fraction of
    # peers scored strictly below, so with N names it maxes at (N-1)/N. Never long.
    track = P.long_flat_single(panel, "T00", 20, "return_20day", enter=1.0, exit_=1.0)
    assert track.dates and set(track.exposure) == {0.0}
    assert sum(track.gross) == 0.0 and sum(track.cost) == 0.0


def test_an_inverted_band_raises_rather_than_trading_something_undefined():
    panel = _panel(n_dates=60, horizon=20)
    with pytest.raises(ValueError):
        P.long_flat_single(panel, "T00", 20, "return_20day", enter=0.5, exit_=0.9)


# ------------------------------------------------------------------ the statistics


def test_sharpe_carries_its_own_error_bar_and_it_is_large_at_this_sample_size():
    """⚠️ THE COLUMN TO READ FIRST. A 2.6-year test at h=20 is ~33 periods, and
    `SE(Sharpe) = sqrt((1 + S^2/2)/n)` is then ~0.18 — so a 0.6 and a 0.9 are one
    measurement. experiment_10 found all 23 reviewed papers omitting this."""
    track = P.Track(name="t")
    rng = np.random.default_rng(0)
    for i, r in enumerate(rng.normal(0.02, 0.06, 33)):
        track.dates.append(i)
        track.gross.append(float(r))
        track.cost.append(0.0)
        track.exposure.append(1.0)
        track.n_held.append(1)
    out = P.stats(track, horizon=20)
    assert out["n_periods"] == 33
    expected = np.sqrt((1 + 0.5 * out["sharpe"] ** 2) / 33)
    assert out["se_sharpe"] == pytest.approx(expected)
    assert out["se_sharpe"] > 0.15


def test_cost_drag_is_annualised_and_matches_the_hand_arithmetic():
    """A book fully replaced every rebalance at 50 bps: h=20 -> 12.6 x 0.005 = 6.3%/yr."""
    track = P.Track(name="t")
    for i in range(20):
        track.dates.append(i)
        track.gross.append(0.0)
        track.cost.append(0.005)
        track.exposure.append(1.0)
        track.n_held.append(1)
    out = P.stats(track, horizon=20)
    assert out["cost_drag"] == pytest.approx(12.6 * 0.005, abs=1e-6)


def test_an_empty_track_returns_nan_rather_than_a_flattering_zero():
    """⚠️ CLAUDE.md §5 rule 2: an absent measurement is absent, never inferred."""
    out = P.stats(P.Track(name="empty"), horizon=20)
    assert out["n_periods"] == 0
    assert out["sharpe"] != out["sharpe"]
    assert out["cagr"] != out["cagr"]


# --- PRF-0: the price band, shipped as the DEFAULT on 2026-08-19 -----------------


def _bars(exchange, returns):
    return pd.DataFrame({
        "exchange": [exchange] * len(returns),
        "day_ret": list(returns),
    })


def test_the_ceiling_is_per_exchange_and_not_one_number():
    """HOSE 7 %, HNX 10 %, UPCOM 15 % — a +8 % close is at the band on HOSE and is an
    ordinary day on UPCOM. One global threshold would mis-flag both directions."""
    eight = [0.08]
    assert bool(P.mark_ceiling(_bars("HOSE", eight)).iloc[0]) is True
    assert bool(P.mark_ceiling(_bars("HNX", eight)).iloc[0]) is False
    assert bool(P.mark_ceiling(_bars("UPCOM", eight)).iloc[0]) is False


def test_the_tolerance_catches_a_close_just_under_the_limit():
    """A print at 0.93x the limit is already illiquid on that side; the exact tick is not
    knowable from a daily bar, which is why the rule is a band and not an equality."""
    just_under = 0.07 * P.CEILING_TOLERANCE + 1e-9
    assert bool(P.mark_ceiling(_bars("HOSE", [just_under])).iloc[0]) is True
    assert bool(P.mark_ceiling(_bars("HOSE", [0.07 * 0.90])).iloc[0]) is False


def test_an_unknown_exchange_and_a_missing_return_are_NOT_flagged():
    """⚠️ NaN -> False is the safe direction for an ENTRY screen only: it keeps the row.
    The sell side needs the opposite default, which is why PRF-4 lists floor days as a
    separate unbuilt item rather than something this quietly half-covers."""
    frame = pd.DataFrame({"exchange": ["LSE", "HOSE"], "day_ret": [0.50, float("nan")]})
    assert P.mark_ceiling(frame).tolist() == [False, False]


def test_dropping_the_ceiling_removes_exactly_the_flagged_rows():
    panel = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02"] * 4),
        "ticker": ["A", "B", "C", "D"],
        "at_ceiling": [True, False, False, True],
    })
    buyable, dropped = P.drop_ceiling(panel)
    assert dropped == 2
    assert buyable["ticker"].tolist() == ["B", "C"]


def test_a_panel_that_cannot_say_what_was_buyable_REFUSES_to_be_traded():
    """⚠️ The failure this guards is invisible, not loud: a backtest that silently skips
    the exclusion reports a number the market would not have given you, and its output
    looks identical to one that applied it."""
    panel = pd.DataFrame({"date": [], "ticker": [], "y_pred": []})
    with pytest.raises(ValueError, match="PRF-0"):
        P.drop_ceiling(panel)


# --- PRF-2: the 3-channel hand baseline ----------------------------------------


def _basic(n_dates=30, tickers=("AAA", "BBB", "CCC", "DDD"), seed=0):
    from backtest import handscreen as H

    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2020-01-01", periods=n_dates)
    rows = []
    for j, t in enumerate(tickers):
        close = 100.0 * (1 + j) * np.cumprod(1 + rng.normal(0, 0.02, n_dates))
        for i, d in enumerate(dates):
            rows.append({
                "date": d, "ticker": t, "close_adjust": close[i],
                "value_matched": rng.lognormal(10 + j, 0.3),
                "drv_order_vol_imb_5": rng.normal(),
                "drv_dist_from_high_63": rng.normal(),
            })
    return H, pd.DataFrame(rows), dates, list(tickers)


def test_the_hand_score_is_invariant_to_each_channels_UNITS():
    """It is a mean of within-date PERCENTILE ranks, so rescaling a channel — or changing
    its sign convention's magnitude — cannot move the ordering. Same argument
    `cross_sectional.py` §3 makes for ranking features within a date."""
    H, basic, dates, tickers = _basic()
    a = H.score_frame(basic, tickers, dates)
    scaled = basic.copy()
    scaled["drv_order_vol_imb_5"] = scaled["drv_order_vol_imb_5"] * 1e6 + 42.0
    b = H.score_frame(scaled, tickers, dates)
    pd.testing.assert_series_equal(a["y_pred"], b["y_pred"])


def test_the_trailing_return_is_computed_BEFORE_the_date_filter():
    """⚠️ THE TRAP THE DOCSTRING NAMES. Filtering first makes `shift(5)` step back five
    SCORED rows, which spans the split boundary and every gap — so the first rows of a
    test split would carry a multi-week return labelled as five sessions."""
    H, basic, dates, tickers = _basic(n_dates=40)
    late = dates[20:]

    correct = H.score_frame(basic, tickers, late)
    # what the wrong order produces: cut the history, then derive
    filtered_first = H.score_frame(basic[basic["date"].isin(set(late))], tickers, late)

    assert not np.allclose(correct["y_pred"].to_numpy(),
                           filtered_first["y_pred"].to_numpy(), equal_nan=True), (
        "the two orders agree, so this fixture no longer exercises the trap"
    )
    # the correct one has a trailing return on its FIRST scored date; the wrong one cannot
    first_day = correct[correct["date"] == late[0]]
    assert first_day["y_pred"].notna().all()


def test_the_quintile_screen_is_off_by_default_and_filters_when_asked():
    """§8g screens to the top liquidity quintile; the comparison against a model that
    picks from a fixed 150 must NOT, or it compares two universes."""
    H, basic, dates, tickers = _basic(tickers=tuple(f"T{i}" for i in range(10)))
    wide = H.score_frame(basic, [f"T{i}" for i in range(10)], dates)
    narrow = H.score_frame(basic, [f"T{i}" for i in range(10)], dates, quintile=True)
    assert len(narrow) < len(wide)
    assert narrow["in_quintile"].all()
    assert not wide["in_quintile"].all()


def test_a_name_missing_ONE_channel_still_scores_on_the_others():
    H, basic, dates, tickers = _basic()
    basic.loc[basic["ticker"] == "AAA", "drv_dist_from_high_63"] = np.nan
    out = H.score_frame(basic, tickers, dates)
    aaa = out[out["ticker"] == "AAA"]
    assert aaa["y_pred"].notna().any(), "a name lost every score for one missing channel"
