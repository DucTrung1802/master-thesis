# src\result_evaluator\test_metrics.py
"""One test per way a metric could flatter a model that has nothing.

No run folder, no model, ~2 s. Each test names a specific failure: a core metric that
is not invariant to the score's units (and so cannot compare a regressor to a
classifier), a null that is too tight because it forgot the label overlap, a verdict
that reads noise as skill, or a leaderboard that mixes two different `long_short`.

    python -m pytest result_evaluator/test_metrics.py -q
"""

import numpy as np
import pandas as pd
import pytest

from result_evaluator import metrics as M


def _signal(n=600, strength=0.3, seed=0):
    """A score with a known, tunable relationship to a realised return."""
    rng = np.random.default_rng(seed)
    y = rng.normal(0, 0.03, n)
    score = strength * y + (1 - strength) * rng.normal(0, 0.03, n)
    return y, score


def _overlapping(n=600, horizon=5, seed=0):
    """Returns with the autocorrelation a 5-day forward label actually has.

    Consecutive samples share `h-1` of their `h` days, which is the whole reason the
    null has to be block-shuffled.
    """
    rng = np.random.default_rng(seed)
    daily = rng.normal(0, 0.012, n + horizon)
    return np.array([daily[i : i + horizon].sum() for i in range(n)])


# ------------------------------------------------------ the core is unit-invariant


def test_the_core_metrics_do_not_change_when_the_score_is_rescaled():
    """The reason one block fits every model: a regressor's return, a classifier's
    probability and a ranker's rank are the same ordering in different units."""
    y, score = _signal()
    base = M.core_metrics(y, score)
    for transform in (lambda s: 100 * s, lambda s: 1 / (1 + np.exp(-50 * s))):
        moved = M.core_metrics(y, transform(score))
        for key in ("ic", "dir_auc", "hit_rate", "long_short"):
            assert np.isclose(base[key], moved[key]), key


def test_a_reversed_score_reverses_every_core_metric():
    """A sign error must be loud, not invisible."""
    y, score = _signal(strength=0.6)
    forward = M.core_metrics(y, score)
    backward = M.core_metrics(y, -score)
    assert forward["ic"] > 0 > backward["ic"]
    assert forward["dir_auc"] > 0.5 > backward["dir_auc"]
    assert forward["long_short"] > 0 > backward["long_short"]


def test_two_scores_with_the_same_ic_can_have_very_different_long_short():
    """Why both are reported. An IC can be earned entirely in the middle of the
    distribution, where nothing is traded; `long_short` only pays for the tails."""
    rng = np.random.default_rng(3)
    y = rng.normal(0, 0.03, 1000)
    order = np.argsort(y)
    cut = 200

    tails = np.empty(1000)          # ranks the tails, middle scrambled
    tails[order[:cut]] = rng.normal(-3, 0.2, cut)
    tails[order[-cut:]] = rng.normal(3, 0.2, cut)
    tails[order[cut:-cut]] = rng.normal(0, 0.2, 1000 - 2 * cut)

    middle = np.empty(1000)         # ranks the middle, tails folded inside it
    middle[order[cut:-cut]] = np.linspace(-1, 1, 1000 - 2 * cut)
    middle[order[:cut]] = rng.normal(0, 0.05, cut)
    middle[order[-cut:]] = rng.normal(0, 0.05, cut)

    a, b = M.core_metrics(y, tails), M.core_metrics(y, middle)
    assert a["ic"] > 0.3 and b["ic"] > 0.3          # comparable ranking skill
    assert a["long_short"] > 2 * b["long_short"]    # nothing like the same payoff


# ------------------------------------------------------------------------ the null


def test_a_worthless_score_does_not_clear_its_own_bar():
    """The failure mode the whole package exists for."""
    y = _overlapping(seed=1)
    score = np.random.default_rng(2).normal(size=len(y))
    out = M.evaluate(y, score, horizon=5, lookback=20, draws=200)
    assert not out["ic_clears"]
    assert not out["dir_auc_clears"]
    assert "NO SKILL DEMONSTRATED" in M.verdict(out)


def test_a_strong_score_does_clear_it():
    """A bar nothing can clear is not a bar."""
    y = _overlapping(seed=4)
    score = y + np.random.default_rng(5).normal(0, y.std(), len(y))
    out = M.evaluate(y, score, horizon=5, lookback=20, draws=200)
    assert out["ic_clears"] and out["dir_auc_clears"]
    assert out["ic_p"] < 0.05
    assert "clears the shuffled-label bar" in M.verdict(out)


def test_a_row_wise_null_would_be_too_tight_on_an_overlapping_label():
    """`block=1` destroys the label's autocorrelation, so the bar drops. That is the
    bar a worthless run would be measured against if the shuffle were row-wise."""
    y = _overlapping(seed=6)
    score = np.random.default_rng(7).normal(size=len(y))
    blocked = M.null_metrics(y, score, block=25, draws=300)
    rowwise = M.null_metrics(y, score, block=1, draws=300)
    assert blocked["ic_bar"] > rowwise["ic_bar"]


def test_the_p_value_is_floored_rather_than_reported_as_zero():
    """N draws cannot resolve p below 1/N, and printing 0.000 would claim they can."""
    y = _overlapping(seed=8)
    out = M.null_metrics(y, y, block=25, draws=50)
    assert out["ic_p"] == pytest.approx(1 / 51)


def test_every_draw_is_usable_despite_the_shuffle_padding_with_nan():
    """The bug this caught: `block_shuffle` pads to a whole number of blocks with NaN,
    those NaN land inside the retained slice, the metric comes back NaN, and a p-value
    divided by the REQUESTED draw count instead of the usable one understates itself
    by the discard ratio — turning a worthless run into 'clears the bar'."""
    y = _overlapping(n=635, seed=12)          # 635 is not a multiple of block=25
    score = np.random.default_rng(13).normal(size=len(y))
    out = M.null_metrics(y, score, block=25, draws=100)
    assert out["ic_draws_used"] >= 95, "the padding NaN is eating the draws again"
    assert out["dir_auc_draws_used"] >= 95


def test_the_p_value_uses_the_usable_draw_count_as_its_denominator():
    y = _overlapping(n=635, seed=14)
    out = M.null_metrics(y, y, block=25, draws=60)
    assert out["ic_p"] == pytest.approx(1 / (out["ic_draws_used"] + 1))


def test_a_series_too_short_for_three_blocks_reports_nan_not_a_bar():
    """A null built from two blocks is not a null."""
    y = _overlapping(n=40, seed=9)
    out = M.null_metrics(y, y, block=25, draws=50)
    assert np.isnan(out["ic_p"])


# ----------------------------------------------------------------- the task extras


def test_the_zero_baseline_is_the_weaker_bar_when_the_target_has_a_mean():
    """`RMSE_zero² = var + mean²`, so beating it does NOT imply a positive r2. Both
    bars are reported because on a target with drift they are not the same test."""
    rng = np.random.default_rng(10)
    y = rng.normal(0.02, 0.01, 2000)                 # mean twice the sd
    # RMSE lands between sd(y)=0.010 and RMSE_zero=sqrt(var+mean^2)=0.022: worse than
    # predicting the mean, better than predicting 0.
    y_pred = y.mean() + rng.normal(0, 0.015, 2000)
    extras = M.regression_extras(y, y_pred)
    assert extras["beats_zero_baseline"]
    assert extras["r2"] < 0, "the point is a model that clears the weaker bar only"


def test_a_positive_r2_always_clears_the_zero_baseline():
    """The implication that does hold, asserted so the docstring cannot drift."""
    rng = np.random.default_rng(11)
    y = rng.normal(0.02, 0.01, 2000)
    y_pred = y + rng.normal(0, 0.004, 2000)
    extras = M.regression_extras(y, y_pred)
    assert extras["r2"] > 0 and extras["beats_zero_baseline"]


def test_accuracy_is_uninformative_on_an_imbalanced_target():
    """`probability_gain_5pct_5day` has a 0.071 test base rate — predicting 'never'
    scores 0.929, so `beats_majority` is False for anything worth having."""
    label = np.zeros(1000)
    label[:71] = 1
    prob = np.where(label > 0, 0.4, 0.2)      # ranks perfectly, never crosses 0.5
    extras = M.classification_extras(label, prob)
    assert extras["accuracy"] == pytest.approx(0.929)
    assert not extras["beats_majority"]
    assert extras["pr_auc_lift"] > 5


def test_a_classifier_and_a_regressor_produce_the_same_core_keys():
    """The property that lets them share one leaderboard."""
    y, score = _signal()
    regression = M.evaluate(y, score, task=M.REGRESSION, draws=20)
    classifier = M.evaluate(
        y, 1 / (1 + np.exp(-50 * score)), task=M.CLASSIFICATION, draws=20
    )
    for key in M.CORE_METRICS:
        assert key in regression and key in classifier
    assert set(M.CORE_METRICS) <= set(regression) & set(classifier)


def test_mismatched_lengths_raise_rather_than_scoring_a_shifted_series():
    with pytest.raises(ValueError, match="mismatched"):
        M.core_metrics(np.zeros(10), np.zeros(9))


# ------------------------------------------------------------------- the panel


def _panel(n_dates=600, n_tickers=20, horizon=5, seed=0):
    """An N-ticker panel with a market factor, so pooling and per-date differ."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2015-01-01", periods=n_dates)
    tickers = [f"B{i:02d}" for i in range(n_tickers)]
    market = np.repeat(rng.normal(0, 0.02, n_dates), n_tickers)   # one number per day
    idio = rng.normal(0, 0.03, n_dates * n_tickers)
    return (
        np.repeat(dates.astype(str), n_tickers),
        np.tile(tickers, n_dates),
        market + idio,
        idio,          # a score that knows the IDIOSYNCRATIC part only
    )


def test_panel_n_eff_counts_dates_not_rows():
    """20 banks on one date are ONE observation of the market, not twenty. The
    row-wise figure would overstate the evidence twentyfold."""
    dates, tickers, y, score = _panel()
    out = M.evaluate_panel(dates, tickers, y, score, horizon=5, lookback=20, draws=20)
    assert out["n"] == 12000 and out["n_dates"] == 600
    assert out["n_eff"] == pytest.approx(120.0)      # 600 dates / h, not 12000 / h


def test_a_random_score_fails_the_panel_bar_and_a_real_one_clears_it():
    """A bar nothing can clear is not a bar; one everything clears is worse."""
    dates, tickers, y, score = _panel(seed=1)
    noise = np.random.default_rng(2).normal(size=len(y))
    assert not M.evaluate_panel(
        dates, tickers, y, noise, horizon=5, lookback=20, draws=200
    )["ic_clears"]
    assert M.evaluate_panel(
        dates, tickers, y, score, horizon=5, lookback=20, draws=200
    )["ic_clears"]


def test_the_panel_null_moves_whole_dates_so_each_date_keeps_its_cross_section():
    """A row-wise block shuffle on a date-sorted panel permutes ~1.25 dates and tears
    each date's cross-section apart, which is not the null anyone wants."""
    dates, tickers, y, score = _panel(n_dates=300, seed=3)
    Y, S, valid = M.panel_matrices(dates, tickers, y, score)
    assert Y.shape == (300, 20)
    # Every draw must preserve the per-date width of a dense panel.
    out = M.panel_null_metrics(Y, S, valid, block=25, draws=30)
    assert out["ic_draws_used"] == 30


def test_pooling_a_panel_is_not_the_same_reading_as_scoring_it_per_date():
    """The whole reason `evaluate_panel` exists. A market factor inflates a pooled IC
    — it rewards knowing 'is today a good day' — while the per-date IC measures only
    'which bank beats which', which is what a market-neutral book trades."""
    dates, tickers, y, score = _panel(seed=4)
    pooled = M.core_metrics(y, y)                  # a score that is the outcome itself
    per_date = M.evaluate_panel(
        dates, tickers, y, y, horizon=5, lookback=20, draws=20
    )
    assert pooled["ic"] > 0.99 and per_date["ic"] > 0.99
    # ...but on a score that only knows the idiosyncratic part they diverge.
    assert M.core_metrics(y, score)["ic"] < M.evaluate_panel(
        dates, tickers, y, score, horizon=5, lookback=20, draws=20
    )["ic"]


def test_a_ragged_panel_is_scored_on_the_cells_that_exist():
    """Banks list at different times, so the matrix has holes. They must not become
    zeros, and a date below the width floor must not be averaged in."""
    dates, tickers, y, score = _panel(n_dates=200, seed=5)
    frame = pd.DataFrame({"d": dates, "t": tickers, "y": y, "s": score})
    late = frame["t"].isin(["B18", "B19"]) & (frame["d"] < "2015-04-01")
    frame = frame[~late]
    Y, S, valid = M.panel_matrices(frame["d"], frame["t"], frame["y"], frame["s"])
    assert Y.shape == (200, 20)
    assert valid.sum() == len(frame)
    out = M.panel_core_metrics(Y, S, valid)
    assert out["n"] == len(frame) and np.isfinite(out["ic"])
