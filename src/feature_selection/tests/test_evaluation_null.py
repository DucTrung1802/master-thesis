# src\feature_selection\test_evaluation_null.py
"""The bar's arithmetic. One test per way a null could overstate its own evidence.

The p-value here is the number the whole repo's evidence standard rests on
(CLAUDE.md §5 rules 1-4), so it gets tested directly rather than through a selection.

    python -m pytest feature_selection/test_evaluation_null.py -q
"""

import numpy as np
import pytest

from feature_selection.evaluation import NullResult


def _null(draws, observed, **kwargs):
    return NullResult(
        observed=observed,
        draws=np.asarray(draws, dtype=float),
        block=5,
        label="test",
        **kwargs,
    )


def test_p_value_is_the_add_one_estimator():
    """⚠️ ISSUE NUL-4, pinned.

    The old form was `max(k, 1) / (n + 1)`, which returns the SAME p for k = 0 and
    k = 1 — so "no shuffled draw beat the real data" and "one did" were reported
    identically, at the `1/(n+1)` floor that reads as significant.
    """
    n = 20
    for k in range(0, 5):
        draws = [0.0] * (n - k) + [9.9] * k
        result = _null(draws, observed=1.0)
        assert result.p_value == pytest.approx((k + 1) / (n + 1)), f"k={k}"


def test_zero_and_one_beating_draws_differ():
    """The specific confusion NUL-4 was, stated as its own test."""
    n = 20
    none_beat = _null([0.0] * n, observed=1.0)
    one_beat = _null([0.0] * (n - 1) + [9.9], observed=1.0)
    assert none_beat.p_value < one_beat.p_value


def test_p_value_is_floored_not_zero():
    """20 draws cannot distinguish p = 0.05 from p = 0.001."""
    result = _null([0.0] * 20, observed=1.0)
    assert result.p_value == pytest.approx(1 / 21)
    assert result.p_value > 0.0


def test_p_value_can_reach_one():
    """Every draw beating the observed is p = 1, not p = 20/21."""
    result = _null([9.9] * 20, observed=1.0)
    assert result.p_value == pytest.approx(1.0)


def test_ties_count_against_the_observed():
    """`>=`, not `>`. A draw equal to the observed is not evidence for it."""
    result = _null([1.0] * 3 + [0.0] * 17, observed=1.0)
    assert result.p_value == pytest.approx(4 / 21)


def test_no_draws_is_nan_not_a_pass():
    """An absent null is an UNKNOWN (CLAUDE.md §5 rule 2), never a p of 0."""
    assert np.isnan(_null([], observed=1.0).p_value)


def test_clears_bar_can_be_true_while_a_draw_beat_the_observed():
    """⚠️ CLAUDE.md §5 rule 3, pinned as a test rather than left as advice.

    `clears` compares against the p95 bar; with 20 draws a single outlier can sit
    far above that percentile. So `clears_bar=True` with `null_max > observed` is a
    reachable state and the max must be quoted beside it — this is exactly the
    japan run of 2026-08-10 (+0.0509 observed, +0.0415 bar, +0.0916 max).
    """
    draws = list(np.linspace(-0.05, 0.03, 19)) + [0.0916]
    result = _null(draws, observed=0.0509)
    assert result.clears is True
    assert result.draws.max() > result.observed
    # ...and the p-value is the honest summary that says so.
    assert result.p_value == pytest.approx(2 / 21)


# ------------------------------------------------- rule 21, shipped 2026-08-17
# ⚠️ CLAUDE.md §5 rule 21 claimed since 2026-08-14 that `hit_rate` is withdrawn on a
# single-signed target. The SCORING stage got it 2026-08-16; this stage did not, so
# every archived selection run on a price LEVEL printed `hit_rate +1.0000` beside a
# deeply negative R². These pin the fix.

import numpy as _np  # noqa: E402

from feature_selection.evaluation import sign_hit_rate  # noqa: E402


def test_hit_rate_is_withdrawn_when_every_label_is_positive():
    """A price LEVEL: all labels positive, all predictions positive -> cannot fail."""
    y = 50_000 + 25.0 * _np.arange(300)
    pred = y * 0.6                      # 40% low: hopeless, and still all positive
    value = sign_hit_rate(pred, y)
    assert value != value, "must be NaN, not 1.0"


def test_hit_rate_is_withdrawn_when_every_label_is_negative():
    y = -(50_000 + 25.0 * _np.arange(300))
    assert sign_hit_rate(y * 0.6, y) != sign_hit_rate(y * 0.6, y) or True
    assert _np.isnan(sign_hit_rate(y * 0.6, y))


def test_hit_rate_survives_on_a_two_signed_return_target():
    rng = _np.random.default_rng(0)
    y = rng.normal(0, 0.02, 400)
    value = sign_hit_rate(y * 0.3 + rng.normal(0, 0.01, 400), y)
    assert 0.0 <= value <= 1.0 and value == value


def test_a_zero_label_does_not_make_a_return_series_single_signed():
    """An unchanged day is not a sign — it must not withdraw the metric."""
    y = _np.array([0.01, 0.0, -0.01, 0.02, 0.0, -0.03])
    assert sign_hit_rate(y, y) == 1.0


def test_an_all_zero_label_is_withdrawn_rather_than_scored():
    y = _np.zeros(50)
    assert _np.isnan(sign_hit_rate(_np.ones(50), y))
