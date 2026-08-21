# src\walkforward\test_compare_sharpe.py
"""`P1-9` — pin that `compare.paired` reports the SHARPE difference as its own estimand.

⚠️ The defect this pins is not a crash. `paired()` returned a `t` computed on the mean
period-RETURN difference while `main` printed `d_sharpe` in the same row, so a reader —
including `PRF-8` and CLAUDE.md §6-0-ter-2 — took the `t` as a test of the number beside
it. The two can disagree in SIGN, and the first test below constructs exactly that case.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtest import portfolio as P
from walkforward.compare import paired
from walkforward.pair import sharpe

HORIZON = 20
PER_YEAR = P.SESSIONS_PER_YEAR / HORIZON


def _series(values):
    idx = pd.date_range("2017-01-03", periods=len(values), freq="20D")
    return pd.Series(np.asarray(values, float), index=idx)


def test_the_two_estimands_can_disagree_in_SIGN(rng=np.random.default_rng(11)):
    """The `gbt` case: a LOWER mean return at LOWER volatility — higher Sharpe.

    ⚠️ This is the whole of P1-9 in one assertion. Arm `a` earns less on average and is
    calmer; the mean-return test is negative and the Sharpe difference is positive, so a
    reader taking `t` as a verdict on `d_sharpe` reads the sign backwards.
    """
    common = rng.normal(0.0, 0.05, 400)          # the shared market factor
    a = _series(common * 0.5 + 0.0100)           # calmer, lower mean
    b = _series(common * 1.0 + 0.0125)           # noisier, higher mean

    stat = paired(a, b, HORIZON, draws=400)
    assert stat["mean_diff"] < 0, "arm a must earn less on average"
    assert stat["t"] < 0
    assert stat["d_sharpe"] > 0, "arm a must be BETTER risk-adjusted"
    # the sign disagreement is the point, and it must be visible in the returned dict
    assert np.sign(stat["t"]) != np.sign(stat["d_sharpe"])


def test_the_paired_sharpe_reproduces_the_pooled_definition_exactly():
    """`d_sharpe` must equal `sharpe(a) - sharpe(b)` under `portfolio.stats`' formula.

    ⚠️ `stats` annualises a PERIOD return by `sqrt(252/h)`, not `sqrt(252)`. Using the
    daily constant would scale the difference and leave every p-value untouched — a level
    that silently disagrees with the pooled table printed beside it.
    """
    rng = np.random.default_rng(3)
    a = _series(rng.normal(0.01, 0.04, 200))
    b = _series(rng.normal(0.00, 0.05, 200))

    stat = paired(a, b, HORIZON, draws=0)
    expected = sharpe(a.to_numpy(), PER_YEAR) - sharpe(b.to_numpy(), PER_YEAR)
    assert stat["d_sharpe"] == pytest.approx(expected, abs=1e-12)

    # and that IS `stats`' Sharpe, computed independently here
    def stats_sharpe(x):
        x = np.asarray(x, float)
        return float(x.mean() / x.std(ddof=1) * np.sqrt(PER_YEAR))

    assert stat["d_sharpe"] == pytest.approx(
        stats_sharpe(a) - stats_sharpe(b), abs=1e-12)


def test_the_sharpe_difference_carries_an_interval_and_a_p():
    rng = np.random.default_rng(5)
    a = _series(rng.normal(0.012, 0.04, 300))
    b = _series(rng.normal(0.002, 0.04, 300))

    stat = paired(a, b, HORIZON, draws=1000)
    assert np.isfinite(stat["sharpe_lo"]) and np.isfinite(stat["sharpe_hi"])
    assert stat["sharpe_lo"] < stat["d_sharpe"] < stat["sharpe_hi"]
    assert 0.0 <= stat["p_sharpe"] <= 1.0
    assert stat["p_sharpe"] < 0.10, "a clearly better arm should not read as a tie"


def test_two_identical_arms_give_a_zero_difference_and_a_p_of_one():
    rng = np.random.default_rng(7)
    values = rng.normal(0.01, 0.04, 250)
    a, b = _series(values), _series(values)

    stat = paired(a, b, HORIZON, draws=500)
    assert stat["corr"] == pytest.approx(1.0)
    assert stat["d_sharpe"] == pytest.approx(0.0, abs=1e-12)
    assert stat["mean_diff"] == pytest.approx(0.0, abs=1e-12)
    assert stat["p_sharpe"] == pytest.approx(1.0)


def test_a_tie_is_reported_as_a_tie_rather_than_as_a_small_number():
    """⚠️ The failure mode P1-9 leaves behind if the interval is dropped: a `d_sharpe` of
    +0.36 with a CI spanning zero is a tie, and without the CI it reads as a win."""
    rng = np.random.default_rng(13)
    common = rng.normal(0.0, 0.05, 120)
    a = _series(common + rng.normal(0.0020, 0.012, 120))
    b = _series(common + rng.normal(0.0000, 0.012, 120))

    stat = paired(a, b, HORIZON, draws=2000)
    assert stat["corr"] > 0.9, "the arms must share the market factor for this to be the case"
    if stat["sharpe_lo"] < 0 < stat["sharpe_hi"]:
        assert stat["p_sharpe"] > 0.05
    else:
        assert stat["p_sharpe"] <= 0.05


def test_the_bootstrap_preserves_the_pairing():
    """Resampling the two arms independently would destroy the correlation the pairing
    buys, and the interval would blow up. Pinned by comparing against that mistake."""
    rng = np.random.default_rng(17)
    common = rng.normal(0.0, 0.06, 300)
    a = _series(common + rng.normal(0.004, 0.008, 300))
    b = _series(common + rng.normal(0.000, 0.008, 300))

    stat = paired(a, b, HORIZON, draws=2000)
    width = stat["sharpe_hi"] - stat["sharpe_lo"]

    # the unpaired width, from two INDEPENDENT bootstraps of the same two series
    from walkforward.pair import block_bootstrap_diff
    shuffled = rng.permutation(b.to_numpy())
    loose = block_bootstrap_diff(a.to_numpy(), shuffled, block=2, draws=2000,
                                 sessions=PER_YEAR)["sharpe"]
    assert width < (loose["ci_hi"] - loose["ci_lo"]), (
        "the paired interval must be TIGHTER than one over an arm with the common "
        "factor broken — otherwise the pairing is not being preserved"
    )


def test_ac1_is_reported_so_the_block_length_is_auditable():
    rng = np.random.default_rng(23)
    a = _series(rng.normal(0.01, 0.04, 200))
    b = _series(rng.normal(0.00, 0.04, 200))
    assert np.isfinite(paired(a, b, HORIZON, draws=0)["ac1"])


def test_draws_zero_returns_the_point_estimate_without_an_interval():
    """A caller that does not want the bootstrap still gets both LEVELS."""
    rng = np.random.default_rng(29)
    a = _series(rng.normal(0.01, 0.04, 100))
    b = _series(rng.normal(0.00, 0.04, 100))

    stat = paired(a, b, HORIZON, draws=0)
    assert np.isfinite(stat["d_sharpe"]) and np.isfinite(stat["t"])
    assert not np.isfinite(stat["sharpe_lo"])


def test_too_few_periods_returns_blanks_rather_than_a_number():
    a, b = _series([0.01, 0.02]), _series([0.00, 0.01])
    stat = paired(a, b, HORIZON)
    assert stat["n"] == 2
    assert not np.isfinite(stat["d_sharpe"])
