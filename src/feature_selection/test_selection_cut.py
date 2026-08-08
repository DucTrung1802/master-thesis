# src\feature_selection\test_selection_cut.py
"""One test per way the per-run cut could quietly manufacture a feature list.

No database, no fit, ~1 s — everything here runs on synthetic score matrices of the
shape `feature_importance.csv` has. The point of each test is a specific failure:
a null that passes noise, a knee that admits everything, a prune that keeps twins,
or a rebuild that does not reproduce the ensemble it claims to read.

    python -m pytest feature_selection/test_selection_cut.py -q
"""

import numpy as np
import pandas as pd
import pytest

from feature_selection import selection_cut as cut
from feature_selection.selector import METHODS


def _importance(scores: np.ndarray, channels=None) -> pd.DataFrame:
    """A `feature_importance.csv`-shaped frame from a (p, 6) score matrix."""
    p = scores.shape[0]
    channels = channels or [f"ch{i:03d}" for i in range(p)]
    frame = pd.DataFrame(scores, columns=list(METHODS))
    frame.insert(0, "channel", channels)
    frame["ensemble"] = frame[list(METHODS)].rank(
        ascending=False, method="min"
    ).mean(axis=1)
    frame["spearman_vs_target"] = 0.0
    return frame


def _noise(p=60, seed=1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return _importance(rng.random((p, len(METHODS))))


# --------------------------------------------------------------- the rebuild


def test_method_ranks_reproduce_the_stored_ensemble():
    """The archive stores normalised SCORES; the ensemble is a mean of RANKS.

    If this drifts, every p-value below is computed against the wrong statistic.
    """
    frame = _noise()
    rebuilt = cut.method_ranks(frame).mean(axis=1)
    assert np.allclose(rebuilt, frame["ensemble"])


def test_dead_methods_are_excluded_from_live():
    frame = _noise()
    frame["lasso"] = 0.0
    assert "lasso" not in cut.live_methods(frame)


# ------------------------------------------------------------------- tier 1


def test_null_preserves_each_methods_marginal_rank_distribution():
    """The shuffle must destroy AGREEMENT only.

    A null that also changed a method's own distribution of ranks would be testing
    two things at once, and a dead method's constant column has to stay constant.
    """
    frame = _noise(p=40)
    ranks = cut.method_ranks(frame)
    pool = cut.consensus_null(ranks, seed=0)
    # every draw is a mean of six columns each holding the same multiset of ranks,
    # so the pooled MEAN must match the mean of the observed ensemble exactly
    assert pool.mean() == pytest.approx(ranks.mean(axis=1).mean(), rel=1e-6)


def test_consensus_keeps_nothing_on_pure_noise():
    """The false-positive test. Six independent rankings of 60 channels agree by
    chance; a tier that keeps any of them is not a bar."""
    kept, pvalues = cut.consensus_tier(_noise(p=60, seed=7), fdr_q=0.10)
    assert kept == set()
    assert pvalues.min() > 0.0        # the (k+1)/(N+1) floor, never exactly zero


def test_consensus_keeps_a_channel_all_six_methods_agree_on():
    """The power test — a planted unanimous leader must clear the same bar."""
    rng = np.random.default_rng(3)
    scores = rng.random((60, len(METHODS))) * 0.5
    scores[0, :] = 1.0                # one channel top on every method
    kept, _ = cut.consensus_tier(_importance(scores), fdr_q=0.10)
    assert "ch000" in kept


# ------------------------------------------------------------------- tier 2


def test_knee_finds_the_elbow_of_a_convex_curve():
    assert cut.knee([1.0, 0.9, 0.8, 0.1, 0.08, 0.06, 0.05, 0.04, 0.03]) == 3


def test_knee_returns_one_on_a_straight_line():
    """A curve with no bend must not be read as having one — this is the failure
    that made a knee on the `ensemble` column keep 1,313 of 1,458 channels."""
    assert cut.knee(list(np.linspace(1.0, 0.0, 50))) == 1


def test_specialist_keeps_a_channel_only_one_method_likes():
    """The reason the ensemble alone is not enough: a mean rank buries a channel
    that one method is certain about and five have no opinion on."""
    rng = np.random.default_rng(5)
    scores = rng.random((40, len(METHODS))) * 0.6
    scores[0, :] = 0.0
    scores[0, 2] = 1.0                # top on xgb_gain, last on everything else
    frame = _importance(scores)
    assert frame["ensemble"].idxmax() == 0          # the ensemble ranks it LAST
    kept, knees = cut.specialist_tier(frame)
    assert "ch000" in kept
    assert knees["xgb_gain"] >= 1


# --------------------------------------------------------------------- prune


def test_prune_drops_a_perfect_twin_and_names_the_winner():
    corr = pd.DataFrame(
        [[1.0, 0.99, 0.1], [0.99, 1.0, 0.1], [0.1, 0.1, 1.0]],
        index=["a", "b", "c"], columns=["a", "b", "c"],
    )
    kept, dropped = cut.prune(["a", "b", "c"], corr, threshold=0.9)
    assert kept == ["a", "c"]
    assert dropped == {"b": "a"}


def test_prune_is_uncapped_and_visits_every_channel():
    """The bug the cap caused: below `max_features` the walk stopped, so a channel
    pruned as redundant and a channel never examined were the same output."""
    p = 30
    corr = pd.DataFrame(np.eye(p), index=[f"ch{i:03d}" for i in range(p)],
                        columns=[f"ch{i:03d}" for i in range(p)])
    kept, _ = cut.prune(list(corr.index), corr, threshold=0.9)
    assert len(kept) == p


# ---------------------------------------------------------------- the rule


def test_suitable_returns_only_candidates_and_records_why():
    frame = _noise(p=50, seed=11)
    kept = cut.suitable(frame)
    assert len(kept) == kept["n_candidates"].iloc[0]     # no corr ⇒ no prune
    assert set(kept["kept_by"]) <= {"consensus", "specialist", "consensus+specialist"}
    assert kept["ensemble"].is_monotonic_increasing


def test_suitable_prunes_when_given_a_correlation_matrix():
    """A duplicated channel must not reach the fetch list twice."""
    rng = np.random.default_rng(13)
    scores = rng.random((20, len(METHODS)))
    scores[1, :] = scores[0, :] * 0.99                   # a near-twin of the leader
    frame = _importance(scores)
    p = len(frame)
    corr = pd.DataFrame(np.eye(p), index=frame["channel"], columns=frame["channel"])
    corr.iloc[0, 1] = corr.iloc[1, 0] = 0.97
    kept = cut.suitable(frame, corr)
    assert not {"ch000", "ch001"} <= set(kept["channel"])
    winner = kept[kept["absorbed_as_redundant"] != ""]
    assert len(winner) == 1


def test_cut_report_flags_a_degenerate_specialist_tier():
    """On a narrow pool six knees can cover everything. That is the honest answer at
    that width and must be visible as a number, not read as a selection."""
    report = cut.cut_report(_noise(p=8, seed=2))
    assert report["specialist_share"] >= 0.5
    assert report["n_channels"] == 8
