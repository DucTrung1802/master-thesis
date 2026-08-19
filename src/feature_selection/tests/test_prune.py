# src\feature_selection\tests\test_prune.py
"""PRF-9 — what the pool prune must not get wrong, pinned.

The prune decides which channels a selection is even OFFERED. Every test below names a
way it could stop being LABEL-FREE, stop being deterministic, or quietly keep nothing.
"""

import numpy as np
import pandas as pd
import pytest

from feature_selection.prune import (
    PruneResult,
    numeric_channels,
    prune_frame,
)


def _frame(n=400, seed=0):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2015-01-01", periods=n)
    base = rng.normal(size=n)
    return pd.DataFrame({
        "date": dates,
        "a": base,
        "a_twin": base * 3.0 + 0.001 * rng.normal(size=n),   # ~duplicate of a
        "b": rng.normal(size=n),
        "c": rng.normal(size=n),
        "constant": np.ones(n),
        "sparse": np.where(rng.random(n) < 0.5, rng.normal(size=n), np.nan),
    })


CHANNELS = ["a", "a_twin", "b", "c", "constant", "sparse"]


def test_a_redundant_twin_is_dropped_and_its_representative_named():
    out = prune_frame(_frame(), CHANNELS, corr_threshold=0.9)
    assert ("a" in out.kept) ^ ("a_twin" in out.kept), "the family kept 0 or 2 members"
    dropped = "a_twin" if "a" in out.kept else "a"
    assert out.dropped_redundant[dropped] in {"a", "a_twin"}


def test_a_constant_channel_is_dropped_though_it_correlates_with_NOTHING():
    """⚠️ It would survive every correlation prune while carrying no information — the
    one case where 'not redundant with anything' is exactly the wrong reading."""
    out = prune_frame(_frame(), CHANNELS)
    assert "constant" not in out.kept
    assert out.dropped_redundant["constant"] == "constant"


def test_a_low_coverage_channel_is_dropped_on_coverage_not_on_correlation():
    out = prune_frame(_frame(), CHANNELS, min_coverage=0.95)
    assert "sparse" in out.dropped_coverage
    assert "sparse" not in out.dropped_redundant


def test_the_prune_never_looks_at_a_LABEL():
    """⚠️ THE PROPERTY THE WHOLE MODULE EXISTS TO HAVE. Ranking channels by correlation
    with the target would build PRF-7's selection look-ahead into the candidate set,
    before any null could see it. Adding a label column must change nothing."""
    frame = _frame()
    a = prune_frame(frame, CHANNELS)
    labelled = frame.copy()
    # a label that correlates almost perfectly with `b` — if anything peeked, `b` moves
    labelled["cs_rank_20day"] = labelled["b"] * 2 + 1e-6
    b = prune_frame(labelled, CHANNELS)
    assert a.kept == b.kept


def test_the_prune_is_DETERMINISTIC_because_the_list_defines_an_experiment():
    """Two runs whose candidate sets differ are incomparable for a reason nobody wrote
    down. Ties break on coverage then name, both stable."""
    frame = _frame()
    first = prune_frame(frame, CHANNELS)
    shuffled = prune_frame(frame, list(reversed(CHANNELS)))
    assert first.kept == shuffled.kept


def test_the_budget_cut_is_recorded_as_its_own_reason():
    """⚠️ The budget step is the arbitrary one — it is a fact about the box, not about
    the channels — so it must never be filed under 'redundant'."""
    out = prune_frame(_frame(), CHANNELS, budget=2)
    assert len(out.kept) == 2
    assert out.dropped_budget, "a budget cut left no record of what it removed"
    assert not set(out.dropped_budget) & set(out.dropped_redundant)


def test_booleans_are_not_offered_as_numeric_channels():
    """`pool__ta` carries 208 boolean flags beside its 711 numeric channels. mean/sd/
    slope over a 0/1 flag is a different kind of feature — offering them is a separate
    decision, not a side effect of widening."""
    types = {"date": "date", "ticker": "text", "x": "double precision",
             "n": "bigint", "flag": "boolean"}
    assert numeric_channels(types, ("date", "ticker")) == ["x", "n"]


def test_nothing_survives_is_reported_rather_than_returned_empty_and_silent():
    frame = pd.DataFrame({"date": pd.bdate_range("2020-01-01", periods=10),
                          "x": [np.nan] * 10})
    out = prune_frame(frame, ["x"])
    assert out.kept == []
    assert out.n_candidates == 1
    assert "1 candidates -> 0 offered" in out.describe()
