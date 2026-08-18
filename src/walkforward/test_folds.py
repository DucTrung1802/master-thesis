# src\walkforward\test_folds.py
"""What a walk-forward must not get wrong, pinned.

Each test names a way a fold set stops being out of sample: overlapping test blocks,
a train window that reaches past its own boundary, a fold whose val window is empty.
"""

import pandas as pd
import pytest

from walkforward.folds import Fold, FoldBuilder, make_folds


DATES = pd.bdate_range("2009-01-02", "2026-08-07")


def test_folds_tile_the_out_of_sample_period_without_overlap():
    """⚠️ Overlapping test blocks would double-count dates in the concatenated track,
    and every statistic computed on it would be wrong."""
    folds = make_folds(DATES, "2017-01-01", 12, 12)
    assert len(folds) == 10
    for earlier, later in zip(folds, folds[1:]):
        assert earlier.test_end == later.val_end       # butt-jointed, no gap
        assert earlier.test_end <= later.test_end
    assert folds[0].val_end == pd.Timestamp("2017-01-01")
    assert folds[-1].test_end >= DATES[-1]


def test_the_train_window_expands_and_never_reaches_its_own_test_block():
    folds = make_folds(DATES, "2017-01-01", 12, 12)
    for fold in folds:
        assert fold.train_end < fold.val_end < fold.test_end
    # expanding: each fold's train boundary is later than the previous one's
    for earlier, later in zip(folds, folds[1:]):
        assert later.train_end > earlier.train_end


def test_a_first_test_inside_the_sample_start_raises():
    """A fold with no training data is not a fold."""
    with pytest.raises(ValueError):
        make_folds(DATES, "2008-01-01", 12, 12)


def test_a_step_that_outruns_the_sample_yields_no_empty_block():
    """The trailing partial block is real OOS data and is kept; an empty one is not."""
    folds = make_folds(DATES, "2026-01-01", 12, 12)
    assert len(folds) == 1
    assert folds[0].test_end >= DATES[-1]


def test_a_shorter_step_names_folds_by_date_not_by_year():
    folds = make_folds(DATES, "2024-01-01", 6, 12)
    assert len(folds) >= 4
    assert all(f.tag.startswith("oos20") for f in folds)
    assert folds[0].tag != folds[1].tag


# ------------------------------------------------------------------ the builder


class _Stub(FoldBuilder):
    """`FoldBuilder` without a database — only `_bounds` and `name` are under test."""

    def __init__(self):
        super().__init__(ticker="all", table="rank_20day__final__d20_h20")


def test_bounds_are_taken_from_the_fold_dates_not_from_the_ratios():
    """⚠️ The base class cuts at `int(n x ratio)`, which cannot express "test is 2017".
    If this override stops firing, every fold silently becomes a 70/15/15 split of its
    own truncated frame — still runnable, and no longer a walk-forward."""
    stub = _Stub()
    fold = Fold(0, "oos2017", pd.Timestamp("2016-01-01"),
                pd.Timestamp("2017-01-01"), pd.Timestamp("2018-01-01"))
    dates = pd.Series(pd.bdate_range("2009-01-02", "2017-12-29"))
    bounds = stub.use(fold)._bounds(dates)

    assert bounds.train_end_date == pd.Timestamp("2016-01-01")
    assert bounds.val_end_date == pd.Timestamp("2017-01-01")
    assert bounds.train_dates == int((dates < "2016-01-01").sum())
    assert bounds.test_dates == int((dates >= "2017-01-01").sum())
    assert bounds.n_dates == len(dates)


def test_an_unsliced_frame_raises_rather_than_silently_widening_the_test_block():
    """The caller must slice to `date < test_end`. Without it the test block would run
    to the end of the sample and the fold would not be one."""
    stub = _Stub()
    fold = Fold(0, "oos2017", pd.Timestamp("2016-01-01"),
                pd.Timestamp("2017-01-01"), pd.Timestamp("2018-01-01"))
    # a frame that stops BEFORE the test block starts -> empty test -> must raise
    dates = pd.Series(pd.bdate_range("2009-01-02", "2016-06-30"))
    with pytest.raises(ValueError, match="empty block"):
        stub.use(fold)._bounds(dates)


def test_each_fold_gets_its_own_dataset_name():
    """⚠️ Two folds sharing a folder name would have the second overwrite the first,
    and the concatenated track would be built from one fold's tensors twice."""
    stub = _Stub()
    plain = stub.name
    a = stub.use(Fold(0, "oos2017", pd.Timestamp("2016-01-01"),
                      pd.Timestamp("2017-01-01"), pd.Timestamp("2018-01-01"))).name
    b = stub.use(Fold(1, "oos2018", pd.Timestamp("2017-01-01"),
                      pd.Timestamp("2018-01-01"), pd.Timestamp("2019-01-01"))).name
    assert a != b
    assert a.startswith(plain) and b.startswith(plain)
    assert a.endswith("oos2017") and b.endswith("oos2018")
