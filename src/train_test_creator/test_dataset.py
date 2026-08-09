# src\train_test_creator\test_dataset.py
"""One test per way this stage could quietly hand the model a leaked or wrong tensor.

No database, ~1 s — everything runs on a synthetic panel shaped like a
`<target>__final__d<d>_h<h>` table. Each test names a specific failure: a window
built at the wrong `d`, a train label that reaches into val, a leading gap filled
from the future, a channel that is constant only in training, or a window that
straddles two tickers.

    python -m pytest train_test_creator/test_dataset.py -q
"""

import numpy as np
import pandas as pd
import pytest

from train_test_creator.dataset import TrainTestCreator, parse_final_table


def _panel(n_dates: int = 400, tickers=("AAA",), n_features: int = 4, horizon: int = 2):
    """A labelled daily panel with the key columns every unified table carries."""
    dates = pd.bdate_range("2015-01-01", periods=n_dates)
    rng = np.random.default_rng(0)
    frames = []
    for t, ticker in enumerate(tickers):
        frame = pd.DataFrame({"date": dates, "exchange": "HOSE", "ticker": ticker})
        for f in range(n_features):
            # Each ticker gets its own level, so a window that straddles two of them
            # is visible as a jump rather than as noise.
            frame[f"f{f}"] = rng.normal(100 * (t + 1), 1.0, n_dates)
        frame["return_5day"] = rng.normal(0, 0.02, n_dates)
        frame.loc[frame.index[-horizon:], "return_5day"] = np.nan
        frames.append(frame)
    return pd.concat(frames, ignore_index=True).sort_values(
        ["date", "exchange", "ticker"]
    ).reset_index(drop=True)


def _creator(**kwargs):
    kwargs.setdefault("table", "return_5day__final__d5_h2")
    return TrainTestCreator(ticker="test", **kwargs)


# --------------------------------------------------------------------- the name


def test_lookback_and_horizon_come_from_the_table_name():
    """The old notebook let `LOOKBACK_DAY` disagree with the view it read. `d` and `h`
    now have exactly one source, so that disagreement cannot be expressed."""
    assert parse_final_table("return_5day__final__d20_h5") == ("return_5day", 20, 5)
    creator = _creator()
    assert (creator.lookback, creator.horizon) == (5, 2)


def test_a_name_that_is_not_a_final_table_raises():
    """`<target>__lb20__final` is the DEAD view name. Reading it would window at
    whatever `d` the caller guessed."""
    with pytest.raises(ValueError, match="not a final feature table"):
        parse_final_table("return_5day__lb20__final")


# -------------------------------------------------------------------- the purge


def test_no_train_label_period_reaches_into_the_next_split_window():
    """The failure the old notebook shipped: it started each split `d-1` rows early
    and purged nothing, so the last train labels covered days the val windows read."""
    creator = _creator()
    panel = _panel()
    data = creator.build(panel)

    labelled = panel.dropna(subset=["return_5day"]).reset_index(drop=True)
    position = pd.Index(labelled["date"].dt.strftime("%Y-%m-%d"))
    for earlier, later in (("train", "val"), ("val", "test")):
        last_label_row = position.get_indexer(data.dates[earlier]).max()
        first_window_row = (
            position.get_indexer(data.dates[later]).min() - creator.lookback + 1
        )
        assert last_label_row + creator.horizon < first_window_row


def test_purging_costs_exactly_d_plus_h_minus_1_samples_per_boundary():
    """`d + h - 1` is `PurgedWalkForward.gap`; anything else is a different purge
    from the one the channels were selected under."""
    panel = _panel()
    purged = _creator().build(panel)
    unpurged = _creator(purge=False).build(panel)

    gap = purged.lookback + purged.horizon - 1
    assert purged.purge_gap == gap
    assert len(purged.y["train"]) == len(unpurged.y["train"]) - gap
    assert len(purged.y["val"]) == len(unpurged.y["val"]) - gap
    assert len(purged.y["test"]) == len(unpurged.y["test"])


# --------------------------------------------------------------- the statistics


def test_a_leading_gap_is_never_filled_from_the_future():
    """`ffill().bfill()` fills a leading gap with the first FUTURE value. On the real
    table that is 3,382 of 4,230 rows on one channel.

    The gap here is followed by one spike, so `bfill` and the train median give
    answers three orders of magnitude apart and the test can tell them apart.
    """
    panel = _panel()
    panel.loc[panel.index[:200], "f0"] = np.nan
    panel.loc[panel.index[200], "f0"] = 5000.0  # exactly what bfill would reach for

    data = _creator().build(panel)
    index = data.scaled_columns.index("f0")
    scaler = data.feature_scaler
    first_window = data.X["train"][0, :, data.feature_columns.index("f0")]
    raw = first_window * scaler.scale_[index] + scaler.mean_[index]

    assert np.isfinite(data.X["train"]).all()
    assert raw.max() < 500, "the leading gap was filled from the future"
    assert abs(float(raw[0]) - float(panel["f0"].iloc[200:280].median())) < 20


def test_the_scaler_never_sees_a_row_outside_a_train_sample():
    """The purged tail belongs to no train sample, so a statistic that reads it puts
    val-adjacent rows into the numbers the purge exists to keep out."""
    creator = _creator()
    panel = _panel()
    data = creator.build(panel)

    labelled = panel.dropna(subset=["return_5day"]).reset_index(drop=True)
    mask = creator._fit_mask(labelled, data.bounds)
    last_fit_date = labelled.loc[mask, "date"].max()
    assert str(last_fit_date.date()) == data.dates["train"][-1]


def test_a_channel_constant_in_train_is_dropped_with_its_reason():
    """`prop_buy_vol` on the real table: empty through training, live at test."""
    panel = _panel()
    # NaN past the train cut (0.70 of the dates), so no train sample sees a value.
    panel.loc[panel.index[: int(len(panel) * 0.8)], "f1"] = np.nan

    data = _creator().build(panel)
    assert "f1" not in data.feature_columns
    assert "f1" in data.dropped_columns

    kept = _creator(on_untrainable="keep").build(panel)
    assert "f1" in kept.feature_columns
    with pytest.raises(ValueError, match="constant across the train slice"):
        _creator(on_untrainable="raise").build(panel)


# ------------------------------------------------------------------- the panel


def test_a_window_never_straddles_two_tickers():
    """A single global stride over a multi-ticker panel builds windows whose first
    days are one company and last days another. Each ticker has its own level here,
    so a straddle shows up as a jump inside one window."""
    data = _creator().build(_panel(tickers=("AAA", "BBB")))
    for split in ("train", "val", "test"):
        spread = np.ptp(data.X[split], axis=1).max()
        assert spread < 20, "a window spans two tickers' feature levels"
    assert set(np.unique(data.tickers["train"])) == {"AAA", "BBB"}


def test_the_split_is_cut_on_dates_so_every_ticker_shares_a_boundary():
    """A row-index cut would put one date in train for one ticker and in val for
    another, and the two would then share a label period across the boundary."""
    data = _creator().build(_panel(tickers=("AAA", "BBB")))
    for earlier, later in (("train", "val"), ("val", "test")):
        assert max(data.dates[earlier]) < min(data.dates[later])
    for split in ("train", "val", "test"):
        counts = pd.Series(data.tickers[split]).value_counts()
        assert counts.nunique() == 1, "the two tickers got different sample counts"


# ------------------------------------------------------------------ the tensors


def test_the_window_ends_on_its_label_date():
    """The label is the target AT the window's last day, not one day after it."""
    creator = _creator()
    panel = _panel()
    data = creator.build(panel)

    labelled = panel.dropna(subset=["return_5day"]).reset_index(drop=True)
    position = pd.Index(labelled["date"].dt.strftime("%Y-%m-%d")).get_indexer(
        data.dates["test"]
    )
    expected = labelled["return_5day"].to_numpy()[position]
    actual = data.target_scaler.inverse_transform(
        data.y["test"].reshape(-1, 1)
    ).ravel()
    assert np.allclose(actual, expected, atol=1e-6)


def test_the_unlabelled_tail_is_dropped():
    """The last `h` sessions have no complete forward window."""
    data = _creator().build(_panel(horizon=2))
    assert data.rows_unlabelled == 2


def test_the_ticker_array_is_unicode_not_object():
    """`np.save` writes a PICKLED array for dtype object, and every reader here uses
    `allow_pickle=False` — the file would exist and be unreadable, which is how a
    20-ticker panel silently gets scored as one series."""
    data = _creator().build(_panel(tickers=("AAA", "BBB")))
    assert data.tickers["test"].dtype.kind == "U"


# --------------------------------------------------------- the derived-target table


LABELS = ["date", "exchange", "ticker", "return_2day", "return_10day"]


def _with_channels(creator, channels, target="return_2day"):
    """Stand in for `reports/feature_selection/*/outstanding.csv`."""
    creator.selection = lambda *a, **k: pd.DataFrame(
        {"channel": list(channels), "target": [target] * len(channels)}
    )
    return creator


def test_a_rank_table_reads_the_column_it_actually_stores():
    """`rank_5day__final__d20_h5` STORES `return_5day` — a rank's value depends on
    which other names are in the panel, so final_features refuses to freeze one
    (final_features/CONTEXT.md §5). Demanding the name's target made the whole bank
    schema unreachable: the table was fine, the reader was wrong."""
    creator = TrainTestCreator(ticker="test", table="rank_5day__final__d5_h2")
    creator = _with_channels(creator, ["f0", "f1"], target="cs_rank_2day")
    assert creator.target == "rank_5day"
    columns = ["date", "exchange", "ticker", "return_2day", "f0", "f1"]
    assert creator.resolve_target(columns, LABELS) == "return_2day"


def test_no_horizon_string_is_ever_constructed():
    """The old version built `return_{h}day` from the horizon, duplicating
    `final_features._stored_target` in a place that could drift from it. The answer
    now comes from two records: the shortlists say what is a CHANNEL, `pool__targets`
    says what is a LABEL — so a table whose label is named nothing like the horizon
    still resolves."""
    creator = _with_channels(_creator(), ["f0", "f1", "f2", "f3"])
    columns = ["date", "exchange", "ticker", "excess_vs_vnindex", "f0", "f1"]
    labels = ["date", "exchange", "ticker", "excess_vs_vnindex"]
    assert creator.resolve_target(columns, labels) == "excess_vs_vnindex"


def test_a_channel_is_never_mistaken_for_the_label():
    """A feature that happens to share a name with a label column must lose — the
    shortlist named it, so it is a channel."""
    creator = _with_channels(_creator(), ["return_10day", "f0"])
    columns = ["date", "exchange", "ticker", "return_2day", "return_10day", "f0"]
    assert creator.resolve_target(columns, LABELS) == "return_2day"


def test_two_possible_labels_raise_rather_than_one_being_picked():
    creator = _with_channels(_creator(), ["f0"])
    columns = ["date", "exchange", "ticker", "return_2day", "return_10day", "f0"]
    with pytest.raises(ValueError, match="possible label columns"):
        creator.resolve_target(columns, LABELS)


def test_no_label_column_at_all_raises():
    creator = _with_channels(_creator(), ["f0"])
    with pytest.raises(ValueError, match="no column that is both"):
        creator.resolve_target(["date", "exchange", "ticker", "f0"], LABELS)


def test_columns_in_no_shortlist_are_reported_as_stale_not_raised():
    """A table built before the shortlists were last regenerated legitimately holds
    channels no current run names. It is still readable — but it is not what
    `final_features --apply --replace` would build today, and the caller says so.
    The real VCB table has 26 such columns."""
    creator = _with_channels(_creator(), ["f0"])
    columns = ["date", "exchange", "ticker", "return_2day", "f0", "dropped_last_week"]
    assert creator.resolve_target(columns, LABELS) == "return_2day"
    assert creator.stale_channels == ["dropped_last_week"]
