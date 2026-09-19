"""`utils.event_target` — the one definition of the binary event label. No database."""

import pytest

from utils import event_target as E


def test_column_names_encode_gain_horizon_and_rule():
    assert E.EventTarget(5.0, 5, "close").column == "up_5pct_5day"
    assert E.EventTarget(5.0, 5, "any").column == "upany_5pct_5day"
    assert E.EventTarget(2.5, 10, "close").column == "up_2p5pct_10day"


@pytest.mark.parametrize("column", ["up_5pct_5day", "upany_2p5pct_10day", "up_12pct_20day"])
def test_parse_round_trips(column):
    assert E.parse(column).column == column


@pytest.mark.parametrize("column", ["return_5day", "close_adjust_5day", "up_5pct", "cs_rank_5day"])
def test_other_labels_are_not_event_columns(column):
    assert not E.is_event_column(column)


def test_the_multiplier_is_an_exact_decimal_literal():
    # A float literal would put 20,000 -> 21,000 (exactly +5 %) on the wrong side.
    assert E.EventTarget(5.0, 5).multiplier == "1.05"
    assert E.EventTarget(2.5, 5).multiplier == "1.025"


def test_sql_is_null_exactly_where_the_forward_close_is_missing():
    for rule in ("close", "any"):
        sql = E.EventTarget(5.0, 5, rule).sql("px", "w")
        assert "LEAD(px, 5) OVER w IS NULL" in sql
    assert "ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING" in E.EventTarget(5.0, 5, "any").sql()


def test_invalid_parameters_raise():
    with pytest.raises(ValueError):
        E.EventTarget(0.0, 5)
    with pytest.raises(ValueError):
        E.EventTarget(5.0, 0)
    with pytest.raises(ValueError):
        E.EventTarget(5.0, 5, "touch")


def test_the_default_event_is_carried_first():
    assert E.EVENT_TARGETS[0] == E.DEFAULT_EVENT
    assert len({t.column for t in E.EVENT_TARGETS}) == len(E.EVENT_TARGETS)


def test_selection_refuses_every_event_column_as_a_feature():
    from feature_selection.run import ALL_TARGETS, is_label

    for target in E.EVENT_TARGETS:
        assert target.column in ALL_TARGETS
    # An event column from an OLDER parameter set is refused by pattern.
    assert is_label("up_7pct_3day")
    assert not is_label("drv_ret_1d")


def test_a_binary_event_dataset_is_never_target_scaled():
    from train_test_creator.dataset import TrainTestCreator

    creator = TrainTestCreator(ticker="vcb", table="up_5pct_5day__final__d20_h5")
    assert creator.scale_target is False and creator.task == "classification"
    with pytest.raises(ValueError):
        TrainTestCreator(ticker="vcb", table="up_5pct_5day__final__d20_h5", scale_target=True)
    with pytest.raises(ValueError):
        # The table's h must be the event's own horizon, or the purge is wrong.
        TrainTestCreator(ticker="vcb", table="up_5pct_5day__final__d20_h10")
    assert TrainTestCreator(ticker="vcb", table="return_5day__final__d20_h5").scale_target is True


def test_the_hold_rule_sells_one_session_before_the_open_rule():
    """⚠️ `h` counts SESSIONS HELD in `hold` and sessions AFTER THE ENTRY in `open`.

    Signal Friday: `hold` buys Monday's open and sells Friday's close (one week, 5 sessions);
    `open` buys the same Monday and sells the FOLLOWING Monday's close (6 sessions, two
    weekends). Measured on PNJ 2025-01-10 the extra session was +0.96 pp of a +2.55 % move.
    """
    hold, opn = E.EventTarget(5.0, 5, "hold"), E.EventTarget(5.0, 5, "open")
    assert (hold.column, opn.column) == ("uphold_5pct_5day", "upopen_5pct_5day")
    assert (hold.exit_offset, opn.exit_offset) == (5, 6)
    assert (hold.held_sessions, opn.held_sessions) == (5, 6)
    assert (hold.unlabelled, opn.unlabelled) == (5, 6)
    assert hold.enters_at_open and opn.enters_at_open
    assert not E.EventTarget(5.0, 5, "close").enters_at_open
    assert (hold.aux_return, opn.aux_return) == ("return_hold_5day", "return_open_5day")
    # both buy the SAME session and differ only in the exit
    assert "LEAD(op, 1)" in hold.sql() and "LEAD(op, 1)" in opn.sql()
    assert "LEAD(px, 5)" in hold.sql() and "LEAD(px, 6)" in opn.sql()


def test_every_rule_is_carried_and_round_trips():
    carried = {t.column for t in E.EVENT_TARGETS}
    assert carried == {"uphold_5pct_5day", "upopen_5pct_5day", "up_5pct_5day", "upany_5pct_5day"}
    for column in carried:
        assert E.parse(column).column == column          # `uphold` is not parsed as `up` + "hold"
    assert E.parse("uphold_5pct_5day").rule == "hold"
