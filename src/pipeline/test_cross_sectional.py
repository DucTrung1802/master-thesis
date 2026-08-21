# src\pipeline\test_cross_sectional.py
"""The cross-sectional chain through `pipeline` — stages 3-4 do not EXIST here.

⚠️ `RUNBOOK.md` §8 rule 1 makes `python -m pipeline` the gate on quoting any number, and
until 2026-08-21 §3a had to warn readers OFF that gate for the chain producing every
headline in CLAUDE.md §6-0. Two things made that necessary and both are pinned below:
`shortlist_pool` reported *"would run"* for a pool nothing can ever select over (`CSP-1`),
and `selection_2` named a command that RAISES for a `cs_` target.

⚠️ **NO DATABASE.** Every test here works off `outstanding.csv` files on disk, which is
where the answer genuinely lives — the table NAME cannot carry it, because
`final_features` drops the `cs_` prefix when it names a table.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

# ⚠️ `pipeline/__init__.py` rebinds the name `stages` to the FUNCTION, shadowing the
# module, so a plain `import pipeline.stages as S` hands back the function.
import pipeline.stages  # noqa: F401
S = sys.modules["pipeline.stages"]

CS_TABLE = "rank_20day__final__d20_h20"
SERIES_TABLE = "return_5day__final__d20_h5"


def _run_folder(root, name, schema, target, lookback, horizon, source="pool__basic"):
    directory = os.path.join(root, name)
    os.makedirs(directory, exist_ok=True)
    pd.DataFrame([{
        "channel": "drv_order_vol_imb", "source_table": source, "schema": schema,
        "target": target, "lookback_d": lookback, "horizon_h": horizon,
    }]).to_csv(os.path.join(directory, "outstanding.csv"), index=False)
    return directory


# ── the rule itself ───────────────────────────────────────────────────────────

def test_a_cs_rank_target_is_detected_from_the_SHORTLISTS(tmp_path):
    """⚠️ The table name says `rank_20day`; only `outstanding.csv` says `cs_rank_20day`."""
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_all", "cs_rank_20day", 20, 20)

    assert S.selected_for("all", CS_TABLE, root) == "cs_rank_20day"
    assert S.is_cross_sectional("all", CS_TABLE, root) is True


def test_a_single_series_target_is_not(tmp_path):
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_vcb", "return_5day", 20, 5)

    assert S.is_cross_sectional("vcb", SERIES_TABLE, root) is False


def test_a_MIXED_target_list_is_not_treated_as_a_rank(tmp_path):
    """The same rule `TrainTestCreator._is_ranked` applies: a mixed list is left alone
    rather than guessed at, even when one of its members is a rank."""
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_all", "cs_rank_20day", 20, 20)
    _run_folder(root, "run_b", "unified_schema_all", "return_20day", 20, 20)

    assert "," in S.selected_for("all", CS_TABLE, root)
    assert S.is_cross_sectional("all", CS_TABLE, root) is False


def test_the_horizon_and_lookback_scope_the_answer(tmp_path):
    """A `d20_h10` run must not decide what a `d20_h20` chain is."""
    root = str(tmp_path)
    _run_folder(root, "run_h10", "unified_schema_all", "cs_rank_10day", 20, 10)

    assert S.selected_for("all", CS_TABLE, root) == ""
    assert S.is_cross_sectional("all", CS_TABLE, root) is False


def test_the_schema_scopes_the_answer(tmp_path):
    """A `vcb` run must not decide what an `all` chain is."""
    root = str(tmp_path)
    _run_folder(root, "run_vcb", "unified_schema_vcb", "cs_rank_20day", 20, 20)

    assert S.is_cross_sectional("all", CS_TABLE, root) is False


def test_no_shortlist_at_all_is_a_STATE_not_an_error(tmp_path):
    assert S.selected_for("all", CS_TABLE, str(tmp_path)) == ""
    assert S.is_cross_sectional("all", CS_TABLE, str(tmp_path)) is False


# ── what the two stages then report ───────────────────────────────────────────

def test_shortlist_pool_reports_n_a_and_is_READY(tmp_path):
    """⚠️ `ready=True` is load-bearing: `--apply` skips a ready stage, and a `False`
    here is what would have built the junk pool."""
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_all", "cs_rank_20day", 20, 20)

    state = S.status_shortlist_pool("all", CS_TABLE, root, None)
    assert state.ready is True
    assert "n/a" in state.detail and "CSP-1" in state.detail


def test_selection_2_reports_n_a_rather_than_an_impossible_command(tmp_path):
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_all", "cs_rank_20day", 20, 20)

    state = S.status_selection_2(root, CS_TABLE, None, "all")
    assert state.ready is True
    assert "n/a" in state.detail
    # the old text named a command that RAISES for a `cs_` target
    assert "MANUAL" not in state.detail
    assert "feature_selection.run --pools" not in state.detail


def test_apply_shortlist_pool_REFUSES_on_a_cross_sectional_chain(tmp_path):
    """⚠️ A second guard, because `--only shortlist_pool` forces a stage regardless of
    its `ready` — so the status check alone would still let the junk pool be built."""
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_all", "cs_rank_20day", 20, 20)

    with pytest.raises(ValueError, match="CROSS-SECTIONAL"):
        S.apply_shortlist_pool(root, None, "all", CS_TABLE)


def test_the_single_series_chain_still_gets_the_REAL_stages(tmp_path, monkeypatch):
    """The n/a path must not swallow the chain the two stages were built for."""
    root = str(tmp_path)
    _run_folder(root, "run_a", "unified_schema_vcb", "return_5day", 20, 5)

    called = {}

    def fake(name, ticker, table, root_, scope, shape):
        called["table"] = table
        return S.StageState(name, True, "stub")

    monkeypatch.setattr(S, "_status_built_table", fake)
    S.status_shortlist_pool("vcb", SERIES_TABLE, root, None)
    assert called["table"].startswith("pool__shortlist__")


# ── the two new stages are wired in ───────────────────────────────────────────

def test_backtest_and_walkforward_are_stages_now():
    """⚠️ They produce every headline in CLAUDE.md §6-0 and were invisible to the gate."""
    names = [stage.name for stage in S.stages()]
    assert names[-2:] == ["backtest", "walkforward"]
    assert len(names) == 10


def test_walkforward_is_MANUAL_and_has_no_apply():
    """A sweep is ~35 GPU minutes and ten run folders; `--apply` must not spend that."""
    stage = next(s for s in S.stages() if s.name == "walkforward")
    assert stage.manual is True
    assert stage.apply is None


def test_backtest_reports_a_missing_null_as_ABSENT(tmp_path, monkeypatch):
    """§5 rule 2 — a costed Sharpe with no bar is descriptive, never a pass."""
    run_dir = tmp_path / "run"
    os.makedirs(run_dir / "results")
    pd.DataFrame([{"bps": 30, "sharpe": 1.9}]).to_csv(
        run_dir / "results" / "backtest_test.csv", index=False)

    monkeypatch.setattr(S, "_latest_run", lambda config=None: str(run_dir))
    state = S.status_backtest("anything")
    assert state.ready is True
    assert "NO NULL" in state.detail

    pd.DataFrame([{"bps": 30}]).to_csv(
        run_dir / "results" / "backtest_null_test.csv", index=False)
    assert "nulled" in S.status_backtest("anything").detail


def test_walkforward_finds_a_track_by_its_TABLE_not_by_a_fixed_path(tmp_path):
    from walkforward import manifest as W

    results = tmp_path / "results"
    track = results / "some_hand_chosen_name"
    os.makedirs(track)
    W.write(str(track), W.identity(
        ticker="all", table=CS_TABLE, first_test="2017-01-01", step_months=12,
        val_months=12, scale_target=True, rank_min_width=5))
    pd.DataFrame([{"fold": "oos2017", "sharpe@30": 2.0}]).to_csv(
        track / "per_fold.csv", index=False)

    state = S.status_walkforward("all", CS_TABLE, out=str(results))
    assert state.ready is True
    assert "some_hand_chosen_name" in state.detail

    # a different table must NOT match it
    assert S.status_walkforward("all", "rank_10day__final__d20_h10",
                                out=str(results)).ready is False


def test_a_swept_but_UNSCORED_track_is_not_ready(tmp_path):
    """`per_fold.csv` is where the fold series lives, and the SHAPE is PRF-1's point."""
    from walkforward import manifest as W

    results = tmp_path / "results"
    track = results / "swept"
    os.makedirs(track)
    W.write(str(track), W.identity(
        ticker="all", table=CS_TABLE, first_test="2017-01-01", step_months=12,
        val_months=12, scale_target=True, rank_min_width=5))

    state = S.status_walkforward("all", CS_TABLE, out=str(results))
    assert state.ready is False
    assert "NOT scored" in state.detail
