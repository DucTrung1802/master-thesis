# src\orchestration\preprocessor\test_filters.py
"""Tests for the FILTER layer's registry and SQL generation — **no database needed**.

That is the whole point of splitting `filters.py` out of `preprocessor.py`: the half
that decides WHAT a screen means is pure, so it can be pinned here, while the half that
runs it needs PostgreSQL and is exercised by materialising the asset.

    python -m pytest src/orchestration/preprocessor/test_filters.py -q

⚠️ **THE ASSERTIONS THAT MATTER ARE THE NEGATIVE ONES.** A screen that builds a
plausible table from a wrong definition is this layer's characteristic failure — 781
rows, a `passes` column, nothing raised, and a universe that means something other than
what it says. So most of what is pinned below is refusal: an undeclared parameter, a
threshold that reaches SQL unbound, a name that could collide with a listing, a NULL
that could reach `passes`.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

_SRC = str(Path(__file__).resolve().parents[2])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from orchestration.preprocessor import filters as F
from utils.exceptions import PipelineError


# ── the registry as shipped ──────────────────────────────────────────────────
def test_every_screen_resolves_and_names_a_legal_schema():
    """A screen must be able to name `unified_schema_<slug>`, or it is unbuildable."""
    from orchestration.preprocessor.preprocessor import DataPreprocessor

    for name, scr in F.SCREENS.items():
        assert scr.resolve(), f"{name} resolved to no conditions"
        assert DataPreprocessor.UNIFIED_TICKER_PATTERN.match(name)
        assert scr.qualified_table.startswith("filter_schema.universe__")


def test_screen_names_cannot_collide_with_a_ticker():
    """Every VN ticker is exactly 3 characters; a screen must be 4 or more.

    ⚠️ This is the structural version of the argument `UNIFIED_UNIVERSE` makes one name
    at a time ("no VN ticker is `ALL`, checked"). A 3-character screen would take over
    a real company's partition silently.
    """
    for name in F.SCREENS:
        assert len(name) >= 4
    with pytest.raises(PipelineError, match="3, so a screen must be 4"):
        F.Screen("VNM", "collides with a listing", ("close_raw_min_10k",))


def test_every_screen_is_wired_into_the_unified_member_filters():
    """A screen with no `UNIFIED_MEMBER_FILTERS` entry would fall through to
    `ticker = %s` and build a real, empty, correctly-typed table."""
    from orchestration.preprocessor.preprocessor import DataPreprocessor

    for name, scr in F.SCREENS.items():
        predicate, params = DataPreprocessor.UNIFIED_MEMBER_FILTERS[name]
        assert scr.qualified_table in predicate
        assert "WHERE passes" in predicate
        assert params == ()


def test_the_requested_screen_means_what_it_says():
    """PRICE10K is close_raw >= 10,000 VND from 2026-01-01, and MIN not MEAN."""
    condition = F.CONDITIONS["close_raw_min_10k"]
    assert condition.metric == "MIN(close_raw)"
    assert (condition.op, condition.threshold) == (">=", 10_000.0)
    assert condition.params == {"start": date(2026, 1, 1)}
    assert condition.on_missing == "reject"
    assert F.SCREENS["PRICE10K"].conditions == ("close_raw_min_10k",)


# ── validation refuses the things that fail silently ─────────────────────────
def test_undeclared_parameter_is_refused():
    with pytest.raises(PipelineError, match="undeclared parameter"):
        F.Condition(
            name="x", description="", source="silver_schema.stocks_basic",
            metric="MIN(close_raw)", op=">=", threshold=1.0,
            where="date >= %(start)s",  # never declared
        )


def test_dangling_parameter_is_refused():
    """A declared-but-unused param is a renamed placeholder, i.e. a window that is
    still whatever it used to be — the failure mode with no symptom."""
    with pytest.raises(PipelineError, match="that no expression uses"):
        F.Condition(
            name="x", description="", source="silver_schema.stocks_basic",
            metric="MIN(close_raw)", op=">=", threshold=1.0,
            where="date >= %(start)s",
            params={"start": date(2026, 1, 1), "cutoff": date(2020, 1, 1)},
        )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"op": "; DROP TABLE"}, "is not one of"),
        ({"source": "stocks_basic"}, "qualified"),
        ({"name": "Bad Name"}, "column name"),
        ({"on_missing": "maybe"}, "on_missing"),
        ({"latest_by": "date DESC"}, "plain column name"),
        ({"metric": "  "}, "metric is empty"),
    ],
)
def test_malformed_conditions_are_refused(kwargs, match):
    base = dict(
        name="x", description="", source="silver_schema.stocks_basic",
        metric="MIN(close_raw)", op=">=", threshold=1.0,
    )
    with pytest.raises(PipelineError, match=match):
        F.Condition(**{**base, **kwargs})


def test_duplicate_condition_in_one_screen_is_refused():
    with pytest.raises(PipelineError, match="twice"):
        F.Screen("DOUBLE", "", ("close_raw_min_10k", "close_raw_min_10k"))


def test_empty_screen_is_refused():
    with pytest.raises(PipelineError, match="no conditions"):
        F.Screen("EMPTY", "", ())


def test_unknown_condition_is_refused_at_resolve_time():
    with pytest.raises(PipelineError, match="unknown condition"):
        F.Screen("GHOST", "", ("no_such_condition",)).resolve()


def test_registering_the_same_condition_twice_is_refused():
    with pytest.raises(PipelineError, match="already defined"):
        F.register(F.CONDITIONS["close_raw_min_10k"])


# ── SQL generation ───────────────────────────────────────────────────────────
def test_no_threshold_is_interpolated():
    """Every threshold and every window value reaches PostgreSQL BOUND.

    ⚠️ The literal `10000` must not appear in the statement text at all. `metric` and
    `where` are raw SQL by design; a VALUE never is.
    """
    sql, params = F.build_universe_sql(F.SCREENS["PRICE10K"])
    assert "10000" not in sql and "10000.0" not in sql
    assert params["c0__threshold"] == 10_000.0
    assert params["c0__start"] == date(2026, 1, 1)
    assert "2026-01-01" not in sql


def test_parameters_are_namespaced_per_condition():
    """Two conditions both using `start` for DIFFERENT windows must not collide.

    `LIQUID`'s four conditions share the name `start`/`cutoff`; without the `c<i>__`
    prefix the last one written would silently set the window for all of them.
    """
    sql, params = F.build_universe_sql(F.SCREENS["LIQUID"])
    starts = {k: v for k, v in params.items() if k.endswith("__start")}
    assert len(starts) == 3, starts  # three of the four take a `start`
    assert all(k.startswith("c") for k in params)
    assert "%(start)s" not in sql


def test_every_pass_expression_is_total():
    """`passes` is an AND over these, and a NULL there reads as 'not selected' while
    meaning 'not known'. Each expression must guard its own NULL."""
    for scr in F.SCREENS.values():
        for i, condition in enumerate(scr.resolve()):
            expression = F._pass_expression(condition, i)
            guard = "IS NULL OR" if condition.on_missing == "keep" else "IS NOT NULL AND"
            assert guard in expression, (condition.name, expression)


def test_on_missing_keep_and_reject_differ_in_the_right_direction():
    reject = F._pass_expression(F.CONDITIONS["close_raw_min_10k"], 0)
    keep = F._pass_expression(F.CONDITIONS["debt_to_equity_max_12"], 0)
    assert "c0.value IS NOT NULL AND" in reject
    assert "c0.value IS NULL OR" in keep


def test_latest_mode_generates_its_own_not_null_guard():
    """'The latest row' and 'the latest row that reported this' are different questions
    and only the second is ever what a fundamental screen means."""
    cte, _ = F._cte(F.CONDITIONS["debt_to_equity_max_12"], 0)
    assert "DISTINCT ON (exchange, ticker)" in cte
    assert ") IS NOT NULL" in cte
    assert "ORDER BY exchange, ticker, date DESC" in cte


def test_aggregate_mode_groups_by_the_grain():
    cte, _ = F._cte(F.CONDITIONS["close_raw_min_10k"], 0)
    assert "GROUP BY exchange, ticker" in cte
    assert "DISTINCT ON" not in cte


def test_every_candidate_is_written_not_only_survivors():
    """The audit is the point: 781 rows with their measurements, not 480 winners."""
    sql, _ = F.build_universe_sql(F.SCREENS["QUALITY"])
    assert f"candidates AS (SELECT DISTINCT exchange, ticker FROM {F.CANDIDATE_SOURCE})" in sql
    assert sql.count("LEFT JOIN") == len(F.SCREENS["QUALITY"].conditions)
    assert "first_failed" in sql
    # ⚠️ NOT an INNER join and NOT a WHERE — either would drop the rejected rows and
    # with them every answer to "why is this ticker out".
    assert " INNER JOIN " not in sql
    assert " WHERE passes" not in sql


def test_first_failed_arms_are_in_screen_order():
    scr = F.SCREENS["QUALITY"]
    sql, _ = F.build_universe_sql(scr)
    case = sql[sql.index("CASE WHEN NOT") :]
    positions = [case.index(f"'{name}'") for name in scr.conditions]
    assert positions == sorted(positions)


def test_every_condition_contributes_two_columns_and_one_conjunct():
    """One verdict, written THREE TIMES from ONE string.

    `pass__<cond>`, the `passes` conjunction and the `first_failed` CASE arm are the
    same generated expression reused verbatim — which is what makes it impossible for
    the audit column, the flag and the membership to disagree about a single name. A
    second implementation of any of the three is how they would.
    """
    for scr in F.SCREENS.values():
        sql, params = F.build_universe_sql(scr)
        assert sql.count(") AS passes") == 1
        for i, condition in enumerate(scr.resolve()):
            assert sql.count(f'AS "{condition.value_column}"') == 1
            assert sql.count(f'AS "{condition.pass_column}"') == 1
            assert sql.count(F._pass_expression(condition, i)) == 3
        assert len([k for k in params if k.endswith("__threshold")]) == len(scr.conditions)


# ── provenance ───────────────────────────────────────────────────────────────
def test_comment_round_trips_and_ignores_its_own_timestamp():
    """`built_at` moves every materialisation and says nothing about the definition."""
    scr = F.SCREENS["LIQUID"]
    import time

    first = F.parse_comment(F.build_comment(scr))
    time.sleep(1.01)
    second = F.parse_comment(F.build_comment(scr))
    assert first["built_at"] != second["built_at"]
    assert F.definition_fingerprint(first) == F.definition_fingerprint(second)


def test_comment_carries_every_window_and_the_not_point_in_time_warning():
    """The windows are the only record of WHICH data chose the basket, and the
    look-ahead warning is the one thing a reader must not have to find in a docstring."""
    payload = F.parse_comment(F.build_comment(F.SCREENS["QUALITY"]))
    assert "not_point_in_time" in payload
    names = {c["name"] for c in payload["conditions"]}
    assert names == set(F.SCREENS["QUALITY"].conditions)
    for entry in payload["conditions"]:
        assert entry["window"]
        assert entry["on_missing"] in F.ON_MISSING


def test_window_rendering_never_builds_sql():
    """`window()` inlines values for DISPLAY; the statement must still bind them."""
    condition = F.CONDITIONS["close_raw_min_10k"]
    assert "2026" in condition.window()
    sql, _ = F.build_universe_sql(F.SCREENS["PRICE10K"])
    assert condition.window() not in sql


# ── the seam into the unified layer ──────────────────────────────────────────
def test_member_predicate_binds_nothing_and_reads_the_table():
    """⚠️ Zero parameters is the case that broke `_ingest_unified_pool_basic`'s scope
    string on 2026-08-22 — it assumed exactly one. Pinned here and there."""
    for name, scr in F.SCREENS.items():
        predicate, params = F.member_predicate(name)
        assert params == ()
        assert scr.qualified_table in predicate
    from orchestration.preprocessor.preprocessor import DataPreprocessor

    describe = DataPreprocessor._helper_unified_describe_predicate
    assert describe(*F.member_predicate("PRICE10K"))  # must not raise
    assert describe("industry_code = %s", ("401010",)) == "industry_code = '401010'"


def test_unknown_screen_names_the_ones_that_exist():
    with pytest.raises(PipelineError, match="Unknown screen"):
        F.screen("NOPE")
    assert F.is_screen("price10k") and not F.is_screen("VCB")
