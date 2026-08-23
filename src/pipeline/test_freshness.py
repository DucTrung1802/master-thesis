# src\pipeline\test_freshness.py
"""`pipeline.freshness` — the per-ticker measurement, pinned without a database.

⚠️ **NO DATABASE.** Everything worth pinning here is either a pure reduction of a frame
(`summarise`, `describe`) or a string (`calendar_sql`, `layer_branch_sql`), and both halves
are where the defects live: the cliff threshold decides whether a gate goes red, and the
SQL decides whether the number means anything. The two frames below are this repo's own
history, six weeks apart — see the module docstring for how they were measured.
"""

from __future__ import annotations

import datetime as dt
import sys

import pandas as pd
import pytest

# ⚠️ `pipeline/__init__.py` rebinds the name `stages` to the FUNCTION, shadowing the
# module; `freshness` is not shadowed, but it is imported the same way for symmetry with
# `test_cross_sectional.py` and so the two files fail the same way if that ever changes.
import pipeline.freshness  # noqa: F401

F = sys.modules["pipeline.freshness"]


def frame(rows):
    """`[(ticker, last_date, sessions_behind)]` -> the frame `summarise` reads."""
    return pd.DataFrame(
        [
            {
                "exchange": "HOSE",
                "ticker": ticker,
                "last_date": dt.date.fromisoformat(last),
                "sessions_behind": behind,
            }
            for ticker, last, behind in rows
        ]
    )


def spread(n_current: int, groups):
    """A frame with `n_current` fresh tickers and `groups` of `(date, n)` stale ones."""
    rows = [(f"CUR{i}", "2026-08-21", 0) for i in range(n_current)]
    for date, count in groups:
        rows += [(f"S{date[-2:]}{i}", date, 10) for i in range(count)]
    return frame(rows)


# ------------------------------------------------------------------ the two regimes


def test_the_frz1_freeze_is_a_cliff():
    """757 of 781 stale with 599 on one date — the state that hid for two months."""
    got = F.summarise(spread(24, [("2026-06-26", 599), ("2026-06-25", 152), ("2026-07-08", 6)]))
    assert got["n_stale"] == 757
    assert got["cliff_n"] == 599
    assert got["is_cliff"] is True


def test_the_post_rescrape_stragglers_are_not_a_cliff():
    """13 of 784 over SEVEN dates, the largest group 5 — delistings, measured 2026-08-23.

    ⚠️ This is the case that corrected the first draft: an absolute floor of 5 tickers
    called this a scrape failure. It is 0.6 % of the universe, against the 77 % above.
    """
    got = F.summarise(
        spread(
            771,
            [
                ("2026-05-21", 1), ("2026-06-23", 1), ("2026-06-26", 1),
                ("2026-07-06", 2), ("2026-07-08", 5), ("2026-07-14", 2),
                ("2026-07-30", 1),
            ],
        )
    )
    assert got["n_tickers"] == 784 and got["n_stale"] == 13
    assert got["cliff_n"] == 5
    assert got["is_cliff"] is False


def test_a_frozen_single_ticker_schema_is_a_cliff():
    """⚠️ Why the threshold is a SHARE: one stale name out of one is the whole story."""
    got = F.summarise(frame([("VNM", "2026-06-26", 40)]))
    assert got["cliff_share"] == 1.0
    assert got["is_cliff"] is True


def test_a_fresh_table_reports_no_cliff_date():
    got = F.summarise(frame([("VCB", "2026-08-21", 0), ("HPG", "2026-08-21", 0)]))
    assert got["n_current"] == 2 and got["n_stale"] == 0
    assert got["cliff_date"] is None and got["is_cliff"] is False


def test_an_empty_frame_is_not_a_pass():
    """An absent measurement is absent (§5 rule 2) — never a silent all-clear."""
    got = F.summarise(pd.DataFrame(columns=["ticker", "last_date", "sessions_behind"]))
    assert got["n_tickers"] == 0 and got["n_current"] == 0
    assert got["is_cliff"] is False


def test_max_sessions_behind_is_the_worst_ticker_not_the_table():
    got = F.summarise(frame([("A", "2026-08-21", 0), ("B", "2026-05-21", 66)]))
    assert got["max_sessions_behind"] == 66


# ---------------------------------------------------------------------- the console


@pytest.mark.parametrize(
    "summary",
    [
        F.summarise(frame([("VCB", "2026-08-21", 0)])),
        F.summarise(frame([("VNM", "2026-06-26", 40)])),
        F.summarise(spread(771, [("2026-07-08", 5), ("2026-07-30", 1)])),
        F.summarise(spread(24, [("2026-06-26", 599)])),
    ],
)
def test_describe_is_ascii(summary):
    """⚠️ §5 rule 18: this prints to a cp1252 console, and one glyph kills a whole plan."""
    summary["describe"] = F.describe(summary)
    summary["describe"].encode("cp1252")
    assert summary["describe"] == summary["describe"].encode("ascii").decode("ascii")


def test_describe_names_the_cliff_date():
    text = F.describe(F.summarise(spread(24, [("2026-06-26", 599)])))
    assert "CLIFF" in text and "2026-06-26" in text


def test_describe_calls_a_wholly_frozen_table_frozen_not_a_cliff():
    """A cliff is "some names"; 100 % is "the table". They read differently on a console."""
    text = F.describe(F.summarise(frame([("VNM", "2026-06-26", 40)])))
    assert "FROZEN" in text and "CLIFF" not in text


# --------------------------------------------------------------------------- the SQL


def test_the_calendar_comes_from_the_spine_and_nothing_else():
    """⚠️ The one design decision: a table aged against its OWN dates is always current."""
    sql = F.calendar_sql()
    assert f"{F.SPINE_SCHEMA}.{F.SPINE_TABLE}" in sql
    assert "ROW_NUMBER() OVER (ORDER BY date DESC)" in sql


# ------------------------------------------------------------------ functions, not views


@pytest.mark.parametrize(
    "builder",
    [F.layers_function_sql, F.calendar_function_sql, F.freshness_function_sql],
)
def test_nothing_installed_is_a_view(builder):
    """⚠️ The defect that cost a rebuild: a view records a dependency on its tables, so
    `DROP TABLE` — which every builder here opens with — fails while it exists."""
    sql = builder()
    assert "CREATE OR REPLACE FUNCTION" in sql
    assert "CREATE VIEW" not in sql and "CREATE OR REPLACE VIEW" not in sql


def test_the_layer_registry_is_a_query_not_a_frozen_list():
    """A schema built after install must appear on its own — no re-install."""
    sql = F.layers_function_sql()
    assert "information_schema.tables" in sql
    assert "unified" in sql
    for layer, schema, table in F.CORE_LAYERS:
        assert f"'{layer}'::text" in sql and f"'{schema}'::text" in sql


def test_the_freshness_function_takes_the_layer_as_an_argument():
    """Filtering by argument touches ONE table; a UNION ALL view relied on the planner."""
    sql = F.freshness_function_sql()
    assert "layer_filter text DEFAULT NULL" in sql
    assert "WHERE layer_filter IS NULL OR f.layer = layer_filter" in sql


def test_a_missing_table_is_skipped_rather_than_raised_on():
    """A rebuild drops its table before writing one, and that is when this gets read."""
    assert "CONTINUE WHEN to_regclass(" in F.freshness_function_sql()


def test_the_function_inlines_the_calendar_instead_of_calling_it_per_ticker():
    sql = F.freshness_function_sql()
    assert "WITH cal AS (" in sql
    assert "LEFT JOIN cal c ON c.date = p.last_date" in sql


@pytest.mark.parametrize(
    "bad",
    ["stocks; DROP TABLE silver_schema.stocks_basic", "a b", "", "1abc", "sch'ema"],
)
def test_identifiers_are_validated_because_they_cannot_be_bound(bad):
    """A schema name is interpolated, not parameterised — `unified_reader` does the same."""
    with pytest.raises(ValueError):
        F.per_ticker_sql(bad, "pool__basic")
    with pytest.raises(ValueError):
        F.per_ticker_sql("silver_schema", bad)


def test_per_ticker_sql_groups_by_the_ticker_not_the_table():
    sql = F.per_ticker_sql("unified_schema_all", "pool__basic")
    assert "GROUP BY 1, 2" in sql
    assert "MAX(date) AS last_date" in sql
    # ⚠️ standalone on purpose: `status_data` must not need the views installed.
    assert f"{F.HEALTH_SCHEMA}.{F.CALENDAR_FUNCTION}" not in sql
