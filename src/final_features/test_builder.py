# src\final_features\test_builder.py
"""`UNI-1`: the universe a cross-sectional label was ranked across must travel.

⚠️ **This file exists because the package had no tests and the defect it guards was
invisible for exactly that reason.** `cs_rank_{h}day` is a rank WITHIN a chosen set of
names; the run folder has recorded that set since 2026-08-18 and this module ignored it,
so a table built from a 150-name shortlist would have been built over
`unified_schema_all`'s 781 and the model trained on a label the selection never scored.

    python -m pytest final_features/test_builder.py -q
"""

import json
import os

import pandas as pd
import pytest

from feature_selection.contract import SETUP_KEYS
from final_features.builder import (
    FinalTablePlan,
    _universe_digest,
    build_sql,
    plan_from_reports,
)

UNIVERSE = ["VCB", "SSI", "HPG"]


def _setup() -> dict:
    setup = {k: "x" for k in SETUP_KEYS}
    setup["lookback_d"], setup["horizon_h"] = 20, 20
    return setup


def _plan(**kwargs) -> FinalTablePlan:
    base = dict(
        schema="unified_schema_all",
        table="rank_20day__final__d20_h20",
        target="cs_rank_20day",
        stored_target="return_20day",
        setup=_setup(),
        columns_by_table={"pool__basic": ["drv_clv"]},
        runs=["r1"],
        evidence={"cleared_p95": 1},
    )
    base.update(kwargs)
    return FinalTablePlan(**base)


# ------------------------------------------------------------------ the SQL


def test_the_build_is_restricted_to_the_universe():
    """⚠️ Without this the build runs over every name in the source pools — 781 on
    `unified_schema_all` — and `train_test_creator` re-ranks over 781 while the
    shortlist above it was chosen over 150."""
    sql = build_sql(_plan(universe=UNIVERSE))
    assert "WHERE  base.ticker IN ('HPG', 'SSI', 'VCB')" in sql
    assert sql.index("WHERE") < sql.index("ORDER BY")


def test_a_plan_without_a_universe_is_untouched():
    """The regression guard. Every single-ticker chain in the repo has no universe,
    and `unified_schema_bank` IS its universe — neither may grow a WHERE clause."""
    assert "WHERE" not in build_sql(_plan(universe=[]))


def test_a_ticker_that_is_not_an_identifier_raises():
    """The names arrive from a JSON artefact and are interpolated into DDL."""
    with pytest.raises(ValueError):
        build_sql(_plan(universe=["VCB", "'; DROP TABLE x; --"]))


def test_the_comment_names_the_population_not_just_its_size():
    """A rank over 150 liquid names and a rank over 150 different ones are two
    labels; a table recording only "150" cannot say which it holds."""
    comment = _plan(universe=UNIVERSE, target_derived=True).comment()
    assert "UNIVERSE: restricted to the 3 names" in comment
    assert _universe_digest(UNIVERSE) in comment
    assert "re-ranks it at dataset build (RNK-1)" in comment


def test_the_digest_is_order_insensitive_and_content_sensitive():
    assert _universe_digest(["A", "B"]) == _universe_digest(["B", "A"])
    assert _universe_digest(["A", "B"]) != _universe_digest(["A", "C"])


# --------------------------------------------------- the handoff off disk


def _run_folder(root, name, universe, target="cs_rank_20day"):
    folder = os.path.join(root, name)
    os.makedirs(folder)
    pd.DataFrame(
        {
            "channel": ["drv_clv"],
            "source_table": ["pool__basic"],
            "schema": ["unified_schema_all"],
            "run_id": [name],
            "target": [target],
            "evidence": ["cleared_p95"],
            "lookback_d": [20],
            "horizon_h": [20],
            # ⚠️ `CUT_KEYS` — stamped into every shortlist since the measured cut
            # replaced `max_features`. `_read_outstanding` raises without them rather
            # than reading a row it cannot describe.
            "cut_fdr_q": [0.1],
            "cut_corr_threshold": [0.9],
        }
    ).to_csv(os.path.join(folder, "outstanding.csv"), index=False)
    meta = {"setup": _setup(), "input": {"universe": universe}}
    with open(os.path.join(folder, "metadata.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh)
    return folder


def test_the_universe_reaches_the_plan_from_the_run_folder(tmp_path):
    """`metadata.json → input.universe` is where a selection records what it ranked
    across, and this module ignored it until 2026-08-18."""
    root = str(tmp_path)
    _run_folder(root, "2026-08-18_000000__all__basic__cs_rank_20day", UNIVERSE)
    plans = plan_from_reports(root=root)
    assert len(plans) == 1
    assert plans[0].universe == sorted(UNIVERSE)


def test_two_universes_refuse_to_become_one_table(tmp_path):
    """⚠️ **THE POINT OF GROUPING ON IT.** Same target, same knobs, different
    populations: unioning them would produce a table whose label is a rank across a
    population neither run used. It must raise rather than pick one."""
    root = str(tmp_path)
    _run_folder(root, "2026-08-18_000001__all__basic__cs_rank_20day", UNIVERSE)
    _run_folder(root, "2026-08-18_000002__all__basic__cs_rank_20day", ["ACB", "MBB"])
    with pytest.raises(ValueError, match="both want"):
        plan_from_reports(root=root)
