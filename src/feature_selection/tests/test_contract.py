# src\feature_selection\tests\test_contract.py
"""The `feature_selection` ↔ `final_features` handoff — and issue **MTH-1**.

MTH-1 was this: the default ensemble narrowed from six rankers to three on
2026-08-16, which changes which channels a run keeps, but `methods` was not in
`contract.SETUP_KEYS` — so a three-ranker run and a six-ranker run were grouped
into ONE `__final__` table with nothing on disk recording the difference. It was
left out because every run archived before the change records no `methods` at
all, and an absent SETUP_KEY raises.

These tests pin BOTH halves: that the key now groups, and that adding it did not
orphan the 19 archived runs that predate it.
"""

import pandas as pd
import pytest

from feature_selection import contract
from feature_selection.selector import ALL_METHODS, METHODS


# --------------------------------------------------------------- the key itself

def test_methods_is_a_setup_key():
    """The whole of MTH-1 in one assertion."""
    assert "methods" in contract.SETUP_KEYS


def _setup(**over):
    """A setup block that satisfies every SETUP_KEY, overridable."""
    base = {
        "lookback_d": 20, "horizon_h": 5, "normalize": "none",
        "feature_normalize": None, "corr_threshold": 0.9, "n_splits": 5,
        "min_train": 500, "random_state": 18, "selector_class": "FeatureSelector",
        "methods": ", ".join(METHODS),
    }
    base.update(over)
    return base


# ------------------------------------------------- the two sides must not merge

def test_three_ranker_and_six_ranker_runs_do_not_group_together():
    """The defect MTH-1 names: these two are different experiments."""
    new = contract.normalise_setup(_setup(methods=", ".join(METHODS)))
    old = contract.normalise_setup(_setup(methods=", ".join(ALL_METHODS)))
    assert new["methods"] != old["methods"]


def test_a_legacy_run_groups_apart_from_both():
    """A run that recorded no ensemble is neither the three nor the six."""
    legacy = contract.normalise_setup(_setup(methods=None))
    assert legacy["methods"] == contract.METHODS_UNRECORDED

    three = contract.normalise_setup(_setup(methods=", ".join(METHODS)))
    six = contract.normalise_setup(_setup(methods=", ".join(ALL_METHODS)))
    assert legacy["methods"] not in (three["methods"], six["methods"])


def test_absent_key_reads_the_same_as_an_explicit_none():
    """The archive omits `methods` entirely; it must not depend on which."""
    absent = _setup()
    absent.pop("methods")
    explicit = _setup(methods=None)
    assert (contract.normalise_setup(absent)["methods"]
            == contract.normalise_setup(explicit)["methods"])


def test_unrecorded_is_not_silently_read_as_the_six():
    """⚠️ CLAUDE.md §5 rule 2 — an absent measurement is absent, never inferred.

    Those runs did use all six; recording that here would make a guess into a fact
    and would merge a legacy run with a deliberate `methods=ALL_METHODS`
    reproduction, which are distinguishable and must stay so.
    """
    legacy = contract.normalise_setup(_setup(methods=None))["methods"]
    reproduction = contract.normalise_setup(_setup(methods=", ".join(ALL_METHODS)))
    assert legacy != reproduction["methods"]


# ------------------------------------------------------------ order-insensitive

def test_member_order_does_not_split_a_group():
    """The ensemble is a MEAN over members, so order cannot change the answer."""
    forward = contract.canonical_methods("spearman, xgb_shap, permutation")
    backward = contract.canonical_methods("permutation, xgb_shap, spearman")
    assert forward == backward


@pytest.mark.parametrize("value", [
    ("spearman", "xgb_shap"),
    ["xgb_shap", "spearman"],
    "xgb_shap,spearman",
    " spearman ,  xgb_shap ",
])
def test_canonical_methods_accepts_every_shape_the_json_may_hold(value):
    assert contract.canonical_methods(value) == "spearman, xgb_shap"


@pytest.mark.parametrize("empty", [None, "", "   ", [], ","])
def test_empty_readings_fall_back_to_unrecorded(empty):
    assert contract.canonical_methods(empty) == contract.METHODS_UNRECORDED


# ----------------------------------------------------- what it must NOT paper over

def test_normalise_does_not_invent_other_missing_keys():
    """⚠️ Only keys with a documented legacy default are filled. A genuinely
    missing SETUP_KEY must still surface, or the grouping silently weakens."""
    setup = _setup()
    setup.pop("random_state")
    filled = contract.normalise_setup(setup)
    assert "random_state" not in filled
    absent = [k for k in contract.SETUP_KEYS if k not in filled]
    assert absent == ["random_state"]


def test_normalise_does_not_mutate_the_caller():
    setup = _setup(methods=None)
    before = dict(setup)
    contract.normalise_setup(setup)
    assert setup == before


# ----------------------------------------------- the archive is not orphaned

def _shortlist():
    """The minimum `outstanding.csv` that satisfies the contract."""
    return pd.DataFrame({
        "channel": ["hose__vcb__close_adjust"],
        "source_table": ["pool__basic"],
        "schema": ["unified_schema_vcb"],
        "target": ["close_adjust_5day"],
        "lookback_d": [20], "horizon_h": [5],
        "run_id": ["2026-08-13_042815__vcb__economy_united_kingdom"],
        "evidence": ["no_null"],
        "cut_fdr_q": [0.1], "cut_corr_threshold": [0.9],
    })


def test_a_legacy_run_still_validates():
    """⚠️ The reason `methods` could not join SETUP_KEYS before. A run whose
    metadata predates the key must NOT be reported as broken — the 19 archived
    country runs are exactly this shape."""
    legacy = _setup()
    legacy.pop("methods")
    assert contract.validate_shortlist(_shortlist(), legacy) == []


def test_a_genuinely_missing_key_is_still_reported():
    """The guard still guards."""
    setup = _setup()
    setup.pop("n_splits")
    problems = contract.validate_shortlist(_shortlist(), setup)
    assert len(problems) == 1 and "n_splits" in problems[0]
