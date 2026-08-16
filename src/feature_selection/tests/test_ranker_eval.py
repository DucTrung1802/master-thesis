# src\feature_selection\test_ranker_eval.py
"""The scorecard arithmetic, pinned — including the two bugs it was written after.

    python -m pytest src/feature_selection/test_ranker_eval.py -q

⚠️ Every test here except the last two is a REGRESSION test for something that was
published wrong on 2026-08-16 and had to be corrected the same day. That is the whole
argument for `ranker_eval.py` existing instead of a scratchpad script.
"""

import json
import os

import numpy as np
import pandas as pd
import pytest

from feature_selection import ranker_eval as re_
from feature_selection.selector import ALL_METHODS, METHODS


def _advantage(cells: dict, targets=("t1", "t2"), ks=(10, 20)) -> pd.DataFrame:
    """`{selector: [c1, c2, c3, c4]}` → the frame `scorecard` consumes."""
    rows = []
    for selector, values in cells.items():
        i = 0
        for target in targets:
            for k in ks:
                rows.append({
                    "selector": selector, "target": target, "k": k,
                    "advantage_pct": values[i], "ic_mean": 0.0,
                })
                i += 1
    return pd.DataFrame(rows)


def _cost(pct_of_ranking: dict) -> pd.DataFrame:
    return pd.DataFrame([
        {"ranker": m,
         "pct_of_ranking__level": pct_of_ranking.get(m, 0.0),
         "pct_of_run__level": pct_of_ranking.get(m, 0.0)}
        for m in ALL_METHODS
    ]).set_index("ranker")


# ------------------------------------------------------- the arithmetic bug


def test_mean_is_over_the_cells_and_never_includes_its_own_min():
    """⚠️ **THE BUG THAT SHIPPED.** `min` was written into the frame before
    `mean(axis=1)` ran, so every published mean averaged the four measured cells AND
    its own minimum — biasing every row low by 2-12 points, and forcing one published
    conclusion to be withdrawn. The mean of (82.5, 55, 35, 97.5) is 67.5; the buggy
    version returned 61.0, which is the mean of those four AND the 35."""
    frame = _advantage({"spearman": [82.5, 55.0, 35.0, 97.5]})
    card = re_.scorecard(frame, _cost({}))
    assert card.loc["spearman", "advantage_pct"] == pytest.approx(67.5)
    assert card.loc["spearman", "advantage_min_cell"] == pytest.approx(35.0)
    # the buggy value, named so a re-introduction is unmistakable
    assert card.loc["spearman", "advantage_pct"] != pytest.approx(61.0)


def test_necessity_is_signed_so_a_useful_member_is_positive():
    """`necessity_delta = full - without`. A member the blend NEEDS scores > 0."""
    frame = _advantage({
        "ENSEMBLE (all)": [80.0] * 4,
        "ensemble -permutation": [56.0] * 4,   # blend collapses without it
        "ensemble -lasso": [80.0] * 4,         # blend unchanged without it
    })
    card = re_.scorecard(frame, _cost({}))
    assert card.loc["permutation", "necessity_delta"] == pytest.approx(24.0)
    assert card.loc["lasso", "necessity_delta"] == pytest.approx(0.0)


# --------------------------------------------------------- the withdrawal rule


def test_a_constant_score_column_is_withdrawn_not_scored():
    """⚠️ **THE SECOND BUG.** `lasso` zeroes every coefficient on a return target, so
    every channel ties and `sort_values()` returns them in POOL COLUMN ORDER. Scoring
    that gave 92.5th percentile in one cell and 2.5th in another — both artefacts. A
    ranking that does not rank must produce NaN, never a number someone can quote."""
    ranks = pd.DataFrame({
        "lasso": [1.0, 1.0, 1.0, 1.0],          # every channel tied: ranked nothing
        "xgb_shap": [1.0, 2.0, 3.0, 4.0],
    })
    dead = {m for m in ranks.columns if ranks[m].nunique() <= 1}
    assert dead == {"lasso"}

    frame = _advantage({"lasso": [92.5, 2.5, 82.5, 80.0]})
    frame.loc[frame["selector"] == "lasso", "advantage_pct"] = np.nan
    card = re_.scorecard(frame, _cost({"lasso": 95.6}))
    assert np.isnan(card.loc["lasso", "advantage_pct"])
    # ⚠️ and efficiency must not be computed from a withdrawn advantage
    assert np.isnan(card.loc["lasso", "edge_per_pct"])


# --------------------------------------------------------------- efficiency


def test_efficiency_is_edge_over_chance_per_point_of_cost():
    frame = _advantage({"xgb_shap": [70.0] * 4, "mutual_info": [40.0] * 4})
    card = re_.scorecard(frame, _cost({"xgb_shap": 0.2, "mutual_info": 1.8}))
    assert card.loc["xgb_shap", "edge_per_pct"] == pytest.approx(100.0)
    # ⚠️ below chance is NO edge, not a negative one — clipped, never signed
    assert card.loc["mutual_info", "edge_per_pct"] == pytest.approx(0.0)


def test_a_free_member_has_undefined_efficiency_not_infinite():
    """`spearman` costs 0.0 % because `target_corr` is computed regardless. An
    undefined ratio is reported undefined; `inf` would sort to the top of the table."""
    frame = _advantage({"spearman": [70.0] * 4})
    card = re_.scorecard(frame, _cost({"spearman": 0.0}))
    assert np.isnan(card.loc["spearman", "edge_per_pct"])


# --------------------------------------------------------------- the cost side


def test_runtime_shares_splits_the_shared_xgb_timer_in_half(tmp_path):
    """⚠️ `xgb_gain` and `xgb_shap` come from ONE fit and ONE timer, so charging each
    the full number would double-count — and would imply that dropping either saves
    the whole thing, which is false."""
    run = tmp_path / "2026-08-16_000000__vcb__basic__return_5day"
    run.mkdir()
    (run / "metadata.json").write_text(json.dumps({
        "target": {"name": "return_5day"},
        "results": {"n_channels": 10, "timings_seconds": {
            "xgb gain + shap (cuda)": 10.0, "lasso (cuda)": 80.0,
            "permutation (cuda)": 10.0, "window design (cpu)": 100.0,
        }},
    }), encoding="utf-8")
    shares = re_.runtime_shares(str(tmp_path))
    row = shares.iloc[0]
    assert row["xgb_gain"] == pytest.approx(5.0)
    assert row["xgb_shap"] == pytest.approx(5.0)
    assert row["spearman"] == pytest.approx(0.0)      # free, by construction
    assert row["run_total_s"] == pytest.approx(200.0)  # includes the untimed-ranker work
    assert row["rank_total_s"] == pytest.approx(100.0)  # rankers only
    assert row["kind"] == "return"


def test_level_and_return_targets_are_separate_cost_regimes(tmp_path):
    """⚠️ Averaging them together hides the finding: `lasso` is ~96 % of the ranking
    phase on a level target and ~11 % on a return one, because a return collapses the
    solver to zero coefficients at once."""
    for name, target, lasso_s in (
        ("a__close_adjust_5day", "close_adjust_5day", 96.0),
        ("b__return_5day", "return_5day", 11.0),
    ):
        run = tmp_path / name
        run.mkdir()
        (run / "metadata.json").write_text(json.dumps({
            "target": {"name": target},
            "results": {"n_channels": 10, "timings_seconds": {
                "lasso (cuda)": lasso_s, "permutation (cuda)": 100.0 - lasso_s,
            }},
        }), encoding="utf-8")
    cost = re_.cost_table(re_.runtime_shares(str(tmp_path)))
    assert cost.loc["lasso", "pct_of_ranking__level"] == pytest.approx(96.0)
    assert cost.loc["lasso", "pct_of_ranking__return"] == pytest.approx(11.0)


# ------------------------------------------------------------- the guard rails


def test_it_does_not_write_into_the_selection_run_root():
    """⚠️ `contract.run_folders` calls ANY directory holding a `metadata.json` a
    selection run. A scorecard written into `reports/feature_selection/` would appear
    in `final_features.plan_from_reports` as a run whose channels belong in a
    `__final__` table."""
    from feature_selection import report

    assert os.path.normpath(re_.DEFAULT_ROOT) != os.path.normpath(
        report.DEFAULT_REPORT_ROOT
    )
    assert "ranker_evaluation" in re_.DEFAULT_ROOT


def test_all_targets_matches_the_cli_that_owns_the_list():
    """A second hand-maintained label list is a second thing to drift; the guard in
    `run.py` only protects `run.py`'s copy."""
    from feature_selection import run as run_cli

    assert re_.ALL_TARGETS == run_cli.ALL_TARGETS
    assert re_.IDENTITY == run_cli.IDENTITY


def test_the_default_is_a_subset_of_what_is_implemented():
    assert set(METHODS) <= set(ALL_METHODS)


def test_all_but_zero_scores_are_withdrawn_too_not_only_identical_ones():
    """⚠️ **THE THIRD BUG, and the subtlest.** The withdrawal rule was `nunique() <= 1`.
    On `return_5day` LassoCV returns byte-identical zeros and it fired; on
    `return_rel_5day` it returns values all <= 1e-12 that differ in their last bits, so
    `nunique()` was 84, the rule did NOT fire, and the ranker scored the 81.25th
    percentile for sorting floating-point noise. A degenerate ranking that does not look
    degenerate is worse than one that does."""
    noise = pd.Series([1e-18, 3e-19, 7e-20, 2e-18])
    assert noise.nunique() > 1                       # the old rule would pass it
    assert bool((noise.abs() <= re_.ZERO_TOL).all())  # the new rule catches it

    real = pd.Series([0.0, 0.4, 1.0, 0.2])
    assert not bool((real.abs() <= re_.ZERO_TOL).all())
