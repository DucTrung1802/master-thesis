"""The reuse key of a selection run.

⚠️ A selection at `d = 10` costs ~101 minutes per pool, so these tests are about two
failures with very different prices: **re-running something identical** (an hour) and
**reusing something that is not identical** (a wrong result that nothing announces).
Every case below is one of the inputs the old key — `(schema, target, lookback_d,
horizon_h)` — could not see.
"""

from __future__ import annotations

import json
import os

from feature_selection import footprint as fp


def _meta(**over) -> dict:
    base = {
        "input": {"schema": "unified_schema_liquid",
                  "tables": ["pool__event_features", "pool__targets"],
                  "panel_rows": 710683, "panel_columns": 89,
                  "first_date": "2009-01-02", "last_date": "2026-08-21",
                  "sessions": 4398, "tickers": 228,
                  "columns_by_table": {"pool__event_features": ["evt_vol_10", "har_lrv_22"]},
                  "source_rows": 710683, "source_last_date": "2026-08-21"},
        "setup": {"target": "uphold_5pct_5day", "horizon_h": 5, "lookback_d": 10,
                  "holdout_start": "2021-05-04", "device": "cuda", "random_state": 18,
                  "env_fingerprint": "cf51e65b3a15"},
        "execution": {"device": "cuda"},
        "null": {"draws": 10},
    }
    for block, values in over.items():
        base[block] = {**base[block], **values}
    return base


def test_the_same_inputs_give_the_same_footprint():
    """Stable across processes: the hash is over canonical JSON, not a dict's order."""
    assert (fp.of_metadata(_meta(), code="abc")["footprint"]
            == fp.of_metadata(_meta(), code="abc")["footprint"])


def test_a_column_order_is_not_an_input():
    """Two runs that read the same channels in a different order ARE the same run."""
    a = _meta()
    b = _meta()
    b["input"] = {**b["input"],
                  "columns_by_table": {"pool__event_features": ["har_lrv_22", "evt_vol_10"]}}
    assert fp.of_metadata(a, code="x")["footprint"] == fp.of_metadata(b, code="x")["footprint"]


def test_more_null_draws_is_a_different_run():
    """⚠️ §5 rule 1 — a 10-draw bar does not answer a request for 20."""
    a, b = _meta(), _meta(null={"draws": 20})
    assert fp.differences(fp.of_metadata(a, code="x"), fp.of_metadata(b, code="x")) \
        == ["null_draws"]


def test_a_rescraped_pool_is_a_different_run():
    """⚠️ The one input no code digest can see. §5 rule 10's shape, one layer up."""
    a = _meta()
    b = _meta(input={"panel_rows": 712000, "last_date": "2026-09-18"})
    assert fp.differences(fp.of_metadata(a, code="x"), fp.of_metadata(b, code="x")) == ["data"]


def test_the_holdout_and_the_device_are_part_of_the_key():
    """Both were READ by the old reuse path and neither was filtered on."""
    for block, change in (("setup", {"holdout_start": "2020-01-01"}),
                          ("setup", {"device": "cpu"})):
        other = _meta(**{block: change})
        assert fp.differences(fp.of_metadata(_meta(), code="x"),
                              fp.of_metadata(other, code="x")) == ["setup"]


def test_a_changed_ranking_path_is_a_different_run():
    a = fp.of_metadata(_meta(), code="aaaaaaaaaaaa")
    b = fp.of_metadata(_meta(), code="bbbbbbbbbbbb")
    assert fp.differences(a, b) == ["code"] and a["footprint"] != b["footprint"]


def test_an_archived_run_with_no_code_digest_is_never_reusable():
    """⚠️ `git_commit: 1593f663+dirty` cannot be resolved to bytes — `FPR-1`, §5 rule 2."""
    mark = fp.of_metadata(_meta(), code=None)
    assert mark["code"] == fp.UNKNOWN and mark["reusable"] is False
    # it still has a footprint: readable and comparable, just not skippable
    assert len(mark["footprint"]) == 12


def test_the_code_digest_covers_the_ranking_path_and_not_the_reporting(tmp_path):
    """⚠️ `report.py` is deliberately outside it — it writes artefacts, it cannot rank."""
    assert "report.py" not in fp.CODE_FILES
    assert {"selector.py", "windows.py", "gpu.py", "run.py"} <= set(fp.CODE_FILES)
    digest = fp.code_digest()
    assert len(digest) == 12 and digest == fp.code_digest()


def test_read_computes_one_for_a_folder_that_has_none(tmp_path):
    with open(os.path.join(tmp_path, "metadata.json"), "w", encoding="utf-8") as handle:
        json.dump(_meta(), handle)
    mark = fp.read(str(tmp_path))
    assert mark["reusable"] is False and mark["parts"]["data"]
    assert fp.read(str(tmp_path / "nothing")) is None
