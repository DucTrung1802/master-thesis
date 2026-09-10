"""The arithmetic screens standing in for the magnitude band — `merge_batch(screen=True)`.

⚠️ **`FORCE_EMPTY_BAND` LIFTS A REAL GUARD (`BND-1`) AND THE SCREENS ARE WHAT REPLACES IT.**
That sentence has been in `.claude/docs/PDF_OCR.md` since 2026-09-04 and it was ADVICE: a person
had to call `statement_screens.screen_run` and hold the flagged pairs out of the merge by hand.
Measured 2026-09-10, advice nobody executes looks like **147 cells parsed, accepted, sitting in
run folders and never written**, across six tickers — the parse durable in the artefact, the CSV
never opened, which is `BND-1`'s exact shape.

These pin the four properties that make running the screens automatically defensible:

  * a statement whose own arithmetic fails is WITHHELD where `sane` had nothing to say;
  * a statement `sane` DID judge keeps that verdict — the screens are not a second gate, they
    check different things, and one that could veto a guarded row would be a gate nobody
    measured;
  * a period whose every report is flagged is SKIPPED, never merged with an empty `reports`
    list, which `merge_run` reads as "all three";
  * `screen=False` is byte-for-byte the behaviour that shipped before.
"""
import csv
import json
import os
import shutil
from pathlib import Path

import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_batch as batch
from web_scraper import pdf_ocr_merge as merge

TEMPLATE = "bank"
ASSETS = fin.FinancialsBuilder.C_ASSETS[0]
RESOURCES = fin.FinancialsBuilder.C_RESOURCES[0]
REPO_SCHEMA = Path(__file__).resolve().parents[2] / "raw_data" / "cafef" / "financials" / "schema"


@pytest.fixture()
def root(tmp_path, monkeypatch):
    base = tmp_path / "cafef"
    (base / "financials" / "schema").mkdir(parents=True)
    for chart in REPO_SCHEMA.glob(f"{TEMPLATE}_*.csv"):
        shutil.copy2(chart, base / "financials" / "schema" / chart.name)
    (base / "pdfs" / "index").mkdir(parents=True)
    monkeypatch.setattr(fin, "PDFS_DIR", str(base / "pdfs"))
    monkeypatch.setattr(fin, "SCHEMA_DIR", str(base / "financials" / "schema"))
    monkeypatch.setattr(fin, "STATEMENTS_DIR", str(base / "financials" / "statements"))
    monkeypatch.setattr(merge, "BACKUP_ROOT", tmp_path / "_backup")
    return base


def _statement(**values):
    return {"layer": "onnx@200", "items": len(values), "rows": 20, "rows_sha": "abc",
            "pages": [1], "unit": 1, "n_columns": 2, "cash_flow_method": "",
            "quarter_column": False, "months": None, "values": values}


def _folder(tmp_path, *, accepted, bands=None, period="Q3-2014"):
    """A run folder `merge_batch` can read — `inputs` and `results` as well as the document."""
    folder = tmp_path / "20260910-000000__hose_tst__pdf_ocr"
    (folder / "documents").mkdir(parents=True, exist_ok=True)
    (folder / "metadata.json").write_text(json.dumps({
        "run_id": folder.name,
        "inputs": {"exchange": "HOSE", "symbol": "TST", "template": TEMPLATE},
        "results": [{"period": period, "report": r, "status": "pdf", "layer": "onnx@200",
                     "items": 3, "verdict": "", "seconds": 1.0} for r in accepted],
    }), "utf-8")
    (folder / "documents" / f"HOSE_TST__{period}.json").write_text(json.dumps({
        "exchange": "HOSE", "symbol": "TST", "period": period, "template": TEMPLATE,
        "document": f"{period}.pdf", "consolidated": "True", "assurance": "unaudited",
        "cumulative": False, "seconds": 1.0, "accepted": accepted,
        "absent": [r for r in fin.REPORTS if r not in accepted],
        "facts": {"publish_date": "2014-11-14", "shares_authorized": None,
                  "shares_issued": None, "shares_outstanding": None},
        # ⚠️ `{"True": 0}` IS THE EMPTY BAND — `sane` judged nothing and the guard is the thing
        # under test. A non-zero size is a band, and then the screens must stay out of the way.
        "history_sizes": bands or {r: {"True": 0, "False": 0} for r in fin.REPORTS},
        "open_ref": None, "error": None, "log": [],
    }), "utf-8")
    return folder


def _on_disk(report):
    path = fin.statement_path(TEMPLATE, report, "HOSE", "TST")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8-sig") as f:
        return {r["period"]: r for r in csv.DictReader(f)}


# a balance sheet that fails the identity it asserts about itself: assets != liab + equity
BROKEN_BS = {ASSETS: 1_000_000_000_000, RESOURCES: 400_000_000_000}
SOUND_BS = {ASSETS: 1_000_000_000_000, RESOURCES: 1_000_000_000_000}


def test_a_statement_that_fails_its_own_identity_is_withheld_when_sane_had_no_band(root,
                                                                                  tmp_path):
    """The screens stand in for the guard exactly where the guard was lifted."""
    folder = _folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**BROKEN_BS)})
    said = []
    tally = batch.merge_batch([folder], apply=True, force_empty_band=True, log=said.append)
    assert tally["withheld"] == 1
    assert tally["written"] == 0
    # ⚠️ The ONLY accepted statement is the withheld one, so the period has nothing left to
    # merge and is skipped whole — no CSV is created. Where something else of the quarter IS
    # written the withheld row appears as `missing`; the test below pins that half.
    assert not _on_disk(fin.BALANCE_SHEET)
    assert any("hold" in line and "balance_sheet" in line for line in said)


def test_a_sound_statement_is_still_written_beside_a_withheld_one(root, tmp_path):
    """Withholding is per (period, report) — one bad statement does not cost the others."""
    folder = _folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**BROKEN_BS),
        fin.CASH_FLOW: _statement(**{fin.FinancialsBuilder.C_CASH_CLOSE[0]: 5_000_000_000}),
    })
    tally = batch.merge_batch([folder], apply=True, force_empty_band=True, log=lambda _s: None)
    assert tally["withheld"] == 1
    assert tally["written"] == 1
    assert _on_disk(fin.CASH_FLOW)["Q3-2014"]["source"] == "pdf"
    # ⚠️ **THE WITHHELD ONE GETS A ROW, AND THE ROW SAYS `missing`.** `_write` renders all three
    # CSVs on every call, so "not written" is never "no file" — it is a quarter the grid lists
    # and no figure fills, which is the same answer a filing that could not be read gets and is
    # the only honest one: the statement was READ and is not believed.
    assert _on_disk(fin.BALANCE_SHEET)["Q3-2014"]["source"] == "missing"


def test_the_screens_do_not_overrule_a_band_that_actually_judged_the_row(root, tmp_path):
    """⚠️ `sane` and the identities check DIFFERENT things, so a screen may not veto a guarded
    row. It is consulted only where the guard was lifted — otherwise it would be a second gate
    nobody measured, on every merge this repo has ever run."""
    folder = _folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**BROKEN_BS)},
                     bands={r: {"True": 12, "False": 0} for r in fin.REPORTS})
    tally = batch.merge_batch([folder], apply=True, force_empty_band=False,
                              log=lambda _s: None)
    assert tally["withheld"] == 0


def test_a_period_whose_every_report_is_flagged_is_skipped_not_merged_wide_open(root, tmp_path):
    """⚠️ AN EMPTY `reports` LIST IS FALSY AND `merge_run` READS THAT AS "ALL THREE" — so the
    one thing withholding must never do is hand it the empty list it would ignore."""
    folder = _folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**BROKEN_BS),
        # a cash flow whose closing balance is negative — `screen_document`'s own check
        fin.CASH_FLOW: _statement(**{fin.FinancialsBuilder.C_CASH_CLOSE[0]: -5_000_000_000}),
    })
    tally = batch.merge_batch([folder], apply=True, force_empty_band=True, log=lambda _s: None)
    assert tally["withheld"] == 2
    assert tally["written"] == 0
    # nothing was merged for this period at all, so no CSV was created either
    assert not _on_disk(fin.BALANCE_SHEET)
    assert not _on_disk(fin.CASH_FLOW)


def test_screen_false_is_the_behaviour_that_shipped_before(root, tmp_path):
    """The escape hatch, and it has to be exact: nothing is screened, nothing is withheld."""
    folder = _folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**BROKEN_BS)})
    tally = batch.merge_batch([folder], apply=True, force_empty_band=True, screen=False,
                              log=lambda _s: None)
    assert tally["withheld"] == 0
    assert tally["written"] == 1
    assert "Q3-2014" in _on_disk(fin.BALANCE_SHEET)


def test_a_write_the_machine_refuses_is_reported_and_the_batch_goes_on(root, tmp_path,
                                                                      monkeypatch):
    """⚠️ A file lock is a fact about the MACHINE, not the filing — measured 2026-09-10, when
    `PermissionError: [WinError 5]` on one `os.replace` ended a corpus sweep with ten tickers
    still unmerged behind it."""
    folder = _folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**SOUND_BS)})

    def _boom(*_a, **_k):
        raise PermissionError(5, "Access is denied")

    monkeypatch.setattr(batch.job.FinancialsBuilder, "_write", _boom)
    said = []
    tally = batch.merge_batch([folder], apply=True, force_empty_band=True, log=said.append)
    assert tally["failed"] == 1
    assert tally["written"] == 0
    assert any("the WRITE failed" in line for line in said)
    assert any("re-run the sweep" in line for line in said)
