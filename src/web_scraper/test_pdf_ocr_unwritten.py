"""The cells a run WON and no merge ever wrote — `unwritten_cells` / `release_batch`.

⚠️ **THIS IS THE BLIND SPOT `exhausted_quarters` LEAVES, AND IT WAS THE EXPENSIVE HALF.**
That function answers *"which cells has this cascade already lost?"* so a gap plan can stop
re-asking. Nothing answered the opposite question — *"which cells did it WIN, that disk does
not hold?"* — and from the statement CSV the two are the same word: the cell reads `missing`
and `plan_batch` opens the document again.

⚠️ **MEASURED ON VN30, 2026-09-12, BEFORE ANY OF THIS EXISTED: 358 of 779 open cells (46 %)
were parsed and unwritten**, PLX alone holding 78 from a single 54-document run. The release
wrote **246** of them with no OCR at all and took the VN30 cell rate **85.1 % -> 89.7 %**; the
screens withheld 122, which is the guard the empty band did not provide.

Every test here runs without a PDF, a network or an OCR engine.
"""
import json

import pytest

from web_scraper import pdf_ocr_batch as batch


REPORTS = ("balance_sheet", "income_statement", "cash_flow")


def _folder(root, name):
    path = root / name
    (path / "documents").mkdir(parents=True)
    (path / "metadata.json").write_text(json.dumps({
        "inputs": {"exchange": "HOSE", "symbol": "PLX"}, "results": [],
    }), encoding="utf-8")
    return path


def _document(folder, period, accepted, **extra):
    (folder / "documents" / f"HOSE_PLX__{period}.json").write_text(
        json.dumps({"period": period, "exchange": "HOSE", "symbol": "PLX",
                    "accepted": {r: {"layer": "onnx@200", "items": 30} for r in accepted},
                    **extra}),
        encoding="utf-8")


class _Task:
    """The one thing `job.plan` gives that this function reads."""

    def __init__(self, period):
        self.period = period


@pytest.fixture()
def disk(monkeypatch):
    """`{quarter: [reports already `pdf` on disk]}`, patched in as the CSVs' answer."""
    held = {}

    def _plan(_builder, _exchange, _symbol, **_kw):
        return [_Task(p) for p in ("Q1-2018", "Q2-2018", "Q3-2018")]

    def _parsed(_builder, task):
        # keyed on the REPO-NATIVE `Q1-2018`, which is what a task carries; `as_quarter`
        # converts one way only and a second spelling in the fixture is a bug in the fixture
        return list(held.get(task.period, ()))

    monkeypatch.setattr(batch.job, "plan", _plan)
    monkeypatch.setattr(batch.job, "parsed_reports", _parsed)
    monkeypatch.setattr(batch.job, "resolve_template", lambda *_a, **_k: ("corp", "given"))
    monkeypatch.setattr(batch.fin, "FinancialsBuilder", lambda **_k: object())
    return held


# ── what it finds ─────────────────────────────────────────────────────────────

def test_a_statement_accepted_in_a_run_folder_and_missing_on_disk_is_found(tmp_path, disk):
    """The 2-of-3 quarter, which is the population that made this worth writing.

    ⚠️ `_merge_finished_quarters` lifts the empty-band refusal only for a quarter whose filing
    produced ALL THREE statements, so on a ticker bootstrapping from nothing the two good
    statements of a 2-of-3 filing are held — and the band therefore stays empty, so the next
    run holds them again. `BND-1`'s loop, one level in from where it was closed.
    """
    disk["Q1-2018"] = []
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    _document(folder, "Q1-2018", ["balance_sheet", "cash_flow"])

    found = batch.unwritten_cells(tmp_path, "HOSE", "PLX")

    assert set(found) == {("2018-Q1", "balance_sheet"), ("2018-Q1", "cash_flow")}
    assert found[("2018-Q1", "balance_sheet")] == ["20260912-000000__hose_plx__pdf_ocr"]


def test_a_cell_already_pdf_on_disk_is_never_reported(tmp_path, disk):
    """⚠️ Otherwise a re-sweep of every ticker's whole history would be planned to move
    nothing — FPT has 347 run folders, and `merge_batch` makes one call per (ticker, period).
    """
    disk["Q1-2018"] = list(REPORTS)
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    _document(folder, "Q1-2018", REPORTS)

    assert batch.unwritten_cells(tmp_path, "HOSE", "PLX") == {}


def test_a_document_whose_layers_RAISED_is_never_offered_for_release(tmp_path, disk):
    """⚠️ `VCR-1`: an exception measures the MACHINE, so whatever won the cascade won BY
    DEFAULT — and its `accepted` block looks exactly like a good one, a real layer and a real
    item count. `complete_periods` refuses such a document whole; so must this, or the release
    becomes the one path that writes a figure nothing chose.
    """
    disk["Q1-2018"] = []
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    _document(folder, "Q1-2018", REPORTS,
              engine_errors=[["onnx@200", "CUDA failure 2: out of memory"]])

    assert batch.unwritten_cells(tmp_path, "HOSE", "PLX") == {}


def test_a_document_that_errored_is_never_offered_either(tmp_path, disk):
    disk["Q1-2018"] = []
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    _document(folder, "Q1-2018", REPORTS, error="the child died")

    assert batch.unwritten_cells(tmp_path, "HOSE", "PLX") == {}


def test_a_quarter_the_ticker_never_filed_is_not_a_cell_at_all(tmp_path, disk):
    """⚠️ The denominator is `job.plan`, i.e. the FILING index — never the run folders.
    A folder holding a period outside the plan is a stale artefact, not a recoverable cell.
    """
    disk["Q1-2018"] = list(REPORTS)
    disk["Q2-2018"] = list(REPORTS)
    disk["Q3-2018"] = list(REPORTS)
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    _document(folder, "Q4-2017", REPORTS)

    assert batch.unwritten_cells(tmp_path, "HOSE", "PLX") == {}


def test_every_folder_holding_the_cell_is_named_oldest_first(tmp_path, disk):
    """⚠️ The release sweeps folders, not cells, so which folders hold it is the answer —
    and `merge_run` picks the reading; this function only says where to look.
    """
    disk["Q2-2018"] = []
    late = _folder(tmp_path, "20260912-120000__hose_plx__pdf_ocr")
    early = _folder(tmp_path, "20260901-120000__hose_plx__pdf_ocr")
    _document(early, "Q2-2018", ["cash_flow"])
    _document(late, "Q2-2018", ["cash_flow"])

    found = batch.unwritten_cells(tmp_path, "HOSE", "PLX")

    assert found[("2018-Q2", "cash_flow")] == [early.name, late.name]


# ── what it then does ─────────────────────────────────────────────────────────

def test_the_release_lifts_the_band_and_never_forces_a_DIFFERS(tmp_path, disk, monkeypatch):
    """⚠️ THE TWO HALVES OF WHY THIS IS SAFE TO RUN ON ANYTHING.

    The empty-band refusal is LIFTED — that is the whole point, and the arithmetic screens are
    what stands in its place (`MERGE_SCREEN`, 2026-09-10). `force_differs` is never passed: a
    figure disagreeing with a good `pdf` row on disk is a different judgement with its own
    scoped tool (`REPAIR`), and taking the newer of two readings is not how it is settled.
    """
    disk["Q1-2018"] = []
    _document(_folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr"),
              "Q1-2018", ["balance_sheet"])
    seen = {}

    def _merge_batch(folders, **kw):
        seen.update(kw)
        seen["folders"] = list(folders)
        return {"written": 1, "skipped": 0, "already": 0, "withheld": 0,
                "failed": 0, "passes": 1}

    monkeypatch.setattr(batch, "merge_batch", _merge_batch)
    out = batch.release_batch(["PLX"], reports_root=tmp_path, apply=True, log=lambda _l: None)

    assert seen["force_empty_band"] is True
    assert seen["force_differs"] is False
    assert seen["screen"] is True
    assert seen["apply"] is True
    assert out["cells"] == 1 and out["folders"] == 1


def test_a_ticker_with_nothing_unwritten_reaches_no_merge_at_all(tmp_path, disk, monkeypatch):
    """⚠️ A sweep of a clean ticker is not free: it re-plans every period of every folder
    against disk to write nothing. The release must not start one.
    """
    disk["Q1-2018"] = list(REPORTS)
    disk["Q2-2018"] = list(REPORTS)
    disk["Q3-2018"] = list(REPORTS)
    _document(_folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr"), "Q1-2018", REPORTS)

    def _never(*_a, **_k):
        raise AssertionError("merge_batch must not be called for a clean ticker")

    monkeypatch.setattr(batch, "merge_batch", _never)
    out = batch.release_batch(["PLX"], reports_root=tmp_path, apply=True, log=lambda _l: None)

    assert out == {"cells": 0, "written": 0, "withheld": 0, "folders": 0}


# ── the one GPU question that does not depend on the parser ───────────────────

def test_a_cell_no_run_ever_opened_is_the_only_unspent_gpu(tmp_path, disk):
    """⚠️ `exhausted_quarters` may reuse a refusal only when the PARSER is the same file, and
    for 1,214 of VN30's 1,472 run folders it cannot — they were written at a dirty tree. This
    asks the strictly weaker question with no such precondition: **did anything ever ASK?**
    """
    disk["Q1-2018"] = REPORTS
    disk["Q2-2018"] = []
    disk["Q3-2018"] = []
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    (folder / "metadata.json").write_text(json.dumps({
        "inputs": {"exchange": "HOSE", "symbol": "PLX"},
        "results": [{"period": "Q2-2018", "report": r, "status": "absent"} for r in REPORTS],
    }), encoding="utf-8")

    assert batch.unasked_quarters(tmp_path, "HOSE", "PLX") == {"2018-Q3": sorted(REPORTS)}


def test_an_ACCEPTED_result_row_still_counts_AS_ASKED(tmp_path, disk):
    """⚠️ **THIS DISTINCTION COST A WRONG ANSWER, AND THE WRONG ANSWER WAS 113.** A first pass
    counted only rows whose status was NOT `pdf`, reasoning that a `pdf` row means the cell is
    done. It was counting `HLD-1`'s population backwards: a statement the run ACCEPTED and the
    merge then withheld carries a `pdf` result row and a `missing` cell on disk, so "no refusal
    recorded" read as "never asked" for exactly the cells that had been asked and answered.
    **A run that opened a document asked every statement of it.**
    """
    disk["Q1-2018"] = REPORTS
    disk["Q2-2018"] = REPORTS
    disk["Q3-2018"] = []
    folder = _folder(tmp_path, "20260912-000000__hose_plx__pdf_ocr")
    (folder / "metadata.json").write_text(json.dumps({
        "inputs": {"exchange": "HOSE", "symbol": "PLX"},
        "results": [{"period": "Q3-2018", "report": r, "status": "pdf"} for r in REPORTS],
    }), encoding="utf-8")

    assert batch.unasked_quarters(tmp_path, "HOSE", "PLX") == {}
