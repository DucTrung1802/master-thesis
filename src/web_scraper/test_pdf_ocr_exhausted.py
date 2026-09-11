"""A plan that remembers what was ASKED — `ASK-1`. No PDF, no network, no OCR engine.

⚠️ **THE GAP PLAN SELECTED ON WHAT WAS WON AND NOT ON WHAT WAS TRIED, AND THAT COST FOUR HOURS
IN ONE DAY** (2026-09-11). `plan_batch` picks a quarter when any of its three reports is not
`pdf`, which answers *"what does this ticker still owe"* and never *"what is worth spending GPU
on"*. Measured the same day: of 241 open cells across the corpus **184 had already met the full
115-layer ONNX cascade and lost**, so a gap run spends its budget reproducing refusals. SHB's 18
`SPR-1` documents returned **0 cells in 2.2 h**; a tesseract-only pass then read 37 SHB
documents, wrote ONE statement, and — the cells still being `missing` — left the next pass
planning all 37 again.

⚠️ **THE DANGEROUS HALF IS THE SKIP, NOT THE PLAN, WHICH IS WHY MOST OF THIS FILE IS ABOUT WHEN
IT MUST NOT FIRE.** A skip that outlives the code it was measured under is `SET-3`'s
self-sealing loop one register up: a cell never re-run, so its recorded reason never updates, so
it is never re-run. Two guards keep that shut and both are pinned below — the cascade must be at
least the one about to run, and the PARSER must be the same file.
"""
import json

import pytest

from web_scraper import pdf_ocr_batch as pb

PARSER = ("blob-parser", "blob-financials")
FULL = ["onnx@200", "onnx@300", "onnx@400"]


def _folder(root, run_id, commit, layers, results):
    """One run folder in the shape `exhausted_quarters` reads."""
    folder = root / f"{run_id}__hose_acb__pdf_ocr"
    folder.mkdir(parents=True)
    (folder / "metadata.json").write_text(json.dumps({
        "git_commit": commit,
        "inputs": {"symbol": "ACB", "layers": layers},
        "results": results,
    }), encoding="utf-8")
    return folder


def _result(period, report, status="absent"):
    return {"period": period, "report": report, "status": status}


@pytest.fixture()
def same_parser(monkeypatch):
    """Every commit carries the parser that is about to run — the skip's precondition."""
    monkeypatch.setattr(pb, "parser_blobs", lambda commit=None: PARSER)


# ── the fingerprint ───────────────────────────────────────────────────────────

def test_a_dirty_tree_can_never_be_fingerprinted():
    """⚠️ §5 rule 2 at the fingerprint: `None` is UNKNOWN, and unknown means run the document.

    A run folder records `git_commit` as `<sha>` or `<sha>+dirty`. A dirty tree says the parser
    of that moment was never committed and cannot be recovered — it is not a match and not a
    mismatch, and reading it either way is a guess.
    """
    assert pb.parser_blobs("ced123b8+dirty") is None
    assert pb.parser_blobs("") is None


def test_the_working_tree_can_be_fingerprinted_at_all():
    """The other side of the same coin — without this the feature is permanently inert."""
    assert pb.parser_blobs() is not None


# ── when the skip must NOT fire ───────────────────────────────────────────────

def test_an_unfingerprintable_parser_skips_NOTHING(tmp_path, monkeypatch):
    """⚠️ The safe direction is always to run. A `None` fingerprint yields an EMPTY answer."""
    _folder(tmp_path, "20260911-000000", "abc123", FULL,
            [_result("Q1-2015", "balance_sheet")])
    monkeypatch.setattr(pb, "parser_blobs", lambda commit=None: None)

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {}


def test_a_folder_whose_PARSER_DIFFERS_skips_nothing(tmp_path, monkeypatch):
    """⚠️ A parser change makes every stale verdict re-winnable — `SPR-1` and `TSM-2` both
    landed on 2026-09-11, and every folder written before them is arguing about other code."""
    _folder(tmp_path, "20260911-000000", "old-commit", FULL,
            [_result("Q1-2015", "balance_sheet")])
    monkeypatch.setattr(pb, "parser_blobs",
                        lambda commit=None: PARSER if commit is None else ("other", "blobs"))

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {}


def test_a_SUBSET_cascade_settles_nothing_about_the_layers_it_never_reached(
        tmp_path, same_parser):
    """⚠️ The refusal of a shorter cascade is a smaller question, answered.

    `tesseract@200` + `tesseract@400+relax` is two layers of 117; a document it lost says
    nothing about the 115 ONNX layers, and reading it as "exhausted" is how a real cell would
    be retired by the cheapest run in the corpus.
    """
    _folder(tmp_path, "20260911-000000", "abc123", ["onnx@200"],
            [_result("Q1-2015", "balance_sheet")])

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {}


def test_a_folder_with_no_recorded_cascade_settles_nothing(tmp_path, same_parser):
    """A run that did not write down which layers it ran cannot be compared to one."""
    _folder(tmp_path, "20260911-000000", "abc123", None,
            [_result("Q1-2015", "balance_sheet")])

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {}


def test_a_statement_the_run_PARSED_is_not_exhausted(tmp_path, same_parser):
    """`status == "pdf"` is a win, not a refusal — it belongs in neither list."""
    _folder(tmp_path, "20260911-000000", "abc123", FULL,
            [_result("Q1-2015", "balance_sheet", status="pdf")])

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {}


def test_another_tickers_folder_does_not_leak_in(tmp_path, same_parser):
    """The glob is the filter, exactly as it is in `settled_absences`."""
    folder = tmp_path / "20260911-000000__hose_vic__pdf_ocr"
    folder.mkdir(parents=True)
    (folder / "metadata.json").write_text(json.dumps({
        "git_commit": "abc123",
        "inputs": {"symbol": "VIC", "layers": FULL},
        "results": [_result("Q1-2015", "balance_sheet")]}), encoding="utf-8")

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {}


def test_a_damaged_folder_is_skipped_rather_than_raising(tmp_path, same_parser):
    """An artefact is read by a notebook cell; one unreadable folder may not end the plan."""
    _folder(tmp_path, "20260911-000000", "abc123", FULL,
            [_result("Q1-2015", "balance_sheet")])
    bad = tmp_path / "20260911-111111__hose_acb__pdf_ocr"
    bad.mkdir(parents=True)
    (bad / "metadata.json").write_text("{not json", encoding="utf-8")

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {
        "2015-Q1": ["balance_sheet"]}


# ── when it must ──────────────────────────────────────────────────────────────

def test_the_same_cascade_under_the_same_parser_IS_exhausted(tmp_path, same_parser):
    """The case the feature exists for, keyed `YYYY-QQ` like every other batch-facing map."""
    _folder(tmp_path, "20260911-000000", "abc123", FULL,
            [_result("Q1-2015", "balance_sheet"), _result("Q1-2015", "cash_flow")])

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {
        "2015-Q1": ["balance_sheet", "cash_flow"]}


def test_a_DEEPER_cascade_covers_the_one_about_to_run(tmp_path, same_parser):
    """A superset answered the question and more — re-asking it is the waste being removed."""
    _folder(tmp_path, "20260911-000000", "abc123", FULL + ["onnx@200+joinlost"],
            [_result("Q1-2015", "balance_sheet")])

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {
        "2015-Q1": ["balance_sheet"]}


def test_two_folders_contribute_to_the_same_quarter(tmp_path, same_parser):
    """One document per folder is how `run_batch` writes them, so a quarter's three reports
    routinely arrive from several — the answer is their union."""
    _folder(tmp_path, "20260911-000000", "abc123", FULL,
            [_result("Q1-2015", "balance_sheet")])
    _folder(tmp_path, "20260911-010000", "abc123", FULL,
            [_result("Q1-2015", "income_statement")])

    assert pb.exhausted_quarters(tmp_path, "HOSE", "ACB", layers=FULL) == {
        "2015-Q1": ["balance_sheet", "income_statement"]}
