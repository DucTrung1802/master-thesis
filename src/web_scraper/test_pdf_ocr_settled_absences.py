"""Pure tests for the SETTLED-ABSENCE record — no PDF, no network, no OCR engine.

⚠️ **THE DISTINCTION UNDER TEST IS BETWEEN TWO IDENTICAL-LOOKING FAILURES, AND IT COST FOUR
RE-RUNS.** `_parse_cascaded` prints the word `absent` both when a filing does not CONTAIN a
statement and when every layer refused the one it found. The first is permanent — `missing` is
the correct answer (§5 rule 24) and no layer, engine or re-run can change it; the second may
still be overturned. Until 2026-08-31 the reason lived only in `run.log` prose, and ACB's
Q2-2009 and Q3-2009 cash flows were put through all 50 layers four times on 2026-08-30 because
nothing in the artefact said which kind they were. Both filings are three-page **BÁO CÁO TÀI
CHÍNH TÓM TẮT** forms (Mẫu CBTT-03) carrying a condensed balance sheet and a four-line P&L.

⚠️ **AND THE ABSENT-RECORD CASE IS THE ONE THAT MATTERS MOST.** A run that predates the field
must contribute NOTHING, never "nothing was permanently absent" — an absent measurement is
absent, not evidence either way (§5 rule 2). A helper that guessed there would re-create the
defect it exists to remove, one register down.
"""
import json
from pathlib import Path

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job


def _folder(root: Path, run_id: str, period: str, doc: dict) -> Path:
    """One run folder on disk, in the shape `settled_absences` reads."""
    folder = root / f"{run_id}__hose_acb__pdf_ocr"
    (folder / "documents").mkdir(parents=True)
    payload = {"exchange": "HOSE", "symbol": "ACB", "period": period}
    payload.update(doc)
    (folder / "documents" / f"HOSE_ACB__{period}.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return folder


def test_the_cascade_and_the_reader_share_ONE_string(tmp_path):
    """The constant is the contract — a reader must not re-type the sentence.

    ⚠️ `layer_errors` was made data for exactly this argument ("a WARNING is prose, and the
    decision downstream must not depend on matching a sentence"), and the refusal reason was
    left as prose anyway. Asserted structurally: the emit site and the reader are the same
    object, so a reworded message cannot silently stop the reader from matching.
    """
    source = Path(fin.__file__).read_text(encoding="utf-8")
    assert "NO_SUCH_STATEMENT))" in source, "the cascade must emit the constant, not a literal"
    assert source.count('"no such statement on any page of this filing"') == 1, (
        "the sentence may exist ONCE — as the constant's own definition")


def test_a_recorded_no_such_statement_is_reported(tmp_path):
    _folder(tmp_path, "20260831-000000", "Q2-2009", {
        "accepted": {"balance_sheet": {"layer": "onnx@200", "items": 19}},
        "absent": ["cash_flow"],
        "absent_reasons": {"cash_flow": [["onnx@200", fin.NO_SUCH_STATEMENT]]},
    })
    # ⚠️ Keyed in the SORTABLE `YYYY-QQ` form the batch filter is written in, not in the
    # artefact's own `QQ-YYYY`: the conversion belongs in one place, not at every call site.
    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {
        "2009-Q2": {"cash_flow": "20260831-000000__hose_acb__pdf_ocr"}}


def test_a_run_predating_the_field_contributes_NOTHING(tmp_path):
    """§5 rule 2 — the whole point of the version bump.

    A v3 folder records `absent: ["cash_flow"]` and no reason. That is an UNKNOWN: the filing
    may hold no cash flow, or every layer may have refused one. Reading it either way is a
    guess, and the safe-looking guess ("nothing is settled") is the correct one only because it
    leaves the question open rather than answering it.
    """
    _folder(tmp_path, "20260830-232851", "Q2-2009", {"absent": ["cash_flow"]})
    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {}


def test_an_ordinary_refusal_is_not_settled(tmp_path):
    """A statement the OCR failed on is recoverable, and must never be reported as permanent."""
    _folder(tmp_path, "20260831-000000", "Q3-2016", {
        "absent": ["cash_flow"],
        "absent_reasons": {"cash_flow": [["onnx@200", "cash flow does not close"],
                                         ["onnx@300", "no closing cash balance"]]},
    })
    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {}


def test_only_the_named_ticker_is_read(tmp_path):
    """The glob is the filter — a sibling ticker's run folder must not leak into the answer."""
    folder = tmp_path / "20260831-000000__hose_vic__pdf_ocr"
    (folder / "documents").mkdir(parents=True)
    (folder / "documents" / "HOSE_VIC__Q2-2009.json").write_text(json.dumps(
        {"period": "Q2-2009",
         "absent_reasons": {"cash_flow": [["onnx@200", fin.NO_SUCH_STATEMENT]]}}),
        encoding="utf-8")
    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {}


def test_a_damaged_run_folder_is_skipped_rather_than_raising(tmp_path):
    """An artefact is read by a notebook cell; one unreadable folder may not end the report."""
    good = _folder(tmp_path, "20260831-000000", "Q2-2009", {
        "absent_reasons": {"cash_flow": [["onnx@200", fin.NO_SUCH_STATEMENT]]}})
    bad = tmp_path / "20260831-111111__hose_acb__pdf_ocr"
    (bad / "documents").mkdir(parents=True)
    (bad / "documents" / "HOSE_ACB__Q3-2009.json").write_text("{not json", encoding="utf-8")
    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {
        "2009-Q2": {"cash_flow": good.name}}


def test_a_second_folder_that_FOUND_the_page_keeps_the_cell_open(tmp_path):
    """⚠️ `SET-3`, measured 2026-09-11 — and it moved the corpus parse rate by 0.14 points.

    The rule *every recorded reason, not any of them* was applied to ONE folder at a time, so a
    single run recording nothing but `no such statement` settled the cell however many other
    runs had found the page and refused it on arithmetic. A two-layer
    `tesseract@200` + `tesseract@400+relax` cascade cannot classify most pages and records
    exactly that sentence: one such run over SHB retired SIX balance sheets that 115-layer runs
    were still refusing as `assets 300,000,000 != liabilities + equity 800,000,000`.

    ⚠️ **A SETTLED CELL LEAVES THE `winnable` DENOMINATOR**, so retiring a winnable one RAISES
    the reported rate — the corpus read 94.15 % with those six gone and 94.01 % with them back.
    """
    _folder(tmp_path, "20260909-000000", "Q3-2019", {
        "absent": ["balance_sheet"],
        "absent_reasons": {"balance_sheet": [
            ["onnx@200", "reconcile: assets 300,000,000 != liabilities + equity 800,000,000"]]},
    })
    _folder(tmp_path, "20260911-074400", "Q3-2019", {
        "absent": ["balance_sheet"],
        "absent_reasons": {"balance_sheet": [["tesseract@200", fin.NO_SUCH_STATEMENT]]},
    })

    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {}


def test_two_folders_that_BOTH_find_nothing_still_settle_the_cell(tmp_path):
    """The other half: agreement across folders is the ordinary case and must still settle.

    ⚠️ The folder NAMED is the last one to have said it, which is what the per-folder
    assignment left behind before `SET-3` — a caller reading that name sees no change.
    """
    _folder(tmp_path, "20260909-000000", "Q2-2009", {
        "absent_reasons": {"cash_flow": [["onnx@200", fin.NO_SUCH_STATEMENT]]}})
    last = _folder(tmp_path, "20260911-074400", "Q2-2009", {
        "absent_reasons": {"cash_flow": [["tesseract@200", fin.NO_SUCH_STATEMENT]]}})

    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {
        "2009-Q2": {"cash_flow": last.name}}


def test_a_dissenting_folder_settles_nothing_even_when_it_comes_FIRST(tmp_path):
    """Order must not decide it. The old code assigned as it read, so the LAST folder won."""
    _folder(tmp_path, "20260901-000000", "Q1-2015", {
        "absent_reasons": {"balance_sheet": [["tesseract@200", fin.NO_SUCH_STATEMENT]]}})
    _folder(tmp_path, "20260911-074400", "Q1-2015", {
        "absent_reasons": {"balance_sheet": [
            ["onnx@300", "reconcile: 6 figure(s) split across two boxes"]]}})

    assert job.settled_absences(tmp_path, "HOSE", "ACB") == {}


def test_one_report_settling_does_not_drag_its_neighbours_along(tmp_path):
    """A document settles per REPORT — the cash flow may be absent while the P&L is refused."""
    _folder(tmp_path, "20260911-074400", "Q2-2009", {
        "absent": ["cash_flow", "income_statement"],
        "absent_reasons": {
            "cash_flow": [["onnx@200", fin.NO_SUCH_STATEMENT]],
            "income_statement": [["onnx@200", "reconcile: no profit before tax"]]},
    })

    settled = job.settled_absences(tmp_path, "HOSE", "ACB")
    assert list(settled) == ["2009-Q2"]
    assert list(settled["2009-Q2"]) == ["cash_flow"]

def test_the_document_result_carries_the_reasons_into_its_json():
    """The artefact is the deliverable — a field the writer drops is a field nobody has."""
    task = job.DocumentTask(exchange="HOSE", symbol="ACB", period="Q2-2009", template="bank",
                            file="f.pdf", path="f.pdf", consolidated="False",
                            assurance="unaudited", cumulative=False)
    result = job.DocumentResult(task=task, seconds=1.0)
    result.absent = ["cash_flow"]
    result.absent_reasons = {"cash_flow": [("onnx@200", fin.NO_SUCH_STATEMENT)]}
    assert result.to_json()["absent_reasons"] == {
        "cash_flow": [["onnx@200", fin.NO_SUCH_STATEMENT]]}


def test_the_deepest_refusal_is_carried_BESIDE_the_first(tmp_path):
    """⚠️ **A CENSUS OF FIRST REFUSALS IS A CENSUS OF LAYER 1** (`RSN-1`).

    `absent_reasons` keeps the FIRST layer per distinct reason, which is the right record of
    what the cascade met and the wrong one for aiming a fix: layer 1 is `onnx@200` with no
    flags and no `join_lost`, the repair that arrives at layer 55 of 115. `SPR-1`'s sweep was
    aimed at 89 cells on exactly that evidence and returned ONE. `absent_deepest` is the same
    map built by overwriting, so the two together say whether a reason SURVIVED the cascade.
    """
    task = job.DocumentTask(exchange="HOSE", symbol="ACB", period="Q2-2009", template="bank",
                            file="f.pdf", path="f.pdf", consolidated="False",
                            assurance="unaudited", cumulative=False)
    result = job.DocumentResult(task=task, seconds=1.0)
    result.absent = ["cash_flow"]
    result.absent_reasons = {"cash_flow": [("onnx@200", "reconcile: no closing cash balance")]}
    result.absent_deepest = {"cash_flow": [("onnx@300+trunctotal+relax",
                                            "reconcile: no closing cash balance")]}
    payload = result.to_json()
    assert payload["absent_reasons"] == {
        "cash_flow": [["onnx@200", "reconcile: no closing cash balance"]]}
    assert payload["absent_deepest"] == {
        "cash_flow": [["onnx@300+trunctotal+relax", "reconcile: no closing cash balance"]]}


def test_a_run_that_recorded_no_deepest_map_serialises_an_EMPTY_one():
    """⚠️ An absent field must read as "this run predates v5", never as "layer 1 had the last
    word" — the same contract `absent_reasons` keeps for schema < 4 (§5 rule 2)."""
    task = job.DocumentTask(exchange="HOSE", symbol="ACB", period="Q2-2009", template="bank",
                            file="f.pdf", path="f.pdf", consolidated="False",
                            assurance="unaudited", cumulative=False)
    result = job.DocumentResult(task=task, seconds=1.0)
    assert result.to_json()["absent_deepest"] == {}
