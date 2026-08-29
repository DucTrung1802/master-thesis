"""Pure tests for `pdf_ocr_merge` — no PDF, no network, no OCR engine.

⚠️ **THE REFUSALS ARE THE FEATURE.** Merging a run folder into `raw_data/` gives back the risk
`pdf_ocr_job` was built to remove, and each refusal here exists for a measurement, not for
caution. A test that only checked "the row lands on disk" would go green on every one of the
failures these pin.
"""
import csv
import json
import shutil
from pathlib import Path

import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_merge as merge

TEMPLATE = "bank"
ASSETS = fin.FinancialsBuilder.C_ASSETS[0]
PBT = fin.FinancialsBuilder.C_PBT[0]


REPO_SCHEMA = Path(__file__).resolve().parents[2] / "raw_data" / "cafef" / "financials" / "schema"


@pytest.fixture()
def root(tmp_path, monkeypatch):
    """A CafeF data root the merge can write into, with nothing else in it.

    ⚠️ The real charts of accounts are copied in rather than faked: `_write` orders its columns
    by the chart, so a fake one would test a column order that ships nowhere.
    """
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


def _run_folder(tmp_path, *, period="Q3-2014", accepted=None, cumulative=False,
                bands=None, error=None, consolidated="True"):
    """A minimal `pdf_ocr_job` run folder — the shape `merge_run` reads."""
    folder = tmp_path / "20260829-000000__hose_tst__pdf_ocr"
    (folder / "documents").mkdir(parents=True, exist_ok=True)
    (folder / "metadata.json").write_text(json.dumps({"run_id": folder.name}), "utf-8")
    doc = {
        "exchange": "HOSE", "symbol": "TST", "period": period, "template": TEMPLATE,
        "document": f"{period}.pdf", "consolidated": consolidated, "assurance": "unaudited",
        "cumulative": cumulative, "seconds": 1.0,
        "accepted": accepted if accepted is not None else {},
        "absent": [r for r in fin.REPORTS if r not in (accepted or {})],
        "facts": {"publish_date": "2014-11-14", "shares_authorized": None,
                  "shares_issued": None, "shares_outstanding": None},
        "history_sizes": bands or {r: {"True": 12, "False": 0} for r in fin.REPORTS},
        "open_ref": None, "error": error, "log": [],
    }
    (folder / "documents" / f"HOSE_TST__{period}.json").write_text(
        json.dumps(doc), "utf-8")
    return folder


def _statement(layer="onnx@200", **values):
    return {"layer": layer, "items": len(values), "rows": 20, "rows_sha": "abc",
            "pages": [1], "unit": 1, "n_columns": 2, "cash_flow_method": "",
            "quarter_column": False, "values": values}


def _write_disk(report, period, source="pdf", method="onnx@200", **values):
    """One row already on disk, in the layout `_existing` reads."""
    path = fin.statement_path(TEMPLATE, report, "HOSE", "TST")
    import os

    os.makedirs(os.path.dirname(path), exist_ok=True)
    columns = list(fin.DATA_COLS) + [c for c in values if c not in fin.DATA_COLS]
    row = {"symbol": "TST", "exchange": "HOSE", "template": TEMPLATE, "period": period,
           "year": period.split("-")[1], "quarter": period[1], "method": method,
           "source": source, **values}
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(row)
    return path


def _reason(report_obj, name):
    """The decision for one report — `decisions` carries all three and index 0 is arbitrary."""
    return next(d for d in report_obj.decisions if d.report == name)


def _rows(report):
    path = fin.statement_path(TEMPLATE, report, "HOSE", "TST")
    with open(path, encoding="utf-8-sig") as f:
        return {r["period"]: r for r in csv.DictReader(f)}


# ──────────────────────────────────────────────────────────────────────────────
# the dry run
# ──────────────────────────────────────────────────────────────────────────────


def test_apply_false_writes_nothing_and_says_what_it_would_write(root, tmp_path):
    """⚠️ The merge WRITES by default since 2026-08-29; `apply=False` is the explicit look."""
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, apply=False, quiet=True)

    assert [d.report for d in report.to_write] == [fin.BALANCE_SHEET]
    assert report.applied is False
    import os
    assert not os.path.exists(fin.statement_path(TEMPLATE, fin.BALANCE_SHEET, "HOSE", "TST"))


def test_apply_writes_the_row_and_records_its_provenance(root, tmp_path):
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(layer="onnx@300", **{ASSETS: 1_234})})

    merge.merge_run(folder, apply=True, quiet=True)

    row = _rows(fin.BALANCE_SHEET)["Q3-2014"]
    assert row[ASSETS] == "1234"
    assert row["source"] == "pdf"          # rule 24: there is no other origin on offer
    assert row["method"] == "onnx@300"     # the layer that produced it, not a default
    assert row["document"] == "Q3-2014.pdf"
    assert row["consolidated"] == "True"
    assert row["publish_date"] == "2014-11-14"


def test_a_backup_is_taken_before_anything_is_written(root, tmp_path):
    """⚠️ `SAN-1` was found by diffing against a backup, not by reading a log."""
    _write_disk(fin.BALANCE_SHEET, "Q1-2014", **{ASSETS: 900})
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert report.backup is not None and report.backup.is_dir()
    backed_up = list(report.backup.glob("*.csv"))
    assert backed_up, "the pre-merge state must be recoverable"
    with open(backed_up[0], encoding="utf-8-sig") as f:
        assert next(csv.DictReader(f))[ASSETS] == "900"


# ──────────────────────────────────────────────────────────────────────────────
# refusal 1 — a cumulative income statement is not a quarter
# ──────────────────────────────────────────────────────────────────────────────


def test_a_cumulative_income_statement_is_refused(root, tmp_path):
    """An annual filing prints the year to date and the CSV column holds the quarter.
    `pdf_ocr_job` cannot de-cumulate — a one-document run has no Q1..Q(q-1) — so writing its
    figure would put a 9-month total in a 3-month column with nothing saying so."""
    folder = _run_folder(tmp_path, cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(**{PBT: 4_807})})

    report = merge.merge_run(folder, quiet=True)

    assert report.to_write == []
    assert "cumulative" in _reason(report, fin.INCOME_STATEMENT).reason


def test_the_cumulative_refusal_is_scoped_to_the_income_statement(root, tmp_path):
    """A balance sheet at 31 December IS the Q4 balance sheet, and a cash flow is cumulative
    to year end either way — only the P&L needs de-cumulating."""
    folder = _run_folder(tmp_path, cumulative=True, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    assert [d.report for d in merge.merge_run(folder, quiet=True).to_write] \
        == [fin.BALANCE_SHEET]


def test_force_cumulative_is_available_and_is_not_the_default(root, tmp_path):
    folder = _run_folder(tmp_path, cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(**{PBT: 4_807})})

    assert merge.merge_run(folder, quiet=True).to_write == []
    assert len(merge.merge_run(folder, force_cumulative=True, quiet=True).to_write) == 1


# ──────────────────────────────────────────────────────────────────────────────
# refusal 2 — an empty band means `sane` failed open
# ──────────────────────────────────────────────────────────────────────────────


def test_a_statement_whose_magnitude_band_was_empty_is_refused(root, tmp_path):
    """With no band `sane` cannot reject anything, so the figure passed no guard at all —
    which is the documented way a subset run writes a wrong figure (§6-2-octodecies)."""
    folder = _run_folder(tmp_path, bands={r: {"True": 0, "False": 0} for r in fin.REPORTS},
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, quiet=True)

    assert report.to_write == []
    assert "EMPTY" in _reason(report, fin.BALANCE_SHEET).reason


def test_the_band_is_read_for_the_ENTITY_the_filing_actually_used(root, tmp_path):
    """⚠️ `sane` bands per entity — a standalone company is not the consolidated group — so a
    consolidated band says nothing about a standalone filing."""
    folder = _run_folder(tmp_path, consolidated="False",
                         bands={r: {"True": 12, "False": 0} for r in fin.REPORTS},
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    assert merge.merge_run(folder, quiet=True).to_write == []


# ──────────────────────────────────────────────────────────────────────────────
# refusal 3 — two runs disagreeing is not an upsert
# ──────────────────────────────────────────────────────────────────────────────


def test_a_figure_that_differs_from_a_good_row_on_disk_is_refused(root, tmp_path):
    """`compare()` already scored it. A `DIFFERS` verdict means two runs disagree about a
    number, and taking the newer one is a coin toss dressed as an upsert."""
    _write_disk(fin.BALANCE_SHEET, "Q3-2014", **{ASSETS: 999})
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, quiet=True)

    assert report.to_write == []
    assert "DIFFERS" in _reason(report, fin.BALANCE_SHEET).reason
    assert _reason(report, fin.BALANCE_SHEET).changed[ASSETS] == (999, 1000)
    assert _rows(fin.BALANCE_SHEET)["Q3-2014"][ASSETS] == "999"


def test_an_identical_row_is_skipped_rather_than_rewritten(root, tmp_path):
    """Rewriting a row that is already right is a diff nobody can read past."""
    _write_disk(fin.BALANCE_SHEET, "Q3-2014", **{ASSETS: 1_000})
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, quiet=True)

    assert report.to_write == []
    assert "identical" in _reason(report, fin.BALANCE_SHEET).reason


def test_a_missing_row_on_disk_is_a_RECOVERY_and_is_written(root, tmp_path):
    """The case this feature exists for: disk says `missing`, the run read the filing."""
    _write_disk(fin.BALANCE_SHEET, "Q3-2014", source="missing", method="")
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert [d.report for d in report.to_write] == [fin.BALANCE_SHEET]
    assert _rows(fin.BALANCE_SHEET)["Q3-2014"]["source"] == "pdf"


# ──────────────────────────────────────────────────────────────────────────────
# what a merge must never touch
# ──────────────────────────────────────────────────────────────────────────────


def test_a_quarter_the_run_did_not_produce_keeps_whatever_disk_holds(root, tmp_path):
    """⚠️ The upsert is `_write(merge=True)`, and this is the property that makes it safe:
    four measured builds silently downgraded a quarter that was only along for the ride."""
    _write_disk(fin.BALANCE_SHEET, "Q1-2014", method="onnx@400", **{ASSETS: 777})
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    merge.merge_run(folder, apply=True, quiet=True)

    rows = _rows(fin.BALANCE_SHEET)
    assert rows["Q1-2014"][ASSETS] == "777"
    assert rows["Q1-2014"]["method"] == "onnx@400"     # every column, not just the figure
    assert rows["Q3-2014"][ASSETS] == "1000"


def test_a_run_that_errored_is_refused_whole(root, tmp_path):
    """A raise mid-document can still leave an `accepted` block for the statements that got
    through; deciding which half survived is not a judgement this module may make."""
    folder = _run_folder(tmp_path, error="RuntimeError: boom", accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    report = merge.merge_run(folder, quiet=True)

    assert report.to_write == []
    assert "errored" in _reason(report, fin.BALANCE_SHEET).reason


def test_a_report_filter_narrows_the_merge_without_touching_the_others(root, tmp_path):
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000}),
        fin.INCOME_STATEMENT: _statement(**{PBT: 500})})

    report = merge.merge_run(folder, reports=[fin.INCOME_STATEMENT], quiet=True)

    assert [d.report for d in report.to_write] == [fin.INCOME_STATEMENT]


def test_writing_is_the_DEFAULT_and_the_refusals_are_what_keeps_it_honest(root, tmp_path):
    """⚠️ The default flipped on 2026-08-29 by request. Pin it: a caller that passes nothing
    gets a WRITE, and a caller whose statement trips a refusal still gets nothing — the safety
    is the refusal, not the extra argument."""
    ok = _run_folder(tmp_path / "ok", accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})
    merge.merge_run(ok, quiet=True)
    assert _rows(fin.BALANCE_SHEET)["Q3-2014"][ASSETS] == "1000"

    refused = _run_folder(tmp_path / "refused", period="Q4-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(**{PBT: 4_807})})
    report = merge.merge_run(refused, quiet=True)
    assert report.to_write == []
    assert report.applied is False


# ──────────────────────────────────────────────────────────────────────────────
# merging quarter by quarter, as a long run proceeds
# ──────────────────────────────────────────────────────────────────────────────


def test_only_the_named_quarters_record_is_parsed(tmp_path):
    """⚠️ Each record carries a `row_dump` of every row the OCR read, so re-parsing the whole
    folder once per quarter would make a 70-quarter run read 2,450 files for no reason."""
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    _run_folder(tmp_path, period="Q4-2014",
                accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 200})})
    assert [d["period"] for d in merge._documents(folder, ["Q4-2014"])] == ["Q4-2014"]
    assert len(merge._documents(folder)) == 2


def test_a_quarter_written_earlier_survives_the_next_quarters_merge(root, tmp_path):
    """⚠️ **THE INTERRUPTION GUARANTEE.** `_write` renders to a `.tmp` and `os.replace`s it, and
    only the quarters a merge PRODUCED are rewritten — so stopping a 12-hour run at hour 6
    keeps every quarter that finished."""
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    merge.merge_run(folder, periods=["Q3-2014"], quiet=True)
    _run_folder(tmp_path, period="Q4-2014",
                accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 200})})
    merge.merge_run(folder, periods=["Q4-2014"], backup=False, quiet=True)

    rows = _rows(fin.BALANCE_SHEET)
    assert rows["Q3-2014"][ASSETS] == "100"
    assert rows["Q4-2014"][ASSETS] == "200"
    assert rows["Q3-2014"]["source"] == rows["Q4-2014"]["source"] == "pdf"


def test_the_backup_can_be_taken_once_for_a_run_rather_than_once_per_quarter(root, tmp_path):
    """Seventy timestamped copies of three CSVs answer "what did this run change?" worse than
    one taken before the first write — so `pdf_ocr_job.run` asks for exactly one."""
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    first = merge.merge_run(folder, periods=["Q3-2014"], quiet=True)
    _run_folder(tmp_path, period="Q4-2014",
                accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 200})})
    second = merge.merge_run(folder, periods=["Q4-2014"], backup=False, quiet=True)

    assert first.backup is not None and first.backup.is_dir()
    assert second.backup is None
    assert second.applied is True
    assert "taken earlier in this run" in "\n".join(second.lines())


def test_the_default_is_still_a_backup_every_time(root, tmp_path):
    """`backup=False` is for ONE caller. Nothing else may quietly stop making a merge
    reversible — `SAN-1` was found by diffing against a backup, not by reading a log."""
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    assert merge.merge_run(folder, quiet=True).backup is not None


def test_a_document_whose_layers_RAISED_is_refused_whole(root, tmp_path):
    """⚠️ Refused for all three reports, not statement by statement: the broken tool was broken
    for the whole document, so whichever layer won did so by default."""
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    doc = folder / "documents" / "HOSE_TST__Q3-2014.json"
    payload = json.loads(doc.read_text(encoding="utf-8"))
    payload["engine_errors"] = [["onnx@200", "SSLError: certificate has expired"]]
    doc.write_text(json.dumps(payload), encoding="utf-8")

    report = merge.merge_run(folder, quiet=True)
    assert [d.action for d in report.decisions] == ["skip"] * 3
    assert "RAISED rather than refusing" in _reason(report, fin.BALANCE_SHEET).reason
    assert report.backup is None                    # nothing written, nothing to back up


def test_force_engine_errors_exists_and_is_not_the_default(root, tmp_path):
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    doc = folder / "documents" / "HOSE_TST__Q3-2014.json"
    payload = json.loads(doc.read_text(encoding="utf-8"))
    payload["engine_errors"] = [["onnx@200", "SSLError"]]
    doc.write_text(json.dumps(payload), encoding="utf-8")

    forced = merge.merge_run(folder, force_engine_errors=True, quiet=True)
    assert _reason(forced, fin.BALANCE_SHEET).writing


def test_a_run_folder_written_before_the_field_existed_still_merges(root, tmp_path):
    """Absent is read as "none recorded" — the only reading available. Refusing every v1 folder
    would break `kgpu merge` on artefacts already pulled, and they carry `schema_version`."""
    folder = _run_folder(tmp_path, period="Q3-2014",
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 100})})
    payload = json.loads(
        (folder / "documents" / "HOSE_TST__Q3-2014.json").read_text(encoding="utf-8"))
    assert "engine_errors" not in payload
    assert _reason(merge.merge_run(folder, apply=False, quiet=True),
                   fin.BALANCE_SHEET).writing
