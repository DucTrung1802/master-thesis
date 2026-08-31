"""Pure tests for `pdf_ocr_merge` — no PDF, no network, no OCR engine.

⚠️ **THE REFUSALS ARE THE FEATURE.** Merging a run folder into `raw_data/` gives back the risk
`pdf_ocr_job` was built to remove, and each refusal here exists for a measurement, not for
caution. A test that only checked "the row lands on disk" would go green on every one of the
failures these pin.
"""
import csv
import json
import os
import shutil
from pathlib import Path

import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job
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


def _statement(layer="onnx@200", months=None, **values):
    """`months` is the span the run recorded — the default states none, which is both what a
    balance sheet gets (a stock has no span) and what a folder written before 2026-08-30 has.
    A test that cares about the span passes it."""
    return {"layer": layer, "items": len(values), "rows": 20, "rows_sha": "abc",
            "pages": [1], "unit": 1, "n_columns": 2, "cash_flow_method": "",
            "quarter_column": False, "months": months, "values": values}


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
    figure would put a 9-month total in a 3-month column with nothing saying so.

    ⚠️ **`months=12` IS WHAT MAKES THIS STATEMENT CUMULATIVE, NOT THE FOLDER FLAG.** Since
    2026-08-30 the run records the span it read, and the span outranks the index — a filing
    the index calls cumulative that prints its own quarter column is a quarter. With no PDF
    index on disk `_unfiled_priors` cannot show the priors were never filed, so the refusal
    stands, which is the direction §5 rule 2 requires."""
    folder = _run_folder(tmp_path, cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 4_807})})

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
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 4_807})})

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


def test_force_empty_band_is_how_a_NEW_TICKER_is_bootstrapped_at_all(root, tmp_path):
    """⚠️ `BND-1` CLOSES ON ITSELF WITHOUT THIS, and that is the whole reason it exists.

    `seed_history` rebuilds the band from the `pdf` rows ALREADY ON DISK, so a ticker parsed
    for the first time has none; the refusal above then skips every statement the run
    produced; nothing is written; the band stays empty; the next run refuses again. Measured
    on HOSE_BSR, 2026-08-30: a green 14-document Kaggle run created no CSV at all.

    ⚠️ It lifts a real guard — those figures passed no magnitude check — so it is a
    named argument and never a default. The test pins both halves.
    """
    folder = _run_folder(tmp_path, bands={r: {"True": 0, "False": 0} for r in fin.REPORTS},
                         accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 1_000})})

    assert merge.merge_run(folder, quiet=True).to_write == []       # the default still refuses

    report = merge.merge_run(folder, apply=True, force_empty_band=True, quiet=True)

    assert [d.report for d in report.to_write] == [fin.BALANCE_SHEET]


def test_force_empty_band_does_not_lift_the_OTHER_refusals(root, tmp_path):
    """One escape, one refusal. A cumulative income statement is refused for a reason that
    has nothing to do with the band, and bootstrapping a ticker must not smuggle it in."""
    folder = _run_folder(tmp_path, cumulative=True,
                         bands={r: {"True": 0, "False": 0} for r in fin.REPORTS},
                         accepted={fin.INCOME_STATEMENT: _statement(months=12,
                                                                   **{PBT: 5_000})})

    report = merge.merge_run(folder, force_empty_band=True, quiet=True)

    assert report.to_write == []
    assert "cumulative" in _reason(report, fin.INCOME_STATEMENT).reason


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
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 4_807})})
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

# ──────────────────────────────────────────────────────────────────────────────
# WHICH DISK — the statements directory is resolved from this file, not from the CWD
# ──────────────────────────────────────────────────────────────────────────────

def test_the_disk_it_compares_against_is_resolved_from_the_repo_not_the_cwd(
        tmp_path, monkeypatch):
    """⚠️ `on_disk="absent"` is a LEGITIMATE state (a ticker being bootstrapped, `BND-1`), so
    reading the wrong directory produced the same word as reading the right one and refusal 3
    silently could not fire.

    Measured 2026-08-30 on the BID Q4-2016 repair: the identical call planned **2 writes** from
    `src/` and **0** from the repo root. `kgpu merge` runs from `src/kaggle_gpu/`.
    """
    for name in ("PDFS_DIR", "FIN_DIR", "SCHEMA_DIR", "STATEMENTS_DIR", "TEMPLATES_INDEX"):
        monkeypatch.setattr(fin, name, getattr(fin, name))     # restored on teardown
    monkeypatch.setenv(job.DATA_ROOT_ENV, os.environ.get(job.DATA_ROOT_ENV, ""))
    monkeypatch.setattr(fin, "STATEMENTS_DIR", os.path.join("raw_data", "cafef",
                                                            "financials", "statements"))
    monkeypatch.chdir(tmp_path)                                # a cwd with no raw_data/

    merge.plan_merge(_run_folder(tmp_path))

    expected = job.DEFAULT_DATA_ROOT.resolve() / "financials" / "statements"
    assert Path(fin.STATEMENTS_DIR) == expected
    assert os.path.isabs(fin.STATEMENTS_DIR)


def test_an_absolute_statements_dir_is_a_DELIBERATE_root_and_is_left_alone(root, tmp_path):
    """The other half, and it is the one that keeps this test file honest.

    ⚠️ The first version of the anchor called `use_data_root` unconditionally, which pointed
    every `apply=True` test in this file at the real `raw_data/`. An absolute value was put
    there on purpose — by `pdf_ocr_job.run`, by an experiment harness, or by this fixture — and
    the merge must not overrule it.
    """
    before = fin.STATEMENTS_DIR
    assert os.path.isabs(before) and str(tmp_path) in before

    merge.plan_merge(_run_folder(tmp_path))

    assert fin.STATEMENTS_DIR == before


# ──────────────────────────────────────────────────────────────────────────────
# refusal 1, second half — a YTD figure NOTHING can ever split
#
# ⚠️ Refusing a cumulative income statement is right while a later run can do better. It is
# not right when no later run can: BSR filed no quarterly report for 2016 at all, so its
# Q4-2016 P&L read `missing` for a reason that was never going to change. The span column is
# what makes writing it safe — see `fin.DATA_COLS`."months".
# ──────────────────────────────────────────────────────────────────────────────


def _write_index(base, *filed, symbol="TST"):
    """A PDF index holding one consolidated filing per named period.

    `("Q4-2016", 5)` files it as CafeF quarter 5 — the annual — which `documents()` folds
    onto that year's Q4, exactly as it does for a real ticker.
    """
    path = base / "pdfs" / "index" / f"HOSE_{symbol}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["symbol", "exchange", "year", "quarter", "period", "name", "consolidated",
            "assurance", "half_year", "file_date", "bytes", "file", "path", "url"]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for entry in filed:
            period, quarter = entry if isinstance(entry, tuple) else (entry, int(entry[1]))
            year = period.split("-")[1]
            w.writerow({"symbol": symbol, "exchange": "HOSE", "year": year,
                        "quarter": quarter, "period": period, "name": period,
                        "consolidated": "True", "assurance": "audited",
                        "half_year": "False", "file_date": f"{year}-01-01", "bytes": 1,
                        "file": f"{period}.pdf", "path": f"files/HOSE_TST/{period}.pdf",
                        "url": ""})
    return path


def test_a_cumulative_pl_is_still_refused_when_its_priors_were_filed(root, tmp_path):
    """The common case, and the one the refusal was written for: an authoritative `build()`
    over the whole ticker CAN subtract Q1..Q3, so writing the year-to-date figure now would
    pre-empt a better answer with a worse one."""
    _write_index(root, "Q1-2016", "Q2-2016", "Q3-2016", ("Q4-2016", 5))
    folder = _run_folder(tmp_path, period="Q4-2016", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 4_807})})

    report = merge.merge_run(folder, quiet=True)

    assert report.to_write == []
    assert "WERE filed" in _reason(report, fin.INCOME_STATEMENT).reason


def test_a_cumulative_pl_whose_priors_were_never_filed_is_written_and_labelled(root, tmp_path):
    """⚠️ Nothing will ever subtract quarters that were never reported, so the choice is not
    "cumulative now or a quarter later" — it is "cumulative now or nothing, ever". Measured on
    BSR, whose only 2016 filing is the FY-2016 annual."""
    _write_index(root, ("Q4-2016", 5))
    folder = _run_folder(tmp_path, period="Q4-2016", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 4_807})})

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert [d.report for d in report.to_write] == [fin.INCOME_STATEMENT]
    row = _rows(fin.INCOME_STATEMENT)["Q4-2016"]
    assert row[PBT] == "4807"
    # the whole reason the write is allowed: the row says what span it covers
    assert row["months"] == "12"
    assert "never filed" in _reason(report, fin.INCOME_STATEMENT).note


def test_a_missing_index_keeps_the_refusal_rather_than_assuming(root, tmp_path):
    """⚠️ `_unfiled_priors` cannot show a quarter was never filed without the index, and §5
    rule 2 says an absent measurement is absent — never read in the permissive direction."""
    folder = _run_folder(tmp_path, period="Q4-2016", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 4_807})})

    assert merge.merge_run(folder, quiet=True).to_write == []


def test_a_half_year_filing_that_prints_a_quarter_column_is_not_refused(root, tmp_path):
    """⚠️ THE DOCUMENT OUTRANKS THE INDEX. VCB Q2-2014 prints its quarter column beside the
    cumulative one, so column 0 is already the quarter — `months = 3` — and refusing it on
    the index flag alone was an over-refusal `build()` has never made."""
    _write_index(root, "Q1-2014", "Q2-2014")
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=3, **{PBT: 1_345})})

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert [d.report for d in report.to_write] == [fin.INCOME_STATEMENT]
    assert _rows(fin.INCOME_STATEMENT)["Q2-2014"]["months"] == "3"


def test_a_run_folder_with_no_span_falls_back_to_the_cumulative_flag(root, tmp_path):
    """A folder written before `months` existed records no span. The index flag is then the
    only thing available, and it must still refuse."""
    _write_index(root, "Q1-2016", "Q2-2016", "Q3-2016", ("Q4-2016", 5))
    statement = _statement(**{PBT: 4_807})
    statement.pop("months", None)
    folder = _run_folder(tmp_path, period="Q4-2016", cumulative=True,
                         accepted={fin.INCOME_STATEMENT: statement})

    assert merge.merge_run(folder, quiet=True).to_write == []


# ──────────────────────────────────────────────────────────────────────────────
# filling in a span disk does not record
# ──────────────────────────────────────────────────────────────────────────────


def test_a_known_span_fills_an_unrecorded_one_without_touching_a_figure(root, tmp_path):
    """Every row parsed before `months` existed carries a blank there. Same figures, same
    layer, blank span: that is not `identical`, it is incomplete."""
    _write_disk(fin.CASH_FLOW, "Q4-2016", **{PBT: 500})
    folder = _run_folder(tmp_path, period="Q4-2016", accepted={
        fin.CASH_FLOW: _statement(months=12, **{PBT: 500})})

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert [d.report for d in report.to_write] == [fin.CASH_FLOW]
    row = _rows(fin.CASH_FLOW)["Q4-2016"]
    assert row["months"] == "12"
    assert row[PBT] == "500"          # the figure did not move


def test_a_run_that_knows_no_span_never_blanks_one_disk_already_has(root, tmp_path):
    """⚠️ Only ever in one direction. A blank overwriting a known 12 would delete the one
    thing the column exists to say."""
    _write_disk(fin.CASH_FLOW, "Q4-2016", months="12", **{PBT: 500})
    statement = _statement(**{PBT: 500})
    statement.pop("months", None)
    folder = _run_folder(tmp_path, period="Q4-2016",
                         accepted={fin.CASH_FLOW: statement})

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert report.to_write == []
    assert _rows(fin.CASH_FLOW)["Q4-2016"]["months"] == "12"


# ──────────────────────────────────────────────────────────────────────────────
# recording the outcome back into the run folder
#
# ⚠️ **THE ARTEFACT USED TO SAY `merged_into_csv: false` ON EVERY KAGGLE RUN AND COULD NEVER
# HAVE SAID ANYTHING ELSE.** `metadata.json` is written by the process that PARSES; on a
# Kaggle round trip that is a worker with no path to this disk, and the pull that does the
# merge wrote nothing back. A run that upserted 126 statements and one that upserted none
# carried the identical file. These pin the fix.
# ──────────────────────────────────────────────────────────────────────────────


def test_record_merge_writes_what_the_merge_actually_did(root, tmp_path):
    folder = _run_folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 900})})

    report = merge.merge_run(folder, apply=True, quiet=True)
    path = merge.record_merge(folder, report)

    meta = json.loads(path.read_text(encoding="utf-8"))
    assert meta["merge"]["statements_written"] == 1
    assert meta["merge"]["periods_written"] == ["Q3-2014"]
    assert meta["inputs"]["merged_into_csv"] is True
    written = [d for d in meta["merge"]["events"][0]["decisions"] if d["action"] == "write"]
    assert [d["report"] for d in written] == [fin.BALANCE_SHEET]


def test_a_merge_that_refused_everything_is_NOT_recorded_as_merged(root, tmp_path):
    """⚠️ The defect one level down. An applied merge every refusal turned away is not a
    merge, and a flag that says otherwise re-creates exactly what this block exists to fix."""
    folder = _run_folder(
        tmp_path, accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 900})},
        bands={r: {"True": 0, "False": 0} for r in fin.REPORTS})

    report = merge.merge_run(folder, apply=True, quiet=True)
    path = merge.record_merge(folder, report)

    meta = json.loads(path.read_text(encoding="utf-8"))
    assert report.to_write == []
    assert meta["merge"]["statements_written"] == 0
    assert meta["merge"]["statements_skipped"] == 3
    assert meta["inputs"]["merged_into_csv"] is False


def test_a_second_merge_APPENDS_rather_than_replacing(root, tmp_path):
    """⚠️ A folder is legitimately merged more than once — the CTG bootstrap took three
    calls, one per report, each carrying only the periods an external screen had cleared.
    A block that overwrote would have kept the last and lost the other two."""
    folder = _run_folder(tmp_path, accepted={
        fin.BALANCE_SHEET: _statement(**{ASSETS: 900}),
        fin.CASH_FLOW: _statement(**{PBT: 7})})

    first = merge.merge_run(folder, apply=True, reports=[fin.BALANCE_SHEET], quiet=True)
    merge.record_merge(folder, first)
    second = merge.merge_run(folder, apply=True, reports=[fin.CASH_FLOW], quiet=True)
    path = merge.record_merge(folder, second)

    meta = json.loads(path.read_text(encoding="utf-8"))
    assert len(meta["merge"]["events"]) == 2
    assert meta["merge"]["statements_written"] == 2
    assert sorted(d["report"] for ev in meta["merge"]["events"]
                  for d in ev["decisions"] if d["action"] == "write") == sorted(
        [fin.BALANCE_SHEET, fin.CASH_FLOW])


def test_a_later_event_supersedes_an_earlier_one_for_the_same_statement(root, tmp_path):
    """The union is keyed by (period, report) — the question a reader asks is "is this
    quarter on disk?", and the last word on it is the one that answers."""
    folder = _run_folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 900})})

    merge.record_merge(folder, merge.merge_run(folder, apply=True, quiet=True))
    # The second pass finds the row identical and skips it — the statement is still on disk.
    path = merge.record_merge(folder, merge.merge_run(folder, apply=True, quiet=True))

    meta = json.loads(path.read_text(encoding="utf-8"))
    assert len(meta["merge"]["events"]) == 2
    assert meta["merge"]["statements_written"] == 0     # the LAST word: nothing was written
    assert meta["inputs"]["merged_into_csv"] is True    # ...but it had been, and stays true


def test_record_merge_is_silent_when_there_is_no_metadata_yet(root, tmp_path):
    """A LOCAL run upserts quarter by quarter and writes its metadata once, at the end —
    there is nothing to amend while it is still going, and inventing a file would leave a
    run folder whose metadata predates its own results."""
    folder = _run_folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 900})})
    (folder / "metadata.json").unlink()

    report = merge.merge_run(folder, apply=True, quiet=True)

    assert merge.record_merge(folder, report) is None


def test_the_event_names_the_file_total_for_what_it_is(root, tmp_path):
    """⚠️ `_write` returns the number of `pdf` rows in the WHOLE csv after the upsert, not
    the number this merge produced. Summing it as "statements written" reports a ticker's
    entire history as one run's work — which is the mistake this key's name prevents."""
    _write_disk(fin.BALANCE_SHEET, "Q1-2014", **{ASSETS: 100})
    folder = _run_folder(tmp_path, accepted={fin.BALANCE_SHEET: _statement(**{ASSETS: 900})})

    report = merge.merge_run(folder, apply=True, quiet=True)
    event = merge.merge_event(report)

    assert "written" not in event
    assert event["pdf_rows_on_disk"][fin.BALANCE_SHEET] == 2    # the file holds two
    assert merge.merge_block([event])["statements_written"] == 1  # this run wrote one


# ──────────────────────────────────────────────────────────────────────────────
# refusal 1, the other half — DE-CUMULATING a year-to-date P&L from the rows on disk
#
# ⚠️ Refusal 1 used to be the end of the road: a cumulative income statement whose priors WERE
# filed was left `missing` until somebody ran a multi-hour authoritative `build()` over the
# whole ticker. BSR's Q2/Q4 income statements sat that way with their figures already in a run
# folder. Since 2026-08-31 the merge does the subtraction itself — on operands that are already
# `pdf` rows on disk, so it needs no OCR — and refuses only when an operand is missing or its
# SPAN is not a known three months.
# ──────────────────────────────────────────────────────────────────────────────


def _disk_rows(report, rows):
    """Several rows already on disk. `_write_disk` writes one and truncates; a de-cumulation
    needs Q1..Q(q-1) present at once."""
    path = fin.statement_path(TEMPLATE, report, "HOSE", "TST")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    columns = list(fin.DATA_COLS)
    for _, values in rows:
        columns += [c for c in values if c not in columns]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for period, values in rows:
            writer.writerow({"symbol": "TST", "exchange": "HOSE", "template": TEMPLATE,
                             "period": period, "year": period.split("-")[1],
                             "quarter": period[1], "method": "onnx@200", "source": "pdf",
                             **values})
    return path


def test_a_half_year_pnl_is_de_cumulated_against_the_Q1_on_disk(root, tmp_path):
    """The whole point: Q2 = the six-month figure minus Q1, and Q1 needs no recorded span
    because a first quarter's year-to-date IS the quarter."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 400})])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000})})

    report = merge.merge_run(folder, apply=True, quiet=True)
    decision = _reason(report, fin.INCOME_STATEMENT)

    assert decision.writing
    assert decision.values == {PBT: 600}
    assert decision.months == 3
    assert "de-cumulated" in decision.note
    assert _rows(fin.INCOME_STATEMENT)["Q2-2014"][PBT] == "600"
    assert _rows(fin.INCOME_STATEMENT)["Q2-2014"]["months"] == "3"


def test_a_Q4_is_de_cumulated_against_a_Q2_THIS_PASS_recovered(root, tmp_path):
    """⚠️ The chain, and the reason `_documents` yields oldest first. BSR's Q4-2019 needs
    Q2-2019, which is `missing` on disk until the same merge writes it — so the operand comes
    from `decumulated`, the quarters this pass has already decided."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2019", {PBT: 100, "months": 3}),
                                      ("Q3-2019", {PBT: 300, "months": 3})])
    folder = tmp_path / "20260829-000000__hose_tst__pdf_ocr"
    for period, months, pbt in (("Q2-2019", 6, 300), ("Q4-2019", 12, 1_000)):
        sub = _run_folder(tmp_path, period=period, cumulative=True, accepted={
            fin.INCOME_STATEMENT: _statement(months=months, **{PBT: pbt})})
        assert sub == folder

    report = merge.merge_run(folder, apply=True, quiet=True)
    written = {d.period: d for d in report.to_write}

    assert written["Q2-2019"].values == {PBT: 200}          # 300 - Q1 100
    assert written["Q4-2019"].values == {PBT: 400}          # 1000 - (100 + 200 + 300)
    assert _rows(fin.INCOME_STATEMENT)["Q4-2019"][PBT] == "400"


def test_a_prior_whose_span_is_UNRECORDED_refuses(root, tmp_path):
    """⚠️ §5 rule 2 at the column. Most of the corpus predates `months`, and reading a blank as
    3 would infer a measurement nobody took — a six-month prior subtracted from a twelve-month
    figure yields a number that is neither, and nothing downstream could tell. Re-parsing the
    prior records its span, after which this same merge succeeds."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 100}), ("Q2-2014", {PBT: 200}),
                                      ("Q3-2014", {PBT: 300})])
    folder = _run_folder(tmp_path, period="Q4-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=12, **{PBT: 1_000})})

    decision = _reason(merge.merge_run(folder, quiet=True), fin.INCOME_STATEMENT)

    assert not decision.writing
    assert "unrecorded" in decision.reason and "Q2-2014" in decision.reason


def test_a_prior_that_is_MISSING_on_disk_refuses(root, tmp_path):
    """Subtracting a blank returns the year-to-date figure unchanged — the exact wrong write,
    and it would look like an ordinary quarter."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 100, "months": 3})])
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {"source": "missing"})])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000})})

    decision = _reason(merge.merge_run(folder, quiet=True), fin.INCOME_STATEMENT)

    assert not decision.writing
    assert "Q1-2014" in decision.reason


def test_a_column_no_prior_carries_is_DROPPED_not_treated_as_zero(root, tmp_path):
    """`FinancialsBuilder._decumulate`'s own rule: a line the prior filing printed and this
    parse missed would otherwise have its whole year-to-date value land in a 3-month cell."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 400})])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000, "i_1_tien": 55})})

    decision = _reason(merge.merge_run(folder, quiet=True), fin.INCOME_STATEMENT)

    assert decision.values == {PBT: 600}
    assert "i_1_tien" not in decision.values


def test_a_year_to_date_row_whose_priors_were_NEVER_filed_is_still_kept_and_labelled(
        root, tmp_path, monkeypatch):
    """The other branch is untouched: nothing will ever subtract quarters that were not
    reported, so the choice is "cumulative now or nothing, ever" and the span says which."""
    monkeypatch.setattr(merge, "_unfiled_priors",
                        lambda builder, exchange, symbol, period: ["Q1-2014"])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000})})

    decision = _reason(merge.merge_run(folder, apply=True, quiet=True), fin.INCOME_STATEMENT)

    assert decision.writing and decision.months == 6
    assert decision.values is None            # the run's own figures, unchanged
    assert _rows(fin.INCOME_STATEMENT)["Q2-2014"][PBT] == "1000"


def test_the_DIFFERS_check_scores_the_DE_CUMULATED_figures_not_the_year_to_date_ones(
        root, tmp_path):
    """⚠️ Comparing the six-month figure against a disk row holding the quarter would report a
    difference in every column and refuse a row that is right.

    Here the two AGREE (1,000 - 400 = 600, which is what disk holds), so `changed` is empty —
    and the write that remains is the span-fill branch recording `months=3` on a row that
    predates the column. A figures-only disagreement would have refused."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 400}),
                                      ("Q2-2014", {PBT: 600})])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000})})

    decision = _reason(merge.merge_run(folder, quiet=True), fin.INCOME_STATEMENT)

    assert decision.changed == {}, "the year-to-date figures were compared against the quarter"
    assert "recording the span" in decision.reason


def test_a_de_cumulated_figure_that_DISAGREES_with_disk_is_still_refused(root, tmp_path):
    """Refusal 3 judges the de-cumulated row exactly as it judges any other: two runs
    disagreeing about a quarter is not settled by preferring the newer one."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 400, "months": 3}),
                                      ("Q2-2014", {PBT: 999, "months": 3})])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000})})

    decision = _reason(merge.merge_run(folder, quiet=True), fin.INCOME_STATEMENT)

    assert not decision.writing
    assert "DIFFERS" in decision.reason
    assert decision.changed == {PBT: (999, 600)}


def test_a_de_cumulated_quarter_REFUSED_by_a_later_gate_is_not_a_later_quarter_s_operand(
        root, tmp_path):
    """⚠️ Refusals 2-4 run AFTER refusal 1 has done the subtraction, so a Q2 that was split and
    then refused must not be subtracted from Q4 — disk still holds the other value, and the two
    would disagree about the same year. Q4 falls back to the disk read, which is what a reader
    can check.

    Here Q2-2019 de-cumulates to 200 and is refused as DIFFERS against a disk row of 999. Q4
    must then use 999, not 200."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2019", {PBT: 100, "months": 3}),
                                      ("Q2-2019", {PBT: 999, "months": 3}),
                                      ("Q3-2019", {PBT: 300, "months": 3})])
    folder = tmp_path / "20260829-000000__hose_tst__pdf_ocr"
    for period, months, pbt in (("Q2-2019", 6, 300), ("Q4-2019", 12, 2_000)):
        assert _run_folder(tmp_path, period=period, cumulative=True, accepted={
            fin.INCOME_STATEMENT: _statement(months=months, **{PBT: pbt})}) == folder

    report = merge.merge_run(folder, apply=False, quiet=True)
    def pick(period):
        return next(d for d in report.decisions
                    if d.period == period and d.report == fin.INCOME_STATEMENT)

    q2, q4 = pick("Q2-2019"), pick("Q4-2019")

    assert not q2.writing and "DIFFERS" in q2.reason
    assert q4.values == {PBT: 601}, "Q4 must subtract disk's 999, not the refused 200"


def test_a_de_cumulated_row_is_not_rewritten_on_every_later_merge(root, tmp_path):
    """⚠️ THE SPAN COMPARED MUST BE THE ONE THIS DECISION WILL WRITE, NOT THE RUN'S. A quarter
    refusal 1 de-cumulated covers THREE months where the filing printed 6, and `_write` uses
    `Decision.months`. Comparing the RUN's span instead made a correct `months=3` row look like
    it was missing the filing's `6`, so it was "filled" on every merge for ever, under a
    message naming a figure it was not writing. Measured on BSR 2026-08-31."""
    _disk_rows(fin.INCOME_STATEMENT, [("Q1-2014", {PBT: 400, "months": 3}),
                                      ("Q2-2014", {PBT: 600, "months": 3})])
    folder = _run_folder(tmp_path, period="Q2-2014", cumulative=True, accepted={
        fin.INCOME_STATEMENT: _statement(months=6, **{PBT: 1_000})})

    decision = _reason(merge.merge_run(folder, apply=False, quiet=True),
                       fin.INCOME_STATEMENT)

    assert not decision.writing, "a de-cumulated row already on disk must settle"
    assert decision.reason == "identical to the row already on disk"
