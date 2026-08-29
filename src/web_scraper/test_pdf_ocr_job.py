"""`pdf_ocr_job` — the portable parse job, pinned without a PDF, a network or an OCR engine.

What is worth pinning here is NOT the parse: that is `_parse_cascaded`, which has its own
tests and its own 47 layers. It is the four decisions this module makes AROUND the parse, each
of which changes a verdict silently when it is wrong:

  * the magnitude band handed to `sane` — an empty one makes the gate FAIL OPEN, which is the
    documented way a subset run writes a wrong figure (CLAUDE.md §6-2-octodecies);
  * the ENTITY split of that band — a standalone company is legitimately smaller than the
    consolidated group, and pooling them makes the band meaningless in both directions
    (`SAN-1`);
  * the cascade ORDER, because a half-right layer that passes the gates ends the cascade;
  * the refusal to score a CUMULATIVE income statement against a de-cumulated row on disk.
"""
import csv
import json
import shutil
from pathlib import Path

import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job
from web_scraper import pdf_ocr_merge
from web_scraper.cafef_financials import FinancialsBuilder


TEMPLATE = "bank"
ASSETS = fin.FinancialsBuilder.C_ASSETS[0]
PBT = fin.FinancialsBuilder.C_PBT[0]
CLOSE = fin.FinancialsBuilder.C_CASH_CLOSE[0]


def _write_statement(root, report, rows):
    """One statement CSV in the layout `statement_path` forms."""
    path = root / "financials" / "statements" / TEMPLATE / report
    path.mkdir(parents=True, exist_ok=True)
    columns = list(fin.DATA_COLS)
    for row in rows:
        columns += [c for c in row if c not in columns]
    target = path / f"{fin.REPORT_PREFIX[report]}_HOSE_TST.csv"
    with target.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return target


def _row(period, probe_column, probe, *, source="pdf", consolidated="True", items=12,
         **extra):
    """A statement row with `items` line items, one of which is the probe `sane` reads."""
    # ⚠️ `year` and `quarter` are not decoration: `_write`'s merge branch sorts the grid on
    # `int(row["year"])`, so a fixture row without them makes an upsert raise on a real file's
    # own contract rather than on anything the test is about.
    row = {"period": period, "source": source, "consolidated": consolidated,
           "symbol": "TST", "exchange": "HOSE", "template": TEMPLATE,
           "year": period.split("-")[1], "quarter": period[1],
           "method": "onnx@200", "unit": "1000000", "publish_date": "2026-04-29"}
    row[probe_column] = probe
    for i in range(items - 1):
        row[f"line_{i}"] = 1_000 + i
    row.update(extra)
    return row


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    """A CafeF data root with the two directories `use_data_root` requires."""
    root = tmp_path / "cafef"
    (root / "pdfs" / "index").mkdir(parents=True)
    (root / "financials" / "schema").mkdir(parents=True)
    monkeypatch.setattr(fin, "PDFS_DIR", str(root / "pdfs"))
    monkeypatch.setattr(fin, "SCHEMA_DIR", str(root / "financials" / "schema"))
    monkeypatch.setattr(fin, "STATEMENTS_DIR", str(root / "financials" / "statements"))
    return root


# ──────────────────────────────────────────────────────────────────────────────
# use_data_root
# ──────────────────────────────────────────────────────────────────────────────


def test_a_root_without_a_schema_directory_is_refused_before_any_ocr(tmp_path):
    """⚠️ `schema_of` already raises on an absent chart of accounts — AFTER the OCR.

    That is the 2.4 hours `utils/inputs.py` is named for. Checking the directory at the point
    the root is chosen costs nothing and moves the failure to before the expensive step.
    """
    root = tmp_path / "cafef"
    (root / "pdfs").mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="not a CafeF data root"):
        job.use_data_root(root)


# ──────────────────────────────────────────────────────────────────────────────
# select_layers
# ──────────────────────────────────────────────────────────────────────────────


def test_the_default_cascade_is_the_production_one_unchanged():
    assert job.select_layers(None) == list(FinancialsBuilder.LAYERS)


def test_a_subset_keeps_the_cascades_own_order_whatever_order_it_was_asked_in():
    """⚠️ ORDER IS LOAD-BEARING: a half-right layer that passes the gates ENDS the cascade.

    `PGB-1` and §6-2-unvicies each measured this independently — `+notes+seam` must run before
    bare `+notes`, and `+unit+tail` before bare `+unit`, or the cascade stops on a layer that
    writes a wrong figure past both gates. A subset may drop layers; it may never re-order them.
    """
    names = [layer.name for layer in FinancialsBuilder.LAYERS]
    early, late = names[1], names[-2]
    chosen = [layer.name for layer in job.select_layers([late, early])]
    assert chosen == [early, late]


def test_a_layer_name_that_matches_nothing_raises_rather_than_widening_the_run():
    with pytest.raises(ValueError, match="no parse layer"):
        job.select_layers(["onnx@200", "onnx@999+imaginary"])


# ──────────────────────────────────────────────────────────────────────────────
# seed_history — the band `sane` judges a candidate statement against
# ──────────────────────────────────────────────────────────────────────────────


def test_the_band_holds_only_pdf_rows_from_earlier_periods(data_root):
    """A `cafef` or `missing` row was never accepted by the gates, so it was never history."""
    _write_statement(data_root, fin.BALANCE_SHEET, [
        _row("Q1-2025", ASSETS, 1_000_000),
        _row("Q2-2025", ASSETS, 1_100_000, source="missing"),
        _row("Q3-2025", ASSETS, 1_200_000),
        _row("Q1-2026", ASSETS, 9_999_999),          # the target period itself
        _row("Q2-2026", ASSETS, 8_888_888),          # its future
    ])
    builder = FinancialsBuilder(logger=None)
    band = job.seed_history(builder, "HOSE", "TST", TEMPLATE, before="Q1-2026")

    assert band[fin.BALANCE_SHEET]["True"] == [1_000_000, 1_200_000]


def test_a_thin_statement_is_excluded_from_the_band_exactly_as_build_withholds_it(data_root):
    """⚠️ `SAN-1`: one 2-item statement became the whole reference population, and `sane`'s
    median-of-one band then silently rejected every correct quarter after it. `build()` writes
    such a statement and withholds it from `history`; this must do the same or a run seeded
    from disk is judged against a band a full run would never have had."""
    _write_statement(data_root, fin.BALANCE_SHEET, [
        _row("Q1-2025", ASSETS, 1_000_000, items=2),
        _row("Q2-2025", ASSETS, 1_100_000, items=FinancialsBuilder.MIN_ITEMS_FOR_HISTORY),
    ])
    builder = FinancialsBuilder(logger=None)
    band = job.seed_history(builder, "HOSE", "TST", TEMPLATE, before="Q1-2026")

    assert band[fin.BALANCE_SHEET]["True"] == [1_100_000]


def test_the_band_is_split_by_entity_because_a_standalone_company_is_a_smaller_company(
        data_root):
    """⚠️ Pooling the two makes the ±20× band meaningless in both directions. A ticker can file
    standalone early and consolidated later — ACB is exactly that — so this is reachable."""
    _write_statement(data_root, fin.BALANCE_SHEET, [
        _row("Q1-2025", ASSETS, 900_000, consolidated="False"),
        _row("Q2-2025", ASSETS, 1_000_000, consolidated="True"),
    ])
    builder = FinancialsBuilder(logger=None)
    band = job.seed_history(builder, "HOSE", "TST", TEMPLATE, before="Q1-2026")

    assert band[fin.BALANCE_SHEET] == {"True": [1_000_000], "False": [900_000]}


def test_each_report_is_probed_on_its_own_anchor(data_root):
    """The balance sheet is judged on total assets, the P&L on profit before tax and the cash
    flow on the closing balance — `_probe`'s mapping, and reading the wrong column would band a
    statement against a figure of an entirely different magnitude."""
    _write_statement(data_root, fin.BALANCE_SHEET, [_row("Q1-2025", ASSETS, 1_000_000)])
    _write_statement(data_root, fin.INCOME_STATEMENT, [_row("Q1-2025", PBT, 12_000)])
    _write_statement(data_root, fin.CASH_FLOW, [_row("Q1-2025", CLOSE, 55_000)])
    builder = FinancialsBuilder(logger=None)
    band = job.seed_history(builder, "HOSE", "TST", TEMPLATE, before="Q1-2026")

    assert [band[r]["True"] for r in (fin.BALANCE_SHEET, fin.INCOME_STATEMENT,
                                      fin.CASH_FLOW)] == [[1_000_000], [12_000], [55_000]]


def test_a_row_carrying_no_anchor_contributes_nothing_rather_than_a_zero(data_root):
    """§5 rule 2 at the band: a statement whose probe column is absent is an ABSENT
    measurement, and a 0 in the band would drag the median toward zero."""
    _write_statement(data_root, fin.BALANCE_SHEET, [
        _row("Q1-2025", "some_other_line", 1_000_000),
    ])
    builder = FinancialsBuilder(logger=None)
    band = job.seed_history(builder, "HOSE", "TST", TEMPLATE, before="Q1-2026")

    assert band[fin.BALANCE_SHEET]["True"] == []


# ──────────────────────────────────────────────────────────────────────────────
# open_reference — the 1 January opening balance a subset run cannot compute
# ──────────────────────────────────────────────────────────────────────────────


def test_the_opening_reference_is_the_previous_years_q4_closing_balance(data_root):
    _write_statement(data_root, fin.CASH_FLOW, [
        _row("Q4-2024", CLOSE, 41_000),
        _row("Q4-2025", CLOSE, 55_000),
    ])
    builder = FinancialsBuilder(logger=None)

    assert job.open_reference(builder, "HOSE", "TST", TEMPLATE, "Q2-2026") == 55_000


def test_no_opening_reference_is_taken_from_a_row_that_did_not_come_from_a_pdf(data_root):
    """§5 rule 24: a figure this repo may quote comes from the filing. A `cafef` row is not a
    parse, so it may not rescue a cash flow that OCR could not read."""
    _write_statement(data_root, fin.CASH_FLOW, [
        _row("Q4-2025", CLOSE, 55_000, source="cafef"),
    ])
    builder = FinancialsBuilder(logger=None)

    assert job.open_reference(builder, "HOSE", "TST", TEMPLATE, "Q1-2026") is None


# ──────────────────────────────────────────────────────────────────────────────
# compare — how a run scores itself
# ──────────────────────────────────────────────────────────────────────────────


def _task(period="Q1-2026", cumulative=False):
    return job.DocumentTask(
        exchange="HOSE", symbol="TST", period=period, template=TEMPLATE,
        path="nowhere.pdf", file="nowhere.pdf", consolidated="True",
        assurance="unaudited", cumulative=cumulative)


def _result(task, report, values, layer="onnx@200", unit=1_000_000,
            publish_date="2026-04-29"):
    result = job.DocumentResult(task=task, seconds=1.0)
    result.accepted[report] = {"layer": layer, "items": len(values), "pages": [1],
                               "unit": unit, "n_columns": 2, "cash_flow_method": "",
                               "quarter_column": False, "values": values}
    result.facts = {"publish_date": publish_date}
    result.absent = [r for r in fin.REPORTS if r != report]
    return result


def test_an_identical_parse_is_reported_as_reproduced(data_root):
    _write_statement(data_root, fin.BALANCE_SHEET,
                     [_row("Q1-2026", ASSETS, 1_000_000, items=3)])
    builder = FinancialsBuilder(logger=None)
    disk = builder._existing("HOSE", "TST", TEMPLATE, fin.BALANCE_SHEET)["Q1-2026"]
    values = job._line_items(disk)

    verdict = job.compare(builder, _result(_task(), fin.BALANCE_SHEET, values))

    assert verdict[fin.BALANCE_SHEET]["verdict"] == "REPRODUCED"
    assert verdict[fin.BALANCE_SHEET]["identical"] == len(values)


def test_a_changed_cell_is_named_with_both_figures(data_root):
    _write_statement(data_root, fin.BALANCE_SHEET,
                     [_row("Q1-2026", ASSETS, 1_000_000, items=3)])
    builder = FinancialsBuilder(logger=None)
    disk = builder._existing("HOSE", "TST", TEMPLATE, fin.BALANCE_SHEET)["Q1-2026"]
    values = dict(job._line_items(disk), **{ASSETS: 999_999})

    verdict = job.compare(builder, _result(_task(), fin.BALANCE_SHEET, values))

    assert verdict[fin.BALANCE_SHEET]["verdict"] == "DIFFERS"
    assert verdict[fin.BALANCE_SHEET]["changed"][ASSETS] == [1_000_000, 999_999]


def test_a_run_that_only_lost_a_publish_date_is_not_reported_as_clean(data_root):
    """⚠️ MEASURED, TWICE: a run whose figures were untouched dropped one `publish_date` per
    statement and a figures-only diff called it clean (§6-2-quatervicies, §6-2-quinvicies).
    Diff every column, not the numbers."""
    _write_statement(data_root, fin.BALANCE_SHEET,
                     [_row("Q1-2026", ASSETS, 1_000_000, items=3)])
    builder = FinancialsBuilder(logger=None)
    disk = builder._existing("HOSE", "TST", TEMPLATE, fin.BALANCE_SHEET)["Q1-2026"]
    values = job._line_items(disk)

    verdict = job.compare(
        builder, _result(_task(), fin.BALANCE_SHEET, values, publish_date=""))

    assert verdict[fin.BALANCE_SHEET]["verdict"] == "DIFFERS"
    assert verdict[fin.BALANCE_SHEET]["same_publish_date"] is False


def test_a_cumulative_income_statement_is_refused_rather_than_scored(data_root):
    """⚠️ An annual filing prints the year to date; the row on disk has been de-cumulated.
    Scoring the two against each other would report every cell as changed and mean nothing —
    so the comparison abstains and SAYS it abstained, rather than producing a number."""
    _write_statement(data_root, fin.INCOME_STATEMENT,
                     [_row("Q4-2025", PBT, 12_000, items=3)])
    builder = FinancialsBuilder(logger=None)
    task = _task(period="Q4-2025", cumulative=True)

    verdict = job.compare(
        builder, _result(task, fin.INCOME_STATEMENT, {PBT: 48_000}))

    assert "skipped" in verdict[fin.INCOME_STATEMENT]["verdict"]
    assert "changed" not in verdict[fin.INCOME_STATEMENT]


def test_a_statement_absent_from_the_run_is_said_to_be_absent_not_reproduced(data_root):
    _write_statement(data_root, fin.CASH_FLOW, [_row("Q1-2026", CLOSE, 55_000, items=3)])
    builder = FinancialsBuilder(logger=None)

    verdict = job.compare(builder, _result(_task(), fin.BALANCE_SHEET, {ASSETS: 1}))

    assert verdict[fin.CASH_FLOW]["verdict"] == "absent in this run"


# ──────────────────────────────────────────────────────────────────────────────
# _line_items — what counts as a figure
# ──────────────────────────────────────────────────────────────────────────────


def test_provenance_columns_are_never_read_as_line_items():
    """`DATA_COLS` is imported rather than re-listed, so a new provenance column cannot start
    reading as a figure here while the writer treats it as metadata."""
    row = {"period": "Q1-2026", "source": "pdf", "unit": "1000000", "n_columns": "2",
           "shares_issued": "5000", ASSETS: "1000000"}

    assert job._line_items(row) == {ASSETS: 1_000_000}


# ──────────────────────────────────────────────────────────────────────────────
# resolve_template — the input that used to be silently defaulted
# ──────────────────────────────────────────────────────────────────────────────


def test_an_unresolvable_template_raises_rather_than_assuming_bank(data_root, monkeypatch):
    """⚠️ THE DEFAULT THIS REPLACES WAS `or "bank"`, AND IT IS A SILENT WRONG ANSWER.

    761 of 781 listed names are not banks. Mapping a corporate filing against the bank chart
    of accounts rejects every statement as unreconcilable — hours of OCR later, reported as a
    parse failure rather than as the wrong schema. `utils/inputs.py` is named for this shape.
    """
    monkeypatch.setattr(fin, "TEMPLATES_INDEX", str(data_root / "financials" / "nope.csv"))
    monkeypatch.setattr("web_scraper.cafef_schema.detect_template", lambda symbol: None)
    builder = FinancialsBuilder(logger=None)

    with pytest.raises(ValueError, match="cannot resolve the accounting template"):
        job.resolve_template(builder, "VIC")


def test_templates_csv_answers_before_the_network_does(data_root, monkeypatch):
    """`detect_template` is a CafeF API call. A ticker already in `templates.csv` must never
    reach it — on a Kaggle worker that round trip is the difference between a resolved
    template and a run that cannot start."""
    index = data_root / "financials" / "templates.csv"
    index.write_text("exchange,symbol,template\nHOSE,TST,securities\n", encoding="utf-8")
    monkeypatch.setattr(fin, "TEMPLATES_INDEX", str(index))

    def _boom(symbol):
        raise AssertionError("detect_template must not be reached")

    monkeypatch.setattr("web_scraper.cafef_schema.detect_template", _boom)
    template, how = job.resolve_template(FinancialsBuilder(logger=None), "TST")

    assert (template, how) == ("securities", "templates.csv")


def test_an_override_naming_a_template_that_does_not_exist_is_refused():
    with pytest.raises(ValueError, match="unknown template"):
        job.resolve_template(FinancialsBuilder(logger=None), "TST", override="banking")


def test_the_route_that_answered_is_reported_because_the_claims_differ():
    """"read off templates.csv" and "guessed from a line-item count" are not the same claim,
    and the artefact has to be able to tell a reader which one it was."""
    template, how = job.resolve_template(FinancialsBuilder(logger=None), "TST",
                                         override="corp")
    assert (template, how) == ("corp", "override")


# ──────────────────────────────────────────────────────────────────────────────
# Progress — every percentage names its denominator
# ──────────────────────────────────────────────────────────────────────────────


def _layer(name="onnx@200"):
    return next(l for l in FinancialsBuilder.LAYERS if l.name == name)


def test_each_percentage_says_what_it_is_a_percentage_OF(tmp_path):
    """⚠️ `kaggle_gpu/README.md` §3 records that this repo's progress readouts have different
    denominators and only one predicts time. Printing the number without the denominator is
    how "32 %" gets read as "a third of the way through"."""
    log = job.Progress(3, log_path=tmp_path / "run.log", echo=False)
    log.document(1, _task(), 5.0)
    log.layer(12, 47, _layer(), cached=False)
    log.page(0, 10)
    text = "\n".join(log.lines)
    log.close()

    assert "of DOCUMENTS, not of time" in text
    assert "of POSITIONS" in text
    assert "of PAGES" in text and "predicts time" in text


def test_a_cached_layer_says_so_because_that_is_the_whole_cost_story(tmp_path):
    """A layer whose parse key is already cached re-maps in milliseconds; the next one re-OCRs
    every page. Without the flag the two look identical in the log and the reader cannot tell
    a stalled run from a fast one."""
    log = job.Progress(1, log_path=tmp_path / "run.log", echo=False)
    log.layer(5, 47, _layer(), cached=True)
    log.close()

    assert "cached parse, re-map only" in log.lines[-1]


def test_page_progress_is_rate_limited_so_a_96_page_document_is_not_960_lines(tmp_path):
    """10 percentage points or 15 s, whichever comes first — the same shape `kgpu wait` uses."""
    log = job.Progress(1, log_path=tmp_path / "run.log", echo=False)
    log.layer(1, 47, _layer(), cached=False)
    before = len(log.lines)
    for i in range(96):
        log.page(i, 96)
    emitted = len(log.lines) - before

    assert 5 <= emitted <= 12, emitted


def test_the_log_file_is_written_as_the_run_goes_not_at_the_end(tmp_path):
    """§5 rule 20: a 4-hour run was lost entirely to a wrapper that re-buffered on top of
    `python -u`. The file must hold the line before the next unit of work starts."""
    path = tmp_path / "run.log"
    log = job.Progress(1, log_path=path, echo=False)
    log.line("first")

    assert "first" in path.read_text(encoding="utf-8")
    log.close()


# ──────────────────────────────────────────────────────────────────────────────
# plan(quarters=...) — the batch filter, YYYY-QQ
#
# ⚠️ It was `years: list[int]` until 2026-08-29. The unit was a YEAR on the argument that the
# statement BUILD skips whole years (`orchestration` §2a: `_decumulate` needs Q1..Q(q-1) of the
# same year) — a fact about the WRITE, which this module does not do. The wider unit only ever
# bought extra OCR, so the grain is a quarter and the form is the one that SORTS.
# ──────────────────────────────────────────────────────────────────────────────

_INDEX_COLS = ["symbol", "exchange", "year", "quarter", "period", "name", "consolidated",
               "assurance", "half_year", "file_date", "bytes", "file", "path", "url"]


def _index_row(year, quarter, consolidated="True", assurance="unaudited"):
    period = f"FY-{year}" if quarter == 5 else f"Q{quarter}-{year}"
    name = f"{period}-{consolidated}"
    return {"symbol": "TST", "exchange": "HOSE", "year": str(year), "quarter": str(quarter),
            "period": period, "name": name, "consolidated": consolidated,
            "assurance": assurance, "half_year": "False", "file_date": "", "bytes": "1",
            "file": name + ".pdf", "path": f"files/HOSE_TST/{name}.pdf", "url": ""}


@pytest.fixture()
def filings(data_root):
    """A PDF index for HOSE_TST: 2013 and 2014 in full, plus a 2015 audited annual."""
    rows = [_index_row(y, q) for y in (2013, 2014) for q in (1, 2, 3, 4)]
    rows.append(_index_row(2015, 5, assurance="audited"))
    path = data_root / "pdfs" / "index" / "HOSE_TST.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_INDEX_COLS)
        writer.writeheader()
        writer.writerows(rows)
    return FinancialsBuilder(logger=None)


def _periods(builder, **kwargs):
    return [t.period for t in job.plan(builder, "HOSE", "TST", template=TEMPLATE, **kwargs)]


def test_no_quarters_is_every_quarter_the_ticker_files(filings):
    assert _periods(filings) == [f"Q{q}-{y}" for y in (2013, 2014) for q in (1, 2, 3, 4)] \
        + ["Q4-2015"]


def test_an_empty_list_means_the_same_as_absent_and_is_not_an_empty_run(filings):
    """⚠️ The whole point of the parameter's contract: `[]` is 'every quarter', never 'none'.

    A falsy filter that returned nothing would be a run that parses nothing and reports
    success — the failure `plan` already raises to prevent for `periods`.
    """
    assert _periods(filings, quarters=[]) == _periods(filings)
    assert _periods(filings, quarters=None) == _periods(filings)


def test_one_quarter_selects_that_quarter_only(filings):
    assert _periods(filings, quarters=["2014-Q3"]) == ["Q3-2014"]


def test_a_batch_comes_back_in_calendar_order_not_the_order_asked_for(filings):
    assert _periods(filings, quarters=["2014-Q1", "2013-Q4"]) == ["Q4-2013", "Q1-2014"]


def test_quarters_and_periods_intersect_rather_than_contradict(filings):
    assert _periods(filings, quarters=["2014-Q3"], periods=["Q3-2014"]) == ["Q3-2014"]


def test_a_quarter_the_ticker_does_not_file_raises_rather_than_running_empty(filings):
    with pytest.raises(ValueError) as excinfo:
        _periods(filings, quarters=["1999-Q1"])
    assert "1999-Q1" in str(excinfo.value)
    assert "Quarters available" in str(excinfo.value)


@pytest.mark.parametrize("bad", ["Q3-2014", "2014-3", "2014Q3", "2014-Q5", "14-Q3"])
def test_the_repo_native_form_and_any_other_shape_is_refused(filings, bad):
    """⚠️ `Q3-2014` names the same quarter and is REFUSED anyway.

    A lenient parser would be easy — and then a caller who used the wrong form would never
    find out, while a caller who made a typo would get "files no document for [...]" and go
    looking at CafeF for a filing that is sitting there. The message names the right form.
    """
    with pytest.raises(ValueError) as excinfo:
        _periods(filings, quarters=[bad])
    assert "YYYY-QQ" in str(excinfo.value)


def test_the_form_is_checked_before_the_corpus_is(filings):
    """A malformed quarter must not be reported as a quarter the ticker does not file."""
    with pytest.raises(ValueError) as excinfo:
        _periods(filings, quarters=["Q3-2014"])
    assert "Quarters available" not in str(excinfo.value)


def test_the_periods_error_names_the_quarters_filter_that_emptied_the_plan(filings):
    """Both filters are live, so the message must say which one did the cutting."""
    with pytest.raises(ValueError) as excinfo:
        _periods(filings, quarters=["2014-Q3"], periods=["Q3-2013"])
    assert "quarters=['2014-Q3']" in str(excinfo.value)


def test_an_annual_report_is_filed_under_the_quarter_of_the_PERIOD_it_serves(filings):
    """⚠️ CafeF files the annual under quarter 5 and `documents()` folds it onto that year's
    Q4 — so the 2015 annual is `2015-Q4` because its PERIOD is Q4-2015. Reading the raw index
    column would agree here and disagree wherever CafeF's `Year` is `0`, `202` or `203`
    (10 of 84,076 documents, CLAUDE.md §6-2-septies)."""
    assert _periods(filings, quarters=["2015-Q4"]) == ["Q4-2015"]
    assert job.as_quarter("Q4-2015") == "2015-Q4"


def test_the_filter_is_recorded_in_the_spec_so_the_artefact_says_what_was_asked(filings):
    """§5 rule 2 at the manifest: 'every quarter' and 'nobody wrote it down' must be tellable
    apart by a reader who has only the run folder."""
    assert (job.JobSpec(symbol="TST", quarters=["2014-Q3"]).to_json()["quarters"]
            == ["2014-Q3"])
    assert job.JobSpec(symbol="TST").to_json()["quarters"] is None
    assert job.JobSpec(symbol="TST", quarters=[]).to_json()["quarters"] is None


# ──────────────────────────────────────────────────────────────────────────────
# the two spellings of one quarter
# ──────────────────────────────────────────────────────────────────────────────


def test_the_zero_padded_spelling_names_the_same_quarter():
    """⚠️ `2026-04` is Q4 of 2026, not April. The unit of this module is a QUARTER, and both
    spellings sort, which is the whole reason the `Q3-2014` form is refused."""
    assert job.normalise_quarter("2026-04") == "2026-Q4"
    assert job.normalise_quarter("2026-Q4") == "2026-Q4"
    assert job.normalise_quarter(" 2014-01 ") == "2014-Q1"


def test_two_spellings_of_one_quarter_can_never_become_two_runs():
    """`cfg.name`, the payload directory and the Kaggle kernel slug are all derived from this
    list, so folding happens before anything is named — not after two jobs exist."""
    assert job.canonical_quarters(["2014-03", "2014-Q3"]) == ["2014-Q3"]
    assert job.canonical_quarters(["2014-Q4", "2013-04"]) == ["2013-Q4", "2014-Q4"]


def test_empty_and_absent_are_one_answer_and_it_is_none():
    assert job.canonical_quarters([]) is None
    assert job.canonical_quarters(None) is None


@pytest.mark.parametrize("bad", ["2014-3", "2014-05", "2014-00", "Q3-2014", "2014-Q0"])
def test_a_shape_one_keystroke_from_a_month_is_refused(bad):
    """⚠️ `2014-3` is refused although it is unambiguous: one digit is a keystroke away from a
    MONTH, and a form that accepts both invites a caller to write `2014-11` and mean November.
    """
    with pytest.raises(ValueError):
        job.normalise_quarter(bad)


def test_the_zero_padded_form_selects_the_same_filing(filings):
    assert _periods(filings, quarters=["2014-03"]) == ["Q3-2014"]
    assert _periods(filings, quarters=["2014-03", "2014-Q3"]) == ["Q3-2014"]


# ──────────────────────────────────────────────────────────────────────────────
# partition_by_disk — what a re-parse could still win
# ──────────────────────────────────────────────────────────────────────────────


def _all_three(root, *periods, source="pdf"):
    """These quarters present in all three statement CSVs.

    ⚠️ Every period in ONE call: `_write_statement` rewrites the file, so writing them one at a
    time leaves only the last — which is how the year-grain test first passed for the wrong
    reason.
    """
    for report, column, probe in ((fin.BALANCE_SHEET, ASSETS, 100),
                                  (fin.INCOME_STATEMENT, PBT, 10),
                                  (fin.CASH_FLOW, CLOSE, 5)):
        _write_statement(root, report,
                         [_row(p, column, probe, source=source) for p in periods])


def _split(builder, **kwargs):
    return job.partition_by_disk(
        builder, job.plan(builder, "HOSE", "TST", template=TEMPLATE, **kwargs))


def test_a_quarter_already_pdf_in_all_three_statements_is_not_re_opened(data_root, filings):
    """The point of the flag: at 4-18 min a document, re-reading a finished quarter is the
    single most expensive way to learn nothing."""
    _all_three(data_root, "Q1-2013")
    todo, done = _split(filings)
    assert [t.period for t in done] == ["Q1-2013"]
    assert "Q1-2013" not in [t.period for t in todo]
    assert len(todo) == 8


def test_one_absent_statement_re_opens_the_whole_filing(data_root, filings):
    """⚠️ One filing produces all three statements, so a quarter missing its cash flow has to
    open the document again — and the two statements that come back with it are then judged on
    their own merits by `pdf_ocr_merge`, not written over what disk holds."""
    _write_statement(data_root, fin.BALANCE_SHEET, [_row("Q1-2013", ASSETS, 100)])
    _write_statement(data_root, fin.INCOME_STATEMENT, [_row("Q1-2013", PBT, 10)])
    todo, done = _split(filings)
    assert done == []
    assert "Q1-2013" in [t.period for t in todo]


def test_a_transcribed_row_is_not_evidence_that_a_quarter_is_done(data_root, filings):
    """§5 rule 24: a `cafef` row is somebody else's parse, so it is exactly the row a re-parse
    exists to replace. `missing` says the same thing from the other side."""
    _all_three(data_root, "Q1-2013", "Q2-2013", source="cafef")
    todo, done = _split(filings)
    assert done == []


def test_the_grain_is_a_quarter_here_and_a_year_in_build(data_root, filings):
    """⚠️ `_skippable_years` keeps a YEAR whole because `_decumulate` needs this run's own
    Q1..Q(q-1). Nothing in this module de-cumulates, so Q1 may be skipped while Q4 is parsed —
    and that is the difference between three wasted documents and none."""
    _all_three(data_root, "Q1-2013", "Q2-2013", "Q3-2013")
    todo, done = _split(filings, quarters=["2013-Q1", "2013-Q2", "2013-Q3", "2013-Q4"])
    assert [t.period for t in todo] == ["Q4-2013"]
    assert len(done) == 3


# ──────────────────────────────────────────────────────────────────────────────
# prepare() — the two knobs, and an empty plan that explains itself
# ──────────────────────────────────────────────────────────────────────────────


def _spec(data_root, **kwargs):
    return job.JobSpec(symbol="TST", template=TEMPLATE, data_root=str(data_root), **kwargs)


def test_when_every_selected_quarter_is_done_the_error_names_the_way_out(
        data_root, filings, monkeypatch):
    """⚠️ "nothing was selected" and "everything selected was already done" are different
    facts, and a run that reported the first for the second would send a reader to CafeF."""
    monkeypatch.setenv(job.DATA_ROOT_ENV, str(data_root))
    _all_three(data_root, "Q3-2014")
    with pytest.raises(ValueError) as excinfo:
        _spec(data_root, quarters=["2014-Q3"]).prepare()
    assert "already read `pdf`" in str(excinfo.value)
    assert "overwrite=True" in str(excinfo.value)


def test_overwrite_re_opens_what_the_skip_would_have_dropped(data_root, filings, monkeypatch):
    monkeypatch.setenv(job.DATA_ROOT_ENV, str(data_root))
    _all_three(data_root, "Q3-2014")
    prepared = _spec(data_root, quarters=["2014-Q3"], overwrite=True).prepare()
    assert [t.period for t in prepared.tasks] == ["Q3-2014"]
    assert prepared.skipped == []


def test_what_was_skipped_survives_into_the_prepared_job(data_root, filings, monkeypatch):
    """§5 rule 2 at the artefact: a run folder must be able to say which quarters it declined
    to re-open, or a reader cannot tell a complete corpus from an unattempted one."""
    monkeypatch.setenv(job.DATA_ROOT_ENV, str(data_root))
    _all_three(data_root, "Q3-2014")
    prepared = _spec(data_root, quarters=["2014-Q3", "2014-Q4"]).prepare()
    assert [t.period for t in prepared.tasks] == ["Q4-2014"]
    assert [t.period for t in prepared.skipped] == ["Q3-2014"]
    assert any("skipped" in line for line in prepared.describe())


def test_both_knobs_are_recorded_in_the_spec_a_reader_gets():
    spec = job.JobSpec(symbol="TST", overwrite=True, merge_into_csv=True)
    assert spec.to_json()["overwrite"] is True
    assert spec.to_json()["merge_into_csv"] is True
    assert job.JobSpec(symbol="TST").to_json() == dict(
        job.JobSpec(symbol="TST").to_json(), overwrite=False, merge_into_csv=False)


def test_the_two_log_sinks_are_one_class_so_a_warning_is_written_once(tmp_path):
    """`Progress` extends `CollectingLogger`; both were separate implementations of the four
    `log_*` methods until 2026-08-29, which is two places for the same change."""
    assert issubclass(job.Progress, job.CollectingLogger)
    log = job.Progress(1, log_path=tmp_path / "run.log", echo=False)
    log.log_warning("the band is EMPTY")
    log.log_debug("noise")
    log.close()
    written = (tmp_path / "run.log").read_text(encoding="utf-8")
    assert "WARNING: the band is EMPTY" in written
    assert "noise" not in written          # DEBUG is kept in memory, never in the file
    assert "DEBUG: noise" in log.lines


# ──────────────────────────────────────────────────────────────────────────────
# run() with the per-quarter upsert — the interruption guarantee, end to end
# ──────────────────────────────────────────────────────────────────────────────

REPO_SCHEMA = Path(__file__).resolve().parents[2] / "raw_data" / "cafef" / "financials" / "schema"


def _accepted(**values):
    """The `accepted[report]` block `run_document` returns and the upsert reads."""
    return {"layer": "onnx@200", "items": len(values), "rows": 20, "rows_sha": "abc",
            "row_dump": [], "pages": [1], "unit": 1, "n_columns": 2,
            "cash_flow_method": "", "quarter_column": False, "values": values}


@pytest.fixture()
def parseable(data_root, filings, monkeypatch, tmp_path):
    """A root where `run()` can complete without an OCR engine, a PDF or a GPU.

    ⚠️ The real charts of accounts are copied in: `_write` orders its columns by the chart, so
    a fake one would pin a column order that ships nowhere.
    """
    for chart in REPO_SCHEMA.glob(f"{TEMPLATE}_*.csv"):
        shutil.copy2(chart, data_root / "financials" / "schema" / chart.name)
    files = data_root / "pdfs" / "files" / "HOSE_TST"
    files.mkdir(parents=True, exist_ok=True)
    for period in ("Q1-2013", "Q2-2013", "Q3-2013"):
        (files / f"{period}-True.pdf").write_bytes(b"%PDF-1.4")
    monkeypatch.setenv(job.DATA_ROOT_ENV, str(data_root))
    monkeypatch.setattr(job, "DEFAULT_DATA_ROOT", data_root)
    monkeypatch.setattr(job, "engine_report", lambda: {})
    monkeypatch.setattr(job, "_ocr_device", lambda: "cpu")
    monkeypatch.setattr(pdf_ocr_merge, "BACKUP_ROOT", tmp_path / "_backup")
    # Q1-2013 complete on disk: it is what gives `sane` a non-empty band for the quarters
    # after it, and — being complete — it is also what the skip must drop.
    _all_three(data_root, "Q1-2013")
    return data_root


def test_each_quarter_is_on_disk_before_the_next_document_is_opened(
        parseable, tmp_path, monkeypatch):
    """⚠️ **THE INTERRUPTION GUARANTEE, MEASURED RATHER THAN ASSERTED IN A DOCSTRING.** A
    12-hour run stopped at hour 6 must keep every quarter that finished — which is only true if
    the upsert happens between documents, not at the end.
    """
    seen = {}

    def fake_parse(builder, task, history, open_ref=None, logger=None):
        # what disk holds at the MOMENT this document is opened
        seen[task.period] = {
            p: row.get("source")
            for p, row in builder._existing("HOSE", "TST", TEMPLATE,
                                            fin.BALANCE_SHEET).items()}
        return job.DocumentResult(
            task=task, seconds=0.1,
            accepted={fin.BALANCE_SHEET: _accepted(**{ASSETS: 100, "line_0": 7})},
            absent=[fin.INCOME_STATEMENT, fin.CASH_FLOW],
            facts={"publish_date": "2013-08-01"})

    monkeypatch.setattr(job, "run_document", fake_parse)
    folder = job.run(job.JobSpec(
        symbol="TST", template=TEMPLATE, quarters=["2013-Q2", "2013-Q3"],
        data_root=str(parseable), out_root=str(tmp_path / "reports"),
        merge_into_csv=True))

    # Q3 was opened knowing Q2 was already `pdf` on disk — that is the whole claim.
    assert seen["Q2-2013"].get("Q3-2013") is None
    assert seen["Q3-2013"]["Q2-2013"] == "pdf"

    rows = {}
    with open(fin.statement_path(TEMPLATE, fin.BALANCE_SHEET, "HOSE", "TST"),
              encoding="utf-8-sig") as f:
        rows = {r["period"]: r for r in csv.DictReader(f)}
    assert rows["Q2-2013"]["source"] == rows["Q3-2013"]["source"] == "pdf"
    assert rows["Q2-2013"][ASSETS] == "100"
    # ⚠️ and the quarter that was ALREADY complete keeps what it had — an upsert, not a rewrite
    assert rows["Q1-2013"][ASSETS] == "100"
    assert (folder / "documents" / "HOSE_TST__Q2-2013.json").is_file()


def test_the_backup_is_taken_once_for_the_run_and_named_in_the_metadata(
        parseable, tmp_path, monkeypatch):
    monkeypatch.setattr(job, "run_document", lambda builder, task, history, open_ref=None,
                        logger=None: job.DocumentResult(
                            task=task, seconds=0.1,
                            accepted={fin.BALANCE_SHEET: _accepted(**{ASSETS: 100,
                                                                     "line_0": 7})},
                            absent=[fin.INCOME_STATEMENT, fin.CASH_FLOW], facts={}))
    folder = job.run(job.JobSpec(
        symbol="TST", template=TEMPLATE, quarters=["2013-Q2", "2013-Q3"],
        data_root=str(parseable), out_root=str(tmp_path / "reports"), merge_into_csv=True))

    meta = json.loads((folder / "metadata.json").read_text(encoding="utf-8"))
    assert meta["inputs"]["merged_into_csv"] is True
    assert meta["inputs"]["skipped_already_parsed"] == []
    assert meta["inputs"]["merge_backup"]
    assert len(list((tmp_path / "_backup").iterdir())) == 1


def test_a_payload_root_refuses_the_upsert_rather_than_writing_a_copy_that_dies(
        parseable, tmp_path, monkeypatch):
    """⚠️ On a Kaggle worker `CAFEF_DATA_ROOT` is an unpacked payload. Writing the statement
    CSVs there edits a copy that is deleted with the kernel — and reports success. The write
    belongs to whoever holds the real `raw_data/`, which is what `kgpu pull` does."""
    monkeypatch.setattr(job, "DEFAULT_DATA_ROOT", tmp_path / "somewhere" / "else")
    monkeypatch.setattr(job, "run_document", lambda builder, task, history, open_ref=None,
                        logger=None: job.DocumentResult(
                            task=task, seconds=0.1,
                            accepted={fin.BALANCE_SHEET: _accepted(**{ASSETS: 100})},
                            absent=[fin.INCOME_STATEMENT, fin.CASH_FLOW], facts={}))
    folder = job.run(job.JobSpec(
        symbol="TST", template=TEMPLATE, quarters=["2013-Q2"], data_root=str(parseable),
        out_root=str(tmp_path / "reports"), merge_into_csv=True))

    meta = json.loads((folder / "metadata.json").read_text(encoding="utf-8"))
    assert meta["inputs"]["merged_into_csv"] is False
    assert "REFUSED" in (folder / "run.log").read_text(encoding="utf-8")
    assert not (tmp_path / "_backup").exists()


def test_a_layer_that_raised_is_recorded_as_an_engine_error_not_as_a_refusal(
        parseable, tmp_path, monkeypatch):
    """⚠️ **A REFUSAL MEASURES THE DOCUMENT; AN EXCEPTION MEASURES THE MACHINE.** Measured
    2026-08-29: `vocr.vn`'s certificate expired, every `onnx@*` layer raised, and
    `tesseract@200` won a filing that had read `onnx@200` — with different figures and both
    gates passing. The warning existed; nothing downstream could see it."""
    def cascade(self, path, period_end, template, band, open_ref=None):
        self.layer_errors = [("onnx@200", "SSLError: certificate has expired")]
        return {}, {"publish_date": "", "shares": {}}

    monkeypatch.setattr(FinancialsBuilder, "_parse_cascaded", cascade)
    folder = job.run(job.JobSpec(
        symbol="TST", template=TEMPLATE, quarters=["2013-Q2"], data_root=str(parseable),
        out_root=str(tmp_path / "reports"), merge_into_csv=True))

    doc = json.loads((folder / "documents" / "HOSE_TST__Q2-2013.json").read_text("utf-8"))
    assert doc["engine_errors"] == [["onnx@200", "SSLError: certificate has expired"]]
    assert "RAISED rather than refusing" in (folder / "run.log").read_text(encoding="utf-8")
