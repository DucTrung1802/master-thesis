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

import pytest

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job
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
    row = {"period": period, "source": source, "consolidated": consolidated,
           "symbol": "TST", "exchange": "HOSE", "template": TEMPLATE,
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
# plan(years=...) — the batch filter
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


def test_no_years_is_every_year_the_ticker_files(filings):
    assert _periods(filings) == [f"Q{q}-{y}" for y in (2013, 2014) for q in (1, 2, 3, 4)] \
        + ["Q4-2015"]


def test_an_empty_list_means_the_same_as_absent_and_is_not_an_empty_run(filings):
    """⚠️ The whole point of the parameter's contract: `[]` is 'every year', never 'none'.

    A falsy filter that returned nothing would be a run that parses nothing and reports
    success — the failure `plan` already raises to prevent for `periods`.
    """
    assert _periods(filings, years=[]) == _periods(filings)
    assert _periods(filings, years=None) == _periods(filings)


def test_one_year_selects_that_years_quarters_only(filings):
    assert _periods(filings, years=[2014]) == ["Q1-2014", "Q2-2014", "Q3-2014", "Q4-2014"]


def test_several_years_come_back_in_calendar_order_not_the_order_asked_for(filings):
    assert _periods(filings, years=[2014, 2013]) == [
        f"Q{q}-{y}" for y in (2013, 2014) for q in (1, 2, 3, 4)]


def test_years_and_periods_intersect_rather_than_contradict(filings):
    assert _periods(filings, years=[2014], periods=["Q3-2014"]) == ["Q3-2014"]


def test_a_year_the_ticker_does_not_file_raises_rather_than_running_empty(filings):
    with pytest.raises(ValueError) as excinfo:
        _periods(filings, years=[1999])
    assert "1999" in str(excinfo.value)
    assert "Years available" in str(excinfo.value)


def test_the_periods_error_names_the_years_filter_that_emptied_the_plan(filings):
    """Both filters are live, so the message must say which one did the cutting."""
    with pytest.raises(ValueError) as excinfo:
        _periods(filings, years=[2014], periods=["Q3-2013"])
    assert "years=[2014]" in str(excinfo.value)


def test_an_annual_report_is_filed_under_the_year_of_the_PERIOD_it_serves(filings):
    """⚠️ CafeF files the annual under quarter 5 and `documents()` folds it onto that year's
    Q4 — so the 2015 annual is year 2015 because its PERIOD is Q4-2015. Reading the raw index
    column would agree here and disagree wherever CafeF's `Year` is `0`, `202` or `203`
    (10 of 84,076 documents, CLAUDE.md §6-2-septies)."""
    assert _periods(filings, years=[2015]) == ["Q4-2015"]


def test_the_filter_is_recorded_in_the_spec_so_the_artefact_says_what_was_asked(filings):
    """§5 rule 2 at the manifest: 'every year' and 'nobody wrote it down' must be tellable
    apart by a reader who has only the run folder."""
    assert job.JobSpec(symbol="TST", years=[2014]).to_json()["years"] == [2014]
    assert job.JobSpec(symbol="TST").to_json()["years"] is None
    assert job.JobSpec(symbol="TST", years=[]).to_json()["years"] is None
