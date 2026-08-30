"""Pure tests for the `months` column — the span a written row covers. No PDF, no network.

⚠️ **THE COLUMN EXISTS SO A ROW THAT IS NOT A QUARTER CAN BE WRITTEN AT ALL.** Until
2026-08-30 the CSV had exactly one way to hold a figure covering the wrong span, which was
not to hold it: `_decumulate` dropped a cumulative Q2/Q4 income statement whenever this run
lacked its Q1..Q(q-1) priors. That is right while a later run can do better and permanently
lossy where those priors were never filed — **9 quarters across BSR, BID and VCB**, measured
the day this shipped. What separates the two cases is whether a filing EXISTS, and that is a
fact about the PDF index, not about any parse.
"""
import csv
import shutil
from pathlib import Path

import pytest

from web_scraper import cafef_financials as fin
from web_scraper.cafef_financials import (
    BALANCE_SHEET,
    CASH_FLOW,
    INCOME_STATEMENT,
    FinancialsBuilder,
    statement_months,
)

REPO_SCHEMA = Path(__file__).resolve().parents[2] / "raw_data" / "cafef" / "financials" / "schema"


# ──────────────────────────────────────────────────────────────────────────────
# the span rule
# ──────────────────────────────────────────────────────────────────────────────


def test_a_balance_sheet_has_no_span_at_all():
    """⚠️ Not "unknown" — a category error. A balance sheet is a STOCK read at a date, and a
    number of months against it would be a claim nobody could act on."""
    for period in ("Q1-2016", "Q4-2016"):
        assert statement_months(BALANCE_SHEET, period, cumulative=True) is None
        assert statement_months(BALANCE_SHEET, period, cumulative=False) is None


@pytest.mark.parametrize("period,months", [("Q1-2019", 3), ("Q2-2019", 6),
                                           ("Q3-2019", 9), ("Q4-2019", 12)])
def test_a_cash_flow_is_cumulative_from_1_january_in_every_quarter(period, months):
    """⚠️ A VN filing's `lưu chuyển tiền tệ` is year-to-date in EVERY quarter — which is why
    `_decumulate` leaves it alone and why every quarter of a year shares one opening balance.
    The index's own flag does not enter into it."""
    assert statement_months(CASH_FLOW, period, cumulative=True) == months
    assert statement_months(CASH_FLOW, period, cumulative=False) == months


def test_an_ordinary_quarterly_income_statement_is_three_months():
    assert statement_months(INCOME_STATEMENT, "Q3-2019", cumulative=False) == 3


@pytest.mark.parametrize("period,months", [("Q2-2016", 6), ("Q4-2016", 12)])
def test_a_cumulative_income_statement_carries_its_year_to_date_span(period, months):
    assert statement_months(INCOME_STATEMENT, period, cumulative=True) == months


def test_the_document_outranks_the_index():
    """⚠️ VCB's Q2-2014 prints its own quarter column beside the cumulative one, so column 0
    is already the quarter even though the index calls the filing semi-annual. `build()` has
    always let the statement overrule the index here; the label must agree with it or it
    would contradict the figure it describes."""
    assert statement_months(INCOME_STATEMENT, "Q2-2014",
                            cumulative=True, quarter_column=True) == 3


# ──────────────────────────────────────────────────────────────────────────────
# `_decumulate` — drop, or keep and label
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def builder():
    return FinancialsBuilder(logger=None)


def _data(period, **values):
    return {r: ({period: dict(values)} if r == INCOME_STATEMENT else {}) for r in fin.REPORTS}


def _meta(period):
    return {r: ({period: {"months": 12}} if r == INCOME_STATEMENT else {})
            for r in fin.REPORTS}


def test_a_quarter_whose_priors_were_filed_but_not_parsed_is_still_dropped(builder):
    """The common case and the one the drop was written for. Every subset run is in it: the
    quarter is recoverable, and writing the year-to-date figure now would pre-empt a better
    answer with a worse one."""
    data, meta = _data("Q4-2016", pbt=4_807), _meta("Q4-2016")
    filed = {"Q1-2016", "Q2-2016", "Q3-2016", "Q4-2016"}

    builder._decumulate(data, {"Q4-2016": True}, meta, filed)

    assert "Q4-2016" not in data[INCOME_STATEMENT]


def test_a_quarter_whose_priors_were_never_filed_is_kept_and_labelled(builder):
    """⚠️ Nothing will ever subtract quarters that were never reported. BSR files only an
    FY-2016 annual for the whole of 2016, so the choice is not "cumulative now or a quarter
    later" — it is "cumulative now or nothing, ever"."""
    data, meta = _data("Q4-2016", pbt=4_807), _meta("Q4-2016")

    builder._decumulate(data, {"Q4-2016": True}, meta, {"Q4-2016"})

    assert data[INCOME_STATEMENT]["Q4-2016"] == {"pbt": 4_807}
    assert meta[INCOME_STATEMENT]["Q4-2016"]["months"] == 12


def test_one_unfiled_prior_is_enough_to_keep_it(builder):
    """De-cumulation needs ALL of Q1..Q(q-1). BSR filed a Q3-2017 quarterly and no Q1 or Q2,
    so its Q4-2017 is as unrecoverable as its Q4-2016."""
    data, meta = _data("Q4-2017", pbt=1_000), _meta("Q4-2017")

    builder._decumulate(data, {"Q4-2017": True}, meta, {"Q3-2017", "Q4-2017"})

    assert "Q4-2017" in data[INCOME_STATEMENT]
    assert meta[INCOME_STATEMENT]["Q4-2017"]["months"] == 12


def test_without_a_filed_set_nothing_is_kept(builder):
    """⚠️ §5 rule 2 at the parameter. A caller that has not said which case it is in has not
    measured that the priors are unfilable, and an absent measurement may not be read as the
    permissive answer."""
    data, meta = _data("Q4-2016", pbt=4_807), _meta("Q4-2016")

    builder._decumulate(data, {"Q4-2016": True}, meta, None)

    assert "Q4-2016" not in data[INCOME_STATEMENT]


def test_a_successful_de_cumulation_relabels_the_row_as_a_quarter(builder):
    """The row that comes out IS three months, whatever the filing printed, so the label has
    to move with the figures."""
    data = {r: {} for r in fin.REPORTS}
    data[INCOME_STATEMENT] = {"Q1-2016": {"pbt": 100}, "Q2-2016": {"pbt": 150},
                              "Q3-2016": {"pbt": 250}, "Q4-2016": {"pbt": 1_000}}
    meta = {r: {} for r in fin.REPORTS}
    meta[INCOME_STATEMENT] = {"Q4-2016": {"months": 12}}
    filed = {f"Q{i}-2016" for i in range(1, 5)}

    builder._decumulate(data, {"Q4-2016": True}, meta, filed)

    assert data[INCOME_STATEMENT]["Q4-2016"] == {"pbt": 500}       # 1000 - (100+150+250)
    assert meta[INCOME_STATEMENT]["Q4-2016"]["months"] == 3


def test_q1_and_a_plain_quarterly_filing_are_never_touched(builder):
    """Q1 IS the year to date, and an ordinary interim filing prints the quarter."""
    data = {r: {} for r in fin.REPORTS}
    data[INCOME_STATEMENT] = {"Q1-2016": {"pbt": 100}, "Q3-2016": {"pbt": 250}}
    meta = {r: {"Q1-2016": {"months": 3}, "Q3-2016": {"months": 3}} for r in fin.REPORTS}

    builder._decumulate(data, {"Q1-2016": True, "Q3-2016": False}, meta, set())

    assert data[INCOME_STATEMENT] == {"Q1-2016": {"pbt": 100}, "Q3-2016": {"pbt": 250}}


# ──────────────────────────────────────────────────────────────────────────────
# the column reaches the CSV
# ──────────────────────────────────────────────────────────────────────────────


def test_months_is_a_data_column_and_not_a_line_item():
    """⚠️ The writer's `DATA_COLS` and the ingest's `CAFEF_FINANCIAL_META_COLS` are two halves
    of one contract with nothing enforcing the match — a column missing from the second is
    coerced to numeric as a LINE ITEM. `pdf_ocr_job.META_COLS` is derived from the first, so
    it follows automatically; this pins that it did."""
    from web_scraper import pdf_ocr_job

    assert "months" in fin.DATA_COLS
    assert "months" in pdf_ocr_job.META_COLS
    assert pdf_ocr_job._line_items({"months": "12", "pbt": "500"}) == {"pbt": 500}


def test_the_written_row_carries_the_span(tmp_path, monkeypatch):
    base = tmp_path / "cafef"
    (base / "financials" / "schema").mkdir(parents=True)
    for chart in REPO_SCHEMA.glob("bank_*.csv"):
        shutil.copy2(chart, base / "financials" / "schema" / chart.name)
    monkeypatch.setattr(fin, "SCHEMA_DIR", str(base / "financials" / "schema"))
    monkeypatch.setattr(fin, "STATEMENTS_DIR", str(base / "financials" / "statements"))

    builder = FinancialsBuilder(logger=None)
    pbt = FinancialsBuilder.C_PBT[0]
    builder._write(
        "HOSE", "TST",
        {r: ({"Q4-2016": {pbt: 4_807}} if r == INCOME_STATEMENT else {}) for r in fin.REPORTS},
        {r: ([pbt] if r == INCOME_STATEMENT else []) for r in fin.REPORTS},
        {r: ({"Q4-2016": {"months": 12, "source": "pdf"}} if r == INCOME_STATEMENT else {})
         for r in fin.REPORTS},
        [(2016, 4)], "bank", {}, {}, {})

    path = fin.statement_path("bank", INCOME_STATEMENT, "HOSE", "TST")
    with open(path, encoding="utf-8-sig") as f:
        rows = {r["period"]: r for r in csv.DictReader(f)}
    assert rows["Q4-2016"]["months"] == "12"

    # ⚠️ and a row nothing produced says nothing about its span, exactly as it says nothing
    # about its entity — provenance is taken only from a period that produced a row.
    path_bs = fin.statement_path("bank", BALANCE_SHEET, "HOSE", "TST")
    with open(path_bs, encoding="utf-8-sig") as f:
        assert next(csv.DictReader(f))["months"] == ""
