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
