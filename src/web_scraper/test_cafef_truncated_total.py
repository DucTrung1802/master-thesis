"""`GTR-1` — a grand total a LOST BOX truncated, rebuilt and confirmed by its counterpart.

⚠️ **`total_from_section` CANNOT REACH THIS, AND THE REASON IS ITS THIRD LOCK.** `SEAL-2`'s
repair demands the rebuilt sum be within `_equal` of the damaged reading, because a company
seal COVERS digits and the magnitude survives. A lost box REMOVES them.

Measured on GAS, 2026-09-07, off `reports/pdf_ocr/20260907-064620__hose_gas__pdf_ocr`:

  Q2-2021  `TỔNG CỘNG NGUỒN VỐN` reads      216,601  against a printed 74,826,437,216,601
  Q2-2020  `TỔNG CỘNG NGUỒN VỐN` reads   47,956,383  against a printed 67,147,956,383,291

Both are a CONTIGUOUS RUN of the printed digits — the tail in the first, the middle in the
second — which is what `_split_number_runs` leaves when the recogniser emits one figure as
several boxes and one piece lands on the value column. Every layer before this block refuses
them: 200 dpi as fragmented, 300 and 400 dpi as `assets != liabilities + equity`. Escalation
reaches nothing because the digits are not covered, they are gone.

⚠️ **THE EVIDENCE IS THE COUNTERPART TOTAL, NOT THE ARITHMETIC THIS FILE EXERCISES.** The
rebuilt figure must equal `TỔNG CỘNG TÀI SẢN` — read off a DIFFERENT PAGE in a different OCR
pass — to the đồng. What `reconcile` then says is trivially true and is not counted (§5 rule
21); `test_reconcile_after_the_repair_is_trivial_and_is_not_the_evidence` pins that.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import BALANCE_SHEET, Row, Statement

ASSETS, RESOURCES = "tong_cong_tai_san", "tong_cong_nguon_von"

# GAS Q2-2021, read at `onnx@300` — the four figures the run folder carries.
Q21_TOTAL = 74_826_437_216_601        # printed, both grand totals
Q21_DAMAGED = 216_601                 # what the NGUỒN VỐN row returns
Q21_LIABILITIES = 26_981_135_438_346  # c_no_phai_tra, via the text fallback (CRP-1)
Q21_EQUITY = 47_845_301_778_255       # d_von_chu_so_huu
# GAS Q2-2020 — the damaged reading is an INTERIOR run of the digits, not the tail.
Q20_TOTAL = 67_147_956_383_291
Q20_DAMAGED = 47_956_383


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def rows_with_liabilities(liabilities=Q21_LIABILITIES):
    """The NỢ PHẢI TRẢ line as OCR returns it on a `corp` chart — `CRP-1`.

    `c_no_phai_tra` maps on almost no corp balance sheet, so lock 2 has to resolve it through
    `SECTION_PART_TEXT`, exactly as `_total_from_section` does.
    """
    return [Row(label="A - NO PHAI TRA", number="",
                key="ma_thuyet_stt_nguon_von_so_minh_a_n_no_phai_tra",
                values=[liabilities, liabilities])] + \
           [Row(label=f"x{i}", key=f"x{i}", number="", values=[i + 1, i + 1])
            for i in range(FinancialsBuilder.MIN_ROWS)]


def bs(rows=None, unit=1):
    return Statement(report=BALANCE_SHEET, pages=[1], unit=unit, n_columns=2,
                     rows=rows if rows is not None else rows_with_liabilities())


def whole_row():
    return {ASSETS: Q21_TOTAL, RESOURCES: Q21_DAMAGED,
            "d_von_chu_so_huu": Q21_EQUITY}


# ── the parts really do rebuild the printed figure ───────────────────────────────────────

def test_the_two_parts_sum_to_the_counterpart_total_to_the_dong():
    """The claim the repair rests on, stated as arithmetic rather than as a tolerance."""
    assert Q21_LIABILITIES + Q21_EQUITY == Q21_TOTAL


# ── the truncation signature ─────────────────────────────────────────────────────────────

def test_the_damaged_reading_is_a_contiguous_run_of_the_printed_digits(b):
    assert b._is_truncation(Q21_DAMAGED, Q21_TOTAL)          # the TAIL
    assert b._is_truncation(Q20_DAMAGED, Q20_TOTAL)          # the MIDDLE


def test_a_misread_digit_is_not_a_truncation(b):
    """⚠️ What separates `GTR-1` from every other damaged reading — see `_is_truncation`."""
    assert not b._is_truncation(74_826_437_216_602, Q21_TOTAL)
    assert not b._is_truncation(30_377_343_376, 60_377_343_375_858)   # GAS Q2-2017, refused


def test_the_code_column_cannot_look_like_a_truncation(b):
    """⚠️ `270` and `440` are the `Mã số` column, and the floor is what keeps them out."""
    assert FinancialsBuilder.TRUNCATION_MIN_DIGITS == 3
    assert not b._is_truncation(2, Q21_TOTAL)
    assert not b._is_truncation(44, Q21_TOTAL)


def test_a_reading_at_least_as_long_as_the_answer_is_never_a_truncation(b):
    assert not b._is_truncation(Q21_TOTAL, Q21_TOTAL)


# ── the repair ───────────────────────────────────────────────────────────────────────────

def test_without_the_repair_the_statement_is_refused_exactly_as_it_was(b):
    why = b.reconcile(bs(), whole_row())
    assert why and "!= liabilities + equity" in why


def test_seal_2_cannot_reach_it(b):
    """⚠️ The measurement that made a second repair necessary rather than a wider tolerance."""
    row = whole_row()
    b._total_from_section(bs(), row)
    assert row[RESOURCES] == Q21_DAMAGED, "SEAL-2's third lock must still refuse a truncation"


def test_the_truncated_total_is_rebuilt_from_the_sections(b):
    row = whole_row()
    b._total_from_counterpart(bs(), row)
    assert row[RESOURCES] == Q21_TOTAL


def test_reconcile_after_the_repair_is_trivial_and_is_not_the_evidence(b):
    """⚠️ §5 rule 21 — say it, do not count it. The evidence is taken BEFORE the write."""
    row, st = whole_row(), bs()
    assert b.reconcile(st, dict(row)) is not None, "refused before the repair"
    b._total_from_counterpart(st, row)
    assert b.reconcile(st, row) is None


# ── the locks ────────────────────────────────────────────────────────────────────────────

def test_the_counterpart_must_agree_to_the_dong(b):
    """⚠️ Not `_equal`: 1e-5 of 74 tn is ±748 million, and this claims an identity."""
    row = whole_row()
    row[ASSETS] = Q21_TOTAL + 1
    b._total_from_counterpart(bs(), row)
    assert row[RESOURCES] == Q21_DAMAGED


def test_an_absent_counterpart_repairs_nothing(b):
    row = whole_row()
    del row[ASSETS]
    b._total_from_counterpart(bs(), row)
    assert row[RESOURCES] == Q21_DAMAGED


def test_an_absent_part_repairs_nothing(b):
    """Lock 2 — a part that resolves through neither the column nor the text."""
    row = whole_row()
    del row["d_von_chu_so_huu"]
    b._total_from_counterpart(bs(), row)
    assert row[RESOURCES] == Q21_DAMAGED


def test_an_absent_total_is_a_statement_that_was_not_read(b):
    """Lock 1 — an ABSENCE is never repaired, only damage is."""
    row = whole_row()
    del row[RESOURCES]
    b._total_from_counterpart(bs(), row)
    assert RESOURCES not in row


def test_a_sound_statement_is_untouched(b):
    row = {ASSETS: Q21_TOTAL, RESOURCES: Q21_TOTAL, "d_von_chu_so_huu": Q21_EQUITY}
    b._total_from_counterpart(bs(), row)
    assert row[RESOURCES] == Q21_TOTAL


def test_the_parts_must_not_merely_be_close(b):
    """A section sum that misses the counterpart by a đồng is a DIFFERENT number."""
    row = whole_row()
    b._total_from_counterpart(bs(rows_with_liabilities(Q21_LIABILITIES + 1)), row)
    assert row[RESOURCES] == Q21_DAMAGED


# ── where the flag sits in the cascade ───────────────────────────────────────────────────

def test_the_flag_is_a_widening_and_no_strict_layer_runs_after_it():
    assert ParseLayer("x", "onnx", 200, truncated_total=True).is_strict is False
    strict = [i for i, l in enumerate(FinancialsBuilder.LAYERS) if l.is_strict]
    first = min(i for i, l in enumerate(FinancialsBuilder.LAYERS) if l.truncated_total)
    assert max(strict) < first


def test_the_block_is_contiguous_and_past_every_strict_layer():
    """⚠️ Only a balance sheet every other reading has already refused may reach it.

    ⚠️ **THE INVARIANT IS "PAST EVERY STRICT LAYER", NOT "LAST".** `test_cafef_cash_wording`
    pinned "last" and this block broke it the day it was added, on a change that broke
    nothing — a widening that follows another widening is fine, and a test that forbids it
    teaches the wrong lesson to whoever adds the next one.
    """
    layers = FinancialsBuilder.LAYERS
    at = [i for i, l in enumerate(layers) if l.truncated_total]
    assert len(at) == 5
    assert at == list(range(at[0], at[0] + 5)), "the block must stay contiguous"
    assert max(i for i, l in enumerate(layers) if l.is_strict) < at[0]
