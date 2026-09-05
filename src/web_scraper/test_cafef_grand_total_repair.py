"""`GTL-1` + `SEAL-2` — the two grand totals are ONE number, and the seal can break it.

⚠️ **`_equal` LET A 247-MILLION DISCREPANCY THROUGH ON A BALANCE SHEET.** `EQUAL_REL = 1e-5`
against 24.7 tn is ±247 million, and `TỔNG CỘNG TÀI SẢN` and `TỔNG CỘNG NGUỒN VỐN` are not two
measurements — they are one accounting number the filing prints twice. Measured over the 837
accepted balance sheets in `reports/pdf_ocr/` that carry both: **819 are EXACTLY equal**, none
is between 1 and 4 units, and the 7 distinct statements past that gap are all misread digits.

⚠️ **THE TOLERANCE IS IN UNITS OF THE STATEMENT AND AN ABSOLUTE ONE WOULD BE WRONG.** A bank
files in triệu đồng, so its rounding unit is 1,000,000 đồng: BID Q3-2019 differs by exactly one
of those and is the filing's own rounding, CTG Q1-2024 by a hundred of them and is a digit.

⚠️ **AND IT IS NOT EXACT.** FPT's Q1-2016 prints 24.695.453.363.506 on the asset side and
24.695.453.363.505 on the resources side — both read off the rendered page — so a filing may
genuinely round the two apart by a unit.

`SEAL-2` is what the refusal then reaches. That quarter is signed across `TỔNG CỘNG NGUỒN VỐN`
and the reading does not converge: **eighteen configurations of dpi × crop_pad × red_channel
return eighteen different wrong answers** against one printed 24.695.453.363.505. The form
prints the arithmetic (`440 = 300 + 400`) and both parts read identically at all eighteen, so
the sum is the two readings that survived put together the way the document says to.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import BALANCE_SHEET, Row, Statement

ASSETS, RESOURCES = "tong_cong_tai_san", "tong_cong_nguon_von"
# FPT Q1-2016, read off the rendered pages at 340 dpi.
A_SHORT, B_LONG = 17_461_658_575_148, 7_233_794_788_358
TOTAL_ASSETS = 24_695_453_363_506          # printed, code 280
LIABILITIES = 14_052_382_733_257           # printed, code 300
EQUITY_I = 10_640_320_630_248              # printed, code 410
FUNDS_II = 2_750_000_000                   # printed, code 430
TOTAL_RESOURCES = 24_695_453_363_505       # printed, code 600
SEALED = 24_695_452_363_505                # what `onnx@200+joinlost+red` returns for it


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def bs(rows=None, unit=1):
    rows = rows or [Row(label=f"x{i}", key=f"x{i}", number="", values=[i + 1, i + 1])
                    for i in range(FinancialsBuilder.MIN_ROWS)]
    return Statement(report=BALANCE_SHEET, pages=[1], unit=unit, n_columns=2, rows=rows)


# ── GTL-1 · the tolerance ────────────────────────────────────────────────────────────────

def test_the_two_grand_totals_may_round_apart_by_a_unit(b):
    """FPT Q1-2016 prints ...506 and ...505 — the filing's own rounding, verified on the page."""
    mapped = {ASSETS: TOTAL_ASSETS, RESOURCES: TOTAL_RESOURCES}
    assert b.reconcile(bs(), mapped) is None


def test_a_misread_digit_is_refused_where_it_used_to_pass(b):
    """⚠️ 1,000,001 đồng on 24.7 tn is 4e-8 relative — four orders inside `_equal`."""
    mapped = {ASSETS: TOTAL_ASSETS, RESOURCES: SEALED}
    why = b.reconcile(bs(), mapped)
    assert why and "!= liabilities + equity" in why


def test_the_tolerance_is_in_units_of_the_statement(b):
    """BID Q3-2019 differs by exactly ONE triệu đồng and is sound; CTG Q1-2024 by a HUNDRED."""
    ok = {ASSETS: 1_425_398_552_000_000, RESOURCES: 1_425_398_551_000_000}
    bad = {ASSETS: 2_032_613_506_000_000, RESOURCES: 2_032_613_606_000_000}
    assert b.reconcile(bs(unit=1_000_000), ok) is None
    assert b.reconcile(bs(unit=1_000_000), bad) is not None
    # ⚠️ and the SAME pair in đồng is a misread digit, which is why an absolute bar is wrong
    assert b.reconcile(bs(unit=1), ok) is not None


def test_the_bar_is_four_units():
    assert FinancialsBuilder.TOTALS_TOL == 4


# ── SEAL-2 · the repair ──────────────────────────────────────────────────────────────────

def rows_with_liabilities():
    """FPT Q1-2016's liabilities line as the OCR returns it: the column headings glued on.

    ⚠️ That is why the fallback exists at all — `c_no_phai_tra` cannot be taken by the ordered
    walk from this key, and `Statement.find` tests containment first, so the needle reaches it.
    """
    return [Row(label="A - NO PHAI TRA", number="",
                key="ma_thuyet_stt_nguon_von_so_minh_a_n_no_phai_tra",
                values=[LIABILITIES, 15_863_302_791_405])] + \
           [Row(label=f"x{i}", key=f"x{i}", number="", values=[i + 1, i + 1])
            for i in range(FinancialsBuilder.MIN_ROWS)]


def test_the_sealed_total_is_repaired_from_the_sections(b):
    row = {RESOURCES: SEALED, "d_von_chu_so_huu": EQUITY_I,
           "ii_nguon_kinh_phi_va_quy_khac_430": FUNDS_II}
    b._total_from_section(bs(rows_with_liabilities()), row)
    assert row[RESOURCES] == TOTAL_RESOURCES


def test_and_the_repaired_statement_then_reconciles(b):
    row = {ASSETS: TOTAL_ASSETS, RESOURCES: SEALED, "d_von_chu_so_huu": EQUITY_I,
           "ii_nguon_kinh_phi_va_quy_khac_430": FUNDS_II,
           "a_tai_san_ngan_han": A_SHORT, "b_tai_san_dai_han": B_LONG}
    st = bs(rows_with_liabilities())
    assert b.reconcile(st, dict(row)) is not None, "refused before the repair"
    b._total_from_section(st, row)
    assert b.reconcile(st, row) is None


def test_a_total_the_page_never_yielded_is_not_invented(b):
    """A repair of a damaged reading, never a substitute for an absent one."""
    for row in ({}, {RESOURCES: None}):
        before = dict(row)
        row.update({"d_von_chu_so_huu": EQUITY_I,
                    "ii_nguon_kinh_phi_va_quy_khac_430": FUNDS_II})
        b._total_from_section(bs(rows_with_liabilities()), row)
        assert row.get(RESOURCES) == before.get(RESOURCES)


def test_a_sum_that_is_not_the_same_figure_is_refused(b):
    """⚠️ Within `_equal` means "this reading, damaged". A sum that is a DIFFERENT number means
    a part was misread, and writing it would replace one wrong figure with another."""
    row = {RESOURCES: 1, "d_von_chu_so_huu": EQUITY_I,
           "ii_nguon_kinh_phi_va_quy_khac_430": FUNDS_II}
    b._total_from_section(bs(rows_with_liabilities()), row)
    assert row[RESOURCES] == 1


def test_an_unresolvable_part_leaves_the_total_alone(b):
    """No liabilities line on the page and no column for it — nothing to sum."""
    row = {RESOURCES: SEALED, "d_von_chu_so_huu": EQUITY_I,
           "ii_nguon_kinh_phi_va_quy_khac_430": FUNDS_II}
    b._total_from_section(bs(), row)
    assert row[RESOURCES] == SEALED


def test_the_optional_terms_are_tried_both_ways(b):
    """A pre-2015 form prints them as separate top-level lines; a modern one folds them into D.

    Here D already IS the 400 line, so the sum WITHOUT the extra is the one that closes.
    """
    row = {RESOURCES: SEALED, "d_von_chu_so_huu": EQUITY_I + FUNDS_II,
           "ii_nguon_kinh_phi_va_quy_khac_430": FUNDS_II}
    b._total_from_section(bs(rows_with_liabilities()), row)
    assert row[RESOURCES] == TOTAL_RESOURCES


def test_the_liabilities_fallback_is_the_only_one(b):
    """⚠️ `d_von_chu_so_huu` is deliberately absent: "von chu so huu" is contained in the grand
    total's own name, which is `NST-1`'s collision, and that column maps on its own wherever
    the form is ordinary. A fallback for a part not measured to need one is a hazard bought
    for nothing."""
    assert set(FinancialsBuilder.SECTION_PART_TEXT) == {
        "a_tai_san_ngan_han", "b_tai_san_dai_han", "c_no_phai_tra"}


def test_at_most_ONE_of_the_two_totals_is_repaired(b):
    """⚠️ And that is what stops the repair rescuing a statement it should not.

    FPT's Q1-2015 balance sheet is the case: `A + B` = 22,206,060,162,448 against a printed
    22,206,060,192,417 — 29,969 apart, so a component is misread and WITHIN `_equal`. The
    ASSETS entry is tried first and repairs, then RETURNS, so the resources total keeps its own
    reading — and `GTL-1` then refuses the statement because the two are 30,000 apart. Repairing
    both would have made them agree and written two figures nothing corroborates.
    """
    row = {ASSETS: 22_206_060_192_417, RESOURCES: 22_206_060_192_448,
           "a_tai_san_ngan_han": 10_000_000_000_000, "b_tai_san_dai_han": 12_206_060_162_448,
           "d_von_chu_so_huu": 8_206_060_162_448}
    st = bs([Row(label="A - NO PHAI TRA", number="", key="a_no_phai_tra",
                 values=[14_000_000_000_000, 0])] +
            [Row(label=f"x{i}", key=f"x{i}", number="", values=[i + 1, i + 1])
             for i in range(FinancialsBuilder.MIN_ROWS)])
    b._total_from_section(st, row)
    assert row[ASSETS] == 22_206_060_162_448, "the assets total was repaired"
    assert row[RESOURCES] == 22_206_060_192_448, "and the resources total was left alone"
    assert b.reconcile(st, row) is not None, "so the statement is still refused"


# ── the cascade ──────────────────────────────────────────────────────────────────────────

def test_every_total_layer_is_late():
    layers = FinancialsBuilder.LAYERS
    block = [i for i, l in enumerate(layers, 1) if l.total_from_section]
    strict = [i for i, l in enumerate(layers, 1) if l.is_strict]
    assert block
    assert min(block) > max(strict)


def test_the_flag_is_a_widening_and_is_counted_as_one():
    assert not ParseLayer("x", "onnx", 200, total_from_section=True).is_strict


def test_the_block_carries_the_merged_row_keys_it_needs():
    """FPT Q1-2016's liabilities line arrives with the column headings glued onto it, so the
    suffix keys `merged_tail` builds are what let the PARTS resolve at all."""
    for l in FinancialsBuilder.LAYERS:
        if l.total_from_section:
            assert l.merged_tail and l.red_channel
