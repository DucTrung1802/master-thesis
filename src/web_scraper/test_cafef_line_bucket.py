"""Two defects that between them wrote 48,726,111,955 as BSR's Q3-2019 pre-tax profit,
where the filing prints 624,185,898,676. Pinned without a PDF, a network or an OCR engine.

Both are in the DEFAULT path, and they had to be: the statement is accepted at `onnx@200`,
layer 1 of 55, so no later layer is ever reached (§6-2-untricies — when the gates cannot see
the defect, the repair cannot be an escalation).

  1. **`table_rows` took the FIRST line bucket within `Y_TOL`, not the NEAREST.** A stray `)`
     off the right margin opened a bucket 3.60pt from a figure whose own line sat 0.72pt
     away, and the figure joined the stray. Line 14 lost its 9-month column and the orphan
     bucket merged into line 15, so the pre-tax profit line was written with line 14's
     9-month figure — which is exactly `51,026,059,759 - 2,299,947,804`.

  2. **`_split_merged` read the VAS item-code formula as a merged seam.** The corporate form
     prints each line's own code arithmetic in the label — "(50 = 30 + 40)" — which is the
     same shape as a header OCR swept into the row beneath it. Splitting there returned
     `3040`, a key that answers no account, so the line was dropped entirely. Four accounts
     went that way on one statement: net revenue, gross profit, pre-tax and post-tax profit.

⚠️ NEITHER FAILS LOUDLY. An income statement is anchored on PBT alone, so `reconcile` never
sums the components against it and `sane` only compares one magnitude to a band. A test
asserting "the statement parses" goes green on both.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.cafef_pdf_parser import PdfParser


COLUMNS = [316.2, 390.9, 465.8, 541.0]          # BSR Q3-2019 page 4, as detected
LO = COLUMNS[0] - PdfParser.LABEL_GAP


def _word(x0, y0, x1, text, h=9.0):
    return (x0, y0, x1, y0 + h, text, 0, 0, 0)


def _page():
    """Lines 14 and 15 of BSR's Q3-2019 income statement, at their measured coordinates.

    ⚠️ **THE ORDER IS THE RECOGNISER'S, NOT THE PAGE'S, AND IT IS THE WHOLE POINT.** The
    stray `)` is emitted before the 9-month figure that sits above it, so it opens a bucket
    first. Sorting these by y would measure a different algorithm and hide the defect.
    """
    return [
        _word(87.8, 356.88, 187.2, "14. Lợi nhuận khác (40?31-32)"),
        _word(344.4, 357.36, 390.7, "3.943.245.732"),
        _word(205.2, 357.60, 215.8, "40"),
        _word(266.4, 357.60, 315.8, "24.490.662.593"),
        _word(493.2, 357.84, 541.2, "3.943.245.732"),
        _word(587.0, 362.16, 591.1, ")"),          # the stray — off the right margin
        _word(416.2, 358.56, 465.6, "48.726.111.955"),
        _word(86.6, 366.96, 195.1, "15. Tổng lọi nhuận kế toán trước"),
        _word(205.2, 373.20, 215.8, "50"),
        _word(263.3, 373.92, 316.3, "624.185.898.676"),
        _word(332.2, 375.36, 390.2, "1.250.303.326.474"),
        _word(406.1, 374.88, 465.1, "1.325.840.206.502"),
        _word(481.2, 375.36, 541.2, "1.250.303.326.474"),
        _word(87.1, 376.80, 140.9, "thuế (50-30+40)"),
    ]


OTHER_PROFIT_9M = 48_726_111_955                # = 51,026,059,759 - 2,299,947,804
PBT_QUARTER = 624_185_898_676


@pytest.fixture
def rows():
    return PdfParser().table_rows({0: _page()}, COLUMNS)


def _find(rows, needle):
    return next((r for r in rows if needle in r.key), None)


# ── 1 · the nearest bucket wins ──────────────────────────────────────────────

def test_a_stray_box_does_not_steal_a_figure_from_the_line_it_belongs_to(rows):
    """The 9-month figure is 0.72pt from line 14 and 3.60pt from the stray."""
    line14 = _find(rows, "loi_nhuan_khac")
    assert line14 is not None
    assert line14.values[2] == OTHER_PROFIT_9M


def test_the_pbt_line_carries_its_own_figure_and_not_its_neighbours(rows):
    """With the orphan bucket gone the label's two halves rejoin, and the row that answers
    `15_tong_loi_nhuan_ke_toan_truoc_thue` holds the quarter the filing prints."""
    pbt = _find(rows, "tong_loi_nhuan_ke_toan_truoc_thue")
    assert pbt is not None, "the wrapped label did not rejoin"
    assert pbt.values[0] == PBT_QUARTER
    assert OTHER_PROFIT_9M not in pbt.values


def test_nearest_is_a_refinement_and_not_a_re_grouping():
    """Where only one bucket is in tolerance, nearest IS first — so a page with no competing
    bucket cannot move. This is what bounds the change to pages that had two candidates."""
    lines = {100.0: []}
    assert PdfParser._line_key(lines, 102.0, 4.0) == 100.0
    assert PdfParser._line_key(lines, 107.0, 4.0) == 107.0        # opens its own
    lines[105.0] = []
    assert PdfParser._line_key(lines, 104.0, 4.0) == 105.0        # nearest, not first
    assert PdfParser._line_key(lines, 101.5, 4.0) == 100.0


# ── 2 · an item-code formula is not a merged seam ────────────────────────────

@pytest.mark.parametrize("key, keeps", [
    # The VAS formulas — every one of these lost its account before 2026-09-01.
    ("doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu_10_01_02", True),
    ("loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu_20_10_11", True),
    ("loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_30_204_21_22", True),
    ("tong_loi_nhuan_ke_toan_truoc_thue_50_30_40", True),
    ("loi_nhuan_sau_thue_thu_nhap_doanh_nghiep_60_50_51_52", True),
])
def test_a_numeric_tail_is_the_line_s_own_code_arithmetic(key, keeps):
    """A formula tail is digits and separators; a line item's name never is."""
    assert (FinancialsBuilder()._split_merged(key) == key) is keeps


@pytest.mark.parametrize("key, tail", [
    # The genuine seams the pattern exists for — each keeps splitting.
    ("nhung_thay_doi_ve_tai_san_hoat_dong_09_tang_giam_cac_khoan_tien_gui",
     "tang_giam_cac_khoan_tien_gui"),
    ("b_hao_mon_bat_dong_san_dau_tu_xii_tai_san_co_khac", "tai_san_co_khac"),
])
def test_a_real_merged_row_still_splits_at_its_marker(key, tail):
    assert FinancialsBuilder()._split_merged(key) == tail


def test_the_loose_pattern_carries_the_same_rule():
    """`relax_merged_seam` widens WHICH markers count, never what a tail may be."""
    b = FinancialsBuilder()
    formula = "tong_loi_nhuan_ke_toan_truoc_thue_50_30_40"
    assert b._split_merged(formula, relax_merged_seam=True) == formula
    merged = "b_no_phai_tra_va_von_chu_so_huu_i_cac_khoan_no_chinh_phu"
    assert b._split_merged(merged, relax_merged_seam=True) == "cac_khoan_no_chinh_phu"


def test_the_repaired_key_reaches_its_account():
    """The point of keeping the key whole: it scores, where `3040` scored nothing."""
    b = FinancialsBuilder()
    key = b._split_merged("tong_loi_nhuan_ke_toan_truoc_thue_50_30_40").replace("_", "")
    account = "tong_loi_nhuan_ke_toan_truoc_thue".replace("_", "")
    assert b._label_score(account, key) >= b.SCHEMA_MATCH
