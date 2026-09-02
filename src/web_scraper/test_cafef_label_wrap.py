"""`tail_continuation` + `label_wrap` — the BID Q1-2012 cash flow, pinned without a PDF.

Two independent defects kept that quarter `missing`, and the second is the dangerous one.

  1. **The statement's LAST page was thrown away.** `_fill_continuations` gives an
     unidentifiable page to the statement running through it only when the page holds
     `MIN_TABLE_WORDS = 15` figures — the rule that keeps a signature page out. A statement's
     final page fails it for the very reason it exists: a few closing rows and then the
     signatures. BID's cash flow runs pages 5-7 and page 7 holds **13** figures, among them
     the closing balance, which is the one anchor `reconcile` cannot do without.
  2. **The label WRAPPED AROUND its own figures and was torn in half.** `table_rows` builds a
     row's label from the lines ABOVE the figures. Here the filing prints the label's first
     half, then the item code on its own baseline, then the second half beside the figures —
     and that second half ("thời điểm đầu kỳ" / "cuối kỳ") is the ONLY thing that tells the
     opening balance from the closing one.

⚠️ Defect 2 is asserted heavily because it does not fail loudly. With the page recovered but
the labels still torn, `map_to_schema` handed BOTH cash figures to the wrong accounts and
`reconcile` and `sane` BOTH PASSED — a wrong figure written as `pdf`. A test that only checked
"the statement is no longer missing" would have gone green on that.
"""
import pytest

from web_scraper.cafef_pdf_parser import PdfParser, CASH_FLOW


LO = 400.0                      # labels end left of this, figures right of it


def _word(x1, y0, text, h=12.0):
    """A word box in the shape `table_rows` reads: (x0, y0, x1, y1, text, …)."""
    return (x1 - 60.0, y0, x1, y0 + h, text, 0, 0, 0)


def _tail_page_words():
    """Page 7 as the OCR actually reads it — measured off the filing, not invented.

    The geometry is the document's own: the item code sits on its own baseline between the two
    halves of a two-line label, and the figures sit beside whichever half they were typeset
    against — the SECOND for the opening balance, the FIRST for the closing one. The FX
    adjustment (code 54) prints no figure at all, which is what makes its label bleed into the
    closing balance below it unless the code is read as an item boundary.
    """
    return [
        _word(330.0, 67.7, "Tiền và các khoản tương đương tiền tại"),
        _word(290.0, 76.0, "53"),                       # the code, alone on its line
        _word(330.0, 80.6, "thời điểm đầu kỳ"),
        _word(439.6, 80.6, "48.919.272.456.242"),
        _word(564.6, 80.6, "56.985.300.509.358"),
        _word(330.0, 97.6, "Điều chỉnh ảnh hưởng của thay đổi ty"),
        _word(290.0, 106.6, "54"),                      # code 54 prints NO figure
        _word(330.0, 113.0, "giá"),
        _word(330.0, 128.2, "Tiền và các khoản tương đương tiến tại"),
        _word(439.6, 138.2, "43.180.157.643.381"),
        _word(564.6, 138.2, "54.237.979.881.580"),
        _word(330.0, 144.0, "thời điểm cuối kỳ"),
    ]


OPEN = 48_919_272_456_242
CLOSE = 43_180_157_643_381
COLUMNS = [439.6, 564.6]


@pytest.fixture(scope="module")
def parser():
    return PdfParser.__new__(PdfParser)


def _rows(parser, on):
    parser.realign_rows = False
    parser.label_wrap = on
    return parser.table_rows({0: _tail_page_words()}, COLUMNS)


def test_label_wrap_off_tears_the_label_in_half(parser):
    """The defect itself, so the fix cannot be quietly undone."""
    keys = [r.key for r in _rows(parser, False)]
    assert "thoi_diem_dau_ky" in keys           # severed from its own cash phrase
    assert not any(k.endswith("thoi_diem_cuoi_ky") for k in keys)


def test_label_wrap_rejoins_both_halves(parser):
    by_val = {r.values[0]: r.key for r in _rows(parser, True)}
    assert by_val[OPEN].startswith("tien_va_cac_khoan_tuong_duong_tien_tai")
    assert by_val[OPEN].endswith("thoi_diem_dau_ky")
    # ⚠️ The closing row must carry its OWN suffix. Without it the two rows are
    # indistinguishable and the ordered walk fills the accounts in the wrong order, silently.
    assert "thoi_diem_cuo" in by_val[CLOSE]


def test_the_fx_label_does_not_bleed_into_the_closing_balance(parser):
    """Code 54 prints no figure, so its label is still pending when 55's figures arrive.

    Keeping the carry across a code line is not enough on its own: the FX wording would then
    prefix the closing label and push its discriminating suffix past `slug`'s 60-character
    cap, which is exactly where it was lost the first time this was attempted.
    """
    by_val = {r.values[0]: r.key for r in _rows(parser, True)}
    assert not by_val[CLOSE].startswith("dieu_chinh")


def test_the_two_balances_are_not_swapped(parser):
    """The failure that passed every gate: both figures present, both on the wrong account."""
    by_val = {r.values[0]: r.key for r in _rows(parser, True)}
    assert by_val[OPEN] != by_val[CLOSE]
    assert by_val[OPEN].endswith("dau_ky")
    assert "cuo" in by_val[CLOSE] and not by_val[CLOSE].endswith("dau_ky")


def test_comparative_column_is_kept_separate(parser):
    """Both period columns survive — the fix must not collapse one onto the other."""
    rows = [r for r in _rows(parser, True) if r.values[0] in (OPEN, CLOSE)]
    assert [r.values for r in rows] == [[OPEN, 56_985_300_509_358],
                                        [CLOSE, 54_237_979_881_580]]


def test_a_far_gap_still_ends_a_pending_label(parser):
    """`CARRY_GAP` is what keeps "contributed nothing" from meaning "never ends".

    A line that yields neither a label nor a figure is not evidence the label ended — but it
    is also what a genuine section break looks like, so the carry survives only while the
    figures stay within `CARRY_GAP` of the line that fed it.
    """
    parser.realign_rows = False
    parser.label_wrap = True
    far = [_word(330.0, 100.0, "Một nhãn bị bỏ lại từ mục trước"),
           _word(590.0, 140.0, "DẤU"),                      # noise, well past CARRY_GAP
           _word(330.0, 180.0, "Chi phí lãi"), _word(439.6, 180.0, "1.234")]
    keys = [r.key for r in parser.table_rows({0: far}, COLUMNS)]
    assert keys == ["chi_phi_lai"], keys

    near = [_word(330.0, 100.0, "Tiền và các khoản tương đương tiền tại"),
            _word(590.0, 108.0, "DẤU"),                     # noise, inside CARRY_GAP
            _word(330.0, 114.0, "thời điểm cuối kỳ"), _word(439.6, 114.0, "1.234")]
    keys = [r.key for r in parser.table_rows({0: near}, COLUMNS)]
    assert keys == ["tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky"], keys


# ── the page that was thrown away ────────────────────────────────────────────────

def _page(numbers, text):
    words = [_word(439.6, 100.0 + i * 14.0, n) for i, n in enumerate(numbers)]
    return {"words": words, "text": text, "kind": None, "from_form": False, "width": 600.0}


CLOSING_LINE = "Tiền và các khoản tương đương tiền tại thời điểm cuối kỳ"


def test_tail_page_is_below_the_table_threshold(parser):
    """The premise: page 7 really is too sparse for `_is_table`, so this is not a no-op."""
    assert not parser._is_table(_page([str(i) for i in range(13)], CLOSING_LINE))


def test_tail_page_is_admitted_only_with_the_flag(parser):
    page = _page([str(i) for i in range(13)], CLOSING_LINE)
    parser.tail_continuation = False
    assert not parser._is_tail_page(page, CASH_FLOW)
    parser.tail_continuation = True
    assert parser._is_tail_page(page, CASH_FLOW)


def test_a_page_without_the_closing_line_is_never_admitted(parser):
    """The threshold is not lowered — admission is on POSITIVE evidence.

    A signature page or a note carrying a handful of stray figures stays out however many it
    holds, which is what makes this safe to run after every other layer has already failed.
    """
    parser.tail_continuation = True
    stray = _page([str(i) for i in range(14)],
                  "Hà Nội, ngày 20 tháng 4 năm 2012 Lập bảng Kiểm soát Kế toán Trưởng")
    assert not parser._is_tail_page(stray, CASH_FLOW)


def test_a_tail_page_needs_some_figures(parser):
    """⚠️ THE FLOOR IS TWO SINCE 2026-09-02, AND TWO IS WHAT THE CLOSING LINE CARRIES.

    This asserted that a page holding the closing line and TWO figures is refused, which
    pinned `MIN_TAIL_WORDS = 4`. That number came from the assumption that all three
    cash-balance rows land on the tail page; CTG's Q1-2014 breaks the page after the FX
    line, so its tail page holds one row -- a note reference and two period figures -- and
    the closing balance 88.180.310.933.901, read correctly at `onnx@200`, was thrown away.
    The count was never the guard: admission is on the closing LINE, and a page carrying it
    with both of its period columns is a tail page.
    """
    parser.tail_continuation = True
    assert parser._is_tail_page(_page(["1", "2"], CLOSING_LINE), CASH_FLOW)
    # ...and one stray figure beside the words is still not a table
    assert not parser._is_tail_page(_page(["1"], CLOSING_LINE), CASH_FLOW)


def test_no_tail_marker_is_defined_for_the_other_statements(parser):
    """Only the cash flow has a measured case, so only the cash flow may use this.

    A balance-sheet or income-statement tail is a real shape too, but neither has been
    measured — and a marker added on argument is how a page gets swept in on argument.
    """
    parser.tail_continuation = True
    page = _page([str(i) for i in range(13)], CLOSING_LINE)
    assert not parser._is_tail_page(page, "balance_sheet")
    assert not parser._is_tail_page(page, "income_statement")


# ── the unit the statement did not name ──────────────────────────────────────────

def _unit_page(kind, text):
    return {"words": [], "text": text, "kind": kind, "from_form": True, "width": 600.0}


MILLIONS = "Đơn vị: Triệu VNĐ"
SILENT = "STT Chỉ tiêu Kỳ này Kỳ trước"


def test_declared_unit_separates_silence_from_dong(parser):
    """⚠️ `unit_of` returns 1 for BOTH, and that is the defect it hides."""
    pages = {0: _unit_page("cash_flow", SILENT)}
    assert parser.declared_unit(pages, [0]) is None
    assert parser.unit_of(pages, [0]) == 1
    pages = {0: _unit_page("cash_flow", MILLIONS)}
    assert parser.declared_unit(pages, [0]) == 1_000_000


def test_document_unit_is_taken_from_the_other_statements(parser):
    """BID Q1-2026: neither cash-flow page names a unit; the balance sheet does."""
    pages = {0: _unit_page("balance_sheet", MILLIONS),
             1: _unit_page("cash_flow", SILENT),
             2: _unit_page("cash_flow", SILENT)}
    assert parser.unit_of(pages, [1, 2]) == 1               # what the pages say
    assert parser.document_unit(pages) == 1_000_000         # what the filing says


def test_document_unit_abstains_when_nothing_declares_one(parser):
    pages = {0: _unit_page("balance_sheet", SILENT), 1: _unit_page("cash_flow", SILENT)}
    assert parser.document_unit(pages) is None


def test_notes_pages_do_not_vote_on_the_unit(parser):
    """Only the statements. A note is printed in whatever unit the note wants."""
    pages = {0: _unit_page("notes", MILLIONS), 1: _unit_page("cash_flow", SILENT)}
    assert parser.document_unit(pages) is None
