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


def test_only_item_code_is_strict(parser):
    """One word of real text, or one figure in the value zone, and the line is ordinary."""
    assert parser._only_item_code([_word(290.0, 10.0, "53")], LO)
    assert parser._only_item_code([_word(290.0, 10.0, "VII")], LO)
    assert not parser._only_item_code([], LO)
    assert not parser._only_item_code([_word(290.0, 10.0, "Tiền mặt")], LO)
    assert not parser._only_item_code([_word(290.0, 10.0, "53"),
                                       _word(439.6, 10.0, "1.234")], LO)
    # a five-digit figure in the left margin is a FIGURE, not a row number
    assert not parser._only_item_code([_word(290.0, 10.0, "48919")], LO)


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
    parser.tail_continuation = True
    assert not parser._is_tail_page(_page(["1", "2"], CLOSING_LINE), CASH_FLOW)


def test_no_tail_marker_is_defined_for_the_other_statements(parser):
    """Only the cash flow has a measured case, so only the cash flow may use this.

    A balance-sheet or income-statement tail is a real shape too, but neither has been
    measured — and a marker added on argument is how a page gets swept in on argument.
    """
    parser.tail_continuation = True
    page = _page([str(i) for i in range(13)], CLOSING_LINE)
    assert not parser._is_tail_page(page, "balance_sheet")
    assert not parser._is_tail_page(page, "income_statement")
