"""`IBC-1` and `CLN-1` — two ways a page's own figure never reaches the row. No PDF, no engine.

`IBC-1`  the page loses BOTH its title and its form code, so `_page_kind` returns `None` and
         `_fill_continuations` gives it to the statement running ABOVE it. FPT's Q1-2012 income
         statement is page 4 of an image-only scan whose title line came back as gibberish
         ("Cho ký hoạt động tù ngày do tháng trong nhành thiếp…"); the statement was reported
         absent AND the balance sheet was written from pages [2, 3, 4], with a page of P&L rows
         inside it. It blocked two more cells: Q2-2012 and Q4-2012 print a cumulative P&L that
         cannot be de-cumulated while Q1-2012 is `missing`.

`CLN-1`  a thousands separator read as a COLON. `parse_num` refused the whole token, so the row
         kept its prior-year cell and lost the current one: FPT's Q3-2025 balance sheet returns
         `82.738.304.930:449` for a `TỔNG CỘNG TÀI SẢN` equal to `A + B` and to
         `TỔNG CỘNG NGUỒN VỐN` to the đồng, and the quarter was refused `no total assets` at
         every layer of the cascade.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser)


# ── IBC-1 ─────────────────────────────────────────────────────────────────────────────

# FPT Q1-2012 page 4's header, as OCR returns it: no statement title, no form code, and the
# two column headings that say the table carries a quarter AND a year to date.
LOST_TITLE = ("CÔNG TY CÔ PHẢN FPT\n"
              "Tòa nhà FPT Cầu Giấy, Phố Duy Tân\n"
              "Phường Dịch Vọng Hậu, Quận Cầu Giấy\n"
              "Báo cáo tài chính hợp nhất\n"
              "Hà Nội, CHXHCN Việt Nam\n"
              "Cho ký hoạt động tù ngày do tháng trong nhành thiếp nhay nhay nhành pháp\n"
              "đến ngày 31 tháng 03 năm 2012\n"
              "STT CHỈ TIÊU Mã Thuyết\n"
              "QUÝ 1 NĂM 2012\n"
              "Luỹ kể từ đầu năm đến cuối quý này\n")
NO_MARKERS = ("CÔNG TY CÔ PHẢN FPT\nBÁO CÁO TÀI CHÍNH\n"
              "STT CHỈ TIÊU Mã Thuyết\nSố cuối kỳ\nSố đầu năm\n")


def _pages(*texts, words=40):
    boxes = [(400.0, 10.0 * i, 460.0, 10.0 * i + 9.0, "1.234.567", 0, 0, i)
             for i in range(words)]
    return {i: {"text": t, "words": list(boxes), "kind": None, "from_form": False,
                "width": 612.0}
            for i, t in enumerate(texts)}


def _parser():
    p = PdfParser.__new__(PdfParser)
    p.income_by_columns = True
    return p


def test_the_page_is_named_by_its_two_column_headings():
    pages = _pages(LOST_TITLE)
    _parser()._classify_income_by_columns(pages)
    assert pages[0]["kind"] == INCOME_STATEMENT


def test_a_page_printing_only_BALANCE_headings_is_left_alone():
    """"Số cuối kỳ / Số đầu năm" are balances at a date, not a quarter and a year to date."""
    pages = _pages(NO_MARKERS)
    _parser()._classify_income_by_columns(pages)
    assert pages[0]["kind"] is None


def test_a_page_the_classifier_ALREADY_named_is_never_re_named():
    pages = _pages(LOST_TITLE)
    pages[0]["kind"] = CASH_FLOW
    _parser()._classify_income_by_columns(pages)
    assert pages[0]["kind"] == CASH_FLOW


def test_a_page_that_is_not_a_TABLE_is_left_alone():
    pages = _pages(LOST_TITLE, words=PdfParser.MIN_TABLE_WORDS - 1)
    _parser()._classify_income_by_columns(pages)
    assert pages[0]["kind"] is None


def test_only_the_FIRST_hit_is_taken():
    """A filing has ONE income statement, so a second page carrying the same markers is not a
    second one — and on the measured corpus 10 of the 26 pages printing both markers are CASH
    FLOWS, which is exactly why this does not sweep."""
    pages = _pages(LOST_TITLE, LOST_TITLE)
    _parser()._classify_income_by_columns(pages)
    assert pages[0]["kind"] == INCOME_STATEMENT
    assert pages[1]["kind"] is None


def test_the_marker_alone_does_NOT_identify_a_statement():
    """⚠️ MEASURED, AND PINNED BECAUSE IT IS THE REASON FOR THE GUARDS. Over the 7,389
    text-layer pages of the eight parsed tickers, 26 print both markers: 7 income statements,
    **10 CASH FLOWS**, 4 balance sheets, 2 notes and 3 unclassified. So "both columns ⇒ income
    statement" is false for 16 of 26; what makes this safe is that the page is UNCLASSIFIED and
    that the document has found no income-statement page anywhere."""
    p = PdfParser.__new__(PdfParser)
    # a CASH FLOW header that prints both markers — the population this must never claim
    cash = ("BÁO CÁO LƯU CHUYỂN TIỀN TỆ HỢP NHẤT\nMẫu số B03-DN/HN\n"
            "CHỈ TIÊU\nQuý III\nLuỹ kế từ đầu năm\n")
    assert p._prints_quarter_column(cash), "the marker really is on a cash flow"
    p.loose_form_code = False
    p.title_over_form = False
    p.column_header_blind = False
    assert p._page_kind(cash)[0] == CASH_FLOW, "…and the classifier names it, so it is not seen"


def test_the_flag_is_off_by_default_and_is_a_PARSE_key_only():
    from web_scraper.cafef_financials import ocr_key as _ok

    assert ParseLayer("x", "onnx", 200).income_by_columns is False
    a = ParseLayer("a", "onnx", 200)
    c = ParseLayer("c", "onnx", 200, income_by_columns=True)
    assert parse_key(a) != parse_key(c)
    assert _ok(a) == _ok(c)


def test_a_bycol_layer_is_WIDENING_and_runs_after_every_strict_read():
    layers = FinancialsBuilder.LAYERS
    flagged = [i for i, l in enumerate(layers) if l.income_by_columns]
    assert flagged, "no +bycol layer in the cascade"
    assert all(not layers[i].is_strict for i in flagged)
    assert min(flagged) > max(i for i, l in enumerate(layers) if l.is_strict)


# ── CLN-1 ─────────────────────────────────────────────────────────────────────────────

def test_a_colon_in_a_separator_position_is_the_separator():
    assert PdfParser.parse_num("82.738.304.930:449") == 82_738_304_930_449
    assert PdfParser.NUM_RE.match("82.738.304.930:449")
    # …and it survives the brackets a negative figure is printed in
    assert PdfParser.parse_num("(1.234:567)") == -1_234_567


@pytest.mark.parametrize("clock", ["10:30", "09:15", "1:2", "23:59"])
def test_a_CLOCK_is_refused_by_BOTH_the_pattern_and_the_parser(clock):
    """⚠️ THE POSITION IS THE WHOLE RULE. A separator is followed by exactly THREE digits;
    a clock is not. Without that, `10:30` parses as 10 — its tail reads as a decimal — and,
    worse, `NUM_RE` would admit it so a time in the value zone would join the column
    clustering, adding a box and losing a value."""
    assert not PdfParser.NUM_RE.match(clock), clock
    assert PdfParser.parse_num(clock) is None, clock


def test_the_substitution_is_LENGTH_PRESERVING():
    """`_split_number_runs` apportions a box by CHARACTER OFFSET, so a rewrite that shortened
    the text would move every right edge it computes — and the right edge is what the column
    clustering reads. `SLASH_GAP_RE` carries the same constraint for the same reason."""
    t = "82.738.304.930:449"
    assert len(PdfParser.COLON_SEPARATOR_RE.sub(".", t)) == len(t)


def test_an_ordinary_figure_is_untouched():
    for t in ("82.738.304.930.449", "(2.298.616)", "-", "1.234", "0"):
        before = PdfParser.parse_num(t)
        assert PdfParser.COLON_SEPARATOR_RE.sub(".", t) == t
        assert PdfParser.parse_num(t) == before


def test_a_numeric_RUN_may_carry_a_colon_so_the_splitter_still_sees_the_box():
    """`NUM_RUN_RE` decides whether a box holding several figures is a numeric run at all. A
    box whose separator came back as a colon must still qualify, or the run is neither split
    nor parsed — which is `SLH-1`'s defect with a different character."""
    assert PdfParser.NUM_RUN_RE.match("82.738.304.930:449 71.999.995.678.620")
