"""`PST-1` — a one-page poster read as the panels and statements it prints side by side.

Pinned without a PDF or an OCR engine, on the layout of SSB's 2008-2015 annual summaries: the
auditor's report in the left panel, the balance sheet down the middle, its equity section and the
income statement stacked in the right panel.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import BALANCE_SHEET, INCOME_STATEMENT, PdfParser

WIDTH = 737.0


def _w(x0, x1, y, text):
    return (x0, y, x1, y + 7.0, text, 0, 0, 0)


def _figure(n):
    return "{:,}".format(n).replace(",", ".")


def _prose(x0, x1, y0, n):
    words = []
    for i in range(n):
        y = y0 + 9.0 * i
        step = (x1 - x0) / 8.0
        words += [_w(x0 + k * step, x0 + (k + 1) * step - 3.0, y, "kiểm") for k in range(8)]
    return words


def _table(x0, y0, n, now, prior, prior_left=None):
    words = []
    for i in range(n):
        y = y0 + 11.0 * i
        words.append(_w(x0, x0 + 10.0, y, "I."))
        words += [_w(x0 + 14.0, x0 + 60.0, y, "Tiền"), _w(x0 + 62.0, x0 + 110.0, y, "gửi")]
        words.append(_w(now - 40.0, now, y, _figure(1_000_000 + 1_000 * i)))
        left = prior - 40.0 if prior_left is None else prior_left
        words.append(_w(left, prior, y, _figure(2_000_000 + 1_000 * i)))
    return words


def _title(x0, y, text):
    return [_w(x0 + 6.0 * k, x0 + 6.0 * k + 5.0, y, t) for k, t in enumerate(text.split())]


def poster():
    words = _prose(10.0, 225.0, 40.0, 60)
    words += _title(245.0, 40.0, "BẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT")
    words += _table(240.0, 60.0, 40, now=420.0, prior=468.0, prior_left=436.0)
    words += _table(485.0, 40.0, 8, now=660.0, prior=715.0)
    words += _title(490.0, 150.0, "BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH HỢP NHẤT")
    words += _table(485.0, 170.0, 20, now=660.0, prior=715.0)
    return {"text": " ".join(w[4] for w in words), "words": words, "kind": BALANCE_SHEET,
            "from_form": False, "width": WIDTH}


@pytest.fixture()
def parser():
    p = PdfParser()
    p.set_poster_split(True)
    return p


def test_the_poster_becomes_its_panels_and_statements(parser):
    virtual = parser._split_poster(poster())
    kinds = [v["kind"] for v in virtual]
    assert kinds.count(BALANCE_SHEET) == 1 and kinds.count(INCOME_STATEMENT) == 1
    assert kinds.index(BALANCE_SHEET) < kinds.index(INCOME_STATEMENT)
    assert len(virtual) == 4


def test_the_equity_section_above_the_income_statement_joins_the_balance_sheet(parser):
    pages = dict(enumerate(parser._split_poster(poster())))
    parser._fill_continuations(pages)
    kinds = [pages[i]["kind"] for i in sorted(pages)]
    assert kinds == [None, BALANCE_SHEET, BALANCE_SHEET, INCOME_STATEMENT]


def test_a_tables_own_second_column_is_not_a_panel(parser):
    """The prior-period column sits 16 pt clear of the current one: a gutter, and not a panel."""
    virtual = parser._split_poster(poster())
    sheet = next(v for v in virtual if v["kind"] == BALANCE_SHEET)
    assert len(parser.value_columns({0: sheet["words"]}, sheet["width"])) == 2


def test_the_words_move_into_the_panels_own_coordinates(parser):
    virtual = parser._split_poster(poster())
    sheet = next(v for v in virtual if v["kind"] == BALANCE_SHEET)
    assert min(w[0] for w in sheet["words"]) < 30.0 and sheet["width"] < WIDTH / 2


def test_an_ordinary_page_is_not_a_poster(parser):
    words = _title(40.0, 40.0, "BẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT") + _table(40.0, 60.0, 40, now=420.0, prior=520.0)
    page = {"text": "", "words": words, "kind": BALANCE_SHEET, "from_form": False, "width": 595.0}
    assert parser._split_poster(page) is None


def test_the_flag_is_off_by_default_a_widening_and_a_parse_key():
    assert ParseLayer("x", "onnx", 200).poster_split is False
    assert PdfParser().poster_split is False
    assert not ParseLayer("x", "onnx", 200, poster_split=True).is_strict
    assert any(l.poster_split for l in FinancialsBuilder.LAYERS)
    a, b = ParseLayer("x", "onnx", 200), ParseLayer("x", "onnx", 200, poster_split=True)
    assert parse_key(a) != parse_key(b)
    assert ocr_key(a) == ocr_key(b)


def poster_2017():
    """SSB's FY-2017 layout: portrait, the auditor's report ACROSS the page above two statement panels."""
    words = []
    for i in range(20):
        y = 20.0 + 9.0 * i
        words += [_w(10.0 + 71.0 * k, 10.0 + 71.0 * k + 68.0, y, "kiểm") for k in range(8)]
    words += _title(20.0, 220.0, "BẢNG CÂN ĐỐI KẾ TOÁN HỢP NHẤT")
    words += _table(20.0, 240.0, 30, now=230.0, prior=280.0)
    words += _table(300.0, 220.0, 8, now=520.0, prior=575.0)
    words += _title(305.0, 320.0, "BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH HỢP NHẤT")
    words += _table(300.0, 340.0, 15, now=520.0, prior=575.0)
    return {"text": "", "words": words, "kind": None, "from_form": False, "width": 596.0}


def test_a_report_printed_across_the_page_above_the_panels_does_not_hide_them(parser):
    pages = dict(enumerate(parser._split_poster(poster_2017())))
    parser._fill_continuations(pages)
    kinds = [pages[i]["kind"] for i in sorted(pages)]
    assert kinds == [None, None, BALANCE_SHEET, BALANCE_SHEET, INCOME_STATEMENT]
