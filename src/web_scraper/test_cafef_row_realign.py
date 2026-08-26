"""`realign_rows` — the row-slide recovery, pinned without a PDF, a network or an OCR engine.

The defect: on some scans the detector puts every numeric box a constant distance ABOVE the
text box of the same printed line. Past `Y_TOL` the two never group, `table_rows` hands each
figure to the label line above it via `carry`, and the statement slides by one row with every
digit read correctly — BID's Q1-2021 balance sheet reported its own cash line, 10,770,158, as
total assets against the 1,558,887,407 printed on page 2.

Two invariants matter and both are asserted here:
  * a page that is ALREADY paired correctly must be left exactly alone (offset 0.0), because
    this layer re-reads pages that parse today;
  * a page carrying a constant offset must have it MEASURED, not guessed.
"""
import pytest

from web_scraper.cafef_pdf_parser import PdfParser


LO = 400.0                      # labels end left of this, figures right of it


def _word(x1, y0, text, h=12.0):
    """A word box in the shape `table_rows` reads: (x0, y0, x1, y1, text, …)."""
    return (x1 - 60.0, y0, x1, y0 + h, text, 0, 0, 0)


def _page(rows, value_offset):
    """One statement page: `rows` is [(label, figure)], laid out 16pt apart.

    A `figure` of None is a SECTION HEADING — a line the filing prints with no figure of its
    own, like "A. TÀI SẢN". It is what makes this defect bite rather than being caught by the
    existing recovery in `table_rows`: that one only fires when the figure line has no label
    available at all, and a heading leaves one waiting in `carry`. Without a heading in the
    fixture the slid page parses correctly and the test proves nothing.

    `value_offset` is how far ABOVE its own label each figure is printed — 0.0 for a page the
    parser already reads correctly.
    """
    words = []
    for i, (label, figure) in enumerate(rows):
        y = 100.0 + i * 16.0
        words.append(_word(250.0, y, label))
        if figure is not None:
            words.append(_word(430.0, y - value_offset, figure))
    return {0: words}


ROWS = [("A. TÀI SẢN", None),
        ("Tiền mặt, vàng bạc, đá quý", "10.770.158"),
        ("Tiền gửi tại NHNN", "91.672.598"),
        ("Tiền, vàng gửi tại và cho vay TCTD khác", "85.087.146"),
        ("Chứng khoán kinh doanh", "3.574.042"),
        ("Cho vay khách hàng", "1.214.295.916"),
        ("Chứng khoán đầu tư", "147.652.939"),
        ("Tài sản có khác", "10.736.555"),
        ("TỔNG TÀI SẢN", "1.558.887.407")]


@pytest.fixture(scope="module")
def parser():
    return PdfParser()


def test_aligned_page_is_left_alone(parser):
    """The gain floor refuses a shift on a page whose figures already sit with their labels.

    Measured on the real corpus: ACB Q1-2021 scores x1.09 and BID Q2-2021 x1.03, and both parse
    correctly today, so anything at that level must return 0.0.
    """
    assert parser._value_row_offset(_page(ROWS, 0.0), LO) == 0.0


@pytest.mark.parametrize("offset", [6.0, 7.0, 8.0])
def test_constant_offset_is_measured(parser, offset):
    """A page with a real slide has its offset recovered exactly.

    `Y_TOL` is a tolerance, so a band of shifts all score the maximum; the estimator returns the
    CENTRE of that band, which is the offset itself. Taking the first point of the band instead
    returned 3.0 for a true 7.0 and left every figure on the edge of the tolerance.
    """
    assert parser._value_row_offset(_page(ROWS, offset), LO) == pytest.approx(offset, abs=0.6)


def test_the_slide_is_real_without_the_flag(parser):
    """The fixture must actually reproduce the defect, or the rest of this file proves nothing.

    With the heading present and the flag off, the grand total lands on the line ABOVE the one
    that prints it — which is how BID's Q1-2021 balance sheet reported total assets of
    10,770,158, its own cash line.
    """
    parser.set_realign_rows(False)
    rows = {r.key: r.values for r in parser.table_rows(_page(ROWS, 7.0), [430.0])}
    # the heading keeps its "A." as the row NUMBER, so its key is the bare `tai_san`
    assert rows["tai_san"] == [10770158], "the heading should have stolen the first figure"
    # …and the grand total ends up on the line above the one that prints it
    assert rows["tai_san_co_khac"] == [1558887407]
    assert "tong_tai_san" not in rows


def test_offset_is_never_applied_unless_asked(parser):
    """`realign_rows` is off by default, so every statement that parses today is untouched.

    Proven against HEAD on the real corpus as well — 12 statements / 643 rows of ACB, VCB and
    BID filings reproduce row for row with the flag off.
    """
    assert PdfParser().realign_rows is False
    slid = _page(ROWS, 7.0)
    columns = [430.0]
    parser.set_realign_rows(False)
    before = [(r.key, r.values) for r in parser.table_rows(slid, columns)]
    parser.set_realign_rows(True)
    after = [(r.key, r.values) for r in parser.table_rows(slid, columns)]
    parser.set_realign_rows(False)
    assert before != after, "the flag must actually change a slid page"
    # …and with the flag on, the grand total belongs to the line that prints it
    assert ("tong_tai_san", [1558887407]) in after
    assert ("tong_tai_san", [1558887407]) not in before
    # every other line moves with it — this is a re-pairing, not a patched total
    assert ("tien_gui_tai_nhnn", [91672598]) in after
    assert ("tien_gui_tai_nhnn", [85087146]) in before


def test_no_figure_is_invented_or_lost(parser):
    """Re-pairing moves figures between labels; it must never add or drop one."""
    slid = _page(ROWS, 7.0)
    columns = [430.0]
    parser.set_realign_rows(False)
    before = sorted(v for r in parser.table_rows(slid, columns) for v in r.values if v is not None)
    parser.set_realign_rows(True)
    after = sorted(v for r in parser.table_rows(slid, columns) for v in r.values if v is not None)
    parser.set_realign_rows(False)
    assert before == after
