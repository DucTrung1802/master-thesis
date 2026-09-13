"""`MXP-1` — a scanned page inside a digital statement, its columns shifted back onto the statement's.

Pinned without a PDF or an OCR engine, on VNM Q1-2011's measured balance-sheet geometry: page 2 is
a Letter text page with figure columns at 469.3 / 566.2, pages 3-4 text pages at 456.6 / 553.9, and
page 5 an A4 scan read by OCR at 438.9 / 534.7.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import PdfParser

WIDTH = 612.0
PAGE2, TEXT, SCAN = (469.3, 566.2), (456.6, 553.9), (438.9, 534.7)


def _box(x1, y, text, w=60.0):
    """A word box in the shape `value_columns` reads: (x0, y0, x1, y1, text, …)."""
    return (x1 - w, y, x1, y + 11.0, text, 0, 0, 0)


def _figure(n):
    return "{:,}".format(n).replace(",", ".")


def _page(cols, n):
    words = []
    for i in range(n):
        y = 100.0 + 18.0 * i
        words.append((40.0, y, 120.0, y + 11.0, "Khoản", 0, 0, 0))
        words.append(_box(cols[0], y, _figure(1_000_000 + 1_000 * i)))
        words.append(_box(cols[1], y, _figure(2_000_000 + 1_000 * i)))
    return words


def _statement(scan=SCAN):
    return {2: _page(PAGE2, 22), 3: _page(TEXT, 32), 4: _page(TEXT, 36), 5: _page(scan, 8)}


def _rows(parser, words):
    return parser.table_rows(words, parser.value_columns(words, WIDTH))


@pytest.fixture()
def parser():
    p = PdfParser()
    p.set_align_pages(True)
    return p


def test_without_the_flag_the_scans_prior_column_lands_on_nothing():
    rows = _rows(PdfParser(), _statement())
    assert sum(1 for r in rows if r.values[0] is not None and r.values[1] is None) == 8


def test_with_it_every_line_carries_both_figures(parser):
    rows = _rows(parser, parser._align_pages(_statement(), WIDTH))
    assert len(rows) == 98
    assert all(None not in r.values for r in rows)


def test_the_reference_page_is_untouched_and_the_offset_text_page_moves_too(parser):
    before = _statement()
    after = parser._align_pages(before, WIDTH)
    assert after[4] == before[4]
    assert after[2] != before[2]
    edges = {round(w[2], 1) for w in after[2] + after[5] if w[4][0].isdigit()}
    assert all(min(abs(e - c) for c in TEXT) <= parser.EDGE_TOL / 2 for e in edges)


def test_a_page_with_another_layout_is_left_as_read(parser):
    """A page 118 pt away is a different table, not a shifted one."""
    before = _statement(scan=(338.9, 434.7))
    assert parser._align_pages(before, WIDTH)[5] == before[5]


def test_columns_that_disagree_about_the_shift_are_left_as_read(parser):
    before = _statement(scan=(438.9, 520.0))
    assert parser._align_pages(before, WIDTH)[5] == before[5]


def test_the_flag_is_off_by_default_a_widening_and_a_parse_key():
    assert ParseLayer("x", "onnx", 200).align_pages is False
    assert PdfParser().align_pages is False
    assert not ParseLayer("x", "onnx", 200, align_pages=True).is_strict
    assert any(l.align_pages for l in FinancialsBuilder.LAYERS)
    a, b = ParseLayer("x", "onnx", 200), ParseLayer("x", "onnx", 200, align_pages=True)
    assert parse_key(a) != parse_key(b)
    assert ocr_key(a) == ocr_key(b)
