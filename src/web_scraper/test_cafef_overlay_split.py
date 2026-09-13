"""`OVL-1` — a box inside another box that repeats its tail is one figure printed twice, not a split.

Measured on SSB's FY-2009 annual poster: `18.239.254` spans 315.3-357.9 pt and `254` sits inside it.
"""
from web_scraper.cafef_pdf_parser import PdfParser

WIDTH = 376.5


def _w(x0, x1, y, text):
    return (x0, y, x1, y + 7.0, text, 0, 0, 0)


def test_an_overlaid_duplicate_is_not_a_split():
    words = [_w(20.0, 90.0, 100.0, "TỔNG"), _w(252.0, 295.8, 100.0, "25.115.555"),
             _w(315.3, 357.9, 100.0, "18.239.254"), _w(343.6, 357.9, 100.0, "254")]
    assert PdfParser().split_figures({0: words}, WIDTH) == 0


def test_a_real_split_is_still_counted():
    words = [_w(20.0, 90.0, 100.0, "TỔNG"), _w(300.0, 335.0, 100.0, "5.209.108"),
             _w(338.8, 360.0, 100.0, "954.978")]
    assert PdfParser().split_figures({0: words}, WIDTH) == 1


def test_a_box_inside_another_with_other_digits_is_still_counted():
    """Containment alone is not the test: the inner box must repeat the outer box's tail."""
    words = [_w(20.0, 90.0, 100.0, "TỔNG"), _w(315.3, 357.9, 100.0, "18.239.254"), _w(343.6, 357.9, 100.0, "731")]
    assert PdfParser().split_figures({0: words}, WIDTH) == 1
