"""`parse_num` — a figure printed with DECIMALS, in either convention.

The defect: `parse_num` stripped every `.` and `,` and read what was left as an integer. That
is right for `165.878.786` and 100x wrong for `1,630,428.99`, which is how TCB prints its 2012
filings — comma thousands and a dot decimal, the international convention. Q2-2012's income
statement went to disk with a **profit before tax of 163,042,899 million** — 163 tn for a bank
with 180 tn of assets — and nothing caught it: the error is uniform, so the statement
reconciles perfectly against itself, and `sane` had no band when it was written.

⚠️ **AND IT POISONED THE BAND FOR EVERY LATER QUARTER.** `sane` compares a candidate against
the MEDIAN of the quarters already accepted; with one 100x row among seven, TCB Q3-2013's own
income statement — read correctly at 97,315 mn — was refused as a magnitude outlier
(`magnitude 9.73e+10 vs typical 2.13e+12`). One wrong figure on disk cost a later quarter.

⚠️ **WHICH CHARACTER IS THE DECIMAL POINT CANNOT BE READ OFF THE CHARACTER**, because OCR
confuses `.` and `,` constantly. The test is the LENGTH of the last group: a thousands group is
always three digits, so a separator followed by one or two digits at the end of the token is a
decimal point and nothing else can be.
"""
import pytest

from web_scraper.cafef_pdf_parser import PdfParser


@pytest.mark.parametrize("text,value", [
    # TCB Q2-2012, verbatim from the filing: comma thousands, dot decimal.
    ("9,729,852.10", 9_729_852),
    ("1,630,428.99", 1_630_429),            # rounded — see below
    ("(11,226.88)", -11_227),
    # the Vietnamese convention, decimals and all
    ("1.234,56", 1_235),
    # …and the ordinary case, which must not move
    ("165.878.786", 165_878_786),
    ("2.989.205", 2_989_205),
    ("(43.157)", -43_157),
    ("1.000.000", 1_000_000),
    ("-1.234", -1_234),
    ("12", 12),
    ("0", 0),
    ("-", 0),
])
def test_a_figure_is_read_in_whichever_convention_it_was_printed(text, value):
    assert PdfParser.parse_num(text) == value


def test_a_three_digit_tail_is_a_thousands_group_however_the_separators_are_mixed():
    """⚠️ THE CASE THAT FORBIDS READING THE SEPARATOR CHARACTER. "1,234.567" is an ordinary
    Vietnamese figure with one separator misread by OCR, and it must stay 1,234,567 — three
    digits after the last separator, so that separator is a thousands group."""
    assert PdfParser.parse_num("1,234.567") == 1_234_567


def test_the_fraction_is_rounded_away_and_that_is_deliberate():
    """Every figure here is an integer of the statement's own unit (`Statement.unit` scales the
    row afterwards), so hundredths of a million are a precision this scale does not carry. On
    the measured case the rounding is 990,000 VND on 1.63 tn."""
    assert PdfParser.parse_num("1,630,428.99") == 1_630_429
    assert PdfParser.parse_num("1,630,428.01") == 1_630_428


def test_a_token_that_is_not_a_figure_is_still_refused():
    for text in ("abc", "", "V.13", "..", "1.2.3.a"):
        assert PdfParser.parse_num(text) is None
