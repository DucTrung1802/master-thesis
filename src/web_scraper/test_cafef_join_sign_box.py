"""`SGB-1` — the MINUS SIGN as a box of its own, blocking the join that recovers the figure.

Pinned without a PDF or an engine: the defect is entirely inside `_join_split_number`, and the
boxes below are real reads off HOSE_HPG's Q1-2014 consolidated cash flow at `onnx@200`.

⚠️ **THE SHAPE.** HPG's self-filed Q1/Q3 scans (2011-2015) lose every thousands separator on
the cash-flow page: the detector returns ONE box reading `'1 070 843 098 197'` for a printed
1.070.843.098.197. `join_digits` was built for exactly that (`JOIN_HEAD_RE`, a bare 1-3 digit
head) and joins it. But where the figure is NEGATIVE the sign comes back as its own part —
`'- 102 574 263 389'` — and `JOIN_HEAD_RE` needs a digit, so the whole run falls through to
`_split_number_runs`, which cuts it into five pieces that land on no column. `split_figures`
counts them and `reconcile` refuses the statement.

⚠️ **MEASURED, NOT ARGUED.** HOSE_HPG Q1-2014 cash flow at `onnx@200+join+components`:
**26 fragments, 24 of them on the EIGHT lines whose sign is its own box** — interest paid, tax
paid, cash out for investments, dividends paid, repayments. The same eight lines carry the same
standalone sign at 300 and 400 dpi, so raising the resolution is not a remedy. The ninth line
is unrelated OCR damage (`'4 11 500 046 151'`, a lost digit) and is left refused, which is the
correct answer.

⚠️ **A BARE `-` MEANS NIL IN A VAS STATEMENT, AND THAT CASE CANNOT REACH HERE.** A nil marker
and a figure sit in DIFFERENT period columns, tens of points apart; this only ever re-reads
what the DETECTOR placed inside ONE box. `NUM_RUN_RE` already requires a digit somewhere in the
box, so a row of "-" placeholders is left alone exactly as before.

⚠️ **IT IS SCOPED TO `join_digits`, WHICH IS A LAYER FLAG.** Layers 15-16 of the cascade are
the only ones that call this, so nothing accepted at layers 1-14 can move.
"""
import pytest

from web_scraper.cafef_pdf_parser import PdfParser


def _one(text, x0=486.0, x1=552.0, y=370.4):
    """PyMuPDF's word shape: (x0, y0, x1, y1, text, block, line, n)."""
    return (x0, y, x1, y + 10.0, text, 0, 0, 0)


def _texts(words):
    return [w[4] for w in words]


# ── the eight HPG Q1-2014 lines, joined ───────────────────────────────────────
@pytest.mark.parametrize("raw,joined", [
    ("- 102 574 263 389", "-102.574.263.389"),   # 13  tiền lãi vay đã trả
    ("- 64 265 003 857", "-64.265.003.857"),     # 14  thuế TNDN đã nộp
    ("- 980 583 115 840", "-980.583.115.840"),   # 21
    ("- 7 389 599 995", "-7.389.599.995"),       # 25  chi đầu tư góp vốn
    ("- 822 578 350 224", "-822.578.350.224"),   # 30
    ("- 44 238 550", "-44.238.550"),             # 36  cổ tức đã trả
    ("- 378 716 396 831", "-378.716.396.831"),   # 40
    ("- 298 178 671 828", "-298.178.671.828"),   # 50
])
def test_a_sign_of_its_own_no_longer_blocks_the_join(raw, joined):
    assert PdfParser._join_split_number(raw) == joined


def test_the_positive_shapes_join_exactly_as_before():
    """The flag's original case and the whole-figure case it already handled — unchanged."""
    assert PdfParser._join_split_number("3 396.864") == "3.396.864"
    assert PdfParser._join_split_number("329 110.639.451") == "329.110.639.451"
    assert PdfParser._join_split_number("1 070 843 098 197") == "1.070.843.098.197"


def test_the_joined_box_keeps_its_own_geometry():
    """Only the TEXT was wrong. The right edge is what the column clustering reads, and the
    detector already had it right."""
    out = PdfParser._split_number_runs([_one("- 102 574 263 389")], True, False)
    assert len(out) == 1
    assert (out[0][0], out[0][2]) == (486.0, 552.0)
    assert out[0][4] == "-102.574.263.389"
    assert PdfParser.parse_num(out[0][4]) == -102_574_263_389


def test_a_sign_with_only_one_group_after_it_is_left_alone():
    """⚠️ TWO PARTS AFTER THE SIGN, MINIMUM — the same bound the flag always had. One group is
    not a lost separator, it is a figure the box already holds whole."""
    assert PdfParser._join_split_number("- 389") is None


def test_a_row_of_placeholders_is_untouched():
    """The nil marker, which is what a bare `-` means in a VAS statement."""
    assert PdfParser._join_split_number("- - -") is None
    assert PdfParser._join_split_number("-") is None


def test_the_ninth_line_stays_refused():
    """⚠️ THE ONE HPG Q1-2014 LINE THIS DOES NOT FIX, asserted so the measurement above is not
    read as a claim to have cleared the page. `'4 11 500 046 151'` lost a DIGIT, not a
    separator: `'11'` is not a three-digit group, so no grouping of these parts is well formed
    and the splitter keeps them apart — `split_figures` still counts them and the statement is
    still refused, which is the correct answer for a reading that is wrong."""
    assert PdfParser._join_split_number("4 11 500 046 151") is None


def test_two_period_figures_in_one_box_are_still_not_joined():
    """⚠️ THE CASE THE HEAD RULE EXISTS TO EXCLUDE, and the sign must not open a door to it.
    ACB Q1-2025 genuinely boxes two period figures together; both are complete grouped figures,
    so neither can be a lost-separator head."""
    assert PdfParser._join_split_number("135.272.610 126.501.216") is None
    assert PdfParser._join_split_number("- 135.272.610 126.501.216") is None


def test_a_bracketed_figure_is_left_to_the_bracket_rule():
    """`_split_number_runs` handles a box the parentheses SPAN before it reaches the join, and
    that path is unchanged — the sign rule must not claim it first."""
    assert PdfParser._join_split_number("(1.029 827)") is None
    out = PdfParser._split_number_runs([_one("(1.029 827)")], True, False)
    assert _texts(out) == ["(1.029.827)"]
