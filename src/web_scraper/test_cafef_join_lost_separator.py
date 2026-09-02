"""A thousands separator the recogniser read as a SPACE — pinned without a PDF or an engine.

The defect is the mirror image of `test_cafef_split_figures.py`'s. There the DETECTOR emits one
printed figure as two boxes; here it emits ONE box whose separator came back as a space, and
`_split_number_runs` — built for the opposite case, a box holding two period figures — cuts it
into pieces. The pieces land on no column, `split_figures` counts them, and `reconcile` refuses
the whole statement (`SPL-1`). BSR's Q3-2018 balance sheet returns `'9.964.924.167 838'` for a
printed 9.964.924.167.838, and four of its filings carry 76 such runs between them.

⚠️ `join_digits` ALREADY COVERS ONE SHAPE OF THIS AND ONLY ONE: a bare 1-3 digit HEAD,
`'3 396.864'` (ACB Q2-2012). BSR loses separators anywhere in the number, so the head is a
complete grouped figure and `JOIN_HEAD_RE` cannot match it.

⚠️ THE GROUND TRUTH IS THE SAME DOCUMENT AT A HIGHER DPI. At onnx@300 and onnx@400 the detector
returns `'9.964.924.167.838'` WHOLE, over the identical x-range [347.8, 423.4] — which is what
settles that the box is one figure and not two. Measured 2026-08-31: 69 of the 76 runs join
into one well-formed grouped figure.

⚠️ AND IT CANNOT BE TOLD FROM THE OPPOSITE CASE BY ANYTHING INSIDE THE BOX — MEASURED, NOT
ASSUMED. ACB's Q1-2025 cash flow really does box two period figures together
(`'135.272.610 126.501.216'`), and that joins well-formed too. A character-density test was
tried first, on the theory that a lost separator costs one character where an inter-column gap
costs none: ACB's genuine pair came back at 1.03x its own page's median pt/char — identical to
a clean single box. The hypothesis is disproven and is recorded here so it is not re-made.

⚠️ SO THE SEPARATION IS CASCADE POSITION, AND THAT IS WHAT THESE TESTS PIN. ACB Q1-2025's cash
flow is accepted at `onnx@300+relax`, layer 6 of 55; the `+joinlost` layers are the last five,
so only a statement every other reading has already refused can reach them.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import PdfParser


def _one(text, x0=347.8, x1=423.4, y=238.7):
    """PyMuPDF's word shape: (x0, y0, x1, y1, text, block, line, n)."""
    return (x0, y, x1, y + 10.0, text, 0, 0, 0)


def _texts(words):
    return [w[4] for w in words]


# ── the BSR shapes, joined ────────────────────────────────────────────────────
# Every one of these is a real run read off a BSR filing on 2026-08-31.
@pytest.mark.parametrize("raw,joined", [
    ("9.964.924.167 838", "9.964.924.167.838"),      # separator lost near the END
    ("382.080.998 409", "382.080.998.409"),
    ("28.030.253.520 487", "28.030.253.520.487"),
    ("10 982 779.849.642", "10.982.779.849.642"),    # TWO separators lost, three parts
    ("46.625 723 403.018", "46.625.723.403.018"),
    ("10.294 732.376.959", "10.294.732.376.959"),    # tail carries its own separators
    ("329 110.639.451", "329.110.639.451"),          # the `join_digits` shape, also covered
    ("73.686.050 815 612", "73.686.050.815.612"),
])
def test_a_lost_separator_is_rejoined_under_the_flag(raw, joined):
    assert _texts(PdfParser._split_number_runs([_one(raw)], False, True)) == [joined]


def test_the_joined_box_keeps_its_own_geometry():
    """The box is not re-measured — only its text was wrong. Its right edge is what the column
    clustering reads, and it was already correct."""
    out = PdfParser._split_number_runs([_one("9.964.924.167 838")], False, True)
    assert len(out) == 1
    assert (out[0][0], out[0][2]) == (347.8, 423.4)


def test_a_malformed_run_is_still_split():
    """7 of the 76 measured runs do not join into a well-formed figure — OCR damage rather than
    a lost separator. They are left to the splitter exactly as before, and the statement then
    still fails `split_figures`, which is the correct answer."""
    for raw in ("25 1961177.684.364", "697 188.266,449", "31.001 0961100.000"):
        assert len(PdfParser._split_number_runs([_one(raw)], False, True)) > 1, raw


def test_a_negative_figure_keeps_its_parentheses():
    """`parse_num` carries the sign from the brackets, so a join that dropped them would turn an
    expense into income — `QUO-1`/`PAR-1`'s failure mode, reached a different way."""
    out = PdfParser._split_number_runs([_one("(6 190.773.155)")], False, True)
    assert _texts(out) == ["(6.190.773.155)"]
    assert PdfParser.parse_num(out[0][4]) == -6190773155


# ── the negative control: the case the splitter exists for ────────────────────
def test_the_default_path_is_untouched():
    """⚠️ THE WHOLE SAFETY ARGUMENT. Off, `_split_number_runs` does exactly what it did — so no
    filing that parses today can move, whatever this flag would have done to it."""
    for raw in ("9.964.924.167 838", "135.272.610 126.501.216", "10 982 779.849.642"):
        assert (PdfParser._split_number_runs([_one(raw)])
                == PdfParser._split_number_runs([_one(raw)], False, False)), raw
    assert _texts(PdfParser._split_number_runs([_one("135.272.610 126.501.216")])) == \
        ["135.272.610", "126.501.216"]


def test_two_period_figures_ARE_wrongly_joined_under_the_flag():
    """⚠️ THIS IS THE FLAG'S KNOWN COST, RECORDED RATHER THAN HIDDEN. ACB Q1-2025's cash flow
    boxes two real period figures together and this joins them into one absurd number. Nothing
    inside the box distinguishes it from BSR's lost separator — the density test that was
    supposed to came back at 1.03x, i.e. no signal at all. What protects ACB is that its cash
    flow is accepted at layer 6; see the two tests below."""
    assert _texts(PdfParser._split_number_runs(
        [_one("135.272.610 126.501.216")], False, True)) == ["135.272.610.126.501.216"]


def test_the_joinlost_layers_run_after_every_strict_layer():
    """The position IS the guard, so it is asserted rather than assumed.

    ⚠️ This asserted `flagged == the last len(flagged) indices` until 2026-09-02 -- that
    the +joinlost block is literally the tail of the list. That is the SAME position
    assertion `test_the_span_layers_run_late_and_relaxed` and
    `test_the_condensed_layer_runs_last` have each already outgrown, and this is its THIRD
    instance: appending the `+merged` block (`column_header_blind` / `merged_tail`) broke it
    while changing nothing about when a lost separator may be rejoined. The property being
    guarded is that no layer reading the box AS PRINTED runs afterwards -- another WIDENING
    layer may.

    ⚠️ AND THE COST OF THAT IS REAL AND IS RECORDED HERE: a statement `+joinlost` ACCEPTS
    never reaches the `+merged` block, so a filing needing both a rejoined figure and a
    merged label is out of reach. No such filing has been measured.
    """
    layers = FinancialsBuilder.LAYERS
    flagged = [i for i, l in enumerate(layers) if l.join_lost_separator]
    assert flagged, "no +joinlost layer in the cascade"
    assert flagged == list(range(flagged[0], flagged[-1] + 1)), "the block must be contiguous"
    # ⚠️ `ParseLayer.is_strict`, not a fifth private copy of the flag list — see its
    # docstring. Four files kept their own and each had to be edited whenever a widening
    # block was added; the one that was forgotten would have counted the NEW layers as
    # strict and moved `max(strict)` past the block it exists to bound.
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    assert flagged[0] > max(strict), \
        "a +joinlost layer must never run before a layer that reads the box as printed"


def test_a_joinlost_layer_reads_no_extra_pixels():
    """It is a per-LAYER post-step on words the page cache already holds, so it changes
    `parse_key` and must NOT change `ocr_key` — otherwise these five layers would each cost a
    fresh OCR pass over every page of a filing that has already defeated fifty."""
    a = FinancialsBuilder.LAYERS[0]
    b = next(l for l in FinancialsBuilder.LAYERS
             if l.join_lost_separator and l.dpi == a.dpi and l.crop_pad == a.crop_pad
             and not l.relax_totals)
    assert ocr_key(a) == ocr_key(b)
    assert parse_key(a) != parse_key(b)


def test_the_flag_is_off_by_default_everywhere():
    from web_scraper.cafef_financials import ParseLayer
    assert ParseLayer("x", "onnx", 200).join_lost_separator is False
    assert PdfParser.__new__(PdfParser).__class__ is PdfParser   # no import side effects
