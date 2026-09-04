"""`ROT-2` — a `/Rotate 90` page hands its NATIVE words back in the UNROTATED space.

`page.rect` accounts for `/Rotate`; `page.get_text("words")` does not. On FPT's Q3-2008
consolidated filing, page 5 — the income statement, a LANDSCAPE table — `page.rect` is
792x612 while the word boxes come back in the 612x792 mediabox: "CÔNG"/"TY"/"CP" stacked
vertically at x=48.4 with DECREASING y, and y running to 756 on a page `scan` records as
612 tall.

`scan` measures columns against `page.rect.width` and `table_rows` groups by y, so the two
disagree about which axis is which: `value_columns` returned four "columns" inside a 55pt band
and the statement came out **6 rows**, refused `only 6 rows parsed` on every layer of the
cascade. The same page read correctly gives **21 rows** and four columns at x = 455 / 552 /
655 / 752, with a 9-month pre-tax profit of 843,409,285,968 — the figure the same filing's
CASH FLOW page prints as its own line 01.

⚠️ **THIS IS `ROT-1`'s DEFECT IN THE ONE PATH `ROT-1` CANNOT REACH.** That one re-RENDERS a
turned SCAN at a different rotation; re-rendering a text layer returns the same unrotated
boxes, so no probe, engine or DPI helps. The fix is the mapping `_ocr_page`'s own docstring
already promises — "words in VISUAL pdf-point space" — which the Tesseract path has always
applied through `_to_visual` and the native path never did.

⚠️ **INERT ON AN UPRIGHT PAGE BY CONSTRUCTION**: `_to_visual` returns the list unchanged when
`page.rotation` is 0. Measured across the 73,780 pages of the eight parsed tickers, **2,116**
are read through the native path at all and **122** of those carry a non-zero `/Rotate` —
0.17 %, and every one is being measured in the wrong space today.
"""
import fitz
import pytest

from web_scraper.cafef_pdf_parser import PdfParser

LINE = "TONG CONG TAI SAN 6.200.411.917.821"


def _page(rotate: int):
    """A one-page PDF shaped like the real filing: the line is drawn TURNED inside the
    mediabox, and `/Rotate` is what displays it upright.

    ⚠️ **THE ORDER MATTERS, AND GETTING IT WRONG MAKES THE FIXTURE TEST THE OPPOSITE THING.**
    Drawing the line horizontally and rotating the PAGE afterwards gives a document whose text
    is VERTICAL when displayed — the mirror image of FPT's Q3-2008 page 5, where the content is
    landscape and the rotation is what makes it readable. Written that way every assertion
    below passes with its sign flipped, which is how this fixture was first written and how the
    measurement caught it. Counter-rotating the INSERT is what puts the content in the space
    the real filing keeps it in.
    """
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((240, 400), LINE, fontsize=11, rotate=(360 - rotate) % 360)
    page.set_rotation(rotate)
    # Reopened from bytes, so this is a page a reader OPENS rather than one just built.
    reopened = fitz.open(stream=doc.tobytes(), filetype="pdf")
    doc.close()
    return reopened, reopened.load_page(0)


def _parser():
    p = PdfParser.__new__(PdfParser)          # no OCR engine, no models
    p.ocr_ready = False                       # forces the native-text path
    p.join_split_digits = False
    p.join_lost_separator = False
    p._ocr_cache = {}
    p._page_rot = {}
    return p


def _words(page):
    return _parser()._read_page(page, page.get_text())[1]


def test_an_upright_page_is_untouched():
    """`_to_visual` short-circuits on `page.rotation == 0`, so 99.8 % of the corpus is inert."""
    doc, page = _page(0)
    assert _words(page) == page.get_text("words")
    doc.close()


@pytest.mark.parametrize("rotate", [90, 180, 270])
def test_a_rotated_page_comes_back_inside_its_own_rect(rotate):
    """The property that matters downstream: every box lies inside `page.rect`, which is what
    `scan` records as the page width and what every column measurement is taken against."""
    doc, page = _page(rotate)
    words = _words(page)
    assert words, "the fixture must produce word boxes"
    for w in words:
        assert -1 <= w[0] <= page.rect.width + 1 and -1 <= w[2] <= page.rect.width + 1
        assert -1 <= w[1] <= page.rect.height + 1 and -1 <= w[3] <= page.rect.height + 1
    doc.close()


def test_the_raw_boxes_really_are_in_the_other_space():
    """⚠️ THE DEFECT ITSELF, ASSERTED RATHER THAN DESCRIBED — otherwise the test above could
    pass on a page where the two spaces happen to coincide.

    A text LINE is far wider than it is tall. RAW, every box on this page is TALLER than wide,
    because they are in the mediabox space the content was drawn in; MAPPED, every one is wider
    than tall. That is the difference `table_rows` groups on and `value_columns` clusters on,
    and it is why the real page came out 6 rows instead of 21.
    """
    doc, page = _page(90)
    raw = page.get_text("words")
    assert raw, "the fixture must produce word boxes"
    assert all((w[3] - w[1]) > (w[2] - w[0]) for w in raw)
    assert all((w[2] - w[0]) > (w[3] - w[1]) for w in _words(page))
    doc.close()


def test_the_text_itself_is_unchanged():
    """Only the geometry moves — the words and their order are the page's own."""
    doc, page = _page(90)
    assert [w[4] for w in _words(page)] == [w[4] for w in page.get_text("words")]
    doc.close()


def test_the_mapping_is_to_visual_and_not_a_second_copy_of_it():
    """One rotation rule in this file, the one the Tesseract path has always used."""
    doc, page = _page(270)
    p = _parser()
    assert _words(page) == p._to_visual(page, page.get_text("words"))
    doc.close()
