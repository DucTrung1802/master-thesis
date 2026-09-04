"""A landscape statement scanned into a portrait page — and the two defects behind it.

BID's Q3-2011 consolidated income statement is page 6 of 32 and its image is turned 90°. The
page's own `/Rotate` is 0, like every other page of that scan, so nothing in the PDF says so;
every one of the 49 cascade layers reported `no such statement on any page of this filing`,
and the quarter had read `missing` in `is_HOSE_BID.csv` since the ticker was first parsed.

Three things had to change, and each is pinned below.

1. **`_page_rotation`** — the DETECTOR already knows. A text LINE is far wider than it is tall
   and a turned one is far taller than it is wide. Measured over all 32 pages of that filing
   (2026-08-30), the share of boxes taller than wide is **0-19 % upright and 92-100 % rotated**,
   with nothing between — so the cut has room on both sides. ⚠️ It is read off the boxes the
   upright pass ALREADY returned, so an upright page pays nothing at all.
   ⚠️ **The direction is per PAGE, not per document**: in that same filing the income statement
   needs 90 and all eleven rotated NOTES pages need 270 (measured, which is the only reason
   the probe reads the page both ways instead of deciding once and reusing it).

2. **`_enforce_order`** — BID prints its balance sheet, then its CASH FLOW, then its income
   statement. The canonical order is a convention, not a rule, and enforcing it deleted the
   cash flow outright the moment the income statement on page 6 became visible; then
   `_fill_continuations` handed its two pages to the balance sheet running above them, so 55
   balance-sheet rows became 55 balance-sheet-plus-cash-flow rows. The guard now drops the
   out-of-order pages only when the statement SURVIVES somewhere else, which is the duplicate
   it was written for.

3. **`parse_num`** — the opening bracket of a negative figure comes back as a quote mark. That
   page yields `"9,797,589,605,016)` and `"299,126,415,190)`, closing bracket intact. Both were
   refused, column 0 was left empty, and `_first_value` took the **prior-period** column
   instead: interest expense read 5,417,947,722,487 where the filing prints 9,797,589,605,016.
   It RECONCILED — an income statement is only anchored on PBT, and that cell was sound.
   `SLD-1`'s shape again: a wrong figure every gate passes.

Verified on the filing itself: with all three, the quarter accepts at **layer 1 of 49 in 29 s**
(the cascade had cost 2 m 30 s and produced nothing), and the recovered statement closes on
five of its own printed subtotals — II = 3+4, VI = 5+6, IX = I..VIII, XI = IX+X, XIII = XI+XII
— exactly, with I = 1+2 out by 1 dong, which is the filing's own rounding.
"""
import pytest

from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser)


# ──────────────────────────────────────────────────────────────────────────────
# 1. the orientation probe
# ──────────────────────────────────────────────────────────────────────────────


class _Page:
    """Only what `_page_rotation` touches. `set_rotation` records, so a test can prove the
    probe leaves the page as it found it."""

    def __init__(self, number=5, rotation=0):
        self.number = number
        self.rotation = rotation
        self.seen = []

    def set_rotation(self, r):
        self.rotation = r
        self.seen.append(r)


class _Engine:
    """Reads whatever the test says each rotation yields."""

    def __init__(self, by_rotation):
        self.by_rotation = by_rotation
        self.dpi = 200
        self.calls = []

    def read_page(self, page):
        self.calls.append((page.rotation, self.dpi))
        return "", self.by_rotation.get(page.rotation, [])


def _word(x0, y0, x1, y1, text="x"):
    return (x0, y0, x1, y1, text, 0, 0, 0)


def _lines(n, wide=True, text="mot dong chu"):
    """`n` word boxes, laid out as ordinary text lines or as turned ones.

    ⚠️ **THE TEXT IS NOT DECORATION SINCE `ROT-3` (2026-09-04).** `_page_rotation` has a second
    entry signal — *the upright read is nearly empty* — measured in CHARACTERS, so a fixture
    whose boxes carry one letter each is a fixture of a page that reads as unreadable. The
    default here is twelve characters a box, which puts any page of a dozen boxes or more well
    over `MIN_UPRIGHT_CHARS`; pass a shorter one to build the `ROT-3` page deliberately.
    """
    return [_word(10.0, 10.0 * i, 90.0, 10.0 * i + 8.0, text) if wide
            else _word(10.0 * i, 10.0, 10.0 * i + 8.0, 90.0, text) for i in range(n)]


def _parser(engine="onnx", onnx=None):
    p = PdfParser.__new__(PdfParser)
    p.engine = engine
    p.dpi = 200
    p._onnx = onnx
    p._logger = None
    return p


def test_an_upright_page_is_left_alone_and_costs_nothing():
    """The common case: wide boxes, no probe, the engine is never called."""
    engine = _Engine({})
    page = _Page()
    assert _parser(onnx=engine)._page_rotation(page, _lines(60, wide=True)) == 0
    assert engine.calls == []
    assert page.seen == []


def test_a_page_of_vertical_lines_is_turned_the_way_the_digits_read():
    """BID Q3-2011 page 6: 90 yields 100 parseable numbers against 7 at 270."""
    engine = _Engine({90: [_word(0, 0, 9, 9, "12,879,817,911,898")] * 30,
                      270: [_word(0, 0, 9, 9, "lIl")] * 30})
    page = _Page(rotation=0)
    assert _parser(onnx=engine)._page_rotation(page, _lines(60, wide=False)) == 90
    # ⚠️ THE BASE IS READ TOO, AND IT WAS NOT BEFORE `ROT-3`. The old loop seeded `best_key`
    # with None, so the first candidate won whether or not turning helped — see
    # `test_a_rotation_that_reads_worse_than_upright_is_refused` for what that cost.
    assert [r for r, _ in engine.calls] == [0, 90, 270]      # the base, then both directions
    assert page.rotation == 0                               # and the page is put back


def test_the_other_direction_wins_when_the_digits_are_there_instead():
    """⚠️ The eleven rotated NOTES pages of that same filing need 270 — one document, two
    directions, which is why this cannot be decided once and reused."""
    engine = _Engine({90: [_word(0, 0, 9, 9, "lIl")] * 30,
                      270: [_word(0, 0, 9, 9, "1.234.567")] * 30})
    assert _parser(onnx=engine)._page_rotation(_Page(), _lines(60, wide=False)) == 270


def test_the_probe_renders_small_and_restores_the_dpi():
    """It is counting numbers, not reading them."""
    engine = _Engine({90: [], 270: []})
    p = _parser(onnx=engine)
    p._page_rotation(_Page(), _lines(60, wide=False))
    assert {dpi for _, dpi in engine.calls} == {PdfParser.ROT_PROBE_DPI}
    assert p.dpi == 200 and engine.dpi == 200


def test_a_page_already_turned_by_its_own_rotate_is_turned_on_top_of_that():
    """`/Rotate 180` scans exist; the probe reports an ABSOLUTE rotation, not a delta."""
    engine = _Engine({270: [_word(0, 0, 9, 9, "1.234")] * 30, 90: []})
    assert _parser(onnx=engine)._page_rotation(_Page(rotation=180),
                                               _lines(60, wide=False)) == 270


def test_a_handful_of_stray_marks_never_turns_a_page():
    """Below `MIN_ROT_WORDS` there is nothing to measure a share against."""
    engine = _Engine({90: [], 270: []})
    n = PdfParser.MIN_ROT_WORDS - 1
    assert _parser(onnx=engine)._page_rotation(_Page(), _lines(n, wide=False)) == 0
    assert engine.calls == []


def test_a_mixed_page_stays_upright():
    """Half and half is under the cut, and the cut is nowhere near either measured
    population."""
    words = _lines(30, wide=False) + _lines(30, wide=True)
    engine = _Engine({90: [], 270: []})
    assert _parser(onnx=engine)._page_rotation(_Page(), words) == 0
    assert engine.calls == []


def test_only_the_onnx_path_is_probed():
    """Tesseract's boxes come back through `_to_visual` and the native-text path has none of
    its own; neither has ever met this defect."""
    engine = _Engine({90: [_word(0, 0, 9, 9, "1.234")] * 30})
    assert _parser(engine="tesseract", onnx=engine)._page_rotation(
        _Page(), _lines(60, wide=False)) == 0
    assert engine.calls == []


# ──────────────────────────────────────────────────────────────────────────────
# 1b. `ROT-3` — the page the first signal cannot see, and the seed that makes it safe
# ──────────────────────────────────────────────────────────────────────────────
#
# FPT's Q1-2012, Q3-2011 and Q3-2013 CONSOLIDATED income statements are turned scans in a
# `/Rotate 0` portrait page, and the tall-box signal above cannot reach them: a page turned
# that badly gives the detector nothing to segment, so it returns **25-29 near-square blobs
# reading 31-56 characters** — 48-68 % tall against a 70 % bar, median w/h 0.82-1.00. The same
# pages at +90 give 178-185 boxes, 121-127 numbers and 2,500-2,800 characters. All three
# quarters reported `no such statement on any page of this filing` on every layer, and each
# also blocked a cumulative Q4.
#
# ⚠️ Widening the entry exposed a latent defect in the probe: it seeded `best_key = None`, so
# the FIRST candidate always won and the page was turned whether or not turning helped. Over
# 460 pages of 11 filings, 38 read under `MIN_UPRIGHT_CHARS` and 27 really are turned; among
# the other 11 is a cover page reading 211 characters upright whose +90 read is ONE — and the
# numbers-first key preferred the ONE, because a cover page has no digits for the base to win
# on. A rotation must now read MORE CHARACTERS than the base before it is ranked at all.


def _blobs(n, text="I"):
    """`n` near-square boxes carrying almost nothing — what a badly turned scan returns."""
    return [_word(10.0 * i, 10.0 * i, 10.0 * i + 8.0, 10.0 * i + 9.0, text) for i in range(n)]


def test_a_page_that_reads_as_nothing_upright_is_probed():
    """⚠️ THE SIGNAL THE TALL-BOX TEST CANNOT GIVE. These boxes are near-square — the share
    taller than wide is 100 % here only because the fixture must be one thing or the other, so
    the test that matters is the one below it, where the shape is explicitly mixed."""
    engine = _Engine({90: [_word(0, 0, 9, 9, "12,879,817,911,898")] * 40, 270: []})
    assert _parser(onnx=engine)._page_rotation(_Page(), _blobs(28)) == 90


def test_and_the_tall_box_signal_alone_would_not_have_found_it():
    """The measured shape: HALF the boxes tall, i.e. under `VERTICAL_LINES_SHARE`, and 28
    characters of text. The first signal abstains and the second one carries it."""
    words = _lines(14, wide=True, text="I") + _lines(14, wide=False, text="I")
    tall = sum(1 for w in words if (w[3] - w[1]) > (w[2] - w[0]))
    assert tall / len(words) < PdfParser.VERTICAL_LINES_SHARE      # the first signal is silent
    assert sum(len(w[4]) for w in words) < PdfParser.MIN_UPRIGHT_CHARS
    engine = _Engine({90: [_word(0, 0, 9, 9, "12,879,817,911,898")] * 40, 270: []})
    assert _parser(onnx=engine)._page_rotation(_Page(), words) == 90


def test_a_rotation_that_reads_worse_than_upright_is_refused():
    """⚠️ THE COVER PAGE, AND THE REASON THE PROBE IS SEEDED WITH THE BASE. Measured on TCB
    Q1-2014 page 4 and FPT Q3-2011 page 1: the upright read is 211 and 128 characters of prose
    with NO parseable numbers, and +90 reads one or two digits and nothing else. Numbers-first
    on an unseeded probe preferred the digits."""
    engine = _Engine({0: [_word(0, 0, 9, 9, "Bao cao tai chinh hop nhat quy I")] * 8,
                      90: [_word(0, 0, 9, 9, "12")],
                      270: [_word(0, 0, 9, 9, "7")]})
    assert _parser(onnx=engine)._page_rotation(_Page(), _blobs(28)) == 0


def test_a_blank_page_stays_upright():
    """Nothing either way — and `>` rather than `>=` is what keeps it where it is."""
    engine = _Engine({0: [], 90: [], 270: []})
    assert _parser(onnx=engine)._page_rotation(_Page(), _blobs(28)) == 0


def test_numbers_still_choose_BETWEEN_two_rotations_that_both_read_well():
    """The character count is a GATE, not the ranking: 90 and 270 both lay the lines flat, and
    only the digit count separates them (121 against 19 on FPT's Q1-2012 page 4)."""
    engine = _Engine({0: [],
                      90: [_word(0, 0, 9, 9, "12,879,817,911,898")] * 30,      # 121-ish numbers
                      270: [_word(0, 0, 9, 9, "lIlIlIlIlIlIlIlIlI")] * 34})    # more chars, no digits
    assert _parser(onnx=engine)._page_rotation(_Page(), _blobs(28)) == 90


def test_a_page_with_plenty_of_text_and_wide_boxes_is_never_probed():
    """⚠️ THE COST BOUND: the new signal is a FLOOR on characters, so an ordinary page — which
    is 91.7 % of the corpus measured — pays nothing at all."""
    engine = _Engine({})
    assert _parser(onnx=engine)._page_rotation(_Page(), _lines(60, wide=True)) == 0
    assert engine.calls == []


# ──────────────────────────────────────────────────────────────────────────────
# 2. the print order
# ──────────────────────────────────────────────────────────────────────────────


def _pages(spec):
    return {i: {"kind": kind, "from_form": ff, "text": "", "words": [], "width": 595.0}
            for i, (kind, ff) in enumerate(spec)}


def _kinds(pages):
    return [pages[i]["kind"] for i in sorted(pages)]


def test_a_filing_may_print_its_cash_flow_before_its_income_statement():
    """BID Q3-2011: balance sheet 1-2, cash flow 3-5, income statement 6. All three stand."""
    pages = _pages([
        (BALANCE_SHEET, False), (BALANCE_SHEET, False),
        (CASH_FLOW, False), (CASH_FLOW, False),
        (None, False),
        (INCOME_STATEMENT, False),
    ])
    PdfParser._enforce_order(pages)
    assert _kinds(pages) == [BALANCE_SHEET, BALANCE_SHEET, CASH_FLOW, CASH_FLOW,
                             None, INCOME_STATEMENT]


def test_an_early_duplicate_is_still_dropped():
    """The defect this guard exists for: a page that merely NAMES a statement matches its
    title, while the statement itself is printed further on. Clearing the early match loses
    nothing — which is exactly what makes it safe to clear."""
    pages = _pages([
        (CASH_FLOW, False), (CASH_FLOW, False),          # the contents / opinion pages
        (BALANCE_SHEET, True), (INCOME_STATEMENT, True),
        (CASH_FLOW, True), (CASH_FLOW, False),           # the statement itself
    ])
    PdfParser._enforce_order(pages)
    assert _kinds(pages) == [None, None, BALANCE_SHEET, INCOME_STATEMENT,
                             CASH_FLOW, CASH_FLOW]


def test_a_form_coded_page_is_never_dropped_for_being_out_of_order():
    """A form code is definitive; the order guard is for fuzzy TITLE matches."""
    pages = _pages([
        (CASH_FLOW, True),
        (BALANCE_SHEET, True), (INCOME_STATEMENT, True), (CASH_FLOW, True),
    ])
    PdfParser._enforce_order(pages)
    assert _kinds(pages)[0] == CASH_FLOW


def test_the_canonical_order_is_untouched():
    pages = _pages([(None, False), (BALANCE_SHEET, True), (BALANCE_SHEET, False),
                    (INCOME_STATEMENT, True), (CASH_FLOW, True), (CASH_FLOW, False)])
    before = _kinds(pages)
    PdfParser._enforce_order(pages)
    assert _kinds(pages) == before


# ──────────────────────────────────────────────────────────────────────────────
# 3. the opening bracket read as a quote
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("token, value", [
    ('"9,797,589,605,016)', -9_797_589_605_016),   # BID Q3-2011, interest expense
    ('"299,126,415,190)', -299_126_415_190),       # ...and other operating expenses
    ('"(3,298,495,452)', -3_298_495_452),          # a stray mark BEFORE a real bracket
    ("'1.234)", -1234),
    ("|1.234)", -1234),
])
def test_a_quote_standing_in_for_an_opening_bracket_is_read_as_negative(token, value):
    assert PdfParser.parse_num(token) == value
    assert PdfParser.NUM_RE.match(token), "and `_numbers` must count it as a figure"


@pytest.mark.parametrize("token", ['"12,345', '"(a)', '"abc)', "VI.24", '"', '")'])
def test_nothing_else_is_widened(token):
    """⚠️ The mark stands in for `(` ONLY where the matching `)` proves a bracket was printed.
    A token that merely STARTS with a quote is not a number, then or now."""
    assert PdfParser.parse_num(token) is None
    assert not PdfParser.NUM_RE.match(token)


@pytest.mark.parametrize("token, value", [
    ("(177,445,466,169)", -177_445_466_169),
    ("12,879,817,911,898", 12_879_817_911_898),
    ("1,630,428.99", 1_630_429),
    ("-", 0),
])
def test_the_readings_that_already_worked_are_unchanged(token, value):
    assert PdfParser.parse_num(token) == value
