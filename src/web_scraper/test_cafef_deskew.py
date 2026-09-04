"""`SKW-2` / `UNP-1` / `MSO-3` / `JVW-1` - a crooked scan, and the defects it hides.

No PDF, no network, no OCR engine: every fixture here is hand-made word boxes, in the shape
the recogniser returns them (`x0, y0, x1, y1, text`).
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import PdfParser


@pytest.fixture(scope="module")
def parser():
    # ⚠️ `__new__`, so this carries no instance attributes - the per-layer flags come from
    # the CLASS defaults, which is exactly what those defaults exist for (see PdfParser).
    return PdfParser.__new__(PdfParser)


# -- the page: three rows, four columns, skewed 0.013 (about 0.75 degrees) -----
SLOPE = 0.013
COLUMNS = [500.0, 610.0, 720.0, 830.0]
ROWS = [
    ("1 Doanh thu", [15_972_397_069_700, 13_761_831_884_948,
                     45_311_586_755_696, 37_929_368_276_576]),
    ("2 Cac khoan giam tru", [69_575_353_943, 86_909_091,
                              70_610_210_220, 1_880_152_514]),
    ("3 Doanh thu thuan", [15_902_821_715_757, 13_761_744_975_857,
                           45_240_976_545_476, 37_927_488_124_062]),
]


def _page(slope):
    """The same printed page, rotated by `slope`: y grows with x, exactly as a scan does."""
    words = []
    for r, (label, values) in enumerate(ROWS):
        base = 200.0 + 14.0 * r
        # the label box is TALLER than a figure box, which is why the estimator uses centres
        words.append((90.0, base + slope * 90.0, 260.0, base + 14.0 + slope * 90.0, label))
        for c, v in zip(COLUMNS, values):
            x0 = c - 76.0
            mid = (x0 + c) / 2.0
            words.append((x0, base + 2.0 + slope * mid, c, base + 12.0 + slope * mid,
                          "{:,}".format(v).replace(",", ".")))
    return words


def _rows(parser, on, slope=SLOPE):
    parser.deskew_rows = on
    return parser.table_rows({0: _page(slope)}, COLUMNS)


# -- the defect itself, so the fix cannot be quietly undone -------------------
def test_a_skewed_page_hands_each_label_its_neighbours_figures(parser):
    got = {r.key: r.values for r in _rows(parser, False)}
    assert not any(v == ROWS[0][1] for v in got.values()), \
        "row 1 kept all four figures on a page skewed past Y_TOL - no defect to fix"


def test_deskewing_puts_every_row_back_on_its_own_figures(parser):
    got = {r.key: r.values for r in _rows(parser, True)}
    for label, values in ROWS:
        key = PdfParser.slug(label)
        assert got[key] == values, "{}: {} != {}".format(key, got.get(key), values)


def test_a_clean_page_is_left_exactly_as_it_was(parser):
    """⚠️ The search always finds SOMETHING; `DESKEW_GAIN` stops it acting on noise."""
    assert [(r.key, r.values) for r in _rows(parser, True, 0.0)] \
        == [(r.key, r.values) for r in _rows(parser, False, 0.0)]


def _clusters(parser, words, slope):
    """How well `slope` groups these boxes into lines - the estimator's own score."""
    pts = [((w[0] + w[2]) / 2.0, (w[1] + w[3]) / 2.0) for w in words]
    x0 = min(x for x, _ in pts)
    ys = sorted(y - slope * (x - x0) for x, y in pts)
    total, start, n = 0, ys[0], 1
    for v in ys[1:]:
        if v - start <= parser.Y_TOL:
            n += 1
        else:
            total, start, n = total + n * n, v, 1
    return total + n * n


def test_the_slope_is_measured_from_the_page_and_groups_every_row(parser):
    """⚠️ **IT IS IDENTIFIED ONLY UP TO THE WIDTH OF THE PLATEAU, so asserting a tight
    bound would assert something the method cannot deliver.** `Y_TOL` is a tolerance: once a
    slope brings a row inside it, every nearby slope scores identically - on this fixture the
    score is flat from 0.009 to past 0.020 for a true 0.013. What the estimate must do is
    group every row, which is what is asserted, and it must be the CENTRE of that band rather
    than its first point (see `_page_skew`)."""
    words = _page(SLOPE)
    got = parser._page_skew(words)
    perfect = len(ROWS) * (1 + len(COLUMNS)) ** 2
    assert _clusters(parser, words, got) == perfect, got
    assert _clusters(parser, words, 0.0) < perfect, "the fixture is not skewed"
    assert abs(got - SLOPE) < 0.005, got
    assert parser._page_skew(_page(0.0)) == 0.0


def test_the_estimator_uses_box_CENTRES_not_tops(parser):
    """⚠️ Labels are taller than figures and sit at low x, so scoring on `y0` reads that
    constant height difference as slope. Measured on FPT Q3-2024: 0.0151 against a true
    0.0125 - an over-estimate, in the one direction that matters."""
    words = _page(SLOPE)
    tops = [((w[0] + w[2]) / 2.0, w[1]) for w in words]
    centres = [((w[0] + w[2]) / 2.0, (w[1] + w[3]) / 2.0) for w in words]
    assert tops != centres, "the fixture must model a label box taller than a figure box"
    # Scored on TOPS the winning band is dragged high by the label boxes; on centres it is not.
    on_tops = max((_clusters(parser, [(w[0], w[1], w[2], w[1], w[4]) for w in words], s), -s)
                  for s in (i * 0.001 for i in range(0, 31)))
    assert _clusters(parser, words, parser._page_skew(words))         == len(ROWS) * (1 + len(COLUMNS)) ** 2 >= on_tops[0]


def test_both_bucketing_passes_measure_a_word_the_same_way(parser):
    """⚠️ `_reseat` re-derived the y inline and silently undid the correction - the reason
    `_row_y` exists. A re-seated de-skewed page must agree with a de-skewed one."""
    parser.deskew_rows = True
    parser.reseat_words = True
    try:
        got = {r.key: r.values for r in parser.table_rows({0: _page(SLOPE)}, COLUMNS)}
    finally:
        parser.reseat_words = False
    for label, values in ROWS:
        assert got[PdfParser.slug(label)] == values


# -- the cascade --------------------------------------------------------------
def test_the_deskew_layers_run_after_every_layer_that_reads_the_page_as_printed():
    layers = FinancialsBuilder.LAYERS
    flagged = [i for i, l in enumerate(layers) if l.deskew_rows]
    assert flagged, "the block was removed - do not re-add it without a quarter it recovers"
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    assert min(flagged) > max(strict)


def test_a_deskew_layer_carries_what_it_was_measured_with():
    for l in FinancialsBuilder.LAYERS:
        if not l.deskew_rows:
            continue
        # ⚠️ Each was measured NECESSARY on FPT, not assumed: without `reseat_words` the
        # widest rows still split, and without `join_lost_separator` Q3-2019 is refused as
        # fragmented on all three statements however straight the page is made.
        assert l.reseat_words and l.label_wrap and l.join_lost_separator


def test_deskewing_is_a_parse_key_and_not_an_ocr_key():
    """It rebuilds the rows, so a cached parse taken without it must not be served - and it
    moves boxes the recogniser has already returned, so it buys no second OCR pass."""
    a = ParseLayer("a", "onnx", 200)
    b = ParseLayer("b", "onnx", 200, deskew_rows=True)
    assert parse_key(a) != parse_key(b)
    assert ocr_key(a) == ocr_key(b)


# -- `UNP-1` - one statement, two printed units -------------------------------
def _pages(*texts):
    return {i: {"text": t} for i, t in enumerate(texts)}


def test_the_first_page_that_states_a_unit_wins(parser):
    """FPT Q3-2024: page 8 is the statement and declares VND, page 9 is an appended
    explanation table and declares millions. Scanning for millions alone took page 9."""
    pages = _pages("\u0110\u01a1n v\u1ecb: VND ...",
                   "\u0110VT: Tri\u1ec7u \u0111\u1ed3ng ...")
    assert parser.declared_unit(pages, [0, 1]) == 1
    assert parser.unit_of(pages, [0, 1]) == 1


def test_a_silent_first_page_still_defers_to_a_later_one(parser):
    """The case the old rule was written for - a continuation page may not repeat it - read
    from the other side, and it must keep working."""
    pages = _pages("BAO CAO KET QUA ...",
                   "\u0110\u01a1n v\u1ecb t\u00ednh: Tri\u1ec7u \u0111\u1ed3ng")
    assert parser.declared_unit(pages, [0, 1]) == 1_000_000


def test_silence_is_still_None_and_not_dong(parser):
    """⚠️ Rule 2 at the unit: did not say is not said dong, and only the second may be
    believed. `unit_of` defaults the first to 1; `declared_unit` must not."""
    pages = _pages("BAO CAO KET QUA ...", "c\u1ed9ng ...")
    assert parser.declared_unit(pages, [0, 1]) is None
    assert parser.unit_of(pages, [0, 1]) == 1


@pytest.mark.parametrize("text", [
    "\u0110\u01a1n v\u1ecb t\u00ednh: Tri\u1ec7u \u0111\u1ed3ng",
    "\u0110\u01a1n v\u1ecb: Tri\u1ec7u \u0111\u1ed3ng",
    "\u0110VT: Tri\u1ec7u VN\u0110",
])
def test_no_dong_needle_fires_inside_a_millions_declaration(parser, text):
    """⚠️ The needles are anchored to don-vi / DVT glued to the unit token precisely so a
    millions declaration cannot produce one. A bare `dong` would fire on all of these."""
    bare = parser.norm(text).replace(" ", "")
    assert parser._declares_millions(bare)
    assert not parser._declares_dong(bare), bare
    assert parser.declared_unit(_pages(text), [0]) == 1_000_000


# -- `MSO-3` - a heading set on two lines -------------------------------------
def _header(stacked):
    """The item-code column heading, over a code column at 314 and two figure columns."""
    head = ([(298.6, 159.6, 313.2, 170.2, "M\u00e3"),
             (300.7, 169.4, 312.0, 179.3, "s\u1ed1")]
            if stacked else [(298.6, 159.6, 315.0, 170.2, "M\u00e3 s\u1ed1")])
    return head + [
        (299.8, 187.7, 315.1, 197.3, "100"),
        (379.9, 187.7, 445.4, 196.8, "18.431.909.603.327"),
        (460.8, 187.7, 524.6, 196.8, "18.406.087.226.041"),
    ]


@pytest.mark.parametrize("stacked", [False, True])
def test_a_ma_so_heading_drops_the_item_code_column_however_it_is_set(parser, stacked):
    assert parser._code_column([314.2, 445.3, 525.3], {0: _header(stacked)}) == 314.2


def test_a_stacked_join_still_needs_a_column_under_it(parser):
    """⚠️ Conditions 2 and 3 are untouched and remain the real protection: the join widens
    what may be READ as a heading, never what may be dropped."""
    assert parser._code_column([445.3, 525.3], {0: _header(True)}) is None


def test_two_table_rows_are_not_joined_into_a_heading(parser):
    """⚠️ The join is vertical adjacency plus x-overlap. Ordinary rows are 13-18pt apart
    and horizontally offset, which is what keeps this off a table."""
    far = [(298.6, 159.6, 313.2, 170.2, "M\u00e3"),
           (300.7, 200.0, 312.0, 210.0, "s\u1ed1")]
    assert parser._code_column([314.2, 445.3], {0: far}) is None


# -- `JVW-1` - the corp income statement joint-venture line -------------------
def test_the_jv_alias_is_offered_only_to_its_own_account():
    b = FinancialsBuilder(logger=None)
    account = "phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket"
    printed = "loi_nhuan_tu_cong_ty_lien_doanh_lien_ket"
    assert b._label_score(account, printed) < b.SCHEMA_MATCH
    assert b._label_score(account, printed, equity_wording=True) >= b.SCHEMA_MATCH
    # ⚠️ An alias only ever raises ITS OWN account score - the mechanism is keyed on the
    # account, so a name scoring well against another chart account cannot reach it.
    other = "i_2_3_dau_tu_vao_cong_ty_lien_doanh_lien_ket"
    assert b._label_score(other, printed, equity_wording=True) \
        == b._label_score(other, printed)


def test_every_account_wording_key_names_exactly_one_column():
    """The safety standard ACCOUNT_WORDING shipped on, asserted for EVERY entry rather than
    argued for the newest one: an equality test may only ever reach one account."""
    import glob
    import os

    from web_scraper import cafef_financials as fin

    b = FinancialsBuilder(logger=None)
    charts = {}
    for f in glob.glob(os.path.join(fin.SCHEMA_DIR, "*.csv")):
        template, report = os.path.basename(f)[:-4].split("_", 1)
        # ⚠️ `schema_of`, never the raw `column`: the account a score is taken against has
        # had its section prefix and item numbering stripped, so `viii_von_chu_so_huu` IS
        # `vonchusohuu` here. Reading the CSV column instead measures a different string, and
        # the test would have passed while the key reached nothing.
        charts[(template, report)] = [PdfParser.norm(a).replace(" ", "")
                                      for _col, a in b.schema_of(template, report)]
    assert charts, "no chart of accounts on disk - this test measures nothing"
    # ⚠️ **THE CLAIM `ACCOUNT_WORDING` SHIPPED WITH WAS NARROWER THAN THE CODE, AND THIS
    # TEST IS WHAT MEASURED IT.** Its comment reads *"the only column in any of the twelve
    # charts whose account text IS 'von chu so huu'"*; measured, that key names an account on
    # FIVE charts and TWICE on three of them (corp / securities / insurance balance sheets
    # each carry a section and a subtotal that reduce to the same text once the prefix and
    # numbering are stripped). True on the BANK balance sheet, where it was measured, and not
    # in general - so uniqueness is not what makes this safe and must not be asserted.
    #
    # ⚠️ **WHAT DOES MAKE IT SAFE IS THAT AN ALIAS IS NOT ITSELF AN ACCOUNT.** The rewrite
    # offers an account one extra spelling; if that spelling were another account of the same
    # chart, two real accounts would compete for one row and the shorter would sometimes win
    # (`NST-1`). And a key naming no account anywhere is a dead entry - the alias could never
    # be offered - which is the other way this table rots.
    for key, aliases in FinancialsBuilder.ACCOUNT_WORDING.items():
        assert any(key in accounts for accounts in charts.values()),             "{} names no account in any chart - the alias can never be offered".format(key)
        for alias in aliases:
            for chart, accounts in charts.items():
                assert alias not in accounts, (alias, chart)
