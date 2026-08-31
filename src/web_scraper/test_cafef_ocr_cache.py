"""The page OCR cache and the capital-note skip — the two things that made a hard filing 6.8x
cheaper without moving a character of its output.

⚠️ **BOTH ARE COST-ONLY CHANGES TO A PATH THAT WRITES FUNDAMENTALS**, which is the reason they
are pinned here rather than trusted to a stopwatch. What each one claims:

  * `_ocr_cache` — the pixels->text step depends on `(engine, dpi, crop_pad)` and on NOTHING
    else. Every other `ParseLayer` flag runs AFTER `scan` has read the page, in `_page_kind`,
    `_fill_continuations`, `table_rows` or `parse`. Measured over the 49 layers on 2026-08-30:
    **24 distinct `parse_key` against 7 distinct `ocr_key`**, so a filing that defeats the
    cascade was reading every page of itself 24 times to produce 7 answers.
    ⚠️ The one post-step that IS per-layer — `join_split_digits` — must therefore stay OUTSIDE
    the cache, and two of the tests below are what say so.
  * `want_shares` — `share_capital` walks from the last statement page to the END of the filing
    and is invisible to the page-progress hook, so it never showed up in a run log. Measured
    2026-08-30: BID's FY-2016 annual, **50 pages / 84.8 s, 68.8 % of one `parse()`**, returning
    nothing; VIC's Q1-2026 (a `corp` filing, where the anchor is bank-worded and can never
    match) **58 pages / 81.9 s, 76.6 %**. `_parse_cascaded` reads the counts only while the
    document's facts are still open, so every layer after the first one to produce a publish
    date was paying for a value that is thrown away.

Verified end to end on BID Q4-2016, the hardest filing on disk: **64.6 min -> 9.5 min**, the
same winning layer (`onnx@200+pad6+annual+extra`), and all three statements IDENTICAL on
`rows_sha` — every row the OCR read, mapped or not, not merely the 76 cells that map.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import PdfParser


# PyMuPDF's word shape: (x0, y0, x1, y1, text, block, line, n) — `_split_number_runs`
# reads w[4], so a 2-tuple placeholder would fail inside the code under test rather than in it.
_WORD = (100.0, 10.0, 140.0, 20.0, "1.234", 0, 0, 0)


class _Page:
    """What `_ocr_page` reads before it reaches the engine. `rotation` joined `number` in the
    cache key on 2026-08-30: a page whose scan is turned is read at a different `/Rotate`, and
    the upright reading and the turned one are two different answers about the same page."""

    def __init__(self, number=0, rotation=0):
        self.number = number
        self.rotation = rotation

    def set_rotation(self, r):
        self.rotation = r


def _parser(**kw):
    """A parser with the OCR engine stubbed out — no onnxruntime, no tesseract, no PDF."""
    p = PdfParser.__new__(PdfParser)
    p.engine = kw.get("engine", "onnx")
    p.dpi = kw.get("dpi", 200)
    p._onnx = None
    p.join_split_digits = False
    p.join_lost_separator = False
    p.ocr_ready = True
    p._ocr_cache = {}
    p._ocr_cache_path = None
    p._page_rot = {}
    p.reads = []
    return p


def _stub_read(parser):
    """Record every call and return a distinct payload per call, so a HIT is detectable."""
    def read(page, native):
        parser.reads.append((page.number,) + parser._ocr_config())
        return (f"text-{len(parser.reads)}", [_WORD], True)
    parser._read_page = read


# ──────────────────────────────────────────────────────────────────────────────
# the cache key
# ──────────────────────────────────────────────────────────────────────────────

def test_the_same_page_at_the_same_ocr_config_is_read_once():
    p = _parser()
    _stub_read(p)
    first = p._ocr_page(_Page(3), "")
    second = p._ocr_page(_Page(3), "")

    assert len(p.reads) == 1
    assert first[0] == second[0] == "text-1"


def test_a_different_page_is_a_different_key():
    p = _parser()
    _stub_read(p)
    p._ocr_page(_Page(3), "")
    p._ocr_page(_Page(4), "")

    assert len(p.reads) == 2


@pytest.mark.parametrize("attr,value", [("dpi", 300), ("engine", "tesseract")])
def test_a_different_ocr_config_re_reads_the_page(attr, value):
    """dpi and engine change the pixels and the characters, so they must never share."""
    p = _parser()
    _stub_read(p)
    p._ocr_page(_Page(0), "")
    setattr(p, attr, value)
    p._ocr_page(_Page(0), "")

    assert len(p.reads) == 2


def test_crop_pad_is_part_of_the_key():
    """⚠️ ACB Q3-2023 reads 93.261.018 as 261.018 at the default crop and correctly at 6 — the
    detector box starts inside the number, so the recogniser is never shown the leading digit.
    Handing the wider-crop layer the narrow crop's cached page would hand it the reading that
    already failed, and the layer would 'run' without reading a pixel."""
    p = _parser()
    _stub_read(p)

    class _Onnx:
        crop_pad = 2.0

    p._onnx = _Onnx()
    p._ocr_page(_Page(0), "")
    p._onnx.crop_pad = 6.0
    p._ocr_page(_Page(0), "")

    assert len(p.reads) == 2


def test_a_per_layer_flag_that_runs_after_the_read_does_not_re_read():
    """`join_split_digits` is a `ParseLayer` flag applied to the CACHED words, so the layer
    that needs it costs a re-split and not a second OCR pass."""
    p = _parser()
    _stub_read(p)
    p._ocr_page(_Page(0), "")
    p.join_split_digits = True
    p._ocr_page(_Page(0), "")

    assert len(p.reads) == 1


def test_the_per_layer_split_is_replayed_on_every_hit_not_frozen_into_the_cache():
    """⚠️ THE ONE WAY THIS CACHE COULD CHANGE AN ANSWER. If `_split_number_runs` ran before the
    store, a cache HIT would return the FIRST layer's split — so `onnx@200+join+components`
    would silently get `onnx@200`'s words and the join flag would do nothing at all."""
    p = _parser()
    _stub_read(p)
    seen = []

    def split(words, join, join_lost=False):
        seen.append(join)
        return list(words)

    p._split_number_runs = split
    p._ocr_page(_Page(0), "")
    p.join_split_digits = True
    p._ocr_page(_Page(0), "")

    assert seen == [False, True]
    assert len(p.reads) == 1


def test_a_path_that_never_splits_is_not_split_on_a_hit_either():
    """The native-text and Tesseract paths have never run `_split_number_runs`, and a cache
    that forgot WHICH path produced a page would start splitting their words on the hit."""
    p = _parser()
    p._read_page = lambda page, native: ("native", [_WORD], False)

    def split(words, join, join_lost=False):
        pytest.fail("a native/tesseract page must not be split")

    p._split_number_runs = split
    p._ocr_page(_Page(0), "")
    p._ocr_page(_Page(0), "")


# ──────────────────────────────────────────────────────────────────────────────
# the cache is scoped to ONE document
# ──────────────────────────────────────────────────────────────────────────────

def test_a_new_document_discards_the_previous_one_s_pages():
    """⚠️ Page 3 of one filing is not page 3 of the next, and a parser instance is reused for a
    whole run (`FinancialsBuilder._parser_for`) — so without this the cache would both answer
    wrongly and grow without bound."""
    p = _parser()
    _stub_read(p)
    p._use_document("a.pdf")
    p._ocr_page(_Page(3), "")
    p._use_document("b.pdf")
    p._ocr_page(_Page(3), "")

    assert len(p.reads) == 2


def test_re_entering_the_same_document_keeps_its_pages():
    """`_parse_cascaded` calls `parse()` once per parse key on the SAME path — which is exactly
    where the 24-passes-for-7-configs saving lives."""
    p = _parser()
    _stub_read(p)
    p._use_document("a.pdf")
    p._ocr_page(_Page(3), "")
    p._use_document("a.pdf")
    p._ocr_page(_Page(3), "")

    assert len(p.reads) == 1


# ──────────────────────────────────────────────────────────────────────────────
# the two keys, and the gap between them
# ──────────────────────────────────────────────────────────────────────────────

def test_the_cascade_reads_its_pages_once_per_ocr_config_not_once_per_parse_key():
    """⚠️ STRUCTURAL. `ocr_key` names three fields; `parse_key` names eleven. The eight in the
    gap are all read AFTER the page has been OCR'd, so a filing's pages are read once per OCR
    configuration and re-MAPPED once per parse key. If a genuinely pixel-changing field is ever
    added to `ParseLayer` it must join `ocr_key`, or its layer will silently reuse another
    configuration's pages — which is the wider-crop failure `test_crop_pad_is_part_of_the_key`
    describes, arriving through the back door."""
    layers = FinancialsBuilder.LAYERS
    by_ocr = {}
    for layer in layers:
        by_ocr.setdefault(ocr_key(layer), set()).add(parse_key(layer))

    assert sum(len(v) for v in by_ocr.values()) == len({parse_key(x) for x in layers})
    # at least one OCR configuration is shared by two parse keys, or the cache gives nothing
    assert max(len(v) for v in by_ocr.values()) > 1
    assert len(by_ocr) < len({parse_key(x) for x in layers})


# ──────────────────────────────────────────────────────────────────────────────
# the capital-note skip
# ──────────────────────────────────────────────────────────────────────────────

def test_the_share_scan_is_requested_only_while_the_document_facts_are_still_open():
    """⚠️ **THE FLAG IS THE CONDITION THE VALUE IS READ UNDER, WHICH IS WHY THIS IS FREE.**
    `_parse_cascaded` assigns `facts["shares"]` inside `if not facts["publish_date"]`, so once
    a layer has produced a publish date no later layer's counts are ever looked at — and
    `parse()` went on paying for them once per parse key, 50 pages and 84.8 s a time on BID's
    FY-2016 annual. `facts` cannot change between the `parse()` call and that check, so the
    accepted rows and the written facts are identical.
    """
    import web_scraper.cafef_financials as fin

    class _Statement:
        publish_date = "2017-04-17"
        shares_authorized = shares_issued = shares_outstanding = None

    asked = []

    class _Parser:
        ocr_ready = True
        on_page = None
        _logger = None

        def parse(self, path, period_end, want_shares=True):
            asked.append(want_shares)
            return {"balance_sheet": _Statement()}

        def __getattr__(self, name):          # every set_* the cascade calls
            return lambda *a, **k: None

    builder = fin.FinancialsBuilder.__new__(fin.FinancialsBuilder)
    builder._logger = None
    builder.on_layer = builder.on_page = None
    builder._parsers = {}
    builder._parser_for = lambda engine: _Parser()
    builder.map_to_schema = lambda *a, **k: {}
    builder.reconcile = lambda *a, **k: "nothing reconciles here"
    builder.sane = lambda *a, **k: None
    builder._warn = lambda *a, **k: None

    accepted, facts = builder._parse_cascaded(
        "x.pdf", None, "bank", {r: [] for r in fin.REPORTS})

    assert facts["publish_date"] == "2017-04-17"
    # the first layer asks; nothing after it does, because nothing after it could use the answer
    assert asked[0] is True
    assert not any(asked[1:]), asked
    assert len(asked) > 1, "the cascade should have run past its first layer"


def test_the_share_scan_keeps_being_requested_while_no_publish_date_has_been_found():
    """The complement, and it is the case that must NOT be optimised away: a filing whose
    pages carry no signing date leaves `facts` open, so every layer's counts are still live and
    every layer must still be asked for them. TCB's Q3-2013 is exactly this shape."""
    import web_scraper.cafef_financials as fin

    class _Statement:
        publish_date = ""
        shares_authorized = shares_issued = shares_outstanding = None

    asked = []

    class _Parser:
        ocr_ready = True
        on_page = None
        _logger = None

        def parse(self, path, period_end, want_shares=True):
            asked.append(want_shares)
            return {"balance_sheet": _Statement()}

        def __getattr__(self, name):
            return lambda *a, **k: None

    builder = fin.FinancialsBuilder.__new__(fin.FinancialsBuilder)
    builder._logger = None
    builder.on_layer = builder.on_page = None
    builder._parsers = {}
    builder._parser_for = lambda engine: _Parser()
    builder.map_to_schema = lambda *a, **k: {}
    builder.reconcile = lambda *a, **k: "nothing reconciles here"
    builder.sane = lambda *a, **k: None
    builder._warn = lambda *a, **k: None

    builder._parse_cascaded("x.pdf", None, "bank", {r: [] for r in fin.REPORTS})

    assert all(asked), asked
