"""`SPB-1` — a BRACKET is a figure boundary, and the split gate was ignoring both of them.

`split_figures` refuses a reading in which the detector cut one printed figure into two boxes
(`SPL-1`). It counted a pair whenever the gap was under `SPLIT_MAX_GAP` and the digits joined
into a well-formed grouped figure — and it stripped `()` from BOTH boxes before joining, so two
ADJACENT PERIOD COLUMNS could be counted as one split figure whenever the right one happened to
be negative.

⚠️ MEASURED ON FPT's FY-2008 INCOME STATEMENT: `'85.604.572.576'` and `'(132.899.704.388)'`
sit **4.32pt apart** on the line "Phân bổ vào các quý", and their digits join into
`85.604.572.576.132.899.704.388` — a well-formed grouped figure. The statement was refused as
fragmented at SIX configurations including 300 and 400 dpi, i.e. a false positive that no
escalation can clear, which is the worst kind: the cascade has nowhere left to go.

⚠️ AND `_merge_split_figures` HAS ALWAYS BEEN PROTECTED FROM THE SAME PAIR — its
`MERGE_TAIL_RE` requires the right box to begin with a full THREE-DIGIT group, which `'(132…'`
does not. So the REPAIR refused this pair while the GATE counted it; this is the gate adopting
the repair's own evidence.
"""
from web_scraper.cafef_pdf_parser import PdfParser


def _num(text, x0, y=100.0, w=70.0):
    return (x0, y, x0 + w, y + 9.0, text, 0, 0, 0)


def _parser():
    p = PdfParser.__new__(PdfParser)
    p.join_split_digits = False
    p.join_lost_separator = False
    return p


WIDTH = 595.44          # VALUE_ZONE puts the lo edge at 238.2


def _count(*words):
    return _parser().split_figures({1: list(words)}, WIDTH)


def test_the_two_period_columns_of_FPTs_FY2008_are_not_a_split_figure():
    """The measured pair, at its measured gap."""
    left = _num("85.604.572.576", 300.0)
    right = _num("(132.899.704.388)", 300.0 + 70.0 + 4.32)
    assert _count(left, right) == 0


def test_a_genuine_split_is_still_counted():
    """VIC Q3-2014's own shape: one printed 5.209.108.954.978 as two boxes 3.8pt apart, and
    neither half carries a bracket."""
    left = _num("5.209.108", 300.0)
    right = _num("954.978", 300.0 + 70.0 + 3.8)
    assert _count(left, right) == 1


def test_a_genuine_split_of_a_NEGATIVE_figure_is_still_counted():
    """⚠️ THE CASE THE RULE MUST NOT BREAK, and it is why the test is on the INNER edges: a
    negative figure split across two boxes keeps its `(` on the LEFT half and its `)` on the
    RIGHT one. So the left never ends with `)` and the right never begins with `(` — the two
    boundaries the rule reads are exactly the ones a continuation cannot have."""
    left = _num("(5.209.108", 300.0)
    right = _num("954.978)", 300.0 + 70.0 + 3.8)
    assert _count(left, right) == 1


def test_two_adjacent_negatives_are_not_a_split_figure():
    left = _num("(85.604.572.576)", 300.0)
    right = _num("(132.899.704.388)", 300.0 + 70.0 + 4.0)
    assert _count(left, right) == 0


def test_the_rule_reads_only_the_brackets_and_nothing_else():
    """`_joinable` is the whole of it, so it can be asserted directly — and a rule this small
    is one a later reader can check against a page by eye."""
    j = PdfParser._joinable
    assert j("5.209.108", "954.978")
    assert not j("85.604.572.576", "(132.899.704.388)")
    assert not j("(85.604.572.576)", "132.899.704.388")
    assert j("(5.209.108", "954.978)")
    # whitespace the recogniser leaves around a box must not hide the bracket
    assert not j("85.604.572.576 ", " (132.899.704.388)")


def test_the_gate_and_the_repair_now_agree_about_what_a_continuation_is():
    """⚠️ THE POINT OF THE FIX. `_merge_split_figures` would never have joined FPT's pair, and
    the gate counted it — a statement refused for a repair the code itself would have refused
    to make. The two must answer the same question the same way."""
    p = _parser()
    left = _num("85.604.572.576", 300.0)
    right = _num("(132.899.704.388)", 300.0 + 70.0 + 4.32)
    merged = p._merge_split_figures([left, right], p.Y_TOL, WIDTH * p.VALUE_ZONE)
    assert len(merged) == 2, "the repair leaves the two figures alone…"
    assert _count(left, right) == 0, "…and the gate must not count them either"


# ── TSM-1: the same disagreement, one engine over ─────────────────────────────

def test_the_repair_reaches_the_tesseract_path_too():
    """⚠️ `SPB-1`'s RULE, VIOLATED BY THE ENGINE SPLIT RATHER THAN BY THE BRACKETS. The gate
    counts fragments on every engine; `_merge_split_figures` sat inside `_read_page`'s onnx
    branch, so a Tesseract reading was refused for boxes nothing was ever going to join.

    HPG's Q1-2022 income statement is the case: onnx loses that page's title AND its form code,
    so `tesseract@200` is the only layer that finds the statement at all, and it was refused
    `2 figure(s) split across two boxes` on these two pairs.
    """
    p = _parser()
    for whole, tail, gap in (("7.005.559", "045", 4.1), ("6", "977.554.343", 1.5)):
        left = _num(whole, 300.0, w=len(whole) * 6.0)
        right = _num(tail, 300.0 + len(whole) * 6.0 + gap, w=len(tail) * 6.0)
        assert _count(left, right) == 1, f"the gate counts {whole}+{tail}"
        merged = p._merge_split_figures([left, right], p.Y_TOL, WIDTH * p.VALUE_ZONE)
        assert len(merged) == 1, f"so the repair must join {whole}+{tail}"
        assert merged[0][4] == f"{whole}.{tail}"


def test_the_tesseract_path_still_does_not_run_the_number_run_splitter():
    """⚠️ ONLY THE MERGE CROSSED OVER. `_split_number_runs` repairs a box holding SEVERAL
    figures, which is a LINE detector's artefact — Tesseract boxes words — so `splittable`
    stays False for that path and says so in the tuple `_read_page` returns."""
    import inspect

    from web_scraper.cafef_pdf_parser import PdfParser as P

    src = inspect.getsource(P._read_page)
    code = "\n".join(l for l in src[src.index("get_textpage_ocr"):].splitlines()
                     if not l.lstrip().startswith("#"))       # the CODE, not the reasoning
    assert "_merge_split_figures" in code
    assert "_split_number_runs" not in code
    assert code.rstrip().endswith("return text, words, False")
