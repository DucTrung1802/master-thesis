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
