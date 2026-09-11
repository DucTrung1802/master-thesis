"""A figure broken into THREE boxes — the repair walked pairwise and the gate counted runs.

⚠️ **`SPR-1`, measured 2026-09-11.** `_merge_split_figures` merged `ws[i]` with `ws[i+1]` and
then stepped `i += 2`. A printed figure that lost ONE thousands separator is two boxes and was
repaired; a figure that lost TWO is three boxes, and the repair took the first pair, skipped
past the third piece, and left the line holding `'304.809.430'` and `'862'`. `split_figures`
counts exactly ONE fragment on that, and `reconcile` refuses the whole statement as fragmented.

⚠️ **THE SIGNATURE IS THE MEASUREMENT, AND IT IS NOT A GUESS.** Over every run folder in
`reports/pdf_ocr/`, of the 89 still-open cells whose best layer refuses on split boxes, the
minimum fragment count any layer reached was:

| fragments | cells |
|---|---|
| **1** | **55** |
| 2 | 12 |
| 3-12 | 17 |
| more | 5 |

A one-fragment refusal is what a three-piece run leaves behind after the pairwise repair, and
nothing else in the pipeline produces it. `.claude/findings/data-state.md`'s GAS pass had
already established the other half — **19 of 20 counted pairs are ONE box that lost a
separator**, split into pieces `box_width / len(text)` apart, 3.58 to 4.47pt against a
`SPLIT_MAX_GAP` of 4.5 — so the pieces are close enough for the gate and were being handed to a
repair that could only take two of them.

⚠️ **WHAT MUST NOT MERGE IS THE POINT OF THE TEST, NOT WHAT MUST.** Absorbing a run applies the
SAME three conditions once more per box — the gap from the head's current right edge, a right
box that is a whole three-digit group (`MERGE_TAIL_RE`), and a joined string that is one
well-formed grouped figure (`MERGE_JOIN_RE`). Two adjacent period columns join to a one-digit
group and fail the third; a right box opening with `(` fails the second (`SPB-1`'s pair, which
the GATE had to learn from this repair in the first place). Both are pinned below.
"""
from web_scraper.cafef_pdf_parser import PdfParser


def _box(x0, x1, text, y=100.0):
    """One detector box. Only x0, x1 and the text matter to the repair."""
    return (x0, y, x1, y + 8.0, text)


def _merged(words):
    return [w[4] for w in PdfParser._merge_split_figures(words, PdfParser.Y_TOL, 0.0)]


def _fragments(words):
    """What `reconcile` would refuse on, after the repair has had its turn."""
    parser = PdfParser.__new__(PdfParser)
    return PdfParser.split_figures(
        parser, {0: PdfParser._merge_split_figures(words, PdfParser.Y_TOL, 0.0)}, 1000.0)


def test_two_pieces_still_merge_exactly_as_before():
    """The case the pairwise walk already handled — unchanged, and it has to stay that way."""
    words = [_box(500.0, 532.0, "3.170.949.624"), _box(536.1, 552.0, "222")]

    assert _merged(words) == ["3.170.949.624.222"]
    assert _fragments(words) == 0


def test_three_pieces_become_one_figure():
    """`SPR-1` itself: a printed 304.809.430.862 that lost two separators."""
    words = [_box(500.0, 516.0, "304"), _box(520.1, 552.0, "809.430"),
             _box(556.1, 572.0, "862")]

    assert _merged(words) == ["304.809.430.862"]
    assert _fragments(words) == 0


def test_four_pieces_become_one_figure():
    """BSR's scans lose a separator anywhere and more than once — `46.625 723 403.018`."""
    words = [_box(500.0, 512.0, "46"), _box(516.1, 532.0, "625"),
             _box(536.1, 552.0, "723"), _box(556.1, 572.0, "403")]

    assert _merged(words) == ["46.625.723.403"]
    assert _fragments(words) == 0


def test_a_negative_broken_into_three_keeps_its_brackets():
    """BID's FY-2016 shape, one piece further: `(1.029.827)` as `(1` + `029` + `827)`.

    The head's trailing `)` is stripped before each join and the tail keeps its own, so the
    figure comes back bracketed rather than as a positive number three orders out.
    """
    words = [_box(500.0, 514.0, "(1"), _box(518.1, 534.0, "029"), _box(538.1, 554.0, "827)")]

    assert _merged(words) == ["(1.029.827)"]
    assert _fragments(words) == 0


def test_two_adjacent_period_columns_are_not_absorbed():
    """⚠️ The safety claim. Joined they give a ONE-DIGIT group, which `MERGE_JOIN_RE` refuses —
    and that is what separates a continuation from the next period, not the gap."""
    words = [_box(500.0, 540.0, "1.558.887.407"), _box(544.1, 584.0, "1.541.259.663")]

    assert _merged(words) == ["1.558.887.407", "1.541.259.663"]


def test_a_bracketed_neighbour_is_not_absorbed():
    """⚠️ `SPB-1`'s pair, from FPT's FY-2008 income statement, 4.32pt apart. A box that OPENS
    with `(` is starting its own figure however well the digits would join."""
    words = [_box(500.0, 540.0, "85.604.572.576"), _box(544.1, 584.0, "(132.899.704.388)")]

    assert _merged(words) == ["85.604.572.576", "(132.899.704.388)"]


def test_a_run_stops_where_the_gap_stops():
    """A third piece beyond `MERGE_MAX_GAP` is a different figure and is left alone.

    ⚠️ The gap is measured from the HEAD's current right edge, not from the first box's — so
    absorbing does not let a run creep across a column boundary one point at a time.
    """
    words = [_box(500.0, 516.0, "304"), _box(520.1, 552.0, "809.430"),
             _box(600.0, 616.0, "862")]

    assert _merged(words) == ["304.809.430", "862"]
