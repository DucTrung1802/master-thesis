"""A figure split across two detector boxes — pinned without a PDF, a network or an OCR engine.

The defect: the DB text detector sometimes emits one printed figure as TWO boxes a few points
apart. VIC Q3-2014's balance sheet at onnx@200 returns `'5.209.108'` ending at x=405.7 and
`'954.978'` starting at x=409.5 — one figure, 5.209.108.954.978, in two boxes 3.8pt apart.

Both halves are plausible numbers, so nothing downstream can tell. The left half lands on no
column and is dropped, leaving the row holding `954.978`; where enough left halves line up they
form a spurious column of their own and are kept as a period. 60 of that statement's figures go
that way while BOTH GRAND TOTALS survive whole — so `reconcile` passes, `sane` probes a total
that is correct, and a statement reading `i_1_tien = 158.154` against a printed 945.186.158.154
would be written as `pdf`. That is `SLD-1`'s shape a fourth time.

⚠️ THERE ARE TWO ANSWERS AND BOTH SHIP. `_merge_split_figures` REPAIRS the pairs it can prove
at the OCR seam; `split_figures` + `reconcile` REFUSE whatever is left, which escalates the
cascade — and the same document at onnx@300 splits NOTHING. The merge was held back on
2026-08-29 morning as too wide a change on four bank filings; the measurement that released it
is below.

⚠️ AND THE GAP IS MEASURED. Across 21 bank statements — VCB Q1-2021/Q2-2021/Q1-2024/Q1-2026/
Q4-2016, ACB Q1-2021/Q1-2024/Q1-2026/Q4-2016, BID Q1-2021/Q1-2024/Q1-2026/Q4-2016, every one of
which parses today — the merge fires ONCE and changes NO mapped cell. At 6pt it starts taking
pairs that are two adjacent period columns. Real inter-column gaps in the same corpus reach
3.6pt, so the gap alone cannot do it: the TAIL test (a continuation always begins with a full
three-digit group) is what separates them.

⚠️ AND THE REPAIR IS SCORED AGAINST A GROUND TRUTH, not argued. onnx@300 reads VIC Q3-2014 with
0 splits, so it is what 200 dpi should agree with. Raw, they agree on 29 of 45 balance-sheet
cells; with the merge, **43** — and the safety claim that matters is the other column:
**17 cells repaired, 0 cells broken** across the three statements.
"""
import pytest

from web_scraper.cafef_pdf_parser import PdfParser


WIDTH = 595.44
X_NOW, X_PRIOR = 436.5, 540.4


def _box(x1, y, text, w=60.0):
    return (x1 - w, y, x1, y + 11.0, text, 0, 0, 0)


def _whole_page():
    """Two period columns, every figure in one box — the reading onnx@300 gives."""
    rows = [("Tiền và các khoản tương đương tiền", "5.209.108.954.978", "7.534.048.703.295"),
            ("Tiền", "945.186.158.154", "830.125.906.471"),
            ("Hàng tồn kho", "13.607.211.035.291", "18.913.717.422.013"),
            ("TỔNG CỘNG TÀI SẢN", "82.791.938.275.549", "75.772.648.425.795")]
    words = []
    for i, (label, now, prior) in enumerate(rows):
        y = 100.0 + i * 16.0
        words.append(_box(250.0, y, label, 170.0))
        words.append(_box(X_NOW, y, now, 69.0))
        words.append(_box(X_PRIOR, y, prior, 69.0))
    return {0: words}


def _split_page(gap=3.8):
    """The same page with the first two figures of each row split in two, `gap` points apart.

    The grand total is deliberately left WHOLE — that is what the real document does, and it is
    why every gate downstream passes on a statement that is wrong in its detail lines.
    """
    words = []
    rows = [("Tiền và các khoản tương đương tiền", "5.209.108", "954.978", "7.534.048.703.295"),
            ("Tiền", "945.186", "158.154", "830.125.906.471"),
            ("Hàng tồn kho", "13.607.211", "035.291", "18.913.717.422.013")]
    for i, (label, head, tail, prior) in enumerate(rows):
        y = 100.0 + i * 16.0
        words.append(_box(250.0, y, label, 170.0))
        words.append(_box(X_NOW - len(tail) * 6.0 - gap, y, head, 34.0))
        words.append(_box(X_NOW, y, tail, len(tail) * 6.0))
        words.append(_box(X_PRIOR, y, prior, 69.0))
    # …and the rest of the page whole, so the left halves stay a MINORITY and are dropped
    # rather than clustering into a column of their own. Both outcomes happen in the real
    # document; this is the one that leaves the row holding the right half.
    for j, (label, now, prior) in enumerate(
            [("Các khoản phải thu ngắn hạn", "4.548.163.562.289", "3.791.905.544.739"),
             ("Tài sản ngắn hạn khác", "5.950.465.229.474", "4.092.164.983.094"),
             ("Tài sản cố định", "21.760.091.615.152", "11.224.114.750.220"),
             ("Bất động sản đầu tư", "14.859.073.309.138", "13.628.734.369.628"),
             ("Đầu tư tài chính dài hạn", "3.542.468.135.179", "1.532.383.326.600"),
             ("Lợi thế thương mại", "4.887.476.928.394", "4.803.912.193.645"),
             ("TỔNG CỘNG TÀI SẢN", "82.791.938.275.549", "75.772.648.425.795")]):
        y = 100.0 + (3 + j) * 16.0
        words.append(_box(250.0, y, label, 170.0))
        words.append(_box(X_NOW, y, now, 69.0))
        words.append(_box(X_PRIOR, y, prior, 69.0))
    return {0: words}


@pytest.fixture(scope="module")
def parser():
    return PdfParser()


def test_a_whole_reading_counts_no_split(parser):
    """The invariant every filing that parses today relies on: a clean page scores 0.

    Measured on the real corpus as well — 12 statements across VCB, ACB and BID score 0.
    """
    assert parser.split_figures(_whole_page(), WIDTH) == 0


def test_every_split_figure_is_counted(parser):
    """Three split figures on the page, three counted — and the whole total is not one."""
    assert parser.split_figures(_split_page(), WIDTH) == 3


def test_the_defect_is_real_and_silent_without_the_gate(parser):
    """The fixture must reproduce the failure, or the gate proves nothing.

    The row keeps ONE HALF of its own figure — which half depends on whether enough left halves
    line up to form a spurious column, and neither is the printed number — while the grand
    total, which the filing did not split, reads correctly. That combination is what makes
    `reconcile` and `sane` both pass on a statement whose detail lines are wrong.
    """
    page = _split_page()
    cols = parser.value_columns(page, WIDTH)
    rows = {r.key: r.values for r in parser.table_rows(page, cols)}
    assert rows["tien_va_cac_khoan_tuong_duong_tien"][0] == 954_978
    assert rows["tong_cong_tai_san"][0] == 82_791_938_275_549


def test_two_adjacent_period_figures_are_not_a_split(parser):
    """The rule may not fire on a page whose columns simply sit close together.

    Real inter-column gaps in the corpus reach 3.6pt (BID Q4-2016's balance sheet), so the join
    test carries the rest of the weight: two full figures joined leave a group of the wrong
    width and cannot be read as one number.
    """
    words = [_box(250.0, 100.0, "TỔNG CỘNG TÀI SẢN", 170.0),
             _box(470.0, 100.0, "1.558.887.407", 60.0),
             _box(473.6 + 60.0, 100.0, "1.541.259.663", 60.0)]
    assert parser.split_figures({0: words}, WIDTH) == 0


def test_a_wide_gap_is_never_a_split(parser):
    """At 4.5pt the two populations part; a page whose halves sit a column apart is untouched."""
    assert parser.split_figures(_split_page(gap=12.0), WIDTH) == 0


def test_the_gate_refuses_a_fragmented_statement():
    """`reconcile` stops it before any figure is believed, and says how many.

    A refusal is what escalates the cascade, and escalation is the whole fix: VIC Q3-2014's
    balance sheet splits 60 figures at onnx@200 and none at onnx@300.
    """
    from web_scraper.cafef_financials import FinancialsBuilder
    from web_scraper.cafef_pdf_parser import Statement, Row

    rows = [Row(label=f"line {i}", key=f"line_{i}", number="",
                values=[i * 1000]) for i in range(30)]
    st = Statement(report="balance_sheet", pages=[1], unit=1, n_columns=2, rows=rows,
                   split_figures=60)
    why = FinancialsBuilder().reconcile(st)
    assert why is not None and "fragmented" in why and "60" in why


def test_a_clean_statement_is_not_touched_by_the_gate():
    """`split_figures = 0` is the default, so nothing that reconciles today starts failing."""
    from web_scraper.cafef_pdf_parser import Statement

    assert Statement(report="balance_sheet", pages=[1], unit=1,
                     n_columns=2).split_figures == 0


# ──────────────────────────────────────────────────────────────────────────────
# _merge_split_figures — the repair, which runs BEFORE columns exist
# ──────────────────────────────────────────────────────────────────────────────

LO = WIDTH * PdfParser.VALUE_ZONE


def test_a_split_figure_is_rejoined_and_keeps_the_printed_right_edge(parser):
    """The merged box spans both halves: left x0, RIGHT x1.

    The right edge is what `value_columns` clusters on, and the printed figure's right edge is
    the right half's — taking the left box's would put the column where no column is.
    """
    words = [_box(405.7, 100.0, "5.209.108", 35.0),
             _box(436.3, 100.0, "954.978", 26.8),
             _box(540.7, 100.0, "7.534.048.703.295", 63.7)]
    out = PdfParser._merge_split_figures(words, PdfParser.Y_TOL, LO)
    assert len(out) == 2
    assert out[0][4] == "5.209.108.954.978"
    assert out[0][0] == pytest.approx(405.7 - 35.0)      # the LEFT box's x0
    assert out[0][2] == pytest.approx(436.3)             # the RIGHT box's x1
    assert out[1][4] == "7.534.048.703.295"              # untouched


def test_two_adjacent_period_figures_are_never_merged(parser):
    """BID Q4-2016's balance sheet has real columns 3.6pt apart, inside `MERGE_MAX_GAP`.

    The TAIL test is what refuses them: a continuation of a split figure always begins with a
    full three-digit group, and a fresh figure printed in Triệu VND usually does not.
    """
    words = [_box(470.0, 100.0, "1.558.887.407", 60.0),
             _box(473.6 + 60.0, 100.0, "1.541.259.663", 60.0)]
    assert PdfParser._merge_split_figures(words, PdfParser.Y_TOL, LO) == words


def test_a_wide_gap_is_never_merged(parser):
    """Beyond 4.5pt the two populations part, and everything wider is left alone."""
    words = [_box(405.7, 100.0, "5.209.108", 35.0),
             _box(436.3 + 20.0, 100.0, "954.978", 26.8)]
    assert PdfParser._merge_split_figures(words, PdfParser.Y_TOL, LO) == words


def test_the_label_half_of_the_page_is_left_alone(parser):
    """⚠️ Confined to the VALUE ZONE — the half the measurement covers.

    A label can hold two adjacent numbers ("Điều 4", a note index), and nothing has measured
    what this rule would do to them. Shipping wider than the measurement is how a rule with 0
    false positives acquires some.
    """
    words = [_box(120.0, 100.0, "12", 8.0), _box(124.0 + 8.0, 100.0, "345", 12.0)]
    assert PdfParser._merge_split_figures(words, PdfParser.Y_TOL, LO) == words


def test_boxes_on_different_lines_are_not_neighbours(parser):
    """`Y_TOL` decides what a line is; a figure below another is not its continuation."""
    words = [_box(405.7, 100.0, "5.209.108", 35.0),
             _box(436.3, 100.0 + 40.0, "954.978", 26.8)]
    assert PdfParser._merge_split_figures(words, PdfParser.Y_TOL, LO) == words


def test_a_three_box_split_merges_the_pair_it_can_prove(parser):
    """A partial repair is a repair: whatever is left still fails `split_figures` and escalates.

    VIC Q3-2014 is exactly this — the merge takes its balance sheet from 60 split figures to 6,
    and the 6 are why the cascade still escalates to onnx@300 rather than accepting at layer 1.
    """
    words = [_box(380.0, 100.0, "3.193", 20.0),
             _box(408.0, 100.0, "352", 14.0),
             _box(436.3, 100.0, "522.499", 26.0)]
    out = PdfParser._merge_split_figures(words, PdfParser.Y_TOL, LO)
    assert len(out) < len(words)


def test_the_splitters_own_pieces_would_be_rejoined_at_a_narrow_enough_box(parser):
    """⚠️ ORDER, and the reason is a RATIO rather than a constant.

    `_split_number_runs` apportions a box holding two figures by CHARACTER OFFSET, so the gap it
    leaves is the width of the separator character — `box width / len(text)`. At 130pt over 23
    characters that is 5.7pt, safely outside `MERGE_MAX_GAP`; at 100pt it is 4.3pt and INSIDE
    it, and a merge running afterwards joins the splitter's own pieces straight back together.
    Merging FIRST cannot make that mistake at any width, which is why `_ocr_page` does.

    ⚠️ I wrote this test the other way round first — asserting the pieces are contiguous — and
    the measurement said 5.7pt. The claim was wrong and the ordering is right anyway.
    """
    wide = PdfParser._split_number_runs(
        [_box(560.0, 100.0, "135.272.610 126.501.216", 130.0)], False)
    assert [w[4] for w in wide] == ["135.272.610", "126.501.216"]
    assert wide[1][0] - wide[0][2] == pytest.approx(130.0 / 23, abs=0.05)
    assert len(PdfParser._merge_split_figures(wide, PdfParser.Y_TOL, LO)) == 2

    narrow = PdfParser._split_number_runs(
        [_box(560.0, 100.0, "135.272.610 126.501.216", 100.0)], False)
    assert narrow[1][0] - narrow[0][2] < PdfParser.MERGE_MAX_GAP
    assert len(PdfParser._merge_split_figures(narrow, PdfParser.Y_TOL, LO)) == 1, \
        "at this width the splitter's pieces WOULD be re-joined — hence merge-then-split"
