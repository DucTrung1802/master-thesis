"""The "Mã số" item-code column — pinned without a PDF, a network or an OCR engine.

The defect: VAS form B01-DN prints `Chỉ tiêu | Mã số | Thuyết minh | Số cuối kỳ | Số đầu năm`,
and that second column holds the filing's own item numbering — 100, 110, 111, 270, 300, 440.
`value_columns` separates a note-reference column from a period column by MEDIAN DIGIT COUNT
(`NOTE_MAX_DIGITS = 2`), and a three-digit code sits exactly in the overlap between a note (1-2
digits) and a figure (4-14). So the code column survives as column 0 and `_first_value` returns
every row's item code as its figure.

Measured on VIC Q3-2014: the code column clusters at x=279.7 of a 595pt page — 47%, inside the
right-60% value zone — 86 numbers, 79 three-digit and 7 four-digit. Total assets read 270 and
total resources 440, so `reconcile` refused the whole statement with "assets != liabilities +
equity" and the quarter was recorded `missing`.

⚠️ AND ON THE OTHER TWO STATEMENTS THE GATES DO NOT CATCH IT. An income statement only has to
present a PBT line and `50` is one; a cash flow only has to present a closing balance and `70`
is one. Both are then left to `sane`, which fails open on a ticker with no accepted history —
which is every non-bank ticker on its first run. VIC Q1-2011 is on disk with
`a_tai_san_ngan_han = 100` and `i_no_ngan_han = 310` written as figures. That is why the drop
is in the default path and not a late cascade layer.

The geometry below is VIC Q3-2014's own, read off page 4 at onnx@200.
"""
import pytest

from web_scraper.cafef_pdf_parser import PdfParser


WIDTH = 595.44                  # the filing's page width; VALUE_ZONE puts the lo edge at 238.2

# Right edges measured on the real page. The code column at 281 is INSIDE the value zone, which
# is the whole reason this defect exists.
X_CODE, X_NOW, X_PRIOR = 281.2, 436.5, 540.4
CODE_HEADER = (261.0, 286.2)    # the "Mã số" box: the code column's right edge sits inside it


def _box(x1, y, text, w=60.0):
    """A word box in the shape `value_columns` reads: (x0, y0, x1, y1, text, …)."""
    return (x1 - w, y, x1, y + 11.0, text, 0, 0, 0)


ROWS = [("A. TÀI SẢN NGẮN HẠN", "100", "36.550.263.468.338", "39.844.677.687.769"),
        ("Tiền và các khoản tương đương tiền", "110", "5.209.108.954.978", "7.534.048.703.295"),
        ("Tiền", "111", "945.186.158.154", "830.125.906.471"),
        ("Các khoản tương đương tiền", "112", "4.263.922.796.824", "6.703.850.319.014"),
        ("II. Các khoản đầu tư tài chính ngắn hạn", "120", "7.235.314.686.306", "5.512.841.034.628"),
        ("III. Các khoản phải thu ngắn hạn", "130", "4.548.163.562.289", "3.791.905.544.739"),
        ("IV. Hàng tồn kho", "140", "13.607.211.035.291", "18.913.717.422.013"),
        ("B. TÀI SẢN DÀI HẠN", "200", "46.241.674.807.211", "35.927.970.738.026"),
        ("II. Tài sản cố định", "220", "21.760.091.615.152", "11.224.114.750.220"),
        ("IV. Bất động sản đầu tư", "240", "14.859.073.309.138", "13.628.734.369.628"),
        ("TỔNG CỘNG TÀI SẢN", "270", "82.791.938.275.549", "75.772.648.425.795"),
        ("A. NỢ PHẢI TRẢ", "300", "55.496.405.803.121", "57.156.105.584.507"),
        ("I. Nợ ngắn hạn", "310", "18.675.297.482.344", "26.675.265.408.188"),
        ("II. Nợ dài hạn", "330", "36.821.108.320.777", "30.480.840.176.319"),
        ("B. VỐN CHỦ SỞ HỮU", "400", "18.426.424.611.027", "14.471.837.198.264"),
        ("TỔNG CỘNG NGUỒN VỐN", "440", "82.791.938.275.549", "75.772.648.425.795")]


def _page(header=True, codes=True, header_span=CODE_HEADER, header_text="Mã số"):
    """One B01-DN statement page, rows 16pt apart.

    `header=False` is an unreadable heading; `codes=False` is a filing whose codes OCR merged
    into the labels instead — VIC's own income statement reads "02 Các khoản giảm trừ" — so
    there is no code column for a heading to sit over. `header_text` is what the recogniser
    returned for the heading box, which is not always the heading alone.
    """
    words = []
    if header:
        words += [_box(115.9, 84.0, "TÀI SẢN", 34.5),
                  _box(header_span[1], 84.0, header_text, header_span[1] - header_span[0]),
                  _box(333.4, 84.0, "Ghi chú", 31.0),
                  _box(X_NOW, 84.0, "30/9/2014", 36.0),
                  _box(X_PRIOR, 84.0, "01/01/2014", 40.0)]
    for i, (label, code, now, prior) in enumerate(ROWS):
        y = 100.0 + i * 16.0
        words.append(_box(250.0, y, label, 170.0))
        if codes:
            words.append(_box(X_CODE, y, code, 16.0))
        words.append(_box(X_NOW, y, now, 69.0))
        words.append(_box(X_PRIOR, y, prior, 69.0))
    return {0: words}


@pytest.fixture(scope="module")
def parser():
    return PdfParser()


def test_the_defect_is_real_when_the_heading_is_unreadable(parser):
    """The fixture must reproduce the defect, or nothing else in this file proves anything.

    With no heading to name it, the code column survives the note-digit filter — its median is
    3 digits and `NOTE_MAX_DIGITS` is 2 — and becomes column 0.
    """
    page = _page(header=False)
    cols = parser.value_columns(page, WIDTH)
    assert len(cols) == 3
    assert min(cols) == pytest.approx(X_CODE, abs=PdfParser.EDGE_TOL)
    rows = {r.key: r.values for r in parser.table_rows(page, cols)}
    # this is exactly what `reconcile` saw on VIC Q3-2014: 270 against 440
    assert rows["tong_cong_tai_san"][0] == 270
    assert rows["tong_cong_nguon_von"][0] == 440


def test_the_heading_names_the_code_column_and_it_is_dropped(parser):
    """With "Mã số" printed above it, the column is the filing's numbering and not a period."""
    page = _page()
    cols = parser.value_columns(page, WIDTH)
    assert len(cols) == 2
    assert min(cols) == pytest.approx(X_NOW, abs=PdfParser.EDGE_TOL)
    rows = {r.key: r.values for r in parser.table_rows(page, cols)}
    assert rows["tong_cong_tai_san"][0] == 82_791_938_275_549
    assert rows["tong_cong_nguon_von"][0] == 82_791_938_275_549
    # and the figures are the filing's own: A + B == the grand total, to the đồng. The
    # short-term line keys off the header row, which prints no figure of its own and so is
    # still carried when the first figures arrive — that is `table_rows` behaving normally
    # and is not what this file is about.
    assert (rows["tai_san_ma_so_ghi_chu_a_tai_san_ngan_han"][0]
            + rows["tai_san_dai_han"][0] == rows["tong_cong_tai_san"][0])
    assert (rows["no_ngan_han"][0] + rows["no_dai_han"][0] == rows["no_phai_tra"][0])


@pytest.mark.parametrize("heading", ["Mã số", "Mã sô", "Mãsố", "MÃ SỐ", "Ma so",
                                    # ⚠️ `MSO-4`, 2026-09-04: at 300 dpi FPT's Q1-2016 balance
                                    # sheet returns the tone-marked vowel as a DIFFERENT LETTER
                                    # — `moso` on page 2, `miso` on page 3. One substitution in
                                    # a four-character needle is 0.750 against a bar of 0.80,
                                    # and there is no threshold between them: 0.75 IS one
                                    # substitution, so admitting it by score would admit every
                                    # four-character box sharing three characters. The shape is
                                    # named instead — see `CODE_HEADER_RE`.
                                    "Mô số", "Mi số"])
def test_the_heading_is_matched_through_ocr_damage(parser, heading):
    """Tone marks are the first thing OCR drops, so the match is on the normalised box."""
    page = _page()
    page[0] = [w if w[4] != "Mã số" else (w[0], w[1], w[2], w[3], heading, 0, 0, 0)
               for w in page[0]]
    assert len(parser.value_columns(page, WIDTH)) == 2


def test_a_heading_over_nothing_drops_nothing(parser):
    """Where OCR merged the codes into the labels there is no column under the heading.

    VIC's own income statement is this case — "02 Các khoản giảm trừ" — and its two period
    columns must survive untouched.
    """
    assert (parser.value_columns(_page(codes=False), WIDTH)
            == parser.value_columns(_page(header=False, codes=False), WIDTH))


def test_only_the_leftmost_column_can_be_taken(parser):
    """A heading that lands over a period column may not take it.

    "Mã số" precedes "Thuyết minh" and both period columns on the form, so a candidate that is
    not the leftmost is a mis-read heading and is refused — the failure mode is then a
    statement that fails exactly as it does today, never one missing a figure column.
    """
    page = _page(header_span=(X_NOW - 20.0, X_NOW + 5.0))
    assert len(parser.value_columns(page, WIDTH)) == 3


def test_a_page_that_prints_no_such_heading_is_left_alone(parser):
    """Every filing that parses today reads its heading or does not print one; neither moves.

    This is the invariant the real-corpus regression measures — the bank statements of VCB, ACB
    and BID re-detect the identical column set with the drop in place.
    """
    bank = _page(header=False, codes=False)
    assert parser._code_column(parser.value_columns(bank, WIDTH), bank) is None


def test_the_last_column_is_never_dropped(parser):
    """Dropping the only column helps nobody: the caller would parse an empty statement."""
    assert parser._code_column([X_CODE], _page()) is None


# ── The heading is not always alone in its box — BSR FY-2019 ─────────────────
#
# The form sets "Mã số" and "Thuyết minh" on ONE baseline, and the recogniser is as willing to
# merge two HEADER words as any others. BSR's FY-2019 consolidated balance sheet comes back
# with `Mã số minh` on page 7 (the "minh" of "Thuyết minh" swept in) and, on page 8 of the same
# filing, the opposite — `Mã` and `số` as two boxes. Whole-box scoring gives 0.667 for all
# three, under the 0.80 bar, so the code column survived and `TỔNG CỘNG TÀI SẢN` read 270.
MERGED_HEADINGS = ["Mã số minh", "Mã số Thuyết", "Mã số  minh"]


@pytest.mark.parametrize("heading", MERGED_HEADINGS)
def test_a_heading_box_that_swallowed_the_next_header_still_names_the_column(parser, heading):
    """⚠️ Asserted through `value_columns`, which is where the drop happens — calling
    `_code_column` on its RESULT asks whether the already-filtered leftmost is the code
    column, which is a different question and answers None on a page that worked."""
    page = _page(header_text=heading)
    cols = parser.value_columns(page, WIDTH)
    assert len(cols) == 2, f"{heading!r} left the item-code column in place"
    rows = {r.key: r.values for r in parser.table_rows(page, cols)}
    assert rows["tong_cong_tai_san"][0] == 82_791_938_275_549
    assert rows["tong_cong_nguon_von"][0] == 82_791_938_275_549


@pytest.mark.parametrize("heading, whole, head", [
    # The form code printed in the same band of every B01-DN filing. It begins with the same
    # two syllables and is still refused: 0.50 whole, 0.75 on its leading text.
    ("MẪU SỐ B 01-DN/HN", 0.500, 0.750),
    # A prose label that merely CONTAINS the phrase scores 0.00 on its head — the head is
    # scored, never searched for, which is what keeps containment out.
    ("Chỉ tiêu và Mã số", 0.471, 0.000),
])
def test_a_box_that_only_resembles_the_heading_is_still_refused(parser, heading, whole, head):
    from difflib import SequenceMatcher

    ns = PdfParser.norm(heading).replace(" ", "")
    want = PdfParser.CODE_HEADER_NS
    assert SequenceMatcher(None, want, ns).ratio() == pytest.approx(whole, abs=0.001)
    assert SequenceMatcher(None, want, ns[:len(want)]).ratio() == pytest.approx(head, abs=0.001)
    assert max(whole, head) < PdfParser.CODE_HEADER_MATCH
    assert len(parser.value_columns(_page(header_text=heading), WIDTH)) == 3


def test_a_substituted_vowel_is_admitted_by_SHAPE_and_not_by_a_LOWER_BAR(parser):
    """⚠️ `MSO-4` — WHY THIS IS A REGEX AND NOT A SMALLER NUMBER.

    `SequenceMatcher` scores one substitution in a four-character needle at exactly 0.750, so
    every box below scores the same and only some of them are the heading. A threshold that
    admitted `moso` would admit `mxso`, `xaso` and `masx` alike; naming the shape — `m`, `s`,
    `o` in place, SAME LENGTH, one character between — separates them, and it is the CONSONANT
    skeleton because the character OCR damages is the tone-marked vowel.
    """
    from difflib import SequenceMatcher

    want = PdfParser.CODE_HEADER_NS
    for ns in ("moso", "miso", "muso"):
        assert SequenceMatcher(None, want, ns).ratio() == pytest.approx(0.750, abs=0.001)
        assert PdfParser.CODE_HEADER_RE.fullmatch(ns), ns
    for ns in ("xaso", "masx", "mas", "masoo"):
        assert not PdfParser.CODE_HEADER_RE.fullmatch(ns), ns


def test_the_form_code_printed_in_the_same_band_is_STILL_refused(parser):
    """⚠️ THE IMPOSTOR THE WIDENING MUST NOT REACH, and it is why the rule is SAME LENGTH.
    `MẪU SỐ B 01-DN/HN` is printed in the header band of every VAS form; it normalises to
    `mausob01dnhn`, whose score is **0.750** — the same score `moso` gets. Twelve characters
    cannot match a four-character shape, whole or on its head (`maus`), so the shape rule
    separates the two where a threshold could not."""
    ns = PdfParser.norm("MẪU SỐ B 01-DN/HN").replace(" ", "")
    assert not PdfParser.CODE_HEADER_RE.fullmatch(ns)
    assert not PdfParser.CODE_HEADER_RE.fullmatch(ns[:len(PdfParser.CODE_HEADER_NS)])
    # and end to end: the column is NOT dropped, so all three columns survive
    assert len(parser.value_columns(_page(header_text="MẪU SỐ B 01-DN/HN"), WIDTH)) == 3


def test_conditions_2_and_3_still_gate_a_shape_match(parser):
    """⚠️ THE SHAPE RULE WIDENS CONDITION 1 AND NOTHING ELSE. A heading that reads `Mô số` over
    NOTHING still drops nothing, and one that reaches past the leftmost column still cannot
    take it — which is where the real protection has always been."""
    # a heading whose span sits nowhere near a detected column
    assert len(parser.value_columns(_page(header_span=(60.0, 90.0),
                                          header_text="Mô số"), WIDTH)) == 3
    # …and one over the SECOND column rather than the first
    assert len(parser.value_columns(_page(header_span=(X_NOW - 12, X_NOW + 12),
                                          header_text="Mô số"), WIDTH)) == 3
