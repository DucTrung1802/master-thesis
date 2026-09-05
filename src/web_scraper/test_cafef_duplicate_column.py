"""`DPC-1` — a Q1 income statement prints every figure TWICE, and both readings have to agree.

"Quý I" is 1 Jan – 31 Mar and "Lũy kế từ đầu năm đến cuối quý" is 1 Jan – 31 Mar. So a Q1
filing has four columns and two quantities, and prints each figure twice for this year and
twice for last: FPT's Q1-2015 prints `1.235 / 1.051 / 1.235 / 1.051` across its four columns
for basic EPS, and every other line the same way.

That matters because the page is a LANDSCAPE rotated scan the recogniser damages badly — and
in a DIFFERENT set of cells in each column. At 200 dpi column 0 reads 16 of 23 known figures
and column 2 reads 15; at 500 it is 16 and 18. **Neither is right on its own at 200, 300, 400,
500 or 600 dpi.**

⚠️ **WHAT MAKES AGREEMENT EVIDENCE IS THAT IT WAS MEASURED, NOT THAT IT SOUNDS RIGHT.** Across
those five DPIs, **85 of 86 agreeing pairs are the figure the filing prints**. The one
exception is a leading `4` read as `1` in BOTH columns at 300 dpi — a confusion the recogniser
makes the same way twice, which is the failure mode this cannot catch and does not claim to.

So under a `duplicate_period` layer `_first_value` believes only what both columns say, drops
the rest (§5 rule 2 — at most one of two contradicting readings is right and nothing on the
page says which), and `_resolve_duplicate_identity` then lets the operating-profit identity
choose between the two readings of any term it contradicted.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser, Row, Statement)

# FPT Q1-2015, read off the rendered page at 430 dpi. Column 0 == column 2 in the filing;
# what differs below is what the OCR made of them at `onnx@200+joinlost`.
GOP = 1_689_717_875_941        # 20 · lợi nhuận gộp                     — both columns agree
TC_EXP = 120_816_576_677       # 22 · chi phí tài chính                 — both columns agree
QLDN = 506_656_453_189         # 25 · chi phí quản lý doanh nghiệp      — both columns agree
TC_INC_0, TC_INC_2 = 89_420_081_565, 89_420_051_565        # 21 · truth is column 2
BAN_HANG_0, BAN_HANG_2 = 522_721_917_777, 522_723_919_797  # 24 · truth is column 2
THUAN_0, THUAN_2 = 628_940_977_843, 618_940_977_843        # 30 · truth is column 0


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def st(values_by_row, report=INCOME_STATEMENT, dup=2, n_columns=4):
    rows = [Row(label=k, key=k, number="", values=list(v)) for k, v in values_by_row]
    return Statement(report=report, pages=[1], unit=1, n_columns=n_columns, rows=rows,
                     quarter_column=True, duplicate_column=dup)


# ── what `_first_value` does with a duplicate column ─────────────────────────────────────

def test_two_readings_that_agree_are_believed():
    s = st([("a", [7, 3, 7, 3])])
    assert s._first_value(s.rows[0].values) == 7


def test_two_readings_that_disagree_are_DROPPED_not_guessed():
    """⚠️ At most one is right and nothing on the page says which — §5 rule 2."""
    s = st([("a", [7, 3, 8, 3])])
    assert s._first_value(s.rows[0].values) is None


def test_an_absent_duplicate_cell_leaves_the_old_behaviour_alone():
    """A row the duplicate column did not read cannot contradict anything."""
    s = st([("a", [7, 3, None, 3])])
    assert s._first_value(s.rows[0].values) == 7


def test_a_statement_with_no_duplicate_column_is_untouched():
    """`duplicate_column` is None on every statement no layer measured it on, and there
    `_first_value` is exactly what it has always been — including its fall-through."""
    s = st([("a", [None, 3, 8, 3])], dup=None)
    assert s._first_value(s.rows[0].values) == 3


def test_duplicate_pair_returns_both_readings_only_where_they_disagree():
    s = st([("agree", [7, 3, 7, 3]), ("differ", [7, 3, 8, 3]), ("absent", [7, 3, None, 3])])
    assert s.duplicate_pair(s.rows[0].values) is None
    assert s.duplicate_pair(s.rows[1].values) == (7, 8)
    assert s.duplicate_pair(s.rows[2].values) is None


# ── detecting the duplicate column ───────────────────────────────────────────────────────

HEADER_Q1 = "CONG TY CO PHAN FPT\nBAO CAO KET QUA HOAT DONG KINH DOANH\nQuy I  Luy ke tu dau nam"
HEADER_Q3 = "CONG TY CO PHAN FPT\nBAO CAO KET QUA HOAT DONG KINH DOANH\nQuy III  Luy ke tu dau nam"


def repeating_rows(n=10):
    return [Row(label=f"r{i}", key=f"r{i}", number="",
                values=[100 + i, 50 + i, 100 + i, 50 + i]) for i in range(n)]


def test_a_q1_with_repeating_columns_is_detected():
    p = PdfParser(engine="onnx")
    p.set_duplicate_period(True)
    assert p._duplicate_period(INCOME_STATEMENT, repeating_rows(), True, HEADER_Q1) == 2


def test_a_q3_is_REFUSED_because_the_two_columns_are_different_quantities():
    """⚠️ In Q2 the cumulative column is six months and in Q3 nine.

    `_prints_quarter_column` cannot tell those apart — it only says both KINDS of column are
    present — so the quarter number is required as well, and treating a Q3 this way would drop
    every cell of a sound statement.
    """
    p = PdfParser(engine="onnx")
    p.set_duplicate_period(True)
    assert p._duplicate_period(INCOME_STATEMENT, repeating_rows(), True, HEADER_Q3) is None


def test_the_header_must_say_the_filing_prints_BOTH_kinds_of_column():
    """Without `quarter_column` a four-column reading is far more likely to be an
    over-segmented two-column one, which is what `_first_value`'s fall-through is for."""
    p = PdfParser(engine="onnx")
    p.set_duplicate_period(True)
    assert p._duplicate_period(INCOME_STATEMENT, repeating_rows(), False, HEADER_Q1) is None


def test_columns_that_do_not_repeat_are_not_a_duplicate():
    """A comparative column agrees with the current one on 0-2 rows, never on eight."""
    p = PdfParser(engine="onnx")
    p.set_duplicate_period(True)
    rows = [Row(label=f"r{i}", key=f"r{i}", number="", values=[100 + i, 50 + i, 7 + i, 3 + i])
            for i in range(10)]
    assert p._duplicate_period(INCOME_STATEMENT, rows, True, HEADER_Q1) is None


def test_too_few_agreeing_rows_proves_nothing():
    """A ratio over three rows is not a measurement — `DUP_MIN_AGREE_ROWS`."""
    p = PdfParser(engine="onnx")
    p.set_duplicate_period(True)
    rows = [Row(label=f"r{i}", key=f"r{i}", number="", values=[i, i, i, i]) for i in range(3)]
    assert p._duplicate_period(INCOME_STATEMENT, rows, True, HEADER_Q1) is None


def test_the_flag_is_required_and_off_by_default():
    """Off, `Statement.duplicate_column` stays None and nothing in the parser changes."""
    p = PdfParser(engine="onnx")
    assert p.duplicate_period is False
    assert p._duplicate_period(INCOME_STATEMENT, repeating_rows(), True, HEADER_Q1) is None


def test_only_an_income_statement_can_carry_one():
    """A balance sheet is a stock at a date and a cash flow is cumulative from 1 January —
    neither prints the quarter beside the year to date."""
    p = PdfParser(engine="onnx")
    p.set_duplicate_period(True)
    for r in (BALANCE_SHEET, CASH_FLOW):
        assert p._duplicate_period(r, repeating_rows(), True, HEADER_Q1) is None


# ── the identity as arbiter ──────────────────────────────────────────────────────────────

def fpt_q1_2015(thuan_first=THUAN_0, thuan_second=THUAN_2):
    """The six operating-profit terms of FPT Q1-2015 as `onnx@200+joinlost` read them."""
    return st([
        ("loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu", [GOP, 0, GOP, 0]),
        ("doanh_thu_hoat_dong_tai_chinh", [TC_INC_0, 0, TC_INC_2, 0]),
        ("chi_phi_tai_chinh", [TC_EXP, 0, TC_EXP, 0]),
        ("chi_phi_ban_hang", [BAN_HANG_0, 0, BAN_HANG_2, 0]),
        ("chi_phi_quan_ly_doanh_nghiep", [QLDN, 0, QLDN, 0]),
        ("loi_nhuan_thuan_tu_hoat_dong_kinh_doanh", [thuan_first, 0, thuan_second, 0]),
    ])


def test_the_identity_picks_the_one_assignment_that_closes(b):
    """⚠️ EIGHT candidates over 13-digit figures, and exactly one closes to the đồng:

        1,689,717,875,941 + 89,420,051,565 - 120,816,576,677
                         - 522,723,919,797 - 506,656,453,189 = 628,940,977,843

    ⚠️ And note it is NOT "prefer the duplicate column": two of the three values it keeps come
    from column 2 and the third from column 0.
    """
    s = fpt_q1_2015()
    mapped = {"5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": GOP,
              "8_chi_phi_tai_chinh": TC_EXP,
              "10_chi_phi_quan_ly_doanh_nghiep": QLDN}
    b._resolve_duplicate_identity(s, mapped, "corp")
    assert mapped["7_doanh_thu_hoat_dong_tai_chinh"] == TC_INC_2
    assert mapped["9_chi_phi_ban_hang"] == BAN_HANG_2
    assert mapped["11_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh"] == THUAN_0
    assert b._operating_profit_identity(mapped) is None


def test_nothing_is_written_when_no_assignment_closes(b):
    """A statement whose damage the identity cannot undo keeps its dropped cells."""
    s = fpt_q1_2015(thuan_first=THUAN_0 + 5_000_000_000, thuan_second=THUAN_2)
    mapped = {"5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": GOP,
              "8_chi_phi_tai_chinh": TC_EXP,
              "10_chi_phi_quan_ly_doanh_nghiep": QLDN}
    before = dict(mapped)
    b._resolve_duplicate_identity(s, mapped, "corp")
    assert mapped == before


def test_two_assignments_that_both_close_settle_nothing(b):
    """⚠️ EXACTLY ONE, never the best. Two closing means the identity cannot tell them apart,
    which is the same answer as none closing."""
    s = st([
        ("loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu", [100, 0, 100, 0]),
        # both readings of the pair below reach the same operating profit
        ("doanh_thu_hoat_dong_tai_chinh", [10, 0, 20, 0]),
        ("chi_phi_tai_chinh", [0, 0, 0, 0]),
        ("chi_phi_ban_hang", [10, 0, 20, 0]),
        ("chi_phi_quan_ly_doanh_nghiep", [0, 0, 0, 0]),
        ("loi_nhuan_thuan_tu_hoat_dong_kinh_doanh", [100, 0, 100, 0]),
    ])
    mapped = {"5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": 100,
              "8_chi_phi_tai_chinh": 0, "10_chi_phi_quan_ly_doanh_nghiep": 0,
              "11_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh": 100}
    before = dict(mapped)
    b._resolve_duplicate_identity(s, mapped, "corp")
    assert mapped == before


def test_a_term_absent_for_an_ORDINARY_reason_is_not_decided(b):
    """A cell the parse never produced leaves the identity unanswerable, exactly as today —
    only a cell the duplicate column CONTRADICTED may be decided here."""
    s = fpt_q1_2015()
    s.rows = [r for r in s.rows if r.key != "chi_phi_ban_hang"]
    mapped = {"5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": GOP,
              "8_chi_phi_tai_chinh": TC_EXP,
              "10_chi_phi_quan_ly_doanh_nghiep": QLDN}
    before = dict(mapped)
    b._resolve_duplicate_identity(s, mapped, "corp")
    assert mapped == before


def test_nothing_happens_without_a_duplicate_column(b):
    s = fpt_q1_2015()
    s.duplicate_column = None
    mapped = {"5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": GOP}
    before = dict(mapped)
    b._resolve_duplicate_identity(s, mapped, "corp")
    assert mapped == before


def test_the_search_is_bounded(b):
    """⚠️ A unique exact solution over a wide enough space is arithmetic, not evidence."""
    assert FinancialsBuilder.DUP_MAX_UNKNOWNS <= 6


# ── the cascade ──────────────────────────────────────────────────────────────────────────

def test_every_dup_layer_is_late():
    """Dropping a contradicted cell makes a statement STRICTLY thinner, so a filing that
    already parses must never reach these."""
    layers = FinancialsBuilder.LAYERS
    block = [i for i, l in enumerate(layers, 1) if l.duplicate_period]
    strict = [i for i, l in enumerate(layers, 1) if l.is_strict]
    assert block
    assert min(block) > max(strict)


def test_the_flag_is_a_widening_and_is_counted_as_one():
    assert not ParseLayer("x", "onnx", 200, duplicate_period=True).is_strict


def test_the_flag_is_a_parse_key_but_costs_no_ocr_pass():
    """⚠️ It changes the VALUES a row yields, so a layer carrying it must never be served a
    parse taken without it — the defect `reseat_words` cost a run to find. It compares figures
    `scan` has already read, so it cannot change a recognised character."""
    a = ParseLayer("a", "onnx", 200)
    d = ParseLayer("d", "onnx", 200, duplicate_period=True)
    assert parse_key(a) != parse_key(d)
    assert ocr_key(a) == ocr_key(d)
