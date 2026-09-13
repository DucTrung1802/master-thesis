"""`GTT-1` — a balance sheet's two grand totals from its two EQUAL LARGEST figures.

⚠️ **TPB PRINTS ITS TOTALS ON THE SECTION HEADERS, GLUED TO THE PAGE TITLE**, so no label names
`TỔNG TÀI SẢN` and every layer refused `no total assets` with both figures on the page. Pinned
without a PDF, on TPB Q1-2017's own two figures, and on each of the four locks.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import BALANCE_SHEET, Row, Statement

M = 1_000_000
TOTAL = 104_791_710 * M


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def _sheet(figures):
    rows = [Row(label=k, key=k, number=None, values=[v, None]) for k, v in figures]
    return Statement(report=BALANCE_SHEET, pages=[0], unit=M, n_columns=2, rows=rows)


def tpb_q1_2017(total_on_b=TOTAL):
    lines = [("ngan_hang_tmcp_tien_phong_bao_cao_tai_chinh_quy_i_2017_bang", TOTAL),
             ("tien_mat_vang_bac_da_quy", 784_449 * M), ("tien_gui_tai_nhnn", 1_162_538 * M),
             ("cho_vay_khach_hang", 54_761_405 * M), ("chung_khoan_dau_tu", 30_112_004 * M),
             ("tai_san_co_khac", 3_214_551 * M),
             ("no_phai_tra_va_von_chu_so_huu", total_on_b),
             ("tien_gui_cua_khach_hang", 70_023_118 * M), ("von_cua_to_chuc_tin_dung", 5_837_294 * M)]
    return _sheet(lines)


def test_the_measured_case_is_recovered(b):
    row = {}
    b._twin_totals(tpb_q1_2017(), row, "bank")
    assert row == {"tong_tai_san": TOTAL, "tong_no_phai_tra_va_von_chu_so_huu": TOTAL}


def test_two_different_largest_figures_recover_nothing(b):
    row = {}
    b._twin_totals(tpb_q1_2017(total_on_b=TOTAL - M), row, "bank")
    assert row == {}


def test_a_figure_printed_three_times_recovers_nothing(b):
    st = tpb_q1_2017()
    st.rows.append(Row(label="x", key="x", number=None, values=[TOTAL, None]))
    row = {}
    b._twin_totals(st, row, "bank")
    assert row == {}


def test_a_total_repeated_on_the_next_line_is_not_two_sides(b):
    st = _sheet([("a", TOTAL), ("b", TOTAL)] + [(f"l{i}", (i + 1) * M) for i in range(8)])
    row = {}
    b._twin_totals(st, row, "bank")
    assert row == {}


def test_a_total_already_mapped_to_another_figure_is_left_alone(b):
    row = {"tong_tai_san": 99 * M}
    b._twin_totals(tpb_q1_2017(), row, "bank")
    assert row == {"tong_tai_san": 99 * M}


def test_both_totals_mapped_is_left_alone(b):
    row = {"tong_tai_san": 1, "tong_no_phai_tra_va_von_chu_so_huu": 1}
    b._twin_totals(tpb_q1_2017(), row, "bank")
    assert row == {"tong_tai_san": 1, "tong_no_phai_tra_va_von_chu_so_huu": 1}


def test_the_corp_chart_gets_its_own_columns(b):
    row = {}
    b._twin_totals(tpb_q1_2017(), row, "corp")
    assert row == {"tong_cong_tai_san": TOTAL, "tong_cong_nguon_von": TOTAL}


def test_the_flag_is_off_by_default_and_a_widening():
    assert ParseLayer("x", "onnx", 200).twin_totals is False
    assert not ParseLayer("x", "onnx", 200, twin_totals=True).is_strict
    assert any(l.twin_totals for l in FinancialsBuilder.LAYERS)
