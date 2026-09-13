"""`BIS-1` — a bank income statement's net lines against the two components printed above each.

⚠️ **TPB Q3-2019 WAS WRITTEN WITH ITS SERVICE LINE TAKEN FROM THE FOREIGN-EXCHANGE ROW BELOW IT**
(`onnx@200+sandwich`, 2026-09-13): `ii` = -48,307 m while the same reading's row dump prints `3`
255,936, `4` -60,757 and `II` 195,179. `OP_IDENTITY`'s bank entry is `XI = IX + X`, so nothing
looked at the lines PBT is built from. Pinned without a PDF: the method on the measured figures,
and one statement through `reconcile` so the gate is proven to be CALLED.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder as F
from web_scraper.cafef_pdf_parser import INCOME_STATEMENT, Row, Statement

M = 1_000_000
I, L1, L2 = ("i_thu_nhap_lai_thuan", "1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu",
             "2_chi_phi_lai_va_cac_chi_phi_tuong_tu")
II, L3, L4 = ("ii_lai_lo_thuan_tu_hoat_dong_dich_vu", "3_thu_nhap_tu_hoat_dong_dich_vu",
              "4_chi_phi_hoat_dong_dich_vu")
PBT, OP, PROV = ("xi_tong_loi_nhuan_truoc_thue",
                 "ix_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truoc_chi_phi_du_phong_rui_ro_tin_dung",
                 "x_chi_phi_du_phong_rui_ro_tin_dung")


@pytest.fixture
def b():
    return F(logger=None)


def tpb_q3_2019(service_net):
    """TPB Q3-2019's own figures in đồng; `service_net` is what the reading put in column II."""
    return {L1: 2_356_118 * M, L2: -1_236_613 * M, I: 1_119_505 * M,
            L3: 255_936 * M, L4: -60_757 * M, II: service_net * M,
            OP: 659_419 * M, PROV: -70_227 * M, PBT: 589_192 * M}


def test_the_measured_case_is_refused(b):
    why = b._bank_net_lines(tpb_q3_2019(-48_307), M)
    assert why and why.startswith(II)


def test_the_printed_figure_passes(b):
    assert b._bank_net_lines(tpb_q3_2019(195_179), M) is None


def test_an_expense_stored_positive_still_closes(b):
    """CTG Q3-2011 stores its interest expense unbracketed: 14,866,761 and 9,509,774 give 5,356,987."""
    mapped = {L1: 14_866_761 * M, L2: 9_509_774 * M, I: 5_356_987 * M}
    assert b._bank_net_lines(mapped, M) is None


def test_the_filings_rounding_is_not_a_defect(b):
    mapped = {L1: 18_104_035 * M, L2: -9_069_714 * M, I: 9_034_322 * M}      # one unit off
    assert b._bank_net_lines(mapped, M) is None


def test_a_misread_digit_is_refused(b):
    """VCB Q1-2020 on disk: 18,104,035 - 9,069,714 = 9,034,321 against a printed 9,034,121."""
    mapped = {L1: 18_104_035 * M, L2: -9_069_714 * M, I: 9_034_121 * M}
    assert b._bank_net_lines(mapped, M)


def test_it_abstains_when_a_term_is_unmapped(b):
    mapped = tpb_q3_2019(-48_307)
    del mapped[L3]
    assert b._bank_net_lines(mapped, M) is None


def test_it_abstains_on_a_corp_chart(b):
    assert b._bank_net_lines({"5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": 1,
                              "11_loi_nhuan_thuan_tu_hoat_dong_kinh_doanh": 2}, 1) is None


def test_reconcile_calls_it(b):
    """⚠️ A gate nobody calls is not a gate: the whole statement is refused through `reconcile`."""
    rows = [Row(label=k, key=k, number=None, values=[v]) for k, v in tpb_q3_2019(195_179).items()]
    rows += [Row(label=f"line {n}", key=f"line_{n}", number=None, values=[n]) for n in range(8)]
    st = Statement(report=INCOME_STATEMENT, pages=[0], unit=M, n_columns=1, rows=rows)
    assert b.reconcile(st, tpb_q3_2019(-48_307)).startswith(II)
    assert b.reconcile(st, tpb_q3_2019(195_179)) is None
