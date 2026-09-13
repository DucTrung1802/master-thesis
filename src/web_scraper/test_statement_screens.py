"""`P47`(b) - the arithmetic screens, on hand-made documents. No PDF, no OCR, no network."""
import json

import pytest

from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.statement_screens import (MAX_STEP, income_statement_screens, screen_document,
                                           screen_run)


@pytest.fixture(scope="module")
def builder():
    return FinancialsBuilder(logger=None)


def _doc(period, **reports):
    return {"period": period,
            "accepted": {r: {"values": v} for r, v in reports.items()}}


def _bs(assets, liabilities=None, equity=None, resources=None):
    v = {"tong_cong_tai_san": assets}
    if resources is not None:
        v["tong_cong_nguon_von"] = resources
    if liabilities is not None:
        v["tong_no_phai_tra"] = liabilities
    if equity is not None:
        v["von_chu_so_huu"] = equity
    return v


def _cf(opening, net, close, fx=None):
    v = {"hdtc_v_tien_va_cac_khoan_tuong_duong_tien_dau_ky": opening,
         "luu_chuyen_tien_thuan_trong_ky": net,
         "hdtc_vi_tien_va_cac_khoan_tuong_duong_tien_cuoi_ky": close}
    if fx is not None:
        v["hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia"] = fx
    return v


# -- the balance sheet --------------------------------------------------------
def test_a_sound_balance_sheet_is_not_flagged(builder):
    doc = _doc("Q1-2024", balance_sheet=_bs(100, 60, 40, resources=100))
    assert screen_document(doc, builder) == {}


def test_a_component_that_does_not_add_up_is_flagged(builder):
    """FPT Q3-2022: A + B = 35,467,952,822,566 against a printed 55,127,101,516,155,
    with `B` reading 198,477,998,944 for a company holding 55 tn."""
    doc = _doc("Q3-2022", balance_sheet=_bs(55_127_101_516_155,
                                            35_269_474_823_622, 198_477_998_944,
                                            resources=55_127_101_516_155))
    why = screen_document(doc, builder)["balance_sheet"]
    assert any("liabilities + equity" in w for w in why), why


def test_the_filings_own_rounding_is_not_a_defect(builder):
    """A subtotal off by one dong must not be flagged - measured cases sit at 1 in 1e13."""
    assert screen_document(_doc("Q1-2024", balance_sheet=_bs(1_000_000_000_001,
                                                            600_000_000_000,
                                                            400_000_000_000)),
                           builder) == {}


# -- the cash flow ------------------------------------------------------------
def test_a_cash_identity_that_closes_is_not_flagged(builder):
    assert screen_document(_doc("Q1-2024", cash_flow=_cf(100, 20, 118, fx=-2)),
                           builder) == {}


def test_a_negative_cash_balance_is_flagged(builder):
    """`CFB-1`: BID Q3-2011 holds a closing balance of -23,457,326,032,339."""
    why = screen_document(_doc("Q3-2011", cash_flow=_cf(100, 20, -23_457_326_032_339)),
                          builder)["cash_flow"]
    assert any("NEGATIVE" in w for w in why), why


def test_a_cash_identity_that_misses_is_flagged(builder):
    """FPT Q1-2023: closing read 424,451,239 where the identity gives 3,289,424,451,229."""
    why = screen_document(_doc("Q1-2023", cash_flow=_cf(3_000_000_000_000,
                                                        289_424_451_229,
                                                        424_451_239)),
                          builder)["cash_flow"]
    assert any("!= closing" in w for w in why), why


# -- continuity, which is the one that needs the whole batch ------------------
def test_a_magnitude_error_is_only_visible_beside_its_neighbours(builder, tmp_path):
    """A figure wrong by 10^6 reconciles perfectly against itself - `unit_of`'s own
    docstring - so no per-document identity can see it."""
    folder = tmp_path / "run"
    (folder / "documents").mkdir(parents=True)
    rows = [("Q1-2024", 100_000_000_000_000), ("Q2-2024", 101_000_000_000_000),
            ("Q3-2024", 102_000_000_000),      # x1000 too small
            ("Q4-2024", 104_000_000_000_000)]
    for period, total in rows:
        doc = _doc(period, balance_sheet=_bs(total, total // 2, total - total // 2,
                                             resources=total))
        (folder / "documents" / (period + ".json")).write_text(
            json.dumps(doc), encoding="utf-8")
        # every one of them passes on its own
        assert screen_document(doc, builder) == {}
    flagged = screen_run([folder], builder)
    assert ("Q3-2024", "balance_sheet") in flagged
    # ⚠️ RESTATED 2026-09-13 (`NBR-1`): this asserted that the step BACK UP convicts Q4-2024 too —
    # a right figure held because its neighbour is wrong, which is how TPB Q1-2010 and Q1-2011 and
    # BVH Q4-2025 were held. Q3 breaks with both neighbours; Q4 agrees with Q2.
    assert ("Q4-2024", "balance_sheet") not in flagged
    assert ("Q2-2024", "balance_sheet") not in flagged


def test_ordinary_growth_is_not_a_step(builder, tmp_path):
    folder = tmp_path / "run"
    (folder / "documents").mkdir(parents=True)
    total = 1_000_000_000_000
    for year in (2022, 2023, 2024):
        for q in (1, 2, 3, 4):
            period = "Q{}-{}".format(q, year)
            total = int(total * 1.12)
            (folder / "documents" / (period + ".json")).write_text(
                json.dumps(_doc(period, balance_sheet=_bs(total, total // 2,
                                                          total - total // 2))),
                encoding="utf-8")
    assert screen_run([folder], builder) == {}
    assert MAX_STEP > 1.12, "the fixture must model growth the screen tolerates"


def test_an_absent_statement_is_not_screened(builder):
    """A statement the run refused has no figures to judge - and `missing` is an answer,
    not a suspect."""
    assert screen_document({"period": "Q1-2024", "accepted": {}}, builder) == {}


# -- the corp section sums, and the gap the continuity rule has to survive ----
def test_the_corp_section_sum_catches_what_the_trivial_identity_cannot(builder):
    """FPT Q3-2022, to the dong. `C_LIABILITIES` does not map on `corp`, so `A != L+E` cannot
    run and `assets == resources` is true by construction - this is the only check left."""
    doc = _doc("Q3-2022", balance_sheet={
        "tong_cong_tai_san": 55_127_101_516_155,
        "tong_cong_nguon_von": 55_127_101_516_155,
        "a_tai_san_ngan_han": 35_269_474_823_622,
        "b_tai_san_dai_han": 198_477_998_944})
    why = screen_document(doc, builder)["balance_sheet"]
    assert any("assets A+B" in w for w in why), why


def test_a_bank_balance_sheet_does_not_carry_those_columns(builder):
    """The check needs no template argument because it simply cannot fire on `bank`."""
    assert screen_document(_doc("Q1-2024", balance_sheet=_bs(100, 60, 40, resources=100)),
                           builder) == {}


def test_two_quarters_a_year_apart_are_judged_per_quarter(builder, tmp_path):
    """A batch parses the OUTSTANDING quarters, so consecutive here is not consecutive on the
    calendar. FPT's run held Q2-2009 and then Q2-2010 and the honest 1.79x between them was
    flagged, while every neighbour of the pair confirmed both figures."""
    folder = tmp_path / "run"
    (folder / "documents").mkdir(parents=True)
    for period, total in (("Q2-2009", 6_407_989_491_090), ("Q2-2010", 11_481_761_767_631)):
        (folder / "documents" / (period + ".json")).write_text(
            json.dumps(_doc(period, balance_sheet=_bs(total))), encoding="utf-8")
    assert screen_run([folder], builder) == {}


def test_a_magnitude_slip_is_caught_even_spread_over_a_year(builder, tmp_path):
    folder = tmp_path / "run"
    (folder / "documents").mkdir(parents=True)
    for period, total in (("Q2-2009", 6_407_989_491_090), ("Q2-2010", 11_481_761_767)):
        (folder / "documents" / (period + ".json")).write_text(
            json.dumps(_doc(period, balance_sheet=_bs(total))), encoding="utf-8")
    assert ("Q2-2010", "balance_sheet") in screen_run([folder], builder)


# -- `A != L + E` and the terms a Decision-15 sheet prints outside equity -------------------------
# ⚠️ BVH Q1-2009 and Q4-2008, off their own run folder (2026-09-13): both grand totals equal to the
# đồng, and the gap is EXACTLY `II. Nguồn kinh phí và quỹ khác` + `C. LỢI ÍCH CỦA CỔ ĐÔNG THIỂU SỐ`.
def _insurance_bs(assets, liabilities, equity, other_funds, minority):
    return {"tong_cong_tai_san": assets, "tong_cong_nguon_von": assets,
            "c_no_phai_tra": liabilities, "d_von_chu_so_huu": equity,
            "ii_nguon_kinh_phi_va_quy_khac": other_funds, "c_loi_ich_co_dong_thieu_so": minority}


@pytest.mark.parametrize("period, a, l, e, funds, minority", [
    ("Q1-2009", 28_114_653_581_831, 18_946_609_050_313, 7_893_035_286_750, 35_760_473_126,
     1_239_248_771_642),
    ("Q4-2008", 25_317_575_407_946, 16_526_705_083_134, 8_265_011_167_953, 36_500_034_959,
     489_359_121_900),
])
def test_a_sheet_whose_resources_include_funds_and_minority_is_not_flagged(builder, period, a, l, e,
                                                                           funds, minority):
    doc = _doc(period, balance_sheet=_insurance_bs(a, l, e, funds, minority))
    assert "balance_sheet" not in screen_document(doc, builder)


def test_a_gap_the_extra_terms_do_not_explain_is_still_flagged(builder):
    """The extras widen the candidates and nothing else: a gap of 1 tn beside a 1.24 tn minority
    line and a 35.8 bn fund is no subset of them."""
    doc = _doc("Q1-2009", balance_sheet=_insurance_bs(28_114_653_581_831, 18_946_609_050_313,
                                                      6_893_035_286_750, 35_760_473_126,
                                                      1_239_248_771_642))
    why = screen_document(doc, builder)["balance_sheet"]
    assert any("liabilities + equity" in w for w in why), why


# -- `CXT-1`: the lines printed BETWEEN the two cash balances ------------------
def _cf_dumped(opening, net, close, fx, between):
    """VJC Q4-2015's shape: net, opening, FX, a translation line the chart has no column for, closing."""
    doc = _doc("Q4-2015", cash_flow=_cf(opening, net, close, fx=fx))
    doc["accepted"]["cash_flow"]["row_dump"] = (
        [[None, "luu_chuyen_tien_thuan_trong_nam", "", [net, 1]],
         [None, "tien_va_cac_khoan_tuong_duong_tien_dau_nam", "", [opening, 2]],
         [None, "anh_huong_cua_thay_doi_ty_gia_hoi_doai", "", [fx, 3]]]
        + [[None, "chenh_lech_quy_doi_ngoai_te_cac_hoat_dong_o_nuoc_ngoai", "", [x, 4]] for x in between]
        + [[None, "tien_va_cac_khoan_tuong_duong_tien_cuoi_nam", "", [close, 5]]])
    return doc


def test_a_translation_line_between_the_balances_closes_the_identity(builder):
    """VJC Q4-2015, to the đồng: 526,748,546,327 + 391,119,906,214 - 4,474,463,459
    + 10,118,928,613 = 923,512,917,695. `reconcile` accepted it on its span; the screen held it."""
    doc = _cf_dumped(526_748_546_327, 391_119_906_214, 923_512_917_695, -4_474_463_459,
                     [10_118_928_613])
    assert screen_document(doc, builder) == {}


def test_a_span_that_does_not_close_is_still_flagged(builder):
    """⚠️ The span is a fourth TERM, not a licence: a line that does not make the sum close
    to `_equal` leaves the statement flagged."""
    doc = _cf_dumped(526_748_546_327, 391_119_906_214, 923_512_917_695, -4_474_463_459,
                     [10_000_000_000])
    why = screen_document(doc, builder)["cash_flow"]
    assert any("!= closing" in w for w in why), why


def test_without_a_row_dump_the_identity_is_judged_as_before(builder):
    why = screen_document(_doc("Q4-2015", cash_flow=_cf(526_748_546_327, 391_119_906_214,
                                                        923_512_917_695, fx=-4_474_463_459)),
                          builder)["cash_flow"]
    assert any("!= closing" in w for w in why), why


def test_the_span_is_anchored_on_the_accepted_balances(builder):
    """A dump whose closing row carries some other figure has no span to sum."""
    doc = _cf_dumped(526_748_546_327, 391_119_906_214, 923_512_917_695, -4_474_463_459,
                     [10_118_928_613])
    doc["accepted"]["cash_flow"]["row_dump"][-1][3][0] = 923_512_917_000
    assert "cash_flow" in screen_document(doc, builder)


def test_single_digit_codes_do_not_close_on_a_span(builder):
    """⚠️ PLX Q1-2013's `Mã số` reading: opening 4, movement 3, closing 8 — released by a span
    bound of two, which is the bound deciding and not the figures."""
    doc = _cf_dumped(4, 3, 8, 3, [-2])
    assert "cash_flow" in screen_document(doc, builder)



# -- `NBR-1`: the outlier is convicted, not its right neighbour ---------------------
def _run_folder(tmp_path, totals):
    folder = tmp_path / "run_nbr"
    (folder / "documents").mkdir(parents=True, exist_ok=True)
    for period, total in totals.items():
        doc = {"period": period, "accepted": {"balance_sheet": {"values": {"tong_tai_san": total}}}}
        (folder / "documents" / f"HOSE_TPB__{period}.json").write_text(json.dumps(doc), encoding="utf-8")
    return folder


def _continuity(flagged):
    return sorted(p for (p, r), why in flagged.items()
                  if r == "balance_sheet" and any("total assets" in w for w in why))


def test_a_sound_quarter_after_a_wrong_one_is_not_convicted(builder, tmp_path):
    """TPB: Q4-2009 on disk reads a thousandth of the bank, Q4-2010's reading 20.9 bn."""
    M = 1_000_000
    folder = _run_folder(tmp_path, {"Q4-2009": 10_728_532_331, "Q1-2010": 13_465_108 * M,
                                    "Q2-2010": 12_399_950 * M, "Q3-2010": 14_656_764 * M,
                                    "Q4-2010": 20_889_254_217, "Q1-2011": 25_570_040 * M,
                                    "Q2-2011": 27_050_271 * M})
    assert _continuity(screen_run([folder], builder)) == ["Q4-2009", "Q4-2010"]


def test_a_wrong_last_quarter_is_still_convicted(builder, tmp_path):
    folder = _run_folder(tmp_path, {"Q1-2025": 100, "Q2-2025": 105, "Q3-2025": 110, "Q4-2025": 5})
    assert _continuity(screen_run([folder], builder)) == ["Q4-2025"]


def test_a_right_last_quarter_beside_a_wrong_one_is_not(builder, tmp_path):
    """BVH: Q2-2025 read 18.8 tn between 255.8 tn and 291.9 tn."""
    folder = _run_folder(tmp_path, {"Q4-2024": 250, "Q1-2025": 256, "Q2-2025": 19, "Q4-2025": 292})
    assert _continuity(screen_run([folder], builder)) == ["Q2-2025"]


def test_two_readings_that_disagree_with_nothing_to_arbitrate_are_both_held(builder, tmp_path):
    folder = _run_folder(tmp_path, {"Q1-2025": 100, "Q2-2025": 1000})
    assert _continuity(screen_run([folder], builder)) == ["Q2-2025"]


# -- `GTT-3`: a stored reading with the grand total in a line item is held --------
def test_a_line_item_holding_the_grand_total_is_flagged(builder):
    doc = _doc("Q1-2013", balance_sheet={"tong_tai_san": 112_611, "tong_no_phai_tra_va_von_chu_so_huu": 112_611,
                                         "viii_von_chu_so_huu": 112_611, "tong_no_phai_tra": 105_000})
    why = screen_document(doc, builder)["balance_sheet"]
    assert any("holds the grand total" in w and "viii_von_chu_so_huu" in w for w in why), why


def test_the_charts_section_header_total_is_not_a_line_item(builder):
    doc = _doc("Q1-2020", balance_sheet={"tong_tai_san": 176_632, "tong_no_phai_tra_va_von_chu_so_huu": 176_632,
                                         "b_no_phai_tra_va_von_chu_so_huu": 176_632})
    assert "balance_sheet" not in screen_document(doc, builder)


# -- `ISR-1`: an income statement no filing can print ------------------------------------
_M = 1_000_000


def test_net_revenue_below_gross_profit_is_flagged(builder):
    """GVR Q1-2023's stored reading: net revenue 167,499 đồng beside a gross profit of 1.0 tn."""
    doc = _doc("Q1-2023", income_statement={
        "1_doanh_thu_ban_hang_va_cung_cap_dich_vu": 446,
        "3_doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu": 167_499,
        "5_loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu": 1_005_877_717_111})
    why = screen_document(doc, builder)["income_statement"]
    assert sum("is below" in w for w in why) == 2


def test_a_negative_printed_income_line_is_flagged(builder):
    """BID Q2-2019 on disk: interest income -54,438,116 m between 23.5 tn and 25.5 tn."""
    doc = _doc("Q2-2019", income_statement={
        "1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": -54_438_116 * _M})
    assert screen_document(doc, builder)["income_statement"][0].startswith("NEGATIVE")


def test_a_stored_reading_one_row_up_is_flagged_by_its_net_lines(builder):
    """MBB Q3-2017 as the 2026-09-08 run read it: every figure one row up."""
    doc = _doc("Q3-2017", income_statement={
        "i_thu_nhap_lai_thuan": -2_262_098 * _M, "3_thu_nhap_tu_hoat_dong_dich_vu": 2_834_971 * _M,
        "4_chi_phi_hoat_dong_dich_vu": 697_863 * _M,
        "ii_lai_lo_thuan_tu_hoat_dong_dich_vu": -101_427 * _M})
    why = screen_document(doc, builder)["income_statement"]
    assert any("does not close" in w for w in why)


def test_a_sound_bank_income_statement_is_not_flagged(builder):
    """The same quarter as the 2026-09-14 run read it: both net lines close to the million."""
    doc = _doc("Q3-2017", income_statement={
        "1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu": 5_097_069 * _M,
        "2_chi_phi_lai_va_cac_chi_phi_tuong_tu": -2_262_098 * _M,
        "i_thu_nhap_lai_thuan": 2_834_971 * _M,
        "3_thu_nhap_tu_hoat_dong_dich_vu": 936_355 * _M,
        "4_chi_phi_hoat_dong_dich_vu": -561_531 * _M,
        "ii_lai_lo_thuan_tu_hoat_dong_dich_vu": 374_824 * _M})
    assert screen_document(doc, builder) == {}


def test_an_absurd_figure_is_flagged_on_any_statement(builder):
    """FPT Q2-2025 on disk: `12_thu_nhap_khac` -2,993,843,310,849,432,064 đồng."""
    doc = _doc("Q2-2025", balance_sheet=_bs(100, 60, 40, resources=100),
               cash_flow={"luu_chuyen_tien_thuan_trong_ky": 10 ** 17},
               income_statement={"12_thu_nhap_khac": -2_993_843_310_849_432_064})
    assert sorted(screen_document(doc, builder)) == ["cash_flow", "income_statement"]


def test_other_income_is_judged_only_on_a_printed_statement(builder):
    """A de-cumulated quarter of other income can go negative on a restated year-to-date."""
    v = {"5_thu_nhap_tu_hoat_dong_khac": -26_440 * _M}
    assert income_statement_screens(v, builder, derived=True) == []
    assert income_statement_screens(v, builder) != []
