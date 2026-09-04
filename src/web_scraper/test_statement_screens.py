"""`P47`(b) - the arithmetic screens, on hand-made documents. No PDF, no OCR, no network."""
import json

import pytest

from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.statement_screens import (MAX_STEP, screen_document, screen_run)


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
    assert ("Q4-2024", "balance_sheet") in flagged      # and the step back up
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
