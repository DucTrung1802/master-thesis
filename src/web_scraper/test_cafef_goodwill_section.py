"""`GWL-1` — the pre-2015 consolidated form prints goodwill (code 269) outside both asset sections.

GAS Q4-2011's own total label reads `270 = 100 + 200 + 269`, and A + B falls short by exactly its
`Lợi thế thương mại` 692,064,922,695. Pinned through `reconcile` on hand-made rows, with the
magnitude guard that stops the same term closing a sum of fragments.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.cafef_pdf_parser import BALANCE_SHEET, Row, Statement


@pytest.fixture(scope="module")
def b():
    return FinancialsBuilder(logger=None)


def _sheet(a, bb, goodwill, total):
    lines = [("a_tai_san_ngan_han", a), ("tien", a // 10), ("hang_ton_kho", a // 5),
             ("b_tai_san_dai_han", bb), ("tai_san_co_dinh", bb // 2), ("loi_the_thuong_mai", goodwill),
             ("tong_cong_tai_san", total), ("c_no_phai_tra", total // 2), ("no_ngan_han", total // 4),
             ("d_von_chu_so_huu", total - total // 2), ("von_dau_tu_cua_chu_so_huu", total // 3),
             ("tong_cong_nguon_von", total)]
    rows = [Row(label=k, key=k, number=None, values=[v, None]) for k, v in lines]
    st = Statement(report=BALANCE_SHEET, pages=[0], unit=1, n_columns=2, rows=rows)
    mapped = {"a_tai_san_ngan_han": a, "b_tai_san_dai_han": bb, "tong_cong_tai_san": total,
              "c_no_phai_tra": total // 2, "d_von_chu_so_huu": total - total // 2,
              "tong_cong_nguon_von": total}
    return st, mapped


def test_goodwill_outside_both_sections_closes_the_sum(b):
    st, mapped = _sheet(19_228_454_339_267, 25_690_247_699_060, 692_064_922_695, 45_610_766_961_022)
    assert b.reconcile(st, mapped) is None


def test_a_gap_that_is_not_the_goodwill_line_is_still_refused(b):
    st, mapped = _sheet(19_228_454_339_267, 25_690_247_699_060, 600_000_000_000, 45_610_766_961_022)
    assert "section sum does not close" in (b.reconcile(st, mapped) or "")


def test_fragments_do_not_close_on_the_floor(b):
    """⚠️ PLX Q1-2015's shredded reading: `123 + 765 + 9` against a printed `898`."""
    st, mapped = _sheet(123, 765, 9, 898)
    assert "section sum does not close" in (b.reconcile(st, mapped) or "")
