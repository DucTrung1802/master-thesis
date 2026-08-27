"""`cash_extra_terms` — the cash flow's FOURTH term, pinned without a PDF.

BID's FY-2016 consolidated cash flow prints FIVE lines where the bank chart of accounts has
four: IV movement, V opening, "…từ việc nhận sáp nhập MHB", "…nhận từ các công ty con khi hợp
nhất", VIII closing. A bank that absorbs another bank gains cash that is neither a flow nor an
FX effect, and there is no column for it — so `_cash_flow_identity` was unanswerable and the
quarter was refused for `fx not mapped` while every figure on the page was right:

    55,806,145 + 6,711,633 + 3,004,011 = 65,521,789      (Triệu VND, exact to the đồng)

Three things are asserted here and only the first is the recovery.

  1. The identity closes once the span between the two balances is counted.
  2. ⚠️ **ONLY THE CURRENT-PERIOD CELL COUNTS.** The 2016 column leaves the MHB line blank and
     prints 1,477,340 beside it in the 2015 comparative. `_first_value` would fall through to
     that prior-year figure and break a sum that closes exactly without it — so a blank cell
     must contribute NOTHING rather than its neighbour.
  3. ⚠️ **THE TERM IS COUNTED, NEVER WRITTEN.** Claiming it as the FX adjustment would put
     merger cash in `hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia`, and the identity would
     then CONFIRM the wrong account because the arithmetic is right — the failure CLAUDE.md
     §6-2-vicies measured on BID's FY-2015. A test that only checked "the quarter is no longer
     `missing`" would go green on that.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.cafef_pdf_parser import Row, Statement, CASH_FLOW


OPEN = 55_806_145
NET = 6_711_633
MERGER = 3_004_011
CLOSE = 65_521_789
MHB_PRIOR = 1_477_340                      # 2015's merger cash, printed in the comparative

C_OPEN = "hdtc_v_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky"
C_CLOSE = "hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky"
C_NET = "hdtc_iv_luu_chuyen_tien_thuan_trong_ky"
C_FX = "hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia"


@pytest.fixture
def builder():
    return FinancialsBuilder()


def _row(key, current, prior=None):
    return Row(label=key.replace("_", " "), key=key, number="", values=[current, prior])


def _bid_fy2016_tail():
    """The five printed lines, in the order BID prints them.

    Both balance rows carry the dated wording `_is_cash_tail` recognises; the two merger lines
    do not, which is what makes them a SPAN rather than a named account.
    """
    return [
        _row("hdtc_iv_luu_chuyen_tien_thuan_trong_nam", NET, 4_129_579),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_nam", OPEN, 50_199_476),
        # 2016 blank, the 2015 comparative populated — the fall-through trap
        _row("tien_va_cac_khoan_tuong_duong_tien_tu_viec_nhan_sap_nhap_mhb", None, MHB_PRIOR),
        _row("tien_va_cac_khoan_tuong_duong_tien_nhan_tu_cac_cong_ty_con_khi_hop_nhat",
             MERGER, None),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_nam", CLOSE, OPEN),
    ]


def _statement(rows):
    return Statement(report=CASH_FLOW, pages=[11, 12], unit=1_000_000,
                     n_columns=2, rows=rows)


MAPPED = {C_OPEN: OPEN, C_NET: NET, C_CLOSE: CLOSE}


# ── the arithmetic the filing actually prints ────────────────────────────────────────────

def test_the_filing_closes_only_with_the_fourth_term():
    assert OPEN + NET + MERGER == CLOSE
    assert OPEN + NET != CLOSE


# ── 1. the recovery ─────────────────────────────────────────────────────────────────────

def test_without_the_statement_the_identity_is_unanswerable(builder):
    why = builder._cash_flow_identity(dict(MAPPED))
    assert why is not None and "fx" in why


def test_the_span_between_the_balances_closes_it(builder):
    st = _statement(_bid_fy2016_tail())
    assert builder._cash_flow_identity(dict(MAPPED), st=st) is None


def test_the_span_is_the_merger_line_alone(builder):
    st = _statement(_bid_fy2016_tail())
    assert builder._extra_cash_terms(st) == MERGER


def test_the_balance_span_is_the_two_dated_rows(builder):
    st = _statement(_bid_fy2016_tail())
    assert builder._cash_balance_span(st) == (1, 4)


# ── 2. the comparative column must not leak in ──────────────────────────────────────────

def test_a_blank_current_cell_contributes_nothing(builder):
    """The MHB line's 2016 cell is blank; its 1,477,340 belongs to 2015 and must stay there."""
    st = _statement(_bid_fy2016_tail())
    assert builder._extra_cash_terms(st) != MERGER + MHB_PRIOR
    st.rows[2].values = [None, 999_999_999]
    assert builder._extra_cash_terms(st) == MERGER


def test_an_unreadable_fourth_term_refuses_the_statement(builder):
    """OCR losing the merger figure is a refusal, not a fall-through to some other number."""
    rows = _bid_fy2016_tail()
    rows[3].values = [None, None]
    why = builder._cash_flow_identity(dict(MAPPED), st=_statement(rows))
    assert why is not None


# ── 3. nothing is written to the FX column ──────────────────────────────────────────────

def test_the_fourth_term_never_lands_in_the_fx_column(builder):
    mapped = dict(MAPPED)
    builder._cash_flow_identity(mapped, st=_statement(_bid_fy2016_tail()))
    assert C_FX not in mapped
    assert mapped == MAPPED


def test_a_positional_fx_guess_is_refused_when_the_row_does_not_say_fx(builder):
    """One row between the balances, and it is a MERGER line: the FX column stays empty.

    `_recover_totals` claims `first_i + 1` as the FX adjustment whenever the balances sit two
    rows apart. That is BID's FY-2015 shape, where the row between them is the MHB line — so
    the guess writes merger cash into the FX account and the identity, testing arithmetic that
    is correct, confirms it.
    """
    rows = [
        _row("hdtc_iv_luu_chuyen_tien_thuan_trong_nam", 4_129_579),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_nam", 50_199_476),
        _row("tien_va_cac_khoan_tuong_duong_tien_tu_viec_nhan_sap_nhap_mhb", MHB_PRIOR),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_nam", 55_806_145),
    ]
    st = _statement(rows)

    loose: dict = {}
    builder._recover_totals(loose, st, {}, False, False)
    assert loose.get(C_FX) == MHB_PRIOR          # today's behaviour, and it is the defect

    guarded: dict = {}
    builder._recover_totals(guarded, st, {}, False, True)
    assert C_FX not in guarded


def test_a_real_fx_line_is_still_claimed(builder):
    """The guard is about the LABEL, not about suppressing the recovery."""
    rows = [
        _row("hdtc_iv_luu_chuyen_tien_thuan_trong_ky", 1_000),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky", 10_000),
        _row("dieu_chinh_anh_huong_cua_thay_doi_ty_gia", -25),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky", 10_975),
    ]
    out: dict = {}
    builder._recover_totals(out, _statement(rows), {}, False, True)
    assert out.get(C_FX) == -25


# ── the default path is untouched ───────────────────────────────────────────────────────

def test_an_ordinary_cash_flow_is_judged_exactly_as_before(builder):
    """Opening + movement + FX, one row between the balances, nothing exotic."""
    rows = [
        _row("hdtc_iv_luu_chuyen_tien_thuan_trong_ky", 1_000),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky", 10_000),
        _row("dieu_chinh_anh_huong_cua_thay_doi_ty_gia", -25),
        _row("tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky", 10_975),
    ]
    mapped = {C_OPEN: 10_000, C_NET: 1_000, C_FX: -25, C_CLOSE: 10_975}
    assert builder._cash_flow_identity(dict(mapped)) is None
    assert builder._cash_flow_identity(dict(mapped), st=_statement(rows)) is None


def test_a_broken_statement_is_still_refused_with_the_span(builder):
    """The span may not rescue a statement whose figures genuinely disagree."""
    rows = _bid_fy2016_tail()
    rows[0].values = [NET + 1, None]                     # movement off by one đồng
    why = builder._cash_flow_identity({**MAPPED, C_NET: NET + 1}, st=_statement(rows))
    assert why is not None


def test_the_span_needs_both_balances(builder):
    st = _statement(_bid_fy2016_tail()[:2])
    assert builder._cash_balance_span(st) is None
    assert builder._extra_cash_terms(st) == 0


# ── the cascade wiring ──────────────────────────────────────────────────────────────────

def test_the_new_layers_are_last_and_relaxed():
    names = [l.name for l in FinancialsBuilder.LAYERS]
    extra = [l for l in FinancialsBuilder.LAYERS if l.cash_extra_terms]
    assert extra, "no layer carries cash_extra_terms"
    assert names[-3:] == [l.name for l in extra], "the new layers must sit last"
    for l in extra:
        # the identity only runs under `verify_cash`, which rides with `relax_totals`
        assert l.relax_totals
        # every figure of BID's FY-2016 tail is only correct at pad 6
        assert l.crop_pad == 6.0


def test_the_label_repair_runs_before_the_bare_layer():
    """§6-2-unvicies' rule: when two layers differ by a LABEL repair, the repair goes first."""
    extra = [l for l in FinancialsBuilder.LAYERS if l.cash_extra_terms and l.dpi == 200]
    assert [l.annual_tail for l in extra] == [True, False]
