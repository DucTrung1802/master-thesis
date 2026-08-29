"""The reconcile anchors on a NON-BANK chart of accounts — pinned without a PDF or an engine.

Until 2026-08-28 the nine `C_*` role tuples and the `ANCHORS` list held BANK column names
only, while `reconcile`, `_probe` and `_cash_flow_identity` take no `template` argument to
pick a set with. On a `corp`, `securities` or `insurance` filing every one of them therefore
fell through to `Statement.find`'s fuzzy TEXT search, and that is not a graceful degradation:
measured on VIC Q1-2026, the closing-cash needle answered with the OPENING balance and the
statement was accepted carrying 72,226,561 where the filing closes at 54,750,360 (TPL-1,
CRP-1).

Three defects were found and each has its own section below:

  1. the role tuples and `ANCHORS` were bank-only, so `_anchor` re-matched 2 of 7 roles on
     `corp` and `_cash_flow_identity` could not run at all;
  2. adding them exposed a latent hazard in `_anchor` itself — an 11-character account
     ("vốn chủ sở hữu") wins containment against any line that merely MENTIONS it, and the
     length-ratio tie-break then prefers the short impostor;
  3. the FX line, printed blank, rode its label onto the closing balance's figures, so the
     closing balance was written into the FX column with both gates passing.

⚠️ THE FIXTURES CARRY REAL LABELS FROM REAL FILINGS. A synthetic label that is merely
plausible proves nothing about a fuzzy matcher: the whole difficulty here is that the wrong
answer scores 0.90 and the right one 0.95.
"""
import pytest

from web_scraper.cafef_financials import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          FinancialsBuilder)
from web_scraper.cafef_pdf_parser import Row, Statement

TEMPLATES = ("bank", "corp", "securities", "insurance")

# The nine roles, and which report each one lives in. `C_FLOW_SECTIONS` is deliberately absent:
# it is a SUM over however many section subtotals a chart prints, so several matches per chart
# is the correct answer and the at-most-one invariant does not apply to it.
ROLES = {
    "C_ASSETS": BALANCE_SHEET,
    "C_RESOURCES": BALANCE_SHEET,
    "C_LIABILITIES": BALANCE_SHEET,
    "C_EQUITY": BALANCE_SHEET,
    "C_PBT": INCOME_STATEMENT,
    "C_NET_CF": CASH_FLOW,
    "C_CASH_OPEN": CASH_FLOW,
    "C_CASH_FX": CASH_FLOW,
    "C_CASH_CLOSE": CASH_FLOW,
}

# What the BANK charts resolved before the non-bank columns were added. Re-listed as literals
# rather than recomputed, because a test that derives its expectation from the code under test
# cannot catch the code changing.
BANK_BEFORE = {
    "C_ASSETS": "tong_tai_san",
    "C_RESOURCES": "tong_no_phai_tra_va_von_chu_so_huu",
    "C_LIABILITIES": "tong_no_phai_tra",
    "C_EQUITY": "viii_von_chu_so_huu",
    "C_PBT": "xi_tong_loi_nhuan_truoc_thue",
    "C_NET_CF": "hdtc_iv_luu_chuyen_tien_thuan_trong_ky",
    "C_CASH_OPEN": "hdtc_v_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_dau_ky",
    "C_CASH_FX": "hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia",
    "C_CASH_CLOSE": "hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky",
}


@pytest.fixture(scope="module")
def builder():
    """A builder reading the repo's own charts of accounts, wherever pytest was started.

    The twelve `schema/*.csv` files are a git-tracked repo INPUT with no producer in the
    pipeline (`schema_of`'s docstring says so), so a test may depend on them — but the module
    path globals are relative, and pytest runs from `src/`. `use_data_root()` is the seam that
    re-points them to an absolute path anchored on the module file, which is exactly what it
    exists for and what a Kaggle worker uses.
    """
    from web_scraper import pdf_ocr_job

    pdf_ocr_job.use_data_root()
    return FinancialsBuilder(logger=None)


def _resolve(builder, role, template):
    """Which column of this chart the role answers with — the lookup every caller performs."""
    cols = {c for c, _ in builder.schema_of(template, ROLES[role])}
    return [c for c in getattr(builder, role) if c in cols]


# ──────────────────────────────────────────────────────────────────────────────
# 1. the union is equivalent to a per-template table
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("role", sorted(ROLES))
@pytest.mark.parametrize("template", TEMPLATES)
def test_a_role_resolves_to_at_most_one_column_per_chart(builder, role, template):
    """THE INVARIANT THAT LETS ONE TUPLE SERVE FOUR CHARTS.

    Every caller looks a role up as `next(c for c in C_X if c in mapped)`, so a union behaves
    exactly like a per-template table **provided no chart answers twice**. If one ever did,
    the winner would be decided by tuple ORDER rather than by the filing, which is precisely
    the kind of silent, position-dependent answer this module exists to remove.
    """
    assert len(_resolve(builder, role, template)) <= 1


@pytest.mark.parametrize("role,template", [
    # A role a chart genuinely does not print. Recorded as a FACT about the chart, never
    # filled in with the nearest thing: `securities` prints no "lưu chuyển tiền thuần trong
    # kỳ" at all (so `_cash_flow_identity` sums the section subtotals, which is what that
    # fallback exists for), and the `insurance` cash flow ends at "đầu kỳ" and the FX line
    # with NO CLOSING BALANCE LINE — that one needs a schema repair, not a tuple entry.
    ("C_NET_CF", "securities"),
    ("C_CASH_CLOSE", "insurance"),
])
def test_the_two_roles_a_chart_genuinely_does_not_print(builder, role, template):
    assert _resolve(builder, role, template) == []


@pytest.mark.parametrize("role,template", [
    (r, t) for r in ROLES for t in TEMPLATES
    if (r, t) not in {("C_NET_CF", "securities"), ("C_CASH_CLOSE", "insurance")}
])
def test_every_other_role_resolves_on_every_chart(builder, role, template):
    """The point of the change: a role that the chart DOES print must be found on it.

    Before 2026-08-28 this failed for 21 of the 36 combinations — every non-bank one except
    the two grand totals, whose column names happen to be shared.
    """
    assert len(_resolve(builder, role, template)) == 1


# ──────────────────────────────────────────────────────────────────────────────
# 2. bank behaviour is unchanged BY CONSTRUCTION
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("role", sorted(ROLES))
def test_bank_anchor_resolution_is_unchanged(builder, role):
    """⚠️ THE REGRESSION THAT MATTERS. Three tickers, 405 `pdf` statement-rows and
    every published fundamental in this repo are bank filings, so the one thing a non-bank
    fix may not do is move a bank one. The added column names do not exist in the bank charts,
    which is what makes that a proof rather than a hope."""
    assert _resolve(builder, role, "bank") == [BANK_BEFORE[role]]


def test_anchors_is_derived_from_the_roles_not_re_listed():
    """The defect was DUPLICATION: `ANCHORS` was a hand-written literal beside the role
    tuples, so adding a column to a role left the anchor list behind — which is how
    `_anchor`'s position-independent re-match came to be switched off for five of the seven
    roles on every non-bank filing. Deriving it makes that impossible; this asserts the
    derivation still holds so nobody re-lists it later."""
    F = FinancialsBuilder
    expected = set(F.C_ASSETS + F.C_RESOURCES + F.C_LIABILITIES + F.C_EQUITY
                   + F.C_PBT + F.C_NET_CF + F.C_CASH_CLOSE)
    assert set(F.ANCHORS) == expected


def test_the_cash_flow_positional_roles_are_deliberately_not_anchored():
    """⚠️ NOT AN OVERSIGHT, AND NOT TO BE 'FIXED'. `_anchor`'s own docstring records
    that the two dated cash balances are separated ONLY by a position tie-break, because they
    are the same words with a different date. Anchoring the opening and the FX line would put
    them into that competition and change which row wins on a BANK filing — a separate change
    needing its own regression, not a side effect of the non-bank one."""
    F = FinancialsBuilder
    for role in ("C_CASH_OPEN", "C_CASH_FX", "C_FLOW_SECTIONS"):
        for column in getattr(F, role):
            assert column not in F.ANCHORS, role


# ──────────────────────────────────────────────────────────────────────────────
# 3. _anchor may not steal a row that fits another account better
# ──────────────────────────────────────────────────────────────────────────────


def _bs(rows):
    return Statement(report=BALANCE_SHEET, pages=[1], unit=1_000_000, n_columns=2,
                     rows=[Row(label=l, key=k, number="", values=v) for l, k, v in rows])


# VIC Q1-2026, verbatim. The equity row's label carries the page header OCR merged onto it
# ("Thuyết minh / Mã số / NGUỒN VỐN"), which is why its length ratio is WORSE than the
# impostor's — 0.32 against 0.48 — while both score the flat 0.95 that containment awards.
VIC_EQUITY_ROWS = [
    ("TỔNG CỘNG TÀI SẢN", "tong_cong_tai_san", [1_178_694_748, None]),
    ("Thuyết Mã số NGUỒN VỐN minh C. NỢ PHẢI TRẢ",
     "thuyet_ma_so_nguon_von_minh_c_no_phai_tra", [1_024_990_928, None]),
    ("Thuyết Mã số NGUỒN VỐN minh D. VỐN CHỦ SỞ HỮU",
     "thuyet_ma_so_nguon_von_minh_d_von_chu_so_huu", [153_703_820, None]),
    ("9. Quỹ khác thuộc vốn chủ sở hữu", "quy_khac_thuoc_von_chu_so_huu", [117_845, None]),
    ("TỔNG CỘNG NGUỒN VỐN", "tong_cong_nguon_von", [1_178_694_748, None]),
]


def test_an_anchor_does_not_steal_a_row_that_fits_another_account_better(builder):
    """⚠️ THE REGRESSION THE NON-BANK ANCHORS INTRODUCED, AND IT COST TWO CELLS AT ONCE.

    "Vốn chủ sở hữu" is ELEVEN characters, one over MIN_CONTAINS, so containment gives a flat
    0.95 to every line that mentions it — including "Quỹ khác thuộc vốn chủ sở hữu", a line
    the chart of accounts already has and the ordered walk had matched EXACTLY. The
    length-ratio tie-break then preferred the impostor, and `_claim` evicted the account it
    came from, so one anchor turned two correct cells into two wrong ones: equity read 117,845
    for a company with 153,703,820.
    """
    mapped = builder.map_to_schema(_bs(VIC_EQUITY_ROWS), "corp")
    assert mapped["d_von_chu_so_huu"] == 153_703_820
    assert mapped["i_9_quy_khac_thuoc_von_chu_so_huu"] == 117_845


def test_an_anchor_still_wins_a_TIE_which_is_claims_documented_case(builder):
    """The guard is `strictly better`, and that is what keeps `_claim`'s own case working.

    ACB's Q1-2022 prints "Dự phòng rủi ro khác" and "TỔNG NỢ PHẢI TRẢ" on what OCR reads as ONE
    row. Both accounts score 0.95 by containment, so the anchor must still take it — the
    alternative is a provision line holding total liabilities, with nothing downstream able to
    tell, which is the defect `_claim` was written for.
    """
    st = _bs([
        ("TỔNG TÀI SẢN", "tong_tai_san", [611_223_523, None]),
        ("Dự phòng rủi ro khác TỔNG NỢ PHẢI TRẢ",
         "du_phong_rui_ro_khac_tong_no_phai_tra", [480_433_095, None]),
    ])
    mapped = builder.map_to_schema(st, "bank")
    assert mapped["tong_no_phai_tra"] == 480_433_095
    assert "vii_3_du_phong_rui_ro_khac" not in mapped


# ──────────────────────────────────────────────────────────────────────────────
# 4. the FX line that rode onto the closing balance
# ──────────────────────────────────────────────────────────────────────────────

FX = "hdtc_anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te"
CLOSE = "hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61"
OPEN = "hdtc_tien_va_tuong_duong_tien_dau_ky_60"
NET = "hdtc_luu_chuyen_tien_thuan_trong_ky_50_20_30_40"

# VIC Q1-2026's cash-flow tail, verbatim. The FX line prints no figure, so `table_rows` carries
# its label forward and the closing balance's figures arrive under BOTH labels joined — and
# `slug` caps a key at 60 characters, which is exactly where "…tương đương tiền cuối kỳ" was
# cut off. The row therefore reads as a pure FX line.
MERGED = ("Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ "
          "Tiền và tương đương tiền cuối kỳ")
MERGED_KEY = "anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te_tien"


def _cf(rows):
    return Statement(report=CASH_FLOW, pages=[1], unit=1_000_000, n_columns=2,
                     rows=[Row(label=l, key=k, number="", values=v) for l, k, v in rows])


# The rows above the tail, also verbatim. They are here only to clear `MIN_ROWS = 12` — a
# statement with fewer parsed rows than that is refused as "not a statement" before any of the
# subtotal lookups run, so a six-row fixture would test the wrong gate.
VIC_BODY = [
    ("Lợi nhuận trước thuế", "luu_chuyen_tien_tu_hoat_dong_kinh_doanh_loi_nhuan_truoc_thue",
     [11_536_718, None]),
    ("Khấu hao và hao mòn", "dieu_chinh_cho_cac_khoan_khau_hao_va_hao_mon", [7_913_123, None]),
    ("Thay đổi các khoản dự phòng", "thay_doi_cac_khoan_du_phong", [4_477_515, None]),
    ("Lãi, lỗ chênh lệch tỷ giá", "lai_oo_chenh_lech_ty_gia", [165_105, None]),
    ("Lãi, lỗ từ hoạt động đầu tư tài chính", "lai_lo_tu_hoat_dong_dau_tu_tai_chinh",
     [-3_694_095, None]),
    ("Chi phí đi vay", "chi_phi_di_vay", [7_522_882, None]),
    ("Tăng, giảm các khoản phải thu", "tang_giam_cac_khoan_phai_thu", [-11_836_014, None]),
    ("Tăng, giảm hàng tồn kho", "tang_giam_hang_ton_kho", [-11_314_593, None]),
    ("Tiền lãi vay đã trả", "chi_phi_di_vay_da_tra", [-1_182_924, None]),
    ("Thuế thu nhập doanh nghiệp đã nộp", "thue_thu_nhap_doanh_nghiep_da_nop",
     [-892_445, None]),
]

VIC_TAIL = VIC_BODY + [
    ("Lưu chuyển tiền thuần từ hoạt động kinh doanh",
     "luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh", [26_509_936, None]),
    ("Lưu chuyển tiền thuần từ hoạt động đầu tư",
     "luu_chuyen_tien_thuan_tu_hoat_dong_dau_tu", [-68_459_360, None]),
    ("Lưu chuyển tiền thuần từ hoạt động tài chính",
     "luu_chuyen_tien_thuan_tu_hoat_dong_tai_chinh", [24_473_223, None]),
    ("Lưu chuyển tiền thuần trong kỳ", "luu_chuyen_tien_thuan_trong_ky", [-17_476_201, None]),
    ("Tiền và tương đương tiền đầu kỳ", "tien_va_tuong_duong_tien_dau_ky", [72_226_561, None]),
    (MERGED, MERGED_KEY, [54_750_360, None]),
]


def test_the_closing_balance_is_taken_out_of_the_fx_column(builder):
    """⚠️ A 54.75 tn FX EFFECT ON 72 tn OF CASH, WRITTEN AS `pdf` WITH BOTH GATES
    PASSING. This is the figure CRP-1 records: the closing balance in the FX column and no
    closing balance at all, on a statement `reconcile` and `sane` both accepted."""
    mapped = builder.map_to_schema(_cf(VIC_TAIL), "corp")
    assert mapped[CLOSE] == 54_750_360
    assert FX not in mapped, "the filing printed no FX figure — inventing one asserts a number nothing can attribute"
    assert mapped[OPEN] == 72_226_561
    assert mapped[NET] == -17_476_201


def test_the_identity_then_closes_which_is_what_makes_the_move_verifiable(builder):
    """The repair is not trusted because it looks right — it is trusted because the statement's
    own arithmetic then holds: 72,226,561 - 17,476,201 + 0 = 54,750,360, to the đồng. Before
    it, `_cash_flow_identity` could not even run."""
    st = _cf(VIC_TAIL)
    mapped = builder.map_to_schema(st, "corp")
    assert builder.reconcile(st, mapped, verify_cash=True) is None


def test_a_statement_that_already_found_its_closing_balance_is_untouched(builder):
    """The repair fires only into an EMPTY closing column. A cash flow that mapped both lines
    is the ordinary case and must not be rewritten."""
    rows = list(VIC_TAIL[:-1]) + [
        ("Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ",
         "anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te", [1_234, None]),
        ("Tiền và tương đương tiền cuối kỳ", "tien_va_tuong_duong_tien_cuoi_ky",
         [54_751_594, None]),
    ]
    mapped = builder.map_to_schema(_cf(rows), "corp")
    assert mapped[CLOSE] == 54_751_594
    assert mapped[FX] == 1_234


def test_an_ordinary_fx_row_is_left_alone(builder):
    """The precondition is that what FOLLOWS the FX wording names the closing balance. An FX
    line carrying its own figure and nothing else keeps it."""
    rows = list(VIC_TAIL[:-1]) + [
        ("Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ",
         "anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te", [-9_876, None]),
    ]
    mapped = builder.map_to_schema(_cf(rows), "corp")
    assert mapped[FX] == -9_876
    assert CLOSE not in mapped


def test_a_chart_with_no_closing_line_is_skipped_not_guessed_at(builder):
    """`insurance` prints no closing-cash line at all, so there is nowhere to move the figure
    to. The repair returns rather than inventing a column — CLAUDE.md 6-2-quaterdecies calls
    that one a schema repair, and it still is."""
    mapped = builder.map_to_schema(_cf(VIC_TAIL), "insurance")
    assert not any(c.endswith("cuoi_ky") for c in mapped)


# ──────────────────────────────────────────────────────────────────────────────
# 5. the closing-cash needle must never answer with the opening balance
# ──────────────────────────────────────────────────────────────────────────────


def test_the_closing_needle_matches_the_opening_row_without_the_guard(builder):
    """THE HAZARD, ASSERTED SO THE GUARD CANNOT BE QUIETLY REMOVED. `find` scans in statement
    order and the opening balance is printed FIRST, so a fuzzy window hands it over: measured
    at 0.90 against a 0.85 threshold on VIC Q1-2026. TPL-1 predicted exactly this from the
    charts of accounts, before it was ever run."""
    st = _cf([("Tiền và tương đương tiền đầu kỳ", "tien_va_tuong_duong_tien_dau_ky",
               [72_226_561, None])])
    assert st.find(*builder.CASH_CLOSE) == 72_226_561


def test_the_guard_refuses_rather_than_answering_with_the_opening(builder):
    """And the right failure mode is a REFUSAL. `None` escalates the cascade; 72,226,561
    accepted as a closing balance is a wrong number in a canonical file."""
    st = _cf([("Tiền và tương đương tiền đầu kỳ", "tien_va_tuong_duong_tien_dau_ky",
               [72_226_561, None])])
    assert st.find(*builder.CASH_CLOSE, reject=builder.CASH_OPEN_WORDS) is None


def test_the_real_closing_row_is_still_found_through_the_guard(builder):
    """The disqualifier must not cost the line it is protecting."""
    st = _cf([("Tiền và tương đương tiền đầu kỳ", "tien_va_tuong_duong_tien_dau_ky",
               [72_226_561, None]),
              ("Tiền và tương đương tiền cuối kỳ", "tien_va_tuong_duong_tien_cuoi_ky",
               [54_750_360, None])])
    assert st.find(*builder.CASH_CLOSE, reject=builder.CASH_OPEN_WORDS) == 54_750_360


def test_an_annual_report_says_dau_nam_and_is_rejected_too(builder):
    """An annual filing dates its balances by the YEAR where the chart names the period — the
    same wording difference `ANNUAL_WORDING` exists for, so the disqualifier carries both."""
    st = _cf([("Tiền và tương đương tiền đầu năm", "tien_va_tuong_duong_tien_dau_nam",
               [50_202_708, None])])
    assert st.find(*builder.CASH_CLOSE, reject=builder.CASH_OPEN_WORDS) is None


def test_reconcile_refuses_a_cash_flow_whose_only_balance_is_the_opening(builder):
    """End to end through the gate that decides whether a quarter is written: the statement is
    refused with "no closing cash balance", which escalates to the relaxed layers, instead of
    being accepted on the opening balance."""
    rows = list(VIC_TAIL[:-1])
    st = _cf(rows)
    assert builder.reconcile(st, builder.map_to_schema(st, "corp")) == "no closing cash balance"


def test_the_probe_bands_sane_on_the_closing_balance_not_the_opening(builder):
    """`_probe` is what `sane` compares a candidate against, so answering it with the opening
    balance poisons the magnitude history of every quarter that follows (SAN-1)."""
    st = _cf(VIC_TAIL)
    assert builder._probe(CASH_FLOW, builder.map_to_schema(st, "corp"), st) == 54_750_360


# ──────────────────────────────────────────────────────────────────────────────
# 5. "TỔNG CỘNG" is "TỔNG", and an account buried in a merged row is a MENTION
#
# TCB's Q3-2013 balance sheet, verbatim at onnx@300. Three anchors were wrong at once and
# every gate passed, because `reconcile` falls through to `Statement.find`, which reads the
# right rows out of the OCR text while the written ROW carries the wrong ones:
#
#   * "TỔNG CỘNG TÀI SẢN CÓ" scored 0.769 against the chart's "TỔNG TÀI SẢN" — one inserted
#     syllable, no containment because it sits in the middle — so TOTAL ASSETS did not map;
#   * "TỔNG CỘNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" scored 0.929 for its own anchor while merely
#     CONTAINING "vốn chủ sở hữu", which containment awards a flat 0.95, so the EQUITY anchor
#     took the grand total — 165,878,786 mn against a real 13,857,834;
#   * and once that row went to its own anchor, equity fell to the row where OCR had merged
#     the section header "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" onto "II. Tiền gửi và vay các
#     TCTD khác", so equity read 24,686,177 mn of interbank deposits and `_claim` evicted the
#     deposits line the ordered walk had placed correctly.
# ──────────────────────────────────────────────────────────────────────────────

TCB_Q3_2013 = [
    ("Tổng cộng tài sản Có", "tong_cong_tai_san_co", [165_878_786, 179_933_598]),
    ("B. Nợ phải trả và vốn chủ sở hữu II. Tiền gửi và vay các TCTD khác",
     "no_phai_tra_va_von_chu_so_huu_tien_gui_va_vay_cac_tctd_khac",
     [24_686_177, 39_170_405]),
    ("III. Tiền gửi của khách hàng", "tien_gui_cua_khach_hang_v_15",
     [117_236_302, 111_462_288]),
    ("TỔNG NỢ PHẢI TRẢ", "tong_no_phai_tra", [152_030_952, 166_644_022]),
    ("Vốn và các quỹ VIII v.20", "von_va_cac_quy_v_20", [13_857_834, 13_289_576]),
    ("TỔNG CỘNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU",
     "tong_cong_no_phai_tra_va_von_chu_so_huu", [165_878_786, 179_933_598]),
]


def test_tong_cong_tai_san_co_is_total_assets(builder):
    """"Tổng cộng" and "tổng" are one word in a statement heading, normalised on both sides."""
    assert builder.map_to_schema(_bs(TCB_Q3_2013), "bank")["tong_tai_san"] == 165_878_786


def test_the_grand_total_goes_to_its_own_anchor_not_to_equity(builder):
    """With the wording normalised the grand-total row scores 1.000 for its own account and
    0.95 for equity's, so the anchors settle it themselves — no new threshold."""
    mapped = builder.map_to_schema(_bs(TCB_Q3_2013), "bank")
    assert mapped["tong_no_phai_tra_va_von_chu_so_huu"] == 165_878_786
    assert mapped["tong_no_phai_tra"] == 152_030_952


def test_a_merged_section_header_does_not_feed_the_equity_anchor(builder):
    """⚠️ THE ROW IS `HEADER + LINE`, so the header is a PREFIX and the item is the SUFFIX: an
    account matched strictly INSIDE is a mention in somebody else's line. Equity is then
    ABSENT — the correct answer for a figure this filing never printed under a name the chart
    of accounts knows ("Vốn và các quỹ", not "Vốn chủ sở hữu") — and the deposits line keeps
    the figure the ordered walk gave it."""
    mapped = builder.map_to_schema(_bs(TCB_Q3_2013), "bank")
    assert mapped.get("viii_von_chu_so_huu") != 24_686_177
    assert mapped.get("viii_von_chu_so_huu") != 165_878_786
    assert mapped["ii_tien_gui_va_vay_cac_tctd_khac"] == 24_686_177


def test_the_edge_rule_is_confined_to_the_anchors(builder):
    """⚠️ MEASURED: gating the ORDERED WALK the same way changed 23 of 228 archived statements
    and lost sound cells; confined to `_anchor` it changes 4, all of them repairs. The walk has
    position to keep a fuzzy match honest — `_anchor`, by its own docstring, has none."""
    import inspect

    source = inspect.getsource(FinancialsBuilder._anchor)
    assert "edge_containment=True" in source
    assert builder._label_score("vonchusohuu", "nophaitravavonchusohuutiengui") >= 0.95
    assert builder._label_score("vonchusohuu", "nophaitravavonchusohuutiengui",
                                edge_containment=True) < 0.95


def test_a_statement_that_says_tong_tai_san_is_unchanged(builder):
    """The normalisation may not disturb the wording 57 of the 59 archived TCB quarters use."""
    st = _bs([("TỔNG TÀI SẢN", "tong_tai_san", [164_134_583, None]),
              ("TỔNG NỢ PHẢI TRẢ", "tong_no_phai_tra", [149_685_836, None]),
              ("TỔNG VỐN CHỦ SỞ HỮU", "tong_von_chu_so_huu", [14_448_747, None])])
    mapped = builder.map_to_schema(st, "bank")
    assert mapped["tong_tai_san"] == 164_134_583
    assert mapped["viii_von_chu_so_huu"] == 14_448_747
