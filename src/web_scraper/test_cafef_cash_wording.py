"""The cash flow's dated balances under the "Tiền tồn" wording — `GCW-1`.

Pinned without a PDF, a network or an OCR engine: every row below is the real OCR output of
HOSE_GAS, copied from `reports/pdf_ocr/20260907-064620__hose_gas__pdf_ocr/documents/`.

The defect: the corp chart of accounts calls VAS codes 60 and 70 "Tiền và tương đương tiền đầu
kỳ (60)" and "Tiền và tương đương tiền cuối kỳ (70 = 50+60+61)". GAS's filings print **"Tiền tồn
đầu năm"** and **"Tiền tồn cuối năm"** — an older B03 phrasing naming the same two codes with a
different noun. `tientoncuoinam` scores **0.550** against the chart's wording, nowhere near
`SCHEMA_MATCH = 0.80`, so the closing line does not map; `reconcile` REQUIRES a closing balance,
so the whole statement is refused with `no closing cash balance`.

⚠️ **IT IS A FALSE REFUSAL AND IT COST A WHOLE TICKER'S CASH FLOWS.** Measured off the
2026-09-07 T4 run folder at no OCR cost: 33 of GAS's 35 refused cash flows carry that reason, 9
of them at every one of the 100 layers tried, and `tien_ton_cuoi_nam` is the printed label on 28
of 35.

⚠️ **THE OPENING BALANCE IS THE ONE THING THE CLOSING ALIAS MUST NEVER ANSWER**, and it very
nearly does: the two lines are the same three words with one different, and `tientoncuoinam`
scores **0.815** against the OPENING row's `tien_ton_dau_nam` — over the bar. That is
`ANNUAL_WORDING`'s BID Q4-2016 failure (0.804) arriving by a second route, so the period word is
a HARD GATE here and not a score. `test_opening_row_can_never_win_the_closing_slot` is the pin.
"""
import pytest

from web_scraper import cafef_financials as fin
from web_scraper.cafef_pdf_parser import Row, Statement


CLOSE = "hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61"
OPEN = "hdtc_tien_va_tuong_duong_tien_dau_ky_60"


@pytest.fixture(scope="module")
def builder():
    return fin.FinancialsBuilder(logger=None)


def _statement(rows):
    return Statement(report=fin.CASH_FLOW, pages=[5], unit=1, n_columns=2,
                     rows=[Row(label=lb, key=k, number="", values=list(v))
                           for lb, k, v in rows])


# HOSE_GAS Q1-2016, page 5 at onnx@200 — the parsed rows, verbatim.
GAS_Q1_2016 = [
    ("LƯU CHUYÊN TIÊN TU HOẠT ĐỘNG KINH DOANH 1. Lợi nhuận trước thuế", "luu_chuyen_tien_tu_hoat_dong_kinh_doanh_1_loi_nhuan_truoc_th", [1950610722293, 3371071064998]),
    ("2 Điều chính cho các khoản Khâu hao tài sản cổ định", "dieu_chinh_cho_cac_khoan_khau_hao_tai_san_co_dinh", [81, 926654772900]),
    ("Các khoản dự phòng", "cac_khoan_du_phong", [-15765280230, 210289089798]),
    ("Lãi, lỗ chênh lệch tý giá hồi đoái do đánh giá lại các khoản mục", "lai_lo_chenh_lech_ty_gia_hoi_doai_do_danh_gia_lai_cac_khoan", [-33547530271, 853088924]),
    ("tiên tệ có gốc ngoai tế Lai lỗ từ hoạt đông đầu tin", "tien_te_co_goc_ngoai_te_lai_lo_tu_hoat_dong_dau_tin", [-556821837982, -127536734449]),
    ("Chi phi lai vay", "chi_phi_lai_vay", [955, 450]),
    ("3.Lợi nhuận từ hoạt động kinh doanh trước thay đổi vôn lưu", "loi_nhuan_tu_hoat_dong_kinh_doanh_truoc_thay_doi_von_luu", [2147584978846, 4472892349621]),
    ("động (Tăng)/Giam các khoản phải thu", "dong_tang_giam_cac_khoan_phai_thu", [-566826114818, 411]),
    ("TTRNG)/Giảm hàng tôn kho", "ttrng_giam_hang_ton_kho", [-152914089035, 766738349746]),
    ("Tăng/(Giảm) các khoản phải tra", "tang_giam_cac_khoan_phai_tra", [-3546606684, -1520079598112]),
    ("(Tăng)/Giám chi phi trà trước", "tang_giam_chi_phi_tra_truoc", [-49332071782, 104625891895]),
    ("Tang/(Giám) chững khoán kinh doanh Tiên lãi vay đã trả", "tang_giam_chung_khoan_kinh_doanh_tien_lai_vay_da_tra", [-76717589040, -63795008827]),
    ("Thuế thư nhập doanh nghiệp đã nộp", "thue_thu_nhap_doanh_nghiep_da_nop", [-312817176813, -1293319954281]),
    ("Tiền thu khác từ hoạt đông kinh doanh", "tien_thu_khac_tu_hoat_dong_kinh_doanh", [252062, 519233892198]),
    ("Tiên chi khác cho hoạt đông kinh doanh", "tien_chi_khac_cho_hoat_dong_kinh_doanh", [-175118143661, -169638570331]),
    ("Lưu chuyển tiên thuần từ hoạt động kinh doanh", "luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh", [960547439075, 3550916420320]),
    ("LƯU CHUYỂN TIÊN TỪ HOẤT ĐỘNG ĐẦU TU Tiên chi để mua săm, xây dựng TSCĐ và các tài sản dài hạn", "luu_chuyen_tien_tu_hoat_dong_dau_tu_tien_chi_de_mua_sam_xay", [None, -434169771648]),
    ("khác Tiên thu từ thanh lý, nhượng bản TSCĐ và các tài sản dài han", "khac_tien_thu_tu_thanh_ly_nhuong_ban_tscd_va_cac_tai_san_dai", [258, None]),
    ("khác 3. Tiên chi cho vay, mua các công cụ no của đơn vi khác", "khac_3_tien_chi_cho_vay_mua_cac_cong_cu_no_cua_don_vi_khac", [-53850000000, -313000000000]),
    ("4 Tiên thu hồi cho vày, bán lại các công cu nọ của đơn vị khác", "tien_thu_hoi_cho_vay_ban_lai_cac_cong_cu_no_cua_don_vi_khac", [169208000000, None]),
    ("5.Tiên chi đầu tư góp vôn vào đơn vị khác", "tien_chi_dau_tu_gop_von_vao_don_vi_khac", [-545165000000, None]),
    ("6 Tiên thu hồi đầu tư góp vốn vào đơn vị khác", "tien_thu_hoi_dau_tu_gop_von_vao_don_vi_khac", [218540026785, 96505070612]),
    ("6 Tiên thu lãi cho vay, cô tức và lơi nhuận được chia", "tien_thu_lai_cho_vay_co_tuc_va_loi_nhuan_duoc_chia", [290475795563, 276025403906]),
    ("Lưu chuyển tiền thuần từ hoạt động đầu tư", "luu_chuyen_tien_thuan_tu_hoat_dong_dau_tu", [-903759680871, -374639297130]),
    ("MI LƯU CHUYỂN TIÊN TU HOẠT ĐỘNG TAI CHÍNH Tiên thu tù phát hành có phiếu, nhân vôn góp của chủ sở hữu", "mi_luu_chuyen_tien_tu_hoat_dong_tai_chinh_tien_thu_tu_phat_h", [379000000000, None]),
    ("T. Tiền chi trả vôn góp cho các chủ sở hữu, mua lại có phiếu của", "tien_chi_tra_von_gop_cho_cac_chu_so_huu_mua_lai_co_phieu_cua", [None, -39911223937]),
    ("doanh nghiệp đã phát hanh 3. Tiền thu từ đi vay", "doanh_nghiep_da_phat_hanh_3_tien_thu_tu_di_vay", [2849310987008, 127657283918]),
    ("4 Tiên trà nơ gốc vay", "tien_tra_no_goc_vay", [-1840066102774, -1492386419384]),
    ("5. Tiên trà nợ gốc thuê tài chinh", "tien_tra_no_goc_thue_tai_chinh", [-19522555880, None]),
    ("6. Cổ tức, lợi nhuân đã trà cho chủ sở hữu", "co_tuc_loi_nhuan_da_tra_cho_chu_so_huu", [619000, 880]),
    ("Lưu chuyên tiên thuần từ hoạt động tài chính", "luu_chuyen_tien_thuan_tu_hoat_dong_tai_chinh", [-112498290646, -2727998270283]),
    ("Lưu chuyển tiền thuần trong năm", "luu_chuyen_tien_thuan_trong_nam", [-55710532442, 448278852907]),
    ("Tiền tôn đầu năm", "tien_ton_dau_nam", [18030043218216, 944]),
    ("Anh hưởng của thay đối tỷ giá quy đổi ngoại tệ", "anh_huong_cua_thay_doi_ty_gia_quy_doi_ngoai_te", [416, 78359317]),
    ("Tiên tôn cuối năm", "tien_ton_cuoi_nam", [190, 24528362820168]),
]

# HOSE_GAS Q2-2014, page 10 at onnx@200 — the parsed rows, verbatim.
GAS_Q2_2014 = [
    ("Mã CHỈ TIÊU số LƯU CHUYỂN TIÊN TỪ HOẠT ĐỘNG KINH DOANH 1. Lợi nhuận trước thuế", "ma_chi_tieu_so_luu_chuyen_tien_tu_hoat_dong_kinh_doanh_1_loi", [8091139299720, 9083098249148]),
    ("2. Điều chỉnh cho các khoản: Khấu hao tài sản cố định", "dieu_chinh_cho_cac_khoan_khau_hao_tai_san_co_dinh", [1690393965900, 1623098513562]),
    ("Các khoàn dự phòng", "cac_khoan_du_phong", [-17385091098, 18944728162]),
    ("Lỗ chênh lệch tỷ giá hối đoái chưa thực hiện", "lo_chenh_lech_ty_gia_hoi_doai_chua_thuc_hien", [30012068879, 72032465184]),
    ("(Lãi) từ hoạt động đầu tư", "lai_tu_hoat_dong_dau_tu", [-590080433425, None]),
    ("Chi phí lãi vay", "chi_phi_lai_vay", [204732577390, 209755353313]),
    ("3. Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn", "loi_nhuan_tu_hoat_dong_kinh_doanh_truoc_thay_doi_von", [9408812387366, 10454393078964]),
    ("lưu động Thay đối các khoản phải thu", "luu_dong_thay_doi_cac_khoan_phai_thu", [365970361366, -485169789385]),
    ("Thay đổi hàng tồn kho", "thay_doi_hang_ton_kho", [893878500186, None]),
    ("Thay đồi các khoản phải trả (không bao gồm lãi vay phải trà,", "thay_doi_cac_khoan_phai_tra_khong_bao_gom_lai_vay_phai_tra", [-2715134011692, -267873819467]),
    ("thuế thu nhập doanh nghiệp phải nộp)", "thue_thu_nhap_doanh_nghiep_phai_nop", [None, 640277958649]),
    ("Thay đổi chi phí trả trước và tài sản khác", "thay_doi_chi_phi_tra_truoc_va_tai_san_khac", [726536812049, None]),
    ("Tiền lãi vay đã trà", "tien_lai_vay_da_tra", [-210841012117, -247557281740]),
    ("Thuế thu nhập doanh nghiệp đã nộp", "thue_thu_nhap_doanh_nghiep_da_nop", [-1368626507389, None]),
    ("Tiền thu khác từ hoạt động kinh doanh", "tien_thu_khac_tu_hoat_dong_kinh_doanh", [None, 27014880807]),
    ("Tiền chi khác cho hoạt động kinh doanh", "tien_chi_khac_cho_hoat_dong_kinh_doanh", [-169069641429, -356242435351]),
    ("Lưu chuyển tiền thuần từ hoạt động kinh doanh", "luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh", [6931526888340, 8148075988069]),
    ("II LƯU CHUYỀN TIÊN TỪ HOẠT ĐỘNG ĐẦU TƯ 1. Tiền chi để mua săm, xây dựng tài sản cố định", "luu_chuyen_tien_tu_hoat_dong_dau_tu_1_tien_chi_de_mua_sam_xa", [-652036321296, -846453902010]),
    ("2. Tiền thu từ thanh lý tài sản cố định", "tien_thu_tu_thanh_ly_tai_san_co_dinh", [4956364, None]),
    ("3. Tiền chi cho vay, mua công cụ nợ của đơn vị khác", "tien_chi_cho_vay_mua_cong_cu_no_cua_don_vi_khac", [-311230416667, -355637836]),
    ("4. Tiền thu hồi cho vay, bán lại công cụ nợ của đơn vị khác", "tien_thu_hoi_cho_vay_ban_lai_cong_cu_no_cua_don_vi_khac", [68000000000, 48350000000]),
    ("5. Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia", "tien_thu_lai_cho_vay_co_tuc_va_loi_nhuan_duoc_chia", [575980396427, 571827493544]),
    ("Lưu chuyển tiền thuần từ hoạt động đầu tư", "luu_chuyen_tien_thuan_tu_hoat_dong_dau_tu", [-319281385172, -226632046302]),
    ("1. Tiền vay ngắn hạn, dài hạn nhận được", "tien_vay_ngan_han_dai_han_nhan_duoc", [2645300442392, 1064571836283]),
    ("2. Tiền chi trả nợ gốc vay", "tien_chi_tra_no_goc_vay", [-3042512974811, -2022733824423]),
    ("3. Tiền trả nợ thuê tài chính", "tien_tra_no_thue_tai_chinh", [-18942957334, None]),
    ("4. Cổ tức, lợi nhuận đã trả cho chủ sở hữu", "co_tuc_loi_nhuan_da_tra_cho_chu_so_huu", [-4168882560000, -1914318474270]),
    ("Lưu chuyển tiền thuần từ hoạt động tài chính", "luu_chuyen_tien_thuan_tu_hoat_dong_tai_chinh", [-4585038049753, -2893031717766]),
    ("Lưu chuyển tiền thuần trong kỳ", "luu_chuyen_tien_thuan_trong_ky", [2027207453415, 5028412224001]),
    ("Tiền và tương đương tiền đầu kỳ", "tien_va_tuong_duong_tien_dau_ky", [None, 12753084518890]),
    ("Ảnh hưởng của thay đồi tỳ giá hối đoái quy đổi ngoại tệ", "anh_huong_cua_thay_doi_ty_gia_hoi_doai_quy_doi_ngoai_te", [None, -290303915]),
    ("Tiền và tương đương tiền cuối kỳ", "tien_va_tuong_duong_tien_cuoi_ky", [30401800, 81206438976]),
]

def test_the_wording_scores_under_the_bar_which_is_why_the_flag_exists(builder):
    """0.550 against a 0.80 bar — the measurement the whole block rests on."""
    account = "tienvatuongduongtiencuoiky"
    assert builder._label_score(account, "tientoncuoinam") == pytest.approx(0.550, abs=0.005)
    assert builder._label_score(account, "tientoncuoinam") < builder.SCHEMA_MATCH


def test_without_the_flag_the_statement_is_refused_exactly_as_it_was(builder):
    """The default path is UNCHANGED — this is what makes the block additive."""
    st = _statement(GAS_Q1_2016)
    mapped = builder.map_to_schema(st, "corp")
    assert CLOSE not in mapped
    assert builder.reconcile(st, mapped) == "no closing cash balance"


def test_with_the_flag_the_closing_balance_maps_and_reconcile_passes(builder):
    st = _statement(GAS_Q1_2016)
    mapped = builder.map_to_schema(st, "corp", cash_wording=True)
    assert CLOSE in mapped
    assert OPEN in mapped
    assert builder.reconcile(st, mapped) is None


def test_opening_row_can_never_win_the_closing_slot(builder):
    """⚠️ The measured hazard: the alias scores 0.815 against the OPENING row, over the bar.

    The period word is a gate, so the score is never consulted — and each balance keeps its own
    figure even on this layer's badly damaged read.
    """
    # The alias itself is what scores over the bar against the OPENING row — 0.815 — and
    # `_label_score` returns a `max` over the alternatives, so without the gate that is what
    # the closing account would be scored at.
    assert builder._label_score("tientoncuoinam", "tientondaunam") == pytest.approx(0.815,
                                                                                   abs=0.005)
    assert builder._label_score("tienvatuongduongtiencuoiky", "tientondaunam",
                                cash_wording=True) == 0.0
    assert builder._label_score("tienvatuongduongtiendauky", "tientoncuoinam",
                                cash_wording=True) == 0.0

    st = _statement(GAS_Q1_2016)
    mapped = builder.map_to_schema(st, "corp", cash_wording=True)
    assert mapped[OPEN] == 18030043218216        # "Tiền tồn đầu năm", its own figure
    assert mapped[CLOSE] != mapped[OPEN]


def test_a_statement_that_already_maps_is_untouched(builder):
    """The alias is scored as an alternative and returned as a `max`, so it can only RAISE.

    HOSE_GAS Q2-2014 prints the chart's own wording — `tien_va_tuong_duong_tien_cuoi_ky` —
    and already maps; turning the flag on must change nothing about it. (Its figure is OCR
    damage that `sane` refuses on its own; what is pinned here is the MAPPING.)
    """
    st = _statement(GAS_Q2_2014)
    assert (builder.map_to_schema(st, "corp")
            == builder.map_to_schema(st, "corp", cash_wording=True))


def test_the_aliases_name_no_account_on_any_of_the_twelve_charts(builder):
    """`NST-1`'s safety test: an alias that IS an account puts two real lines in competition."""
    charts = [(t, r) for t in ("bank", "corp", "securities", "insurance")
              for r in fin.REPORTS]
    every = {a.replace("_", "")
             for t, r in charts for _, a in (builder.schema_of(t, r) or ())}
    for aliases in builder.CASH_WORDING.values():
        for alias in aliases:
            assert alias not in every, alias


def test_the_flag_is_a_widening_and_no_strict_layer_runs_after_it():
    """`is_strict` must count it, or `max(strict)` moves past the block it is meant to bound."""
    layers = fin.FinancialsBuilder.LAYERS
    assert any(l.cash_wording for l in layers)
    assert not any(l.is_strict for l in layers if l.cash_wording)
    first = min(i for i, l in enumerate(layers) if l.cash_wording)
    assert not any(l.is_strict for l in layers[first:])


def test_the_block_is_contiguous_and_past_every_strict_layer():
    """Only a statement every layer before it refused may reach these.

    ⚠️ **THIS TEST SAID "LAST IN THE CASCADE" UNTIL 2026-09-07 AND THAT WAS THE WRONG
    INVARIANT.** `GTR-1` appended a widening block of its own behind this one and the
    assertion failed on a change that broke nothing — the property that keeps `cash_wording`
    safe is that it runs after every STRICT layer, not that no other widening follows it.
    Pinning "last" makes the next widening look like a regression, which is a test teaching
    the wrong lesson to whoever adds one.
    """
    layers = fin.FinancialsBuilder.LAYERS
    at = [i for i, l in enumerate(layers) if l.cash_wording]
    assert len(at) == 5
    assert at == list(range(at[0], at[0] + 5)), "the block must stay contiguous"
    assert max(i for i, l in enumerate(layers) if l.is_strict) < at[0]
