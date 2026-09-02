# -*- coding: utf-8 -*-
"""A grand total whose own anchor cannot reach it — VCB Q2-2009 (`NST-1`).

`_anchor`'s docstring already names the nesting this file is about: "TỔNG NỢ PHẢI TRẢ" is a
literal PREFIX of "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU", so each scores the full containment
0.95 against the other's row, and the anchors are settled by making them COMPETE — "the
grand-total row matches its own anchor exactly (1.00) and is taken there".

⚠️ **THAT ARGUMENT HAS A PREMISE, AND VCB'S 2009 BALANCE SHEETS BREAK IT.** They print the
grand total as "TỔNG NỢ PHẢI TRẢ, VỐN CSH VÀ LỢI ÍCH CỦA CỔ ĐÔNG THIỂU SỐ" — a comma where the
chart writes "VÀ", and "CSH" for "CHỦ SỞ HỮU" — so the row scores **0.760** against its own
account, never becomes a candidate, and `tong_no_phai_tra` takes the whole balance sheet
(215,651,790,234,750) by containment while the real total liabilities, printed one row above at
200,472,828,741,799, is left with nothing. `reconcile` then compares assets against itself plus
whatever the text fallback calls equity and refuses the statement on ALL 55 layers.

Two halves, and this file pins that NEITHER works alone:

  1. `ABBREV["csh"]` — without it the grand-total row scores 0.760 and there is nothing for
     `tong_no_phai_tra` to yield TO;
  2. the nested-anchor rule in `_anchor` — without it the containment floor 0.95 still outranks
     the expanded row's 0.873, and the short anchor still wins the sort.

⚠️ THE FIXTURE CARRIES THE REAL LABELS, read off the filing at `onnx@200`. A synthetic label
proves nothing about a fuzzy matcher: the whole difficulty is that the wrong answer scores 0.95
and the right one 0.873.
"""
import itertools

import pytest

from web_scraper.cafef_financials import BALANCE_SHEET, REPORTS, FinancialsBuilder
from web_scraper.cafef_pdf_parser import Row, Statement

TEMPLATES = ("bank", "corp", "securities", "insurance")

# HOSE_VCB Q2-2009_bao_cao_tai_chinh_hop_nhat_quy_2_nam_2009.pdf, pages 2-4, `onnx@200`.
# Two columns: this quarter, then the comparative. The figures the statement is judged on close
# EXACTLY, which is how the reading is known to be sound before any gate has seen it:
#     200,472,828,741,799 + 15,080,830,208,647 + 98,131,284,304 = 215,651,790,234,750
VCB_Q2_2009 = [
    ("A", "tai_san_i_tien_mat_vang_bac_da_qui", "A TÀI SẢN I Tiền mặt, vàng bạc, đá quí",
     [3243980648913, 3482209000000]),
    ("II", "tien_gui_tai_nhnn", "II Tiền gửi tại NHNN", [4318170130892, 30561417000000]),
    ("", "cho_vay_khach_hang", "Cho vay khách hàng", [131220995518738, 112792965000000]),
    ("", "co_noi_bang_khac_tong_tai_san_co", "Có nội bảng khác (*) TỔNG TÀI SẢN CÓ",
     [215651790234750, 221950448000000]),
    ("B", "no_phai_tra_va_von_chu_so_huu_i_cac_khoan_no_chinh_phu_va_nh",
     "B NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU I Các khoản nợ Chính phủ và NHNN V.6",
     [5845337295484, 9515633000000]),
    ("III", "tien_gui_cua_khach_hang_v_8", "III Tiền gửi của khách hàng V.8",
     [154848458473932, 157067019000000]),
    ("", "no_tiem_an_va_cam_ket_ngoai_bang_tong_no_phai_tra",
     "nợ tiềm ẩn và cam kết ngoại bảng) TỔNG NỢ PHẢI TRẢ",
     [200472828741799, 208057011000000]),
    ("S", "th_chi_tieu_so_cuoi_t_minh_t_viii_von_va_cac_quy_v_11",
     "S TH. CHỈ TIÊU SỐ CUỐI T MINH T VIII VỐN VÀ CÁC QUỸ V.11",
     [15080830208647, 13790042000000]),
    ("", "von_cua_tctd", "Vốn của TCTD", [12177067630039, 12164475000000]),
    ("a", "von_dieu_le", "a Vốn điều lệ", [12100860260000, 12100860000000]),
    ("", "loi_ich_cua_co_dong_thieu_so", "Lợi ích của cổ đông thiểu số",
     [98131284304, 103395000000]),
    # ⚠️ THE ROW THIS FILE IS ABOUT. OCR cut the label after "LỢI"; the printed line reads
    # "TỔNG NỢ PHẢI TRẢ, VỐN CSH VÀ LỢI ÍCH CỦA CỔ ĐÔNG THIỂU SỐ".
    ("", "tong_no_phai_tra_von_csh_va_loi", "TỔNG NỢ PHẢI TRẢ, VỐN CSH VÀ LỢI",
     [215651790234750, 221950448000000]),
]

ASSETS = 215651790234750
LIABILITIES = 200472828741799
EQUITY_AND_FUNDS = 15080830208647
MINORITY = 98131284304

# The two flattened account texts that nest, and the row key that both reach. Written as
# literals rather than read out of the schema, because a test deriving its expectation from the
# code under test cannot catch the code changing.
ACCOUNT_LIABILITIES = "tongnophaitra"
ACCOUNT_RESOURCES = "tongnophaitravavonchusohuu"
ROW_KEY_RAW = "tongnophaitravoncshvaloi"
ROW_KEY_EXPANDED = "tongnophaitravonchusohuuvaloi"


@pytest.fixture(scope="module")
def builder():
    """A builder reading the repo's own charts of accounts, wherever pytest was started.

    The twelve `schema/*.csv` files are a git-tracked repo INPUT with no producer in the
    pipeline, so a test may depend on them — but the module path globals are relative and
    pytest runs from `src/`. `use_data_root()` is the seam that re-points them.
    """
    from web_scraper import pdf_ocr_job

    pdf_ocr_job.use_data_root()
    return FinancialsBuilder(logger=None)


def _statement(rows=VCB_Q2_2009):
    return Statement(report=BALANCE_SHEET, pages=[2, 3, 4], unit=1, n_columns=2,
                     rows=[Row(label=lab, key=key, number=num, values=list(vals))
                           for num, key, lab, vals in rows])


# ──────────────────────────────────────────────────────────────────────────────
# The defect, and the reading that replaces it
# ──────────────────────────────────────────────────────────────────────────────

def test_the_grand_total_goes_to_the_grand_total_and_not_to_total_liabilities(builder):
    """The row the two anchors fight over is the RESOURCES line, and liabilities keeps its own.

    Both halves of the assertion matter. Before the fix `tong_no_phai_tra` held the grand total
    and `tong_no_phai_tra_va_von_chu_so_huu` held nothing, so the balance sheet described a bank
    whose liabilities equalled its total assets.
    """
    out = builder.map_to_schema(_statement(), "bank")
    assert out["tong_tai_san"] == ASSETS
    assert out["tong_no_phai_tra_va_von_chu_so_huu"] == ASSETS
    assert out["tong_no_phai_tra"] == LIABILITIES


def test_the_statement_then_reconciles(builder):
    """`reconcile` passes — the same call `_parse_cascaded` makes, at layer 1 of 55."""
    st = _statement()
    assert builder.reconcile(st, builder.map_to_schema(st, "bank")) is None


def test_equity_is_left_ABSENT_rather_than_given_a_plausible_row(builder):
    """This filing prints "VIII VỐN VÀ CÁC QUỸ", which is not a name this chart of accounts has.

    ⚠️ §5 rule 2: an absent measurement is absent. The two rows that MENTION "vốn chủ sở hữu"
    are a merged section header ("B NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU | I Các khoản nợ Chính phủ")
    carrying 5,845,337,295,484 of government debt, and the grand total itself — writing either
    into the equity column is `MEN-1`'s defect, and no later gate would refuse it, because
    `reconcile` never needs equity once resources is present.
    """
    out = builder.map_to_schema(_statement(), "bank")
    assert all(c not in out for c in FinancialsBuilder.C_EQUITY)


def test_the_figures_the_filing_prints_close_exactly():
    """The arithmetic that says the READING was never the problem — only the mapping was."""
    assert LIABILITIES + EQUITY_AND_FUNDS + MINORITY == ASSETS


# ──────────────────────────────────────────────────────────────────────────────
# Neither half works alone
# ──────────────────────────────────────────────────────────────────────────────

def test_without_the_csh_expansion_the_grand_total_cannot_reach_its_own_anchor(builder,
                                                                              monkeypatch):
    """Half one. 0.760 is under ANCHOR_MATCH and under the relaxed ANCHOR_MATCH_LONG floor too,
    so the grand-total row is not even a candidate and the defect returns in full."""
    monkeypatch.setattr(FinancialsBuilder, "ABBREV",
                        {k: v for k, v in FinancialsBuilder.ABBREV.items() if k != "csh"})
    score = builder._label_score(ACCOUNT_RESOURCES, ROW_KEY_RAW, edge_containment=True)
    assert score == pytest.approx(0.760, abs=1e-3)
    assert score < FinancialsBuilder.ANCHOR_MATCH_LONG
    assert builder.map_to_schema(_statement(), "bank")["tong_no_phai_tra"] == ASSETS


def test_the_expansion_alone_is_outranked_by_the_containment_floor(builder):
    """Half two. 0.873 clears ANCHOR_MATCH — and 0.95 is still a strictly better SCORE.

    So the sort would hand the SHORT anchor the row anyway, and it is the nested rule, not the
    expansion, that stops it. This is the number that makes the rule necessary rather than tidy:
    containment's 0.95 is a FLOOR awarded because the account APPEARS in the label, never a
    measurement that the label IS the account.
    """
    assert FinancialsBuilder._expand(ROW_KEY_RAW) == ROW_KEY_EXPANDED
    long_ = builder._label_score(ACCOUNT_RESOURCES, ROW_KEY_EXPANDED, edge_containment=True)
    short = builder._label_score(ACCOUNT_LIABILITIES, ROW_KEY_EXPANDED, edge_containment=True)
    assert long_ == pytest.approx(0.873, abs=1e-3)
    assert long_ >= FinancialsBuilder.ANCHOR_MATCH
    assert short == pytest.approx(0.95, abs=1e-9)
    assert short > long_


# ──────────────────────────────────────────────────────────────────────────────
# How far the rule can reach
# ──────────────────────────────────────────────────────────────────────────────

def test_exactly_one_anchor_pair_NESTS_in_the_whole_corpus_of_charts(builder):
    """The blast radius as a fact about the twelve charts, rather than as an argument.

    A prefix pair is the only shape the rule can act on, so counting them bounds it: ONE pair,
    on the bank balance sheet, and it is the pair `_anchor`'s own docstring already names.
    """
    nested = [(template, report, c1, c2)
              for template in TEMPLATES for report in REPORTS
              for c1, a1 in [(c, a.replace("_", ""))
                             for c, a in builder.schema_of(template, report)
                             if c in FinancialsBuilder.ANCHORS]
              for c2, a2 in [(c, a.replace("_", ""))
                             for c, a in builder.schema_of(template, report)
                             if c in FinancialsBuilder.ANCHORS]
              if c1 != c2 and a2.startswith(a1) and len(a2) > len(a1)]
    assert nested == [("bank", BALANCE_SHEET, "tong_no_phai_tra",
                       "tong_no_phai_tra_va_von_chu_so_huu")]


def test_the_csh_expansion_adds_no_account_collision(builder):
    """An ABBREV entry is only safe if two accounts of one chart do not become the same thing.

    Measured the way the `tongcong` entry was — every pair of accounts in every chart, scored
    against each other — and additionally: no chart of accounts contains "csh" AT ALL, so this
    entry can only ever rewrite the ROW side.
    """
    def collisions(abbrev):
        saved = FinancialsBuilder.ABBREV
        FinancialsBuilder.ABBREV = abbrev
        try:
            return {(t, r, c1, c2)
                    for t in TEMPLATES for r in REPORTS
                    for (c1, a1), (c2, a2) in itertools.combinations(builder.schema_of(t, r), 2)
                    if builder._label_score(a1.replace("_", ""), a2.replace("_", ""))
                    >= FinancialsBuilder.SCHEMA_MATCH}
        finally:
            FinancialsBuilder.ABBREV = saved

    with_csh = dict(FinancialsBuilder.ABBREV)
    assert "csh" in with_csh
    assert collisions(with_csh) == collisions({k: v for k, v in with_csh.items()
                                               if k != "csh"})
    assert not [a for t in TEMPLATES for r in REPORTS
                for _c, a in builder.schema_of(t, r) if "csh" in a.replace("_", "")]


# HOSE_CTG Q2-2009, `onnx@200` — the shape the rule must NOT touch, and the shape that caught
# the rule's first version. Here the grand total DOES reach its own anchor, so the competition
# `_anchor` was built on settles the two by itself.
CTG_Q2_2009 = [
    ("", "tong_tai_san", "TỔNG TÀI SẢN", [218561995000000, 193590357000000]),
    ("", "tong_no_phai_tra", "TỔNG NỢ PHẢI TRẢ", [204985759000000, 181254198000000]),
    ("", "tong_von_chu_so_huu", "TỔNG VỐN CHỦ SỞ HỮU", [13381740000000, 12336159000000]),
    ("", "loi_ich_co_dong_thieu_so", "LỢI ÍCH CÓ ĐÔNG THIÊU SỐ", [194496000000, None]),
    ("", "tong_no_phai_tra_von_chu_so_huu_va_loi_ich",
     "TÔNG NỢ PHẢI TRẢ VÔN CHỦ SỞ HỮU VÀ LỢI ÍCH", [218561995000000, 193590357000000]),
]


def test_a_PLAIN_total_liabilities_row_keeps_its_own_figure(builder):
    """⚠️ THE REGRESSION THE FIRST VERSION OF THE RULE CAUSED, PINNED SO IT CANNOT RETURN.

    Containment runs in BOTH directions, so on a row printed plainly as "TỔNG NỢ PHẢI TRẢ" the
    LONGER account scores the flat 0.95 too — and a rule that yielded on the bare EXISTENCE of a
    longer candidate therefore gave total liabilities' own line away. Re-mapping the archive
    measured it at **15 sound statements moved**, this one among them: liabilities went from
    204,985,759 mn to the grand total's 218,561,995 mn and the grand-total column was emptied,
    which is the defect this file exists to remove, in reverse.

    The length ratio separates them: here the short account spans **1.00** of the label and the
    long one 0.50, where on VCB's Q2-2009 grand total it is 0.45 against 0.90.
    """
    out = builder.map_to_schema(_statement(CTG_Q2_2009), "bank")
    assert out["tong_tai_san"] == 218561995000000
    assert out["tong_no_phai_tra"] == 204985759000000
    assert out["tong_no_phai_tra_va_von_chu_so_huu"] == 218561995000000


def test_the_rule_bites_only_where_the_score_and_the_span_DISAGREE(builder):
    """What makes the rule minimal: where they agree, the SORT already settles it.

    Candidates are ranked `(score, length ratio, position)`, so a longer anchor that both scores
    at least as high AND spans more of the row already wins it without any rule. The rule can
    therefore only ever act where the SHORT anchor scores higher — which is only possible when
    it got there by containment — and spans LESS. That is one situation, not a threshold.
    """
    long_row = builder._label_score(ACCOUNT_RESOURCES, ROW_KEY_EXPANDED, edge_containment=True)
    short_row = builder._label_score(ACCOUNT_LIABILITIES, ROW_KEY_EXPANDED,
                                     edge_containment=True)
    assert short_row > long_row                                   # score says the short one
    assert (len(ACCOUNT_RESOURCES) / len(ROW_KEY_EXPANDED)
            > len(ACCOUNT_LIABILITIES) / len(ROW_KEY_EXPANDED))   # span says the long one

    # And on a row printed plainly as "TỔNG NỢ PHẢI TRẢ" they AGREE — the short account matches
    # it exactly and spans the whole label, the long one only contains it — so the sort settles
    # that row on its own and the rule has nothing to do.
    plain = ACCOUNT_LIABILITIES
    assert builder._label_score(ACCOUNT_LIABILITIES, plain, edge_containment=True) == 1.0
    assert builder._label_score(ACCOUNT_RESOURCES, plain, edge_containment=True) ==         pytest.approx(0.95)
    assert len(plain) / len(plain) > len(plain) / len(ACCOUNT_RESOURCES)


def test_a_suffix_containment_still_wins_its_row(builder):
    """⚠️ THE CASE `_claim` EXISTS TO PIN, and the rule must not take it away.

    ACB's Q1-2022 reads "Dự phòng rủi ro khác" and "TỔNG NỢ PHẢI TRẢ" as ONE row. The grand
    total does not reach that row at all, so nothing nests ON it and `tong_no_phai_tra` still
    takes it — which is exactly what separates "a longer anchor also fits THIS ROW" from "a
    longer anchor exists in this chart".
    """
    rows = [("", "du_phong_rui_ro_khac_tong_no_phai_tra",
             "Dự phòng rủi ro khác TỔNG NỢ PHẢI TRẢ", [548693358, 501000000]),
            ("", "tong_tai_san", "TỔNG TÀI SẢN", [611223523, 570000000])]
    out = builder.map_to_schema(_statement(rows), "bank")
    assert out["tong_no_phai_tra"] == 548693358
    assert out["tong_tai_san"] == 611223523
