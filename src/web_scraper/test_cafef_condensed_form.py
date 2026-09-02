"""The condensed disclosure form: the VNI unit spelling (`LGU-1`) and Mẫu CBTT-03's 4-line P&L.

No PDF, no network, no OCR engine — every case is built from the text and rows a parse would
have produced, which is the only part these two changes touch.
"""
import inspect

import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import (CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser, Row, Statement)


@pytest.fixture
def parser():
    return PdfParser(engine="onnx")


@pytest.fixture
def builder():
    return FinancialsBuilder()


def _pages(*texts):
    """{index: page} carrying only what these two features read — the text."""
    return {i: {"text": t, "kind": INCOME_STATEMENT, "words": [], "width": 612.0}
            for i, t in enumerate(texts)}


# ── LGU-1: the unit declared in a pre-Unicode encoding ──────────────────────────────────

def test_the_vni_spelling_of_trieu_dong_declares_millions(parser):
    """ACB Q3-2009's text layer is VNI-Times: `ñ` is `đ` and a tone mark is its own character.

    `norm` strips accents; it does not know VNI. So "Trieäu ñoàng" normalises to `trieaunoang`
    and neither original needle sees it — every figure was read as đồng, a uniform 10^6 error
    that reconciles perfectly against itself.
    """
    assert parser.norm("ÑVT : Trieäu ñoàng").replace(" ", "") == "nvttrieaunoang"
    assert parser.declared_unit(_pages("ÑVT : Trieäu ñoàng"), [0]) == 1_000_000
    assert parser.unit_of(_pages("ÑVT : Trieäu ñoàng"), [0]) == 1_000_000


def test_the_unicode_spellings_still_declare_millions(parser):
    for text in ("Đơn vị tính: Triệu đồng", "Triệu VNĐ"):
        assert parser.declared_unit(_pages(text), [0]) == 1_000_000


def test_a_statement_that_names_no_unit_is_still_UNKNOWN_not_dong(parser):
    """⚠️ The whole point of `declared_unit` — silence and "printed in đồng" must not be one
    answer. Widening the needle set must not turn silence into a value."""
    assert parser.declared_unit(_pages("Stt Chỉ tiêu 30/09/2009"), [0]) is None


def test_the_tcvn3_spelling_is_deliberately_NOT_matched(parser):
    """⚠️ TCVN3/ABC writes "TriÖu ®ång" -> `triouang`, and it has **0 hits** in this corpus.

    Adding an unmeasured needle is §5 rule 2 in reverse: a match nothing has ever needed, that
    nothing has ever tested. Recorded as a decision so the next reader does not "fix" it.
    """
    assert parser.declared_unit(_pages("§VT : TriÖu ®ång"), [0]) is None


# ── the condensed P&L fingerprint ───────────────────────────────────────────────────────

CONDENSED = ("II.B. KẾT QUẢ HOẠT ĐỘNG KINH DOANH ÑVT : Trieäu ñoàng "
             "Stt Chỉ tiêu I Tổng thu nhập II Tổng chi phí "
             "III Lợi nhuận trước thuế IV Lợi nhuận sau thuế")
FULL_PL = ("BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH Đơn vị: Triệu đồng "
           "1 Thu nhập lãi và các khoản thu nhập tương tự I Thu nhập lãi thuần "
           "Chi phí hoạt động XI Tổng lợi nhuận trước thuế")


def test_the_condensed_pl_is_recognised_by_its_own_two_summary_lines(parser):
    assert parser.condensed_income(_pages(CONDENSED), [0]) is True


def test_a_full_income_statement_is_not_condensed(parser):
    """A full bank P&L prints "Thu nhập lãi thuần", never a line called simply "Tổng thu nhập"."""
    assert parser.condensed_income(_pages(FULL_PL), [0]) is False


def test_both_summary_lines_are_required(parser):
    """"Tổng chi phí" alone appears in an operating-expense note."""
    assert parser.condensed_income(_pages("Tổng chi phí hoạt động"), [0]) is False
    assert parser.condensed_income(_pages("Tổng thu nhập"), [0]) is False


def test_the_fingerprint_is_scoped_to_the_statements_own_pages(parser):
    """⚠️ **THE SCOPE IS THE MEASUREMENT.** Searched over a whole document the same two words
    also hit VCB's 44-page Q4-2009 filings, where they sit in a note: 4 documents matched and 2
    of them wrongly. Scoped to the P&L's own pages the count is 2 of 1,196, both genuine."""
    pages = _pages(FULL_PL, "phụ lục: Tổng thu nhập và Tổng chi phí theo bộ phận")
    assert parser.condensed_income(pages, [0]) is False       # the P&L's page
    assert parser.condensed_income(pages, [0, 1]) is True      # …with the note swept in


# ── the floor, and what may lower it ────────────────────────────────────────────────────

def _short_pl(condensed: bool) -> Statement:
    """The four lines Mẫu CBTT-03 prints, one of which maps to the PBT anchor."""
    rows = [Row(label="Tổng thu nhập", key="tong_thu_nhap",
                number="I", values=[3_033_480]),
            Row(label="Tổng chi phí", key="tong_chi_phi",
                number="II", values=[2_391_731]),
            Row(label="Lợi nhuận trước thuế", key="loi_nhuan_truoc_thue",
                number="III", values=[641_749]),
            Row(label="Lợi nhuận sau thuế", key="loi_nhuan_sau_thue",
                number="IV", values=[496_469])]
    return Statement(report=INCOME_STATEMENT, pages=[3], unit=1_000_000, n_columns=2,
                     rows=rows, condensed_income=condensed)


PBT = {"xi_tong_loi_nhuan_truoc_thue": 641_749_000_000}


def test_a_four_line_condensed_pl_is_refused_by_default(builder):
    assert builder.reconcile(_short_pl(True), PBT) == "only 4 rows parsed"


def test_it_is_accepted_when_the_layer_permits_AND_the_statement_has_the_evidence(builder):
    assert builder.reconcile(_short_pl(True), PBT, condensed_income=True) is None


def test_the_flag_alone_does_not_lower_the_floor(builder):
    """⚠️ Otherwise it is a slackened threshold rather than an admission on evidence — and a
    32-page filing whose cash flow classifies 2 rows would walk straight through it."""
    assert builder.reconcile(_short_pl(False), PBT,
                             condensed_income=True) == "only 4 rows parsed"


def test_the_evidence_alone_does_not_lower_the_floor(builder):
    """…and the layer must permit it, so the widening cannot take effect at layer 1, where
    nothing has yet failed."""
    assert builder.reconcile(_short_pl(True), PBT) == "only 4 rows parsed"


def test_the_pbt_anchor_is_still_required(builder):
    """The floor is not what proves this statement — `reconcile` still demands the anchor.

    ⚠️ The anchor has to be taken away at the SOURCE, not merely left out of `mapped`: with an
    empty mapping `reconcile` falls back to `st.find`, which reads the row's own printed label
    and answers correctly. That fallback is the point of it, so a test passing `{}` measures
    nothing — it was written that way first and passed for the wrong reason.
    """
    st = _short_pl(True)
    # REPLACED, not removed: dropping it takes the statement to 3 rows and the floor answers
    # first, which would test the floor a second time and the anchor not at all.
    st.rows = [Row(label="Cổ tức trên mỗi cổ phiếu", key="co_tuc_tren_moi_co_phieu",
                   number="VIII", values=[0]) if "truoc_thue" in r.key else r
               for r in st.rows]
    assert builder.reconcile(st, {}, condensed_income=True) == "no profit before tax"


def test_below_the_forms_own_length_it_is_still_refused(builder):
    st = _short_pl(True)
    st.rows = st.rows[:3]
    assert builder.reconcile(st, PBT, condensed_income=True) == "only 3 rows parsed"


def test_only_an_income_statement_is_ever_marked_condensed():
    """⚠️ VIC Q3-2008 classifies **2 rows** as its cash flow across a 32-page filing and carries
    the "Mẫu CBTT-03" boilerplate on those pages. Keyed on that marker the floor would have been
    lowered for it; keyed on a P&L line it cannot be — and `parse()` sets the field for the
    income statement only, so no other report can carry the evidence however its pages read.
    """
    src = inspect.getsource(PdfParser.parse)
    assert "condensed_income=(report == INCOME_STATEMENT" in src
    assert Statement(report=CASH_FLOW, pages=[1], unit=1, n_columns=2).condensed_income is False


# ── the layer ───────────────────────────────────────────────────────────────────────────

def test_the_condensed_layer_carries_the_document_unit_and_there_is_no_bare_one():
    """⚠️ **THE PAIRING IS THE SAFETY PROPERTY, not a convenience.** The condensed form prints
    its unit once in the page-1 header while the P&L is on page 3, so ACB Q2-2009 declares
    nothing on its own pages; a bare `+condensed` layer accepts a pre-tax profit of 868,056
    **đồng**. These are a ticker's earliest quarters, where `sane`'s band is empty by
    construction (`BND-1`) — the one gate that could catch it is guaranteed off.
    """
    cond = [l for l in FinancialsBuilder.LAYERS if l.condensed_income]
    assert cond, "no layer carries condensed_income"
    for l in cond:
        assert l.unit_from_document, f"{l.name} widens acceptance without fixing the unit"


def test_the_condensed_layer_runs_last():
    """It widens what may be accepted, so only a statement that defeated every other layer
    reaches it.

    ⚠️ This asserted `cond[-1] == len(layers) - 1` until 2026-08-31 — that the condensed layer
    is literally the last element. That is the same POSITION assertion
    `test_the_span_layers_run_late_and_relaxed` records having already outgrown once: appending
    the `+joinlost` layers broke it while changing nothing about when the condensed floor
    applies. The property being guarded is the ORDER relative to the strict layers, so that is
    what is asserted; a widening layer appended after this one is legitimate, another STRICT
    layer appended after it is not, and only the second now fails.
    """
    layers = FinancialsBuilder.LAYERS
    cond = [i for i, l in enumerate(layers) if l.condensed_income]
    assert cond, "no layer carries condensed_income"
    # ⚠️ `ParseLayer.is_strict`, not a fifth private copy of the flag list — see its
    # docstring. Four files kept their own and each had to be edited whenever a widening
    # block was added; the one that was forgotten would have counted the NEW layers as
    # strict and moved `max(strict)` past the block it exists to bound.
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    assert min(cond) > max(strict)


def test_the_cascade_passes_the_flag_through():
    """A guard wired to nothing is a guard that is off — `P39` shipped exactly that defect."""
    src = inspect.getsource(FinancialsBuilder._parse_cascaded)
    assert "condensed_income=layer.condensed_income" in src


def test_the_flag_exists_on_ParseLayer_and_defaults_off():
    assert ParseLayer("x", "onnx", 200).condensed_income is False
