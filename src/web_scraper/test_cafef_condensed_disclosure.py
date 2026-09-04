"""The CONDENSED disclosure form and the three defects around it — no PDF, no engine.

Mẫu CBTT-03 (Thông tư 38/2007) lets an issuer publish a two-page "BÁO CÁO TÀI CHÍNH" whose
only heading is the FILING's: FPT's Q1-2009 and Q3-2010 print `BÁO CÁO TÀI CHÍNH HỢP NHẤT`
over `STT | Nội dung | Số dư cuối kỳ | Số dư đầu năm` on page 1 and `STT | Chỉ tiêu | Kỳ báo
cáo | Lũy kế` on page 2, and no statement title anywhere. Three separate things then refuse a
statement whose every figure is readable:

  `CDF-2`  `_page_kind` has no title to score and no form code to read, so both pages come back
           `None` and all three statements are reported `no such statement on any page of this
           filing` — the one refusal this repo treats as PERMANENT (`SET-2`).
  `CDF-3`  the NGUỒN VỐN side of a Decision 15/2006 balance sheet has FOUR top-level lines
           where Circular 200 has two, so `SEC-1`'s section-sum gate falls short by exactly
           `Nguồn kinh phí và quỹ khác` + `Lợi ích của cổ đông thiểu số`.
  `CDF-1`  the chart of accounts carries the VAS CODE FORMULA at the end of several account
           names — `tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61` — which costs 0.035 of the
           match and is not part of the name.

⚠️ ALL THREE ARE MEASURED ON FPT Q1-2009, whose balance sheet reads assets == resources ==
5,966,094,898,667 to the đồng and whose income statement reconciles with 14 mapped items.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer, REPORTS, parse_key
from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT,
                                          PdfParser)


@pytest.fixture
def b():
    return FinancialsBuilder(logger=None)


# ── CDF-1 · the VAS code formula at the END of an account name ────────────────────────

def test_the_trailing_code_formula_is_stripped_from_the_account_name(b):
    """⚠️ `INDEX_RE` strips a LEADING index because it is bookkeeping and not part of the name.
    The same annotation is printed at the END of several accounts and was left on."""
    corp = dict(b.schema_of("corp", CASH_FLOW))
    assert corp["hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61"] == \
        "tien_va_tuong_duong_tien_cuoi_ky"
    assert corp["hdtc_tien_va_tuong_duong_tien_dau_ky_60"] == "tien_va_tuong_duong_tien_dau_ky"


def test_it_is_what_lets_a_filing_that_says_CAC_KHOAN_reach_the_closing_balance(b):
    """⚠️ THE MEASUREMENT THAT FORCED IT. FPT prints "Tiền và CÁC KHOẢN tương đương tiền cuối
    kỳ"; against the un-stripped account that is **0.765** with `SCHEMA_MATCH` at 0.80 — and
    the OPENING account, four characters shorter, sits at 0.754 right behind it. Stripped, the
    closing account scores 0.867 and the opening 0.780: the right row wins by a WIDER margin
    than before, which is the direction that matters."""
    from difflib import SequenceMatcher

    row = PdfParser.norm("Tiền và các khoản tương đương tiền cuối kỳ").replace(" ", "")
    corp = dict(b.schema_of("corp", CASH_FLOW))
    close = PdfParser.norm(corp["hdtc_tien_va_tuong_duong_tien_cuoi_ky_70_50_60_61"]) \
        .replace(" ", "")
    opening = PdfParser.norm(corp["hdtc_tien_va_tuong_duong_tien_dau_ky_60"]).replace(" ", "")
    s_close = SequenceMatcher(None, close, row).ratio()
    s_open = SequenceMatcher(None, opening, row).ratio()
    assert s_close >= b.SCHEMA_MATCH
    assert s_open < b.SCHEMA_MATCH
    assert s_close - s_open > 0.05, "the closing account must win by more than a rounding"


def test_a_strip_that_would_COLLIDE_is_refused(b):
    """⚠️ THE GUARD IS THE MEASUREMENT, AND IT SEPARATES 10 CODES FROM 3 DISAMBIGUATORS.
    `securities`' cash flow prints "Tiền" twice and numbers the second one; stripping that
    digit would give one chart two accounts with one name and the ordered walk could no longer
    tell them apart. Those three are exactly the entries whose stripped form is ALREADY another
    account of the same chart, so the collision test names neither population."""
    sec = dict(b.schema_of("securities", CASH_FLOW))
    assert sec["hdtc_tien"] == "tien"
    assert sec["hdtc_tien_2"] == "tien_2", "the disambiguator must survive"


def test_across_all_twelve_charts_the_strip_is_bounded_and_collision_free(b):
    """⚠️ THE BLAST RADIUS AS A COUNT, so a chart that gains a line cannot quietly widen it."""
    stripped, collided = 0, 0
    for tpl in ("bank", "corp", "securities", "insurance"):
        for rep in REPORTS:
            items = b.schema_of(tpl, rep)
            for col, account in items:
                raw = col
                for p in b.COL_PREFIXES:
                    if raw.startswith(p):
                        raw = raw[len(p):]
                        break
                raw = b.INDEX_RE.sub("", raw) or raw
                if b.TRAILING_CODE_RE.sub("", raw) != raw:
                    if account == raw:
                        collided += 1          # the strip was refused
                    else:
                        stripped += 1
            # ⚠️ THE INVARIANT IS THAT THE STRIP CREATES NO NEW DUPLICATE — not that a chart
            # has none. It has: the bank balance sheet carries 90 accounts under 84 distinct
            # names, because a section and its subtotal reduce to the same text once the
            # prefix and the numbering are gone. `test_cafef_deskew` records the same thing.
            unstripped = []
            for col, _account in items:
                raw = col
                for p in b.COL_PREFIXES:
                    if raw.startswith(p):
                        raw = raw[len(p):]
                        break
                unstripped.append(b.INDEX_RE.sub("", raw) or raw)
            assert len(set(a for _c, a in items)) == len(set(unstripped)), (tpl, rep)
    assert (stripped, collided) == (10, 3), (stripped, collided)


# ── CDF-2 · the form prints no statement title at all ─────────────────────────────────

BS_TEXT = ("CÔNG TY CỔ PHẦN FPT\nBÁO CÁO TÀI CHÍNH HỢP NHẤT\n"
           "STT\nNội dung\nSố dư cuối kỳ\nSố dư đầu năm\n"
           "I Tài sản ngắn hạn\nIII Tổng tài sản\nIV Nợ phải trả\nV Vốn chủ sở hữu\n")
IS_TEXT = ("CÔNG TY CỔ PHẦN FPT\nBÁO CÁO TÀI CHÍNH HỢP NHẤT\n"
           "STT\nChỉ tiêu\nKỳ báo cáo\nLũy kế\n"
           "1 Doanh thu thuần về bán hàng và dịch vụ\n2 Giá vốn hàng bán\n"
           "3 Lợi nhuận gộp về bán hàng và dịch vụ\n")


def _pages(*texts, words=40):
    """`scan`'s page dict, with enough numeric boxes to clear `MIN_TABLE_WORDS`."""
    boxes = [(400.0, 10.0 * i, 460.0, 10.0 * i + 9.0, "1.234.567", 0, 0, i)
             for i in range(words)]
    return {i: {"text": t, "words": list(boxes), "kind": None, "from_form": False,
                "width": 612.0}
            for i, t in enumerate(texts)}


def _parser(**flags):
    p = PdfParser.__new__(PdfParser)
    p.condensed_form = True
    for k, v in flags.items():
        setattr(p, k, v)
    return p


def test_the_two_pages_are_named_by_the_lines_only_that_statement_prints():
    pages = _pages(BS_TEXT, IS_TEXT)
    _parser()._classify_condensed(pages)
    assert pages[0]["kind"] == BALANCE_SHEET
    assert pages[1]["kind"] == INCOME_STATEMENT


def test_neither_fingerprint_can_hit_the_other_page():
    """⚠️ The two are disjoint by construction, and that is why one pass over the pages is
    enough: a balance sheet never prints "giá vốn hàng bán" and a P&L never prints all three
    of total assets, liabilities and owners' equity."""
    ns_bs = PdfParser.norm(BS_TEXT).replace(" ", "")
    ns_is = PdfParser.norm(IS_TEXT).replace(" ", "")
    assert all(n in ns_bs for n in PdfParser.CONDENSED_BS)
    assert not all(n in ns_is for n in PdfParser.CONDENSED_BS)
    assert all(n in ns_is for n in PdfParser.CONDENSED_IS)
    assert not all(n in ns_bs for n in PdfParser.CONDENSED_IS)


def test_a_page_that_is_not_a_TABLE_is_left_alone():
    """A cover page naming the three totals in prose is refused the way it always was —
    `MIN_TABLE_WORDS` is untouched and does the work."""
    pages = _pages(BS_TEXT, words=PdfParser.MIN_TABLE_WORDS - 1)
    _parser()._classify_condensed(pages)
    assert pages[0]["kind"] is None


def test_a_page_the_classifier_ALREADY_named_is_never_re_named():
    pages = _pages(BS_TEXT)
    pages[0]["kind"] = CASH_FLOW
    _parser()._classify_condensed(pages)
    assert pages[0]["kind"] == CASH_FLOW


def test_the_flag_is_off_by_default_and_is_a_PARSE_key():
    """It names pages the classifier left unnamed, which is `notes_head`'s class exactly — so
    two layers differing only in it must not share a parse. ⚠️ And it is deliberately NOT an
    `ocr_key`: it re-labels pages `scan` has already read."""
    from web_scraper.cafef_financials import ocr_key

    assert ParseLayer("x", "onnx", 200).condensed_form is False
    a = ParseLayer("a", "onnx", 200)
    c = ParseLayer("c", "onnx", 200, condensed_form=True)
    assert parse_key(a) != parse_key(c)
    assert ocr_key(a) == ocr_key(c)


def test_a_condensed_layer_is_a_WIDENING_one_and_runs_after_every_strict_read():
    layers = FinancialsBuilder.LAYERS
    flagged = [i for i, l in enumerate(layers) if l.condensed_form]
    assert flagged, "no +condensed layer in the cascade"
    assert all(not layers[i].is_strict for i in flagged)
    assert min(flagged) > max(i for i, l in enumerate(layers) if l.is_strict)


def test_it_never_ships_without_the_document_unit():
    """⚠️ THE RULE `annual_tail` AND `condensed_income` EACH EARNED: a layer that widens what
    is ACCEPTED may not also be the layer that reads the wrong unit. The condensed form prints
    its unit once, in the page-1 header, while the P&L is on page 2 — and these are a ticker's
    EARLIEST quarters, where `sane`'s band is empty by construction and a 10^6 error cannot be
    caught by anything."""
    for layer in FinancialsBuilder.LAYERS:
        if layer.condensed_form:
            assert layer.unit_from_document, layer.name


# ── CDF-3 · four top-level NGUỒN VỐN lines under Decision 15/2006 ─────────────────────

def test_the_section_sum_carries_the_pre_2015_lines_as_OPTIONAL_terms(b):
    parts, optional, total = b.SECTION_SUMS[1]
    assert parts == ("c_no_phai_tra", "d_von_chu_so_huu")
    assert total == "tong_cong_nguon_von"
    assert set(optional) == {"ii_nguon_kinh_phi_va_quy_khac_430",
                             "i_11_loi_ich_co_dong_khong_kiem_soat"}
    # …and the assets side has none: `A + B` is the whole of it under either circular.
    assert b.SECTION_SUMS[0][1] == ()


def test_every_optional_term_carries_a_TEXT_needle(b):
    """⚠️ WITHOUT ONE THEY CANNOT BE FOUND AT ALL ON THE FORM THAT NEEDS THEM. Decision
    15/2006 calls the minority interest "lợi ích của cổ đông THIỂU SỐ" where Circular 200
    renames it "lợi ích cổ đông KHÔNG KIỂM SOÁT", so the pre-2015 row maps to no column of the
    corp balance-sheet chart and only `Statement.find` can reach it."""
    for _parts, optional, _total in b.SECTION_SUMS:
        for c in optional:
            assert b.SECTION_EXTRA_TEXT.get(c), c


def test_the_minority_wording_is_NOT_an_ACCOUNT_WORDING_alias(b):
    """⚠️ MEASURED, AND IT IS WHY THE TEXT NEEDLE EXISTS. `loi_ich_cua_co_dong_thieu_so` IS
    ITSELF AN ACCOUNT — on the bank balance sheet, the bank income statement and the corp
    income statement — so offering it as an alias would put two real accounts in competition
    for one row, which is `NST-1`'s hazard."""
    from web_scraper.cafef_pdf_parser import PdfParser as P

    alias = "loiichcuacodongthieuso"
    owners = [(t, r) for t in ("bank", "corp", "securities", "insurance") for r in REPORTS
              if alias in [P.norm(a).replace(" ", "") for _c, a in b.schema_of(t, r)]]
    assert owners, "the alias names no account — then the argument above is stale"
    assert all(alias not in aliases
               for aliases in FinancialsBuilder.ACCOUNT_WORDING.values())


def test_ACCOUNT_WORDING_is_keyed_by_report_so_an_alias_cannot_reach_another_chart(b):
    """⚠️ The corp income statement's PBT needs "lợi nhuận trước thuế" — the condensed form's
    wording, 0.77 against the chart's own "tổng lợi nhuận KẾ TOÁN trước thuế" — and that
    spelling IS an account of the corp CASH FLOW, where it can never meet this chart."""
    assert (INCOME_STATEMENT, "tongloinhuanketoantruocthue") \
        in FinancialsBuilder.ACCOUNT_WORDING
    assert b.account_aliases(INCOME_STATEMENT, "tong_loi_nhuan_ke_toan_truoc_thue") \
        == ("loinhuantruocthue",)
    assert b.account_aliases(CASH_FLOW, "tong_loi_nhuan_ke_toan_truoc_thue") == ()
    # …and a `None` entry still reaches every report, which is what the original two were.
    assert b.account_aliases(BALANCE_SHEET, "von_chu_so_huu") == ("vonvacacquy",)
    assert b.account_aliases(CASH_FLOW, "von_chu_so_huu") == ("vonvacacquy",)
