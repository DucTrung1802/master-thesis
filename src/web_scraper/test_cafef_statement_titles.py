"""`VAS-3` and `NOT-2` — two ways a page's own title failed to identify it.

Both were found on FPT's 2008-2010 filings, which carry a real TEXT LAYER, so both were
diagnosed and measured without an OCR engine.

`VAS-3` — **the pre-2015 VAS wording puts "SẢN XUẤT" inside the income statement's title.**
Decision 15/2006 heads the form "BÁO CÁO KẾT QUẢ HOẠT ĐỘNG **SẢN XUẤT** KINH DOANH"; Circular
200/2014 dropped the two words, and the needle is the later spelling. `_title_score` matches a
CONTIGUOUS substring, so the older wording scores **0.696** against a bar of 0.80 — the page
classified as NOTHING, `_fill_continuations` handed it to the balance sheet running above it,
and the income statement was reported `only 1 rows parsed` from an unrelated page.

`NOT-2` — **an EXACT statement title lost to an INEXACT notes verdict.** `_page_kind` took the
notes verdict BEFORE comparing the three titles, so a page whose own title is printed verbatim
read as a note on the 0.8125 fragment `NOT-1` records. ⚠️ `column_header_blind` (`NOT-1`)
cannot reach it: that fix re-takes the notes verdict without the table's column-heading ROW,
which works when the row is one line, and these filings emit each narrow heading CELL as its
own line — `STT` / `TÀI SẢN` / `Mã ` / `số ` / `Thuyết ` / `minh` / `Số cuối quý`. No single
line carries both of `COLUMN_HEADER_NS`, and the form says `TÀI SẢN` where that test looks for
`chỉ tiêu`.

⚠️ **THE COST OF `NOT-2` WAS A SECOND DEFECT TWO PAGES DOWNSTREAM.** The page it lost is the
balance sheet's FIRST page, which is the one carrying the `Mã số` column heading — so
`_code_column` had nothing to read, the item-code column survived as a value column, and
`TỔNG CỘNG TÀI SẢN` was read as **270**. That is `MSO-1`'s symptom produced by a
classification failure, not by a heading the recogniser mangled.

⚠️ **BOTH ARE DEFAULT-PATH CHANGES, MEASURED OVER THE 7,404 TEXT-LAYER PAGES OF THE EIGHT
PARSED TICKERS BEFORE THEY SHIPPED**: `VAS-3` moves **11 pages, every one `None` ->
income_statement**; `NOT-2` moves **10 pages, every one `notes` -> balance_sheet**. Not one
page moves BETWEEN statements and not one is lost — they are recoveries, in both directions.
"""
from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, NOTES,
                                          PdfParser)

# FPT Q3-2008 page 5, header block, as the filing's own text layer prints it.
OLD_VAS_INCOME = ("CONG TY CP PHAT TRIEN DAU TU CONG NGHE FPT\n"
                  "89 Lang Ha, Ha noi, Viet Nam\n"
                  "BAO CAO TAI CHINH HOP NHAT QUY III\n"
                  "BAO CAO KET QUA HOAT DONG SAN XUAT KINH DOANH\n"
                  "Don vi: VND\n"
                  "Nam nay\nNam truoc\n")

# FPT Q3-2008 page 2 — the balance sheet's FIRST page. Each narrow column heading is its own
# line, which is what `column_header_blind` cannot see, and the first heading is `TAI SAN`.
SPLIT_COLUMN_HEADINGS = ("CONG TY CP PHAT TRIEN DAU TU CONG NGHE FPT\n"
                         "89 Lang Ha, Ha noi, Viet Nam\n"
                         "BAO CAO TAI CHINH HOP NHAT QUY III\n"
                         "BANG CAN DOI KE TOAN\n"
                         "Don vi: VND\n"
                         "STT\nTAI SAN\nMa \nso \nThuyet \nminh\nSo cuoi quy\n")


def _parser(**flags):
    p = PdfParser.__new__(PdfParser)          # no OCR engine, no models, no PDF
    p.loose_form_code = False
    p.title_over_form = False
    p.column_header_blind = False
    for k, v in flags.items():
        setattr(p, k, v)
    return p


# ── VAS-3 ─────────────────────────────────────────────────────────────────────

def test_the_pre_2015_income_statement_title_is_recognised():
    assert _parser()._page_kind(OLD_VAS_INCOME)[0] == INCOME_STATEMENT


def test_and_it_scores_verbatim_rather_than_squeaking_over_the_bar():
    """1.000, not 0.81 — the needle IS the title, so no threshold is being leaned on."""
    p = _parser()
    ns = p.norm(OLD_VAS_INCOME).replace(" ", "")
    assert p._title_score(ns, ["ketquahoatdongsanxuatkinhdoanh"]) == 1.0
    # …and the two spellings the needle set already had are both under the bar on this page.
    assert p._title_score(ns, ["ketquahoatdongkinhdoanh"]) < p.TITLE_MATCH
    assert p._title_score(ns, ["baocaoketquakinhdoanh"]) < p.TITLE_MATCH


def test_the_post_2014_wording_still_works():
    """The needle is ADDED, not swapped — every filing that parses today prints this one."""
    text = ("CONG TY CO PHAN FPT\n"
            "BAO CAO KET QUA HOAT DONG KINH DOANH HOP NHAT\n"
            "Don vi: VND\n")
    assert _parser()._page_kind(text)[0] == INCOME_STATEMENT


def test_a_cash_flow_page_is_not_stolen_by_the_longer_needle():
    """⚠️ A LONGER NEEDLE SCORES LOWER ON UNRELATED TEXT, NOT HIGHER — the property that makes
    adding one safe, and the reason the corpus measurement found 0 pages moving between
    statements. `_page_kind` takes the BEST title, so a cash flow keeps its verbatim 1.0."""
    text = ("CONG TY CP PHAT TRIEN DAU TU CONG NGHE FPT\n"
            "BAO CAO TAI CHINH HOP NHAT QUY III\n"
            "BAO CAO LUU CHUYEN TIEN TE\n"
            "Theo Phuong phap gian tiep\n")
    assert _parser()._page_kind(text)[0] == CASH_FLOW


# ── NOT-2 ─────────────────────────────────────────────────────────────────────

def test_a_verbatim_statement_title_beats_a_fuzzy_notes_match():
    assert _parser()._page_kind(SPLIT_COLUMN_HEADINGS)[0] == BALANCE_SHEET


def test_and_that_page_really_did_read_as_notes_on_the_fragment():
    """The measurement the fix exists for: 0.8125 notes against 1.0 balance sheet."""
    p = _parser()
    ns = p.norm("\n".join([l for l in SPLIT_COLUMN_HEADINGS.splitlines() if l.strip()]
                          [:p.HEADER_LINES])).replace(" ", "")
    assert 0.80 <= p._title_score(ns, [p.NOTES_NS]) < 1.0
    assert p._title_score(ns, p.HEADING[BALANCE_SHEET]) == 1.0


def test_column_header_blind_cannot_reach_this_page():
    """⚠️ WHY `NOT-1`'s FIX IS NOT THE FIX HERE, asserted rather than argued: no single line
    of this header carries both of `COLUMN_HEADER_NS`, so the row it removes is not there to
    remove."""
    p = _parser()
    lines = [l for l in SPLIT_COLUMN_HEADINGS.splitlines() if l.strip()][:p.HEADER_LINES]
    assert not any(all(n in p.norm(l).replace(" ", "") for n in p.COLUMN_HEADER_NS)
                   for l in lines)


def test_a_real_notes_page_is_still_a_note_even_when_it_names_a_statement():
    """⚠️ BOTH HALVES ARE REQUIRED. A page that announces itself as notes VERBATIM keeps that
    verdict however many statement names it also prints — the override needs the notes match
    to be INEXACT."""
    text = ("Ngan hang TMCP Cong thuong Viet Nam\n"
            "THUYET MINH BAO CAO TAI CHINH HOP NHAT\n"
            "33. Thuyet minh Bang can doi ke toan\n")
    assert _parser()._page_kind(text)[0] == NOTES


def test_an_inexact_title_does_not_override_an_inexact_notes_match():
    """A fuzzy title is not evidence enough to overrule a fuzzy notes header — only 1.0 is."""
    p = _parser()
    text = ("Ngan hang TMCP Cong thuong Viet Nam\n"
            "Chi tieu Thuyet minh So cuoi quy So dau nam\n"
            "1 2 3 4\n")
    ns = p.norm(text).replace(" ", "")
    assert max(p._title_score(ns, n) for n in p.HEADING.values()) < 1.0
    assert p._page_kind(text)[0] == NOTES
