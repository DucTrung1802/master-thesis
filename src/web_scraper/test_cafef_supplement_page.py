"""`GTR-1` / `SUPPLEMENT_NS` — the variance explanation appended after the income statement.

Circular 155/2015 makes an issuer explain any profit swing over 10 %, and FPT prints that
explanation on the page IMMEDIATELY AFTER its consolidated income statement: a grid of this
quarter / last year's quarter / the two year-to-date columns / the change, under the heading
"GIẢI TRÌNH:", in **"ĐVT: Triệu đồng"** where the statement itself is in đồng.

It carries a real table, so `_fill_continuations` absorbed it as the statement's second page,
and its rows are the statement's OWN account names. Measured on HOSE_FPT 2026-09-04, **twelve
quarters — every Q1 and Q3 from 2020-Q3 to 2026-Q1**: the row "Tổng lợi nhuận kế toán trước
thuế" appears twice in the parsed statement, once from the statement itself (whose columns the
six-column grid then mis-clusters into `None`) and once from the explanation, in millions.
`Statement.find` skips a row with no value and returns the SECOND, so `sane` banded the quarter
on 8,111,171 against a typical 6.58e11 and refused it:

    sane: magnitude 8.11e+06 vs typical 6.58e+11 (units? cumulative column? OCR misread?)

⚠️ Each of the twelve blocks a CUMULATIVE Q2/Q4 that then has no prior to subtract, so they are
worth roughly twenty-four cells between them.

⚠️ This is `AUDIT_NS`' rule at the other end of the filing — *a page that announces itself as
something other than a statement is never a statement* — and it is a DEFAULT-path change for
`AUDIT_NS`' reason too: the income statement is accepted at layer 1 on every one of these
filings, so no later layer is ever reached (§6-2-untricies).

⚠️ **AND IT CAN ONLY EVER REFUSE A PAGE THE CLASSIFIER COULD NOT IDENTIFY** — the test is on
the branch of `_fill_continuations` that absorbs an UNIDENTIFIED page into the run above it, so
a page carrying its own form code or its own statement title never reaches it. The worst it can
do is end a run one page early, and only on a page whose header says "giải trình".
"""
from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, NOTES,
                                          PdfParser)

# The header block of page 9 of FPT's Q3-2024 consolidated filing, as `onnx@200` reads it —
# transcribed from the run folder, not invented.
FPT_GIAI_TRINH = (
    "CONG TY CO PHAN FPT\n"
    "So 10 pho Pham Van Bach\n"
    "Bao cao tai chinh hop nhat\n"
    "Phuong Dich Vong, Quan Cau Giay\n"
    "Cho ky hoat dong tu ngay 01 thang 01 nam 2024\n"
    "Thanh pho Ha Noi, Viet Nam\n"
    "den ngay 30 thang 9 nam 2024\n"
    "GIAI TRINH:\n"
    "Cong ty Co phan FPT giai trinh bien dong ket qua kinh doanh Quy 3 nam 2024 nhu sau:\n"
    "DVT: Trieu dong\n"
    "Chi tieu Nam 2024 Nam 2023 QUY III QUY III Gia tri Ty le\n"
    "Doanh thu thuan 15.902.822 13.761.745 2.141.077 15,6%\n"
    "Tong loi nhuan ke toan truoc thue 2.908.621 2.429.150 479.470 19,7%\n"
)

# The income statement's own continuation page — no such heading, and it must still be absorbed.
IS_CONTINUATION = (
    "CONG TY CO PHAN FPT\n"
    "Bao cao tai chinh hop nhat\n"
    "11 Loi nhuan thuan tu hoat dong kinh doanh 2.948.226.004.570\n"
    "12 Thu nhap khac 34.674.999.189\n"
)


def _numbered(text, n=20):
    """Word boxes for `text` plus `n` figures, so `_is_table` sees a table.

    ⚠️ Above `MIN_TABLE_WORDS`, deliberately: every page here has to be one the absorbing
    branch would otherwise take, or a test could pass because the page was too sparse.
    """
    words = [(10.0, 10.0, 100.0, 20.0, text)]
    words += [(400.0, 30.0 + 10 * i, 440.0, 40.0 + 10 * i, f"{i + 1}.234")
              for i in range(n)]
    return words


def _pages(spec):
    return {i: {"kind": kind, "from_form": False, "text": text,
                "words": _numbered(text), "width": 595.0}
            for i, (kind, text) in enumerate(spec)}


def _parser():
    p = PdfParser.__new__(PdfParser)          # no OCR engine, no models, no PDF
    p.notes_tail = False
    p.notes_head = False
    p.tail_continuation = False
    return p


def _kinds(pages):
    return [pages[i]["kind"] for i in sorted(pages)]


# ── the defect ────────────────────────────────────────────────────────────────

def test_the_variance_explanation_is_not_absorbed_into_the_income_statement():
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH HOP NHAT"),
                    (None, FPT_GIAI_TRINH)])
    _parser()._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, None]


def test_the_run_ends_there_so_the_page_after_it_is_not_absorbed_either():
    """A statement does not resume on the far side of its own explanation."""
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH HOP NHAT"),
                    (None, FPT_GIAI_TRINH),
                    (None, "Mot bang so lieu nao do")])
    _parser()._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, None, None]


# ── what it must NOT cost ─────────────────────────────────────────────────────

def test_an_ordinary_continuation_page_is_still_absorbed():
    """The guard is the heading, not the position: page 2 of a statement still belongs to it."""
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH HOP NHAT"),
                    (None, IS_CONTINUATION)])
    _parser()._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, INCOME_STATEMENT]


def test_it_reads_the_header_block_only():
    """A statement line, or a note, that uses the word further down the page is not a heading.

    ⚠️ The same confinement `AUDIT_NS` and the three statement titles use, and for the same
    reason: "giải trình" appears in ordinary prose ("thuyết minh và giải trình"), and a page
    is only ANNOUNCING itself in its first `HEADER_LINES` lines.
    """
    deep = "\n".join(["CONG TY CO PHAN FPT", "Bao cao tai chinh hop nhat"]
                     + [f"{i} Mot dong nao do 1.234.567.890" for i in range(1, 14)]
                     + ["Thuyet minh va giai trinh bo sung"])
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH HOP NHAT"),
                    (None, deep)])
    _parser()._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, INCOME_STATEMENT]


def test_it_cannot_refuse_a_page_the_classifier_identified():
    """⚠️ THE SAFETY PROPERTY, AND IT IS STRUCTURAL RATHER THAN A THRESHOLD.

    The test sits on the branch that absorbs an UNIDENTIFIED page, so a page carrying its own
    form code or its own statement title is `kind in REPORTS` and never reaches it. A filing
    that somehow prints "giải trình" over a real statement page keeps that statement.
    """
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH HOP NHAT"),
                    (CASH_FLOW, FPT_GIAI_TRINH)])
    _parser()._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, CASH_FLOW]


def test_it_does_not_fire_without_a_run_open():
    """Nothing to end, and nothing to refuse — the page was never going to be absorbed."""
    pages = _pages([(NOTES, "THUYET MINH BAO CAO TAI CHINH HOP NHAT"),
                    (None, FPT_GIAI_TRINH)])
    _parser()._fill_continuations(pages)

    assert _kinds(pages) == [NOTES, None]


def test_the_predicate_itself():
    """`_is_supplement` on the header block alone, both directions."""
    p = _parser()
    assert p._is_supplement({"text": FPT_GIAI_TRINH})
    assert not p._is_supplement({"text": IS_CONTINUATION})
