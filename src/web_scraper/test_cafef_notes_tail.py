"""`notes_tail` — a page whose HEADER says notes and whose CONTENT continues the statement.

The defect: `_fill_continuations` reads a NOTES page as "the statements are over" and ends the
run there, which is right until the filing prints its own notes header on a statement's SECOND
page. TCB's Q2-2019, Q3-2019 and Q1-2021 consolidated cash flows each run onto a page headed
"THUYẾT MINH BÁO CÁO TÀI CHÍNH HỢP NHẤT", and Q1-2021 stamps the notes FORM CODE on it as well
("Mẫu B050/TCTD - HN", B05 = notes). That page carries sections II-VII — the opening balance,
the FX line and the CLOSING balance — so losing it fails the statement outright, and all three
quarters were `missing` for "no closing cash balance" on every layer of the cascade.

⚠️ The header was read off the RENDERED PAGE before anything was written: it is the DOCUMENT's
own, not OCR damage (its period line is an unresolved Word field, "cho giai đoạn từ ngày REF
Yea01 …"), so no engine, DPI or crop setting reaches it.

⚠️ `TAIL` cannot admit the page either, and that is a SECOND defect rather than a threshold:
the needle is a contiguous phrase and the closing line's label wraps AROUND its own figures, so
the flattened page reads "…tuongduong VII 33 47141880 50050197 tientaithoidiemcuoiky…". The
evidence is therefore taken in two parts that each survive the wrap — see `SECTION_HEADING`.
"""
from web_scraper.cafef_financials import FinancialsBuilder, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import (BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, NOTES,
                                          PdfParser)

# The two markers, as the three TCB filings print them, plus enough of the tail to be a table.
TCB_TAIL = ("Ngan hang Thuong mai Co phan Ky thuong Viet Nam\n"
            "THUYET MINH BAO CAO TAI CHINH HOP NHAT\n"
            "LUU CHUYEN TIEN THUAN TU HOAT DONG DAU TU\n"
            "IV LUU CHUYEN TIEN THUAN TRONG KY 9.942.971 27.369.348\n"
            "TIEN VA CAC KHOAN TUONG DUONG\n"
            "VII 33 47.141.880 50.050.197\n"
            "TIEN TAI THOI DIEM CUOI KY\n")
# A real note that follows the cash flow and NAMES the same account — the false positive this
# test exists to keep out. It is note 33, the one the closing balance itself references.
NOTE_33 = ("Ngan hang Thuong mai Co phan Ky thuong Viet Nam\n"
           "THUYET MINH BAO CAO TAI CHINH HOP NHAT\n"
           "33. Tien va cac khoan tuong duong tien\n"
           "Tien mat 1.234 2.345\nTien gui tai NHNN 3.456 4.567\n")


def _numbered(text, n=8):
    """Word boxes for `text` plus `n` figures, so `_numbers` sees a table."""
    words = [(10.0, 10.0, 100.0, 20.0, text)]
    words += [(400.0, 30.0 + 10 * i, 440.0, 40.0 + 10 * i, f"{i + 1}.234")
              for i in range(n)]
    return words


def _pages(spec):
    """{index: page} from `[(kind, text), ...]`, indexed from 0 like `scan`."""
    return {i: {"kind": kind, "from_form": False, "text": text,
                "words": _numbered(text), "width": 595.0}
            for i, (kind, text) in enumerate(spec)}


def _parser(notes_tail, notes_head=False):
    p = PdfParser.__new__(PdfParser)          # no OCR engine, no models, no PDF
    p.notes_tail = notes_tail
    p.notes_head = notes_head
    p.tail_continuation = False
    return p


def _kinds(pages):
    return [pages[i]["kind"] for i in sorted(pages)]


# ── the recovery ──────────────────────────────────────────────────────────────

def test_a_notes_headed_page_carrying_the_statements_tail_is_admitted():
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"),
                    (NOTES, TCB_TAIL),
                    (NOTES, NOTE_33)])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, CASH_FLOW, NOTES]


def test_the_run_ends_at_the_page_it_admits():
    """A page reached through its neighbour's identity may not pass that licence on."""
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"),
                    (NOTES, TCB_TAIL),
                    (None, "34. Cac cam ket ngoai bang\n")])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, CASH_FLOW, None]


def test_it_is_off_by_default_so_no_statement_that_parses_today_is_re_judged():
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"),
                    (NOTES, TCB_TAIL)])
    _parser(False)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, NOTES]


# ── what it must NOT admit ────────────────────────────────────────────────────

def test_a_note_naming_the_same_account_is_refused():
    """⚠️ Note 33 IS "Tiền và các khoản tương đương tiền" — the account the closing line names,
    and the note the closing line REFERENCES. Only the two-part evidence separates them."""
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"),
                    (NOTES, NOTE_33)])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, NOTES]


def test_the_section_heading_alone_is_not_enough():
    """A page quoting the cash flow's section heading with no closing balance on it."""
    text = ("THUYET MINH BAO CAO TAI CHINH HOP NHAT\n"
            "LUU CHUYEN TIEN THUAN TU HOAT DONG KINH DOANH\n")
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"), (NOTES, text)])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, NOTES]


def test_the_closing_clause_alone_is_not_enough():
    text = ("THUYET MINH BAO CAO TAI CHINH HOP NHAT\n"
            "So du tai thoi diem cuoi ky 1.234 2.345\n")
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"), (NOTES, text)])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, NOTES]


def test_a_notes_page_that_follows_no_statement_is_never_admitted():
    """The run has to be OPEN: this is a continuation rule, not a page-classification rule."""
    pages = _pages([(NOTES, "THUYET MINH BAO CAO TAI CHINH HOP NHAT"), (NOTES, TCB_TAIL)])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [NOTES, NOTES]


def test_only_the_cash_flow_has_a_section_marker():
    """⚠️ `SECTION_HEADING` covers the statement whose tail was MEASURED and no other. A balance
    sheet's continuation page carrying the same words is not evidence about a balance sheet."""
    pages = _pages([(BALANCE_SHEET, "BANG CAN DOI KE TOAN"), (NOTES, TCB_TAIL)])
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [BALANCE_SHEET, NOTES]
    assert set(PdfParser.SECTION_HEADING) == {CASH_FLOW}
    assert INCOME_STATEMENT not in PdfParser.SECTION_HEADING


def test_a_page_with_almost_no_figures_is_refused():
    """`MIN_TAIL_WORDS` keeps a page holding one stray number from reaching the evidence."""
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"), (NOTES, TCB_TAIL)])
    pages[1]["words"] = _numbered(TCB_TAIL, n=1)
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, NOTES]


# ── how the flag is wired ─────────────────────────────────────────────────────

def test_notes_tail_is_a_parse_key_and_not_an_ocr_key():
    """It moves which page belongs to which statement, so two layers differing only in it must
    not share a parse — and it cannot change a recognised character, so it costs no OCR pass."""
    a = next(l for l in FinancialsBuilder.LAYERS if l.notes_tail)
    plain = next(l for l in FinancialsBuilder.LAYERS
                 if not l.notes_tail and l.engine == a.engine and l.dpi == a.dpi)

    assert parse_key(a) != parse_key(plain)
    assert ocr_key(a) == ocr_key(plain)


def test_every_notes_tail_layer_is_relaxed_and_repairs_its_labels():
    """⚠️ MEASURED, not preferred. `verify_cash` rides with `relax_totals`, so a STRICT layer
    skips `_cash_flow_identity` — and TCB's Q2-2019 closing balance is printed UNDER THE
    COMPANY'S ROUND STAMP: the filing prints 47.141.880 and the recogniser returns 171414880 at
    200 dpi, 17141880 at 300 and 500, 19111880 at 400+pad6 and 17141.880 at 600 — never the
    right figure, at any resolution or crop. A strict `+notestail` layer ACCEPTS that and would
    have written 171,414,880; the relaxed one refuses it. And without `label_wrap` the two cash
    balances key identically, which is `SLD-1`'s shape."""
    layers = [l for l in FinancialsBuilder.LAYERS if l.notes_tail]

    assert layers, "the block was removed — do not re-add it without a quarter it recovers"
    for l in layers:
        assert l.relax_totals, f"{l.name} would accept a statement without checking its sums"
        assert l.label_wrap, f"{l.name} would key both cash balances the same way"
        assert l.reseat_words, f"{l.name} would seat the closing figures on two rows"


def test_the_notes_tail_layers_run_after_every_layer_that_wins_a_row_today():
    """⚠️ The POSITION is what bounds them. Measured 2026-09-04 over all 1,182 `pdf` rows on
    disk: the latest cascade position any was won at is 65 of 65, so a layer at 66 or beyond is
    unreachable for every row this repo has already written.

    Asserted as an ORDER, never as an index: a test that pins a POSITION fails the first time
    something legitimate is appended, which this repo has now had to restate three times."""
    layers = FinancialsBuilder.LAYERS
    first = min(i for i, l in enumerate(layers) if l.notes_tail)
    strict = [i for i, l in enumerate(layers) if l.is_strict]
    # ⚠️ RESTATED 2026-09-04 — the FOURTH position assertion in this suite to be outgrown.
    # It read `first > max(others)`, i.e. the block is literally the tail of the cascade, and
    # the `+deskew` block broke it by being appended after it while changing nothing about
    # when a mis-titled continuation page may be admitted. What bounds these layers is that no
    # layer reading the page AS PRINTED runs after them — `ParseLayer.is_strict`.
    assert first > max(strict),         "a +notestail layer must never run before a layer that reads the page as printed"


# ── `notes_head` — the same defect on the statement's FIRST page ───────────────

# TCB's Q1-2017 page 8 and Q3-2017 page 9: the notes title AND the notes form code over the
# cash flow's own opening section. No run is ever opened, so the tail rule has nothing to
# extend and every layer reports `no such statement on any page of this filing` — which
# `settled_absences` treats as PERMANENT.
TCB_HEAD = ("Ngan hang Thuong mai Co phan Ky thuong Viet Nam\n"
            "THUYET MINH BAO CAO TAI CHINH HOP NHAT Mau B050/TCTD - HN\n"
            "cho giai doan tu ngay 01 thang 01 nam 2017 den ngay 31 thang 03 nam 2017\n"
            "LUU CHUYEN TIEN THUAN TU HOAT\n"
            "DONG KINH DOANH\n"
            "1 Thu nhap lai va cac khoan thu nhap tuong tu 3.954.371 3.453.084\n")


def test_a_notes_headed_page_can_START_the_cash_flow():
    pages = _pages([(BALANCE_SHEET, "BANG CAN DOI KE TOAN"),
                    (INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"),
                    (NOTES, TCB_HEAD),
                    (NOTES, TCB_TAIL)])
    # ⚠️ A HEAD page must clear `MIN_TABLE_WORDS`, where a TAIL page needs only two figures —
    # the real page 8 of Q1-2017 carries 67. The default fixture's 8 is a tail's worth.
    pages[2]["words"] = _numbered(TCB_HEAD, n=20)
    _parser(True, notes_head=True)._fill_continuations(pages)

    assert _kinds(pages) == [BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW, CASH_FLOW]


def test_the_head_it_opens_is_continued_by_the_tail_rule():
    """⚠️ The two ride together on the same layers because the same filings need both: the
    cash flow's SECOND page is mis-titled too, so without the tail rule the run this just
    opened is closed again by the very next page."""
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"),
                    (NOTES, TCB_HEAD),
                    (NOTES, TCB_TAIL)])
    pages[1]["words"] = _numbered(TCB_HEAD, n=20)
    _parser(False, notes_head=True)._fill_continuations(pages)      # head WITHOUT tail

    assert _kinds(pages) == [INCOME_STATEMENT, CASH_FLOW, NOTES]


def test_it_is_off_by_default():
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"),
                    (NOTES, TCB_HEAD)])
    pages[1]["words"] = _numbered(TCB_HEAD, n=20)     # a table, so ONLY the flag refuses it
    _parser(True)._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, NOTES]


def test_a_report_that_already_has_pages_of_its_own_is_never_claimed():
    """⚠️ `seen` is taken from the FINAL classification, so this protects FORWARDS too — a
    filing whose cash flow is correctly titled later cannot have an earlier note claimed."""
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"),
                    (NOTES, TCB_HEAD),
                    (CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT")])
    pages[1]["words"] = _numbered(TCB_HEAD, n=20)     # a table, so ONLY `seen` refuses it
    _parser(True, notes_head=True)._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, NOTES, CASH_FLOW]


def test_a_page_out_in_the_notes_is_never_claimed():
    """⚠️ THE RUN MUST BE OPEN. `_fill_continuations`' own premise is that a statement's pages
    are contiguous; without this guard the same words in a narrative note would be evidence."""
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"),
                    (NOTES, "THUYET MINH BAO CAO TAI CHINH HOP NHAT\n1. Dac diem hoat dong\n"),
                    (NOTES, TCB_HEAD)])
    pages[2]["words"] = _numbered(TCB_HEAD, n=20)     # a table, so ONLY the run guard refuses
    _parser(True, notes_head=True)._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, NOTES, NOTES]


def test_the_needle_must_be_in_the_HEADER_BLOCK_not_merely_on_the_page():
    """A note that mentions the section heading far below its own title is not a statement."""
    text = ("THUYET MINH BAO CAO TAI CHINH HOP NHAT\n"
            + "".join(f"line {i}\n" for i in range(PdfParser.HEADER_LINES + 2))
            + "LUU CHUYEN TIEN THUAN TU HOAT DONG KINH DOANH\n")
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"), (NOTES, text)])
    pages[1]["words"] = _numbered(text, n=20)         # a table, so ONLY the window refuses it
    _parser(True, notes_head=True)._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, NOTES]


def test_a_page_that_is_not_a_table_is_never_claimed():
    pages = _pages([(INCOME_STATEMENT, "BAO CAO KET QUA HOAT DONG KINH DOANH"),
                    (NOTES, TCB_HEAD)])
    pages[1]["words"] = _numbered(TCB_HEAD, n=2)         # under MIN_TABLE_WORDS

    _parser(True, notes_head=True)._fill_continuations(pages)

    assert _kinds(pages) == [INCOME_STATEMENT, NOTES]


def test_the_statement_currently_RUNNING_is_not_re_opened_as_a_head():
    """Extending it is the tail rule's job, and it has had its turn by the time this runs.

    ⚠️ **THIS IS `seen`'s WORK, NOT A SEPARATE GUARD, AND A MUTATION CHECK IS WHAT SETTLED
    THAT.** `_notes_head_report` carried `report in seen or report == run` until 2026-09-04;
    deleting the second clause broke no test, because `run` is only ever a report the classifier
    already found — so it was removed rather than kept as a clause nothing could falsify."""
    pages = _pages([(CASH_FLOW, "BAO CAO LUU CHUYEN TIEN TE HOP NHAT"), (NOTES, TCB_HEAD)])
    pages[1]["words"] = _numbered(TCB_HEAD, n=20)     # a table, so only the guards refuse it
    _parser(False, notes_head=True)._fill_continuations(pages)

    assert _kinds(pages) == [CASH_FLOW, NOTES]


def test_the_noteshead_layers_run_after_the_notestail_ones():
    """⚠️ THE NARROWER CLAIM FIRST. Extending an open run says this page belongs to the
    statement above it; opening one says what a page IS."""
    names = [l.name for l in FinancialsBuilder.LAYERS]
    tail = [names.index(l.name) for l in FinancialsBuilder.LAYERS
            if l.notes_tail and not l.notes_head]
    head = [names.index(l.name) for l in FinancialsBuilder.LAYERS if l.notes_head]

    assert tail and head and min(head) > max(tail)


def test_every_noteshead_layer_is_relaxed_and_carries_the_tail_rule():
    layers = [l for l in FinancialsBuilder.LAYERS if l.notes_head]

    assert layers, "the block was removed — do not re-add it without a quarter it recovers"
    for l in layers:
        assert l.relax_totals, f"{l.name} would accept a statement without checking its sums"
        assert l.notes_tail, f"{l.name} would open a run the next page closes again"
        assert l.label_wrap and l.reseat_words, f"{l.name} would key both cash balances alike"


def test_notes_head_is_a_parse_key_and_not_an_ocr_key():
    a = next(l for l in FinancialsBuilder.LAYERS if l.notes_head)
    tail = next(l for l in FinancialsBuilder.LAYERS
                if l.notes_tail and not l.notes_head and l.dpi == a.dpi
                and l.cash_extra_terms == a.cash_extra_terms)

    assert parse_key(a) != parse_key(tail)
    assert ocr_key(a) == ocr_key(tail)
