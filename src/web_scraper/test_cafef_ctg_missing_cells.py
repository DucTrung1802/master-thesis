"""CTG's `missing` cells — the column heading, the slash, the tail page and the merge.

Four defects, all found on 2026-09-02 in the quarters of HOSE_CTG that had read `missing`
since the ticker was first parsed. None of them is an OCR failure: every figure named below
was read correctly at `onnx@200` and then thrown away by something downstream.

  * `NOT-1`  the statement TABLE's own column heading — `Chi tieu | Thuyet minh | So cuoi
             quy | So dau nam` — scores 0.8125 against the NOTES title, one hundredth over
             `TITLE_MATCH`, so every CONTINUATION page of a VAS statement reads as a note and
             `_fill_continuations` ends the run there. CTG Q1-2009's balance sheet was
             truncated to its first page: 17 rows, no TONG TAI SAN.
  * `SLH-1`  the recogniser puts a "/" in the COLUMN GAP, so the box holding both period
             figures parses as no number at all. CTG Q2-2011 returns
             `'395.852.473 /367.712.191'` for the two columns of TONG TAI SAN.
  * `TAI-1`  the filing words the closing line "Tien va tuong duong tien" where `TAIL` knew
             only "Tien va CAC KHOAN tuong duong tien", and its tail page carries three
             numbers against a `MIN_TAIL_WORDS` of four. CTG Q1-2014's closing balance
             88.180.310.933.901 sat on a page the test refused.
  * `MTL-1`  a grand total OCR merged onto the line above it, in three shapes no seam can
             cut: past the 60-character slug cap (Q2-2011 cash flow), with a trailing
             classifier the chart omits (Q3-2010 "TONG CONG TAI SAN CO"), and with a word
             dropped (Q1-2009 "tong no phai tra von chu so huu", missing the "VA").

⚠️ THE FIRST AND FOURTH ARE A LAYER AT THE END OF THE CASCADE, and the position is what bounds
them: measured 2026-09-02 over all **1,168 `pdf` rows on disk**, the latest cascade position
any of them was won at is **53**. The other two are in the DEFAULT path — see the two tests
that say so, and why each can only add a figure rather than move one.

No PDF, no network, no OCR engine.
"""
import pytest

from web_scraper.cafef_financials import FinancialsBuilder, ocr_key, parse_key
from web_scraper.cafef_pdf_parser import PdfParser, Row


def _w(text, x0=100.0, x1=200.0, y=200.0):
    """PyMuPDF's word shape: (x0, y0, x1, y1, text, block, line, n)."""
    return (x0, y, x1, y + 10.0, text, 0, 0, 0)


def _page(text, *numbers):
    return {"text": text, "words": [_w(n) for n in numbers]}


def _parser(**flags):
    p = PdfParser.__new__(PdfParser)
    p.loose_form_code = False
    p.title_over_form = False
    p.column_header_blind = False
    p.tail_continuation = False
    for k, v in flags.items():
        setattr(p, k, v)
    return p


# ── SLH-1 · the slash in the column gap ───────────────────────────────────────
def test_a_slash_in_the_column_gap_splits_into_two_figures():
    out = PdfParser._split_number_runs([_w("395.852.473 /367.712.191", 410.4, 534.24)])
    assert [w[4] for w in out] == ["395.852.473", "367.712.191"]


def test_the_split_lands_the_right_edge_where_the_column_clustering_reads_it():
    # Apportioning is by CHARACTER OFFSET, so the substitution has to preserve length.
    out = PdfParser._split_number_runs([_w("395.852.473 /367.712.191", 410.4, 534.24)])
    assert round(out[-1][2], 2) == 534.24


@pytest.mark.parametrize("txt", [
    "30/06/2011 01/01/2011",              # the period header these very pages print
    "1/1/2014 den 31/3/2014",
])
def test_a_date_is_not_a_column_gap(txt):
    # No whitespace beside those slashes, so the substitution never fires and the box is left
    # exactly as it was — which is what keeps a date out of the table.
    assert [w[4] for w in PdfParser._split_number_runs([_w(txt)])] == [txt]


def test_the_slash_rule_is_in_the_default_path_and_can_only_add_figures():
    # A box carrying a slash parses as NO number today, so nothing downstream can be reading
    # one: the rule turns a discarded box into figures and never rewrites a figure already
    # read. That is why it is not behind a layer.
    assert PdfParser.parse_num("395.852.473 /367.712.191") is None


# ── TAI-1 · the tail page ─────────────────────────────────────────────────────
CLOSING_LINE = ("VII Tien va tuong duong tien tai thoi diem cuoi ky 20 "
                "88.180.310.933.901 51.486.402.665.787")


def test_both_spellings_of_the_closing_line_are_known():
    for line in (CLOSING_LINE,
                 "Tien va cac khoan tuong duong tien tai thoi diem cuoi ky"):
        ns = PdfParser.norm(line).replace(" ", "")
        assert any(n in ns for n in PdfParser.TAIL["cash_flow"]), line


def test_a_tail_page_may_carry_the_closing_line_and_nothing_else():
    page = _page(CLOSING_LINE, "20", "88.180.310.933.901", "51.486.402.665.787")
    assert _parser(tail_continuation=True)._is_tail_page(page, "cash_flow") is True


def test_the_needle_is_the_guard_and_not_the_count():
    page = _page("Ha Noi, ngay 14 thang 5 nam 2014 Lap bang Ke toan truong",
                 "14", "5", "2014", "2", "1")
    assert _parser(tail_continuation=True)._is_tail_page(page, "cash_flow") is False


def test_a_tail_page_is_refused_without_the_flag():
    page = _page(CLOSING_LINE, "20", "88.180.310.933.901", "51.486.402.665.787")
    assert _parser(tail_continuation=False)._is_tail_page(page, "cash_flow") is False


# ── NOT-1 · the table's own column heading ────────────────────────────────────
CONTINUATION = ("Chi tieu Thuyet minh So cuoi quy So dau nam\n"
                "1 2 3 4\n"
                "4/Dau tu dai han khac 158,941,418,600 134,063,888,600\n"
                "IX Tai san co dinh 1,524,823,965,265 1,570,086,648,024")


def _kind(text, blind):
    return _parser(column_header_blind=blind)._page_kind(text)


def test_the_column_heading_alone_reads_as_notes_today():
    # The measurement this fix exists for: 0.8125 against a TITLE_MATCH of 0.80.
    assert _kind(CONTINUATION, blind=False)[0] == "notes"


def test_and_is_refused_when_the_column_heading_may_not_decide():
    assert _kind(CONTINUATION, blind=True)[0] is None


def test_a_real_notes_page_is_still_a_note_under_the_flag():
    # CTG's own notes pages announce themselves, and their title does not sit on the
    # column-heading row — so removing that row leaves the verdict untouched.
    text = ("Ngan hang TMCP Cong thuong Viet Nam\n"
            "THUYET MINH CAC BAO CAO TAI CHINH HOP NHAT GIUA NIEN DO\n"
            "Chi tieu Thuyet minh So cuoi quy So dau nam\n"
            "1 2 3 4")
    assert _kind(text, blind=True)[0] == "notes"
    assert _kind(text, blind=False)[0] == "notes"


def test_the_flag_never_touches_a_statement_title():
    """The three statement titles keep the FULL header, so a title printed on the same OCR
    line as the column heading cannot be lost by removing that row.

    ⚠️ **THE SECOND ASSERTION USED TO READ `== "notes"`, PINNING THE DEFECT AS BEHAVIOUR** —
    with a comment saying so in as many words ("AND THE OTHER HALF IS THE DEFECT ITSELF,
    RECORDED"). `NOT-2` (2026-09-04) is that defect fixed: the notes verdict was taken BEFORE
    the three titles were compared, so a page whose own title is printed VERBATIM read as a
    note on a 0.8125 fuzzy match. An exact title now wins, so the flag is no longer what
    separates these two answers. **A test that pins a defect is how the defect survives a
    rewrite** (§6-2-quinvicies), and this is the second instance in this file's own subject.
    """
    text = "BANG CAN DOI KE TOAN Chi tieu Thuyet minh So cuoi quy\n1 2 3 4"
    assert _kind(text, blind=True)[0] == "balance_sheet"
    assert _kind(text, blind=False)[0] == "balance_sheet"


def test_column_header_blind_is_still_load_bearing_for_a_continuation_page():
    """⚠️ `NOT-2` DOES NOT SUBSUME `NOT-1`, AND THAT IS WHY BOTH ARE KEPT.

    A CONTINUATION page has no title of its own — that is the whole reason the column-heading
    row is its entire header — so there is no verbatim title for the new rule to prefer, and
    `column_header_blind` remains the only thing that reaches it. The two answer two different
    pages of the same statement: `NOT-1` the continuation, `NOT-2` the FIRST page of a filing
    that sets its column headings on separate lines (FPT 2008-2010).
    """
    assert _kind(CONTINUATION, blind=False)[0] == "notes"
    assert _kind(CONTINUATION, blind=True)[0] is None


# ── MTL-1 · the merged grand total ────────────────────────────────────────────
Q3_2010_ASSETS = Row(
    key="cac_khoan_du_phong_rui_ro_cho_cac_tai_san_co_noi_bang_khac_t",
    label=("Cac khoan du phong rui ro cho cac tai san co noi bang khac (t44) "
           "TONG CONG TAI SAN CO"),
    number="",
    values=[321339286721871, 243785208000000])
Q1_2009_GRAND = Row(
    key="loi_ich_cua_co_dong_thieu_so_tong_no_phai_tra_von_chu_so_huu",
    label="IX - Loi ich cua co dong thieu so tong no phai tra von chu so huu",
    number="",
    values=[193280787212094, 194414991581990])
Q2_2011_CLOSE = Row(
    key="tien_nhan_chuyen_giao_tu_doanh_nghiep_truoc_co_phan_hoa_dieu",
    label=("Tien nhan chuyen giao tu doanh nghiep truoc co phan hoa Dieu chinh anh huong "
           "cua thay doi ty gia Tien va cac khoan tuong duong tien tai thoi diem cuoi ky"),
    number="",
    values=[35696868000000, 36677733000000])
CLOSE_ACCOUNT = "tienvacackhoantuongduongtientaithoidiemcuoiky"


def test_the_cut_suffixes_are_off_by_default():
    keys, tails = FinancialsBuilder()._anchor_keys(Q3_2010_ASSETS, False, False)
    assert tails == []
    assert keys == [Q3_2010_ASSETS.key.replace("_", "")]


@pytest.mark.parametrize("row,wanted", [
    (Q3_2010_ASSETS, "tongcongtaisanco"),
    (Q1_2009_GRAND, "tongnophaitravonchusohuu"),
    (Q2_2011_CLOSE, CLOSE_ACCOUNT),
])
def test_the_line_that_owns_the_figure_is_among_the_cuts(row, wanted):
    _, tails = FinancialsBuilder()._anchor_keys(row, False, True)
    assert wanted in tails


def test_a_cut_reaches_past_the_sixty_character_slug_cap():
    # Q2-2011's key STOPS at `..._co_phan_hoa_dieu`; the account lives beyond the cap.
    keys, tails = FinancialsBuilder()._anchor_keys(Q2_2011_CLOSE, False, True)
    assert CLOSE_ACCOUNT not in keys[0]
    assert CLOSE_ACCOUNT in tails


@pytest.mark.parametrize("account,cut,expected", [
    # the account SPANS the cut bar one classifier the chart omits — this is the case
    ("tongtaisan", "tongcongtaisanco", True),
    # ...and a cut that is the account plus a whole other line item is NOT
    ("vonchusohuu", "vonchusohuu1cackhoannochinhphuvanhnn", False),
])
def test_containment_on_a_cut_is_one_way_and_bounded(account, cut, expected):
    b = FinancialsBuilder()
    got = b._label_score(account, cut, edge_containment=True, cut_fragment=True) >= 0.95
    assert got is expected


def test_a_short_cut_cannot_borrow_a_long_account():
    # "khoan", cut from "chung khoan", scored the flat 0.95 against the closing-balance
    # account on two unrelated CTG rows before `cut_fragment` was one-way.
    b = FinancialsBuilder()
    assert b._label_score(CLOSE_ACCOUNT, "khoan", edge_containment=True) >= 0.95
    assert b._label_score(CLOSE_ACCOUNT, "khoan",
                          edge_containment=True, cut_fragment=True) < 0.5


def test_the_dropped_word_is_reached_by_the_ratio_and_not_by_containment():
    # Q1-2009's grand total lost its "VA", so neither string contains the other; 0.96 is the
    # plain SequenceMatcher, and it is what lets the long anchor reach the row at all.
    b = FinancialsBuilder()
    r = b._label_score("tongnophaitravavonchusohuu", "tongnophaitravonchusohuu",
                       edge_containment=True, cut_fragment=True)
    assert r >= b.ANCHOR_MATCH


def test_the_nesting_widens_only_under_the_flag():
    # `von_chu_so_huu` sits INSIDE `tong_no_phai_tra_va_von_chu_so_huu` without being a
    # prefix of it, which is why Q1-2009's equity anchor could take the whole balance sheet.
    accounts = {"viii_von_chu_so_huu": "vonchusohuu",
                "tong_no_phai_tra": "tongnophaitra",
                "tong_no_phai_tra_va_von_chu_so_huu": "tongnophaitravavonchusohuu"}

    def nested(merged_tail):
        return {c1: [c2 for c2, a2 in accounts.items()
                     if c2 != c1 and len(a2) > len(a1)
                     and (a1 in a2 if merged_tail else a2.startswith(a1))]
                for c1, a1 in accounts.items()}

    assert nested(False)["viii_von_chu_so_huu"] == []
    assert nested(True)["viii_von_chu_so_huu"] == ["tong_no_phai_tra_va_von_chu_so_huu"]
    # the prefix pair NST-1 measured is unchanged in both
    assert nested(False)["tong_no_phai_tra"] == ["tong_no_phai_tra_va_von_chu_so_huu"]
    assert nested(True)["tong_no_phai_tra"] == ["tong_no_phai_tra_va_von_chu_so_huu"]


# ── the cascade ───────────────────────────────────────────────────────────────
def test_the_new_layers_are_last_and_reachable_by_nothing_on_disk():
    """⚠️ THIS ASSERTED `names[first:] == new` -- that the +merged block is literally the
    tail of the list -- until 2026-09-03, when the `+reseat` / `+equity` block was appended
    after it. That is the FOURTH time a test in this repo has pinned a POSITION and broken on
    a legitimate append (see `test_the_span_layers_run_late_and_relaxed`,
    `test_the_condensed_layer_runs_last`, `test_the_joinlost_layers_run_after_every_strict_
    layer`). The property being guarded is that nothing reading the box AS PRINTED runs
    afterwards, and that a row already on disk cannot reach here.
    """
    layers = FinancialsBuilder.LAYERS
    new = [i for i, l in enumerate(layers) if l.column_header_blind]
    assert new, "no +merged layer in the cascade"
    assert new == list(range(new[0], new[-1] + 1)), "the block must be contiguous"
    # measured 2026-09-02: the latest position any `pdf` row on disk was won at is 53
    assert new[0] + 1 > 53
    assert new[0] > max(i for i, l in enumerate(layers) if l.is_strict)


def test_the_new_flags_are_off_on_every_other_layer():
    """⚠️ `merged_tail` IS NO LONGER PRIVATE TO THE +merged BLOCK, and that is deliberate:
    `equity_wording` needs the merged-row suffix keys to find CTG Q3-2010's grand total, so
    the two travel together. What still holds — and is what this guards — is that neither flag
    is ever on outside a WIDENING block at the end of the cascade.

    ⚠️ **RESTATED 2026-09-05, AND THE OLD SHAPE IS WHY.** It enumerated block NAMES
    (`+merged`, `+equity`) and an `else` that refused everything else, so appending a
    legitimate widening block broke it while changing nothing about the guard: `SEAL-2`'s
    `+total` layers need the merged-row suffix keys for exactly `equity_wording`'s reason —
    FPT Q1-2016's liabilities line arrives with the column headings glued onto it. That is the
    third time in this repo a test pinning a POSITION or a NAME LIST has failed an append; the
    invariant is what it says it is, so it is now asserted directly.
    """
    strict = max(i for i, l in enumerate(FinancialsBuilder.LAYERS, 1) if l.is_strict)
    for i, layer in enumerate(FinancialsBuilder.LAYERS, 1):
        if "+merged" in layer.name:
            assert layer.column_header_blind and layer.merged_tail
        elif "+equity" in layer.name:
            assert layer.merged_tail and layer.column_header_blind and layer.equity_wording
        elif layer.column_header_blind or layer.merged_tail:
            assert not layer.is_strict and i > strict, (
                f"{layer.name} carries a widening flag and must sit past every strict layer")


def test_the_page_flag_is_in_the_parse_key_and_the_mapping_flag_is_not():
    # `column_header_blind` moves which page is a NOTE, so two layers differing in it may not
    # share a parse; `merged_tail` only re-maps rows that have already been read, and neither
    # costs a new OCR pass.
    strict = FinancialsBuilder.LAYERS[0]
    merged = next(l for l in FinancialsBuilder.LAYERS if l.name == "onnx@200+merged")
    assert parse_key(strict) != parse_key(merged)
    assert ocr_key(strict) == ocr_key(merged)


# ── PYR-1 · a filing dated before the quarter it claims to report ─────────────
def _doc(file_date, quarter="1", year="2024", consolidated="True",
         assurance="unaudited"):
    return {"file_date": file_date, "quarter": quarter, "year": year,
            "consolidated": consolidated, "assurance": assurance}


@pytest.mark.parametrize("file_date,expected", [
    ("2024-03-29", True),      # CTG's and ANV's mislabelled FY-2023 reports
    ("2024-04-26", False),     # the genuine Q1-2024 quarterly
    ("2024-03-31", False),     # the last day of the quarter is not too early
    ("", False),               # no evidence -> no claim (§5 rule 2)
    ("nonsense", False),
])
def test_a_quarterly_cannot_be_filed_before_its_quarter_ends(file_date, expected):
    assert FinancialsBuilder._filed_before_period_end(_doc(file_date)) is expected


def test_an_annual_folded_onto_q4_is_not_premature():
    # An FY-2023 report filed 2024-03-29 contributes Q4-2023, which ended 2023-12-31.
    assert FinancialsBuilder._filed_before_period_end(
        _doc("2024-03-29", quarter="4", year="2023")) is False


def test_the_period_check_ranks_behind_entity_and_ahead_of_assurance():
    """⚠️ A wrong period is not a reason to change WHICH COMPANY the row describes, and it is a
    stronger objection than how well the document was produced.

    The first half is `documents`' oldest invariant (86 consolidated periods moved when a bare
    `dict.update` broke it); the second is why an AUDITED filing dated before its own quarter
    still loses to an UNAUDITED one dated after it — a filing that predates the period is not
    that period's report at all.
    """
    b = FinancialsBuilder()
    pref = lambda r: (0 if r["consolidated"] == "True" else 1,
                      1 if b._filed_before_period_end(r) else 0,
                      b.ASSURANCE_RANK.get(r["assurance"], 9))
    early_consolidated = _doc("2024-03-29")
    late_standalone = _doc("2024-04-26", consolidated="False")
    late_unaudited = _doc("2024-04-26")
    early_audited = _doc("2024-03-29", assurance="audited")
    assert pref(early_consolidated) < pref(late_standalone)     # entity still first
    assert pref(late_unaudited) < pref(early_audited)           # period beats assurance
