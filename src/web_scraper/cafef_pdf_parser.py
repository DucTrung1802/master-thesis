# src\web_scraper\cafef_pdf_parser.py

# ===== Standard Library =====
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Sequence

# Tesseract ships no Vietnamese data and Program Files is not writable without admin, so the
# language pack lives in a user-writable dir. PyMuPDF needs the binary on PATH and the
# tessdata directory. Both are resolved once, at import.
TESSERACT_DIR = r"C:\Program Files\Tesseract-OCR"
TESSDATA_DIR = os.environ.get(
    "TESSDATA_PREFIX", os.path.join(os.environ.get("LOCALAPPDATA", ""), "tessdata"))
OCR_LANG = "vie"

# Which OCR engine backs the image-only pages. Override via the CAFEF_OCR_ENGINE env var.
#   "tesseract" (default) — the CPU engine the reconciliation thresholds were tuned against;
#                unchanged, byte-identical behaviour.
#   "onnx"     — DeepDoc DB detection + VietOCR (see onnx_ocr.py). Experiments 8-9 measured it as
#                accuracy-tied with the PaddleOCR-server stack and ~10× faster than it; the engine
#                to re-OCR the archive with. Reads the same word boxes the parser expects.
#   "easyocr"  — a CUDA/GPU alternative (see _ocr_page): it fragments boxes differently and is NOT
#                adopted; kept for comparison.
OCR_ENGINE = os.environ.get("CAFEF_OCR_ENGINE", "tesseract").lower()

BALANCE_SHEET = "balance_sheet"
INCOME_STATEMENT = "income_statement"
CASH_FLOW = "cash_flow"
NOTES = "notes"
REPORTS = (BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW)


@dataclass
class Row:
    """One printed line of a statement."""
    label: str                      # as printed, e.g. "Tiền gửi của khách hàng"
    key: str                        # ascii snake_case, e.g. "tien_gui_cua_khach_hang"
    number: str                     # the filing's own numbering: "I", "1", "a", ""
    values: List[Optional[int]]     # one per period column, left to right


@dataclass
class Statement:
    report: str
    pages: List[int]
    unit: int                       # 1 (đồng) or 1_000_000 (Triệu VNĐ)
    n_columns: int
    rows: List[Row] = field(default_factory=list)
    publish_date: str = ""          # ISO; the day the filing was signed off
    # Share capital, read from the "Vốn cổ phần" note, not from any statement — a per-document
    # fact like publish_date, so all three statements of a filing carry the same numbers.
    shares_authorized: Optional[int] = None    # "Vốn cổ phần theo giấy phép"
    shares_issued: Optional[int] = None        # "Cổ phiếu đã phát hành" (published)
    shares_outstanding: Optional[int] = None   # "Cổ phiếu đang lưu hành"
    # ⚠️ THE STATEMENT PRINTS A STANDALONE-QUARTER COLUMN BESIDE THE CUMULATIVE ONE.
    # An interim income statement is assumed to print ONLY "Lũy kế từ đầu năm", which is why a
    # half-year or annual filing is de-cumulated (YTD − the quarters already accepted). VCB's
    # Q2-2014 prints FOUR columns — "Quý II" (this year, last year) AND "Lũy kế" (this year,
    # last year) — so column 0 is ALREADY the standalone quarter and subtracting Q1 from it
    # takes the quarter off twice: interest income 6,928,272 was written as 226,746 and PBT came
    # out at −154,988 for a bank that earned 1,345,661. Reconcile cannot see it (a quarter column
    # balances against itself perfectly) and `sane` fails open in a subset run, so nothing else
    # catches it. When this is True, `build` does NOT mark the period cumulative.
    quarter_column: bool = False
    # ⚠️ THIS STATEMENT IS THE **CONDENSED** DISCLOSURE P&L, NOT A FULL INCOME STATEMENT.
    # Mẫu CBTT-03 (Thông tư 38/2007/TT-BTC) lets an issuer publish a "BÁO CÁO TÀI CHÍNH TÓM
    # TẮT" whose profit-and-loss statement is FOUR printed lines — Tổng thu nhập, Tổng chi phí,
    # Lợi nhuận trước thuế, Lợi nhuận sau thuế — followed by non-financial disclosure rows.
    # ACB filed on that form for its early quarters, so `reconcile`'s `MIN_ROWS` floor rejects
    # a statement that is complete: Q2-2009 parses 5 rows against a floor of 12.
    #
    # ⚠️ **EVIDENCE, NOT A THRESHOLD, AND IT IS THE P&L'S OWN WORDING** — see
    # `PdfParser.CONDENSED_PL`. A full bank or corporate income statement never prints a line
    # called simply "Tổng thu nhập"/"Tổng chi phí"; it prints "Thu nhập lãi thuần", "Doanh thu
    # thuần" and so on. This is recorded whether or not any layer acts on it, because it is a
    # fact about the document — like `quarter_column` and `split_figures`.
    condensed_income: bool = False
    # ⚠️ HOW MANY FIGURES THIS READING SPLIT ACROSS TWO BOXES — see `PdfParser.split_figures`.
    # A count, not a flag, because `reconcile` reports it and a reader of the log should see
    # the size of what was refused. 0 on every reading that is not fragmented, which is every
    # bank filing measured.
    split_figures: int = 0

    @property
    def cash_flow_method(self) -> Optional[str]:
        """`indirect` | `direct`, read from THIS filing — a company chooses the method and may
        switch, so it is a property of the document, never of the sector or the template."""
        if self.report != CASH_FLOW or not self.rows:
            return None
        from web_scraper.cafef_schema import method_of
        return method_of(self.rows[0].label)

    # How close an OCR'd line name must be to count as the line we are looking for.
    NAME_MATCH = 0.85

    def _first_value(self, values: List[Optional[int]]) -> Optional[int]:
        """The current-period figure = the FIRST populated column, not literally column 0.

        Columns are left-to-right (current period first), so the leftmost non-None value is
        the current period. It is NOT always index 0: OCR sometimes over-segments the columns
        — the note-reference number on the left of the row can cluster as its own spurious
        column, pushing the real value into index 1. ACB's Q2-2010 balance sheet parsed its
        grand total as [None, 176999825…, None, 167881047…, None] — 5 columns where there are
        2 — so a strict `values[0]` read it as empty and the statement was rejected for "no
        total assets" even though the figure was parsed correctly.

        EXCEPT when the statement has exactly TWO columns, where there is nowhere for an
        over-segmented figure to hide: index 0 IS the current period and index 1 IS the
        comparative, so falling through returns LAST YEAR'S number for a line the filing
        printed as a dash. ACB's Q1-2022 does this four times — `hdkd_20` reads 9,009,073 and
        `hddt_mua_sam_bat_dong_san_dau_tu` 148,453, both of them 2021 figures against a blank
        2022 column, and both silently plausible. It also splits a wrapped label across two
        rows, leaving the first holding only the comparative ("…uy_thac_dau_tu_cho_vay_ma_tctd"
        [., -8,456]) and the continuation holding the real one ("chiu_rui_ro" [-6,890, .]);
        returning None for the first is what lets the second be found.

        The over-segmentation this guards against needs three or more columns by definition —
        a spurious column plus the two real ones — so the two cases cannot collide.
        """
        if not values:
            return None
        if self.n_columns == 2 and len(values) == 2 and values[0] is None:
            return None
        return next((v for v in values if v is not None), None)

    def find(self, *needles: str, reject: Sequence[str] = ()) -> Optional[int]:
        """The current-period value on a row whose name is (or is close to) one of `needles`.

        The match must tolerate OCR damage. The lines reconciliation depends on are exactly
        the ones it mangles — VCB's Q4-2021 balance sheet reads "TỔNG NỢ PHẢI TRẢ" as
        `tong_nuphai_tra` (ợ -> u) and the grand total as `toong_nophai_thava_von_chusohuu`.
        On an exact match both are invisible, and a statement that is complete and balanced
        (total assets == total resources, to the dong) gets rejected for "no total".

        Rows are scanned in statement order and the first hit wins, which is what keeps
        "tổng nợ phải trả" from being answered by the grand total that contains it as a
        substring — the liabilities line always precedes it.

        ⚠️ AND FIRST-HIT-WINS IS EXACTLY WHY `reject` EXISTS. Fuzzy matching plus
        statement order gives a wrong ANSWER, not a refusal, whenever two lines differ in one
        word and the wrong one is printed first. Measured on VIC Q1-2026: the closing-cash
        needle "tien va tuong duong tien cuoi ky" scores 0.90 against the row
        `tien_va_tuong_duong_tien_dau_ky` - the OPENING balance, printed two lines above - so
        `reconcile` passed on 72,226,561 for a statement that closes at 54,750,360. TPL-1
        predicted this from the charts of accounts before it was ever run.

        A row whose key contains any `reject` token is skipped OUTRIGHT, whatever it scores.
        That is the same hard-discriminator shape `_label_score` uses under `annual_tail` - "a
        row that says dau cannot be the closing balance" - and it is a discriminator rather
        than a threshold because no amount of resolution separates two labels that differ by
        one word a fuzzy window mostly ignores.
        """
        from difflib import SequenceMatcher

        flat = [n.replace(" ", "").replace("_", "") for n in needles]
        bad = [n.replace(" ", "").replace("_", "") for n in reject]
        for r in self.rows:
            v0 = self._first_value(r.values)
            if v0 is None:
                continue
            k = r.key.replace("_", "")
            if any(n in k for n in bad):
                continue
            for n in flat:
                if n in k:
                    return v0
                w = len(n)
                for i in range(0, max(1, len(k) - w + 1)):
                    if SequenceMatcher(None, n, k[i:i + w]).ratio() >= self.NAME_MATCH:
                        return v0
        return None


class PdfParser:
    """Parse a financial statement out of a CafeF filing PDF.

    Two front-ends feed ONE row-builder: the PDF's own text layer, or Tesseract OCR for the
    documents that have none (90% of VCB's filings are page scans, and not only the old
    ones — its Q1-2026 report is 53 pages of image). Both hand back word boxes in the same
    coordinate system, so the builder does not care which produced them.

    What the filing does and does not give you:

      * It prints NO item codes. CafeF's `300`/`411`/`800` are CafeF's own numbering, absent
        from the document — the left column holds section numbering (I, 1, a, VII) and the
        middle one is the "Thuyết minh" note reference. Line items are therefore keyed by the
        LABEL the filing prints, which is the only identifier that actually exists on paper.
      * It DOES print its statutory form code ("Mẫu B02/TCTD-HN"), which is how the
        statements are told apart — see `_page_kind`.

    Rows are rebuilt from WORD COORDINATES, never the raw text stream: PyMuPDF emits label
    fragments out of order, and a line-based read silently mis-assigns values.
    """

    # Every statutory statement prints its form code in the page header (Decision 16/2007 and
    # Circular 49/2014 for banks; "-DN" for everyone else). It is the reliable discriminator,
    # and the decisive one under OCR: OCR drops spaces ("cân đối kế toán" -> "kếtoán") so a
    # heading regex misses, while the auditor's report at the front NAMES every statement, so
    # heading matching tags pages that are not statements at all.
    # The prefix and separator are both optional — filings print the code inconsistently
    # ("Mâu số: B03TCTD") and OCR eats punctuation.
    # NOTE B02 is the BALANCE SHEET for a bank but the INCOME STATEMENT for a non-bank, so
    # the suffix must be honoured.
    # The suffix is matched as a PREFIX, with no word boundary after it: OCR adds junk letters
    # to it, and one of them is enough to lose a whole statement. VCB's Q2-2023 assets page
    # reads "Mẫu B02a/TCTDP-HN" — the stray P defeated a `(TCTD|DN)\b` match, and the entire
    # asset half of the balance sheet was dropped, taking TỔNG TÀI SẢN with it and failing
    # reconciliation on a filing that was perfectly readable.
    FORM_RE = re.compile(
        r"(?:M[ẫâa]u\s*(?:s[ốô]\s*)?[:.]?\s*)?\b(B\s*\d{2})\s*[a-z]?\s*[-/]?\s*(TCTD|DN)",
        re.I)
    # ⚠️ OCR ALSO APPENDS A STRAY DIGIT, and that costs far more than the code itself. VCB's
    # Q1-2009 prints "Mẫu số: B040/TCTD-HN" and its Q2-2014 balance sheet "Mẫu B020/TCTD-HN" —
    # the strict pattern tolerates a junk LETTER after the two digits but not a junk digit, so
    # neither matches and the page has NO form-coded anchor. `_drop_islands` prunes by anchor, so
    # without one every notes page that fuzzy-matches a statement title is kept: Q1-2009's income
    # statement came out as pages [5, 14, 28, 29, 30] — 57 rows of which 2 mapped — and was
    # refused for "no profit before tax" although its own page 5 read perfectly. Note 15 is
    # titled "Lãi/lỗ thuần từ hoạt động kinh doanh (mua bán) chứng khoán", which clears the title
    # threshold against "BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH".
    #
    # Up to two junk characters, of either kind. Form codes are B01..B09 — two digits, never
    # three — so anything past them is noise. Reached only via `loose_form_code`, so a filing
    # whose codes read cleanly is untouched.
    FORM_RE_LOOSE = re.compile(
        r"(?:M[ẫâa]u\s*(?:s[ốô]\s*)?[:.]?\s*)?\b(B\s*\d{2})\s*[0-9a-z]{0,2}\s*[-/]?\s*(TCTD|DN)",
        re.I)
    FORMS = {
        "TCTD": {"B02": BALANCE_SHEET, "B03": INCOME_STATEMENT,
                 "B04": CASH_FLOW, "B05": NOTES},
        "DN": {"B01": BALANCE_SHEET, "B02": INCOME_STATEMENT,
               "B03": CASH_FLOW, "B09": NOTES},
    }
    # The statement TITLE, which every page of a statement repeats in its header next to the
    # form code. Matched with accents stripped and spaces removed, so OCR's lost spaces cannot
    # defeat it.
    #
    # This is not merely a fallback: it is what saves a filing whose form-code DIGITS are
    # mangled, which is common — VCB's Q4-2021 balance sheet prints "Mẫu BU2/TCTD-HN",
    # "Mẫu Bữ2/TCTD-HN" and "Mẫu BUT/TCTD-HN" across its three pages (0 -> U / ữ, 5 -> S,
    # B -> H), so all three failed `B\d{2}` and the balance sheet was lost outright. The title
    # — "Bảng cân đối kế toán" — came through on every one of them.
    #
    # It is matched ONLY within the page's header block (HEADER_LINES). The auditor's report at
    # the front NAMES every statement in its prose, so matching the whole page tags those pages
    # as statements and drags audit text into the table; a statement announces itself at the
    # top of the page, an auditor merely mentions it further down.
    # ⚠️ **THE PRE-2015 VAS WORDING PUTS "SAN XUAT" INSIDE THE INCOME STATEMENT'S TITLE, AND
    # THE NEEDLE IS A CONTIGUOUS SUBSTRING** (`VAS-3`, 2026-09-04). Decision 15/2006 heads the
    # form "BÁO CÁO KẾT QUẢ HOẠT ĐỘNG **SẢN XUẤT** KINH DOANH"; Circular 200/2014 dropped the
    # two words, and `ketquahoatdongkinhdoanh` is that later spelling. Measured on FPT's
    # Q3-2008 consolidated filing, page 5: `ketquahoatdongkinhdoanh` scores **0.696** and
    # `baocaoketquakinhdoanh` **0.762**, both under `TITLE_MATCH` — so the page classified as
    # NOTHING, `_fill_continuations` handed it to the balance sheet running above it, and the
    # income statement was reported `only 1 rows parsed` from an unrelated page. The full
    # wording scores **1.000**.
    #
    # ⚠️ **A LONGER NEEDLE CANNOT STEAL A PAGE, AND THAT WAS MEASURED RATHER THAN ARGUED.**
    # `_page_kind` takes the BEST-scoring title, so adding one can only raise the income
    # statement's score on a page — and a longer needle scores LOWER on unrelated text, not
    # higher. Across the **7,404 text-layer pages of the eight parsed tickers**, exactly
    # **11 pages** change verdict and **every one is `None` -> income_statement**: FPT
    # Q1-2007, Q3-2008, Q2-2009, Q3-2009, Q4-2009, Q1-2010, Q2-2010, Q2-2011, Q2-2007 and ACB
    # Q1-2007, Q2-2007. Not one page moves between statements and not one is lost.
    HEADING = {
        BALANCE_SHEET: ["bangcandoiketoan", "baocaotinhhinhtaichinh"],
        INCOME_STATEMENT: ["ketquahoatdongkinhdoanh", "ketquahoatdongsanxuatkinhdoanh",
                           "baocaoketquakinhdoanh"],
        CASH_FLOW: ["luuchuyentiente"],
    }
    NOTES_NS = "thuyetminhbaocao"

    # ⚠️ **THE STATEMENT TABLE'S OWN COLUMN HEADING CARRIES THE NOTES TITLE'S FIRST TEN
    # CHARACTERS, AND THAT IS ENOUGH TO CLASSIFY A CONTINUATION PAGE AS A NOTE.** The standard
    # VAS forms print `Chi tieu | Thuyet minh | So cuoi quy | So dau nam` at the top of every
    # page of every statement, and a CONTINUATION page has no title of its own -- so that row
    # is the whole header. Scored against `NOTES_NS` it returns **0.8125**, one hundredth over
    # `TITLE_MATCH`, because ten of the needle's sixteen characters are the shared words
    # "thuyet minh"; the remaining six ("bao cao") are matched three-for-six against "so cuoi".
    #
    # ⚠️ A NOTES page ENDS THE RUN in `_fill_continuations`, so the cost is the rest of the
    # statement. Measured on CTG's Q1-2009 consolidated filing: the balance sheet runs pages
    # 1-4, pages 2, 3 and 4 each read as NOTES, and the statement was truncated to page 1 --
    # **17 rows, no TONG TAI SAN**, refused `no total assets` on all 55 layers. The quarter had
    # been `missing` since the ticker was first parsed.
    #
    # The verdict is re-taken on a header with this row removed, and ONLY the notes verdict:
    # the row can never be evidence FOR a statement title, so removing it cannot lose one, and
    # a genuine notes page whose own title sits elsewhere in the header still scores. Both
    # halves are required -- the flag permits it and the header must actually carry the row.
    COLUMN_HEADER_NS = ("chitieu", "thuyetminh")

    # The auditor's report at the front of every filing. It is NOT a statement, but its header
    # says "Báo cáo tài chính hợp nhất…", which is close enough to the balance sheet's own
    # title ("Báo cáo TÌNH HÌNH tài chính") to fool a fuzzy match — and once it does, the
    # contiguity fill drags the whole audit section into the table. A page that announces
    # itself as a review or an audit opinion is never a statement.
    AUDIT_NS = ("baocaosoatxet", "soatxetthongtintaichinh", "baocaokiemtoandoclap",
                "baocaocuakiemtoanvien", "ykienkiemtoan")

    # ⚠️ **THE FILING APPENDS A VARIANCE EXPLANATION AFTER THE INCOME STATEMENT, IT IS A
    # TABLE, AND IT IS PRINTED IN A DIFFERENT UNIT — `GTR-1`, 2026-09-04.** Circular 155/2015 makes an issuer explain
    # any profit swing over 10 %, and FPT prints that explanation on the page immediately
    # AFTER its income statement: a five-column grid (this quarter, last year's quarter,
    # the two year-to-date columns, the change) under the heading "GIẢI TRÌNH:", in
    # **"ĐVT: Triệu đồng"** where the statement itself is in đồng.
    #
    # It carries a real table, so `_fill_continuations` absorbs it as the statement's second
    # page — and its rows are the statement's OWN account names. Measured on HOSE_FPT
    # 2026-09-04, **twelve quarters, every Q1 and Q3 from 2020-Q3 to 2026-Q1**: the row
    # "Tổng lợi nhuận kế toán trước thuế" appears TWICE in the parsed statement, once from the
    # statement (whose own columns the six-column grid then mis-clusters into `None`) and once
    # from the explanation, in millions. `Statement.find` skips a row with no value and
    # returns the SECOND, so `sane` bands the quarter on 8,111,171 against a typical 6.58e11
    # and refuses it:
    #
    #     sane: magnitude 8.11e+06 vs typical 6.58e+11 (units? cumulative column? OCR misread?)
    #
    # ⚠️ Each of those twelve blocks a CUMULATIVE Q2/Q4 that then has no prior to subtract, so
    # the twelve are worth roughly twenty-four cells.
    #
    # This is `AUDIT_NS`' rule at the other end of the filing — *a page that announces itself
    # as something other than a statement is never a statement* — and it has to be a DEFAULT
    # path change for `AUDIT_NS`' reason too: the income statement is accepted at layer 1 on
    # every one of these filings, so no later layer is ever reached (§6-2-untricies: when the
    # gates cannot see the defect, the repair cannot be an escalation).
    #
    # ⚠️ **IT CAN ONLY EVER REFUSE A PAGE THE CLASSIFIER COULD NOT IDENTIFY.** The test is
    # applied in `_fill_continuations`, on the branch that absorbs an UNIDENTIFIED page into
    # the run above it — a page carrying its own form code or its own statement title is
    # `kind in REPORTS` and never reaches it. So the worst it can do is end a run one page
    # early, and only on a page whose header says "giải trình".
    SUPPLEMENT_NS = ("giaitrinh",)

    HEADER_LINES = 12       # the page header: company, form code, statement title, period
    TITLE_MATCH = 0.80      # how close an OCR'd title must be to count as that statement
    MIN_TABLE_WORDS = 15    # a page with fewer figures than this is not a statement page

    # ⚠️ **A LANDSCAPE STATEMENT SCANNED INTO A PORTRAIT PAGE READS AS NOTHING, AND THE PDF
    # SAYS `/Rotate 0`.** BID's Q3-2011 consolidated income statement is page 6 of 32, its
    # image turned 90°, and every layer of the cascade reported `no such statement on any page
    # of this filing` — the quarter had been `missing` since the ticker was first parsed. The
    # page's own `/Rotate` cannot help: it is 0, like every other page of that scan.
    #
    # The DETECTOR alone settles it, because a text LINE is wide and a rotated one is tall.
    # Measured over all 32 pages of that filing (2026-08-30), share of boxes taller than wide:
    #
    #     upright pages   0-19 %,  median w/h 4.3 … 7.8
    #     rotated pages  92-100 %, median w/h 0.16 … 0.25
    #
    # Two orders of magnitude apart with nothing in between, so 0.70 is a cut with room on
    # both sides rather than a tuned threshold. ⚠️ **AND IT COSTS NOTHING TO ASK**: the boxes
    # are the ones the upright read has ALREADY returned, so an upright page pays not one
    # extra pixel. Only a page the signal condemns is read again.
    VERTICAL_LINES_SHARE = 0.70
    # …of at least this many recognised boxes. A page holding a handful of stray marks must not
    # be turned on the strength of three of them; every rotated page measured carried 46+.
    MIN_ROT_WORDS = 20
    # ⚠️ **THE DETECTOR CANNOT TELL 90 FROM 270** — both make the lines horizontal — so the
    # direction is decided by READING the page each way and counting the tokens that parse as
    # numbers. Upside down, digits do not: on BID's Q3-2011 income statement that is 100
    # against 7. ⚠️ **AND IT IS NOT ONE DIRECTION PER DOCUMENT**: in that same filing the
    # income statement needs 90 and all eleven rotated NOTES pages need 270 (measured, which
    # is the only reason this is a per-page decision).
    #
    # The probe renders at a low DPI because it is counting numbers, not reading them, and the
    # answer is cached for the life of the document — so it is paid once per rotated page, not
    # once per OCR pass.
    ROT_PROBE_DPI = 100
    # ⚠️ **A PAGE THIS FAR SHORT OF TEXT IS NOT A PAGE WITHOUT CONTENT, IT IS A PAGE READ THE
    # WRONG WAY UP** — `ROT-3`. FPT's Q1-2012 / Q3-2011 / Q3-2013 consolidated income statements
    # read 31, 56 and 32 characters upright and 2,525-2,797 at +90. Measured over 460 pages of
    # 11 filings, **38 (8.3 %)** fall under this floor and **27** of those really are turned;
    # the other 11 stay upright because a rotation must first read MORE than the base does. It
    # bounds the CANDIDATE set and decides nothing — every candidate is settled by reading it.
    MIN_UPRIGHT_CHARS = 200

    # ⚠️ A STATEMENT'S FINAL PAGE IS LEGITIMATELY SPARSE, AND `MIN_TABLE_WORDS` REFUSES IT.
    # The last page holds the closing rows and then the signature block, so it carries a
    # fraction of a full page's figures — and for the CASH FLOW that is the one page that must
    # not be lost: it prints the opening balance, the FX adjustment and the CLOSING balance,
    # which is the anchor `reconcile` refuses the whole statement without.
    #
    # BID's Q1-2012 consolidated cash flow runs pages 5-7. Page 7 holds codes 53/54/55 —
    # opening 48,919,272,456,242 and closing 43,180,157,643,381, every digit read correctly —
    # in **13 numeric words against a threshold of 15**. `_fill_continuations` dropped it, and
    # the quarter was recorded `missing` for "no closing cash balance" while the balance sat
    # on a page that had been thrown away for being two numbers short.
    #
    # Lowering `MIN_TABLE_WORDS` is not the fix: it is what keeps a signature page or a
    # narrative page out of a statement, and a tail page IS mostly signature. So the page is
    # admitted on POSITIVE evidence instead — it must carry the statement's own closing line.
    # Reached only through `tail_continuation`, so no statement that parses today is re-judged.
    TAIL = {
        # "Tiền và các khoản tương đương tiền tại thời điểm cuối kỳ". Accents stripped and
        # spaces removed like every other header test, and cut before "tại" so the phrase
        # matches whether the filing DATES the line ("tại ngày 31 tháng 3") or names the
        # period — the same split `FinancialsBuilder.CASH_TAIL` makes for the same reason.
        # ⚠️ **AND THE FILING MAY DROP "CAC KHOAN" ALTOGETHER.** CTG words the same line
        # "Tien va tuong duong tien tai thoi diem cuoi ky" from at least 2014, and the two
        # spellings share no substring long enough to cover both -- so the needle set carries
        # each. Measured on CTG's Q1-2014 consolidated filing: the closing balance
        # 88.180.310.933.901 is read correctly at `onnx@200`, on a page this test then refused,
        # and the quarter was `missing` for `no closing cash balance` on all 55 layers.
        CASH_FLOW: ("tienvacackhoantuongduongtien", "tienvatuongduongtien"),
    }
    # A tail page still has to be a TABLE, just a small one. WARN **AND THE FLOOR IS TWO, NOT
    # FOUR: A TAIL PAGE MAY CARRY THE CLOSING LINE AND NOTHING ELSE.** This was set at 4 on the
    # assumption that all three cash-balance rows land on it ("the three cash-balance rows carry
    # two period columns each"); CTG's Q1-2014 breaks the page after the FX line, so its tail
    # page holds one row -- a note reference and two period figures, **three numbers** -- and
    # was refused for holding too few. Two period columns is what the closing line itself
    # carries, and is therefore the true floor.
    #
    # ⚠️ Lowering it costs nothing, because the count was never the guard: a page qualifies
    # only by carrying the statement's own closing line (`TAIL`), which is POSITIVE evidence a
    # signature or narrative page cannot fake. The count only stops a page holding one stray
    # figure -- a page number, a date -- from reaching that test at all.
    MIN_TAIL_WORDS = 2

    # ⚠️ **THE FILING ITSELF PUTS THE NOTES HEADER ON A STATEMENT'S CONTINUATION PAGE, AND
    # `_fill_continuations` READS A NOTES PAGE AS "THE STATEMENTS ARE OVER".** TCB's Q2-2019,
    # Q3-2019 and Q1-2021 consolidated filings print "THUYẾT MINH BÁO CÁO TÀI CHÍNH HỢP NHẤT"
    # at the top of the cash flow's SECOND page — the page carrying sections II, III, IV and
    # the opening, FX and CLOSING balances — and Q1-2021 stamps the notes FORM CODE on it too
    # ("Mẫu B050/TCTD - HN", B05 = notes). Rendered and read off the page image: it is the
    # document's own header, not OCR damage, and its period line is a broken Word mail-merge
    # field ("cho giai đoạn từ ngày REF Yea01 …"). So no engine, DPI or crop setting reaches
    # it, and all three quarters were `missing` for `no closing cash balance` on every layer.
    #
    # ⚠️ **AND `TAIL` ALONE CANNOT ADMIT IT, WHICH IS THE SECOND HALF OF THE DEFECT.** That
    # needle is a contiguous phrase, and on these pages the closing line's label WRAPS AROUND
    # its own figures — "TIỀN VÀ CÁC KHOẢN TƯƠNG ĐƯƠNG" / "VII 33 47.141.880 50.050.197" /
    # "TIỀN TẠI THỜI ĐIỂM CUỐI KỲ" — so the flattened page reads
    # "…tienvacackhoantuongduongvii3317141488050050197tientaithoidiemcuoiky…" and
    # "tienvacackhoantuongduongtien" is broken by the numerals between the halves.
    #
    # So the evidence is taken in TWO parts, each of which survives the wrap:
    #   * the statement's own SECTION heading, which only that statement prints. A cash flow
    #     is the only place "LƯU CHUYỂN TIỀN THUẦN" appears — the title in `HEADING` is "lưu
    #     chuyển tiền TỆ", a different phrase, so this is not a second copy of it.
    #   * the CLOSING BALANCE's own date clause, "tại thời điểm cuối kỳ/năm/quý", which a note
    #     merely TITLED "Tiền và các khoản tương đương tiền" does not carry.
    #
    # ⚠️ **MEASURED OVER EVERY PAGE OF ALL THREE FILINGS BEFORE IT SHIPPED (2026-09-04):**
    # 197 pages, and exactly TWO per filing carry either marker — the cash flow's own two
    # pages. Zero notes pages, zero signature pages, zero of the 30-odd note tables that
    # follow. Reached only through `notes_tail`, so no statement that parses today is
    # re-judged, and the run ENDS at the page it admits.
    # ⚠️ RENAMED FROM `TAIL_SECTION` ON 2026-09-04, hours after it was added, because it stopped
    # being about tails: `notes_head` uses the same needle to OPEN a run. A constant whose name
    # says where it is used rather than what it is drifts the first time it is used elsewhere.
    SECTION_HEADING = {
        CASH_FLOW: "luuchuyentienthuan",
    }
    TAIL_CLOSING = ("taithoidiemcuoiky", "taithoidiemcuoinam", "taithoidiemcuoiquy")

    # How far below the last line that fed it a pending wrapped label survives a line that
    # contributed nothing. Rows on these statements are set 13-32pt apart and a wrapped half
    # sits 6-13pt from its own text, so 24pt admits one intervening noise line and no more.
    # `label_wrap` only.
    CARRY_GAP = 24.0

    # ⚠️ **THE OPENING BRACKET OF A NEGATIVE FIGURE COMES BACK AS A QUOTE MARK.** A `(` printed
    # tight against a digit is a thin arc, and on BID's Q3-2011 income statement the recogniser
    # returned `"9,797,589,605,016)` and `"299,126,415,190)` — the CLOSING bracket intact, so
    # nothing is ambiguous about the sign. `parse_num` refused both, `table_rows` left column 0
    # empty, and `_first_value` then took the **prior-period** column instead: interest expense
    # read 5,417,947,722,487 where the filing prints 9,797,589,605,016, and it RECONCILED,
    # because the only anchor an income statement is checked on is PBT and that cell was sound.
    # `SLD-1`'s shape once more — a wrong figure every gate passes.
    #
    # The second branch is deliberately narrow: the mark stands in for `(` only where the
    # matching `)` is there to prove a bracket was printed. A token that merely STARTS with a
    # quote is not a number, then or now. `parse_num` does the same substitution.
    QUOTE_FOR_BRACKET = "\"'`´|"
    NUM_RE = re.compile(r"^\(?-?[\d][\d.,]*\)?$"
                        r"|^[" + QUOTE_FOR_BRACKET + r"]+\(?[\d][\d.,]*\)$"
                        r"|^[-–—]$")
    # The filing's own row numbering, in the left margin.
    NUMBER_RE = re.compile(r"^[IVXLC]+$|^\d{1,2}$|^[a-z]$|^[A-Z]$", re.I)

    # The Vietnamese date a document is signed off on: "…ngày DD tháng MM năm YYYY". Tolerant of
    # OCR and of the legacy TCVN3 font that garbles the letters ("ngµy 20 th¸ng 07 n¨m 2009") —
    # the digits stay ASCII, which is all that is needed.
    SIGN_RE = re.compile(
        r"ng[^\s]{0,3}y\s*(\d{1,2})\s*th[^\s]{0,3}ng\s*(\d{1,2})\s*n[^\s]{0,3}m\s*(\d{4})",
        re.I)
    # The approval line the statements are signed under.
    APPROVED_NS = ("phe duyet", "duoc ban dieu hanh phe duyet", "lap ngay", "ngay lap")

    OCR_DPI = 200          # numbers need the resolution; below this, digits are lost
    MIN_PAGE_TEXT = 200    # a page with less native text than this is an image -> OCR it
    # Words whose boxes START within this many points share a row. Raised 3.0 -> 4.0: a label is
    # typeset taller than the digits beside it (diacritics above, descenders below), so its box
    # begins slightly higher up, and at 3.0 the two fell either side of the line. ACB's Q1-2024
    # cash flow puts VI's label 3.36pt below its own figures, which paired the figures with the
    # STALE wrapped label above them ("11 THÁNG 1") and left VI empty, so `_first_value` took the
    # comparative column and the statement filled with prior-year numbers.
    #
    # 4.0 is not a round number — it is where that page's gaps actually separate. Same-line pairs
    # measure 0.72, 1.20 and 3.36pt; the nearest DIFFERENT-line gap is 4.80pt, and rows are set
    # 13-18pt apart, so the cut has room on both sides.
    Y_TOL = 4.0            # words within this many points share a row
    VALUE_ZONE = 0.40      # period columns live in the right 60% of the page
    EDGE_TOL = 9.0         # right edges within this many points are the same column
    LABEL_GAP = 30.0       # a word ending this far left of column 1 belongs to the label

    def __init__(self, logger=None, engine=None, dpi=None):
        self._logger = logger
        # Engine and render DPI are per-instance so a caller can sweep configurations — the
        # cascade in cafef_financials retries a statement at a higher DPI or a different engine
        # until it reconciles (see FinancialsBuilder.CASCADE). They default to the module/class
        # values, so an unparametrised parser behaves exactly as before.
        self.engine = (engine or OCR_ENGINE)
        self.dpi = dpi or self.OCR_DPI
        self._onnx = None
        # set per PARSE LAYER; see _join_split_number
        self.join_split_digits = False
        self.join_lost_separator = False
        # set per PARSE LAYER; see _page_kind
        self.title_over_form = False
        # set per PARSE LAYER; see FORM_RE_LOOSE
        self.loose_form_code = False
        # set per PARSE LAYER; see _value_row_offset
        self.realign_rows = False
        # set per PARSE LAYER; see _drop_after_notes
        self.notes_boundary = False
        # set per PARSE LAYER; see TAIL / _is_tail_page
        self.tail_continuation = False
        # set per PARSE LAYER; see SECTION_HEADING / _is_notes_tail_page
        self.notes_tail = False
        # set per PARSE LAYER; see SECTION_HEADING / _notes_head_report
        self.notes_head = False
        # set per PARSE LAYER; see table_rows' carry
        self.label_wrap = False
        # set per PARSE LAYER; see table_rows' second bucketing pass
        self.reseat_words = False
        self.deskew_rows = False
        # set per PARSE LAYER; see document_unit
        self.unit_from_document = False
        # set per PARSE LAYER; see COLUMN_HEADER_NS / _page_kind
        self.column_header_blind = False
        # ⚠️ **PROGRESS HOOK, AND THE ONLY DENOMINATOR IN THIS FILE THAT PREDICTS TIME.**
        # `on_page(index, total)` is called once per page of `scan`, before it is read. Pages
        # of one document cost roughly the same (0.87 s/page at onnx@200, measured for `P41`),
        # so a page fraction really is a fraction of the WORK — where a document count and a
        # layer index are positions in a list of wildly unequal items. `None` = no reporting,
        # which is every caller but `pdf_ocr_job`.
        self.on_page = None
        # ⚠️ **THE PIXELS→TEXT STEP DEPENDS ON `(engine, dpi, crop_pad)` AND ON NOTHING ELSE,
        # SO IT IS CACHED PER PAGE UNDER EXACTLY THAT KEY.** Every other per-layer knob —
        # `join_digits`, `title_over_form`, `loose_form_code`, `realign_rows`,
        # `notes_boundary`, `tail_continuation`, `label_wrap`, `unit_from_document` — runs
        # AFTER the page has been read, in `_page_kind`, `_fill_continuations`, `table_rows`
        # or `parse`, and cannot change a single recognised character. `_parse_cascaded`
        # caches a whole parse under `parse_key`, which carries all of them: **24 distinct
        # keys over the 49 layers against 7 distinct OCR configurations** (counted
        # 2026-08-30), so a filing that defeats the cascade was re-reading every page of
        # itself 24 times to produce 7 different answers.
        #
        # ⚠️ **AND IT IS WHAT MAKES `share_capital` AFFORDABLE.** That scan walks from the
        # last statement page to the END of the document and is invisible to `on_page`, so
        # it never appeared in a run log: measured on BID's FY-2016 filing it OCR'd **50
        # pages in 84.8 s, 68.8 % of one `parse()`**, and returned nothing. It runs once per
        # `parse()`, i.e. once per parse key.
        #
        # Scoped to ONE document — cleared the moment `parse` is handed a different path —
        # because a parser instance is reused across a whole run (`_parser_for`) and the
        # cache would otherwise grow without bound. `TODO.md` `P41`/`P42`.
        self._ocr_cache: Dict[tuple, tuple] = {}
        self._ocr_cache_path: Optional[str] = None
        # The absolute `/Rotate` each page is read at, decided ONCE per document — see
        # `VERTICAL_LINES_SHARE`. Scoped to one filing like `_ocr_cache`, and for the same
        # reason: a parser instance outlives the document.
        self._page_rot: Dict[int, int] = {}
        self.ocr_ready = self._init_ocr()

    def _ocr_config(self) -> tuple:
        """The three things that decide what the OCR returns. See `_ocr_cache`."""
        pad = self._onnx.crop_pad if (self.engine == "onnx" and self._onnx is not None) else None
        return (self.engine, self.dpi, pad)

    def _use_document(self, pdf_path: str) -> None:
        """Point the page cache at one filing, discarding the previous one's pages."""
        if self._ocr_cache_path != pdf_path:
            self._ocr_cache = {}
            self._page_rot = {}
            self._ocr_cache_path = pdf_path

    def set_dpi(self, dpi: int) -> None:
        """Re-point this parser at a new render DPI, reusing the loaded OCR models. The onnx
        engine renders at this DPI; the Tesseract path passes it to `get_textpage_ocr`."""
        self.dpi = dpi
        if self._onnx is not None:
            self._onnx.dpi = dpi

    def set_join_split(self, on: bool) -> None:
        """Treat a lost thousands SEPARATOR as one number rather than several (see
        `_join_split_number`). Set per parse layer, off by default."""
        self.join_split_digits = bool(on)

    def set_join_lost_separator(self, on: bool) -> None:
        """Treat a whitespace-separated numeric run as ONE figure whenever joining ALL of its
        parts yields a well-formed grouped figure — a thousands separator read as a space,
        anywhere in the number (see `_split_number_runs`). Wider than `set_join_split`, which
        only covers a bare 1-3 digit head. Set per parse layer, off by default."""
        self.join_lost_separator = bool(on)

    def set_loose_form_code(self, on: bool) -> None:
        """Tolerate junk characters OCR appends to a form code, so the page keeps its ANCHOR and
        `_drop_islands` can prune the notes pages that merely echo a statement title."""
        self.loose_form_code = bool(on)

    def set_title_over_form(self, on: bool) -> None:
        """Let a VERBATIM statement title overrule a form code that names a different
        statement — for a filing that mis-stamps its own pages (see `_page_kind`)."""
        self.title_over_form = bool(on)

    def set_realign_rows(self, on: bool) -> None:
        """Re-pair labels with figures when the OCR emits the two at a CONSTANT vertical offset.

        A printed line is one label and its figures, but the detector boxes them separately, and
        on some scans every numeric box lands a fixed distance above the text box of the same
        line. Past `Y_TOL` the two never group, and `table_rows` then hands each figure to the
        label line ABOVE it via `carry` — so the whole statement slides by one row while every
        digit is read correctly. See `_value_row_offset` for how the offset is measured and why
        this is a layer rather than a wider `Y_TOL`.
        """
        self.realign_rows = bool(on)

    def set_notes_boundary(self, on: bool) -> None:
        """Stop a fuzzy TITLE match from re-opening a statement after the notes have begun.

        `_drop_islands` prunes a stray title page by its distance from a FORM-CODED one, so a
        filing whose every form code is unreadable cannot be pruned at all. See
        `_drop_after_notes` for what this puts in its place and why it is a layer.
        """
        self.notes_boundary = bool(on)

    def set_tail_continuation(self, on: bool) -> None:
        """Admit a statement's sparse FINAL page, which `MIN_TABLE_WORDS` refuses.

        See `TAIL`. The page must carry the statement's own closing line, so this widens what
        counts as a continuation by EVIDENCE rather than by lowering a threshold.
        """
        self.tail_continuation = bool(on)

    def set_notes_tail(self, on: bool) -> None:
        """Admit a page whose HEADER says notes but whose CONTENT continues the statement.

        `_fill_continuations` reads a NOTES page as "the statements are over" and ends the run
        — which is right, and wrong for a filing that prints its own notes header on a
        statement's continuation page. See `SECTION_HEADING` for the three TCB quarters this was
        measured on and for the two-part evidence a page must carry to be admitted.
        """
        self.notes_tail = bool(on)

    def set_notes_head(self, on: bool) -> None:
        """Let a NOTES-headed page START a statement, not merely continue one.

        `set_notes_tail` extends a run that is already open; this OPENS one, which is the wider
        claim and is why it is a separate flag on later layers. See `_notes_head_report`.
        """
        self.notes_head = bool(on)

    def set_label_wrap(self, on: bool) -> None:
        """Reassemble a label that WRAPPED AROUND its own value line.

        `table_rows` builds a row's label from the lines ABOVE the figures. That is right when
        the label wraps upward, and wrong when the figures sit BETWEEN the label's two halves —
        the second half is then either discarded (it becomes the next row's carry) or, worse,
        the first half is discarded before it and the row is keyed on the fragment. Both halves
        of that failure are repaired here; see `table_rows`.
        """
        self.label_wrap = bool(on)

    def set_reseat_words(self, on: bool) -> None:
        """Re-seat every word on the NEAREST final line bucket, not merely a near one.

        `_line_key` already prefers the nearest bucket -- but only among buckets that ALREADY
        EXIST, and buckets are opened in the recogniser's emission order, which has nothing to
        do with the page. So a word whose own line has not been opened yet joins a neighbour
        it is merely near, and the answer depends on the order the words arrived in.

        A second pass over the FINAL key set removes that dependence. See `table_rows`.
        """
        self.reseat_words = bool(on)

    def set_deskew_rows(self, on: bool) -> None:
        """Correct a whole page's SCAN SKEW before the words are grouped into lines.

        ⚠️ **A SKEWED PAGE IS NOT `SLD-1` AND `realign_rows` CANNOT REACH IT.** That flag
        shifts every figure by ONE CONSTANT; a skew is a drift that GROWS with x, so the
        further right a figure sits the further it has slid, and no single offset fixes more
        than one column of it.

        ⚠️ **MEASURED ON FPT Q3-2024, PAGE 8 (`SKW-2`).** Line 1 of the income statement runs
        `y0 = 172.08` at the label (x=84) to `y0 = 181.20` at the last figure (x=734) — **9.1
        pt of drift across 650 pt of width, about 0.8 degrees**. `Y_TOL` is 4.0, so the label
        and the two right-hand columns never group: line 1 kept the columns it could reach and
        its remaining figures were handed to the label BELOW it, and so on down the page. The
        result is a statement whose every row carries its neighbour's figures — read
        correctly, seated wrongly — which is `SLD-1`'s consequence from a different cause.

        The slope is measured from the page itself and never assumed: see `_page_skew`.
        """
        self.deskew_rows = bool(on)

    def set_column_header_blind(self, on: bool) -> None:
        """Stop the statement TABLE's own column heading from classifying a page as NOTES.

        See `COLUMN_HEADER_NS`. Scoped to the notes verdict, so it can prevent a
        classification and never destroy a statement title.
        """
        self.column_header_blind = bool(on)

    def set_unit_from_document(self, on: bool) -> None:
        """Let a statement that names no unit take the one the rest of the filing names.

        See `document_unit`. Off by default: it changes every figure of the statement it
        touches by a factor of a million, so it may only judge a statement that has already
        been refused.
        """
        self.unit_from_document = bool(on)

    def set_crop_pad(self, pad: Optional[float]) -> None:
        """How far outside a detected box to crop before RECOGNISING it (onnx only; points).

        `None` restores the engine default. A wider crop recovers a leading digit the detector
        box cut off — ACB's Q3-2023 reads 93.261.018 as 261.018 at the default and correctly at
        6 — and it is set per PARSE LAYER rather than globally, so only a statement that has
        already failed every other layer is ever re-read this way. A no-op on the other engines,
        which do their own cropping.
        """
        if self._onnx is None:
            return
        if pad is None:
            # imported lazily, like the engine itself — this module must stay importable on a
            # machine with no onnxruntime
            from web_scraper.onnx_ocr import CROP_PAD_PT
            pad = CROP_PAD_PT
        self._onnx.crop_pad = pad

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger.log_info(msg)

    # EasyOCR's Reader is expensive to build (loads the detection+recognition models onto the
    # GPU) and holds VRAM, so it is a process-wide singleton — built once, reused by every
    # PdfParser and every page.
    _easyocr_reader = None

    def _init_ocr(self) -> bool:
        if self.engine == "onnx":
            try:
                from web_scraper.onnx_ocr import OnnxOcr
                self._onnx = OnnxOcr(self._logger, dpi=self.dpi)
                return True
            except Exception as e:
                self._log(f"onnx OCR unavailable ({e}); OCR disabled")
                return False
        if self.engine == "easyocr":
            try:
                import easyocr  # noqa: F401  (import-time check only)
                return True
            except Exception as e:
                self._log(f"easyocr unavailable ({e}); OCR disabled")
                return False
        if os.path.isdir(TESSERACT_DIR) and TESSERACT_DIR not in os.environ.get("PATH", ""):
            os.environ["PATH"] = TESSERACT_DIR + os.pathsep + os.environ.get("PATH", "")
        if TESSDATA_DIR:
            os.environ.setdefault("TESSDATA_PREFIX", TESSDATA_DIR)
        return os.path.isfile(os.path.join(TESSDATA_DIR, f"{OCR_LANG}.traineddata"))

    @classmethod
    def _easyocr(cls):
        """Lazily build (once) and return the shared GPU EasyOCR reader."""
        if cls._easyocr_reader is None:
            import easyocr
            cls._easyocr_reader = easyocr.Reader(["vi"], gpu=True, verbose=False)
        return cls._easyocr_reader

    # ──────────────────────────────────────────────────────────────────────
    # Text helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def norm(s: str) -> str:
        s = (s or "").replace("đ", "d").replace("Đ", "D")
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"[^A-Za-z0-9]+", " ", s).strip().lower()
        return re.sub(r"\s+", " ", s)

    @classmethod
    def slug(cls, label: str, maxlen: int = 60) -> str:
        s = cls.norm(label)
        s = re.sub(r"^(?:[ivxlc]+|\d+|[a-z])\s+", "", s)   # drop the leading numbering
        return re.sub(r"\s+", "_", s.strip())[:maxlen].strip("_")

    # A separator followed by ONE OR TWO digits at the very end of a figure is a DECIMAL
    # point, not a thousands separator — in either convention, because a thousands group is
    # always three digits.
    #
    # ⚠️ **MEASURED ON TCB, WHOSE 2012 FILINGS PRINT `9,729,852.10` — COMMA THOUSANDS AND A
    # DOT DECIMAL, THE INTERNATIONAL CONVENTION.** Stripping both separators read that as
    # 972,985,210: every figure of the statement 100x too large, internally consistent, and
    # therefore invisible to `reconcile`. Q2-2012's income statement went to disk with a
    # PROFIT BEFORE TAX OF 163,042,899 MILLION — 163 tn for a bank with 180 tn of assets —
    # and stayed there, because `sane` had no band when it was written. It then POISONED the
    # band for every later quarter: TCB Q3-2013's own income statement, read correctly at
    # 97,315 mn, was refused as a magnitude outlier against a median that one 100x row had
    # dragged up (`sane: magnitude 9.73e+10 vs typical 2.25e+12`).
    #
    # ⚠️ **THE TAIL LENGTH IS THE WHOLE TEST, AND IT HAS TO BE: WHICH CHARACTER IS THE DECIMAL
    # POINT CANNOT BE READ OFF THE CHARACTER.** OCR confuses `.` and `,` constantly, so
    # "1,234.567" is an ordinary Vietnamese figure with one mangled separator and must stay
    # 1,234,567 — three digits after the last separator, so it is a thousands group. Only a
    # one- or two-digit tail is a decimal, which no thousands group can be.
    #
    # ⚠️ The fraction is ROUNDED away: every figure here is an integer of the statement's own
    # unit, and a filing that prints hundredths of a million is stating đồng this scale does
    # not carry. On the measured case that is 990,000 VND on 1.63 tn.
    DECIMAL_TAIL_RE = re.compile(r"^(?P<int>-?[\d.,]*\d)[.,](?P<frac>\d{1,2})$")

    @classmethod
    def parse_num(cls, t: str) -> Optional[int]:
        t = (t or "").strip()
        if t in ("-", "–", "—"):
            return 0
        # See `NUM_RE`: an opening bracket the recogniser read as a quote. Only where the
        # closing one survived, so the bracket is evidence and not a guess.
        if t.endswith(")"):
            t = re.sub(r"^[" + cls.QUOTE_FOR_BRACKET + r"]+(?=\(?\d)", "(", t)
        neg = t.startswith("(") and t.endswith(")")
        t = t.strip("()")
        if not re.fullmatch(r"-?[\d.,]+", t):
            return None
        frac = 0.0
        m = cls.DECIMAL_TAIL_RE.match(t)
        if m:
            t, frac = m.group("int"), int(m.group("frac")) / 10 ** len(m.group("frac"))
        d = re.sub(r"[.,]", "", t)
        if not d.lstrip("-").isdigit() or not d.strip("-"):
            return None
        v = int(round(abs(int(d)) + frac))
        return -v if (neg or d.startswith("-")) else v

    # ──────────────────────────────────────────────────────────────────────
    # Pages
    # ──────────────────────────────────────────────────────────────────────

    def _page_kind(self, text: str):
        """-> (which statement this page is, whether a FORM CODE said so).

        The form code is definitive when it survives OCR. When its digits do not, the
        statement title in the same header block answers the same question — so both are read,
        and the title is confined to the header so an auditor's report merely NAMING a
        statement cannot masquerade as one.
        """
        # A TABLE OF CONTENTS is not a statement. A filing opens with a "NỘI DUNG" page that
        # lists every statement WITH its form code ("Bảng cân đối kế toán hợp nhất (Mẫu
        # B02/TCTD-HN)", B03, B04 below it). A form code is otherwise trusted absolutely, so that
        # page was classified as the first statement it named, anchored the run pages early, and
        # fed its own page numbers into the period-column clustering. A real statement page
        # carries exactly ONE form code, its own; two or more distinct ones mean the page is
        # talking ABOUT the statements. (Found via experiment_8 on ACB's FY-2013 filing.)
        form_re = self.FORM_RE_LOOSE if self.loose_form_code else self.FORM_RE
        codes = {re.sub(r"\s+", "", m.group(1)).upper()
                 for m in form_re.finditer(text)}
        if len(codes) > 1:
            return None, False

        m = form_re.search(text)
        if m:
            code = re.sub(r"\s+", "", m.group(1)).upper()
            kind = self.FORMS[m.group(2).upper()].get(code)
            if kind:
                # ⚠️ THE FORM CODE CAN BE WRONG IN THE FILING ITSELF, and then trusting it
                # absolutely loses a whole statement. VCB's Q2-2014 interim report stamps
                # "Mẫu B04a/TCTD-HN" on BOTH its income statement (page 9) and its cash flow
                # (page 11) — B04 maps to the cash flow, so the income statement is claimed as
                # one and disappears; its balance sheet is correctly B02a, so the document is
                # not garbled, it is simply mis-stamped. No OCR setting can help: all sixteen
                # layers, both engines, every dpi, produce the same wrong classification.
                #
                # The TITLE is the semantic truth ("BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH" vs
                # "BÁO CÁO LƯU CHUYỂN TIỀN TỆ"), so when it matches a DIFFERENT statement
                # verbatim, it wins. Gated behind `title_over_form` and reached only by a
                # relaxed parse layer, because the reverse case is real too: a page whose title
                # merely mentions another statement must not be able to overrule a sound code.
                # Requiring an exact containment (score 1.0) is what separates the two.
                if self.title_over_form:
                    header_ns = self.norm("\n".join(
                        [l for l in text.splitlines() if l.strip()][:self.HEADER_LINES]
                    )).replace(" ", "")
                    for report, needles in self.HEADING.items():
                        if report != kind and self._title_score(header_ns, needles) == 1.0:
                            return report, True
                return kind, True

        header = "\n".join(
            [l for l in text.splitlines() if l.strip()][:self.HEADER_LINES])
        ns = self.norm(header).replace(" ", "")
        if any(a in ns for a in self.AUDIT_NS):
            return None, False              # the auditor's report is not a statement
        # ⚠️ The NOTES verdict alone is re-taken without the table's column-heading row --
        # see COLUMN_HEADER_NS. The three statement titles keep the FULL header, because that
        # row can only ever add a spurious match for them, never carry a real one.
        notes_ns = ns
        if self.column_header_blind:
            notes_ns = self.norm("\n".join(
                l for l in [x for x in text.splitlines() if x.strip()][:self.HEADER_LINES]
                if not all(n in self.norm(l).replace(" ", "")
                           for n in self.COLUMN_HEADER_NS))).replace(" ", "")
        # ⚠️ **AN EXACT STATEMENT TITLE BEATS AN INEXACT NOTES VERDICT — `NOT-2`, 2026-09-04,
        # AND IT IS `NOT-1`'s DEFECT WITH `column_header_blind` UNABLE TO REACH IT.** That fix
        # re-takes the notes verdict without the table's column-heading ROW, which works when
        # the row is one line. FPT's 2008-2010 filings emit each narrow heading CELL as its own
        # line — `STT` / `TÀI SẢN` / `Mã ` / `số ` / `Thuyết ` / `minh` / `Số cuối quý` — so no
        # single line carries both of `COLUMN_HEADER_NS`, and the form says `TÀI SẢN` where the
        # test looks for `chỉ tiêu`. Measured on FPT Q3-2008 page 2, the balance sheet's own
        # FIRST page: notes **0.8125** (the same fragment `NOT-1` records) against
        # `bangcandoiketoan` **1.000**, and the page read as a note. The cost is the page that
        # carries the `Mã số` column heading, so `_code_column` could not fire and the item
        # codes were read as figures — `TỔNG CỘNG TÀI SẢN` = **270** (`MSO-1`'s symptom, caused
        # by a classification failure two pages upstream).
        #
        # So the rule is the one `title_over_form` already makes against a wrong FORM CODE:
        # **the title is the semantic truth, and a VERBATIM one wins.** Both halves are needed —
        # the statement title must be an exact containment (1.0) and the notes match must be
        # INEXACT (< 1.0), so a page that genuinely announces itself as notes keeps that verdict
        # however many statement names it also prints.
        #
        # ⚠️ **MEASURED, NOT ARGUED**: across the 7,404 text-layer pages of the eight parsed
        # tickers, **10 pages** change and **every one is `notes` -> balance_sheet** — FPT
        # Q3-2008, Q2-2009, Q3-2009, Q4-2009, Q1-2010, Q2-2010, all of them pages 2-3, i.e. the
        # balance sheet's own opening pages. Not one page moves between statements.
        notes_exact = self._title_score(notes_ns, [self.NOTES_NS])
        title_exact = max(self._title_score(ns, n) for n in self.HEADING.values())
        if notes_exact >= self.TITLE_MATCH and not (title_exact == 1.0 and notes_exact < 1.0):
            return NOTES, False
        # The BEST-matching title wins, not the first to clear the threshold. A form code with an
        # OCR-mangled digit falls through to here — ACB's cash-flow page prints "Mẫu BO4/TCTD-HN"
        # (letter O for the zero), so `B\d{2}` misses — and page boilerplate can score above the
        # threshold for the WRONG statement before the right title is even tried, in dict order.
        # ACB's cash flow was lost that way (declared the income statement) though "lưu chuyển
        # tiền tệ" is in its header verbatim. Scoring all three and taking the best cannot do
        # worse: an exact title always beats a coincidence. (experiment_8.)
        best, score = None, 0.0
        for report, needles in self.HEADING.items():
            s = self._title_score(ns, needles)
            if s > score:
                best, score = report, s
        if best is not None and score >= self.TITLE_MATCH:
            return best, False
        return None, False

    # Column headings that say the statement carries BOTH a standalone quarter and a
    # year-to-date column. Accents stripped and spaces removed, like every other header test.
    QUARTER_COL_NS = ("quy",)
    CUMULATIVE_COL_NS = ("luyke", "luykotudaunam", "luykotu")

    def _prints_quarter_column(self, text: str) -> bool:
        """Does this income statement print a standalone-quarter column beside the cumulative
        one? (VCB Q2-2014: "Quý II | Lũy kế từ đầu năm".)

        Both must be present. "Lũy kế" alone is the ordinary interim statement, which IS
        cumulative and must still be de-cumulated; "Quý" alone appears in ordinary prose. Only
        the two together mean column 0 is already the quarter.
        """
        header = "\n".join([l for l in text.splitlines() if l.strip()][:self.HEADER_LINES])
        ns = self.norm(header).replace(" ", "")
        return (any(q in ns for q in self.QUARTER_COL_NS)
                and any(c in ns for c in self.CUMULATIVE_COL_NS))

    def _title_score(self, header_ns: str, needles: List[str]) -> float:
        """How well the header matches a statement's title: 1.0 for a verbatim hit, else the best
        sliding-window ratio — the same measure `_titled` thresholds, but returned not thresholded
        so `_page_kind` can compare the three statements against one another."""
        from difflib import SequenceMatcher

        best = 0.0
        for n in needles:
            if n in header_ns:
                return 1.0
            w = len(n)
            for i in range(0, max(1, len(header_ns) - w + 1)):
                best = max(best, SequenceMatcher(None, n, header_ns[i:i + w]).ratio())
        return best

    def _titled(self, header_ns: str, needles: List[str]) -> bool:
        """Does the header carry this statement's title, allowing for OCR damage?

        Exact containment is not enough: the same title comes back as "Bảng cân đối kế toán"
        on one page and "Hãng cần đải kếtoán" on the next (B->H, ô->a). So each needle is slid
        along the header and accepted on a close-enough match.
        """
        from difflib import SequenceMatcher

        for n in needles:
            if n in header_ns:
                return True
            w = len(n)
            for i in range(0, max(1, len(header_ns) - w + 1)):
                if SequenceMatcher(None, n, header_ns[i:i + w]).ratio() >= self.TITLE_MATCH:
                    return True
        return False

    @staticmethod
    def _to_visual(page, words: list) -> list:
        """Map OCR word boxes into the page's VISUAL space.

        These scans are stored with /Rotate 180. PyMuPDF rasterises the page upright to OCR
        it — so the text comes back correct — but returns the boxes in UNROTATED pdf space,
        mirrored against what was actually read. Left and right swap, and the parser's whole
        premise (label left, values right) inverts. Clearing the rotation is not a fix: OCR
        then reads an upside-down image and returns gibberish.
        """
        if not page.rotation:
            return words
        import fitz

        m = page.rotation_matrix
        out = []
        for w in words:
            r = (fitz.Rect(w[0], w[1], w[2], w[3]) * m).normalize()
            out.append((r.x0, r.y0, r.x1, r.y1) + tuple(w[4:]))
        return out

    # Above this fraction of ≤2-char alphabetic tokens, a native text layer is legacy-font
    # MOJIBAKE, not Vietnamese. A broken CMap leaves the ascii letters but shreds every
    # diacritic word into fragments — "LƯU CHUYỂN TIỀN" → "llfu chuyttn t n" — so the token
    # stream fills with 1-2 char junk ("th ng m ci a ni n t n tu"). Real Vietnamese runs ~0.23
    # short-token fraction (co, a, te, do, i…); ACB's mojibake pages run ~0.51. 0.40 splits them.
    GARBLED_SHORT_FRAC = 0.40

    # Every precomposed Vietnamese accented letter. Genuine Vietnamese text is dense with these
    # (~0.10-0.13 of its letters); a TCVN3/VNI legacy font substitutes an ASCII letter for each,
    # so a mojibake page carries essentially NONE. That gap is the surest tell of the substitution
    # mojibake the token-length test cannot see (its words stay medium-length, just wrong).
    VN_DIACRITICS = set(
        "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ"
        "ÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ")
    # Below this diacritic-per-letter ratio, a page with real text is mojibake. 0.02 sits far
    # under genuine Vietnamese (~0.10) and far over the mojibake floor (0.00).
    MIN_DIACRITIC_RATIO = 0.02

    def _native_garbled(self, native: str) -> bool:
        """True when a non-trivial native text layer is legacy-font mojibake and must be OCR'd.

        ACB's 2013-2015 filings embed a broken-encoding text layer: get_text() returns 1700+
        readable-length chars that are pure junk. Two distinct flavours, one gate for each:
          * SUBSTITUTION — every accented letter mapped to an ASCII one ("Bảng cân đối kế toán"
            → "Bine can ddi k6loAn"). Words stay medium-length, so the token test misses it, but
            the diacritics are gone: the diacritic-per-letter ratio collapses to ~0.00.
          * SHREDDING — a broken CMap that fragments every diacritic word ("LƯU CHUYỂN TIỀN" →
            "llfu chuyttn t n"), spiking the ≤2-char-token fraction.
        Either one means the page classifier would match nothing and the statement would be lost,
        so the page must be OCR'd. Genuine Vietnamese text trips neither.
        """
        ns = self.norm(native).replace(" ", "")
        if len(ns) < self.MIN_PAGE_TEXT:
            return False            # too little text to judge; the length gate handles it

        # Substitution mojibake: substantial text, essentially no Vietnamese diacritics. Measured
        # on NFC-normalised raw text (decomposed accents would otherwise read as plain letters).
        nfc = unicodedata.normalize("NFC", native)
        alpha = [c for c in nfc if c.isalpha()]
        if alpha and (sum(1 for c in alpha if c in self.VN_DIACRITICS) / len(alpha)
                      < self.MIN_DIACRITIC_RATIO):
            return True

        # Shredding mojibake: a flood of 1-2 char fragments.
        toks = [t for t in self.norm(native).split() if not t.isdigit()]
        if len(toks) < 20:
            return False            # too few words to trust the ratio
        short = sum(1 for t in toks if len(t) <= 2)
        return short / len(toks) >= self.GARBLED_SHORT_FRAC

    # How much of a word's area must fall inside a signature widget for the widget to own it.
    # The stamp's appearance can overhang its own rect by a fraction of a point (ACB Q1-2023's
    # words run x 24.7-99.1 against a rect of 25.05-98.26), so containment must be by area, not
    # by strict inclusion.
    SIG_INSIDE = 0.6

    @staticmethod
    def _signature_rects(page) -> List:
        """Rects of the page's digital-signature widgets (empty for an unsigned page)."""
        try:
            return [w.rect for w in page.widgets()
                    if w.field_type_string == "Signature"]
        except Exception:
            return []                       # no AcroForm, or a damaged one

    def _page_content_text(self, page, native: str) -> str:
        """The page's OWN text — a page whose whole text layer is a SIGNATURE STAMP has none.

        A scanned page that was e-signed carries a text layer holding only the signature
        appearance ("Digitally signed by NGÂN HÀNG…, Reason: I am the author of this document,
        Foxit Reader Version: 10.0.0" — ~350 chars of perfectly good Vietnamese and English).
        That is the document's *provenance*, not its content: the table is in the pixels
        underneath. But it is long enough to clear MIN_PAGE_TEXT and trips neither mojibake
        test, so the page was accepted as a text page and never OCR'd — and the parser read the
        stamp instead of the statement.

        The stamp usually lands on a cover page, where it costs nothing. On ACB's Q1-2023 filing
        it lands on the BALANCE SHEET'S SECOND PAGE: page 3 carries the whole liabilities-and-
        equity side (TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU = 611,223,523, matching total assets
        exactly), and reading the stamp left it with no form code and 2 numbers, so
        `_fill_continuations` saw a gap, ended the run, and the balance sheet stopped at TỔNG
        TÀI SẢN. With no liabilities and no grand total it could never reconcile, at any DPI or
        engine — which is why all six ParseLayers failed it identically.

        The test is EXACT rather than a coverage heuristic: text inside a signature widget is by
        definition not page content. Only a page whose text is ENTIRELY the stamp is emptied, so
        a real text page that merely carries a signature keeps every character it had.
        """
        if not native.strip():
            return native                   # already empty; the length gate has it
        rects = self._signature_rects(page)
        if not rects:
            return native                   # unsigned page — the overwhelming majority
        import fitz

        words = page.get_text("words")
        if not words:
            return native
        for w in words:
            box = fitz.Rect(w[:4])
            area = abs(box.get_area())
            if not area:
                continue
            covered = max((abs((box & r).get_area()) for r in rects), default=0.0)
            if covered / area < self.SIG_INSIDE:
                return native               # real content here — leave the page alone
        return ""                           # nothing but the stamp: treat as an image page

    # A token that is nothing but numbers, separators and whitespace — two or more figures that
    # a LINE-level detector has boxed together. Letters anywhere disqualify it, so a label is
    # never touched, and it must carry at least one digit so a row of "-" placeholders is left
    # alone.
    NUM_RUN_RE = re.compile(r"^[\s\d.,()\-–—]+$")

    # ⚠️ **A SLASH THE RECOGNISER PUT IN THE COLUMN GAP MAKES THE WHOLE BOX PARSE AS NOTHING.**
    # `NUM_RUN_RE` does not admit "/", so a box holding both period figures with one between
    # them -- CTG's Q2-2011 balance sheet returns `'395.852.473 /367.712.191'` for the two
    # columns of TONG TAI SAN -- is neither split nor parsed, and the row loses every value.
    # `table_rows` then has a label with no figure, turns it into `carry`, and the grand total
    # is gone: that quarter was refused `no total assets` on all 55 layers with both totals
    # printed and read correctly.
    #
    # ⚠️ **ONLY A SLASH THAT TOUCHES WHITESPACE, WHICH IS WHAT A COLUMN GAP LOOKS LIKE.** A
    # DATE is the reason: these pages print `30/06/2011 01/01/2011` in the header, and admitting
    # "/" anywhere would turn each into a numeric run and split it into 30, 06, 2011. A date
    # has no space beside its slashes and is untouched.
    #
    # ⚠️ The substitution is LENGTH-PRESERVING, and it has to be: `_split_number_runs`
    # apportions the box by CHARACTER OFFSET, so a rewrite that shortened the text would move
    # every right edge it computes -- and the right edge is what the column clustering uses.
    SLASH_GAP_RE = re.compile(r"(?<=\s)/|/(?=\s)")

    # A THOUSANDS SEPARATOR READ AS A SPACE. The same box that can hold two period figures can
    # instead hold ONE whose separator the recogniser lost: ACB's Q2-2012 returns '3 396.864'
    # for a printed 3.396.864. Splitting that on whitespace keeps 396.864 and throws the leading
    # group away — a figure short by exactly 3,000,000, which is why the breakdown missed by that
    # amount and the quarter could not be read at ANY dpi or crop padding.
    #
    # The two cases are told apart by the FIRST part. A box holding two figures has a
    # well-formed grouped number on both sides ('135.272.610 126.501.216'); a lost separator
    # leaves a BARE 1-3 digit group in front ('3'), which cannot be a period figure of its own —
    # every figure in these statements is 4-9 digits — and what follows it must continue the
    # grouping exactly.
    JOIN_HEAD_RE = re.compile(r"^\(?-?\d{1,3}$")
    JOIN_TAIL_RE = re.compile(r"^\d{3}(\.\d{3})*\)?$")

    @classmethod
    def _join_split_number(cls, txt: str) -> Optional[str]:
        """'3 396.864' -> '3.396.864', or None when the run is genuinely several figures."""
        parts = txt.split()
        if len(parts) < 2 or not cls.JOIN_HEAD_RE.match(parts[0]):
            return None
        if not all(cls.JOIN_TAIL_RE.match(p) for p in parts[1:]):
            return None
        return ".".join(parts)

    @classmethod
    def _split_number_runs(cls, words: list, join_split: bool = False,
                           join_lost: bool = False) -> list:
        """Split a box holding SEVERAL period figures into one box per figure.

        The onnx engine detects text LINES, not words, and on some rows it boxes both period
        columns together: ACB's Q1-2025 cash flow returns '135.272.610 126.501.216' as a single
        token spanning x 428-559, where the component lines below it come back as two boxes.
        Such a token parses as no number at all, so the row loses every value and is dropped —
        which is how that filing lost IV, V and VII, the whole basis for reconciling a cash flow,
        while the rows either side of them read perfectly.

        The figures are laid out in a fixed-width column of digits, so apportioning the box by
        CHARACTER OFFSET puts each part where it was printed. Only the right edge has to be
        right, since that is what the column clustering uses, and it lands within a point:
        Q1-2025's split predicts right edges of 490.7 and 558.7 against the 491.3 and 559.2 its
        neighbouring rows report.
        """
        out = []
        for w in words:
            txt = cls.SLASH_GAP_RE.sub(" ", w[4])          # see SLASH_GAP_RE
            parts = txt.split()
            if (len(parts) < 2 or not cls.NUM_RUN_RE.match(txt)
                    or not any(c.isdigit() for c in txt)):
                out.append(w)
                continue
            # ⚠️ **A BOX THE PARENTHESES SPAN IS ONE FIGURE, AND SPLITTING IT LEAVES BOTH HALVES
            # UNBALANCED.** BID's FY-2016 cash flow returns `'(1.029 827)'` for a printed
            # (1.029.827) — the thousands separator read as a space, inside a negative figure.
            # Split, that becomes `'(1.029'` and `'827)'`, and the row keeps the RIGHT half as a
            # positive number: **BID Q4-2016 is on disk with `hddt_mua_sam_tai_san_co_dinh` =
            # 616 mn for a printed (2.298.616) and dividends of 383 mn for a printed
            # (2.940.383)**, both positive, both three orders out. `_join_split_number` cannot
            # reach it either — its head must be a BARE 1-3 digit group, and "(1.029" is not.
            #
            # Two figures boxed together each carry their OWN parentheses, so requiring the pair
            # to span the whole box is what separates the cases: `'(135.272.610) (126.501.216)'`
            # has a `)` before the end and is left to the splitter exactly as before.
            # ⚠️ It is in the DEFAULT path and not behind a flag, because the split it prevents
            # is what `split_figures` then refuses (`SPL-1`) — the two halves sit
            # `box_width / len(text)` = 4.1pt apart, under `SPLIT_MAX_GAP` — so the alternative
            # is not a wrong figure but a whole statement lost.
            if (txt.startswith("(") and txt.endswith(")")
                    and "(" not in txt[1:] and ")" not in txt[:-1]):
                joined = ".".join(parts)
                if cls.MERGE_JOIN_RE.match(joined):
                    out.append((w[0], w[1], w[2], w[3], joined) + tuple(w[5:]))
                    continue
            # ⚠️ **THE SAME DEFECT AS `join_split`, WITHOUT ITS ASSUMPTION ABOUT WHERE.** That
            # rule requires the HEAD to be a bare 1-3 digit group, because '3 396.864' is the
            # shape it was measured on. BSR's scans lose a separator anywhere in the number —
            # '9.964.924.167 838', '10 982 779.849.642', '46.625 723 403.018' — and each is cut
            # into pieces that land on no column, so `split_figures` counts the fragments and
            # `reconcile` refuses the whole statement (`SPL-1`). Measured 2026-08-31 across four
            # BSR filings: 69 of 76 runs join into one well-formed figure, and the SAME boxes
            # come back WHOLE at 300 and 400 dpi over the identical x-range, which is what
            # establishes they are one figure rather than two.
            #
            # ⚠️ **AND IT CANNOT BE DISTINGUISHED HERE FROM THE OPPOSITE CASE — MEASURED.** ACB
            # Q1-2025 genuinely boxes two period figures together ('135.272.610 126.501.216'),
            # which also joins well-formed; its character density is 1.03x its own page's
            # median, i.e. identical to a clean single box. Nothing inside the box separates
            # them, so this is confined to the LAST layers of the cascade, where only a
            # statement every other reading has already refused can reach it — ACB Q1-2025 is
            # accepted at layer 6. `reconcile` and `sane` still judge whatever it recovers.
            if join_lost:
                joined = ".".join(q.strip("()") for q in parts)
                if cls.MERGE_JOIN_RE.match(joined):
                    if txt.strip().endswith(")") or txt.strip().startswith("("):
                        joined = f"({joined})"
                    out.append((w[0], w[1], w[2], w[3], joined) + tuple(w[5:]))
                    continue
            if join_split:
                joined = cls._join_split_number(txt)
                if joined is not None:
                    # keep the ORIGINAL box: its right edge is what the column clustering uses
                    # and it is already correct — only the text was wrong
                    out.append((w[0], w[1], w[2], w[3], joined) + tuple(w[5:]))
                    continue
            x0, y0, x1, y1 = w[0], w[1], w[2], w[3]
            width, n = x1 - x0, len(txt)
            if width <= 0 or n == 0:
                out.append(w)
                continue
            pos = 0
            for part in parts:
                i = txt.index(part, pos)
                pos = i + len(part)
                out.append((x0 + width * i / n, y0, x0 + width * pos / n, y1, part) +
                           tuple(w[5:]))
        return out

    # ⚠️ THE DETECTOR ALSO DOES THE OPPOSITE OF `_split_number_runs`: ONE FIGURE, TWO BOXES.
    # `'5.209.108'` ending at x=405.7 and `'954.978'` starting at x=409.5 is one printed
    # 5.209.108.954.978 (VIC Q3-2014 at onnx@200 — 60 figures on the balance sheet alone). Both
    # halves parse as plausible numbers, so nothing downstream can tell: the left half lands on
    # no column and is dropped, leaving the row holding `954.978`, or enough left halves line up
    # and become a spurious period column. Either way BOTH GRAND TOTALS survive whole, so
    # `reconcile` passes and `sane` probes a correct total — `SLD-1`'s shape, a wrong figure
    # that passes every gate.
    #
    # ⚠️ **THIS RUNS BEFORE COLUMNS EXIST, AND IT HAS TO.** The obvious test — "the left box
    # ends on no column" — is circular: the fragments cluster into a column of their own, so it
    # answers YES for the very halves it is hunting. Geometry and text are all there is here.
    #
    # ⚠️ **AND IT IS CONFINED TO THE VALUE ZONE**, the same right-60 % `value_columns` reads.
    # Not because a label cannot hold two adjacent numbers, but because that is the half of the
    # page the measurement below covers, and shipping wider than the measurement is how a rule
    # with 0 false positives acquires some.
    MERGE_MAX_GAP = 4.5
    # The join must be a well-formed thousands-grouped figure, and the tail must itself start
    # with a FULL group of three — which a continuation of a split figure always does. Two
    # adjacent PERIOD figures usually fail the join outright ("1.558.887.407" + "1.541.259.663"
    # leaves a group of ONE digit); where they would not, the gap is what separates them.
    MERGE_JOIN_RE = re.compile(r"^\(?-?\d{1,3}(\.\d{3})+\)?$")
    MERGE_TAIL_RE = re.compile(r"^\d{3}(\.\d{3})*\)?$")

    @classmethod
    def _merge_split_figures(cls, words: list, y_tol: float, lo: float) -> list:
        """Re-join figures the detector emitted as two boxes -> the same word list, repaired.

        The merged box keeps the LEFT box's x0 and the RIGHT box's x1, so the right edge the
        column clustering reads is the edge the printed figure actually has.

        A pair is merged only when all five hold:
          * both boxes end inside the value zone (`lo`);
          * same line, within `y_tol`, the right box immediately after the left one;
          * the gap is under `MERGE_MAX_GAP`;
          * the right box begins with a FULL three-digit group;
          * the two joined with a thousands separator form one well-formed figure.
        """
        by_line: dict = {}
        for w in words:
            if w[2] < lo:
                continue
            k = next((k for k in by_line if abs(k - w[1]) <= y_tol), w[1])
            by_line.setdefault(k, []).append(w)
        merged, replacements = set(), {}
        for ws in by_line.values():
            ws = sorted(ws, key=lambda w: w[0])
            i = 0
            while i < len(ws) - 1:
                a, b = ws[i], ws[i + 1]
                at, bt = str(a[4]).strip(), str(b[4]).strip()
                joined = at.rstrip(")") + "." + bt
                if (b[0] - a[2] < cls.MERGE_MAX_GAP
                        and cls.MERGE_TAIL_RE.match(bt)
                        and cls.MERGE_JOIN_RE.match(joined)):
                    replacements[id(a)] = ((a[0], a[1], b[2], max(a[3], b[3]), joined)
                                           + tuple(a[5:]))
                    merged.add(id(b))
                    i += 2
                    continue
                i += 1
        if not replacements:
            return words
        return [replacements.get(id(w), w) for w in words if id(w) not in merged]

    def _ocr_page(self, page, native: str):
        """(text, words) for one page, words in VISUAL pdf-point space.

        Single seam for both OCR engines. When the page has a usable native text layer it is
        used as-is (no OCR). A page is OCR'd when its native text is too SHORT (an image page —
        including one whose only text is a signature stamp, see _page_content_text) OR
        present-but-GARBLED (a legacy-font mojibake text layer — see _native_garbled).
        Otherwise the configured engine reads the rasterised page:

          * tesseract — PyMuPDF's get_textpage_ocr, then _to_visual to undo the /Rotate 180
            box mirroring (the engine reads an upright raster but returns unrotated boxes).
          * easyocr    — rasterise the page in its VISUAL (already-rotated) space and OCR the
            pixels; boxes come back in pixel space of the upright image, so scaling them by
            72/DPI yields visual pdf-points directly — no _to_visual needed.

        Returns word tuples shaped like PyMuPDF's "words": (x0,y0,x1,y1, word, b, l, n).
        """
        # ⚠️ **THE ROTATION IS APPLIED HERE, OUTSIDE THE CACHE, BECAUSE IT IS NOT ONLY THE
        # PIXELS IT CHANGES.** `scan` reads `page.rect.width` after this returns and every
        # column measurement downstream is taken against it, so a page whose rotation was set
        # on the first pass and skipped on the second — a cache HIT — would be measured 585pt
        # wide having been read 842pt wide. Setting it before the lookup makes the two agree
        # whether or not the read is cached, and `page.rotation` in the key keeps the upright
        # read and the rotated one apart.
        rot = self._page_rot.get(page.number)
        if rot is not None and page.rotation != rot:
            page.set_rotation(rot)
        key = (page.number, page.rotation) + self._ocr_config()
        hit = self._ocr_cache.get(key)
        if hit is None:
            hit = self._read_page(page, native)
            self._ocr_cache[key] = hit
        text, words, split = hit
        # ⚠️ **THE ONLY POST-STEP THAT IS PER-LAYER, SO IT IS THE ONLY ONE OUTSIDE THE CACHE.**
        # `join_split_digits` is a `ParseLayer` flag and `_split_number_runs` returns a NEW
        # list, so replaying it over the cached words is the same operation on the same input
        # that ran here before the cache existed. The native-text and Tesseract paths never
        # took it and still do not (`split` says which).
        return (text, self._split_number_runs(words, self.join_split_digits,
                                             self.join_lost_separator) if split
                else words)

    def _read_page(self, page, native: str):
        """`(text, words, splittable)` for one page — the step the cache keys on.

        Split out of `_ocr_page` so the expensive half can be memoised on
        `(page, engine, dpi, crop_pad)` while the per-LAYER `join_split_digits` post-step
        stays outside it. `splittable` is False for the native-text and Tesseract paths,
        which have never run `_split_number_runs`.
        """
        native = self._page_content_text(page, native)
        need_ocr = self.ocr_ready and (
            len(native.strip()) < self.MIN_PAGE_TEXT or self._native_garbled(native))
        if not need_ocr:
            # ⚠️ **A `/Rotate 90` PAGE HANDS ITS NATIVE WORDS BACK IN THE *UNROTATED* SPACE
            # WHILE `page.rect` IS THE ROTATED ONE — `ROT-2`, 2026-09-04.** Measured on FPT's
            # Q3-2008 consolidated filing, page 5 (the income statement, a LANDSCAPE table):
            # `page.rect` is 792x612 and `/Rotate` is 90, and `get_text("words")` returns boxes
            # in the 612x792 mediabox — "CÔNG"/"TY"/"CP" stacked vertically at x=48.4 with
            # DECREASING y, and y running to 756 on a page `scan` records as 612 tall.
            #
            # `scan` reads `page.rect.width` for the column measurement and `table_rows` groups
            # by y, so the two disagree about which axis is which: `value_columns` returned four
            # "columns" inside a 55pt band and the statement came out **6 rows**, refused
            # `only 6 rows parsed` on every layer of the cascade. That is `ROT-1`'s defect in
            # the one path `ROT-1` cannot reach — it re-RENDERS a turned scan, and re-rendering
            # a text layer returns the same unrotated boxes.
            #
            # ⚠️ **AND IT IS THE MAPPING `_ocr_page`'s OWN DOCSTRING ALREADY PROMISES** ("words
            # in VISUAL pdf-point space"), which the Tesseract path has always applied and this
            # one never did. `_to_visual` returns the list UNCHANGED when `page.rotation` is 0,
            # so the change is inert on every upright page by construction.
            #
            # ⚠️ **MEASURED BEFORE IT SHIPPED**: across the 73,780 pages of the eight parsed
            # tickers, **2,116** are read through this path at all and **122** of those carry a
            # non-zero `/Rotate` — 0.17 % of the corpus, and every one of them is being measured
            # in the wrong space today. Nothing else can move.
            return native, self._to_visual(page, page.get_text("words")), False

        if self.engine == "onnx":
            text, words = self._onnx.read_page(page)
            # ⚠️ MERGE FIRST, THEN SPLIT, AND THE ORDER IS NOT COSMETIC. `_split_number_runs`
            # apportions a multi-figure box by CHARACTER OFFSET, so the gap it leaves between
            # two pieces is the width of the SEPARATOR CHARACTER — `box width / len(text)`.
            # That is 5.7pt for the measured case ('135.272.610 126.501.216' across 130pt) and
            # therefore wider than `MERGE_MAX_GAP` — but it is a ratio, not a constant, and a
            # narrower box or a longer run puts it under 4.5pt, at which point a merge running
            # afterwards would join the splitter's own pieces straight back together. Merging
            # FIRST cannot make that mistake at any width: it only ever sees boxes the DETECTOR
            # emitted separately, and the box it produces holds no separator to act on.
            words = self._merge_split_figures(words, self.Y_TOL,
                                              page.rect.width * self.VALUE_ZONE)
            return text, words, True
        if self.engine == "easyocr":
            text, words = self._ocr_page_easyocr(page)
            return text, words, True

        tp = page.get_textpage_ocr(language=OCR_LANG, dpi=self.dpi, full=True,
                                   tessdata=TESSDATA_DIR)
        text = page.get_text(textpage=tp)
        words = self._to_visual(page, page.get_text("words", textpage=tp))
        return text, words, False

    def _ocr_page_easyocr(self, page):
        """Rasterise the visual page and OCR it with EasyOCR → (text, words) in pdf-points."""
        import fitz
        import numpy as np

        scale = self.dpi / 72.0
        # get_pixmap already applies /Rotate, so a plain scale matrix yields an upright (visual)
        # raster whose boxes need no un-mirroring (do NOT prerotate — see onnx_ocr.read_page).
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width)

        # detail=1 → (box, text, conf); paragraph=False keeps one entry per word-ish token so
        # the downstream right-edge column clustering still has individual boxes to work with.
        results = self._easyocr().readtext(img, detail=1, paragraph=False)

        words, lines = [], []
        for i, (box, txt, conf) in enumerate(results):
            if not txt or not txt.strip():
                continue
            xs = [p[0] for p in box]
            ys = [p[1] for p in box]
            # pixel → pdf-point (divide by the raster scale)
            x0, y0 = min(xs) / scale, min(ys) / scale
            x1, y1 = max(xs) / scale, max(ys) / scale
            words.append((x0, y0, x1, y1, txt.strip(), 0, 0, i))
            lines.append(txt.strip())
        return " ".join(lines), words

    def _page_rotation(self, page, words: list) -> int:
        """The absolute `/Rotate` this page should be READ at — `page.rotation` unless its scan
        is turned, in which case 90° or 270° on top of it.

        Two questions, and only the second costs anything. **Is the page turned?** is answered
        from `words`, which the caller has already paid for, by either of the two signals below.
        **Which way up?** cannot be answered from geometry — 90 and 270 both lay the lines flat
        — so the page is read at the probe DPI each way and the direction that yields more
        parseable NUMBERS wins. That is the right discriminator here and not a general one:
        these are financial statements, and a column of digits read upside down stops being
        digits.

        ⚠️ **THE TWO ENTRY SIGNALS ARE DIFFERENT PAGES, AND THE SECOND IS `ROT-3` (2026-09-04).**

          * **the lines are VERTICAL** — the original `ROT-1` signal, measured on BID's Q3-2011
            income statement, where the detector still finds text lines and 92-100 % of the
            boxes come back taller than wide;
          * ⚠️ **the upright read is NEARLY EMPTY** — a page turned badly enough that the
            detector cannot segment it at all. FPT's Q1-2012, Q3-2011 and Q3-2013 CONSOLIDATED
            income statements come back as **25-29 near-square blobs reading 31-56 characters**
            ("I | I | 1 | I | l | I | 5 | E 8 a"), so the tall-box share is **48-68 % against a
            70 % bar** and the median w/h is 0.82-1.00 — the first signal cannot see them. The
            same pages at +90 give 178-185 boxes, **121-127 numbers and 2,500-2,800
            characters**. All three quarters reported `no such statement on any page of this
            filing` on every layer of the cascade, and each also blocked a cumulative Q4.

        ⚠️ **AND THE PROBE IS SEEDED WITH THE BASE ROTATION, WHICH IT WAS NOT BEFORE.** The old
        loop started at `best_key = None`, so the FIRST candidate always won and the page was
        turned whether or not turning helped — harmless while the entry signal was "the lines
        are definitely vertical", and wrong the moment the entry widened. Measured over 460
        pages of 11 filings: **38 read under `MIN_UPRIGHT_CHARS` (8.3 %)** and **27 read better
        turned**; among the other 11 is a cover page whose upright read is 211 characters and
        whose +90 read is ONE — and the numbers-first key preferred the ONE, because a cover
        page carries no digits at all for the base to win on.

        ⚠️ **SO A ROTATION MUST FIRST READ MORE CHARACTERS THAN THE BASE, AND ONLY THEN IS IT
        RANKED ON NUMBERS.** That one condition separates the 27 from the 11 exactly, in both
        directions and with orders of magnitude to spare: a true positive goes 17-56 characters
        to 1,100-3,200 and every false positive goes DOWN. Numbers stay the tie-break among
        rotations that clear it, which is what keeps 90 apart from 270 (121 against 19 on the
        same page).
        """
        base = page.rotation
        # Only the onnx path: Tesseract's boxes come back through `_to_visual` and — since
        # `ROT-2` (2026-09-04) — so do the native text layer's, so on both of those a turned
        # page is already square with `page.rect` and there is nothing here to detect. This
        # probe exists for the case neither can see: a scan whose IMAGE is turned inside a page
        # the PDF calls upright.
        if self.engine != "onnx" or self._onnx is None or len(words) < self.MIN_ROT_WORDS:
            return base
        tall = sum(1 for w in words if (w[3] - w[1]) > (w[2] - w[0]))
        upright_chars = sum(len(w[4]) for w in words)
        vertical = tall / len(words) >= self.VERTICAL_LINES_SHARE
        unreadable = upright_chars < self.MIN_UPRIGHT_CHARS
        if not (vertical or unreadable):
            return base

        def probe(extra: int):
            """`(numbers, characters)` for this page turned `extra` degrees, at the probe DPI."""
            page.set_rotation((base + extra) % 360)
            _, boxes = self._onnx.read_page(page)
            return (sum(1 for w in boxes if self.parse_num(w[4]) is not None),
                    sum(len(w[4]) for w in boxes))

        dpi = self.dpi
        best, best_key = base, None
        try:
            self.set_dpi(self.ROT_PROBE_DPI)
            # ⚠️ THE BASE IS READ TOO, AT THE SAME DPI. A rotation is only ever better than
            # SOMETHING, and comparing a 100-dpi probe against the caller's 200-400 dpi read
            # would not be comparing anything.
            floor = probe(0)
            for extra in (90, 270):
                key = probe(extra)
                # ⚠️ MORE CHARACTERS THAN UPRIGHT IS THE GATE; numbers only RANK what clears it.
                if key[1] <= floor[1]:
                    continue
                if best_key is None or key > best_key:
                    best, best_key = (base + extra) % 360, key
        finally:
            self.set_dpi(dpi)
            page.set_rotation(base)
        if best != base:
            self._log(f"page {page.number + 1}: "
                      + (f"text lines are vertical ({tall}/{len(words)} boxes)" if vertical
                         else f"upright read is nearly empty ({upright_chars} chars)")
                      + f" — reading it at /Rotate {best}")
        return best

    def scan(self, doc) -> Dict[int, dict]:
        """Read each page ONCE — OCR only the pages that need it — and cache text + words."""
        pages: Dict[int, dict] = {}
        seen = set()
        # Load pages BY INDEX. Several filings have a damaged page tree ("non-page object in
        # page tree") and iterating the document simply stops at the bad node — one VCB
        # filing yielded 14 of its 58 pages that way, dropping every statement after it.
        for i in range(doc.page_count):
            if self.on_page is not None:
                self.on_page(i, doc.page_count)
            try:
                page = doc.load_page(i)
            except Exception:
                continue
            # Decide PER PAGE, not per document: some filings are mixed, with a text layer in
            # the notes and image-only statement pages.
            native = page.get_text()
            text, words = self._ocr_page(page, native)
            # ⚠️ Decided from the boxes the read above ALREADY returned, so an upright page
            # costs nothing; only a page whose text lines are vertical is read a second time.
            # `_ocr_page` applies the answer on every later pass, from the cache.
            if page.number not in self._page_rot:
                self._page_rot[page.number] = self._page_rotation(page, words)
                if page.rotation != self._page_rot[page.number]:
                    text, words = self._ocr_page(page, native)

            kind, from_form = self._page_kind(text)
            pages[i] = {"text": text, "words": words, "kind": kind,
                        "from_form": from_form, "width": page.rect.width}

            if kind in REPORTS:
                seen.add(kind)
            elif kind == NOTES and from_form and len(seen) == len(REPORTS):
                break            # the statements are behind us; the rest is notes

        self._drop_islands(pages)
        if self.notes_boundary:
            self._drop_after_notes(pages)
        self._enforce_order(pages)
        self._fill_continuations(pages)
        return pages

    @staticmethod
    def _drop_islands(pages: Dict[int, dict]) -> None:
        """Discard title-only pages that sit apart from the statement's real run.

        When a report HAS a form-coded page, that page is where the statement actually is. A
        page identified only by a fuzzy title, and separated from it by a gap, is something
        else wearing the same words — the auditor's report or a contents page. VCB's Q2-2023
        matched its balance-sheet title on pages 6-7, two pages clear of the real statement on
        9-11, and pulled them in; they carry no "Triệu VNĐ" header, so the unit came out ×1
        instead of ×10⁶ — a uniform 10^6 error that still reconciles perfectly.

        ⚠️ **THE GAP IS MEASURED ALONG THE STATEMENT'S OWN RUN, NOT IN A ±1 WINDOW AROUND THE
        FORM-CODED PAGES — and it was the window until 2026-08-29.** A form code has to survive
        OCR to count, and on an old scan usually only one of a statement's pages keeps it: TCB's
        Q3-2013 balance sheet runs pages 2-4, all three classified `balance_sheet`, and only
        page 4 kept its "B02a/TCTD-HN". Page 2 then measured TWO pages from the single anchor
        and was discarded as an island — **while page 3, between them, was kept**, which is what
        a gap cannot look like. What went with it was the FIRST page of the statement, and with
        it the one place in the whole filing that prints `Đơn vị tính: triệu đồng`: every figure
        of all three statements was then read as đồng, a uniform 10^6 error that reconciles
        perfectly against itself and that only `sane` could refuse. The quarter had been
        `missing` since the ticker was first parsed.

        Walking outward through CONTIGUOUS pages the same report already owns keeps that page
        and still drops VCB's Q2-2023 islands: page 8 there belongs to no report, so the walk
        stops at 9 and pages 6-7 remain two pages clear. **The ±1 tolerance is unchanged** — it
        is what admits a continuation page whose own header OCR destroyed.
        """
        for report in REPORTS:
            owned = sorted(i for i in pages if pages[i]["kind"] == report)
            anchors = [i for i in owned if pages[i]["from_form"]]
            if not anchors or not owned:
                continue
            run = set(owned)
            lo, hi = min(anchors), max(anchors)
            while lo - 1 in run:                  # the statement's own contiguous run
                lo -= 1
            while hi + 1 in run:
                hi += 1
            for i in owned:
                if i < lo - 1 or i > hi + 1:      # not touching the form-coded run
                    pages[i]["kind"] = None

    @staticmethod
    def _drop_after_notes(pages: Dict[int, dict]) -> None:
        """A filing prints its statements, then its notes. Once the notes have begun, a page
        identified only by a fuzzy TITLE is a note ABOUT a statement, not the statement.

        ⚠️ **THIS EXISTS BECAUSE `_drop_islands` IS DISABLED WHEN NO FORM CODE SURVIVES OCR.**
        That pruner measures a stray page's distance from a FORM-CODED one and returns early
        with no anchor to measure from — `if not anchors: continue`. BID's Q3-2025 filing reads
        `from_form = False` on all 37 pages, so nothing was pruned: pages 12-13 and 18-34 are
        notes whose headers score against the balance-sheet title, and `_fill_continuations`
        then swept every numbered table after them into the statement. **22 pages, 316 rows**,
        and the grand-total anchors were taken from a NOTE table — 115,110 for a bank whose
        real total is 3,071,970,196. ⚠️ `reconcile` PASSES on that, because assets and resources
        are the same piece of garbage; only `sane` refused it.

        Two conditions keep this off a sound filing, and both are needed:

          * the page must carry **no form code** — a code is definitive and always wins, so a
            statement genuinely printed after a note page is untouched;
          * the report must already have had a run **before** the notes began. A filing that
            opens with a CONTENTS page naming every statement classifies that page as notes
            before any statement is seen, and without this second condition the whole filing
            would then be pruned away.

        A layer, not a default: it re-reads pages that parse today. See `set_notes_boundary`.
        """
        notes_at = next((i for i in sorted(pages) if pages[i]["kind"] == NOTES), None)
        if notes_at is None:
            return
        established = {pages[i]["kind"] for i in sorted(pages)
                       if i < notes_at and pages[i]["kind"] in REPORTS}
        for i in sorted(pages):
            if (i > notes_at and pages[i]["kind"] in established
                    and not pages[i]["from_form"]):
                pages[i]["kind"] = None

    @staticmethod
    def _enforce_order(pages: Dict[int, dict]) -> None:
        """A filing prints its statements in one order: balance sheet, income statement, cash
        flow. Anything claiming to be a statement out of that order is not one.

        This is the guard a fuzzy title match needs. The auditor's report and the contents page
        NAME the statements, and OCR damage makes those mentions close enough to score a match
        — VCB's Q2-2023 tagged pages 6-8 as the cash-flow statement, six pages before the
        balance sheet even began. A form-code page is definitive and always kept; a page
        identified only by its title must respect the order.
        """
        first: Dict[str, int] = {}
        for i in sorted(pages):
            k = pages[i]["kind"]
            if k in REPORTS and (k not in first or pages[i]["from_form"]):
                first.setdefault(k, i)

        floor = -1
        for report in REPORTS:                # BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW
            start = first.get(report)
            if start is None:
                continue
            if start < floor:                 # out of order -> suspect, but see below
                early = [i for i in pages if pages[i]["kind"] == report
                         and i < floor and not pages[i]["from_form"]]
                rest = [i for i in pages if pages[i]["kind"] == report and i not in set(early)]
                # ⚠️ **AND THE PAGES ARE ONLY DROPPED WHEN THE STATEMENT SURVIVES SOMEWHERE
                # ELSE.** The defect this guards against is a DUPLICATE: a page that merely
                # NAMES a statement matches its title early, while the statement itself is
                # printed further on — so clearing the early match loses nothing. A report
                # whose only pages are the early ones is a different thing entirely, and BID's
                # Q3-2011 is it: that filing prints its balance sheet (pages 1-2), then its
                # CASH FLOW (3-5), then its income statement (6). Nothing is wrong with it —
                # the canonical order is a convention, not a rule — and enforcing the order
                # here deleted the cash flow outright, after which `_fill_continuations`
                # handed its two pages to the balance sheet running above them. Measured
                # 2026-08-30, the moment the rotated income statement on page 6 became
                # visible: 55 balance-sheet rows became 55 balance-sheet-plus-cash-flow rows.
                if rest:
                    for i in early:
                        pages[i]["kind"] = None
                    start = min(rest)
            floor = max(floor, start)

    def _fill_continuations(self, pages: Dict[int, dict]) -> None:
        """Give an unidentifiable page to the statement it sits inside.

        A statement's pages are CONTIGUOUS, so a page that carries a table but whose header OCR
        destroyed belongs to the statement running through it. VCB's Q4-2021 balance sheet is
        three pages and the middle one lost its title line entirely — leaving the statement
        truncated at one page, and TỔNG TÀI SẢN with it.

        Only pages with a real table are absorbed (`MIN_TABLE_WORDS`), so a signature or
        narrative page between two statements is not swept in.
        """
        run: Optional[str] = None
        # Which statements the page classifier found ANYWHERE in this document — so a page can
        # never be claimed for a report that already has real pages of its own. See
        # `_notes_head_report`; computed once, and added to as heads are opened.
        seen = {p["kind"] for p in pages.values() if p["kind"] in REPORTS}
        for i in sorted(pages):
            kind = pages[i]["kind"]
            if kind in REPORTS:
                run = kind
                continue
            if kind == NOTES:
                # ⚠️ …unless the page's CONTENT says otherwise. A filing may print its own
                # notes header on a statement's page, and the header is then the only thing
                # that is wrong about it — see `SECTION_HEADING`.
                # ⚠️ **THE TAIL RULE IS TRIED FIRST, AND THE ORDER IS THE NARROWER CLAIM
                # FIRST.** Extending a run that is already open says only that this page
                # belongs to the statement above it; opening a new one says what statement a
                # page IS, which is a bigger claim and gets the later layers.
                if run and self._is_notes_tail_page(pages[i], run):
                    pages[i]["kind"] = run
                    pages[i]["from_form"] = False
                    run = None                  # a tail page is by definition the last one
                    continue
                head = self._notes_head_report(pages[i], seen, run)
                if head is not None:
                    pages[i]["kind"] = head
                    pages[i]["from_form"] = False
                    seen.add(head)
                    run = head                  # and its own continuation pages may follow
                    continue
                run = None                      # the statements are over
                continue
            if run and self._is_supplement(pages[i]):
                # ⚠️ A variance explanation is not a continuation, and it IS a table — so
                # without this it is absorbed on the branch below. See `SUPPLEMENT_NS`.
                run = None                      # the statement ended on the page before
                continue
            if run and self._is_table(pages[i]):
                pages[i]["kind"] = run
                pages[i]["from_form"] = False
            elif run and self._is_tail_page(pages[i], run):
                pages[i]["kind"] = run
                pages[i]["from_form"] = False
                # ⚠️ AND THE RUN ENDS HERE, DELIBERATELY. A tail page is by definition the LAST
                # page of its statement — it was admitted for carrying the closing line — so
                # letting the run continue would hand the next page a licence this one earned.
                run = None
            else:
                run = None                      # a gap ends the run

    def _is_table(self, page: dict) -> bool:
        return len(self._numbers(page["words"])) >= self.MIN_TABLE_WORDS

    def _notes_head_report(self, page: dict, seen: set,
                           run: Optional[str]) -> Optional[str]:
        """Which statement this NOTES-headed page STARTS, or None.

        ⚠️ **`notes_tail` ASSUMED THE STATEMENT'S FIRST PAGE WAS CLASSIFIED CORRECTLY, AND ON
        TCB's Q1-2017 AND Q3-2017 IT IS NOT.** Those filings print "THUYẾT MINH BÁO CÁO TÀI
        CHÍNH HỢP NHẤT" and the notes FORM CODE ("Mẫu B050/TCTD - HN") on the cash flow's
        **first** page as well as its second, so no cash-flow run is ever opened and the tail
        rule has nothing to extend. Every layer reported `no such statement on any page of this
        filing` — ⚠️ **and `settled_absences` treats that reason as PERMANENT**, so both
        quarters were recorded as filings that contain no cash flow. They contain one: page 8
        of Q1-2017 prints "LƯU CHUYỂN TIỀN THUẦN TỪ HOẠT ĐỘNG KINH DOANH" over 67 figures.

        The evidence is the statement's own SECTION heading (`SECTION_HEADING`) in the page's
        HEADER BLOCK, and three guards around it:

          * ⚠️ **THE RUN MUST BE OPEN** — `run is not None` means the PREVIOUS page belonged to
            a statement, which is `_fill_continuations`' own premise that a statement's pages
            are contiguous. It keeps this to the statements block and out of the notes, where
            the same words in a narrative paragraph would otherwise be evidence.
          * ⚠️ **THE REPORT MUST NOT ALREADY HAVE PAGES**, anywhere in the document (`seen` is
            taken from the FINAL classification, so this protects forwards as well as back). A
            filing whose cash flow is correctly titled somewhere cannot have a note claimed for
            it here.
          * **it must be a TABLE** (`MIN_TABLE_WORDS`), the same floor a continuation page
            clears.

        ⚠️ **MEASURED OVER EVERY PAGE OF THREE TCB FILINGS BEFORE IT SHIPPED — 191 pages, and
        exactly SIX carry the needle in their header block: the six cash-flow pages.** Zero
        notes pages, zero narrative pages, zero of the ~150 note tables that follow.

        ⚠️ It says nothing about whether the statement RECONCILES; `reconcile` and `sane` judge
        that exactly as before, and this only decides which pages they are handed.
        """
        if not self.notes_head or run is None:
            return None
        if not self._is_table(page):
            return None
        lines = [l for l in page["text"].splitlines() if l.strip()][:self.HEADER_LINES]
        ns = self.norm("\n".join(lines)).replace(" ", "")
        for report, needle in self.SECTION_HEADING.items():
            # ⚠️ `report == run` was here too and was REMOVED on the mutation check that was
            # supposed to defend it: deleting the clause broke no test, because `run` is only
            # ever a report the classifier already found (or one this opened and added), so
            # `report in seen` implies it. A clause no test can fail is a clause that will be
            # wrong one day with nothing saying so.
            if report in seen:
                continue
            if needle in ns:
                return report
        return None

    def _is_notes_tail_page(self, page: dict, run: str) -> bool:
        """Is this NOTES-headed page the running statement's own continuation?

        A NOTES page ends the run in `_fill_continuations`, and that is right for every filing
        but one shape: the document itself prints the notes header — sometimes the notes FORM
        CODE too — on a statement's second page. The header is then the only thing wrong with
        the page, so the evidence has to come from what is PRINTED ON IT.

        Two markers, both required, and neither defeated by the label wrap that makes `TAIL`
        miss here (see `SECTION_HEADING` for the measurement and for why each was chosen):

          * the statement's own SECTION heading — a phrase only that statement prints;
          * the CLOSING BALANCE's date clause, which a note merely NAMING the same account
            does not carry.

        ⚠️ The run ENDS at the page this admits, like `_is_tail_page`: a page reached through
        its neighbour's identity must not pass that licence on to the next one.
        """
        if not self.notes_tail:
            return False
        section = self.SECTION_HEADING.get(run)
        if not section:
            return False
        if len(self._numbers(page["words"])) < self.MIN_TAIL_WORDS:
            return False
        # The WHOLE page, not the header block — this evidence is printed in the table.
        ns = self.norm(page["text"]).replace(" ", "")
        return section in ns and any(c in ns for c in self.TAIL_CLOSING)

    def _is_supplement(self, page: dict) -> bool:
        """Does this page announce itself as an EXPLANATION rather than a statement?

        See `SUPPLEMENT_NS`. Read from the HEADER BLOCK, like `AUDIT_NS` and the three
        statement titles, so a note or a statement line that merely uses the word in prose
        further down the page cannot trigger it.
        """
        header = "\n".join(
            [l for l in page["text"].splitlines() if l.strip()][:self.HEADER_LINES])
        ns = self.norm(header).replace(" ", "")
        return any(n in ns for n in self.SUPPLEMENT_NS)

    def _is_tail_page(self, page: dict, run: str) -> bool:
        """Is this the sparse LAST page of the statement running through it?

        Not a looser `_is_table`: a page qualifies only by carrying the statement's own closing
        line (`TAIL`), so a signature page, a narrative page or a note that merely follows the
        statement can never be swept in however many stray figures it holds. See `TAIL` for the
        BID Q1-2012 cash flow this was measured on.
        """
        if not self.tail_continuation:
            return False
        needles = self.TAIL.get(run)
        if not needles:
            return False
        if len(self._numbers(page["words"])) < self.MIN_TAIL_WORDS:
            return False
        # The WHOLE page, not the header block: a closing balance is printed at the BOTTOM of
        # the statement, which is the opposite end from a title.
        ns = self.norm(page["text"]).replace(" ", "")
        return any(n in ns for n in needles)

    # ──────────────────────────────────────────────────────────────────────
    # The table
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _clusters(xs: List[float], tol: float) -> List[List[float]]:
        out: List[List[float]] = []
        for x in xs:
            if out and x - out[-1][-1] <= tol:
                out[-1].append(x)
            else:
                out.append([x])
        return out

    def _numbers(self, words: list) -> list:
        return [w for w in words
                if self.NUM_RE.match(w[4]) and self.parse_num(w[4]) is not None]

    def value_columns(self, words_by_page: Dict[int, list], width: float) -> List[float]:
        """The right edge of each period column, left to right.

        Columns of figures are RIGHT-ALIGNED, so their right edges line up tightly while
        their left edges do not — clustering the centres instead lets a wide number bridge
        two columns into one.

        Only the right of the page is considered. The left holds the section numbering and
        the "Thuyết minh" note reference, which are numbers too: counted as a period column
        they drag the label boundary left of the labels themselves, and then EVERY label
        parses as empty.
        """
        nums = [w for ws in words_by_page.values() for w in self._numbers(ws)]
        edges = sorted(w[2] for w in nums if w[2] >= width * self.VALUE_ZONE)
        if not edges:
            return []
        clusters = self._clusters(edges, self.EDGE_TOL)
        biggest = max(len(c) for c in clusters)
        keep = [c for c in clusters if len(c) >= 0.35 * biggest]
        cols = [sum(c) / len(c) for c in keep][:5]

        # Drop a "Thuyết minh" note-reference column that survived into the value zone. The
        # right-60% rule already excludes the note column for WORD-level OCR, which scatters the
        # label's words across the left of the page. A LINE-level detector (the onnx engine)
        # instead emits ONE tight box per note number, and they cluster as a well-populated
        # right-edge column INSIDE the value zone — it becomes column 1, and `_first_value` then
        # reads every line's NOTE NUMBER as its figure, so nothing maps or reconciles. Separate it
        # by magnitude, which needs no threshold: a period column's figures are 4-9 digits (Triệu
        # VND), a note reference 1-2. (experiment_8.)
        if len(cols) <= 1:
            return cols
        kept = []
        for c in cols:
            digits = sorted(len(re.sub(r"\D", "", w[4])) for w in nums
                            if abs(w[2] - c) <= self.EDGE_TOL)
            if digits and digits[len(digits) // 2] > self.NOTE_MAX_DIGITS:
                kept.append(c)
        kept = kept or cols          # never leave the caller with nothing to parse
        # ⚠️ AND THE ITEM-CODE COLUMN IS THE ONE MAGNITUDE CANNOT REACH — see _code_column.
        code = self._code_column(kept, words_by_page)
        return [c for c in kept if c != code] if code is not None else kept

    # A note reference is 1-2 digits ("Thuyết minh 4", "…21"); a Triệu-VND figure is 4-9. The
    # median digit-count of a column's numbers separates them with room to spare.
    NOTE_MAX_DIGITS = 2

    # ⚠️ THE DETECTOR SPLITS ONE FIGURE INTO TWO BOXES, AND THE HALVES ARE BOTH PLAUSIBLE
    # NUMBERS. On VIC Q3-2014 at onnx@200 the balance sheet returns `'5.209.108'` ending at
    # x=405.7 and `'954.978'` starting at x=409.5 — one printed figure, 5.209.108.954.978, in
    # two boxes 3.8pt apart. The left half lands on no column and is dropped, so the row keeps
    # `954.978`; where enough left halves line up they instead form a SPURIOUS COLUMN and are
    # kept as a period of their own. 60 of that statement's figures are split this way.
    #
    # ⚠️ **AND NOTHING DOWNSTREAM CAN SEE IT.** The grand totals were not split, so `reconcile`
    # passes on a statement whose detail lines read `i_1_tien = 158.154` against a printed
    # 945.186.158.154, and `sane` probes a total that is correct. That is `SLD-1`'s shape a
    # fourth time — a wrong figure that passes every gate — and it is why this is refused rather
    # than repaired here: a refusal escalates the cascade, and `onnx@300` reads the same
    # document with **0** split figures and two clean columns.
    #
    # ⚠️ **THE GAP IS MEASURED, NOT CHOSEN.** Across 12 statements of VCB Q1-2021, VCB Q1-2026,
    # ACB Q1-2024 and BID Q4-2016 — every one of which parses today — this rule fires **0**
    # times at 4.5pt. On VIC Q3-2014 it fires 60 times on the balance sheet and 27 on the income
    # statement at onnx@200, and 0 times on either at onnx@300. Widening it to 6pt starts
    # picking up one or two pairs per bank statement, so 4.5 is where the two populations are
    # actually separated and not where a round number fell.
    SPLIT_MAX_GAP = 4.5
    # Joining the two halves with a thousands separator has to yield a well-formed figure. Two
    # ADJACENT PERIOD figures usually fail this — "1.558.887.407" + "1.541.259.663" joins to a
    # group of ONE digit — but not always, which is why the gap does the separating and this
    # only stops the obvious nonsense.
    SPLIT_JOIN_RE = re.compile(r"^\(?-?\d{1,3}(\.\d{3})*\)?$")

    def split_figures(self, words_by_page: Dict[int, list], width: float) -> int:
        """How many figures this reading split across two boxes.

        Geometry and text only — deliberately NOT keyed on the detected columns, because the
        left halves cluster into a column of their own and anything asking "is this box on a
        column?" would then answer yes for the very fragments it is looking for.
        """
        lo = width * self.VALUE_ZONE
        n = 0
        for words in words_by_page.values():
            lines: Dict[float, list] = {}
            for w in sorted(words, key=lambda w: (w[1], w[0])):
                k = next((k for k in lines if abs(k - w[1]) <= self.Y_TOL), w[1])
                lines.setdefault(k, []).append(w)
            for ws in lines.values():
                nums = sorted([w for w in self._numbers(ws) if w[2] >= lo],
                              key=lambda w: w[0])
                for a, b in zip(nums, nums[1:]):
                    if (b[0] - a[2] < self.SPLIT_MAX_GAP
                            and self.SPLIT_JOIN_RE.match(
                                a[4].strip("()") + "." + b[4].strip("()"))):
                        n += 1
        return n

    # ⚠️ THE "Mã số" COLUMN IS WHERE THE MAGNITUDE RULE ABOVE RUNS OUT, AND IT IS THE STANDARD
    # CORPORATE FORM — not a quirk of one filing. VAS form B01-DN prints
    # `Chỉ tiêu | Mã số | Thuyết minh | Số cuối kỳ | Số đầu năm`, and that second column holds
    # the filing's own item numbering: 100, 110, 111, 270, 300, 440 — THREE digits, sometimes
    # four (3131, 3161). A note reference is 1-2 and a period figure is 4-14, so the code sits
    # exactly in the overlap and `NOTE_MAX_DIGITS` cannot be moved to cover it without throwing
    # away real figure columns. Measured on VIC Q3-2014: the code column clusters at x=279.7 of
    # a 595pt page — 47%, INSIDE the right-60% value zone — 86 numbers, 79 of them 3-digit and
    # 7 four-digit, and it became column 0.
    #
    # ⚠️ **AND IT PRODUCES WRONG VALUES, NOT A REFUSAL, WHICH IS WHY THIS IS THE DEFAULT PATH
    # AND NOT A LATE CASCADE LAYER.** On a balance sheet the gates do catch it — assets read
    # 270 and resources 440, so `reconcile` refuses with "assets != liabilities + equity" and
    # the quarter is recorded `missing`. On the other two statements they do not: an income
    # statement only has to present a PBT line, and `50` is a PBT line as far as `reconcile` is
    # concerned; a cash flow only has to present a closing balance, and `70` is one. Both are
    # then left to `sane`, which FAILS OPEN on a ticker with no accepted history — exactly the
    # position every non-bank ticker is in on its first run. VIC Q1-2011 is the measured case
    # and it is already on disk: both grand totals happened to read correctly so `reconcile`
    # passed, and `a_tai_san_ngan_han = 100`, `b_tai_san_dai_han = 200`, `i_no_ngan_han = 310`
    # and `ii_no_dai_han = 330` were written as figures. §6-2-untricies' rule applies — when
    # the gates cannot see the defect, the repair cannot be an escalation.
    #
    # The discriminator is the COLUMN HEADING, which is what `parse` already reads rather than
    # counting columns (`_prints_quarter_column`, "4 columns can equally be a note reference
    # plus an over-segmented pair"). The heading is printed once, above the column, and OCR
    # reads it cleanly: VIC Q3-2014 page 4 gives the box `'Mã số'` at x0=261.0 x1=286.2 with
    # the column's right edge at 279.7, inside it.
    CODE_HEADER_NS = "maso"
    CODE_HEADER_MATCH = 0.80

    # ── PER-LAYER FLAGS, DECLARED ON THE CLASS ────────────────────────────
    # ⚠️ **SO THAT A PARSER BUILT WITHOUT `__init__` STILL CARRIES THEM, AND ADDING A FLAG
    # STOPS BREAKING EVERY TEST THAT DOES.** Several test modules build their subject with
    # `PdfParser.__new__(PdfParser)` — deliberately, to drive `table_rows` on hand-made word
    # boxes with no document, no engine and no OCR — and each of them then had to assign every
    # per-layer flag `table_rows` reads. Adding `deskew_rows` on 2026-09-04 broke 36 tests in
    # four files at once, and not one of the failures was about de-skewing: they were
    # `AttributeError` on a flag the test had never heard of.
    #
    # ⚠️ **IT CHANGES NO BEHAVIOUR.** `__init__` assigns every one of these, so a normally
    # built parser shadows all of them with an instance attribute of the same value; these are
    # the fallback for an instance that skipped `__init__`, and the default is OFF in every
    # case, which is what `apply_layer` sets anyway on the first layer of any cascade.
    join_split_digits = False
    join_lost_separator = False
    title_over_form = False
    loose_form_code = False
    realign_rows = False
    notes_boundary = False
    tail_continuation = False
    notes_tail = False
    notes_head = False
    label_wrap = False
    reseat_words = False
    deskew_rows = False
    unit_from_document = False
    column_header_blind = False

    def _code_column(self, cols: List[float],
                     words_by_page: Dict[int, list]) -> Optional[float]:
        """The right edge of the "Mã số" item-code column among `cols`, or None.

        Three conditions, and every one of them fails SAFE — an unreadable heading, a heading
        over nothing, or a heading over a column that is not the leftmost all return None and
        leave the caller exactly as it was. A statement then fails the way it does today rather
        than losing a column it needed.

        1. A whole word box normalises to "mã số". Whole, not contained: `SequenceMatcher`
           against the entire box is what keeps a label mentioning the phrase in prose from
           answering, and it still tolerates the tone marks OCR drops.
        2. A detected column's right edge lies under that box, within `EDGE_TOL` of its span.
           This alone is most of the protection: where the codes are merged into the labels
           instead of forming a column — VIC's own income statement reads "02 Các khoản giảm
           trừ" — the heading sits at x≈261-286 and the leftmost figure column at x≈436, so
           nothing is under it and nothing is dropped.
        3. It is the LEFTMOST column. That is the form's layout, not a heuristic: `Mã số`
           precedes `Thuyết minh` and both period columns. Requiring it means a mis-read
           heading cannot reach past a real figure column to take one.
        """
        from difflib import SequenceMatcher

        if len(cols) < 2:
            return None                      # dropping the only column helps nobody
        leftmost = min(cols)
        want = self.CODE_HEADER_NS
        for words in words_by_page.values():
            for ns, wx0, wx1 in self._header_candidates(words, len(want)):
                # ⚠️ **THE WHOLE BOX, OR THE TEXT IT BEGINS WITH — because the recogniser
                # merges neighbouring HEADER words as readily as it merges anything else.**
                # BSR's FY-2019 balance sheet sets "Mã số" and "Thuyết minh" on the same
                # baseline, and page 7 comes back as ONE box reading `Mã số minh`: `masominh`
                # against `maso` is **0.667**, under the bar, so the item-code column was not
                # dropped and `TỔNG CỘNG TÀI SẢN` read **270**. (Page 8 of the same filing
                # splits it the other way, into `Mã` and `số`, 0.667 each — one page
                # answering is enough, and this is why the scan does not stop at the first.)
                #
                # Reading the LEADING text keeps condition 1 a statement about what the box
                # begins with rather than a containment test: `MẪU SỐ B 01-DN/HN`, the form
                # code printed in the same band, scores 0.50 whole and **0.75** on its head,
                # and is still refused. Conditions 2 and 3 are untouched and remain the real
                # protection.
                if max(SequenceMatcher(None, want, ns).ratio(),
                       SequenceMatcher(None, want, ns[:len(want)]).ratio()) \
                        < self.CODE_HEADER_MATCH:
                    continue
                if wx0 - self.EDGE_TOL <= leftmost <= wx1 + self.EDGE_TOL:
                    return leftmost
        return None

    def _header_candidates(self, words: list, fragment: int = 4):
        """`(text, x0, x1)` per header box — and per box STACKED on the one below it.

        ⚠️ **A NARROW COLUMN HEADING IS SET ON TWO LINES, AND `_code_column` COULD NOT
        READ ONE (`MSO-3`, 2026-09-04).** The `Mã số` column is ~16pt wide, so a landscape
        VAS form prints its heading as "Mã" over "số". Measured on FPT's Q3-2019
        consolidated balance sheet: `Mã` at y0=159.60 x=298.6-313.2 and `số` at y0=169.44
        x=300.7-312.0 — two boxes, one directly under the other. Each normalises to `ma` / `so`,
        which score **0.667** against `maso`, under the bar — so the item-code column survived
        and `TỔNG CỘNG TÀI SẢN` was read as **270**.

        ⚠️ **THAT IS `MSO-1`/`MSO-2` A THIRD TIME AND NEITHER FIX REACHES IT.** `MSO-2`
        joins a heading the recogniser merged HORIZONTALLY ("Mã số minh"); this one is not
        merged at all, it is set on two baselines by the FILING.

        ⚠️ **THE JOIN IS THE ONLY THING WIDENED — CONDITIONS 2 AND 3 ARE UNTOUCHED AND
        REMAIN THE REAL PROTECTION.** A candidate still has to sit over a detected column and
        that column still has to be the LEFTMOST, so an invented `maso` over nothing, or over a
        figure column, drops nothing. The join itself is tight: the lower box must OVERLAP the
        upper one horizontally and start within one box-height of its bottom — a stacked
        heading, not two rows of a table, which are 13-18pt apart and horizontally offset.
        """
        seen = []
        heads = []
        for w in words:
            ns = self.norm(w[4]).replace(" ", "")
            if not ns:
                continue
            yield ns, w[0], w[2]
            seen.append((ns, w))
            # ⚠️ **ONLY A SHORT BOX MAY BEGIN A JOIN, AND THE BOUND IS THE NEEDLE'S OWN
            # LENGTH.** The upper half of a heading split across two lines is a FRAGMENT by
            # construction ("ma"); a box already at least as long as the whole needle cannot
            # need a join to reach it, since the single-box branch above scores its leading
            # text too. The LOWER half is unbounded — "Mã" over "số minh" is a real layout
            # and its tail is not short.
            # ⚠️ It is also what keeps this affordable. Unpruned the join is quadratic in
            # every box on the page and cost **185 ms on a 1,200-box page** — paid once per
            # statement per parse, in the DEFAULT path, for every filing in the corpus. Bounded
            # to short heads it is **10.7 ms** there and 2.8 ms on a 170-box page.
            if len(ns) < fragment:
                heads.append((ns, w))
        for ns_a, w in heads:
            budget = w[3] - w[1]              # this box's own height, as the vertical reach
            for ns_b, v in seen:
                if v is w or not (w[3] - self.EDGE_TOL <= v[1] <= w[3] + budget):
                    continue
                if min(w[2], v[2]) <= max(w[0], v[0]):        # must overlap in x
                    continue
                yield ns_a + ns_b, min(w[0], v[0]), max(w[2], v[2])

    # How far a numeric box may be searched for its label, and how much better the shifted
    # pairing must be before it is believed. Both measured 2026-08-25 on four bank filings:
    #
    #   BID Q1-2021 (broken)  offset* = 7   co-location  55 -> 174   x3.16
    #   VCB Q1-2021 (sound)   offset* = 4               23 ->  64   x2.78
    #   ACB Q1-2021 (sound)   offset* = 2               57 ->  62   x1.09
    #   BID Q2-2021 (sound)   offset* = 2               63 ->  65   x1.03
    #
    # The search stops at 12 because rows are set 13-18pt apart, so a larger shift would pair a
    # figure with the NEXT item's label rather than its own. The 1.5x gain floor keeps the
    # correction off a document that is already paired correctly — the two sound filings that
    # peak near zero score ~1.05 and are left exactly as they were.
    #
    # NOTE VCB scores 2.78 and is sound: there the shift merely MERGES a pair `carry` was
    # already joining correctly, so it changes the mechanism and not the result. That is why the
    # gain is a floor for applying a correction and never, on its own, a diagnosis of breakage —
    # and why this whole path is reached only from a parse layer that runs after every other.
    REALIGN_MAX = 12
    REALIGN_MIN_GAIN = 1.5

    def _value_row_offset(self, words_by_page: Dict[int, list], lo: float) -> float:
        """The constant vertical gap between a numeric box and the label box of its own line.

        Chosen as the shift that maximises CO-LOCATION — the number of lines holding both a
        label and a figure. That criterion never looks at what the figures are, only at whether
        the page reassembles into whole rows, so it cannot be tuned toward a total that
        reconciles; a wrong offset scatters labels and figures apart and scores worse.

        -> 0.0 when no shift beats leaving the page alone by `REALIGN_MIN_GAIN`.
        """
        def colocated(delta: float) -> int:
            hits = 0
            for words in words_by_page.values():
                lines: Dict[float, list] = {}
                for w in words:
                    num = (self.NUM_RE.match(w[4]) is not None
                           and self.parse_num(w[4]) is not None and w[2] >= lo)
                    y = w[1] + (delta if num else 0.0)
                    k = next((k for k in lines if abs(k - y) <= self.Y_TOL), y)
                    lines.setdefault(k, []).append(num)
                hits += sum(1 for kinds in lines.values() if any(kinds) and not all(kinds))
            return hits

        scores = {float(d): colocated(float(d)) for d in range(0, self.REALIGN_MAX + 1)}
        best_score = max(scores.values())
        # ⚠️ THE CENTRE OF THE PLATEAU, NOT ITS FIRST POINT. `Y_TOL` is a tolerance, so every
        # shift that brings a figure within it of its label scores the same — a true offset of
        # 7pt reads as a flat maximum over 3..11, and taking the first of those would sit a
        # figure right on the edge of the tolerance, where a page with tighter line spacing
        # would pair it with the NEXT label instead. The midpoint is the offset itself, and on a
        # real scan whose maximum is a single point it changes nothing (BID Q1-2021 peaks at 7
        # alone: 6 -> 170, 7 -> 174, 8 -> 167).
        top = sorted(d for d, sc in scores.items() if sc == best_score)
        best = top[len(top) // 2] if len(top) % 2 else (top[len(top) // 2 - 1]
                                                        + top[len(top) // 2]) / 2.0
        if best and best_score >= max(scores[0.0], 1) * self.REALIGN_MIN_GAIN:
            return best
        return 0.0

    # ── SCAN SKEW ─────────────────────────────────────────────────────────
    # A search, not a fit. A least-squares line through the boxes would measure the PAGE's
    # centre of mass, not its baselines — a statement whose labels are long and whose figures
    # are short puts far more ink on the left, and the fitted slope follows that instead of
    # the text. What identifies a skew is that correcting for it makes the boxes CLUSTER into
    # lines, so the score below is the clustering itself and the slope is whatever maximises
    # it. That also means a page with no skew scores best at exactly 0.0 and is left alone.
    DESKEW_MAX = 0.030      # ±1.7°, past anything a flatbed or a phone camera produces here
    DESKEW_STEP = 0.0010    # ~0.06°, and then refined ten times finer around the winner
    DESKEW_GAIN = 1.05      # must beat the UNCORRECTED page by 5 %, or it is noise

    def _page_skew(self, words: list) -> float:
        """This page's baseline slope (dy per dx), or 0.0 when it is not skewed.

        ⚠️ **THE BOX CENTRE, NEVER `y0` — and the difference is most of the answer.** A label
        box is taller than a figure box (diacritics above, descenders below), so their TOPS
        differ by ~5 pt on the same printed line while their centres do not. Labels sit at low
        x and figures at high x, so scoring on `y0` reads that constant height difference as
        slope: on FPT Q3-2024 page 8 it gives 0.0151 against the true 0.0125, a 20 % error in
        the one direction that matters, because it is the right-hand columns that fall out of
        tolerance first.
        """
        pts = [((w[0] + w[2]) / 2.0, (w[1] + w[3]) / 2.0)
               for w in words if str(w[4]).strip()]
        if len(pts) < self.MIN_TABLE_WORDS:
            return 0.0
        x0 = min(x for x, _ in pts)

        def score(s: float) -> int:
            # Greedy clustering on the corrected y, tolerance `Y_TOL`, scored by the sum of
            # squared line sizes — so ten boxes on one line beat five lines of two.
            ys = sorted(y - s * (x - x0) for x, y in pts)
            total, start, n = 0, ys[0], 1
            for v in ys[1:]:
                if v - start <= self.Y_TOL:
                    n += 1
                else:
                    total += n * n
                    start, n = v, 1
            return total + n * n

        # ⚠️ **THE CENTRE OF THE MAXIMAL BAND, NOT ITS FIRST POINT.** `Y_TOL` is a
        # TOLERANCE, so once a slope brings a row's boxes inside it every nearby slope scores
        # the same and the maximum is a PLATEAU, not a peak — on the synthetic three-row page
        # in `test_cafef_deskew.py` the score is flat at 75 from 0.009 to past 0.020 for a
        # true 0.013. Taking the first point of that band leaves every figure on the very edge
        # of the tolerance, where one more crooked row falls out again. This is the identical
        # lesson `_value_row_offset` records for `realign_rows` (SLD-1, 2026-08-26), and it
        # survived that fix's real-data check the same way: the real page's plateau is narrow
        # enough that the two answers differ by little, and the synthetic one is what caught it.
        def peak(candidates):
            scored = [(score(s), s) for s in candidates]
            top = max(v for v, _ in scored)
            band = [s for v, s in scored if v == top]
            return top, (min(band) + max(band)) / 2.0

        base = score(0.0)
        steps = int(round(self.DESKEW_MAX / self.DESKEW_STEP))
        best, best_s = peak([i * self.DESKEW_STEP for i in range(-steps, steps + 1)])
        if best_s:
            fine = self.DESKEW_STEP / 10.0
            best, best_s = peak([best_s + i * fine for i in range(-9, 10)])
        # ⚠️ A MARGIN, BECAUSE THE SEARCH ALWAYS FINDS SOMETHING. `score` is maximised over 61
        # candidates, so on a clean page the winner is a fraction of a percent above 0.0 —
        # noise, and applying it would move words across a 4 pt tolerance for nothing.
        return best_s if best >= base * self.DESKEW_GAIN else 0.0

    @staticmethod
    def _line_key(lines: Dict[float, list], y: float, y_tol: float) -> float:
        """The existing printed line this word belongs to, or `y` to open a new one.

        ⚠️ **NEAREST WITHIN THE TOLERANCE, NEVER THE FIRST ONE FOUND.** `Y_TOL` is a
        tolerance, not a line height, so on a tightly-set page TWO buckets can both sit
        inside it — and buckets are keyed by whichever y opened them, in OCR order, which
        has nothing to do with the page. Taking the first match then hands a figure to a
        line it is merely *near* while the line it belongs to is closer.

        ⚠️ **MEASURED, ON BSR's Q3-2019 CONSOLIDATED INCOME STATEMENT.** A stray `)` at
        x=587 — outside the table, off the right margin — opened a bucket at y=362.16.
        The 9-month "lợi nhuận khác" figure at y=358.56 belongs to line 14 at y=357.84
        (**0.72pt away**) and joined the stray instead (**3.60pt away**), because the
        stray was inserted first. Line 14 lost its 9-month column and the orphan bucket
        then merged into line 15, so `15. Tổng lợi nhuận kế toán trước thuế` was written
        with **48,726,111,955** — which is line 14's 9-month figure, exactly
        (51,026,059,759 − 2,299,947,804) — against a printed **624,185,898,676**.

        ⚠️ **AND EVERY GATE PASSED.** An income statement is anchored on PBT alone, so
        `reconcile` never sums the components against it and `sane` only compares one
        magnitude to a band. `SLD-1`'s shape again: a wrong figure written as `pdf`.

        Nearest-match is a strict refinement — where one bucket is in tolerance it is also
        the nearest, so only a page that had two candidates can move at all.
        """
        near = [k for k in lines if abs(k - y) <= y_tol]
        return min(near, key=lambda k: abs(k - y)) if near else y

    def _row_y(self, w, offset: float, lo: float, skew: float, x0: float) -> float:
        """The y `table_rows` groups this word by — the ONE place that decides it.

        ⚠️ **BOTH BUCKETING PASSES MUST MEASURE A WORD THE SAME WAY, AND `_reseat` DID NOT.**
        It re-derived `w[1] + offset` inline, so when `deskew_rows` was first wired into
        `table_rows` the second pass silently re-seated every word on its UNCORRECTED y and
        undid the correction — the rows came back split much as before, and the layer looked
        like a failed idea rather than a missing line. `realign_rows` had been safe only
        because both copies happened to spell it identically.
        """
        y = w[1] + (offset if (offset and w[2] >= lo
                               and self.NUM_RE.match(w[4]) is not None
                               and self.parse_num(w[4]) is not None) else 0.0)
        return y - skew * ((w[0] + w[2]) / 2.0 - x0) if skew else y

    def _reseat(self, lines: Dict[float, list], offset: float,
                lo: float, skew: float = 0.0, x0: float = 0.0) -> Dict[float, list]:
        """A SECOND bucketing pass: every word goes to the nearest FINAL bucket key.

        ⚠️ **`_line_key` PREFERS THE NEAREST BUCKET, BUT ONLY AMONG THE ONES ALREADY OPEN.**
        Buckets are opened in the RECOGNISER's emission order, which has nothing to do with
        the page, so a word whose own line has not been opened yet joins a neighbour it is
        merely near -- and which line wins then depends on the order the boxes arrived in.
        `_line_key`'s own docstring already claims the property this restores: *"where one
        bucket is in tolerance it is also the nearest"*. That is true of the FINAL key set and
        was not true of the partial one.

        ⚠️ **MEASURED ON CTG's Q3-2014 CONSOLIDATED INCOME STATEMENT.** The label of
        "X. Chi phí dự phòng rủi ro tín dụng" wraps, and its continuation "ro tín dụng"
        (y=654.24) is emitted BEFORE the figures of the row it belongs to. The prior-year
        column at y=650.40 is 3.84pt from that continuation -- inside `Y_TOL` -- and 0.48pt
        from y=649.92, where the other three columns land a moment later. It joined the
        continuation, so `X` was written with **796,825,875,834**: the Q3-2013 column, not
        Q3-2014's **775,245,756,517**. Both figures are real, both are printed on that page,
        and the wrong one balances against nothing -- IX - X missed the printed XI by
        21,579,895,652 while the filing's own three columns each close to the đồng.

        ⚠️ **THIS IS NOT THE y-ORDER BUCKETING THAT WAS TRIED AND REJECTED ON 2026-09-01**
        (see `table_rows`). That change keyed each bucket on its topmost word, so buckets
        CHAIN and one wrong pairing walks down the page -- it wrote 1,648,126,921 as BSR's
        post-tax profit. Here the key set is FIXED by the first pass and only membership is
        revised, so nothing can chain: a word may move to a bucket that already exists and can
        never create one, merge two, or shift a key.

        ⚠️ **AND IT SHIPS ONLY BESIDE `label_wrap`, never alone -- measured, not argued.** The
        word this moves is usually the one that was holding a label continuation in place. On
        CTG Q3-2014 the reseat alone left the X row keyed `x_chi_phi_du_phong_rui` and the XI
        row `xi_tong_loi_nhuan_truoc`, so BOTH the deduction and the PBT anchor stopped
        mapping and the statement went from a wrong figure to no figure at all (16 items ->
        11, `no profit before tax`). With `label_wrap`, `take_below` re-attaches each
        continuation and the same reading maps 18 items and reconciles. §6-2-unvicies drew
        this rule from the other side: *when two new layers differ by a LABEL REPAIR, the
        repair goes first.*

        `offset`, `lo`, `skew` and `x0` are `table_rows`' own and reach `_row_y`, so a word's y
        is measured by the SAME code the first pass measured it with -- a `realign_rows` shift
        must not be applied twice, and must not be dropped either, and neither must a skew.
        """
        keys = sorted(lines)
        if len(keys) < 2:
            return lines
        out: Dict[float, list] = {k: [] for k in keys}
        for k in keys:
            for w in lines[k]:
                y = self._row_y(w, offset, lo, skew, x0)
                # Ties keep the bucket the word is already in: this pass exists to remove an
                # ordering dependence, so it must not introduce one of its own.
                best = min(keys, key=lambda kk: (abs(kk - y), abs(kk - k)))
                out[best if abs(best - y) <= self.Y_TOL else k].append(w)
        return {k: v for k, v in out.items() if v}

    def table_rows(self, words_by_page: Dict[int, list], columns: List[float]) -> List[Row]:
        """Rows rebuilt from word coordinates.

        A number is assigned to the column whose right edge it lines up with, so a row with a
        missing figure keeps its remaining values in the right columns instead of shifting
        them left.
        """
        if not columns:
            return []
        lo = columns[0] - self.LABEL_GAP        # labels live to the left of column 1
        # ⚠️ SET PER PARSE LAYER, off by default — see `set_realign_rows`. Measured once per
        # STATEMENT, from the pages this call was handed, because the offset is a property of
        # how those pages were scanned and a filing may mix clean pages with scanned ones.
        offset = (self._value_row_offset(words_by_page, lo)
                  if self.realign_rows else 0.0)

        out: List[Row] = []
        for page in sorted(words_by_page):
            lines: Dict[float, list] = {}
            # ⚠️ **PER PAGE, NOT PER STATEMENT.** A filing is scanned a page at a time, so each
            # page carries its own skew — and the same statement can run across a clean page
            # and a crooked one. `offset` above is per statement because `SLD-1` is a property
            # of the RECOGNISER's box placement; this is a property of the paper.
            skew = self._page_skew(words_by_page[page]) if self.deskew_rows else 0.0
            x0 = (min((w[0] + w[2]) / 2.0 for w in words_by_page[page])
                  if skew and words_by_page[page] else 0.0)
            # ⚠️ **CONSUMED IN THE RECOGNISER'S OWN ORDER, AND SORTING BY y FIRST WAS TRIED
            # AND REJECTED — 2026-09-01.** Bucketing in y order is the more principled shape
            # (a bucket then always grows downward from its topmost word, so the grouping is
            # a property of the page rather than of the emission order), and on BSR's
            # Q3-2019 income statement it gains a row at `onnx@200`. But a bucket keyed on
            # its topmost word CHAINS: at `onnx@400` the same change swept the deferred-tax
            # comparative into line 18 and wrote **1,648,126,921** as post-tax profit, and at
            # `onnx@300` it put the prior-year column into the parent-company line. Both are
            # WRONG FIGURES that `reconcile` passes, which is worse than the row it buys.
            # The tolerance is 4.0pt against wrapped halves 4-8pt apart on this page, so the
            # clustering has no margin and a chain rule spends it. Left as it was.
            for w in words_by_page[page]:
                # ⚠️ The skew correction is RELATIVE, so it may be applied to `y0` even though
                # the slope was measured on box centres: every box on one printed line moves
                # by the same amount and the line's own spread is what closes.
                y = self._row_y(w, offset, lo, skew, x0)
                k = self._line_key(lines, y, self.Y_TOL)
                lines.setdefault(k, []).append(w)

            if self.reseat_words:
                lines = self._reseat(lines, offset, lo, skew, x0)

            parsed: List[tuple] = []
            for y in sorted(lines):
                label: List[str] = []
                vals: List[Optional[int]] = [None] * len(columns)
                for w in sorted(lines[y], key=lambda w: w[0]):
                    v = self.parse_num(w[4]) if self.NUM_RE.match(w[4]) else None
                    if v is not None and w[2] >= lo:
                        j = min(range(len(columns)), key=lambda i: abs(columns[i] - w[2]))
                        if abs(columns[j] - w[2]) <= self.EDGE_TOL * 2 and vals[j] is None:
                            vals[j] = v
                    elif w[2] < lo and not self.NUM_RE.match(w[4]):
                        label.append(w[4])
                parsed.append((y, label, vals))

            carry: List[str] = []               # a label that wrapped onto its own line
            # ⚠️ THE FILING'S ITEM CODE MARKS WHERE A NEW ITEM BEGINS, and on this page it is
            # the only thing that says so. The code column can sit on its OWN baseline,
            # vertically centred on a two-line label, so neither "the label above" nor "the
            # label below" is right on its own. Text printed AFTER the code belongs to that
            # item; text before it is used only when the item printed nothing after its code.
            # `None` = no code line seen since the last row was emitted. `label_wrap` only.
            since_code: Optional[List[str]] = None
            carry_y = 0.0                       # y of the last line that fed `carry`
            for i, (y, label, vals) in enumerate(parsed):
                if any(v is not None for v in vals):
                    # An EMPTY `since_code` means the code was the last thing before these
                    # figures, so the label is above it — BID Q1-2012's opening balance.
                    base = since_code if (self.label_wrap and since_code) else carry
                    words_ = base + label
                    # ⚠️ `not label`, NOT `not words_`, UNDER `label_wrap`. The original test
                    # fires only when the value line has neither a label nor a carry; a label
                    # that wrapped AROUND its figures leaves a carry, so the branch was skipped
                    # and the half BELOW the figures — the only half that says which account
                    # this is — went to the next row as its carry. BID's Q1-2012 cash flow
                    # keeps "thời điểm cuối kỳ" on the line under its figures, so the closing
                    # balance was keyed on the opening line's wording, and the ordered walk
                    # then handed BOTH cash figures to the wrong accounts while `reconcile` and
                    # `sane` both passed. The proximity guard below is what separates a wrapped
                    # tail from the NEXT item's label: here the gap is 5.8pt against ordinary
                    # row spacing of 13-18pt on the same page.
                    # ⚠️ UNDER `label_wrap` THE VALUE LINE MAY CARRY HALF ITS OWN LABEL, so
                    # `not label` is still too strict. BID's Q1-2026 cash flow prints
                    # "Tiền và các khoản tương đương tiền tại t" WITH the figures and
                    # "V điểm đầu kỳ" 7.5pt below, and that suffix is what names the account.
                    # The separator is DISTANCE, not emptiness: a wrapped half sits inside
                    # `Y_TOL * 2` (8pt) while ordinary rows on the same page are 15-32pt apart,
                    # so the proximity guard below is what keeps the NEXT item's label out.
                    take_below = True if self.label_wrap else (not words_)
                    if take_below and i + 1 < len(parsed):
                        # A label sits BELOW its own figures when its box is the taller of the
                        # two — diacritics and descenders push its top down past Y_TOL, leaving
                        # one line with values and no label and the next with the label alone.
                        # The row was then dropped for having an empty key, taking a real figure
                        # with it: ACB's Q1-2024 lost "- Chứng khoán đầu tư 1.003.259" that way
                        # (label 3.3pt below its value against a 3.0pt tolerance), which is
                        # exactly the amount by which its cash breakdown then failed to add up.
                        # Only ever reached when the row would otherwise be discarded, so no row
                        # that already parses can change.
                        ny, nlabel, nvals = parsed[i + 1]
                        if (nlabel and not any(v is not None for v in nvals)
                                and ny - y <= self.Y_TOL * 2):
                            words_ = words_ + nlabel
                            parsed[i + 1] = (ny, [], nvals)     # consumed, not a carry
                    number = words_[0] if words_ and self.NUMBER_RE.match(words_[0]) else ""
                    text = " ".join(words_)
                    key = self.slug(text)
                    if key:
                        out.append(Row(label=text, key=key, number=number, values=vals))
                    carry = []
                    since_code = None
                elif label:
                    carry = (carry + label)[-12:]
                    carry_y = y
                    if since_code is not None:
                        since_code = (since_code + label)[-12:]
                elif self.label_wrap and carry and y - carry_y <= self.CARRY_GAP:
                    # ⚠️ A LINE HOLDING ONLY THE FILING'S OWN ITEM CODE IS NOT A GAP, and
                    # clearing the carry on it TEARS A WRAPPED LABEL IN HALF. The code column
                    # can sit on its own baseline, between the two halves of a label whose
                    # figures are on the second: BID's Q1-2012 cash flow prints
                    #
                    #     Tiền và các khoản tương đương tiền tại      <- carry
                    #     53                                          <- code alone, cleared it
                    #     thời điểm đầu kỳ   48,919,272,456,242 …     <- values
                    #
                    # so the row came out keyed `thoi_diem_dau_ky`, which names no account —
                    # and the suffix it was severed from is the ONLY thing telling the opening
                    # balance from the closing one. Left alone, the carry rejoins its own tail.
                    #
                    # ⚠️ **GENERALISED 2026-08-27 FROM "only the item code" TO "contributed
                    # nothing at all".** This branch is reached only when the line yielded
                    # NEITHER a label NOR a figure, and such a line is not evidence that the
                    # label ended, whatever it holds. BID's Q1-2026 puts the round company
                    # stamp ("BẮT TR", x=594) between the two halves of its closing label —
                    # outside the label zone and outside every value column, so it contributes
                    # nothing and used to clear the carry.
                    #
                    # ⚠️ BOUNDED BY DISTANCE (`CARRY_GAP`), because "contributed nothing" is
                    # also what a genuine GAP between sections looks like. A wrapped half sits
                    # 6-13pt from its own text; past the bound the carry is dropped as before.
                    #
                    # ⚠️ AND IT OPENS A NEW ITEM. Keeping the carry alone is not enough: the
                    # line above may belong to the PREVIOUS item, which printed no figure of
                    # its own — BID Q1-2012's FX adjustment (code 54) is exactly that, and its
                    # label would otherwise bleed into the closing balance below it and push
                    # the discriminating suffix past `slug`'s 60-character cap. So text after
                    # the code is preferred, and the carry is the fallback.
                    since_code = []
                else:
                    carry = []
                    since_code = None
        return out

    # The two summary lines Mẫu CBTT-03's condensed profit-and-loss prints, ascii-normalised
    # and space-stripped, which is the form `norm()` leaves. BOTH are required: "tổng chi phí"
    # alone appears in an operating-expense note.
    CONDENSED_PL = ("tongthunhap", "tongchiphi")

    def condensed_income(self, pages: Dict[int, dict], on: List[int]) -> bool:
        """Do THESE pages print the condensed disclosure P&L (Mẫu CBTT-03)?

        ⚠️ **SCOPED TO THE STATEMENT'S OWN PAGES, AND THAT SCOPE IS THE WHOLE MEASUREMENT.**
        Searched over the whole document instead, the same two words also hit VCB's 44-page
        Q4-2009 filings, where they sit in a note — 4 documents matched, 2 of them wrongly. On
        the income statement's own pages the count is 2 of 1,196 filings, and both are ACB
        condensed forms (Q1-2008, Q3-2009).

        ⚠️ **AND IT IS WHY THE "Mẫu CBTT-03" MARKER IS NOT USED, THOUGH IT LOOKS LIKE THE
        OBVIOUS ONE.** That marker is boilerplate quoting the circular, and a FULL filing quotes
        it too: VIC carries it on the statement pages of eleven 24-32 page filings. One of them,
        Q3-2008, classifies pages [8, 13, 30, 32] as its income statement (8 rows) and [14, 15]
        as its cash flow (**2 rows**) — exactly the junk `MIN_ROWS` exists to reject. Keyed on
        the marker, the floor would have been lowered for it; keyed on the P&L's own wording it
        is not, and a cash flow can never carry this fingerprint at all.
        """
        text = self.norm(" ".join(pages[i]["text"] for i in on)).replace(" ", "")
        return all(n in text for n in self.CONDENSED_PL)

    def unit_of(self, pages: Dict[int, dict], on: List[int]) -> int:
        """×1e6 when the statement is printed in "Triệu VNĐ", else ×1 (plain đồng).

        VCB's 2009 filings are in plain đồng while most are in millions — read the wrong one
        and every figure is out by 10^6 while still reconciling perfectly against itself, since
        the error is uniform. Nothing downstream can catch that, so it must not be decided by
        ONE page: every page of the statement is consulted, because the unit is printed in the
        column header and a continuation page may not repeat it.
        """
        return self.declared_unit(pages, on) or 1

    # ⚠️ **THE THIRD SPELLING IS A LEGACY ENCODING, NOT A TYPO — `LGU-1`, 2026-08-30.** A
    # pre-Unicode filing carries a real text layer in **VNI-Times**, where a tone mark is a
    # SEPARATE character appended after the base vowel and `đ` is the codepoint `ñ`: the page
    # prints "Trieäu ñoàng" for "Triệu đồng". `norm` maps `ä`->`a` and `ñ`->`n` (it strips
    # accents; it does not know VNI), so the same declaration normalises to `trieaunoang` and
    # neither needle above can see it. ACB's Q3-2009 declares its unit that way on BOTH its
    # balance-sheet and its income-statement page, and every figure was therefore read as đồng
    # — a uniform 10^6 error which, as `unit_of`'s docstring says, reconciles perfectly against
    # itself. `sane` refused the balance sheet (`magnitude 1.7e+08 vs typical 1.3e+14`) and had
    # an EMPTY band for the income statement, so that one reached disk with a pre-tax profit of
    # 641,749 **đồng** for a bank holding 169 trillion of assets.
    #
    # ⚠️ **IT IS IN THE DEFAULT PATH AND HAS TO BE**, per the rule §6-2-untricies drew: when the
    # gates cannot see the defect, the repair cannot be an escalation. The statement is ACCEPTED
    # at layer 1 with the wrong unit, so no later layer is ever reached.
    #
    # ⚠️ **MEASURED, NOT GUESSED, IN BOTH DIRECTIONS.** Across the 1,196 filings of the seven
    # parsed tickers, 366 carry a text layer and exactly FOUR carry this spelling — ACB
    # Q1-2007, Q2-2007, Q1-2008 and Q3-2009, of which the first two are before
    # `FINANCIALS_PERIOD_MIN` and are never opened. `trieaunoang` is not a string ordinary
    # Vietnamese normalises to, so it cannot fire by accident.
    #
    # ⚠️ **TCVN3/ABC IS DELIBERATELY ABSENT.** That encoding writes "TriÖu ®ång", which
    # normalises to `triouang`; it has **0 hits** in this corpus, so adding it would be an
    # unmeasured needle (§5 rule 2). Add it when a filing needs it, with the filing named.
    @staticmethod
    def _declares_millions(text: str) -> bool:
        return any(n in text for n in ("trieuvnd", "trieudong", "trieaunoang"))

    # ⚠️ **AND THE OTHER HALF OF THAT SENTENCE — AN EXPLICIT `đồng` — WAS NOT DETECTABLE AT
    # ALL UNTIL 2026-09-04 (`UNP-1`).** `declared_unit` could only ever answer "millions" or
    # "silence", so a page SAYING đồng and a page saying nothing were one answer, and a later
    # page could therefore overrule an earlier one that had stated the unit outright.
    #
    # ⚠️ **MEASURED ON FPT Q3-2024, AND IT IS NOT AN OCR FAILURE.** Its consolidated income
    # statement runs pages 8-9: page 8 is the statement and prints `Đơn vị: VND`, page 9 is
    # the appended "giải trình kết quả kinh doanh" table and prints `ĐVT: Triệu đồng`. Both
    # are read correctly. `declared_unit` scanned for millions across every page of the
    # statement, found page 9, and multiplied the whole statement by 10^6 — so a Q3 revenue
    # of 15,972,397,069,700 was written as 1.597e19 and `OP_IDENTITY` refused the statement.
    # The balance sheet of the SAME filing, whose pages carry only page 8's declaration, took
    # unit=1 and parsed. **One statement, two printed units, and the scan took the wrong one.**
    #
    # ⚠️ **THE NEEDLES ARE ANCHORED TO THE DECLARATION, NEVER TO A BARE `dong`.** `norm` strips
    # punctuation, so "Đơn vị: VND" -> `donvivnd` and "ĐVT: Triệu đồng" -> `dvttrieudong`;
    # matching a bare `dong` would fire on every millions declaration there is. Each needle
    # here is `đơn vị`/`ĐVT` glued to the unit token, which the millions forms cannot produce:
    # "Đơn vị tính: Triệu đồng" normalises to `donvitinhtrieudong`, which contains neither
    # `donvitinhdong` nor `donvidong`. Asserted by test rather than argued.
    @staticmethod
    def _declares_dong(text: str) -> bool:
        return any(n in text for n in ("donvivnd", "donvitinhvnd", "dvtvnd",
                                       "donvidong", "donvitinhdong", "dvtdong"))

    def declared_unit(self, pages: Dict[int, dict], on: List[int]) -> Optional[int]:
        """The unit these pages STATE, or `None` when they state nothing.

        ⚠️ **`unit_of` cannot tell "printed in đồng" from "did not say", and the two must not
        be one answer.** A statement that says nothing is a statement whose unit is UNKNOWN,
        and defaulting it to x1 is a silent 10^6 error that reconciles perfectly against
        itself — `unit_of`'s own docstring says nothing downstream can catch it, and on BID's
        Q1-2026 cash flow only `sane` did (`magnitude 5.45e+08 vs typical 1.19e+14`). Neither
        of that statement's two pages prints "Triệu VNĐ" while the balance sheet in the SAME
        filing does, so the figures are millions and were read as đồng.

        ⚠️ **THE FIRST PAGE THAT SAYS ANYTHING WINS, AND THAT IS THE WHOLE RULE.** The unit is
        printed with the statement's own column header, i.e. on the page where the statement
        STARTS; a declaration further down belongs to whatever table that page carries. Until
        2026-09-04 this scanned every page for `millions` alone and returned on the first hit,
        so an appended note table in millions silently overruled a statement page that had
        said VND (`UNP-1`, above). Reading in page order preserves the case the old rule was
        written for — *"a continuation page may not repeat it"*, i.e. a silent first page and
        a later one that declares — and only changes the verdict where an EARLIER page had
        already stated the unit.
        """
        for i in on:
            text = self.norm(pages[i]["text"]).replace(" ", "")
            # ⚠️ Millions first WITHIN a page, so a page that somehow carries both keeps the
            # answer it had before this change. Across pages the order is the page order.
            if self._declares_millions(text):
                return 1_000_000
            if self._declares_dong(text):
                return 1
        return None

    def document_unit(self, pages: Dict[int, dict]) -> Optional[int]:
        """The unit the FILING declares, from whichever of its statements say so.

        ⚠️ Only consulted for a statement that declares nothing itself (`unit_from_document`),
        and only when the filing is not self-contradictory — if one statement said millions and
        another said đồng outright there would be no document-level answer, and guessing one is
        how a correct statement gets multiplied by a million.

        ⚠️ **THAT CASE IS REACHABLE SINCE 2026-09-04 AND WAS NOT BEFORE.** This docstring used
        to end *"a statement can only declare millions or stay silent, so this returns 1e6 or
        `None`"*; `declared_unit` can now answer 1 as well (`UNP-1`), so a filing whose
        statements disagree returns `None` — the abstention the sentence above always
        promised, now with something able to trigger it. It returns 1e6, 1 or `None`.
        """
        units = {self.declared_unit(pages, [i])
                 for i, pg in pages.items() if pg["kind"] in REPORTS}
        units.discard(None)
        return units.pop() if len(units) == 1 else None

    # ──────────────────────────────────────────────────────────────────────
    # Share capital (from the notes, not a statement)
    # ──────────────────────────────────────────────────────────────────────

    # The three lines of the "Vốn cổ phần đã được duyệt và đã phát hành" note, ascii-normalised
    # (norm() strips accents and collapses spaces). Matched fuzzily because the note is deep in
    # the scanned notes section, where the TCVN3-font text layer garbles worst: "đang lưu hành"
    # comes through as "llfll hanh". "đã phát hành" survives cleanly and is the anchor for the
    # published count; par-value ("mệnh giá") pins the note so a stray big number elsewhere on
    # the page is not mistaken for a share count.
    SHARE_LABELS = {
        "shares_authorized": "von co phan theo giay phep",
        "shares_issued": "co phieu da phat hanh",
        "shares_outstanding": "co phieu dang luu hanh",
    }
    SHARE_NOTE_ANCHOR = "phat hanh cua ngan hang"    # the note's own header tail, survives OCR
    SHARE_LABEL_MATCH = 0.80        # fuzzy floor for a garbled row label
    MIN_SHARE_COUNT = 1_000_000     # a share count is a big integer; smaller values are the
    #                                 million-đồng capital column beside it, not the count
    MAX_SHARE_COUNT = 100_000_000_000   # …and an upper bound: no Vietnamese listing has 100bn
    #   shares. A value above this is two RIGHT-aligned columns OCR merged into one token
    #   ("2665020334826650203" = the share count and the đồng column with no space between), so
    #   it is rejected rather than written as a nonsense count.

    def share_capital(self, doc, after: int = 0) -> Dict[str, Optional[int]]:
        """Read {authorized, issued, outstanding} share counts from the capital note.

        The note lives in the NOTES section, past the three statements, so scan() never reaches
        it. It is scanned here directly, starting just after the last statement page (`after`)
        and stopping at the first page that carries it — one OCR'd page, not the whole tail.

        The share count is the leftmost (current-period) big integer on each labelled row;
        the columns to its right are the prior period and the đồng-value columns.
        """
        from difflib import SequenceMatcher

        out: Dict[str, Optional[int]] = {k: None for k in self.SHARE_LABELS}
        anchor = self.SHARE_NOTE_ANCHOR.replace(" ", "")
        for i in range(after, doc.page_count):
            try:
                page = doc.load_page(i)
            except Exception:
                continue
            native = page.get_text()
            text, words = self._ocr_page(page, native)
            ns = self.norm(text).replace(" ", "")
            # a coarse gate first: the note names the bank's own issued shares
            if anchor not in ns and "cophieudaphathanh" not in ns:
                continue

            lines: Dict[float, list] = {}
            for w in words:
                k = next((k for k in lines if abs(k - w[1]) <= self.Y_TOL), w[1])
                lines.setdefault(k, []).append(w)
            ys = sorted(lines)

            def first_count(ws: list) -> Optional[int]:
                for w in sorted(ws, key=lambda w: w[0]):
                    v = self.parse_num(w[4]) if self.NUM_RE.match(w[4]) else None
                    if v is not None and self.MIN_SHARE_COUNT <= abs(v) <= self.MAX_SHARE_COUNT:
                        return v
                return None

            for field_name, label in self.SHARE_LABELS.items():
                flat = label.replace(" ", "")
                for idx, y in enumerate(ys):
                    lab = self.norm(
                        " ".join(w[4] for w in sorted(lines[y], key=lambda w: w[0]))
                    ).replace(" ", "")
                    if not lab:
                        continue
                    # slide the label along the line so leading numbering/noise doesn't defeat it
                    hit = flat in lab
                    if not hit:
                        w_ = len(flat)
                        for j in range(0, max(1, len(lab) - w_ + 1)):
                            if SequenceMatcher(None, flat, lab[j:j + w_]).ratio() >= \
                                    self.SHARE_LABEL_MATCH:
                                hit = True
                                break
                    if not hit:
                        continue
                    # the count is on this line, or the next value-bearing line (the "issued"
                    # heading sits above a "Cổ phiếu phổ thông" line that carries the figures)
                    v = first_count(lines[y])
                    if v is None:
                        for yy in ys[idx + 1:idx + 3]:
                            v = first_count(lines[yy])
                            if v is not None:
                                break
                    if v is not None:
                        out[field_name] = v
                    break

            # outstanding often garbles below the fuzzy floor ("llfll hanh"); when the note was
            # found and issued read but outstanding did not, they are equal for a company with no
            # treasury shares — which VCB is — so fall back to the issued count.
            if out["shares_issued"] is not None and out["shares_outstanding"] is None:
                out["shares_outstanding"] = out["shares_issued"]
            if any(v is not None for v in out.values()):
                return out
        return out

    # ──────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────

    def publish_date(self, pages: Dict[int, dict],
                     period_end: Optional[date] = None) -> str:
        """The date the filing was signed off — ISO, or "" if it prints none.

        A report is signed AFTER every date it reports on, so the signing date is the LATEST
        date printed in it that falls after the period end. Taking the first date instead would
        pick up the period itself ("tại ngày 31 tháng 12 năm 2024"), and taking the maximum
        without a floor would pick up a comparative period from years earlier.

        This is what makes the fundamentals point-in-time safe: a figure is not knowable until
        the document carrying it was published, and joining on the period end instead leaks
        months of look-ahead.
        """
        hits: List[date] = []
        for i in sorted(pages):
            for m in self.SIGN_RE.finditer(pages[i]["text"]):
                try:
                    d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                except ValueError:
                    continue
                if period_end is None or period_end < d <= period_end + timedelta(days=400):
                    hits.append(d)
        return max(hits).isoformat() if hits else ""

    TAIL_PAGES = 4          # how far into the end of a filing to hunt for its signing date

    def _tail_date(self, doc, period_end: Optional[date]) -> str:
        """Look for the signing date at the END of the filing.

        Older reports do not sign under the statements at all — they approve the whole thing in
        the last note ("28. Phê duyệt báo cáo tài chính giữa niên độ … được Ban Điều hành phê
        duyệt vào ngày 20 tháng 10 năm 2009"). The page scan stops at the notes, for speed, so
        that page is never read and the quarter ends up with no date at all.
        """
        pages: Dict[int, dict] = {}
        for i in range(max(0, doc.page_count - self.TAIL_PAGES), doc.page_count):
            try:
                page = doc.load_page(i)
            except Exception:
                continue
            native = page.get_text()
            try:
                native, _ = self._ocr_page(page, native)
            except Exception:
                continue
            pages[i] = {"text": native}
        return self.publish_date(pages, period_end)

    def parse(self, pdf_path: str,
              period_end: Optional[date] = None,
              want_shares: bool = True) -> Dict[str, Statement]:
        """-> {report: Statement} for whichever of the three statements the filing contains.

        Every statement carries the filing's `publish_date` — the same document produced them
        all, so they share it.

        ⚠️ **`want_shares=False` SKIPS THE CAPITAL-NOTE SCAN AND LEAVES THE THREE COUNTS
        `None`.** That scan walks from the last statement page to the END of the filing
        (`share_capital`) and is the single most expensive thing here — 50 pages / 84.8 s on
        BID's FY-2016 annual, 68.8 % of one `parse()`, returning nothing. `_parse_cascaded`
        reads the counts only while the document's facts are still open, so every layer after
        the first one that produced a statement was paying for a value that is discarded.
        The DEFAULT is True, so a caller that has not thought about it keeps today's
        behaviour.
        """
        import fitz

        self._use_document(pdf_path)
        doc = fitz.open(pdf_path)
        try:
            pages = self.scan(doc)
            published = (self.publish_date(pages, period_end)
                         or self._tail_date(doc, period_end))
            # The share-capital note sits in the notes, past the last statement page. Scan from
            # there so we OCR one note page, not the whole tail. A per-document fact, shared by
            # all three statements — like publish_date.
            last_stmt = max((i for i, p in pages.items() if p["kind"] in REPORTS), default=-1)
            shares = (self.share_capital(doc, after=last_stmt + 1) if want_shares
                      else {k: None for k in self.SHARE_LABELS})
            out: Dict[str, Statement] = {}
            for report in REPORTS:
                on = sorted(i for i, p in pages.items() if p["kind"] == report)
                if not on:
                    continue
                words_by_page = {i: pages[i]["words"] for i in on}
                width = pages[on[0]]["width"]
                columns = self.value_columns(words_by_page, width)
                if not columns:
                    continue
                unit = self.unit_of(pages, on)
                if self.unit_from_document and self.declared_unit(pages, on) is None:
                    unit = self.document_unit(pages) or unit
                # Read the COLUMN HEADINGS, not the column count: 4 columns can equally be a
                # note reference plus an over-segmented pair. The words "quý" and "lũy kế"
                # appearing together in the header is the filing stating outright that it prints
                # both, which is the only thing that licenses skipping de-cumulation.
                qcol = (report == INCOME_STATEMENT
                        and self._prints_quarter_column(pages[on[0]]["text"]))
                rows = self.table_rows(words_by_page, columns)
                # scale here, once: the values leave the parser in đồng
                for r in rows:
                    r.values = [None if v is None else v * unit for v in r.values]
                out[report] = Statement(report=report, pages=[i + 1 for i in on],
                                        unit=unit, n_columns=len(columns), rows=rows,
                                        quarter_column=qcol,
                                        condensed_income=(report == INCOME_STATEMENT
                                                          and self.condensed_income(pages, on)),
                                        split_figures=self.split_figures(words_by_page, width),
                                        publish_date=published,
                                        shares_authorized=shares["shares_authorized"],
                                        shares_issued=shares["shares_issued"],
                                        shares_outstanding=shares["shares_outstanding"])
            return out
        finally:
            doc.close()
