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
    HEADING = {
        BALANCE_SHEET: ["bangcandoiketoan", "baocaotinhhinhtaichinh"],
        INCOME_STATEMENT: ["ketquahoatdongkinhdoanh", "baocaoketquakinhdoanh"],
        CASH_FLOW: ["luuchuyentiente"],
    }
    NOTES_NS = "thuyetminhbaocao"

    # The auditor's report at the front of every filing. It is NOT a statement, but its header
    # says "Báo cáo tài chính hợp nhất…", which is close enough to the balance sheet's own
    # title ("Báo cáo TÌNH HÌNH tài chính") to fool a fuzzy match — and once it does, the
    # contiguity fill drags the whole audit section into the table. A page that announces
    # itself as a review or an audit opinion is never a statement.
    AUDIT_NS = ("baocaosoatxet", "soatxetthongtintaichinh", "baocaokiemtoandoclap",
                "baocaocuakiemtoanvien", "ykienkiemtoan")

    HEADER_LINES = 12       # the page header: company, form code, statement title, period
    TITLE_MATCH = 0.80      # how close an OCR'd title must be to count as that statement
    MIN_TABLE_WORDS = 15    # a page with fewer figures than this is not a statement page

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
        CASH_FLOW: ("tienvacackhoantuongduongtien",),
    }
    # A tail page still has to be a TABLE, just a small one: the three cash-balance rows carry
    # two period columns each. Set at 4 so a page holding a single stray figure — a page
    # number, a date — can never qualify.
    MIN_TAIL_WORDS = 4

    # How far below the last line that fed it a pending wrapped label survives a line that
    # contributed nothing. Rows on these statements are set 13-32pt apart and a wrapped half
    # sits 6-13pt from its own text, so 24pt admits one intervening noise line and no more.
    # `label_wrap` only.
    CARRY_GAP = 24.0

    NUM_RE = re.compile(r"^\(?-?[\d][\d.,]*\)?$|^[-–—]$")
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
        # set per PARSE LAYER; see table_rows' carry
        self.label_wrap = False
        # set per PARSE LAYER; see document_unit
        self.unit_from_document = False
        # ⚠️ **PROGRESS HOOK, AND THE ONLY DENOMINATOR IN THIS FILE THAT PREDICTS TIME.**
        # `on_page(index, total)` is called once per page of `scan`, before it is read. Pages
        # of one document cost roughly the same (0.87 s/page at onnx@200, measured for `P41`),
        # so a page fraction really is a fraction of the WORK — where a document count and a
        # layer index are positions in a list of wildly unequal items. `None` = no reporting,
        # which is every caller but `pdf_ocr_job`.
        self.on_page = None
        self.ocr_ready = self._init_ocr()

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

    def set_label_wrap(self, on: bool) -> None:
        """Reassemble a label that WRAPPED AROUND its own value line.

        `table_rows` builds a row's label from the lines ABOVE the figures. That is right when
        the label wraps upward, and wrong when the figures sit BETWEEN the label's two halves —
        the second half is then either discarded (it becomes the next row's carry) or, worse,
        the first half is discarded before it and the row is keyed on the fragment. Both halves
        of that failure are repaired here; see `table_rows`.
        """
        self.label_wrap = bool(on)

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
        if self._titled(ns, [self.NOTES_NS]):
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
    def _split_number_runs(cls, words: list, join_split: bool = False) -> list:
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
            txt = w[4]
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
        native = self._page_content_text(page, native)
        need_ocr = self.ocr_ready and (
            len(native.strip()) < self.MIN_PAGE_TEXT or self._native_garbled(native))
        if not need_ocr:
            return native, page.get_text("words")

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
            return text, self._split_number_runs(words, self.join_split_digits)
        if self.engine == "easyocr":
            text, words = self._ocr_page_easyocr(page)
            return text, self._split_number_runs(words, self.join_split_digits)

        tp = page.get_textpage_ocr(language=OCR_LANG, dpi=self.dpi, full=True,
                                   tessdata=TESSDATA_DIR)
        text = page.get_text(textpage=tp)
        words = self._to_visual(page, page.get_text("words", textpage=tp))
        return text, words

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
            if start < floor:                 # out of order -> not this statement
                for i in list(pages):
                    if pages[i]["kind"] == report and i < floor and not pages[i]["from_form"]:
                        pages[i]["kind"] = None
                start = min((i for i in pages
                             if pages[i]["kind"] == report), default=None)
                if start is None:
                    continue
            floor = start

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
        for i in sorted(pages):
            kind = pages[i]["kind"]
            if kind in REPORTS:
                run = kind
                continue
            if kind == NOTES:
                run = None                      # the statements are over
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
        for words in words_by_page.values():
            for w in words:
                ns = self.norm(w[4]).replace(" ", "")
                if not ns or SequenceMatcher(
                        None, self.CODE_HEADER_NS, ns).ratio() < self.CODE_HEADER_MATCH:
                    continue
                if w[0] - self.EDGE_TOL <= leftmost <= w[2] + self.EDGE_TOL:
                    return leftmost
        return None

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
            for w in words_by_page[page]:
                y = w[1] + (offset if (offset and w[2] >= lo
                                       and self.NUM_RE.match(w[4]) is not None
                                       and self.parse_num(w[4]) is not None) else 0.0)
                k = next((k for k in lines if abs(k - y) <= self.Y_TOL), y)
                lines.setdefault(k, []).append(w)

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

    def unit_of(self, pages: Dict[int, dict], on: List[int]) -> int:
        """×1e6 when the statement is printed in "Triệu VNĐ", else ×1 (plain đồng).

        VCB's 2009 filings are in plain đồng while most are in millions — read the wrong one
        and every figure is out by 10^6 while still reconciling perfectly against itself, since
        the error is uniform. Nothing downstream can catch that, so it must not be decided by
        ONE page: every page of the statement is consulted, because the unit is printed in the
        column header and a continuation page may not repeat it.
        """
        return self.declared_unit(pages, on) or 1

    @staticmethod
    def _declares_millions(text: str) -> bool:
        return "trieuvnd" in text or "trieudong" in text

    def declared_unit(self, pages: Dict[int, dict], on: List[int]) -> Optional[int]:
        """The unit these pages STATE, or `None` when they state nothing.

        ⚠️ **`unit_of` cannot tell "printed in đồng" from "did not say", and the two must not
        be one answer.** A statement that says nothing is a statement whose unit is UNKNOWN,
        and defaulting it to x1 is a silent 10^6 error that reconciles perfectly against
        itself — `unit_of`'s own docstring says nothing downstream can catch it, and on BID's
        Q1-2026 cash flow only `sane` did (`magnitude 5.45e+08 vs typical 1.19e+14`). Neither
        of that statement's two pages prints "Triệu VNĐ" while the balance sheet in the SAME
        filing does, so the figures are millions and were read as đồng.
        """
        for i in on:
            if self._declares_millions(self.norm(pages[i]["text"]).replace(" ", "")):
                return 1_000_000
        return None

    def document_unit(self, pages: Dict[int, dict]) -> Optional[int]:
        """The unit the FILING declares, from whichever of its statements say so.

        ⚠️ Only consulted for a statement that declares nothing itself (`unit_from_document`),
        and only when the filing is not self-contradictory — if one statement said millions and
        another said đồng outright there would be no document-level answer, and guessing one is
        how a correct statement gets multiplied by a million. As it stands a statement can only
        declare millions or stay silent, so this returns 1e6 or `None`.
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
              period_end: Optional[date] = None) -> Dict[str, Statement]:
        """-> {report: Statement} for whichever of the three statements the filing contains.

        Every statement carries the filing's `publish_date` — the same document produced them
        all, so they share it.
        """
        import fitz

        doc = fitz.open(pdf_path)
        try:
            pages = self.scan(doc)
            published = (self.publish_date(pages, period_end)
                         or self._tail_date(doc, period_end))
            # The share-capital note sits in the notes, past the last statement page. Scan from
            # there so we OCR one note page, not the whole tail. A per-document fact, shared by
            # all three statements — like publish_date.
            last_stmt = max((i for i, p in pages.items() if p["kind"] in REPORTS), default=-1)
            shares = self.share_capital(doc, after=last_stmt + 1)
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
                                        split_figures=self.split_figures(words_by_page, width),
                                        publish_date=published,
                                        shares_authorized=shares["shares_authorized"],
                                        shares_issued=shares["shares_issued"],
                                        shares_outstanding=shares["shares_outstanding"])
            return out
        finally:
            doc.close()
