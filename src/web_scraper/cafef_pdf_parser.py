# src\web_scraper\cafef_pdf_parser.py

# ===== Standard Library =====
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional

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

    @staticmethod
    def _first_value(values: List[Optional[int]]) -> Optional[int]:
        """The current-period figure = the FIRST populated column, not literally column 0.

        Columns are left-to-right (current period first), so the leftmost non-None value is
        the current period. It is NOT always index 0: OCR sometimes over-segments the columns
        — the note-reference number on the left of the row can cluster as its own spurious
        column, pushing the real value into index 1. ACB's Q2-2010 balance sheet parsed its
        grand total as [None, 176999825…, None, 167881047…, None] — 5 columns where there are
        2 — so a strict `values[0]` read it as empty and the statement was rejected for "no
        total assets" even though the figure was parsed correctly.
        """
        return next((v for v in values if v is not None), None) if values else None

    def find(self, *needles: str) -> Optional[int]:
        """The current-period value on a row whose name is (or is close to) one of `needles`.

        The match must tolerate OCR damage. The lines reconciliation depends on are exactly
        the ones it mangles — VCB's Q4-2021 balance sheet reads "TỔNG NỢ PHẢI TRẢ" as
        `tong_nuphai_tra` (ợ -> u) and the grand total as `toong_nophai_thava_von_chusohuu`.
        On an exact match both are invisible, and a statement that is complete and balanced
        (total assets == total resources, to the dong) gets rejected for "no total".

        Rows are scanned in statement order and the first hit wins, which is what keeps
        "tổng nợ phải trả" from being answered by the grand total that contains it as a
        substring — the liabilities line always precedes it.
        """
        from difflib import SequenceMatcher

        flat = [n.replace(" ", "").replace("_", "") for n in needles]
        for r in self.rows:
            v0 = self._first_value(r.values)
            if v0 is None:
                continue
            k = r.key.replace("_", "")
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
    Y_TOL = 3.0            # words within this many points share a row
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
        self.ocr_ready = self._init_ocr()

    def set_dpi(self, dpi: int) -> None:
        """Re-point this parser at a new render DPI, reusing the loaded OCR models. The onnx
        engine renders at this DPI; the Tesseract path passes it to `get_textpage_ocr`."""
        self.dpi = dpi
        if self._onnx is not None:
            self._onnx.dpi = dpi

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

    @classmethod
    def parse_num(cls, t: str) -> Optional[int]:
        t = (t or "").strip()
        if t in ("-", "–", "—"):
            return 0
        neg = t.startswith("(") and t.endswith(")")
        t = t.strip("()")
        if not re.fullmatch(r"-?[\d.,]+", t):
            return None
        d = re.sub(r"[.,]", "", t)
        if not d.lstrip("-").isdigit() or not d.strip("-"):
            return None
        return -int(d) if neg else int(d)

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
        codes = {re.sub(r"\s+", "", m.group(1)).upper()
                 for m in self.FORM_RE.finditer(text)}
        if len(codes) > 1:
            return None, False

        m = self.FORM_RE.search(text)
        if m:
            code = re.sub(r"\s+", "", m.group(1)).upper()
            kind = self.FORMS[m.group(2).upper()].get(code)
            if kind:
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

    @classmethod
    def _split_number_runs(cls, words: list) -> list:
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
            return text, self._split_number_runs(words)
        if self.engine == "easyocr":
            text, words = self._ocr_page_easyocr(page)
            return text, self._split_number_runs(words)

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
        """
        for report in REPORTS:
            owned = sorted(i for i in pages if pages[i]["kind"] == report)
            anchors = [i for i in owned if pages[i]["from_form"]]
            if not anchors or not owned:
                continue
            lo, hi = min(anchors), max(anchors)
            for i in owned:
                if i < lo - 1 or i > hi + 1:      # not touching the form-coded run
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
            else:
                run = None                      # a gap ends the run

    def _is_table(self, page: dict) -> bool:
        return len(self._numbers(page["words"])) >= self.MIN_TABLE_WORDS

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
        return kept or cols          # never leave the caller with nothing to parse

    # A note reference is 1-2 digits ("Thuyết minh 4", "…21"); a Triệu-VND figure is 4-9. The
    # median digit-count of a column's numbers separates them with room to spare.
    NOTE_MAX_DIGITS = 2

    def table_rows(self, words_by_page: Dict[int, list], columns: List[float]) -> List[Row]:
        """Rows rebuilt from word coordinates.

        A number is assigned to the column whose right edge it lines up with, so a row with a
        missing figure keeps its remaining values in the right columns instead of shifting
        them left.
        """
        if not columns:
            return []
        lo = columns[0] - self.LABEL_GAP        # labels live to the left of column 1

        out: List[Row] = []
        for page in sorted(words_by_page):
            lines: Dict[float, list] = {}
            for w in words_by_page[page]:
                k = next((k for k in lines if abs(k - w[1]) <= self.Y_TOL), w[1])
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
            for i, (y, label, vals) in enumerate(parsed):
                if any(v is not None for v in vals):
                    words_ = carry + label
                    if not words_ and i + 1 < len(parsed):
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
                            words_ = nlabel
                            parsed[i + 1] = (ny, [], nvals)     # consumed, not a carry
                    number = words_[0] if words_ and self.NUMBER_RE.match(words_[0]) else ""
                    text = " ".join(words_)
                    key = self.slug(text)
                    if key:
                        out.append(Row(label=text, key=key, number=number, values=vals))
                    carry = []
                elif label:
                    carry = (carry + label)[-12:]
                else:
                    carry = []
        return out

    def unit_of(self, pages: Dict[int, dict], on: List[int]) -> int:
        """×1e6 when the statement is printed in "Triệu VNĐ", else ×1 (plain đồng).

        VCB's 2009 filings are in plain đồng while most are in millions — read the wrong one
        and every figure is out by 10^6 while still reconciling perfectly against itself, since
        the error is uniform. Nothing downstream can catch that, so it must not be decided by
        ONE page: every page of the statement is consulted, because the unit is printed in the
        column header and a continuation page may not repeat it.
        """
        for i in on:
            ns = self.norm(pages[i]["text"]).replace(" ", "")
            if "trieuvnd" in ns or "trieudong" in ns:
                return 1_000_000
        return 1

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
                rows = self.table_rows(words_by_page, columns)
                # scale here, once: the values leave the parser in đồng
                for r in rows:
                    r.values = [None if v is None else v * unit for v in r.values]
                out[report] = Statement(report=report, pages=[i + 1 for i in on],
                                        unit=unit, n_columns=len(columns), rows=rows,
                                        publish_date=published,
                                        shares_authorized=shares["shares_authorized"],
                                        shares_issued=shares["shares_issued"],
                                        shares_outstanding=shares["shares_outstanding"])
            return out
        finally:
            doc.close()
