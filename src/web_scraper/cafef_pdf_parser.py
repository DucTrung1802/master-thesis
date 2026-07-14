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

    def find(self, *needles: str) -> Optional[int]:
        """First column-0 value on a row whose name is (or is close to) one of `needles`.

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
            if not r.values or r.values[0] is None:
                continue
            k = r.key.replace("_", "")
            for n in flat:
                if n in k:
                    return r.values[0]
                w = len(n)
                for i in range(0, max(1, len(k) - w + 1)):
                    if SequenceMatcher(None, n, k[i:i + w]).ratio() >= self.NAME_MATCH:
                        return r.values[0]
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

    def __init__(self, logger=None):
        self._logger = logger
        self.ocr_ready = self._init_ocr()

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger.log_info(msg)

    def _init_ocr(self) -> bool:
        if os.path.isdir(TESSERACT_DIR) and TESSERACT_DIR not in os.environ.get("PATH", ""):
            os.environ["PATH"] = TESSERACT_DIR + os.pathsep + os.environ.get("PATH", "")
        if TESSDATA_DIR:
            os.environ.setdefault("TESSDATA_PREFIX", TESSDATA_DIR)
        return os.path.isfile(os.path.join(TESSDATA_DIR, f"{OCR_LANG}.traineddata"))

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
        for report, needles in self.HEADING.items():
            if self._titled(ns, needles):
                return report, False
        return None, False

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
            need_ocr = self.ocr_ready and len(native.strip()) < self.MIN_PAGE_TEXT
            tp = (page.get_textpage_ocr(language=OCR_LANG, dpi=self.OCR_DPI, full=True,
                                        tessdata=TESSDATA_DIR) if need_ocr else None)
            text = page.get_text(textpage=tp) if tp else native
            words = page.get_text("words", textpage=tp) if tp else page.get_text("words")
            if tp:
                words = self._to_visual(page, words)

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
        return [sum(c) / len(c) for c in keep][:5]

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

            carry: List[str] = []               # a label that wrapped onto its own line
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

                if any(v is not None for v in vals):
                    words_ = carry + label
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
            if self.ocr_ready and len(native.strip()) < self.MIN_PAGE_TEXT:
                try:
                    tp = page.get_textpage_ocr(language=OCR_LANG, dpi=self.OCR_DPI,
                                               full=True, tessdata=TESSDATA_DIR)
                    native = page.get_text(textpage=tp)
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
                                        publish_date=published)
            return out
        finally:
            doc.close()
