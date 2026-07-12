# src\web_scraper\cafef_pdf_parser.py

# ===== Standard Library =====
import os
import re
import unicodedata
from dataclasses import dataclass, field
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

    def find(self, *needles: str) -> Optional[int]:
        """First column-0 value whose key contains any of `needles` (spaces stripped)."""
        for r in self.rows:
            k = r.key.replace("_", "")
            if any(n.replace(" ", "").replace("_", "") in k for n in needles):
                if r.values and r.values[0] is not None:
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
    FORM_RE = re.compile(
        r"(?:M[ẫâa]u\s*(?:s[ốô]\s*)?[:.]?\s*)?\b(B\s*\d{2})\s*[a-z]?\s*[-/]?\s*(TCTD|DN)\b",
        re.I)
    FORMS = {
        "TCTD": {"B02": BALANCE_SHEET, "B03": INCOME_STATEMENT,
                 "B04": CASH_FLOW, "B05": NOTES},
        "DN": {"B01": BALANCE_SHEET, "B02": INCOME_STATEMENT,
               "B03": CASH_FLOW, "B09": NOTES},
    }
    # Fallback only, for a filing that prints no form code. Matched against text with accents
    # stripped AND spaces removed, so OCR's missing spaces cannot defeat it.
    HEADING = {
        BALANCE_SHEET: ["bangcandoiketoan", "baocaotinhhinhtaichinh"],
        INCOME_STATEMENT: ["ketquahoatdongkinhdoanh", "baocaoketquakinhdoanh"],
        CASH_FLOW: ["luuchuyentiente"],
    }
    NOTES_NS = "thuyetminhbaocao"

    NUM_RE = re.compile(r"^\(?-?[\d][\d.,]*\)?$|^[-–—]$")
    # The filing's own row numbering, in the left margin.
    NUMBER_RE = re.compile(r"^[IVXLC]+$|^\d{1,2}$|^[a-z]$|^[A-Z]$", re.I)

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
        """-> (which statement this page is, whether a FORM CODE said so)."""
        m = self.FORM_RE.search(text)
        if m:
            code = re.sub(r"\s+", "", m.group(1)).upper()
            kind = self.FORMS[m.group(2).upper()].get(code)
            if kind:
                return kind, True
        ns = self.norm(text).replace(" ", "")
        if self.NOTES_NS in ns:
            return NOTES, False
        for report, needles in self.HEADING.items():
            if any(n in ns for n in needles):
                return report, False
        return None, False

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

        # A form code is definitive; a heading match is a guess. The auditor's report at the
        # front names every statement, so heading matching tags those pages as statements and
        # drags audit prose into the table. Once any form code is seen, trust only form codes.
        if any(p["kind"] and p["from_form"] for p in pages.values()):
            for p in pages.values():
                if not p["from_form"]:
                    p["kind"] = None
        return pages

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
        and every figure is out by 10^6 while still reconciling perfectly against itself.
        """
        ns = self.norm(pages[on[0]]["text"]).replace(" ", "")
        return 1_000_000 if ("trieuvnd" in ns or "trieudong" in ns) else 1

    # ──────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────

    def parse(self, pdf_path: str) -> Dict[str, Statement]:
        """-> {report: Statement} for whichever of the three statements the filing contains."""
        import fitz

        doc = fitz.open(pdf_path)
        try:
            pages = self.scan(doc)
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
                                        unit=unit, n_columns=len(columns), rows=rows)
            return out
        finally:
            doc.close()
