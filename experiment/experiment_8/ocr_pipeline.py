"""
Everything in experiments 8 and 9 that is NOT the OCR.

Both experiments point a different Vietnamese OCR stack at the same scanned filing:

    experiment_8   PaddleOCR-DB detection  +  VietOCR vgg_transformer   (bmd1905/vietnamese-ocr)
    experiment_9   DeepDoc ONNX detection  +  VietOCR vgg_seq2seq       (hoaivannguyen/deepdoc_vietocr)

If each also built its own table reconstruction, a difference in the final statements would say
nothing about the OCR. So the downstream half lives HERE, once, and experiment_9 imports it from
experiment_8 — the two runs differ in exactly one component, the engine that turns page pixels
into positioned words.

The downstream half is not new code either: it is `src/web_scraper/cafef_pdf_parser.PdfParser`
and `cafef_financials.FinancialsBuilder`, the parser already in production. An engine here is a
drop-in replacement for its Tesseract seam (`_ocr_page`), so what these experiments measure is
"what would the existing pipeline produce if the OCR were better?" — which is the only question
worth asking, since the pipeline's own row/column/schema logic is already tuned and validated.

An ENGINE is any object with

    read_page(page) -> (text, words)

where `page` is a PyMuPDF page, `text` is the page in reading order (the page classifier reads
it) and `words` are `(x0, y0, x1, y1, text, block, line, n)` tuples in **visual pdf-point**
space — the same shape PyMuPDF's own `get_text("words")` returns, because that is what the row
builder consumes.
"""

# ===== Standard Library =====
import csv
import os
import sys
from typing import Dict, List, Optional, Sequence, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# ===== Local / Custom Modules =====
from web_scraper.cafef_pdf_parser import (  # noqa: E402
    BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, REPORTS, PdfParser, Statement,
)
from web_scraper import cafef_financials  # noqa: E402
from web_scraper.cafef_financials import FinancialsBuilder  # noqa: E402

FINANCIALS_DIR = os.path.join(REPO_ROOT, "raw_data", "cafef", "financials")
STATEMENTS_DIR = os.path.join(FINANCIALS_DIR, "statements")

# `utils.constants.RAW_DATA_DIR` is the relative string "raw_data", so every path in
# cafef_financials is relative to the WORKING DIRECTORY — fine for `python src/main.py` at the
# repo root, silently empty when run from an experiment folder. `schema_of` then returns no
# columns, `map_to_schema` maps nothing onto them, and the run reports a statement that parsed
# 63 rows and produced 0 values, which looks like an OCR failure and is not one. Re-anchoring
# them here keeps the experiment runnable from its own directory.
cafef_financials.SCHEMA_DIR = os.path.join(FINANCIALS_DIR, "schema")
cafef_financials.STATEMENTS_DIR = STATEMENTS_DIR
cafef_financials.TEMPLATES_INDEX = os.path.join(FINANCIALS_DIR, "templates.csv")
cafef_financials.PDFS_DIR = os.path.join(REPO_ROOT, "raw_data", "cafef", "pdfs")


# ──────────────────────────────────────────────────────────────────────────────
# Rasterising a page for an OCR engine
# ──────────────────────────────────────────────────────────────────────────────

def page_raster(page, dpi: int = 200):
    """(RGB ndarray, scale) of the page as a HUMAN sees it.

    `prerotate(page.rotation)` is the whole point. These scans are stored `/Rotate 180`: without
    it the raster is upside-down and OCR returns gibberish, and clearing the rotation instead
    hands back word boxes mirrored in unrotated space, which inverts the parser's premise that
    labels are left and figures are right (see `PdfParser._to_visual`). Rasterising in visual
    space means the pixel coordinates ARE visual coordinates — dividing by `scale` is the only
    conversion needed.
    """
    import fitz
    import numpy as np

    scale = dpi / 72.0
    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale).prerotate(page.rotation))
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    return img[:, :, :3].copy(), scale


def to_words(boxes: Sequence[Tuple[float, float, float, float, str]], scale: float) -> list:
    """Pixel-space OCR boxes -> PyMuPDF-shaped word tuples in pdf points."""
    out = []
    for i, (x0, y0, x1, y1, text) in enumerate(boxes):
        text = (text or "").strip()
        if not text:
            continue
        out.append((x0 / scale, y0 / scale, x1 / scale, y1 / scale, text, 0, 0, i))
    return out


def reading_order_text(words: Sequence[tuple], y_tol: float = 4.0) -> str:
    """The page as text, top-to-bottom then left-to-right.

    `PdfParser._page_kind` reads only the first `HEADER_LINES` lines of this to decide which
    statement a page is, so the line grouping matters as much as the words: detection boxes come
    back in the detector's own order, and a page whose header lands anywhere but the top of the
    string is a page whose form code and title are invisible.
    """
    lines: Dict[float, list] = {}
    for w in sorted(words, key=lambda w: (w[1], w[0])):
        key = next((k for k in lines if abs(k - w[1]) <= y_tol), w[1])
        lines.setdefault(key, []).append(w)
    return "\n".join(" ".join(w[4] for w in sorted(lines[y], key=lambda w: w[0]))
                     for y in sorted(lines))


class CachingEngine:
    """An engine that remembers what it read.

    OCR is the slow half by two orders of magnitude — a 16-page filing is ~20 minutes — and the
    table logic downstream is where the iteration actually happens (which page is which
    statement, where the period columns are, what maps onto the chart of accounts). Caching the
    words lets that half be re-run in a second, on byte-identical input, which also makes any
    change to it attributable: if the numbers move, the change moved them.

    The cache is keyed on everything that would change the pixels or the reading of them, so a
    different DPI or a different engine cannot silently reuse the wrong words.
    """

    def __init__(self, engine, path: str, key: str):
        self.engine = engine
        self.path = path
        self.key = key
        # Wall clock of the OCR alone. The run's own elapsed time stops meaning anything the
        # moment a cache exists — a cached re-run finishes in under a second — so the cost of
        # each engine is measured where it is actually paid.
        self.ocr_seconds = 0.0
        self.ocr_pages = 0
        self.cached_pages = 0
        self.data = {"key": key, "pages": {}}
        if os.path.exists(path):
            import json
            with open(path, encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get("key") == key:
                self.data = cached

    def read_page(self, page):
        import time

        n = str(page.number)
        hit = self.data["pages"].get(n)
        if hit is not None:
            self.cached_pages += 1
            # The per-page OCR time is stored WITH the page, so a re-score from cache still
            # reports the true OCR cost of each engine — the number that decides adoption survives
            # even though no OCR ran this time.
            self.ocr_seconds += hit.get("sec", 0.0)
            self.ocr_pages += 1 if hit.get("sec") else 0
            words = [tuple(w[:4]) + (w[4], 0, 0, i) for i, w in enumerate(hit["words"])]
            return hit["text"], words

        t0 = time.time()
        text, words = self.engine.read_page(page)
        sec = time.time() - t0
        self.ocr_seconds += sec
        self.ocr_pages += 1
        self.data["pages"][n] = {"text": text, "sec": round(sec, 3),
                                 "words": [[w[0], w[1], w[2], w[3], w[4]] for w in words]}
        self.save()
        return text, words

    def save(self) -> None:
        import json

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False)
        os.replace(tmp, self.path)          # a killed run never leaves half a cache


# ──────────────────────────────────────────────────────────────────────────────
# The production parser, with its OCR seam replaced
# ──────────────────────────────────────────────────────────────────────────────

class OcrPdfParser(PdfParser):
    """`PdfParser` reading every page through the supplied engine.

    Two deliberate differences from the base class:

    * `_init_ocr` never looks for Tesseract — the engine IS the OCR.
    * `_ocr_page` OCRs UNCONDITIONALLY. The base class skips OCR when a page has a usable native
      text layer, and this filing is precisely the case where that gate misfires: ACB's 2013
      report carries a legacy-font text layer that is long enough to look real and is pure
      mojibake ("Bảng cân đối kế toán" comes back as "Bine can ddi k6loAn"), with its FIGURES
      damaged too ("151.161" -> "t5l.l6l"). `_native_garbled` catches the worst of that family at
      a ≤2-char-token fraction of 0.40; these pages sit at 0.16-0.24 and sail through it. Since
      the point of the experiment is to read the pixels, there is nothing to gate.
    """

    def __init__(self, engine, logger=None, verbose: bool = True):
        self.engine = engine
        self.verbose = verbose
        self.page_texts: Dict[int, str] = {}
        super().__init__(logger)

    def _init_ocr(self) -> bool:
        return True

    def _page_kind(self, text: str):
        """As the base class, except that a table of CONTENTS is not a statement.

        A filing opens with a "NỘI DUNG" page listing every statement it contains, form code and
        all — "Bảng cân đối kế toán hợp nhất (Mẫu B02/TCTD-HN)" on one line, B03 on the next,
        B04 below that. The base classifier trusts a form code absolutely (it is the one signal
        OCR cannot fake), so that page came back as the balance sheet, `_drop_islands` then
        treated it as a legitimate anchor six pages from the real statement and kept everything
        between them, and its PAGE NUMBERS (3, 4, 9…) joined the figures being clustered into
        period columns.

        The tell is unambiguous and needs no threshold: a statement carries ONE form code, its
        own. Two or more on a page means the page is talking ABOUT the statements.
        """
        import re as _re

        codes = {_re.sub(r"\s+", "", m.group(1)).upper()
                 for m in self.FORM_RE.finditer(text)}
        if len(codes) > 1:
            return None, False

        kind, from_form = super()._page_kind(text)
        if from_form or kind not in REPORTS:
            return kind, from_form
        return self._best_titled(text) or (kind, from_form)

    def _best_titled(self, text):
        """Among the three statement titles, the one the header actually matches BEST.

        The base class takes the first title that clears `TITLE_MATCH` in dict order — balance
        sheet, then income statement, then cash flow — which is safe when the fallback is rare.
        It is not rare here. This filing's cash-flow page prints "Mẫu BO4/TCTD-HN" with a letter
        O where the zero should be, so `B\\d{2}` misses and the page falls through to the title;
        the header then scored 0.80+ for "ketquahoatdongkinhdoanh" on a window of the BOILERPLATE
        ("...quyetdinhsothanhphohochiminh...") and the page was declared the income statement —
        even though "luuchuyentiente" appears in it verbatim, a perfect 1.0, two entries later.

        The whole cash-flow statement was lost to that: its first page was swallowed by the
        income statement's run, `_fill_continuations` never started a cash-flow run, and only the
        pages after it survived. Scoring all three and taking the best costs nothing and cannot
        do worse — an exact title always beats a coincidence.
        """
        header = "\n".join([l for l in text.splitlines() if l.strip()][:self.HEADER_LINES])
        ns = self.norm(header).replace(" ", "")
        best, score = max(((r, self._title_score(ns, n)) for r, n in self.HEADING.items()),
                          key=lambda rs: rs[1])
        return (best, False) if score >= self.TITLE_MATCH else None

    def _title_score(self, header_ns: str, needles: List[str]) -> float:
        """How well the header matches a statement's title: 1.0 for a verbatim hit, else the
        best sliding-window ratio (the same measure `PdfParser._titled` thresholds)."""
        from difflib import SequenceMatcher

        best = 0.0
        for n in needles:
            if n in header_ns:
                return 1.0
            w = len(n)
            for i in range(0, max(1, len(header_ns) - w + 1)):
                best = max(best, SequenceMatcher(None, n, header_ns[i:i + w]).ratio())
        return best

    # A note reference is a 1-2 digit number ("Thuyết minh 4", "Thuyết minh 21"); a figure in
    # Triệu VNĐ is 4-9 digits. Nothing in between needs deciding, so the median width of a
    # column's numbers separates them with room to spare.
    NOTE_MAX_DIGITS = 2

    def value_columns(self, words_by_page, width: float) -> List[float]:
        """The base class's period columns, minus the "Thuyết minh" note-reference column.

        The note column has always been the hazard here — the base class keeps period columns to
        the right 60% of the page precisely to avoid it — but line-level detection moves the
        boundary. A word-level engine scatters the label's words across the left of the page and
        the note numbers land wherever they land; a line-level detector emits ONE box for the
        whole label and one for the note, so the notes form a tight, well-populated right-edge
        cluster sitting comfortably inside the value zone. It survives, becomes column 1, and
        `Statement._first_value` — which takes the first populated column as the current period —
        then reads every line's NOTE NUMBER as its figure. The statement still reconciles against
        nothing and maps to nothing, which is exactly what the first run produced.

        Dropping it by magnitude rather than by position is what makes the fix independent of
        where the document chose to print it.
        """
        import re as _re

        cols = super().value_columns(words_by_page, width)
        if len(cols) <= 1:
            return cols
        nums = [w for ws in words_by_page.values() for w in self._numbers(ws)]

        keep = []
        for c in cols:
            digits = sorted(len(_re.sub(r"\D", "", w[4])) for w in nums
                            if abs(w[2] - c) <= self.EDGE_TOL)
            if digits and digits[len(digits) // 2] > self.NOTE_MAX_DIGITS:
                keep.append(c)
        return keep or cols          # never leave the caller with nothing to parse

    def _ocr_page(self, page, native: str):
        text, words = self.engine.read_page(page)
        self.page_texts[page.number] = text
        if self.verbose:
            print(f"    page {page.number + 1}: {len(words)} words", flush=True)
        return text, words


def trimmed(pdf_path: str, max_pages: Optional[int]):
    """The document, cut to its first `max_pages` pages.

    The three statements are at the front of a filing and the notes behind them run to a hundred
    pages; `PdfParser.scan` stops on the first form-coded notes page, but only once it has seen
    all three statements — so a filing whose cash flow fails to classify would OCR the entire
    document. A hard cap keeps a bad run bounded.
    """
    import fitz

    doc = fitz.open(pdf_path)
    if not max_pages or doc.page_count <= max_pages:
        return doc
    cut = fitz.open()
    cut.insert_pdf(doc, from_page=0, to_page=max_pages - 1)
    doc.close()
    return cut


def parse_statements(engine, pdf_path: str, max_pages: Optional[int] = 24,
                     verbose: bool = True) -> Tuple[Dict[str, Statement], OcrPdfParser, dict]:
    """-> ({report: Statement}, the parser, {page index: classification}).

    Mirrors `PdfParser.parse` for the two things it does that matter here — find each statement's
    pages and rebuild its rows — and drops the two it does that do not: the publish date and the
    share-capital note both live in the tail of the document, which is trimmed away.
    """
    parser = OcrPdfParser(engine, verbose=verbose)
    doc = trimmed(pdf_path, max_pages)
    try:
        pages = parser.scan(doc)
        out: Dict[str, Statement] = {}
        for report in REPORTS:
            on = sorted(i for i, p in pages.items() if p["kind"] == report)
            if not on:
                continue
            words_by_page = {i: pages[i]["words"] for i in on}
            columns = parser.value_columns(words_by_page, pages[on[0]]["width"])
            if not columns:
                continue
            unit = parser.unit_of(pages, on)
            rows = parser.table_rows(words_by_page, columns)
            # scale once, here: values leave the parser in đồng. Getting this wrong is invisible
            # downstream — a uniform 10^6 error reconciles perfectly against itself.
            for r in rows:
                r.values = [None if v is None else v * unit for v in r.values]
            out[report] = Statement(report=report, pages=[i + 1 for i in on], unit=unit,
                                    n_columns=len(columns), rows=rows)
        return out, parser, pages
    finally:
        doc.close()


# ──────────────────────────────────────────────────────────────────────────────
# Rows -> canonical columns -> verdict
# ──────────────────────────────────────────────────────────────────────────────

def to_canonical(st: Statement, template: str = "bank") -> Tuple[Dict[str, int], Optional[str]]:
    """(canonical column -> value, reconciliation failure or None).

    Mapping onto the chart of accounts is what makes the output comparable at all: keyed on what
    OCR read, the same printed line becomes a different column in every document.
    """
    fb = FinancialsBuilder()
    mapped = fb.map_to_schema(st, template)
    return mapped, fb.reconcile(st, mapped)


def schema_labels(template: str, report: str) -> Dict[str, str]:
    """canonical column -> the account name the chart of accounts prints for it."""
    return dict(FinancialsBuilder().schema_of(template, report))


# ──────────────────────────────────────────────────────────────────────────────
# Ground truth — what CafeF's own tabs say for the same period
# ──────────────────────────────────────────────────────────────────────────────

def cafef_truth(report: str, year: int, symbol: str = "ACB", exchange: str = "HOSE",
                template: str = "bank") -> Dict[str, float]:
    """The FULL-YEAR figure per canonical column, from `raw_data/cafef/financials/`.

    This is an INDEPENDENT reading of the same filing — CafeF transcribes the document into its
    own tabs, keyed by item code, so a value that agrees with it agrees with the paper. For ACB's
    FY-2013 all three statements were taken from those tabs (`source=cafef`), which is the
    experiment's premise: the production parser could not read this scan.

    Each report needs its own arithmetic, and getting it wrong looks exactly like an OCR error:

      * balance sheet — a stock at 31 Dec. The Q4 row IS the year end.
      * income statement — the quarterly rows are STANDALONE, so the year the annual report
        prints is their SUM. Comparing the printed year against the Q4 row alone would report
        every line as a ~4x mismatch.
      * cash flow — already cumulative year-to-date, so Q4 IS the full year. Summing it would
        quadruple-count.
    """
    path = os.path.join(STATEMENTS_DIR, template, report, f"{exchange}_{symbol}.csv")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8-sig") as f:
        rows = [r for r in csv.DictReader(f) if r.get("year") == str(year)]
    if not rows:
        return {}

    # A line-item column is one the chart of accounts defines. Listing the metadata columns to
    # exclude instead is how this first broke: the file also carries `document`,
    # `shares_authorized`, `shares_issued`, `shares_outstanding`, and `float("Q1-2013_bao_cao…
    # .pdf")` is not a helpful error message.
    cols = [c for c, _ in FinancialsBuilder().schema_of(template, report) if c in rows[0]]

    if report == INCOME_STATEMENT:
        out: Dict[str, float] = {}
        for c in cols:
            vals = [float(r[c]) for r in rows if r.get(c) not in (None, "")]
            if len(vals) == 4:                   # only a complete year sums to the year
                out[c] = sum(vals)
        return out

    q4 = next((r for r in rows if r.get("quarter") == "4"), None)
    if q4 is None:
        return {}
    return {c: float(q4[c]) for c in cols if q4.get(c) not in (None, "")}


REL_TOL = 1e-6          # a match is the same figure, not a similar one


def compare(mapped: Dict[str, int], truth: Dict[str, float]) -> Tuple[List[dict], dict]:
    """Per-column verdicts + counts.

    Only columns BOTH sides populated can be judged. A column CafeF has and the parse does not is
    a miss (a line the OCR lost); one the parse has and CafeF does not is unverifiable, not
    wrong — CafeF's tabs have their own gaps.

    `sign_only` — the magnitude matches but the sign does not — is kept SEPARATE from `differ`,
    because on the income statement it is not an OCR error at all: the filing prints expenses in
    parentheses (a negative), while CafeF stores them as POSITIVE magnitudes (documented in
    experiment_7's caveats). The digits are read correctly; only the convention differs. Folding
    it into `differ` would blame the OCR for CafeF's storage choice — so the OCR-accuracy figure
    is `agree + sign_only`, and a genuine wrong number stays in `differ`.
    """
    rows = []
    stats = {"agree": 0, "sign_only": 0, "differ": 0, "parsed_only": 0, "truth_only": 0}
    for col in sorted(set(mapped) | set(truth)):
        got, want = mapped.get(col), truth.get(col)
        if got is not None and want is not None:
            tol = max(2.0, abs(want) * REL_TOL)
            if abs(got - want) <= tol:
                verdict = "agree"
            elif abs(abs(got) - abs(want)) <= max(2.0, abs(want) * REL_TOL):
                verdict = "sign_only"
            else:
                verdict = "differ"
        elif got is not None:
            verdict = "parsed_only"
        else:
            verdict = "truth_only"
        stats[verdict] += 1
        rows.append({"column": col, "parsed": got, "cafef": want, "verdict": verdict})
    return rows, stats


# ──────────────────────────────────────────────────────────────────────────────
# Output
# ──────────────────────────────────────────────────────────────────────────────

def write_csv(path: str, rows: List[dict], fields: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def run(engine, engine_name: str, pdf_path: str, out_dir: str, year: int = 2013,
        symbol: str = "ACB", exchange: str = "HOSE", template: str = "bank",
        max_pages: Optional[int] = 24, verbose: bool = True, dpi: int = 200,
        cache: bool = True) -> dict:
    """OCR the filing, rebuild the three statements, score them, write everything.

    The driver both experiments share. Returns the summary dict it also writes to `report.md`,
    so a caller can compare two runs without re-reading the files.
    """
    import time

    os.makedirs(out_dir, exist_ok=True)
    if cache:
        engine = CachingEngine(engine, os.path.join(out_dir, "ocr_cache.json"),
                               key=f"{engine_name}|{os.path.basename(pdf_path)}|dpi={dpi}")
    t0 = time.time()
    print(f"[{engine_name}] reading {os.path.basename(pdf_path)}", flush=True)
    statements, parser, pages = parse_statements(engine, pdf_path, max_pages, verbose)
    elapsed = time.time() - t0

    # Every box the engine returned, with its coordinates. The statements are what the pipeline
    # made of the read; this is the read itself, and it is where a lost line or a column that
    # clustered wrongly is actually visible.
    write_csv(os.path.join(out_dir, "words.csv"),
              [{"page": i + 1, "x0": round(w[0], 1), "y0": round(w[1], 1),
                "x1": round(w[2], 1), "y1": round(w[3], 1), "text": w[4]}
               for i in sorted(pages) for w in pages[i]["words"]],
              ["page", "x0", "y0", "x1", "y1", "text"])

    # The OCR text itself, page by page. When a statement is missing, this is where the answer
    # is: either the form code did not survive the read, or the page never became a table.
    with open(os.path.join(out_dir, "ocr_pages.txt"), "w", encoding="utf-8") as f:
        for i in sorted(parser.page_texts):
            kind = pages.get(i, {}).get("kind")
            f.write(f"\n{'=' * 70}\npage {i + 1}   kind={kind}   "
                    f"form_code={pages.get(i, {}).get('from_form')}\n{'=' * 70}\n")
            f.write(parser.page_texts[i] + "\n")

    summary = {"engine": engine_name, "pdf": os.path.basename(pdf_path),
               "seconds": round(elapsed, 1), "pages_read": len(parser.page_texts),
               "ocr_pages": getattr(engine, "ocr_pages", None),
               "ocr_seconds": round(getattr(engine, "ocr_seconds", 0.0), 1),
               "cached_pages": getattr(engine, "cached_pages", 0),
               "reports": {}}
    all_rows: List[dict] = []
    for report in REPORTS:
        st = statements.get(report)
        if st is None:
            summary["reports"][report] = {"found": False}
            print(f"  {report:18s} NOT FOUND", flush=True)
            continue
        mapped, why = to_canonical(st, template)
        truth = cafef_truth(report, year, symbol, exchange, template)
        labels = schema_labels(template, report)
        rows = dump_statement(out_dir, st, mapped, truth, labels)
        for r in rows:
            r["report"] = report
        all_rows += rows
        _, stats = compare(mapped, truth)
        summary["reports"][report] = {
            "found": True, "pages": st.pages, "unit": st.unit, "columns": st.n_columns,
            "rows_parsed": len(st.rows), "mapped": len(mapped),
            "reconciles": why is None, "why": why, **stats,
        }
        print(f"  {report:18s} pages {st.pages} rows {len(st.rows):3d} "
              f"mapped {len(mapped):3d}  agree {stats['agree']:3d} differ {stats['differ']:3d} "
              f"missed {stats['truth_only']:3d}  reconcile: {why or 'OK'}", flush=True)

    if all_rows:
        write_csv(os.path.join(out_dir, "comparison.csv"), all_rows,
                  ["report", "column", "account", "parsed", "cafef", "verdict"])
    write_report(os.path.join(out_dir, "report.md"), summary)
    return summary


def write_report(path: str, summary: dict) -> None:
    ocr_pages = summary.get("ocr_pages") or 0
    per_page = f" ({summary['ocr_seconds'] / ocr_pages:.0f} s/page)" if ocr_pages else ""
    lines = [f"# {summary['engine']}", "",
             f"- file: `{summary['pdf']}`",
             f"- pages read: {summary['pages_read']}"
             f" ({summary.get('cached_pages', 0)} from cache)",
             f"- OCR: {ocr_pages} pages in {summary['ocr_seconds']} s{per_page}",
             f"- wall clock: {summary['seconds']} s", "",
             "| statement | pages | rows | mapped | reconciles | agree | differ | missed |",
             "|---|---|---|---|---|---|---|---|"]
    for report, r in summary["reports"].items():
        if not r.get("found"):
            lines.append(f"| {report} | — | — | — | NOT FOUND | — | — | — |")
            continue
        lines.append(
            f"| {report} | {r['pages']} | {r['rows_parsed']} | {r['mapped']} | "
            f"{'yes' if r['reconciles'] else 'no — ' + str(r['why'])} | "
            f"{r['agree']} | {r['differ']} | {r['truth_only']} |")
    lines += ["", "`agree` / `differ` are canonical columns both this parse and CafeF's own "
              "transcription populated; `missed` are lines CafeF has and the OCR did not "
              "recover.", ""]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def dump_statement(out_dir: str, st: Statement, mapped: Dict[str, int],
                   truth: Dict[str, float], labels: Dict[str, str]) -> List[dict]:
    """Write the three views of one statement and return its comparison rows.

    `<report>_rows.csv` is the raw parse (what the OCR read, in the order it was printed),
    `<report>.csv` is the same values on the canonical chart of accounts, and
    `<report>_vs_cafef.csv` is the verdict. The raw rows are kept because they are the only place
    an OCR failure is legible — a canonical column that is simply absent says nothing about why.
    """
    n_cols = max((len(r.values) for r in st.rows), default=0)
    write_csv(os.path.join(out_dir, f"{st.report}_rows.csv"),
              [{"n": i, "number": r.number, "label": r.label, "key": r.key,
                **{f"col{j + 1}": r.values[j] if j < len(r.values) else None
                   for j in range(n_cols)}}
               for i, r in enumerate(st.rows)],
              ["n", "number", "label", "key"] + [f"col{j + 1}" for j in range(n_cols)])

    write_csv(os.path.join(out_dir, f"{st.report}.csv"),
              [{"column": c, "account": labels.get(c, ""), "value_vnd": v}
               for c, v in mapped.items()],
              ["column", "account", "value_vnd"])

    rows, _ = compare(mapped, truth)
    for r in rows:
        r["account"] = labels.get(r["column"], "")
    write_csv(os.path.join(out_dir, f"{st.report}_vs_cafef.csv"), rows,
              ["column", "account", "parsed", "cafef", "verdict"])
    return rows
