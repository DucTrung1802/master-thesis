# src\web_scraper\cafef_financials.py

# ===== Standard Library =====
import csv
import os
import re
from typing import Dict, List, Optional, Tuple

# ===== Local / Custom Modules =====
from web_scraper.cafef_pdf_parser import (
    BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, REPORTS, PdfParser, Statement,
)
from utils.constants import CAFEF_RAW_DATA_DIR

PDFS_DIR = os.path.join(CAFEF_RAW_DATA_DIR, "pdfs")
FIN_DIR = os.path.join(CAFEF_RAW_DATA_DIR, "financials")

# financials/
# ├── schema/<template>_<report>.csv          the 4 charts of accounts x 3 statements
# ├── statements/<template>/<report>/<EXCHANGE>_<SYMBOL>.csv
# └── templates.csv                           ticker -> template + cash-flow method
#
# The TEMPLATE is a folder, not a column, because the four charts of accounts share no line
# items: a directory is then schema-homogeneous — every file under `statements/bank/` has the
# same 90 columns and they mean the same thing. Mixing them would give a table whose columns
# depend on which row you are reading.
SCHEMA_DIR = os.path.join(FIN_DIR, "schema")
STATEMENTS_DIR = os.path.join(FIN_DIR, "statements")
TEMPLATES_INDEX = os.path.join(FIN_DIR, "templates.csv")

DATA_COLS = ["symbol", "exchange", "template", "period", "year", "quarter", "source",
             "assurance", "cash_flow_method", "unit", "n_columns", "document"]
INDEX_COLS = ["exchange", "symbol", "template", "cash_flow_method", "sector",
              "industry_group_code", "industry_group_slug"]


class FinancialsBuilder:
    """Build quarterly financial statements for a ticker from its LOCAL PDF archive.

        raw_data/cafef/pdfs/     (in)   downloaded by CafeFPdfScraper
        raw_data/cafef/financials/<report>/<EXCHANGE>_<SYMBOL>.csv   (out)

    One row per quarter, one column per line item, keyed by the label the filing prints —
    the filing has no item codes of its own (CafeF's 300/411/800 are CafeF's numbering, not
    the document's), so the printed label is the only identifier that actually exists.

    Nothing is written unless it reconciles. A parsed statement must balance against its own
    printed subtotals and be of a sane magnitude beside its neighbours; a quarter that fails
    is left blank. A wrong figure is worse than a missing one — and there are three ways to
    be wrong that no single check catches:

      * UNITS      — most filings are in Triệu VNĐ, VCB's 2009 ones in plain đồng. Read the
                     wrong one and every figure is out by 10^6 while reconciling perfectly.
      * CUMULATIVE — the semi-annual report prints ONLY the Jan-Jun column, so its income
                     statement is not the standalone quarter (VCB Q2-2024 prints PBT 20,835bn
                     where the quarter is 10,116bn). The cumulative figures balance against
                     each other, and 2x sits well inside any magnitude band.
      * OCR        — a misread digit. Reconciliation catches it only when the line takes part
                     in an identity.
    """

    # Which document to read for a quarter. Consolidated only — the parent-company report
    # covers a different entity. Prefer the reviewed/audited version when a quarter was filed
    # twice, since the later document restates the earlier.
    ASSURANCE_RANK = {"audited": 0, "reviewed": 1, "unaudited": 2}

    # Subtotals used to reconcile, by the words the filing prints. Matched with spaces and
    # underscores stripped, so OCR losing a space cannot defeat them.
    TOTAL_ASSETS = ("tong tai san", "tong cong tai san")
    TOTAL_RESOURCES = ("tong no phai tra va von chu so huu", "tong cong nguon von")
    TOTAL_LIABILITIES = ("tong no phai tra",)
    TOTAL_EQUITY = ("von chu so huu", "tong von chu so huu")
    PBT = ("tong loi nhuan truoc thue", "loi nhuan truoc thue",
           "tong loi nhuan ke toan truoc thue")
    NET_CF = ("luu chuyen tien thuan trong ky", "luu chuyen tien thuan trong nam")
    CASH_CLOSE = ("tien va cac khoan tuong duong tien tai thoi diem cuoi",
                  "tien va tuong duong tien cuoi ky")

    # The same subtotals, as CANONICAL columns (schema/<template>_<report>.csv). Looked up here
    # a line cannot be lost to OCR damage — which is what most rejections were.
    C_ASSETS = ("tong_tai_san", "tong_cong_tai_san")
    C_RESOURCES = ("tong_no_phai_tra_va_von_chu_so_huu", "tong_cong_nguon_von")
    C_LIABILITIES = ("tong_no_phai_tra", "no_phai_tra")
    C_EQUITY = ("viii_von_chu_so_huu", "von_chu_so_huu", "d_von_chu_so_huu")
    C_PBT = ("xi_tong_loi_nhuan_truoc_thue", "tong_loi_nhuan_truoc_thue",
             "tong_loi_nhuan_ke_toan_truoc_thue")
    C_NET_CF = ("hdtc_iv_luu_chuyen_tien_thuan_trong_ky", "luu_chuyen_tien_thuan_trong_ky")
    C_CASH_CLOSE = ("hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky",
                    "tien_va_tuong_duong_tien_cuoi_ky")

    MIN_ROWS = 12          # a statement with fewer parsed rows than this is not a statement

    # How alike two labels must be to be the SAME line item across quarters.
    #
    # The filing prints no item codes, so a line is identified by its label — and OCR renders
    # the same printed label slightly differently from one scan to the next ("kế toán" ->
    # "kếtoán", a dropped diacritic, a swallowed space). Keyed literally, every quarter mints
    # fresh columns and the panel does not line up in time: VCB's balance sheet produced 123
    # columns across 3 quarters, of which only 35 appeared in all three.
    #
    # So a parsed label is matched against the columns already seen and reuses the closest
    # one. The threshold is deliberately high: it must absorb OCR noise without merging two
    # genuinely different lines, and "chi phí hoạt động" vs "chi phí hoạt động khác" differ by
    # more than this.
    SIMILARITY = 0.92

    def __init__(self, logger=None):
        self._logger = logger
        self._parser = PdfParser(logger=logger)
        self._schema_cache: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}

    # ──────────────────────────────────────────────────────────────────────
    # Which template does a ticker file on?
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def build_templates_index(symbols: List[Tuple[str, str]],
                             logger=None) -> List[dict]:
        """Resolve every ticker to its accounting template -> financials/templates.csv.

        This is the map that says WHERE a ticker's statements live, and it is built by
        FINGERPRINTING each ticker (`cafef_schema.detect_template`) — reading the chart of
        accounts CafeF actually renders for it — never by classifying it from GICS. The two
        disagree: HVA sits in the securities industry group and files on the CORPORATE
        template. The GICS columns are carried alongside so a disagreement is visible, but the
        fingerprint is the one the parser obeys.
        """
        from web_scraper.cafef_schema import cash_flow_method, detect_template

        gics = {}
        path = os.path.join(CAFEF_RAW_DATA_DIR, "..", "simplize", "industry.csv")
        if os.path.exists(path):
            with open(path, encoding="utf-8-sig") as f:
                gics = {r["ticker"]: r for r in csv.DictReader(f)}

        rows = []
        for exchange, symbol in symbols:
            template = detect_template(symbol)
            method = cash_flow_method(symbol)
            g = gics.get(symbol, {})
            rows.append({
                "exchange": exchange, "symbol": symbol,
                "template": template or "", "cash_flow_method": method or "",
                "sector": g.get("economic_sector_name", ""),
                "industry_group_code": g.get("industry_group_code", ""),
                "industry_group_slug": g.get("industry_group_slug", ""),
            })
            if logger:
                logger.log_info(f"cafef financials: {symbol} -> {template} / {method}")

        rows.sort(key=lambda r: (r["template"], r["symbol"]))
        os.makedirs(FIN_DIR, exist_ok=True)
        tmp = TEMPLATES_INDEX + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=INDEX_COLS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, TEMPLATES_INDEX)
        return rows

    def template_of(self, symbol: str) -> Optional[str]:
        """From templates.csv when it is there, else fingerprint the ticker."""
        if os.path.exists(TEMPLATES_INDEX):
            with open(TEMPLATES_INDEX, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    if r["symbol"] == symbol and r["template"]:
                        return r["template"]
        from web_scraper.cafef_schema import detect_template
        return detect_template(symbol)

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger.log_info(msg)
        else:
            print(msg)

    def _warn(self, msg: str) -> None:
        if self._logger:
            self._logger.log_warning(msg)
        else:
            print(msg)

    # ──────────────────────────────────────────────────────────────────────
    # Choosing the documents
    # ──────────────────────────────────────────────────────────────────────

    def documents(self, exchange: str, symbol: str) -> List[dict]:
        """The one consolidated filing to read per quarter, oldest first.

        Q4 is taken from the AUDITED ANNUAL report (CafeF files it under quarter 5) whenever
        one exists. It is the same period — the balance sheet at 31 December IS the Q4 balance
        sheet, and the cash flow is cumulative to year end either way — but audited rather than
        unaudited, and it is the better-produced document.

        The income statement is the exception and must be de-cumulated: an annual report's P&L
        is the whole year, so the Q4 quarter is FY − (Q1+Q2+Q3). The row is tagged `annual` so
        `_decumulate` knows to do that.
        """
        index = os.path.join(PDFS_DIR, "index", f"{exchange}_{symbol}.csv")
        if not os.path.exists(index):
            raise FileNotFoundError(
                f"no PDF index at {index} — run CafeFPdfScraper for {symbol} first")
        with open(index, encoding="utf-8-sig") as f:
            rows = [r for r in csv.DictReader(f) if r["consolidated"] == "True"]

        best: Dict[str, dict] = {}
        annual: Dict[str, dict] = {}
        for r in rows:
            q = str(r["quarter"])
            if not q.isdigit():
                continue
            q = int(q)
            if q == 5:                        # the audited annual -> stands in for Q4
                p = f"Q4-{r['year']}"
                rank = self.ASSURANCE_RANK.get(r["assurance"], 9)
                if p not in annual or rank < self.ASSURANCE_RANK.get(
                        annual[p]["assurance"], 9):
                    annual[p] = {**r, "period": p, "quarter": "4", "annual": "True"}
                continue
            if q not in (1, 2, 3, 4):
                continue
            p = r["period"]
            rank = self.ASSURANCE_RANK.get(r["assurance"], 9)
            if p not in best or rank < self.ASSURANCE_RANK.get(best[p]["assurance"], 9):
                best[p] = {**r, "annual": "False"}

        best.update(annual)                   # the annual report wins Q4
        return sorted(best.values(),
                      key=lambda r: (int(r["year"]), int(r["quarter"])))

    # ──────────────────────────────────────────────────────────────────────
    # Gates
    # ──────────────────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────────────────
    # Mapping a parsed statement onto the canonical chart of accounts
    # ──────────────────────────────────────────────────────────────────────

    # How close an OCR'd line must be to a schema line to BE it.
    #
    # 0.80, not lower: a shorter name is a subsequence of a longer one far more often than it
    # looks. "TỔNG VỐN CHỦ SỞ HỮU" scores 0.75 against "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" —
    # every character of the first appears, in order, inside the second. At 0.72 total equity
    # captured the grand total's column, the real grand total then had nowhere to go, and 48 of
    # 69 balance sheets were rejected for "assets != liabilities + equity".
    SCHEMA_MATCH = 0.80
    MIN_CONTAINS = 10        # too short a name is contained in too many others to prove much

    # The cash-flow section prefixes, and the method tags the union adds. Only these may be
    # stripped from the front of a column — a blanket "drop the first word" also eats the
    # first word of `tong_tai_san`, leaving `tai_san`, which then fuzzy-matches any asset line
    # and hands TOTAL ASSETS the value of some line halfway up the statement.
    COL_PREFIXES = ("hdkd_indirect_", "hdkd_direct_", "hdkd_", "hddt_", "hdtc_")
    # An index: roman, digit or single letter, e.g. `vii_1_a_`.
    INDEX_RE = re.compile(r"^(?:[ivxlc]+_|\d+_|[a-z]_)+")

    def schema_of(self, template: str, report: str) -> List[Tuple[str, str]]:
        """[(canonical column, its account name)] in statement order, from schema/."""
        key = (template, report)
        if key in self._schema_cache:
            return self._schema_cache[key]

        path = os.path.join(SCHEMA_DIR, f"{template}_{report}.csv")
        items: List[Tuple[str, str]] = []
        if os.path.exists(path):
            with open(path, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    col = r["column"]
                    rest = col
                    for p in self.COL_PREFIXES:
                        if rest.startswith(p):
                            rest = rest[len(p):]
                            break
                    account = self.INDEX_RE.sub("", rest)
                    items.append((col, account or rest))
        self._schema_cache[key] = items
        return items

    def map_to_schema(self, st: Statement, template: str) -> Dict[str, int]:
        """Parsed rows -> canonical columns.

        This is what makes the output a PANEL rather than a pile. Keyed on the OCR text, the
        same printed line becomes a different column every quarter — VCB's balance sheet threw
        332 columns against a 90-column chart of accounts, so nothing lined up in time. Keyed
        on the schema, a line is the same column in every quarter of every ticker on that
        template.

        Matching walks the schema and the parsed rows together, in statement order, and is
        fuzzy because OCR damages the names ("TỔNG NỢ PHẢI TRẢ" -> `tong_nuphai_tra`). Order is
        what keeps a fuzzy match honest: the schema's own sequence stops "chi phí hoạt động"
        from being answered by "chi phí hoạt động khác" further down.
        """
        from difflib import SequenceMatcher

        schema = self.schema_of(template, st.report)
        if not schema:
            return {}

        out: Dict[str, int] = {}
        i = 0                                   # next schema line still open
        for row in st.rows:
            if row.values[0] is None:
                continue
            key = row.key.replace("_", "")
            if not key:
                continue
            best_j, best = -1, 0.0
            # a window: a line never moves far from its place in the statement
            for j in range(i, min(len(schema), i + self.SCHEMA_WINDOW)):
                col, account = schema[j]
                if col in out:
                    continue
                a = account.replace("_", "")
                r = SequenceMatcher(None, a, key).ratio()
                if len(a) >= self.MIN_CONTAINS and (a in key or key in a):
                    r = max(r, 0.95)            # one is contained in the other
                if r > best:
                    best_j, best = j, r
            if best_j >= 0 and best >= self.SCHEMA_MATCH:
                out[schema[best_j][0]] = row.values[0]
                i = best_j + 1                  # never match backwards

        self._anchor(out, schema, st)
        return out

    # The lines reconciliation stands on. They are unambiguous — no other line in a statement
    # is called "TỔNG TÀI SẢN" — so they are re-matched GLOBALLY, ignoring position.
    ANCHORS = ("tong_tai_san", "tong_cong_tai_san", "tong_no_phai_tra",
               "tong_no_phai_tra_va_von_chu_so_huu", "tong_cong_nguon_von",
               "viii_von_chu_so_huu", "xi_tong_loi_nhuan_truoc_thue",
               "hdtc_iv_luu_chuyen_tien_thuan_trong_ky",
               "hdtc_vii_tien_va_cac_khoan_tuong_duong_tien_tai_thoi_diem_cuoi_ky")
    ANCHOR_MATCH = 0.86      # stricter than the ordered pass: this one has no order to lean on

    def _anchor(self, out: Dict[str, int], schema: List[Tuple[str, str]],
                st: Statement) -> None:
        """Re-match the subtotals without regard to position.

        The ordered walk drifts. Once it has advanced past a column, a row that belongs there
        lands on the next-best thing instead — VCB's Q2-2014 and Q2-2023 gave the GRAND TOTAL
        column the value of TỔNG NỢ PHẢI TRẢ, and `assets - resources` came out exactly equal to
        equity. These few lines are unambiguous (nothing else in a statement is called "TỔNG
        TÀI SẢN"), so they are searched for over the whole statement and the best match wins.
        """
        from difflib import SequenceMatcher

        accounts = {c: a.replace("_", "") for c, a in schema if c in self.ANCHORS}
        for col, a in accounts.items():
            best_v, best = None, 0.0
            for row in st.rows:
                if row.values[0] is None:
                    continue
                k = row.key.replace("_", "")
                r = SequenceMatcher(None, a, k).ratio()
                if len(a) >= self.MIN_CONTAINS and (a in k or k in a):
                    r = max(r, 0.95)
                if r > best:
                    best_v, best = row.values[0], r
            if best_v is not None and best >= self.ANCHOR_MATCH:
                out[col] = best_v

    SCHEMA_WINDOW = 25       # how far ahead in the chart of accounts a line may be found

    def _canonical(self, key: str, known: List[str]) -> str:
        """The column this label belongs to: an existing one if close enough, else itself.

        Matching is on the label with separators stripped, so a lost space or underscore is
        not a difference. A suffixed duplicate column ("..._2") is never a match target — it
        exists to separate two lines that really are distinct.
        """
        from difflib import SequenceMatcher

        flat = key.replace("_", "")
        best, score = None, 0.0
        for col in known:
            if re.match(r".*__\d+$", col):
                continue
            r = SequenceMatcher(None, flat, col.replace("_", "")).ratio()
            if r > score:
                best, score = col, r
        return best if score >= self.SIMILARITY else key

    def reconcile(self, st: Statement,
                  mapped: Optional[Dict[str, int]] = None) -> Optional[str]:
        """None if the statement balances against its OWN printed subtotals, else why not.

        The subtotals are taken from the CANONICAL columns when the rows have been mapped —
        `mapped` — and only fall back to searching the OCR text when they have not. Searching
        the text is what most rejections actually were: the row was parsed, its figure correct,
        and the lookup simply could not recognise the name OCR had mangled.
        """
        if len(st.rows) < self.MIN_ROWS:
            return f"only {len(st.rows)} rows parsed"

        def get(canonical: Tuple[str, ...], *text: str) -> Optional[int]:
            if mapped:
                for c in canonical:
                    if c in mapped:
                        return mapped[c]
            return st.find(*text)

        if st.report == BALANCE_SHEET:
            assets = get(self.C_ASSETS, *self.TOTAL_ASSETS)
            resources = get(self.C_RESOURCES, *self.TOTAL_RESOURCES)
            if assets is None:
                return "no total assets"
            if resources is not None and not self._equal(assets, resources):
                return "assets != liabilities + equity"
            if resources is None:
                liab = get(self.C_LIABILITIES, *self.TOTAL_LIABILITIES)
                eq = get(self.C_EQUITY, *self.TOTAL_EQUITY)
                if liab is None or eq is None:
                    return "no total to balance against"
                if abs(assets - (liab + eq)) > assets * 0.02:
                    return "assets != liabilities + equity"

        if st.report == INCOME_STATEMENT:
            if get(self.C_PBT, *self.PBT) is None:
                return "no profit before tax"

        if st.report == CASH_FLOW:
            if (get(self.C_NET_CF, *self.NET_CF) is None
                    and get(self.C_CASH_CLOSE, *self.CASH_CLOSE) is None):
                return "no cash-flow subtotal"
        return None

    # Two figures that should be identical, allowing for OCR. VCB's Q1-2020 balance sheet reads
    # total assets 1,144,270,267 and total resources 1,144,270,262 (in millions) — the same
    # figure with ONE digit misread, 4.4e-6 apart. An absolute tolerance of 2 rejected it.
    #
    # 1e-5 is a millionth of a percent, and still nowhere near a real discrepancy: the errors
    # that matter are whole wrong rows (Q2-2014 was out by exactly the equity figure, ~9%) or a
    # digit inserted into the total (Q2-2018 reads 10x). Those are 3-6 orders of magnitude
    # larger and are still caught.
    EQUAL_REL = 1e-5

    @classmethod
    def _equal(cls, a: int, b: int) -> bool:
        return abs(a - b) <= max(2, abs(a) * cls.EQUAL_REL)

    def _probe(self, report: str, mapped: Dict[str, int],
               st: Statement) -> Optional[int]:
        """The one figure a statement is size-checked on."""
        canonical, text = {
            BALANCE_SHEET: (self.C_ASSETS, self.TOTAL_ASSETS),
            INCOME_STATEMENT: (self.C_PBT, self.PBT),
            CASH_FLOW: (self.C_CASH_CLOSE, self.CASH_CLOSE),
        }[report]
        for c in canonical:
            if c in mapped:
                return mapped[c]
        return st.find(*text)

    def sane(self, st: Statement, history: List[int],
             mapped: Optional[Dict[str, int]] = None) -> Optional[str]:
        """Magnitude guard against the quarters already accepted.

        Reconciliation is ratio-based: it cannot see a UNITS error or a CUMULATIVE column,
        because both balance perfectly and are still wrong. This is the only thing that does.
        """
        got = self._probe(st.report, mapped or {}, st)
        if got is None or not history:
            return None
        ref = sorted(abs(v) for v in history)
        median = ref[len(ref) // 2]
        if median and abs(got) and not (median / 20 <= abs(got) <= median * 20):
            return (f"magnitude {abs(got):.3g} vs typical {median:.3g} "
                    f"(units? cumulative column? OCR misread?)")
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Build
    # ──────────────────────────────────────────────────────────────────────

    def build(self, exchange: str, symbol: str,
              periods: Optional[List[str]] = None,
              use_api: bool = True) -> Dict[str, int]:
        docs = self.documents(exchange, symbol)
        if periods:
            docs = [d for d in docs if d["period"] in set(periods)]
        # the chart of accounts the parsed rows are mapped onto — fingerprinted, not guessed
        template = self.template_of(symbol) or "unknown"
        self._log(f"cafef financials: {symbol} ({template}): "
                  f"{len(docs)} consolidated quarters to parse")

        # report -> period -> {column: value}; and the column order as first seen
        data: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        items: Dict[str, List[str]] = {r: [] for r in REPORTS}
        meta: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        history: Dict[str, List[int]] = {r: [] for r in REPORTS}
        half_year: Dict[str, bool] = {}

        for d in docs:
            period = d["period"]
            # both a semi-annual and an annual report print a CUMULATIVE income statement
            half_year[period] = (d["half_year"] == "True"
                                 or d.get("annual") == "True")
            path = os.path.join(PDFS_DIR, d["path"].replace("/", os.sep))
            if not os.path.exists(path):
                self._warn(f"  {period}: file missing on disk — {d['path']}")
                continue
            try:
                statements = self._parser.parse(path)
            except Exception as e:
                self._warn(f"  {period}: parse failed — {type(e).__name__}: {e}")
                continue

            notes = []
            for report in REPORTS:
                st = statements.get(report)
                if st is None:
                    notes.append(f"{report}=absent")
                    continue

                # onto the canonical chart of accounts FIRST — reconciliation then reads its
                # subtotals from columns rather than from OCR text
                row = self.map_to_schema(st, template)
                why = self.reconcile(st, row) or self.sane(st, history[report], row)
                if why:
                    notes.append(f"{report}=REJECTED({why})")
                    continue

                for col in row:
                    if col not in items[report]:
                        items[report].append(col)

                data[report][period] = row
                meta[report][period] = {
                    "assurance": d["assurance"], "unit": st.unit,
                    "n_columns": st.n_columns, "document": d["file"],
                    # read from THIS filing: a company chooses the method and may switch it
                    "cash_flow_method": st.cash_flow_method or "",
                }
                v = self._probe(report, row, st)
                if v is not None:
                    history[report].append(v)
                notes.append(f"{report}={len(row)} items")
            self._log(f"  {period:<8} {'; '.join(notes)}")

        self._decumulate(data, half_year)

        # Whatever the filings could not give us, take from CafeF's own tabs. Keyed by item
        # CODE, so it lands on the canonical column exactly — for a quarter whose scan is too
        # degraded to read, this is the better source, not the lesser one.
        if use_api:
            api = self.from_api(symbol, template)
            for report in REPORTS:
                filled = 0
                for period, row in api[report].items():
                    if period in data[report] or not row:
                        continue
                    data[report][period] = row
                    meta[report][period] = {"source": "cafef"}
                    for col in row:
                        if col not in items[report]:
                            items[report].append(col)
                    filled += 1
                if filled:
                    self._log(f"cafef financials: {symbol} {report}: "
                              f"{filled} quarters filled from the CafeF tabs")

        # every quarter we ATTEMPTED, so one we failed to read is reported as missing rather
        # than vanishing from the grid
        attempted = [(int(d["year"]), int(d["quarter"])) for d in docs]
        attempted += [(int(p.split("-")[1]), int(p[1]))
                      for r in REPORTS for p in data[r]]
        return self._write(exchange, symbol, data, items, meta, attempted, template)

    # ──────────────────────────────────────────────────────────────────────
    # The fallback: CafeF's own tabs
    # ──────────────────────────────────────────────────────────────────────

    # CafeF's "not reported" sentinel is -1, not 0 and not null. Written through it becomes a
    # literal -1 dong in a column of billions, and no reconciliation catches it because an
    # unreported line takes part in no subtotal.
    NOT_REPORTED = "-1"

    def from_api(self, symbol: str, template: str) -> Dict[str, Dict[str, dict]]:
        """{report: {period: {canonical column: value}}} from the three tabs of
        cafef.vn/du-lieu/<exchange>/<sym>-tai-chinh.chn.

        This is not a lesser source — for the quarters OCR cannot read it is a BETTER one. The
        tabs are keyed by the same item CODES the schema was built from, so a value lands on
        its canonical column exactly: no OCR, no fuzzy matching, no chance of a line being
        mistaken for its neighbour. What it is not is the filing itself — CafeF transcribes,
        and it has gaps (it omits Q2-2024 market-wide) and it rounds — so the PDF is still
        read first and this fills only what the PDF could not.
        """
        from web_scraper.cafef_schema import TABS, _get

        # canonical column for each of CafeF's codes, from the schema we already built
        by_code: Dict[str, Dict[str, str]] = {}
        for report in REPORTS:
            path = os.path.join(SCHEMA_DIR, f"{template}_{report}.csv")
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8-sig") as f:
                by_code[report] = {r["cafef_code"]: r["column"]
                                   for r in csv.DictReader(f) if r["cafef_code"]}

        out: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        for report, (url, sections, shape) in TABS.items():
            codes = by_code.get(report, {})
            if not codes:
                continue
            for sec in sections:
                try:
                    head = _get(f"{url}?symbol={symbol}&pageIndex=1&pageSize=1"
                                f"&reportType={sec}&TypeTime=QUY").get("value") or {}
                    n = head.get("count") or 0
                    if not n:
                        continue
                    v = _get(f"{url}?symbol={symbol}&pageIndex=1&pageSize={n}"
                             f"&reportType={sec}&TypeTime=QUY").get("value") or {}
                except Exception as e:
                    self._warn(f"cafef financials: {symbol} {report}/{sec} tab failed: {e}")
                    continue
                blocks = (v["data"][0]["data"] if shape == "nested" and v.get("data")
                          else v.get("data") or [])
                for blk in blocks:
                    period = blk.get("time")
                    if not period:
                        continue
                    row = out[report].setdefault(period, {})
                    for cell in blk.get("data", []):
                        col = codes.get(str(cell.get("code")))
                        val = cell.get("value")
                        if not col or val in (None, "") or str(val) == self.NOT_REPORTED:
                            continue
                        try:
                            row[col] = int(val)
                        except (TypeError, ValueError):
                            continue
        return out

    def _decumulate(self, data: Dict[str, Dict[str, dict]],
                    half_year: Dict[str, bool]) -> None:
        """Turn a year-to-date income statement into the standalone quarter.

        A semi-annual filing prints only the cumulative Jan-Jun column, so its figures are
        Jan-Jun, not Apr-Jun. Nothing else catches this: the cumulative numbers reconcile
        perfectly against each other and 2x is well inside any magnitude band. The quarter is
        recovered as YTD minus the quarters already accepted for that year; a line whose
        prior quarters are not all available is DROPPED rather than guessed.

        Cash flow is NOT adjusted — it is cumulative by nature.
        """
        rows = data[INCOME_STATEMENT]
        for period in sorted(rows):
            q, year = int(period[1]), int(period.split("-")[1])
            # a half-year filing prints Jan-Jun; an ANNUAL one prints the whole year. Both are
            # cumulative, and the quarter is what is left when the earlier quarters come off.
            if q == 1 or not half_year.get(period):
                continue
            prior = [rows.get(f"Q{i}-{year}") for i in range(1, q)]
            if any(p is None for p in prior):
                self._warn(f"  {period}: cumulative, but Q1..Q{q-1} not all parsed — "
                           f"dropping (would otherwise be a 6-month figure in a "
                           f"quarterly row)")
                del rows[period]
                continue
            out = {}
            for col, ytd in rows[period].items():
                vals = [p.get(col) for p in prior]
                if any(v is None for v in vals):
                    continue
                out[col] = ytd - sum(vals)
            rows[period] = out
            self._log(f"  {period:<8} income_statement de-cumulated "
                      f"(YTD - Q1..Q{q-1}), {len(out)} items")

    def _write(self, exchange: str, symbol: str,
               data: Dict[str, Dict[str, dict]], items: Dict[str, List[str]],
               meta: Dict[str, Dict[str, dict]],
               attempted: List[Tuple[int, int]], template: str) -> Dict[str, int]:
        """One CSV per report, under the ticker's own TEMPLATE — so every file in a directory
        has the same columns and they mean the same thing.

        The quarter grid spans every quarter ATTEMPTED, not merely the ones that parsed. A
        grid built from the parsed periods hides its own failures: VIC's balance sheet read 6
        quarters out of 71 and reported "6/10", because the 61 it could not read fell outside
        the min..max of the 6 it could. A quarter we failed on is a blank `source=missing` row
        — never skipped, never zero-filled.

        Columns are the CANONICAL ones, in the chart of accounts' own order, so the same column
        means the same line in every quarter and across every ticker on this template."""
        counts = {}
        for report in REPORTS:
            rows = data[report]
            if not rows and not attempted:
                counts[report] = 0
                continue
            ys = sorted(set(attempted) | {(int(p.split("-")[1]), int(p[1])) for p in rows})
            (y0, q0), (y1, q1) = ys[0], ys[-1]

            out = []
            y, q = y0, q0
            while (y, q) <= (y1, q1):
                period = f"Q{q}-{y}"
                m = meta[report].get(period, {})
                row = {"symbol": symbol, "exchange": exchange, "template": template,
                       "period": period, "year": y, "quarter": q,
                       # `pdf` = read off the filing; `cafef` = taken from CafeF's tabs
                       # because the filing could not be read
                       "source": (m.get("source", "pdf") if period in rows else "missing"),
                       "assurance": m.get("assurance", ""),
                       "cash_flow_method": m.get("cash_flow_method", ""),
                       "unit": m.get("unit", ""),
                       "n_columns": m.get("n_columns", ""),
                       "document": m.get("document", "")}
                row.update(rows.get(period, {}))
                out.append(row)
                y, q = (y + 1, 1) if q == 4 else (y, q + 1)

            path = os.path.join(STATEMENTS_DIR, template, report,
                                f"{exchange}_{symbol}.csv")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # every column the chart of accounts defines, in ITS order — a line the filings
            # never reported is an empty column, not an absent one, so the shape of the table
            # is the same for every ticker on this template
            schema_cols = [c for c, _ in self.schema_of(template, report)]
            extra = [c for c in items[report] if c not in schema_cols]
            head = DATA_COLS + schema_cols + extra
            tmp = path + ".tmp"
            with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=head, extrasaction="ignore")
                w.writeheader()
                w.writerows(out)
            os.replace(tmp, path)

            n = sum(1 for r in out if r["source"] == "pdf")
            counts[report] = n
            self._log(f"cafef financials: {symbol} {report}: {len(out)} quarters "
                      f"({out[0]['period']}..{out[-1]['period']}), {n} parsed, "
                      f"{len(out) - n} missing, {len(items[report])} line items -> {path}")
        return counts
