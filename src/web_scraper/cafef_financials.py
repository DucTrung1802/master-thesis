# src\web_scraper\cafef_financials.py

# ===== Standard Library =====
import csv
import os
import re
from typing import Dict, List, Optional

# ===== Local / Custom Modules =====
from web_scraper.cafef_pdf_parser import (
    BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, REPORTS, PdfParser, Statement,
)
from utils.constants import CAFEF_RAW_DATA_DIR

PDFS_DIR = os.path.join(CAFEF_RAW_DATA_DIR, "pdfs")
OUT_DIR = os.path.join(CAFEF_RAW_DATA_DIR, "financials")

DATA_COLS = ["symbol", "exchange", "period", "year", "quarter",
             "source", "assurance", "unit", "n_columns", "document"]


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
        """The one consolidated filing to read per quarter, oldest first."""
        index = os.path.join(PDFS_DIR, "index", f"{exchange}_{symbol}.csv")
        if not os.path.exists(index):
            raise FileNotFoundError(
                f"no PDF index at {index} — run CafeFPdfScraper for {symbol} first")
        with open(index, encoding="utf-8-sig") as f:
            rows = [r for r in csv.DictReader(f) if r["consolidated"] == "True"]

        best: Dict[str, dict] = {}
        for r in rows:
            if not str(r["quarter"]).isdigit() or int(r["quarter"]) not in (1, 2, 3, 4):
                continue                      # quarter 5 is the audited annual — skip
            p = r["period"]
            rank = self.ASSURANCE_RANK.get(r["assurance"], 9)
            if p not in best or rank < self.ASSURANCE_RANK.get(best[p]["assurance"], 9):
                best[p] = r
        return sorted(best.values(),
                      key=lambda r: (int(r["year"]), int(r["quarter"])))

    # ──────────────────────────────────────────────────────────────────────
    # Gates
    # ──────────────────────────────────────────────────────────────────────

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

    def reconcile(self, st: Statement) -> Optional[str]:
        """None if the statement balances against its OWN printed subtotals, else why not."""
        if len(st.rows) < self.MIN_ROWS:
            return f"only {len(st.rows)} rows parsed"

        if st.report == BALANCE_SHEET:
            assets = st.find(*self.TOTAL_ASSETS)
            resources = st.find(*self.TOTAL_RESOURCES)
            if assets is None:
                return "no total assets"
            if resources is not None and abs(assets - resources) > 2:
                return "assets != liabilities + equity"
            if resources is None:
                liab, eq = st.find(*self.TOTAL_LIABILITIES), st.find(*self.TOTAL_EQUITY)
                if liab is None or eq is None:
                    return "no total to balance against"
                if abs(assets - (liab + eq)) > assets * 0.02:
                    return "assets != liabilities + equity"

        if st.report == INCOME_STATEMENT:
            if st.find(*self.PBT) is None:
                return "no profit before tax"

        if st.report == CASH_FLOW:
            if st.find(*self.NET_CF) is None and st.find(*self.CASH_CLOSE) is None:
                return "no cash-flow subtotal"
        return None

    def sane(self, st: Statement, history: List[int]) -> Optional[str]:
        """Magnitude guard against the quarters already accepted.

        Reconciliation is ratio-based: it cannot see a UNITS error or a CUMULATIVE column,
        because both balance perfectly and are still wrong. This is the only thing that does.
        """
        probe = {BALANCE_SHEET: self.TOTAL_ASSETS,
                 INCOME_STATEMENT: self.PBT,
                 CASH_FLOW: self.CASH_CLOSE}[st.report]
        got = st.find(*probe)
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
              periods: Optional[List[str]] = None) -> Dict[str, int]:
        docs = self.documents(exchange, symbol)
        if periods:
            docs = [d for d in docs if d["period"] in set(periods)]
        self._log(f"cafef financials: {symbol}: {len(docs)} consolidated quarters to parse")

        # report -> period -> {column: value}; and the column order as first seen
        data: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        items: Dict[str, List[str]] = {r: [] for r in REPORTS}
        meta: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        history: Dict[str, List[int]] = {r: [] for r in REPORTS}
        half_year: Dict[str, bool] = {}

        for d in docs:
            period = d["period"]
            half_year[period] = d["half_year"] == "True"
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
                why = self.reconcile(st) or self.sane(st, history[report])
                if why:
                    notes.append(f"{report}=REJECTED({why})")
                    continue

                row: dict = {}
                for r in st.rows:
                    if r.values[0] is None:
                        continue
                    col = self._canonical(r.key, items[report])
                    if col in row:            # the same label printed twice in one statement
                        n = 2
                        while f"{col}__{n}" in row:
                            n += 1
                        col = f"{col}__{n}"
                    row[col] = r.values[0]
                    if col not in items[report]:
                        items[report].append(col)

                data[report][period] = row
                meta[report][period] = {
                    "assurance": d["assurance"], "unit": st.unit,
                    "n_columns": st.n_columns, "document": d["file"],
                }
                probe = {BALANCE_SHEET: self.TOTAL_ASSETS,
                         INCOME_STATEMENT: self.PBT,
                         CASH_FLOW: self.CASH_CLOSE}[report]
                v = st.find(*probe)
                if v is not None:
                    history[report].append(v)
                notes.append(f"{report}={len(row)} items")
            self._log(f"  {period:<8} {'; '.join(notes)}")

        self._decumulate(data, half_year)
        return self._write(exchange, symbol, data, items, meta)

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
               meta: Dict[str, Dict[str, dict]]) -> Dict[str, int]:
        """One CSV per report. The quarter grid is CONTIGUOUS — a quarter we could not read
        is emitted as a blank `source=missing` row, never skipped and never zero-filled."""
        counts = {}
        for report in REPORTS:
            rows = data[report]
            if not rows:
                counts[report] = 0
                continue
            ys = sorted((int(p.split("-")[1]), int(p[1])) for p in rows)
            (y0, q0), (y1, q1) = ys[0], ys[-1]

            out = []
            y, q = y0, q0
            while (y, q) <= (y1, q1):
                period = f"Q{q}-{y}"
                m = meta[report].get(period, {})
                row = {"symbol": symbol, "exchange": exchange, "period": period,
                       "year": y, "quarter": q,
                       "source": "pdf" if period in rows else "missing",
                       "assurance": m.get("assurance", ""), "unit": m.get("unit", ""),
                       "n_columns": m.get("n_columns", ""),
                       "document": m.get("document", "")}
                row.update(rows.get(period, {}))
                out.append(row)
                y, q = (y + 1, 1) if q == 4 else (y, q + 1)

            path = os.path.join(OUT_DIR, report, f"{exchange}_{symbol}.csv")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            head = DATA_COLS + items[report]
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
