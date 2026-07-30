# src\web_scraper\cafef_pdf_scraper.py

# ===== Standard Library =====
import csv
import hashlib
import os
import re
import time
import unicodedata
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import List, Optional, Tuple

# ===== Third-Party Libraries =====
import requests

# ===== Local / Custom Modules =====
from logger.logger import Logger
from utils.constants import (
    CAFEF_PDF_TICKERS,
    CAFEF_RAW_DATA_DIR,
    TRADING_VIEW_RAW_DATA_DIR,
    SCRAPER_RETRY_ATTEMPTS,
    SCRAPER_RETRY_DELAY,
    SCRAPER_MAX_WORKERS,
)
from dtos.thread_manager_dtos.task import Task
from web_scraper.base_scraper import BaseScraper, register_scraper


@register_scraper
class CafeFPdfScraper(BaseScraper):
    """Download every financial-report PDF CafeF holds for a ticker. Nothing else.

        pdfs/index/<EXCHANGE>_<SYMBOL>.csv     one row per document (the index)
        pdfs/files/<EXCHANGE>_<SYMBOL>/*.pdf   the documents themselves

    Index and documents live in separate trees so the archive stays navigable across a whole
    universe: at VN100 scale a flat folder would interleave 100 CSVs with 100 directories.
    `index/` then mirrors every other CafeF folder — one `<EXCHANGE>_<SYMBOL>.csv` per stock,
    which is the shape `data_preprocessor` already ingests — while the ~1.7 GB-per-ticker of
    binaries sits apart under `files/`.

    This scraper only FETCHES — it never opens, parses or OCRs a PDF, and needs no PDF
    library at all. It just lands the archive, so that reading the documents is a separate,
    offline, repeatable step.

    The PDFs are the primary source: CafeF's JSON API is a transcription of them, and where
    the API has a gap (VCB is missing 20 statement-quarters) the PDF is the only place those
    figures exist.

    Source — one endpoint, the same one the disclosure-date scraper reads:
        cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol=<sym>&Type=1&Year=0
    It lists every document for the code (206 for VCB, back to 2002) with a link to the PDF.

    Each index row records what the document IS, which a caller must know before trusting
    its numbers — all three of these are traps that have already bitten:

      * `consolidated` — "hợp nhất". The parent-company ("công ty mẹ") report covers a
        different entity and carries different figures; taking one for the other yields a
        coherent statement about the wrong thing. CafeF lists both, ~50/50.
      * `assurance`    — audited ("kiểm toán") > reviewed ("soát xét") > unaudited. A
        quarter is often filed twice and the later document restates the earlier.
      * `half_year`    — the semi-annual report prints ONLY the cumulative Jan-Jun column,
        so its income statement is not the standalone quarter (VCB Q2-2024 prints PBT
        20,835bn where the quarter is 10,116bn).

    Two fetch quirks: the API advertises `cafefnew.mediacdn.vn` but older files live only on
    `cafef1.mediacdn.vn`, so both hosts are tried; and downloads are verified against
    Content-Length, because a short read does not raise — it yields a PDF that still opens
    and still reports its true page count, with the missing pages failing one by one. That
    silently cost the back half of VCB's Q2-2014 filing.
    """

    SOURCE_NAME = "cafef_pdf"

    DOCS_URL = "https://cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol={sym}&Type=1&Year=0"
    VN_EXCHANGES = ("HOSE", "HNX", "UPCOM")
    FOLDER = "pdfs"
    INDEX_DIR = "index"          # pdfs/index/<EXCHANGE>_<SYMBOL>.csv
    FILES_DIR = "files"          # pdfs/files/<EXCHANGE>_<SYMBOL>/*.pdf
    DOWNLOAD_WORKERS = 6

    COLUMNS = ["symbol", "exchange", "year", "quarter", "period", "name",
               "consolidated", "assurance", "half_year", "file_date",
               "bytes", "file", "path", "url"]

    # The disclosure date CafeF embeds in the filename. Only files from ~2022 on carry one.
    LINK_DATE_PRE = re.compile(r"/BCTC/(\d{8})_")
    LINK_DATE_EMB = re.compile(r"(\d{2})(\d{2})(\d{4})-?\d{6}")

    def __init__(
        self,
        logger: Logger,
        switch_handler=None,
        power: int = 30,
        retry_attempts: int = SCRAPER_RETRY_ATTEMPTS,
        retry_delay: float = SCRAPER_RETRY_DELAY,
        max_workers: int = SCRAPER_MAX_WORKERS,
    ):
        super().__init__(
            logger=logger,
            switch_handler=switch_handler,
            power=power,
            retry_attempts=retry_attempts,
            retry_delay=retry_delay,
            max_workers=max_workers,
        )
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/123.0.0.0 Safari/537.36"),
            "Referer": "https://cafef.vn/du-lieu/",
        })

    # ──────────────────────────────────────────────────────────────────────
    # HTTP
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _encode_url(url: str) -> str:
        """Percent-encode the path — some filing paths contain spaces, which raise
        InvalidURL if passed through raw."""
        p = urllib.parse.urlsplit(url)
        return urllib.parse.urlunsplit(
            (p.scheme, p.netloc, urllib.parse.quote(p.path), p.query, p.fragment))

    def documents(self, symbol: str) -> List[dict]:
        """Every financial-report document CafeF lists for the ticker."""
        url = self.DOCS_URL.format(sym=symbol.lower())
        referer = f"https://cafef.vn/du-lieu/hose/{symbol.lower()}-tai-lieu.chn"
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(url, headers={"Referer": referer}, timeout=60)
                return r.json().get("Data") or []
            except Exception as e:
                if attempt == self._retry_attempts:
                    self._logger.log_warning(
                        f"CafeF pdf: document list failed for '{symbol}': {e}")
                    return []
                time.sleep(self._retry_delay)
        return []

    def _download(self, url: str) -> Optional[bytes]:
        """Fetch one PDF, verifying the response is COMPLETE (see the class docstring).

        A 4xx is NOT retried: it is the server's final answer, and re-asking five times with
        a delay between each only postpones the fallback host that does have the file. Whole
        years of VIC live only on cafef1 — every one of those 404s on cafefnew was costing 25
        seconds before the working URL was even tried. Timeouts and 5xx still retry.
        """
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(self._encode_url(url), timeout=300)
                if 400 <= r.status_code < 500:
                    return None               # permanent -> let the caller try the next host
                r.raise_for_status()
                raw = r.content
                expected = r.headers.get("Content-Length")
                if expected and len(raw) != int(expected):
                    raise IOError(f"truncated: {len(raw)} of {expected} bytes")
                return raw
            except Exception as e:
                if attempt == self._retry_attempts:
                    self._logger.log_warning(f"CafeF pdf: download failed {url}: {e}")
                    return None
                time.sleep(self._retry_delay)
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Classification (all of it from CafeF's own listing — no PDF is opened)
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _slug(name: str, maxlen: int = 60) -> str:
        s = (name or "").replace("đ", "d").replace("Đ", "D")
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
        return s[:maxlen].rstrip("_") or "report"

    def _base_name(self, d: dict) -> str:
        """The filename a document wants, before any collision is resolved."""
        year, quarter = d.get("Year"), d.get("Quarter")
        period = (f"Q{quarter}-{year}" if quarter in (1, 2, 3, 4)
                  else f"FY-{year}" if quarter == 5 else f"NA-{year}")
        return f"{period}_{self._slug((d.get('Name') or '').strip())}.pdf"

    @staticmethod
    def _url_tag(link: str) -> str:
        """A short, stable discriminator for two documents CafeF gives the same title."""
        return hashlib.sha1((link or "").encode("utf-8")).hexdigest()[:8]

    @classmethod
    def _classify(cls, name: str, link: str, quarter) -> Tuple[bool, str, bool]:
        """-> (consolidated, assurance, half_year).

        `half_year` cannot be read from the title alone: CafeF calls the semi-annual report
        "Báo cáo tài chính hợp nhất quý 2 năm 2024 (đã soát xét)" — it says *quý 2*, never
        "6 tháng". The half-year nature shows only in the URL ("…bn-nin…" = bán niên,
        "…6T_2025…") and in what the document IS: a REVIEWED Q2 filing is the semi-annual
        report by definition, and it prints only the cumulative Jan-Jun column.
        """
        t = (name or "").lower()
        u = (link or "").lower()
        consolidated = "hợp nhất" in t
        if "kiểm toán" in t:
            assurance = "audited"
        elif "soát xét" in t:
            assurance = "reviewed"
        else:
            assurance = "unaudited"
        half_year = bool(
            "6 tháng" in t or "bán niên" in t
            or re.search(r"6t[_\-]|ban[_\-]?nien|bn[_\-]nin|banni", u)
            or (quarter == 2 and assurance == "reviewed")   # the semi-annual, by definition
        )
        return consolidated, assurance, half_year

    def _link_date(self, link: str) -> str:
        m = self.LINK_DATE_PRE.search(link or "")
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d").date().isoformat()
            except ValueError:
                pass
        for m in self.LINK_DATE_EMB.finditer(link or ""):
            dd, mm, yyyy = m.groups()
            try:
                return date(int(yyyy), int(mm), int(dd)).isoformat()
            except ValueError:
                continue
        return ""

    # ──────────────────────────────────────────────────────────────────────
    # Per-stock scrape
    # ──────────────────────────────────────────────────────────────────────

    def scrape_pdfs(self, exchange: str, symbol: str, skip_existing: bool = True,
                    years: Optional[Tuple[int, ...]] = None) -> None:
        """`years` limits the download to those filing years. A full archive is ~1.7 GB per
        ticker, so pulling one year across a wide universe is the difference between a few GB
        and tens of them."""
        base = os.path.join(CAFEF_RAW_DATA_DIR, self.FOLDER)
        pdf_dir = os.path.join(base, self.FILES_DIR, f"{exchange}_{symbol}")
        index_dir = os.path.join(base, self.INDEX_DIR)
        os.makedirs(pdf_dir, exist_ok=True)
        os.makedirs(index_dir, exist_ok=True)

        docs = self.documents(symbol)
        if not docs:
            self._logger.log_warning(f"CafeF pdf: no documents for '{symbol}'.")
            return
        listed = len(docs)
        if years:
            wanted = set(years)
            docs = [d for d in docs if d.get("Year") in wanted]
        self._logger.log_info(
            f"CafeF pdf: '{exchange}:{symbol}' — {listed} documents listed"
            + (f", {len(docs)} in {sorted(years)}" if years else ""))

        # Name every file BEFORE fetching anything, so a collision can be seen. CafeF lists
        # the same document twice under an identical title when a filing is re-uploaded (VCB
        # has two "Báo cáo tài chính hợp nhất quý 4 năm 2023"), and both slugify to the same
        # filename — the second silently overwrote the first, losing a document while the
        # index still claimed 206. Only the colliding names get a URL-hash suffix, so every
        # other file keeps its stable name and is not re-downloaded.
        planned = [(d, self._base_name(d)) for d in docs
                   if (d.get("Link") or "").strip().lower().endswith(".pdf")]
        clashes = {n for n, c in Counter(n for _, n in planned).items() if c > 1}
        names = {
            id(d): (f"{base[:-4]}_{self._url_tag(d['Link'])}.pdf"
                    if base in clashes else base)
            for d, base in planned
        }

        def one(d: dict) -> Optional[dict]:
            link = (d.get("Link") or "").strip()
            if not link.lower().endswith(".pdf"):
                return None                    # a few entries are .rar / .xls
            name = (d.get("Name") or "").strip()
            year, quarter = d.get("Year"), d.get("Quarter")
            # CafeF files the audited annual under quarter 5; keep it, labelled.
            period = (f"Q{quarter}-{year}" if quarter in (1, 2, 3, 4)
                      else f"FY-{year}" if quarter == 5 else f"NA-{year}")
            fname = names[id(d)]
            dest = os.path.join(pdf_dir, fname)

            if skip_existing and os.path.exists(dest) and os.path.getsize(dest) > 0:
                nbytes = os.path.getsize(dest)
            else:
                raw = None
                for url in (link,
                            link.replace("cafefnew.mediacdn.vn", "cafef1.mediacdn.vn")):
                    raw = self._download(url)
                    if raw:
                        break
                if not raw:
                    return None
                tmp = dest + ".tmp"            # atomic: never leave a half file behind
                with open(tmp, "wb") as f:
                    f.write(raw)
                os.replace(tmp, dest)
                nbytes = len(raw)

            consolidated, assurance, half_year = self._classify(name, link, quarter)
            return {
                "symbol": symbol, "exchange": exchange,
                "year": year, "quarter": quarter, "period": period, "name": name,
                "consolidated": consolidated, "assurance": assurance,
                "half_year": half_year, "file_date": self._link_date(link),
                "bytes": nbytes, "file": fname,
                # where the document actually is, relative to raw_data/cafef/pdfs/
                "path": f"{self.FILES_DIR}/{exchange}_{symbol}/{fname}",
                "url": link,
            }

        with ThreadPoolExecutor(max_workers=self.DOWNLOAD_WORKERS) as pool:
            rows = [r for r in pool.map(one, docs) if r]

        index_path = os.path.join(index_dir, f"{exchange}_{symbol}.csv")
        # A year-filtered run must not erase the rest of an index that is already complete —
        # merge on the filename, keeping whatever this run did not look at.
        if years and os.path.exists(index_path):
            with open(index_path, encoding="utf-8-sig") as f:
                fresh = {r["file"] for r in rows}
                rows += [r for r in csv.DictReader(f) if r["file"] not in fresh]
        rows.sort(key=lambda r: (int(r["year"] or 0), int(r["quarter"] or 0), r["name"]))

        tmp = index_path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=self.COLUMNS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, index_path)

        n_cons = sum(1 for r in rows if r["consolidated"])
        n_half = sum(1 for r in rows if r["half_year"])
        mb = sum(r["bytes"] for r in rows) / (1024 * 1024)
        self._logger.log_info(
            f"CafeF pdf: {symbol}: {len(rows)} PDFs ({n_cons} consolidated, "
            f"{n_half} half-year), {mb:.0f} MB -> {pdf_dir}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Universe + batch driver
    # ──────────────────────────────────────────────────────────────────────

    def get_stock_symbols(self, exchanges: Tuple[str, ...] = None) -> List[Tuple[str, str]]:
        """The (exchange, symbol) universe, from the TradingView stock link CSVs."""
        import glob

        wanted = {e.upper() for e in (exchanges or self.VN_EXCHANGES)}
        links_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "links", "stocks")
        seen, out = set(), []
        for path in glob.glob(os.path.join(links_dir, "**", "*.csv"), recursive=True):
            with open(path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    url = row.get("url", "")
                    sym = url.split("symbol=")[-1] if "symbol=" in url else ""
                    if ":" not in sym:
                        continue
                    exchange, ticker = sym.split(":", 1)
                    if exchange.upper() in wanted and (exchange, ticker) not in seen:
                        seen.add((exchange, ticker))
                        out.append((exchange, ticker))
        return sorted(out)

    def scrape(self, exchanges: Tuple[str, ...] = None,
               symbols: List[Tuple[str, str]] = None) -> None:
        """Switch-driven entry point (`web_scraper/cafef/pdfs`).

        Defaults to CAFEF_PDF_TICKERS rather than the full ~777-code universe: the
        archive is ~1.7 GB per ticker, so an unscoped run is a terabyte-scale download,
        not a longer version of the same job. Pass `symbols` to override for a one-off.
        """
        if self._switch_handler and not self._switch_handler.is_enabled(
            "web_scraper", "cafef", "pdfs"
        ):
            return
        self.scrape_all_pdfs(
            exchanges=exchanges,
            symbols=symbols if symbols is not None else list(CAFEF_PDF_TICKERS),
        )

    def scrape_all_pdfs(self, skip_existing: bool = True,
                        exchanges: Tuple[str, ...] = None,
                        symbols: List[Tuple[str, str]] = None,
                        years: Optional[Tuple[int, ...]] = None) -> None:
        """`symbols` overrides the universe, so a run can be scoped to VN30/VN100 rather
        than all ~777 listed codes; `years` scopes it in time. The archive is ~1.7 GB per
        ticker, so both matter."""
        universe = symbols if symbols is not None else self.get_stock_symbols(exchanges)
        self._thread_manager.remove_all_tasks()
        for exchange, symbol in universe:
            self._thread_manager.add_task(
                Task(f"cafef/pdfs/{exchange}_{symbol}",
                     self.scrape_pdfs, exchange, symbol, skip_existing, years)
            )
        self._logger.log_info(
            f"CafeF: executing {self._thread_manager.get_current_number_of_task()} "
            f"pdf scraping tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("CafeF: finished scraping all pdfs.")
