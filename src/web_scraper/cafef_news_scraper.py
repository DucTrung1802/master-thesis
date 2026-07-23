# src\web_scraper\cafef_news_scraper.py

# ===== Standard Library =====
import html
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

# ===== Local / Custom Modules =====
from logger.logger import Logger
from utils.constants import (
    CAFEF_RAW_DATA_DIR,
    SCRAPER_RETRY_ATTEMPTS,
    SCRAPER_RETRY_DELAY,
    SCRAPER_MAX_WORKERS,
)
from dtos.thread_manager_dtos.task import Task
from web_scraper.base_scraper import register_scraper
from web_scraper.cafef_scraper import CafeFScraper


@register_scraper
class CafeFNewsScraper(CafeFScraper):
    """Per-stock company-news and disclosure feed from CafeF, with article content.

        news/<EXCHANGE>_<SYMBOL>.csv
        (order, timestamp, type, headline, category, content, url, pdf_url)

    A point-in-time event stream: headline counts, event flags, announcement dates and
    article text, joinable to prices without look-ahead.

    Two stages. (1) List headlines: every category tab of
    cafef.vn/du-lieu/tin-doanh-nghiep/<sym>/event.chn hits one AJAX endpoint with a
    different configID; PageSize caps at 30, so each category is paginated until a page
    repeats or runs short. Categories 1..5 are scraped first (they give the TRUE
    category), then category 0 backfills whatever is left uncategorised; rows dedup by
    URL. (2) Fetch each article and extract its body.

    `type` (editorial | disclosure | error) is provenance — a disclosure article is a
    filing CafeF republished, and is where `pdf_url` comes from; `category` is topic.
    They are orthogonal, so both are kept.

    Article bodies are fetched in parallel (this is the expensive stage — VCB alone has
    ~1,600 articles), with a small delay per worker so CafeF is not hammered.
    """

    SOURCE_NAME = "cafef_news"

    LIST_ENDPOINT = "https://cafef.vn/du-lieu/Ajax/Events_RelatedNews_New.aspx"
    PAGE_SIZE = 30              # server caps here
    MAX_PAGES = 300             # safety stop (~9,000 items)
    ARTICLE_WORKERS = 8         # parallel article fetches per ticker
    FETCH_DELAY = 0.15          # seconds each worker waits between fetches

    COLUMNS = ["order", "timestamp", "type", "headline", "category",
               "content", "url", "pdf_url"]

    # configID (the CafeF tab) -> english snake_case category
    CATEGORY_EN = {
        0: "general_uncategorized",
        1: "business_results_and_analysis",
        2: "dividends_and_record_date",
        3: "personnel_changes",
        4: "capital_increase_and_treasury_shares",
        5: "major_and_insider_shareholder_transactions",
    }

    # <li>: <span class="timeTitle">DD/MM/YYYY HH:MM</span> ... <a ...>TITLE</a>
    # The title comes from the anchor's INNER TEXT — the title="" attribute breaks on
    # the embedded quotes in legacy headlines.
    ITEM_RE = re.compile(
        r'timeTitle">\s*(?P<dt>\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2})?)\s*</span>'
        r'.*?<a\s+class=[\'"]docnhanhTitle[\'"]\s+href="(?P<url>[^"]+)"[^>]*>(?P<title>.*?)</a>',
        re.S,
    )
    TAG_RE = re.compile(r"<[^>]+>")

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

    # ──────────────────────────────────────────────────────────────────────
    # HTML helpers
    # ──────────────────────────────────────────────────────────────────────

    @classmethod
    def _text(cls, s: str) -> str:
        s = re.sub(r"<(script|style)\b.*?</\1>", " ", s, flags=re.S | re.I)
        return re.sub(r"\s+", " ", html.unescape(cls.TAG_RE.sub(" ", s))).strip()

    @staticmethod
    def _clean_url(u: str) -> str:
        u = re.sub(r"[?&]utm_source=[^&]*", "", u)
        return "https://cafef.vn" + u if u.startswith("/") else u

    @staticmethod
    def _balanced_div(h: str, attr: str, val: str) -> Optional[str]:
        """The inner HTML of the first <div> whose `attr` contains `val`, matched by
        counting nested <div> opens/closes (a non-greedy regex would stop at the first
        </div>, which lands mid-article)."""
        m = re.search(r'<div[^>]*' + attr + r'="[^"]*' + re.escape(val) + r'[^"]*"[^>]*>',
                      h, re.I)
        if not m:
            return None
        i, depth = m.end(), 1
        for mm in re.finditer(r"<div\b|</div>", h[i:], re.I):
            depth += 1 if mm.group().lower() == "<div" else -1
            if depth == 0:
                return h[i:i + mm.start()]
        return h[i:]

    @staticmethod
    def _jsonld(h: str) -> dict:
        for ld in re.findall(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', h, re.S):
            try:
                d = json.loads(ld)
            except Exception:
                continue
            for it in (d if isinstance(d, list) else [d]):
                if isinstance(it, dict) and "NewsArticle" in str(it.get("@type", "")):
                    return it
        return {}

    @staticmethod
    def _norm_ts(pub: str, mod: str, listing_dt: str) -> str:
        """The article's own timestamp when it has one, else the listing's DD/MM/YYYY."""
        for s in (pub, mod):
            if s:
                s = s[:19].replace("T", " ")
                return s if len(s) == 19 else (s + ":00")
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}))?", listing_dt or "")
        if m:
            d, mo, y, hh, mm = m.groups()
            return f"{y}-{mo}-{d} {hh or '00'}:{mm or '00'}:00"
        return listing_dt or ""

    # ──────────────────────────────────────────────────────────────────────
    # Stage 1 — headline listing
    # ──────────────────────────────────────────────────────────────────────

    def _referer(self, symbol: str) -> str:
        return f"https://cafef.vn/du-lieu/tin-doanh-nghiep/{symbol.lower()}/event.chn"

    def _fetch(self, url: str, referer: str) -> str:
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(url, headers={"Referer": referer}, timeout=30)
                r.encoding = "utf-8"
                return r.text
            except Exception:
                if attempt == self._retry_attempts:
                    raise
                time.sleep(self._retry_delay)
        return ""

    def _fetch_list(self, symbol: str, config_id: int, page: int) -> str:
        url = (f"{self.LIST_ENDPOINT}?symbol={symbol}&floorID=0&configID={config_id}"
               f"&PageIndex={page}&PageSize={self.PAGE_SIZE}&Type=2")
        html_txt = self._fetch(url, self._referer(symbol))
        time.sleep(0.2)
        return html_txt

    def _parse_list(self, html_txt: str, config_id: int) -> List[dict]:
        out = []
        for m in self.ITEM_RE.finditer(html_txt):
            out.append({
                "datetime": re.sub(r"\s+", " ", m.group("dt")).strip(),
                "category_id": config_id,
                "title": self._text(m.group("title")),
                "url": self._clean_url(m.group("url")),
            })
        return out

    def _scrape_category(self, symbol: str, config_id: int) -> List[dict]:
        items, seen = [], set()
        for page in range(1, self.MAX_PAGES + 1):
            rows = self._parse_list(self._fetch_list(symbol, config_id, page), config_id)
            fresh = [r for r in rows if r["url"] not in seen]
            if not rows or not fresh:
                break
            for r in fresh:
                seen.add(r["url"])
                items.append(r)
            if len(rows) < self.PAGE_SIZE:
                break
        return items

    def build_headlines(self, symbol: str) -> List[dict]:
        """Every headline for the ticker, newest first, one row per URL. Categories 1..5
        first so a row keeps its TRUE category; 0 only backfills what is left over."""
        by_url = {}
        for cid in (1, 2, 3, 4, 5):
            for r in self._scrape_category(symbol, cid):
                by_url.setdefault(r["url"], r)
        for r in self._scrape_category(symbol, 0):
            by_url.setdefault(r["url"], r)

        def key(r):                       # chronological: yyyy, mm, dd, hh:mm
            parts = r["datetime"].split(" ")
            dd, mm, yy = parts[0].split("/")
            return (yy, mm, dd, parts[1] if len(parts) > 1 else "")

        return sorted(by_url.values(), key=key, reverse=True)

    # ──────────────────────────────────────────────────────────────────────
    # Stage 2 — article content
    # ──────────────────────────────────────────────────────────────────────

    def _extract(self, symbol: str, row: dict) -> dict:
        r = self._session.get(row["url"], headers={"Referer": self._referer(symbol)},
                              timeout=30)   # follows the 301 redirect
        r.encoding = "utf-8"
        h, final = r.text, r.url
        time.sleep(self.FETCH_DELAY)

        meta = self._jsonld(h)
        # A disclosure article redirects into the ticker's own data section
        # (/du-lieu/<sym>-...); an editorial one stays in the newsroom.
        is_disclosure = f"/du-lieu/{symbol.lower()}-" in final.lower()
        headline = html.unescape(html.unescape(meta.get("headline", ""))) or row["title"]

        pdf_url = ""
        if is_disclosure:
            content = self._text(self._balanced_div(h, "id", "divContent") or "")
            pdfs = sorted(set(re.findall(r'https?://[^"]*mediacdn[^"]*\.pdf', h, re.I)))
            pdf_url = pdfs[0] if pdfs else ""
        else:
            inner = self._balanced_div(h, "class", "detail-content") or ""
            paras = [self._text(p) for p in re.findall(r"<p\b[^>]*>(.*?)</p>", inner, re.S)]
            content = " ".join(p for p in paras if p)
            if len(content) < 50:      # legacy articles: <br>-separated, not in <p>
                full = self._text(inner)
                m = re.search(r"TIN M[ỚO]I\s+", full)   # cut the price-quote widget
                content = full[m.end():].strip() if m else full

        return {
            "timestamp": self._norm_ts(meta.get("datePublished", ""),
                                       meta.get("dateModified", ""),
                                       row.get("datetime", "")),
            "type": "disclosure" if is_disclosure else "editorial",
            "headline": headline,
            "category": self.CATEGORY_EN.get(row["category_id"], "general_uncategorized"),
            "content": content,
            "url": row["url"],
            "pdf_url": pdf_url,
        }

    def _extract_safe(self, symbol: str, row: dict) -> dict:
        """A dead link must not lose the headline — the row is kept with type=error."""
        try:
            return self._extract(symbol, row)
        except Exception as e:
            return {
                "timestamp": self._norm_ts("", "", row.get("datetime", "")),
                "type": "error", "headline": row.get("title", ""),
                "category": self.CATEGORY_EN.get(row["category_id"], "general_uncategorized"),
                "content": f"[ERROR: {e}]", "url": row["url"], "pdf_url": "",
            }

    # ──────────────────────────────────────────────────────────────────────
    # Per-stock scrape
    # ──────────────────────────────────────────────────────────────────────

    def scrape_news(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        folder = os.path.join(CAFEF_RAW_DATA_DIR, "news")
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{exchange}_{symbol}.csv")

        if skip_existing and os.path.exists(file_path):
            self._logger.log_info(f"CafeF news: '{symbol}' already scraped, skipping.")
            return

        self._logger.log_info(f"CafeF news: listing headlines for '{exchange}:{symbol}'...")
        headlines = self.build_headlines(symbol)
        if not headlines:
            self._logger.log_warning(f"CafeF news: no headlines for '{symbol}', skipping.")
            return
        headlines = headlines[::-1]        # oldest first -> order 1 = the oldest news

        self._logger.log_info(
            f"CafeF news: {symbol}: {len(headlines)} headlines, fetching articles...")
        with ThreadPoolExecutor(max_workers=self.ARTICLE_WORKERS) as pool:
            records = list(pool.map(lambda r: self._extract_safe(symbol, r), headlines))

        # Number the rows from the timestamps we ended up with, not from the order the
        # headline feed listed them in. The listing carries a minute-precision date, but the
        # article's own datePublished supersedes it — and the two disagree, so ordering by
        # the listing left `order` claiming a chronology the timestamps did not have (PNJ
        # had a pair five days out of sequence). order 1 is now genuinely the oldest.
        records.sort(key=lambda r: (r["timestamp"], r["url"]))
        for i, rec in enumerate(records, 1):
            rec["order"] = i

        self._write_csv(file_path, self.COLUMNS, records)
        n_pdf = sum(1 for r in records if r["pdf_url"])
        n_err = sum(1 for r in records if r["type"] == "error")
        self._logger.log_info(
            f"CafeF news: {symbol}: saved {len(records)} rows "
            f"({records[0]['timestamp'][:10]}..{records[-1]['timestamp'][:10]}), "
            f"{n_pdf} with pdf_url, {n_err} errors -> {file_path}"
        )

    def _write_csv(self, file_path: str, columns: List[str], rows: List[dict]) -> None:
        """Temp-file + atomic replace, so an interrupted run never leaves a partial CSV."""
        import csv
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, file_path)

    # ──────────────────────────────────────────────────────────────────────
    # Batch driver
    # ──────────────────────────────────────────────────────────────────────

    def scrape(self, exchanges: Tuple[str, ...] = None,
               symbols: List[Tuple[str, str]] = None) -> None:
        self.scrape_all_news(exchanges=exchanges, symbols=symbols)

    def scrape_all_news(self, skip_existing: bool = True,
                        exchanges: Tuple[str, ...] = None,
                        symbols: List[Tuple[str, str]] = None) -> None:
        """`symbols` overrides the universe, so a run can be scoped to VN30/VN100 rather
        than all ~777 listed codes."""
        universe = symbols if symbols is not None else self.get_stock_symbols(exchanges)
        self._thread_manager.remove_all_tasks()
        for exchange, symbol in universe:
            self._thread_manager.add_task(
                Task(f"cafef/news/{exchange}_{symbol}",
                     self.scrape_news, exchange, symbol, skip_existing)
            )
        self._logger.log_info(
            f"CafeF: executing {self._thread_manager.get_current_number_of_task()} "
            f"news scraping tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("CafeF: finished scraping all news.")
