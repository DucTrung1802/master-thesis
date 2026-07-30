# src\web_scraper\simplize_scraper.py

# ===== Standard Library =====
import csv
import glob
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Optional, Tuple

# ===== Third-Party Libraries =====
import requests

# ===== Local / Custom Modules =====
from logger.logger import Logger
from utils.constants import (
    SIMPLIZE_RAW_DATA_DIR,
    TRADING_VIEW_RAW_DATA_DIR,
    SCRAPER_RETRY_ATTEMPTS,
    SCRAPER_RETRY_DELAY,
)
from dtos.thread_manager_dtos.task import Task
from web_scraper.base_scraper import BaseScraper, register_scraper


@register_scraper
class SimplizeScraper(BaseScraper):
    """Scrape per-stock daily data from Simplize (simplize.vn) — the single source
    that fills the whole daily panel correctly for VN stocks: fully dividend-adjusted
    OHLC (CafeF only adjusts the close; TradingView's volume is split-inflated), true
    total traded volume, and foreign buy/sell/net flow (volume + value) plus room.

    Both the price-history tab and the foreign-investor tab on Simplize read the same
    rows, so one endpoint yields everything:

        GET https://api.simplize.vn/api/historical/quote/prices/<SYMBOL>?page=<n>&size=<s>
        -> {"status":200,"total":N,"data":[ {row}, ... ]}   (newest-first)

    Quirks handled: rows are newest-first and paginated (server caps page size at
    1000, so history is fetched page 0..N); `date` is a unix timestamp (UTC midnight).

    Output: one CSV per stock at SIMPLIZE_RAW_DATA_DIR/stocks/<EXCHANGE>_<SYMBOL>.csv.
    """

    SOURCE_NAME = "simplize"
    BASE = "https://api.simplize.vn/api/historical/quote/prices"
    PAGE_SIZE = 1000  # server-side cap; larger values are rejected (401)

    # Per-ticker company industry (GICS-based VN taxonomy): 10 economic sectors /
    # 50 industry groups, accurate per ticker — the source for GICS classification.
    INDUSTRY_BASE = "https://api.simplize.vn/api/company/summary"
    INDUSTRY_COLUMNS = [
        "exchange",
        "ticker",
        "industry_activity",
        "economic_sector_id",
        "economic_sector_slug",
        "economic_sector_name",
        "industry_group_id",
        "industry_group_code",
        "industry_group_slug",
        "industry_group_type",
    ]

    OUTPUT_COLUMNS = [
        "date",
        "exchange",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "net_change",
        "percentage_change",
        "volume",
        "foreign_room",
        "foreign_buy_volume",
        "foreign_sell_volume",
        "foreign_net_volume",
        "foreign_buy_value",
        "foreign_sell_value",
        "foreign_net_value",
    ]

    def __init__(
        self,
        logger: Logger,
        switch_handler=None,
        power: int = 30,
        retry_attempts: int = SCRAPER_RETRY_ATTEMPTS,
        retry_delay: float = SCRAPER_RETRY_DELAY,
    ):
        super().__init__(
            logger=logger,
            switch_handler=switch_handler,
            power=power,
            retry_attempts=retry_attempts,
            retry_delay=retry_delay,
        )
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                "Referer": "https://simplize.vn/",
            }
        )

    # ──────────────────────────────────────────────────────────────────────
    # HTTP helpers
    # ──────────────────────────────────────────────────────────────────────

    def _get_page(self, symbol: str, page: int) -> Tuple[list, int]:
        """Fetch one page for a symbol; return (rows, total). Retried; ([], 0) on failure."""
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(
                    f"{self.BASE}/{symbol}",
                    params={"page": page, "size": self.PAGE_SIZE},
                    timeout=30,
                )
                payload = r.json()
                return payload.get("data") or [], payload.get("total") or 0
            except Exception as e:
                if attempt == self._retry_attempts:
                    self._logger.log_warning(
                        f"Simplize prices failed after {attempt} tries "
                        f"({symbol} page {page}): {e}"
                    )
                    return [], 0
                time.sleep(self._retry_delay)
        return [], 0

    def _collect(self, symbol: str) -> list:
        """Fetch all rows for a symbol across pages (newest-first, deduped by date)."""
        by_date: dict = {}
        page = 0
        total = None
        while True:
            rows, page_total = self._get_page(symbol, page)
            if total is None:
                total = page_total
            if not rows:
                break
            for row in rows:
                by_date[row["date"]] = row
            if len(by_date) >= (total or 0):
                break
            page += 1
        return list(by_date.values())

    # ──────────────────────────────────────────────────────────────────────
    # Parsing
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _num(v):
        return "" if v is None else v

    def _build_rows(self, exchange: str, symbol: str, raw: list) -> List[dict]:
        rows = []
        for d in raw:
            date = datetime.fromtimestamp(d["date"], timezone.utc).date().isoformat()
            rows.append(
                {
                    "date": date,
                    "exchange": exchange,
                    "symbol": symbol,
                    "open": self._num(d.get("priceOpen")),
                    "high": self._num(d.get("priceHigh")),
                    "low": self._num(d.get("priceLow")),
                    "close": self._num(d.get("priceClose")),
                    "net_change": self._num(d.get("netChange")),
                    "percentage_change": self._num(d.get("pctChange")),
                    "volume": self._num(d.get("volume")),
                    "foreign_room": self._num(d.get("cfr")),
                    "foreign_buy_volume": self._num(d.get("bfq")),
                    "foreign_sell_volume": self._num(d.get("sfq")),
                    "foreign_net_volume": self._num(d.get("fnbsq")),
                    "foreign_buy_value": self._num(d.get("bfv")),
                    "foreign_sell_value": self._num(d.get("sfv")),
                    "foreign_net_value": self._num(d.get("fnbsv")),
                }
            )
        rows.sort(key=lambda r: r["date"])
        return rows

    # ──────────────────────────────────────────────────────────────────────
    # Per-stock scrape
    # ──────────────────────────────────────────────────────────────────────

    def scrape_stock(
        self, exchange: str, symbol: str, skip_existing: bool = True
    ) -> None:
        folder = os.path.join(SIMPLIZE_RAW_DATA_DIR, "stocks")
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{exchange}_{symbol}.csv")

        if skip_existing and os.path.exists(file_path):
            self._logger.log_info(f"Simplize: '{symbol}' already scraped, skipping.")
            return

        self._logger.log_info(f"Simplize: scraping '{exchange}:{symbol}'...")
        raw = self._collect(symbol)
        if not raw:
            self._logger.log_warning(f"Simplize: no data for '{symbol}', skipping.")
            return
        rows = self._build_rows(exchange, symbol, raw)

        # Write to a temp file then atomically rename, so an interrupted run never
        # leaves a partial CSV that skip_existing would wrongly treat as complete.
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.OUTPUT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, file_path)
        self._logger.log_info(
            f"Simplize: saved {len(rows)} rows ({rows[0]['date']}..{rows[-1]['date']}) "
            f"-> {file_path}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Stock universe + batch driver
    # ──────────────────────────────────────────────────────────────────────

    def get_stock_symbols(self) -> List[Tuple[str, str]]:
        """Derive the (exchange, symbol) list from the TradingView stock link CSVs."""
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
                    if (exchange, ticker) not in seen:
                        seen.add((exchange, ticker))
                        out.append((exchange, ticker))
        self._logger.log_info(
            f"Simplize: {len(out)} stock symbols found from TradingView links."
        )
        return sorted(out)

    # ──────────────────────────────────────────────────────────────────────
    # Per-ticker industry (GICS-based VN taxonomy) — for GICS classification
    # ──────────────────────────────────────────────────────────────────────

    def _fetch_industry(self, exchange: str, ticker: str) -> Optional[dict]:
        """Fetch one ticker's company-summary industry fields (retried)."""
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(f"{self.INDUSTRY_BASE}/{ticker}", timeout=20)
                if r.status_code == 404:
                    return None  # genuinely not on Simplize
                if r.status_code != 200:
                    # Rate-limit / transient server error — back off and retry.
                    if attempt == self._retry_attempts:
                        self._logger.log_warning(
                            f"Simplize industry {ticker}: status {r.status_code} "
                            f"after {attempt} tries."
                        )
                        return None
                    time.sleep(self._retry_delay)
                    continue
                d = r.json().get("data") or {}
                if not d.get("bcEconomicSectorSlug"):
                    return None
                return {
                    "exchange": exchange,
                    "ticker": ticker,
                    "industry_activity": d.get("industryActivity"),
                    "economic_sector_id": d.get("bcEconomicSectorId"),
                    "economic_sector_slug": d.get("bcEconomicSectorSlug"),
                    "economic_sector_name": d.get("bcEconomicSectorName"),
                    "industry_group_id": d.get("bcIndustryGroupId"),
                    "industry_group_code": d.get("bcIndustryGroupCode"),
                    "industry_group_slug": d.get("bcIndustryGroupSlug"),
                    "industry_group_type": d.get("bcIndustryGroupType"),
                }
            except Exception as e:
                if attempt == self._retry_attempts:
                    self._logger.log_warning(
                        f"Simplize industry failed after {attempt} tries ({ticker}): {e}"
                    )
                    return None
                time.sleep(self._retry_delay)
        return None

    def scrape_all_industries(self, workers: int = 8) -> None:
        """Scrape per-ticker GICS-based industry for the whole universe into one
        reference CSV at SIMPLIZE_RAW_DATA_DIR/industry.csv (one row per ticker)."""
        symbols = self.get_stock_symbols()
        self._logger.log_info(
            f"Simplize: scraping industry for {len(symbols)} tickers..."
        )

        rows: List[dict] = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(self._fetch_industry, exchange, ticker): ticker
                for exchange, ticker in symbols
            }
            for fut in as_completed(futures):
                rec = fut.result()
                if rec is not None:
                    rows.append(rec)

        if not rows:
            self._logger.log_warning("Simplize: no industry data scraped.")
            return

        rows.sort(key=lambda r: (r["exchange"], r["ticker"]))
        os.makedirs(SIMPLIZE_RAW_DATA_DIR, exist_ok=True)
        file_path = os.path.join(SIMPLIZE_RAW_DATA_DIR, "industry.csv")
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.INDUSTRY_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, file_path)
        self._logger.log_info(
            f"Simplize: saved industry for {len(rows)} tickers -> {file_path}"
        )

    def scrape(self) -> None:
        """BaseScraper entry point: the daily price panel and the industry reference.

        Two independent leaves — `industry.csv` is one row per ticker and takes minutes,
        the price panel is 2.6 M rows and takes hours, so they are rarely wanted together.
        Industry runs first: it is what `templates.csv` and the GICS tree read.
        """

        if self._switch_handler.is_enabled("web_scraper", "simplize", "industry"):
            self.scrape_all_industries()

        if self._switch_handler.is_enabled("web_scraper", "simplize", "stocks"):
            self.scrape_all_stocks()

    def scrape_all_stocks(self, skip_existing: bool = True) -> None:
        symbols = self.get_stock_symbols()
        self._thread_manager.remove_all_tasks()
        for exchange, symbol in symbols:
            self._thread_manager.add_task(
                Task(
                    f"simplize/stocks/{exchange}_{symbol}",
                    self.scrape_stock,
                    exchange,
                    symbol,
                    skip_existing,
                )
            )
        self._logger.log_info(
            f"Simplize: executing {self._thread_manager.get_current_number_of_task()} "
            f"stock scraping tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("Simplize: finished scraping all stocks.")
