# src\web_scraper\cafef_scraper.py

# ===== Standard Library =====
import calendar
import csv
import glob
import os
import time
from datetime import date, datetime, timedelta
from typing import List, Tuple

# ===== Third-Party Libraries =====
import requests

# ===== Local / Custom Modules =====
from logger.logger import Logger
from utils.constants import (
    CAFEF_RAW_DATA_DIR,
    TRADING_VIEW_RAW_DATA_DIR,
    SCRAPER_RETRY_ATTEMPTS,
    SCRAPER_RETRY_DELAY,
)
from dtos.thread_manager_dtos.task import Task
from web_scraper.base_scraper import BaseScraper, register_scraper


@register_scraper
class CafeFScraper(BaseScraper):
    """Scrape per-stock daily data from CafeF (cafef.vn/du-lieu) — the fields
    TradingView lacks: raw + adjusted close, matched & negotiated (block) volume,
    and foreign buy/sell flow (volume/value, net, remaining room, ownership %).

    CafeF quirks handled: StartDate/EndDate are parsed MM/dd/yyyy (US); a query is
    capped at ~63 rows and PageSize at 20, so history is fetched in ~2-month windows
    (kept under the cap, overlapped to avoid boundary gaps) and paginated.

    Output: one CSV per stock at CAFEF_RAW_DATA_DIR/stocks/<EXCHANGE>_<SYMBOL>.csv.
    """

    SOURCE_NAME = "cafef"
    BASE = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/"
    PRICE_START_YEAR = 2009
    FOREIGN_START_YEAR = 2012  # CafeF foreign-flow history does not go back further

    OUTPUT_COLUMNS = [
        "date", "exchange", "symbol",
        "open", "high", "low", "close_raw", "close_adj",
        "vol_matched", "val_matched_bn", "vol_negotiated", "val_negotiated_bn",
        "f_buy_vol", "f_buy_val", "f_sell_vol", "f_sell_val",
        "f_net_vol", "f_net_val", "room_left", "own_pct",
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
        self._session.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/123.0.0.0 Safari/537.36"),
            "Referer": "https://cafef.vn/du-lieu/",
        })

    # ──────────────────────────────────────────────────────────────────────
    # HTTP helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _windows(start_year: int, step_months: int = 2, overlap_days: int = 6):
        """Yield (StartDate, EndDate) MM/dd/yyyy windows of ~step_months, each
        overlapped a few days into the next so no boundary trading day is lost.
        Span stays under CafeF's ~63-row cap."""
        today = date.today()
        y, m = start_year, 1
        while date(y, m, 1) <= today:
            sd = date(y, m, 1)
            em = m + step_months - 1
            ey, em = y + (em - 1) // 12, (em - 1) % 12 + 1
            ed = date(ey, em, calendar.monthrange(ey, em)[1]) + timedelta(days=overlap_days)
            yield sd.strftime("%m/%d/%Y"), ed.strftime("%m/%d/%Y")
            nm = m + step_months
            y, m = y + (nm - 1) // 12, (nm - 1) % 12 + 1

    def _get(self, ashx: str, params: dict) -> list:
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(self.BASE + ashx, params=params, timeout=30)
                return r.json()["Data"]["Data"]
            except Exception as e:
                if attempt == self._retry_attempts:
                    self._logger.log_warning(
                        f"CafeF {ashx} failed after {attempt} tries "
                        f"({params.get('Symbol')} {params.get('StartDate')}): {e}"
                    )
                    return []
                time.sleep(self._retry_delay)
        return []

    def _collect(self, ashx: str, symbol: str, start_year: int) -> dict:
        """Fetch all rows for a symbol across windows, keyed by Ngay (dedup)."""
        by_date: dict = {}
        for sd, ed in self._windows(start_year):
            page = 1
            while page <= 6:
                rec = self._get(ashx, {"Symbol": symbol, "StartDate": sd, "EndDate": ed,
                                       "PageIndex": page, "PageSize": 20})
                if not rec:
                    break
                for row in rec:
                    by_date[row["Ngay"]] = row
                if len(rec) < 20:
                    break
                page += 1
        return by_date

    # ──────────────────────────────────────────────────────────────────────
    # Parsing
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _num(v):
        try:
            return float(str(v).replace(",", "")) if v not in (None, "") else ""
        except (ValueError, TypeError):
            return ""

    def _build_rows(self, exchange: str, symbol: str,
                    price: dict, foreign: dict) -> List[dict]:
        rows = []
        for ngay, p in price.items():
            f = foreign.get(ngay, {})
            d = datetime.strptime(ngay, "%d/%m/%Y").date()
            close_raw = self._num(p.get("GiaDongCua"))
            close_adj = self._num(p.get("GiaDieuChinh"))
            rows.append({
                "date": d.isoformat(), "exchange": exchange, "symbol": symbol,
                "open": self._mul(p.get("GiaMoCua")), "high": self._mul(p.get("GiaCaoNhat")),
                "low": self._mul(p.get("GiaThapNhat")),
                "close_raw": self._mul(close_raw), "close_adj": self._mul(close_adj),
                "vol_matched": self._num(p.get("KhoiLuongKhopLenh")),
                "val_matched_bn": self._num(p.get("GiaTriKhopLenh")),
                "vol_negotiated": self._num(p.get("KLThoaThuan")),
                "val_negotiated_bn": self._num(p.get("GtThoaThuan")),
                "f_buy_vol": self._num(f.get("KLMua")), "f_buy_val": self._num(f.get("GtMua")),
                "f_sell_vol": self._num(f.get("KLBan")), "f_sell_val": self._num(f.get("GtBan")),
                "f_net_vol": self._num(f.get("KLGDRong")), "f_net_val": self._num(f.get("GTDGRong")),
                "room_left": self._num(f.get("RoomConLai")), "own_pct": self._num(f.get("DangSoHuu")),
            })
        rows.sort(key=lambda r: r["date"])
        return rows

    def _mul(self, v):
        n = self._num(v)
        return n * 1000 if n != "" else ""   # CafeF prices are in '000 VND

    # ──────────────────────────────────────────────────────────────────────
    # Per-stock scrape
    # ──────────────────────────────────────────────────────────────────────

    def scrape_stock(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        folder = os.path.join(CAFEF_RAW_DATA_DIR, "stocks")
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{exchange}_{symbol}.csv")

        if skip_existing and os.path.exists(file_path):
            self._logger.log_info(f"CafeF: '{symbol}' already scraped, skipping.")
            return

        self._logger.log_info(f"CafeF: scraping '{exchange}:{symbol}'...")
        price = self._collect("PriceHistory.ashx", symbol, self.PRICE_START_YEAR)
        if not price:
            self._logger.log_warning(f"CafeF: no price data for '{symbol}', skipping.")
            return
        foreign = self._collect("GDKhoiNgoai.ashx", symbol, self.FOREIGN_START_YEAR)
        rows = self._build_rows(exchange, symbol, price, foreign)

        # Write to a temp file then atomically rename, so an interrupted run never
        # leaves a partial CSV that skip_existing would wrongly treat as complete.
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.OUTPUT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, file_path)
        self._logger.log_info(
            f"CafeF: saved {len(rows)} rows ({rows[0]['date']}..{rows[-1]['date']}) "
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
        self._logger.log_info(f"CafeF: {len(out)} stock symbols found from TradingView links.")
        return sorted(out)

    def scrape(self) -> None:
        """BaseScraper entry point: scrape all stocks from CafeF."""
        self.scrape_all_stocks()

    def scrape_all_stocks(self, skip_existing: bool = True) -> None:
        symbols = self.get_stock_symbols()
        self._thread_manager.remove_all_tasks()
        for exchange, symbol in symbols:
            self._thread_manager.add_task(
                Task(f"cafef/stocks/{exchange}_{symbol}",
                     self.scrape_stock, exchange, symbol, skip_existing)
            )
        self._logger.log_info(
            f"CafeF: executing {self._thread_manager.get_current_number_of_task()} "
            f"stock scraping tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("CafeF: finished scraping all stocks.")
