# src\web_scraper\cafef_scraper.py

# ===== Standard Library =====
import calendar
import csv
import glob
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
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
    """Scrape per-stock data from CafeF (cafef.vn/du-lieu). Each numbered tab of the
    history page (lich-su-giao-dich/<exchange>/<sym>-<n>.chn) is one .ashx endpoint,
    and each is written to its own folder — one folder per link:

        vcb-1 → PriceHistory.ashx → price/        (OHLC, raw+adj close, matched/neg vol)
        vcb-2 → ThongKeDL.ashx    → order_stats/   (order-placement buy/sell stats)
        vcb-3 → GDKhoiNgoai.ashx  → foreign/       (foreign buy/sell/net flow, room, own%)
        vcb-4 → GDTuDoanh.ashx    → prop_trading/  (proprietary-desk buy/sell vol+val)
        vcb-6 → GDCoDong.ashx     → insider_txn/   (insider/major-shareholder transactions)

    CafeF quirks handled: StartDate/EndDate are parsed MM/dd/yyyy (US); a query is
    capped at ~63 rows and PageSize at 20, so history is fetched in ~2-month windows
    (kept under the cap, overlapped to avoid boundary gaps) and paginated.

    Output: one CSV per stock at CAFEF_RAW_DATA_DIR/<folder>/<EXCHANGE>_<SYMBOL>.csv.
    """

    SOURCE_NAME = "cafef"
    BASE = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/"
    # The Vietnamese exchanges CafeF serves; the scrape universe is restricted to
    # these (every stock link from TradingView carries a HOSE/HNX/UPCOM prefix).
    VN_EXCHANGES = ("HOSE", "HNX", "UPCOM")
    PRICE_START_YEAR = 2009
    FOREIGN_START_YEAR = 2012  # CafeF foreign-flow history does not go back further
    ORDER_STATS_START_YEAR = 2010  # order-placement stats (ThongKeDL) history starts ~2010
    PROP_START_YEAR = 2023  # proprietary-desk trades (GDTuDoanh) history starts ~2023

    # Price-history tab (cafef .../vcb-1.chn → PriceHistory.ashx).
    PRICE_COLUMNS = [
        "date", "exchange", "symbol",
        "open", "high", "low", "close_raw", "close_adj",
        "vol_matched", "val_matched_bn", "vol_negotiated", "val_negotiated_bn",
    ]

    # Foreign-trading tab (cafef .../vcb-3.chn → GDKhoiNgoai.ashx).
    FOREIGN_COLUMNS = [
        "date", "exchange", "symbol",
        "f_buy_vol", "f_buy_val", "f_sell_vol", "f_sell_val",
        "f_net_vol", "f_net_val", "room_left", "own_pct",
    ]

    # Order-placement statistics tab (cafef .../vcb-2.chn → ThongKeDL.ashx): the
    # order-book demand/supply — count + volume of buy vs sell orders placed.
    ORDER_STATS_COLUMNS = [
        "date", "exchange", "symbol",
        "n_buy_orders", "buy_order_vol", "avg_vol_per_buy_order",
        "n_sell_orders", "sell_order_vol", "avg_vol_per_sell_order",
    ]

    # Proprietary-desk trading tab (cafef .../vcb-4.chn → GDTuDoanh.ashx): securities
    # firms' own-account buy/sell volume and value.
    PROP_COLUMNS = [
        "date", "exchange", "symbol",
        "prop_buy_vol", "prop_sell_vol", "prop_buy_val", "prop_sell_val",
    ]

    # Insider / major-shareholder transactions tab (cafef .../fpt-6.chn →
    # GDCoDong.ashx): one row per registered/executed insider transaction (event-
    # based, NOT a daily series) — who, position, planned vs realised buy/sell
    # volume, the registration window, published date, and pre/post holdings.
    INSIDER_COLUMNS = [
        "exchange", "symbol", "transaction_man", "position",
        "related_man", "related_position",
        "vol_before", "plan_buy_vol", "plan_sell_vol",
        "plan_begin_date", "plan_end_date",
        "real_buy_vol", "real_sell_vol", "real_end_date",
        "order_date", "published_date",
        "vol_after", "ownership_pct", "note", "shareholder_code", "profile_url",
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

    def _get(self, ashx: str, params: dict, list_key: str = None) -> list:
        for attempt in range(1, self._retry_attempts + 1):
            try:
                r = self._session.get(self.BASE + ashx, params=params, timeout=30)
                data = r.json()["Data"]["Data"]
                # Most endpoints return a flat list at Data.Data; a few (GDTuDoanh)
                # nest the list one level deeper under a named key.
                if list_key is not None:
                    data = (data or {}).get(list_key, [])
                return data or []
            except Exception as e:
                if attempt == self._retry_attempts:
                    self._logger.log_warning(
                        f"CafeF {ashx} failed after {attempt} tries "
                        f"({params.get('Symbol')} {params.get('StartDate')}): {e}"
                    )
                    return []
                time.sleep(self._retry_delay)
        return []

    def _collect(self, ashx: str, symbol: str, start_year: int, exchange: str,
                 list_key: str = None, date_key: str = "Ngay") -> dict:
        """Fetch all rows for a symbol across windows, keyed by the row's date (dedup).

        ExchangeType (HOSE/HNX/UPCOM, UPPERCASE) is required for HNX/UPCOM — without
        it CafeF silently defaults to HOSE and returns nothing for those tickers.

        date_key differs by endpoint (price/foreign use "Ngay"; the order-stats and
        proprietary-trade tabs use "Date"); list_key handles endpoints that nest the
        row list one level deeper (GDTuDoanh → "ListDataTudoanh").
        """
        by_date: dict = {}
        for sd, ed in self._windows(start_year):
            page = 1
            while page <= 6:
                rec = self._get(ashx, {"Symbol": symbol, "ExchangeType": exchange.upper(),
                                       "StartDate": sd, "EndDate": ed,
                                       "PageIndex": page, "PageSize": 20},
                                list_key=list_key)
                if not rec:
                    break
                for row in rec:
                    by_date[row[date_key]] = row
                if len(rec) < 20:
                    break
                page += 1
        return by_date

    def _collect_paged(self, ashx: str, symbol: str, exchange: str,
                       list_key: str = None, page_size: int = 20,
                       max_pages: int = 500) -> list:
        """Collect an event-based (non-daily) endpoint by paginating PageIndex over
        the full history in a single wide date range, until a short/empty page. Used
        by the insider-transaction tab, whose rows are transactions, not trading days
        (so no date-window splitting or by-date dedup applies)."""
        out: list = []
        today = date.today().strftime("%m/%d/%Y")
        page = 1
        while page <= max_pages:
            rec = self._get(ashx, {"Symbol": symbol, "ExchangeType": exchange.upper(),
                                   "StartDate": "01/01/2000", "EndDate": today,
                                   "PageIndex": page, "PageSize": page_size},
                            list_key=list_key)
            if not rec:
                break
            out.extend(rec)
            if len(rec) < page_size:
                break
            page += 1
        return out

    # ──────────────────────────────────────────────────────────────────────
    # Parsing
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _num(v):
        try:
            return float(str(v).replace(",", "")) if v not in (None, "") else ""
        except (ValueError, TypeError):
            return ""

    @staticmethod
    def _net_date(v) -> str:
        """Parse CafeF's .NET JSON date ('/Date(1782147600000)/', ms since epoch) to
        an ISO date in Vietnam local time (UTC+7). Returns '' for null/unparseable."""
        m = re.search(r"Date\((-?\d+)", str(v or ""))
        if not m:
            return ""
        ms = int(m.group(1))
        return datetime.fromtimestamp(ms / 1000, timezone(timedelta(hours=7))).date().isoformat()

    @staticmethod
    def _href(v) -> str:
        """Extract the profile href from CafeF's anchor-wrapped Url field."""
        m = re.search(r"href='([^']+)'", str(v or ""))
        if not m:
            return ""
        path = m.group(1)
        return path if path.startswith("http") else "https://cafef.vn" + path

    def _build_price_rows(self, exchange: str, symbol: str, raw: dict) -> List[dict]:
        """Rows for the price tab (PriceHistory). OHLC + closes are '000 VND → ×1000;
        matched/negotiated volume and value are raw."""
        rows = []
        for ngay, p in raw.items():
            d = datetime.strptime(ngay, "%d/%m/%Y").date()
            rows.append({
                "date": d.isoformat(), "exchange": exchange, "symbol": symbol,
                "open": self._mul(p.get("GiaMoCua")), "high": self._mul(p.get("GiaCaoNhat")),
                "low": self._mul(p.get("GiaThapNhat")),
                "close_raw": self._mul(p.get("GiaDongCua")),
                "close_adj": self._mul(p.get("GiaDieuChinh")),
                "vol_matched": self._num(p.get("KhoiLuongKhopLenh")),
                "val_matched_bn": self._num(p.get("GiaTriKhopLenh")),
                "vol_negotiated": self._num(p.get("KLThoaThuan")),
                "val_negotiated_bn": self._num(p.get("GtThoaThuan")),
            })
        rows.sort(key=lambda r: r["date"])
        return rows

    def _build_foreign_rows(self, exchange: str, symbol: str, raw: dict) -> List[dict]:
        """Rows for the foreign-trading tab (GDKhoiNgoai)."""
        rows = []
        for ngay, f in raw.items():
            d = datetime.strptime(ngay, "%d/%m/%Y").date()
            rows.append({
                "date": d.isoformat(), "exchange": exchange, "symbol": symbol,
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

    def _build_order_stats_rows(self, exchange: str, symbol: str,
                                raw: dict) -> List[dict]:
        """Rows for the order-placement tab (ThongKeDL). Counts/volumes are already
        raw (shares/order counts) — no '000 scaling. ThayDoi (price change) and
        ChenhLechKL (buy-sell vol diff, derivable) are dropped as redundant."""
        rows = []
        for ngay, r in raw.items():
            d = datetime.strptime(ngay, "%d/%m/%Y").date()
            rows.append({
                "date": d.isoformat(), "exchange": exchange, "symbol": symbol,
                "n_buy_orders": self._num(r.get("SoLenhMua")),
                "buy_order_vol": self._num(r.get("KLDatMua")),
                "avg_vol_per_buy_order": self._num(r.get("KLTB1LenhMua")),
                "n_sell_orders": self._num(r.get("SoLenhDatBan")),
                "sell_order_vol": self._num(r.get("KLDatBan")),
                "avg_vol_per_sell_order": self._num(r.get("KLTB1LenhBan")),
            })
        rows.sort(key=lambda x: x["date"])
        return rows

    def _build_prop_rows(self, exchange: str, symbol: str, raw: dict) -> List[dict]:
        """Rows for the proprietary-desk tab (GDTuDoanh). Volumes are raw shares,
        values raw VND — no '000 scaling."""
        rows = []
        for ngay, r in raw.items():
            d = datetime.strptime(ngay, "%d/%m/%Y").date()
            rows.append({
                "date": d.isoformat(), "exchange": exchange, "symbol": symbol,
                "prop_buy_vol": self._num(r.get("KLcpMua")),
                "prop_sell_vol": self._num(r.get("KlcpBan")),
                "prop_buy_val": self._num(r.get("GtMua")),
                "prop_sell_val": self._num(r.get("GtBan")),
            })
        rows.sort(key=lambda x: x["date"])
        return rows

    def _build_insider_rows(self, exchange: str, symbol: str,
                            raw: list) -> List[dict]:
        """Rows for the insider-transaction tab (GDCoDong). One row per transaction;
        .NET dates parsed to ISO; volumes/ownership are raw. Sorted by published then
        order date (oldest first)."""
        rows = []
        for r in raw:
            rows.append({
                "exchange": exchange, "symbol": symbol,
                "transaction_man": r.get("TransactionMan") or "",
                "position": r.get("TransactionManPosition") or "",
                "related_man": r.get("RelatedMan") or "",
                "related_position": r.get("RelatedManPosition") or "",
                "vol_before": self._num(r.get("VolumeBeforeTransaction")),
                "plan_buy_vol": self._num(r.get("PlanBuyVolume")),
                "plan_sell_vol": self._num(r.get("PlanSellVolume")),
                "plan_begin_date": self._net_date(r.get("PlanBeginDate")),
                "plan_end_date": self._net_date(r.get("PlanEndDate")),
                "real_buy_vol": self._num(r.get("RealBuyVolume")),
                "real_sell_vol": self._num(r.get("RealSellVolume")),
                "real_end_date": self._net_date(r.get("RealEndDate")),
                "order_date": self._net_date(r.get("OrderDate")),
                "published_date": self._net_date(r.get("PublishedDate")),
                "vol_after": self._num(r.get("VolumeAfterTransaction")),
                "ownership_pct": self._num(r.get("TyLeSoHuu")),
                "note": r.get("TransactionNote") or "",
                "shareholder_code": r.get("ShareHolderCode") or "",
                "profile_url": self._href(r.get("Url")),
            })
        rows.sort(key=lambda x: (x["published_date"] or "", x["order_date"] or ""))
        return rows

    # ──────────────────────────────────────────────────────────────────────
    # Per-stock scrape
    # ──────────────────────────────────────────────────────────────────────

    def scrape_price(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        """Price-history tab (vcb-1.chn → PriceHistory.ashx)."""
        self._scrape_tab(
            "price", "PriceHistory.ashx", self.PRICE_START_YEAR,
            self.PRICE_COLUMNS, self._build_price_rows,
            exchange, symbol, skip_existing, date_key="Ngay",
        )

    def scrape_foreign(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        """Foreign-trading tab (vcb-3.chn → GDKhoiNgoai.ashx)."""
        self._scrape_tab(
            "foreign", "GDKhoiNgoai.ashx", self.FOREIGN_START_YEAR,
            self.FOREIGN_COLUMNS, self._build_foreign_rows,
            exchange, symbol, skip_existing, date_key="Ngay",
        )

    def _scrape_tab(self, folder_name: str, ashx: str, start_year: int,
                    columns: List[str], build_rows, exchange: str, symbol: str,
                    skip_existing: bool = True,
                    list_key: str = None, date_key: str = "Date") -> None:
        """Generic single-tab per-stock scrape → CAFEF_RAW_DATA_DIR/<folder_name>/
        <EXCHANGE>_<SYMBOL>.csv. Shares the temp-file + atomic-rename write so an
        interrupted run never leaves a partial CSV that skip_existing trusts."""
        folder = os.path.join(CAFEF_RAW_DATA_DIR, folder_name)
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{exchange}_{symbol}.csv")

        if skip_existing and os.path.exists(file_path):
            self._logger.log_info(f"CafeF {folder_name}: '{symbol}' already scraped, skipping.")
            return

        self._logger.log_info(f"CafeF {folder_name}: scraping '{exchange}:{symbol}'...")
        raw = self._collect(ashx, symbol, start_year, exchange,
                            list_key=list_key, date_key=date_key)
        if not raw:
            self._logger.log_warning(f"CafeF {folder_name}: no data for '{symbol}', skipping.")
            return
        rows = build_rows(exchange, symbol, raw)

        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, file_path)
        self._logger.log_info(
            f"CafeF {folder_name}: saved {len(rows)} rows "
            f"({rows[0]['date']}..{rows[-1]['date']}) -> {file_path}"
        )

    def scrape_order_stats(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        """Order-placement statistics tab (vcb-2.chn → ThongKeDL.ashx)."""
        self._scrape_tab(
            "order_stats", "ThongKeDL.ashx", self.ORDER_STATS_START_YEAR,
            self.ORDER_STATS_COLUMNS, self._build_order_stats_rows,
            exchange, symbol, skip_existing, date_key="Date",
        )

    def scrape_prop_trading(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        """Proprietary-desk trading tab (vcb-4.chn → GDTuDoanh.ashx)."""
        self._scrape_tab(
            "prop_trading", "GDTuDoanh.ashx", self.PROP_START_YEAR,
            self.PROP_COLUMNS, self._build_prop_rows,
            exchange, symbol, skip_existing,
            list_key="ListDataTudoanh", date_key="Date",
        )

    def scrape_insider_txn(self, exchange: str, symbol: str, skip_existing: bool = True) -> None:
        """Insider / major-shareholder transactions tab (fpt-6.chn → GDCoDong.ashx).

        Event-based: one row per registered/executed transaction, not a daily series,
        so it paginates the full history (PageIndex) rather than date-windowing, and
        writes to its own CAFEF_RAW_DATA_DIR/insider_txn/<EXCHANGE>_<SYMBOL>.csv."""
        folder = os.path.join(CAFEF_RAW_DATA_DIR, "insider_txn")
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{exchange}_{symbol}.csv")

        if skip_existing and os.path.exists(file_path):
            self._logger.log_info(f"CafeF insider_txn: '{symbol}' already scraped, skipping.")
            return

        self._logger.log_info(f"CafeF insider_txn: scraping '{exchange}:{symbol}'...")
        raw = self._collect_paged("GDCoDong.ashx", symbol, exchange)
        if not raw:
            self._logger.log_warning(f"CafeF insider_txn: no data for '{symbol}', skipping.")
            return
        rows = self._build_insider_rows(exchange, symbol, raw)

        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.INSIDER_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, file_path)
        self._logger.log_info(
            f"CafeF insider_txn: saved {len(rows)} transactions -> {file_path}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Stock universe + batch driver
    # ──────────────────────────────────────────────────────────────────────

    def get_stock_symbols(self, exchanges: Tuple[str, ...] = None) -> List[Tuple[str, str]]:
        """Derive the (exchange, symbol) universe from the TradingView stock link CSVs,
        restricted to the given exchanges (default: all three VN exchanges — HOSE, HNX,
        UPCOM). Pass e.g. exchanges=("HOSE",) to scope to a single exchange."""
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
                    if exchange.upper() not in wanted:
                        continue
                    if (exchange, ticker) not in seen:
                        seen.add((exchange, ticker))
                        out.append((exchange, ticker))
        by_exchange = {}
        for exchange, _ in out:
            by_exchange[exchange] = by_exchange.get(exchange, 0) + 1
        self._logger.log_info(
            f"CafeF: {len(out)} stock symbols across {sorted(wanted)} "
            f"from TradingView links ({by_exchange})."
        )
        return sorted(out)

    def scrape(self, exchanges: Tuple[str, ...] = None) -> None:
        """BaseScraper entry point: scrape every per-stock CafeF tab into its own
        folder — price (vcb-1), order-placement stats (vcb-2), foreign flow (vcb-3),
        proprietary-desk trades (vcb-4), and insider transactions (vcb-6) — for every
        ticker across the given exchanges (default: all of HOSE, HNX, UPCOM)."""
        self.scrape_all_price(exchanges=exchanges)
        self.scrape_all_order_stats(exchanges=exchanges)
        self.scrape_all_foreign(exchanges=exchanges)
        self.scrape_all_prop_trading(exchanges=exchanges)
        self.scrape_all_insider_txn(exchanges=exchanges)

    def _scrape_all(self, label: str, scrape_fn, skip_existing: bool,
                    exchanges: Tuple[str, ...] = None) -> None:
        """Queue one per-stock task per symbol over the exchange universe and run them."""
        symbols = self.get_stock_symbols(exchanges)
        self._thread_manager.remove_all_tasks()
        for exchange, symbol in symbols:
            self._thread_manager.add_task(
                Task(f"cafef/{label}/{exchange}_{symbol}",
                     scrape_fn, exchange, symbol, skip_existing)
            )
        self._logger.log_info(
            f"CafeF: executing {self._thread_manager.get_current_number_of_task()} "
            f"{label} scraping tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info(f"CafeF: finished scraping all {label}.")

    def scrape_all_price(self, skip_existing: bool = True, exchanges: Tuple[str, ...] = None) -> None:
        self._scrape_all("price", self.scrape_price, skip_existing, exchanges)

    def scrape_all_foreign(self, skip_existing: bool = True, exchanges: Tuple[str, ...] = None) -> None:
        self._scrape_all("foreign", self.scrape_foreign, skip_existing, exchanges)

    def scrape_all_order_stats(self, skip_existing: bool = True, exchanges: Tuple[str, ...] = None) -> None:
        self._scrape_all("order_stats", self.scrape_order_stats, skip_existing, exchanges)

    def scrape_all_prop_trading(self, skip_existing: bool = True, exchanges: Tuple[str, ...] = None) -> None:
        self._scrape_all("prop_trading", self.scrape_prop_trading, skip_existing, exchanges)

    def scrape_all_insider_txn(self, skip_existing: bool = True, exchanges: Tuple[str, ...] = None) -> None:
        self._scrape_all("insider_txn", self.scrape_insider_txn, skip_existing, exchanges)
