# src\web_scraper\cafef_index_scraper.py

# ===== Standard Library =====
from typing import List, Optional, Sequence, Tuple

# ===== Local / Custom Modules =====
from dtos.thread_manager_dtos.task import Task
from web_scraper.base_scraper import register_scraper
from web_scraper.cafef_scraper import CafeFScraper


@register_scraper
class CafeFIndexScraper(CafeFScraper):
    """Scrape the MARKET INDICES from the same CafeF history tabs the per-stock
    scraper reads (cafef.vn/du-lieu/lich-su-giao-dich/<exchange>/<index>-<n>.chn).

    An index is served by the identical `.ashx` endpoints, with the identical JSON
    keys, so this subclasses `CafeFScraper` and reuses its windowing, pagination,
    retry and row builders wholesale. Only three things differ, and each is
    overridden here:

      1. **The universe is a fixed list of six indices** (`INDEXES`), not the
         TradingView-derived ticker set — there is no link CSV for an index.
      2. **An index value is a POINT, not '000 VND** — so `_mul` is neutralised
         (see its docstring). This is the one difference that silently corrupts
         data if missed: VNINDEX would be written as 1,824,090 instead of 1824.09.
      3. **Output goes to `index_`-prefixed folders** so it never mixes with the
         per-stock CSVs, whose `symbol` column means something different (a
         company, not an aggregate) and which downstream joins on a ticker.

        vnindex-1 → PriceHistory.ashx → index_price/
        vnindex-2 → ThongKeDL.ashx    → index_order_stats/
        vnindex-3 → GDKhoiNgoai.ashx  → index_foreign/
        vnindex-4 → GDTuDoanh.ashx    → index_prop_trading/

    The insider-transaction tab (-6) has no index analogue and is not scraped.

    Output: CAFEF_RAW_DATA_DIR/index_<tab>/<EXCHANGE>_<INDEX>.csv, sharing the
    column layout of the matching per-stock folder exactly, so a consumer can read
    an index series with the same code that reads a stock series.
    """

    SOURCE_NAME = "cafef_index"

    # ── The universe: (exchange, index code, inception year) ───────────────────
    # The index code is the URL slug of its history page, uppercased — CafeF accepts
    # either case and echoes uppercase, and uppercase is what the codes are called
    # everywhere else in the repo (cf. `MarketIndexConfig(index_code="VNINDEX")` in
    # main.py). The inception year is the first year the PRICE tab answers, verified
    # against the endpoint year by year; VNINDEX's 2000 is HOSE's opening day itself
    # (28/07/2000, the first row CafeF holds).
    INDEXES: Tuple[Tuple[str, str, int], ...] = (
        ("HOSE", "VNINDEX", 2000),
        ("HOSE", "VN30INDEX", 2012),
        ("HOSE", "VN100-INDEX", 2014),
        ("HNX", "HNX-INDEX", 2005),
        ("HNX", "HNX30-INDEX", 2012),
        ("UPCOM", "UPCOM-INDEX", 2009),
    )

    # Earliest year each TAB holds anything, for any index — a floor applied on top
    # of the index's own inception, so a window is only requested where both could
    # have data. An index that listed later still starts at its inception.
    TAB_START_FLOOR = {
        "index_price": 2000,
        "index_order_stats": 2007,
        "index_foreign": 2007,
        "index_prop_trading": 2022,
    }

    # ──────────────────────────────────────────────────────────────────────
    # The one unit difference from the per-stock scraper
    # ──────────────────────────────────────────────────────────────────────

    def _mul(self, v):
        """⚠️ AN INDEX VALUE IS A POINT, NOT A PRICE IN '000 VND.

        The per-stock scraper multiplies OHLC and both closes by 1000 because CafeF
        quotes a share price in thousands of đồng. An index level is a pure number:
        VNINDEX closed at 1824.09 on 13/02/2026, and ×1000 would write 1,824,090 —
        a figure that is internally consistent, plots fine, and is wrong by three
        orders of magnitude.

        Neutralising `_mul` here (rather than copying `_build_price_rows`) is what
        keeps the two paths from drifting: the inherited row builder is unchanged,
        and the scaling decision lives in exactly one place per class.
        """
        return self._num(v)

    # ──────────────────────────────────────────────────────────────────────
    # Universe
    # ──────────────────────────────────────────────────────────────────────

    def get_index_symbols(
        self, symbols: Optional[Sequence[Tuple]] = None
    ) -> List[Tuple[str, str, int]]:
        """The (exchange, index, inception_year) universe. `symbols` overrides it for
        a one-off run and accepts either shape — a 3-tuple carrying its own inception
        year, or a plain (exchange, index) pair whose year is looked up in `INDEXES`
        (falling back to the earliest floor, so an index not listed here still
        scrapes its full history rather than silently nothing)."""
        if symbols is None:
            return list(self.INDEXES)

        known = {(e.upper(), s.upper()): y for e, s, y in self.INDEXES}
        earliest = min(self.TAB_START_FLOOR.values())
        out: List[Tuple[str, str, int]] = []
        for item in symbols:
            if len(item) == 3:
                exchange, symbol, year = item
            else:
                exchange, symbol = item
                year = known.get((exchange.upper(), symbol.upper()), earliest)
            out.append((exchange.upper(), symbol.upper(), year))
        return out

    def _start_year(self, folder_name: str, inception: int) -> int:
        """A tab is only worth querying from the later of the index's inception and
        the tab's own earliest data."""
        return max(inception, self.TAB_START_FLOOR[folder_name])

    # ──────────────────────────────────────────────────────────────────────
    # Per-index scrape (one method per tab, mirroring the per-stock scraper)
    # ──────────────────────────────────────────────────────────────────────

    def scrape_index_price(self, exchange: str, symbol: str, inception: int,
                           skip_existing: bool = True) -> None:
        """Price-history tab (vnindex-1.chn → PriceHistory.ashx). Index POINTS, so
        `open`/`high`/`low`/`close_raw`/`close_adjust` are unscaled.

        Both closes are kept even though CafeF derives no dividend adjustment for an
        index: on HOSE they are identical, but on HNX/UPCOM `close_raw` carries the
        FULL precision (HNX-INDEX 235.1647) where `close_adjust` is rounded to 2dp
        (235.16) — so the columns are not redundant, and `close_raw` is the better
        series to read for those three."""
        self._scrape_tab(
            "index_price", "PriceHistory.ashx",
            self._start_year("index_price", inception),
            self.PRICE_COLUMNS, self._build_price_rows,
            exchange, symbol, skip_existing, date_key="Ngay",
        )

    def scrape_index_order_stats(self, exchange: str, symbol: str, inception: int,
                                 skip_existing: bool = True) -> None:
        """Order-placement statistics tab (vnindex-2.chn → ThongKeDL.ashx), i.e. the
        whole market's order book aggregated.

        ⚠️ Populated unevenly by index: VN30INDEX/VN100-INDEX return rows whose every
        count is literally 0, and the HNX/UPCOM indices report `n_sell_orders` while
        leaving `sell_order_vol` at 0. The rows are recorded as served — a zero here
        is CafeF's, not the scraper's, and is not distinguishable from a real zero."""
        self._scrape_tab(
            "index_order_stats", "ThongKeDL.ashx",
            self._start_year("index_order_stats", inception),
            self.ORDER_STATS_COLUMNS, self._build_order_stats_rows,
            exchange, symbol, skip_existing, date_key="Date",
        )

    def scrape_index_foreign(self, exchange: str, symbol: str, inception: int,
                             skip_existing: bool = True) -> None:
        """Foreign-trading tab (vnindex-3.chn → GDKhoiNgoai.ashx).

        ⚠️ THIS SERIES IS GENUINELY PATCHY AND THE HOLES ARE CAFEF'S. Whole 2-month
        windows return nothing while the price tab answers for the same dates and the
        same index — deterministically, on repeated attempts, at every page and at
        1-month granularity. Do not read a gap here as a failed request.

        `foreign_room_left` / `foreign_own` are always 0: an index has no foreign
        ownership limit. The columns are kept for layout parity with `foreign/`."""
        self._scrape_tab(
            "index_foreign", "GDKhoiNgoai.ashx",
            self._start_year("index_foreign", inception),
            self.FOREIGN_COLUMNS, self._build_foreign_rows,
            exchange, symbol, skip_existing, date_key="Ngay",
        )

    def scrape_index_prop_trading(self, exchange: str, symbol: str, inception: int,
                                  skip_existing: bool = True) -> None:
        """Proprietary-desk trading tab (vnindex-4.chn → GDTuDoanh.ashx).

        ⚠️ Essentially an EXCHANGE-LEVEL series: VNINDEX, HNX-INDEX and UPCOM-INDEX
        carry history, while the three sub-indices (VN30/VN100/HNX30) hold only a
        handful of 2026 rows — a prop desk's trades are reported per exchange, not per
        index basket. All six are queried anyway; a sub-index simply writes little or
        nothing, which is the honest record of what CafeF serves."""
        self._scrape_tab(
            "index_prop_trading", "GDTuDoanh.ashx",
            self._start_year("index_prop_trading", inception),
            self.PROP_COLUMNS, self._build_prop_rows,
            exchange, symbol, skip_existing,
            list_key="ListDataTudoanh", date_key="Date",
        )

    # ──────────────────────────────────────────────────────────────────────
    # Batch driver
    # ──────────────────────────────────────────────────────────────────────

    def scrape(self, symbols: Optional[Sequence[Tuple]] = None) -> None:
        """BaseScraper entry point: scrape each enabled index tab into its own
        `index_`-prefixed folder, for all six indices (or the `symbols` override).

        Gated by `web_scraper/cafef_index/{price,order_stats,foreign,prop_trading}`.
        """
        if self._switch_handler.is_enabled("web_scraper", "cafef_index", "price"):
            self.scrape_all_index_price(symbols=symbols)

        if self._switch_handler.is_enabled("web_scraper", "cafef_index", "order_stats"):
            self.scrape_all_index_order_stats(symbols=symbols)

        if self._switch_handler.is_enabled("web_scraper", "cafef_index", "foreign"):
            self.scrape_all_index_foreign(symbols=symbols)

        if self._switch_handler.is_enabled("web_scraper", "cafef_index", "prop_trading"):
            self.scrape_all_index_prop_trading(symbols=symbols)

    def _scrape_all_indexes(self, label: str, scrape_fn, skip_existing: bool,
                            symbols: Optional[Sequence[Tuple]] = None) -> None:
        """Queue one task per index and run them. Six tasks, so the shared pool is
        never the bottleneck here — each task is sequential over its own date
        windows, and the longest (VNINDEX price, 2000→today) sets the wall clock."""
        universe = self.get_index_symbols(symbols)
        self._thread_manager.remove_all_tasks()
        for exchange, symbol, inception in universe:
            self._thread_manager.add_task(
                Task(f"cafef_index/{label}/{exchange}_{symbol}",
                     scrape_fn, exchange, symbol, inception, skip_existing)
            )
        self._logger.log_info(
            f"CafeF index: executing {self._thread_manager.get_current_number_of_task()} "
            f"{label} scraping tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info(f"CafeF index: finished scraping all {label}.")

    def scrape_all_index_price(self, skip_existing: bool = True,
                               symbols: Optional[Sequence[Tuple]] = None) -> None:
        self._scrape_all_indexes("price", self.scrape_index_price, skip_existing, symbols)

    def scrape_all_index_order_stats(self, skip_existing: bool = True,
                                     symbols: Optional[Sequence[Tuple]] = None) -> None:
        self._scrape_all_indexes("order_stats", self.scrape_index_order_stats,
                                 skip_existing, symbols)

    def scrape_all_index_foreign(self, skip_existing: bool = True,
                                 symbols: Optional[Sequence[Tuple]] = None) -> None:
        self._scrape_all_indexes("foreign", self.scrape_index_foreign, skip_existing, symbols)

    def scrape_all_index_prop_trading(self, skip_existing: bool = True,
                                      symbols: Optional[Sequence[Tuple]] = None) -> None:
        self._scrape_all_indexes("prop_trading", self.scrape_index_prop_trading,
                                 skip_existing, symbols)
