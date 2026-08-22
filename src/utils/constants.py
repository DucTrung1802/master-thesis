# src\utils\constants.py

import csv
import os
from datetime import datetime

# ===========================
# COMMON CONFIGURATION
# ===========================
RANDOM_SEED = 42
DOWNLOAD_FOLDER_PATH = "C:/Users/ADMIN/Downloads"

# ===========================
# LOGGER CONFIGURATION
# ===========================
LOG_DIR = "logs"
LOG_FILE_BASE = f"{LOG_DIR}/app"


# ===========================
# CHARTS CONFIGURATION
# ===========================
CHARTS_DIR_BASE = "../../charts"
CHARTS_DIR_MACROECONOMICS = f"{CHARTS_DIR_BASE}/macroeconomics"
CHARTS_DIR_STOCK_MARKET = f"{CHARTS_DIR_BASE}/stock_market"
CHARTS_DIR_ENTERPRISE = f"{CHARTS_DIR_BASE}/enterprise"
CHARTS_DIR_ARIMA = f"{CHARTS_DIR_BASE}/arima"

PDF_OUTPUT_DIR = "../pdfs"

# ===========================
# TA CONFIGURATION
# ===========================
TA_LOG_FILE_BASE = f"{LOG_DIR}/ta"

TA_NAME_MAP = {
    "add_bbands": "Bollinger Bands",
    "add_dema": "Double Exponential Moving Average",
    "add_ema": "Exponential Moving Average",
    "add_kama": "Kaufman's Adaptive Moving Average",
    "add_midpoint": "Midpoint",
    "add_midprice": "Mid Price",
    "add_sar": "Parabolic SAR (Stop and Reverse)",
    "add_sma": "Simple Moving Average",
    "add_t3": "T3 Moving Average",
    "add_tema": "Triple Exponential Moving Average",
    "add_trima": "Triangular Moving Average",
    "add_wma": "Weighted Moving Average",
    "add_adx": "Average Directional Index",
    "add_aroon": "Aroon Indicator",
    "add_bop": "Balance of Power",
    "add_cci": "Commodity Channel Index",
    "add_cmo": "Chande Momentum Oscillator",
    "add_macd": "Moving Average Convergence Divergence",
    "add_mfi": "Money Flow Index",
    "add_mom": "Momentum",
    "add_ppo": "Percentage Price Oscillator",
    "add_roc": "Rate of Change",
    "add_rsi": "Relative Strength Index",
    "add_stoch": "Stochastic Oscillator",
    "add_stoch_rsi": "Stochastic RSI",
    "add_trix": "Triple Exponential Average",
    "add_ultosc": "Ultimate Oscillator",
    "add_willr": "Williams %R",
    "add_ad": "Accumulation/Distribution Line",
    "add_adosc": "Accumulation/Distribution Oscillator",
    "add_obv": "On-Balance Volume",
    "add_ht_dcperiod": "Hilbert Transform - Dominant Cycle Period",
    "add_ht_dcphase": "Hilbert Transform - Dominant Cycle Phase",
    "add_ht_phasor": "Hilbert Transform - Phasor Components",
    "add_ht_sine": "Hilbert Transform - SineWave",
    "add_ht_trendmode": "Hilbert Transform - Trend vs Cycle Mode",
    "add_avgprice": "Average Price",
    "add_medprice": "Median Price",
    "add_typprice": "Typical Price",
    "add_wclprice": "Weighted Close Price",
    "add_atr": "Average True Range",
    "add_natr": "Normalized Average True Range",
    "add_trange": "True Range",
}


# ===========================
# VISUALIZATION CONFIGURATION
# ===========================
SILVER_VISUALIZATION_LOG_FILE_BASE = f"../../{LOG_DIR}/silver_visualization"
GOLD_VISUALIZATION_LOG_FILE_BASE = f"../../{LOG_DIR}/gold_visualization"
ARIMA_VISUALIZATION_LOG_FILE_BASE = f"../../{LOG_DIR}/arima_visualization"


# ===========================
# FEATURE SELECTION CONFIGURATION
# ===========================
FEATURE_SELECTION_LOG_FILE_BASE = f"../../{LOG_DIR}/feature_selection"
FEATURE_SELECTION_LOG_FILE_TEST = (
    f"{FEATURE_SELECTION_LOG_FILE_BASE}/feature_selection_test"
)
FEATURE_SELECTION_CHARTS_DIR = f"{FEATURE_SELECTION_LOG_FILE_BASE}/feature_selection"
FEATURE_SELECTION_RESULT_DIR = f"../../src/feature_selection/feature_selection_result"

# ===========================
# TRAINED MODELS CONFIGURATION
# ===========================
TRAINED_MODELS_LOG_FILE_BASE = f"../../{LOG_DIR}/train_model"


# ===========================
# DATABASE CONFIGURATION
# ===========================
DATABASE_MAIN_V2 = "database_main_v2"
BRONZE_SCHEMA = "bronze_schema"
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"
UNIFIED_SCHEMA = "unified_schema"

# ⚠️ THE FILTER LAYER (2026-08-22) — one table per SCREEN, `universe__<screen>`, holding
# every `(exchange, ticker)` silver knows with the measurement and the verdict of every
# condition that screen names. It sits BETWEEN gold and unified: gold answers "what
# happened to this ticker on this date", this answers "is this ticker allowed into a
# unified schema at all". `orchestration/preprocessor/filters.py` is the registry.
FILTER_SCHEMA = "filter_schema"

# GOLD_PROTOTYPE_TICKERS: list = ["VNM", "VIC", "FPT", "VCB"]

# Stocks (by ticker) to build a per-stock unified table for (unified_<ticker>).
# VN30 index constituents (HOSE).
UNIFIED_TICKERS: list = [
    "ACB",
    "BCM",
    "BID",
    "BVH",
    "CTG",
    "FPT",
    "GAS",
    "GVR",
    "HDB",
    "HPG",
    "MBB",
    "MSN",
    "MWG",
    "PLX",
    "POW",
    "SAB",
    "SHB",
    "SSB",
    "SSI",
    "STB",
    "TCB",
    "TPB",
    "VCB",
    "VHM",
    "VIB",
    "VIC",
    "VJC",
    "VNM",
    "VPB",
    "VRE",
]

# Macro gold tables joined into each unified table (raw `value`, forward-filled
# onto the stock's trading-day date spine).
UNIFIED_MACRO_TABLES: list = ["economy", "bonds", "indices", "stocks"]

# The `economy` gold table holds ~1,034 global macro series; joining them all
# blows past PostgreSQL's 1,600-column-per-table limit. Restrict the economy
# join to series whose ticker starts with this prefix (Vietnam macro = 88
# series). Set to None to join every economy series.
UNIFIED_ECONOMY_TICKER_PREFIX: str | None = "VN"

# Supervised target: percentage simple return of `close` this many trading days
# into the future, e.g. close=100 today and 120 in UNIFIED_TARGET_HORIZON days -> target=20.
UNIFIED_TARGET_HORIZON: int = 5


# ===========================
# THREAD MANAGER CONFIGURATION
# ===========================
THREAD_MANAGER_POWER = 50  # unit: %


# ===========================
# SCRAPER CONFIGURATION
# ===========================
SCRAPER_START_DATE = datetime(2000, 1, 1)
SCRAPER_END_DATE = datetime(2026, 4, 30)

# Raw data is organised by source under raw_data/<source>/...
RAW_DATA_DIR = "raw_data"
TRADING_VIEW_RAW_DATA_DIR = f"{RAW_DATA_DIR}/trading_view"
CAFEF_RAW_DATA_DIR = f"{RAW_DATA_DIR}/cafef"
GICS_RAW_DATA_DIR = f"{RAW_DATA_DIR}/gics"
SIMPLIZE_RAW_DATA_DIR = f"{RAW_DATA_DIR}/simplize"

# ── Per-filing CafeF pipelines: the tickers they run on ────────────────────────
# These two read or produce ONE DOCUMENT AT A TIME and cost orders of magnitude more
# per ticker than the daily tabs, so neither may default to the ~777-code universe the
# way `price`/`foreign`/`order_stats` do. Each is scoped explicitly here, so turning
# the switch on has a cost you can read off before you run it. (The news feed is NOT
# in this group — it is one request per article but has already been scraped for the
# full universe, so `CafeFNewsScraper` keeps the universe default.)


def _vn100_symbols() -> list[tuple[str, str]]:
    """VN100 constituents from the repo-root `vn100.csv` (utf-8-BOM, see
    web_scraper/CONTEXT.md §8). Falls back to VN30 (UNIFIED_TICKERS, all HOSE) if the
    file is absent, so an import never fails on a missing reference file."""
    path = os.path.join(os.path.dirname(__file__), "..", "..", "vn100.csv")
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return [
                (r["exchange"], r["ticker"])
                for r in csv.DictReader(f)
                if r.get("ticker")
            ]
    except OSError:
        return [("HOSE", t) for t in UNIFIED_TICKERS]


# PDF filing archive (CafeFPdfScraper) — ~1.0-1.7 GB per ticker into raw_data/cafef/pdfs/.
# VN100 is what is on disk today (108 folders = VN100 + 8 leftovers, ~97 GB) and is the
# most that is practical; the full universe would be terabyte-scale.
CAFEF_PDF_TICKERS: list[tuple[str, str]] = _vn100_symbols()

# Statement parse (FinancialsBuilder) — OCRs the archive above into the three statement
# CSVs. ~2.4 h per ticker, and it needs that ticker's PDFs already on disk, so this is a
# far shorter list than CAFEF_PDF_TICKERS and grows one ticker at a time.
# ⚠️ `build_templates_index` REWRITES templates.csv from exactly this list, so a ticker
# dropped from here loses its row (and therefore its template mapping) even though its
# statement CSVs survive. Add, never substitute.
CAFEF_FINANCIALS_TICKERS: list[tuple[str, str]] = [
    ("HOSE", "VCB"),
    ("HOSE", "ACB"),
]

# ── GICS classification crosswalk ──────────────────────────────────────────────
# Map each Simplize industry group (its GICS-based VN taxonomy, 50 groups) to an
# OFFICIAL GICS sub-industry code (the 8-digit leaf in bronze.gics). The full GICS
# hierarchy (sector → industry group → industry → sub-industry, codes + snake
# names) is then read entirely from bronze.gics by joining on this leaf code, so
# the classification is sourced fully from the official GICS table.
#
# Simplize groups are often broader than a single GICS sub-industry; the target is
# the most representative leaf within the correct GICS industry, so the upper three
# levels are exact while the sub-industry is a best-fit (see _helper_build_gics_
# classification). GICS-correct placement wins over Simplize's sector where they
# differ (e.g. renewables → Utilities; telecom & publishing → Communication Services).
SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY = {
    # Energy
    "501010": "10102050",  # than → coal_and_consumable_fuels
    "501020": "10102010",  # dau-va-khi-dot → integrated_oil_and_gas
    "501030": "10101020",  # dich-vu-thiet-bi-dau-khi → oil_and_gas_equipment_and_services
    "502010": "55105020",  # nang-luong-tai-tao → renewable_electricity (GICS: Utilities)
    # Materials
    "511010": "15101010",  # hoa-chat → commodity_chemicals
    "512010": "15104020",  # kim-loai-va-khai-khoang → diversified_metals_and_mining
    "512020": "15102010",  # vat-lieu-xay-dung → construction_materials
    "513010": "15105020",  # giay-va-lam-san → paper_products
    "513020": "15103010",  # hop-dung-va-bao-bi → metal_glass_and_plastic_containers
    # Industrials
    "521010": "20101010",  # hang-khong-vu-tru-quoc-phong → aerospace_and_defense
    "521020": "20106010",  # may-moc-thiet-bi-nang-dong-tau → construction_machinery_and_heavy_transportation_equipment
    "522010": "20103010",  # xay-dung → construction_and_engineering
    "522020": "20107010",  # ban-buon-cong-nghiep → trading_companies_and_distributors
    "522030": "20201070",  # dich-vu-cong-nghiep-thuong-mai → diversified_support_services
    "524050": "20301010",  # van-chuyen-hang-hoa-giao-nhan → air_freight_and_logistics
    "524060": "20304040",  # van-chuyen-hanh-khach → passenger_ground_transportation
    "524070": "20305030",  # co-so-ha-tang-gtvt → marine_ports_and_services
    # Consumer Discretionary
    "531010": "25101010",  # o-to-va-phu-tung → automotive_parts_and_equipment
    "532020": "25203030",  # det-may → textiles
    "532030": "25201030",  # xay-dung-vlxd-dan-dung → homebuilding
    "532040": "25201050",  # hang-gia-dung → housewares_and_specialties
    "532050": "25202010",  # san-pham-giai-tri → leisure_products
    "533010": "25301020",  # khach-san-giai-tri → hotels_resorts_and_cruise_lines
    "533020": "50201040",  # truyen-thong-xuat-ban → publishing (GICS: Communication Services)
    "534020": "25503030",  # ban-le-tong-hop → broadline_retail
    "534030": "25504040",  # ban-le-chuyen-dung → other_specialty_retail
    # Consumer Staples
    "541010": "30201030",  # do-uong → soft_drinks_and_non_alcoholic_beverages
    "541020": "30202030",  # thuc-pham-thuoc-la → packaged_foods_and_meats
    "542010": "30301010",  # san-pham-ca-nhan-gia-dung → household_products
    "543010": "30101030",  # ban-le-thuc-pham-thuoc → food_retail
    "544010": "30202030",  # tap-doan-da-nganh-tieu-dung → packaged_foods_and_meats
    # Financials
    "551010": "40101010",  # tai-chinh-ngan-hang → diversified_banks
    "551020": "40203020",  # chung-khoan-nhdt → investment_banking_and_brokerage
    "553010": "40301030",  # bao-hiem → multi_line_insurance
    "556010": "40201030",  # cong-ty-dau-tu → multi_sector_holdings
    # Health Care
    "561010": "35101010",  # thiet-bi-vat-tu-y-te → health_care_equipment
    "561020": "35102015",  # dich-vu-cham-soc-suc-khoe → health_care_services
    "562010": "35202010",  # duoc-pham → pharmaceuticals
    # Information Technology (+ Communication Services for telecom)
    "571010": "45301020",  # chat-ban-dan → semiconductors
    "571020": "45201020",  # truyen-thong-mang → communications_equipment
    "571040": "45203015",  # thiet-bi-phu-tung-dien-tu → electronic_components
    "571050": "45202030",  # thiet-bi-van-phong → technology_hardware_storage_and_peripherals
    "571060": "45202030",  # may-tinh-dien-thoai-dien-tu → technology_hardware_storage_and_peripherals
    "572010": "45102010",  # phan-mem-dich-vu-cntt → it_consulting_and_other_services
    "574010": "50101020",  # dich-vu-vien-thong → integrated_telecommunication_services (GICS: Communication Services)
    # Utilities
    "591010": "55101010",  # tien-ich-dien → electric_utilities
    "591020": "55102010",  # tien-ich-khi → gas_utilities
    "591030": "55104010",  # nuoc-tien-ich → water_utilities
    "591040": "55103010",  # da-tien-ich → multi_utilities
    # Real Estate
    "601010": "60201030",  # quan-ly-phat-trien-bds → real_estate_development
}

SCRAPER_BASE_WAIT_TIME = 1  # seconds

SCRAPER_RETRY_ATTEMPTS = 5  # number of retry attempts on failure
SCRAPER_RETRY_DELAY = 5  # seconds to wait between retries
# ⚠️ THE HARD CAP ON CONCURRENT CHROME INSTANCES — the one number to change.
# `TradingViewScraper` sizes BOTH its browser semaphore AND its thread pool from this, so
# a thread can never be waiting to open a browser it is not allowed to have. It is the
# only Selenium user in the repo, so this is the whole browser budget.
# Override without editing code:  $env:SCRAPER_MAX_CONCURRENT_BROWSERS = "2"
# ⚠️ It is an IN-PROCESS semaphore. One Dagster run holds one TradingView step at a time
# (the `resource: browser` tag limit), so the cap is exact there — but a multi-run
# backfill launches one process per partition and multiplies it. See orchestration §2a.
SCRAPER_MAX_CONCURRENT_BROWSERS = int(
    os.getenv("SCRAPER_MAX_CONCURRENT_BROWSERS", "4")
)
SCRAPER_NAV_STAGGER = 8.0  # minimum seconds between browser page navigations
SCRAPER_MAX_WORKERS = 2  # thread-pool size for I/O-bound (requests) scrapers

TRADING_VIEW_HOME_PAGE_URL = "https://www.tradingview.com/"
TRADING_VIEW_TABLE_SCHEMA = [
    "scrape_main_type",
    "sub_type_name_1",
    "sub_type_value_1",
    "sub_type_name_2",
    "sub_type_value_2",
    "sub_type_name_3",
    "sub_type_value_3",
    "url",
]
