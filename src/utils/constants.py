# src\utils\constants.py

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

# GOLD_PROTOTYPE_TICKERS: list = ["VNM", "VIC", "FPT", "VCB"]

# Stocks (by ticker) to build a per-stock unified table for (unified_<ticker>).
# VN30 index constituents (HOSE).
UNIFIED_TICKERS: list = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
    "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
    "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE",
]

# Macro gold tables joined into each unified table (raw `value`, forward-filled
# onto the stock's trading-day date spine).
UNIFIED_MACRO_TABLES: list = ["economy", "bonds"]

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
SCRAPER_RAW_DATA_DIR = "raw_data"
SCRAPER_BASE_WAIT_TIME = 1  # seconds

SCRAPER_RETRY_ATTEMPTS = 5  # number of retry attempts on failure
SCRAPER_RETRY_DELAY = 5  # seconds to wait between retries
SCRAPER_MAX_CONCURRENT_BROWSERS = 8  # cap concurrent Chrome instances
SCRAPER_NAV_STAGGER = 8.0  # minimum seconds between browser page navigations

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
