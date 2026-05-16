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
# THREAD MANAGER CONFIGURATION
# ===========================
THREAD_MANAGER_POWER = 50  # unit: %


# ===========================
# SCRAPER CONFIGURATION
# ===========================
SCRAPER_START_DATE = datetime(2000, 1, 1)
SCRAPER_END_DATE = datetime(2026, 4, 30)
SCRAPER_BRONZE_DATA_DIR = "bronze_data"
SCRAPER_BASE_WAIT_TIME = 1  # seconds


# ===========================
# MACROECONOMICS CONFIGURATION
# ===========================


# ===========================
# STOCK MARKET CONFIGURATION
# ===========================
STOCK_MARKET_INDEX_HEADER = [
    "date",
    "close",
    "adjusted_close",
    "change",
    "matched_volume",
    "matched_value",
    "negotiated_volume",
    "negotiated_value",
    "open",
    "high",
    "low",
]

STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB = ["FPT", "GAS", "VIC"]


# ===========================
# ENTERPRISE CONFIGURATION
# ===========================
PARALLEL_SCRAPE_ENTERPRISE_STOCK_INFORMATION = 8


# ===========================
# TRAIN TEST CREATOR CONFIGURATION
# ===========================
TRAIN_TEST_SET_DIR = f"../../train_test_set"


# ===========================
# MODEL CONFIGURATION
# ===========================
PATIENCE = 15  # Continuous epoches that the validation loss does not decrease


# ===========================
# WANDB CONFIGURATION
# ===========================
WANDB_ENTITY_NAME = "trung-lyduc18"
WANDB_PROJECT_NAME = "master_thesis"
