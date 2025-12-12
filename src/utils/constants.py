from datetime import datetime

# ===========================
# COMMON CONFIGURATION
# ===========================
RANDOM_SEED = 42

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
SCRAPER_END_DATE = datetime(2025, 6, 30)
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

STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB = ["FPT", "GAS"]


# ===========================
# ENTERPRISE CONFIGURATION
# ===========================
PARALLEL_SCRAPE_ENTERPRISE_STOCK_INFORMATION = 8


# ===========================
# TRAIN TEST CREATOR CONFIGURATION
# ===========================
TOP_N_FEATURES = 40
DEFAULT_CORRELATION_THRESHOLD = 0.85
TRAIN_TEST_CREATOR_START_DATE = datetime(2005, 4, 1)
TRAIN_TEST_CREATOR_END_DATE = datetime(2025, 6, 30)
DEFAULT_INPUT_SIZE = 240  # days
DEFAULT_FORECAST_SIZE = 30  # days
UNIFIED_DATAFRAME_DIR = f"unified_dataframe"


# ===========================
# MODEL CONFIGURATION
# ===========================
PATIENCE = 15  # Continuous epoches that the validation loss does not decrease


# ===========================
# WANDB CONFIGURATION
# ===========================
WANDB_ENTITY_NAME = "trung-lyduc18"
WANDB_PROJECT_NAME = "master_thesis"
