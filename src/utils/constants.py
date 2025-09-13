from datetime import datetime

# ===========================
# LOGGER CONFIGURATION
# ===========================
LOG_DIR = "logs"
LOG_FILE_BASE = f"{LOG_DIR}/app"


# ===========================
# LOGGER CONFIGURATION
# ===========================
CHARTS_DIR = "../charts"


# ===========================
# TA CONFIGURATION
# ===========================
TA_LOG_FILE_BASE = f"{LOG_DIR}/ta"


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


# ===========================
# ENTERPRISE CONFIGURATION
# ===========================
PARALLEL_SCRAPE_ENTERPRISE_STOCK_INFORMATION = 8
