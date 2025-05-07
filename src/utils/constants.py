from datetime import datetime

# ===========================
# LOGGER CONFIGURATION
# ===========================
LOG_DIR = "logs"
LOG_FILE_BASE = f"{LOG_DIR}/app"


# ===========================
# THREAD MANAGER CONFIGURATION
# ===========================
THREAD_MANAGER_POWER = 50  # unit: %

# ===========================
# POSTGRESQL CONFIGURATION
# ===========================
POSTGRES = {
    "HOST": "localhost",
    "USER": "postgres",
    "PASSWORD": "changeme",
    "PORT": 5432,
    "DATABASE": "postgres",
    "SCHEMA": "public",
}


# ===========================
# SCRAPER CONFIGURATION
# ===========================
SCRAPER_START_DATE = datetime(2000, 1, 1)
SCRAPER_RAW_DATA_DIR = "raw_data"
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
