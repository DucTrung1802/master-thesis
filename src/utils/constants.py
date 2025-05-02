from datetime import datetime

# ===========================
# LOGGER CONFIGURATION
# ===========================
LOG_DIR = "logs"
LOG_FILE_BASE = f"{LOG_DIR}/app"


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
SCRAPER_MACROECONOMICS_DIR = f"{SCRAPER_RAW_DATA_DIR}/macroeconomics"

MACROECONOMICS_INDICATORS = {
    "GDP": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/gdp",
        "FILENAME": "macroeconomics_gdp",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/43/thu-nhap.htm",
    },
    "CPI": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/cpi",
        "FILENAME": "macroeconomics_cpi",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/52/cpi.htm",
    },
    "EXCHANGE_RATE": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/exchange_rate",
        "FILENAME": "macroeconomics_exchange_rate",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/53-64/ty-gia-lai-suat.htm",
    },
    "INTEREST_RATE": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/interest_rate",
        "FILENAME": "macroeconomics_interest_rate",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/53-64/ty-gia-lai-suat.htm",
    },
    "EXPORT_IMPORT": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/export_import",
        "FILENAME": "macroeconomics_export_import",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/48-49/xuat-nhap-khau.htm",
    },
    "IPI": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/ipi",
        "FILENAME": "macroeconomics_ipi",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/46/san-xuat-cong-nghiep.htm",
    },
    "FDI": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/fdi",
        "FILENAME": "macroeconomics_fdi",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/50/fdi.htm",
    },
    "M2": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/m2",
        "FILENAME": "macroeconomics_m2",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/51/tin-dung.htm",
    },
    "RETAIL": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/retail",
        "FILENAME": "macroeconomics_retail",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/47/ban-le.htm",
    },
    "POPULATION_UNEMPLOYMENT": {
        "FOLDER": f"{SCRAPER_MACROECONOMICS_DIR}/population_unemployment",
        "FILENAME": "macroeconomics_population",
        "URL": "https://finance.vietstock.vn/du-lieu-vi-mo/55-56/dan-so-va-lao-dong.htm",
    },
}

# ===========================
# STOCK MARKET CONFIGURATION
# ===========================
SCRAPER_STOCK_MARKET_DIR = f"{SCRAPER_RAW_DATA_DIR}/stock_market"
STOCK_MARKET_INDICATORS = {
    "VN_INDEX": {
        "FOLDER": f"{SCRAPER_STOCK_MARKET_DIR}/vn_index",
        "FILENAME": "stock_market_vnindex",
        "URL": "https://cafef.vn/du-lieu/du-lieu-download.chn",
    },
    "VN30_INDEX": {
        "FOLDER": f"{SCRAPER_STOCK_MARKET_DIR}/vn30_index",
        "FILENAME": "stock_market_vn30index",
        "URL": "https://cafef.vn/du-lieu/lich-su-giao-dich-vn30index-1.chn#data",
    },
    "VN100_INDEX": {
        "FOLDER": f"{SCRAPER_STOCK_MARKET_DIR}/vn100_index",
        "FILENAME": "stock_market_vn100index",
        "URL": "https://cafef.vn/du-lieu/lich-su-giao-dich-vn100-index-1.chn",
    },
    "HNX_INDEX": {
        "FOLDER": f"{SCRAPER_STOCK_MARKET_DIR}/hnx_index",
        "FILENAME": "stock_market_hnxindex",
        "URL": "https://cafef.vn/du-lieu/du-lieu-download.chn",
    },
    "HNX30_INDEX": {
        "FOLDER": f"{SCRAPER_STOCK_MARKET_DIR}/hnx30_index",
        "FILENAME": "stock_market_hnx30index",
        "URL": "https://cafef.vn/du-lieu/lich-su-giao-dich-hnx30-index-1.chn",
    },
    "UPCOM_INDEX": {
        "FOLDER": f"{SCRAPER_STOCK_MARKET_DIR}/upcom_index",
        "FILENAME": "stock_market_upcomindex",
        "URL": "https://cafef.vn/du-lieu/lich-su-giao-dich-upcom-index-1.chn",
    },
}

# ===========================
# ENTERPRISE CONFIGURATION
# ===========================
SCRAPER_ENTERPRISE_DIR = f"{SCRAPER_RAW_DATA_DIR}/enterprise"
ENTERPRISE_INDICATORS = {
    "FINANCE_INFO": {
        "FOLDER": f"{SCRAPER_ENTERPRISE_DIR}/finance_info",
        "FILENAME": "enterprise_finance_info",
        "URL": "https://cafef.vn/",
    },
    "DAILY_PRICE": {
        "FOLDER": f"{SCRAPER_ENTERPRISE_DIR}/daily_price",
        "FILENAME": "enterprise_daily_price",
        "URL": "https://cafef.vn/du-lieu/du-lieu-download.chn",
    },
}
