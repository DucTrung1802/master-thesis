from datetime import datetime, timedelta

# General constants
COOL_DOWN_BETWEEN_API_CALL = 1.1  # seconds | MUST BE GREATER THAN 1 second


# Relational database constants
ENABLE_CRAWL_RELATIONAL_DATABASE = True

RELATIONAL_DATABASE_NAME = "SSI_STOCKS"

# Time series database constants
ENABLE_CRAWL_TIME_SERIES_DATABASE = True

DEFAULT_CRAWL_DATA_START_DATE = datetime(2020, 2, 1)
CRAWL_DATA_TIME_INTERVAL = timedelta(days=30)

BUCKET_NAME = "root"
MEASUREMENT_NAME = "ssi_stocks"
