from data_preprocessor.data_preprocessor import DataPreprocessor
from logger.logger import Logger, LogType
from utils.constants import LOG_FILE_BASE
from web_scraper.web_scraper import WebScraper


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_logger.log_info("START")
    # my_web_scraper = WebScraper(logger=my_logger, power=100)
    # my_web_scraper.start_scraping()
    my_data_preprocessor = DataPreprocessor(logger=my_logger)
    my_data_preprocessor.ingest_bronze_data()
    my_data_preprocessor.ingest_silver_data()


if __name__ == "__main__":
    main()
