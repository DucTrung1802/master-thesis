from logger.logger import Logger
from web_scraper.web_scraper import WebScraper
# from data_postprocessor.data_postprocessor import DataPostprocessor
# from data_preprocessor.data_preprocessor import DataPreprocessor
from utils.constants import LOG_FILE_BASE
from utils.enums import *


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_logger.log_info("START")

    my_web_scraper = WebScraper(logger=my_logger, power=100)
    my_web_scraper.start_scraping()

    # my_data_preprocessor = DataPreprocessor(logger=my_logger)
    # my_data_preprocessor.ingest_bronze_data()
    # # my_data_preprocessor.ingest_silver_data()
    # my_data_preprocessor.ingest_gold_data()

    # my_data_postprocessor = DataPostprocessor(logger=my_logger)
    # my_data_postprocessor.export_common_dataframe_to_db()

    # stock_code_list = STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB

    # for stock_code in stock_code_list:
    #     my_logger.log_info(f"Exporting unified dataframe for stock code: {stock_code}")
    #     my_data_postprocessor.export_unified_dataframe(stock_code=stock_code)


if __name__ == "__main__":
    main()
