from data_preprocessor.data_preprocessor import DataPreprocessor
from logger.logger import Logger, LogType
from train_test_creator.train_test_creator import TrainTestCreator
from utils.constants import LOG_FILE_BASE
from web_scraper.web_scraper import WebScraper
from utils.enums import *


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_logger.log_info("START")

    # my_web_scraper = WebScraper(logger=my_logger, power=100)
    # my_web_scraper.start_scraping()

    # my_data_preprocessor = DataPreprocessor(logger=my_logger)
    # my_data_preprocessor.ingest_bronze_data()
    # my_data_preprocessor.ingest_silver_data()
    # my_data_preprocessor.ingest_gold_data()

    my_train_test_creator = TrainTestCreator(logger=my_logger)
    # my_train_test_creator.export_common_dataframe_to_db()
    # my_train_test_creator.create_unified_dataframe(stock_code="FPT")
    # my_train_test_creator.create_unified_dataframe(stock_code="GAS")
    gas_train_test_set = my_train_test_creator.create_train_test_set(
        stock_code="GAS",
        input_window_size=360,
        forecast_horizon_size=30,
    )


if __name__ == "__main__":
    main()
