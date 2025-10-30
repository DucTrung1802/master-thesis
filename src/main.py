import os
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
    my_train_test_creator.export_common_dataframe_to_db()

    stock_code_list = STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB

    for stock_code in stock_code_list:
        my_logger.log_info(f"Exporting unified dataframe for stock code: {stock_code}")
        my_train_test_creator.export_unified_dataframe(stock_code=stock_code)

        # # Load data with template "unified_dataframe/unified_{str.lower(stock_code)}.csv"
        # dataframe = my_train_test_creator.load_dataframe(stock_code=stock_code)

        # normalized_df = my_train_test_creator.normalize_unified_dataframe(
        #     dataframe=dataframe
        # )

        # train_test_set = my_train_test_creator.create_train_test_set(
        #     normalized_df=normalized_df,
        #     stock_code=stock_code,
        #     input_window_size=DEFAULT_INPUT_WINDOW_SIZE,
        #     forecast_horizon_size=DEFAULT_FORECAST_HORIZON_SIZE,
        # )


if __name__ == "__main__":
    main()
