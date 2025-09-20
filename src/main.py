import os

from data_preprocessor.data_preprocessor import DataPreprocessor
from logger.logger import Logger, LogType
from train_test_splitter.train_test_splitter import TraninTestSplitter
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

    my_train_test_splitter = TraninTestSplitter(logger=my_logger)

    my_train_test_splitter.connect_to_database(
        database_name=os.getenv("GOLD_POSTGRES_DATABASE")
    )
    gas_df = my_train_test_splitter.select(
        schema_name=Schema.ENTERPRISE.value,
        table_name="gas",
    )

    gas_train_df, gas_test_df = my_train_test_splitter.split_train_test(
        df=gas_df,
        test_size=90,
    )

    gas_sliding_window_list = my_train_test_splitter.create_sliding_window_list(
        df=gas_train_df,
        input_window_length=270,
        forecast_horizon_length=90,
        step_size=90,
        time_base_column_name="date",
    )


if __name__ == "__main__":
    main()
