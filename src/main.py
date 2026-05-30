from logger.logger import LogType, Logger
from web_scraper.web_scraper import WebScraper
from data_preprocessor.data_preprocessor import DataPreprocessor
from data_postprocessor.data_postprocessor import DataPostprocessor, MarketIndexConfig
from utils.switch_handler import SwitchHandler
from utils.constants import LOG_FILE_BASE
from utils.enums import *
from plyer import notification


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    my_logger.log_info("START")

    my_switch_handler = SwitchHandler(logger=my_logger)

    my_web_scraper = WebScraper(
        logger=my_logger, switch_handler=my_switch_handler, power=100
    )
    my_web_scraper.start_scraping()

    my_data_preprocessor = DataPreprocessor(
        logger=my_logger,
        switch_handler=my_switch_handler,
    )
    my_data_preprocessor.ingest_bronze_data()
    my_data_preprocessor.ingest_silver_data()
    my_data_preprocessor.ingest_gold_data()

    stock_code_list = STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB

    my_data_postprocessor = DataPostprocessor(
        logger=my_logger,
        switch_handler=my_switch_handler,
        stock_list=stock_code_list,
        include_macroeconomics=False,  # commented out for now
        market_index_configs=[
            MarketIndexConfig(index_code="VNINDEX", prefix="vnindex"),
            MarketIndexConfig(index_code="HNX-INDEX", prefix="hnx_index"),
            MarketIndexConfig(index_code="UPCOM-INDEX", prefix="upcom_index"),
        ],
    )
    my_data_postprocessor.export_unified_dataframe()

    # for stock_code in stock_code_list:
    #     my_logger.log_info(f"Exporting unified dataframe for stock code: {stock_code}")
    #     my_data_postprocessor.export_unified_dataframe(stock_code=stock_code)

    notification.notify(
        title="Complete main.py",
        message="main.py has completed execution.",
        timeout=5,  # seconds
    )


if __name__ == "__main__":
    main()
