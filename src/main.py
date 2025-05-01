from logger.logger import Logger, LogType
from utils.constants import LOGGER_LOG_FILE_NAME
from web_scraper.web_scraper import WebScraper


def main():
    my_logger = Logger(file_name=LOGGER_LOG_FILE_NAME)
    my_web_scraper = WebScraper(my_logger)
    my_web_scraper.scrape_macroeconomics_data_gdp()


if __name__ == "__main__":
    main()
