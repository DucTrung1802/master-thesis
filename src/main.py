from logger.logger import Logger, LogType
from utils.constants import LOG_FILE_BASE
from web_scraper.web_scraper import WebScraper


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_web_scraper = WebScraper(my_logger)


if __name__ == "__main__":
    main()
