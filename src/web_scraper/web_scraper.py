from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from bs4 import BeautifulSoup

import csv

import os
import time

from logger.logger import Logger
from utils.constants import *


class WebScraper:
    def __init__(self, logger: Logger):
        self.logger: Logger = logger
        self._create_folder_raw_data()

    # Top-level raw_data folder
    def _create_folder_raw_data(self):
        self.logger.log_info(
            f'Creating folder "{SCRAPER_RAW_DATA_DIR}". Path: "{SCRAPER_RAW_DATA_DIR}"'
        )
        os.makedirs(SCRAPER_RAW_DATA_DIR, exist_ok=True)

        self._create_folder_macroeconomics()
        self._create_folder_stock_market()
        self._create_folder_enterprise()

    # Macroeconomics folders
    def _create_folder_macroeconomics(self):
        self.logger.log_info(
            f'Creating folder "{SCRAPER_MACROECONOMICS_DIR}". Path: "{SCRAPER_MACROECONOMICS_DIR}"'
        )
        os.makedirs(SCRAPER_MACROECONOMICS_DIR, exist_ok=True)

        for key, indicator in MACROECONOMICS_INDICATORS.items():
            folder_path = indicator.get("FOLDER")
            self.logger.log_info(
                f'Creating folder "{folder_path}". Path: "{folder_path}"'
            )
            os.makedirs(folder_path, exist_ok=True)

    # Stock market folders
    def _create_folder_stock_market(self):
        self.logger.log_info(
            f'Creating folder "{SCRAPER_STOCK_MARKET_DIR}". Path: "{SCRAPER_STOCK_MARKET_DIR}"'
        )
        os.makedirs(SCRAPER_STOCK_MARKET_DIR, exist_ok=True)

        for key, indicator in STOCK_MARKET_INDICATORS.items():
            folder_path = indicator.get("FOLDER")
            self.logger.log_info(
                f'Creating folder "{folder_path}". Path: "{folder_path}"'
            )
            os.makedirs(folder_path, exist_ok=True)

    # Enterprise folders
    def _create_folder_enterprise(self):
        self.logger.log_info(
            f'Creating folder "{SCRAPER_ENTERPRISE_DIR}". Path: "{SCRAPER_ENTERPRISE_DIR}"'
        )
        os.makedirs(SCRAPER_ENTERPRISE_DIR, exist_ok=True)

        for key, indicator in ENTERPRISE_INDICATORS.items():
            folder_path = indicator.get("FOLDER")
            self.logger.log_info(
                f'Creating folder "{folder_path}". Path: "{folder_path}"'
            )
            os.makedirs(folder_path, exist_ok=True)
