from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from bs4 import BeautifulSoup

import csv

import os
import time
from typing import List, Tuple

from logger.logger import Logger
from utils.constants import *


class WebScraper:
    def __init__(self, logger: Logger):
        self.logger: Logger = logger
        self._create_folder_raw_data()

        chrome_options = Options()
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        self.web_driver: ChromiumDriver = webdriver.Chrome(options=chrome_options)

        self.bs4_parser: BeautifulSoup = None

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

    def _update_bs4_parser(self):
        self.bs4_parser = BeautifulSoup(self.web_driver.page_source, "html.parser")

    def _navigate_to_url(self, url: str):
        self.logger.log_info(f'Navigating to URL: "{url}"')
        self.web_driver.get(url)
        self._update_bs4_parser()

    def _extract_tbl_macro_data(self) -> Tuple[List, List]:
        # Extract data from the table
        table = self.bs4_parser.find("table", id="tbl-macro-data")

        # Extract headers
        headers = []
        thead = table.find("thead")
        if thead:
            header_row = thead.find_all("tr")[
                -1
            ]  # use the last row if rowspan is present
            headers = [th.get_text(strip=True) for th in header_row.find_all("th")]

        # Extract rows
        rows = []
        tbody = table.find("tbody")
        for tr in tbody.find_all("tr"):
            # Skip group title rows
            if tr.get("class") and "group" in tr.get("class"):
                continue
            row = []
            for td in tr.find_all(["td"]):
                text = td.get_text(strip=True)
                row.append(text)
            rows.append(row)

        return (headers, rows)

    def _scrape_macroeconomics_data_gdp_by_year(self, file_path: str, year: int):
        # Select time unit
        time_unit_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select""",
        )
        time_unit_select_element = Select(time_unit_element)
        time_unit_select_element.select_by_visible_text("Quý")

        # Select time period
        from_quarter_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select""",
        )
        from_quarter_select_element = Select(from_quarter_element)
        from_quarter_select_element.select_by_visible_text("Q1")

        from_year_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select""",
        )
        from_year_select_element = Select(from_year_element)
        from_year_select_element.select_by_visible_text(str(year))

        to_quarter_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select""",
        )
        to_quarter_select_element = Select(to_quarter_element)
        to_quarter_select_element.select_by_visible_text("Q4")

        to_year_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select""",
        )
        to_year_select_element = Select(to_year_element)
        to_year_select_element.select_by_visible_text(str(year))

        # View data in 01 year period
        view_button_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button""",
        )
        view_button_element.click()

        time.sleep(2)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        # Write to CSV
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_gdp(self):
        self.logger.log_info("Start scraping macroeconomics data for GDP.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS.get("GDP").get("URL"))

        # Scrape data each year from 2000 to now
        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        for year in range(start_year, current_year + 1):
            self.logger.log_info(f"Scraping data for year: {year}")

            # Check if the file already exists
            folder_path = MACROECONOMICS_INDICATORS.get("GDP").get("FOLDER")
            file_name = MACROECONOMICS_INDICATORS.get("GDP").get("FILENAME")
            file_path = f"{folder_path}/{file_name}_{year}.csv"
            if os.path.exists(file_path):
                self.logger.log_info(f"File already exists: {file_path}")
                continue
            else:
                self._scrape_macroeconomics_data_gdp_by_year(file_path, year)

    def _scrape_macroeconomics_data_cpi_by_year(self, file_path: str, year: int):
        # Select time unit
        time_unit_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select""",
        )
        time_unit_select_element = Select(time_unit_element)
        time_unit_select_element.select_by_visible_text("Tháng")

        # Select time period
        from_month_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select""",
        )
        from_month_select_element = Select(from_month_element)
        from_month_select_element.select_by_visible_text("1")

        from_year_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select""",
        )
        from_year_select_element = Select(from_year_element)
        from_year_select_element.select_by_visible_text(str(year))

        to_month_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select""",
        )
        to_month_select_element = Select(to_month_element)
        to_month_select_element.select_by_visible_text("12")

        to_year_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select""",
        )
        to_year_select_element = Select(to_year_element)
        to_year_select_element.select_by_visible_text(str(year))

        # View data in 01 year period
        view_button_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button""",
        )
        view_button_element.click()

        time.sleep(2)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        # Write to CSV
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_cpi(self):
        self.logger.log_info("Start scraping macroeconomics data for CPI.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS.get("CPI").get("URL"))

        # Scrape data each year from 2000 to now
        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        for year in range(start_year, current_year + 1):
            self.logger.log_info(f"Scraping data for year: {year}")

            # Check if the file already exists
            folder_path = MACROECONOMICS_INDICATORS.get("CPI").get("FOLDER")
            file_name = MACROECONOMICS_INDICATORS.get("CPI").get("FILENAME")
            file_path = f"{folder_path}/{file_name}_{year}.csv"
            if os.path.exists(file_path):
                self.logger.log_info(f"File already exists: {file_path}")
                continue
            else:
                self._scrape_macroeconomics_data_cpi_by_year(file_path, year)

    def scrape_macroeconomics_data(self):
        self.logger.log_info("Start scraping macroeconomics data.")

        # GDP
        self._scrape_macroeconomics_data_gdp()

        # CPI
        self._scrape_macroeconomics_data_cpi()

    def scrape_stock_market_data(self):
        self.logger.log_info("Start scraping stock market data.")

    def scrape_enterprise_data(self):
        self.logger.log_info("Start scraping enterprise data.")

    def start_scraping(self):
        self.logger.log_info("Start scraping data.")

        self.scrape_macroeconomics_data()
        self.scrape_stock_market_data()
        self.scrape_enterprise_data()

        self.logger.log_info("Finished scraping data.")
