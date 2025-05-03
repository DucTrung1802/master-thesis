from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

import csv

import os
import time
import requests
import zipfile
from typing import List, Tuple

from logger.logger import Logger
from utils.constants import *
from utils.utils import *
from models.thread_manager_models.task import *
from thread_manager.thread_manager import ThreadManager


class WebScraper:
    def __init__(self, logger: Logger, power: int = THREAD_MANAGER_POWER):
        self._logger: Logger = logger
        self._thread_manager = ThreadManager(logger=self._logger, power=power)

        self._create_folder_raw_data()

        self._chrome_options = Options()
        self._chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )

    # Top-level raw_data folder
    def _create_folder_raw_data(self):
        self._logger.log_info(
            f'Creating folder "{SCRAPER_RAW_DATA_DIR}". Path: "{SCRAPER_RAW_DATA_DIR}"'
        )
        os.makedirs(SCRAPER_RAW_DATA_DIR, exist_ok=True)

        self._create_folder_macroeconomics()
        self._create_folder_stock_market()
        self._create_folder_enterprise()

    # Macroeconomics folders
    def _create_folder_macroeconomics(self):
        self._logger.log_info(
            f'Creating folder "{SCRAPER_MACROECONOMICS_DIR}". Path: "{SCRAPER_MACROECONOMICS_DIR}"'
        )
        os.makedirs(SCRAPER_MACROECONOMICS_DIR, exist_ok=True)

        for key, indicator in MACROECONOMICS_INDICATORS.items():
            folder_path = indicator.get("FOLDER")
            self._logger.log_info(
                f'Creating folder "{folder_path}". Path: "{folder_path}"'
            )
            os.makedirs(folder_path, exist_ok=True)

    # Stock market folders
    def _create_folder_stock_market(self):
        self._logger.log_info(
            f'Creating folder "{SCRAPER_STOCK_MARKET_DIR}". Path: "{SCRAPER_STOCK_MARKET_DIR}"'
        )
        os.makedirs(SCRAPER_STOCK_MARKET_DIR, exist_ok=True)

        for key, indicator in STOCK_MARKET_INDICATORS.items():
            folder_path = indicator.get("FOLDER")
            self._logger.log_info(
                f'Creating folder "{folder_path}". Path: "{folder_path}"'
            )
            os.makedirs(folder_path, exist_ok=True)

    # Enterprise folders
    def _create_folder_enterprise(self):
        self._logger.log_info(
            f'Creating folder "{SCRAPER_ENTERPRISE_DIR}". Path: "{SCRAPER_ENTERPRISE_DIR}"'
        )
        os.makedirs(SCRAPER_ENTERPRISE_DIR, exist_ok=True)

        for key, indicator in ENTERPRISE_INDICATORS.items():
            folder_path = indicator.get("FOLDER")
            self._logger.log_info(
                f'Creating folder "{folder_path}". Path: "{folder_path}"'
            )
            os.makedirs(folder_path, exist_ok=True)

    def _initialize_web_driver_and_bs4_parser(
        self,
    ) -> Tuple[ChromiumDriver, BeautifulSoup]:
        web_driver: ChromiumDriver = webdriver.Chrome(options=self._chrome_options)
        bs4_parser: BeautifulSoup = BeautifulSoup(web_driver.page_source, "html.parser")

        return (web_driver, bs4_parser)

    def _update_bs4_parser(self, web_driver: ChromiumDriver) -> BeautifulSoup:
        return BeautifulSoup(web_driver.page_source, "html.parser")

    def _navigate_to_url(
        self, web_driver: ChromiumDriver, url: str
    ) -> Tuple[ChromiumDriver, BeautifulSoup]:
        self._logger.log_info(f'Navigating to URL: "{url}"')
        web_driver.get(url)
        time.sleep(SCRAPER_BASE_WAIT_TIME)
        bs4_parser = self._update_bs4_parser(web_driver)

        return (web_driver, bs4_parser)

    def _select_dropdown_by_text(
        self, web_driver: ChromiumDriver, xpath: str, text: str
    ) -> None:
        element = WebDriverWait(web_driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        Select(element).select_by_visible_text(text)

    def _input_text(self, web_driver: ChromiumDriver, xpath: str, value: str) -> None:
        input_element = WebDriverWait(web_driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        input_element.clear()
        input_element.send_keys(value)

    def _click_element(self, web_driver: ChromiumDriver, xpath: str) -> None:
        element = WebDriverWait(web_driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        element.click()

    def _extract_table_by_id(
        self, bs4_parser: BeautifulSoup, id: str
    ) -> Tuple[List, List]:
        # Extract data from the table
        table = bs4_parser.find("table", id=id)

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

    def _scrape_macroeconomics_data_gdp(self):
        self._logger.log_info("Start scraping macroeconomics data for GDP.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["GDP"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["GDP"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["GDP"]["FILENAME"]
            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping GDP data from {start_year} to {current_year}."
            )

            # Define XPaths
            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_quarter": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_quarter": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            # Select dropdown values
            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Quý")
            self._select_dropdown_by_text(web_driver, xpaths["from_quarter"], "Q1")
            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(web_driver, xpaths["to_quarter"], "Q4")
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            # Click view button
            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for GDP.")

    def _scrape_macroeconomics_data_cpi(self):
        self._logger.log_info("Start scraping macroeconomics data for CPI.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["CPI"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["CPI"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["CPI"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping CPI data from {start_year} to {current_year}."
            )

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Tháng")
            self._select_dropdown_by_text(web_driver, xpaths["from_month"], "1")
            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(web_driver, xpaths["to_month"], "12")
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for CPI.")

    def _scrape_macroeconomics_data_exchange_rate(self):
        self._logger.log_info("Start scraping macroeconomics data for EXCHANGE_RATE.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["EXCHANGE_RATE"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_date = SCRAPER_START_DATE
            start_year = start_date.year
            input_start_date = start_date.strftime("%d/%m/%Y")

            current_date = datetime.now().date()
            current_year = current_date.year
            input_current_date = current_date.strftime("%d/%m/%Y")

            folder_path = MACROECONOMICS_INDICATORS["EXCHANGE_RATE"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["EXCHANGE_RATE"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping EXCHANGE_RATE data from {start_year} to {current_year}."
            )

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[3]/div/div[1]/select',
                "from_date": '//*[@id="txtFromTradeDate"]/input',
                "to_date": '//*[@id="txtToTradeDate"]/input',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[3]/div/button',
            }

            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Ngày")
            self._input_text(web_driver, xpaths["from_date"], input_start_date)
            self._input_text(web_driver, xpaths["to_date"], input_current_date)

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for EXCHANGE_RATE.")

    def _scrape_macroeconomics_data_interest_rate(self):
        self._logger.log_info("Start scraping macroeconomics data for INTEREST_RATE.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["INTEREST_RATE"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["INTEREST_RATE"]["FILENAME"]

            xpaths = {
                "from_date": '//*[@id="txtFromTradeDate"]/input',
                "to_date": '//*[@id="txtToTradeDate"]/input',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[3]/div/button',
            }

            for year in range(start_year, current_year + 1):
                file_path = f"{folder_path}/{file_name}_{year}.csv"
                if os.path.exists(file_path):
                    self._logger.log_info(f"File already exists: {file_path}")
                    continue

                self._logger.log_info(f"Scraping INTEREST_RATE data in {year}.")

                web_driver, bs4_parser = self._navigate_to_url(
                    web_driver, MACROECONOMICS_INDICATORS["INTEREST_RATE"]["URL"]
                )

                interest_rate_tab_xpath = (
                    '//*[@id="macro-content"]/div/div/div[3]/div/div[1]/a[2]'
                )

                # Try to click
                element = WebDriverWait(web_driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, interest_rate_tab_xpath))
                )
                web_driver.execute_script("arguments[0].scrollIntoView(true);", element)
                try:
                    element.click()
                except Exception:
                    web_driver.execute_script("arguments[0].click();", element)
                time.sleep(SCRAPER_BASE_WAIT_TIME + 1)

                self._input_text(web_driver, xpaths["from_date"], f"01/01/{year}")
                self._input_text(web_driver, xpaths["to_date"], f"31/12/{year}")

                self._click_element(web_driver, xpaths["view_button"])
                time.sleep(SCRAPER_BASE_WAIT_TIME + 1)

                bs4_parser = self._update_bs4_parser(web_driver)

                headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for INTEREST_RATE.")

    def _scrape_macroeconomics_data_export_import(self):
        self._logger.log_info("Start scraping macroeconomics data for EXPORT_IMPORT.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["EXPORT_IMPORT"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["EXPORT_IMPORT"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["EXPORT_IMPORT"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping EXPORT_IMPORT data from {start_year} to {current_year}."
            )

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Tháng")
            self._select_dropdown_by_text(web_driver, xpaths["from_month"], "1")
            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(web_driver, xpaths["to_month"], "12")
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 1)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for EXPORT_IMPORT.")

    def _scrape_macroeconomics_data_ipi(self):
        self._logger.log_info("Start scraping macroeconomics data for IPI.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["IPI"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["IPI"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["IPI"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping IPI data from {start_year} to {current_year}."
            )

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Tháng")
            self._select_dropdown_by_text(web_driver, xpaths["from_month"], "1")
            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(web_driver, xpaths["to_month"], "12")
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for IPI.")

    def _scrape_macroeconomics_data_fdi(self):
        self._logger.log_info("Start scraping macroeconomics data for FDI.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            start_year = 2001
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["FDI"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["FDI"]["FILENAME"]

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["FDI"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            for year in range(start_year, current_year + 1, 5):
                from_year = year
                to_year = year + 4 if year + 4 <= current_year else current_year
                file_path = f"{folder_path}/{file_name}_{from_year}_{to_year}.csv"
                if os.path.exists(file_path):
                    self._logger.log_info(f"File already exists: {file_path}")
                    continue

                self._logger.log_info(
                    f"Scraping FDI data from {from_year} to {to_year}."
                )

                self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Tháng")
                self._select_dropdown_by_text(web_driver, xpaths["from_month"], "1")
                self._select_dropdown_by_text(
                    web_driver, xpaths["from_year"], str(from_year)
                )
                self._select_dropdown_by_text(web_driver, xpaths["to_month"], "12")
                self._select_dropdown_by_text(
                    web_driver, xpaths["to_year"], str(to_year)
                )

                self._click_element(web_driver, xpaths["view_button"])
                time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

                bs4_parser = self._update_bs4_parser(web_driver)

                headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for FDI.")

    def _scrape_macroeconomics_data_m2(self):
        self._logger.log_info("Start scraping macroeconomics data for M2.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["M2"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["M2"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["M2"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping M2 data from {start_year} to {current_year}."
            )

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Tháng")
            self._select_dropdown_by_text(web_driver, xpaths["from_month"], "1")
            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(web_driver, xpaths["to_month"], "12")
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 3)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for M2.")

    def _scrape_macroeconomics_data_retail(self):
        self._logger.log_info("Start scraping macroeconomics data for RETAIL.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["RETAIL"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["RETAIL"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["RETAIL"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping RETAIL data from {start_year} to {current_year}."
            )

            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            self._select_dropdown_by_text(web_driver, xpaths["time_unit"], "Tháng")
            self._select_dropdown_by_text(web_driver, xpaths["from_month"], "1")
            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(web_driver, xpaths["to_month"], "12")
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping macroeconomics data for RETAIL.")

    def _scrape_macroeconomics_data_population_unemployment(self):
        self._logger.log_info(
            "Start scraping macroeconomics data for POPULATION_UNEMPLOYMENT."
        )

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, MACROECONOMICS_INDICATORS["POPULATION_UNEMPLOYMENT"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            folder_path = MACROECONOMICS_INDICATORS["POPULATION_UNEMPLOYMENT"]["FOLDER"]
            file_name = MACROECONOMICS_INDICATORS["POPULATION_UNEMPLOYMENT"]["FILENAME"]

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            self._logger.log_info(
                f"Scraping POPULATION_UNEMPLOYMENT data from {start_year} to {current_year}."
            )

            xpaths = {
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            self._select_dropdown_by_text(
                web_driver, xpaths["from_year"], str(start_year)
            )
            self._select_dropdown_by_text(
                web_driver, xpaths["to_year"], str(current_year)
            )

            self._click_element(web_driver, xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(
            "Finish scraping macroeconomics data for POPULATION_UNEMPLOYMENT."
        )

    def add_macroeconomics_data_scraping_tasks(self):
        self._logger.log_info("Adding macroeconomic data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # GDP
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_gdp.__name__,
                self._scrape_macroeconomics_data_gdp,
            )
        )

        # CPI
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_cpi.__name__,
                self._scrape_macroeconomics_data_cpi,
            )
        )

        # Exchange rate
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_exchange_rate.__name__,
                self._scrape_macroeconomics_data_exchange_rate,
            )
        )

        # Interest rate
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_interest_rate.__name__,
                self._scrape_macroeconomics_data_interest_rate,
            )
        )

        # Export + Import
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_export_import.__name__,
                self._scrape_macroeconomics_data_export_import,
            )
        )

        # IPI
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_ipi.__name__,
                self._scrape_macroeconomics_data_ipi,
            )
        )

        # FDI
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_fdi.__name__,
                self._scrape_macroeconomics_data_fdi,
            )
        )

        # M2
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_m2.__name__,
                self._scrape_macroeconomics_data_m2,
            )
        )

        # Retail
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_retail.__name__,
                self._scrape_macroeconomics_data_retail,
            )
        )

        # Population + Unemployment
        self._thread_manager.add_task(
            Task(
                self._scrape_macroeconomics_data_population_unemployment.__name__,
                self._scrape_macroeconomics_data_population_unemployment,
            )
        )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} macroeconomic data scraping tasks."
        )

    def _scrape_stock_market_data_vn_hnx_index(self):
        self._logger.log_info("Start scraping stock market data for VN_HNX_INDEX.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, STOCK_MARKET_INDICATORS["VN_HNX_INDEX"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            current_date = datetime.now()

            folder_path = STOCK_MARKET_INDICATORS["VN_HNX_INDEX"]["FOLDER"]
            file_name = STOCK_MARKET_INDICATORS["VN_HNX_INDEX"]["FILENAME"]

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # Remove all current files in folder_path
            remove_all_files_with_extensions(self._logger, folder_path)

            xpath = '//*[@id="container"]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[4]/td[3]/a'
            download_link_element = web_driver.find_element("xpath", xpath)
            file_url = download_link_element.get_attribute("href")

            zip_path = file_path.replace(".csv", ".zip")
            self._logger.log_info(f"Downloading ZIP file from: {file_url}")

            response = requests.get(file_url)
            if response.status_code == 200:
                with open(zip_path, "wb") as f:
                    f.write(response.content)
                self._logger.log_info(f"ZIP file downloaded to: {zip_path}")
            else:
                self._logger.log_error(
                    f"Failed to download file. Status code: {response.status_code}"
                )
                return

            # Extract ZIP file
            extracted_files = extract_zip_file(self._logger, zip_path, folder_path)

            # Rename the first .csv found to the target file_path
            rename_first_csv_file(self._logger, extracted_files, folder_path, file_path)

            os.remove(zip_path)
            self._logger.log_info(f"Removed temporary ZIP file: {zip_path}")

            self._logger.log_info(
                f"Scraping VN_HNX_INDEX data upto {current_date.strftime('%d/%m/%Y')}."
            )

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping stock market data for VN_HNX_INDEX.")

    def _scrape_stock_market_data_vn30_index(self):
        self._logger.log_info("Start scraping stock market data for VN30_INDEX.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, STOCK_MARKET_INDICATORS["VN30_INDEX"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            current_date = datetime.now()

            folder_path = STOCK_MARKET_INDICATORS["VN30_INDEX"]["FOLDER"]
            file_name = STOCK_MARKET_INDICATORS["VN30_INDEX"]["FILENAME"]

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # Remove all current files in folder_path
            remove_all_files_with_extensions(self._logger, folder_path)

            # Create file and header
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(STOCK_MARKET_INDEX_HEADER)

            max_index_xpath = '//*[@id="wraper-content-paging"]/div[11]/p'
            max_index = int(web_driver.find_element("xpath", max_index_xpath).text)
            next_page_button_xpath = '//*[@id="divStart"]/div/div[3]/div[3]'

            for page in range(1, max_index + 1):
                headers, rows = self._extract_table_by_id(
                    bs4_parser, "owner-contents-table"
                )

                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                # Click next page

                # Get a reference element inside the table BEFORE clicking "Next"
                table_xpath = '//*[@id="owner-contents-table"]'
                old_content = web_driver.find_element(
                    By.XPATH, table_xpath
                ).get_attribute("innerHTML")

                # Click the Next button
                self._click_element(web_driver, next_page_button_xpath)

                # Wait until the content inside the table changes
                WebDriverWait(web_driver, 10).until(
                    lambda driver: driver.find_element(
                        By.XPATH, table_xpath
                    ).get_attribute("innerHTML")
                    != old_content
                )
                bs4_parser = self._update_bs4_parser(web_driver)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping stock market data for VN30_INDEX.")

    def _scrape_stock_market_data_vn100_index(self):
        self._logger.log_info("Start scraping stock market data for VN100_INDEX.")

        try:
            web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, STOCK_MARKET_INDICATORS["VN100_INDEX"]["URL"]
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            current_date = datetime.now()

            folder_path = STOCK_MARKET_INDICATORS["VN100_INDEX"]["FOLDER"]
            file_name = STOCK_MARKET_INDICATORS["VN100_INDEX"]["FILENAME"]

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # Remove all current files in folder_path
            remove_all_files_with_extensions(self._logger, folder_path)

            # Create file and header
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(STOCK_MARKET_INDEX_HEADER)

            max_index_xpath = '//*[@id="wraper-content-paging"]/div[11]/p'
            max_index = int(web_driver.find_element("xpath", max_index_xpath).text)
            next_page_button_xpath = '//*[@id="divStart"]/div/div[3]/div[3]'

            for page in range(1, max_index + 1):
                headers, rows = self._extract_table_by_id(
                    bs4_parser, "owner-contents-table"
                )

                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                # Click next page

                # Get a reference element inside the table BEFORE clicking "Next"
                table_xpath = '//*[@id="owner-contents-table"]'
                old_content = web_driver.find_element(
                    By.XPATH, table_xpath
                ).get_attribute("innerHTML")

                # Click the Next button
                self._click_element(web_driver, next_page_button_xpath)

                # Wait until the content inside the table changes
                WebDriverWait(web_driver, 10).until(
                    lambda driver: driver.find_element(
                        By.XPATH, table_xpath
                    ).get_attribute("innerHTML")
                    != old_content
                )
                bs4_parser = self._update_bs4_parser(web_driver)

        finally:
            web_driver.close()

        self._logger.log_info("Finish scraping stock market data for VN100_INDEX.")

    def add_stock_market_data_scraping_tasks(self):
        self._logger.log_info("Adding stock market data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # VN_HNX_INDEX
        # self._thread_manager.add_task(
        #     Task(
        #         self._scrape_stock_market_data_vn_hnx_index.__name__,
        #         self._scrape_stock_market_data_vn_hnx_index,
        #     )
        # )

        # # VN30_INDEX
        # self._thread_manager.add_task(
        #     Task(
        #         self._scrape_stock_market_data_vn30_index.__name__,
        #         self._scrape_stock_market_data_vn30_index,
        #     )
        # )

        # VN100_INDEX
        self._thread_manager.add_task(
            Task(
                self._scrape_stock_market_data_vn100_index.__name__,
                self._scrape_stock_market_data_vn100_index,
            )
        )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} stock market data scraping tasks."
        )

    def add_enterprise_data_scraping_tasks(self):
        self._logger.log_info("Adding enterprise data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # Add tasks here

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} enterprise data scraping tasks."
        )

    def start_scraping(self):
        self._logger.log_info("Start scraping data using ThreadManager.")

        self._thread_manager.remove_all_tasks()

        # Add tasks
        self._logger.log_info("Adding data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # self.add_macroeconomics_data_scraping_tasks()
        self.add_stock_market_data_scraping_tasks()
        self.add_enterprise_data_scraping_tasks()

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added total {number_of_task_after - number_of_task_before} data scraping tasks."
        )

        # Execute tasks
        self._logger.log_info(
            f"Start executing {self._thread_manager.get_current_number_of_task()} tasks."
        )

        self._thread_manager.execute()

        self._logger.log_info("Finished scraping data.")
