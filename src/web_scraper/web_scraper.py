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
        time.sleep(SCRAPER_BASE_WAIT_TIME)
        self._update_bs4_parser()

    def _select_dropdown_by_text(self, xpath: str, text: str):
        element = WebDriverWait(self.web_driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        Select(element).select_by_visible_text(text)

    def _input_text(self, xpath: str, value: str):
        input_element = WebDriverWait(self.web_driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        input_element.clear()
        input_element.send_keys(value)

    def _click_element(self, xpath: str):
        element = WebDriverWait(self.web_driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        element.click()

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

    def _scrape_macroeconomics_data_gdp(self):
        self.logger.log_info("Start scraping macroeconomics data for GDP.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["GDP"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        self.logger.log_info(f"Scraping data from {start_year} to {current_year}.")

        folder_path = MACROECONOMICS_INDICATORS["GDP"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["GDP"]["FILENAME"]
        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(f"Scraping GDP data from {start_year} to {current_year}.")

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
        self._select_dropdown_by_text(xpaths["time_unit"], "Quý")
        self._select_dropdown_by_text(xpaths["from_quarter"], "Q1")
        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_quarter"], "Q4")
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        # Click view button
        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        # Write to CSV
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_cpi(self):
        self.logger.log_info("Start scraping macroeconomics data for CPI.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["CPI"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        folder_path = MACROECONOMICS_INDICATORS["CPI"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["CPI"]["FILENAME"]

        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(f"Scraping CPI data from {start_year} to {current_year}.")

        xpaths = {
            "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
            "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
            "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
            "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
            "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
            "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
        }

        self._select_dropdown_by_text(xpaths["time_unit"], "Tháng")
        self._select_dropdown_by_text(xpaths["from_month"], "1")
        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_month"], "12")
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_exchange_rate(self):
        self.logger.log_info("Start scraping macroeconomics data for EXCHANGE_RATE.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["EXCHANGE_RATE"]["URL"])
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
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(
            f"Scraping EXCHANGE_RATE data from {start_year} to {current_year}."
        )

        xpaths = {
            "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[3]/div/div[1]/select',
            "from_date": '//*[@id="txtFromTradeDate"]/input',
            "to_date": '//*[@id="txtToTradeDate"]/input',
            "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[3]/div/button',
        }

        self._select_dropdown_by_text(xpaths["time_unit"], "Ngày")
        self._input_text(xpaths["from_date"], input_start_date)
        self._input_text(xpaths["to_date"], input_current_date)

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_interest_rate(self):
        self.logger.log_info("Start scraping macroeconomics data for INTEREST_RATE.")

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
                self.logger.log_info(f"File already exists: {file_path}")
                continue

            self.logger.log_info(f"Scraping INTEREST_RATE data in {year}.")

            self._navigate_to_url(MACROECONOMICS_INDICATORS["INTEREST_RATE"]["URL"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            interest_rate_tab_xpath = (
                '//*[@id="macro-content"]/div/div/div[3]/div/div[1]/a[2]'
            )
            self._click_element(interest_rate_tab_xpath)
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            self._input_text(xpaths["from_date"], f"01/01/{year}")
            self._input_text(xpaths["to_date"], f"31/12/{year}")

            self._click_element(xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            self._update_bs4_parser()

            headers, rows = self._extract_tbl_macro_data()

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

    def _scrape_macroeconomics_data_export_import(self):
        self.logger.log_info("Start scraping macroeconomics data for EXPORT_IMPORT.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["EXPORT_IMPORT"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        folder_path = MACROECONOMICS_INDICATORS["EXPORT_IMPORT"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["EXPORT_IMPORT"]["FILENAME"]

        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(
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

        self._select_dropdown_by_text(xpaths["time_unit"], "Tháng")
        self._select_dropdown_by_text(xpaths["from_month"], "1")
        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_month"], "12")
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME + 1)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_ipi(self):
        self.logger.log_info("Start scraping macroeconomics data for IPI.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["IPI"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        folder_path = MACROECONOMICS_INDICATORS["IPI"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["IPI"]["FILENAME"]

        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(f"Scraping IPI data from {start_year} to {current_year}.")

        xpaths = {
            "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
            "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
            "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
            "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
            "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
            "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
        }

        self._select_dropdown_by_text(xpaths["time_unit"], "Tháng")
        self._select_dropdown_by_text(xpaths["from_month"], "1")
        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_month"], "12")
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_fdi(self):
        self.logger.log_info("Start scraping macroeconomics data for FDI.")

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

        self._navigate_to_url(MACROECONOMICS_INDICATORS["FDI"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        for year in range(start_year, current_year + 1, 5):
            from_year = year
            to_year = year + 4 if year + 4 <= current_year else current_year
            file_path = f"{folder_path}/{file_name}_{from_year}_{to_year}.csv"
            if os.path.exists(file_path):
                self.logger.log_info(f"File already exists: {file_path}")
                continue

            self.logger.log_info(f"Scraping FDI data from {from_year} to {to_year}.")

            self._select_dropdown_by_text(xpaths["time_unit"], "Tháng")
            self._select_dropdown_by_text(xpaths["from_month"], "1")
            self._select_dropdown_by_text(xpaths["from_year"], str(from_year))
            self._select_dropdown_by_text(xpaths["to_month"], "12")
            self._select_dropdown_by_text(xpaths["to_year"], str(to_year))

            self._click_element(xpaths["view_button"])
            time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

            self._update_bs4_parser()

            headers, rows = self._extract_tbl_macro_data()

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

    def _scrape_macroeconomics_data_m2(self):
        self.logger.log_info("Start scraping macroeconomics data for M2.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["M2"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        folder_path = MACROECONOMICS_INDICATORS["M2"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["M2"]["FILENAME"]

        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(f"Scraping M2 data from {start_year} to {current_year}.")

        xpaths = {
            "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
            "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
            "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
            "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
            "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
            "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
        }

        self._select_dropdown_by_text(xpaths["time_unit"], "Tháng")
        self._select_dropdown_by_text(xpaths["from_month"], "1")
        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_month"], "12")
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME + 3)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_retail(self):
        self.logger.log_info("Start scraping macroeconomics data for RETAIL.")

        self._navigate_to_url(MACROECONOMICS_INDICATORS["RETAIL"]["URL"])
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        folder_path = MACROECONOMICS_INDICATORS["RETAIL"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["RETAIL"]["FILENAME"]

        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(
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

        self._select_dropdown_by_text(xpaths["time_unit"], "Tháng")
        self._select_dropdown_by_text(xpaths["from_month"], "1")
        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_month"], "12")
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _scrape_macroeconomics_data_population_unemployment(self):
        self.logger.log_info(
            "Start scraping macroeconomics data for POPULATION_UNEMPLOYMENT."
        )

        self._navigate_to_url(
            MACROECONOMICS_INDICATORS["POPULATION_UNEMPLOYMENT"]["URL"]
        )
        time.sleep(SCRAPER_BASE_WAIT_TIME)

        start_year = SCRAPER_START_DATE.year
        current_year = datetime.now().year

        folder_path = MACROECONOMICS_INDICATORS["POPULATION_UNEMPLOYMENT"]["FOLDER"]
        file_name = MACROECONOMICS_INDICATORS["POPULATION_UNEMPLOYMENT"]["FILENAME"]

        file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"
        if os.path.exists(file_path):
            self.logger.log_info(f"File already exists: {file_path}")
            return

        self.logger.log_info(
            f"Scraping POPULATION_UNEMPLOYMENT data from {start_year} to {current_year}."
        )

        xpaths = {
            "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
            "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
            "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
        }

        self._select_dropdown_by_text(xpaths["from_year"], str(start_year))
        self._select_dropdown_by_text(xpaths["to_year"], str(current_year))

        self._click_element(xpaths["view_button"])
        time.sleep(SCRAPER_BASE_WAIT_TIME + 2)

        self._update_bs4_parser()

        headers, rows = self._extract_tbl_macro_data()

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def scrape_macroeconomics_data(self):
        self.logger.log_info("Start scraping macroeconomics data.")

        # GDP
        self._scrape_macroeconomics_data_gdp()

        # CPI
        self._scrape_macroeconomics_data_cpi()

        # Exchange rate
        self._scrape_macroeconomics_data_exchange_rate()

        # Interest rate
        self._scrape_macroeconomics_data_interest_rate()

        # Export + Import
        self._scrape_macroeconomics_data_export_import()

        # IPI
        self._scrape_macroeconomics_data_ipi()

        # FDI
        self._scrape_macroeconomics_data_fdi()

        # M2
        self._scrape_macroeconomics_data_m2()

        # Retail
        self._scrape_macroeconomics_data_retail()

        # Population
        self._scrape_macroeconomics_data_population_unemployment()

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
