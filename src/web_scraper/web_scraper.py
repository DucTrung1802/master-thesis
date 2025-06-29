from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup, Tag

import csv

import os
import time
import re
from pathlib import Path
from typing import List, Optional, Tuple


from logger.logger import Logger
from utils.constants import *
from utils.enums import *
from utils.utils import *
from models.thread_manager_models.task import *
from thread_manager.thread_manager import ThreadManager


class WebScraper:
    def __init__(self, logger: Logger, power: int = THREAD_MANAGER_POWER):
        self._logger: Logger = logger
        self._thread_manager = ThreadManager(logger=self._logger, power=power)

        self._chrome_options = Options()
        self._chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )

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

    def _extract_table(self, table: Optional[Tag]):
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

    def _extract_table_by_id(
        self, bs4_parser: BeautifulSoup, id: str
    ) -> Tuple[List, List]:
        # Extract data from the table
        table = bs4_parser.find("table", id=id)

        return self._extract_table(table)

    def _scrape_data_macroeconomics_gdp_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_gdp_worldometer(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            table = bs4_parser.find(
                "table",
                {"class": "datatable w-full border border-zinc-200 datatable-table"},
            )
            headers, rows = self._extract_table(table)

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_cpi_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_exchange_rate_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_date = SCRAPER_START_DATE
            start_year = start_date.year
            input_start_date = start_date.strftime("%d/%m/%Y")

            current_date = datetime.now().date()
            current_year = current_date.year
            input_current_date = current_date.strftime("%d/%m/%Y")

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_interest_rate_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
                    web_driver, source_info.url
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
                WebDriverWait(web_driver, 10).until(
                    EC.presence_of_element_located((By.ID, "tbl-macro-data"))
                )

                bs4_parser = self._update_bs4_parser(web_driver)

                headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

                # Write to CSV
                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_export_import_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6 Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_ipi_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_fdi_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = 2001
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            xpaths = {
                "time_unit": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[1]/select',
                "from_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[2]/select',
                "from_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[3]/select',
                "to_month": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[4]/select',
                "to_year": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/div[5]/select',
                "view_button": '//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button',
            }

            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
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
                WebDriverWait(web_driver, 10).until(
                    EC.presence_of_element_located((By.ID, "tbl-macro-data"))
                )

                bs4_parser = self._update_bs4_parser(web_driver)

                headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

                # Write to CSV
                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_m2_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_retail_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_population_unemployment_vietstock(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            start_year = SCRAPER_START_DATE.year
            current_year = datetime.now().year

            file_path = f"{folder_path}/{file_name}_{start_year}_{current_year}.csv"

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
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
            WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.ID, "tbl-macro-data"))
            )

            bs4_parser = self._update_bs4_parser(web_driver)

            headers, rows = self._extract_table_by_id(bs4_parser, "tbl-macro-data")

            # Write to CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_stock_market_vn_hnx_index_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            xpath = '//*[@id="container"]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[4]/td[3]/a'
            download_link_element = web_driver.find_element("xpath", xpath)
            download_url = download_link_element.get_attribute("href")

            zip_path = file_path.replace(".csv", ".zip")
            self._logger.log_info(f"Downloading ZIP file from: {download_url}")

            # Download file
            download_file(download_url, zip_path, self._logger)

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

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_stock_market_vn_30_index_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(STOCK_MARKET_INDEX_HEADER)

            max_index_xpath = '//*[@id="wraper-content-paging"]/div[11]/p'
            max_index = int(web_driver.find_element(By.XPATH, max_index_xpath).text)
            next_page_button_xpath = '//*[@id="divStart"]/div/div[3]/div[3]'

            for page in range(1, max_index + 1):
                headers, rows = self._extract_table_by_id(
                    bs4_parser, "owner-contents-table"
                )

                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                if page == max_index:
                    break

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

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_stock_market_vn_100_index_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(STOCK_MARKET_INDEX_HEADER)

            max_index_xpath = '//*[@id="wraper-content-paging"]/div[11]/p'
            max_index = int(web_driver.find_element(By.XPATH, max_index_xpath).text)
            next_page_button_xpath = '//*[@id="divStart"]/div/div[3]/div[3]'

            for page in range(1, max_index + 1):
                headers, rows = self._extract_table_by_id(
                    bs4_parser, "owner-contents-table"
                )

                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                if page == max_index:
                    break

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

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_stock_market_hnx_30_index_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(STOCK_MARKET_INDEX_HEADER)

            max_index_xpath = '//*[@id="wraper-content-paging"]/div[11]/p'
            max_index = int(web_driver.find_element(By.XPATH, max_index_xpath).text)
            next_page_button_xpath = '//*[@id="divStart"]/div/div[3]/div[3]'

            for page in range(1, max_index + 1):
                headers, rows = self._extract_table_by_id(
                    bs4_parser, "owner-contents-table"
                )

                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                if page == max_index:
                    break

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

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_stock_market_upcom_index_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(STOCK_MARKET_INDEX_HEADER)

            max_index_xpath = '//*[@id="wraper-content-paging"]/div[11]/p'
            max_index = int(web_driver.find_element(By.XPATH, max_index_xpath).text)
            next_page_button_xpath = '//*[@id="divStart"]/div/div[3]/div[3]'

            for page in range(1, max_index + 1):
                headers, rows = self._extract_table_by_id(
                    bs4_parser, "owner-contents-table"
                )

                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                if page == max_index:
                    break

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

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_enterprise_daily_price_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = (
                f"{SCRAPER_RAW_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            )
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Check if file(s) already exists
            if os.path.exists(file_path):
                self._logger.log_info(f"File already exists: {file_path}")
                return

            # 4. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # 5. Get SourceInfo
            source_info = SCRAPE_MAPPING[key]

            # 6. Navigate to URL
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # 7. Logic for scraping
            xpath = '//*[@id="container"]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[2]/td[4]/a'
            download_link_element = web_driver.find_element("xpath", xpath)
            download_url = download_link_element.get_attribute("href")

            zip_path = file_path.replace(".csv", ".zip")
            self._logger.log_info(f"Downloading ZIP file from: {download_url}")

            # Download file
            download_file(download_url, zip_path, self._logger)

            # Extract ZIP file
            extract_zip_file(self._logger, zip_path, folder_path)

            # Regex to extract stock_market and date from filenames
            pattern = re.compile(
                r"CafeF\.(?P<stock_market>\w+)\.Upto(?P<date>\d{2}\.\d{2}\.\d{4})\.csv"
            )

            for file_name in os.listdir(folder_path):
                match = pattern.match(file_name)
                if match:
                    stock_market = match.group("stock_market")
                    date_str = match.group("date")  # e.g., 29.04.2025
                    # Reformat date to YYYYMMDD
                    date_parts = date_str.split(".")  # ['29', '04', '2025']
                    reformatted_date = (
                        f"{date_parts[2]}{date_parts[1]}{date_parts[0]}"  # '20250429'
                    )
                    new_file_name = f"{stock_market}_upto_{reformatted_date}.csv"
                    src = Path(folder_path) / file_name
                    dst = Path(folder_path) / new_file_name
                    os.rename(src, dst)
                    self._logger.log_info(f"Renamed '{file_name}' -> '{dst}'")
                else:
                    self._logger.log_info(f"Skipped file (no match): {file_name}")

            os.remove(zip_path)
            self._logger.log_info(f"Removed temporary ZIP file: {zip_path}")

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_from(self, key: Tuple[ScrapeMainType, ScrapeSubType, Source]):
        if key not in SCRAPE_MAPPING:
            raise ValueError(f"No mapping found for {key}")

        match (key):

            # MACROECONOMICS
            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.GDP,
                GdpSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_gdp_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.GDP,
                GdpSource.WORLDOMETER,
            ):
                self._scrape_data_macroeconomics_gdp_worldometer(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.CPI,
                CpiSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_cpi_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.EXCHANGE_RATE,
                ExchangeRateSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_exchange_rate_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.INTEREST_RATE,
                InterestRateSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_interest_rate_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.EXPORT,
                ExportImportSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_export_import_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.IPI,
                IpiSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_ipi_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.FDI,
                FdiSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_fdi_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.M2,
                M2Source.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_m2_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.RETAIL,
                RetailSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_retail_vietstock(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.POPULATION_UNEMPLOYMENT,
                PopulationUnemploymentSource.VIETSTOCK,
            ):
                self._scrape_data_macroeconomics_population_unemployment_vietstock(key)

            # STOCK_MARKET
            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_HNX_INDEX,
                VnHnxIndexSource.CAFEF,
            ):
                self._scrape_data_stock_market_vn_hnx_index_cafef(key)

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_30_INDEX,
                Vn30IndexSource.CAFEF,
            ):
                self._scrape_data_stock_market_vn_30_index_cafef(key)

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_100_INDEX,
                Vn100IndexSource.CAFEF,
            ):
                self._scrape_data_stock_market_vn_100_index_cafef(key)

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_30_INDEX,
                Hnx30IndexSource.CAFEF,
            ):
                self._scrape_data_stock_market_hnx_30_index_cafef(key)

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.UPCOM_INDEX,
                UpcomIndexSource.CAFEF,
            ):
                self._scrape_data_stock_market_upcom_index_cafef(key)

            # ENTERPRISE
            case (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.DAILY_PRICE,
                DailyPriceSource.CAFEF,
            ):
                self._scrape_data_enterprise_daily_price_cafef(key)

    def add_macroeconomics_data_scraping_tasks(self):
        self._logger.log_info("Adding macroeconomic data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # MACROECONOMICS_GDP_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_GDP_WORLDOMETER
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.GDP,
            GdpSource.WORLDOMETER,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_CPI_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.CPI,
            CpiSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_EXCHANGE_RATE_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXCHANGE_RATE,
            ExchangeRateSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_INTEREST_RATE_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.INTEREST_RATE,
            InterestRateSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_EXPORT_IMPORT_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.EXPORT,
            ExportImportSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_IPI_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.IPI,
            IpiSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_FDI_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.FDI,
            FdiSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_M2_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.M2,
            M2Source.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_RETAIL_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.RETAIL,
            RetailSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_POPULATION_UNEMPLOYMENT_VIETSTOCK
        key = (
            ScrapeMainType.MACROECONOMICS,
            MacroeconomicsSubType.POPULATION_UNEMPLOYMENT,
            PopulationUnemploymentSource.VIETSTOCK,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # MACROECONOMICS_POPULATION_GOLD_PRICE_INVESTING
        # Gold price is scaped MANUALLY from investing.com

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} macroeconomic data scraping tasks."
        )

    def add_stock_market_data_scraping_tasks(self):
        self._logger.log_info("Adding stock market data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # VN_HNX_INDEX
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_HNX_INDEX,
            VnHnxIndexSource.CAFEF,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # VN30_INDEX
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_30_INDEX,
            Vn30IndexSource.CAFEF,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # VN100_INDEX
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.VN_100_INDEX,
            Vn100IndexSource.CAFEF,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # HNX30_INDEX
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.HNX_30_INDEX,
            Hnx30IndexSource.CAFEF,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        # UPCOM_INDEX
        key = (
            ScrapeMainType.STOCK_MARKET,
            StockMarketSubType.UPCOM_INDEX,
            UpcomIndexSource.CAFEF,
        )
        self._thread_manager.add_task(
            Task(format_key_for_name(key), self._scrape_data_from, key)
        )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} stock market data scraping tasks."
        )

    def add_enterprise_data_scraping_tasks(self):
        self._logger.log_info("Adding enterprise data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # # DAILY_PRICE
        # key = (
        #     ScrapeMainType.ENTERPRISE,
        #     EnterpriseSubType.DAILY_PRICE,
        #     DailyPriceSource.CAFEF,
        # )
        # self._thread_manager.add_task(
        #     Task(format_key_for_name(key), self._scrape_data_from, key)
        # )

        n = 4
        callbacks = self._thread_manager.generate_callbacks(self.my_callback_handler, n)

        self._thread_manager.add_task(
            Task(
                name="hello_1",
                func=self.hello_1,
                callbacks=callbacks,
            )
        )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} enterprise data scraping tasks."
        )

    def hello_1(self):
        print("Hello 1")
        # Configurable number
        n = 4
        result = [["abc", "def"], ["adu", "ahn"], ["hello", "oreo"], ["hi", "aloha"]]
        return result[:n]

    def my_callback_handler(self, sublist, index):
        print(f"[Callback {index}] received: {sublist}")

    def final_callback_handler(self):
        print(f"Final callback received results: Goodbye.")

    def start_scraping(self):
        self._logger.log_info("Start scraping data using ThreadManager.")

        self._thread_manager.remove_all_tasks()

        # Add tasks
        self._logger.log_info("Adding data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # self.add_macroeconomics_data_scraping_tasks()
        # self.add_stock_market_data_scraping_tasks()
        self.add_enterprise_data_scraping_tasks()

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added total {number_of_task_after - number_of_task_before} data scraping tasks."
        )

        # Execute tasks
        self._logger.log_info(
            f"Start executing {self._thread_manager.get_current_number_of_task()} tasks."
        )

        self._thread_manager.execute(final_callback=self.final_callback_handler)

        self._logger.log_info("Finished scraping data.")
