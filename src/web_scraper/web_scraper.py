# ===== Standard Library =====
import csv
import os
import re
import threading
import time
from pathlib import Path
from typing import List, Optional, Tuple, Literal
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# ===== Third-Party Libraries =====
from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from bs4 import BeautifulSoup, Tag

# ===== Local / Custom Modules =====
from logger.logger import Logger
from utils.constants import *
from utils.enums import *
from utils.utils import *
from utils.switch_handler import SwitchHandler
from dtos.thread_manager_dtos.task import *
from thread_manager.thread_manager import ThreadManager


class WebScraper:
    def __init__(
        self,
        logger: Logger,
        switch_handler: SwitchHandler,
        power: int = THREAD_MANAGER_POWER,
    ):
        self._logger: Logger = logger
        self._switch_handler: SwitchHandler = switch_handler
        self._thread_manager = ThreadManager(logger=self._logger, power=power)

        self._chrome_options = Options()
        self._chrome_options.add_experimental_option(
            "prefs",
            {
                "profile.managed_default_content_settings.images": 2,  # Disable images
                "profile.managed_default_content_settings.stylesheets": 2,  # Disable CSS
                "profile.managed_default_content_settings.javascript": 1,  # Keep JS if needed
            },
        )
        self._chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )

    def _helper_initialize_web_driver_and_bs4_parser(
        self,
    ) -> Tuple[ChromiumDriver, BeautifulSoup]:
        web_driver: ChromiumDriver = webdriver.Chrome(options=self._chrome_options)
        bs4_parser: BeautifulSoup = BeautifulSoup(web_driver.page_source, "html.parser")
        web_driver.minimize_window()

        return (web_driver, bs4_parser)

    def _helper_update_bs4_parser(self, web_driver: ChromiumDriver) -> BeautifulSoup:
        return BeautifulSoup(web_driver.page_source, "html.parser")

    def _helper_navigate_to_url(
        self, web_driver: ChromiumDriver, url: str
    ) -> Tuple[ChromiumDriver, BeautifulSoup]:
        self._logger.log_info(f'Navigating to URL: "{url}"')
        web_driver.get(url)
        time.sleep(SCRAPER_BASE_WAIT_TIME)
        bs4_parser = self._helper_update_bs4_parser(web_driver)

        return (web_driver, bs4_parser)

    def _helper_select_dropdown_by_text(
        self, web_driver: ChromiumDriver, xpath: str, text: str
    ) -> None:
        element = WebDriverWait(web_driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        Select(element).select_by_visible_text(text)

    def _helper_input_text(
        self,
        web_driver: ChromiumDriver,
        xpath: str,
        value: str,
        char_by_char: bool = False,
        char_delay: float = 0.01,
        confirm: Literal["none", "tab", "enter"] = "none",
    ) -> None:
        input_element = WebDriverWait(web_driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        input_element.click()
        time.sleep(0.1)

        input_element.send_keys(Keys.CONTROL + "a")
        input_element.send_keys(Keys.DELETE)
        time.sleep(0.1)

        if char_by_char:
            for char in value:
                input_element.send_keys(char)
                time.sleep(char_delay)
        else:
            input_element.send_keys(value)

        match confirm:
            case "tab":
                input_element.send_keys(Keys.TAB)
            case "enter":
                input_element.send_keys(Keys.ENTER)

    def _helper_click_element(self, web_driver: ChromiumDriver, xpath: str) -> None:
        element = WebDriverWait(web_driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        element.click()

    def _helper_extract_table(self, table: Optional[Tag]):
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

    def _helper_extract_table_by_id(
        self, bs4_parser: BeautifulSoup, id: str
    ) -> Tuple[List, List]:
        # Extract data from the table
        table = bs4_parser.find("table", id=id)
        return self._helper_extract_table(table)

    def _helper_extract_table_by_class(
        self, bs4_parser: BeautifulSoup, class_name: str
    ) -> Tuple[List, List]:
        table = bs4_parser.find("table", class_=class_name)
        return self._helper_extract_table(table)

    def _helper_find_first_valid_element_by_xpath(
        self, web_driver: ChromiumDriver, xpaths: List[str]
    ):
        for xpath in xpaths:
            elements = web_driver.find_elements(By.XPATH, xpath)
            if elements:
                return elements[0]
        return False

    def _helper_find_first_valid_element_by_class(
        self, web_driver: ChromiumDriver, class_names: List[str]
    ):
        for class_name in class_names:
            if " " in class_name:
                selector = "." + ".".join(class_name.split())
                elements = web_driver.find_elements(By.CSS_SELECTOR, selector)
            else:
                elements = web_driver.find_elements(By.CLASS_NAME, class_name)

            if elements:
                return elements[0]
        return False

    def _helper_find_first_valid_xpath(
        self, web_driver: ChromiumDriver, xpaths: List[str]
    ):
        for xpath in xpaths:
            elements = web_driver.find_elements(By.XPATH, xpath)
            if elements:
                return xpath
        return False

    def _helper_wait_until_text_not_equals(
        self,
        web_driver: ChromiumDriver,
        xpath: str,
        expected_text: str,
        timeout: int = 10,
    ) -> bool:
        try:
            WebDriverWait(web_driver, timeout).until(
                lambda driver: driver.find_element(By.XPATH, xpath).text
                != expected_text
            )
            return True
        except TimeoutException:
            self._logger.log_warning(
                f"Timeout waiting for text to NOT equal '{expected_text}' at xpath: {xpath}"
            )
            return False

    def _helper_remove_elements_by_xpath(
        self, web_driver: ChromiumDriver, xpath: str
    ) -> None:
        try:
            elements = web_driver.find_elements(By.XPATH, xpath)
            for index in range(len(elements)):
                web_driver.execute_script("arguments[0].remove();", elements[index])
        except Exception as e:
            self._logger.log_warning(f"Failed to remove elements by xpath: {xpath}")

    def _scrape_links_stocks_vietnam_common_stock_commercial_services(self, key: Tuple):
        self._logger.log_info(f'Start scraping links for "{format_key_for_name(key)}".')

    def _scrape_link_from(self, key: Tuple):

        match (key):

            # region STOCKS
            case (
                "links",
                ScrapeMainType.STOCKS.value,
                Country.VIETNAM.value,
                StockType.COMMON_STOCK.value,
                StockSector.COMMERCIAL_SERVICES.value,
            ):
                return (
                    self._scrape_links_stocks_vietnam_common_stock_commercial_services(
                        key
                    )
                )

            # endregion STOCKS

    def add_trading_view_links_scraping_tasks(self):
        self._logger.log_info(f"Adding Trading View links scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # STOCKS
        if self._switch_handler.is_enabled(
            "web_scraper",
            "trading_view",
            "links",
            f"{ScrapeMainType.STOCKS.value}",
        ):
            key = (
                "links",
                ScrapeMainType.STOCKS.value,
                Country.VIETNAM.value,
                StockType.COMMON_STOCK.value,
                StockSector.COMMERCIAL_SERVICES.value,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_link_from, key)
            )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} Trading View links scraping tasks."
        )

    def _scrape_data_macroeconomics_exchange_rate_usd_vnd(
        self, key: Tuple[ScrapeMainType, ScrapeSubType]
    ) -> None:
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        web_driver, bs4_parser = self._helper_initialize_web_driver_and_bs4_parser()

        try:
            scrape_main_type = key[0].value
            scrape_sub_type = key[1].value
            folder_path = f"{SCRAPER_RAW_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
            file_name = scrape_sub_type

            start_time = SCRAPER_START_DATE
            current_time = datetime.now()
            start_time_second_str = str(int(SCRAPER_START_DATE.timestamp()))
            current_time_second_str = str(int(current_time.timestamp()))
            file_path = f"{folder_path}/{file_name}_{start_time.strftime('%Y-%m-%d')}_{current_time.strftime('%Y-%m-%d')}.csv"

            if os.path.exists(file_path):
                self._logger.log_info(
                    f"File exists: {file_path}, deleting to re-fetch."
                )
                os.remove(file_path)

            os.makedirs(folder_path, exist_ok=True)

            self._logger.log_info(
                f"Scraping {scrape_sub_type} data from {start_time.strftime('%Y-%m-%d')} to {current_time.strftime('%Y-%m-%d')}."
            )

            source_info = SCRAPE_MAPPING[key]
            full_url = (
                source_info.url
                + f"&period1={start_time_second_str}&period2={current_time_second_str}"
            )

            web_driver, bs4_parser = self._helper_navigate_to_url(web_driver, full_url)

            table_class = "table yf-u4m6f0 noDl hideOnPrint"
            headers, rows = self._helper_extract_table_by_class(bs4_parser, table_class)

            with open(file_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_macroeconomics_vietnam_interbank_rate(
        self, key: Tuple[ScrapeMainType, ScrapeSubType]
    ) -> None:
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        web_driver, bs4_parser = self._helper_initialize_web_driver_and_bs4_parser()

        stop_event = threading.Event()

        def _dialog_remover_loop():
            dialog_xpath = '//*[@id="overlap-manager-root"]/div[3]/div'
            while not stop_event.is_set():
                self._helper_remove_elements_by_xpath(web_driver, dialog_xpath)
                time.sleep(0.5)

        dialog_thread = threading.Thread(target=_dialog_remover_loop, daemon=True)

        try:
            scrape_main_type = key[0].value
            scrape_sub_type = key[1].value
            folder_path = f"{SCRAPER_RAW_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
            file_name = scrape_sub_type

            start_time = SCRAPER_START_DATE
            current_time = datetime.now()

            start_time_date_str = SCRAPER_START_DATE.strftime("%Y-%m-%d")
            current_time_date_str = current_time.strftime("%Y-%m-%d")

            file_path = f"{folder_path}/{file_name}_{start_time_date_str}_{current_time_date_str}.csv"

            os.makedirs(folder_path, exist_ok=True)

            self._logger.log_info(
                f"Scraping {scrape_sub_type} data from {start_time.strftime('%Y-%m-%d')} to {current_time.strftime('%Y-%m-%d')}."
            )

            source_info = SCRAPE_MAPPING[key]
            web_driver, bs4_parser = self._helper_navigate_to_url(
                web_driver, source_info.url
            )

            # XPATHS
            select_date_range_button_xpath = (
                "/html/body/div[2]/div/div[5]/div[2]/div/div[2]/div/button"
            )
            custom_range_button_xpath = '//*[@id="CustomRange"]'
            input_start_date_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div/span/span[1]/input'
            input_end_date_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]/div/div/div[2]/div[1]/div/div/div/span/span[1]/input'
            apply_range_button_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[4]/div/span/button'

            # Step 1: Set date range
            self._helper_click_element(web_driver, select_date_range_button_xpath)
            self._helper_click_element(web_driver, custom_range_button_xpath)

            dialog_thread.start()

            self._helper_input_text(
                web_driver,
                input_start_date_xpath,
                start_time_date_str,
                char_by_char=True,
                confirm="tab",
            )
            self._helper_input_text(
                web_driver,
                input_end_date_xpath,
                current_time_date_str,
                char_by_char=True,
                confirm="tab",
            )

            self._helper_click_element(web_driver, apply_range_button_xpath)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # Step 2: Read all data in one JS call
            bulk_js = """
            try {
                const collection = window._exposed_chartWidgetCollection;
                const chartWidget = collection.activeChartWidget._value;
                const model = chartWidget._modelWV._value.m_model;
                const mainSeries = model._mainSeries;
                const seriesSource = mainSeries._seriesSource;
                const data = seriesSource._data;
                const bars = data.m_bars;
                const items = bars._items;
                const lastBarCloseTime = mainSeries._lastBarCloseTime;

                const parts = arguments[0].split('-');
                const startTs = Date.UTC(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])) / 1000;

                const keys = Object.keys(items).map(Number).sort((a, b) => a - b);

                // Detect isOHLC from first valid item
                let isOHLC = false;
                for (const k of keys) {
                    const item = items[k];
                    if (!item || !item.value) continue;
                    isOHLC = item.value[1] !== item.value[2];
                    break;
                }

                const records = [];
                for (const k of keys) {
                    const item = items[k];
                    if (!item || !item.value) continue;
                    const ts = item.value[0];
                    if (ts >= lastBarCloseTime) continue;
                    if (ts < startTs) continue;
                    if (isOHLC) {
                        records.push([
                            new Date(ts * 1000).toISOString().split('T')[0],
                            item.value[1],
                            item.value[2],
                            item.value[3],
                            item.value[4],
                            item.value[5],
                        ]);
                    } else {
                        records.push([
                            new Date(ts * 1000).toISOString().split('T')[0],
                            item.value[4],
                        ]);
                    }
                }
                return JSON.stringify({ is_ohlc: isOHLC, records: records });
            } catch(e) { return '{"is_ohlc": false, "records": []}'; }
            """

            result = json.loads(web_driver.execute_script(bulk_js, start_time_date_str))
            is_ohlc = result["is_ohlc"]
            records = result["records"]
            self._logger.log_info(f"Fetched {len(records)} records, isOHLC: {is_ohlc}.")

            # Step 3: Write to CSV
            with open(file_path, "w", newline="") as f:
                writer = csv.writer(f)
                if is_ohlc:
                    writer.writerow(["date", "open", "high", "low", "close", "volume"])
                else:
                    writer.writerow(["date", "value"])
                writer.writerows(records)

            self._logger.log_info(f"Saved {len(records)} records to {file_path}.")

        finally:
            stop_event.set()
            if dialog_thread.is_alive():
                dialog_thread.join()
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_from(self, key: Tuple[ScrapeMainType]):

        match (key):

            # region STOCKS
            case (
                ScrapeMainType.STOCKS.value,
                Country.VIETNAM.value,
                StockType.COMMON_STOCK.value,
                StockSector.COMMERCIAL_SERVICES.value,
            ):
                return self._scrape_data_macroeconomics_exchange_rate_usd_vnd(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.VIETNAM_INTERBANK_RATE,
            ):
                return self._scrape_data_macroeconomics_vietnam_interbank_rate(key)

            # endregion STOCKS

    def add_stocks_data_scraping_tasks(self):
        self._logger.log_info(
            f"Adding '{ScrapeMainType.STOCKS.value}' data scraping tasks."
        )
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        if self._switch_handler.is_enabled(
            "web_scraper",
            f"{ScrapeMainType.STOCKS.value}",
            f"{Country.VIETNAM.value}",
            f"{StockType.COMMON_STOCK.value}",
            f"{StockSector.COMMERCIAL_SERVICES.value}",
        ):
            key = (
                ScrapeMainType.STOCKS.value,
                Country.VIETNAM.value,
                StockType.COMMON_STOCK.value,
                StockSector.COMMERCIAL_SERVICES.value,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} macroeconomic data scraping tasks."
        )

    def start_scraping(self):
        self._logger.log_info("Start scraping data using ThreadManager.")

        self._thread_manager.remove_all_tasks()

        # Add tasks
        self._logger.log_info("Adding data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        if self._switch_handler.is_enabled("web_scraper", "trading_view", "links"):
            self.add_trading_view_links_scraping_tasks()

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
