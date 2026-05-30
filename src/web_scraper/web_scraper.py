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

    def _initialize_web_driver_and_bs4_parser(
        self,
    ) -> Tuple[ChromiumDriver, BeautifulSoup]:
        web_driver: ChromiumDriver = webdriver.Chrome(options=self._chrome_options)
        bs4_parser: BeautifulSoup = BeautifulSoup(web_driver.page_source, "html.parser")
        web_driver.minimize_window()

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

    def _input_text(
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

    def _extract_table_by_class(
        self, bs4_parser: BeautifulSoup, class_name: str
    ) -> Tuple[List, List]:
        table = bs4_parser.find("table", class_=class_name)
        return self._extract_table(table)

    def _find_first_valid_element_by_xpath(
        self, web_driver: ChromiumDriver, xpaths: List[str]
    ):
        for xpath in xpaths:
            elements = web_driver.find_elements(By.XPATH, xpath)
            if elements:
                return elements[0]
        return False

    def _find_first_valid_element_by_class(
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

    def _find_first_valid_xpath(self, web_driver: ChromiumDriver, xpaths: List[str]):
        for xpath in xpaths:
            elements = web_driver.find_elements(By.XPATH, xpath)
            if elements:
                return xpath
        return False

    def _wait_until_text_not_equals(
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

    def _remove_elements_by_xpath(self, web_driver: ChromiumDriver, xpath: str) -> None:
        try:
            elements = web_driver.find_elements(By.XPATH, xpath)
            for index in range(len(elements)):
                web_driver.execute_script("arguments[0].remove();", elements[index])
        except Exception as e:
            self._logger.log_warning(f"Failed to remove elements by xpath: {xpath}")

    def _scrape_data_macroeconomics_exchange_rate_usd_vnd(
        self, key: Tuple[ScrapeMainType, ScrapeSubType]
    ) -> None:
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            scrape_main_type = key[0].value
            scrape_sub_type = key[1].value
            folder_path = (
                f"{SCRAPER_BRONZE_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
            )
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

            web_driver, bs4_parser = self._navigate_to_url(web_driver, full_url)

            table_class = "table yf-u4m6f0 noDl hideOnPrint"
            headers, rows = self._extract_table_by_class(bs4_parser, table_class)

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

        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        stop_event = threading.Event()

        def _dialog_remover_loop():
            dialog_xpath = '//*[@id="overlap-manager-root"]/div[3]/div'
            while not stop_event.is_set():
                self._remove_elements_by_xpath(web_driver, dialog_xpath)
                time.sleep(0.5)

        dialog_thread = threading.Thread(target=_dialog_remover_loop, daemon=True)

        try:
            scrape_main_type = key[0].value
            scrape_sub_type = key[1].value
            folder_path = (
                f"{SCRAPER_BRONZE_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
            )
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
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)

            # XPATHS
            select_date_range_button_xpath = (
                "/html/body/div[2]/div/div[5]/div[2]/div/div[2]/div/button"
            )
            custom_range_button_xpath = '//*[@id="CustomRange"]'
            input_start_date_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div/span/span[1]/input'
            input_end_date_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]/div/div/div[2]/div[1]/div/div/div/span/span[1]/input'
            apply_range_button_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[4]/div/span/button'
            canvas_xpath = '//canvas[@data-qa-id="pane-top-canvas"]'

            # Step 1: Set date range
            self._click_element(web_driver, select_date_range_button_xpath)
            self._click_element(web_driver, custom_range_button_xpath)

            dialog_thread.start()

            self._input_text(
                web_driver,
                input_start_date_xpath,
                start_time_date_str,
                char_by_char=True,
                confirm="tab",
            )
            self._input_text(
                web_driver,
                input_end_date_xpath,
                current_time_date_str,
                char_by_char=True,
                confirm="tab",
            )

            self._click_element(web_driver, apply_range_button_xpath)
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # Step 2: Snap to last bar, zoom in, hover
            web_driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
            time.sleep(0.5)

            for _ in range(10):
                actions = ActionChains(web_driver)
                actions.key_down(Keys.CONTROL).send_keys(Keys.ARROW_UP).key_up(
                    Keys.CONTROL
                ).perform()
                time.sleep(0.1)
            time.sleep(0.3)

            web_driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
            time.sleep(0.5)

            canvas = WebDriverWait(web_driver, 10).until(
                EC.presence_of_element_located((By.XPATH, canvas_xpath))
            )

            canvas_width = canvas.size["width"]
            canvas_height = canvas.size["height"]
            hover_x = (canvas_width // 2) - 5
            hover_y = canvas_height // 2

            self._logger.log_info(f"Canvas size: {canvas_width} x {canvas_height}")

            actions = ActionChains(web_driver)
            actions.move_to_element_with_offset(canvas, hover_x, hover_y).perform()
            time.sleep(0.5)

            # Step 3: Get last data point date from details widget
            label_date_js = """
            try {
                const items = document.querySelectorAll('.item-DVns3SZf');
                for (const item of items) {
                    const title = item.querySelector('.title-DVns3SZf');
                    const data = item.querySelector('.data-DVns3SZf');
                    if (title && title.innerText.trim() === 'Observation period') {
                        return data ? data.innerText.trim() : '';
                    }
                }
                return '';
            } catch(e) { return ''; }
            """
            label_date_raw = web_driver.execute_script(label_date_js)
            self._logger.log_info(f"Label date raw: {label_date_raw}")

            from datetime import datetime as dt
            try:
                label_date_str = dt.strptime(label_date_raw, "%d %b %Y").strftime("%Y-%m-%d")
            except Exception:
                label_date_str = current_time_date_str
            self._logger.log_info(f"Label date parsed: {label_date_str}")

            # Step 4: Compute match_key and initial_crosshair_index using baseIndex
            setup_js = """
            try {
                const collection = window._exposed_chartWidgetCollection;
                const chartWidget = collection.activeChartWidget._value;
                const model = chartWidget._modelWV._value.m_model;
                const timeScale = model._timeScale;
                const items = timeScale._points._items;
                const keys = Object.keys(items).map(Number).sort((a, b) => a - b);
                const baseIndex = timeScale._baseIndex;
                const parts = arguments[0].split('-');
                const labelTs = Date.UTC(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])) / 1000;
                let matchKey = null;
                for (const k of keys) {
                    if (items[k] === labelTs) { matchKey = k; break; }
                }
                return JSON.stringify({ match_key: matchKey, initial_crosshair_index: baseIndex });
            } catch(e) { return null; }
            """
            setup_result = json.loads(web_driver.execute_script(setup_js, label_date_str))
            match_key = setup_result["match_key"]
            initial_crosshair_index = setup_result["initial_crosshair_index"]
            self._logger.log_info(f"Match key: {match_key}, Initial crosshair: {initial_crosshair_index}")

            # Step 5: JS to read date and value together
            date_value_js = """
            try {
                const collection = window._exposed_chartWidgetCollection;
                const chartWidget = collection.activeChartWidget._value;
                const model = chartWidget._modelWV._value.m_model;
                const crosshair = model._crossHairSource;
                const timeScale = model._timeScale;
                const items = timeScale._points._items;
                const crosshairIndex = crosshair.index;
                const matchKey = arguments[0];
                const initialCrosshairIndex = arguments[1];
                const actualKey = matchKey + (crosshairIndex - initialCrosshairIndex);
                const timestamp = items[actualKey];

                // Skip bars with no real data
                const lastBarCloseTime = model._mainSeries._lastBarCloseTime;
                if (!timestamp || timestamp >= lastBarCloseTime) {
                    return JSON.stringify({ date: '', value: '' });
                }

                const date = new Date(timestamp * 1000).toISOString().split('T')[0];

                const valueItems = document.querySelectorAll('.valueItem-l31H9iuA');
                let value = '';
                for (const item of valueItems) {
                    if (!item.className.includes('blockHidden') && !item.className.includes('unimportant')) {
                        const valueEl = item.querySelector('.valueValue-l31H9iuA');
                        if (valueEl && valueEl.innerText.trim() !== '' && valueEl.innerText.trim() !== '∅') {
                            value = valueEl.innerText.trim();
                            break;
                        }
                    }
                }

                return JSON.stringify({ date: date, value: value });
            } catch(e) { return JSON.stringify({ date: '', value: '' }); }
            """

            first = json.loads(web_driver.execute_script(date_value_js, match_key, initial_crosshair_index))
            self._logger.log_info(f"First date: {first.get('date')}, value: {first.get('value')}")

            # Step 6: Scrape by pressing LEFT arrow day by day
            total_days = (current_time.date() - start_time.date()).days
            seen_dates = set()
            records = []

            self._logger.log_info(f"Starting hover scrape for {total_days} days.")

            for i in range(total_days + 1):
                try:
                    result = json.loads(
                        web_driver.execute_script(date_value_js, match_key, initial_crosshair_index)
                    )
                    date_text = result.get("date", "")
                    value_text = result.get("value", "")
                except Exception:
                    date_text = ""
                    value_text = ""

                if date_text and date_text not in seen_dates:
                    seen_dates.add(date_text)
                    self._logger.log_debug(f"{date_text}: {value_text}")
                    records.append((date_text, value_text))

                if date_text and date_text <= start_time_date_str:
                    break

                actions = ActionChains(web_driver)
                actions.move_to_element_with_offset(canvas, hover_x, hover_y).perform()
                web_driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ARROW_LEFT)
                time.sleep(0.1)

            # Step 7: Write to CSV
            records.reverse()  # oldest → newest
            with open(file_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["date", "value"])
                writer.writerows(records)

            self._logger.log_info(f"Saved {len(records)} records to {file_path}.")

        finally:
            stop_event.set()
            if dialog_thread.is_alive():
                dialog_thread.join()
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _wait_loading_done(self, web_driver):
        loading_xpath = '//*[@id="loading-table-owner"]'

        while True:
            el = self._find_first_valid_element_by_xpath(web_driver, [loading_xpath])
            if "flex" not in el.get_attribute("style").lower():
                break
            time.sleep(0.05)

    def _is_no_result(self, web_driver) -> bool:
        no_result_xpath = '//*[@id="render-table-owner"]/tr/td'
        el = self._find_first_valid_element_by_xpath(web_driver, [no_result_xpath])

        return el and el.text.lower() == "không có kết quả phù hợp"

    def _scrape_stock_data(
        self,
        key: Tuple[ScrapeMainType, ScrapeSubType],
        url: str,
        column_names: list[str],
        find_button_xpath: str,
        next_page_xpath: str,
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        scrape_main_type = get_value(key[0])
        scrape_sub_type = get_value(key[1])

        folder_path = f"{SCRAPER_BRONZE_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
        file_name = scrape_sub_type

        os.makedirs(folder_path, exist_ok=True)

        # Prepare date ranges
        start_date = first_day_of_month(SCRAPER_START_DATE)
        end_date = SCRAPER_END_DATE
        month_list = month_ranges(start_date, end_date)

        # 👉 Step 1: collect missing files
        missing_jobs = []

        for first_day, last_day in month_list:
            file_path = (
                f"{folder_path}/{file_name}_"
                f"{first_day:%Y-%m-%d}_{last_day:%Y-%m-%d}.csv"
            )

            if not os.path.isfile(file_path):
                missing_jobs.append((first_day, last_day, file_path))
            else:
                self._logger.log_debug(f"File exists: {file_path}, skip.")

        # 👉 Step 2: early exit if nothing to scrape
        if not missing_jobs:
            self._logger.log_info("No missing files. Skipping scraping.")
            return

        # 👉 Step 3: initialize only when needed
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            web_driver, bs4_parser = self._navigate_to_url(web_driver, url)

            for first_day, last_day, file_path in missing_jobs:
                self._logger.log_info(
                    f"Scraping {format_key_for_name(key)}: {first_day:%Y-%m-%d} → {last_day:%Y-%m-%d}"
                )

                self._input_text(
                    web_driver, '//*[@id="date-from"]', f"{first_day:%d/%m/%Y}"
                )
                ActionChains(web_driver).send_keys(Keys.ESCAPE).perform()

                self._input_text(
                    web_driver, '//*[@id="date-to"]', f"{last_day:%d/%m/%Y}"
                )
                ActionChains(web_driver).send_keys(Keys.ESCAPE).perform()

                self._click_element(web_driver, find_button_xpath)
                self._wait_loading_done(web_driver)

                all_data = []

                if self._is_no_result(web_driver):
                    pd.DataFrame(all_data, columns=column_names).to_csv(
                        file_path, index=False
                    )
                    continue

                while True:
                    bs4_parser = self._update_bs4_parser(web_driver)

                    _, rows = self._extract_table_by_id(
                        bs4_parser=bs4_parser,
                        id="owner-contents-table",
                    )
                    all_data.extend(rows)

                    next_btn = self._find_first_valid_element_by_xpath(
                        web_driver, [next_page_xpath]
                    )

                    if "disabled" in next_btn.get_attribute("class").lower():
                        break

                    self._click_element(web_driver, next_page_xpath)
                    self._wait_loading_done(web_driver)

                pd.DataFrame(all_data, columns=column_names).to_csv(
                    file_path, index=False
                )

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _scrape_data_stock_market_price(self, key, url: str):
        self._scrape_stock_data(
            key=key,
            url=url,
            column_names=[
                "code",
                "date",
                "close",
                "adjust",
                "change",
                "matching_volume",
                "matching_value",
                "negotiate_volume",
                "negotiate_value",
                "open",
                "high",
                "low",
            ],
            find_button_xpath='//*[@id="owner-find"]',
            next_page_xpath='//*[@id="divStart"]/div/div[3]/div[3]',
        )

    def _scrape_data_stock_market_order(self, key, url: str):
        self._scrape_stock_data(
            key=key,
            url=url,
            column_names=[
                "code",
                "date",
                "change",
                "number_of_buy_orders",
                "buy_volume",
                "average_volume_per_buy_order",
                "number_of_sell_orders",
                "sell_volume",
                "average_volume_per_sell_order",
                "net_volume",
            ],
            find_button_xpath='//*[@id="divStart"]/div/div[1]/div[1]/div/div[3]/div[1]',
            next_page_xpath='//*[@id="divStart"]/div/div[4]/div[3]',
        )

    def _scrape_data_stock_market_list(
        self,
        key,
        column_names=[
            "code",
            "date",
            "close",
            "adjust",
            "change",
            "matching_volume",
            "matching_value",
            "negotiate_volume",
            "negotiate_value",
            "open",
            "high",
            "low",
        ],
        find_button_xpath='//*[@id="owner-find"]',
        next_page_xpath='//*[@id="divStart"]/div/div[3]/div[3]',
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            scrape_main_type = key[0].value
            scrape_sub_type = key[1].value

            folder_path = (
                f"{SCRAPER_BRONZE_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
            )
            file_name = scrape_sub_type

            os.makedirs(folder_path, exist_ok=True)

            source_info = SCRAPE_MAPPING[key]
            web_driver, bs4_parser = self._navigate_to_url(web_driver, source_info.url)

            end_date = SCRAPER_END_DATE
            get_data = False

            while not get_data:
                file_path = f"{folder_path}/{file_name}_{end_date:%Y-%m-%d}.csv"

                if os.path.isfile(file_path):
                    self._logger.log_debug(f"File exists: {file_path}, skip.")
                    break

                self._logger.log_info(f"Scraping stock list on {end_date:%Y-%m-%d}")

                self._input_text(
                    web_driver, '//*[@id="date-from"]', f"{end_date:%d/%m/%Y}"
                )
                ActionChains(web_driver).send_keys(Keys.ESCAPE).perform()

                self._input_text(
                    web_driver, '//*[@id="date-to"]', f"{end_date:%d/%m/%Y}"
                )
                ActionChains(web_driver).send_keys(Keys.ESCAPE).perform()

                self._click_element(web_driver, find_button_xpath)

                self._wait_loading_done(web_driver)

                all_data = []

                if is_weekend(end_date) or self._is_no_result(web_driver):
                    self._logger.log_info(
                        f"No data found on {end_date:%Y-%m-%d}. Step back a day."
                    )
                    end_date -= timedelta(days=1)
                    continue

                while True:
                    bs4_parser = self._update_bs4_parser(web_driver)

                    _, rows = self._extract_table_by_id(
                        bs4_parser=bs4_parser,
                        id="owner-contents-table",
                    )
                    all_data.extend(rows)

                    next_btn = self._find_first_valid_element_by_xpath(
                        web_driver, [next_page_xpath]
                    )

                    if "disabled" in next_btn.get_attribute("class").lower():
                        break

                    self._click_element(web_driver, next_page_xpath)
                    self._wait_loading_done(web_driver)

                pd.DataFrame(all_data, columns=column_names).to_csv(
                    file_path, index=False
                )
                get_data = True

        finally:
            web_driver.close()

        self._logger.log_info(f'Finish scraping data for "{format_key_for_name(key)}".')

    def _add_tasks_for_stock_market_price_order(
        self,
        _result,
        stock_market_name: str,
        scrape_type: Literal["PRICE", "ORDER"],
    ):
        # Normalize input
        scrape_type = scrape_type.upper()

        if scrape_type not in {"PRICE", "ORDER"}:
            self._logger.log_warning(
                f"Invalid scrape_type: {scrape_type}. Must be PRICE or ORDER."
            )
            return

        # Find stock list file
        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/enterprise/stock_list_{stock_market_name}"
        )
        if not os.path.exists(folder_path):
            self._logger.log_warning(
                f"Folder does not exist: {folder_path}. Skip stock market {stock_market_name}."
            )
            return

        file_path = get_newest_file_path(folder_path, extension=FileExtension.CSV)

        if file_path is None:
            self._logger.log_warning(
                f"File does not exist: {file_path}. Skip stock market {stock_market_name}."
            )
            return

        # Read stock list file
        df = pd.read_csv(file_path)
        df["code"] = df["code"].str.lower()
        stock_list = df["code"].tolist()

        # Add tasks for each stock
        for stock in stock_list:

            if scrape_type in {"PRICE"}:
                price_key = (ScrapeMainType.ENTERPRISE, f"{stock}_price")
                price_url = f"https://cafef.vn/du-lieu/lich-su-giao-dich/{stock_market_name}/{stock}-1.chn"
                self._thread_manager.add_task(
                    Task(
                        format_key_for_name(price_key),
                        self._scrape_data_stock_market_price,
                        key=price_key,
                        url=price_url,
                    )
                )

            if scrape_type in {"ORDER"}:
                order_key = (ScrapeMainType.ENTERPRISE, f"{stock}_order")
                order_url = f"https://cafef.vn/du-lieu/lich-su-giao-dich/{stock_market_name}/{stock}-2.chn"
                self._thread_manager.add_task(
                    Task(
                        format_key_for_name(order_key),
                        self._scrape_data_stock_market_order,
                        key=order_key,
                        url=order_url,
                    )
                )

    def _scrape_data_enterprise_daily_price_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType]
    ):
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')

        # Initialize web driver and bs4 parser
        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()

        try:
            # 1. Initialize folder path and file name
            folder_path = f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = (
                f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}.csv"
            )

            # 3. Remove old file if exists
            remove_all_files_with_extensions(
                logger=self._logger,
                folder_path=folder_path,
                extensions=[FileExtension.CSV],
            )

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

        return key

    def _scrape_data_from(self, key: Tuple[ScrapeMainType, ScrapeSubType]):
        if key not in SCRAPE_MAPPING:
            raise ValueError(f"No mapping found for {key}")

        match (key):

            # region MACROECONOMICS
            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.EXCHANGE_RATE_USD_VND,
            ):
                return self._scrape_data_macroeconomics_exchange_rate_usd_vnd(key)

            case (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.VIETNAM_INTERBANK_RATE,
            ):
                return self._scrape_data_macroeconomics_vietnam_interbank_rate(key)

            # endregion MACROECONOMICS

            # region STOCK_MARKET
            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_INDEX_PRICE,
            ):
                return self._scrape_data_stock_market_price(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_INDEX_ORDER,
            ):
                return self._scrape_data_stock_market_order(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_30_INDEX_PRICE,
            ):
                return self._scrape_data_stock_market_price(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_30_INDEX_ORDER,
            ):
                return self._scrape_data_stock_market_order(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_100_INDEX_PRICE,
            ):
                return self._scrape_data_stock_market_price(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_100_INDEX_ORDER,
            ):
                return self._scrape_data_stock_market_order(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_INDEX_PRICE,
            ):
                return self._scrape_data_stock_market_price(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_INDEX_ORDER,
            ):
                return self._scrape_data_stock_market_order(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_30_INDEX_PRICE,
            ):
                return self._scrape_data_stock_market_price(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_30_INDEX_ORDER,
            ):
                return self._scrape_data_stock_market_order(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.UPCOM_INDEX_PRICE,
            ):
                return self._scrape_data_stock_market_price(
                    key, SCRAPE_MAPPING[key].url
                )

            case (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.UPCOM_INDEX_ORDER,
            ):
                return self._scrape_data_stock_market_order(
                    key, SCRAPE_MAPPING[key].url
                )

            # endregion STOCK_MARKET

            # region ENTERPRISE
            case (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.STOCK_LIST_HOSE,
            ):
                return self._scrape_data_stock_market_list(key)

            case (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.STOCK_LIST_HNX,
            ):
                return self._scrape_data_stock_market_list(key)

            case (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.STOCK_LIST_UPCOM,
            ):
                return self._scrape_data_stock_market_list(key)

            # endregion ENTERPRISE

    def add_macroeconomics_data_scraping_tasks(self):
        self._logger.log_info("Adding macroeconomic data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # EXCHANGE_RATE_USD_VND
        if self._switch_handler.is_enabled(
            "web_scraper", "macroeconomics", "exchange_rate_usd_vnd"
        ):
            key = (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.EXCHANGE_RATE_USD_VND,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # VIETNAM_INTERBANK_RATE
        if self._switch_handler.is_enabled(
            "web_scraper", "macroeconomics", "vietnam_interbank_rate"
        ):
            key = (
                ScrapeMainType.MACROECONOMICS,
                MacroeconomicsSubType.VIETNAM_INTERBANK_RATE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        number_of_task_after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added {number_of_task_after - number_of_task_before} macroeconomic data scraping tasks."
        )

    def add_stock_market_data_scraping_tasks(self):
        self._logger.log_info("Adding stock market data scraping tasks.")
        number_of_task_before = self._thread_manager.get_current_number_of_task()

        # VN_INDEX_PRICE
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "vn_index_price"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_INDEX_PRICE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # VN_INDEX_ORDER
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "vn_index_order"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_INDEX_ORDER,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # VN_30_INDEX_PRICE
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "vn_30_index_price"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_30_INDEX_PRICE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # VN_30_INDEX_ORDER
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "vn_30_index_order"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_30_INDEX_ORDER,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # VN_100_INDEX_PRICE
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "vn_100_index_price"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_100_INDEX_PRICE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # VN_100_INDEX_ORDER
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "vn_100_index_order"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.VN_100_INDEX_ORDER,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # HNX_INDEX_PRICE
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "hnx_index_price"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_INDEX_PRICE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # HNX_INDEX_ORDER
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "hnx_index_order"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_INDEX_ORDER,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # HNX_30_INDEX_PRICE
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "hnx_30_index_price"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_30_INDEX_PRICE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # HNX_30_INDEX_ORDER
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "hnx_30_index_order"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.HNX_30_INDEX_ORDER,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # UPCOM_INDEX_PRICE
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "upcom_index_price"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.UPCOM_INDEX_PRICE,
            )
            self._thread_manager.add_task(
                Task(format_key_for_name(key), self._scrape_data_from, key)
            )

        # UPCOM_INDEX_ORDER
        if self._switch_handler.is_enabled(
            "web_scraper", "stock_market", "upcom_index_order"
        ):
            key = (
                ScrapeMainType.STOCK_MARKET,
                StockMarketSubType.UPCOM_INDEX_ORDER,
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

        # STOCK_LIST_HOSE
        if self._switch_handler.is_enabled(
            "web_scraper", "enterprise", "stock_list_hose"
        ):
            key = (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.STOCK_LIST_HOSE,
            )

            callbacks = []
            if self._switch_handler.is_enabled(
                "web_scraper", "enterprise", "stock_list_hose", "stock_price_hose"
            ):
                callbacks.append(
                    (
                        self._add_tasks_for_stock_market_price_order,
                        (),
                        {"stock_market_name": "hose", "scrape_type": "PRICE"},
                    )
                )

            if self._switch_handler.is_enabled(
                "web_scraper", "enterprise", "stock_list_hose", "stock_order_hose"
            ):
                callbacks.append(
                    (
                        self._add_tasks_for_stock_market_price_order,
                        (),
                        {"stock_market_name": "hose", "scrape_type": "ORDER"},
                    )
                )

            self._thread_manager.add_task(
                Task(
                    format_key_for_name(key),
                    self._scrape_data_from,
                    key,
                    callbacks=callbacks,
                )
            )

        # STOCK_LIST_HNX
        if self._switch_handler.is_enabled(
            "web_scraper", "enterprise", "stock_list_hnx"
        ):
            key = (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.STOCK_LIST_HNX,
            )

            callbacks = []
            if self._switch_handler.is_enabled(
                "web_scraper", "enterprise", "stock_list_hnx", "stock_price_hnx"
            ):
                callbacks.append(
                    (
                        self._add_tasks_for_stock_market_price_order,
                        (),
                        {"stock_market_name": "hnx", "scrape_type": "PRICE"},
                    )
                )

            if self._switch_handler.is_enabled(
                "web_scraper", "enterprise", "stock_list_hnx", "stock_order_hnx"
            ):
                callbacks.append(
                    (
                        self._add_tasks_for_stock_market_price_order,
                        (),
                        {"stock_market_name": "hnx", "scrape_type": "ORDER"},
                    )
                )

            self._thread_manager.add_task(
                Task(
                    format_key_for_name(key),
                    self._scrape_data_from,
                    key,
                    callbacks=callbacks,
                )
            )

        # STOCK_LIST_UPCOM
        if self._switch_handler.is_enabled(
            "web_scraper", "enterprise", "stock_list_upcom"
        ):
            key = (
                ScrapeMainType.ENTERPRISE,
                EnterpriseSubType.STOCK_LIST_UPCOM,
            )

            callbacks = []
            if self._switch_handler.is_enabled(
                "web_scraper", "enterprise", "stock_list_upcom", "stock_price_upcom"
            ):
                callbacks.append(
                    (
                        self._add_tasks_for_stock_market_price_order,
                        (),
                        {"stock_market_name": "upcom", "scrape_type": "PRICE"},
                    )
                )

            if self._switch_handler.is_enabled(
                "web_scraper", "enterprise", "stock_list_upcom", "stock_order_upcom"
            ):
                callbacks.append(
                    (
                        self._add_tasks_for_stock_market_price_order,
                        (),
                        {"stock_market_name": "upcom", "scrape_type": "ORDER"},
                    )
                )

            self._thread_manager.add_task(
                Task(
                    format_key_for_name(key),
                    self._scrape_data_from,
                    key,
                    callbacks=callbacks,
                )
            )

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

        if self._switch_handler.is_enabled("web_scraper", "macroeconomics"):
            self.add_macroeconomics_data_scraping_tasks()

        if self._switch_handler.is_enabled("web_scraper", "stock_market"):
            self.add_stock_market_data_scraping_tasks()

        if self._switch_handler.is_enabled("web_scraper", "enterprise"):
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
