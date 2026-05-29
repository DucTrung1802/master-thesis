# ===== Standard Library =====
import csv
import os
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple, Literal
from datetime import datetime, timedelta

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

    def _scrape_data_enterprise_stock_information_cafef_callback(
        self, index: int, total: int, stock_codes: List[str]
    ):
        key = (
            ScrapeMainType.ENTERPRISE,
            EnterpriseSubType.STOCK_INFORMATION,
            StockInformationSource.CAFEF,
        )
        self._logger.log_info(
            f'Start scraping data for "{format_key_for_name(key)}" - Callback {index}.'
        )

        web_driver, bs4_parser = self._initialize_web_driver_and_bs4_parser()
        try:

            # 1. Initialize folder path and file name
            folder_path = f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
            file_name = f"{key[2].value}"

            # 2. Initialize start time and current time
            current_date = datetime.now()

            file_path = f"{folder_path}/{file_name}_upto_{current_date.strftime('%Y%m%d')}_{index}.csv"

            # 3. Create folder if not exists
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)

            # Write header to CSV file
            if len(stock_codes) > 0:
                with open(file_path, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(
                        ["Code", "Listed Shares", "Outstanding Shares", "Market Cap"]
                    )

            web_driver, bs4_parser = self._navigate_to_url(
                web_driver, url=SCRAPE_MAPPING[key].url
            )
            time.sleep(SCRAPER_BASE_WAIT_TIME)

            # Write data to CSV file
            count = 0
            for stock_code in stock_codes:
                self._logger.log_info(
                    f"Stock information cafef callback index: {index}/{total} | Scraping stock: {stock_code} | Count: ({count + 1}/{len(stock_codes)})."
                )
                for attempt in range(3):
                    try:
                        search_box_xpath_list = [
                            '//*[@id="CafeF_SearchKeyword_Companyv2"]',
                            '//*[@id="search-header"]',
                        ]
                        search_box_xpath = self._find_first_valid_xpath(
                            web_driver=web_driver, xpaths=search_box_xpath_list
                        )
                        _ = WebDriverWait(web_driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, search_box_xpath))
                        )

                        self._input_text(
                            web_driver=web_driver,
                            xpath=search_box_xpath,
                            value=stock_code,
                        )

                        time.sleep(2)

                        bs4_parser = self._update_bs4_parser(web_driver=web_driver)

                        # Extract list of results
                        result_list = bs4_parser.select(
                            ".ac_results ul li a div p"
                        )  # UI v1
                        if len(result_list) == 0:
                            result_list = bs4_parser.select(
                                ".list-search-result ul li a div p"
                            )  # UI v2

                        if not result_list or len(result_list) == 0:
                            self._logger.log_warning(
                                f"Cannot find stock with code: `{stock_code}`"
                            )
                            break

                        found_stock_code = None
                        for element in result_list:
                            try:
                                found_stock_code = element.get_text().split(" - ")[0]
                                if found_stock_code.lower() == stock_code.lower():
                                    a_tag = element.find_parent("a")
                                    if a_tag:
                                        href = a_tag["href"]
                                        # Click matched element
                                        web_driver.get(f"https://cafef.vn{href}")
                                        break
                            except:
                                continue

                        if not found_stock_code:
                            self._logger.log_warning(
                                f"Cannot find stock with code: `{stock_code}`"
                            )
                            break

                        listed_shares = 0
                        outstanding_shares = 0
                        market_cap = 0

                        stock_name_xpaths = [
                            '//*[@id="symbolbox"]',
                            '//*[@id="contentV1"]/div[2]/div[1]',
                            '//*[@id="real-time-stock-exchange"]',
                        ]
                        WebDriverWait(web_driver, 10).until(
                            lambda driver: self._find_first_valid_element_by_xpath(
                                web_driver=driver, xpaths=stock_name_xpaths
                            )
                        )

                        bs4_parser = self._update_bs4_parser(web_driver)

                        # Listed Shares
                        listed_shares_xpaths = [
                            '//*[@id="contentV1"]/div[4]/div[4]/div/ul/li[2]/div[2]',
                            '//*[@id="content"]/div/div[7]/div[4]/div/ul/li[2]/div[2]',
                            '//*[@id="content"]/div/div[6]/div[4]/div/ul/li[2]/div[2]',
                            '//*[@id="content"]/div/div[5]/div[4]/div/ul/li[2]/div[2]',
                            '//*[@id="transaction-information-table-right"]/div[8]/p[2]',
                        ]
                        listed_shares_component = WebDriverWait(web_driver, 10).until(
                            lambda driver: self._find_first_valid_element_by_xpath(
                                web_driver=driver, xpaths=listed_shares_xpaths
                            )
                        )
                        if listed_shares_component:
                            try:
                                listed_shares = int(
                                    listed_shares_component.text.replace(",", "")
                                )
                            except ValueError:
                                self._logger.log_error(
                                    f"Listed shares not found for {stock_code}. Set listed shares to 0."
                                )
                                listed_shares = 0

                        # Outstanding Shares
                        outstanding_shares_xpaths = [
                            '//*[@id="contentV1"]/div[4]/div[4]/div/ul/li[3]/div[2]',
                            '//*[@id="content"]/div/div[7]/div[4]/div/ul/li[3]/div[2]',
                            '//*[@id="content"]/div/div[6]/div[4]/div/ul/li[3]/div[2]',
                            '//*[@id="content"]/div/div[5]/div[4]/div/ul/li[3]/div[2]',
                            '//*[@id="transaction-information-table-right"]/div[9]/p[2]',
                        ]
                        outstanding_shares_component = WebDriverWait(
                            web_driver, 10
                        ).until(
                            lambda driver: self._find_first_valid_element_by_xpath(
                                web_driver=driver, xpaths=outstanding_shares_xpaths
                            )
                        )
                        if outstanding_shares_component:
                            try:
                                outstanding_shares = int(
                                    outstanding_shares_component.text.replace(",", "")
                                )
                            except ValueError:
                                self._logger.log_error(
                                    f"Outstanding shares not found for {stock_code}. Set outstanding shares to 0."
                                )
                                outstanding_shares = 0

                        # Market Cap
                        market_cap_xpaths = [
                            '//*[@id="contentV1"]/div[4]/div[4]/div/ul/li[4]/div[2]',
                            '//*[@id="content"]/div/div[7]/div[4]/div/ul/li[4]/div[2]',
                            '//*[@id="content"]/div/div[6]/div[4]/div/ul/li[4]/div[2]',
                            '//*[@id="content"]/div/div[5]/div[4]/div/ul/li[4]/div[2]',
                            '//*[@id="transaction-information-table-right"]/div[6]/p[2]',
                        ]
                        market_cap_component = WebDriverWait(web_driver, 10).until(
                            lambda driver: self._find_first_valid_element_by_xpath(
                                web_driver=driver, xpaths=market_cap_xpaths
                            )
                        )
                        if market_cap_component:
                            try:
                                market_cap = float(
                                    market_cap_component.text.replace(",", "")
                                )
                            except ValueError:
                                self._logger.log_error(
                                    f"Market cap not found for {stock_code}. Set market cap to 0."
                                )
                                market_cap = 0

                        # Write to CSV
                        with open(
                            file_path, mode="a", newline="", encoding="utf-8"
                        ) as file:
                            writer = csv.writer(file)
                            writer.writerow(
                                [
                                    stock_code,
                                    listed_shares,
                                    outstanding_shares,
                                    market_cap,
                                ]
                            )

                        break  # Success, exit retry loop

                    except Exception as e:
                        print(f"[Attempt {attempt+1}/3] Failed for {stock_code}: {e}")
                        web_driver.refresh()
                        search_box_xpath_list = [
                            '//*[@id="CafeF_SearchKeyword_Companyv2"]',
                            '//*[@id="search-header"]',
                        ]
                        search_box_xpath = self._find_first_valid_xpath(
                            web_driver=web_driver, xpaths=search_box_xpath_list
                        )
                        _ = WebDriverWait(web_driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, search_box_xpath))
                        )
                        bs4_parser = self._update_bs4_parser(web_driver)
                        if attempt == 2:
                            self._logger.log_error(
                                f"Failed to scrape stock code {stock_code} after 3 attempts."
                            )

                        continue

                count += 1
        except Exception as e:
            self._logger.log_error(f"Error scraping data - Callback {index}: " + str(e))

        finally:
            web_driver.close()

        self._logger.log_info(
            f'Finish scraping data for "{format_key_for_name(key)}" - Callback {index}.'
        )

    def _scrape_data_enterprise_stock_information_cafef(
        self, key: Tuple[ScrapeMainType, ScrapeSubType]
    ):
        folder_path = (
            f"{SCRAPER_BRONZE_DATA_DIR}/{key[0].value}/{key[1].value}/{key[2].value}"
        )
        all_files = get_all_file_names_with_extensions(
            self._logger, folder_path=folder_path, extensions=[FileExtension.CSV]
        )

        all_stock_codes = set()
        for file in all_files:
            with open(file, mode="r", newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header row

                for row in reader:
                    if row and len(row[0].strip()) == 3:  # Skip all Derivatives
                        all_stock_codes.add(row[0].strip())

        all_stock_code_chunks = divided_into_chunks(
            all_stock_codes, PARALLEL_SCRAPE_ENTERPRISE_STOCK_INFORMATION
        )

        for chunk, index in zip(
            all_stock_code_chunks, range(len(all_stock_code_chunks))
        ):
            self._thread_manager.add_task(
                Task(
                    f"{format_key_for_name(key)}_callback_{index + 1}",
                    self._scrape_data_enterprise_stock_information_cafef_callback,
                    index + 1,
                    len(all_stock_code_chunks),
                    chunk,
                )
            )

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
