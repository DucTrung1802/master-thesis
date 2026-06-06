# src\web_scraper\web_scraper.py

# ===== Standard Library =====
import csv
import json
import os
import threading
import time
from typing import Callable, List, Optional, Tuple, Literal
from datetime import datetime

# ===== Third-Party Libraries =====
from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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
from dtos.thread_manager_dtos.task import Task
from thread_manager.thread_manager import ThreadManager


class WebScraper:
    def __init__(
        self,
        logger: Logger,
        switch_handler: SwitchHandler,
        power: int = THREAD_MANAGER_POWER,
        retry_attempts: int = SCRAPER_RETRY_ATTEMPTS,
        retry_delay: float = SCRAPER_RETRY_DELAY,
    ):
        self._logger: Logger = logger
        self._switch_handler: SwitchHandler = switch_handler
        self._thread_manager = ThreadManager(logger=self._logger, power=power)
        self._retry_attempts: int = retry_attempts
        self._retry_delay: float = retry_delay

        self._chrome_options = Options()
        self._chrome_options.add_experimental_option(
            "prefs",
            {
                "profile.managed_default_content_settings.images": 2,  # Disable images
                "profile.managed_default_content_settings.stylesheets": 2,  # Disable CSS
                "profile.managed_default_content_settings.javascript": 1,  # Keep JS
            },
        )
        self._chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Selenium / BS4 helpers
    # ──────────────────────────────────────────────────────────────────────

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
        def find_visible(driver):
            candidates = driver.find_elements(By.XPATH, xpath)
            for el in candidates:
                if el.is_displayed():
                    return el
            return False

        element = WebDriverWait(web_driver, 10).until(find_visible)
        web_driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", element
        )
        time.sleep(0.3)
        try:
            element.click()
        except Exception:
            web_driver.execute_script("arguments[0].click();", element)

    def _helper_extract_table(self, table: Optional[Tag]) -> Tuple[List, List]:
        headers = []
        thead = table.find("thead")
        if thead:
            header_row = thead.find_all("tr")[-1]
            headers = [th.get_text(strip=True) for th in header_row.find_all("th")]

        rows = []
        tbody = table.find("tbody")
        for tr in tbody.find_all("tr"):
            if tr.get("class") and "group" in tr.get("class"):
                continue
            row = [td.get_text(strip=True) for td in tr.find_all("td")]
            rows.append(row)

        return (headers, rows)

    def _helper_extract_table_by_id(
        self, bs4_parser: BeautifulSoup, id: str
    ) -> Tuple[List, List]:
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
            for el in elements:
                web_driver.execute_script("arguments[0].remove();", el)
        except Exception:
            self._logger.log_warning(f"Failed to remove elements by xpath: {xpath}")

    def _helper_execute_scrape_actions(
        self,
        web_driver: ChromiumDriver,
        scrape_action_list: List[ScrapeAction],
    ) -> None:
        """Execute a sequence of ScrapeActions (click / input_text / go_to_link) in order."""
        for action in scrape_action_list:
            xpath = action.xpath_str
            label = (
                action.xpath.name
                if isinstance(action.xpath, TradingViewXpath)
                else xpath[:60]
            )

            match action.scrape_action_type:
                case ScrapeActionType.CLICK_BUTTON:
                    self._logger.log_info(f"Clicking element: {label}")
                    self._helper_click_element(web_driver, xpath)

                case ScrapeActionType.INPUT_TEXT:
                    self._logger.log_info(
                        f"Inputting text into {label}: '{action.value}'"
                    )
                    self._helper_input_text(
                        web_driver,
                        xpath,
                        action.value,
                        char_by_char=True,
                        confirm="none",
                    )

                case ScrapeActionType.GO_TO_LINK:
                    self._logger.log_info(f"Navigating to link: {action.value}")
                    web_driver.get(action.value)
                    time.sleep(SCRAPER_BASE_WAIT_TIME)

                case ScrapeActionType.EXECUTE_SCRIPT:
                    self._logger.log_info(
                        "Executing script to inject localStorage state."
                    )
                    web_driver.execute_script(action.value)

            time.sleep(1)

    def _helper_extract_trading_view_links(
        self, web_driver: ChromiumDriver
    ) -> List[str]:
        """
        Scroll the TradingView symbol list and collect every data-symbol-name attribute.
        Stops after 3 consecutive scroll attempts that produce no movement.
        """
        collected_symbols = set()
        last_scroll_top = -1
        no_move_count = 0
        max_no_move = 3

        while no_move_count < max_no_move:
            result = web_driver.execute_script("""
                const container = document.querySelector('.scrollContainer-FSX6AatX');
                if (!container) return { symbols: [], scrollTop: -1 };

                const symbols = Array.from(
                    document.querySelectorAll('div[data-role="list-item"][data-symbol-name]')
                ).map(el => el.getAttribute('data-symbol-name'));

                const currentTop = container.scrollTop;
                container.scrollTop += 3000;

                return { symbols: symbols, scrollTop: currentTop };
            """)

            for symbol in result["symbols"]:
                if symbol:
                    collected_symbols.add(symbol.strip())

            self._logger.log_info(f"Collected symbols: {len(collected_symbols)}")

            current_scroll_top = result["scrollTop"]
            if current_scroll_top == last_scroll_top:
                no_move_count += 1
            else:
                no_move_count = 0
                last_scroll_top = current_scroll_top

            time.sleep(0.3)

        links = [
            f"https://www.tradingview.com/chart/?symbol={symbol}"
            for symbol in sorted(collected_symbols)
        ]
        self._logger.log_info(f"Finished collecting {len(links)} chart links.")
        return links

    # ──────────────────────────────────────────────────────────────────────
    # Generic link-scraping engine (shared by every asset class)
    # ──────────────────────────────────────────────────────────────────────

    def _scrape_links_attempt(
        self,
        key: tuple,
        scrape_actions: List[ScrapeAction],
        csv_rows_builder: Callable[[str], List],
        file_path: str,
        folder_path: str,
    ) -> None:
        """
        Single attempt at scraping TradingView chart links for any asset class.
        Opens a dedicated Chrome instance, applies filters, scrolls the symbol
        list, and writes results to CSV.  Raises on any failure so the retry
        wrapper can catch it.

        Parameters
        ----------
        key : tuple
            Identifier tuple used only for logging.
        scrape_actions : List[ScrapeAction]
            Filter actions to apply before extracting symbols.
        csv_rows_builder : Callable[[str], List]
            Given a chart URL, returns the full CSV row to write.
        file_path : str
            Destination CSV file path.
        folder_path : str
            Parent directory; created if absent.
        """
        web_driver, _ = self._helper_initialize_web_driver_and_bs4_parser()
        try:
            if os.path.exists(file_path):
                self._logger.log_info(
                    f"File exists: {file_path}, deleting to re-fetch."
                )
                os.remove(file_path)

            os.makedirs(folder_path, exist_ok=True)

            web_driver, _ = self._helper_navigate_to_url(
                web_driver, TRADING_VIEW_HOME_PAGE_URL
            )

            self._helper_execute_scrape_actions(web_driver, scrape_actions)
            time.sleep(5)  # allow filtered list to fully render

            self._logger.log_info(
                f"Current URL after filters: {web_driver.current_url}"
            )

            links = self._helper_extract_trading_view_links(web_driver)
            self._logger.log_info(f"Total links extracted: {len(links)}")

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(TRADING_VIEW_TABLE_SCHEMA)
                for url in links:
                    writer.writerow(csv_rows_builder(url))

            self._logger.log_info(f"Saved {len(links)} links to {file_path}")

        finally:
            web_driver.quit()

    def _scrape_links_with_retry(
        self,
        key: tuple,
        scrape_actions: List[ScrapeAction],
        csv_rows_builder: Callable[[str], List],
    ) -> None:
        """
        Retry wrapper around _scrape_links_attempt.
        Folder / file paths are derived automatically from *key*.
        Raises the last exception if all attempts are exhausted.
        """
        folder_path = f"{SCRAPER_RAW_DATA_DIR}/{format_key_for_path(key)}"
        file_path = f"{folder_path}/trading_view_links_{datetime.now().strftime('%Y-%m-%d')}.csv"
        key_name = format_key_for_name(key)

        self._logger.log_info(f'Start scraping links for "{key_name}".')

        last_error: Exception = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                if attempt > 1:
                    self._logger.log_info(
                        f'Retry {attempt}/{self._retry_attempts} for "{key_name}" '
                        f"(waiting {self._retry_delay}s)."
                    )
                    time.sleep(self._retry_delay)

                self._scrape_links_attempt(
                    key, scrape_actions, csv_rows_builder, file_path, folder_path
                )

                self._logger.log_info(
                    f'Finish scraping links for "{key_name}" '
                    f"(attempt {attempt}/{self._retry_attempts})."
                )
                return  # success — exit immediately

            except Exception as e:
                last_error = e
                self._logger.log_error(
                    f'Attempt {attempt}/{self._retry_attempts} failed for "{key_name}": {e}'
                )

        self._logger.log_error(
            f'All {self._retry_attempts} attempts failed for "{key_name}". '
            f"Last error: {last_error}"
        )
        raise last_error

    # ──────────────────────────────────────────────────────────────────────
    # Per-asset-class scrape methods
    # Each method is the callable passed directly to Task as func.
    # Arguments are passed as Task *args so task.run() calls func(*args).
    # ──────────────────────────────────────────────────────────────────────

    def _scrape_stock_links(
        self,
        country: Country,
        stock_type: StockType,
        sector: StockSector,
    ) -> None:
        key = (
            "links",
            ScrapeMainType.STOCKS.value,
            country.value,
            stock_type.value,
            sector.value,
        )
        self._scrape_links_with_retry(
            key,
            build_stock_link_scrape_actions(country, stock_type, sector),
            lambda url: [
                ScrapeMainType.STOCKS.value,
                "country",
                country.value,
                "stock_type",
                stock_type.value,
                "sector",
                sector.value,
                url,
            ],
        )

    def _scrape_stock_no_sector_links(
        self,
        country: Country,
        stock_type: StockType,
    ) -> None:
        """Scrape stock links for types that have no sector sub-dimension
        (preferred_stock, depository_receipt, warrant, pre_ipo)."""
        key = ("links", ScrapeMainType.STOCKS.value, country.value, stock_type.value)
        self._scrape_links_with_retry(
            key,
            build_stock_no_sector_link_scrape_actions(country, stock_type),
            lambda url: [
                ScrapeMainType.STOCKS.value,
                "country",
                country.value,
                "stock_type",
                stock_type.value,
                "",
                "",
                url,
            ],
        )

    def _scrape_fund_links(
        self,
        country: Country,
        fund_type: FundType,
    ) -> None:
        key = ("links", ScrapeMainType.FUNDS.value, country.value, fund_type.value)
        self._scrape_links_with_retry(
            key,
            build_fund_link_scrape_actions(country, fund_type),
            lambda url: [
                ScrapeMainType.FUNDS.value,
                "country",
                country.value,
                "fund_type",
                fund_type.value,
                "",
                "",
                url,
            ],
        )

    def _scrape_futures_links(
        self,
        country: Country,
        category: FutureCategory,
    ) -> None:
        key = ("links", ScrapeMainType.FUTURES.value, country.value, category.value)
        self._scrape_links_with_retry(
            key,
            build_futures_link_scrape_actions(country, category),
            lambda url: [
                ScrapeMainType.FUTURES.value,
                "country",
                country.value,
                "category",
                category.value,
                "",
                "",
                "",
                "",
                url,
            ],
        )

    def _scrape_forex_links(
        self,
        source: ForexSource,
    ) -> None:
        key = ("links", ScrapeMainType.FOREX.value, source.value)
        self._scrape_links_with_retry(
            key,
            build_forex_link_scrape_actions(source),
            lambda url: [
                ScrapeMainType.FOREX.value,
                "source",
                source.value,
                "",
                "",
                "",
                "",
                url,
            ],
        )

    def _scrape_crypto_links(
        self,
        source: CryptoSource,
        crypto_type: CryptoType,
        exchange_type: CryptoExchangeType,
    ) -> None:
        key = (
            "links",
            ScrapeMainType.CRYPTO.value,
            source.value,
            crypto_type.value,
            exchange_type.value,
        )
        self._scrape_links_with_retry(
            key,
            build_crypto_link_scrape_actions(source, crypto_type, exchange_type),
            lambda url: [
                ScrapeMainType.CRYPTO.value,
                "source",
                source.value,
                "crypto_type",
                crypto_type.value,
                "exchange_type",
                exchange_type.value,
                url,
            ],
        )

    def _scrape_indices_links(
        self,
        country: Country,
    ) -> None:
        key = ("links", ScrapeMainType.INDICES.value, country.value)
        self._scrape_links_with_retry(
            key,
            build_indices_link_scrape_actions(country),
            lambda url: [
                ScrapeMainType.INDICES.value,
                "country",
                country.value,
                "",
                "",
                "",
                "",
                url,
            ],
        )

    def _scrape_bonds_links(
        self,
        country: Country,
        bonds_type: BondsType,
    ) -> None:
        key = ("links", ScrapeMainType.BONDS.value, country.value, bonds_type.value)
        self._scrape_links_with_retry(
            key,
            build_bonds_link_scrape_actions(country, bonds_type),
            lambda url: [
                ScrapeMainType.BONDS.value,
                "country",
                country.value,
                "bonds_type",
                bonds_type.value,
                "",
                "",
                url,
            ],
        )

    def _scrape_economy_links(
        self,
        country: Country,
        category: EconomyCategory,
    ) -> None:
        key = ("links", ScrapeMainType.ECONOMY.value, country.value, category.value)
        self._scrape_links_with_retry(
            key,
            build_economy_link_scrape_actions(country, category),
            lambda url: [
                ScrapeMainType.ECONOMY.value,
                "country",
                country.value,
                "category",
                category.value,
                "",
                "",
                url,
            ],
        )

    def _scrape_options_links(
        self,
        country: Country,
    ) -> None:
        key = ("links", ScrapeMainType.OPTIONS.value, country.value)
        self._scrape_links_with_retry(
            key,
            build_options_link_scrape_actions(country),
            lambda url: [
                ScrapeMainType.OPTIONS.value,
                "country",
                country.value,
                "",
                "",
                "",
                "",
                url,
            ],
        )

    # ──────────────────────────────────────────────────────────────────────
    # Task-registration helpers (one per asset class)
    #
    # Each Task is created as:
    #   Task(name, func, *args)
    # so task.run() calls func(*args) directly — no lambda wrapper needed.
    # ──────────────────────────────────────────────────────────────────────

    def _add_stock_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.STOCKS.value
        ):
            parts = path.split("/")
            # parts layout:
            #   [0]web_scraper [1]trading_view [2]links [3]stocks
            #   [4]country     [5]stock_type
            #   [6]sector      ← only present for common_stock
            country = Country(parts[4])
            stock_type = StockType(parts[5])

            if len(parts) == 7:
                # common_stock: has a sector sub-dimension
                sector = StockSector(parts[6])
                task_name = format_key_for_name(
                    (
                        "links",
                        ScrapeMainType.STOCKS.value,
                        country.value,
                        stock_type.value,
                        sector.value,
                    )
                )
                self._thread_manager.add_task(
                    Task(
                        task_name,
                        self._scrape_stock_links,
                        country,
                        stock_type,
                        sector,
                    )
                )
            else:
                # preferred_stock / depository_receipt / warrant / pre_ipo:
                # no sector filter on TradingView
                task_name = format_key_for_name(
                    (
                        "links",
                        ScrapeMainType.STOCKS.value,
                        country.value,
                        stock_type.value,
                    )
                )
                self._thread_manager.add_task(
                    Task(
                        task_name,
                        self._scrape_stock_no_sector_links,
                        country,
                        stock_type,
                    )
                )
            added += 1
        return added

    def _add_fund_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.FUNDS.value
        ):
            parts = path.split("/")
            country = Country(parts[4])
            fund_type = FundType(parts[5])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.FUNDS.value,
                    country.value,
                    fund_type.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_fund_links,
                    country,
                    fund_type,
                )
            )
            added += 1
        return added

    def _add_futures_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.FUTURES.value
        ):
            parts = path.split("/")
            country = Country(parts[4])
            category = FutureCategory(parts[5])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.FUTURES.value,
                    country.value,
                    category.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_futures_links,
                    country,
                    category,
                )
            )
            added += 1
        return added

    def _add_forex_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.FOREX.value
        ):
            parts = path.split("/")
            source = ForexSource(parts[4])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.FOREX.value,
                    source.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_forex_links,
                    source,
                )
            )
            added += 1
        return added

    def _add_crypto_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.CRYPTO.value
        ):
            parts = path.split("/")
            source = CryptoSource(parts[4])
            crypto_type = CryptoType(parts[5])
            exchange_type = CryptoExchangeType(parts[6])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.CRYPTO.value,
                    source.value,
                    crypto_type.value,
                    exchange_type.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_crypto_links,
                    source,
                    crypto_type,
                    exchange_type,
                )
            )
            added += 1
        return added

    def _add_indices_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.INDICES.value
        ):
            parts = path.split("/")
            country = Country(parts[4])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.INDICES.value,
                    country.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_indices_links,
                    country,
                )
            )
            added += 1
        return added

    def _add_bonds_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.BONDS.value
        ):
            parts = path.split("/")
            country = Country(parts[4])
            bonds_type = BondsType(parts[5])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.BONDS.value,
                    country.value,
                    bonds_type.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_bonds_links,
                    country,
                    bonds_type,
                )
            )
            added += 1
        return added

    def _add_economy_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.ECONOMY.value
        ):
            parts = path.split("/")
            country = Country(parts[4])
            category = EconomyCategory(parts[5])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.ECONOMY.value,
                    country.value,
                    category.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_economy_links,
                    country,
                    category,
                )
            )
            added += 1
        return added

    def _add_options_links_tasks(self) -> int:
        added = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "links", ScrapeMainType.OPTIONS.value
        ):
            parts = path.split("/")
            country = Country(parts[4])
            task_name = format_key_for_name(
                (
                    "links",
                    ScrapeMainType.OPTIONS.value,
                    country.value,
                )
            )
            self._thread_manager.add_task(
                Task(
                    task_name,
                    self._scrape_options_links,
                    country,
                )
            )
            added += 1
        return added

    # ──────────────────────────────────────────────────────────────────────
    # Public dispatcher
    # ──────────────────────────────────────────────────────────────────────

    def add_trading_view_links_scraping_tasks(self) -> None:
        """
        Register TradingView link-scraping tasks for every enabled asset class.

        Switch config hierarchy:
          web_scraper → trading_view → links → {asset_class} → {dim1} → {dim2?} → {dim3?}

        Asset-class filter dimensions:
          stocks  : country / stock_type / sector
          funds   : country / fund_type
          futures : category
          forex   : source
          crypto  : source / crypto_type / exchange_type
          indices : country
          bonds   : country / bonds_type
          economy : country / category
          options : country
        """
        self._logger.log_info("Adding Trading View links scraping tasks.")
        before = self._thread_manager.get_current_number_of_task()

        asset_adders = [
            (ScrapeMainType.STOCKS, self._add_stock_links_tasks),
            (ScrapeMainType.FUNDS, self._add_fund_links_tasks),
            (ScrapeMainType.FUTURES, self._add_futures_links_tasks),
            (ScrapeMainType.FOREX, self._add_forex_links_tasks),
            (ScrapeMainType.CRYPTO, self._add_crypto_links_tasks),
            (ScrapeMainType.INDICES, self._add_indices_links_tasks),
            (ScrapeMainType.BONDS, self._add_bonds_links_tasks),
            (ScrapeMainType.ECONOMY, self._add_economy_links_tasks),
            (ScrapeMainType.OPTIONS, self._add_options_links_tasks),
        ]

        for asset_type, adder in asset_adders:
            if self._switch_handler.is_enabled(
                "web_scraper", "trading_view", "links", asset_type.value
            ):
                count = adder()
                self._logger.log_info(
                    f"Added {count} Trading View {asset_type.value} links scraping tasks."
                )
            else:
                self._logger.log_info(
                    f"Trading View {asset_type.value} links scraping is disabled – skipping."
                )

        after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added total {after - before} Trading View links scraping tasks."
        )

    # ──────────────────────────────────────────────────────────────────────
    # Macroeconomics data scraping (non-TradingView-links)
    # ──────────────────────────────────────────────────────────────────────

    def _scrape_data_macroeconomics_exchange_rate_usd_vnd(self, key: Tuple) -> None:
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
            file_path = (
                f"{folder_path}/{file_name}"
                f"_{start_time.strftime('%Y-%m-%d')}"
                f"_{current_time.strftime('%Y-%m-%d')}.csv"
            )

            if os.path.exists(file_path):
                self._logger.log_info(
                    f"File exists: {file_path}, deleting to re-fetch."
                )
                os.remove(file_path)

            os.makedirs(folder_path, exist_ok=True)
            self._logger.log_info(
                f"Scraping {scrape_sub_type} data from "
                f"{start_time.strftime('%Y-%m-%d')} to {current_time.strftime('%Y-%m-%d')}."
            )

            # TODO: define SCRAPE_MAPPING in utils/constants.py or utils/enums.py
            #       key is a tuple of enum members, e.g. (ScrapeMainType.X, SubType.Y)
            source_info = SCRAPE_MAPPING[key]  # noqa: F821
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

    def _scrape_data_macroeconomics_vietnam_interbank_rate(self, key: Tuple) -> None:
        self._logger.log_info(f'Start scraping data for "{format_key_for_name(key)}".')
        web_driver, bs4_parser = self._helper_initialize_web_driver_and_bs4_parser()

        stop_event = threading.Event()
        dialog_thread = threading.Thread(
            target=self._dialog_remover_loop,
            args=(web_driver, stop_event),
            daemon=True,
        )

        try:
            scrape_main_type = key[0].value
            scrape_sub_type = key[1].value
            folder_path = f"{SCRAPER_RAW_DATA_DIR}/{scrape_main_type}/{scrape_sub_type}"
            file_name = scrape_sub_type

            start_time = SCRAPER_START_DATE
            current_time = datetime.now()
            start_time_date_str = SCRAPER_START_DATE.strftime("%Y-%m-%d")
            current_time_date_str = current_time.strftime("%Y-%m-%d")
            file_path = (
                f"{folder_path}/{file_name}"
                f"_{start_time_date_str}_{current_time_date_str}.csv"
            )

            os.makedirs(folder_path, exist_ok=True)
            self._logger.log_info(
                f"Scraping {scrape_sub_type} data from "
                f"{start_time.strftime('%Y-%m-%d')} to {current_time.strftime('%Y-%m-%d')}."
            )

            # TODO: define SCRAPE_MAPPING in utils/constants.py or utils/enums.py
            source_info = SCRAPE_MAPPING[key]  # noqa: F821
            web_driver, bs4_parser = self._helper_navigate_to_url(
                web_driver, source_info.url
            )

            # XPaths
            select_date_range_button_xpath = (
                "/html/body/div[2]/div/div[5]/div[2]/div/div[2]/div/button"
            )
            custom_range_button_xpath = '//*[@id="CustomRange"]'
            input_start_date_xpath = (
                '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]'
                "/div/div/div[1]/div[1]/div/div/div/span/span[1]/input"
            )
            input_end_date_xpath = (
                '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]'
                "/div/div/div[2]/div[1]/div/div/div/span/span[1]/input"
            )
            apply_range_button_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[4]/div/span/button'

            # Step 1 – set custom date range
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

            # Step 2 – extract all chart data in a single JS call
            bulk_js = """
            try {
                const collection = window._exposed_chartWidgetCollection;
                const chartWidget = collection.activeChartWidget._value;
                const model = chartWidget._modelWV._value.m_model;
                const mainSeries = model._mainSeries;
                const seriesSource = mainSeries._seriesSource;
                const items = seriesSource._data.m_bars._items;
                const lastBarCloseTime = mainSeries._lastBarCloseTime;

                const parts = arguments[0].split('-');
                const startTs = Date.UTC(
                    parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])
                ) / 1000;

                const keys = Object.keys(items).map(Number).sort((a, b) => a - b);

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
                    if (ts >= lastBarCloseTime || ts < startTs) continue;
                    if (isOHLC) {
                        records.push([
                            new Date(ts * 1000).toISOString().split('T')[0],
                            item.value[1], item.value[2],
                            item.value[3], item.value[4], item.value[5],
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

            # Step 3 – write to CSV
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

    @staticmethod
    def _dialog_remover_loop(
        web_driver: ChromiumDriver, stop_event: threading.Event
    ) -> None:
        """Background thread: continuously removes TradingView pop-up dialogs."""
        dialog_xpath = '//*[@id="overlap-manager-root"]/div[3]/div'
        while not stop_event.is_set():
            try:
                elements = web_driver.find_elements(By.XPATH, dialog_xpath)
                for el in elements:
                    web_driver.execute_script("arguments[0].remove();", el)
            except Exception:
                pass
            time.sleep(0.5)

    # ──────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────

    def start_scraping(self) -> None:
        self._logger.log_info("Start scraping data using ThreadManager.")

        self._thread_manager.remove_all_tasks()

        self._logger.log_info("Adding data scraping tasks.")
        before = self._thread_manager.get_current_number_of_task()

        if self._switch_handler.is_enabled("web_scraper", "trading_view", "links"):
            self.add_trading_view_links_scraping_tasks()

        after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(f"Added total {after - before} data scraping tasks.")

        self._logger.log_info(
            f"Start executing {self._thread_manager.get_current_number_of_task()} tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("Finished scraping data.")
