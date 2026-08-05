# src\web_scraper\trading_view_scraper.py

# ===== Standard Library =====
import csv
import glob
import json
import os
import random
import threading
import time
from contextlib import contextmanager
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
from utils.exceptions import PipelineError
from utils.utils import *
from utils.switch_handler import SwitchHandler
from dtos.thread_manager_dtos.task import Task
from thread_manager.thread_manager import ThreadManager
from web_scraper.base_scraper import BaseScraper, register_scraper


@register_scraper
class TradingViewScraper(BaseScraper):
    SOURCE_NAME = "trading_view"

    def __init__(
        self,
        logger: Logger,
        switch_handler: SwitchHandler,
        power: int = THREAD_MANAGER_POWER,
        retry_attempts: int = SCRAPER_RETRY_ATTEMPTS,
        retry_delay: float = SCRAPER_RETRY_DELAY,
        max_browsers: int = SCRAPER_MAX_CONCURRENT_BROWSERS,
    ):
        max_browsers = max(1, int(max_browsers))

        # ⚠️ THE THREAD POOL IS SIZED TO THE BROWSER CAP, not to SCRAPER_MAX_WORKERS.
        # Every task in this scraper opens a browser, so a pool WIDER than the cap only
        # buys threads that block on the semaphore, and a pool NARROWER than it makes the
        # cap unreachable — which is what happened when the pool was 2 and the cap 8: the
        # effective concurrency was 2, and the documented 8 was fiction. One knob now.
        super().__init__(
            logger=logger,
            switch_handler=switch_handler,
            power=power,
            retry_attempts=retry_attempts,
            retry_delay=retry_delay,
            max_workers=max_browsers,
        )
        self._max_browsers = max_browsers
        self._browser_semaphore = threading.Semaphore(max_browsers)
        self._browsers_live = 0
        self._browsers_peak = 0
        self._browser_count_lock = threading.Lock()
        self._logger.log_info(
            f"TradingViewScraper: at most {max_browsers} concurrent Chrome instance(s) "
            f"(SCRAPER_MAX_CONCURRENT_BROWSERS); thread pool sized to match."
        )
        self._nav_last_ts: float = 0.0
        self._nav_time_lock = threading.Lock()

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

        # ⚠️ WITHOUT THESE, A COVERED WINDOW SILENTLY STOPS PAGINATING. Chrome throttles
        # timers and rendering in windows it believes nobody can see — and TradingView's
        # symbol list appends its next 50 rows from a visibility-driven callback. During
        # a run measured in hours the windows WILL end up behind something, so occlusion
        # is the normal case, not the edge case. See `_helper_extract_trading_view_links`
        # for what that cost: every leaf over 50 symbols truncated to exactly 50.
        for flag in (
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-background-timer-throttling",
        ):
            self._chrome_options.add_argument(flag)

    # ──────────────────────────────────────────────────────────────────────
    # The browser budget
    # ──────────────────────────────────────────────────────────────────────

    @contextmanager
    def _browser_slot(self):
        """Hold one of the `max_browsers` permits for a driver's WHOLE life.

        Every `webdriver.Chrome()` in this class is created inside this block and quit
        before leaving it — a permit taken after the driver exists would cap nothing.

        The live/peak counters are not decoration: they make "strictly N" an assertion
        the log can be read back for, rather than a claim about a semaphore. The
        `PipelineError` cannot fire while the permit accounting is correct, which is the
        point — it fires only if someone opens a browser outside this block.
        """
        self._browser_semaphore.acquire()
        try:
            with self._browser_count_lock:
                self._browsers_live += 1
                live = self._browsers_live
                self._browsers_peak = max(self._browsers_peak, live)
                peak = self._browsers_peak
            if live > self._max_browsers:
                raise PipelineError(
                    f"{live} Chrome instances alive but the cap is "
                    f"{self._max_browsers} — a driver was created outside "
                    f"`_browser_slot`."
                )
            self._logger.log_info(
                f"Chrome slot taken: {live}/{self._max_browsers} live (peak {peak})."
            )
            yield
        finally:
            with self._browser_count_lock:
                self._browsers_live -= 1
            self._browser_semaphore.release()

    # ──────────────────────────────────────────────────────────────────────
    # Selenium / BS4 helpers
    # ──────────────────────────────────────────────────────────────────────

    def _helper_initialize_web_driver_and_bs4_parser(
        self,
    ) -> Tuple[ChromiumDriver, BeautifulSoup]:
        web_driver: ChromiumDriver = webdriver.Chrome(options=self._chrome_options)
        bs4_parser: BeautifulSoup = BeautifulSoup(web_driver.page_source, "html.parser")

        # ⚠️ THIS USED TO BE `minimize_window()`, AND THAT ONE CALL COST 998 US SERIES.
        # A minimized window has no layout, so TradingView renders the first 50 symbols
        # and its lazy loader never fires again however far the container is scrolled.
        # Leaves under 50 symbols were unaffected, which is why it hid for months: 17 of
        # 19 economy countries matched TradingView's own counts EXACTLY, and only the USA
        # — the one country with categories over 50 — came back at exactly 50 per
        # category. Proven by probe: same leaf, minimized 50 rows, sized window 276.
        #
        # The size is deliberate too. The list appends a page when the scroll approaches
        # the bottom, so the viewport has to be tall enough to have a bottom worth
        # approaching.
        web_driver.set_window_size(1400, 1000)
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

    def _helper_click_element(
        self, web_driver: ChromiumDriver, xpath: str, timeout: int = 10
    ) -> None:
        def find_visible(driver):
            candidates = driver.find_elements(By.XPATH, xpath)
            for el in candidates:
                if el.is_displayed():
                    return el
            return False

        element = WebDriverWait(web_driver, timeout).until(find_visible)
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

    def _helper_toggle_adj_dividends(
        self, web_driver: ChromiumDriver, symbol: str
    ) -> None:
        """Enable TradingView's "Adjust data for dividends" (ADJ) toggle so scraped
        prices are back-adjusted for cash dividends (matching CafeF's "Giá điều
        chỉnh" and vnstock's fully-adjusted series). The chart defaults to ADJ OFF.

        Best-effort and non-fatal: assets without a dividend-adjust control simply
        have no such button (no-op). MUST run BEFORE the custom date range is applied,
        because clicking ADJ resets the chart's visible range.
        """
        toggle_js = r"""
        function at(e){return ((e.getAttribute('aria-label')||'')+' '+(e.getAttribute('title')||''));}
        function tx(e){return (e.textContent||'').trim();}
        let el = Array.from(document.querySelectorAll('button,div[role="button"]'))
            .find(e => /adjust data for dividends/i.test(at(e)));
        if (!el) { el = Array.from(document.querySelectorAll('button,div'))
            .find(e => /^adj$/i.test(tx(e)) && e.children.length === 0); }
        if (!el) return JSON.stringify({found:false});
        const on = el.getAttribute('aria-pressed') === 'true';
        if (!on) { el.scrollIntoView({block:'center'}); el.click(); }
        return JSON.stringify({found:true, clicked:!on,
            label:(el.getAttribute('aria-label') || tx(el))});
        """
        try:
            result = json.loads(web_driver.execute_script(toggle_js))
        except Exception as e:
            self._logger.log_warning(f'ADJ-toggle script failed for "{symbol}": {e}')
            return

        if not result.get("found"):
            self._logger.log_info(
                f'No dividend-adjust (ADJ) control for "{symbol}"; scraping unadjusted.'
            )
            return

        if result.get("clicked"):
            self._logger.log_info(f'Enabled ADJ (dividend adjustment) for "{symbol}".')
            time.sleep(SCRAPER_BASE_WAIT_TIME)
        else:
            self._logger.log_info(f'ADJ already enabled for "{symbol}".')

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

    # The symbol list is a VIRTUALISED list: only the visible rows exist in the DOM, so
    # collecting everything means scrolling its container. Two selectors are involved and
    # they are not equally trustworthy:
    #
    #   * `div[data-role="list-item"][data-symbol-name]` — SEMANTIC markup. Stable.
    #   * `.scrollContainer-<hash>` — a CSS-module class whose suffix is a BUILD HASH.
    #     It rotates whenever TradingView redeploys.
    #
    # ⚠️ On 2026-07-31 `scrollContainer-FSX6AatX` became `scrollContainer-Q9nrHY0X` and
    # took every links task with it: 140 of 140 tasks collected 0 symbols, wrote
    # header-only CSVs and reported SUCCESS. The items were on the page the whole time
    # (verified: 50 rendered for stocks/vietnam/finance, `HOSE:VCB` first) — the old
    # `if (!container) return { symbols: [] }` threw them away. So: match the hash-free
    # PREFIX, fall back to the first scrollable ancestor of a real list item, and never
    # discard symbols because the container lookup failed.
    _SYMBOL_ITEM_SELECTOR = 'div[data-role="list-item"][data-symbol-name]'

    _EXTRACT_SYMBOLS_JS = """
        const items = Array.from(document.querySelectorAll(arguments[0]));
        const symbols = items.map(el => el.getAttribute('data-symbol-name'));

        let container = document.querySelector('[class*="scrollContainer"]');
        if (!container && items.length) {
            let el = items[0].parentElement;
            while (el && el.scrollHeight <= el.clientHeight + 5) el = el.parentElement;
            container = el;
        }

        // Symbols are returned even with no container — a failed container lookup must
        // cost us the SCROLLING, never the rows already rendered.
        if (!container) return { symbols: symbols, scrollTop: -1, container: false };

        // ⚠️ JUMP TO THE BOTTOM, don't nudge. `scrollTop += 3000` was a fixed step, and
        // the list grows as it pages in — 50 rows is ~2,000px but 276 rows is ~11,000px,
        // so a 3,000px step stops landing at the bottom once the list is a few pages
        // deep. The next page is appended when the scroll REACHES the bottom, so a step
        // that falls short simply stops the pagination. `scrollHeight` is always the
        // bottom, whatever the list has grown to.
        const currentTop = container.scrollTop;
        container.scrollTop = container.scrollHeight;
        return { symbols: symbols, scrollTop: currentTop, container: true };
    """

    # How long the symbol list may sit still before the scroll loop calls it finished.
    # `_SCROLL_POLL_S * _SCROLL_MAX_NO_PROGRESS` must exceed the time TradingView takes
    # to deliver the NEXT PAGE of 50 rows; at 0.3 x 3 = 0.9 s it did not, and every leaf
    # with more than 50 symbols was silently truncated to exactly 50 (see
    # `_helper_extract_trading_view_links`). 0.8 x 6 = 4.8 s.
    _SCROLL_POLL_S = 0.8
    _SCROLL_MAX_NO_PROGRESS = 6

    def _helper_extract_trading_view_links(
        self, web_driver: ChromiumDriver
    ) -> List[str]:
        """
        Scroll the TradingView symbol list and collect every data-symbol-name attribute.
        Waits for items to actually render before starting, then gives up once
        `_SCROLL_POLL_S * _SCROLL_MAX_NO_PROGRESS` seconds pass with neither a scroll nor
        a new symbol.

        ⚠️ **THE IDLE BUDGET IS WHY THIS USED TO STOP AT EXACTLY 50 ROWS** (found
        2026-08-05). The list is paged: TradingView renders 50, and fetches the next 50
        only once the scroll approaches the bottom. The old budget was 3 iterations x
        0.3 s = **0.9 s**, which is shorter than that round-trip — so the loop declared
        "no progress" while the next page was still in flight and returned the first page
        alone.

        It was invisible because it only bites a leaf with MORE than 50 symbols, and
        almost every leaf has fewer. Measured against TradingView's own symbol-search API
        on the data this repo had already scraped:

        | country | on disk | actually exists |
        |---|---|---|
        | vietnam / japan / china / south korea | 88 / 181 / 164 / 140 | **identical** |
        | usa | **464** | **1,462** |

        and within the USA the signature is unmistakable — `health` 4/4 and `taxes` 10/10
        complete, while `labor` took 50 of 276, `money` 50 of 320, `business` 50 of 225.
        Every category over 50 landed exactly 50.

        Waiting LONGER is the fix, and the cost is bounded: it is paid once per leaf, at
        the end, and only by leaves that really have stopped producing. 209 economy link
        leaves x ~4 extra seconds is ~14 minutes against a run measured in hours — set
        against silently keeping 32% of the US series.

        Raises `PipelineError` when the page rendered symbol rows but extraction got
        nothing out of them, or when the list could not be scrolled — both mean the
        selectors have drifted, and a drifted selector must not read as "this filter
        matched no symbols". A genuinely empty result (no rows rendered at all) is
        legitimate — `futures/vietnam/*` and `economy/*/health` have always been empty —
        and still returns [].
        """
        # Wait until the list has actually populated. Some asset classes (indices,
        # bonds, economy) load noticeably slower than stocks, and the upstream
        # time.sleep(5) is not always enough.
        items_rendered = True
        try:
            WebDriverWait(web_driver, 20).until(
                lambda d: d.execute_script(
                    "return document.querySelectorAll(arguments[0]).length > 0;",
                    self._SYMBOL_ITEM_SELECTOR,
                )
            )
        except TimeoutException:
            # NOT an error by itself: some filter combinations legitimately match
            # nothing. It is the only thing that distinguishes "empty" from "broken"
            # below, which is why it is recorded rather than just logged.
            items_rendered = False
            self._logger.log_warning(
                "No symbol list items rendered within 20s — treating as an empty "
                "result. If this filter is supposed to have symbols, the page or the "
                f"selector ({self._SYMBOL_ITEM_SELECTOR}) has changed."
            )

        collected_symbols = set()
        last_scroll_top = -1
        no_progress_count = 0
        max_no_progress = self._SCROLL_MAX_NO_PROGRESS
        container_found = False

        while no_progress_count < max_no_progress:
            result = web_driver.execute_script(
                self._EXTRACT_SYMBOLS_JS, self._SYMBOL_ITEM_SELECTOR
            )

            before = len(collected_symbols)
            for symbol in result["symbols"]:
                if symbol:
                    collected_symbols.add(symbol.strip())
            found_new = len(collected_symbols) > before
            container_found = container_found or result["container"]

            self._logger.log_info(f"Collected symbols: {len(collected_symbols)}")

            # Progress is EITHER the container moving OR a new symbol appearing. Scroll
            # position alone was the old test, so a stuck/absent container ended the
            # loop after three no-op iterations — the shape every one of the 140 broken
            # tasks had in the log.
            current_scroll_top = result["scrollTop"]
            if current_scroll_top == last_scroll_top and not found_new:
                no_progress_count += 1
            else:
                no_progress_count = 0
                last_scroll_top = current_scroll_top

            time.sleep(self._SCROLL_POLL_S)

        # ── Fail loudly rather than return a plausible-looking empty list ────────────
        if items_rendered and not collected_symbols:
            raise PipelineError(
                "TradingView rendered symbol rows but none could be read. The "
                f"item selector ({self._SYMBOL_ITEM_SELECTOR}) no longer matches the "
                "page. Refusing to report an empty result as success."
            )

        if collected_symbols and not container_found:
            raise PipelineError(
                f"Collected {len(collected_symbols)} symbol(s) but found no scrollable "
                "container, so the virtualised list could not be paged — this result is "
                "the first screen only, not the full set. Fix the container lookup in "
                "`_EXTRACT_SYMBOLS_JS` before trusting it."
            )

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
        Opens a dedicated Chrome instance — under `_browser_semaphore`, so at most
        SCRAPER_MAX_CONCURRENT_BROWSERS are alive at once — applies filters, scrolls
        the symbol list, and writes results to CSV.  Raises on any failure so the
        retry wrapper can catch it.

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
        # Hard cap on concurrent Chrome instances, the same permit the data phase takes
        # (`SCRAPER_MAX_CONCURRENT_BROWSERS`, which also sizes the thread pool). Acquired
        # BEFORE the driver is created and held until it quits — a permit taken after
        # `webdriver.Chrome()` would cap nothing.
        with self._browser_slot():
            web_driver, _ = self._helper_initialize_web_driver_and_bs4_parser()
            try:
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

                # ⚠️ The existing file is replaced only ONCE THE SCRAPE HAS SUCCEEDED.
                # This used to run before the browser even opened, so a failed attempt
                # left nothing behind and a broken-selector run overwrote a good CSV
                # with a header-only one (2026-07-31: 140 of them).
                if os.path.exists(file_path):
                    self._logger.log_info(
                        f"File exists: {file_path}, replacing with the fresh scrape."
                    )

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
        folder_path = f"{TRADING_VIEW_RAW_DATA_DIR}/{format_key_for_path(key)}"
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
            # Expected: web_scraper/trading_view/links/crypto/{source}/{crypto_type}/{exchange_type}
            if len(parts) < 7:
                self._logger.log_warning(
                    f"Skipping incomplete crypto links path (expected 7 parts, got {len(parts)}): {path}"
                )
                continue
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
            # Expected: web_scraper/trading_view/links/options/{country}
            #
            # ⚠️ The 4-part path IS reachable, which is why this guard exists (crypto has
            # the same one). `options` and `crypto` are the two asset classes whose switch
            # node is a LEAF — no countries configured beneath them — so
            # `SwitchConfig.build_unblocked`, which forces the run-plan ancestors true for
            # every Dagster TradingView materialisation, makes the node itself an enabled
            # path. Without this, `trading_view_links` partition `options` died on
            # `IndexError: list index out of range` before queueing a single task.
            if len(parts) < 5:
                self._logger.log_warning(
                    f"Skipping incomplete options links path (expected 5 parts, got "
                    f"{len(parts)}): {path}"
                )
                continue
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

    def aggregate_trading_view_links(self) -> None:
        self._logger.log_info("Aggregating Trading View link CSV files.")

        links_dir = f"{TRADING_VIEW_RAW_DATA_DIR}/links"
        output_dir = f"{TRADING_VIEW_RAW_DATA_DIR}/collected_links"

        # Collect all CSV files recursively
        csv_files = []
        for root, _, files in os.walk(links_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        if not csv_files:
            self._logger.log_warning(f'No CSV files found in "{links_dir}".')
            return

        # Date = latest OS modification timestamp across all source files
        latest_mtime = max(os.path.getmtime(f) for f in csv_files)
        latest_date_str = datetime.fromtimestamp(latest_mtime).strftime("%Y-%m-%d")

        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/all_links_{latest_date_str}.csv"

        seen_urls: set[str] = set()
        total_written = 0

        with open(output_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(TRADING_VIEW_TABLE_SCHEMA)

            for csv_file in sorted(csv_files):
                with open(csv_file, "r", encoding="utf-8") as infile:
                    reader = csv.reader(infile)
                    next(reader, None)  # skip header row
                    for row in reader:
                        if not any(cell.strip() for cell in row):
                            continue
                        url = row[-1].strip()
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            writer.writerow(row)
                            total_written += 1

        self._logger.log_info(
            f"Aggregated {total_written} unique links from {len(csv_files)} files → {output_path}"
        )

    def add_trading_view_data_scraping_tasks(self) -> None:
        self._logger.log_info("Adding Trading View data scraping tasks.")
        before = self._thread_manager.get_current_number_of_task()

        asset_adders = [
            (ScrapeMainType.STOCKS, self._add_stock_data_tasks),
            (ScrapeMainType.FUNDS, self._add_fund_data_tasks),
            (ScrapeMainType.FUTURES, self._add_futures_data_tasks),
            (ScrapeMainType.FOREX, self._add_forex_data_tasks),
            (ScrapeMainType.CRYPTO, self._add_crypto_data_tasks),
            (ScrapeMainType.INDICES, self._add_indices_data_tasks),
            (ScrapeMainType.BONDS, self._add_bonds_data_tasks),
            (ScrapeMainType.ECONOMY, self._add_economy_data_tasks),
            (ScrapeMainType.OPTIONS, self._add_options_data_tasks),
        ]

        for asset_type, adder in asset_adders:
            if self._switch_handler.is_enabled(
                "web_scraper", "trading_view", "data", asset_type.value
            ):
                count = adder()
                self._logger.log_info(
                    f"Added {count} Trading View {asset_type.value} data scraping tasks."
                )
            else:
                self._logger.log_info(
                    f"Trading View {asset_type.value} data scraping is disabled – skipping."
                )

        after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(
            f"Added total {after - before} Trading View data scraping tasks."
        )

    # ──────────────────────────────────────────────────────────────────────
    # TradingView link-based data scraping – task adders
    # ──────────────────────────────────────────────────────────────────────

    def _add_generic_link_data_tasks(
        self, asset_type: ScrapeMainType, skip_existing: bool = True
    ) -> int:
        """Generic task adder for any TradingView link-based asset type.

        Maps each enabled switch path to the corresponding links CSV directory
        by stripping the 'web_scraper/trading_view/data' prefix and prepending
        TRADING_VIEW_RAW_DATA_DIR/links, which works regardless of directory depth
        (forex has 2 sub-parts, bonds 3, stocks 4, etc.).

        ⚠️ `skip_existing` IS CHECKED HERE, AT TASK-ADD TIME, NOT INSIDE THE SCRAPE.
        Every navigation passes through a GLOBAL 8-second gate
        (`SCRAPER_NAV_STAGGER`, a monotonic lock — see
        `_scrape_data_trading_view_link_attempt`), so a symbol that is merely skipped
        late still costs its 8 s. Skipping before the task is queued costs nothing, and
        across the 4,675 links currently on disk that is the difference between ~10.4 h
        and minutes. This scraper had NO skip at all before: the data path never checked,
        and the links path explicitly deletes and re-fetches, so a full run re-scraped
        everything every time.

        ⚠️ THE FILENAME CARRIES ITS END DATE (`<SYMBOL>_<start>_<today>.csv`), so an
        existing file is matched by GLOB on the symbol prefix, never by equality — the
        name changes every day and an equality test would skip nothing.

        ⚠️ SKIPPING NEVER REFRESHES. A file from three months ago satisfies the check,
        exactly as it does for the CafeF and Simplize scrapers. To re-fetch a symbol,
        delete its CSV; to re-fetch everything, pass `skip_existing=False`.
        """
        added = skipped = 0
        for path in self._switch_handler.get_enabled_paths(
            "web_scraper", "trading_view", "data", asset_type.value
        ):
            # parts[3:] = [asset_type, sub1, ..., subN] — mirrors the links dir layout
            sub_parts = path.split("/")[3:]
            links_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "links", *sub_parts)
            if not os.path.isdir(links_dir):
                self._logger.log_warning(f"Links directory not found: {links_dir}")
                continue
            csv_files = sorted(
                [f for f in os.listdir(links_dir) if f.endswith(".csv")], reverse=True
            )
            if not csv_files:
                self._logger.log_warning(f"No links CSV found in: {links_dir}")
                continue
            # The data tree mirrors the links tree, which is what makes this cheap:
            # the same `sub_parts` name both folders.
            data_dir = os.path.join(TRADING_VIEW_RAW_DATA_DIR, "data", *sub_parts)
            links_path = os.path.join(links_dir, csv_files[0])
            with open(links_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("url", "").strip()
                    if not url:
                        continue
                    symbol = url.split("symbol=")[-1] if "symbol=" in url else url
                    if skip_existing and glob.glob(
                        os.path.join(data_dir, f"{symbol.replace(':', '_')}_*.csv")
                    ):
                        skipped += 1
                        continue
                    task_name = format_key_for_name(("data", *sub_parts, symbol))
                    self._thread_manager.add_task(
                        Task(
                            task_name,
                            self._scrape_data_trading_view_link,
                            dict(row),
                        )
                    )
                    added += 1
        if skipped:
            self._logger.log_info(
                f"Trading View {asset_type.value} data: skipped {skipped} symbol(s) "
                f"already on disk, queued {added}. (skip_existing=True; delete a CSV "
                f"to re-fetch it.)"
            )
        return added

    # Each forwards `skip_existing` so a caller can force a full re-fetch per asset class
    # (`_add_stock_data_tasks(skip_existing=False)`); the default matches the CafeF and
    # Simplize scrapers, which have always skipped what is already on disk.
    def _add_stock_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.STOCKS, skip_existing)

    def _add_fund_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.FUNDS, skip_existing)

    def _add_futures_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.FUTURES, skip_existing)

    def _add_forex_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.FOREX, skip_existing)

    def _add_crypto_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.CRYPTO, skip_existing)

    def _add_indices_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.INDICES, skip_existing)

    def _add_bonds_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.BONDS, skip_existing)

    def _add_economy_data_tasks(self, skip_existing: bool = True) -> int:
        return self._add_generic_link_data_tasks(ScrapeMainType.ECONOMY, skip_existing)

    def _add_options_data_tasks(self) -> int:
        return 0

    # ──────────────────────────────────────────────────────────────────────
    # TradingView link-based data scraping – core scraper
    # ──────────────────────────────────────────────────────────────────────

    def _scrape_data_trading_view_link(self, row: dict) -> None:
        symbol = (
            row.get("url", "").split("symbol=")[-1]
            if "symbol=" in row.get("url", "")
            else row.get("url", "")
        )
        last_error: Exception = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                if attempt > 1:
                    self._logger.log_info(
                        f'Retry {attempt}/{self._retry_attempts} for "{symbol}" '
                        f"(waiting {self._retry_delay}s)."
                    )
                    time.sleep(self._retry_delay)
                self._scrape_data_trading_view_link_attempt(row)
                self._logger.log_info(
                    f'Finish scraping data for "{symbol}" '
                    f"(attempt {attempt}/{self._retry_attempts})."
                )
                return
            except Exception as e:
                last_error = e
                self._logger.log_error(
                    f'Attempt {attempt}/{self._retry_attempts} failed for "{symbol}": {e}'
                )
        self._logger.log_error(
            f'All {self._retry_attempts} attempts failed for "{symbol}". '
            f"Last error: {last_error}"
        )
        raise last_error

    def _scrape_data_trading_view_link_attempt(self, row: dict) -> None:
        scrape_main_type = row.get("scrape_main_type", "")
        url = row.get("url", "")

        # Sub-type (name, value) pairs mirror the links CSV schema, e.g.
        # (country, vietnam) / (stock_type, common_stock) / (sector, finance).
        # Every populated pair becomes both a folder level under raw_data/trading_view/data/
        # and a metadata column in the output CSV — so the data tree mirrors
        # the links tree regardless of how many sub-dimensions an asset has.
        sub_type_pairs = [
            (row.get(f"sub_type_name_{i}", ""), row.get(f"sub_type_value_{i}", ""))
            for i in (1, 2, 3)
        ]

        symbol = url.split("symbol=")[-1] if "symbol=" in url else url
        symbol_safe = symbol.replace(":", "_")

        # Stagger before acquiring the semaphore so threads desync their acquisition attempts
        time.sleep(random.uniform(1, 5))

        # Hard cap on concurrent Chrome instances to avoid resource exhaustion
        with self._browser_slot():
            web_driver, bs4_parser = self._helper_initialize_web_driver_and_bs4_parser()

            stop_event = threading.Event()
            dialog_thread = threading.Thread(
                target=self._dialog_remover_loop,
                args=(web_driver, stop_event),
                daemon=True,
            )

            try:
                # Mirror the links folder structure under raw_data/trading_view/data/
                folder_parts = [TRADING_VIEW_RAW_DATA_DIR, "data", scrape_main_type]
                folder_parts += [value for _, value in sub_type_pairs if value]
                folder_path = "/".join(folder_parts)
                os.makedirs(folder_path, exist_ok=True)

                start_time = SCRAPER_START_DATE
                current_time = datetime.now()
                start_time_date_str = start_time.strftime("%Y-%m-%d")
                current_time_date_str = current_time.strftime("%Y-%m-%d")
                file_path = (
                    f"{folder_path}/{symbol_safe}"
                    f"_{start_time_date_str}_{current_time_date_str}.csv"
                )

                # Rate-limit page navigations so at most one browser is in the
                # heavy JS page-load phase at any given time. With 8 concurrent
                # browsers launching simultaneously, CPU/RAM pressure prevents
                # TradingView's toolbar from rendering within the button timeout.
                # Staggering navigations by SCRAPER_NAV_STAGGER seconds each gives
                # every page a window to load before the next one starts.
                with self._nav_time_lock:
                    _now = time.time()
                    _nav_time = max(_now, self._nav_last_ts + SCRAPER_NAV_STAGGER)
                    self._nav_last_ts = _nav_time
                    _wait = _nav_time - _now
                if _wait > 0:
                    time.sleep(_wait)

                web_driver, bs4_parser = self._helper_navigate_to_url(web_driver, url)

                # Wait until the TradingView chart widget JS object is populated
                chart_ready_js = (
                    "try { return !!window._exposed_chartWidgetCollection"
                    " && !!window._exposed_chartWidgetCollection.activeChartWidget._value; }"
                    " catch(e) { return false; }"
                )
                WebDriverWait(web_driver, 60).until(
                    lambda d: d.execute_script(chart_ready_js)
                )

                dialog_thread.start()

                # Enable dividend adjustment (ADJ) for dividend-bearing assets BEFORE
                # the date range is selected — clicking ADJ resets the chart's range.
                if scrape_main_type in (
                    ScrapeMainType.STOCKS.value,
                    ScrapeMainType.FUNDS.value,
                ):
                    self._helper_toggle_adj_dividends(web_driver, symbol)

                # ─── Date range selection via JS-powered UI clicks ────────────────
                # The chart toolbar button exists in the DOM early but Selenium's
                # is_displayed() returns False under high concurrency (many browsers
                # competing for CPU/RAM). EC.presence_of_element_located checks only
                # that the element is in the DOM; JS .click() then bypasses the
                # visibility requirement entirely, making this reliable at 8 browsers.
                _date_range_btn_xpath = (
                    "/html/body/div[2]/div/div[5]/div[2]/div/div[2]/div/button"
                )
                _custom_range_xpath = '//*[@id="CustomRange"]'
                _input_start_xpath = (
                    '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]'
                    "/div/div/div[1]/div[1]/div/div/div/span/span[1]/input"
                )
                _input_end_xpath = (
                    '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]'
                    "/div/div/div[2]/div[1]/div/div/div/span/span[1]/input"
                )
                _apply_btn_xpath = '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[4]/div/span/button'

                def _presence_js_click(xpath, timeout=90):
                    el = WebDriverWait(web_driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, xpath))
                    )
                    web_driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", el
                    )
                    web_driver.execute_script("arguments[0].click();", el)
                    return el

                _presence_js_click(_date_range_btn_xpath)
                self._logger.log_info(f'Clicked date range button for "{symbol}".')
                time.sleep(1.5)

                _presence_js_click(_custom_range_xpath)
                self._logger.log_info(f'Clicked Custom Range for "{symbol}".')
                time.sleep(0.5)

                self._helper_input_text(
                    web_driver,
                    _input_start_xpath,
                    start_time_date_str,
                    char_by_char=True,
                    confirm="tab",
                )
                self._helper_input_text(
                    web_driver,
                    _input_end_xpath,
                    current_time_date_str,
                    char_by_char=True,
                    confirm="tab",
                )
                _presence_js_click(_apply_btn_xpath)
                self._logger.log_info(
                    f"Applied date range [{start_time_date_str} → {current_time_date_str}]"
                    f' for "{symbol}".'
                )
                time.sleep(SCRAPER_BASE_WAIT_TIME)

                _bar_count_js = (
                    "try { return Object.keys(window._exposed_chartWidgetCollection"
                    ".activeChartWidget._value._modelWV._value.m_model._mainSeries"
                    "._seriesSource._data.m_bars._items).length;"
                    " } catch(e) { return 0; }"
                )
                _pre_apply_count = web_driver.execute_script(_bar_count_js)
                self._logger.log_info(
                    f'Bar count immediately after Apply for "{symbol}": {_pre_apply_count}.'
                )

                # Phase 1 — wait up to 30 s for bar count to change from the
                # post-Apply baseline (TradingView clears then reloads bars).
                for _ in range(60):
                    time.sleep(0.5)
                    if web_driver.execute_script(_bar_count_js) != _pre_apply_count:
                        break

                # Phase 2 — wait for count to stabilize for 3 consecutive seconds
                # (signals that full history has finished loading).
                _cur_count = _pre_apply_count
                _prev_count = -1
                _stable_ticks = 0
                for _ in range(120):  # max 60 s
                    time.sleep(0.5)
                    _cur_count = web_driver.execute_script(_bar_count_js)
                    if _cur_count == _prev_count:
                        _stable_ticks += 1
                        if _stable_ticks >= 6:
                            break
                    else:
                        _stable_ticks = 0
                        _prev_count = _cur_count
                self._logger.log_info(f'Final bar count for "{symbol}": {_cur_count}.')

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

                    // Two-layer OHLC detection sampled across up to 30 bars:
                    //   Layer 1 (structural)  – every bar in a series shares the same
                    //     internal layout; if any bar has fewer than 6 slots the whole
                    //     series is non-OHLC and we bail immediately.
                    //   Layer 2 (semantic)    – verify true OHLC price invariants:
                    //     high >= open/close and low <= open/close.
                    //     This accepts dojis (all four equal) but rejects indicator
                    //     series that happen to have 6 slots with unrelated values.
                    let isOHLC = false;
                    const sampleKeys = keys.slice(0, Math.min(keys.length, 30));
                    for (const k of sampleKeys) {
                        const item = items[k];
                        if (!item || !item.value || !Array.isArray(item.value)) continue;
                        const v = item.value;

                        // Layer 1: structural length check
                        if (v.length < 6) { isOHLC = false; break; }

                        const [, o, h, l, c] = v;
                        if (typeof o !== 'number' || typeof h !== 'number' ||
                            typeof l !== 'number' || typeof c !== 'number') continue;

                        // Layer 2: OHLC price invariants
                        if (h >= o && h >= c && l <= o && l <= c) {
                            isOHLC = true; break;
                        }
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

                result = json.loads(
                    web_driver.execute_script(bulk_js, start_time_date_str)
                )
                is_ohlc = result["is_ohlc"]
                records = result["records"]
                self._logger.log_info(
                    f"Fetched {len(records)} records, isOHLC: {is_ohlc}."
                )

                # Build header: scrape_main_type, <named sub-types…>, symbol, date, value cols
                header = ["scrape_main_type"]
                header += [name for name, _ in sub_type_pairs if name]
                header.append("symbol")
                header.append("date")
                if is_ohlc:
                    header += ["open", "high", "low", "close", "volume"]
                else:
                    header.append("value")

                # Metadata prefix prepended to every data row (aligned with header)
                meta = [scrape_main_type]
                meta += [value for name, value in sub_type_pairs if name]
                meta.append(symbol)

                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(header)
                    for rec in records:
                        writer.writerow(meta + list(rec))

                self._logger.log_info(f"Saved {len(records)} records to {file_path}.")

            finally:
                stop_event.set()
                if dialog_thread.is_alive():
                    dialog_thread.join()
                web_driver.close()

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

    def scrape(self) -> None:
        self._logger.log_info("Start scraping data using ThreadManager.")

        # ── Phase 1: scrape links ─────────────────────────────────────────────
        self._thread_manager.remove_all_tasks()
        before = self._thread_manager.get_current_number_of_task()

        if self._switch_handler.is_enabled("web_scraper", "trading_view", "links"):
            self.add_trading_view_links_scraping_tasks()

        after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(f"Added total {after - before} links scraping tasks.")
        self._logger.log_info(
            f"Start executing {self._thread_manager.get_current_number_of_task()} tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("Finished links scraping phase.")

        # ── Phase 2: aggregate collected links into one CSV ───────────────────
        if self._switch_handler.is_enabled(
            "web_scraper", "trading_view", "collected_links"
        ):
            self.aggregate_trading_view_links()

        self._logger.log_info("Finished link aggregation phase.")

        # ── Phase 3: scrape data from collected links ─────────────────────────
        self._thread_manager.remove_all_tasks()
        before = self._thread_manager.get_current_number_of_task()

        if self._switch_handler.is_enabled("web_scraper", "trading_view", "data"):
            self.add_trading_view_data_scraping_tasks()

        after = self._thread_manager.get_current_number_of_task()
        self._logger.log_info(f"Added total {after - before} data scraping tasks.")
        self._logger.log_info(
            f"Start executing {self._thread_manager.get_current_number_of_task()} tasks."
        )
        self._thread_manager.execute()
        self._logger.log_info("Finished scraping data.")
