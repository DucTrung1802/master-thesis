from selenium import webdriver
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from bs4 import BeautifulSoup

import csv

import time

from logger.logger import Logger
from utils.constants import MACROECONOMICS_URL_GDP


class WebScraper:
    def __init__(self, logger: Logger):
        self.logger: Logger = logger

        chrome_options = Options()
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        self.web_driver: ChromiumDriver = webdriver.Chrome(options=chrome_options)

        self.bs4_parser: BeautifulSoup = None

    def _update_bs4_parser(self):
        self.bs4_parser = BeautifulSoup(self.web_driver.page_source, "html.parser")

    def _navigate_to_url(self, url: str):
        self.logger.log_info(f'Navigating to URL: "{url}"')
        self.web_driver.get(url)
        self._update_bs4_parser()

    def scrape_macroeconomics_data_gdp(self):
        self.logger.log_info("Start scraping macroeconomics data for GDP.")

        self._navigate_to_url(MACROECONOMICS_URL_GDP)

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
        from_year_select_element.select_by_visible_text("2024")

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
        to_year_select_element.select_by_visible_text("2024")

        view_button_element = self.web_driver.find_element(
            by=By.XPATH,
            value="""//*[@id="macro-content"]/div/div/div[3]/div/div[2]/div/button""",
        )
        view_button_element.click()

        time.sleep(2)

        self._update_bs4_parser()

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

        # Write to CSV
        with open("macro_data.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def scrape_macroeconomics_data(self):
        self.logger.log_info("Start scraping macroeconomics data.")

        self.scrape_macroeconomics_data_gdp()
