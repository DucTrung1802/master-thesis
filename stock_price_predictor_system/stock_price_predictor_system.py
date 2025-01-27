import os
from typing import List
import time

from .logger.logger import Logger

from .helper.helper import Helper

from .config_helper.config_helper import ConfigHelper
from .config_helper.model import ConfigModel

from .relational_database_driver.relational_database_driver import (
    RelationalDatabaseDriver,
)
from .relational_database_driver.sql_server_driver import SqlServerDriver
from .relational_database_driver.model import *

from .ssi_data_crawler.ssi_data_crawler import SsiDataCrawler


class StockPricePredictorSystem(Helper):

    def __init__(self):
        self._logger = Logger(
            f"stock_price_predictor_{self.get_today()}",
        )
        self._config_helper = ConfigHelper(self._logger)
        self._config: ConfigModel = None

        self._relational_database_driver: RelationalDatabaseDriver = SqlServerDriver(
            self._logger
        )
        self._time_series_database_driver = None

        self._ssi_data_crawler = SsiDataCrawler(self._logger)

    def _print_banner(self):
        banner = r"""    
  /$$$$$$  /$$$$$$$  /$$$$$$$   /$$$$$$ 
 /$$__  $$| $$__  $$| $$__  $$ /$$__  $$
| $$  \__/| $$  \ $$| $$  \ $$| $$  \__/
|  $$$$$$ | $$$$$$$/| $$$$$$$/|  $$$$$$ 
 \____  $$| $$____/ | $$____/  \____  $$
 /$$  \ $$| $$      | $$       /$$  \ $$
|  $$$$$$/| $$      | $$      |  $$$$$$/
 \______/ |__/      |__/       \______/ 
                                        
Welcome to Stock Price Predictor System!
Created by Trung Ly Duc         
"""

        print(banner)

    def _generate_config_template(self, overwrite: bool = False):
        return self._config_helper.generate_config_template(overwrite)

    def _load_config(self) -> ConfigModel:
        print("\nLoading configuration file...")
        self._logger.log_info("Loading configuration file...")
        return self._config_helper.load_config()

    def _validate_config(self, _config: ConfigModel):
        print("\nValidating configuration file...")
        self._logger.log_info("Validating configuration file...")

        successful = True

        successful &= self._relational_database_driver.open_connection(
            SqlServerAuthentication(
                server=_config.relational_database.server_name,
                login=_config.relational_database.login,
                password=_config.relational_database.password,
            )
        )

        return successful

    def _create_relational_database_schemas(self):
        print("\nStart creating relational database schemas.")
        self._logger.log_info("Start creating relational database schemas.")

        try:
            # region Create relational database
            self._relational_database_driver.create_database("SSI_STOCKS")
            # endregion

            # region Create Market Table
            market_table_columns: List[Column] = [
                Column(columnName="ID", dataType=DataType.INT(), nullable=False),
                Column(
                    columnName="Symbol", dataType=DataType.NVARCHAR(5), nullable=False
                ),
                Column(
                    columnName="Name", dataType=DataType.NVARCHAR(200), nullable=False
                ),
                Column(
                    columnName="EnName", dataType=DataType.NVARCHAR(200), nullable=False
                ),
                Column(
                    columnName="CreateDate",
                    dataType=DataType.DATETIME(),
                    nullable=False,
                ),
                Column(
                    columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True
                ),
                Column(
                    columnName="DeleteDate", dataType=DataType.DATETIME(), nullable=True
                ),
            ]

            self._relational_database_driver.create_table(
                database_name="SSI_STOCKS",
                table_name="Market",
                columns=market_table_columns,
                key_column_name="ID",
            )
            # endregion

            # region Create SecurityType Table
            security_type_table_columns: List[Column] = [
                Column(columnName="ID", dataType=DataType.INT(), nullable=False),
                Column(
                    columnName="Symbol", dataType=DataType.NVARCHAR(2), nullable=False
                ),
                Column(
                    columnName="Name", dataType=DataType.NVARCHAR(30), nullable=False
                ),
                Column(
                    columnName="EnName", dataType=DataType.NVARCHAR(30), nullable=False
                ),
                Column(
                    columnName="CreateDate",
                    dataType=DataType.DATETIME(),
                    nullable=False,
                ),
                Column(
                    columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True
                ),
                Column(
                    columnName="DeleteDate", dataType=DataType.DATETIME(), nullable=True
                ),
            ]

            self._relational_database_driver.create_table(
                database_name="SSI_STOCKS",
                table_name="SecurityType",
                columns=security_type_table_columns,
                key_column_name="ID",
            )
            # endregion

            # region Create Security Table
            security_table_columns: List[Column] = [
                Column(columnName="ID", dataType=DataType.INT(), nullable=False),
                Column(
                    columnName="Symbol", dataType=DataType.NVARCHAR(12), nullable=False
                ),
                Column(
                    columnName="Name", dataType=DataType.NVARCHAR(200), nullable=True
                ),
                Column(
                    columnName="EnName", dataType=DataType.NVARCHAR(200), nullable=True
                ),
                Column(
                    columnName="ListedShare", dataType=DataType.BIGINT(), nullable=True
                ),
                Column(
                    columnName="MarketCapitalization",
                    dataType=DataType.BIGINT(),
                    nullable=True,
                ),
                Column(columnName="Market_ID", dataType=DataType.INT(), nullable=False),
                Column(
                    columnName="SecurityType_ID",
                    dataType=DataType.INT(),
                    nullable=True,
                ),
                Column(
                    columnName="CreateDate",
                    dataType=DataType.DATETIME(),
                    nullable=False,
                ),
                Column(
                    columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True
                ),
                Column(
                    columnName="DelistDate", dataType=DataType.DATETIME(), nullable=True
                ),
            ]

            security_table_foreign_keys: List[ForeignKey] = [
                ForeignKey(name="Market_ID", tableToRefer="Market", columnToRefer="ID"),
                ForeignKey(
                    name="SecurityType_ID",
                    tableToRefer="SecurityType",
                    columnToRefer="ID",
                ),
            ]

            self._relational_database_driver.create_table(
                database_name="SSI_STOCKS",
                table_name="Security",
                columns=security_table_columns,
                key_column_name="ID",
                foreign_keys=security_table_foreign_keys,
            )
            # endregion

            return True

        except Exception as e:
            print(f"Error creating schemas in relational database: {e}.")
            self._logger.log_error(
                f"Error creating schemas in relational database: {e}."
            )

            return False

    def _create_database_schemas(self):
        print("\nCreating database schema...")
        self._logger.log_info("Creating database schema...")

        successful = True
        successful &= self._create_relational_database_schemas()

        return successful

    def _add_api_config_for_crawler(self, api_crawler_config: SsiDataCrawler):
        self._ssi_data_crawler.add_crawler_config(api_crawler_config)

    def _crawl_relational_data(self) -> bool:
        print("\nStart crawling relational data. Please wait...")
        self._logger.log_info("Start crawling relational data. Please wait...")
        if not self._ssi_data_crawler.crawl_relational_data(
            self._relational_database_driver
        ):
            print("\nCannot crawl relational data.")
            self._logger.log_error("\nCannot crawl relational data.")
            return False

        return True

    def _crawl_time_series_data(self) -> bool:
        return True

    def _crawl_data(self):
        self._config = self._load_config()

        if not self._config:
            return

        print("Successfully loaded .config configuration file.")

        if not self._validate_config(self._config):
            return

        print("\nSuccessfully validated .config configuration file.")

        if not self._create_database_schemas():
            return

        print("\nSuccessfully created all database schemas.")

        self._add_api_config_for_crawler(self._config.ssi_crawler_info)

        self._crawl_relational_data()

        # self._crawl_time_series_data()

        print("\nCrawling data has been completed.")

    def _confirm_action(self) -> bool:
        self._clear_console()
        print(
            "You are going to perform an irreversible action. Please retype the following string to continue."
        )
        self._logger.log_info("Prompting for confirmation.")
        confirm_string = self.generate_lower_string(3)
        input_string = input(f"{confirm_string}\nInput: ")

        if input_string == confirm_string:
            print("Confirm action successfully. Wait a second...")
            self._logger.log_info("Confirm action successfully. Wait a second...")
            time.sleep(1)
            return True

        print("Confirm action failed. Wait a second...")
        self._logger.log_info("Confirm action failed. Wait a second...")
        time.sleep(1)
        return False

    def _purge_relational_data(self):
        if not self._confirm_action():
            return

        self._clear_console()
        print("Start purging all relational data...")
        self._logger.log_info("Start purging all relational data...")

        self._relational_database_driver.delete(
            database_name="SSI_STOCKS", table_name="Market"
        )
        self._relational_database_driver.delete(
            database_name="SSI_STOCKS", table_name="Security"
        )
        self._relational_database_driver.delete(
            database_name="SSI_STOCKS", table_name="SecurityType"
        )

        print("Finish purging all relational data.")
        self._logger.log_info("Finish purging all relational data.")

        return True

    def _purge_all_data(self):
        self._config = self._load_config()

        if not self._config:
            return

        print("Successfully loaded .config configuration file.")

        if not self._validate_config(self._config):
            return

        print("\nSuccessfully validated .config configuration file.")

        input("\nPress Enter to continue...")

        while True:
            self._clear_console()
            print(
                "\nWARNING: Once you delete data, there is no going back. Please be certain.\n"
            )
            print("[1] Purge relational data")
            print("[2] Purge time series data")
            print("[0] Go back")

            choice = input("Enter your choice: ").strip()

            match (choice):

                case "1":
                    self._purge_relational_data()
                    break

                case "2":
                    break

                case "0":
                    break

                case _:
                    self._clear_console()
                    print("Invalid choice. Please try again.")
                    input("\nPress Enter to return to the menu...")
                    continue

        input("\nPress Enter to return to the menu...")

    def _train_model(self):
        print("\nTraining the prediction model...")
        # Placeholder for training logic
        print("Model training completed.")

    def _predict_prices(self):
        print("\nPredicting stock prices...")
        # Placeholder for prediction logic
        print("Prediction completed.")

    def _clear_console(self):
        os.system("cls" if os.name == "nt" else "clear")

    def _print_menu(self):
        self._print_banner()
        print("[1] Generate configuration file .config")
        print("[2] Start crawling data")
        print("[3] Purge all data")
        print("[4] Predict stock prices")
        print("[x] Exit")

    def run(self):
        while True:
            self._clear_console()
            self._print_menu()
            choice = input("Enter your choice: ").strip()

            match (choice.lower()):

                case "1":
                    self._clear_console()
                    if not self._generate_config_template():
                        generate_choice = input(
                            "Configuration has been generated before, do you want to re-generate and overwrite current configuration? [y/n]\nEnter your choice: "
                        ).strip()
                        if generate_choice.lower() == "y":
                            self._generate_config_template(overwrite=True)
                            print("Configuration has been overwritten successfully.")
                        elif generate_choice.lower() == "n":
                            print("Canceled re-generating configiuration file.")
                        else:
                            print("Invalid choice.")
                    else:
                        print("Configuration has been generated successfully.")

                    input("\nPress Enter to return to the menu...")

                case "2":
                    self._clear_console()
                    self._crawl_data()
                    input("\nPress Enter to return to the menu...")

                case "3":
                    self._clear_console()
                    self._purge_all_data()

                case "x":
                    print("Exiting the system...")
                    break

                case _:
                    self._clear_console()
                    print("Invalid choice. Please try again.")
                    input("\nPress Enter to return to the menu...")


if __name__ == "__main__":
    system = StockPricePredictorSystem()
    system.run()
