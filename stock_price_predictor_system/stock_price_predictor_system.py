import os
import pyodbc
from .helper.helper import Helper
from .config_helper.config_helper import ConfigHelper
from .config_helper.models import ConfigModel
from .logger.logger import Logger
from .sql_server_driver.models import SqlServerAuthentication
from .sql_server_driver.sql_server_driver import SqlServerDriver


class StockPricePredictorSystem(Helper):

    def __init__(self):
        self._logger = Logger(
            f"stock_price_predictor_{self.get_today()}",
        )
        self._config_helper = ConfigHelper(self._logger)
        self._config: ConfigModel = None
        self._sql_server_driver = None

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

    def _print_menu(self):
        self._print_banner()
        print("[1] Generate configuration file .config")
        print("[2] Start crawling data")
        print("[3] Train prediction model")
        print("[4] Predict stock prices")
        print("[x] Exit")

    def _generate_config_template(self, overwrite: bool = False):
        return self._config_helper.generate_config_template(overwrite)

    def _load_config(self) -> ConfigModel:
        self._logger.log_info("Loading configuration file...")
        print("\nLoading configuration file...")
        return self._config_helper.load_config()

    def _validate_sql_server_db(self) -> bool:
        print("\nValidating sql server")
        self._logger.log_info("Validating sql server")

        sql_server_authentication = SqlServerAuthentication(
            server=self._config.sql_server.server_name,
            login=self._config.sql_server.login,
            password=self._config.sql_server.password,
        )

        connection_string = f"DRIVER={sql_server_authentication.driver};SERVER={sql_server_authentication.server};DATABASE={sql_server_authentication.database};UID={sql_server_authentication.login};PWD={sql_server_authentication.password}"
        try:
            with pyodbc.connect(connection_string, timeout=10) as conn:
                print("Connected successfully to SQL Server DB!")
                self._logger.log_info("Connected successfully to SQL Server DB!")
                return True
        except pyodbc.Error as e:
            print("Failed to connect to SQL Server.")
            print(f"Error: {e}")
            self._logger.log_debug("Failed to connect to SQL Server.")
            self._logger.log_debug(f"Error: {e}")
            return False

    def _validate_config(self, _config: ConfigModel):
        self._logger.log_info("Validating configuration file...")
        print("\nValidating configuration file...")

        successful = True
        successful &= self._validate_sql_server_db()

        return successful

    def _create_sql_server_database_schema(self):
        sql_server_authentication = SqlServerAuthentication(
            server=self._config.sql_server.server_name,
            login=self._config.sql_server.login,
            password=self._config.sql_server.password,
        )
        
        self._sql_server_driver = SqlServerDriver(
            self._logger, sql_server_authentication
        )
        
        self._sql_server_driver.

    def _create_database_schemas(self):
        self._logger.log_info("Creating database schema...")
        print("\nCreating database schema...")

        successful = True
        successful &= self._create_sql_server_database_schema()

        return successful

    def _inner_crawl_data(self):
        self._logger.log_info("Start crawling data")
        print("\nStart crawling data")

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

        # self._inner_crawl_data()

        # print("Crawling data has been completed.")

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

    def run(self):
        while True:
            self._clear_console()
            self._print_menu()
            choice = input("Enter your choice: ").strip()

            if choice == "1":
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

                input("Press Enter to return to the menu...")

            elif choice == "2":
                self._clear_console()
                self._crawl_data()
                input("Press Enter to return to the menu...")
            elif choice == "3":
                self._clear_console()
                self._predict_prices()
                input("Press Enter to return to the menu...")
            elif choice.lower() == "x":
                print("Exiting the system...")
                break
            else:
                self._clear_console()
                print("Invalid choice. Please try again.")
                input("Press Enter to return to the menu...")


if __name__ == "__main__":
    system = StockPricePredictorSystem()
    system.run()
