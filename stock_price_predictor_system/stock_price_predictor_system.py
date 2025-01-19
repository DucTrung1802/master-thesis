import os
from .helper.helper import Helper
from .config_helper.config_generator import ConfigGenerator
from .logger.logger import Logger


class StockPricePredictorSystem(Helper):

    def __init__(self):
        self._logger = Logger(
            f"stock_price_predictor_{self.get_today()}",
        )
        self._config_generator = ConfigGenerator(self._logger)

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

    def _generate_sample_config(self):
        self._config_generator._generate_config_template()

    def _crawl_data(self):
        print("Starting the data crawling process...")
        # Placeholder for data crawling logic
        print("Data crawling completed.")

    def _train_model(self):
        print("Training the prediction model...")
        # Placeholder for training logic
        print("Model training completed.")

    def _predict_prices(self):
        print("Predicting stock prices...")
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
                if not self._config_generator.generate_config_template():
                    generate_choice = input(
                        "Configuration has been generated before, do you want to re-generate and overwrite current configuration? [y/n]\nEnter your choice: "
                    ).strip()
                    if generate_choice.lower() == "y":
                        self._config_generator.generate_config_template(overwrite=True)
                        print("Configuration has been overwritten successfully.")
                    elif generate_choice.lower() == "n":
                        print("Canceled re-generating configiuration file.")
                else:
                    print("Configuration has been generated successfully.")

                input("Press Enter to return to the menu...")
            elif choice == "2":
                self._clear_console()
                self._train_model()
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
