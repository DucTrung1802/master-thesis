from stock_price_predictor_system.config_helper.config_generator import ConfigGenerator
from stock_price_predictor_system.logger.logger import Logger, LogType


def main():
    logger = Logger(file_name="stock_price_predictor", level=LogType.INFO)
    my_config_generator = ConfigGenerator(logger)
    my_config_generator._generate_config_template(overwrite=True)


if __name__ == "__main__":
    main()
