import os
import configparser
from pydantic import ValidationError
from ..helper.helper import Helper
from ..logger.logger import Logger
from .model import *


class ConfigHelper(Helper):

    def __init__(self, _logger: Logger):
        self._logger = _logger

    def generate_config_template(self, overwrite: bool = False):
        config_file_path = ".config"

        if os.path.isfile(config_file_path):
            if not overwrite:
                self._logger.log_warning(f'"{config_file_path}" already exists.')
                return False
            else:
                self._logger.log_warning(
                    f'Overwriting "{config_file_path}", generate sample {config_file_path} file.'
                )
                self._generate_config_template(config_file_path)
                return True
        else:
            self._generate_config_template(config_file_path)
            self._logger.log_info(f'Generated sample config file "{config_file_path}"')
            return True

    def _generate_config_template(self, config_file_path: str):
        content = """
[general]
app_name = StockPricePredictorSystem
author = Trung Ly Duc
version = 0.1.0

[ssi_crawler_info]
auth_type = Bearer
consumerID = your_consumerID
consumerSecret = your_consumerSecret
url = https://fc-data.ssi.com.vn/
stream_url = https://fc-datahub.ssi.com.vn

[realtional_database]
server_name = your_server_name
login = your_username
password = your_password

[time_series_database]
url = your_url
org = your_org
token = your_token
"""
        with open(config_file_path, "w") as config_file:
            config_file.write(content.strip())

    def load_config(self):
        config = configparser.ConfigParser()

        try:
            config.read(".config")

            general_config = GeneralConfig(
                app_name=config.get("general", "app_name"),
                author=config.get("general", "author"),
                version=config.get("general", "version"),
            )

            ssi_crawler_info_config = SsiCrawlerInfoConfig(
                auth_type=config.get("ssi_crawler_info", "auth_type"),
                consumerID=config.get("ssi_crawler_info", "consumerID"),
                consumerSecret=config.get("ssi_crawler_info", "consumerSecret"),
                url=config.get("ssi_crawler_info", "url"),
                stream_url=config.get("ssi_crawler_info", "stream_url"),
            )

            relational_database_config = RelationalDatabaseConfig(
                server_name=config.get("realtional_database", "server_name"),
                login=config.get("realtional_database", "login"),
                password=config.get("realtional_database", "password"),
            )

            time_series_database_config = TimeSeriesDatabaseConfig(
                url=config.get("time_series_database", "url"),
                org=config.get("time_series_database", "org"),
                token=config.get("time_series_database", "token"),
            )

            # Combine both configurations into the main model
            return ConfigModel(
                general=general_config,
                ssi_crawler_info=ssi_crawler_info_config,
                relational_database=relational_database_config,
                time_series_database=time_series_database_config,
            )

        except Exception as e:
            print(f"An error occurred while loading .config file: {e}")
            return None
            # raise ValueError(f"Error validating configuration: {e}")
