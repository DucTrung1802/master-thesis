import os
from ..helper.helper import Helper
from ..logger.logger import Logger


class ConfigGenerator(Helper):

    def __init__(self, _logger: Logger):
        self._logger = _logger

    def generate_config_template(self, overwrite=False):
        config_file_path = ".config"

        if os.path.isfile(config_file_path):
            if not overwrite:
                self._logger.log_warning(f'"{config_file_path}" already exists.')
                return False
            else:
                self._logger.log_warning(
                    f'Overwriting "{config_file_path}", generate sample .config file.'
                )
                self._inner_generate_config_template(config_file_path)
                return True
        else:
            self._inner_generate_config_template(config_file_path)
            self._logger.log_info(f'Generated sample config file "{config_file_path}"')
            return True

    def _inner_generate_config_template(self, config_file_path: str):
        content = """
[general]
app_name = StockPricePredictorSystem
author = Trung Ly Duc
version = 0.1.0

[sql_server_database]
server_name = localhost
login = admin
password = secretpassword
"""
        with open(config_file_path, "w") as config_file:
            config_file.write(content.strip())
