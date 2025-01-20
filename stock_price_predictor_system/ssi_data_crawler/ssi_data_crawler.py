from ssi_fc_data import fc_md_client
from ssi_fc_data.fc_md_client import MarketDataClient
from .models import *
from ..logger.logger import Logger
from ..config_helper.models import SsiCrawlerInfoConfig
from ..sql_server_driver.sql_server_driver import SqlServerDriver


class SsiDataCrawler:

    def __init__(self, _logger: Logger):
        self._logger = _logger
        # self._config: config = _config
        # self._client: MarketDataClient = fc_md_client.MarketDataClient(self._config)

    def crawl_tabular_data(
        self,
        config: SsiCrawlerInfoConfig,
        sql_server_driver: SqlServerDriver,
    ):
        client = fc_md_client.MarketDataClient(config)
        securities_list_input_model = SecuritiesInputModel(market=Market_5.HNX.value)
        print(client.securities(config, securities_list_input_model))

    def crawl_time_series_data(self):
        pass

    def retrieve_securities_list(
        self, securities_list_input_model: SecuritiesInputModel
    ):
        pass
