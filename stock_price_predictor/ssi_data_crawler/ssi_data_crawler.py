from ssi_fc_data import fc_md_client
from ssi_fc_data.fc_md_client import MarketDataClient
import models
import config


class SsiDataCrawler:

    def __init__(self, _config: config):
        self._config: config = _config
        self._client: MarketDataClient = fc_md_client.MarketDataClient(self._config)

    def retrieve_securities_list(
        self, securities_list_input_model: models.SecuritiesInputModel
    ):
        self._client.securities(self._config, securities_list_input_model)
