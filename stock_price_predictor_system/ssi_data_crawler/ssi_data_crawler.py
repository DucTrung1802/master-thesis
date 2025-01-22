import datetime
from ssi_fc_data.fc_md_client import MarketDataClient
from .models import *
from ..logger.logger import Logger
from ..config_helper.models import SsiCrawlerInfoConfig
from ..sql_server_driver.sql_server_driver import SqlServerDriver
from ..sql_server_driver.models import *


class SsiDataCrawler:

    def __init__(self, _logger: Logger):
        self._logger = _logger
        self._config: SsiCrawlerInfoConfig = None
        self._client: MarketDataClient = None
        self._sql_server_driver: SqlServerDriver = None

    def _retrieve_all_market_data(self):
        return self._sql_server_driver.retrieve_data(
            database_name="SSI_STOCKS", table_name="Market"
        )

    def _create_all_market_data(self):

        # Retrieve all market data
        all_market_data = self._retrieve_all_market_data()

        # Check if all market data had been created
        is_five_elements = len(all_market_data) == 5

        symbols = [item[1] for item in all_market_data]
        expected_symbols = {"HNX", "HOSE", "UPCOM", "DER", "BOND"}
        is_correct_symbols = set(symbols) == expected_symbols

        if is_five_elements and is_correct_symbols:
            print(
                "All market data has been created before. No need to create market data."
            )
            self._logger.log_info(
                "All market data has been created before. No need to create market data."
            )
            return

        self._sql_server_driver.purge_data(
            database_name="SSI_STOCKS", table_name="Market"
        )

        market_record_1: Record = Record(
            [
                DataModel(columnName="Symbol", value="HNX", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Sở Giao dịch Chứng khoán Hà Nội",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Hanoi Stock Exchange",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        market_record_2: Record = Record(
            [
                DataModel(
                    columnName="Symbol", value="HOSE", dataType=DataType.NVARCHAR
                ),
                DataModel(
                    columnName="Name",
                    value="Sở Giao dịch Chứng khoán Thành phố Hồ Chí Minh",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Ho Chi Minh City Stock Exchange",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        market_record_3: Record = Record(
            [
                DataModel(
                    columnName="Symbol", value="UPCOM", dataType=DataType.NVARCHAR
                ),
                DataModel(
                    columnName="Name",
                    value="Hệ Thống Giao Dịch Cho Chứng Khoán Chưa Niêm Yết",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Unlisted Public Company Market",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        market_record_4: Record = Record(
            [
                DataModel(columnName="Symbol", value="DER", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Chứng Khoán Phái Sinh",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Derivatives Market",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        market_record_5: Record = Record(
            [
                DataModel(
                    columnName="Symbol", value="BOND", dataType=DataType.NVARCHAR
                ),
                DataModel(
                    columnName="Name",
                    value="Trái Phiếu",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Bond Market",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        market_record_list = [
            market_record_1,
            market_record_2,
            market_record_3,
            market_record_4,
            market_record_5,
        ]

        database_name = "SSI_STOCKS"
        table_name = "Market"
        self._sql_server_driver.insert_data(
            database_name=database_name,
            table_name=table_name,
            records=market_record_list,
        )

        print(
            f"Successfully inserted {len(market_record_list)} in [{database_name}].[dbo].[{table_name}]."
        )
        self._logger.log_info(
            f"Successfully inserted {len(market_record_list)} in [{database_name}].[dbo].[{table_name}]."
        )

    def _retrieve_all_security_type_data(self):
        return self._sql_server_driver.retrieve_data(
            database_name="SSI_STOCKS", table_name="SecurityType"
        )

    def _create_all_security_type_data(self):

        # Retrieve all security type data
        all_security_type_data = self._retrieve_all_security_type_data()

        # Check if all market data had been created
        is_seven_elements = len(all_security_type_data) == 7

        symbols = [item[1] for item in all_security_type_data]
        expected_symbols = {"ST", "CW", "FU", "EF", "BO", "OF", "MF"}
        is_correct_symbols = set(symbols) == expected_symbols

        if is_seven_elements and is_correct_symbols:
            print(
                "All security type data has been created before. No need to create security type data."
            )
            self._logger.log_info(
                "All security type data has been created before. No need to create security type data."
            )
            return

        self._sql_server_driver.purge_data(
            database_name="SSI_STOCKS", table_name="SecurityType"
        )

        security_type_record_1: Record = Record(
            [
                DataModel(columnName="Symbol", value="ST", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Cổ phiếu",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Stock",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_2: Record = Record(
            [
                DataModel(columnName="Symbol", value="CW", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Chứng quyền có bảo đảm",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Covered Warrant",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_3: Record = Record(
            [
                DataModel(columnName="Symbol", value="FU", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Hợp đồng tương lai",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Futures",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_4: Record = Record(
            [
                DataModel(columnName="Symbol", value="EF", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Quỹ hoán đổi danh mục",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Exchange Traded Fund",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_5: Record = Record(
            [
                DataModel(columnName="Symbol", value="BO", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Trái phiếu",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="BOND",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_6: Record = Record(
            [
                DataModel(columnName="Symbol", value="OF", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Quỹ mở",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Open-ended Funds",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_7: Record = Record(
            [
                DataModel(columnName="Symbol", value="MF", dataType=DataType.NVARCHAR),
                DataModel(
                    columnName="Name",
                    value="Quỹ tương hỗ",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="EnName",
                    value="Mutual Fund",
                    dataType=DataType.NVARCHAR,
                ),
                DataModel(
                    columnName="CreateDate",
                    value=datetime.now(),
                    dataType=DataType.DATETIME,
                ),
            ]
        )

        security_type_record_list = [
            security_type_record_1,
            security_type_record_2,
            security_type_record_3,
            security_type_record_4,
            security_type_record_5,
            security_type_record_6,
            security_type_record_7,
        ]

        database_name = "SSI_STOCKS"
        table_name = "SecurityType"
        self._sql_server_driver.insert_data(
            database_name=database_name,
            table_name=table_name,
            records=security_type_record_list,
        )

        print(
            f"Successfully inserted {len(security_type_record_list)} in [{database_name}].[dbo].[{table_name}]."
        )
        self._logger.log_info(
            f"Successfully inserted {len(security_type_record_list)} in [{database_name}].[dbo].[{table_name}]."
        )

    def _crawl_all_securities_data(self):
        securities_input_model = SecuritiesInputModel(pageIndex=1, pageSize=100)
        response = self._client.securities(self._config, securities_input_model)

        securities_output_model = SecuritiesOutputModel(**response)

        securities_data_model_list = securities_output_model.data

        print(securities_output_model)

    def crawl_tabular_data(
        self,
        config: SsiCrawlerInfoConfig,
        sql_server_driver: SqlServerDriver,
    ):
        self._config = config
        self._client = MarketDataClient(config)
        self._sql_server_driver = sql_server_driver

        # Create all markets data
        self._create_all_market_data()

        # Create all security types data
        self._create_all_security_type_data()

        # Crawl all securities data
        # self._crawl_all_securities_data()

    def crawl_time_series_data(self):
        pass

    def retrieve_securities_list(
        self, securities_list_input_model: SecuritiesInputModel
    ):
        pass
