import datetime
from ssi_fc_data.fc_md_client import MarketDataClient
import math
import time

from .api_model import *

from .database_model import *

from ..logger.logger import Logger

from ..config_helper.model import SsiCrawlerInfoConfig

from ..relational_database_driver.relational_database_driver import (
    RelationalDatabaseDriver,
)
from ..relational_database_driver.sql_server_driver import SqlServerDriver
from ..relational_database_driver.model import *

from ..time_series_database_driver.time_series_database_driver import (
    TimeSeriesDatabaseDriver,
)
from ..time_series_database_driver.influxdb_driver import InfluxdbDriver
from ..time_series_database_driver.model import *

from ..helper.helper import Helper

from .enum import *


class SsiDataCrawler(Helper):

    def __init__(self, _logger: Logger):
        self._logger = _logger

        self._config: SsiCrawlerInfoConfig = None
        self._client: MarketDataClient = None

        self._relational_database_driver: RelationalDatabaseDriver = None
        self._time_series_database_driver: TimeSeriesDatabaseDriver = None

    def add_crawler_config(self, add_crawler_config: SsiCrawlerInfoConfig):
        self._config = add_crawler_config
        self._client = MarketDataClient(self._config)

    def _is_initialized(self) -> bool:
        return (
            self._config
            and self._client
            and isinstance(self._config, SsiCrawlerInfoConfig)
            and isinstance(self._client, MarketDataClient)
        )

    # Wrapper
    def _get_securities(self, securities_input_model: SecuritiesInputModel):
        result = self._client.securities(self._config, securities_input_model)
        time.sleep(2)
        return result

    def _retrieve_all_market_data(self):
        return self._relational_database_driver.select(
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
            return True

        self._relational_database_driver.delete(
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
        if not self._relational_database_driver.insert(
            database_name=database_name,
            table_name=table_name,
            records=market_record_list,
        ):
            return False

        print(
            f"Successfully inserted {len(market_record_list)} in [{database_name}].[dbo].[{table_name}]."
        )
        self._logger.log_info(
            f"Successfully inserted {len(market_record_list)} in [{database_name}].[dbo].[{table_name}]."
        )

        return True

    def _retrieve_all_security_type_data(self):
        return self._relational_database_driver.select(
            database_name="SSI_STOCKS", table_name="SecurityType"
        )

    def _create_all_security_type_data(self) -> bool:

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
            return True

        # Truncate former security type data
        if not self._relational_database_driver.truncate_table(
            database_name="SSI_STOCKS", table_name="SecurityType"
        ):
            print("\nCannot truncate former security type data.")
            self._logger.log_error("Cannot truncate former security type data.")
            return False

        # Create new security type data
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
                    value=datetime.now().replace(microsecond=0),
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
        self._relational_database_driver.insert(
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

        return True

    def _create_and_truncate_temp_security_table(self):
        database_name = "SSI_STOCKS"
        temp_security_table_name = "TempSecurity"

        if not self._relational_database_driver.check_database_exist(
            database_name=database_name
        ):
            print(
                f"Database [{database_name}] does not exist. Cannot create {temp_security_table_name} table."
            )
            self._logger.log_error(
                f"Database [{database_name}] does not exist. Cannot create {temp_security_table_name} table."
            )
            return False

        if not self._relational_database_driver.check_table_exist(
            database_name=database_name, table_name=temp_security_table_name
        ):
            temp_security_columns: List[Column] = [
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
                Column(columnName="Market_ID", dataType=DataType.INT(), nullable=False),
                Column(
                    columnName="CreateDate",
                    dataType=DataType.DATETIME(),
                    nullable=False,
                ),
            ]

            if not self._relational_database_driver.create_table(
                database_name=database_name,
                table_name=temp_security_table_name,
                columns=temp_security_columns,
                key_column_name="ID",
            ):
                return False

        self._relational_database_driver.truncate_table(
            database_name=database_name, table_name=temp_security_table_name
        )

        return True

    def _crawl_all_securities_data(self):

        page_size = 100

        # Crawl to know the total number of record
        securities_input_model = SecuritiesInputModel()
        print("\nCrawling to know the total number of records.")
        self._logger.log_info("Crawling to know the total number of records.")
        response = self._get_securities(securities_input_model)
        securities_output_model = SecuritiesOutputModel(**response)

        # Process if no records were found
        if securities_output_model.totalRecord == 0:
            print("Successfully crawl securities data but no records were found.")
            self._logger.log_info(
                "Successfully crawl securities data but no records were found."
            )
            return False

        total_record = securities_output_model.totalRecord
        print(f"Total records found: {total_record}")
        self._logger.log_info(f"Total records found: {total_record}")

        number_of_page = math.ceil(total_record / page_size)

        # Create temp table for joining security data
        if not self._create_and_truncate_temp_security_table():
            return False

        # Crawl all securities to temp table
        for i in range(1, number_of_page + 1):
            securities_input_model = SecuritiesInputModel(
                pageIndex=i, pageSize=page_size
            )
            print(
                f"\nCrawling security data from API. PageIndex: {securities_input_model.pageIndex}/{number_of_page}. PageSize: {securities_input_model.pageSize}."
            )
            self._logger.log_info(
                f"Crawling security data from API. PageIndex: {securities_input_model.pageIndex}/{number_of_page}. PageSize: {securities_input_model.pageSize}."
            )
            response = self._get_securities(securities_input_model)
            securities_output_model = SecuritiesOutputModel(**response)
            securities_data_model = [
                SecuritiesDataModel(
                    market=MarketCode.get_market_code(security_data_model["Market"]),
                    symbol=security_data_model["Symbol"],
                    stockName=security_data_model["StockName"],
                    stockEnName=security_data_model["StockEnName"],
                )
                for security_data_model in securities_output_model.data
            ]

            # Insert data to temp security table
            security_record_list = [
                Record(
                    [
                        DataModel(
                            columnName="Symbol",
                            dataType=DataType.NVARCHAR,
                            value=security.symbol,
                        ),
                        DataModel(
                            columnName="Name",
                            dataType=DataType.NVARCHAR,
                            value=security.stockName,
                        ),
                        DataModel(
                            columnName="EnName",
                            dataType=DataType.NVARCHAR,
                            value=security.stockEnName,
                        ),
                        DataModel(
                            columnName="Market_ID",
                            dataType=DataType.INT,
                            value=security.market,
                        ),
                        DataModel(
                            columnName="CreateDate",
                            dataType=DataType.DATETIME,
                            value=self.get_current_timestamp(),
                        ),
                    ]
                )
                for security in securities_data_model
            ]

            self._relational_database_driver.insert(
                database_name="SSI_STOCKS",
                table_name="TempSecurity",
                records=security_record_list,
            )

        # Merge temp security table to security table
        self._relational_database_driver.merge(
            database_name="SSI_STOCKS",
            source_table="TempSecurity",
            target_table="Security",
            matching_column="Symbol",
            action_when_match=UpdateInMerge(
                [
                    ColumnToUpdate(source_column="Name", target_column="Name"),
                    ColumnToUpdate(source_column="EnName", target_column="EnName"),
                    ColumnToUpdate(
                        source_column="Market_ID", target_column="Market_ID"
                    ),
                    ColumnToUpdate(
                        source_column="CreateDate",
                        target_column="UpdateDate",
                    ),
                ]
            ),
            action_when_not_match_by_target=InsertInMerge(
                [
                    ColumnToUpdate(target_column="Symbol", source_column="Symbol"),
                    ColumnToUpdate(target_column="Name", source_column="Name"),
                    ColumnToUpdate(target_column="EnName", source_column="EnName"),
                    ColumnToUpdate(
                        target_column="Market_ID", source_column="Market_ID"
                    ),
                    ColumnToUpdate(
                        target_column="CreateDate", source_column="CreateDate"
                    ),
                ]
            ),
            action_when_not_match_by_source=UpdateInMerge(
                [
                    ColumnToUpdate(
                        target_column="DelistDate",
                        dataType=DataType.DATETIME,
                        value=self.get_current_timestamp(),
                    )
                ]
            ),
        )

        return True

    def _retrieve_all_security_data(self) -> List[Security]:

        not_delisted_condition = Condition(
            column="DelistDate",
            operator=Operator.IS,
            value="NULL",
            dataType=DataType.RAW,
        )

        security_list = self._relational_database_driver.select(
            database_name="SSI_STOCKS",
            table_name="Security",
            condition_list=not_delisted_condition,
        )

        return [
            Security(**dict(zip(Security.get_key_list(), row))) for row in security_list
        ]

    def crawl_relational_data(
        self, _relational_database_driver: RelationalDatabaseDriver
    ) -> bool:
        if not self._is_initialized():
            print(
                "\nClient is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            self._logger.log_error(
                "Client is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            return False

        if not isinstance(_relational_database_driver, RelationalDatabaseDriver):
            print('\nInvalid "_relational_database_driver".')
            self._logger.log_error('Invalid "_relational_database_driver".')
            return False

        self._relational_database_driver = _relational_database_driver

        # Create all markets data
        if not self._create_all_market_data():
            print("\nCannot create all markets.")
            self._logger.log_error("Cannot create all markets.")
            return False

        # Create all security types data
        if not self._create_all_security_type_data():
            print("\nCannot create all security types.")
            self._logger.log_error("Cannot create all security types.")
            return False

        # Crawl all securities data
        if not self._crawl_all_securities_data():
            print("\nCannot crawl all securities data.")
            self._logger.log_error("Cannot crawl all securities data.")
            return False

        return True

    def crawl_time_series_data(
        self, _time_series_database_driver: TimeSeriesDatabaseDriver
    ) -> bool:
        if not self._is_initialized():
            print(
                "\nClient is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            self._logger.log_error(
                "Client is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            return False

        if not isinstance(_time_series_database_driver, TimeSeriesDatabaseDriver):
            print('\nInvalid "_time_series_database_driver".')
            self._logger.log_error('Invalid "_time_series_database_driver".')
            return False

        self._time_series_database_driver = _time_series_database_driver

        read_component = ReadComponent(
            bucket="root",
            start_time=datetime(2025, 1, 28, 2, 27, 0),
            end_time=datetime(2025, 1, 28, 3, 27, 0),
            measurement="go_info",
        )

        records = self._time_series_database_driver.read(read_component)

        return True
