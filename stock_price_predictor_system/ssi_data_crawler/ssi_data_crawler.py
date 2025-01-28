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
from ..relational_database_driver.model import *

from ..time_series_database_driver.time_series_database_driver import (
    TimeSeriesDatabaseDriver,
)
from ..time_series_database_driver.model import *

from ..helper.helper import Helper

from .enum import *

from ..constant import *


class SsiDataCrawler(Helper):

    def __init__(self, _logger: Logger):
        self._logger = _logger

        self._config: SsiCrawlerInfoConfig = None
        self._client: MarketDataClient = None

        self._relational_database_driver: RelationalDatabaseDriver = None
        self._time_series_database_driver: TimeSeriesDatabaseDriver = None

    # region Public methods

    def add_crawler_config(self, add_crawler_config: SsiCrawlerInfoConfig):
        self._config = add_crawler_config
        self._client = MarketDataClient(self._config)

    def add_relational_database_driver(
        self,
        relational_database_driver: RelationalDatabaseDriver,
    ):
        self._relational_database_driver = relational_database_driver

    def add_time_series_database_driver(
        self, time_series_database_driver: TimeSeriesDatabaseDriver
    ):
        self._time_series_database_driver = time_series_database_driver

    def crawl_relational_data(self) -> bool:
        if not self._is_initialized():
            print(
                "\nClient is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            self._logger.log_error(
                "Client is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            return False

        if not isinstance(self._relational_database_driver, RelationalDatabaseDriver):
            print('\nInvalid "_relational_database_driver".')
            self._logger.log_error('Invalid "_relational_database_driver".')
            return False

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

    def crawl_time_series_data(self) -> bool:
        if not self._is_initialized():
            print(
                "\nClient is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            self._logger.log_error(
                "Client is not initialized. Cannot crawl data. Double check configuration and try again."
            )
            return False

        if not isinstance(self._time_series_database_driver, TimeSeriesDatabaseDriver):
            print('\nInvalid "_time_series_database_driver".')
            self._logger.log_error('Invalid "_time_series_database_driver".')
            return False

        all_securities = self._retrieve_all_security_data()

        all_security_symbols = [security.Symbol for security in all_securities]

        start_interval = DEFAULT_CRAWL_DATA_START_DATE
        checkpoint_symbol = None
        found_checkpoint_symbol = False

        crawl_checkpoint = self._get_time_series_data_crawl_checkpoint()

        if (
            crawl_checkpoint
            and crawl_checkpoint.CurrentStartInterval
            and crawl_checkpoint.CurrentSymbol in all_security_symbols
        ):
            start_interval = crawl_checkpoint.CurrentStartInterval
            checkpoint_symbol = crawl_checkpoint.CurrentSymbol

        while start_interval < datetime.now():
            end_interval = start_interval + CRAWL_DATA_TIME_INTERVAL - timedelta(days=1)

            print(
                f"\nCrawling data in interval {start_interval.strftime("%d/%m/%Y")} - {end_interval.strftime("%d/%m/%Y")}."
            )
            self._logger.log_info(
                f"Crawling data in interval {start_interval.strftime("%d/%m/%Y")} - {end_interval.strftime("%d/%m/%Y")}."
            )

            for symbol in all_security_symbols:
                if (
                    checkpoint_symbol
                    and symbol != checkpoint_symbol
                    and not found_checkpoint_symbol
                ):
                    continue

                found_checkpoint_symbol = True

                print(f"\nCrawling data for security: {symbol}")
                self._logger.log_info(f"Crawling data for security: {symbol}")

                daily_stock_price_input_model = DailyStockPriceInputModel(
                    symbol=symbol,
                    fromDate=start_interval,
                    toDate=end_interval,
                    market=None,
                    pageIndex=1,
                    pageSize=50,
                )

                response = self._get_daily_stock_price(daily_stock_price_input_model)
                daily_stock_price_output_model = DailyStockPriceOutputModel(**response)

                # Process if no records were found
                if daily_stock_price_output_model.totalRecord == 0:
                    print(
                        f"\nSuccessfully crawl daily stock price data from {start_interval.strftime("%d/%m/%Y")} to {end_interval.strftime("%d/%m/%Y")} but no records were found. Skip to next stock."
                    )
                    self._logger.log_info(
                        f"Successfully crawl daily stock price data from {start_interval.strftime("%d/%m/%Y")} to {end_interval.strftime("%d/%m/%Y")} but no records were found. Skip to next stock."
                    )

                    self._set_time_series_data_crawl_checkpoint(start_interval, symbol)
                    continue

                daily_stock_price_data_model_list = [
                    DailyStockPriceDataModel(
                        symbol=daily_stock_price_data_model["Symbol"],
                        tradingDate=datetime.strptime(
                            daily_stock_price_data_model["TradingDate"], "%d/%m/%Y"
                        ),
                        time=daily_stock_price_data_model[
                            "Time"
                        ],  # Assuming `Time` is None
                        priceChange=int(
                            float(daily_stock_price_data_model["PriceChange"])
                        ),
                        perPriceChange=float(
                            daily_stock_price_data_model["PerPriceChange"]
                        ),
                        ceilingPrice=int(
                            float(daily_stock_price_data_model["CeilingPrice"])
                        ),
                        floorPrice=int(
                            float(daily_stock_price_data_model["FloorPrice"])
                        ),
                        refPrice=int(float(daily_stock_price_data_model["RefPrice"])),
                        openPrice=int(float(daily_stock_price_data_model["OpenPrice"])),
                        highestPrice=int(
                            float(daily_stock_price_data_model["HighestPrice"])
                        ),
                        lowestPrice=int(
                            float(daily_stock_price_data_model["LowestPrice"])
                        ),
                        closePrice=int(
                            float(daily_stock_price_data_model["ClosePrice"])
                        ),
                        averagePrice=int(
                            float(daily_stock_price_data_model["AveragePrice"])
                        ),
                        closePriceAdjusted=int(
                            float(daily_stock_price_data_model["ClosePriceAdjusted"])
                        ),
                        totalMatchVol=int(
                            float(daily_stock_price_data_model["TotalMatchVol"])
                        ),
                        totalMatchVal=int(
                            float(daily_stock_price_data_model["TotalMatchVal"])
                        ),
                        totalDealVal=int(
                            float(daily_stock_price_data_model["TotalDealVal"])
                        ),
                        totalDealVol=int(
                            float(daily_stock_price_data_model["TotalDealVol"])
                        ),
                        foreignBuyVolTotal=int(
                            float(daily_stock_price_data_model["ForeignBuyVolTotal"])
                        ),
                        foreignCurrentRoom=int(
                            float(daily_stock_price_data_model["ForeignCurrentRoom"])
                        ),
                        foreignSellVolTotal=int(
                            float(daily_stock_price_data_model["ForeignSellVolTotal"])
                        ),
                        foreignBuyValTotal=int(
                            float(daily_stock_price_data_model["ForeignBuyValTotal"])
                        ),
                        foreignSellValTotal=int(
                            float(daily_stock_price_data_model["ForeignSellValTotal"])
                        ),
                        totalBuyTrade=int(
                            float(daily_stock_price_data_model["TotalBuyTrade"])
                        ),
                        totalBuyTradeVol=int(
                            float(daily_stock_price_data_model["TotalBuyTradeVol"])
                        ),
                        totalSellTrade=int(
                            float(daily_stock_price_data_model["TotalSellTrade"])
                        ),
                        totalSellTradeVol=int(
                            float(daily_stock_price_data_model["TotalSellTradeVol"])
                        ),
                        netBuySellVol=int(
                            float(daily_stock_price_data_model["NetBuySellVol"])
                        ),
                        netBuySellVal=int(
                            float(daily_stock_price_data_model["NetBuySellVal"])
                        ),
                        totalTradedVol=int(
                            float(daily_stock_price_data_model["TotalTradedVol"])
                        ),
                        totalTradedValue=int(
                            float(daily_stock_price_data_model["TotalTradedValue"])
                        ),
                    )
                    for daily_stock_price_data_model in daily_stock_price_output_model.data
                ]

                if not self._save_daily_stock_price(daily_stock_price_data_model_list):
                    print(
                        f"\nCannot save daily stock price. Stock: {symbol}. Interval: {start_interval.strftime("%d/%m/%Y")} - {end_interval.strftime("%d/%m/%Y")}."
                    )
                    self._logger.log_error(
                        f"Cannot save daily stock price. Stock: {symbol}. Interval: {start_interval.strftime("%d/%m/%Y")} - {end_interval.strftime("%d/%m/%Y")}."
                    )
                    return False

                self._set_time_series_data_crawl_checkpoint(start_interval, symbol)

            start_interval += CRAWL_DATA_TIME_INTERVAL

        return True

    # endregion

    # region Private methods

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
        time.sleep(COOL_DOWN_BETWEEN_API_CALL)
        return result

    def _get_daily_stock_price(
        self, daily_stock_price_input_model: DailyStockPriceInputModel
    ):
        result = self._client.daily_stock_price(
            self._config, daily_stock_price_input_model
        )
        time.sleep(COOL_DOWN_BETWEEN_API_CALL)
        return result

    def _retrieve_all_market_data(self):
        return self._relational_database_driver.select(
            database_name=RELATIONAL_DATABASE_NAME, table_name="Market"
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
            database_name=RELATIONAL_DATABASE_NAME, table_name="Market"
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

        database_name = RELATIONAL_DATABASE_NAME
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
            database_name=RELATIONAL_DATABASE_NAME, table_name="SecurityType"
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
            database_name=RELATIONAL_DATABASE_NAME, table_name="SecurityType"
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

        database_name = RELATIONAL_DATABASE_NAME
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
        database_name = RELATIONAL_DATABASE_NAME
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
        print(
            f"\nSuccessfully crawl securities data. Total records found: {total_record}"
        )
        self._logger.log_info(
            f"Successfully crawl securities data. Total records found: {total_record}"
        )

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
                database_name=RELATIONAL_DATABASE_NAME,
                table_name="TempSecurity",
                records=security_record_list,
            )

        # Merge temp security table to security table
        self._relational_database_driver.merge(
            database_name=RELATIONAL_DATABASE_NAME,
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
            database_name=RELATIONAL_DATABASE_NAME,
            table_name="Security",
            condition_list=not_delisted_condition,
        )

        return [
            Security(**dict(zip(Security.get_key_list(), row))) for row in security_list
        ]

    def _save_daily_stock_price(
        self, daily_stock_price_data_model_list: List[DailyStockPriceDataModel]
    ) -> bool:
        if not daily_stock_price_data_model_list:
            print('\nInvalid "daily_stock_price_data_model_list". Cannot save data.')
            self._logger.log_error(
                'Invalid "daily_stock_price_data_model_list". Cannot save data.'
            )
            return False

        point_component_list = [
            PointComponent(
                measurement=MEASUREMENT_NAME,
                tags={"symbol": daily_stock_price_data_model.symbol},
                fields={
                    "price_change": daily_stock_price_data_model.priceChange,
                    "per_price_change": daily_stock_price_data_model.perPriceChange,
                    "ceiling_price": daily_stock_price_data_model.ceilingPrice,
                    "floor_price": daily_stock_price_data_model.floorPrice,
                    "ref_price": daily_stock_price_data_model.refPrice,
                    "open_price": daily_stock_price_data_model.openPrice,
                    "highest_price": daily_stock_price_data_model.highestPrice,
                    "lowest_price": daily_stock_price_data_model.lowestPrice,
                    "close_price": daily_stock_price_data_model.closePrice,
                    "average_price": daily_stock_price_data_model.averagePrice,
                    "close_price_adjusted": daily_stock_price_data_model.closePriceAdjusted,
                    "total_match_vol": daily_stock_price_data_model.totalMatchVol,
                    "total_match_val": daily_stock_price_data_model.totalMatchVal,
                    "total_deal_val": daily_stock_price_data_model.totalDealVal,
                    "total_deal_vol": daily_stock_price_data_model.totalDealVol,
                    "foreign_buy_vol_total": daily_stock_price_data_model.foreignBuyVolTotal,
                    "foreign_current_room": daily_stock_price_data_model.foreignCurrentRoom,
                    "foreign_sell_vol_total": daily_stock_price_data_model.foreignSellVolTotal,
                    "foreign_buy_val_total": daily_stock_price_data_model.foreignBuyValTotal,
                    "foreign_sell_val_total": daily_stock_price_data_model.foreignSellValTotal,
                    "total_buy_trade": daily_stock_price_data_model.totalBuyTrade,
                    "total_buy_trade_vol": daily_stock_price_data_model.totalBuyTradeVol,
                    "total_sell_trade": daily_stock_price_data_model.totalSellTrade,
                    "total_sell_trade_vol": daily_stock_price_data_model.totalSellTradeVol,
                    "net_buy_sell_vol": daily_stock_price_data_model.netBuySellVol,
                    "net_buy_sell_val": daily_stock_price_data_model.netBuySellVal,
                    "total_traded_vol": daily_stock_price_data_model.totalTradedVol,
                    "total_traded_value": daily_stock_price_data_model.totalTradedValue,
                },
                time=daily_stock_price_data_model.tradingDate,
            )
            for daily_stock_price_data_model in daily_stock_price_data_model_list
        ]

        write_component = WriteComponent(
            bucket=BUCKET_NAME, point_component_list=point_component_list
        )

        if not self._time_series_database_driver.write(write_component):
            print(f"\nCannot write points to bucket {BUCKET_NAME}.")
            self._logger.log_error(f"Cannot write points to bucket {BUCKET_NAME}.")
            return False

        return True

    def _set_time_series_data_crawl_checkpoint(
        self, start_interval: datetime, symbol: str
    ) -> bool:

        record = Record(
            [
                DataModel(
                    columnName="CurrentStartInterval",
                    value=start_interval,
                    dataType=DataType.DATETIME,
                ),
                DataModel(
                    columnName="CurrentSymbol", value=symbol, dataType=DataType.NVARCHAR
                ),
            ]
        )

        if not self._get_time_series_data_crawl_checkpoint():
            self._relational_database_driver.insert(
                database_name=RELATIONAL_DATABASE_NAME,
                table_name="CrawlCheckpoint",
                records=[record],
            )
        else:
            condition = Condition(
                column="ID", operator=Operator.EQUAL_TO, value=1, dataType=DataType.INT
            )

            self._relational_database_driver.update(
                database_name=RELATIONAL_DATABASE_NAME,
                table_name="CrawlCheckpoint",
                record=record,
                condition_list=[condition],
            )

    def _get_time_series_data_crawl_checkpoint(self) -> CrawlCheckpoint:
        condition = Condition(
            column="ID", operator=Operator.EQUAL_TO, value=1, dataType=DataType.INT
        )

        result = self._relational_database_driver.select(
            database_name=RELATIONAL_DATABASE_NAME,
            table_name="CrawlCheckpoint",
            condition_list=[condition],
        )

        if result:
            return CrawlCheckpoint(
                **dict(zip(CrawlCheckpoint.get_key_list(), result[0]))
            )

        return None

    # endregion
