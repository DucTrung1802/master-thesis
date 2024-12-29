import datetime
import os
from stock_price_predictor.logger.logger import *
from stock_price_predictor.sql_server_driver.sql_server_driver import *


def main():
    logger = Logger(file_name="stock_price_predictor", level=LogType.DEBUG)

    sql_server_authentication = SqlServerAuthentication(
        server=os.getenv("sql_server_server"),
        login=os.getenv("sql_server_login"),
        password=os.getenv("sql_server_password"),
    )
    sql_server_driver = SqlServerDriver(logger, sql_server_authentication)

    sql_server_driver.create_new_database("SSI_STOCKS")

    market_table_columns: List[Column] = [
        Column(columnName="ID", dataType=DataType.INT(), nullable=False),
        Column(columnName="Symbol", dataType=DataType.NVARCHAR(5), nullable=False),
        Column(columnName="Name", dataType=DataType.NVARCHAR(200), nullable=False),
        Column(columnName="EnName", dataType=DataType.NVARCHAR(200), nullable=False),
        Column(columnName="CreateDate", dataType=DataType.DATETIME(), nullable=False),
        Column(columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True),
        Column(columnName="DeleteDate", dataType=DataType.DATETIME(), nullable=True),
    ]

    sql_server_driver.create_table(
        database_name="SSI_STOCKS",
        table_name="Market",
        columns=market_table_columns,
        key_column_name="ID",
    )

    security_type_table_columns: List[Column] = [
        Column(columnName="ID", dataType=DataType.INT(), nullable=False),
        Column(columnName="Symbol", dataType=DataType.NVARCHAR(2), nullable=False),
        Column(columnName="Name", dataType=DataType.NVARCHAR(30), nullable=False),
        Column(columnName="CreateDate", dataType=DataType.DATETIME(), nullable=False),
        Column(columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True),
        Column(columnName="DeleteDate", dataType=DataType.DATETIME(), nullable=True),
    ]

    sql_server_driver.create_table(
        database_name="SSI_STOCKS",
        table_name="SecurityType",
        columns=security_type_table_columns,
        key_column_name="ID",
    )

    security_table_columns: List[Column] = [
        Column(columnName="ID", dataType=DataType.INT(), nullable=False),
        Column(columnName="Symbol", dataType=DataType.NVARCHAR(12), nullable=False),
        Column(columnName="Name", dataType=DataType.NVARCHAR(200), nullable=False),
        Column(columnName="EnName", dataType=DataType.NVARCHAR(200), nullable=False),
        Column(columnName="ListedShare", dataType=DataType.BIGINT(), nullable=False),
        Column(
            columnName="MarketCapitalization",
            dataType=DataType.BIGINT(),
            nullable=False,
        ),
        Column(columnName="Market_ID", dataType=DataType.INT(), nullable=False),
        Column(columnName="SecurityType_ID", dataType=DataType.INT(), nullable=False),
        Column(columnName="CreateDate", dataType=DataType.DATETIME(), nullable=False),
        Column(columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True),
        Column(columnName="DeleteDate", dataType=DataType.DATETIME(), nullable=True),
    ]

    security_table_foreign_keys: List[ForeignKey] = [
        ForeignKey(name="Market_ID", tableToRefer="Market", columnToRefer="ID"),
        ForeignKey(
            name="SecurityType_ID", tableToRefer="SecurityType", columnToRefer="ID"
        ),
    ]

    sql_server_driver.create_table(
        database_name="SSI_STOCKS",
        table_name="Security",
        columns=security_table_columns,
        key_column_name="ID",
        foreign_keys=security_table_foreign_keys,
    )

    merket_data_models: List[DataModel] = [
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
            value=datetime.datetime.now().replace(microsecond=0),
            dataType=DataType.DATETIME,
        ),
    ]

    sql_server_driver.insert_data(
        database_name="SSI_STOCKS",
        table_name="Market",
        data_models=merket_data_models,
    )

    sql_server_driver.close_connection()


if __name__ == "__main__":
    main()
