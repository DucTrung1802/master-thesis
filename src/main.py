from utils.constants import LOGGER_LOG_FILE_NAME
from logger.logger import Logger
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import (
    Column,
    DataType,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver


def main():
    my_logger = Logger(file_name=LOGGER_LOG_FILE_NAME)
    my_sql_driver = PostgreSQLDriver(logger=my_logger)
    my_connection_model = PostgreSQLConnectionModel(
        logger=my_logger,
        host="localhost",
        user="postgres",
        password="changeme",
    )
    my_sql_driver.connect(connection_model=my_connection_model)
    my_sql_driver.create_database(database_name="test_db")
    my_sql_driver.create_schema(schema_name="test_schema")
    my_sql_driver.create_table(
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            Column(name="id", data_type=DataType.INT(), nullable=False),
            Column(name="name", data_type=DataType.VARCHAR(30), nullable=False),
            Column(name="age", data_type=DataType.INT(), nullable=False),
        ],
        key_column_name="id",
    )
    my_sql_driver.drop_table(schema_name="test_schema", table_name="test_table")
    my_sql_driver.drop_schema(schema_name="test_schema")
    my_sql_driver.drop_database(database_name="test_db")
    my_sql_driver.disconnect()


if __name__ == "__main__":
    main()
