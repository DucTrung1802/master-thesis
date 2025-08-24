from typing import List
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

from logger.logger import Logger
from models.tabular_database_driver_models.tabular_database_driver_models import (
    Condition,
    JoinModel,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from utils.enums import GenerateDateTimeType

parent_dir = os.path.dirname(os.getcwd())

load_dotenv()

my_logger = Logger(file_name=os.path.join(parent_dir, "logs/visualizer"))


class Visualizer:
    def __init__(self, logger: Logger = my_logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self._connect_to_database()

    def _connect_to_database(self, database_name: str = "postgres") -> None:
        connection_model = PostgreSQLConnectionModel(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=database_name,
        )

        self._database_driver.connect(connection_model)

    def generate_date_time_series(
        self,
        generate_date_time_type: GenerateDateTimeType,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.Series:
        match generate_date_time_type:
            case GenerateDateTimeType.YEAR:
                dates = pd.date_range(start=start_date, end=end_date, freq="YE")
            case GenerateDateTimeType.QUARTER:
                dates = pd.date_range(start=start_date, end=end_date, freq="QE")
            case GenerateDateTimeType.MONTH:
                dates = pd.date_range(start=start_date, end=end_date, freq="ME")
            case GenerateDateTimeType.DAY:
                dates = pd.date_range(start=start_date, end=end_date, freq="D")
            case _:
                raise ValueError(f"Unsupported type: {generate_date_time_type}")

        return pd.Series(dates)

    def select(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str] = None,
        join_model: JoinModel = None,
        conditions: List[Condition] = None,
        order_by: List[str] = None,
        limit: int = None,
    ) -> pd.DataFrame:
        return self._database_driver.select(
            schema_name=schema_name, table_name=table_name
        )
