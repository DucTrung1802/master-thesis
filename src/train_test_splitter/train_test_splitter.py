from dotenv import load_dotenv

import pandas as pd
from typing import List, Tuple, Union

from dtos.train_test_splitter_dtos.sliding_window import SlidingWindow
from logger.logger import Logger
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Condition,
    JoinModel,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from utils.constants import *
from utils.utils import *

load_dotenv()


class TraninTestSplitter:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self.connect_to_database()

    def connect_to_database(self, database_name: str = "postgres") -> None:
        connection_model = PostgreSQLConnectionDto(
            logger=self._logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=database_name,
        )
        return self._database_driver.connect(connection_model)

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
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            join_model=join_model,
            conditions=conditions,
            order_by=order_by,
            limit=limit,
        )

    def split_train_test(
        self, df: pd.DataFrame, test_size: Union[float, int]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split DataFrame into train and test sets.
        """
        if isinstance(test_size, float) and 0 < test_size < 1:
            test_count = int(len(df) * test_size)
        elif isinstance(test_size, int) and test_size >= 1:
            test_count = min(test_size, len(df))
        else:
            self._logger.log_error(
                "test_size must be a float (0 < test_size < 1) or int (test_size >= 1)."
            )

        split_index = len(df) - test_count

        train_set = df.iloc[:split_index]
        test_set = df.iloc[split_index:]

        self._logger.log_info(
            f"Train set rows: {len(train_set)}, Test set rows: {len(test_set)}"
        )

        return train_set, test_set

    def create_sliding_window_list(
        self,
        df: pd.DataFrame,
        input_window_length: int,
        forecast_horizon_length: int,
        step_size: int,
        time_base_column_name: str,
    ) -> List[SlidingWindow]:
        # --- Input validation ---
        if not isinstance(df, pd.DataFrame):
            self._logger.log_error("df must be a pandas DataFrame.")
        if df.empty:
            self._logger.log_error("df must not be empty.")
        if not isinstance(input_window_length, int) or input_window_length <= 0:
            self._logger.log_error("input_window_length must be a positive integer.")
        if not isinstance(forecast_horizon_length, int) or forecast_horizon_length <= 0:
            self._logger.log_error(
                "forecast_horizon_length must be a positive integer."
            )
        if not isinstance(step_size, int) or step_size <= 0:
            self._logger.log_error("step_size must be a positive integer.")
        if not isinstance(time_base_column_name, str):
            self._logger.log_error("time_base_column_name must be a string.")
        if time_base_column_name not in df.columns:
            self._logger.log_error(
                f"Column '{time_base_column_name}' not found in DataFrame."
            )
        if len(df) < input_window_length + forecast_horizon_length:
            self._logger.log_error(
                "DataFrame is too short for the given input_window_length and forecast_horizon_length."
            )

        # --- Create sliding windows ---
        sliding_windows = []
        max_start = len(df) - (input_window_length + forecast_horizon_length) + 1

        for start in range(0, max_start, step_size):
            window_df = df.iloc[
                start : start + input_window_length + forecast_horizon_length
            ].copy()

            sliding_windows.append(
                SlidingWindow(
                    df=window_df,
                    time_base_column_name=time_base_column_name,
                    input_window_length=input_window_length,
                    forecast_horizon_length=forecast_horizon_length,
                )
            )

        self._logger.log_info(
            f"Created {len(sliding_windows)} sliding windows with: input_window_length={input_window_length}, forecast_horizon_length={forecast_horizon_length}, step_size={step_size}."
        )

        return sliding_windows

    def test(self):
        data = {
            "id": range(1, 11),
            "feature": [x * 2 for x in range(1, 11)],
            "label": ["A" if x % 2 == 0 else "B" for x in range(1, 11)],
        }
        df = pd.DataFrame(data)
        print("Original DataFrame:")
        print(df)

        # Example 1: test_size as ratio
        train_df, test_df = self.split_train_test(df, 0.3)
        print("\n--- Split with test_size=0.3 (30% test) ---")
        print("Train:")
        print(train_df)
        print("Test:")
        print(test_df)

        sliding_window_list = self.create_sliding_window_list(
            df=train_df,
            input_window_length=3,
            forecast_horizon_length=1,
            step_size=1,
            time_base_column_name="id",
        )

        print("\n--- Sliding Windows ---")
        for i, sw in enumerate(sliding_window_list, start=1):
            print(f"\nSliding Window {i}:")
            print("Input Window:")
            print(sw.input_window)
            print("Forecast Window:")
            print(sw.forecast_window)
