from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
from functools import reduce
from math import ceil

from logger.logger import Logger
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Column,
    Condition,
    DataModel,
    ForeignKey,
    JoinModel,
    Record,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from train_test_creator.train_test_set import TrainTestSet
from utils.constants import *
from utils.utils import *


load_dotenv()


class TrainTestCreator:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._database_driver = PostgreSQLDriver(logger=logger)
        self.connect_to_database(database_name=os.getenv("GOLD_POSTGRES_DATABASE"))

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

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: List[Column],
        primary_keys: List[str],
        foreign_keys: List[ForeignKey] = None,
    ):
        return self._database_driver.create_table(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
        )

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

    def _save_pandas_table_to_database(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: List[str],
        df: pd.DataFrame,
    ) -> None:
        self._logger.log_info(
            f'Saving dataframe to table "{schema_name}.{table_name}".'
        )

        # Drop rows where all values are NaN
        df = df.dropna(how="all")

        if df.empty:
            self._logger.log_info("DataFrame is empty after cleaning. Nothing to save.")
            return

        # Convert entire DataFrame into a list of Records (vectorized)
        column_names = list(df.columns)
        records = []

        for row in df.itertuples(index=False, name=None):
            data_dto_list = [
                DataModel(column_name=col, value=(val if pd.notna(val) else None))
                for col, val in zip(column_names, row)
            ]
            records.append(Record(data_dto_list=data_dto_list))

        # Batch upsert once
        result = self._database_driver.upsert(
            schema_name=schema_name,
            table_name=table_name,
            records=records,
            primary_keys=primary_keys,
        )

        if result[0] == DatabaseExecutionStatus.SUCCESS:
            inserted_count = result[1]
            updated_count = result[2]
        else:
            inserted_count = updated_count = 0

        self._logger.log_info(
            f"Saved {inserted_count + updated_count}/{len(df)} records into table '{schema_name}.{table_name}'."
            f" (Inserted: {inserted_count}, Updated: {updated_count}) successfully."
        )

    def extract_unified_macroeconomic_dataframe(self) -> pd.DataFrame:
        macroeconomics_gold_table_enums = {
            name: cls
            for name, cls in vars(Table).items()
            if isinstance(cls, type) and name.startswith("G_")
        }

        macroeconomics_gold_table_names = [
            cls.name for cls in macroeconomics_gold_table_enums.values()
        ]

        self._logger.log_info(
            f"Selecting MACROECONOMIC data from tables: {macroeconomics_gold_table_names}"
        )

        macroeconomics_df_list = []
        for table in macroeconomics_gold_table_names:
            df = self.select(schema_name="macroeconomics", table_name=table)
            self._logger.log_info(f"Selected {len(df)} rows from table: '{table}'")

            # ✅ Rename all columns except 'date' to include table name
            df = df.rename(
                columns={col: f"{table}_{col}" for col in df.columns if col != "date"}
            )
            macroeconomics_df_list.append(df)

        # ✅ Merge on 'date'
        if macroeconomics_df_list:
            macroeconomics_df = reduce(
                lambda left, right: pd.merge(left, right, on="date", how="outer"),
                macroeconomics_df_list,
            )
            self._logger.log_info(
                f"Original 'macroeconomics_df' has {len(macroeconomics_df)} rows and {len(macroeconomics_df.columns)} columns."
            )

            # Try casting each column to Float64 (skip if not possible)
            for col in macroeconomics_df.columns:
                try:
                    macroeconomics_df[col] = macroeconomics_df[col].astype("Float64")
                except Exception:
                    # Skip columns that can't be converted to Float64
                    pass

            # Drop existng unified table if any
            self._database_driver.drop_table(
                schema_name=Schema.MACROECONOMICS.value,
                table_name=Table.UNIFIED_MACROECONOMIC.name,
            )

            # Drop lack data columns
            data_lack_threshold = 0.22
            columns_to_drop = [
                col
                for col in macroeconomics_df.columns
                if col != "date"
                and macroeconomics_df[col].isna().sum() / len(macroeconomics_df)
                > data_lack_threshold
            ]

            self._logger.log_info(
                f"Dropping {len(columns_to_drop)} columns with more than {data_lack_threshold * 100}% missing data: {columns_to_drop}"
            )
            macroeconomics_df = macroeconomics_df.drop(columns=columns_to_drop)

            # Cutoff according to available date range
            start_date = TRAIN_TEST_CREATOR_START_DATE
            end_date = TRAIN_TEST_CREATOR_END_DATE

            self._logger.log_info(
                f"Filtering 'macroeconomics_df' for dates between {start_date.date()} and {end_date.date()}."
            )
            macroeconomics_df = macroeconomics_df[
                (macroeconomics_df["date"] >= start_date.date())
                & (macroeconomics_df["date"] <= end_date.date())
            ]

            self._logger.log_info(
                f"Filtered 'macroeconomics_df' has {len(macroeconomics_df)} rows and {len(macroeconomics_df.columns)} columns after filter."
            )

            # Print each column name and its pandas dtype
            self._logger.log_info("\nMACROECONOMIC Column data types:\n")
            for col in macroeconomics_df.columns:
                self._logger.log_info(f"{col:<60} → {macroeconomics_df[col].dtype}")

            return macroeconomics_df

        else:
            macroeconomics_df = pd.DataFrame()
            self._logger.log_error("No macroeconomics tables found to merge.")
            raise ValueError("No macroeconomics tables found to merge.")

    def extract_unified_stock_market_dataframe(self) -> pd.DataFrame:
        stock_market_gold_table_names = [
            Table.HNX_30_INDEX.name,
            Table.HNX_INDEX.name,
            Table.UPCOM_INDEX.name,
            Table.VN_30_INDEX.name,
            Table.VN_100_INDEX.name,
            Table.VN_INDEX.name,
        ]

        self._logger.log_info(
            f"Selecting STOCK MARKET data from tables: {stock_market_gold_table_names}"
        )

        stock_market_df_list = []
        for table in stock_market_gold_table_names:
            df = self.select(schema_name="stock_market", table_name=table)
            self._logger.log_info(f"Selected {len(df)} rows from table: '{table}'")

            # ✅ Rename all columns except 'date' to include table name
            df = df.rename(
                columns={col: f"{table}_{col}" for col in df.columns if col != "date"}
            )
            stock_market_df_list.append(df)

        # ✅ Merge on 'date'
        if stock_market_df_list:
            stock_market_df = reduce(
                lambda left, right: pd.merge(left, right, on="date", how="outer"),
                stock_market_df_list,
            )
            self._logger.log_info(
                f"Original 'stock_market_df' has {len(stock_market_df)} rows and {len(stock_market_df.columns)} columns."
            )

            # Try casting each column to Float64 (skip if not possible)
            for col in stock_market_df.columns:
                try:
                    stock_market_df[col] = stock_market_df[col].astype("Float64")
                except Exception:
                    # Skip columns that can't be converted to Float64
                    pass

            # Drop existng unified table if any
            self._database_driver.drop_table(
                schema_name=Schema.STOCK_MARKET.value,
                table_name=Table.UNIFIED_STOCK_MARKET.name,
            )

            # Cutoff according to available date range
            start_date = TRAIN_TEST_CREATOR_START_DATE
            end_date = TRAIN_TEST_CREATOR_END_DATE

            self._logger.log_info(
                f"Filtering 'stock_market_df' for dates between {start_date.date()} and {end_date.date()}."
            )
            stock_market_df = stock_market_df[
                (stock_market_df["date"] >= start_date.date())
                & (stock_market_df["date"] <= end_date.date())
            ]

            self._logger.log_info(
                f"Filtered 'stock_market_df' has {len(stock_market_df)} rows and {len(stock_market_df.columns)} columns after filter."
            )

            # Print each column name and its pandas dtype
            self._logger.log_info("\nSTOCK MARKET Column data types:\n")
            for col in stock_market_df.columns:
                self._logger.log_info(f"{col:<60} → {stock_market_df[col].dtype}")

            return stock_market_df

        else:
            stock_market_df = pd.DataFrame()
            self._logger.log_error("No stock market tables found to merge.")
            raise ValueError("No stock market tables found to merge.")

    def export_common_dataframe_to_db(self) -> pd.DataFrame:
        # UNIFIED MACROECONOMIC DF
        unified_macroeconomic_df = self.extract_unified_macroeconomic_dataframe()

        self.create_table(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.UNIFIED_MACROECONOMIC.name,
            columns=[
                Column(name="date", data_type="DATE", nullable=False),
            ]
            + [
                Column(name=col, data_type=DataType.DECIMAL(), nullable=True)
                for col in unified_macroeconomic_df.columns
                if col != "date"
            ],
            primary_keys=["date"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.MACROECONOMICS.value,
            table_name=Table.UNIFIED_MACROECONOMIC.name,
            primary_keys=Table.UNIFIED_MACROECONOMIC.primary_key,
            df=unified_macroeconomic_df,
        )

        # UNIFIED STOCK MARKET DF
        unified_stock_market_df = self.extract_unified_stock_market_dataframe()

        self.create_table(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UNIFIED_STOCK_MARKET.name,
            columns=[
                Column(name="date", data_type="DATE", nullable=False),
            ]
            + [
                Column(name=col, data_type=DataType.DECIMAL(), nullable=True)
                for col in unified_stock_market_df.columns
                if col != "date"
            ],
            primary_keys=["date"],
        )

        self._save_pandas_table_to_database(
            schema_name=Schema.STOCK_MARKET.value,
            table_name=Table.UNIFIED_STOCK_MARKET.name,
            primary_keys=Table.UNIFIED_STOCK_MARKET.primary_key,
            df=unified_stock_market_df,
        )

    def create_unified_dataframe(self, stock_code: str) -> pd.DataFrame:
        self.connect_to_database(database_name=os.getenv("GOLD_POSTGRES_DATABASE"))
        try:
            if not stock_code:
                raise ValueError("Stock code must be provided.")

            stock_code = stock_code.lower()

            # Load all three datasets
            stock_code_df = self.select(
                schema_name=Schema.ENTERPRISE.value, table_name=stock_code
            )
            stock_code_df = stock_code_df.drop(columns=["code"])

            if len(stock_code_df) == 0:
                self._logger.log_error(f"No data found for stock code: {stock_code}")
                raise ValueError(f"No data found for stock code: {stock_code}")

            # Drop existng unified table if any
            self._database_driver.drop_table(
                schema_name=Schema.ENTERPRISE.value,
                table_name=f"unified_{stock_code}",  # Template name in database
            )

            unified_macro_df = self.select(
                schema_name=Schema.MACROECONOMICS.value,
                table_name=Table.UNIFIED_MACROECONOMIC.name,
            )
            unified_stock_df = self.select(
                schema_name=Schema.STOCK_MARKET.value,
                table_name=Table.UNIFIED_STOCK_MARKET.name,
            )

            # Join all at once
            unified_df = unified_macro_df.merge(
                unified_stock_df, on="date", how="inner"
            ).merge(stock_code_df, on="date", how="inner")

            self._logger.log_info(
                f"Original 'unified_df' has {len(unified_df)} rows and {len(unified_df.columns)} columns."
            )

            # Drop lack data columns
            data_lack_threshold = 0.01
            columns_to_drop = [
                col
                for col in unified_df.columns
                if col != "date"
                and unified_df[col].isna().sum() / len(unified_df) > data_lack_threshold
            ]

            columns_to_drop.append("market_id")

            self._logger.log_info(
                f"Dropping {len(columns_to_drop)} columns with more than {data_lack_threshold * 100}% missing data: {columns_to_drop}"
            )
            unified_df = unified_df.drop(columns=columns_to_drop)
            self._logger.log_info(
                f"Filtered 'stock_market_df' has {len(unified_df)} rows and {len(unified_df.columns)} columns after filter."
            )

            self.create_table(
                schema_name=Schema.ENTERPRISE.value,
                table_name=f"unified_{stock_code}",
                columns=[
                    Column(name="date", data_type="DATE", nullable=False),
                ]
                + [
                    Column(name=col, data_type=DataType.DECIMAL(), nullable=True)
                    for col in unified_df.columns
                    if col != "date"
                ],
                primary_keys=["date"],
            )

            self._save_pandas_table_to_database(
                schema_name=Schema.ENTERPRISE.value,
                table_name=f"unified_{stock_code}",
                primary_keys=["date"],
                df=unified_df,
            )

        except Exception as e:
            self._logger.log_error(
                f"Error fetching data for stock code {stock_code}: {e}"
            )
            return None

    def create_train_test_set(
        self,
        stock_code: str,
        input_window_size: int,
        forecast_horizon_size: int,
        train_ratio: float = DEFAULT_TRAIN_RATIO,
    ) -> "TrainTestSet":
        # Validate inputs
        if not (0 < train_ratio < 1):
            raise ValueError("train_ratio must be between 0 and 1.")
        if input_window_size <= 0 or forecast_horizon_size <= 0:
            raise ValueError(
                "input_window_size and forecast_horizon_size must be positive integers."
            )

        # Fetch unified dataframe
        self.connect_to_database(database_name=os.getenv("GOLD_POSTGRES_DATABASE"))
        unified_df = self.select(
            schema_name=Schema.ENTERPRISE.value,
            table_name=f"unified_{stock_code.lower()}",
        )

        if unified_df.empty:
            raise ValueError(f"No data found for stock_code: {stock_code}")

        # Sort by date or time index if available
        if "date" in unified_df.columns:
            unified_df = unified_df.sort_values("date").reset_index(drop=True)
        else:
            unified_df = unified_df.reset_index(drop=True)

        # --- Adjusted split index logic ---
        split_index = ceil(len(unified_df) * train_ratio)
        split_index -= input_window_size  # ensure full input window fits

        # Make split_index divisible by forecast_horizon_size
        remainder = split_index % forecast_horizon_size
        if remainder != 0:
            split_index -= remainder

        # Guard against invalid split index
        if split_index <= 0 or split_index >= len(unified_df):
            raise ValueError(
                f"Adjusted split_index ({split_index}) is invalid for data length {len(unified_df)}"
            )

        # Add back input windwo size to split_index
        split_index += input_window_size

        # --- Split train/test ---
        full_train_df = unified_df.iloc[:split_index].reset_index(drop=True)
        test_set = unified_df.iloc[split_index:].reset_index(drop=True)

        # --- Create sliding windows on training set ---
        train_sets = []
        total_window_size = input_window_size + forecast_horizon_size

        for start_idx in range(
            0, len(full_train_df) - total_window_size + 1, forecast_horizon_size
        ):
            end_idx = start_idx + total_window_size
            window_df = full_train_df.iloc[start_idx:end_idx].reset_index(drop=True)
            train_sets.append(window_df)

        # --- Return TrainTestSet object ---
        return TrainTestSet(
            name=f"{stock_code}_{input_window_size}_{forecast_horizon_size}",
            train_set=train_sets,
            test_set=test_set,
            input_window_size=input_window_size,
            forecast_horizon_size=forecast_horizon_size,
        )
