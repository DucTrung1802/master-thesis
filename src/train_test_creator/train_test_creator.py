from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
from functools import reduce
from math import ceil

from feature_selector.feature_selector import FeatureSelector
from logger.logger import Logger
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Column,
    Condition,
    DataModel,
    ForeignKey,
    JoinModel,
    Record,
)
from ta.ta_functions import add_one_for_all_ta
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

        self._full_train_df = None
        self._full_test_df = None

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
            f"Saved {inserted_count + updated_count}/{len(df)} records with {len(column_names)} columns into table '{schema_name}.{table_name}'."
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
            self._logger.log_info("\n\nMACROECONOMIC Column data types:\n")
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
            self._logger.log_info("\n\nSTOCK MARKET Column data types:\n")
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

    def export_unified_dataframe(self, stock_code: str) -> pd.DataFrame:
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

            # Expand "date" column
            unified_df = expand_date_column(unified_df)

            # Add technical analysis columns
            unified_df = add_one_for_all_ta(unified_df)
            self._logger.log_info(
                f"Unified dataframe after TA has {len(unified_df)} rows and {len(unified_df.columns)} columns."
            )

            unified_df = unified_df.dropna()
            self._logger.log_info(
                f"Unified dataframe after dropping NA has {len(unified_df)} rows and {len(unified_df.columns)} columns."
            )

            # Export to dataframe
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

            # Export to file
            os.makedirs(UNIFIED_DATAFRAME_DIR, exist_ok=True)
            file_path = os.path.join(UNIFIED_DATAFRAME_DIR, f"unified_{stock_code}.csv")
            unified_df.to_csv(file_path, index=False)
            self._logger.log_info(
                f"Exported unified dataframe for stock code '{stock_code}' to file: {file_path}"
            )

        except Exception as e:
            self._logger.log_error(
                f"Error fetching data for stock code {stock_code}: {e}"
            )
            return None

    def load_dataframe(self, stock_code: str, file_path: str = None) -> pd.DataFrame:
        if not stock_code:
            raise ValueError("Stock code must be provided.")

        stock_code = stock_code.lower()

        if not file_path:
            file_path = os.path.join(UNIFIED_DATAFRAME_DIR, f"unified_{stock_code}.csv")

        dataframe = pd.read_csv(file_path)
        self._logger.log_info(
            f"Loaded unified dataframe for stock code '{stock_code}' from file with {len(dataframe)} rows and {len(dataframe.columns)} columns."
        )
        return dataframe

    def normalize_unified_dataframe(
        self,
        dataframe: pd.DataFrame,
        output_range: tuple,
        output_column: str = "close",
    ) -> pd.DataFrame:
        df = dataframe.copy()

        # Move the output column to the end
        df = df[[col for col in df.columns if col != output_column] + [output_column]]

        df.drop(columns=["date"], inplace=True)

        # Identify numeric columns (floats or ints) except "date"
        numeric_cols = df.select_dtypes(include=["float", "int"]).columns.difference(
            [output_column]
        )

        # Remove columns with constant values (min == max)
        constant_cols = [
            col
            for col in numeric_cols
            if df[col].nunique() == 1 and df[col].min() == df[col].max()
        ]
        df.drop(columns=constant_cols, inplace=True)

        numeric_cols = [col for col in numeric_cols if col not in constant_cols]

        # Apply min-max normalization
        df[numeric_cols] = (df[numeric_cols] - df[numeric_cols].min()) / (
            df[numeric_cols].max() - df[numeric_cols].min()
        )

        # Scale to output range
        min_range, max_range = output_range
        df[output_column] = (df[output_column] - min_range) / (max_range - min_range)

        return df

    def _validate_input(
        self,
        dataframe: pd.DataFrame,
        output_column: str,
        output_range: tuple,
        stock_code: str,
        train_window_size: int,
        validation_window_size: int,
        test_window_size: int,
        train_ratio: float = DEFAULT_TRAIN_RATIO,
    ) -> bool:
        """
        Validate input parameters for stock forecasting.

        Validation Rules:
        1. len(dataframe) must be greater than the sum of
        train_window_size + validation_window_size + test_window_size.
        2. output_column must exist in dataframe.columns.
        3. stock_code must be a non-empty string after stripping whitespace.
        4. train_window_size, validation_window_size, and test_window_size must:
        - Be integers (`int` type)
        - Be greater than 0
        5. train_ratio must be a float greater than 0 and less than 1.

        Parameters
        ----------
        dataframe : pd.DataFrame
            Input data containing stock features and target column.
        output_column : str
            The column name in `dataframe` to be predicted.
        output_range : tuple
            Expected range of output values (used for scaling/validation).
        stock_code : str
            The identifier for the stock being analyzed.
        train_window_size : int
            Number of time steps used for training input.
        validation_window_size : int
            Number of time steps used for validation.
        test_window_size : int
            Number of time steps to forecast ahead (testing).
        train_ratio : float, optional
            Ratio of training data (default: DEFAULT_TRAIN_RATIO).

        Returns
        -------
        bool
            True if all validation rules pass, otherwise raises a ValueError.

        Raises
        ------
        ValueError
            If any validation rule is violated.
        """

        # 1. Check dataframe length
        total_required = train_window_size + validation_window_size + test_window_size
        if len(dataframe) <= total_required:
            raise ValueError(
                f"Dataframe length ({len(dataframe)}) must be greater than "
                f"train_window_size + validation_window_size + test_window_size "
                f"({total_required})."
            )

        # 2. Check output column existence
        if output_column not in dataframe.columns:
            raise ValueError(
                f"Output column '{output_column}' not found in dataframe columns: {list(dataframe.columns)}."
            )

        # 3. Check stock_code validity
        if not isinstance(stock_code, str) or not stock_code.strip():
            raise ValueError(
                "Stock code must be a non-empty string after stripping whitespace."
            )

        # 4. Check each window size
        for name, value in {
            "train_window_size": train_window_size,
            "validation_window_size": validation_window_size,
            "test_window_size": test_window_size,
        }.items():
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer (got {value!r}).")

        # 5. Check train_ratio
        if not isinstance(train_ratio, (float, int)) or not (0 < train_ratio < 1):
            raise ValueError(
                f"train_ratio must be between 0 and 1 (got {train_ratio})."
            )

        return True

    def _find_split_index(
        self,
        dataframe: pd.DataFrame,
        train_window_size: int,
        validation_window_size: int,
        test_window_size: int,
        train_ratio: float,
    ) -> int:
        """
        Determine the index to split the dataframe into training and testing portions
        for time series forecasting with explicit validation and test windows.

        Rules:
        1. Split based on `train_ratio`.
        2. Ensure there's enough data left for both validation and test windows.
        3. Ensure the split index aligns cleanly with test window size for consistency.
        """

        # --- Sort chronologically if 'date' column exists ---
        if "date" in dataframe.columns:
            dataframe = dataframe.sort_values("date").reset_index(drop=True)
        else:
            dataframe = dataframe.reset_index(drop=True)

        total_len = len(dataframe)
        min_required = train_window_size + validation_window_size + test_window_size

        if total_len <= min_required:
            raise ValueError(
                f"Data length ({total_len}) must exceed total required window size "
                f"({min_required})."
            )

        # --- Initial rough split ---
        split_index = ceil(total_len * train_ratio)

        # --- Adjust to ensure validation & test windows fit after split ---
        split_index = min(
            split_index, total_len - (validation_window_size + test_window_size)
        )

        # --- Align to multiple of test_window_size (optional smoothing) ---
        remainder = split_index % test_window_size
        if remainder != 0:
            split_index -= remainder

        # --- Guard against invalid split ---
        if split_index <= train_window_size or split_index >= total_len:
            raise ValueError(
                f"Adjusted split_index ({split_index}) invalid for data length {total_len}"
            )

        return split_index

    def create_train_test_set(
        self,
        dataframe: pd.DataFrame,
        output_column: str,
        output_range: tuple,
        stock_code: str,
        train_window_size: int,
        validation_window_size: int,
        test_window_size: int,
        train_ratio: float = DEFAULT_TRAIN_RATIO,
    ) -> "TrainTestSet":
        """
        Create train, validation, and test rolling windows for time series forecasting.
        """

        # --- Validate input ---
        if not self._validate_input(
            dataframe=dataframe,
            output_column=output_column,
            output_range=output_range,
            stock_code=stock_code,
            train_window_size=train_window_size,
            validation_window_size=validation_window_size,
            test_window_size=test_window_size,
            train_ratio=train_ratio,
        ):
            raise ValueError("Invalid input parameters for creating TrainTestSet.")

        stock_code = str.lower(stock_code)

        # Ensure output column is last
        dataframe = move_column_to_end(dataframe, output_column)

        # --- Split train/test base ---
        split_index = self._find_split_index(
            dataframe=dataframe,
            train_window_size=train_window_size,
            validation_window_size=validation_window_size,
            test_window_size=test_window_size,
            train_ratio=train_ratio,
        )

        self._full_train_df = dataframe.iloc[:split_index].reset_index(drop=True)
        self._full_test_df = dataframe.iloc[split_index:].reset_index(drop=True)

        # --- Feature selection ---
        feature_selector_df = self._full_train_df.drop(columns=["date"])
        feature_columns = feature_selector_df.columns.tolist()
        feature_columns.remove(output_column)

        self._feature_selector = FeatureSelector(
            logger=self._logger,
            stock_code=stock_code,
            dataframe=feature_selector_df,
            feature_columns=feature_columns,
            target_column=output_column,
        )
        features_to_drop = self._feature_selector.get_features_to_drop()

        self._selected_full_train_df = self._full_train_df.drop(
            columns=features_to_drop
        )

        # --- Normalize ---
        self._normalized_selected_full_train_df = self.normalize_unified_dataframe(
            dataframe=self._selected_full_train_df,
            output_range=output_range,
            output_column=output_column,
        )

        final_train_df = self._normalized_selected_full_train_df

        # --- Create rolling windows ---
        train_sets = []
        total_window_size = (
            train_window_size + validation_window_size + test_window_size
        )

        for start_idx in range(
            0,
            len(final_train_df) - total_window_size + 1,
            test_window_size,  # shift by test window each time
        ):
            end_idx = start_idx + total_window_size
            train_df = final_train_df.iloc[start_idx:end_idx].reset_index(drop=True)
            train_sets.append(train_df)

        # --- Create test set from unseen data ---
        test_sets = [
            self._full_test_df.iloc[i : i + test_window_size].reset_index(drop=True)
            for i in range(
                0,
                len(self._full_test_df) - test_window_size + 1,
                test_window_size,
            )
        ]

        # --- Return TrainTestSet object ---
        return TrainTestSet(
            name=f"{stock_code}_{train_window_size}_{validation_window_size}_{test_window_size}",
            train_sets=train_sets,
            test_sets=test_sets,
            output_column=output_column,
            output_range=output_range,
            train_window_size=train_window_size,
            validation_window_size=validation_window_size,
            test_window_size=test_window_size,
        )
