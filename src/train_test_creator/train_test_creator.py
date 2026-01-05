from dotenv import load_dotenv
import os
import pandas as pd
from math import ceil

from feature_selector.feature_selector import FeatureSelector
from logger.logger import Logger
from train_test_creator.train_test_set import TrainTestSet
from utils.constants import *
from utils.utils import *


load_dotenv()


class TrainTestCreator:
    def __init__(self, logger: Logger):
        self._logger = logger

        self._full_train_df = None
        self._full_test_df = None

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
