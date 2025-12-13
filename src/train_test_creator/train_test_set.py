import pandas as pd
from typing import List, Tuple

from utils.constants import *
from utils.utils import *
from sklearn.preprocessing import MinMaxScaler


class TrainTestSet:
    def __init__(
        self,
        name: str,
        train_set: pd.DataFrame,
        val_set: pd.DataFrame,
        test_set: pd.DataFrame,
        output_column: str,
        input_size: int,
        forecast_size: int,
        train_windows: List[pd.DataFrame],
        norm_train_set: pd.DataFrame,
        norm_val_set: pd.DataFrame,
        norm_test_set: pd.DataFrame,
        feature_scaler: MinMaxScaler,
        target_scaler: MinMaxScaler,
        numeric_feature_cols: List[str],
    ):
        if not self._validate_input(
            name=name,
            train_set=train_set,
            val_set=val_set,
            test_set=test_set,
            norm_train_set=norm_train_set,
            norm_val_set=norm_val_set,
            norm_test_set=norm_test_set,
            output_column=output_column,
            input_size=input_size,
            forecast_size=forecast_size,
            train_windows=train_windows,
            feature_scaler=feature_scaler,
            target_scaler=target_scaler,
            numeric_feature_cols=numeric_feature_cols,
        ):
            return

        # --- Metadata ---
        self.name: str = name
        self.input_size: int = input_size
        self.forecast_size: int = forecast_size
        self.output_column: str = output_column

        # --- Data ---
        self.train_set: pd.DataFrame = train_set
        self.val_set: pd.DataFrame = val_set
        self.test_set: pd.DataFrame = test_set

        # --- Normalized Data ---
        self.norm_train_set: pd.DataFrame = norm_train_set
        self.norm_val_set: pd.DataFrame = norm_val_set
        self.norm_test_set: pd.DataFrame = norm_test_set

        # --- Scalers ---
        self.feature_scaler: MinMaxScaler = feature_scaler
        self.target_scaler: MinMaxScaler = target_scaler
        self.numeric_feature_cols: List[str] = numeric_feature_cols

        # --- Train windows ---
        self.train_windows: List[pd.DataFrame] = train_windows

    # ------------------------------------------------------------------ #
    def _validate_input(
        self,
        name: str,
        train_set: pd.DataFrame,
        val_set: pd.DataFrame,
        test_set: pd.DataFrame,
        norm_train_set: pd.DataFrame,
        norm_val_set: pd.DataFrame,
        norm_test_set: pd.DataFrame,
        output_column: str,
        input_size: int,
        forecast_size: int,
        train_windows: List[pd.DataFrame],
        feature_scaler: MinMaxScaler,
        target_scaler: MinMaxScaler,
        numeric_feature_cols: List[str],
    ) -> bool:
        """
        Validate all inputs for TrainTestSet initialization.
        Raises ValueError if any validation fails.
        """

        # ------------------------------------------------------------------
        # Basic metadata
        # ------------------------------------------------------------------
        if not isinstance(name, str) or not name.strip():
            raise ValueError("`name` must be a non-empty string.")

        if not isinstance(output_column, str) or not output_column.strip():
            raise ValueError("`output_column` must be a non-empty string.")

        # ------------------------------------------------------------------
        # Dataset validation
        # ------------------------------------------------------------------
        datasets = {
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
            "norm_train_set": norm_train_set,
            "norm_val_set": norm_val_set,
            "norm_test_set": norm_test_set,
        }

        for label, df in datasets.items():
            if not isinstance(df, pd.DataFrame):
                raise ValueError(f"`{label}` must be a pandas DataFrame.")
            if df.empty:
                raise ValueError(f"`{label}` must not be empty.")

        # ------------------------------------------------------------------
        # Column checks
        # ------------------------------------------------------------------
        for label, df in {
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
        }.items():
            if output_column not in df.columns:
                raise ValueError(
                    f"`output_column` '{output_column}' not found in {label}."
                )

        # Normalized sets must have identical columns to raw sets
        for raw, norm, label in [
            (train_set, norm_train_set, "train"),
            (val_set, norm_val_set, "val"),
            (test_set, norm_test_set, "test"),
        ]:
            if list(raw.columns) != list(norm.columns):
                raise ValueError(
                    f"Column mismatch between {label}_set and norm_{label}_set."
                )

        # ------------------------------------------------------------------
        # Window sizes
        # ------------------------------------------------------------------
        for label, value in {
            "input_size": input_size,
            "forecast_size": forecast_size,
        }.items():
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"`{label}` must be a positive integer.")

        min_len = input_size + forecast_size
        for label, df in {
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
        }.items():
            if len(df) < min_len:
                raise ValueError(
                    f"{label} has {len(df)} rows but needs at least {min_len}."
                )

        # ------------------------------------------------------------------
        # Train windows
        # ------------------------------------------------------------------
        if not isinstance(train_windows, list):
            raise ValueError("`train_windows` must be a list of pandas DataFrames.")

        for i, w in enumerate(train_windows):
            if not isinstance(w, pd.DataFrame):
                raise ValueError(f"train_windows[{i}] is not a DataFrame.")
            if len(w) < min_len:
                raise ValueError(
                    f"train_windows[{i}] has {len(w)} rows but needs at least {min_len}."
                )
            if output_column not in w.columns:
                raise ValueError(
                    f"train_windows[{i}] missing required column '{output_column}'."
                )

        # ------------------------------------------------------------------
        # Scalers
        # ------------------------------------------------------------------
        if not isinstance(feature_scaler, MinMaxScaler):
            raise ValueError("`feature_scaler` must be a MinMaxScaler instance.")

        if not isinstance(target_scaler, MinMaxScaler):
            raise ValueError("`target_scaler` must be a MinMaxScaler instance.")

        # ------------------------------------------------------------------
        # Numeric feature columns
        # ------------------------------------------------------------------
        if not isinstance(numeric_feature_cols, list) or not all(
            isinstance(c, str) for c in numeric_feature_cols
        ):
            raise ValueError("`numeric_feature_cols` must be a list of strings.")

        return True

    # ------------------------------------------------------------------ #
    # Getter Methods
    # ------------------------------------------------------------------ #
    def get_name(self) -> int:
        return self.name

    def get_input_size(self) -> int:
        return self.input_size

    def get_forecast_size(self) -> int:
        return self.forecast_size

    def get_output_column(self) -> str:
        return self.output_column

    def get_train_set(self) -> int:
        return self.train_set

    def get_val_set(self) -> pd.DataFrame:
        return self.val_set

    def get_test_set(self) -> pd.DataFrame:
        return self.test_set

    def get_norm_train_set(self) -> pd.DataFrame:
        return self.norm_train_set

    def get_norm_val_set(self) -> pd.DataFrame:
        return self.norm_val_set

    def get_norm_test_set(self) -> pd.DataFrame:
        return self.norm_test_set

    def get_number_of_train_windows(self) -> int:
        return len(self.train_windows)

    def get_train_window(self, index: int = 0) -> pd.DataFrame:
        return self.train_windows[index]
