import pandas as pd
from typing import List, Tuple

from utils.constants import *
from utils.utils import *


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
    ):
        if not self._validate_input(
            name=name,
            train_set=train_set,
            val_set=val_set,
            test_set=test_set,
            output_column=output_column,
            input_size=input_size,
            forecast_size=forecast_size,
            train_windows=train_windows,
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
        self.train_windows: List[pd.DataFrame] = train_windows

    # ------------------------------------------------------------------ #
    def _validate_input(
        self,
        name: str,
        train_set: pd.DataFrame,
        val_set: pd.DataFrame,
        test_set: pd.DataFrame,
        output_column: str,
        input_size: int,
        forecast_size: int,
        train_windows: List[pd.DataFrame],
    ) -> bool:
        """
        Validate all inputs for TrainTestSet initialization.
        Returns True if all validations pass, otherwise raises ValueError.
        """

        # ---- Basic type checks ----
        if not isinstance(name, str) or not name.strip():
            raise ValueError("`name` must be a non-empty string.")

        # Validate datasets
        for label, df in {
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
        }.items():
            if not isinstance(df, pd.DataFrame):
                raise ValueError(f"`{label}` must be a pandas DataFrame.")

        # Output column
        if not isinstance(output_column, str) or not output_column.strip():
            raise ValueError("`output_column` must be a non-empty string.")

        for label, df in {
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
        }.items():
            if output_column not in df.columns:
                raise ValueError(
                    f"`output_column` '{output_column}' not found in {label} columns."
                )

        # Window sizes
        for label, value in {
            "input_size": input_size,
            "forecast_size": forecast_size,
        }.items():
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"`{label}` must be a positive integer.")

        # Validate that datasets are at least large enough for windowing
        for label, df in {
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
        }.items():
            min_len = input_size + forecast_size
            if len(df) < min_len:
                raise ValueError(
                    f"{label} has {len(df)} rows but needs at least {min_len} "
                    f"for input_size={input_size} and forecast_size={forecast_size}."
                )

        # Validate train_windows (list of DataFrames)
        if not isinstance(train_windows, list) or not all(
            isinstance(w, pd.DataFrame) for w in train_windows
        ):
            raise ValueError("`train_windows` must be a list of pandas DataFrames.")

        for i, w in enumerate(train_windows):
            if len(w) < input_size + forecast_size:
                raise ValueError(
                    f"train_windows[{i}] has {len(w)} rows but must have at least "
                    f"{input_size + forecast_size}."
                )
            if output_column not in w.columns:
                raise ValueError(
                    f"train_windows[{i}] does not contain required column '{output_column}'."
                )

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

    def get_train_set_size(self) -> int:
        return len(self.train_set)

    def get_val_set_size(self) -> int:
        return len(self.val_set)

    def get_test_set_size(self) -> int:
        return len(self.test_set)

    def get_number_of_train_windows(self) -> int:
        return len(self.train_windows)

    def get_train_window(self, index: int = 0) -> pd.DataFrame:
        return self.train_sets[index]
