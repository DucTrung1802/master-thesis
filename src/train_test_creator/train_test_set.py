import pandas as pd


from utils.constants import *
from utils.utils import *


class TrainTestSet:
    def __init__(
        self,
        name: str,
        train_sets: List[pd.DataFrame],
        output_column: str,
        output_range: tuple,
        test_sets: List[pd.DataFrame],
        input_window_size: int,
        forecast_horizon_size: int,
    ):
        if not self._validate_input(
            name,
            train_sets,
            output_column,
            output_range,
            test_sets,
            input_window_size,
            forecast_horizon_size,
        ):
            return

        # Meatadata
        self.name: str = name
        self.input_window_size: int = input_window_size
        self.forecast_horizon_size: int = forecast_horizon_size

        # Data
        self.train_sets: List[pd.DataFrame] = train_sets
        self.test_sets: List[pd.DataFrame] = test_sets
        self.output_column: str = output_column
        self.output_range: Tuple[int, int] = output_range

    def _validate_input(
        self,
        name: str,
        train_sets: List[pd.DataFrame],
        output_column: str,
        output_range: tuple,
        test_sets: List[pd.DataFrame],
        input_window_size: int,
        forecast_horizon_size: int,
    ) -> bool:
        """
        Validate all inputs for TrainTestSet initialization.
        Returns True if all validations pass, otherwise raises ValueError.
        """

        # ---- Basic type checks ----
        if not isinstance(name, str) or not name.strip():
            raise ValueError("`name` must be a non-empty string.")

        if not isinstance(train_sets, list) or not all(
            isinstance(df, pd.DataFrame) for df in train_sets
        ):
            raise ValueError("`train_sets` must be a list of pandas DataFrames.")

        if not isinstance(test_sets, list) or not all(
            isinstance(df, pd.DataFrame) for df in test_sets
        ):
            raise ValueError("`test_sets` must be a list of pandas DataFrames.")

        if not isinstance(output_column, str) or not output_column.strip():
            raise ValueError("`output_column` must be a non-empty string.")

        if not isinstance(output_range, tuple) or len(output_range) != 2:
            raise ValueError(
                "`output_range` must be a tuple of (min_value, max_value)."
            )

        if not all(isinstance(x, (int, float)) for x in output_range):
            raise ValueError("`output_range` values must be numeric.")

        if not isinstance(input_window_size, int) or input_window_size <= 0:
            raise ValueError("`input_window_size` must be a positive integer.")

        if not isinstance(forecast_horizon_size, int) or forecast_horizon_size <= 0:
            raise ValueError("`forecast_horizon_size` must be a positive integer.")

        # ---- Data consistency checks ----
        for i, df in enumerate(train_sets):
            if output_column not in df.columns:
                raise ValueError(
                    f"`output_column` '{output_column}' not found in train_sets[{i}] columns."
                )

            if len(df) < input_window_size:
                raise ValueError(
                    f"Train set {i} has fewer rows ({len(df)}) than input_window_size ({input_window_size})."
                )

        for j, df in enumerate(test_sets):
            if output_column not in df.columns:
                raise ValueError(
                    f"`output_column` '{output_column}' not found in test_sets[{j}] columns."
                )

            if len(df) < forecast_horizon_size:
                raise ValueError(
                    f"Test set {j} has fewer rows ({len(df)}) than forecast_horizon_size ({forecast_horizon_size})."
                )

        # ---- Logical checks ----
        if output_range[0] >= output_range[1]:
            raise ValueError(
                "`output_range` minimum value must be less than maximum value."
            )

        # ---- All checks passed ----
        return True

    def get_input_window_size(self) -> int:
        return self.input_window_size

    def get_forecast_horizon_size(self) -> int:
        return self.forecast_horizon_size

    def get_number_of_train_windows(self) -> int:
        return len(self.train_sets)

    def get_number_of_test_forecast_horizons(self) -> int:
        return len(self.test_sets)

    def get_train_window(self, index: int = 0) -> pd.DataFrame:
        return self.train_sets[index]

    def get_test_window(self, index: int = 0) -> pd.DataFrame:
        return self.test_sets[index]
