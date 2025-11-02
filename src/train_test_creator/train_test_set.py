import pandas as pd
from typing import List, Tuple

from utils.constants import *
from utils.utils import *


class TrainTestSet:
    def __init__(
        self,
        name: str,
        train_sets: List[pd.DataFrame],
        test_sets: List[pd.DataFrame],
        output_column: str,
        output_range: tuple,
        train_window_size: int,
        validation_window_size: int,
        test_window_size: int,
    ):
        if not self._validate_input(
            name,
            train_sets,
            test_sets,
            output_column,
            output_range,
            train_window_size,
            validation_window_size,
            test_window_size,
        ):
            return

        # --- Metadata ---
        self.name: str = name
        self.train_window_size: int = train_window_size
        self.validation_window_size: int = validation_window_size
        self.test_window_size: int = test_window_size

        # --- Data ---
        self.train_sets: List[pd.DataFrame] = train_sets
        self.test_sets: List[pd.DataFrame] = test_sets
        self.output_column: str = output_column
        self.output_range: Tuple[int, int] = output_range

    # ------------------------------------------------------------------ #
    def _validate_input(
        self,
        name: str,
        train_sets: List[pd.DataFrame],
        test_sets: List[pd.DataFrame],
        output_column: str,
        output_range: tuple,
        train_window_size: int,
        validation_window_size: int,
        test_window_size: int,
    ) -> bool:
        """
        Validate all inputs for TrainTestSet initialization.
        Returns True if all validations pass, otherwise raises ValueError.
        """

        # ---- Basic type checks ----
        if not isinstance(name, str) or not name.strip():
            raise ValueError("`name` must be a non-empty string.")

        for label, sets in {
            "train_sets": train_sets,
            "test_sets": test_sets,
        }.items():
            if not isinstance(sets, list) or not all(
                isinstance(df, pd.DataFrame) for df in sets
            ):
                raise ValueError(f"`{label}` must be a list of pandas DataFrames.")

        if not isinstance(output_column, str) or not output_column.strip():
            raise ValueError("`output_column` must be a non-empty string.")

        if not isinstance(output_range, tuple) or len(output_range) != 2:
            raise ValueError(
                "`output_range` must be a tuple of (min_value, max_value)."
            )

        if not all(isinstance(x, (int, float)) for x in output_range):
            raise ValueError("`output_range` values must be numeric.")

        for name_, value in {
            "train_window_size": train_window_size,
            "validation_window_size": validation_window_size,
            "test_window_size": test_window_size,
        }.items():
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"`{name_}` must be a positive integer (got {value}).")

        # ---- Data consistency checks ----
        for label, sets, min_size in [
            ("train_sets", train_sets, train_window_size),
            ("test_sets", test_sets, test_window_size),
        ]:
            for i, df in enumerate(sets):
                if output_column not in df.columns:
                    raise ValueError(
                        f"`output_column` '{output_column}' not found in {label}[{i}] columns."
                    )

                if len(df) < min_size:
                    raise ValueError(
                        f"{label}[{i}] has fewer rows ({len(df)}) than expected ({min_size})."
                    )

        # ---- Logical check ----
        if output_range[0] >= output_range[1]:
            raise ValueError(
                "`output_range` minimum value must be less than maximum value."
            )

        return True

    # ------------------------------------------------------------------ #
    # Getter Methods
    # ------------------------------------------------------------------ #
    def get_train_window_size(self) -> int:
        return self.train_window_size

    def get_validation_window_size(self) -> int:
        return self.validation_window_size

    def get_test_window_size(self) -> int:
        return self.test_window_size

    def get_number_of_train_windows(self) -> int:
        return len(self.train_sets)

    def get_train_window(self, index: int = 0) -> pd.DataFrame:
        return self.train_sets[index]

    def get_test_set(self) -> pd.DataFrame:
        return self.test_sets[0]
