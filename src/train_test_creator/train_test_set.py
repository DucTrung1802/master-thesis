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
        # Meatadata
        self.name: str = name
        self.input_window_size: int = input_window_size
        self.forecast_horizon_size: int = forecast_horizon_size

        # Data
        self.train_sets: List[pd.DataFrame] = train_sets
        self.test_sets: List[pd.DataFrame] = test_sets
        self.output_column: str = output_column
        self.output_range: Tuple[int, int] = output_range

    def get_number_of_train_windows(self) -> int:
        return len(self.train_sets)

    def get_number_of_test_forecast_horizons(self) -> int:
        return len(self.test_sets)

    def get_train_window(self, index: int = 0) -> pd.DataFrame:
        return self.train_sets[index]

    def get_test_window(self, index: int = 0) -> pd.DataFrame:
        return self.test_sets[index]
