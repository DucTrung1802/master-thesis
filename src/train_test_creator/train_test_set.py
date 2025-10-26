import pandas as pd


from utils.constants import *
from utils.utils import *


class TrainTestSet:
    def __init__(
        self,
        name: str,
        train_set: List[pd.DataFrame],
        test_set: pd.DataFrame,
        input_window_size: int,
        forecast_horizon_size: int,
    ):
        # Meatadata
        self.name = name
        self.input_window_size = input_window_size
        self.forecast_horizon_size = forecast_horizon_size

        # Data
        self.train_set = train_set
        self.test_set = test_set

    def get_number_of_train_windows(self) -> int:
        return len(self.train_set)

    def get_number_of_test_forecast_horizons(self) -> int:
        return len(self.test_set) // self.forecast_horizon_size

    def get_train_window(self, index: int = 0) -> pd.DataFrame:
        return self.train_set[index]

    def get_test_window(self, index: int = 0) -> pd.DataFrame:
        start_idx = index * self.forecast_horizon_size
        end_idx = start_idx + self.forecast_horizon_size
        return self.test_set.iloc[start_idx:end_idx]
