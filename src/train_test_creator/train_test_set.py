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
