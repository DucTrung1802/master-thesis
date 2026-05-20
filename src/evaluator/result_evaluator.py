import os
from numpy import ndarray
from typing import List
import pandas as pd
import numpy as np
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    mean_absolute_percentage_error,
)


class ResultEvaluator:

    def __init__(
        self, y_predict: ndarray, y_test: ndarray, time_index: List[str] = None
    ):
        self._y_predict = y_predict
        self._y_test = y_test
        self._time_index = time_index
        self._validate()

    def _validate(self):
        if len(self._y_predict) == 0 or len(self._y_test) == 0:
            raise ValueError("y_predict and y_test must not be empty.")

        if len(self._y_predict) != len(self._y_test):
            raise ValueError(
                f"y_predict and y_test must have the same length, "
                f"got y_predict={len(self._y_predict)} and y_test={len(self._y_test)}."
            )

        if self._time_index is not None and len(self._time_index) != len(self._y_test):
            raise ValueError(
                f"time_index must have the same length as y_predict and y_test, "
                f"got time_index={len(self._time_index)} and y_test={len(self._y_test)}."
            )

    def evaluate(self) -> pd.DataFrame:
        metrics = {
            "MAE": mean_absolute_error(self._y_test, self._y_predict),
            "MSE": mean_squared_error(self._y_test, self._y_predict),
            "RMSE": np.sqrt(mean_squared_error(self._y_test, self._y_predict)),
            "MAPE": mean_absolute_percentage_error(self._y_test, self._y_predict),
            "R2": r2_score(self._y_test, self._y_predict),
        }

        return pd.DataFrame(list(metrics.items()), columns=["metric", "value"])

    def save_result(self, output_folder_path: str):
        os.makedirs(output_folder_path, exist_ok=True)

        np.save(os.path.join(output_folder_path, "y_predict.npy"), self._y_predict)
        np.save(os.path.join(output_folder_path, "y_test.npy"), self._y_test)

        if self._time_index is not None:
            np.save(
                os.path.join(output_folder_path, "time_index.npy"), self._time_index
            )

        self.evaluate().to_csv(
            os.path.join(output_folder_path, "metrics.csv"), index=False
        )
