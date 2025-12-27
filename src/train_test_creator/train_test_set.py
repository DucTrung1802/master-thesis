import pandas as pd
from typing import List
from sklearn.preprocessing import MinMaxScaler


class TrainTestSet:
    def __init__(
        self,
        name: str,
        data_set: pd.DataFrame,
        train_set: pd.DataFrame,
        val_set: pd.DataFrame,
        test_set: pd.DataFrame,
        output_column: str,
        input_size: int,
        forecast_size: int,
        train_windows: List[pd.DataFrame],
        val_windows: List[pd.DataFrame],
        test_windows: List[pd.DataFrame],
        output_windows: List[pd.DataFrame],
        norm_train_set: pd.DataFrame,
        norm_val_set: pd.DataFrame,
        norm_test_set: pd.DataFrame,
        feature_scaler: MinMaxScaler,
        target_scaler: MinMaxScaler,
        numeric_feature_cols: List[str],
    ):
        self._validate_input(
            name=name,
            data_set=data_set,
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
            val_windows=val_windows,
            test_windows=test_windows,
            output_windows=output_windows,
            feature_scaler=feature_scaler,
            target_scaler=target_scaler,
            numeric_feature_cols=numeric_feature_cols,
        )

        # --- Metadata ---
        self.name = name
        self.input_size = input_size
        self.forecast_size = forecast_size
        self.output_column = output_column

        # --- Data ---
        self.data_set = data_set
        self.train_set = train_set
        self.val_set = val_set
        self.test_set = test_set

        # --- Normalized Data ---
        self.norm_train_set = norm_train_set
        self.norm_val_set = norm_val_set
        self.norm_test_set = norm_test_set

        # --- Rolling windows ---
        self.train_windows = train_windows
        self.val_windows = val_windows
        self.test_windows = test_windows
        self.output_windows = output_windows

        # --- Scalers & features ---
        self.feature_scaler = feature_scaler
        self.target_scaler = target_scaler
        self.numeric_feature_cols = numeric_feature_cols

    # ------------------------------------------------------------------ #
    # Validation
    # ------------------------------------------------------------------ #
    def _validate_input(
        self,
        name: str,
        data_set: pd.DataFrame,
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
        val_windows: List[pd.DataFrame],
        test_windows: List[pd.DataFrame],
        output_windows: List[pd.DataFrame],
        feature_scaler: MinMaxScaler,
        target_scaler: MinMaxScaler,
        numeric_feature_cols: List[str],
    ) -> None:

        if not isinstance(name, str) or not name.strip():
            raise ValueError("`name` must be a non-empty string.")

        if not isinstance(output_column, str) or not output_column.strip():
            raise ValueError("`output_column` must be a non-empty string.")

        datasets = {
            "data_set": data_set,
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

        for label, df in {
            "data_set": data_set,
            "train_set": train_set,
            "val_set": val_set,
            "test_set": test_set,
        }.items():
            if output_column not in df.columns:
                raise ValueError(
                    f"`output_column` '{output_column}' not found in {label}."
                )

        for raw, norm, label in [
            (train_set, norm_train_set, "train"),
            (val_set, norm_val_set, "val"),
            (test_set, norm_test_set, "test"),
        ]:
            if raw.shape != norm.shape:
                raise ValueError(
                    f"Shape mismatch between {label}_set and norm_{label}_set."
                )
            if list(raw.columns) != list(norm.columns):
                raise ValueError(
                    f"Column mismatch between {label}_set and norm_{label}_set."
                )

        for value in (input_size, forecast_size):
            if not isinstance(value, int) or value <= 0:
                raise ValueError("`input_size` and `forecast_size` must be > 0.")

        min_len = input_size + forecast_size

        if len(test_set) < min_len:
            raise ValueError(
                f"`test_set` must have at least {min_len} rows "
                f"(input_size + forecast_size)."
            )

        def _validate_windows(windows: List[pd.DataFrame], label: str):
            if not isinstance(windows, list):
                raise ValueError(f"`{label}_windows` must be a list.")
            for i, w in enumerate(windows):
                if not isinstance(w, pd.DataFrame):
                    raise ValueError(f"{label}_windows[{i}] is not a DataFrame.")
                if len(w) < min_len:
                    raise ValueError(
                        f"{label}_windows[{i}] must have at least {min_len} rows."
                    )
                if output_column not in w.columns:
                    raise ValueError(
                        f"{label}_windows[{i}] missing column '{output_column}'."
                    )

        _validate_windows(train_windows, "train")
        _validate_windows(val_windows, "val")
        _validate_windows(test_windows, "test")
        _validate_windows(output_windows, "output")

        if not isinstance(feature_scaler, MinMaxScaler) or not hasattr(
            feature_scaler, "scale_"
        ):
            raise ValueError("`feature_scaler` must be a fitted MinMaxScaler.")

        if not isinstance(target_scaler, MinMaxScaler) or not hasattr(
            target_scaler, "scale_"
        ):
            raise ValueError("`target_scaler` must be a fitted MinMaxScaler.")

        if not isinstance(numeric_feature_cols, list) or not all(
            isinstance(c, str) for c in numeric_feature_cols
        ):
            raise ValueError("`numeric_feature_cols` must be a list of strings.")

        for col in numeric_feature_cols:
            if col not in data_set.columns:
                raise ValueError(f"Feature column '{col}' not in data_set.")

    # ------------------------------------------------------------------ #
    # Getters (ALL attributes)
    # ------------------------------------------------------------------ #
    def get_name(self) -> str:
        return self.name

    def get_input_size(self) -> int:
        return self.input_size

    def get_forecast_size(self) -> int:
        return self.forecast_size

    def get_output_column(self) -> str:
        return self.output_column

    def get_data_set(self) -> pd.DataFrame:
        return self.data_set

    def get_train_set(self) -> pd.DataFrame:
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

    def get_train_windows(self) -> List[pd.DataFrame]:
        return self.train_windows

    def get_val_windows(self) -> List[pd.DataFrame]:
        return self.val_windows

    def get_test_windows(self) -> List[pd.DataFrame]:
        return self.test_windows

    def get_output_windows(self) -> List[pd.DataFrame]:
        return self.output_windows

    def get_feature_scaler(self) -> MinMaxScaler:
        return self.feature_scaler

    def get_target_scaler(self) -> MinMaxScaler:
        return self.target_scaler

    def get_numeric_feature_cols(self) -> List[str]:
        return self.numeric_feature_cols
