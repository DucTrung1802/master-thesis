import pandas as pd


class SlidingWindow:
    def __init__(
        self,
        df: pd.DataFrame,
        time_base_column_name: str,
        input_window_length: int,
        forecast_horizon_length: int,
    ):
        if df.empty:
            raise ValueError("SlidingWindow df must not be empty.")

        if time_base_column_name not in df.columns:
            raise ValueError(f"Column {time_base_column_name} not found in DataFrame.")

        if input_window_length <= 0:
            raise ValueError("input_window_length must be positive.")

        if forecast_horizon_length <= 0:
            raise ValueError("forecast_horizon_length must be positive.")

        if len(df) < input_window_length + forecast_horizon_length:
            raise ValueError(
                "df length is too short for given input_window_length and forecast_horizon_length."
            )

        # Store attributes
        self.df: pd.DataFrame = df.sort_values(time_base_column_name).reset_index(
            drop=True
        )
        self.time_base_column_name: str = time_base_column_name
        self.input_window_length: int = input_window_length
        self.forecast_horizon_length: int = forecast_horizon_length

        # Derived windows
        self.input_window = self.df.iloc[:input_window_length].copy()
        self.forecast_window = self.df.iloc[
            input_window_length : input_window_length + forecast_horizon_length
        ].copy()
