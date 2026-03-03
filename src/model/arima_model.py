from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import pmdarima as pm
from statsmodels.tsa.arima.model import ARIMA
from tqdm import tqdm

from logger.logger import Logger
from utils.constants import *
from utils.utils import *
from model.base_model import BaseModel


warnings.filterwarnings(
    "ignore", category=FutureWarning, message=".*force_all_finite.*"
)


class ArimaModel(BaseModel):

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._logger = logger
        self._best_order = None
        self._fitted = False
        self._model = None

    def fit(
        self,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        time_column_name: str,
        value_column_name: str,
        start_p: int = 1,
        start_q: int = 1,
        max_p: int = 10,
        max_q: int = 10,
        m: int = 1,
        d: int = 1,
        seasonal: bool = True,
        start_P: int = 1,
        start_Q: int = 1,
        D: int = 0,
        trace: bool = True,  # 👈 must be True for callback to fire
        error_action: str = "ignore",
        suppress_warnings: bool = True,
        stepwise: bool = True,
    ):
        self._logger.log_info("Fitting ARIMA model...")

        print(
            f"ARIMA fit parameters -> "
            f"start_p={start_p}, start_q={start_q}, max_p={max_p}, max_q={max_q}, "
            f"m={m}, d={d}, seasonal={seasonal}, start_P={start_P}, start_Q={start_Q}, D={D}"
        )

        self._train_date_series = train_df[time_column_name]
        self._train_value_series = train_df[value_column_name]

        self._test_date_series = test_df[time_column_name]
        self._test_value_series = test_df[value_column_name]

        pbar = tqdm(desc="Searching ARIMA models", total=None)

        def progress_callback(*args, **kwargs):
            """
            Flexible callback: works regardless of pmdarima version/signature.
            """
            p_val = d_val = q_val = None

            # Try to extract order
            if "order" in kwargs:
                order = kwargs["order"]
                if isinstance(order, (tuple, list)) and len(order) >= 3:
                    p_val, d_val, q_val = order[:3]
            elif len(args) >= 4 and all(isinstance(x, int) for x in args[1:4]):
                # (arima, p, d, q)
                p_val, d_val, q_val = args[1:4]
            elif len(args) >= 3 and all(isinstance(x, int) for x in args[:3]):
                # (p, d, q)
                p_val, d_val, q_val = args[:3]
            elif len(args) >= 1:
                model_candidate = args[0]
                order = getattr(model_candidate, "order", None)
                if not order:
                    order = getattr(
                        getattr(model_candidate, "model_", None), "order", None
                    )
                if order and len(order) >= 3:
                    p_val, d_val, q_val = order[:3]

            # Update progress bar
            if p_val is not None:
                pbar.set_description(f"Trying ARIMA({p_val},{d_val},{q_val})")
            else:
                pbar.set_description("Trying ARIMA candidate")
            pbar.update(1)

        try:
            self._model = pm.auto_arima(
                self._train_value_series,
                start_p=start_p,
                max_p=max_p,
                start_q=start_q,
                max_q=max_q,
                m=m,
                d=d,
                seasonal=seasonal,
                start_P=start_P,
                start_Q=start_Q,
                D=D,
                trace=trace,  # 👈 required
                error_action=error_action,
                suppress_warnings=suppress_warnings,
                stepwise=stepwise,
                callback=progress_callback,
            )
        finally:
            pbar.close()

        self._best_order = self._model.get_params().get("order")
        self._model_fit = ARIMA(self._train_value_series, order=self._best_order)
        self._fitted = self._model_fit.fit()

    def get_best_order(self):
        return self._best_order

    def rolling_forecast(
        self, mode: str = "expanding", window_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Perform rolling forecast using ARIMA.append to avoid full refits.

        Parameters
        ----------
        mode : str
            "expanding" (default) or "sliding"
        window_size : int, optional
            Required if mode="sliding" (number of days for training window)
        """
        if not self._best_order:
            raise ValueError("Model must be fitted before rolling_forecast.")

        # Combine train + test for easy slicing
        df = pd.DataFrame(
            {
                "ds": pd.concat([self._train_date_series, self._test_date_series]),
                "y": pd.concat([self._train_value_series, self._test_value_series]),
            }
        ).sort_values("ds")

        test = pd.DataFrame(
            {"ds": self._test_date_series, "y": self._test_value_series}
        )
        test["ds_pet"] = test["ds"] - pd.Timedelta(days=7)

        df_preds = []
        first_pet = test.iloc[0]["ds_pet"]

        # Initial training window
        if mode == "expanding":
            train_init = df[df["ds"] < first_pet]
        elif mode == "sliding":
            if window_size is None:
                raise ValueError("window_size must be set for sliding mode")

            self._window_size = window_size
            train_init = df[
                (df["ds"] < first_pet)
                & (df["ds"] >= first_pet - pd.Timedelta(days=window_size))
            ]
        else:
            raise ValueError("mode must be 'expanding' or 'sliding'")

        # Fit once on initial window
        model_refit = ARIMA(train_init["y"], order=self._best_order).fit()

        # Iterate test set
        for _, row in test.iterrows():
            start_date = row["ds"]
            test_point = df[df["ds"] == start_date]
            if test_point.empty:
                continue

            # 1-step forecast
            fc = model_refit.forecast(1).iloc[0]
            df_preds.append(
                {
                    "ds": start_date,
                    "actual_y": test_point.iloc[0]["y"],
                    "test_pred": fc,
                }
            )

            # Update with new observation
            model_refit = model_refit.append([test_point.iloc[0]["y"]], refit=False)

        return pd.DataFrame(df_preds)

    def forecast_future(self, steps: int = 90) -> pd.DataFrame:
        """
        Forecast future values for a given number of steps beyond the training data.

        Parameters
        ----------
        steps : int
            Number of periods to forecast into the future (default: 90).

        Returns
        -------
        pd.DataFrame
            DataFrame with forecasted dates and values.
        """
        if not self._fitted:
            raise ValueError("Model must be fitted before forecasting.")

        # Forecast future values
        forecast = self._fitted.forecast(steps=steps)

        # Build future date index
        last_date = self._train_date_series.iloc[-1]
        future_dates = pd.date_range(
            start=last_date + pd.Timedelta(days=1), periods=steps
        )

        return pd.DataFrame({"ds": future_dates, "forecast": forecast})

    def plot_rolling_forecast(
        self,
        preds_expanding: pd.DataFrame,
        preds_sliding: Optional[pd.DataFrame] = None,
        title: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        """
        Plot rolling forecast(s) vs test data.
        Can show expanding and/or sliding in two subplots.
        """

        fig, axes = plt.subplots(
            2 if preds_sliding is not None else 1, 1, figsize=(14, 10), sharex=True
        )

        if preds_sliding is None:
            axes = [axes]  # make iterable

        # --- Expanding ---
        axes[0].plot(
            self._test_date_series,
            self._test_value_series,
            label="Test (Actual)",
            color="green",
        )
        axes[0].plot(
            preds_expanding["ds"],
            preds_expanding["test_pred"],
            label="Forecast (Expanding)",
            color="red",
            linestyle="--",
        )
        axes[0].set_title("ARIMA Forecast vs Actual (Expanding Window)")
        axes[0].set_ylabel("Value")
        axes[0].legend()
        axes[0].grid(True, linestyle="--", alpha=0.7)

        # --- Sliding ---
        if preds_sliding is not None:
            axes[1].plot(
                self._test_date_series,
                self._test_value_series,
                label="Test (Actual)",
                color="green",
            )
            axes[1].plot(
                preds_sliding["ds"],
                preds_sliding["test_pred"],
                label="Forecast (Sliding)",
                color="blue",
                linestyle="--",
            )
            axes[1].set_title(
                f"ARIMA Forecast vs Actual (Sliding Window, {self._window_size} days)"
            )
            axes[1].set_xlabel("Date")
            axes[1].set_ylabel("Value")
            axes[1].legend()
            axes[1].grid(True, linestyle="--", alpha=0.7)

        fig.tight_layout()

        # Save figure
        os.makedirs(CHARTS_DIR_ARIMA, exist_ok=True)
        order_str_filename = "_".join(map(str, self._best_order))

        if file_name is not None:
            file_name = f"{file_name}_{order_str_filename}.png"
        else:
            file_name = f"rolling_forecast_arima_{order_str_filename}.png"

        save_path = os.path.join(CHARTS_DIR_ARIMA, file_name)
        fig.savefig(save_path, dpi=300)
        print(f"Figure saved to: {save_path}")

        plt.show()

    def plot_future_forecast(
        self,
        forecast_df: pd.DataFrame,
        title: str = "ARIMA Future Forecast",
        file_name: Optional[str] = None,
    ):
        """
        Plot forecasted values vs test data only (no training history).

        Parameters
        ----------
        forecast_df : pd.DataFrame
            Must contain ["ds", "forecast"] (from forecast_future()).
        title : str
            Chart title.
        file_name : str, optional
            If given, save the plot to CHARTS_DIR_ARIMA/file_name.png
        """
        fig, ax = plt.subplots(figsize=(14, 6))

        # Plot test (actual values)
        ax.plot(
            self._test_date_series,
            self._test_value_series,
            label="Test (Actual)",
            color="green",
        )

        # Plot forecast
        ax.plot(
            forecast_df["ds"],
            forecast_df["forecast"],
            label="Forecast",
            color="blue",
            linestyle="--",
        )

        ax.set_title(title)
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.7)

        fig.tight_layout()

        # Save figure
        os.makedirs(CHARTS_DIR_ARIMA, exist_ok=True)

        if file_name is not None:
            save_path = os.path.join(CHARTS_DIR_ARIMA, f"{file_name}.png")
        else:
            order_str_filename = "_".join(map(str, self._best_order))
            save_path = os.path.join(
                CHARTS_DIR_ARIMA, f"future_forecast_arima_{order_str_filename}.png"
            )

        fig.savefig(save_path, dpi=300)
        print(f"Figure saved to: {save_path}")

        plt.show()
