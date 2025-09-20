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

    def forecast(self, steps: int = None):
        if not self._fitted:
            raise ValueError("Model has not been fitted yet.")

        if not steps:
            steps = len(self._test_value_series)

        self._forecast = self._fitted.forecast(steps)

        return self._forecast

    def get_best_order(self):
        return self._best_order

    def plot_forecast(
        self,
        steps: int = None,
        test: Optional[pd.Series] = None,
        title: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        """
        Plot training data, forecasted values, and optional test series.
        Optionally save the plot as a file inside CHARTS_DIR_ARIMA.

        Notes
        -----
        If a forecast has already been generated (via self.forecast),
        it will be reused. Otherwise, a new forecast is computed.
        """
        if not self._fitted:
            raise ValueError("Model has not been fitted yet.")

        # Use existing forecast if available, else compute
        if hasattr(self, "_forecast") and self._forecast is not None:
            forecast = self._forecast
        else:
            if steps is None:
                steps = len(test) if test is not None else 12
            forecast = self.forecast(steps=steps)

        # Forecast length
        steps = len(forecast)

        # Create figure with 2 subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

        # ---------------- First subplot: Train + Forecast (+ Test) ----------------
        ax1.plot(
            self._train_date_series,
            self._train_value_series,
            label="Train",
            linewidth=2,
            color="blue",
        )

        # Forecast index
        if test is not None:
            forecast_index = self._test_date_series[:steps]
        else:
            forecast_index = pd.date_range(
                start=self._train_date_series.iloc[-1],
                periods=steps + 1,
                freq=pd.infer_freq(self._train_date_series),
            )[1:]

        # Plot forecast
        ax1.plot(
            forecast_index,
            forecast.values,
            color="red",
            linestyle="--",
            linewidth=2,
            label="Forecast",
        )

        # Plot test if provided
        if test is not None:
            ax1.plot(
                self._test_date_series[:steps],
                test.values[:steps],
                color="green",
                linestyle="-",
                linewidth=2,
                label="Test",
            )

        ax1.legend()
        order_str = f"ARIMA{self._best_order}"
        default_title = "Train vs Forecast" + (" vs Test" if test is not None else "")
        final_title = (
            title if title is not None else default_title
        ) + f" | {order_str}"
        ax1.set_title(final_title)
        ax1.set_ylabel("Value")
        ax1.grid(True, linestyle="--", alpha=0.6)

        # ---------------- Second subplot: Forecast vs Test only ----------------
        if test is not None:
            ax2.plot(
                forecast_index,
                forecast.values,
                color="red",
                linestyle="--",
                linewidth=2,
                label="Forecast",
            )
            ax2.plot(
                self._test_date_series[:steps],
                test.values[:steps],
                color="green",
                linestyle="-",
                linewidth=2,
                label="Test",
            )

            ax2.legend()
            ax2.set_title("Forecast vs Test only (Zoomed)")
            ax2.set_xlabel("Time")
            ax2.set_ylabel("Value")
            ax2.grid(True, linestyle="--", alpha=0.6)

            # 🔑 Limit x-axis to test data only
            ax2.set_xlim(
                self._test_date_series.iloc[0], self._test_date_series.iloc[steps - 1]
            )

        fig.tight_layout()

        # Save figure
        os.makedirs(CHARTS_DIR_ARIMA, exist_ok=True)
        order_str_filename = "_".join(map(str, self._best_order))

        if file_name is not None:
            file_name = f"{file_name}_{order_str_filename}.png"
        else:
            file_name = f"forecast_arima_{order_str_filename}.png"

        save_path = os.path.join(CHARTS_DIR_ARIMA, file_name)
        fig.savefig(save_path, dpi=300)
        print(f"Figure saved to: {save_path}")

        plt.show()
