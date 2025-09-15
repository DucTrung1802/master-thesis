from dataclasses import dataclass
from typing import Optional, Tuple, Dict
from tqdm import tqdm
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import boxcox
from scipy.special import inv_boxcox
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller, kpss, acf
from joblib import Parallel, delayed

from logger.logger import Logger
from utils.constants import *
from utils.utils import *
from model.base_model import BaseModel


@dataclass
class StationarityResult:
    series_original: pd.Series
    series_transformed: pd.Series
    series_diff: pd.Series
    series_seasonal_diff: pd.Series
    transform: str  # 'log' | 'boxcox' | 'none'
    lambda_boxcox: Optional[float]
    d: int
    D: int
    m: Optional[int]
    adf_pvals: Dict[str, float]
    kpss_pvals: Dict[str, float]
    order: Optional[Tuple[int, int, int]] = None  # (p,d,q)


class ArimaModel(BaseModel):
    """
    Stationarity preparation + basic ARIMA order selection (no pmdarima).
    """

    def __init__(self, logger: Logger, significance: float = 0.05):
        super().__init__(logger)  # call BaseModel initializer
        self._logger = logger
        self._significance = significance
        self._result: Optional[StationarityResult] = None
        self._fit_model = None

    # --- Stationarity tests ---
    def _adf_test(self, series: pd.Series) -> dict:
        res = adfuller(series.dropna(), autolag="AIC")
        return {"statistic": res[0], "pvalue": res[1], "nlags": res[2], "nobs": res[3]}

    def _kpss_test(self, series: pd.Series) -> dict:
        try:
            stat, p_value, nlags, crit = kpss(
                series.dropna(), regression="c", nlags="auto"
            )
        except Exception as e:
            self._logger.log_error(f"KPSS failed: {e}")
            return {"statistic": np.nan, "pvalue": 0.0, "nlags": None}
        return {"statistic": stat, "pvalue": p_value, "nlags": nlags}

    def _is_stationary(self, series: pd.Series) -> Tuple[bool, dict]:
        adf = self._adf_test(series)
        kpss_res = self._kpss_test(series)
        adf_stationary = adf["pvalue"] <= self._significance
        kpss_stationary = (
            (kpss_res["pvalue"] > self._significance)
            if not np.isnan(kpss_res["pvalue"])
            else False
        )
        adf["stationary"] = adf_stationary
        kpss_res["stationary"] = kpss_stationary
        return (adf_stationary and kpss_stationary), {"adf": adf, "kpss": kpss_res}

    # --- Detect seasonality (m) ---
    def _detect_seasonality(
        self, series: pd.Series, max_lag: int = 24
    ) -> Optional[int]:
        acf_vals = acf(series.dropna(), nlags=max_lag)
        # strong spike after lag > 1 indicates seasonality
        threshold = 0.5
        for lag in range(2, max_lag + 1):
            if abs(acf_vals[lag]) > threshold:
                self._logger.log_info(
                    f"Detected seasonal lag={lag}, acf={acf_vals[lag]:.2f}"
                )
                return lag
        return None

    # --- ARIMA order selection (parallelized) ---
    def _select_order(
        self,
        series: pd.Series,
        max_p: int = 3,
        max_d: int = 2,
        max_q: int = 3,
        criterion: str = "aic",
    ) -> Tuple[int, int, int]:
        """
        Grid search ARIMA orders and pick the best according to the chosen criterion.
        """

        def try_order(p, d, q):
            if p == d == q == 0:
                return None
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning)
                    res = ARIMA(series, order=(p, d, q)).fit(
                        method_kwargs={"warn_convergence": False}
                    )

                scores = {
                    "aic": res.aic,
                    "bic": res.bic,
                    "hqic": res.hqic,
                    "aicc": getattr(res, "aicc", np.nan),
                }
                score = scores.get(criterion, res.aic)

                msg = (
                    f"(p={p}, d={d}, q={q}) → "
                    f"AIC={scores['aic']:.2f}, "
                    f"BIC={scores['bic']:.2f}, "
                    f"HQIC={scores['hqic']:.2f}, "
                    f"AICC={scores['aicc']:.2f}"
                )
                return (p, d, q), score, msg

            except Exception as e:
                return None, None, f"❌ Order (p={p}, d={d}, q={q}) failed: {repr(e)}"

        tasks = [
            (p, d, q)
            for p in range(max_p + 1)
            for d in range(max_d + 1)
            for q in range(max_q + 1)
            if not (p == d == q == 0)
        ]
        n_jobs = max(1, os.cpu_count() - 1)

        results = []
        with tqdm(
            total=len(tasks), desc="Searching ARIMA orders (parallelized)"
        ) as pbar:
            parallel = Parallel(n_jobs=n_jobs, return_as="generator", batch_size=1)
            for result in parallel(delayed(try_order)(*task) for task in tasks):
                order, score, msg = result
                if msg:
                    self._logger.log_info(msg)  # ✅ logs now from main process
                if order is not None:
                    results.append((order, score))
                pbar.update(1)

        results = [r for r in results if r is not None]
        if not results:
            raise RuntimeError("No valid ARIMA model could be fit.")

        best_order, best_score = min(results, key=lambda x: x[1])
        self._logger.log_info(
            f"✅ Best order={best_order}, {criterion.upper()}={best_score:.2f} (using {n_jobs} cores)"
        )
        return best_order

    # --- Fit pipeline ---
    def fit(
        self,
        series: pd.Series,
        max_p: int = 3,
        max_d: int = 2,
        max_q: int = 3,
        criterion: str = "aic",
        model_suffix: str = None,
    ):
        """
        Fit an ARIMA model to the given time series.

        This pipeline performs the following steps:
        1. Cleans the series by dropping NaN values.
        2. Applies a transformation (Box-Cox if possible, otherwise log, otherwise none).
        3. Detects seasonality and sets seasonal differencing parameter (D).
        4. Selects the best ARIMA order (p, d, q) based on the given information criterion.
        5. Stores intermediate results (stationarity tests, transformed data, etc.).
        6. Fits the ARIMA model using `statsmodels`.

        Parameters
        ----------
        series : pd.Series
            Input time series to model.
        max_p : int, default=3
            Maximum autoregressive order (p) to search.
        max_d : int, default=2
            Maximum differencing order (d) to search.
        max_q : int, default=3
            Maximum moving average order (q) to search.
        criterion : {"aic", "bic", "aicc", "hqic"}, default="aic"
            Information criterion used for model selection:
            - "aic": Akaike Information Criterion
            - "bic": Bayesian Information Criterion
            - "aicc": Corrected AIC (for small samples)
            - "hqic": Hannan–Quinn Information Criterion

        Attributes (after fitting)
        --------------------------
        _result : StationarityResult
            Stores original, transformed, differenced series, test results, and chosen order.
        _fit_model : statsmodels.tsa.arima.model.ARIMAResults
            The fitted ARIMA model.

        Notes
        -----
        - If the series contains non-positive values, Box-Cox/Log transformations are skipped.
        - Seasonal differencing is applied if seasonality is detected.
        - Model order is selected by minimizing the chosen criterion across the search space.

        Returns
        -------
        self : object
            The fitted instance of the model (allows method chaining).
        """

        series = series.dropna()

        # Step 1: transformation
        transform = "none"
        lam = None
        series_transformed = series
        if (series <= 0).any():
            self._logger.log_warning(
                "Series has non-positive values. Skipping Box-Cox/Log."
            )
        else:
            try:
                series_boxcox, lam = boxcox(series)
                transform = "boxcox"
                series_transformed = pd.Series(series_boxcox, index=series.index)
            except Exception:
                transform = "log"
                series_transformed = np.log(series)

        # Step 2: detect seasonality
        m = self._detect_seasonality(series_transformed)
        D = 1 if m else 0

        # Step 3: select ARIMA order (pass tuning params)
        order = self._select_order(
            series_transformed,
            max_p=max_p,
            max_d=max_d,
            max_q=max_q,
            criterion=criterion,
        )

        # Step 4: save result
        self._result = StationarityResult(
            series_original=series,
            series_transformed=series_transformed,
            series_diff=series_transformed.diff().dropna(),
            series_seasonal_diff=(
                series_transformed.diff(m).dropna() if m else pd.Series(dtype=float)
            ),
            transform=transform,
            lambda_boxcox=lam,
            d=order[1],
            D=D,
            m=m,
            adf_pvals=self._adf_test(series_transformed),
            kpss_pvals=self._kpss_test(series_transformed),
            order=order,
        )

        # Step 5: fit best model
        self._fit_model = ARIMA(series_transformed, order=order).fit()

        # Step 6: save model to pickle with order in name
        os.makedirs(TRAINED_MODELS_LOG_FILE_BASE, exist_ok=True)

        order_str = f"{order[0]}_{order[1]}_{order[2]}"
        suffix = (model_suffix or "").strip()

        if suffix:
            filename = f"arima_model_{order_str}_{suffix}.pkl"
        else:
            filename = f"arima_model_{order_str}.pkl"

        model_path = os.path.join(TRAINED_MODELS_LOG_FILE_BASE, filename)
        self._fit_model.save(model_path)

        self._logger.log_info(f"Model saved to: {model_path}")

    def get_order(self) -> Tuple[int, int, int]:
        if self._result is None:
            raise RuntimeError("Model not fitted. Call .fit() first.")
        return self._result.order

    def forecast(self, steps: int) -> pd.Series:
        if self._fit_model is None:
            raise RuntimeError("Model not fitted. Call .fit() first.")
        forecast_transformed = self._fit_model.forecast(steps=steps)

        if self._result.transform == "boxcox":
            forecast = inv_boxcox(forecast_transformed, self._result.lambda_boxcox)
        elif self._result.transform == "log":
            forecast = np.exp(forecast_transformed)
        else:
            forecast = forecast_transformed
        return pd.Series(forecast, name="forecast")

    def plot_forecast(
        self,
        steps: int = 12,
        test: Optional[pd.Series] = None,
        title: Optional[str] = None,
    ):
        """
        Plot training data, forecasted values, and optional test series.

        Parameters
        ----------
        steps : int, default=12
            Number of future steps to forecast.
        test : pd.Series, optional
            Test series to compare forecasts against.
        title : str, optional
            Custom plot title. If not provided, defaults to:
            "Train vs Forecast" + (" vs Test" if test is not None else "")
            and appends ARIMA order information.
        """
        forecast = self.forecast(steps)

        plt.figure(figsize=(14, 6))

        # Plot train
        plt.plot(self._result.series_original, label="Train", linewidth=2, color="blue")

        # Forecast line (dashed red)
        forecast_index = pd.RangeIndex(
            start=len(self._result.series_original),
            stop=len(self._result.series_original) + steps,
        )
        plt.plot(
            forecast_index,
            forecast.values,
            color="red",
            linestyle="--",
            linewidth=2,
            label="Forecast",
        )

        # Plot test if provided (solid green)
        if test is not None:
            test_index = pd.RangeIndex(
                start=len(self._result.series_original),
                stop=len(self._result.series_original) + len(test),
            )
            plt.plot(
                test_index,
                test.values,
                color="green",
                linestyle="-",
                linewidth=2,
                label="Test",
            )

        plt.legend()

        # Build ARIMA order string
        order_str = f"ARIMA{self._result.order}"
        # if self._result.m and self._result.D > 0:
        #     order_str += f" x (0,{self._result.D},0,{self._result.m})"  # seasonal part

        # Title logic (append order string)
        default_title = "Train vs Forecast" + (" vs Test" if test is not None else "")
        plt.title((title if title is not None else default_title) + f" | {order_str}")

        plt.xlabel("Time")
        plt.ylabel("Value")
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.tight_layout()
        plt.show()
