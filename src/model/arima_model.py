from dataclasses import dataclass
from typing import Optional, Tuple, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import boxcox
from scipy.special import inv_boxcox
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller, kpss, acf

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
            stat, p_value, nlags, crit = kpss(series.dropna(), regression="c", nlags="auto")
        except Exception as e:
            self._logger.log_error(f"KPSS failed: {e}")
            return {"statistic": np.nan, "pvalue": 0.0, "nlags": None}
        return {"statistic": stat, "pvalue": p_value, "nlags": nlags}

    def _is_stationary(self, series: pd.Series) -> Tuple[bool, dict]:
        adf = self._adf_test(series)
        kpss_res = self._kpss_test(series)
        adf_stationary = adf["pvalue"] <= self._significance
        kpss_stationary = (kpss_res["pvalue"] > self._significance) if not np.isnan(kpss_res["pvalue"]) else False
        adf["stationary"] = adf_stationary
        kpss_res["stationary"] = kpss_stationary
        return (adf_stationary and kpss_stationary), {"adf": adf, "kpss": kpss_res}

    # --- Detect seasonality (m) ---
    def _detect_seasonality(self, series: pd.Series, max_lag: int = 24) -> Optional[int]:
        acf_vals = acf(series.dropna(), nlags=max_lag)
        # strong spike after lag > 1 indicates seasonality
        threshold = 0.5
        for lag in range(2, max_lag + 1):
            if abs(acf_vals[lag]) > threshold:
                self._logger.log_info(f"Detected seasonal lag={lag}, acf={acf_vals[lag]:.2f}")
                return lag
        return None

    # --- ARIMA order selection ---
    def _select_order(self, series: pd.Series, max_p: int = 3, max_q: int = 3) -> Tuple[int, int, int]:
        best_aic = np.inf
        best_order = (0, 0, 0)

        # determine d by differencing tests
        stationary, _ = self._is_stationary(series)
        d = 0
        tmp_series = series.copy()
        while not stationary and d < 2:
            d += 1
            tmp_series = tmp_series.diff().dropna()
            stationary, _ = self._is_stationary(tmp_series)

        for p in range(max_p + 1):
            for q in range(max_q + 1):
                try:
                    model = ARIMA(series, order=(p, d, q))
                    res = model.fit()
                    if res.aic < best_aic:
                        best_aic = res.aic
                        best_order = (p, d, q)
                except Exception:
                    continue
        self._logger.log_info(f"Selected order={best_order} with AIC={best_aic:.2f}")
        return best_order

    # --- Fit pipeline ---
    def fit(self, series: pd.Series):
        series = series.dropna()

        # Step 1: transformation
        transform = "none"
        lam = None
        series_transformed = series
        if (series <= 0).any():
            self._logger.log_warning("Series has non-positive values. Skipping Box-Cox/Log.")
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

        # Step 3: select ARIMA order
        order = self._select_order(series_transformed)

        # Step 4: save result
        self._result = StationarityResult(
            series_original=series,
            series_transformed=series_transformed,
            series_diff=series_transformed.diff().dropna(),
            series_seasonal_diff=series_transformed.diff(m).dropna() if m else pd.Series(dtype=float),
            transform=transform,
            lambda_boxcox=lam,
            d=order[1],
            D=D,
            m=m,
            adf_pvals=self._adf_test(series_transformed),
            kpss_pvals=self._kpss_test(series_transformed),
            order=order,
        )

        # Step 5: fit model
        self._fit_model = ARIMA(series_transformed, order=order).fit()

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

    def plot_forecast(self, steps: int = 12):
        forecast = self.forecast(steps)
        plt.figure(figsize=(10, 5))
        plt.plot(self._result.series_original, label="Original")
        plt.plot(
            range(len(self._result.series_original), len(self._result.series_original) + steps),
            forecast.values,
            color="red",
            label="Forecast",
        )
        plt.legend()
        plt.show()
