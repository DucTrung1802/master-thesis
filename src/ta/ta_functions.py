import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys, os
from dotenv import load_dotenv
from itertools import combinations, product
import talib

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from logger.logger import LogType, Logger
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Condition,
    DataType,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import *
from utils.enums import *
from utils.utils import get_weekends

load_dotenv()


def prepare_data(date_series: pd.Series, price_series: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(date_series),
            "price": pd.to_numeric(price_series, errors="coerce"),
        }
    )
    df = df.dropna(subset=["date", "price"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def validate_column(df: pd.DataFrame, column_name: str) -> None:
    """
    Validate that the specified column exists in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column_name : str
        Column name to validate.

    Raises
    ------
    ValueError
        If the column does not exist in the DataFrame.
    """
    if column_name not in df.columns:
        raise ValueError(
            f"Column '{column_name}' not found in DataFrame. Available columns: {list(df.columns)}"
        )


# region OVERLAP STUDIES
def add_bbands(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    k: float = 2.0,
    ma_type: int = 0,
    column_name: str = "close",
    default_bb_periods: list[int] = None,
    distance_mode: str = "pct",  # "abs" or "pct"
    slope_mode: str = "diff",  # "diff" or "pct"
) -> pd.DataFrame:
    """
    Add Bollinger Bands (BB) along with derived features to a DataFrame.

    Moving average types (TA-Lib MA_Type):
        0 = SMA  1 = EMA  2 = WMA  3 = DEMA  4 = TEMA
        5 = TRIMA  6 = KAMA  7 = MAMA  8 = T3

    Columns added (per period, e.g. n=20 → base = '{col}_bb_20')
    -------------------------------------------------------------
    Bands:
        {base}_upper                : upper band
        {base}_middle               : middle band (MA)
        {base}_lower                : lower band

    Distance (mode = 'abs' → price−band, 'pct' → (price−band)/band):
        {base}_dist_upper
        {base}_dist_middle
        {base}_dist_lower

    Slope (mode = 'diff' → .diff(), 'pct' → .pct_change()):
        {base}_slope_upper
        {base}_slope_middle
        {base}_slope_lower
        {base}_slope_upper_acceleration     : second diff of upper slope
        {base}_slope_middle_acceleration    : second diff of middle slope
        {base}_slope_lower_acceleration     : second diff of lower slope

    Bandwidth & squeeze:
        {base}_bandwidth            : (upper − lower) / middle  (volatility proxy)
        {base}_bandwidth_slope      : first difference of bandwidth
        {base}_bandwidth_acceleration : second difference of bandwidth

    %B (position within bands):
        {base}_pct_b                : (price − lower) / (upper − lower), 0=lower, 1=upper
        {base}_pct_b_slope          : first difference of %B
        {base}_pct_b_gt_1           : price above upper band
        {base}_pct_b_lt_0           : price below lower band

    Position flags:
        {base}_above_upper          : price > upper band
        {base}_below_lower          : price < lower band
        {base}_inside_bands         : price between upper and lower bands
        {base}_position             : +1 above upper, -1 below lower, 0 inside

    Parameters
    ----------
    df : pd.DataFrame
    n : int or list[int], optional
        BB period(s). If None, defaults to default_bb_periods.
    k : float
        Std dev multiplier for upper/lower bands (default 2.0).
    ma_type : int
        TA-Lib MA type (default 0 = SMA).
    column_name : str
        Source column (default 'close').
    default_bb_periods : list[int]
        Fallback periods when n is None (default [20]).
    distance_mode : str
        'abs' or 'pct' distance from price to bands (default 'pct').
    slope_mode : str
        'diff' or 'pct' slope of bands (default 'diff').
    """

    validate_column(df, column_name)
    df = df.copy()

    if default_bb_periods is None:
        default_bb_periods = [20]

    if n is None:
        periods = default_bb_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    for period in periods:
        upper, middle, lower = talib.BBANDS(
            source,
            timeperiod=period,
            nbdevup=k,
            nbdevdn=k,
            matype=ma_type,
        )

        base = f"{column_name}_bb_{period}"

        # --- bands ---
        df[f"{base}_upper"] = upper
        df[f"{base}_middle"] = middle
        df[f"{base}_lower"] = lower

        # --- distance ---
        if distance_mode == "abs":
            df[f"{base}_dist_upper"] = price - df[f"{base}_upper"]
            df[f"{base}_dist_middle"] = price - df[f"{base}_middle"]
            df[f"{base}_dist_lower"] = price - df[f"{base}_lower"]
        else:  # pct
            df[f"{base}_dist_upper"] = (price - df[f"{base}_upper"]) / df[
                f"{base}_upper"
            ]
            df[f"{base}_dist_middle"] = (price - df[f"{base}_middle"]) / df[
                f"{base}_middle"
            ]
            df[f"{base}_dist_lower"] = (price - df[f"{base}_lower"]) / df[
                f"{base}_lower"
            ]

        # --- slope + acceleration ---
        for band in ("upper", "middle", "lower"):
            band_col = f"{base}_{band}"
            if slope_mode == "pct":
                df[f"{base}_slope_{band}"] = df[band_col].pct_change()
            else:
                df[f"{base}_slope_{band}"] = df[band_col].diff()
            df[f"{base}_slope_{band}_acceleration"] = df[f"{base}_slope_{band}"].diff()

        # --- bandwidth ---
        df[f"{base}_bandwidth"] = (df[f"{base}_upper"] - df[f"{base}_lower"]) / df[
            f"{base}_middle"
        ]
        df[f"{base}_bandwidth_slope"] = df[f"{base}_bandwidth"].diff()
        df[f"{base}_bandwidth_acceleration"] = df[f"{base}_bandwidth_slope"].diff()

        # --- %B ---
        band_range = df[f"{base}_upper"] - df[f"{base}_lower"]
        df[f"{base}_pct_b"] = (price - df[f"{base}_lower"]) / band_range.replace(
            0, float("nan")
        )
        df[f"{base}_pct_b_slope"] = df[f"{base}_pct_b"].diff()
        df[f"{base}_pct_b_gt_1"] = df[f"{base}_pct_b"] > 1
        df[f"{base}_pct_b_lt_0"] = df[f"{base}_pct_b"] < 0

        # --- position flags ---
        df[f"{base}_above_upper"] = price > df[f"{base}_upper"]
        df[f"{base}_below_lower"] = price < df[f"{base}_lower"]
        df[f"{base}_inside_bands"] = (price <= df[f"{base}_upper"]) & (
            price >= df[f"{base}_lower"]
        )
        df[f"{base}_position"] = df.apply(
            lambda r: (
                1
                if r[f"{base}_above_upper"]
                else (-1 if r[f"{base}_below_lower"] else 0)
            ),
            axis=1,
        )

    return df


def add_dema(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add DEMA columns, their slopes, and pairwise DEMA distances.

    Columns added (per period, e.g. n=50 → suffix '_50')
    -------------
    {col}_dema_{n}                      : DEMA value
    {col}_dema_{n}_slope                : first difference of DEMA (momentum)
    {col}_dema_{n}_acceleration         : second difference of DEMA
    {col}_gt_dema_{n}                   : price > DEMA (bullish bias)
    {col}_dema_{n}_dist                 : price - DEMA (signed distance from price)
    {col}_dema_{n}_dist_abs             : |price - DEMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[50,100] → '_50_100')
    ----------------------------------------------------------------
    {col}_dema_{n1}_{n2}_dist           : dema_{n1} - dema_{n2} (signed, fast - slow)
    {col}_dema_{n1}_{n2}_dist_abs       : |dema_{n1} - dema_{n2}|
    {col}_dema_{n1}_{n2}_direction      : +1 if fast > slow, -1 otherwise (trend alignment)
    {col}_dema_{n1}_{n2}_dist_slope     : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    dema_cols = []

    # --- DEMA + per-period derivatives ---
    for window in n:
        dema_col = f"{column_name}_dema_{window}"

        df[dema_col] = talib.DEMA(source, timeperiod=window)
        df[f"{dema_col}_slope"] = df[dema_col].diff()
        df[f"{dema_col}_acceleration"] = df[f"{dema_col}_slope"].diff()
        df[f"{column_name}_gt_dema_{window}"] = df[column_name] > df[dema_col]
        df[f"{dema_col}_dist"] = df[column_name] - df[dema_col]
        df[f"{dema_col}_dist_abs"] = df[f"{dema_col}_dist"].abs()

        dema_cols.append((window, dema_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(dema_cols, 2):
        pair = f"{column_name}_dema_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_ema(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add EMA columns, their slopes, and pairwise EMA distances.

    Columns added (per period, e.g. n=50 → suffix '_50')
    -------------
    {col}_ema_{n}                   : EMA value
    {col}_ema_{n}_slope             : first difference of EMA (momentum)
    {col}_ema_{n}_acceleration      : second difference of EMA
    {col}_gt_ema_{n}                : price > EMA (bullish bias)
    {col}_ema_{n}_dist              : price - EMA (signed distance from price)
    {col}_ema_{n}_dist_abs          : |price - EMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[50,100] → '_50_100')
    ----------------------------------------------------------------
    {col}_ema_{n1}_{n2}_dist        : ema_{n1} - ema_{n2} (signed, fast - slow)
    {col}_ema_{n1}_{n2}_dist_abs    : |ema_{n1} - ema_{n2}|
    {col}_ema_{n1}_{n2}_direction   : +1 if fast > slow, -1 otherwise
    {col}_ema_{n1}_{n2}_dist_slope  : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    ema_cols = []

    # --- EMA + per-period derivatives ---
    for window in n:
        ema_col = f"{column_name}_ema_{window}"

        df[ema_col] = talib.EMA(source, timeperiod=window)
        df[f"{ema_col}_slope"] = df[ema_col].diff()
        df[f"{ema_col}_acceleration"] = df[f"{ema_col}_slope"].diff()
        df[f"{column_name}_gt_ema_{window}"] = df[column_name] > df[ema_col]
        df[f"{ema_col}_dist"] = df[column_name] - df[ema_col]
        df[f"{ema_col}_dist_abs"] = df[f"{ema_col}_dist"].abs()

        ema_cols.append((window, ema_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(ema_cols, 2):
        pair = f"{column_name}_ema_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_kama(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add KAMA (Kaufman Adaptive Moving Average) columns, their slopes,
    and pairwise distances.

    Columns added (per period, e.g. n=50 → suffix '_50')
    -------------
    {col}_kama_{n}                  : KAMA value
    {col}_kama_{n}_slope            : first difference of KAMA (momentum)
    {col}_kama_{n}_acceleration     : second difference of KAMA
    {col}_gt_kama_{n}               : price > KAMA (bullish bias)
    {col}_kama_{n}_dist             : price - KAMA (signed distance from price)
    {col}_kama_{n}_dist_abs         : |price - KAMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[50,100] → '_50_100')
    ----------------------------------------------------------------
    {col}_kama_{n1}_{n2}_dist       : kama_{n1} - kama_{n2} (signed, fast - slow)
    {col}_kama_{n1}_{n2}_dist_abs   : |kama_{n1} - kama_{n2}|
    {col}_kama_{n1}_{n2}_direction  : +1 if fast > slow, -1 otherwise
    {col}_kama_{n1}_{n2}_dist_slope : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    kama_cols = []

    # --- KAMA + per-period derivatives ---
    for window in n:
        kama_col = f"{column_name}_kama_{window}"

        df[kama_col] = talib.KAMA(source, timeperiod=window)
        df[f"{kama_col}_slope"] = df[kama_col].diff()
        df[f"{kama_col}_acceleration"] = df[f"{kama_col}_slope"].diff()
        df[f"{column_name}_gt_kama_{window}"] = df[column_name] > df[kama_col]
        df[f"{kama_col}_dist"] = df[column_name] - df[kama_col]
        df[f"{kama_col}_dist_abs"] = df[f"{kama_col}_dist"].abs()

        kama_cols.append((window, kama_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(kama_cols, 2):
        pair = f"{column_name}_kama_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_midpoint(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add MIDPOINT columns, their slopes, and pairwise distances.

    MIDPOINT = (highest + lowest) / 2 over a lookback period.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    {col}_midpoint_{n}                  : MIDPOINT value
    {col}_midpoint_{n}_slope            : first difference of MIDPOINT (momentum)
    {col}_midpoint_{n}_acceleration     : second difference of MIDPOINT
    {col}_gt_midpoint_{n}               : price > MIDPOINT (bullish bias)
    {col}_midpoint_{n}_dist             : price - MIDPOINT (signed distance from price)
    {col}_midpoint_{n}_dist_abs         : |price - MIDPOINT| (magnitude only)

    Pairwise columns (per combination, e.g. n=[14,50] → '_14_50')
    ----------------------------------------------------------------
    {col}_midpoint_{n1}_{n2}_dist       : midpoint_{n1} - midpoint_{n2} (signed, fast - slow)
    {col}_midpoint_{n1}_{n2}_dist_abs   : |midpoint_{n1} - midpoint_{n2}|
    {col}_midpoint_{n1}_{n2}_direction  : +1 if fast > slow, -1 otherwise
    {col}_midpoint_{n1}_{n2}_dist_slope : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [14, 50, 100]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    midpoint_cols = []

    # --- MIDPOINT + per-period derivatives ---
    for window in n:
        midpoint_col = f"{column_name}_midpoint_{window}"

        df[midpoint_col] = talib.MIDPOINT(source, timeperiod=window)
        df[f"{midpoint_col}_slope"] = df[midpoint_col].diff()
        df[f"{midpoint_col}_acceleration"] = df[f"{midpoint_col}_slope"].diff()
        df[f"{column_name}_gt_midpoint_{window}"] = df[column_name] > df[midpoint_col]
        df[f"{midpoint_col}_dist"] = df[column_name] - df[midpoint_col]
        df[f"{midpoint_col}_dist_abs"] = df[f"{midpoint_col}_dist"].abs()

        midpoint_cols.append((window, midpoint_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(midpoint_cols, 2):
        pair = f"{column_name}_midpoint_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_midprice(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add MIDPRICE columns, their slopes, and pairwise distances.

    MIDPRICE = (highest high + lowest low) / 2 over a lookback period.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    midprice_{n}                  : MIDPRICE value
    midprice_{n}_slope            : first difference of MIDPRICE (momentum)
    midprice_{n}_acceleration     : second difference of MIDPRICE
    close_gt_midprice_{n}         : close > MIDPRICE (bullish bias, if close_col present)
    midprice_{n}_dist             : close - MIDPRICE (signed distance, if close_col present)
    midprice_{n}_dist_abs         : |close - MIDPRICE| (magnitude only, if close_col present)

    Pairwise columns (per combination, e.g. n=[14,50] → '_14_50')
    ----------------------------------------------------------------
    midprice_{n1}_{n2}_dist       : midprice_{n1} - midprice_{n2} (signed, fast - slow)
    midprice_{n1}_{n2}_dist_abs   : |midprice_{n1} - midprice_{n2}|
    midprice_{n1}_{n2}_direction  : +1 if fast > slow, -1 otherwise
    midprice_{n1}_{n2}_dist_slope : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, high_col)
    validate_column(df, low_col)

    if n is None:
        n = [14, 50, 100]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)

    has_close = close_col and close_col in df.columns
    if has_close:
        close = df[close_col]

    midprice_cols = []

    # --- MIDPRICE + per-period derivatives ---
    for window in n:
        midprice_col = f"midprice_{window}"

        df[midprice_col] = talib.MIDPRICE(high, low, timeperiod=window)
        df[f"{midprice_col}_slope"] = df[midprice_col].diff()
        df[f"{midprice_col}_acceleration"] = df[f"{midprice_col}_slope"].diff()

        if has_close:
            df[f"close_gt_midprice_{window}"] = close > df[midprice_col]
            df[f"{midprice_col}_dist"] = close - df[midprice_col]
            df[f"{midprice_col}_dist_abs"] = df[f"{midprice_col}_dist"].abs()

        midprice_cols.append((window, midprice_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(midprice_cols, 2):
        pair = f"midprice_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_sar(
    df: pd.DataFrame,
    acceleration: list[float] = None,
    maximum: list[float] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Parabolic SAR columns and derived features for each (acceleration, maximum) combo.

    Suffix format: sar_{acc}_{max} with dots stripped, e.g. acc=0.02, max=0.2 → 'sar_002_02'

    Columns added (per combo)
    -------------------------
    sar_{s}                         : raw SAR value
    sar_{s}_slope                   : first difference of SAR (momentum)
    sar_{s}_acceleration            : second difference of SAR
    sar_{s}_above                   : SAR > close (bearish — price below SAR)
    sar_{s}_below                   : SAR < close (bullish — price above SAR)
    sar_{s}_direction               : +1 if close > SAR, -1 otherwise
    sar_{s}_dist                    : close - SAR (signed)
    sar_{s}_dist_abs                : |close - SAR|
    sar_{s}_dist_pct                : (close - SAR) / close (normalised)

    Pairwise SAR columns (per combo pair)
    --------------------------------------
    sar_{s1}_{s2}_dist              : sar_{s1} - sar_{s2} (signed)
    sar_{s1}_{s2}_dist_abs          : |sar_{s1} - sar_{s2}|
    sar_{s1}_{s2}_direction         : +1 if sar_{s1} > sar_{s2}, -1 otherwise
    sar_{s1}_{s2}_dist_slope        : first difference of pairwise distance

    Trend agreement (3-period rolling)
    ------------------------------------
    sar_{s}_trend3                  : +1 both price & SAR rising 3 bars, -1 both falling, 0 neutral
    sar_{s}_up3                     : bool — price and SAR both up 3 consecutive bars
    sar_{s}_down3                   : bool — price and SAR both down 3 consecutive bars
    """

    validate_column(df, high_col)
    validate_column(df, low_col)
    validate_column(df, close_col)

    if acceleration is None:
        acceleration = [0.02, 0.04]
    if maximum is None:
        maximum = [0.2]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col]

    sar_cols = []

    def _suffix(acc, max_val) -> str:
        return f"{acc}_{max_val}".replace(".", "")

    # =========================
    # 1. SAR + per-combo derivatives
    # =========================
    price_diff = close.diff()
    price_up3 = (price_diff > 0).rolling(3).sum() == 3
    price_down3 = (price_diff < 0).rolling(3).sum() == 3

    for acc in acceleration:
        for max_val in maximum:
            s = _suffix(acc, max_val)
            sar_col = f"sar_{s}"

            df[sar_col] = talib.SAR(high, low, acceleration=acc, maximum=max_val)
            df[f"{sar_col}_slope"] = df[sar_col].diff()
            df[f"{sar_col}_acceleration"] = df[f"{sar_col}_slope"].diff()

            # --- price vs SAR ---
            dist = close - df[sar_col]
            df[f"{sar_col}_above"] = df[sar_col] > close  # bearish
            df[f"{sar_col}_below"] = df[sar_col] < close  # bullish
            df[f"{sar_col}_direction"] = dist.apply(lambda x: 1 if x > 0 else -1)
            df[f"{sar_col}_dist"] = dist
            df[f"{sar_col}_dist_abs"] = dist.abs()
            df[f"{sar_col}_dist_pct"] = dist / close.replace(0, float("nan"))

            # --- trend agreement ---
            sar_diff = df[sar_col].diff()
            sar_up3 = (sar_diff > 0).rolling(3).sum() == 3
            sar_down3 = (sar_diff < 0).rolling(3).sum() == 3

            df[f"{sar_col}_up3"] = price_up3 & sar_up3
            df[f"{sar_col}_down3"] = price_down3 & sar_down3
            trend_col = f"{sar_col}_trend3"
            df[trend_col] = 0
            df.loc[price_up3 & sar_up3, trend_col] = 1
            df.loc[price_down3 & sar_down3, trend_col] = -1

            sar_cols.append((s, sar_col))

    # =========================
    # 2. Pairwise SAR distances
    # =========================
    for (s1, col1), (s2, col2) in combinations(sar_cols, 2):
        pair = f"sar_{s1}_{s2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_sma(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add SMA columns, their slopes, and pairwise SMA distances.

    Columns added (per period, e.g. n=50 → suffix '_50')
    -------------
    {col}_sma_{n}                   : SMA value
    {col}_sma_{n}_slope             : first difference of SMA (momentum)
    {col}_sma_{n}_acceleration      : second difference of SMA
    {col}_gt_sma_{n}                : price > SMA (bullish bias)
    {col}_sma_{n}_dist              : price - SMA (signed distance from price)
    {col}_sma_{n}_dist_abs          : |price - SMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[50,100] → '_50_100')
    ----------------------------------------------------------------
    {col}_sma_{n1}_{n2}_dist        : sma_{n1} - sma_{n2} (signed, fast - slow)
    {col}_sma_{n1}_{n2}_dist_abs    : |sma_{n1} - sma_{n2}|
    {col}_sma_{n1}_{n2}_direction   : +1 if fast > slow, -1 otherwise
    {col}_sma_{n1}_{n2}_dist_slope  : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    sma_cols = []

    # --- SMA + per-period derivatives ---
    for window in n:
        sma_col = f"{column_name}_sma_{window}"

        df[sma_col] = talib.SMA(source, timeperiod=window)
        df[f"{sma_col}_slope"] = df[sma_col].diff()
        df[f"{sma_col}_acceleration"] = df[f"{sma_col}_slope"].diff()
        df[f"{column_name}_gt_sma_{window}"] = df[column_name] > df[sma_col]
        df[f"{sma_col}_dist"] = df[column_name] - df[sma_col]
        df[f"{sma_col}_dist_abs"] = df[f"{sma_col}_dist"].abs()

        sma_cols.append((window, sma_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(sma_cols, 2):
        pair = f"{column_name}_sma_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_t3(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
    vfactor: float = 0.7,
) -> pd.DataFrame:
    """
    Add T3 columns, their slopes, and pairwise T3 distances.

    T3 is a smoothed MA using 6 EMAs weighted by vfactor (default 0.7).
    Lower vfactor → closer to EMA; higher → more smoothing with less lag.

    Columns added (per period, e.g. n=5 → suffix '_5')
    -------------
    {col}_t3_{n}                    : T3 value
    {col}_t3_{n}_slope              : first difference of T3 (momentum)
    {col}_t3_{n}_acceleration       : second difference of T3
    {col}_gt_t3_{n}                 : price > T3 (bullish bias)
    {col}_t3_{n}_dist               : price - T3 (signed distance from price)
    {col}_t3_{n}_dist_abs           : |price - T3| (magnitude only)

    Pairwise columns (per combination, e.g. n=[5,10] → '_5_10')
    ----------------------------------------------------------------
    {col}_t3_{n1}_{n2}_dist         : t3_{n1} - t3_{n2} (signed, fast - slow)
    {col}_t3_{n1}_{n2}_dist_abs     : |t3_{n1} - t3_{n2}|
    {col}_t3_{n1}_{n2}_direction    : +1 if fast > slow, -1 otherwise
    {col}_t3_{n1}_{n2}_dist_slope   : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [5]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    t3_cols = []

    # --- T3 + per-period derivatives ---
    for window in n:
        t3_col = f"{column_name}_t3_{window}"

        df[t3_col] = talib.T3(source, timeperiod=window, vfactor=vfactor)
        df[f"{t3_col}_slope"] = df[t3_col].diff()
        df[f"{t3_col}_acceleration"] = df[f"{t3_col}_slope"].diff()
        df[f"{column_name}_gt_t3_{window}"] = df[column_name] > df[t3_col]
        df[f"{t3_col}_dist"] = df[column_name] - df[t3_col]
        df[f"{t3_col}_dist_abs"] = df[f"{t3_col}_dist"].abs()

        t3_cols.append((window, t3_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(t3_cols, 2):
        pair = f"{column_name}_t3_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_tema(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add TEMA columns, their slopes, and pairwise TEMA distances.

    TEMA = 3*EMA - 3*EMA(EMA) + EMA(EMA(EMA)) — reduced lag vs EMA/DEMA.

    Columns added (per period, e.g. n=30 → suffix '_30')
    -------------
    {col}_tema_{n}                  : TEMA value
    {col}_tema_{n}_slope            : first difference of TEMA (momentum)
    {col}_tema_{n}_acceleration     : second difference of TEMA
    {col}_gt_tema_{n}               : price > TEMA (bullish bias)
    {col}_tema_{n}_dist             : price - TEMA (signed distance from price)
    {col}_tema_{n}_dist_abs         : |price - TEMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[30,60] → '_30_60')
    ----------------------------------------------------------------
    {col}_tema_{n1}_{n2}_dist       : tema_{n1} - tema_{n2} (signed, fast - slow)
    {col}_tema_{n1}_{n2}_dist_abs   : |tema_{n1} - tema_{n2}|
    {col}_tema_{n1}_{n2}_direction  : +1 if fast > slow, -1 otherwise
    {col}_tema_{n1}_{n2}_dist_slope : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [30]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    tema_cols = []

    # --- TEMA + per-period derivatives ---
    for window in n:
        tema_col = f"{column_name}_tema_{window}"

        df[tema_col] = talib.TEMA(source, timeperiod=window)
        df[f"{tema_col}_slope"] = df[tema_col].diff()
        df[f"{tema_col}_acceleration"] = df[f"{tema_col}_slope"].diff()
        df[f"{column_name}_gt_tema_{window}"] = df[column_name] > df[tema_col]
        df[f"{tema_col}_dist"] = df[column_name] - df[tema_col]
        df[f"{tema_col}_dist_abs"] = df[f"{tema_col}_dist"].abs()

        tema_cols.append((window, tema_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(tema_cols, 2):
        pair = f"{column_name}_tema_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_trima(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add TRIMA columns, their slopes, and pairwise TRIMA distances.

    TRIMA = double-smoothed SMA (SMA of SMA) — smoother than SMA with more lag.

    Columns added (per period, e.g. n=30 → suffix '_30')
    -------------
    {col}_trima_{n}                  : TRIMA value
    {col}_trima_{n}_slope            : first difference of TRIMA (momentum)
    {col}_trima_{n}_acceleration     : second difference of TRIMA
    {col}_gt_trima_{n}               : price > TRIMA (bullish bias)
    {col}_trima_{n}_dist             : price - TRIMA (signed distance from price)
    {col}_trima_{n}_dist_abs         : |price - TRIMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[30,60] → '_30_60')
    ----------------------------------------------------------------
    {col}_trima_{n1}_{n2}_dist       : trima_{n1} - trima_{n2} (signed, fast - slow)
    {col}_trima_{n1}_{n2}_dist_abs   : |trima_{n1} - trima_{n2}|
    {col}_trima_{n1}_{n2}_direction  : +1 if fast > slow, -1 otherwise
    {col}_trima_{n1}_{n2}_dist_slope : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [30]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    trima_cols = []

    # --- TRIMA + per-period derivatives ---
    for window in n:
        trima_col = f"{column_name}_trima_{window}"

        df[trima_col] = talib.TRIMA(source, timeperiod=window)
        df[f"{trima_col}_slope"] = df[trima_col].diff()
        df[f"{trima_col}_acceleration"] = df[f"{trima_col}_slope"].diff()
        df[f"{column_name}_gt_trima_{window}"] = df[column_name] > df[trima_col]
        df[f"{trima_col}_dist"] = df[column_name] - df[trima_col]
        df[f"{trima_col}_dist_abs"] = df[f"{trima_col}_dist"].abs()

        trima_cols.append((window, trima_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(trima_cols, 2):
        pair = f"{column_name}_trima_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


def add_wma(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add WMA (Weighted Moving Average) columns, their slopes,
    and pairwise WMA distances.

    WMA applies a linearly increasing weight to each period
    (most recent bar gets highest weight). Computed via talib.WMA.

    Note: the previous implementation used ewm(alpha=1/period) which
    is Wilder's Smoothed MA (SMMA/RMA) — a different indicator entirely.

    Columns added (per period, e.g. n=50 → suffix '_50')
    -------------
    {col}_wma_{n}                   : WMA value
    {col}_wma_{n}_slope             : first difference of WMA (momentum)
    {col}_wma_{n}_acceleration      : second difference of WMA
    {col}_gt_wma_{n}                : price > WMA (bullish bias)
    {col}_wma_{n}_dist              : price - WMA (signed distance from price)
    {col}_wma_{n}_dist_abs          : |price - WMA| (magnitude only)

    Pairwise columns (per combination, e.g. n=[50,100] → '_50_100')
    ----------------------------------------------------------------
    {col}_wma_{n1}_{n2}_dist        : wma_{n1} - wma_{n2} (signed, fast - slow)
    {col}_wma_{n1}_{n2}_dist_abs    : |wma_{n1} - wma_{n2}|
    {col}_wma_{n1}_{n2}_direction   : +1 if fast > slow, -1 otherwise
    {col}_wma_{n1}_{n2}_dist_slope  : first difference of pairwise distance (crossover momentum)
    """

    validate_column(df, column_name)

    if n is None:
        n = [7, 14, 21, 50, 100]
    elif isinstance(n, int):
        n = [n]
    else:
        n = list(n)

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    wma_cols = []

    # --- WMA + per-period derivatives ---
    for period in n:
        wma_col = f"{column_name}_wma_{period}"

        df[wma_col] = talib.WMA(source, timeperiod=period)
        df[f"{wma_col}_slope"] = df[wma_col].diff()
        df[f"{wma_col}_acceleration"] = df[f"{wma_col}_slope"].diff()
        df[f"{column_name}_gt_wma_{period}"] = df[column_name] > df[wma_col]
        df[f"{wma_col}_dist"] = df[column_name] - df[wma_col]
        df[f"{wma_col}_dist_abs"] = df[f"{wma_col}_dist"].abs()

        wma_cols.append((period, wma_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(wma_cols, 2):
        pair = f"{column_name}_wma_{w1}_{w2}"

        df[f"{pair}_dist"] = df[col1] - df[col2]
        df[f"{pair}_dist_abs"] = df[f"{pair}_dist"].abs()
        df[f"{pair}_direction"] = df[f"{pair}_dist"].apply(lambda x: 1 if x > 0 else -1)
        df[f"{pair}_dist_slope"] = df[f"{pair}_dist"].diff()

    return df


# endregion OVERLAP STUDIES


# region MOMENTUM INDICATORS
def add_adx(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add ADX, DI lines, and derived signal columns for each period in n.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    adx_{n}                 : raw ADX value
    adx_{n}_gt_20           : ADX > 20 (weak trend threshold)
    adx_{n}_gt_25           : ADX > 25 (strong trend threshold)
    adx_{n}_slope           : first difference of ADX (momentum)
    adx_{n}_acceleration    : second difference of ADX (rate of change of momentum)
    plus_di_{n}             : +DI line
    minus_di_{n}            : -DI line
    plus_di_{n}_slope       : first difference of +DI
    minus_di_{n}_slope      : first difference of -DI
    di_{n}_distance         : +DI - -DI  (signed, positive = bullish bias)
    di_{n}_distance_abs     : |+DI - -DI| (magnitude only)
    di_{n}_ratio            : +DI / -DI  (NaN-safe)
    trend_{n}_direction     : +1 if +DI > -DI, -1 otherwise
    adx_{n}_di_strength     : ADX x di_distance_abs (combines trend strength + clarity)
    """

    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"  # suffix

        # --- core indicators ---
        df[f"adx{s}"] = talib.ADX(high, low, close, timeperiod=period)
        df[f"plus_di{s}"] = talib.PLUS_DI(high, low, close, timeperiod=period)
        df[f"minus_di{s}"] = talib.MINUS_DI(high, low, close, timeperiod=period)

        # --- ADX derivatives ---
        df[f"adx{s}_gt_20"] = df[f"adx{s}"] > 20
        df[f"adx{s}_gt_25"] = df[f"adx{s}"] > 25
        df[f"adx{s}_slope"] = df[f"adx{s}"].diff()
        df[f"adx{s}_acceleration"] = df[f"adx{s}_slope"].diff()

        # --- DI slopes ---
        df[f"plus_di{s}_slope"] = df[f"plus_di{s}"].diff()
        df[f"minus_di{s}_slope"] = df[f"minus_di{s}"].diff()

        # --- DI relationship ---
        df[f"di{s}_distance"] = df[f"plus_di{s}"] - df[f"minus_di{s}"]
        df[f"di{s}_distance_abs"] = df[f"di{s}_distance"].abs()
        df[f"di{s}_ratio"] = df[f"plus_di{s}"] / df[f"minus_di{s}"].replace(
            0, float("nan")
        )
        df[f"trend{s}_direction"] = df[f"di{s}_distance"].apply(
            lambda x: 1 if x > 0 else -1
        )

        # --- combined strength signal ---
        df[f"adx{s}_di_strength"] = df[f"adx{s}"] * df[f"di{s}_distance_abs"]

    return df


def add_aroon(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
) -> pd.DataFrame:
    """
    Add Aroon indicator columns for each period in n.

    Columns added (per period, e.g. n=25 → suffix '_25')
    -------------
    aroon_up_{n}            : Aroon Up line (0–100)
    aroon_down_{n}          : Aroon Down line (0–100)
    aroon_osc_{n}           : Aroon Oscillator = Up - Down (-100 to +100)
    aroon_up_{n}_slope      : first difference of Aroon Up
    aroon_down_{n}_slope    : first difference of Aroon Down
    aroon_osc_{n}_slope     : first difference of Aroon Oscillator
    aroon_{n}_distance      : Aroon Up - Aroon Down (signed, positive = bullish)
    aroon_{n}_distance_abs  : |Aroon Up - Aroon Down| (magnitude only)
    aroon_{n}_ratio         : Aroon Up / Aroon Down (NaN-safe)
    aroon_{n}_direction     : +1 if Up > Down, -1 otherwise
    aroon_up_{n}_gt_70      : Aroon Up > 70 (strong uptrend signal)
    aroon_down_{n}_gt_70    : Aroon Down > 70 (strong downtrend signal)
    aroon_up_{n}_lt_30      : Aroon Up < 30 (weak uptrend / trend absent)
    aroon_down_{n}_lt_30    : Aroon Down < 30 (weak downtrend / trend absent)
    aroon_{n}_strength      : |osc| × distance_abs (combined conviction score)
    """

    for col in (high_col, low_col):
        validate_column(df, col)

    if n is None:
        n = [25]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"  # suffix

        # --- core indicators ---
        aroon_down, aroon_up = talib.AROON(high, low, timeperiod=period)
        df[f"aroon_up{s}"] = aroon_up
        df[f"aroon_down{s}"] = aroon_down
        df[f"aroon_osc{s}"] = talib.AROONOSC(high, low, timeperiod=period)

        # --- slopes ---
        df[f"aroon_up{s}_slope"] = df[f"aroon_up{s}"].diff()
        df[f"aroon_down{s}_slope"] = df[f"aroon_down{s}"].diff()
        df[f"aroon_osc{s}_slope"] = df[f"aroon_osc{s}"].diff()

        # --- up/down relationship ---
        df[f"aroon{s}_distance"] = df[f"aroon_up{s}"] - df[f"aroon_down{s}"]
        df[f"aroon{s}_distance_abs"] = df[f"aroon{s}_distance"].abs()
        df[f"aroon{s}_ratio"] = df[f"aroon_up{s}"] / df[f"aroon_down{s}"].replace(
            0, float("nan")
        )
        df[f"aroon{s}_direction"] = df[f"aroon{s}_distance"].apply(
            lambda x: 1 if x > 0 else -1
        )

        # --- threshold flags ---
        df[f"aroon_up{s}_gt_70"] = df[f"aroon_up{s}"] > 70
        df[f"aroon_down{s}_gt_70"] = df[f"aroon_down{s}"] > 70
        df[f"aroon_up{s}_lt_30"] = df[f"aroon_up{s}"] < 30
        df[f"aroon_down{s}_lt_30"] = df[f"aroon_down{s}"] < 30

        # --- combined conviction score ---
        df[f"aroon{s}_strength"] = (
            df[f"aroon_osc{s}"].abs() * df[f"aroon{s}_distance_abs"]
        )

    return df


def add_bop(
    df: pd.DataFrame,
    n: list[int] = None,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Balance of Power (BOP) indicator columns.

    BOP has no timeperiod parameter — n defines smoothing windows
    applied to the raw BOP signal (like a signal line).

    Base columns (computed once)
    ----------------------------
    bop                     : raw BOP value (open–close / high–low), range [-1, +1]
    bop_slope               : first difference of raw BOP
    bop_acceleration        : second difference of raw BOP
    bop_gt_0                : BOP > 0 (buyers in control)
    bop_lt_0                : BOP < 0 (sellers in control)
    bop_abs                 : |BOP| (conviction magnitude regardless of direction)
    bop_direction           : +1 if BOP > 0, -1 otherwise

    Per smoothing window (e.g. n=14 → suffix '_14')
    ------------------------------------------------
    bop_signal_{n}          : SMA of raw BOP over n periods
    bop_signal_{n}_slope    : first difference of signal line
    bop_hist_{n}            : bop - bop_signal (raw vs smoothed divergence)
    bop_hist_{n}_slope      : first difference of histogram
    bop_hist_{n}_gt_0       : histogram > 0 (momentum turning bullish)
    bop_hist_{n}_lt_0       : histogram < 0 (momentum turning bearish)
    bop_{n}_strength        : bop_abs × |bop_hist| (conviction × divergence score)
    """

    for col in (open_col, high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    open_ = df[open_col].to_numpy(dtype=float)
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    # --- raw BOP (computed once, no period) ---
    df["bop"] = talib.BOP(open_, high, low, close)

    # --- base derivatives ---
    df["bop_slope"] = df["bop"].diff()
    df["bop_acceleration"] = df["bop_slope"].diff()
    df["bop_gt_0"] = df["bop"] > 0
    df["bop_lt_0"] = df["bop"] < 0
    df["bop_abs"] = df["bop"].abs()
    df["bop_direction"] = df["bop"].apply(lambda x: 1 if x > 0 else -1)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"bop_signal{s}"] = df["bop"].rolling(window=period).mean()
        df[f"bop_signal{s}_slope"] = df[f"bop_signal{s}"].diff()

        df[f"bop_hist{s}"] = df["bop"] - df[f"bop_signal{s}"]
        df[f"bop_hist{s}_slope"] = df[f"bop_hist{s}"].diff()
        df[f"bop_hist{s}_gt_0"] = df[f"bop_hist{s}"] > 0
        df[f"bop_hist{s}_lt_0"] = df[f"bop_hist{s}"] < 0

        df[f"bop{s}_strength"] = df["bop_abs"] * df[f"bop_hist{s}"].abs()

    return df


def add_cci(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Commodity Channel Index (CCI) columns for each period in n.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    cci_{n}                 : raw CCI value
    cci_{n}_slope           : first difference of CCI (momentum)
    cci_{n}_acceleration    : second difference of CCI (rate of change of momentum)
    cci_{n}_gt_100          : CCI > +100 (overbought / strong bullish trend)
    cci_{n}_lt_minus100     : CCI < -100 (oversold / strong bearish trend)
    cci_{n}_gt_0            : CCI > 0 (above zero-line, bullish bias)
    cci_{n}_lt_0            : CCI < 0 (below zero-line, bearish bias)
    cci_{n}_abs             : |CCI| (magnitude regardless of direction)
    cci_{n}_direction       : +1 if CCI > 0, -1 otherwise
    cci_{n}_extreme         : +1 if CCI > +100, -1 if CCI < -100, 0 if between
    cci_{n}_signal          : SMA of CCI over same period (smoothed signal line)
    cci_{n}_signal_slope    : first difference of signal line
    cci_{n}_hist            : CCI - signal (raw vs smoothed divergence)
    cci_{n}_hist_slope      : first difference of histogram
    cci_{n}_hist_gt_0       : histogram > 0 (momentum turning bullish)
    cci_{n}_hist_lt_0       : histogram < 0 (momentum turning bearish)
    cci_{n}_strength        : cci_abs × |cci_hist| (conviction × divergence score)
    """

    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"cci{s}"] = talib.CCI(high, low, close, timeperiod=period)

        # --- derivatives ---
        df[f"cci{s}_slope"] = df[f"cci{s}"].diff()
        df[f"cci{s}_acceleration"] = df[f"cci{s}_slope"].diff()

        # --- threshold flags ---
        df[f"cci{s}_gt_100"] = df[f"cci{s}"] > 100
        df[f"cci{s}_lt_minus100"] = df[f"cci{s}"] < -100
        df[f"cci{s}_gt_0"] = df[f"cci{s}"] > 0
        df[f"cci{s}_lt_0"] = df[f"cci{s}"] < 0

        # --- magnitude & direction ---
        df[f"cci{s}_abs"] = df[f"cci{s}"].abs()
        df[f"cci{s}_direction"] = df[f"cci{s}"].apply(lambda x: 1 if x > 0 else -1)
        df[f"cci{s}_extreme"] = df[f"cci{s}"].apply(
            lambda x: 1 if x > 100 else (-1 if x < -100 else 0)
        )

        # --- signal line & histogram ---
        df[f"cci{s}_signal"] = df[f"cci{s}"].rolling(window=period).mean()
        df[f"cci{s}_signal_slope"] = df[f"cci{s}_signal"].diff()
        df[f"cci{s}_hist"] = df[f"cci{s}"] - df[f"cci{s}_signal"]
        df[f"cci{s}_hist_slope"] = df[f"cci{s}_hist"].diff()
        df[f"cci{s}_hist_gt_0"] = df[f"cci{s}_hist"] > 0
        df[f"cci{s}_hist_lt_0"] = df[f"cci{s}_hist"] < 0

        # --- combined conviction score ---
        df[f"cci{s}_strength"] = df[f"cci{s}_abs"] * df[f"cci{s}_hist"].abs()

    return df


def add_cmo(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add Chande Momentum Oscillator (CMO) columns for each period in n.

    CMO measures momentum as (sum of up days - sum of down days) / total sum × 100.
    Range is -100 to +100. Unlike RSI, it uses both up and down days in the denominator.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    cmo_{n}                     : raw CMO value (-100 to +100)
    cmo_{n}_slope               : first difference of CMO (momentum)
    cmo_{n}_acceleration        : second difference of CMO
    cmo_{n}_abs                 : |CMO| (conviction magnitude regardless of direction)
    cmo_{n}_direction           : +1 if CMO > 0, -1 otherwise
    cmo_{n}_gt_50               : CMO > +50 (strong bullish momentum)
    cmo_{n}_lt_minus50          : CMO < -50 (strong bearish momentum)
    cmo_{n}_gt_0                : CMO > 0 (bullish bias)
    cmo_{n}_lt_0                : CMO < 0 (bearish bias)
    cmo_{n}_extreme             : +1 if CMO > +50, -1 if CMO < -50, 0 if between
    cmo_{n}_signal              : SMA of CMO over same period (smoothed signal line)
    cmo_{n}_signal_slope        : first difference of signal line
    cmo_{n}_hist                : CMO - signal (raw vs smoothed divergence)
    cmo_{n}_hist_slope          : first difference of histogram
    cmo_{n}_hist_gt_0           : histogram > 0 (momentum turning bullish)
    cmo_{n}_hist_lt_0           : histogram < 0 (momentum turning bearish)
    cmo_{n}_strength            : cmo_abs × |cmo_hist| (conviction × divergence score)
    """

    validate_column(df, column_name)

    if n is None:
        n = [14]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"cmo{s}"] = talib.CMO(source, timeperiod=period)

        # --- derivatives ---
        df[f"cmo{s}_slope"] = df[f"cmo{s}"].diff()
        df[f"cmo{s}_acceleration"] = df[f"cmo{s}_slope"].diff()
        df[f"cmo{s}_abs"] = df[f"cmo{s}"].abs()
        df[f"cmo{s}_direction"] = df[f"cmo{s}"].apply(lambda x: 1 if x > 0 else -1)

        # --- threshold flags ---
        df[f"cmo{s}_gt_50"] = df[f"cmo{s}"] > 50
        df[f"cmo{s}_lt_minus50"] = df[f"cmo{s}"] < -50
        df[f"cmo{s}_gt_0"] = df[f"cmo{s}"] > 0
        df[f"cmo{s}_lt_0"] = df[f"cmo{s}"] < 0
        df[f"cmo{s}_extreme"] = df[f"cmo{s}"].apply(
            lambda x: 1 if x > 50 else (-1 if x < -50 else 0)
        )

        # --- signal line & histogram ---
        df[f"cmo{s}_signal"] = df[f"cmo{s}"].rolling(window=period).mean()
        df[f"cmo{s}_signal_slope"] = df[f"cmo{s}_signal"].diff()
        df[f"cmo{s}_hist"] = df[f"cmo{s}"] - df[f"cmo{s}_signal"]
        df[f"cmo{s}_hist_slope"] = df[f"cmo{s}_hist"].diff()
        df[f"cmo{s}_hist_gt_0"] = df[f"cmo{s}_hist"] > 0
        df[f"cmo{s}_hist_lt_0"] = df[f"cmo{s}_hist"] < 0

        # --- combined conviction score ---
        df[f"cmo{s}_strength"] = df[f"cmo{s}_abs"] * df[f"cmo{s}_hist"].abs()

    return df


def add_macd(
    df: pd.DataFrame,
    fast: list[int] = None,
    slow: list[int] = None,
    signal: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add MACD columns for each (fast, slow, signal) combination.

    MACD = EMA(fast) - EMA(slow). Signal = EMA(MACD, signal). Hist = MACD - Signal.

    Suffix format: macd_{fast}_{slow}_{signal}, e.g. (12, 26, 9) → 'macd_12_26_9'

    Columns added (per combo)
    -------------------------
    macd_{f}_{sl}_{sg}                  : MACD line (fast EMA - slow EMA)
    macd_{f}_{sl}_{sg}_slope            : first difference of MACD line
    macd_{f}_{sl}_{sg}_acceleration     : second difference of MACD line
    macd_{f}_{sl}_{sg}_abs              : |MACD| (magnitude regardless of direction)
    macd_{f}_{sl}_{sg}_direction        : +1 if MACD > 0, -1 otherwise
    macd_{f}_{sl}_{sg}_gt_0             : MACD > 0 (bullish bias)
    macd_{f}_{sl}_{sg}_lt_0             : MACD < 0 (bearish bias)

    macd_{f}_{sl}_{sg}_signal           : signal line (EMA of MACD)
    macd_{f}_{sl}_{sg}_signal_slope     : first difference of signal line
    macd_{f}_{sl}_{sg}_signal_gt_0      : signal line > 0
    macd_{f}_{sl}_{sg}_signal_lt_0      : signal line < 0

    macd_{f}_{sl}_{sg}_hist             : histogram (MACD - signal)
    macd_{f}_{sl}_{sg}_hist_slope       : first difference of histogram (momentum shift)
    macd_{f}_{sl}_{sg}_hist_acceleration: second difference of histogram
    macd_{f}_{sl}_{sg}_hist_gt_0        : histogram > 0 (bullish momentum)
    macd_{f}_{sl}_{sg}_hist_lt_0        : histogram < 0 (bearish momentum)
    macd_{f}_{sl}_{sg}_hist_abs         : |histogram| (conviction magnitude)

    macd_{f}_{sl}_{sg}_cross_above      : MACD crossed above signal this bar (golden cross)
    macd_{f}_{sl}_{sg}_cross_below      : MACD crossed below signal this bar (death cross)
    macd_{f}_{sl}_{sg}_strength         : macd_abs × hist_abs (trend strength × momentum conviction)
    """

    validate_column(df, column_name)

    if fast is None:
        fast = [12]
    if slow is None:
        slow = [26]
    if signal is None:
        signal = [9]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for f, sl, sg in product(fast, slow, signal):
        if f >= sl:
            continue  # fast must be strictly less than slow

        s = f"_{f}_{sl}_{sg}"

        # --- core indicator ---
        macd_line, signal_line, hist = talib.MACD(
            source,
            fastperiod=f,
            slowperiod=sl,
            signalperiod=sg,
        )

        # --- MACD line ---
        df[f"macd{s}"] = macd_line
        df[f"macd{s}_slope"] = pd.Series(macd_line).diff().values
        df[f"macd{s}_acceleration"] = pd.Series(df[f"macd{s}_slope"]).diff().values
        df[f"macd{s}_abs"] = np.abs(macd_line)
        df[f"macd{s}_direction"] = np.where(macd_line > 0, 1, -1)
        df[f"macd{s}_gt_0"] = macd_line > 0
        df[f"macd{s}_lt_0"] = macd_line < 0

        # --- signal line ---
        df[f"macd{s}_signal"] = signal_line
        df[f"macd{s}_signal_slope"] = pd.Series(signal_line).diff().values
        df[f"macd{s}_signal_gt_0"] = signal_line > 0
        df[f"macd{s}_signal_lt_0"] = signal_line < 0

        # --- histogram ---
        df[f"macd{s}_hist"] = hist
        df[f"macd{s}_hist_slope"] = pd.Series(hist).diff().values
        df[f"macd{s}_hist_acceleration"] = (
            pd.Series(df[f"macd{s}_hist_slope"]).diff().values
        )
        df[f"macd{s}_hist_gt_0"] = hist > 0
        df[f"macd{s}_hist_lt_0"] = hist < 0
        df[f"macd{s}_hist_abs"] = np.abs(hist)

        # --- crossover signals ---
        prev_hist = pd.Series(hist).shift(1)
        df[f"macd{s}_cross_above"] = (pd.Series(hist) > 0) & (prev_hist <= 0)
        df[f"macd{s}_cross_below"] = (pd.Series(hist) < 0) & (prev_hist >= 0)

        # --- combined conviction score ---
        df[f"macd{s}_strength"] = df[f"macd{s}_abs"] * df[f"macd{s}_hist_abs"]

    return df


def add_mfi(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:
    """
    Add Money Flow Index (MFI) columns for each period in n.

    MFI is a volume-weighted RSI — it measures buying and selling pressure
    using both price and volume. Range is 0 to 100.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    mfi_{n}                     : raw MFI value (0 to 100)
    mfi_{n}_slope               : first difference of MFI (momentum)
    mfi_{n}_acceleration        : second difference of MFI
    mfi_{n}_abs                 : |MFI - 50| (deviation from neutral midpoint)
    mfi_{n}_direction           : +1 if MFI > 50, -1 otherwise
    mfi_{n}_gt_80               : MFI > 80 (overbought)
    mfi_{n}_lt_20               : MFI < 20 (oversold)
    mfi_{n}_gt_50               : MFI > 50 (bullish bias)
    mfi_{n}_lt_50               : MFI < 50 (bearish bias)
    mfi_{n}_extreme             : +1 if MFI > 80, -1 if MFI < 20, 0 if between
    mfi_{n}_signal              : SMA of MFI over same period (smoothed signal line)
    mfi_{n}_signal_slope        : first difference of signal line
    mfi_{n}_hist                : MFI - signal (raw vs smoothed divergence)
    mfi_{n}_hist_slope          : first difference of histogram
    mfi_{n}_hist_gt_0           : histogram > 0 (momentum turning bullish)
    mfi_{n}_hist_lt_0           : histogram < 0 (momentum turning bearish)
    mfi_{n}_strength            : mfi_abs × |mfi_hist| (deviation × divergence score)
    """

    for col in (high_col, low_col, close_col, volume_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"mfi{s}"] = talib.MFI(high, low, close, volume, timeperiod=period)

        # --- derivatives ---
        df[f"mfi{s}_slope"] = df[f"mfi{s}"].diff()
        df[f"mfi{s}_acceleration"] = df[f"mfi{s}_slope"].diff()
        df[f"mfi{s}_abs"] = (df[f"mfi{s}"] - 50).abs()  # deviation from neutral
        df[f"mfi{s}_direction"] = df[f"mfi{s}"].apply(lambda x: 1 if x > 50 else -1)

        # --- threshold flags ---
        df[f"mfi{s}_gt_80"] = df[f"mfi{s}"] > 80
        df[f"mfi{s}_lt_20"] = df[f"mfi{s}"] < 20
        df[f"mfi{s}_gt_50"] = df[f"mfi{s}"] > 50
        df[f"mfi{s}_lt_50"] = df[f"mfi{s}"] < 50
        df[f"mfi{s}_extreme"] = df[f"mfi{s}"].apply(
            lambda x: 1 if x > 80 else (-1 if x < 20 else 0)
        )

        # --- signal line & histogram ---
        df[f"mfi{s}_signal"] = df[f"mfi{s}"].rolling(window=period).mean()
        df[f"mfi{s}_signal_slope"] = df[f"mfi{s}_signal"].diff()
        df[f"mfi{s}_hist"] = df[f"mfi{s}"] - df[f"mfi{s}_signal"]
        df[f"mfi{s}_hist_slope"] = df[f"mfi{s}_hist"].diff()
        df[f"mfi{s}_hist_gt_0"] = df[f"mfi{s}_hist"] > 0
        df[f"mfi{s}_hist_lt_0"] = df[f"mfi{s}_hist"] < 0

        # --- combined conviction score ---
        df[f"mfi{s}_strength"] = df[f"mfi{s}_abs"] * df[f"mfi{s}_hist"].abs()

    return df


def add_mom(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add Momentum (MOM) columns for each period in n.

    MOM = close - close[n periods ago]. Unbounded — scale depends on price level.
    Positive = price higher than n bars ago (bullish), negative = lower (bearish).

    Columns added (per period, e.g. n=10 → suffix '_10')
    -------------
    mom_{n}                     : raw MOM value
    mom_{n}_slope               : first difference of MOM (acceleration)
    mom_{n}_acceleration        : second difference of MOM (jerk)
    mom_{n}_abs                 : |MOM| (magnitude regardless of direction)
    mom_{n}_direction           : +1 if MOM > 0, -1 otherwise
    mom_{n}_gt_0                : MOM > 0 (price higher than n bars ago)
    mom_{n}_lt_0                : MOM < 0 (price lower than n bars ago)
    mom_{n}_pct                 : MOM / close[n periods ago] (normalised, scale-free)
    mom_{n}_pct_slope           : first difference of normalised MOM
    mom_{n}_signal              : SMA of MOM over same period (smoothed signal line)
    mom_{n}_signal_slope        : first difference of signal line
    mom_{n}_hist                : MOM - signal (raw vs smoothed divergence)
    mom_{n}_hist_slope          : first difference of histogram
    mom_{n}_hist_acceleration   : second difference of histogram
    mom_{n}_hist_gt_0           : histogram > 0 (momentum turning bullish)
    mom_{n}_hist_lt_0           : histogram < 0 (momentum turning bearish)
    mom_{n}_hist_abs            : |histogram| (divergence magnitude)
    mom_{n}_strength            : mom_abs × hist_abs (raw momentum × divergence score)
    """

    validate_column(df, column_name)

    if n is None:
        n = [10]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"mom{s}"] = talib.MOM(source, timeperiod=period)

        # --- derivatives ---
        df[f"mom{s}_slope"] = df[f"mom{s}"].diff()
        df[f"mom{s}_acceleration"] = df[f"mom{s}_slope"].diff()
        df[f"mom{s}_abs"] = df[f"mom{s}"].abs()
        df[f"mom{s}_direction"] = df[f"mom{s}"].apply(lambda x: 1 if x > 0 else -1)

        # --- zero-line flags ---
        df[f"mom{s}_gt_0"] = df[f"mom{s}"] > 0
        df[f"mom{s}_lt_0"] = df[f"mom{s}"] < 0

        # --- normalised momentum (scale-free) ---
        lagged = price.shift(period).replace(0, float("nan"))
        df[f"mom{s}_pct"] = df[f"mom{s}"] / lagged
        df[f"mom{s}_pct_slope"] = df[f"mom{s}_pct"].diff()

        # --- signal line & histogram ---
        df[f"mom{s}_signal"] = df[f"mom{s}"].rolling(window=period).mean()
        df[f"mom{s}_signal_slope"] = df[f"mom{s}_signal"].diff()
        df[f"mom{s}_hist"] = df[f"mom{s}"] - df[f"mom{s}_signal"]
        df[f"mom{s}_hist_slope"] = df[f"mom{s}_hist"].diff()
        df[f"mom{s}_hist_acceleration"] = df[f"mom{s}_hist_slope"].diff()
        df[f"mom{s}_hist_gt_0"] = df[f"mom{s}_hist"] > 0
        df[f"mom{s}_hist_lt_0"] = df[f"mom{s}_hist"] < 0
        df[f"mom{s}_hist_abs"] = df[f"mom{s}_hist"].abs()

        # --- combined conviction score ---
        df[f"mom{s}_strength"] = df[f"mom{s}_abs"] * df[f"mom{s}_hist_abs"]

    return df


def add_ppo(
    df: pd.DataFrame,
    fast: list[int] = None,
    slow: list[int] = None,
    signal: list[int] = None,
    ma_type: int = 1,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add Percentage Price Oscillator (PPO) columns for each (fast, slow, signal) combo.

    PPO = (EMA(fast) - EMA(slow)) / EMA(slow) × 100.
    Scale-free version of MACD — expressed as a percentage, comparable across assets.

    Suffix format: ppo_{fast}_{slow}_{signal}, e.g. (12, 26, 9) → 'ppo_12_26_9'

    Moving average types (TA-Lib MA_Type):
        0 = SMA  1 = EMA  2 = WMA  3 = DEMA  4 = TEMA
        5 = TRIMA  6 = KAMA  7 = MAMA  8 = T3

    Columns added (per combo)
    -------------------------
    ppo_{f}_{sl}_{sg}                   : PPO line (% distance between fast and slow MA)
    ppo_{f}_{sl}_{sg}_slope             : first difference of PPO line
    ppo_{f}_{sl}_{sg}_acceleration      : second difference of PPO line
    ppo_{f}_{sl}_{sg}_abs               : |PPO| (magnitude regardless of direction)
    ppo_{f}_{sl}_{sg}_direction         : +1 if PPO > 0, -1 otherwise
    ppo_{f}_{sl}_{sg}_gt_0              : PPO > 0 (fast MA above slow MA, bullish)
    ppo_{f}_{sl}_{sg}_lt_0              : PPO < 0 (fast MA below slow MA, bearish)

    ppo_{f}_{sl}_{sg}_signal            : signal line (MA of PPO)
    ppo_{f}_{sl}_{sg}_signal_slope      : first difference of signal line
    ppo_{f}_{sl}_{sg}_signal_gt_0       : signal line > 0
    ppo_{f}_{sl}_{sg}_signal_lt_0       : signal line < 0

    ppo_{f}_{sl}_{sg}_hist              : histogram (PPO - signal)
    ppo_{f}_{sl}_{sg}_hist_slope        : first difference of histogram (momentum shift)
    ppo_{f}_{sl}_{sg}_hist_acceleration : second difference of histogram
    ppo_{f}_{sl}_{sg}_hist_gt_0         : histogram > 0 (bullish momentum)
    ppo_{f}_{sl}_{sg}_hist_lt_0         : histogram < 0 (bearish momentum)
    ppo_{f}_{sl}_{sg}_hist_abs          : |histogram| (conviction magnitude)

    ppo_{f}_{sl}_{sg}_cross_above       : PPO crossed above signal this bar
    ppo_{f}_{sl}_{sg}_cross_below       : PPO crossed below signal this bar
    ppo_{f}_{sl}_{sg}_strength          : ppo_abs × hist_abs (trend strength × momentum conviction)
    """

    validate_column(df, column_name)

    if fast is None:
        fast = [12]
    if slow is None:
        slow = [26]
    if signal is None:
        signal = [9]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for f, sl, sg in product(fast, slow, signal):
        if f >= sl:
            continue  # fast must be strictly less than slow

        s = f"_{f}_{sl}_{sg}"

        # --- core indicator ---
        ppo_line = talib.PPO(source, fastperiod=f, slowperiod=sl, matype=ma_type)

        # TA-Lib PPO does not return a signal/hist — compute manually
        signal_line = pd.Series(ppo_line).rolling(window=sg).mean().values
        hist = ppo_line - signal_line

        # --- PPO line ---
        df[f"ppo{s}"] = ppo_line
        df[f"ppo{s}_slope"] = pd.Series(ppo_line).diff().values
        df[f"ppo{s}_acceleration"] = pd.Series(df[f"ppo{s}_slope"]).diff().values
        df[f"ppo{s}_abs"] = np.abs(ppo_line)
        df[f"ppo{s}_direction"] = np.where(ppo_line > 0, 1, -1)
        df[f"ppo{s}_gt_0"] = ppo_line > 0
        df[f"ppo{s}_lt_0"] = ppo_line < 0

        # --- signal line ---
        df[f"ppo{s}_signal"] = signal_line
        df[f"ppo{s}_signal_slope"] = pd.Series(signal_line).diff().values
        df[f"ppo{s}_signal_gt_0"] = signal_line > 0
        df[f"ppo{s}_signal_lt_0"] = signal_line < 0

        # --- histogram ---
        df[f"ppo{s}_hist"] = hist
        df[f"ppo{s}_hist_slope"] = pd.Series(hist).diff().values
        df[f"ppo{s}_hist_acceleration"] = (
            pd.Series(df[f"ppo{s}_hist_slope"]).diff().values
        )
        df[f"ppo{s}_hist_gt_0"] = hist > 0
        df[f"ppo{s}_hist_lt_0"] = hist < 0
        df[f"ppo{s}_hist_abs"] = np.abs(hist)

        # --- crossover signals ---
        prev_hist = pd.Series(hist).shift(1)
        df[f"ppo{s}_cross_above"] = (pd.Series(hist) > 0) & (prev_hist <= 0)
        df[f"ppo{s}_cross_below"] = (pd.Series(hist) < 0) & (prev_hist >= 0)

        # --- combined conviction score ---
        df[f"ppo{s}_strength"] = df[f"ppo{s}_abs"] * df[f"ppo{s}_hist_abs"]

    return df


def add_roc(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add Rate of Change (ROC) columns for each period in n.

    ROC = ((price / price[n periods ago]) - 1) × 100.
    Scale-free momentum — already expressed as %, comparable across assets.
    Positive = price higher than n bars ago (bullish), negative = lower (bearish).

    Columns added (per period, e.g. n=10 → suffix '_10')
    -------------
    roc_{n}                     : raw ROC value (%)
    roc_{n}_slope               : first difference of ROC (acceleration)
    roc_{n}_acceleration        : second difference of ROC (jerk)
    roc_{n}_abs                 : |ROC| (magnitude regardless of direction)
    roc_{n}_direction           : +1 if ROC > 0, -1 otherwise
    roc_{n}_gt_0                : ROC > 0 (price higher than n bars ago)
    roc_{n}_lt_0                : ROC < 0 (price lower than n bars ago)
    roc_{n}_signal              : SMA of ROC over same period (smoothed signal line)
    roc_{n}_signal_slope        : first difference of signal line
    roc_{n}_hist                : ROC - signal (raw vs smoothed divergence)
    roc_{n}_hist_slope          : first difference of histogram
    roc_{n}_hist_acceleration   : second difference of histogram
    roc_{n}_hist_gt_0           : histogram > 0 (momentum turning bullish)
    roc_{n}_hist_lt_0           : histogram < 0 (momentum turning bearish)
    roc_{n}_hist_abs            : |histogram| (divergence magnitude)
    roc_{n}_strength            : roc_abs × hist_abs (momentum × divergence score)
    """

    validate_column(df, column_name)

    if n is None:
        n = [10]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"roc{s}"] = talib.ROC(source, timeperiod=period)

        # --- derivatives ---
        df[f"roc{s}_slope"] = df[f"roc{s}"].diff()
        df[f"roc{s}_acceleration"] = df[f"roc{s}_slope"].diff()
        df[f"roc{s}_abs"] = df[f"roc{s}"].abs()
        df[f"roc{s}_direction"] = df[f"roc{s}"].apply(lambda x: 1 if x > 0 else -1)

        # --- zero-line flags ---
        df[f"roc{s}_gt_0"] = df[f"roc{s}"] > 0
        df[f"roc{s}_lt_0"] = df[f"roc{s}"] < 0

        # --- signal line & histogram ---
        df[f"roc{s}_signal"] = df[f"roc{s}"].rolling(window=period).mean()
        df[f"roc{s}_signal_slope"] = df[f"roc{s}_signal"].diff()
        df[f"roc{s}_hist"] = df[f"roc{s}"] - df[f"roc{s}_signal"]
        df[f"roc{s}_hist_slope"] = df[f"roc{s}_hist"].diff()
        df[f"roc{s}_hist_acceleration"] = df[f"roc{s}_hist_slope"].diff()
        df[f"roc{s}_hist_gt_0"] = df[f"roc{s}_hist"] > 0
        df[f"roc{s}_hist_lt_0"] = df[f"roc{s}_hist"] < 0
        df[f"roc{s}_hist_abs"] = df[f"roc{s}_hist"].abs()

        # --- combined conviction score ---
        df[f"roc{s}_strength"] = df[f"roc{s}_abs"] * df[f"roc{s}_hist_abs"]

    return df


def add_rsi(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add Relative Strength Index (RSI) columns for each period in n.

    RSI = 100 - (100 / (1 + RS)) where RS = avg up / avg down over n periods.
    Range is 0 to 100. Neutral midpoint is 50.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    rsi_{n}                     : raw RSI value (0 to 100)
    rsi_{n}_slope               : first difference of RSI (momentum)
    rsi_{n}_acceleration        : second difference of RSI
    rsi_{n}_abs                 : |RSI - 50| (deviation from neutral midpoint)
    rsi_{n}_direction           : +1 if RSI > 50, -1 otherwise
    rsi_{n}_gt_70               : RSI > 70 (overbought)
    rsi_{n}_lt_30               : RSI < 30 (oversold)
    rsi_{n}_gt_50               : RSI > 50 (bullish bias)
    rsi_{n}_lt_50               : RSI < 50 (bearish bias)
    rsi_{n}_extreme             : +1 if RSI > 70, -1 if RSI < 30, 0 if between
    rsi_{n}_signal              : SMA of RSI over same period (smoothed signal line)
    rsi_{n}_signal_slope        : first difference of signal line
    rsi_{n}_hist                : RSI - signal (raw vs smoothed divergence)
    rsi_{n}_hist_slope          : first difference of histogram
    rsi_{n}_hist_acceleration   : second difference of histogram
    rsi_{n}_hist_gt_0           : histogram > 0 (momentum turning bullish)
    rsi_{n}_hist_lt_0           : histogram < 0 (momentum turning bearish)
    rsi_{n}_hist_abs            : |histogram| (divergence magnitude)
    rsi_{n}_strength            : rsi_abs × hist_abs (deviation × divergence score)
    """

    validate_column(df, column_name)

    if n is None:
        n = [14]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"rsi{s}"] = talib.RSI(source, timeperiod=period)

        # --- derivatives ---
        df[f"rsi{s}_slope"] = df[f"rsi{s}"].diff()
        df[f"rsi{s}_acceleration"] = df[f"rsi{s}_slope"].diff()
        df[f"rsi{s}_abs"] = (df[f"rsi{s}"] - 50).abs()  # deviation from neutral
        df[f"rsi{s}_direction"] = df[f"rsi{s}"].apply(lambda x: 1 if x > 50 else -1)

        # --- threshold flags ---
        df[f"rsi{s}_gt_70"] = df[f"rsi{s}"] > 70
        df[f"rsi{s}_lt_30"] = df[f"rsi{s}"] < 30
        df[f"rsi{s}_gt_50"] = df[f"rsi{s}"] > 50
        df[f"rsi{s}_lt_50"] = df[f"rsi{s}"] < 50
        df[f"rsi{s}_extreme"] = df[f"rsi{s}"].apply(
            lambda x: 1 if x > 70 else (-1 if x < 30 else 0)
        )

        # --- signal line & histogram ---
        df[f"rsi{s}_signal"] = df[f"rsi{s}"].rolling(window=period).mean()
        df[f"rsi{s}_signal_slope"] = df[f"rsi{s}_signal"].diff()
        df[f"rsi{s}_hist"] = df[f"rsi{s}"] - df[f"rsi{s}_signal"]
        df[f"rsi{s}_hist_slope"] = df[f"rsi{s}_hist"].diff()
        df[f"rsi{s}_hist_acceleration"] = df[f"rsi{s}_hist_slope"].diff()
        df[f"rsi{s}_hist_gt_0"] = df[f"rsi{s}_hist"] > 0
        df[f"rsi{s}_hist_lt_0"] = df[f"rsi{s}_hist"] < 0
        df[f"rsi{s}_hist_abs"] = df[f"rsi{s}_hist"].abs()

        # --- combined conviction score ---
        df[f"rsi{s}_strength"] = df[f"rsi{s}_abs"] * df[f"rsi{s}_hist_abs"]

    return df


def add_stoch(
    df: pd.DataFrame,
    fastk: list[int] = None,
    slowk: list[int] = None,
    slowd: list[int] = None,
    slowk_matype: int = 0,
    slowd_matype: int = 0,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Stochastic Oscillator columns for each (fastk, slowk, slowd) combination.

    %K = (close - lowest_low) / (highest_high - lowest_low) × 100  over fastk periods.
    Slow %K = MA(%K, slowk). Slow %D = MA(Slow %K, slowd).
    Range is 0 to 100. Neutral midpoint is 50.

    Suffix format: stoch_{fastk}_{slowk}_{slowd}, e.g. (5, 3, 3) → 'stoch_5_3_3'

    Moving average types (TA-Lib MA_Type):
        0 = SMA  1 = EMA  2 = WMA  3 = DEMA  4 = TEMA
        5 = TRIMA  6 = KAMA  7 = MAMA  8 = T3

    Columns added (per combo)
    -------------------------
    stoch_{fk}_{sk}_{sd}_k              : Slow %K line (0 to 100)
    stoch_{fk}_{sk}_{sd}_k_slope        : first difference of %K
    stoch_{fk}_{sk}_{sd}_k_acceleration : second difference of %K
    stoch_{fk}_{sk}_{sd}_k_abs          : |%K - 50| (deviation from neutral)
    stoch_{fk}_{sk}_{sd}_k_direction    : +1 if %K > 50, -1 otherwise
    stoch_{fk}_{sk}_{sd}_k_gt_80        : %K > 80 (overbought)
    stoch_{fk}_{sk}_{sd}_k_lt_20        : %K < 20 (oversold)
    stoch_{fk}_{sk}_{sd}_k_gt_50        : %K > 50 (bullish bias)
    stoch_{fk}_{sk}_{sd}_k_lt_50        : %K < 50 (bearish bias)
    stoch_{fk}_{sk}_{sd}_k_extreme      : +1 if %K > 80, -1 if %K < 20, 0 if between

    stoch_{fk}_{sk}_{sd}_d              : Slow %D line (0 to 100)
    stoch_{fk}_{sk}_{sd}_d_slope        : first difference of %D
    stoch_{fk}_{sk}_{sd}_d_acceleration : second difference of %D
    stoch_{fk}_{sk}_{sd}_d_gt_80        : %D > 80 (overbought)
    stoch_{fk}_{sk}_{sd}_d_lt_20        : %D < 20 (oversold)
    stoch_{fk}_{sk}_{sd}_d_gt_50        : %D > 50 (bullish bias)
    stoch_{fk}_{sk}_{sd}_d_lt_50        : %D < 50 (bearish bias)

    stoch_{fk}_{sk}_{sd}_kd_dist        : %K - %D (signed, momentum lead/lag)
    stoch_{fk}_{sk}_{sd}_kd_dist_abs    : |%K - %D|
    stoch_{fk}_{sk}_{sd}_kd_direction   : +1 if %K > %D, -1 otherwise
    stoch_{fk}_{sk}_{sd}_kd_dist_slope  : first difference of %K-%D distance
    stoch_{fk}_{sk}_{sd}_cross_above    : %K crossed above %D this bar (bullish)
    stoch_{fk}_{sk}_{sd}_cross_below    : %K crossed below %D this bar (bearish)
    stoch_{fk}_{sk}_{sd}_both_gt_80     : both %K and %D > 80 (confirmed overbought)
    stoch_{fk}_{sk}_{sd}_both_lt_20     : both %K and %D < 20 (confirmed oversold)
    stoch_{fk}_{sk}_{sd}_strength       : k_abs × kd_dist_abs (deviation × divergence score)
    """

    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if fastk is None:
        fastk = [5]
    if slowk is None:
        slowk = [3]
    if slowd is None:
        slowd = [3]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    for fk, sk, sd in product(fastk, slowk, slowd):
        s = f"_{fk}_{sk}_{sd}"

        # --- core indicator ---
        k, d = talib.STOCH(
            high,
            low,
            close,
            fastk_period=fk,
            slowk_period=sk,
            slowk_matype=slowk_matype,
            slowd_period=sd,
            slowd_matype=slowd_matype,
        )

        k_series = pd.Series(k, index=df.index)
        d_series = pd.Series(d, index=df.index)

        # --- %K line ---
        df[f"stoch{s}_k"] = k_series
        df[f"stoch{s}_k_slope"] = k_series.diff()
        df[f"stoch{s}_k_acceleration"] = k_series.diff().diff()
        df[f"stoch{s}_k_abs"] = (k_series - 50).abs()
        df[f"stoch{s}_k_direction"] = np.where(k > 50, 1, -1)
        df[f"stoch{s}_k_gt_80"] = k_series > 80
        df[f"stoch{s}_k_lt_20"] = k_series < 20
        df[f"stoch{s}_k_gt_50"] = k_series > 50
        df[f"stoch{s}_k_lt_50"] = k_series < 50
        df[f"stoch{s}_k_extreme"] = np.where(k > 80, 1, np.where(k < 20, -1, 0))

        # --- %D line ---
        df[f"stoch{s}_d"] = d_series
        df[f"stoch{s}_d_slope"] = d_series.diff()
        df[f"stoch{s}_d_acceleration"] = d_series.diff().diff()
        df[f"stoch{s}_d_gt_80"] = d_series > 80
        df[f"stoch{s}_d_lt_20"] = d_series < 20
        df[f"stoch{s}_d_gt_50"] = d_series > 50
        df[f"stoch{s}_d_lt_50"] = d_series < 50

        # --- %K vs %D relationship ---
        kd_dist = k_series - d_series
        prev_kd_dist = kd_dist.shift(1)
        df[f"stoch{s}_kd_dist"] = kd_dist
        df[f"stoch{s}_kd_dist_abs"] = kd_dist.abs()
        df[f"stoch{s}_kd_direction"] = np.where(k > d, 1, -1)
        df[f"stoch{s}_kd_dist_slope"] = kd_dist.diff()

        # --- crossover signals ---
        df[f"stoch{s}_cross_above"] = (kd_dist > 0) & (prev_kd_dist <= 0)
        df[f"stoch{s}_cross_below"] = (kd_dist < 0) & (prev_kd_dist >= 0)

        # --- confirmed extreme zone flags ---
        df[f"stoch{s}_both_gt_80"] = (k_series > 80) & (d_series > 80)
        df[f"stoch{s}_both_lt_20"] = (k_series < 20) & (d_series < 20)

        # --- combined conviction score ---
        df[f"stoch{s}_strength"] = df[f"stoch{s}_k_abs"] * df[f"stoch{s}_kd_dist_abs"]

    return df


def add_stoch_rsi(
    df: pd.DataFrame,
    n: list[int] = None,
    fastk: list[int] = None,
    fastd: list[int] = None,
    fastd_matype: int = 0,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add Stochastic RSI columns for each (n, fastk, fastd) combination.

    StochRSI = (RSI - lowest RSI[n]) / (highest RSI[n] - lowest RSI[n]).
    Applies Stochastic formula to RSI values instead of price.
    Range is 0 to 100. Neutral midpoint is 50.

    Suffix format: stoch_rsi_{n}_{fastk}_{fastd}, e.g. (14, 5, 3) → 'stoch_rsi_14_5_3'

    Moving average types (TA-Lib MA_Type):
        0 = SMA  1 = EMA  2 = WMA  3 = DEMA  4 = TEMA
        5 = TRIMA  6 = KAMA  7 = MAMA  8 = T3

    Columns added (per combo)
    -------------------------
    stoch_rsi_{n}_{fk}_{fd}_k              : FastK line (raw stochastic of RSI, 0–100)
    stoch_rsi_{n}_{fk}_{fd}_k_slope        : first difference of %K
    stoch_rsi_{n}_{fk}_{fd}_k_acceleration : second difference of %K
    stoch_rsi_{n}_{fk}_{fd}_k_abs          : |%K - 50| (deviation from neutral)
    stoch_rsi_{n}_{fk}_{fd}_k_direction    : +1 if %K > 50, -1 otherwise
    stoch_rsi_{n}_{fk}_{fd}_k_gt_80        : %K > 80 (overbought)
    stoch_rsi_{n}_{fk}_{fd}_k_lt_20        : %K < 20 (oversold)
    stoch_rsi_{n}_{fk}_{fd}_k_gt_50        : %K > 50 (bullish bias)
    stoch_rsi_{n}_{fk}_{fd}_k_lt_50        : %K < 50 (bearish bias)
    stoch_rsi_{n}_{fk}_{fd}_k_extreme      : +1 if %K > 80, -1 if %K < 20, 0 if between

    stoch_rsi_{n}_{fk}_{fd}_d              : FastD line (MA of FastK, 0–100)
    stoch_rsi_{n}_{fk}_{fd}_d_slope        : first difference of %D
    stoch_rsi_{n}_{fk}_{fd}_d_acceleration : second difference of %D
    stoch_rsi_{n}_{fk}_{fd}_d_gt_80        : %D > 80 (overbought)
    stoch_rsi_{n}_{fk}_{fd}_d_lt_20        : %D < 20 (oversold)
    stoch_rsi_{n}_{fk}_{fd}_d_gt_50        : %D > 50 (bullish bias)
    stoch_rsi_{n}_{fk}_{fd}_d_lt_50        : %D < 50 (bearish bias)

    stoch_rsi_{n}_{fk}_{fd}_kd_dist        : %K - %D (signed, momentum lead/lag)
    stoch_rsi_{n}_{fk}_{fd}_kd_dist_abs    : |%K - %D|
    stoch_rsi_{n}_{fk}_{fd}_kd_direction   : +1 if %K > %D, -1 otherwise
    stoch_rsi_{n}_{fk}_{fd}_kd_dist_slope  : first difference of %K-%D distance
    stoch_rsi_{n}_{fk}_{fd}_cross_above    : %K crossed above %D this bar (bullish)
    stoch_rsi_{n}_{fk}_{fd}_cross_below    : %K crossed below %D this bar (bearish)
    stoch_rsi_{n}_{fk}_{fd}_both_gt_80     : both %K and %D > 80 (confirmed overbought)
    stoch_rsi_{n}_{fk}_{fd}_both_lt_20     : both %K and %D < 20 (confirmed oversold)
    stoch_rsi_{n}_{fk}_{fd}_strength       : k_abs × kd_dist_abs (deviation × divergence score)
    """

    validate_column(df, column_name)

    if n is None:
        n = [14]
    if fastk is None:
        fastk = [5]
    if fastd is None:
        fastd = [3]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for period, fk, fd in product(n, fastk, fastd):
        s = f"_{period}_{fk}_{fd}"

        # --- core indicator ---
        k, d = talib.STOCHRSI(
            source,
            timeperiod=period,
            fastk_period=fk,
            fastd_period=fd,
            fastd_matype=fastd_matype,
        )

        k_series = pd.Series(k, index=df.index)
        d_series = pd.Series(d, index=df.index)

        # --- %K line ---
        df[f"stoch_rsi{s}_k"] = k_series
        df[f"stoch_rsi{s}_k_slope"] = k_series.diff()
        df[f"stoch_rsi{s}_k_acceleration"] = k_series.diff().diff()
        df[f"stoch_rsi{s}_k_abs"] = (k_series - 50).abs()
        df[f"stoch_rsi{s}_k_direction"] = np.where(k > 50, 1, -1)
        df[f"stoch_rsi{s}_k_gt_80"] = k_series > 80
        df[f"stoch_rsi{s}_k_lt_20"] = k_series < 20
        df[f"stoch_rsi{s}_k_gt_50"] = k_series > 50
        df[f"stoch_rsi{s}_k_lt_50"] = k_series < 50
        df[f"stoch_rsi{s}_k_extreme"] = np.where(k > 80, 1, np.where(k < 20, -1, 0))

        # --- %D line ---
        df[f"stoch_rsi{s}_d"] = d_series
        df[f"stoch_rsi{s}_d_slope"] = d_series.diff()
        df[f"stoch_rsi{s}_d_acceleration"] = d_series.diff().diff()
        df[f"stoch_rsi{s}_d_gt_80"] = d_series > 80
        df[f"stoch_rsi{s}_d_lt_20"] = d_series < 20
        df[f"stoch_rsi{s}_d_gt_50"] = d_series > 50
        df[f"stoch_rsi{s}_d_lt_50"] = d_series < 50

        # --- %K vs %D relationship ---
        kd_dist = k_series - d_series
        prev_kd_dist = kd_dist.shift(1)
        df[f"stoch_rsi{s}_kd_dist"] = kd_dist
        df[f"stoch_rsi{s}_kd_dist_abs"] = kd_dist.abs()
        df[f"stoch_rsi{s}_kd_direction"] = np.where(k > d, 1, -1)
        df[f"stoch_rsi{s}_kd_dist_slope"] = kd_dist.diff()

        # --- crossover signals ---
        df[f"stoch_rsi{s}_cross_above"] = (kd_dist > 0) & (prev_kd_dist <= 0)
        df[f"stoch_rsi{s}_cross_below"] = (kd_dist < 0) & (prev_kd_dist >= 0)

        # --- confirmed extreme zone flags ---
        df[f"stoch_rsi{s}_both_gt_80"] = (k_series > 80) & (d_series > 80)
        df[f"stoch_rsi{s}_both_lt_20"] = (k_series < 20) & (d_series < 20)

        # --- combined conviction score ---
        df[f"stoch_rsi{s}_strength"] = (
            df[f"stoch_rsi{s}_k_abs"] * df[f"stoch_rsi{s}_kd_dist_abs"]
        )

    return df


def add_trix(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    """
    Add TRIX columns for each period in n.

    TRIX = 1-day ROC of a triple-smoothed EMA.
    Oscillates around zero — positive = upward momentum, negative = downward.
    Triple smoothing filters out insignificant price movements and noise.

    Columns added (per period, e.g. n=15 → suffix '_15')
    -------------
    trix_{n}                    : raw TRIX value (%)
    trix_{n}_slope              : first difference of TRIX (acceleration)
    trix_{n}_acceleration       : second difference of TRIX (jerk)
    trix_{n}_abs                : |TRIX| (magnitude regardless of direction)
    trix_{n}_direction          : +1 if TRIX > 0, -1 otherwise
    trix_{n}_gt_0               : TRIX > 0 (bullish momentum)
    trix_{n}_lt_0               : TRIX < 0 (bearish momentum)
    trix_{n}_signal             : SMA of TRIX over same period (smoothed signal line)
    trix_{n}_signal_slope       : first difference of signal line
    trix_{n}_hist               : TRIX - signal (raw vs smoothed divergence)
    trix_{n}_hist_slope         : first difference of histogram
    trix_{n}_hist_acceleration  : second difference of histogram
    trix_{n}_hist_gt_0          : histogram > 0 (momentum turning bullish)
    trix_{n}_hist_lt_0          : histogram < 0 (momentum turning bearish)
    trix_{n}_hist_abs           : |histogram| (divergence magnitude)
    trix_{n}_strength           : trix_abs × hist_abs (momentum × divergence score)
    """

    validate_column(df, column_name)

    if n is None:
        n = [15]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        df[f"trix{s}"] = talib.TRIX(source, timeperiod=period)

        # --- derivatives ---
        df[f"trix{s}_slope"] = df[f"trix{s}"].diff()
        df[f"trix{s}_acceleration"] = df[f"trix{s}_slope"].diff()
        df[f"trix{s}_abs"] = df[f"trix{s}"].abs()
        df[f"trix{s}_direction"] = df[f"trix{s}"].apply(lambda x: 1 if x > 0 else -1)

        # --- zero-line flags ---
        df[f"trix{s}_gt_0"] = df[f"trix{s}"] > 0
        df[f"trix{s}_lt_0"] = df[f"trix{s}"] < 0

        # --- signal line & histogram ---
        df[f"trix{s}_signal"] = df[f"trix{s}"].rolling(window=period).mean()
        df[f"trix{s}_signal_slope"] = df[f"trix{s}_signal"].diff()
        df[f"trix{s}_hist"] = df[f"trix{s}"] - df[f"trix{s}_signal"]
        df[f"trix{s}_hist_slope"] = df[f"trix{s}_hist"].diff()
        df[f"trix{s}_hist_acceleration"] = df[f"trix{s}_hist_slope"].diff()
        df[f"trix{s}_hist_gt_0"] = df[f"trix{s}_hist"] > 0
        df[f"trix{s}_hist_lt_0"] = df[f"trix{s}_hist"] < 0
        df[f"trix{s}_hist_abs"] = df[f"trix{s}_hist"].abs()

        # --- combined conviction score ---
        df[f"trix{s}_strength"] = df[f"trix{s}_abs"] * df[f"trix{s}_hist_abs"]

    return df


def add_ultosc(
    df: pd.DataFrame,
    period1: list[int] = None,
    period2: list[int] = None,
    period3: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Ultimate Oscillator (ULTOSC) columns for each (period1, period2, period3) combination.

    UO combines three timeframes of buying pressure / true range to reduce
    false divergence signals common in single-period oscillators.
    Range is 0 to 100. Neutral midpoint is 50.

    Suffix format: ultosc_{p1}_{p2}_{p3}, e.g. (7, 14, 28) → 'ultosc_7_14_28'

    Columns added (per combo)
    -------------------------
    ultosc_{p1}_{p2}_{p3}                   : raw UO value (0 to 100)
    ultosc_{p1}_{p2}_{p3}_slope             : first difference of UO (momentum)
    ultosc_{p1}_{p2}_{p3}_acceleration      : second difference of UO
    ultosc_{p1}_{p2}_{p3}_abs               : |UO - 50| (deviation from neutral midpoint)
    ultosc_{p1}_{p2}_{p3}_direction         : +1 if UO > 50, -1 otherwise
    ultosc_{p1}_{p2}_{p3}_gt_70             : UO > 70 (overbought)
    ultosc_{p1}_{p2}_{p3}_lt_30             : UO < 30 (oversold)
    ultosc_{p1}_{p2}_{p3}_gt_50             : UO > 50 (bullish bias)
    ultosc_{p1}_{p2}_{p3}_lt_50             : UO < 50 (bearish bias)
    ultosc_{p1}_{p2}_{p3}_extreme           : +1 if UO > 70, -1 if UO < 30, 0 if between
    ultosc_{p1}_{p2}_{p3}_signal            : SMA of UO over period1 (smoothed signal line)
    ultosc_{p1}_{p2}_{p3}_signal_slope      : first difference of signal line
    ultosc_{p1}_{p2}_{p3}_hist              : UO - signal (raw vs smoothed divergence)
    ultosc_{p1}_{p2}_{p3}_hist_slope        : first difference of histogram
    ultosc_{p1}_{p2}_{p3}_hist_acceleration : second difference of histogram
    ultosc_{p1}_{p2}_{p3}_hist_gt_0         : histogram > 0 (momentum turning bullish)
    ultosc_{p1}_{p2}_{p3}_hist_lt_0         : histogram < 0 (momentum turning bearish)
    ultosc_{p1}_{p2}_{p3}_hist_abs          : |histogram| (divergence magnitude)
    ultosc_{p1}_{p2}_{p3}_strength          : uo_abs × hist_abs (deviation × divergence score)
    """

    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if period1 is None:
        period1 = [7]
    if period2 is None:
        period2 = [14]
    if period3 is None:
        period3 = [28]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    for p1, p2, p3 in product(period1, period2, period3):
        if not (p1 < p2 < p3):
            continue  # periods must be strictly ascending: short < medium < long

        s = f"_{p1}_{p2}_{p3}"

        # --- core indicator ---
        uo = talib.ULTOSC(
            high, low, close, timeperiod1=p1, timeperiod2=p2, timeperiod3=p3
        )
        uo_series = pd.Series(uo, index=df.index)

        # --- derivatives ---
        df[f"ultosc{s}"] = uo_series
        df[f"ultosc{s}_slope"] = uo_series.diff()
        df[f"ultosc{s}_acceleration"] = uo_series.diff().diff()
        df[f"ultosc{s}_abs"] = (uo_series - 50).abs()
        df[f"ultosc{s}_direction"] = np.where(uo > 50, 1, -1)

        # --- threshold flags ---
        df[f"ultosc{s}_gt_70"] = uo_series > 70
        df[f"ultosc{s}_lt_30"] = uo_series < 30
        df[f"ultosc{s}_gt_50"] = uo_series > 50
        df[f"ultosc{s}_lt_50"] = uo_series < 50
        df[f"ultosc{s}_extreme"] = np.where(uo > 70, 1, np.where(uo < 30, -1, 0))

        # --- signal line & histogram ---
        df[f"ultosc{s}_signal"] = uo_series.rolling(window=p1).mean()
        df[f"ultosc{s}_signal_slope"] = df[f"ultosc{s}_signal"].diff()
        df[f"ultosc{s}_hist"] = uo_series - df[f"ultosc{s}_signal"]
        df[f"ultosc{s}_hist_slope"] = df[f"ultosc{s}_hist"].diff()
        df[f"ultosc{s}_hist_acceleration"] = df[f"ultosc{s}_hist_slope"].diff()
        df[f"ultosc{s}_hist_gt_0"] = df[f"ultosc{s}_hist"] > 0
        df[f"ultosc{s}_hist_lt_0"] = df[f"ultosc{s}_hist"] < 0
        df[f"ultosc{s}_hist_abs"] = df[f"ultosc{s}_hist"].abs()

        # --- combined conviction score ---
        df[f"ultosc{s}_strength"] = df[f"ultosc{s}_abs"] * df[f"ultosc{s}_hist_abs"]

    return df


def add_willr(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Williams' %R columns for each period in n.

    %R = (highest_high - close) / (highest_high - lowest_low) × -100.
    Range is -100 to 0. Neutral midpoint is -50.
    Inverted Stochastic — readings near 0 = overbought, near -100 = oversold.

    Columns added (per period, e.g. n=14 → suffix '_14')
    -------------
    willr_{n}                     : raw %R value (-100 to 0)
    willr_{n}_slope               : first difference of %R (momentum)
    willr_{n}_acceleration        : second difference of %R
    willr_{n}_abs                 : |%R + 50| (deviation from neutral midpoint)
    willr_{n}_direction           : +1 if %R > -50, -1 otherwise
    willr_{n}_gt_minus20          : %R > -20 (overbought)
    willr_{n}_lt_minus80          : %R < -80 (oversold)
    willr_{n}_gt_minus50          : %R > -50 (bullish bias)
    willr_{n}_lt_minus50          : %R < -50 (bearish bias)
    willr_{n}_extreme             : +1 if %R > -20, -1 if %R < -80, 0 if between
    willr_{n}_signal              : SMA of %R over same period (smoothed signal line)
    willr_{n}_signal_slope        : first difference of signal line
    willr_{n}_hist                : %R - signal (raw vs smoothed divergence)
    willr_{n}_hist_slope          : first difference of histogram
    willr_{n}_hist_acceleration   : second difference of histogram
    willr_{n}_hist_gt_0           : histogram > 0 (momentum turning bullish)
    willr_{n}_hist_lt_0           : histogram < 0 (momentum turning bearish)
    willr_{n}_hist_abs            : |histogram| (divergence magnitude)
    willr_{n}_strength            : willr_abs × hist_abs (deviation × divergence score)
    """

    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        willr = talib.WILLR(high, low, close, timeperiod=period)
        willr_series = pd.Series(willr, index=df.index)

        # --- derivatives ---
        df[f"willr{s}"] = willr_series
        df[f"willr{s}_slope"] = willr_series.diff()
        df[f"willr{s}_acceleration"] = willr_series.diff().diff()
        df[f"willr{s}_abs"] = (willr_series + 50).abs()  # deviation from -50 neutral
        df[f"willr{s}_direction"] = np.where(willr > -50, 1, -1)

        # --- threshold flags ---
        df[f"willr{s}_gt_minus20"] = willr_series > -20  # overbought
        df[f"willr{s}_lt_minus80"] = willr_series < -80  # oversold
        df[f"willr{s}_gt_minus50"] = willr_series > -50  # bullish bias
        df[f"willr{s}_lt_minus50"] = willr_series < -50  # bearish bias
        df[f"willr{s}_extreme"] = np.where(willr > -20, 1, np.where(willr < -80, -1, 0))

        # --- signal line & histogram ---
        df[f"willr{s}_signal"] = willr_series.rolling(window=period).mean()
        df[f"willr{s}_signal_slope"] = df[f"willr{s}_signal"].diff()
        df[f"willr{s}_hist"] = willr_series - df[f"willr{s}_signal"]
        df[f"willr{s}_hist_slope"] = df[f"willr{s}_hist"].diff()
        df[f"willr{s}_hist_acceleration"] = df[f"willr{s}_hist_slope"].diff()
        df[f"willr{s}_hist_gt_0"] = df[f"willr{s}_hist"] > 0
        df[f"willr{s}_hist_lt_0"] = df[f"willr{s}_hist"] < 0
        df[f"willr{s}_hist_abs"] = df[f"willr{s}_hist"].abs()

        # --- combined conviction score ---
        df[f"willr{s}_strength"] = df[f"willr{s}_abs"] * df[f"willr{s}_hist_abs"]

    return df


# endregion MOMENTUM INDICATORS


# region VOLUME INDICATORS
def add_ad(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:
    """
    Add Chaikin A/D Line columns and smoothed signal derivatives.

    AD is a cumulative volume-weighted indicator — it has no timeperiod.
    n defines smoothing windows applied to the raw AD line (like a signal line).

    Base columns (computed once)
    ----------------------------
    ad                          : raw cumulative A/D line
    ad_slope                    : first difference of AD (momentum — Chaikin Oscillator numerator)
    ad_acceleration             : second difference of AD
    ad_gt_0                     : AD > 0 (cumulative buying pressure dominates)
    ad_lt_0                     : AD < 0 (cumulative selling pressure dominates)
    ad_direction                : +1 if AD slope > 0 (rising), -1 otherwise

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    ad_signal_{n}               : EMA of AD over n periods (Chaikin Oscillator fast line)
    ad_signal_{n}_slope         : first difference of signal line
    ad_hist_{n}                 : AD - signal (divergence from smoothed line)
    ad_hist_{n}_slope           : first difference of histogram
    ad_hist_{n}_acceleration    : second difference of histogram
    ad_hist_{n}_gt_0            : histogram > 0 (AD accelerating above signal)
    ad_hist_{n}_lt_0            : histogram < 0 (AD decelerating below signal)
    ad_hist_{n}_abs             : |histogram| (divergence magnitude)
    ad_{n}_strength             : |ad_slope| × hist_abs (momentum × divergence score)
    """

    for col in (high_col, low_col, close_col, volume_col):
        validate_column(df, col)

    if n is None:
        n = [10]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    # --- raw AD line (computed once, no period) ---
    df["ad"] = talib.AD(high, low, close, volume)

    # --- base derivatives ---
    df["ad_slope"] = df["ad"].diff()
    df["ad_acceleration"] = df["ad_slope"].diff()
    df["ad_gt_0"] = df["ad"] > 0
    df["ad_lt_0"] = df["ad"] < 0
    df["ad_direction"] = df["ad_slope"].apply(lambda x: 1 if x > 0 else -1)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"ad_signal{s}"] = df["ad"].ewm(span=period, adjust=False).mean()
        df[f"ad_signal{s}_slope"] = df[f"ad_signal{s}"].diff()
        df[f"ad_hist{s}"] = df["ad"] - df[f"ad_signal{s}"]
        df[f"ad_hist{s}_slope"] = df[f"ad_hist{s}"].diff()
        df[f"ad_hist{s}_acceleration"] = df[f"ad_hist{s}_slope"].diff()
        df[f"ad_hist{s}_gt_0"] = df[f"ad_hist{s}"] > 0
        df[f"ad_hist{s}_lt_0"] = df[f"ad_hist{s}"] < 0
        df[f"ad_hist{s}_abs"] = df[f"ad_hist{s}"].abs()
        df[f"ad{s}_strength"] = df["ad_slope"].abs() * df[f"ad_hist{s}_abs"]

    return df


def add_adosc(
    df: pd.DataFrame,
    fast: list[int] = None,
    slow: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:
    """
    Add Chaikin A/D Oscillator (ADOSC) columns for each (fast, slow) combination.

    ADOSC = EMA(fast, AD) - EMA(slow, AD).
    Measures momentum of the A/D line — positive = AD accelerating up, negative = down.
    Unbounded — scale depends on price and volume levels.

    Suffix format: adosc_{fast}_{slow}, e.g. (3, 10) → 'adosc_3_10'

    Base columns (computed once)
    ----------------------------
    ad                              : raw cumulative A/D line (shared with add_ad if present)

    Columns added (per combo)
    -------------------------
    adosc_{f}_{sl}                  : raw ADOSC value
    adosc_{f}_{sl}_slope            : first difference of ADOSC (momentum)
    adosc_{f}_{sl}_acceleration     : second difference of ADOSC
    adosc_{f}_{sl}_abs              : |ADOSC| (magnitude regardless of direction)
    adosc_{f}_{sl}_direction        : +1 if ADOSC > 0, -1 otherwise
    adosc_{f}_{sl}_gt_0             : ADOSC > 0 (AD accelerating upward — bullish)
    adosc_{f}_{sl}_lt_0             : ADOSC < 0 (AD decelerating — bearish)
    adosc_{f}_{sl}_signal           : SMA of ADOSC over fast period (smoothed signal line)
    adosc_{f}_{sl}_signal_slope     : first difference of signal line
    adosc_{f}_{sl}_hist             : ADOSC - signal (raw vs smoothed divergence)
    adosc_{f}_{sl}_hist_slope       : first difference of histogram
    adosc_{f}_{sl}_hist_acceleration: second difference of histogram
    adosc_{f}_{sl}_hist_gt_0        : histogram > 0 (momentum turning bullish)
    adosc_{f}_{sl}_hist_lt_0        : histogram < 0 (momentum turning bearish)
    adosc_{f}_{sl}_hist_abs         : |histogram| (divergence magnitude)
    adosc_{f}_{sl}_cross_above      : ADOSC crossed above zero this bar (bullish)
    adosc_{f}_{sl}_cross_below      : ADOSC crossed below zero this bar (bearish)
    adosc_{f}_{sl}_strength         : adosc_abs × hist_abs (momentum × divergence score)
    """

    for col in (high_col, low_col, close_col, volume_col):
        validate_column(df, col)

    if fast is None:
        fast = [3]
    if slow is None:
        slow = [10]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    # --- raw AD line (computed once, shared across combos) ---
    if "ad" not in df.columns:
        df["ad"] = talib.AD(high, low, close, volume)

    # --- per fast/slow combo ---
    for f, sl in product(fast, slow):
        if f >= sl:
            continue  # fast must be strictly less than slow

        s = f"_{f}_{sl}"

        adosc = talib.ADOSC(high, low, close, volume, fastperiod=f, slowperiod=sl)
        adosc_series = pd.Series(adosc, index=df.index)

        # --- core ---
        df[f"adosc{s}"] = adosc_series
        df[f"adosc{s}_slope"] = adosc_series.diff()
        df[f"adosc{s}_acceleration"] = adosc_series.diff().diff()
        df[f"adosc{s}_abs"] = adosc_series.abs()
        df[f"adosc{s}_direction"] = np.where(adosc > 0, 1, -1)
        df[f"adosc{s}_gt_0"] = adosc_series > 0
        df[f"adosc{s}_lt_0"] = adosc_series < 0

        # --- signal line & histogram ---
        df[f"adosc{s}_signal"] = adosc_series.rolling(window=f).mean()
        df[f"adosc{s}_signal_slope"] = df[f"adosc{s}_signal"].diff()
        df[f"adosc{s}_hist"] = adosc_series - df[f"adosc{s}_signal"]
        df[f"adosc{s}_hist_slope"] = df[f"adosc{s}_hist"].diff()
        df[f"adosc{s}_hist_acceleration"] = df[f"adosc{s}_hist_slope"].diff()
        df[f"adosc{s}_hist_gt_0"] = df[f"adosc{s}_hist"] > 0
        df[f"adosc{s}_hist_lt_0"] = df[f"adosc{s}_hist"] < 0
        df[f"adosc{s}_hist_abs"] = df[f"adosc{s}_hist"].abs()

        # --- zero-line crossover signals ---
        prev_adosc = adosc_series.shift(1)
        df[f"adosc{s}_cross_above"] = (adosc_series > 0) & (prev_adosc <= 0)
        df[f"adosc{s}_cross_below"] = (adosc_series < 0) & (prev_adosc >= 0)

        # --- combined conviction score ---
        df[f"adosc{s}_strength"] = df[f"adosc{s}_abs"] * df[f"adosc{s}_hist_abs"]

    return df


def add_obv(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:
    """
    Add On Balance Volume (OBV) columns and smoothed signal derivatives.

    OBV is a cumulative volume indicator:
        - Add volume when close > previous close
        - Subtract volume when close < previous close
        - No change when equal

    Base columns (computed once)
    ----------------------------
    obv                         : raw OBV line
    obv_slope                   : first difference of OBV (momentum)
    obv_acceleration            : second difference of OBV
    obv_gt_0                    : OBV > 0 (net buying pressure over time)
    obv_lt_0                    : OBV < 0 (net selling pressure)
    obv_direction               : +1 if OBV rising, -1 otherwise

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    obv_signal_{n}              : EMA of OBV over n periods
    obv_signal_{n}_slope        : first difference of signal line
    obv_hist_{n}                : OBV - signal (divergence)
    obv_hist_{n}_slope          : first difference of histogram
    obv_hist_{n}_acceleration   : second difference of histogram
    obv_hist_{n}_gt_0           : histogram > 0 (OBV above signal)
    obv_hist_{n}_lt_0           : histogram < 0 (OBV below signal)
    obv_hist_{n}_abs            : |histogram| (divergence magnitude)
    obv_{n}_strength            : |obv_slope| × hist_abs (momentum × divergence score)
    """

    for col in (close_col, volume_col):
        validate_column(df, col)

    if n is None:
        n = [10]

    df = df.copy()

    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    # --- raw OBV (computed once, no period) ---
    df["obv"] = talib.OBV(close, volume)

    # --- base derivatives ---
    df["obv_slope"] = df["obv"].diff()
    df["obv_acceleration"] = df["obv_slope"].diff()
    df["obv_gt_0"] = df["obv"] > 0
    df["obv_lt_0"] = df["obv"] < 0
    df["obv_direction"] = df["obv_slope"].apply(lambda x: 1 if x > 0 else -1)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"obv_signal{s}"] = df["obv"].ewm(span=period, adjust=False).mean()
        df[f"obv_signal{s}_slope"] = df[f"obv_signal{s}"].diff()
        df[f"obv_hist{s}"] = df["obv"] - df[f"obv_signal{s}"]
        df[f"obv_hist{s}_slope"] = df[f"obv_hist{s}"].diff()
        df[f"obv_hist{s}_acceleration"] = df[f"obv_hist{s}_slope"].diff()
        df[f"obv_hist{s}_gt_0"] = df[f"obv_hist{s}"] > 0
        df[f"obv_hist{s}_lt_0"] = df[f"obv_hist{s}"] < 0
        df[f"obv_hist{s}_abs"] = df[f"obv_hist{s}"].abs()
        df[f"obv{s}_strength"] = df["obv_slope"].abs() * df[f"obv_hist{s}_abs"]
        df[f"obv{s}_cross_above"] = (df[f"obv_hist{s}"] > 0) & (
            df[f"obv_hist{s}"].shift(1) <= 0
        )
        df[f"obv{s}_cross_below"] = (df[f"obv_hist{s}"] < 0) & (
            df[f"obv_hist{s}"].shift(1) >= 0
        )

    return df


# endregion VOLUME INDICATORS


# region CYCLE INDICATORS
def add_ht_dcperiod(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Hilbert Transform - Dominant Cycle Period (HT_DCPERIOD) features.

    HT_DCPERIOD estimates the dominant market cycle length (in bars).
    Useful for adapting indicators dynamically (e.g., adaptive MA, RSI length).

    Base columns (computed once)
    ----------------------------
    ht_dcperiod                  : dominant cycle period (float, typically 10–40 range)
    ht_dcperiod_slope            : first difference (cycle length expansion/contraction)
    ht_dcperiod_acceleration     : second difference
    ht_dcperiod_gt_prev          : period increasing (cycle slowing / lengthening)
    ht_dcperiod_lt_prev          : period decreasing (cycle speeding up)
    ht_dcperiod_direction        : +1 if increasing, -1 otherwise
    ht_dcperiod_valid            : period is finite and > 0

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    ht_dcperiod_signal_{n}       : EMA of period (smoothed cycle estimate)
    ht_dcperiod_signal_{n}_slope : first difference of signal
    ht_dcperiod_hist_{n}         : period - signal (cycle deviation)
    ht_dcperiod_hist_{n}_slope   : first difference of histogram
    ht_dcperiod_hist_{n}_acceleration : second difference of histogram
    ht_dcperiod_hist_{n}_gt_0    : period above signal (cycle expanding)
    ht_dcperiod_hist_{n}_lt_0    : period below signal (cycle contracting)
    ht_dcperiod_hist_{n}_abs     : |histogram|
    ht_dcperiod_{n}_strength     : |slope| × hist_abs (cycle momentum × deviation)
    """

    validate_column(df, close_col)

    if n is None:
        n = [10]

    df = df.copy()

    close = df[close_col].to_numpy(dtype=float)

    # --- raw dominant cycle period ---
    dcperiod = talib.HT_DCPERIOD(close)
    df["ht_dcperiod"] = dcperiod

    # --- base derivatives ---
    df["ht_dcperiod_slope"] = df["ht_dcperiod"].diff()
    df["ht_dcperiod_acceleration"] = df["ht_dcperiod_slope"].diff()
    df["ht_dcperiod_gt_prev"] = df["ht_dcperiod"] > df["ht_dcperiod"].shift(1)
    df["ht_dcperiod_lt_prev"] = df["ht_dcperiod"] < df["ht_dcperiod"].shift(1)
    df["ht_dcperiod_direction"] = df["ht_dcperiod_slope"].apply(
        lambda x: 1 if x > 0 else -1
    )

    # validity (important for HT indicators — initial unstable values)
    df["ht_dcperiod_valid"] = np.isfinite(df["ht_dcperiod"]) & (df["ht_dcperiod"] > 0)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"ht_dcperiod_signal{s}"] = (
            df["ht_dcperiod"].ewm(span=period, adjust=False).mean()
        )
        df[f"ht_dcperiod_signal{s}_slope"] = df[f"ht_dcperiod_signal{s}"].diff()

        df[f"ht_dcperiod_hist{s}"] = df["ht_dcperiod"] - df[f"ht_dcperiod_signal{s}"]
        df[f"ht_dcperiod_hist{s}_slope"] = df[f"ht_dcperiod_hist{s}"].diff()
        df[f"ht_dcperiod_hist{s}_acceleration"] = df[
            f"ht_dcperiod_hist{s}_slope"
        ].diff()

        df[f"ht_dcperiod_hist{s}_gt_0"] = df[f"ht_dcperiod_hist{s}"] > 0
        df[f"ht_dcperiod_hist{s}_lt_0"] = df[f"ht_dcperiod_hist{s}"] < 0
        df[f"ht_dcperiod_hist{s}_abs"] = df[f"ht_dcperiod_hist{s}"].abs()

        df[f"ht_dcperiod{s}_strength"] = (
            df["ht_dcperiod_slope"].abs() * df[f"ht_dcperiod_hist{s}_abs"]
        )

    return df


def add_ht_dcphase(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Hilbert Transform - Dominant Cycle Phase (HT_DCPHASE) features.

    HT_DCPHASE estimates the phase position (in degrees) within the dominant cycle.
    Range is typically 0–360:
        - ~0° / 360° → cycle start
        - ~90°       → rising phase
        - ~180°      → peak
        - ~270°      → falling phase

    Base columns (computed once)
    ----------------------------
    ht_dcphase                   : dominant cycle phase (degrees)
    ht_dcphase_slope             : first difference (phase velocity)
    ht_dcphase_acceleration      : second difference
    ht_dcphase_wrapped           : phase wrapped to [0, 360)
    ht_dcphase_sin               : sin(phase) (cycle representation)
    ht_dcphase_cos               : cos(phase)
    ht_dcphase_quadrant          : 1–4 (cycle stage)
    ht_dcphase_direction         : +1 if phase increasing, -1 otherwise
    ht_dcphase_valid             : finite values

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    ht_dcphase_signal_{n}        : EMA of phase
    ht_dcphase_signal_{n}_slope  : first difference of signal
    ht_dcphase_hist_{n}          : phase - signal (deviation)
    ht_dcphase_hist_{n}_slope    : first difference of histogram
    ht_dcphase_hist_{n}_acceleration : second difference of histogram
    ht_dcphase_hist_{n}_gt_0     : phase above signal
    ht_dcphase_hist_{n}_lt_0     : phase below signal
    ht_dcphase_hist_{n}_abs      : |histogram|
    ht_dcphase_{n}_strength      : |slope| × hist_abs
    """

    validate_column(df, close_col)

    if n is None:
        n = [10]

    df = df.copy()

    close = df[close_col].to_numpy(dtype=float)

    # --- raw phase ---
    phase = talib.HT_DCPHASE(close)
    df["ht_dcphase"] = phase

    # --- base transforms ---
    df["ht_dcphase_wrapped"] = np.mod(df["ht_dcphase"], 360.0)

    # radians for trig features
    phase_rad = np.deg2rad(df["ht_dcphase_wrapped"])
    df["ht_dcphase_sin"] = np.sin(phase_rad)
    df["ht_dcphase_cos"] = np.cos(phase_rad)

    # derivatives
    df["ht_dcphase_slope"] = df["ht_dcphase_wrapped"].diff()
    df["ht_dcphase_acceleration"] = df["ht_dcphase_slope"].diff()

    # quadrant (cycle stage)
    df["ht_dcphase_quadrant"] = pd.cut(
        df["ht_dcphase_wrapped"],
        bins=[0, 90, 180, 270, 360],
        labels=[1, 2, 3, 4],
        include_lowest=True,
    )

    # direction
    df["ht_dcphase_direction"] = df["ht_dcphase_slope"].apply(
        lambda x: 1 if x > 0 else -1
    )

    # validity (HT indicators unstable early)
    df["ht_dcphase_valid"] = np.isfinite(df["ht_dcphase"])

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"ht_dcphase_signal{s}"] = (
            df["ht_dcphase_wrapped"].ewm(span=period, adjust=False).mean()
        )
        df[f"ht_dcphase_signal{s}_slope"] = df[f"ht_dcphase_signal{s}"].diff()

        df[f"ht_dcphase_hist{s}"] = (
            df["ht_dcphase_wrapped"] - df[f"ht_dcphase_signal{s}"]
        )
        df[f"ht_dcphase_hist{s}_slope"] = df[f"ht_dcphase_hist{s}"].diff()
        df[f"ht_dcphase_hist{s}_acceleration"] = df[f"ht_dcphase_hist{s}_slope"].diff()

        df[f"ht_dcphase_hist{s}_gt_0"] = df[f"ht_dcphase_hist{s}"] > 0
        df[f"ht_dcphase_hist{s}_lt_0"] = df[f"ht_dcphase_hist{s}"] < 0
        df[f"ht_dcphase_hist{s}_abs"] = df[f"ht_dcphase_hist{s}"].abs()

        df[f"ht_dcphase{s}_strength"] = (
            df["ht_dcphase_slope"].abs() * df[f"ht_dcphase_hist{s}_abs"]
        )

    return df


def add_ht_phasor(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Hilbert Transform - Phasor Components (HT_PHASOR) features.

    HT_PHASOR returns two components:
        - inphase     (I)
        - quadrature  (Q)

    These form a complex signal representation:
        amplitude = sqrt(I^2 + Q^2)
        phase     = arctan(Q / I)

    Base columns (computed once)
    ----------------------------
    ht_phasor_inphase            : in-phase component (I)
    ht_phasor_quadrature         : quadrature component (Q)
    ht_phasor_amplitude          : sqrt(I^2 + Q^2)
    ht_phasor_phase              : arctan2(Q, I) in degrees
    ht_phasor_phase_wrapped      : phase mapped to [0, 360)
    ht_phasor_slope              : first difference of amplitude (energy change)
    ht_phasor_acceleration       : second difference of amplitude
    ht_phasor_direction          : +1 if amplitude rising, -1 otherwise
    ht_phasor_valid              : finite values

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    ht_phasor_signal_{n}         : EMA of amplitude
    ht_phasor_signal_{n}_slope   : first difference of signal
    ht_phasor_hist_{n}           : amplitude - signal
    ht_phasor_hist_{n}_slope     : first difference of histogram
    ht_phasor_hist_{n}_acceleration : second difference
    ht_phasor_hist_{n}_gt_0      : amplitude above signal
    ht_phasor_hist_{n}_lt_0      : amplitude below signal
    ht_phasor_hist_{n}_abs       : |histogram|
    ht_phasor_{n}_strength       : |slope| × hist_abs
    """

    validate_column(df, close_col)

    if n is None:
        n = [10]

    df = df.copy()
    close = df[close_col].to_numpy(dtype=float)

    # --- core phasor components ---
    inphase, quadrature = talib.HT_PHASOR(close)

    df["ht_phasor_inphase"] = inphase
    df["ht_phasor_quadrature"] = quadrature

    # --- derived features ---
    df["ht_phasor_amplitude"] = np.sqrt(inphase**2 + quadrature**2)

    phase_rad = np.arctan2(quadrature, inphase)
    df["ht_phasor_phase"] = np.degrees(phase_rad)
    df["ht_phasor_phase_wrapped"] = np.mod(df["ht_phasor_phase"], 360.0)

    # amplitude dynamics
    df["ht_phasor_slope"] = df["ht_phasor_amplitude"].diff()
    df["ht_phasor_acceleration"] = df["ht_phasor_slope"].diff()
    df["ht_phasor_direction"] = df["ht_phasor_slope"].apply(
        lambda x: 1 if x > 0 else -1
    )

    # validity
    df["ht_phasor_valid"] = np.isfinite(df["ht_phasor_inphase"]) & np.isfinite(
        df["ht_phasor_quadrature"]
    )

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"ht_phasor_signal{s}"] = (
            df["ht_phasor_amplitude"].ewm(span=period, adjust=False).mean()
        )
        df[f"ht_phasor_signal{s}_slope"] = df[f"ht_phasor_signal{s}"].diff()

        df[f"ht_phasor_hist{s}"] = (
            df["ht_phasor_amplitude"] - df[f"ht_phasor_signal{s}"]
        )
        df[f"ht_phasor_hist{s}_slope"] = df[f"ht_phasor_hist{s}"].diff()
        df[f"ht_phasor_hist{s}_acceleration"] = df[f"ht_phasor_hist{s}_slope"].diff()

        df[f"ht_phasor_hist{s}_gt_0"] = df[f"ht_phasor_hist{s}"] > 0
        df[f"ht_phasor_hist{s}_lt_0"] = df[f"ht_phasor_hist{s}"] < 0
        df[f"ht_phasor_hist{s}_abs"] = df[f"ht_phasor_hist{s}"].abs()

        df[f"ht_phasor{s}_strength"] = (
            df["ht_phasor_slope"].abs() * df[f"ht_phasor_hist{s}_abs"]
        )

    return df


def add_ht_sine(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Hilbert Transform - SineWave (HT_SINE) features.

    HT_SINE returns two series:
        - sine      : sine of dominant cycle phase
        - leadsine  : leading sine (phase advanced ~45°)

    These are powerful for cycle turning points:
        - sine crossing leadsine → potential reversal signals

    Base columns (computed once)
    ----------------------------
    ht_sine                      : sine component
    ht_leadsine                  : leading sine component
    ht_sine_diff                 : sine - leadsine (core signal)
    ht_sine_slope                : first difference of sine
    ht_sine_acceleration         : second difference
    ht_sine_direction            : +1 if sine rising, -1 otherwise
    ht_sine_cross_above          : sine crossed above leadsine (bullish)
    ht_sine_cross_below          : sine crossed below leadsine (bearish)
    ht_sine_gt_0                 : sine > 0 (cycle upper half)
    ht_sine_lt_0                 : sine < 0 (cycle lower half)
    ht_sine_valid                : finite values

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    ht_sine_signal_{n}           : EMA of sine
    ht_sine_signal_{n}_slope     : first difference of signal
    ht_sine_hist_{n}             : sine - signal
    ht_sine_hist_{n}_slope       : first difference of histogram
    ht_sine_hist_{n}_acceleration: second difference
    ht_sine_hist_{n}_gt_0        : sine above signal
    ht_sine_hist_{n}_lt_0        : sine below signal
    ht_sine_hist_{n}_abs         : |histogram|
    ht_sine_{n}_strength         : |slope| × hist_abs
    """

    validate_column(df, close_col)

    if n is None:
        n = [10]

    df = df.copy()
    close = df[close_col].to_numpy(dtype=float)

    # --- core sinewave ---
    sine, leadsine = talib.HT_SINE(close)

    df["ht_sine"] = sine
    df["ht_leadsine"] = leadsine

    # --- core relationships ---
    df["ht_sine_diff"] = df["ht_sine"] - df["ht_leadsine"]

    # derivatives
    df["ht_sine_slope"] = df["ht_sine"].diff()
    df["ht_sine_acceleration"] = df["ht_sine_slope"].diff()
    df["ht_sine_direction"] = df["ht_sine_slope"].apply(lambda x: 1 if x > 0 else -1)

    # regime / position
    df["ht_sine_gt_0"] = df["ht_sine"] > 0
    df["ht_sine_lt_0"] = df["ht_sine"] < 0

    # --- crossover signals ---
    prev_sine = df["ht_sine"].shift(1)
    prev_leadsine = df["ht_leadsine"].shift(1)

    df["ht_sine_cross_above"] = (df["ht_sine"] > df["ht_leadsine"]) & (
        prev_sine <= prev_leadsine
    )

    df["ht_sine_cross_below"] = (df["ht_sine"] < df["ht_leadsine"]) & (
        prev_sine >= prev_leadsine
    )

    # validity (HT unstable early)
    df["ht_sine_valid"] = np.isfinite(df["ht_sine"]) & np.isfinite(df["ht_leadsine"])

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"ht_sine_signal{s}"] = df["ht_sine"].ewm(span=period, adjust=False).mean()
        df[f"ht_sine_signal{s}_slope"] = df[f"ht_sine_signal{s}"].diff()

        df[f"ht_sine_hist{s}"] = df["ht_sine"] - df[f"ht_sine_signal{s}"]
        df[f"ht_sine_hist{s}_slope"] = df[f"ht_sine_hist{s}"].diff()
        df[f"ht_sine_hist{s}_acceleration"] = df[f"ht_sine_hist{s}_slope"].diff()

        df[f"ht_sine_hist{s}_gt_0"] = df[f"ht_sine_hist{s}"] > 0
        df[f"ht_sine_hist{s}_lt_0"] = df[f"ht_sine_hist{s}"] < 0
        df[f"ht_sine_hist{s}_abs"] = df[f"ht_sine_hist{s}"].abs()

        df[f"ht_sine{s}_strength"] = (
            df["ht_sine_slope"].abs() * df[f"ht_sine_hist{s}_abs"]
        )

    return df


def add_ht_trendmode(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Hilbert Transform - Trend vs Cycle Mode (HT_TRENDMODE) features.

    HT_TRENDMODE outputs:
        1 → Trending mode
        0 → Cycle (mean-reverting) mode

    This is a regime classifier — extremely useful for switching strategies.

    Base columns (computed once)
    ----------------------------
    ht_trendmode                 : 1 (trend) or 0 (cycle)
    ht_trendmode_slope           : first difference (state change signal)
    ht_trendmode_acceleration    : second difference
    ht_trendmode_direction       : +1 if trending, -1 if cycle
    ht_trendmode_is_trend        : boolean (trend regime)
    ht_trendmode_is_cycle        : boolean (cycle regime)
    ht_trendmode_switch_on       : switched into trend this bar
    ht_trendmode_switch_off      : switched into cycle this bar
    ht_trendmode_valid           : finite values

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    ht_trendmode_signal_{n}      : EMA of trendmode (probability-like smoothing)
    ht_trendmode_signal_{n}_slope: first difference of signal
    ht_trendmode_hist_{n}        : raw - signal (regime divergence)
    ht_trendmode_hist_{n}_slope  : first difference of histogram
    ht_trendmode_hist_{n}_acceleration : second difference
    ht_trendmode_hist_{n}_gt_0   : regime strengthening toward trend
    ht_trendmode_hist_{n}_lt_0   : regime weakening toward cycle
    ht_trendmode_hist_{n}_abs    : |histogram|
    ht_trendmode_{n}_strength    : |slope| × hist_abs
    """

    validate_column(df, close_col)

    if n is None:
        n = [10]

    df = df.copy()
    close = df[close_col].to_numpy(dtype=float)

    # --- core trendmode ---
    trendmode = talib.HT_TRENDMODE(close)
    df["ht_trendmode"] = trendmode

    # --- base features ---
    df["ht_trendmode_slope"] = df["ht_trendmode"].diff()
    df["ht_trendmode_acceleration"] = df["ht_trendmode_slope"].diff()

    df["ht_trendmode_direction"] = df["ht_trendmode"].apply(
        lambda x: 1 if x == 1 else -1
    )

    df["ht_trendmode_is_trend"] = df["ht_trendmode"] == 1
    df["ht_trendmode_is_cycle"] = df["ht_trendmode"] == 0

    # --- regime switches ---
    prev = df["ht_trendmode"].shift(1)

    df["ht_trendmode_switch_on"] = (df["ht_trendmode"] == 1) & (prev == 0)

    df["ht_trendmode_switch_off"] = (df["ht_trendmode"] == 0) & (prev == 1)

    # validity (HT unstable early)
    df["ht_trendmode_valid"] = np.isfinite(df["ht_trendmode"])

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"ht_trendmode_signal{s}"] = (
            df["ht_trendmode"].ewm(span=period, adjust=False).mean()
        )
        df[f"ht_trendmode_signal{s}_slope"] = df[f"ht_trendmode_signal{s}"].diff()

        df[f"ht_trendmode_hist{s}"] = df["ht_trendmode"] - df[f"ht_trendmode_signal{s}"]
        df[f"ht_trendmode_hist{s}_slope"] = df[f"ht_trendmode_hist{s}"].diff()
        df[f"ht_trendmode_hist{s}_acceleration"] = df[
            f"ht_trendmode_hist{s}_slope"
        ].diff()

        df[f"ht_trendmode_hist{s}_gt_0"] = df[f"ht_trendmode_hist{s}"] > 0
        df[f"ht_trendmode_hist{s}_lt_0"] = df[f"ht_trendmode_hist{s}"] < 0
        df[f"ht_trendmode_hist{s}_abs"] = df[f"ht_trendmode_hist{s}"].abs()

        df[f"ht_trendmode{s}_strength"] = (
            df["ht_trendmode_slope"].abs() * df[f"ht_trendmode_hist{s}_abs"]
        )

    return df


# endregion CYCLE INDICATORS


# region PRICE TRANSFORM
def add_avgprice(
    df: pd.DataFrame,
    n: list[int] = None,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Average Price (AVGPRICE) and derived features.

    AVGPRICE = (open + high + low + close) / 4

    Base columns (computed once)
    ----------------------------
    avgprice                    : OHLC average price
    avgprice_slope              : first difference (momentum)
    avgprice_acceleration       : second difference
    avgprice_gt_close           : avgprice > close
    avgprice_lt_close           : avgprice < close
    avgprice_direction          : +1 if rising, -1 otherwise

    Per smoothing window (e.g. n=10 → suffix '_10')
    ------------------------------------------------
    avgprice_signal_{n}         : EMA of avgprice
    avgprice_signal_{n}_slope   : first difference of signal
    avgprice_hist_{n}           : avgprice - signal
    avgprice_hist_{n}_slope     : first difference of histogram
    avgprice_hist_{n}_acceleration : second difference
    avgprice_hist_{n}_gt_0      : avgprice above signal
    avgprice_hist_{n}_lt_0      : avgprice below signal
    avgprice_hist_{n}_abs       : |histogram|
    avgprice_{n}_strength       : |slope| × hist_abs
    """

    for col in (open_col, high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [10]

    df = df.copy()

    open_ = df[open_col].to_numpy(dtype=float)
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    # --- core AVGPRICE ---
    df["avgprice"] = talib.AVGPRICE(open_, high, low, close)

    # --- base derivatives ---
    df["avgprice_slope"] = df["avgprice"].diff()
    df["avgprice_acceleration"] = df["avgprice_slope"].diff()
    df["avgprice_gt_close"] = df["avgprice"] > df["close"]
    df["avgprice_lt_close"] = df["avgprice"] < df["close"]
    df["avgprice_direction"] = df["avgprice_slope"].apply(
        lambda x: 1 if x > 0 else -1
    )

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        df[f"avgprice_signal{s}"] = (
            df["avgprice"].ewm(span=period, adjust=False).mean()
        )
        df[f"avgprice_signal{s}_slope"] = df[f"avgprice_signal{s}"].diff()

        df[f"avgprice_hist{s}"] = (
            df["avgprice"] - df[f"avgprice_signal{s}"]
        )
        df[f"avgprice_hist{s}_slope"] = df[f"avgprice_hist{s}"].diff()
        df[f"avgprice_hist{s}_acceleration"] = (
            df[f"avgprice_hist{s}_slope"].diff()
        )

        df[f"avgprice_hist{s}_gt_0"] = df[f"avgprice_hist{s}"] > 0
        df[f"avgprice_hist{s}_lt_0"] = df[f"avgprice_hist{s}"] < 0
        df[f"avgprice_hist{s}_abs"] = df[f"avgprice_hist{s}"].abs()

        df[f"avgprice{s}_strength"] = (
            df["avgprice_slope"].abs() * df[f"avgprice_hist{s}_abs"]
        )

    return df

# endregion PRICE TRANSFORM

def add_one_for_all_ta(df: pd.DataFrame) -> pd.DataFrame:
    new_df = df.copy()

    # # OVERLAP STUDIES
    # new_df = add_bbands(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_dema(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ema(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_kama(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_midpoint(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_midprice(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_sar(
    #     new_df,
    # )
    # new_df = add_sma(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_t3(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_tema(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_trima(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_wma(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )

    # # MOMENTUM INDICATORS
    # new_df = add_adx(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_aroon(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_bop(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_cci(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_cmo(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_macd(new_df)
    # new_df = add_mfi(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_mom(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ppo(new_df)
    # new_df = add_roc(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_rsi(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_stoch(new_df)
    # new_df = add_stoch_rsi(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_trix(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ultosc(
    #     new_df,
    # )
    # new_df = add_willr(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )

    # # VOLUME INDICATORS
    # new_df = add_ad(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_adosc(
    #     new_df,
    # )
    # new_df = add_obv(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )

    # CYCLE INDICATORS
    # new_df = add_ht_dcperiod(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ht_dcphase(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ht_phasor(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ht_sine(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    # new_df = add_ht_trendmode(
    #     new_df,
    #     n=[5, 10, 15, 20],
    # )
    new_df = add_avgprice(
        new_df,
        n=[5, 10, 15, 20],
    )

    return new_df


def plot_with_indicators(
    df: pd.DataFrame,
    indicators: list,
    time_column_name: str = "date",
    price_column_name: str = "close",
):
    """
    Plot price with optional indicators using regex patterns.
    - Left y-axis: price + price-style indicators
    - Right y-axis: oscillators / relative indicators
    Prints a warning for any indicators that do not match DataFrame columns.
    """

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Main price axis
    ax1.plot(
        df[time_column_name],
        df[price_column_name],
        label=price_column_name,
        color="black",
        linewidth=2,
    )

    # Second axis for oscillators
    ax2 = ax1.twinx()

    # Collect matched columns
    matched_columns = set()
    unmatched_patterns = []

    for pattern in indicators:
        regex = re.compile(pattern.replace("*", ".*"))  # simple wildcard -> regex
        matched = [col for col in df.columns if regex.fullmatch(col)]
        if matched:
            matched_columns.update(matched)
        else:
            unmatched_patterns.append(pattern)

    # Print warnings for unmatched indicators
    if unmatched_patterns:
        print(
            f"Warning: The following indicators did not match any columns: {unmatched_patterns}"
        )

    # Plot matched columns
    for col in sorted(matched_columns):
        ax2.plot(df[time_column_name], df[col], label=col, linestyle="--")

    ax1.set_xlabel("Date")
    ax1.set_ylabel("Price / Price-based Indicators")
    ax2.set_ylabel("Oscillators / Relative Indicators")

    # Merge legends
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="best")

    ax1.grid(True, linestyle="--", alpha=0.6)
    fig.tight_layout()
    plt.show()


def main():
    # Connect to the database
    ta_logger = Logger(file_name=TA_LOG_FILE_BASE)
    ta_database_driver = PostgreSQLDriver(logger=ta_logger)

    connection_model = PostgreSQLConnectionDto(
        logger=ta_logger,
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("SILVER_POSTGRES_DATABASE"),
    )
    ta_database_driver.connect(connection_model)

    # Select df
    from_date = "2016-06-01"
    to_date = "2016-12-30"

    df = ta_database_driver.select(
        schema_name=Schema.STOCK_MARKET.value,
        table_name=Table.G_VN_INDEX.name,
        conditions=[
            Condition(
                column=Table.G_VN_INDEX.Column.DATE.value,
                operator=SqlOperator.GREATER_THAN_OR_EQUAL_TO,
                value=from_date,
                data_type=DataType.DATE,
            ),
            Condition(
                column=Table.G_VN_INDEX.Column.DATE.value,
                operator=SqlOperator.LESS_THAN_OR_EQUAL_TO,
                value=to_date,
                data_type=DataType.DATE,
            ),
        ],
        order_by=[Table.G_VN_INDEX.Column.DATE.value],
    )

    WEEKENDS = get_weekends(from_date, to_date)
    HOLIDAYS = []

    DAYOFFS = []
    DAYOFFS.extend(WEEKENDS)
    DAYOFFS.extend(HOLIDAYS)

    df = df[~df["date"].isin(DAYOFFS)]

    df["close"] = df["close"].astype(float)

    FORECAST_HORIZON = 5

    df[f"return_{FORECAST_HORIZON}"] = (
        df["close"].shift(-FORECAST_HORIZON) - df["close"]
    )

    # df[f"log_return_{FORECAST_HORIZON}"] = np.log(
    #     df["close"].shift(-FORECAST_HORIZON) / df["close"]
    # )

    df = add_one_for_all_ta(df)

    plot_with_indicators(
        df,
        indicators=["*avgprice*"],
        price_column_name=f"close",
    )

    # plot_with_indicators(
    #     df,
    #     indicators=["close_t3_5", "close_t3_10", "close_t3_15"],
    #     price_column_name=f"return_{FORECAST_HORIZON}",
    # )

    print(len(df.columns))


if __name__ == "__main__":
    main()
