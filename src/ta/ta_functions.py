import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys, os
from dotenv import load_dotenv
from itertools import combinations
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
def add_bollinger_bands(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    k: float = 2.0,
    ma_type: int = 0,
    column_name: str = "close",
    default_bb_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Bollinger Bands (upper, middle, lower) to the DataFrame.

    Default popular Bollinger Band periods:
        20 (SMA period), standard deviation multiplier k=2.0

    class MA_Type(Enum):
        SMA = 0
        EMA = 1
        WMA = 2
        DEMA = 3
        TEMA = 4
        TRIMA = 5
        KAMA = 6
        MAMA = 7
        T3 = 8

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        SMA period(s) for Bollinger Bands. If None, default period 20 is used.
    k : float, optional
        Number of standard deviations for upper/lower bands (default 2.0)
    column_name : str, optional
        Column to calculate Bollinger Bands on. Default is 'close'.
    default_bb_periods : list[int], optional
        Override the default SMA period(s).

    Returns
    -------
    pd.DataFrame
        DataFrame with Bollinger Bands added:
        '{column_name}_bb_middle_{n}', '{column_name}_bb_upper_{n}', '{column_name}_bb_lower_{n}'
    """
    validate_column(df, column_name)
    df = df.copy()

    # Default period
    if default_bb_periods is None:
        default_bb_periods = [20]

    # Determine periods to compute
    if n is None:
        periods = default_bb_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    for period in periods:
        upperband, middleband, lowerband = talib.BBANDS(
            df[column_name].values,
            timeperiod=period,
            nbdevup=k,
            nbdevdn=k,
            matype=ma_type,
        )

        df[f"{column_name}_bb_upper_{period}"] = upperband
        df[f"{column_name}_bb_middle_{period}"] = middleband
        df[f"{column_name}_bb_lower_{period}"] = lowerband

    return df


def add_sma(
    df: pd.DataFrame, n: list[int] = None, column_name: str = "close"
) -> pd.DataFrame:
    """
    Add SMA columns, their slopes, and pairwise SMA distances.
    """

    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()

    sma_cols = []

    # --- SMA + slope ---
    for window in n:
        sma_col = f"{column_name}_sma_{window}"
        slope_col = f"{sma_col}_slope"

        df[sma_col] = talib.SMA(df[column_name].values, window)
        df[slope_col] = df[sma_col].diff()

        sma_cols.append((window, sma_col))

    # --- pairwise distances ---
    for (w1, col1), (w2, col2) in combinations(sma_cols, 2):
        dist_col = f"{column_name}_sma_{w1}_{w2}_dist"
        df[dist_col] = df[col1] - df[col2]

    return df


def add_ema(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
    default_ema_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add one or multiple Exponential Moving Average (EMA) columns.

    Default EMA values reflect commonly used technical analysis periods:
    12, 26, 50, 100, 200.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        EMA span(s). If None, default popular spans will be used.
    column_name : str, optional
        Column to compute EMA on. Default is 'close'.
    default_ema_periods : list[int], optional
        Override the predefined popular EMA spans if desired.

    Returns
    -------
    pd.DataFrame
        DataFrame including added EMA column(s).
    """
    validate_column(df, column_name)
    df = df.copy()

    # Default EMA spans widely used in trading
    if default_ema_periods is None:
        default_ema_periods = [12, 26, 50, 100, 200]

    # If user provides nothing → use defaults
    if n is None:
        periods = default_ema_periods
    # If user provides a single int
    elif isinstance(n, int):
        periods = [n]
    # If user provides list of ints
    else:
        periods = list(n)

    # Compute EMAs
    for period in periods:
        df[f"{column_name}_ema_{period}"] = (
            df[column_name].ewm(span=period, adjust=False).mean()
        )

    return df


def add_lwma(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
    default_lwma_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add one or multiple Linear Weighted Moving Average (LWMA) columns.

    Default LWMA values:
        12, 26, 50, 100, 200

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        LWMA window size(s). If None, default periods are used.
    column_name : str, optional
        Column to compute LWMA on. Default is 'close'.
    default_lwma_periods : list[int], optional
        Override the predefined LWMA spans.

    Returns
    -------
    pd.DataFrame
        DataFrame including added LWMA column(s).
    """
    validate_column(df, column_name)
    df = df.copy()

    # Default LWMA periods you requested
    if default_lwma_periods is None:
        default_lwma_periods = [12, 26, 50, 100, 200]

    # Determine which periods to compute
    if n is None:
        periods = default_lwma_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    # Compute LWMA for each period
    for period in periods:
        weights = np.arange(1, period + 1)
        df[f"{column_name}_lwma_{period}"] = (
            df[column_name]
            .rolling(window=period)
            .apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)
        )

    return df


def add_wma(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
    default_wma_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add one or multiple Wilder's Moving Average (WMA) columns.

    Default WMA values:
        7, 14, 21, 50, 100

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        WMA period(s). If None, default popular periods are used.
    column_name : str, optional
        Column to compute WMA on. Default is 'close'.
    default_wma_periods : list[int], optional
        Override the predefined WMA periods.

    Returns
    -------
    pd.DataFrame
        DataFrame including added WMA column(s).
    """
    validate_column(df, column_name)
    df = df.copy()

    # Default Wilder MA periods
    if default_wma_periods is None:
        default_wma_periods = [7, 14, 21, 50, 100]

    # Determine which periods to compute
    if n is None:
        periods = default_wma_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    # Compute Wilder MA for each period
    for period in periods:
        alpha = 1 / period
        df[f"{column_name}_wma_{period}"] = (
            df[column_name].ewm(alpha=alpha, adjust=False).mean()
        )

    return df


def add_adx(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    default_adx_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add the Average Directional Movement Index (ADX) and related indicators
    (+DI, -DI) to the DataFrame.

    Default popular ADX periods:
        7, 14, 20, 28, 50

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing high, low, close columns.
    n : int or list[int], optional
        ADX period(s). If None, use popular defaults.
    high_col : str, optional
        Column name for high prices. Default 'high'.
    low_col : str, optional
        Column name for low prices. Default 'low'.
    close_col : str, optional
        Column name for close prices. Default 'close'.
    default_adx_periods : list[int], optional
        Optionally override the default period list.

    Returns
    -------
    pd.DataFrame
        DataFrame with +di, -di, and adx_{period} columns added.
    """

    # Validate required columns
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Numeric enforcement
    df[high_col] = pd.to_numeric(df[high_col], errors="coerce")
    df[low_col] = pd.to_numeric(df[low_col], errors="coerce")
    df[close_col] = pd.to_numeric(df[close_col], errors="coerce")

    # Defaults
    if default_adx_periods is None:
        default_adx_periods = [7, 14, 20, 28, 50]

    if n is None:
        periods = default_adx_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    # Compute directional movement values once
    df["tr"] = np.maximum(
        df[high_col] - df[low_col],
        np.maximum(
            abs(df[high_col] - df[close_col].shift()),
            abs(df[low_col] - df[close_col].shift()),
        ),
    )

    df["dm_pos"] = (
        df[high_col]
        .diff()
        .where(
            (df[high_col].diff() > df[low_col].diff() * -1) & (df[high_col].diff() > 0),
            0.0,
        )
    )

    df["dm_neg"] = (-df[low_col].diff()).where(
        (-df[low_col].diff() > df[high_col].diff()) & (-df[low_col].diff() > 0), 0.0
    )

    # +DI and -DI are same for all ADX periods, so compute once using raw DM/TR
    # Smoothing is applied separately for each n
    base_tr = df["tr"]
    base_plus_dm = df["dm_pos"]
    base_minus_dm = df["dm_neg"]

    # Compute ADX for each period
    for period in periods:
        tr_n = base_tr.rolling(window=period, min_periods=1).sum()
        plus_dm_n = base_plus_dm.rolling(window=period, min_periods=1).sum()
        minus_dm_n = base_minus_dm.rolling(window=period, min_periods=1).sum()

        df[f"di_pos_{period}"] = 100 * (plus_dm_n / tr_n)
        df[f"di_neg_{period}"] = 100 * (minus_dm_n / tr_n)

        dx = (
            100
            * abs(df[f"di_pos_{period}"] - df[f"di_neg_{period}"])
            / (df[f"di_pos_{period}"] + df[f"di_neg_{period}"]).replace(0, np.nan)
        ).fillna(0)

        df[f"adx_{period}"] = dx.rolling(window=period, min_periods=1).mean()

    return df


# endregion TREND INDICATORS


# region VOLATILITY INDICATORS
def add_bollinger_bands(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    k: float = 2.0,
    column_name: str = "close",
    default_bb_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Bollinger Bands (upper, middle, lower) to the DataFrame.

    Default popular Bollinger Band periods:
        20 (SMA period), standard deviation multiplier k=2.0

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        SMA period(s) for Bollinger Bands. If None, default period 20 is used.
    k : float, optional
        Number of standard deviations for upper/lower bands (default 2.0)
    column_name : str, optional
        Column to calculate Bollinger Bands on. Default is 'close'.
    default_bb_periods : list[int], optional
        Override the default SMA period(s).

    Returns
    -------
    pd.DataFrame
        DataFrame with Bollinger Bands added:
        '{column_name}_bb_middle_{n}', '{column_name}_bb_upper_{n}', '{column_name}_bb_lower_{n}'
    """
    validate_column(df, column_name)
    df = df.copy()

    # Default period
    if default_bb_periods is None:
        default_bb_periods = [20]

    # Determine periods to compute
    if n is None:
        periods = default_bb_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    for period in periods:
        sma = df[column_name].rolling(window=period, min_periods=1).mean()
        std = df[column_name].rolling(window=period, min_periods=1).std()

        df[f"{column_name}_bb_middle_{period}"] = sma
        df[f"{column_name}_bb_upper_{period}"] = sma + (k * std)
        df[f"{column_name}_bb_lower_{period}"] = sma - (k * std)

    return df


def add_keltner_channel(
    df: pd.DataFrame,
    n: int = 20,
    k: float = 2.0,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Keltner Channel (upper, middle, lower) to the DataFrame.

    The Keltner Channel consists of a middle line (EMA of the close) and
    upper/lower bands calculated as the EMA ± k times the ATR. It is used
    to measure volatility and identify potential breakouts.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    n : int, optional
        Period for EMA and ATR calculation (default is 20).
    k : float, optional
        Multiplier for ATR to calculate upper and lower bands (default is 2.0).
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with added columns:
        'kc_middle_{n}', 'kc_upper_{n}', 'kc_lower_{n}'.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Middle line (EMA of close)
    df[f"kc_middle_{n}"] = df[close_col].ewm(span=n, adjust=False).mean()

    # True Range
    tr = pd.concat(
        [
            df[high_col] - df[low_col],
            (df[high_col] - df[close_col].shift()).abs(),
            (df[low_col] - df[close_col].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # ATR (Wilder’s moving average of TR)
    atr = tr.rolling(window=n, min_periods=1).mean()

    # Upper & Lower bands
    df[f"kc_upper_{n}"] = df[f"kc_middle_{n}"] + k * atr
    df[f"kc_lower_{n}"] = df[f"kc_middle_{n}"] - k * atr

    return df


def add_starc_band(
    df: pd.DataFrame,
    n: int = 20,
    k: float = 2.0,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add STARC Bands (upper, middle, lower) to the DataFrame.

    STARC Bands consist of a middle band (SMA of the close) and upper/lower
    bands calculated as the SMA ± k times the ATR. They are used to measure
    volatility and potential overbought/oversold conditions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    n : int, optional
        Period for SMA and ATR calculation (default is 20).
    k : float, optional
        Multiplier for ATR to calculate upper and lower bands (default is 2.0).
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with added columns:
        'starc_middle_{n}', 'starc_upper_{n}', 'starc_lower_{n}'.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Middle Band (SMA of close)
    df[f"starc_middle_{n}"] = df[close_col].rolling(window=n, min_periods=1).mean()

    # True Range
    tr = pd.concat(
        [
            df[high_col] - df[low_col],
            (df[high_col] - df[close_col].shift()).abs(),
            (df[low_col] - df[close_col].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # ATR (simple rolling mean)
    atr = tr.rolling(window=n, min_periods=1).mean()

    # Upper & Lower Bands
    df[f"starc_upper_{n}"] = df[f"starc_middle_{n}"] + k * atr
    df[f"starc_lower_{n}"] = df[f"starc_middle_{n}"] - k * atr

    return df


def add_atr(
    df: pd.DataFrame,
    n: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Average True Range (ATR) to the DataFrame.

    ATR measures market volatility by calculating the smoothed average
    of True Range over a specified period.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    n : int, optional
        Period for ATR calculation (default is 14).
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'atr_{n}'.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # True Range
    tr = pd.concat(
        [
            df[high_col] - df[low_col],
            (df[high_col] - df[close_col].shift()).abs(),
            (df[low_col] - df[close_col].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder’s ATR (RMA)
    df[f"atr_{n}"] = tr.ewm(alpha=1 / n, adjust=False).mean()

    return df


def add_divergence_index(
    df: pd.DataFrame,
    n: int = 14,
    k: float = 1.0,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Divergence Index (DVI) to the DataFrame.

    The Divergence Index measures the distance of the price from its SMA
    relative to market volatility (ATR), helping identify overbought or oversold conditions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    n : int, optional
        Period for SMA and ATR calculation (default is 14).
    k : float, optional
        Scaling factor for ATR in the calculation (default is 1.0).
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'dvi_{n}'.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Ensure numeric
    df[high_col] = pd.to_numeric(df[high_col], errors="coerce")
    df[low_col] = pd.to_numeric(df[low_col], errors="coerce")
    df[close_col] = pd.to_numeric(df[close_col], errors="coerce")

    # SMA of close
    sma = df[close_col].rolling(window=n, min_periods=1).mean()

    # True Range
    tr = pd.concat(
        [
            df[high_col] - df[low_col],
            (df[high_col] - df[close_col].shift()).abs(),
            (df[low_col] - df[close_col].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder’s ATR
    atr = tr.ewm(alpha=1 / n, adjust=False).mean()

    # Divergence Index
    df[f"dvi_{n}"] = (df[close_col] - sma) / (k * atr)

    return df


# endregion VOLATILITY INDICATORS


# region MOMENTUN INDICATORS


def add_rsi(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
    default_rsi_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Relative Strength Index (RSI) to the DataFrame.

    Default popular RSI periods: 7, 14, 21, 28

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        RSI lookback period(s). If None, popular defaults are used.
    column_name : str, optional
        Column to calculate RSI on. Default is 'close'.
    default_rsi_periods : list[int], optional
        Override the default RSI periods.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'rsi_{period}' columns.
    """
    validate_column(df, column_name)
    df = df.copy()

    if default_rsi_periods is None:
        default_rsi_periods = [7, 14, 21, 28]

    if n is None:
        periods = default_rsi_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    close = np.asarray(df[column_name], dtype="float64")
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    for period in periods:
        avg_gain = pd.Series(gain).rolling(period, min_periods=period).mean()
        avg_loss = pd.Series(loss).rolling(period, min_periods=period).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        df[f"rsi_{period}"] = rsi

    return df


def add_roc(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
    default_roc_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Rate of Change (ROC) indicator to the DataFrame.

    Default popular ROC periods: 9, 12, 14, 20, 25

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the target column.
    n : int or list[int], optional
        Lookback period(s) for ROC. If None, popular defaults are used.
    column_name : str, optional
        Column to calculate ROC on. Default is 'close'.
    default_roc_periods : list[int], optional
        Override the default ROC periods.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'roc_{period}' columns.
    """
    validate_column(df, column_name)
    df = df.copy()

    if default_roc_periods is None:
        default_roc_periods = [9, 12, 14, 20, 25]

    if n is None:
        periods = default_roc_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    close = pd.Series(df[column_name], dtype="float64")

    for period in periods:
        roc = ((close - close.shift(period)) / close.shift(period)) * 100
        df[f"roc_{period}"] = roc

    return df


def add_macd(
    df: pd.DataFrame,
    short_n: int | list[int] = 12,
    long_n: int | list[int] = 26,
    signal_n: int | list[int] = 9,
) -> pd.DataFrame:
    """
    Add Moving Average Convergence/Divergence (MACD) to the DataFrame.

    Default popular MACD parameters:
        short_n = 12, long_n = 26, signal_n = 9

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least a 'close' column.
    short_n : int or list[int], optional
        Short-term EMA period(s). Default 12.
    long_n : int or list[int], optional
        Long-term EMA period(s). Default 26.
    signal_n : int or list[int], optional
        Signal line EMA period(s). Default 9.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added MACD columns:
        - 'macd_{short}_{long}'
        - 'macd_signal_{signal}'
        - 'macd_hist_{short}_{long}_{signal}'
    """
    df = df.copy()

    # Ensure lists for iteration
    short_list = [short_n] if isinstance(short_n, int) else list(short_n)
    long_list = [long_n] if isinstance(long_n, int) else list(long_n)
    signal_list = [signal_n] if isinstance(signal_n, int) else list(signal_n)

    for s in short_list:
        for l in long_list:
            if l <= s:
                continue  # long EMA must be greater than short EMA
            macd_series = (
                df["close"].ewm(span=s, adjust=False).mean()
                - df["close"].ewm(span=l, adjust=False).mean()
            )
            for sig in signal_list:
                signal_series = macd_series.ewm(span=sig, adjust=False).mean()
                hist_series = macd_series - signal_series

                df[f"macd_{s}_{l}"] = macd_series
                df[f"macd_signal_{sig}"] = signal_series
                df[f"macd_hist_{s}_{l}_{sig}"] = hist_series

    return df


def add_stochastic(
    df: pd.DataFrame,
    k_period: int | list[int] | None = None,
    d_period: int | list[int] | None = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    default_k_periods: list[int] = None,
    default_d_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Stochastic Oscillator (%K and %D) to the DataFrame.
    Replaces inf values with NULL (NaN).
    """

    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Default periods
    if default_k_periods is None:
        default_k_periods = [14]
    if default_d_periods is None:
        default_d_periods = [3]

    if k_period is None:
        k_list = default_k_periods
    elif isinstance(k_period, int):
        k_list = [k_period]
    else:
        k_list = list(k_period)

    if d_period is None:
        d_list = default_d_periods
    elif isinstance(d_period, int):
        d_list = [d_period]
    else:
        d_list = list(d_period)

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    for k in k_list:
        low_min = low.rolling(window=k, min_periods=1).min()
        high_max = high.rolling(window=k, min_periods=1).max()

        # Prevent division by zero by producing inf (we will convert to NaN later)
        stoch_k = 100 * (close - low_min) / (high_max - low_min)
        df[f"stoch_k_{k}"] = stoch_k

        for d in d_list:
            stoch_d = stoch_k.rolling(window=d, min_periods=1).mean()
            df[f"stoch_d_{d}"] = stoch_d

    # 🔍 Replace infinite values with NULL (NaN)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df


def add_williams_r(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    default_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Williams %R indicator to the DataFrame.

    Default popular periods: 9, 14, 21

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with high, low, close columns.
    n : int or list[int], optional
        Lookback period(s). If None, popular defaults are used.
    high_col, low_col, close_col : str, optional
        Column names for high, low, close prices.
    default_periods : list[int], optional
        Override the default periods.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'williams_r_{period}' columns.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    if default_periods is None:
        default_periods = [9, 14, 21]

    if n is None:
        periods = default_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    for period in periods:
        highest_high = high.rolling(window=period, min_periods=1).max()
        lowest_low = low.rolling(window=period, min_periods=1).min()
        williams_r = (highest_high - close) / (highest_high - lowest_low) * -100
        df[f"williams_r_{period}"] = williams_r

    # 🔍 Replace infinite values with NULL (NaN)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df


def add_ado(
    df: pd.DataFrame,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Larry Williams' Accumulation/Distribution (AD) Oscillator to the DataFrame.

    Formula:
        ADO = ((Close - Open) / (High - Low)) * 100

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified open, high, low, and close columns.
    open_col : str, optional
        Name of the open price column. Defaults to 'open'.
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'ad'.
    """
    for col in [open_col, high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    open_ = pd.to_numeric(df[open_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    ado = ((close - open_) / (high - low).replace(0, np.nan)) * 100
    df["ado"] = ado

    return df


def add_rvi(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    signal: int | list[int] | None = None,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    default_n: list[int] = None,
    default_signal: list[int] = None,
) -> pd.DataFrame:
    """
    Add Relative Vigor Index (RVI) and its signal line to the DataFrame.

    Default popular parameters:
        n periods: [10, 14]
        signal:   [4]

    Formula:
        RV = (Close - Open) / (High - Low)
        RVI = SMA(RV, n)
        Signal = SMA(RVI, signal)
    """

    # Validate columns
    for col in [open_col, high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Defaults
    if default_n is None:
        default_n = [10, 14]  # Popular RVI periods
    if default_signal is None:
        default_signal = [4]  # Standard signal length

    # Parse user inputs
    if n is None:
        n_list = default_n
    elif isinstance(n, int):
        n_list = [n]
    else:
        n_list = list(n)

    if signal is None:
        signal_list = default_signal
    elif isinstance(signal, int):
        signal_list = [signal]
    else:
        signal_list = list(signal)

    # Numeric columns
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    open_ = pd.to_numeric(df[open_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    # Raw RV value
    rv = (close - open_) / (high - low).replace(0, np.nan)

    # Calculate RVI + signals
    for n_period in n_list:
        rvi = rv.rolling(window=n_period, min_periods=1).mean()
        df[f"rvi_{n_period}"] = rvi

        for sig in signal_list:
            df[f"rvi_signal_{n_period}_{sig}"] = rvi.rolling(
                window=sig, min_periods=1
            ).mean()

    return df


def add_tsi(
    df: pd.DataFrame,
    r: int | list[int] | None = None,
    s: int | list[int] | None = None,
    column_name: str = "close",
    default_r: list[int] = None,
    default_s: list[int] = None,
    add_signal: bool = True,
    signal_period: int = 7,
) -> pd.DataFrame:
    """
    Add True Strength Index (TSI) to the DataFrame with support for
    multiple popular parameter sets.

    Popular defaults:
        r (long periods): [25, 13, 50]
        s (short periods): [13, 7, 25]
        signal: 7 (optional)

    Formula:
        m_t = Close_t - Close_{t-1}
        TSI = 100 * (EMA_r(EMA_s(m_t)) / EMA_r(EMA_s(|m_t|)))
    """

    validate_column(df, column_name)
    df = df.copy()

    # Default parameter lists
    if default_r is None:
        default_r = [25, 13, 50]
    if default_s is None:
        default_s = [13, 7, 25]

    # Normalize inputs
    if r is None:
        r_list = default_r
    elif isinstance(r, int):
        r_list = [r]
    else:
        r_list = list(r)

    if s is None:
        s_list = default_s
    elif isinstance(s, int):
        s_list = [s]
    else:
        s_list = list(s)

    # Price series
    close = pd.to_numeric(df[column_name], errors="coerce").astype("float64")
    momentum = close.diff()

    # Loop through combinations
    for rr in r_list:
        for ss in s_list:
            # Short EMAs
            ema_mom_s = momentum.ewm(span=ss, adjust=False).mean()
            ema_abs_s = momentum.abs().ewm(span=ss, adjust=False).mean()

            # Long EMAs
            ema_mom_r = ema_mom_s.ewm(span=rr, adjust=False).mean()
            ema_abs_r = ema_abs_s.ewm(span=rr, adjust=False).mean()

            tsi = 100 * (ema_mom_r / ema_abs_r)
            name = f"tsi_{rr}_{ss}"
            df[name] = tsi

            # Optional TSI Signal line (common period = 7)
            if add_signal:
                df[f"{name}_signal_{signal_period}"] = tsi.ewm(
                    span=signal_period, adjust=False
                ).mean()

    return df


def add_vortex(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    default_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add Vortex Indicator (+VI and -VI) for one or multiple popular parameter sets.

    Popular periods: 7, 14, 21, 28 (14 is standard)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain high, low, and close columns.
    n : int | list[int] | None, optional
        Single period or list of periods to compute. If None, uses default popular periods.
    high_col, low_col, close_col : str
        Column names for OHLC values.
    default_periods : list[int], optional
        Override list of default popular periods.

    Returns
    -------
    pd.DataFrame
        DataFrame with added vortex indicator columns:
        vi_plus_{n}, vi_minus_{n}
    """

    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    # Default popular periods
    if default_periods is None:
        default_periods = [7, 14, 21, 28]

    # Normalize input
    if n is None:
        periods = default_periods
    elif isinstance(n, int):
        periods = [n]
    else:
        periods = list(n)

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")

    # True Range (TR)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Vortex movements
    vm_plus = (high - low.shift(1)).abs()
    vm_minus = (low - high.shift(1)).abs()

    # Compute for each period
    for p in periods:
        tr_p = tr.rolling(p, min_periods=1).sum()
        vm_plus_p = vm_plus.rolling(p, min_periods=1).sum()
        vm_minus_p = vm_minus.rolling(p, min_periods=1).sum()

        df[f"vi_plus_{p}"] = vm_plus_p / tr_p
        df[f"vi_minus_{p}"] = vm_minus_p / tr_p

    return df


# endregion MOMENTUM INDICATORS


# region VOLUME INDICATORS
def add_obv(
    df: pd.DataFrame,
    close_col: str = "close",
    volume_col: str = "volume",
    ema_periods: list[int] = None,
    sma_periods: list[int] = None,
) -> pd.DataFrame:
    """
    Add On-Balance Volume (OBV) and optional smoothed OBV indicators.

    Popular OBV smoothing periods:
        EMA: 20, 50, 100
        SMA: 10, 20, 50

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing close and volume columns.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.
    ema_periods : list[int], optional
        EMA smoothing periods for OBV. Defaults to [20, 50, 100].
    sma_periods : list[int], optional
        SMA smoothing periods for OBV. Defaults to [10, 20, 50].

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
            obv
            obv_ema_{p}
            obv_sma_{p}
    """

    for col in [close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    # Defaults for popular smoothing periods
    if ema_periods is None:
        ema_periods = [20, 50, 100]
    if sma_periods is None:
        sma_periods = [10, 20, 50]

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    # Base OBV
    obv = [0]
    for i in range(1, len(close)):
        if close.iloc[i] > close.iloc[i - 1]:
            obv.append(obv[-1] + volume.iloc[i])
        elif close.iloc[i] < close.iloc[i - 1]:
            obv.append(obv[-1] - volume.iloc[i])
        else:
            obv.append(obv[-1])

    df["obv"] = obv

    # Add smoothed versions
    for p in ema_periods:
        df[f"obv_ema_{p}"] = df["obv"].ewm(span=p, adjust=False).mean()

    for p in sma_periods:
        df[f"obv_sma_{p}"] = df["obv"].rolling(p, min_periods=1).mean()

    return df


def add_mfi(
    df: pd.DataFrame,
    n_list: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Money Flow Index (MFI) indicators to the DataFrame for multiple popular periods.

    Popular MFI periods:
        5 (very fast)
        7 (fast)
        10 (medium-fast)
        14 (standard)
        20 (slow)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with high, low, close, and volume columns.
    n_list : list[int], optional
        List of MFI periods to compute. Defaults to [5, 7, 10, 14, 20].
    high_col : str
    low_col : str
    close_col : str
    volume_col : str

    Returns
    -------
    pd.DataFrame
        DataFrame with added columns: mfi_{n} for each n in n_list.
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    if n_list is None:
        n_list = [5, 7, 10, 14, 20]  # popular defaults

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    tp = (high + low + close) / 3
    rmf = tp * volume

    pos_mf = np.where(tp > tp.shift(1), rmf, 0.0)
    neg_mf = np.where(tp < tp.shift(1), rmf, 0.0)

    pos_mf_series = pd.Series(pos_mf)
    neg_mf_series = pd.Series(neg_mf)

    for n in n_list:
        pos_sum = pos_mf_series.rolling(n, min_periods=1).sum()
        neg_sum = neg_mf_series.rolling(n, min_periods=1).sum()

        mfi = 100 * (pos_sum / (pos_sum + neg_sum))
        df[f"mfi_{n}"] = mfi

    return df


def add_adl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Larry Williams’ Accumulation/Distribution Line (ADL) to the DataFrame.

    The ADL measures supply and demand by evaluating where the close lies
    within the period’s high-low range, then multiplying by volume.
    It is a cumulative indicator.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'high', 'low', 'close', and 'volume' columns.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added 'adl' column.
    """
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    close = df["close"].astype("float64")
    volume = df["volume"].astype("float64")

    # Avoid division by zero
    clv = ((close - low) - (high - close)) / np.where(high != low, (high - low), 1e-10)

    # Money Flow Volume
    mfv = clv * volume

    # Cumulative ADL
    df["adl"] = mfv.cumsum()

    return df


def add_chaikin_ad(
    df: pd.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Marc Chaikin’s Accumulation/Distribution (AD) Line to the DataFrame.

    The Chaikin AD Line uses the Close Location Value (CLV) multiplied by
    volume to track the flow of money into or out of a security.
    It is a cumulative indicator.

    Formula
    -------
    CLV = ((Close - Low) - (High - Close)) / (High - Low)
    AD  = cumulative sum of (CLV * Volume)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, close, and volume columns.
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'chaikin_ad'.
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    # Close Location Value (CLV)
    clv = ((close - low) - (high - close)) / np.where(high != low, (high - low), 1e-10)

    # Money Flow Volume
    mfv = clv * volume

    # Chaikin AD Line (cumulative)
    df["chaikin_ad"] = mfv.cumsum()

    return df


def add_cmf(
    df: pd.DataFrame,
    periods: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Chaikin Money Flow (CMF) indicators for multiple popular periods.

    Popular CMF periods:
        10, 20 (default), 21, 34, 50

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with high, low, close, and volume columns.
    periods : list[int], optional
        List of lookback periods for CMF. Defaults to [10, 20, 21, 34, 50].
    high_col : str
    low_col : str
    close_col : str
    volume_col : str

    Returns
    -------
    pd.DataFrame
        DataFrame with added columns: cmf_{n} for each period in periods.
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    if periods is None:
        periods = [10, 20, 21, 34, 50]

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    clv = ((close - low) - (high - close)) / np.where(high != low, (high - low), 1e-10)
    mfv = clv * volume

    for n in periods:
        cmf = (
            mfv.rolling(window=n, min_periods=1).sum()
            / volume.rolling(window=n, min_periods=1).sum()
        )
        df[f"cmf_{n}"] = cmf

    return df


def add_vroc(
    df: pd.DataFrame,
    periods: list[int] = None,
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Volume Rate of Change (VROC) indicators for multiple popular periods.

    Popular VROC periods:
        5, 10, 14 (default), 20, 50

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with a volume column.
    periods : list[int], optional
        List of lookback periods for VROC. Defaults to [5, 10, 14, 20, 50].
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.

    Returns
    -------
    pd.DataFrame
        DataFrame with added columns: vroc_{n} for each period in periods.
    """
    validate_column(df, volume_col)
    df = df.copy()

    if periods is None:
        periods = [5, 10, 14, 20, 50]

    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    for n in periods:
        df[f"vroc_{n}"] = (volume - volume.shift(n)) / volume.shift(n) * 100

    return df


def add_eom(
    df: pd.DataFrame,
    smooth_periods: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Ease of Movement (EoM) indicator with smoothed values for multiple popular periods.

    Popular smoothing periods: 5, 10, 14 (default), 20, 50

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with high, low, and volume columns.
    smooth_periods : list[int], optional
        List of SMA periods to smooth EoM. Defaults to [5, 10, 14, 20, 50].
    high_col : str, optional
    low_col : str, optional
    volume_col : str, optional

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
            - 'eom' (raw values)
            - 'eom_{n}' for each period in smooth_periods
    """
    for col in [high_col, low_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    if smooth_periods is None:
        smooth_periods = [5, 10, 14, 20, 50]

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    midpoint_move = ((high + low) / 2) - ((high.shift(1) + low.shift(1)) / 2)
    box_ratio = np.where((high - low) != 0, (volume / 1e6) / (high - low), 0)
    eom = np.where(box_ratio != 0, midpoint_move / box_ratio, 0)

    df["eom"] = eom

    for n in smooth_periods:
        df[f"eom_{n}"] = pd.Series(eom).rolling(window=n, min_periods=1).mean()

    return df


def add_pvi_nvi(
    df: pd.DataFrame, close_col: str = "close", volume_col: str = "volume"
) -> pd.DataFrame:
    """
    Add Positive Volume Index (PVI) and Negative Volume Index (NVI) to the DataFrame.

    PVI assumes that when volume increases, the 'crowd' is driving prices.
    NVI assumes that when volume decreases, the 'smart money' is active.

    Formula
    -------
    If Volume[t] > Volume[t-1]:
        PVI[t] = PVI[t-1] + ((Close[t] - Close[t-1]) / Close[t-1]) * PVI[t-1]
    else:
        PVI[t] = PVI[t-1]

    If Volume[t] < Volume[t-1]:
        NVI[t] = NVI[t-1] + ((Close[t] - Close[t-1]) / Close[t-1]) * NVI[t-1]
    else:
        NVI[t] = NVI[t-1]

    Both series usually start from 1000.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified close and volume columns.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with added columns 'pvi' and 'nvi'.
    """
    for col in [close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64").values
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64").values

    pvi = np.zeros(len(df))
    nvi = np.zeros(len(df))
    pvi[0], nvi[0] = 1000, 1000

    for i in range(1, len(df)):
        if volume[i] > volume[i - 1]:
            pvi[i] = (
                pvi[i - 1] + ((close[i] - close[i - 1]) / close[i - 1]) * pvi[i - 1]
            )
        else:
            pvi[i] = pvi[i - 1]

        if volume[i] < volume[i - 1]:
            nvi[i] = (
                nvi[i - 1] + ((close[i] - close[i - 1]) / close[i - 1]) * nvi[i - 1]
            )
        else:
            nvi[i] = nvi[i - 1]

    df["pvi"] = pvi
    df["nvi"] = nvi

    return df


def add_vw_macd(
    df: pd.DataFrame,
    param_sets: list[tuple[int, int, int]] = None,
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Volume-Weighted MACD (VW-MACD) and signal line for multiple popular parameter sets.

    Popular parameter sets:
        (12, 26, 9) -> standard
        (5, 13, 6)  -> short-term
        (8, 21, 5)  -> medium-term

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with close and volume columns.
    param_sets : list of tuples, optional
        List of (fast, slow, signal) parameter sets. Defaults to [(12, 26, 9)].
    close_col : str
    volume_col : str

    Returns
    -------
    pd.DataFrame
        DataFrame with VW-MACD columns for each parameter set:
            - vw_macd_{fast}_{slow}
            - vw_macd_signal_{signal}
            - vw_macd_hist_{fast}_{slow}_{signal}
    """
    for col in [close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    if param_sets is None:
        param_sets = [(12, 26, 9)]

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    # Volume-weighted price
    vp = close * volume
    vw_price = vp.cumsum() / volume.cumsum()

    for fast, slow, signal in param_sets:
        ema_fast = vw_price.ewm(span=fast, adjust=False).mean()
        ema_slow = vw_price.ewm(span=slow, adjust=False).mean()

        vw_macd = ema_fast - ema_slow
        vw_signal = vw_macd.ewm(span=signal, adjust=False).mean()
        vw_hist = vw_macd - vw_signal

        df[f"vw_macd_{fast}_{slow}"] = vw_macd
        df[f"vw_macd_signal_{signal}"] = vw_signal
        df[f"vw_macd_hist_{fast}_{slow}_{signal}"] = vw_hist

    return df


def add_kvo(
    df: pd.DataFrame,
    param_sets: list[tuple[int, int, int]] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Klinger Volume Oscillator (KVO) and signal line for multiple popular parameter sets.

    Popular parameter sets:
        (34, 55, 13) -> standard
        (13, 34, 5)  -> short-term
        (21, 55, 13) -> medium-term

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with high, low, close, and volume columns.
    param_sets : list of tuples, optional
        List of (fast, slow, signal) parameter sets. Defaults to [(34, 55, 13)].
    high_col : str
    low_col : str
    close_col : str
    volume_col : str

    Returns
    -------
    pd.DataFrame
        DataFrame with KVO columns for each parameter set:
            - kvo_{fast}_{slow}
            - kvo_signal_{signal}
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    if param_sets is None:
        param_sets = [(34, 55, 13)]

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    tp = (high + low + close) / 3
    trend = np.where(tp > tp.shift(1), 1, -1)
    vf = trend * 2 * ((high - low) / (high + low + 1e-9)) * volume

    for fast, slow, signal in param_sets:
        ema_fast = pd.Series(vf).ewm(span=fast, adjust=False).mean()
        ema_slow = pd.Series(vf).ewm(span=slow, adjust=False).mean()
        kvo = ema_fast - ema_slow
        kvo_signal = kvo.ewm(span=signal, adjust=False).mean()

        df[f"kvo_{fast}_{slow}"] = kvo
        df[f"kvo_signal_{signal}"] = kvo_signal

    return df


def add_demand_oscillator(
    df: pd.DataFrame,
    param_sets: list[tuple[int, int]] = None,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Aspray's Demand Oscillator (ADO) to the DataFrame for multiple parameter sets.

    Popular parameter sets:
        (5, 10)  -> standard short-term
        (3, 7)   -> very fast
        (8, 14)  -> medium-term

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with open, high, low, close, and volume columns.
    param_sets : list of tuples, optional
        List of (fast, slow) EMA periods. Defaults to [(5, 10)].
    open_col, high_col, low_col, close_col, volume_col : str
        Column names for OHLCV data.

    Returns
    -------
    pd.DataFrame
        Copy of DataFrame with added ADO columns for each parameter set:
        - 'demand_osc_{fast}_{slow}'
    """
    for col in [open_col, high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    if param_sets is None:
        param_sets = [(5, 10)]

    open_ = pd.to_numeric(df[open_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    prev_close = close.shift(1)
    up_move = high - np.minimum(open_, prev_close)
    down_move = np.maximum(open_, prev_close) - low
    demand = ((up_move - down_move) / (up_move + down_move + 1e-9)) * volume

    for fast, slow in param_sets:
        ema_fast = demand.ewm(span=fast, adjust=False).mean()
        ema_slow = demand.ewm(span=slow, adjust=False).mean()
        df[f"demand_osc_{fast}_{slow}"] = ema_fast - ema_slow

    return df


# endregion VOLUME INDICATORS


def add_one_for_all_ta(df: pd.DataFrame) -> pd.DataFrame:
    new_df = df.copy()
    new_df = add_sma(new_df)
    new_df = add_ema(new_df)
    new_df = add_lwma(new_df)
    new_df = add_wma(new_df)
    new_df = add_adx(new_df)
    new_df = add_bollinger_bands(new_df)
    new_df = add_keltner_channel(new_df)
    new_df = add_keltner_channel(new_df)
    new_df = add_starc_band(new_df)
    new_df = add_atr(new_df)
    new_df = add_divergence_index(new_df)
    new_df = add_rsi(new_df)
    new_df = add_roc(new_df)
    new_df = add_macd(new_df)
    new_df = add_stochastic(new_df)
    new_df = add_williams_r(new_df)
    new_df = add_ado(new_df)
    new_df = add_rvi(new_df)
    new_df = add_tsi(new_df)
    new_df = add_vortex(new_df)
    new_df = add_obv(new_df)
    new_df = add_mfi(new_df)
    new_df = add_adl(new_df)
    new_df = add_chaikin_ad(new_df)
    new_df = add_cmf(new_df)
    new_df = add_vroc(new_df)
    new_df = add_eom(new_df)
    new_df = add_pvi_nvi(new_df)
    new_df = add_vw_macd(new_df)
    new_df = add_kvo(new_df)
    new_df = add_demand_oscillator(new_df)

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
    df = ta_database_driver.select(
        schema_name=Schema.STOCK_MARKET.value,
        table_name=Table.VN_INDEX.name,
        conditions=[
            Condition(
                column=Table.VN_INDEX.Column.DATE.value,
                operator=SqlOperator.GREATER_THAN_OR_EQUAL_TO,
                value="2022-01-01",
                data_type=DataType.DATE,
            ),
            Condition(
                column=Table.VN_INDEX.Column.DATE.value,
                operator=SqlOperator.LESS_THAN_OR_EQUAL_TO,
                value="2024-12-31",
                data_type=DataType.DATE,
            ),
        ],
        order_by=[Table.VN_INDEX.Column.DATE.value],
    )

    df = add_one_for_all_ta(df)

    # plot_with_indicators(
    #     df,
    #     indicators=["demand_*"],
    # )

    print(len(df.columns))


if __name__ == "__main__":
    main()
