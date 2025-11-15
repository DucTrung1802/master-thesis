import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys, os
from dotenv import load_dotenv

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


# region TREND INDICATORS
def add_sma(
    df: pd.DataFrame, n: list[int] = None, column_name: str = "close"
) -> pd.DataFrame:
    """
    Add one or multiple Simple Moving Average (SMA) columns to the DataFrame.

    The SMA is the unweighted mean of the previous `n` values from the specified column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified column (default is 'close').
    n : list[int], optional
        List of window sizes for the SMAs. Defaults to [50, 100, 200].
    column_name : str, optional
        Name of the column to calculate the SMA on. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with added SMA columns for all values in `n`.
    """
    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()

    for window in n:
        df[f"{column_name}_sma_{window}"] = (
            df[column_name].rolling(window=window, min_periods=1).mean()
        )

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

    df["+dm"] = (
        df[high_col]
        .diff()
        .where(
            (df[high_col].diff() > df[low_col].diff() * -1) & (df[high_col].diff() > 0),
            0.0,
        )
    )

    df["-dm"] = (-df[low_col].diff()).where(
        (-df[low_col].diff() > df[high_col].diff()) & (-df[low_col].diff() > 0), 0.0
    )

    # +DI and -DI are same for all ADX periods, so compute once using raw DM/TR
    # Smoothing is applied separately for each n
    base_tr = df["tr"]
    base_plus_dm = df["+dm"]
    base_minus_dm = df["-dm"]

    # Compute ADX for each period
    for period in periods:
        tr_n = base_tr.rolling(window=period, min_periods=1).sum()
        plus_dm_n = base_plus_dm.rolling(window=period, min_periods=1).sum()
        minus_dm_n = base_minus_dm.rolling(window=period, min_periods=1).sum()

        df[f"+di_{period}"] = 100 * (plus_dm_n / tr_n)
        df[f"-di_{period}"] = 100 * (minus_dm_n / tr_n)

        dx = (
            100
            * abs(df[f"+di_{period}"] - df[f"-di_{period}"])
            / (df[f"+di_{period}"] + df[f"-di_{period}"]).replace(0, np.nan)
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


def add_roc(df: pd.DataFrame, n: int = 14, column_name: str = "close") -> pd.DataFrame:
    """
    Add Rate of Change (ROC) indicator to the DataFrame.

    ROC measures the percentage change in the price compared to
    the price n periods ago, indicating the speed of price movements.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified column (default is 'close').
    n : int, optional
        Lookback period for ROC calculation (default is 14).
    column_name : str, optional
        Name of the column to calculate ROC on. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'roc_{n}'.
    """
    validate_column(df, column_name)
    df = df.copy()

    close = np.asarray(df[column_name], dtype="float64")
    roc = (
        (pd.Series(close) - pd.Series(close).shift(n)) / pd.Series(close).shift(n) * 100
    )
    df[f"roc_{n}"] = roc

    return df


def add_macd(
    df: pd.DataFrame, short_n: int = 12, long_n: int = 26, signal_n: int = 9
) -> pd.DataFrame:
    """
    Add Moving Average Convergence/Divergence (MACD) to the dataframe.

    MACD is calculated as the difference between a short-term EMA
    and a long-term EMA. A signal line (EMA of MACD) is also added.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least a 'close' column.
    short_n : int, default 12
        Period for short-term EMA.
    long_n : int, default 26
        Period for long-term EMA.
    signal_n : int, default 9
        Period for the signal line EMA.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added columns:
        - 'macd_{short_n}_{long_n}'
        - 'macd_signal_{signal_n}'
        - 'macd_hist_{short_n}_{long_n}_{signal_n}'
    """
    short_ema = df["close"].ewm(span=short_n, adjust=False).mean()
    long_ema = df["close"].ewm(span=long_n, adjust=False).mean()

    macd = short_ema - long_ema
    signal = macd.ewm(span=signal_n, adjust=False).mean()
    hist = macd - signal

    df[f"macd_{short_n}_{long_n}"] = macd
    df[f"macd_signal_{signal_n}"] = signal
    df[f"macd_hist_{short_n}_{long_n}_{signal_n}"] = hist

    return df


def add_stochastic(
    df: pd.DataFrame,
    k_period: int = 14,
    d_period: int = 3,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Stochastic Oscillator (%K and %D) to the DataFrame.

    %K = (Close - LowestLow) / (HighestHigh - LowestLow) * 100
    %D = SMA of %K

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    k_period : int, optional
        Lookback period for %K calculation (default is 14).
    d_period : int, optional
        Lookback period for %D calculation (smoothing of %K, default is 3).
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
        - 'stoch_k_{k_period}'
        - 'stoch_d_{d_period}'
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    low_min = low.rolling(window=k_period, min_periods=1).min()
    high_max = high.rolling(window=k_period, min_periods=1).max()

    stoch_k = 100 * (close - low_min) / (high_max - low_min)
    stoch_d = stoch_k.rolling(window=d_period, min_periods=1).mean()

    df[f"stoch_k_{k_period}"] = stoch_k
    df[f"stoch_d_{d_period}"] = stoch_d

    return df


def add_williams_r(
    df: pd.DataFrame,
    n: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add William's %R indicator to the DataFrame.

    %R = (HighestHigh - Close) / (HighestHigh - LowestLow) * -100

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    n : int, optional
        Lookback period for calculation (default is 14).
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'williams_r_{n}'.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    highest_high = high.rolling(window=n, min_periods=1).max()
    lowest_low = low.rolling(window=n, min_periods=1).min()

    williams_r = (highest_high - close) / (highest_high - lowest_low) * -100
    df[f"williams_r_{n}"] = williams_r

    return df


def add_ado(
    df: pd.DataFrame,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Larry Williams’ Accumulation/Distribution (AD) Oscillator to the DataFrame.

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
    df["ad"] = ado

    return df


def add_rvi(
    df: pd.DataFrame,
    n: int = 10,
    signal: int = 4,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Relative Vigor Index (RVI) and its signal line to the DataFrame.

    Formula:
        RV = (Close - Open) / (High - Low)
        RVI = SMA(RV, n)
        Signal = SMA(RVI, signal)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified open, high, low, and close columns.
    n : int, optional
        Period for main RVI smoothing (default is 10).
    signal : int, optional
        Period for RVI signal line smoothing (default is 4).
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
        Copy of the input DataFrame with added columns:
        'rvi_{n}' and 'rvi_signal_{signal}'.
    """
    for col in [open_col, high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    open_ = pd.to_numeric(df[open_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")

    rv = (close - open_) / (high - low).replace(0, np.nan)
    rvi = rv.rolling(window=n, min_periods=1).mean()
    rvi_signal = rvi.rolling(window=signal, min_periods=1).mean()

    df[f"rvi_{n}"] = rvi
    df[f"rvi_signal_{signal}"] = rvi_signal

    return df


def add_tsi(
    df: pd.DataFrame, r: int = 25, s: int = 13, column_name: str = "close"
) -> pd.DataFrame:
    """
    Add True Strength Index (TSI) to the DataFrame.

    Formula:
        m_t = Close_t - Close_{t-1}
        TSI = 100 * (EMA_r(EMA_s(m_t)) / EMA_r(EMA_s(|m_t|)))

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified column (default is 'close').
    r : int, optional
        Long smoothing period (default is 25).
    s : int, optional
        Short smoothing period (default is 13).
    column_name : str, optional
        Name of the column to calculate TSI on. Defaults to 'close'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'tsi_{r}_{s}'.
    """
    validate_column(df, column_name)
    df = df.copy()

    close = pd.to_numeric(df[column_name], errors="coerce").astype("float64")
    momentum = close.diff()

    # Short EMA
    ema_mom_s = momentum.ewm(span=s, adjust=False).mean()
    ema_abs_s = momentum.abs().ewm(span=s, adjust=False).mean()

    # Long EMA
    ema_mom_r = ema_mom_s.ewm(span=r, adjust=False).mean()
    ema_abs_r = ema_abs_s.ewm(span=r, adjust=False).mean()

    tsi = 100 * (ema_mom_r / ema_abs_r)
    df[f"tsi_{r}_{s}"] = tsi

    return df


def add_vortex(
    df: pd.DataFrame,
    n: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Add Vortex Indicator (VI) to the DataFrame.

    The Vortex Indicator consists of two lines, +VI and -VI,
    that are derived from True Range (TR) and directional movements,
    helping identify trend direction and strength.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and close columns.
    n : int, optional
        Lookback period for calculation (default is 14).
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
        'vi_plus_{n}' and 'vi_minus_{n}'.
    """
    for col in [high_col, low_col, close_col]:
        validate_column(df, col)

    df = df.copy()

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")

    # True Range
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Vortex movements
    vm_plus = (high - low.shift(1)).abs()
    vm_minus = (low - high.shift(1)).abs()

    # Rolling sums
    tr_n = tr.rolling(n, min_periods=1).sum()
    vm_plus_n = vm_plus.rolling(n, min_periods=1).sum()
    vm_minus_n = vm_minus.rolling(n, min_periods=1).sum()

    df[f"vi_plus_{n}"] = vm_plus_n / tr_n
    df[f"vi_minus_{n}"] = vm_minus_n / tr_n

    return df


# endregion MOMENTUM INDICATORS


# region VOLUME INDICATORS
def add_obv(
    df: pd.DataFrame, close_col: str = "close", volume_col: str = "volume"
) -> pd.DataFrame:
    """
    Add On-Balance Volume (OBV) indicator to the DataFrame.

    OBV measures cumulative buying/selling pressure by adding
    volume on up days and subtracting volume on down days.

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
        Copy of the input DataFrame with an added column 'obv'.
    """
    for col in [close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    obv = [0]  # start OBV at 0
    for i in range(1, len(close)):
        if close.iloc[i] > close.iloc[i - 1]:
            obv.append(obv[-1] + volume.iloc[i])
        elif close.iloc[i] < close.iloc[i - 1]:
            obv.append(obv[-1] - volume.iloc[i])
        else:
            obv.append(obv[-1])

    df["obv"] = obv
    return df


def add_mfi(
    df: pd.DataFrame,
    n: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Money Flow Index (MFI) to the DataFrame.

    The MFI uses price and volume to identify overbought/oversold
    conditions, similar to RSI but volume-adjusted.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, close, and volume columns.
    n : int, optional
        Lookback period for MFI calculation (default is 14).
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
        Copy of the input DataFrame with an added column 'mfi_{n}'.
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    # Typical Price
    tp = (high + low + close) / 3

    # Raw Money Flow
    rmf = tp * volume

    # Positive & Negative Money Flow
    pos_mf = np.where(tp > tp.shift(1), rmf, 0.0)
    neg_mf = np.where(tp < tp.shift(1), rmf, 0.0)

    # Sum over n periods
    pos_mf_sum = pd.Series(pos_mf).rolling(n, min_periods=1).sum()
    neg_mf_sum = pd.Series(neg_mf).rolling(n, min_periods=1).sum()

    # Money Flow Index
    mfi = 100 * (pos_mf_sum / (pos_mf_sum + neg_mf_sum))
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
    n: int = 20,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Chaikin’s Money Flow (CMF) indicator to the DataFrame.

    CMF measures the amount of Money Flow Volume over a specific period.
    It oscillates between -1 and +1, indicating buying/selling pressure.

    Formula
    -------
    CLV = ((Close - Low) - (High - Close)) / (High - Low)
    MFV = CLV * Volume
    CMF = (Sum of MFV over n periods) / (Sum of Volume over n periods)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, close, and volume columns.
    n : int, optional
        Lookback period for CMF calculation (default is 20).
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
        Copy of the input DataFrame with an added column 'cmf_{n}'.
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    clv = ((close - low) - (high - close)) / np.where(high != low, (high - low), 1e-10)
    mfv = clv * volume

    cmf = (
        mfv.rolling(window=n, min_periods=1).sum()
        / volume.rolling(window=n, min_periods=1).sum()
    )
    df[f"cmf_{n}"] = cmf

    return df


def add_vroc(df: pd.DataFrame, n: int = 14, volume_col: str = "volume") -> pd.DataFrame:
    """
    Add Volume Rate of Change (VROC) indicator to the DataFrame.

    VROC measures the percentage change in volume compared to
    the volume n periods ago, indicating surges or drops in trading activity.

    Formula
    -------
    VROC = (Volume_t - Volume_{t-n}) / Volume_{t-n} * 100

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified volume column.
    n : int, optional
        Lookback period for VROC calculation (default is 14).
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'vroc_{n}'.
    """
    validate_column(df, volume_col)
    df = df.copy()

    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")
    vroc = (volume - volume.shift(n)) / volume.shift(n) * 100
    df[f"vroc_{n}"] = vroc

    return df


def add_eom(
    df: pd.DataFrame,
    n: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Ease of Movement (EoM) indicator to the DataFrame.

    EoM relates price change to volume, showing how much volume is
    required to move prices. A smoothed version (SMA of EoM) is
    often used.

    Formula
    -------
    Midpoint Move = ((High + Low)/2) - ((High[-1] + Low[-1])/2)
    Box Ratio     = (Volume / 1e6) / (High - Low)
    EoM           = Midpoint Move / Box Ratio
    EoM_smooth    = SMA(EoM, n)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, and volume columns.
    n : int, optional
        Smoothing period for EoM (default is 14).
    high_col : str, optional
        Name of the high price column. Defaults to 'high'.
    low_col : str, optional
        Name of the low price column. Defaults to 'low'.
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with added columns:
        - 'eom' (raw values)
        - 'eom_{n}' (smoothed values)
    """
    for col in [high_col, low_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    midpoint_move = ((high + low) / 2) - ((high.shift(1) + low.shift(1)) / 2)
    box_ratio = np.where((high - low) != 0, (volume / 1e6) / (high - low), 0)
    eom = np.where(box_ratio != 0, midpoint_move / box_ratio, 0)

    df["eom"] = eom
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
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Volume-Weighted MACD (VW-MACD) and signal line to the DataFrame.

    VW-MACD uses volume-weighted prices instead of closing prices to measure momentum.

    Formula
    -------
    vp = close * volume
    vw_price = cumulative(vp) / cumulative(volume)

    MACD = EMA_fast(vw_price) - EMA_slow(vw_price)
    Signal = EMA_signal(MACD)
    Histogram = MACD - Signal

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified close and volume columns.
    fast : int, optional
        Fast EMA period (default 12).
    slow : int, optional
        Slow EMA period (default 26).
    signal : int, optional
        Signal EMA period (default 9).
    close_col : str, optional
        Name of the close price column. Defaults to 'close'.
    volume_col : str, optional
        Name of the volume column. Defaults to 'volume'.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with added columns:
        - 'vw_macd'
        - 'vw_macd_signal'
        - 'vw_macd_hist'
    """
    for col in [close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    # Volume-weighted price (cumulative)
    vp = close * volume
    vw_price = vp.cumsum() / volume.cumsum()

    # MACD calculation
    ema_fast = vw_price.ewm(span=fast, adjust=False).mean()
    ema_slow = vw_price.ewm(span=slow, adjust=False).mean()

    vw_macd = ema_fast - ema_slow
    vw_signal = vw_macd.ewm(span=signal, adjust=False).mean()
    vw_hist = vw_macd - vw_signal

    df["vw_macd"] = vw_macd
    df["vw_macd_signal"] = vw_signal
    df["vw_macd_hist"] = vw_hist

    return df


def add_kvo(
    df: pd.DataFrame,
    fast: int = 34,
    slow: int = 55,
    signal: int = 13,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Klinger Volume Oscillator (KVO) and signal line to the DataFrame.

    Formula
    -------
    - Typical Price (TP) = (High + Low + Close) / 3
    - Trend = 1 if today's TP > yesterday's TP, else -1
    - Volume Force (VF) = Trend * 2 * ((High - Low) / (High + Low)) * Volume
    - KVO = EMA_fast(VF) - EMA_slow(VF)
    - Signal = EMA_signal(KVO)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified high, low, close, and volume columns.
    fast : int, optional
        Fast EMA period for KVO (default 34).
    slow : int, optional
        Slow EMA period for KVO (default 55).
    signal : int, optional
        Signal EMA period for KVO (default 13).
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
        Copy of the input DataFrame with added columns 'kvo' and 'kvo_signal'.
    """
    for col in [high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    tp = (high + low + close) / 3
    trend = np.where(tp > tp.shift(1), 1, -1)

    vf = trend * 2 * ((high - low) / (high + low + 1e-9)) * volume

    ema_fast = pd.Series(vf).ewm(span=fast, adjust=False).mean()
    ema_slow = pd.Series(vf).ewm(span=slow, adjust=False).mean()
    kvo = ema_fast - ema_slow

    kvo_signal = kvo.ewm(span=signal, adjust=False).mean()

    df["kvo"] = kvo
    df["kvo_signal"] = kvo_signal

    return df


def add_demand_oscillator(
    df: pd.DataFrame,
    fast: int = 5,
    slow: int = 10,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Add Aspray's Demand Oscillator (ADO) to the DataFrame.

    Formula
    -------
    UpMove = High - min(Open, PrevClose)
    DownMove = max(Open, PrevClose) - Low
    Demand = ((UpMove - DownMove) / (UpMove + DownMove)) * Volume
    ADO = EMA_fast(Demand) - EMA_slow(Demand)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain the specified open, high, low, close, and volume columns.
    fast : int, optional
        Fast EMA period (default 5).
    slow : int, optional
        Slow EMA period (default 10).
    open_col : str, optional
        Name of the open price column. Defaults to 'open'.
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
        Copy of the input DataFrame with an added 'demand_osc' column.
    """
    for col in [open_col, high_col, low_col, close_col, volume_col]:
        validate_column(df, col)

    df = df.copy()

    open_ = pd.to_numeric(df[open_col], errors="coerce").astype("float64")
    high = pd.to_numeric(df[high_col], errors="coerce").astype("float64")
    low = pd.to_numeric(df[low_col], errors="coerce").astype("float64")
    close = pd.to_numeric(df[close_col], errors="coerce").astype("float64")
    volume = pd.to_numeric(df[volume_col], errors="coerce").astype("float64")

    prev_close = close.shift(1)

    up_move = high - np.minimum(open_, prev_close)
    down_move = np.maximum(open_, prev_close) - low

    demand = ((up_move - down_move) / (up_move + down_move + 1e-9)) * volume

    ema_fast = demand.ewm(span=fast, adjust=False).mean()
    ema_slow = demand.ewm(span=slow, adjust=False).mean()

    df["demand_osc"] = ema_fast - ema_slow

    return df


# endregion VOLUME INDICATORS


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
                value="2022-05-01",
                data_type=DataType.DATE,
            ),
            Condition(
                column=Table.VN_INDEX.Column.DATE.value,
                operator=SqlOperator.LESS_THAN_OR_EQUAL_TO,
                value="2023-12-31",
                data_type=DataType.DATE,
            ),
        ],
        order_by=[Table.VN_INDEX.Column.DATE.value],
    )

    df = add_ema(df)

    plot_with_indicators(
        df,
        indicators=["close_ema_*"],
    )


if __name__ == "__main__":
    main()
