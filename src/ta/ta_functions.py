import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys, os
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from logger.logger import LogType, Logger
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import (
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


# region TREND INDICATORS
def add_sma(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Add a Simple Moving Average (SMA) column to the DataFrame.

    The SMA is the unweighted mean of the previous `n` prices.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain a 'price' column.
    n : int
        Window size for the SMA.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'sma_{n}'.
    """
    df = df.copy()
    df[f"sma_{n}"] = df["close"].rolling(window=n, min_periods=1).mean()
    return df


def add_ema(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Add an Exponential Moving Average (EMA) column to the DataFrame.

    The EMA applies exponentially decreasing weights, giving more
    significance to recent prices.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain a 'price' column.
    n : int
        Span for the EMA calculation.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'ema_{n}'.
    """
    df = df.copy()
    df[f"ema_{n}"] = df["close"].ewm(span=n, adjust=False).mean()
    return df


def add_lwma(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Add a Linear Weighted Moving Average (LWMA) column to the DataFrame.

    The LWMA assigns linearly increasing weights to prices within the
    window, where the most recent price gets the highest weight (n),
    and the oldest gets weight 1.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain a 'price' column.
    n : int
        Window size for the LWMA.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'lwma_{n}'.
    """
    df = df.copy()
    weights = np.arange(1, n + 1)

    df[f"lwma_{n}"] = (
        df["close"]
        .rolling(window=n)
        .apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)
    )

    return df


def add_wma(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Add Wilder's Moving Average (WMA) column to the DataFrame.

    Wilder’s MA is similar to an EMA but uses an alpha = 1/n.
    It smooths price movements more slowly than a regular EMA.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain a 'price' column.
    n : int
        Period for the WMA calculation.

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'wma_{n}'.
    """
    df = df.copy()
    alpha = 1 / n
    df[f"wma_{n}"] = df["close"].ewm(alpha=alpha, adjust=False).mean()
    return df


def add_adx(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add Average Directional Movement Index (ADX) to the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain 'high', 'low', and 'close' columns.
    n : int, optional
        Period for ADX calculation (default is 14).

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with columns:
        '+di', '-di', 'adx_{n}'.
    """
    df = df.copy()

    # Ensure numeric types
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # True Range (TR)
    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift()), abs(df["low"] - df["close"].shift())
        ),
    )

    # Directional Movement
    df["+dm"] = df["high"].diff()
    df["-dm"] = -df["low"].diff()
    df["+dm"] = df["+dm"].where((df["+dm"] > df["-dm"]) & (df["+dm"] > 0), 0.0)
    df["-dm"] = df["-dm"].where((df["-dm"] > df["+dm"]) & (df["-dm"] > 0), 0.0)

    # Wilder’s smoothing (RMA)
    tr_n = df["tr"].rolling(n).sum()
    plus_dm_n = df["+dm"].rolling(n).sum()
    minus_dm_n = df["-dm"].rolling(n).sum()

    # +DI and -DI
    df["+di"] = 100 * (plus_dm_n / tr_n)
    df["-di"] = 100 * (minus_dm_n / tr_n)

    # DX
    df["dx"] = (100 * abs(df["+di"] - df["-di"]) / (df["+di"] + df["-di"])).fillna(0)

    # ADX = smoothed DX
    df[f"adx_{n}"] = df["dx"].rolling(n).mean()

    return df


# endregion TREND INDICATORS


# region VOLATILITY INDICATORS
def add_bollinger_bands(df: pd.DataFrame, n: int = 20, k: float = 2.0) -> pd.DataFrame:
    """
    Add Bollinger Bands (upper, middle, lower) to the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain 'close' column.
    n : int, optional
        Period for SMA and rolling std (default 20).
    k : float, optional
        Number of standard deviations for bands (default 2.0).

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with columns:
        'bb_middle_{n}', 'bb_upper_{n}', 'bb_lower_{n}'.
    """
    df = df.copy()
    sma = df["close"].rolling(window=n, min_periods=1).mean()
    std = df["close"].rolling(window=n, min_periods=1).std()

    df[f"bb_middle_{n}"] = sma
    df[f"bb_upper_{n}"] = sma + (k * std)
    df[f"bb_lower_{n}"] = sma - (k * std)

    return df


def add_keltner_channel(df: pd.DataFrame, n: int = 20, k: float = 2.0) -> pd.DataFrame:
    """
    Add Keltner Channel (upper, middle, lower) to the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain 'high', 'low', 'close'.
    n : int, optional
        Period for EMA and ATR (default 20).
    k : float, optional
        Multiplier for ATR (default 2.0).

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with columns:
        'kc_middle_{n}', 'kc_upper_{n}', 'kc_lower_{n}'.
    """
    df = df.copy()

    # Middle line (EMA of close)
    df[f"kc_middle_{n}"] = df["close"].ewm(span=n, adjust=False).mean()

    # True Range
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # ATR (Wilder’s moving average of TR)
    atr = tr.rolling(n).mean()

    # Upper & Lower bands
    df[f"kc_upper_{n}"] = df[f"kc_middle_{n}"] + k * atr
    df[f"kc_lower_{n}"] = df[f"kc_middle_{n}"] - k * atr

    return df


def add_starc_band(df: pd.DataFrame, n: int = 20, k: float = 2.0) -> pd.DataFrame:
    """
    Add STARC Bands (upper, middle, lower) to the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain 'high', 'low', 'close'.
    n : int, optional
        Period for SMA and ATR (default 20).
    k : float, optional
        Multiplier for ATR (default 2.0).

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with columns:
        'starc_middle_{n}', 'starc_upper_{n}', 'starc_lower_{n}'.
    """
    df = df.copy()

    # Middle Band (SMA of close)
    df[f"starc_middle_{n}"] = df["close"].rolling(window=n, min_periods=1).mean()

    # True Range
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # ATR (simple rolling mean)
    atr = tr.rolling(n, min_periods=1).mean()

    # Upper & Lower Bands
    df[f"starc_upper_{n}"] = df[f"starc_middle_{n}"] + k * atr
    df[f"starc_lower_{n}"] = df[f"starc_middle_{n}"] - k * atr

    return df


def add_atr(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add Average True Range (ATR) to the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain 'high', 'low', 'close'.
    n : int, optional
        Period for ATR calculation (default is 14).

    Returns
    -------
    pd.DataFrame
        Copy of the input DataFrame with an added column 'atr_{n}'.
    """
    df = df.copy()

    # True Range
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder’s ATR (RMA)
    df[f"atr_{n}"] = tr.ewm(alpha=1 / n, adjust=False).mean()

    return df


def add_divergence_index(df: pd.DataFrame, n: int = 14, k: float = 1.0) -> pd.DataFrame:
    df = df.copy()

    # Ensure numeric (convert Decimal → float)
    for col in ["high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # SMA of close
    sma = df["close"].rolling(window=n, min_periods=1).mean()

    # True Range
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder’s ATR
    atr = tr.ewm(alpha=1 / n, adjust=False).mean()

    # Divergence Index
    df[f"dvi_{n}"] = (df["close"] - sma) / (k * atr)

    return df


# endregion VOLATILITY INDICATORS


# region MOMENTUN INDICATORS


def add_rsi(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add Relative Strength Index (RSI) to the dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least a 'close' column.
    n : int, default 14
        Lookback period.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with an added 'rsi_{n}' column.
    """

    # Ensure float array to avoid Decimal issues
    close = np.asarray(df["close"], dtype="float64")
    delta = np.diff(close, prepend=close[0])

    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    avg_gain = pd.Series(gain).rolling(n, min_periods=n).mean()
    avg_loss = pd.Series(loss).rolling(n, min_periods=n).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    df[f"rsi_{n}"] = rsi
    return df


def add_roc(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add Rate of Change (ROC) indicator to the dataframe.

    ROC measures the percentage change in price compared to
    the price n periods ago.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least a 'close' column.
    n : int, default 14
        Lookback period for ROC calculation.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with an added 'roc_{n}' column.
    """
    close = np.asarray(df["close"], dtype="float64")
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
    df: pd.DataFrame, k_period: int = 14, d_period: int = 3
) -> pd.DataFrame:
    """
    Add Stochastic Oscillator (%K and %D) to the dataframe.

    %K = (Close - LowestLow) / (HighestHigh - LowestLow) * 100
    %D = SMA of %K

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'high', 'low', and 'close' columns.
    k_period : int, default 14
        Lookback period for %K.
    d_period : int, default 3
        Lookback period for %D (smoothing of %K).

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added columns:
        - 'stoch_k_{k_period}'
        - 'stoch_d_{d_period}'
    """
    close = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    high = pd.to_numeric(df["high"], errors="coerce").astype("float64")
    low = pd.to_numeric(df["low"], errors="coerce").astype("float64")

    low_min = low.rolling(window=k_period, min_periods=1).min()
    high_max = high.rolling(window=k_period, min_periods=1).max()

    stoch_k = 100 * (close - low_min) / (high_max - low_min)
    stoch_d = stoch_k.rolling(window=d_period, min_periods=1).mean()

    df[f"stoch_k_{k_period}"] = stoch_k
    df[f"stoch_d_{d_period}"] = stoch_d
    return df


def add_williams_r(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add William's %R indicator to the dataframe.

    %R = (HighestHigh - Close) / (HighestHigh - LowestLow) * -100

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'high', 'low', and 'close' columns.
    n : int, default 14
        Lookback period.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with an added 'williams_r_{n}' column.
    """
    close = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    high = pd.to_numeric(df["high"], errors="coerce").astype("float64")
    low = pd.to_numeric(df["low"], errors="coerce").astype("float64")

    highest_high = high.rolling(window=n, min_periods=1).max()
    lowest_low = low.rolling(window=n, min_periods=1).min()

    williams_r = (highest_high - close) / (highest_high - lowest_low) * -100
    df[f"williams_r_{n}"] = williams_r

    return df


def add_ado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Larry Williams’ Accumulation/Distribution (AD) Oscillator.

    Formula:
        ADO = ((Close - Open) / (High - Low)) * 100

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'open', 'high', 'low', 'close' columns.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with an added 'ad' column.
    """
    close = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    open = pd.to_numeric(df["open"], errors="coerce").astype("float64")
    high = pd.to_numeric(df["high"], errors="coerce").astype("float64")
    low = pd.to_numeric(df["low"], errors="coerce").astype("float64")

    ado = ((close - open) / (high - low).replace(0, np.nan)) * 100
    df["ad"] = ado

    return df


def add_rvi(df: pd.DataFrame, n: int = 10, signal: int = 4) -> pd.DataFrame:
    """
    Add Relative Vigor Index (RVI) and signal line.

    Formula:
        RV = (Close - Open) / (High - Low)
        RVI = SMA(RV, n)
        Signal = SMA(RVI, signal)

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'open', 'high', 'low', 'close' columns.
    n : int, optional (default=10)
        Period for main RVI smoothing.
    signal : int, optional (default=4)
        Period for RVI signal line smoothing.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added 'rvi_{n}' and 'rvi_signal_{signal}' columns.
    """
    close = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    open = pd.to_numeric(df["open"], errors="coerce").astype("float64")
    high = pd.to_numeric(df["high"], errors="coerce").astype("float64")
    low = pd.to_numeric(df["low"], errors="coerce").astype("float64")

    rv = (close - open) / (high - low).replace(0, np.nan)
    rvi = rv.rolling(window=n, min_periods=1).mean()
    rvi_signal = rvi.rolling(window=signal, min_periods=1).mean()

    df[f"rvi_{n}"] = rvi
    df[f"rvi_signal_{signal}"] = rvi_signal

    return df


def add_tsi(df: pd.DataFrame, r: int = 25, s: int = 13) -> pd.DataFrame:
    """
    Add True Strength Index (TSI) to the DataFrame.

    Formula:
        m_t = Close_t - Close_{t-1}
        TSI = 100 * (EMA_r(EMA_s(m_t)) / EMA_r(EMA_s(|m_t|)))

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'close' column.
    r : int, optional (default=25)
        Long smoothing period.
    s : int, optional (default=13)
        Short smoothing period.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with an added 'tsi_{r}_{s}' column.
    """
    close = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    momentum = close.diff()

    # short EMA
    ema_mom_s = momentum.ewm(span=s, adjust=False).mean()
    ema_abs_s = momentum.abs().ewm(span=s, adjust=False).mean()

    # long EMA
    ema_mom_r = ema_mom_s.ewm(span=r, adjust=False).mean()
    ema_abs_r = ema_abs_s.ewm(span=r, adjust=False).mean()

    tsi = 100 * (ema_mom_r / ema_abs_r)
    df[f"tsi_{r}_{s}"] = tsi

    return df


def add_vortex(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add Vortex Indicator (VI) to the DataFrame.

    The Vortex Indicator consists of two lines, +VI and -VI,
    that are derived from True Range (TR) and directional movement.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'high', 'low', and 'close' columns.
    n : int, default 14
        Lookback period.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added columns:
        - 'vi_plus_{n}'
        - 'vi_minus_{n}'
    """
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    close = df["close"].astype("float64")

    # True Range
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Vortex movements
    vm_plus = (high - low.shift(1)).abs()
    vm_minus = (low - high.shift(1)).abs()

    # Rolling sums
    tr_n = tr.rolling(n).sum()
    vm_plus_n = vm_plus.rolling(n).sum()
    vm_minus_n = vm_minus.rolling(n).sum()

    df[f"vi_plus_{n}"] = vm_plus_n / tr_n
    df[f"vi_minus_{n}"] = vm_minus_n / tr_n

    return df


# endregion MOMENTUM INDICATORS


# region VOLUME INDICATORS
def add_obv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add On-Balance Volume (OBV) indicator to the DataFrame.

    OBV measures cumulative buying/selling pressure by adding
    volume on up days and subtracting volume on down days.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'close' and 'volume' columns.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added 'obv' column.
    """
    close = df["close"].astype("float64")
    volume = df["volume"].astype("float64")

    obv = [0]  # start OBV at 0
    for i in range(1, len(close)):
        if close[i] > close[i - 1]:
            obv.append(obv[-1] + volume.iloc[i])
        elif close[i] < close[i - 1]:
            obv.append(obv[-1] - volume.iloc[i])
        else:
            obv.append(obv[-1])

    df["obv"] = obv
    return df


def add_mfi(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """
    Add Money Flow Index (MFI) to the DataFrame.

    The MFI uses price and volume to identify overbought/oversold
    conditions, similar to RSI but volume-adjusted.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'high', 'low', 'close', and 'volume' columns.
    n : int, default 14
        Lookback period.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added 'mfi_{n}' column.
    """
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    close = df["close"].astype("float64")
    volume = df["volume"].astype("float64")

    # Typical Price
    tp = (high + low + close) / 3

    # Raw Money Flow
    rmf = tp * volume

    # Positive & Negative Money Flow
    pos_mf = np.where(tp > tp.shift(1), rmf, 0.0)
    neg_mf = np.where(tp < tp.shift(1), rmf, 0.0)

    # Sum over n periods
    pos_mf_sum = pd.Series(pos_mf).rolling(n).sum()
    neg_mf_sum = pd.Series(neg_mf).rolling(n).sum()

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


def add_chaikin_ad(df: pd.DataFrame) -> pd.DataFrame:
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
        DataFrame with 'high', 'low', 'close', and 'volume' columns.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with added 'chaikin_ad' column.
    """
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    close = df["close"].astype("float64")
    volume = df["volume"].astype("float64")

    # Close Location Value (CLV)
    clv = ((close - low) - (high - close)) / np.where(high != low, (high - low), 1e-10)

    # Money Flow Volume
    mfv = clv * volume

    # Chaikin AD Line (cumulative)
    df["chaikin_ad"] = mfv.cumsum()

    return df


# endregion VOLUME INDICATORS


def plot_with_indicators(df: pd.DataFrame, indicators: list):
    """
    Plot price with optional indicators using up to 2 y-axes:
      - Left y-axis for price-based indicators
      - Right y-axis for oscillators or relative indicators
    """
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Main price axis
    ax1.plot(df["date"], df["close"], label="Close", color="black", linewidth=2)
    # ax1.plot(df["date"], df["volume"], label="Volume", color="brown", linewidth=2)

    # Second axis for oscillators
    ax2 = ax1.twinx()

    for col in indicators:
        if col not in df.columns:
            continue

        ax2.plot(df["date"], df[col], label=col.upper(), linestyle="--")

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

    connection_model = PostgreSQLConnectionModel(
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
                value="2025-01-01",
                data_type=DataType.DATE,
            ),
            Condition(
                column=Table.VN_INDEX.Column.DATE.value,
                operator=SqlOperator.LESS_THAN_OR_EQUAL_TO,
                value="2025-06-30",
                data_type=DataType.DATE,
            ),
        ],
        order_by=[Table.VN_INDEX.Column.DATE.value],
    )

    df = add_chaikin_ad(df)

    plot_with_indicators(
        df,
        indicators=[
            "chaikin_ad",
        ],
    )


if __name__ == "__main__":
    main()
