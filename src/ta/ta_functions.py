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
    distance_mode: str = "pct",
    slope_mode: str = "diff",
) -> pd.DataFrame:
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

    # ── collect ALL new columns here, concat once at the end ──────────────
    new_cols = {}

    for period in periods:
        upper, middle, lower = talib.BBANDS(
            source,
            timeperiod=period,
            nbdevup=k,
            nbdevdn=k,
            matype=ma_type,
        )
        upper = pd.Series(upper, index=df.index)
        middle = pd.Series(middle, index=df.index)
        lower = pd.Series(lower, index=df.index)

        base = f"{column_name}_bb_{period}"

        # --- bands ---
        new_cols[f"{base}_upper"] = upper
        new_cols[f"{base}_middle"] = middle
        new_cols[f"{base}_lower"] = lower

        # --- distance ---
        if distance_mode == "abs":
            new_cols[f"{base}_dist_upper"] = price - upper
            new_cols[f"{base}_dist_middle"] = price - middle
            new_cols[f"{base}_dist_lower"] = price - lower
        else:  # pct
            new_cols[f"{base}_dist_upper"] = (price - upper) / upper
            new_cols[f"{base}_dist_middle"] = (price - middle) / middle
            new_cols[f"{base}_dist_lower"] = (price - lower) / lower

        # --- slope + acceleration ---
        for band_name, band_series in [
            ("upper", upper),
            ("middle", middle),
            ("lower", lower),
        ]:
            slope = (
                band_series.pct_change() if slope_mode == "pct" else band_series.diff()
            )
            new_cols[f"{base}_slope_{band_name}"] = slope
            new_cols[f"{base}_slope_{band_name}_acceleration"] = slope.diff()

        # --- bandwidth ---
        bandwidth = (upper - lower) / middle
        new_cols[f"{base}_bandwidth"] = bandwidth
        new_cols[f"{base}_bandwidth_slope"] = bandwidth.diff()
        new_cols[f"{base}_bandwidth_acceleration"] = bandwidth.diff().diff()

        # --- %B ---
        band_range = (upper - lower).replace(0, float("nan"))
        pct_b = (price - lower) / band_range
        new_cols[f"{base}_pct_b"] = pct_b
        new_cols[f"{base}_pct_b_slope"] = pct_b.diff()
        new_cols[f"{base}_pct_b_gt_1"] = pct_b > 1
        new_cols[f"{base}_pct_b_lt_0"] = pct_b < 0

        # --- position flags ---
        above = price > upper
        below = price < lower
        new_cols[f"{base}_above_upper"] = above
        new_cols[f"{base}_below_lower"] = below
        new_cols[f"{base}_inside_bands"] = ~above & ~below
        new_cols[f"{base}_position"] = np.where(above, 1, np.where(below, -1, 0))

    # ── single concat replaces all fragmented df[col] = ... assignments ───
    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_dema(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
    slope_mode: str = "diff",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    dema_series = {}

    # ── per-period features ──────────────────────────────────────────────────
    for window in n:
        base = f"{column_name}_dema_{window}"
        dema = pd.Series(talib.DEMA(source, timeperiod=window), index=df.index)
        slope = dema.pct_change() if slope_mode == "pct" else dema.diff()

        new_cols[base] = dema
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()

        # distance (signed + absolute + normalised)
        dist = price - dema
        new_cols[f"{base}_dist"] = dist
        new_cols[f"{base}_dist_abs"] = dist.abs()
        new_cols[f"{base}_dist_pct"] = dist / dema  # removes price-level dominance

        # position flag
        new_cols[f"{column_name}_gt_dema_{window}"] = (price > dema).astype(int)

        dema_series[window] = dema

    # ── pairwise features ────────────────────────────────────────────────────
    for w1, w2 in combinations(dema_series.keys(), 2):
        pair = f"{column_name}_dema_{w1}_{w2}"
        d1, d2 = dema_series[w1], dema_series[w2]
        dist = d1 - d2

        # basic distance metrics
        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_dist_pct"] = dist / d2  # normalised, removes price dominance
        new_cols[f"{pair}_dist_slope"] = dist.diff()
        new_cols[f"{pair}_dist_acceleration"] = dist.diff().diff()

        # current regime: +1 when short DEMA above long DEMA, -1 otherwise
        sign = np.sign(dist).replace(0, np.nan).ffill().fillna(1)
        new_cols[f"{pair}_direction"] = sign.astype(int)

        # ── crossover events ─────────────────────────────────────────────────
        prev_sign = sign.shift(1)
        new_cols[f"{pair}_crossover_up"] = ((sign > 0) & (prev_sign <= 0)).astype(int)
        new_cols[f"{pair}_crossover_dn"] = ((sign < 0) & (prev_sign >= 0)).astype(int)

        # bars since last crossover — how "fresh" the signal is
        regime_change = sign.ne(sign.shift(1))
        regime_id = regime_change.cumsum()
        new_cols[f"{pair}_bars_since_crossover"] = sign.groupby(regime_id).cumcount()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
    return df


def add_ema(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    ema_series = {}

    # --- EMA + per-period derivatives ---
    for window in n:
        base = f"{column_name}_ema_{window}"
        ema = pd.Series(talib.EMA(source, timeperiod=window), index=df.index)
        slope = ema.diff()

        new_cols[base] = ema
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_ema_{window}"] = price > ema
        new_cols[f"{base}_dist"] = price - ema
        new_cols[f"{base}_dist_abs"] = (price - ema).abs()

        ema_series[window] = ema

    # --- pairwise distances ---
    for w1, w2 in combinations(ema_series.keys(), 2):
        pair = f"{column_name}_ema_{w1}_{w2}"
        dist = ema_series[w1] - ema_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_kama(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    kama_series = {}

    # --- KAMA + per-period derivatives ---
    for window in n:
        base = f"{column_name}_kama_{window}"
        kama = pd.Series(talib.KAMA(source, timeperiod=window), index=df.index)
        slope = kama.diff()

        new_cols[base] = kama
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_kama_{window}"] = price > kama
        new_cols[f"{base}_dist"] = price - kama
        new_cols[f"{base}_dist_abs"] = (price - kama).abs()

        kama_series[window] = kama

    # --- pairwise distances ---
    for w1, w2 in combinations(kama_series.keys(), 2):
        pair = f"{column_name}_kama_{w1}_{w2}"
        dist = kama_series[w1] - kama_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_midpoint(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [14, 50, 100]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    midpoint_series = {}

    # --- MIDPOINT + per-period derivatives ---
    for window in n:
        base = f"{column_name}_midpoint_{window}"
        midpoint = pd.Series(talib.MIDPOINT(source, timeperiod=window), index=df.index)
        slope = midpoint.diff()

        new_cols[base] = midpoint
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_midpoint_{window}"] = price > midpoint
        new_cols[f"{base}_dist"] = price - midpoint
        new_cols[f"{base}_dist_abs"] = (price - midpoint).abs()

        midpoint_series[window] = midpoint

    # --- pairwise distances ---
    for w1, w2 in combinations(midpoint_series.keys(), 2):
        pair = f"{column_name}_midpoint_{w1}_{w2}"
        dist = midpoint_series[w1] - midpoint_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_midprice(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    validate_column(df, high_col)
    validate_column(df, low_col)

    if n is None:
        n = [14, 50, 100]

    df = df.copy()
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)

    has_close = close_col and close_col in df.columns
    close = df[close_col] if has_close else None

    new_cols = {}
    midprice_series = {}

    # --- MIDPRICE + per-period derivatives ---
    for window in n:
        base = f"midprice_{window}"
        midprice = pd.Series(
            talib.MIDPRICE(high, low, timeperiod=window), index=df.index
        )
        slope = midprice.diff()

        new_cols[base] = midprice
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()

        if has_close:
            new_cols[f"close_gt_midprice_{window}"] = close > midprice
            new_cols[f"{base}_dist"] = close - midprice
            new_cols[f"{base}_dist_abs"] = (close - midprice).abs()

        midprice_series[window] = midprice

    # --- pairwise distances ---
    for w1, w2 in combinations(midprice_series.keys(), 2):
        pair = f"midprice_{w1}_{w2}"
        dist = midprice_series[w1] - midprice_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_sar(
    df: pd.DataFrame,
    acceleration: list[float] = None,
    maximum: list[float] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
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

    new_cols = {}
    sar_series = {}

    def _suffix(acc, max_val) -> str:
        return f"{acc}_{max_val}".replace(".", "")

    # pre-compute price trend flags once (shared across all combos)
    price_diff = close.diff()
    price_up3 = (price_diff > 0).rolling(3).sum() == 3
    price_down3 = (price_diff < 0).rolling(3).sum() == 3

    # =========================
    # 1. SAR + per-combo derivatives
    # =========================
    for acc in acceleration:
        for max_val in maximum:
            s = _suffix(acc, max_val)
            base = f"sar_{s}"
            sar = pd.Series(
                talib.SAR(high, low, acceleration=acc, maximum=max_val), index=df.index
            )
            slope = sar.diff()
            dist = close - sar

            new_cols[base] = sar
            new_cols[f"{base}_slope"] = slope
            new_cols[f"{base}_acceleration"] = slope.diff()
            new_cols[f"{base}_above"] = sar > close  # bearish
            new_cols[f"{base}_below"] = sar < close  # bullish
            new_cols[f"{base}_direction"] = np.where(dist > 0, 1, -1)
            new_cols[f"{base}_dist"] = dist
            new_cols[f"{base}_dist_abs"] = dist.abs()
            new_cols[f"{base}_dist_pct"] = dist / close.replace(0, float("nan"))

            # --- trend agreement ---
            sar_diff = sar.diff()
            sar_up3 = (sar_diff > 0).rolling(3).sum() == 3
            sar_down3 = (sar_diff < 0).rolling(3).sum() == 3

            new_cols[f"{base}_up3"] = price_up3 & sar_up3
            new_cols[f"{base}_down3"] = price_down3 & sar_down3
            new_cols[f"{base}_trend3"] = np.where(
                price_up3 & sar_up3, 1, np.where(price_down3 & sar_down3, -1, 0)
            )

            sar_series[s] = sar

    # =========================
    # 2. Pairwise SAR distances
    # =========================
    for s1, s2 in combinations(sar_series.keys(), 2):
        pair = f"sar_{s1}_{s2}"
        dist = sar_series[s1] - sar_series[s2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_sma(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [50, 100, 200]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    sma_series = {}

    # --- SMA + per-period derivatives ---
    for window in n:
        base = f"{column_name}_sma_{window}"
        sma = pd.Series(talib.SMA(source, timeperiod=window), index=df.index)
        slope = sma.diff()

        new_cols[base] = sma
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_sma_{window}"] = price > sma
        new_cols[f"{base}_dist"] = price - sma
        new_cols[f"{base}_dist_abs"] = (price - sma).abs()

        sma_series[window] = sma

    # --- pairwise distances ---
    for w1, w2 in combinations(sma_series.keys(), 2):
        pair = f"{column_name}_sma_{w1}_{w2}"
        dist = sma_series[w1] - sma_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_t3(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
    vfactor: float = 0.7,
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [5]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    t3_series = {}

    # --- T3 + per-period derivatives ---
    for window in n:
        base = f"{column_name}_t3_{window}"
        t3 = pd.Series(
            talib.T3(source, timeperiod=window, vfactor=vfactor), index=df.index
        )
        slope = t3.diff()

        new_cols[base] = t3
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_t3_{window}"] = price > t3
        new_cols[f"{base}_dist"] = price - t3
        new_cols[f"{base}_dist_abs"] = (price - t3).abs()

        t3_series[window] = t3

    # --- pairwise distances ---
    for w1, w2 in combinations(t3_series.keys(), 2):
        pair = f"{column_name}_t3_{w1}_{w2}"
        dist = t3_series[w1] - t3_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_tema(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [30]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    tema_series = {}

    # --- TEMA + per-period derivatives ---
    for window in n:
        base = f"{column_name}_tema_{window}"
        tema = pd.Series(talib.TEMA(source, timeperiod=window), index=df.index)
        slope = tema.diff()

        new_cols[base] = tema
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_tema_{window}"] = price > tema
        new_cols[f"{base}_dist"] = price - tema
        new_cols[f"{base}_dist_abs"] = (price - tema).abs()

        tema_series[window] = tema

    # --- pairwise distances ---
    for w1, w2 in combinations(tema_series.keys(), 2):
        pair = f"{column_name}_tema_{w1}_{w2}"
        dist = tema_series[w1] - tema_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_trima(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [30]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    trima_series = {}

    # --- TRIMA + per-period derivatives ---
    for window in n:
        base = f"{column_name}_trima_{window}"
        trima = pd.Series(talib.TRIMA(source, timeperiod=window), index=df.index)
        slope = trima.diff()

        new_cols[base] = trima
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_trima_{window}"] = price > trima
        new_cols[f"{base}_dist"] = price - trima
        new_cols[f"{base}_dist_abs"] = (price - trima).abs()

        trima_series[window] = trima

    # --- pairwise distances ---
    for w1, w2 in combinations(trima_series.keys(), 2):
        pair = f"{column_name}_trima_{w1}_{w2}"
        dist = trima_series[w1] - trima_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_wma(
    df: pd.DataFrame,
    n: int | list[int] | None = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [7, 14, 21, 50, 100]
    elif isinstance(n, int):
        n = [n]
    else:
        n = list(n)

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}
    wma_series = {}

    # --- WMA + per-period derivatives ---
    for period in n:
        base = f"{column_name}_wma_{period}"
        wma = pd.Series(talib.WMA(source, timeperiod=period), index=df.index)
        slope = wma.diff()

        new_cols[base] = wma
        new_cols[f"{base}_slope"] = slope
        new_cols[f"{base}_acceleration"] = slope.diff()
        new_cols[f"{column_name}_gt_wma_{period}"] = price > wma
        new_cols[f"{base}_dist"] = price - wma
        new_cols[f"{base}_dist_abs"] = (price - wma).abs()

        wma_series[period] = wma

    # --- pairwise distances ---
    for w1, w2 in combinations(wma_series.keys(), 2):
        pair = f"{column_name}_wma_{w1}_{w2}"
        dist = wma_series[w1] - wma_series[w2]

        new_cols[f"{pair}_dist"] = dist
        new_cols[f"{pair}_dist_abs"] = dist.abs()
        new_cols[f"{pair}_direction"] = np.where(dist > 0, 1, -1)
        new_cols[f"{pair}_dist_slope"] = dist.diff()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

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
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicators ---
        adx = pd.Series(talib.ADX(high, low, close, timeperiod=period), index=df.index)
        plus_di = pd.Series(
            talib.PLUS_DI(high, low, close, timeperiod=period), index=df.index
        )
        minus_di = pd.Series(
            talib.MINUS_DI(high, low, close, timeperiod=period), index=df.index
        )
        adx_slope = adx.diff()
        di_dist = plus_di - minus_di

        # --- ADX ---
        new_cols[f"adx{s}"] = adx
        new_cols[f"adx{s}_gt_20"] = adx > 20
        new_cols[f"adx{s}_gt_25"] = adx > 25
        new_cols[f"adx{s}_slope"] = adx_slope
        new_cols[f"adx{s}_acceleration"] = adx_slope.diff()

        # --- DI lines + slopes ---
        new_cols[f"plus_di{s}"] = plus_di
        new_cols[f"minus_di{s}"] = minus_di
        new_cols[f"plus_di{s}_slope"] = plus_di.diff()
        new_cols[f"minus_di{s}_slope"] = minus_di.diff()

        # --- DI relationship ---
        new_cols[f"di{s}_distance"] = di_dist
        new_cols[f"di{s}_distance_abs"] = di_dist.abs()
        new_cols[f"di{s}_ratio"] = plus_di / minus_di.replace(0, float("nan"))
        new_cols[f"trend{s}_direction"] = np.where(di_dist > 0, 1, -1)

        # --- combined strength signal ---
        new_cols[f"adx{s}_di_strength"] = adx * di_dist.abs()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_aroon(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
) -> pd.DataFrame:
    for col in (high_col, low_col):
        validate_column(df, col)

    if n is None:
        n = [25]

    df = df.copy()
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicators ---
        aroon_down, aroon_up = talib.AROON(high, low, timeperiod=period)
        aroon_up = pd.Series(aroon_up, index=df.index)
        aroon_down = pd.Series(aroon_down, index=df.index)
        aroon_osc = pd.Series(
            talib.AROONOSC(high, low, timeperiod=period), index=df.index
        )
        dist = aroon_up - aroon_down

        # --- core ---
        new_cols[f"aroon_up{s}"] = aroon_up
        new_cols[f"aroon_down{s}"] = aroon_down
        new_cols[f"aroon_osc{s}"] = aroon_osc

        # --- slopes ---
        new_cols[f"aroon_up{s}_slope"] = aroon_up.diff()
        new_cols[f"aroon_down{s}_slope"] = aroon_down.diff()
        new_cols[f"aroon_osc{s}_slope"] = aroon_osc.diff()

        # --- up/down relationship ---
        new_cols[f"aroon{s}_distance"] = dist
        new_cols[f"aroon{s}_distance_abs"] = dist.abs()
        new_cols[f"aroon{s}_ratio"] = aroon_up / aroon_down.replace(0, float("nan"))
        new_cols[f"aroon{s}_direction"] = np.where(dist > 0, 1, -1)

        # --- threshold flags ---
        new_cols[f"aroon_up{s}_gt_70"] = aroon_up > 70
        new_cols[f"aroon_down{s}_gt_70"] = aroon_down > 70
        new_cols[f"aroon_up{s}_lt_30"] = aroon_up < 30
        new_cols[f"aroon_down{s}_lt_30"] = aroon_down < 30

        # --- combined conviction score ---
        new_cols[f"aroon{s}_strength"] = aroon_osc.abs() * dist.abs()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_bop(
    df: pd.DataFrame,
    n: list[int] = None,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (open_col, high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()
    open_ = df[open_col].to_numpy(dtype=float)
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    # --- raw BOP (computed once, no period) ---
    bop = pd.Series(talib.BOP(open_, high, low, close), index=df.index)
    bop_slope = bop.diff()
    bop_abs = bop.abs()

    new_cols["bop"] = bop
    new_cols["bop_slope"] = bop_slope
    new_cols["bop_acceleration"] = bop_slope.diff()
    new_cols["bop_gt_0"] = bop > 0
    new_cols["bop_lt_0"] = bop < 0
    new_cols["bop_abs"] = bop_abs
    new_cols["bop_direction"] = np.where(bop > 0, 1, -1)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"
        signal = bop.rolling(window=period).mean()
        hist = bop - signal

        new_cols[f"bop_signal{s}"] = signal
        new_cols[f"bop_signal{s}_slope"] = signal.diff()
        new_cols[f"bop_hist{s}"] = hist
        new_cols[f"bop_hist{s}_slope"] = hist.diff()
        new_cols[f"bop_hist{s}_gt_0"] = hist > 0
        new_cols[f"bop_hist{s}_lt_0"] = hist < 0
        new_cols[f"bop{s}_strength"] = bop_abs * hist.abs()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_cci(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()
    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        cci = pd.Series(talib.CCI(high, low, close, timeperiod=period), index=df.index)
        cci_slope = cci.diff()
        cci_abs = cci.abs()
        signal = cci.rolling(window=period).mean()
        hist = cci - signal

        # --- core + derivatives ---
        new_cols[f"cci{s}"] = cci
        new_cols[f"cci{s}_slope"] = cci_slope
        new_cols[f"cci{s}_acceleration"] = cci_slope.diff()

        # --- threshold flags ---
        new_cols[f"cci{s}_gt_100"] = cci > 100
        new_cols[f"cci{s}_lt_minus100"] = cci < -100
        new_cols[f"cci{s}_gt_0"] = cci > 0
        new_cols[f"cci{s}_lt_0"] = cci < 0

        # --- magnitude & direction ---
        new_cols[f"cci{s}_abs"] = cci_abs
        new_cols[f"cci{s}_direction"] = np.where(cci > 0, 1, -1)
        new_cols[f"cci{s}_extreme"] = np.where(
            cci > 100, 1, np.where(cci < -100, -1, 0)
        )

        # --- signal line & histogram ---
        new_cols[f"cci{s}_signal"] = signal
        new_cols[f"cci{s}_signal_slope"] = signal.diff()
        new_cols[f"cci{s}_hist"] = hist
        new_cols[f"cci{s}_hist_slope"] = hist.diff()
        new_cols[f"cci{s}_hist_gt_0"] = hist > 0
        new_cols[f"cci{s}_hist_lt_0"] = hist < 0

        # --- combined conviction score ---
        new_cols[f"cci{s}_strength"] = cci_abs * hist.abs()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_cmo(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:
    validate_column(df, column_name)

    if n is None:
        n = [14]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        cmo = pd.Series(talib.CMO(source, timeperiod=period), index=df.index)
        cmo_slope = cmo.diff()
        cmo_abs = cmo.abs()
        signal = cmo.rolling(window=period).mean()
        hist = cmo - signal

        # --- core + derivatives ---
        new_cols[f"cmo{s}"] = cmo
        new_cols[f"cmo{s}_slope"] = cmo_slope
        new_cols[f"cmo{s}_acceleration"] = cmo_slope.diff()
        new_cols[f"cmo{s}_abs"] = cmo_abs
        new_cols[f"cmo{s}_direction"] = np.where(cmo > 0, 1, -1)

        # --- threshold flags ---
        new_cols[f"cmo{s}_gt_50"] = cmo > 50
        new_cols[f"cmo{s}_lt_minus50"] = cmo < -50
        new_cols[f"cmo{s}_gt_0"] = cmo > 0
        new_cols[f"cmo{s}_lt_0"] = cmo < 0
        new_cols[f"cmo{s}_extreme"] = np.where(cmo > 50, 1, np.where(cmo < -50, -1, 0))

        # --- signal line & histogram ---
        new_cols[f"cmo{s}_signal"] = signal
        new_cols[f"cmo{s}_signal_slope"] = signal.diff()
        new_cols[f"cmo{s}_hist"] = hist
        new_cols[f"cmo{s}_hist_slope"] = hist.diff()
        new_cols[f"cmo{s}_hist_gt_0"] = hist > 0
        new_cols[f"cmo{s}_hist_lt_0"] = hist < 0

        # --- combined conviction score ---
        new_cols[f"cmo{s}_strength"] = cmo_abs * hist.abs()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_macd(
    df: pd.DataFrame,
    fast: list[int] = None,
    slow: list[int] = None,
    signal: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if fast is None:
        fast = [12]
    if slow is None:
        slow = [26]
    if signal is None:
        signal = [9]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for f, sl, sg in product(fast, slow, signal):
        if f >= sl:
            continue

        s = f"_{f}_{sl}_{sg}"

        # --- core indicator ---
        macd_line, signal_line, hist = talib.MACD(
            source,
            fastperiod=f,
            slowperiod=sl,
            signalperiod=sg,
        )

        macd = pd.Series(macd_line, index=df.index)
        signal = pd.Series(signal_line, index=df.index)
        hist_s = pd.Series(hist, index=df.index)

        # --- derivatives ---
        macd_slope = macd.diff()
        hist_slope = hist_s.diff()

        # --- reusable ---
        macd_abs = macd.abs()
        hist_abs = hist_s.abs()
        prev_hist = hist_s.shift(1)

        # --- MACD line ---
        new_cols[f"macd{s}"] = macd
        new_cols[f"macd{s}_slope"] = macd_slope
        new_cols[f"macd{s}_acceleration"] = macd_slope.diff()
        new_cols[f"macd{s}_abs"] = macd_abs
        new_cols[f"macd{s}_direction"] = np.where(macd > 0, 1, -1)
        new_cols[f"macd{s}_gt_0"] = macd > 0
        new_cols[f"macd{s}_lt_0"] = macd < 0

        # --- signal line ---
        new_cols[f"macd{s}_signal"] = signal
        new_cols[f"macd{s}_signal_slope"] = signal.diff()
        new_cols[f"macd{s}_signal_gt_0"] = signal > 0
        new_cols[f"macd{s}_signal_lt_0"] = signal < 0

        # --- histogram ---
        new_cols[f"macd{s}_hist"] = hist_s
        new_cols[f"macd{s}_hist_slope"] = hist_slope
        new_cols[f"macd{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"macd{s}_hist_gt_0"] = hist_s > 0
        new_cols[f"macd{s}_hist_lt_0"] = hist_s < 0
        new_cols[f"macd{s}_hist_abs"] = hist_abs

        # --- crossover signals ---
        new_cols[f"macd{s}_cross_above"] = (hist_s > 0) & (prev_hist <= 0)
        new_cols[f"macd{s}_cross_below"] = (hist_s < 0) & (prev_hist >= 0)

        # --- combined conviction score ---
        new_cols[f"macd{s}_strength"] = macd_abs * hist_abs

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_mfi(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:

    for col in (high_col, low_col, close_col, volume_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        mfi = pd.Series(
            talib.MFI(high, low, close, volume, timeperiod=period),
            index=df.index,
        )

        # --- derivatives ---
        mfi_slope = mfi.diff()

        # --- reusable ---
        mfi_abs = (mfi - 50).abs()
        signal = mfi.rolling(window=period).mean()
        hist = mfi - signal

        # --- core + derivatives ---
        new_cols[f"mfi{s}"] = mfi
        new_cols[f"mfi{s}_slope"] = mfi_slope
        new_cols[f"mfi{s}_acceleration"] = mfi_slope.diff()
        new_cols[f"mfi{s}_abs"] = mfi_abs
        new_cols[f"mfi{s}_direction"] = np.where(mfi > 50, 1, -1)

        # --- threshold flags ---
        new_cols[f"mfi{s}_gt_80"] = mfi > 80
        new_cols[f"mfi{s}_lt_20"] = mfi < 20
        new_cols[f"mfi{s}_gt_50"] = mfi > 50
        new_cols[f"mfi{s}_lt_50"] = mfi < 50
        new_cols[f"mfi{s}_extreme"] = np.where(mfi > 80, 1, np.where(mfi < 20, -1, 0))

        # --- signal line & histogram ---
        new_cols[f"mfi{s}_signal"] = signal
        new_cols[f"mfi{s}_signal_slope"] = signal.diff()
        new_cols[f"mfi{s}_hist"] = hist
        new_cols[f"mfi{s}_hist_slope"] = hist.diff()
        new_cols[f"mfi{s}_hist_gt_0"] = hist > 0
        new_cols[f"mfi{s}_hist_lt_0"] = hist < 0

        # --- combined conviction score ---
        new_cols[f"mfi{s}_strength"] = mfi_abs * hist.abs()

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_mom(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if n is None:
        n = [10]

    df = df.copy()

    source = df[column_name].to_numpy(dtype=float)
    price = df[column_name]

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        mom = pd.Series(talib.MOM(source, timeperiod=period), index=df.index)

        # --- derivatives ---
        mom_slope = mom.diff()

        # --- reusable ---
        mom_abs = mom.abs()
        lagged = price.shift(period).replace(0, float("nan"))
        mom_pct = mom / lagged

        signal = mom.rolling(window=period).mean()
        hist = mom - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        # --- core + derivatives ---
        new_cols[f"mom{s}"] = mom
        new_cols[f"mom{s}_slope"] = mom_slope
        new_cols[f"mom{s}_acceleration"] = mom_slope.diff()
        new_cols[f"mom{s}_abs"] = mom_abs
        new_cols[f"mom{s}_direction"] = np.where(mom > 0, 1, -1)

        # --- zero-line flags ---
        new_cols[f"mom{s}_gt_0"] = mom > 0
        new_cols[f"mom{s}_lt_0"] = mom < 0

        # --- normalised momentum ---
        new_cols[f"mom{s}_pct"] = mom_pct
        new_cols[f"mom{s}_pct_slope"] = mom_pct.diff()

        # --- signal line & histogram ---
        new_cols[f"mom{s}_signal"] = signal
        new_cols[f"mom{s}_signal_slope"] = signal.diff()
        new_cols[f"mom{s}_hist"] = hist
        new_cols[f"mom{s}_hist_slope"] = hist_slope
        new_cols[f"mom{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"mom{s}_hist_gt_0"] = hist > 0
        new_cols[f"mom{s}_hist_lt_0"] = hist < 0
        new_cols[f"mom{s}_hist_abs"] = hist_abs

        # --- combined conviction score ---
        new_cols[f"mom{s}_strength"] = mom_abs * hist_abs

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df


def add_ppo(
    df: pd.DataFrame,
    fast: list[int] = None,
    slow: list[int] = None,
    signal: list[int] = None,
    ma_type: int = 1,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if fast is None:
        fast = [12]
    if slow is None:
        slow = [26]
    if signal is None:
        signal = [9]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for f, sl, sg in product(fast, slow, signal):
        if f >= sl:
            continue

        s = f"_{f}_{sl}_{sg}"

        # --- core indicator ---
        ppo_line = pd.Series(
            talib.PPO(source, fastperiod=f, slowperiod=sl, matype=ma_type),
            index=df.index,
        )

        signal_line = ppo_line.rolling(window=sg).mean()
        hist = ppo_line - signal_line

        # --- derivatives ---
        ppo_slope = ppo_line.diff()
        hist_slope = hist.diff()

        # --- reusable ---
        ppo_abs = ppo_line.abs()
        hist_abs = hist.abs()
        prev_hist = hist.shift(1)

        # --- PPO line ---
        new_cols[f"ppo{s}"] = ppo_line
        new_cols[f"ppo{s}_slope"] = ppo_slope
        new_cols[f"ppo{s}_acceleration"] = ppo_slope.diff()
        new_cols[f"ppo{s}_abs"] = ppo_abs
        new_cols[f"ppo{s}_direction"] = np.where(ppo_line > 0, 1, -1)
        new_cols[f"ppo{s}_gt_0"] = ppo_line > 0
        new_cols[f"ppo{s}_lt_0"] = ppo_line < 0

        # --- signal ---
        new_cols[f"ppo{s}_signal"] = signal_line
        new_cols[f"ppo{s}_signal_slope"] = signal_line.diff()
        new_cols[f"ppo{s}_signal_gt_0"] = signal_line > 0
        new_cols[f"ppo{s}_signal_lt_0"] = signal_line < 0

        # --- histogram ---
        new_cols[f"ppo{s}_hist"] = hist
        new_cols[f"ppo{s}_hist_slope"] = hist_slope
        new_cols[f"ppo{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"ppo{s}_hist_gt_0"] = hist > 0
        new_cols[f"ppo{s}_hist_lt_0"] = hist < 0
        new_cols[f"ppo{s}_hist_abs"] = hist_abs

        # --- crossover ---
        new_cols[f"ppo{s}_cross_above"] = (hist > 0) & (prev_hist <= 0)
        new_cols[f"ppo{s}_cross_below"] = (hist < 0) & (prev_hist >= 0)

        # --- strength ---
        new_cols[f"ppo{s}_strength"] = ppo_abs * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_roc(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if n is None:
        n = [10]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        roc = pd.Series(talib.ROC(source, timeperiod=period), index=df.index)

        # --- derivatives ---
        roc_slope = roc.diff()

        # --- reusable ---
        roc_abs = roc.abs()
        signal = roc.rolling(window=period).mean()
        hist = roc - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        # --- core ---
        new_cols[f"roc{s}"] = roc
        new_cols[f"roc{s}_slope"] = roc_slope
        new_cols[f"roc{s}_acceleration"] = roc_slope.diff()
        new_cols[f"roc{s}_abs"] = roc_abs
        new_cols[f"roc{s}_direction"] = np.where(roc > 0, 1, -1)

        # --- flags ---
        new_cols[f"roc{s}_gt_0"] = roc > 0
        new_cols[f"roc{s}_lt_0"] = roc < 0

        # --- signal & hist ---
        new_cols[f"roc{s}_signal"] = signal
        new_cols[f"roc{s}_signal_slope"] = signal.diff()
        new_cols[f"roc{s}_hist"] = hist
        new_cols[f"roc{s}_hist_slope"] = hist_slope
        new_cols[f"roc{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"roc{s}_hist_gt_0"] = hist > 0
        new_cols[f"roc{s}_hist_lt_0"] = hist < 0
        new_cols[f"roc{s}_hist_abs"] = hist_abs

        # --- strength ---
        new_cols[f"roc{s}_strength"] = roc_abs * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_rsi(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if n is None:
        n = [14]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        rsi = pd.Series(talib.RSI(source, timeperiod=period), index=df.index)

        # --- derivatives ---
        rsi_slope = rsi.diff()

        # --- reusable ---
        rsi_abs = (rsi - 50).abs()
        signal = rsi.rolling(window=period).mean()
        hist = rsi - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        # --- core ---
        new_cols[f"rsi{s}"] = rsi
        new_cols[f"rsi{s}_slope"] = rsi_slope
        new_cols[f"rsi{s}_acceleration"] = rsi_slope.diff()
        new_cols[f"rsi{s}_abs"] = rsi_abs
        new_cols[f"rsi{s}_direction"] = np.where(rsi > 50, 1, -1)

        # --- flags ---
        new_cols[f"rsi{s}_gt_70"] = rsi > 70
        new_cols[f"rsi{s}_lt_30"] = rsi < 30
        new_cols[f"rsi{s}_gt_50"] = rsi > 50
        new_cols[f"rsi{s}_lt_50"] = rsi < 50
        new_cols[f"rsi{s}_extreme"] = np.where(rsi > 70, 1, np.where(rsi < 30, -1, 0))

        # --- signal & hist ---
        new_cols[f"rsi{s}_signal"] = signal
        new_cols[f"rsi{s}_signal_slope"] = signal.diff()
        new_cols[f"rsi{s}_hist"] = hist
        new_cols[f"rsi{s}_hist_slope"] = hist_slope
        new_cols[f"rsi{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"rsi{s}_hist_gt_0"] = hist > 0
        new_cols[f"rsi{s}_hist_lt_0"] = hist < 0
        new_cols[f"rsi{s}_hist_abs"] = hist_abs

        # --- strength ---
        new_cols[f"rsi{s}_strength"] = rsi_abs * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


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

    new_cols = {}

    for fk, sk, sd in product(fastk, slowk, slowd):
        s = f"_{fk}_{sk}_{sd}"

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

        k = pd.Series(k, index=df.index)
        d = pd.Series(d, index=df.index)

        k_slope = k.diff()
        d_slope = d.diff()

        k_abs = (k - 50).abs()
        kd_dist = k - d
        kd_dist_abs = kd_dist.abs()
        kd_slope = kd_dist.diff()
        prev_kd = kd_dist.shift(1)

        new_cols.update(
            {
                f"stoch{s}_k": k,
                f"stoch{s}_k_slope": k_slope,
                f"stoch{s}_k_acceleration": k_slope.diff(),
                f"stoch{s}_k_abs": k_abs,
                f"stoch{s}_k_direction": np.where(k > 50, 1, -1),
                f"stoch{s}_k_gt_80": k > 80,
                f"stoch{s}_k_lt_20": k < 20,
                f"stoch{s}_k_gt_50": k > 50,
                f"stoch{s}_k_lt_50": k < 50,
                f"stoch{s}_k_extreme": np.where(k > 80, 1, np.where(k < 20, -1, 0)),
                f"stoch{s}_d": d,
                f"stoch{s}_d_slope": d_slope,
                f"stoch{s}_d_acceleration": d_slope.diff(),
                f"stoch{s}_d_gt_80": d > 80,
                f"stoch{s}_d_lt_20": d < 20,
                f"stoch{s}_d_gt_50": d > 50,
                f"stoch{s}_d_lt_50": d < 50,
                f"stoch{s}_kd_dist": kd_dist,
                f"stoch{s}_kd_dist_abs": kd_dist_abs,
                f"stoch{s}_kd_direction": np.where(k > d, 1, -1),
                f"stoch{s}_kd_dist_slope": kd_slope,
                f"stoch{s}_cross_above": (kd_dist > 0) & (prev_kd <= 0),
                f"stoch{s}_cross_below": (kd_dist < 0) & (prev_kd >= 0),
                f"stoch{s}_both_gt_80": (k > 80) & (d > 80),
                f"stoch{s}_both_lt_20": (k < 20) & (d < 20),
                f"stoch{s}_strength": k_abs * kd_dist_abs,
            }
        )

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_stoch_rsi(
    df: pd.DataFrame,
    n: list[int] = None,
    fastk: list[int] = None,
    fastd: list[int] = None,
    fastd_matype: int = 0,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if n is None:
        n = [14]
    if fastk is None:
        fastk = [5]
    if fastd is None:
        fastd = [3]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for period, fk, fd in product(n, fastk, fastd):
        s = f"_{period}_{fk}_{fd}"

        k, d = talib.STOCHRSI(
            source,
            timeperiod=period,
            fastk_period=fk,
            fastd_period=fd,
            fastd_matype=fastd_matype,
        )

        k = pd.Series(k, index=df.index)
        d = pd.Series(d, index=df.index)

        k_slope = k.diff()
        d_slope = d.diff()

        k_abs = (k - 50).abs()
        kd_dist = k - d
        kd_dist_abs = kd_dist.abs()
        kd_slope = kd_dist.diff()
        prev_kd = kd_dist.shift(1)

        new_cols.update(
            {
                f"stoch_rsi{s}_k": k,
                f"stoch_rsi{s}_k_slope": k_slope,
                f"stoch_rsi{s}_k_acceleration": k_slope.diff(),
                f"stoch_rsi{s}_k_abs": k_abs,
                f"stoch_rsi{s}_k_direction": np.where(k > 50, 1, -1),
                f"stoch_rsi{s}_k_gt_80": k > 80,
                f"stoch_rsi{s}_k_lt_20": k < 20,
                f"stoch_rsi{s}_k_gt_50": k > 50,
                f"stoch_rsi{s}_k_lt_50": k < 50,
                f"stoch_rsi{s}_k_extreme": np.where(k > 80, 1, np.where(k < 20, -1, 0)),
                f"stoch_rsi{s}_d": d,
                f"stoch_rsi{s}_d_slope": d_slope,
                f"stoch_rsi{s}_d_acceleration": d_slope.diff(),
                f"stoch_rsi{s}_d_gt_80": d > 80,
                f"stoch_rsi{s}_d_lt_20": d < 20,
                f"stoch_rsi{s}_d_gt_50": d > 50,
                f"stoch_rsi{s}_d_lt_50": d < 50,
                f"stoch_rsi{s}_kd_dist": kd_dist,
                f"stoch_rsi{s}_kd_dist_abs": kd_dist_abs,
                f"stoch_rsi{s}_kd_direction": np.where(k > d, 1, -1),
                f"stoch_rsi{s}_kd_dist_slope": kd_slope,
                f"stoch_rsi{s}_cross_above": (kd_dist > 0) & (prev_kd <= 0),
                f"stoch_rsi{s}_cross_below": (kd_dist < 0) & (prev_kd >= 0),
                f"stoch_rsi{s}_both_gt_80": (k > 80) & (d > 80),
                f"stoch_rsi{s}_both_lt_20": (k < 20) & (d < 20),
                f"stoch_rsi{s}_strength": k_abs * kd_dist_abs,
            }
        )

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_trix(
    df: pd.DataFrame,
    n: list[int] = None,
    column_name: str = "close",
) -> pd.DataFrame:

    validate_column(df, column_name)

    if n is None:
        n = [15]

    df = df.copy()
    source = df[column_name].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        trix = pd.Series(talib.TRIX(source, timeperiod=period), index=df.index)

        trix_slope = trix.diff()
        trix_abs = trix.abs()

        signal = trix.rolling(window=period).mean()
        hist = trix - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols.update(
            {
                f"trix{s}": trix,
                f"trix{s}_slope": trix_slope,
                f"trix{s}_acceleration": trix_slope.diff(),
                f"trix{s}_abs": trix_abs,
                f"trix{s}_direction": np.where(trix > 0, 1, -1),
                f"trix{s}_gt_0": trix > 0,
                f"trix{s}_lt_0": trix < 0,
                f"trix{s}_signal": signal,
                f"trix{s}_signal_slope": signal.diff(),
                f"trix{s}_hist": hist,
                f"trix{s}_hist_slope": hist_slope,
                f"trix{s}_hist_acceleration": hist_slope.diff(),
                f"trix{s}_hist_gt_0": hist > 0,
                f"trix{s}_hist_lt_0": hist < 0,
                f"trix{s}_hist_abs": hist_abs,
                f"trix{s}_strength": trix_abs * hist_abs,
            }
        )

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_ultosc(
    df: pd.DataFrame,
    period1: list[int] = None,
    period2: list[int] = None,
    period3: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:

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

    new_cols = {}

    for p1, p2, p3 in product(period1, period2, period3):
        if not (p1 < p2 < p3):
            continue

        s = f"_{p1}_{p2}_{p3}"

        uo = pd.Series(
            talib.ULTOSC(high, low, close, p1, p2, p3),
            index=df.index,
        )

        uo_slope = uo.diff()
        uo_abs = (uo - 50).abs()

        signal = uo.rolling(window=p1).mean()
        hist = uo - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols.update(
            {
                f"ultosc{s}": uo,
                f"ultosc{s}_slope": uo_slope,
                f"ultosc{s}_acceleration": uo_slope.diff(),
                f"ultosc{s}_abs": uo_abs,
                f"ultosc{s}_direction": np.where(uo > 50, 1, -1),
                f"ultosc{s}_gt_70": uo > 70,
                f"ultosc{s}_lt_30": uo < 30,
                f"ultosc{s}_gt_50": uo > 50,
                f"ultosc{s}_lt_50": uo < 50,
                f"ultosc{s}_extreme": np.where(uo > 70, 1, np.where(uo < 30, -1, 0)),
                f"ultosc{s}_signal": signal,
                f"ultosc{s}_signal_slope": signal.diff(),
                f"ultosc{s}_hist": hist,
                f"ultosc{s}_hist_slope": hist_slope,
                f"ultosc{s}_hist_acceleration": hist_slope.diff(),
                f"ultosc{s}_hist_gt_0": hist > 0,
                f"ultosc{s}_hist_lt_0": hist < 0,
                f"ultosc{s}_hist_abs": hist_abs,
                f"ultosc{s}_strength": uo_abs * hist_abs,
            }
        )

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_willr(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:

    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    if n is None:
        n = [14]

    df = df.copy()

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    for period in n:
        s = f"_{period}"

        # --- core indicator ---
        willr = pd.Series(
            talib.WILLR(high, low, close, timeperiod=period),
            index=df.index,
        )

        # --- derivatives ---
        willr_slope = willr.diff()

        # --- reusable ---
        willr_abs = (willr + 50).abs()
        signal = willr.rolling(window=period).mean()
        hist = willr - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        # --- core ---
        new_cols[f"willr{s}"] = willr
        new_cols[f"willr{s}_slope"] = willr_slope
        new_cols[f"willr{s}_acceleration"] = willr_slope.diff()
        new_cols[f"willr{s}_abs"] = willr_abs
        new_cols[f"willr{s}_direction"] = np.where(willr > -50, 1, -1)

        # --- threshold flags ---
        new_cols[f"willr{s}_gt_minus20"] = willr > -20
        new_cols[f"willr{s}_lt_minus80"] = willr < -80
        new_cols[f"willr{s}_gt_minus50"] = willr > -50
        new_cols[f"willr{s}_lt_minus50"] = willr < -50
        new_cols[f"willr{s}_extreme"] = np.where(
            willr > -20, 1, np.where(willr < -80, -1, 0)
        )

        # --- signal & histogram ---
        new_cols[f"willr{s}_signal"] = signal
        new_cols[f"willr{s}_signal_slope"] = signal.diff()
        new_cols[f"willr{s}_hist"] = hist
        new_cols[f"willr{s}_hist_slope"] = hist_slope
        new_cols[f"willr{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"willr{s}_hist_gt_0"] = hist > 0
        new_cols[f"willr{s}_hist_lt_0"] = hist < 0
        new_cols[f"willr{s}_hist_abs"] = hist_abs

        # --- strength ---
        new_cols[f"willr{s}_strength"] = willr_abs * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


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
    for col in (high_col, low_col, close_col, volume_col):
        validate_column(df, col)

    if n is None:
        n = [10]

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    new_cols = {}

    # --- raw AD line (computed once) ---
    ad_series = pd.Series(talib.AD(high, low, close, volume), index=df.index)
    ad_slope = ad_series.diff()

    new_cols["ad"] = ad_series
    new_cols["ad_slope"] = ad_slope
    new_cols["ad_acceleration"] = ad_slope.diff()
    new_cols["ad_gt_0"] = ad_series > 0
    new_cols["ad_lt_0"] = ad_series < 0
    new_cols["ad_direction"] = np.where(ad_slope > 0, 1, -1)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        signal = ad_series.ewm(span=period, adjust=False).mean()
        hist = ad_series - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"ad_signal{s}"] = signal
        new_cols[f"ad_signal{s}_slope"] = signal.diff()
        new_cols[f"ad_hist{s}"] = hist
        new_cols[f"ad_hist{s}_slope"] = hist_slope
        new_cols[f"ad_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"ad_hist{s}_gt_0"] = hist > 0
        new_cols[f"ad_hist{s}_lt_0"] = hist < 0
        new_cols[f"ad_hist{s}_abs"] = hist_abs
        new_cols[f"ad{s}_strength"] = ad_slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_adosc(
    df: pd.DataFrame,
    fast: list[int] = None,
    slow: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col, volume_col):
        validate_column(df, col)

    fast = fast or [3]
    slow = slow or [10]

    high = df[high_col].to_numpy(dtype=float)
    low = df[low_col].to_numpy(dtype=float)
    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    new_cols = {}

    # --- raw AD line (shared) ---
    if "ad" not in df.columns:
        new_cols["ad"] = pd.Series(talib.AD(high, low, close, volume), index=df.index)

    for f, sl in product(fast, slow):
        if f >= sl:
            continue

        s = f"_{f}_{sl}"
        adosc = pd.Series(
            talib.ADOSC(high, low, close, volume, fastperiod=f, slowperiod=sl),
            index=df.index,
        )

        adosc_slope = adosc.diff()
        signal = adosc.rolling(window=f).mean()
        hist = adosc - signal
        hist_slope = hist.diff()

        # --- core & derivatives ---
        new_cols[f"adosc{s}"] = adosc
        new_cols[f"adosc{s}_slope"] = adosc_slope
        new_cols[f"adosc{s}_acceleration"] = adosc_slope.diff()
        new_cols[f"adosc{s}_abs"] = adosc.abs()
        new_cols[f"adosc{s}_direction"] = np.where(adosc > 0, 1, -1)
        new_cols[f"adosc{s}_gt_0"] = adosc > 0
        new_cols[f"adosc{s}_lt_0"] = adosc < 0

        # --- signal line & histogram ---
        new_cols[f"adosc{s}_signal"] = signal
        new_cols[f"adosc{s}_signal_slope"] = signal.diff()
        new_cols[f"adosc{s}_hist"] = hist
        new_cols[f"adosc{s}_hist_slope"] = hist_slope
        new_cols[f"adosc{s}_hist_acceleration"] = hist_slope.diff()
        new_cols[f"adosc{s}_hist_gt_0"] = hist > 0
        new_cols[f"adosc{s}_hist_lt_0"] = hist < 0
        new_cols[f"adosc{s}_hist_abs"] = hist.abs()

        # --- crossovers ---
        prev_adosc = adosc.shift(1)
        new_cols[f"adosc{s}_cross_above"] = (adosc > 0) & (prev_adosc <= 0)
        new_cols[f"adosc{s}_cross_below"] = (adosc < 0) & (prev_adosc >= 0)

        # --- strength ---
        new_cols[f"adosc{s}_strength"] = adosc.abs() * hist.abs()

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_obv(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
    volume_col: str = "matching_volume",
) -> pd.DataFrame:
    for col in (close_col, volume_col):
        validate_column(df, col)

    if n is None:
        n = [10]

    close = df[close_col].to_numpy(dtype=float)
    volume = df[volume_col].to_numpy(dtype=float)

    new_cols = {}

    # --- raw OBV (computed once) ---
    obv_series = pd.Series(talib.OBV(close, volume), index=df.index)
    obv_slope = obv_series.diff()

    new_cols["obv"] = obv_series
    new_cols["obv_slope"] = obv_slope
    new_cols["obv_acceleration"] = obv_slope.diff()
    new_cols["obv_gt_0"] = obv_series > 0
    new_cols["obv_lt_0"] = obv_series < 0
    new_cols["obv_direction"] = np.where(obv_slope > 0, 1, -1)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        signal = obv_series.ewm(span=period, adjust=False).mean()
        hist = obv_series - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"obv_signal{s}"] = signal
        new_cols[f"obv_signal{s}_slope"] = signal.diff()
        new_cols[f"obv_hist{s}"] = hist
        new_cols[f"obv_hist{s}_slope"] = hist_slope
        new_cols[f"obv_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"obv_hist{s}_gt_0"] = hist > 0
        new_cols[f"obv_hist{s}_lt_0"] = hist < 0
        new_cols[f"obv_hist{s}_abs"] = hist_abs
        new_cols[f"obv{s}_strength"] = obv_slope.abs() * hist_abs

        # --- crossovers ---
        prev_hist = hist.shift(1)
        new_cols[f"obv{s}_cross_above"] = (hist > 0) & (prev_hist <= 0)
        new_cols[f"obv{s}_cross_below"] = (hist < 0) & (prev_hist >= 0)

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


# endregion VOLUME INDICATORS


# region CYCLE INDICATORS
def add_ht_dcperiod(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    validate_column(df, close_col)

    if n is None:
        n = [10]

    close = df[close_col].to_numpy(dtype=float)
    new_cols = {}

    # --- raw dominant cycle period ---
    dcperiod = pd.Series(talib.HT_DCPERIOD(close), index=df.index)
    dcperiod_slope = dcperiod.diff()

    # --- base derivatives ---
    new_cols["ht_dcperiod"] = dcperiod
    new_cols["ht_dcperiod_slope"] = dcperiod_slope
    new_cols["ht_dcperiod_acceleration"] = dcperiod_slope.diff()
    new_cols["ht_dcperiod_gt_prev"] = dcperiod > dcperiod.shift(1)
    new_cols["ht_dcperiod_lt_prev"] = dcperiod < dcperiod.shift(1)
    new_cols["ht_dcperiod_direction"] = np.where(dcperiod_slope > 0, 1, -1)
    new_cols["ht_dcperiod_valid"] = np.isfinite(dcperiod) & (dcperiod > 0)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        signal = dcperiod.ewm(span=period, adjust=False).mean()
        hist = dcperiod - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"ht_dcperiod_signal{s}"] = signal
        new_cols[f"ht_dcperiod_signal{s}_slope"] = signal.diff()
        new_cols[f"ht_dcperiod_hist{s}"] = hist
        new_cols[f"ht_dcperiod_hist{s}_slope"] = hist_slope
        new_cols[f"ht_dcperiod_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"ht_dcperiod_hist{s}_gt_0"] = hist > 0
        new_cols[f"ht_dcperiod_hist{s}_lt_0"] = hist < 0
        new_cols[f"ht_dcperiod_hist{s}_abs"] = hist_abs
        new_cols[f"ht_dcperiod{s}_strength"] = dcperiod_slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_ht_dcphase(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    validate_column(df, close_col)

    if n is None:
        n = [10]

    close = df[close_col].to_numpy(dtype=float)
    new_cols = {}

    # --- raw phase ---
    phase = pd.Series(talib.HT_DCPHASE(close), index=df.index)
    wrapped = np.mod(phase, 360.0)

    # trig features
    phase_rad = np.deg2rad(wrapped)
    slope = wrapped.diff()

    new_cols["ht_dcphase"] = phase
    new_cols["ht_dcphase_wrapped"] = wrapped
    new_cols["ht_dcphase_sin"] = np.sin(phase_rad)
    new_cols["ht_dcphase_cos"] = np.cos(phase_rad)
    new_cols["ht_dcphase_slope"] = slope
    new_cols["ht_dcphase_acceleration"] = slope.diff()

    # quadrant (cycle stage)
    new_cols["ht_dcphase_quadrant"] = pd.cut(
        wrapped, bins=[0, 90, 180, 270, 360], labels=[1, 2, 3, 4], include_lowest=True
    )

    new_cols["ht_dcphase_direction"] = np.where(slope > 0, 1, -1)
    new_cols["ht_dcphase_valid"] = np.isfinite(phase)

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"

        signal = wrapped.ewm(span=period, adjust=False).mean()
        hist = wrapped - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"ht_dcphase_signal{s}"] = signal
        new_cols[f"ht_dcphase_signal{s}_slope"] = signal.diff()
        new_cols[f"ht_dcphase_hist{s}"] = hist
        new_cols[f"ht_dcphase_hist{s}_slope"] = hist_slope
        new_cols[f"ht_dcphase_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"ht_dcphase_hist{s}_gt_0"] = hist > 0
        new_cols[f"ht_dcphase_hist{s}_lt_0"] = hist < 0
        new_cols[f"ht_dcphase_hist{s}_abs"] = hist_abs
        new_cols[f"ht_dcphase{s}_strength"] = slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_ht_phasor(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    validate_column(df, close_col)

    if n is None:
        n = [10]

    close = df[close_col].to_numpy(dtype=float)
    new_cols = {}

    inphase, quadrature = talib.HT_PHASOR(close)
    amplitude = pd.Series(np.sqrt(inphase**2 + quadrature**2), index=df.index)
    amp_slope = amplitude.diff()

    new_cols["ht_phasor_inphase"] = pd.Series(inphase, index=df.index)
    new_cols["ht_phasor_quadrature"] = pd.Series(quadrature, index=df.index)
    new_cols["ht_phasor_amplitude"] = amplitude

    phase_deg = np.degrees(np.arctan2(quadrature, inphase))
    new_cols["ht_phasor_phase"] = pd.Series(phase_deg, index=df.index)
    new_cols["ht_phasor_phase_wrapped"] = np.mod(phase_deg, 360.0)

    new_cols["ht_phasor_slope"] = amp_slope
    new_cols["ht_phasor_acceleration"] = amp_slope.diff()
    new_cols["ht_phasor_direction"] = np.where(amp_slope > 0, 1, -1)
    new_cols["ht_phasor_valid"] = np.isfinite(inphase) & np.isfinite(quadrature)

    for period in n:
        s = f"_{period}"

        signal = amplitude.ewm(span=period, adjust=False).mean()
        hist = amplitude - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"ht_phasor_signal{s}"] = signal
        new_cols[f"ht_phasor_signal{s}_slope"] = signal.diff()
        new_cols[f"ht_phasor_hist{s}"] = hist
        new_cols[f"ht_phasor_hist{s}_slope"] = hist_slope
        new_cols[f"ht_phasor_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"ht_phasor_hist{s}_gt_0"] = hist > 0
        new_cols[f"ht_phasor_hist{s}_lt_0"] = hist < 0
        new_cols[f"ht_phasor_hist{s}_abs"] = hist_abs
        new_cols[f"ht_phasor{s}_strength"] = amp_slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_ht_sine(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    validate_column(df, close_col)

    if n is None:
        n = [10]

    close = df[close_col].to_numpy(dtype=float)
    new_cols = {}

    sine, leadsine = talib.HT_SINE(close)
    sine_series = pd.Series(sine, index=df.index)
    leadsine_series = pd.Series(leadsine, index=df.index)
    sine_slope = sine_series.diff()

    new_cols["ht_sine"] = sine_series
    new_cols["ht_leadsine"] = leadsine_series
    new_cols["ht_sine_diff"] = sine_series - leadsine_series
    new_cols["ht_sine_slope"] = sine_slope
    new_cols["ht_sine_acceleration"] = sine_slope.diff()
    new_cols["ht_sine_direction"] = np.where(sine_slope > 0, 1, -1)
    new_cols["ht_sine_gt_0"] = sine_series > 0
    new_cols["ht_sine_lt_0"] = sine_series < 0

    # crossovers
    prev_sine = sine_series.shift(1)
    prev_leadsine = leadsine_series.shift(1)
    new_cols["ht_sine_cross_above"] = (sine_series > leadsine_series) & (
        prev_sine <= prev_leadsine
    )
    new_cols["ht_sine_cross_below"] = (sine_series < leadsine_series) & (
        prev_sine >= prev_leadsine
    )
    new_cols["ht_sine_valid"] = np.isfinite(sine) & np.isfinite(leadsine)

    for period in n:
        s = f"_{period}"

        signal = sine_series.ewm(span=period, adjust=False).mean()
        hist = sine_series - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"ht_sine_signal{s}"] = signal
        new_cols[f"ht_sine_signal{s}_slope"] = signal.diff()
        new_cols[f"ht_sine_hist{s}"] = hist
        new_cols[f"ht_sine_hist{s}_slope"] = hist_slope
        new_cols[f"ht_sine_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"ht_sine_hist{s}_gt_0"] = hist > 0
        new_cols[f"ht_sine_hist{s}_lt_0"] = hist < 0
        new_cols[f"ht_sine_hist{s}_abs"] = hist_abs
        new_cols[f"ht_sine{s}_strength"] = sine_slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_ht_trendmode(
    df: pd.DataFrame,
    n: list[int] = None,
    close_col: str = "close",
) -> pd.DataFrame:
    validate_column(df, close_col)

    if n is None:
        n = [10]

    close = df[close_col].to_numpy(dtype=float)
    new_cols = {}

    trendmode = pd.Series(talib.HT_TRENDMODE(close), index=df.index)
    tm_slope = trendmode.diff()

    new_cols["ht_trendmode"] = trendmode
    new_cols["ht_trendmode_slope"] = tm_slope
    new_cols["ht_trendmode_acceleration"] = tm_slope.diff()
    new_cols["ht_trendmode_direction"] = np.where(trendmode == 1, 1, -1)
    new_cols["ht_trendmode_is_trend"] = trendmode == 1
    new_cols["ht_trendmode_is_cycle"] = trendmode == 0

    # regime switches
    prev = trendmode.shift(1)
    new_cols["ht_trendmode_switch_on"] = (trendmode == 1) & (prev == 0)
    new_cols["ht_trendmode_switch_off"] = (trendmode == 0) & (prev == 1)
    new_cols["ht_trendmode_valid"] = np.isfinite(trendmode)

    for period in n:
        s = f"_{period}"

        signal = trendmode.ewm(span=period, adjust=False).mean()
        hist = trendmode - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"ht_trendmode_signal{s}"] = signal
        new_cols[f"ht_trendmode_signal{s}_slope"] = signal.diff()
        new_cols[f"ht_trendmode_hist{s}"] = hist
        new_cols[f"ht_trendmode_hist{s}_slope"] = hist_slope
        new_cols[f"ht_trendmode_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"ht_trendmode_hist{s}_gt_0"] = hist > 0
        new_cols[f"ht_trendmode_hist{s}_lt_0"] = hist < 0
        new_cols[f"ht_trendmode_hist{s}_abs"] = hist_abs
        new_cols[f"ht_trendmode{s}_strength"] = tm_slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


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
    for col in (open_col, high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [10]

    # Prepare numpy arrays for TA-Lib
    op = df[open_col].to_numpy(dtype=float)
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)
    cl = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    # --- core ---
    avgp = pd.Series(talib.AVGPRICE(op, hi, lo, cl), index=df.index)
    slope = avgp.diff()

    new_cols["avgprice"] = avgp
    new_cols["avgprice_slope"] = slope
    new_cols["avgprice_acceleration"] = slope.diff()
    new_cols["avgprice_gt_close"] = avgp > df[close_col]
    new_cols["avgprice_lt_close"] = avgp < df[close_col]
    new_cols["avgprice_direction"] = np.where(slope > 0, 1, -1)

    # --- smoothing ---
    for period in n:
        s = f"_{period}"
        signal = avgp.ewm(span=period, adjust=False).mean()
        hist = avgp - signal
        hist_slope = hist.diff()
        hist_abs = hist.abs()

        new_cols[f"avgprice_signal{s}"] = signal
        new_cols[f"avgprice_signal{s}_slope"] = signal.diff()
        new_cols[f"avgprice_hist{s}"] = hist
        new_cols[f"avgprice_hist{s}_slope"] = hist_slope
        new_cols[f"avgprice_hist{s}_acceleration"] = hist_slope.diff()
        new_cols[f"avgprice_hist{s}_gt_0"] = hist > 0
        new_cols[f"avgprice_hist{s}_lt_0"] = hist < 0
        new_cols[f"avgprice_hist{s}_abs"] = hist_abs
        new_cols[f"avgprice{s}_strength"] = slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_medprice(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [10]
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)

    new_cols = {}

    medp = pd.Series(talib.MEDPRICE(hi, lo), index=df.index)
    slope = medp.diff()

    new_cols["medprice"] = medp
    new_cols["medprice_slope"] = slope
    new_cols["medprice_acceleration"] = slope.diff()
    new_cols["medprice_gt_close"] = medp > df[close_col]
    new_cols["medprice_lt_close"] = medp < df[close_col]
    new_cols["medprice_direction"] = np.where(slope > 0, 1, -1)

    for period in n:
        s = f"_{period}"
        signal = medp.ewm(span=period, adjust=False).mean()
        hist = medp - signal
        hist_abs = hist.abs()

        new_cols[f"medprice_signal{s}"] = signal
        new_cols[f"medprice_signal{s}_slope"] = signal.diff()
        new_cols[f"medprice_hist{s}"] = hist
        new_cols[f"medprice_hist{s}_slope"] = hist.diff()
        new_cols[f"medprice_hist{s}_acceleration"] = new_cols[
            f"medprice_hist{s}_slope"
        ].diff()
        new_cols[f"medprice_hist{s}_gt_0"] = hist > 0
        new_cols[f"medprice_hist{s}_lt_0"] = hist < 0
        new_cols[f"medprice_hist{s}_abs"] = hist_abs
        new_cols[f"medprice{s}_strength"] = slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_typprice(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [10]
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)
    cl = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    typp = pd.Series(talib.TYPPRICE(hi, lo, cl), index=df.index)
    slope = typp.diff()

    new_cols["typprice"] = typp
    new_cols["typprice_slope"] = slope
    new_cols["typprice_acceleration"] = slope.diff()
    new_cols["typprice_gt_close"] = typp > df[close_col]
    new_cols["typprice_lt_close"] = typp < df[close_col]
    new_cols["typprice_direction"] = np.where(slope > 0, 1, -1)

    for period in n:
        s = f"_{period}"
        signal = typp.ewm(span=period, adjust=False).mean()
        hist = typp - signal
        hist_abs = hist.abs()

        new_cols[f"typprice_signal{s}"] = signal
        new_cols[f"typprice_signal{s}_slope"] = signal.diff()
        new_cols[f"typprice_hist{s}"] = hist
        new_cols[f"typprice_hist{s}_slope"] = hist.diff()
        new_cols[f"typprice_hist{s}_acceleration"] = new_cols[
            f"typprice_hist{s}_slope"
        ].diff()
        new_cols[f"typprice_hist{s}_gt_0"] = hist > 0
        new_cols[f"typprice_hist{s}_lt_0"] = hist < 0
        new_cols[f"typprice_hist{s}_abs"] = hist_abs
        new_cols[f"typprice{s}_strength"] = slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_wclprice(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [10]
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)
    cl = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    wclp = pd.Series(talib.WCLPRICE(hi, lo, cl), index=df.index)
    slope = wclp.diff()

    new_cols["wclprice"] = wclp
    new_cols["wclprice_slope"] = slope
    new_cols["wclprice_acceleration"] = slope.diff()
    new_cols["wclprice_gt_close"] = wclp > df[close_col]
    new_cols["wclprice_lt_close"] = wclp < df[close_col]
    new_cols["wclprice_direction"] = np.where(slope > 0, 1, -1)

    for period in n:
        s = f"_{period}"
        signal = wclp.ewm(span=period, adjust=False).mean()
        hist = wclp - signal
        hist_abs = hist.abs()

        new_cols[f"wclprice_signal{s}"] = signal
        new_cols[f"wclprice_signal{s}_slope"] = signal.diff()
        new_cols[f"wclprice_hist{s}"] = hist
        new_cols[f"wclprice_hist{s}_slope"] = hist.diff()
        new_cols[f"wclprice_hist{s}_acceleration"] = new_cols[
            f"wclprice_hist{s}_slope"
        ].diff()
        new_cols[f"wclprice_hist{s}_gt_0"] = hist > 0
        new_cols[f"wclprice_hist{s}_lt_0"] = hist < 0
        new_cols[f"wclprice_hist{s}_abs"] = hist_abs
        new_cols[f"wclprice{s}_strength"] = slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


# endregion PRICE TRANSFORM


# region VOLATILITY INDICATORS
def add_atr(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [14]
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)
    cl = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    # --- base ATR (use first period as default reference) ---
    base_period = n[0]
    atr_base = pd.Series(talib.ATR(hi, lo, cl, timeperiod=base_period), index=df.index)
    slope_base = atr_base.diff()

    new_cols["atr"] = atr_base
    new_cols["atr_slope"] = slope_base
    new_cols["atr_acceleration"] = slope_base.diff()
    new_cols["atr_gt_prev"] = atr_base > atr_base.shift(1)
    new_cols["atr_lt_prev"] = atr_base < atr_base.shift(1)
    new_cols["atr_direction"] = np.where(slope_base > 0, 1, -1)
    new_cols["atr_normalized"] = atr_base / df[close_col]

    # --- per period ---
    for period in n:
        s = f"_{period}"
        atr_p = pd.Series(talib.ATR(hi, lo, cl, timeperiod=period), index=df.index)
        slope_p = atr_p.diff()

        new_cols[f"atr{s}"] = atr_p
        new_cols[f"atr{s}_slope"] = slope_p
        new_cols[f"atr{s}_acceleration"] = slope_p.diff()
        new_cols[f"atr{s}_gt_prev"] = atr_p > atr_p.shift(1)
        new_cols[f"atr{s}_lt_prev"] = atr_p < atr_p.shift(1)
        new_cols[f"atr{s}_normalized"] = atr_p / df[close_col]

        # signal + histogram
        signal = atr_p.ewm(span=period, adjust=False).mean()
        hist = atr_p - signal
        hist_abs = hist.abs()

        new_cols[f"atr{s}_signal"] = signal
        new_cols[f"atr{s}_signal_slope"] = signal.diff()
        new_cols[f"atr{s}_hist"] = hist
        new_cols[f"atr{s}_hist_slope"] = hist.diff()
        new_cols[f"atr{s}_hist_acceleration"] = new_cols[f"atr{s}_hist_slope"].diff()
        new_cols[f"atr{s}_hist_gt_0"] = hist > 0
        new_cols[f"atr{s}_hist_lt_0"] = hist < 0
        new_cols[f"atr{s}_hist_abs"] = hist_abs
        new_cols[f"atr{s}_strength"] = slope_p.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_natr(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [14]
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)
    cl = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    # --- base NATR ---
    base_period = n[0]
    natr_base = pd.Series(
        talib.NATR(hi, lo, cl, timeperiod=base_period), index=df.index
    )
    slope_base = natr_base.diff()

    new_cols["natr"] = natr_base
    new_cols["natr_slope"] = slope_base
    new_cols["natr_acceleration"] = slope_base.diff()
    new_cols["natr_gt_prev"] = natr_base > natr_base.shift(1)
    new_cols["natr_lt_prev"] = natr_base < natr_base.shift(1)
    new_cols["natr_direction"] = np.where(slope_base > 0, 1, -1)

    # --- per period ---
    for period in n:
        s = f"_{period}"
        natr_p = pd.Series(talib.NATR(hi, lo, cl, timeperiod=period), index=df.index)
        slope_p = natr_p.diff()

        new_cols[f"natr{s}"] = natr_p
        new_cols[f"natr{s}_slope"] = slope_p
        new_cols[f"natr{s}_acceleration"] = slope_p.diff()
        new_cols[f"natr{s}_gt_prev"] = natr_p > natr_p.shift(1)
        new_cols[f"natr{s}_lt_prev"] = natr_p < natr_p.shift(1)

        signal = natr_p.ewm(span=period, adjust=False).mean()
        hist = natr_p - signal
        hist_abs = hist.abs()

        new_cols[f"natr{s}_signal"] = signal
        new_cols[f"natr{s}_signal_slope"] = signal.diff()
        new_cols[f"natr{s}_hist"] = hist
        new_cols[f"natr{s}_hist_slope"] = hist.diff()
        new_cols[f"natr{s}_hist_acceleration"] = new_cols[f"natr{s}_hist_slope"].diff()
        new_cols[f"natr{s}_hist_gt_0"] = hist > 0
        new_cols[f"natr{s}_hist_lt_0"] = hist < 0
        new_cols[f"natr{s}_hist_abs"] = hist_abs
        new_cols[f"natr{s}_strength"] = slope_p.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_trange(
    df: pd.DataFrame,
    n: list[int] = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    for col in (high_col, low_col, close_col):
        validate_column(df, col)

    n = n or [14]
    hi = df[high_col].to_numpy(dtype=float)
    lo = df[low_col].to_numpy(dtype=float)
    cl = df[close_col].to_numpy(dtype=float)

    new_cols = {}

    # --- core TRANGE ---
    tr = pd.Series(talib.TRANGE(hi, lo, cl), index=df.index)
    tr_slope = tr.diff()

    new_cols["trange"] = tr
    new_cols["trange_slope"] = tr_slope
    new_cols["trange_acceleration"] = tr_slope.diff()
    new_cols["trange_gt_prev"] = tr > tr.shift(1)
    new_cols["trange_lt_prev"] = tr < tr.shift(1)
    new_cols["trange_direction"] = np.where(tr_slope > 0, 1, -1)
    new_cols["trange_normalized"] = tr / df[close_col]

    # --- per smoothing window ---
    for period in n:
        s = f"_{period}"
        signal = tr.ewm(span=period, adjust=False).mean()
        hist = tr - signal
        hist_abs = hist.abs()

        new_cols[f"trange_signal{s}"] = signal
        new_cols[f"trange_signal{s}_slope"] = signal.diff()
        new_cols[f"trange_hist{s}"] = hist
        new_cols[f"trange_hist{s}_slope"] = hist.diff()
        new_cols[f"trange_hist{s}_acceleration"] = new_cols[
            f"trange_hist{s}_slope"
        ].diff()
        new_cols[f"trange_hist{s}_gt_0"] = hist > 0
        new_cols[f"trange_hist{s}_lt_0"] = hist < 0
        new_cols[f"trange_hist{s}_abs"] = hist_abs
        new_cols[f"trange{s}_strength"] = tr_slope.abs() * hist_abs

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


# endregion VOLATILITY INDICATORS


# region FEATURE ENGINEERING (non-TA, works on OHLC or single-value series)
def add_returns(df: pd.DataFrame, column_name: str = "close") -> pd.DataFrame:
    """Add 1-period simple and log returns of `column_name`.

    return_simple = (p_t - p_{t-1}) / p_{t-1}
    return_log    = log(p_t / p_{t-1})
    """
    validate_column(df, column_name)
    df = df.copy()
    price = pd.to_numeric(df[column_name], errors="coerce")
    df["return_simple"] = price.pct_change()
    df["return_log"] = np.log(price / price.shift(1))
    return df


def add_intraday_range(df: pd.DataFrame) -> pd.DataFrame:
    """Add normalized high-low range and open-close candle body. Requires OHLC."""
    for col in ("open", "high", "low", "close"):
        validate_column(df, col)
    df = df.copy()
    open_ = pd.to_numeric(df["open"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    df["range_hl"] = (high - low) / close
    df["body_oc"] = (close - open_) / open_
    return df


def add_return_volatility(
    df: pd.DataFrame,
    column_name: str = "close",
    windows: list[int] = None,
) -> pd.DataFrame:
    """Add rolling realized volatility (std of log returns) over each window."""
    validate_column(df, column_name)
    windows = windows or [5, 21]
    df = df.copy()
    price = pd.to_numeric(df[column_name], errors="coerce")
    log_ret = np.log(price / price.shift(1))
    new_cols = {f"volatility_{w}": log_ret.rolling(w).std() for w in windows}
    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_rolling_statistics(
    df: pd.DataFrame,
    column_name: str = "close",
    windows: list[int] = None,
) -> pd.DataFrame:
    """Add rolling mean/std/min/max of `column_name` over each window."""
    validate_column(df, column_name)
    windows = windows or [5, 21]
    df = df.copy()
    series = pd.to_numeric(df[column_name], errors="coerce")
    new_cols = {}
    for w in windows:
        roll = series.rolling(w)
        new_cols[f"{column_name}_roll_mean_{w}"] = roll.mean()
        new_cols[f"{column_name}_roll_std_{w}"] = roll.std()
        new_cols[f"{column_name}_roll_min_{w}"] = roll.min()
        new_cols[f"{column_name}_roll_max_{w}"] = roll.max()
    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


# endregion FEATURE ENGINEERING


def add_one_for_all_ta(df: pd.DataFrame) -> pd.DataFrame:
    new_df = df.copy()

    # OVERLAP STUDIES
    new_df = add_bbands(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_dema(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ema(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_kama(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_midpoint(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_midprice(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_sar(
        new_df,
    )
    new_df = add_sma(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_t3(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_tema(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_trima(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_wma(
        new_df,
        n=[5, 10, 15, 20],
    )

    # MOMENTUM INDICATORS
    new_df = add_adx(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_aroon(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_bop(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_cci(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_cmo(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_macd(new_df)
    new_df = add_mfi(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_mom(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ppo(new_df)
    new_df = add_roc(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_rsi(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_stoch(new_df)
    new_df = add_stoch_rsi(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_trix(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ultosc(
        new_df,
    )
    new_df = add_willr(
        new_df,
        n=[5, 10, 15, 20],
    )

    # VOLUME INDICATORS
    new_df = add_ad(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_adosc(
        new_df,
    )
    new_df = add_obv(
        new_df,
        n=[5, 10, 15, 20],
    )

    # CYCLE INDICATORS
    new_df = add_ht_dcperiod(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ht_dcphase(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ht_phasor(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ht_sine(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_ht_trendmode(
        new_df,
        n=[5, 10, 15, 20],
    )

    # PRICE TRANSFORM
    new_df = add_avgprice(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_medprice(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_typprice(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_wclprice(
        new_df,
        n=[5, 10, 15, 20],
    )

    # VOLATILITY INDICATORS
    new_df = add_atr(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_natr(
        new_df,
        n=[5, 10, 15, 20],
    )
    new_df = add_trange(
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

    df = add_one_for_all_ta(df)

    print(len(list(df.columns)))

    # df[f"log_return_{FORECAST_HORIZON}"] = np.log(
    #     df["close"].shift(-FORECAST_HORIZON) / df["close"]
    # )

    # plot_with_indicators(
    #     df,
    #     indicators=["*trange*"],
    #     price_column_name=f"close",
    # )

    # plot_with_indicators(
    #     df,
    #     indicators=["close_t3_5", "close_t3_10", "close_t3_15"],
    #     price_column_name=f"return_{FORECAST_HORIZON}",
    # )

    print(len(df.columns))


if __name__ == "__main__":
    main()
