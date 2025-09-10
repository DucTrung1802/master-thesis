import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


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
    df[f"sma_{n}"] = df["price"].rolling(window=n, min_periods=1).mean()
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
    df[f"ema_{n}"] = df["price"].ewm(span=n, adjust=False).mean()
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
        df["price"]
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
    df[f"wma_{n}"] = df["price"].ewm(alpha=alpha, adjust=False).mean()
    return df


def generate_trend_data():
    dates = pd.date_range(start="2025-01-01", end="2025-08-31", freq="D")
    n = len(dates)

    # Split into 3 phases: up, down, big up
    phase1 = int(n * 0.3)  # ~30%
    phase2 = int(n * 0.4)  # ~40%
    phase3 = n - (phase1 + phase2)

    np.random.seed(42)

    # Phase 1: go up from 100 → 130
    up1 = np.linspace(100, 130, phase1)

    # Phase 2: go down from 130 → 110
    down = np.linspace(130, 110, phase2)

    # Phase 3: strong rally from 110 → 170
    up2 = np.linspace(110, 170, phase3)

    trend = np.concatenate([up1, down, up2])
    noise = np.random.normal(0, 2, n)
    prices = trend + noise

    return prepare_data(dates, prices)


def plot_with_indicators(df: pd.DataFrame, indicators: list):
    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["price"], label="Price", linewidth=2)

    for col in indicators:
        if col in df.columns:
            plt.plot(
                df["date"], df[col], label=col.upper(), linewidth=2, linestyle="--"
            )

    plt.title("Price with Indicators", fontsize=14)
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()


def main():
    df = generate_trend_data()
    df = add_wma(df, n=14)
    df = add_wma(df, n=50)
    df = add_wma(df, n=100)

    # Plot all indicators you calculated
    plot_with_indicators(
        df,
        indicators=[
            "wma_14",
            "wma_50",
            "wma_100",
        ],
    )


if __name__ == "__main__":
    main()
