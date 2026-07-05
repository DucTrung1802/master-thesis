"""
Multi-period TA sweep for VCB: does choosing a different lookback period raise the
univariate signal (ROC-AUC) for the "next 5 trading days up >= 5%" label?

The unified table ships indicators at a few fixed periods (e.g. natr_14,
volatility_21, bb_20). Here we recompute the strongest families across a grid of
periods and see which period maximises the AUC for each family.

    label y[t] = 1 if close[t+5]/close[t] - 1 >= 0.05
    feature X[t] = indicator(period) known at close of day t  (no look-ahead)

Output: ta_period_sweep_vcb.csv (family, period, auc, strength, direction),
plus the best period per family vs the unified table's default.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
import talib
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score

warnings.filterwarnings("ignore")

HORIZON, GAIN = 5, 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(HERE, "ta_period_sweep_vcb.csv")
PERIODS = [2, 3, 5, 7, 10, 14, 20, 28, 40, 50, 63, 100]


def load():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT date, high, low, close, volume "
                     "FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    return df


def indicators(h, l, c, v, p):
    """Period-parameterized indicators for one lookback p -> {name: Series}."""
    up, mid, low = talib.BBANDS(c, p, 2, 2)
    out = {
        "natr": talib.NATR(h, l, c, p),
        "atr_normalized": talib.ATR(h, l, c, p) / c,
        "realized_vol": pd.Series(c).pct_change().rolling(p).std().values * 100,
        "bb_bandwidth": (up - low) / mid,
        "roc": talib.ROC(c, p),
        "rsi": talib.RSI(c, p),
        "cmo": talib.CMO(c, p),
        "adx": talib.ADX(h, l, c, p),
        "willr": talib.WILLR(h, l, c, p),
        "mfi": talib.MFI(h, l, c, v, p),
    }
    return out


def auc_of(feat, y):
    feat = np.asarray(feat, dtype=float)
    ok = ~np.isnan(feat) & ~np.isnan(y)
    if ok.sum() < 300 or len(np.unique(feat[ok])) < 3 or len(np.unique(y[ok])) < 2:
        return np.nan
    return roc_auc_score(y[ok], feat[ok])


def main():
    df = load()
    c = df["close"].astype("float64").values
    h = df["high"].astype("float64").values
    l = df["low"].astype("float64").values
    v = df["volume"].astype("float64").values

    fwd = np.full(len(c), np.nan)
    fwd[:-HORIZON] = c[HORIZON:] / c[:-HORIZON] - 1.0
    y = np.where(np.isnan(fwd), np.nan, (fwd >= GAIN).astype(float))

    rows = []
    for p in PERIODS:
        for name, feat in indicators(h, l, c, v, p).items():
            auc = auc_of(feat, y)
            if not np.isnan(auc):
                rows.append({"family": name, "period": p, "auc": round(auc, 4),
                             "strength": round(abs(auc - 0.5), 4),
                             "direction": "high->up" if auc >= 0.5 else "low->up"})
    res = pd.DataFrame(rows)
    res.to_csv(OUT_CSV, index=False)
    print(f"Saved sweep ({len(res)} rows) -> {os.path.basename(OUT_CSV)}\n")

    # best period per family + the strongest overall
    best = (res.sort_values("strength", ascending=False)
            .groupby("family", as_index=False).first()
            .sort_values("strength", ascending=False))
    print("Best period per family (VCB, 5d+5% label):")
    print(best[["family", "period", "auc", "strength", "direction"]].to_string(index=False))

    print("\nFull period curve for the top volatility/momentum families:")
    for fam in ["natr", "bb_bandwidth", "realized_vol", "roc", "rsi"]:
        sub = res[res.family == fam].sort_values("period")
        curve = "  ".join(f"p{r.period}={r.auc:.3f}" for r in sub.itertuples())
        print(f"  {fam:14s} {curve}")


if __name__ == "__main__":
    main()
