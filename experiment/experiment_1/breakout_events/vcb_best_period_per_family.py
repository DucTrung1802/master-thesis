"""
Best period per indicator family for VCB, using the EXISTING multi-period features
already stored in unified_schema.unified_vcb (not recomputed).

Many features embed a lookback period in their name (natr_14, volatility_5/21,
close_dema_50/100/200, rsi_28_signal, close_bb_20_bandwidth, ppo_12_26_9_*). We
parse family + period from each column, score every column by univariate ROC-AUC
for the 5d+5% label, then for each family report the PERIOD whose best variant has
the highest AUC.

    label y[t] = 1 if close[t+5]/close[t]-1 >= 0.05
    family = name tokens before the first integer  (e.g. close_dema)
    period = the run of integer tokens            (e.g. 50, or 12_26_9)

Output: vcb_best_period_per_family.csv (+ console).
"""

import os
import re
import warnings
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score

warnings.filterwarnings("ignore")
HORIZON, GAIN = 5, 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(HERE, "vcb_best_period_per_family.csv")
ID_COLS = {"exchange", "ticker", "date", "target"}


def load():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    return df


def parse(name):
    """Return (family, period_str). family = tokens before first integer token."""
    toks = name.split("_")
    fam, per = [], []
    i = 0
    while i < len(toks) and not toks[i].isdigit():
        fam.append(toks[i]); i += 1
    while i < len(toks) and toks[i].isdigit():
        per.append(toks[i]); i += 1
    return "_".join(fam), ("_".join(per) if per else "")


def main():
    df = load()
    close = df["close"].astype(float).values
    y = np.full(len(close), np.nan)
    y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)
    mask = ~np.isnan(y)
    yv = y[mask].astype(int)

    rows = []
    for c in df.columns:
        if c in ID_COLS or not np.issubdtype(df[c].dtype, np.number):
            continue
        fam, per = parse(c)
        if per == "":
            continue                                  # only period-bearing features
        col = df[c].astype(float).values[mask]
        ok = ~np.isnan(col)
        if ok.sum() < 300 or len(np.unique(col[ok])) < 3:
            continue
        auc = roc_auc_score(yv[ok], col[ok])
        rows.append({"family": fam, "period": per, "feature": c,
                     "auc": round(auc, 4), "strength": abs(auc - 0.5),
                     "direction": "high->up" if auc >= 0.5 else "low->up"})
    allf = pd.DataFrame(rows)

    # best variant at each (family, period), then best period per family
    fp = (allf.sort_values("strength", ascending=False)
          .groupby(["family", "period"], as_index=False).first())
    best = (fp.sort_values("strength", ascending=False)
            .groupby("family", as_index=False).first())
    best["periods_available"] = best["family"].map(
        allf.groupby("family")["period"].nunique())
    best = best.sort_values("auc", ascending=False).reset_index(drop=True)
    best = best[["family", "period", "auc", "direction", "feature",
                 "periods_available"]]
    best.to_csv(OUT_CSV, index=False)

    base = yv.mean()
    print(f"VCB unified_vcb | label next-{HORIZON}d >= {GAIN:.0%} | base rate {base:.1%}")
    print(f"{len(allf)} period-bearing features across {best.shape[0]} families "
          f"-> saved best period per family to {os.path.basename(OUT_CSV)}\n")
    print("Best period per family (sorted by AUC):")
    print(best.to_string(index=False))


if __name__ == "__main__":
    main()
