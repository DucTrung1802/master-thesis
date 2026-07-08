"""
Find features in the unified VCB dataframe that signal an imminent 5-day +5% move.

Label (what we want to foresee):
    y[t] = 1  if  close[t+5] / close[t] - 1 >= 0.05     (next 5 trading days up >=5%)
           0  otherwise
Features X[t] are every numeric column of unified_schema.unified_vcb known at the
close of day t (all TA / macro / calendar features). 'target' and identifiers are
excluded. Because X[t] uses only information up to day t, predicting y[t] is not
look-ahead leakage.

Two outputs:
  1. signal_ranking_5d5pct.csv  -- every feature ranked by univariate ROC-AUC
     (how well the raw feature value alone separates up-days from the rest).
  2. console -- base rate, top signals, and a time-split gradient-boosting model
     to confirm the features jointly carry predictive signal.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.ensemble import HistGradientBoostingClassifier

warnings.filterwarnings("ignore")

HORIZON = 5
GAIN = 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(HERE, "signal_ranking_5d5pct.csv")

ID_COLS = {"exchange", "ticker", "date", "target"}


def load() -> pd.DataFrame:
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2",
    )
    df = pd.read_sql(
        "SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def main():
    df = load()
    close = df["close"].astype(float)
    y = (close.shift(-HORIZON) / close - 1.0 >= GAIN).astype("float")
    y[close.shift(-HORIZON).isna()] = np.nan  # last HORIZON days have no label

    # feature matrix: numeric + boolean columns, cast to float
    feat_cols = [c for c in df.columns if c not in ID_COLS]
    X = df[feat_cols].copy()
    for c in feat_cols:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float)

    mask = y.notna()
    Xv, yv = X[mask], y[mask].astype(int)
    base = yv.mean()
    print(f"Rows labeled: {len(yv)}   up-days (next {HORIZON}d >= {GAIN:.0%}): "
          f"{yv.sum()}  ({base:.1%} base rate)\n")

    # ── 1. univariate signal strength (ROC-AUC of each raw feature) ──────────
    rows = []
    for c in Xv.columns:
        col = Xv[c]
        ok = col.notna()
        if ok.sum() < 200 or col[ok].nunique() < 3 or yv[ok].nunique() < 2:
            continue
        auc = roc_auc_score(yv[ok], col[ok])
        up_mean = col[ok][yv[ok] == 1].mean()
        flat_mean = col[ok][yv[ok] == 0].mean()
        rows.append({
            "feature": c,
            "auc": round(auc, 4),
            "strength": round(abs(auc - 0.5), 4),       # 0 = useless, .5 = perfect
            "direction": "high->up" if auc >= 0.5 else "low->up",
            "mean_up": round(float(up_mean), 4),
            "mean_flat": round(float(flat_mean), 4),
            "n": int(ok.sum()),
        })
    rank = (pd.DataFrame(rows)
            .sort_values("strength", ascending=False)
            .reset_index(drop=True))
    rank.to_csv(OUT_CSV, index=False)
    print(f"Saved univariate ranking ({len(rank)} features) -> "
          f"{os.path.basename(OUT_CSV)}\n")
    print("Top 25 univariate signals:")
    print(rank.head(25).to_string(index=False))

    # ── 2. joint predictability: time-split gradient boosting ────────────────
    split = int(len(Xv) * 0.8)
    Xtr, Xte = Xv.iloc[:split], Xv.iloc[split:]
    ytr, yte = yv.iloc[:split], yv.iloc[split:]
    clf = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        l2_regularization=1.0, random_state=0)
    clf.fit(Xtr, ytr)
    proba = clf.predict_proba(Xte)[:, 1]
    auc = roc_auc_score(yte, proba)
    ap = average_precision_score(yte, proba)

    # precision in the model's most-confident decile vs base rate
    k = max(1, len(proba) // 10)
    top_idx = np.argsort(proba)[-k:]
    prec_top = yte.iloc[top_idx].mean()
    print(f"\nTime-split model (train {len(Xtr)} / test {len(Xte)} most-recent days):")
    print(f"  test ROC-AUC            : {auc:.3f}   (0.5 = no signal)")
    print(f"  test PR-AUC             : {ap:.3f}   (base rate {yte.mean():.1%})")
    print(f"  precision in top decile : {prec_top:.1%}   "
          f"(lift x{prec_top / max(yte.mean(), 1e-9):.1f} over base)")


if __name__ == "__main__":
    main()
