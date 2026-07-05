"""
Reproduce the VCB test ROC-AUC ~= 0.77 for the "next 5 trading days up >= 5%" signal.

The recipe (this exact combination is what reaches 0.77):
  * single stock VCB (its own model -- VCB is the most predictable VN30 name)
  * ALL ~1058 features from unified_schema.unified_vcb (macro + TA + calendar),
    so the trees can exploit feature interactions
  * gradient-boosted trees (HistGradientBoostingClassifier) -- NOT deep learning,
    NOT a single indicator
  * chronological split: train on the first 80% of days, test on the most-recent 20%
  * evaluate ROC-AUC on that held-out test tail
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
HORIZON, GAIN = 5, 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
ID_COLS = {"exchange", "ticker", "date", "target"}


def main():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()

    # label: next-5-day forward return >= 5%
    close = df["close"].astype(float).values
    y = np.full(len(close), np.nan)
    y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)

    # full numeric feature matrix (bool -> float); no imputation needed (trees handle NaN)
    feat = [c for c in df.columns if c not in ID_COLS]
    X = df[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float)

    m = ~np.isnan(y)
    X, y = X[m], y[m].astype(int)

    # chronological 80/20 split (NO shuffling -> no look-ahead)
    split = int(len(X) * 0.80)
    Xtr, Xte = X.iloc[:split], X.iloc[split:]
    ytr, yte = y[:split], y[split:]

    clf = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        l2_regularization=1.0, random_state=0)
    clf.fit(Xtr, ytr)
    p = clf.predict_proba(Xte)[:, 1]

    auc = roc_auc_score(yte, p)
    ap = average_precision_score(yte, p)
    base = yte.mean()
    k = max(1, len(p) // 10)
    prec_top = yte[np.argsort(p)[-k:]].mean()

    print(f"VCB | {len(X)} labeled days | {X.shape[1]} features | "
          f"train {len(Xtr)} / test {len(Xte)}")
    print(f"test base rate          : {base:.1%}")
    print(f"test ROC-AUC            : {auc:.3f}   <-- the ~0.77 number")
    print(f"test PR-AUC             : {ap:.3f}")
    print(f"precision in top decile : {prec_top:.1%}  (lift x{prec_top/max(base,1e-9):.1f})")


if __name__ == "__main__":
    main()
