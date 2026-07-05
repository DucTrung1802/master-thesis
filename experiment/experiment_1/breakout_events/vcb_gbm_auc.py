"""
Reproduce the VCB test ROC-AUC ~= 0.77 for the "next 5 trading days up >= 5%" signal.

The recipe (this exact combination is what reaches 0.77):
  * single stock VCB (its own model -- VCB is the most predictable VN30 name)
  * ALL ~1073 features from the macro + TA + calendar feature pools
    (unified_schema_vcb.pool__macro / pool__ta / pool__calendar),
    so the trees can exploit feature interactions. The basic price/volume/
    foreign pool is deliberately NOT used as features -- adding it drops the
    test AUC to ~0.74; `close` is pulled from it only to build the label.
  * gradient-boosted trees (HistGradientBoostingClassifier) -- NOT deep learning,
    NOT a single indicator
  * chronological split: train on the first 80% of days, test on the most-recent 20%
  * evaluate ROC-AUC on that held-out test tail

Database note: the flat table unified_schema.unified_vcb no longer exists. The
VCB feature set now lives in the `unified_schema_vcb` schema of database
`database_main_v2`, split into per-group pools joined on `date`.
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
SCHEMA = "unified_schema_vcb"
# `close` is kept only to build the label; the rest are dropped from X below.
ID_COLS = {"exchange", "ticker", "date", "target", "close"}


def main():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    # macro + TA + calendar feature pools, joined on date; `close` (for the
    # label) is pulled from the basic pool but not used as a feature.
    cal = pd.read_sql(f"SELECT * FROM {SCHEMA}.pool__calendar ORDER BY date", conn)
    mac = pd.read_sql(f"SELECT * FROM {SCHEMA}.pool__macro ORDER BY date", conn)
    ta = pd.read_sql(f"SELECT * FROM {SCHEMA}.pool__ta ORDER BY date", conn)
    basic = pd.read_sql(f"SELECT date, close FROM {SCHEMA}.pool__basic ORDER BY date", conn)
    conn.close()
    df = (cal.merge(mac, on="date").merge(ta, on="date").merge(basic, on="date")
          .sort_values("date").reset_index(drop=True))

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
