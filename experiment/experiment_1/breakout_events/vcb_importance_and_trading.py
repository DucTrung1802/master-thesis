"""
Answers three questions about the VCB 0.77-AUC model:
  1. WHICH features matter        -> permutation importance + AUC vs top-K curve
  2. INPUT/OUTPUT/MODEL           -> printed shapes
  3. TRADING MEANING              -> forward 5d returns on high-signal days

Same setup as vcb_gbm_auc.py: unified_vcb, label next-5d >= +5%, chronological
80/20 split, HistGradientBoostingClassifier.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance

warnings.filterwarnings("ignore")
HORIZON, GAIN = 5, 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(HERE, "vcb_feature_importance.csv")
ID_COLS = {"exchange", "ticker", "date", "target"}


def fit_auc(Xtr, ytr, Xte, yte):
    clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
        max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
    clf.fit(Xtr, ytr)
    p = clf.predict_proba(Xte)[:, 1]
    return clf, p, roc_auc_score(yte, p)


def main():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT date, close FROM unified_schema.unified_vcb ORDER BY date", conn)
    full = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()

    close = full["close"].astype(float).values
    fwd = np.full(len(close), np.nan)
    fwd[:-HORIZON] = close[HORIZON:] / close[:-HORIZON] - 1.0      # continuous fwd 5d return
    y = (fwd >= GAIN).astype(float); y[np.isnan(fwd)] = np.nan

    feat = [c for c in full.columns if c not in ID_COLS]
    X = full[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float)
    cols = X.columns

    m = ~np.isnan(y)
    Xv, yv, fwdv = X[m].reset_index(drop=True), y[m].astype(int), fwd[m]
    split = int(len(Xv) * 0.80)
    Xtr, Xte = Xv.iloc[:split], Xv.iloc[split:]
    ytr, yte = yv[:split], yv[split:]
    fwd_te = fwdv[split:]
    # validation slice carved from the END of train (for leak-free feature ranking)
    vsplit = int(len(Xtr) * 0.85)
    Xtr2, Xval = Xtr.iloc[:vsplit], Xtr.iloc[vsplit:]
    ytr2, yval = ytr[:vsplit], ytr[vsplit:]

    # ── full model ───────────────────────────────────────────────────────────
    clf, p, auc = fit_auc(Xtr, ytr, Xte, yte)
    print("=== 2. INPUT / OUTPUT / MODEL ===")
    print(f"  input  X : 2-D tabular MATRIX  (rows=trading days, cols=features)")
    print(f"             train {Xtr.shape}  test {Xte.shape}   <- one row = one day")
    print(f"  output   : probability per day P(next-5d >= +5%), shape ({len(p)},) in [0,1]")
    print(f"  model    : HistGradientBoostingClassifier (gradient-boosted trees)")
    print(f"  full-feature test ROC-AUC = {auc:.3f}\n")

    # ── 1. which features matter (ranking from VALIDATION, evaluated on TEST) ───
    print("=== 1. WHICH FEATURES (permutation importance on validation slice) ===")
    clf_v, _, _ = fit_auc(Xtr2, ytr2, Xval, yval)
    pi = permutation_importance(clf_v, Xval, yval, scoring="roc_auc",
                                n_repeats=5, random_state=0, n_jobs=-1)
    imp = (pd.DataFrame({"feature": cols, "importance": pi.importances_mean})
           .sort_values("importance", ascending=False).reset_index(drop=True))
    imp.to_csv(OUT_CSV, index=False)
    print(imp.head(20).to_string(index=False))
    print(f"  (full ranking -> {os.path.basename(OUT_CSV)})\n")

    print("  AUC when keeping only the top-K features (selected on val, refit on")
    print("  full train, evaluated once on test -> leak-free):")
    for K in [5, 10, 20, 50, 100, 300, len(cols)]:
        top = imp["feature"].head(K).tolist()
        _, _, a = fit_auc(Xtr[top], ytr, Xte[top], yte)
        print(f"    top-{K:<4d} features -> ROC-AUC {a:.3f}")

    # ── 3. trading meaning ─────────────────────────────────────────────────────
    print("\n=== 3. TRADING MEANING (test period, actual forward 5d returns) ===")
    base_ret, base_hit = fwd_te.mean(), (fwd_te > 0).mean()
    print(f"  all test days        : mean fwd-5d {base_ret:+.2%} | win-rate {base_hit:.0%} "
          f"| >=+5% rate {(fwd_te>=GAIN).mean():.0%}  (n={len(fwd_te)})")
    order = np.argsort(p)
    for q, lbl in [(0.10, "top 10%"), (0.20, "top 20%")]:
        k = max(1, int(len(p) * q))
        idx = order[-k:]
        r = fwd_te[idx]
        print(f"  signal {lbl:7s}     : mean fwd-5d {r.mean():+.2%} | win-rate {(r>0).mean():.0%} "
              f"| >=+5% rate {(r>=GAIN).mean():.0%}  (n={len(r)})")


if __name__ == "__main__":
    main()
