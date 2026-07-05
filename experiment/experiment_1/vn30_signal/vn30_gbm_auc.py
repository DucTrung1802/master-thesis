"""
Is VCB's 0.78 test ROC-AUC a real signal or luck? Re-run the EXACT vcb_gbm_auc
recipe on every VN30 stock and look at the distribution.

Method (identical to breakout_events/vcb_gbm_auc.py, per ticker):
  * label  y[t] = 1 if close[t+5]/close[t]-1 >= 5%   (next 5 trading days up >=5%)
  * features = macro + TA + calendar pools (NOT the basic price/volume pool);
    close is used only to build the label
  * HistGradientBoostingClassifier, chronological 80/20 split, test ROC-AUC

Because the flat unified_schema.unified_<ticker> tables no longer exist, the
per-ticker feature matrix is rebuilt from the current DB:
  * TA (905 cols) + close  -> gold_schema.stocks filtered to the ticker
  * macro (149) + calendar (19) -> unified_schema_vcb.pool__macro / pool__calendar
    (both are date-keyed and ticker-independent, so shared across the universe)

For each ticker we also refit WITHOUT the macro pool (TA + calendar only). If the
macro block is doing the heavy lifting, that is a red flag: the TradingView macro
series are stamped to their reference-period date, not their release date, so a
forward-filled macro value can be visible before it was actually published
(look-ahead leakage). auc_full >> auc_nomacro across the board == suspicious.

Output: vn30_gbm_auc.csv (one row per ticker) + console summary.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score
from sklearn.ensemble import HistGradientBoostingClassifier

warnings.filterwarnings("ignore")

HORIZON, GAIN = 5, 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = "unified_schema_vcb"
OUT_CSV = os.path.join(HERE, "vn30_gbm_auc.csv")

VN30 = ["ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
        "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
        "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"]

HGB = dict(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
           l2_regularization=1.0, random_state=0)


def connect():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")


def fit_auc(X, y):
    """Chronological 80/20 split -> (test_auc, base_rate, lift, n_test, n_pos)."""
    split = int(len(X) * 0.80)
    Xtr, Xte = X.iloc[:split], X.iloc[split:]
    ytr, yte = y[:split], y[split:]
    if yte.sum() < 3 or ytr.sum() < 3:
        return None
    clf = HistGradientBoostingClassifier(**HGB).fit(Xtr, ytr)
    p = clf.predict_proba(Xte)[:, 1]
    k = max(1, len(p) // 10)
    prec_top = yte[np.argsort(p)[-k:]].mean()
    base = yte.mean()
    return dict(test_auc=round(roc_auc_score(yte, p), 3),
                base_rate=round(float(base), 4),
                lift=round(float(prec_top / max(base, 1e-9)), 2),
                n_test=int(len(yte)), n_pos_test=int(yte.sum()))


def main():
    conn = connect()
    # date-keyed, ticker-independent feature blocks (loaded once)
    cal = pd.read_sql(f"SELECT * FROM {SCHEMA}.pool__calendar ORDER BY date", conn)
    mac = pd.read_sql(f"SELECT * FROM {SCHEMA}.pool__macro ORDER BY date", conn)
    ta_cols = [c for c in pd.read_sql(
        f"SELECT * FROM {SCHEMA}.pool__ta LIMIT 0", conn).columns if c != "date"]
    macal = cal.merge(mac, on="date")
    cal_feats = [c for c in cal.columns if c != "date"]
    mac_feats = [c for c in mac.columns if c != "date"]

    ta_select = ", ".join(['"date"', '"close"'] + [f'"{c}"' for c in ta_cols])
    rows = []
    for tk in VN30:
        ta = pd.read_sql(
            f"SELECT {ta_select} FROM gold_schema.stocks "
            f"WHERE ticker = %s ORDER BY date", conn, params=(tk,))
        df = ta.merge(macal, on="date").sort_values("date").reset_index(drop=True)

        close = df["close"].astype(float).values
        y = np.full(len(close), np.nan)
        y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)

        full = ta_cols + mac_feats + cal_feats
        X = df[full].copy()
        for c in full:
            if X[c].dtype == bool:
                X[c] = X[c].astype(float)
        X = X.select_dtypes(include=[np.number]).astype("float32")

        m = ~np.isnan(y)
        X, yy = X[m], y[m].astype(int)
        if len(yy) < 300 or yy.sum() < 20:
            print(f"  {tk}: skipped (insufficient data)")
            continue

        r_full = fit_auc(X, yy)
        # ablation: TA + calendar only (drop the macro block)
        keep = [c for c in X.columns if c not in set(mac_feats)]
        r_nom = fit_auc(X[keep], yy)
        if r_full is None or r_nom is None:
            print(f"  {tk}: skipped (too few positives in test)")
            continue

        rows.append(dict(ticker=tk, rows=int(len(yy)),
                         base_rate=r_full["base_rate"],
                         n_test=r_full["n_test"], n_pos_test=r_full["n_pos_test"],
                         auc_full=r_full["test_auc"], auc_nomacro=r_nom["test_auc"],
                         lift=r_full["lift"]))
        print(f"  {tk:4s} auc_full={r_full['test_auc']:.3f} "
              f"auc_nomacro={r_nom['test_auc']:.3f} "
              f"(n_pos_test={r_full['n_pos_test']}, lift={r_full['lift']})")
    conn.close()

    res = pd.DataFrame(rows).sort_values("auc_full", ascending=False)
    res.to_csv(OUT_CSV, index=False)
    print(f"\nSaved -> {os.path.basename(OUT_CSV)}")
    print(res.to_string(index=False))
    print("\n=== summary (test ROC-AUC across VN30) ===")
    for col in ["auc_full", "auc_nomacro"]:
        s = res[col]
        print(f"  {col:11s}: mean {s.mean():.3f}  median {s.median():.3f}  "
              f">=0.70: {(s >= 0.70).sum()}/{len(s)}  "
              f"in [0.45,0.55]: {s.between(0.45, 0.55).sum()}/{len(s)}")
    print(f"  macro effect (auc_full - auc_nomacro): "
          f"mean {(res['auc_full'] - res['auc_nomacro']).mean():+.3f}")


if __name__ == "__main__":
    main()
