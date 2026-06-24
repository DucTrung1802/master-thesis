"""
Extend the "5-day +5% move" signal search from VCB to the whole VN30 universe.

For each of the 30 unified_schema.unified_<ticker> tables we build the same label
    y[t] = 1 if close[t+5]/close[t] - 1 >= 0.05   (next 5 trading days up >= 5%)
and ask two questions:

  1. PER TICKER  -- is the move foreseeable for this stock on its own?
     A time-split gradient-boosting model (train = first 80% of days, test = most
     recent 20%) gives test ROC-AUC and the precision lift of its top-decile
     predictions over the stock's base rate. Top univariate feature is also kept.
     -> vn30_signal_per_ticker.csv  (30 rows)

  2. UNIFIED VN30 -- is there a GENERAL signal shared across the universe?
     All 30 stocks are pooled into one panel (label computed per ticker first, so
     no cross-ticker bleed). Univariate ROC-AUC is recomputed on the pool to find
     features that work for the whole VN30, and one pooled model is trained with a
     global date cutoff (train = older dates, test = newer dates -> no look-ahead).
     -> vn30_general_signal_ranking.csv  + console summary

Note: raw price-level features (close, long DEMA/EMA, peer prices) are non-stationary
and not comparable across stocks of different price scales, so they wash out in the
pooled ranking -- the surviving top signals are the cross-sectionally robust ones.
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
ID_COLS = {"exchange", "ticker", "date", "target"}
PER_TICKER_CSV = os.path.join(HERE, "vn30_signal_per_ticker.csv")
GENERAL_CSV = os.path.join(HERE, "vn30_general_signal_ranking.csv")

VN30 = ["acb", "bcm", "bid", "bvh", "ctg", "fpt", "gas", "gvr", "hdb", "hpg",
        "mbb", "msn", "mwg", "plx", "pow", "sab", "shb", "ssb", "ssi", "stb",
        "tcb", "tpb", "vcb", "vhm", "vib", "vic", "vjc", "vnm", "vpb", "vre"]


def connect():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")


def load_ticker(conn, tk: str) -> pd.DataFrame:
    df = pd.read_sql(
        f"SELECT * FROM unified_schema.unified_{tk} ORDER BY date", conn)
    df["date"] = pd.to_datetime(df["date"])
    return df


def make_xy(df: pd.DataFrame):
    """Return (X float32 numeric features, y label, date) for one ticker."""
    close = df["close"].astype(float)
    y = (close.shift(-HORIZON) / close - 1.0 >= GAIN).astype("float")
    y[close.shift(-HORIZON).isna()] = np.nan
    feat = [c for c in df.columns if c not in ID_COLS]
    X = df[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype("float32")
    return X, y, df["date"]


def time_split_model(X, y):
    """80/20 chronological split -> test AUC, top-decile precision, base rate."""
    m = y.notna()
    X, y = X[m], y[m].astype(int)
    if y.sum() < 20 or len(y) < 300:
        return None
    split = int(len(X) * 0.8)
    Xtr, Xte, ytr, yte = X.iloc[:split], X.iloc[split:], y.iloc[:split], y.iloc[split:]
    if yte.sum() < 3 or ytr.sum() < 3:
        return None
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.05, max_leaf_nodes=31,
        l2_regularization=1.0, random_state=0)
    clf.fit(Xtr, ytr)
    p = clf.predict_proba(Xte)[:, 1]
    k = max(1, len(p) // 10)
    prec_top = yte.iloc[np.argsort(p)[-k:]].mean()
    base = yte.mean()
    return {
        "test_auc": round(roc_auc_score(yte, p), 3),
        "test_base_rate": round(base, 4),
        "top_decile_prec": round(prec_top, 4),
        "lift": round(prec_top / max(base, 1e-9), 2),
    }


def univariate(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    m = y.notna()
    X, y = X[m], y[m].astype(int)
    rows = []
    for c in X.columns:
        col = X[c]
        ok = col.notna()
        if ok.sum() < 200 or col[ok].nunique() < 3 or y[ok].nunique() < 2:
            continue
        auc = roc_auc_score(y[ok], col[ok])
        rows.append((c, auc, abs(auc - 0.5)))
    return pd.DataFrame(rows, columns=["feature", "auc", "strength"])


def main():
    conn = connect()
    per_rows, pool_X, pool_y, pool_date = [], [], [], []

    for tk in VN30:
        df = load_ticker(conn, tk)
        X, y, date = make_xy(df)
        res = time_split_model(X, y)
        uni = univariate(X, y).sort_values("strength", ascending=False)
        top = uni.iloc[0] if len(uni) else None
        m = y.notna()
        per_rows.append({
            "ticker": tk.upper(),
            "rows": int(m.sum()),
            "up_days": int(y[m].sum()),
            "base_rate": round(float(y[m].mean()), 4),
            "test_auc": res["test_auc"] if res else np.nan,
            "top_decile_prec": res["top_decile_prec"] if res else np.nan,
            "lift": res["lift"] if res else np.nan,
            "top_signal": top["feature"] if top is not None else "",
            "top_signal_auc": round(float(top["auc"]), 3) if top is not None else np.nan,
        })
        pool_X.append(X)
        pool_y.append(y)
        pool_date.append(date)
        print(f"  {tk.upper():4s} done  "
              f"(auc={per_rows[-1]['test_auc']}, lift={per_rows[-1]['lift']})")
    conn.close()

    per = pd.DataFrame(per_rows).sort_values("test_auc", ascending=False)
    per.to_csv(PER_TICKER_CSV, index=False)
    print(f"\nSaved per-ticker table (30 rows) -> {os.path.basename(PER_TICKER_CSV)}")
    print(per.to_string(index=False))

    # ── unified VN30 panel ───────────────────────────────────────────────────
    cols = set.intersection(*[set(x.columns) for x in pool_X])
    cols = [c for c in pool_X[0].columns if c in cols]
    X = pd.concat([x[cols] for x in pool_X], ignore_index=True)
    y = pd.concat(pool_y, ignore_index=True)
    d = pd.concat(pool_date, ignore_index=True)

    uni = univariate(X, y).sort_values("strength", ascending=False).reset_index(drop=True)
    uni["direction"] = np.where(uni["auc"] >= 0.5, "high->up", "low->up")
    uni.to_csv(GENERAL_CSV, index=False)
    base = y[y.notna()].mean()
    print(f"\n=== UNIFIED VN30 ({len(X):,} stock-days, base rate {base:.1%}) ===")
    print(f"Saved general ranking ({len(uni)} features) -> {os.path.basename(GENERAL_CSV)}")
    print("\nTop 20 general VN30 signals:")
    print(uni.head(20).to_string(index=False))

    # pooled model with a global date cutoff (no look-ahead across the panel)
    m = y.notna()
    Xv, yv, dv = X[m], y[m].astype(int), d[m]
    cutoff = dv.quantile(0.8)
    tr, te = dv < cutoff, dv >= cutoff
    clf = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        l2_regularization=1.0, random_state=0)
    clf.fit(Xv[tr], yv[tr])
    p = clf.predict_proba(Xv[te])[:, 1]
    k = max(1, len(p) // 10)
    prec_top = yv[te].iloc[np.argsort(p)[-k:]].mean()
    print(f"\nPooled VN30 model (date cutoff {cutoff.date()}, "
          f"train {tr.sum():,} / test {te.sum():,}):")
    print(f"  test ROC-AUC            : {roc_auc_score(yv[te], p):.3f}")
    print(f"  test PR-AUC             : {average_precision_score(yv[te], p):.3f} "
          f"(base {yv[te].mean():.1%})")
    print(f"  precision in top decile : {prec_top:.1%}  "
          f"(lift x{prec_top / max(yv[te].mean(), 1e-9):.1f})")


if __name__ == "__main__":
    main()
