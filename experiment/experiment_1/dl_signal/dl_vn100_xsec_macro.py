"""
VN100 pooled DL signal WITH macro + cross-sectional + index-state features.

Extends dl_vn100_pooled.py (gold.stocks per-stock TA only) by adding the context
features the VN30 unified_* tables carry but gold.stocks doesn't:

  MACRO        economy_* (79) + bonds_* (12) from unified_vcb, joined by date
               (these series are market-wide -> identical for every ticker/date).
  INDEX STATE  full TA of the VN100 and VNINDEX indices (gold_schema.indices),
               joined by date, plus each stock's return RELATIVE to the index.
  CROSS-SECT.  per-date rank/z-score of each stock vs its VN100 peers on return,
               volatility, volume, turnover -> where the stock sits in the pack.

Everything else (label, leakage-guarded date split, per-ticker standardization,
top-K selection, W-day windows, model zoo) is reused unchanged so the AUC is
directly comparable to dl_vn100_pooled_results.csv.

Output: dl_vn100_xsec_macro_results.csv + console.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
import talib
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.ensemble import HistGradientBoostingClassifier

from experiment.experiment_1.dl_signal.dl_vn100_pooled import (
    load_panel, build, select_topk, make_windows,
    MLP, RNN, CNN1D, TransformerNet, train_eval, metrics,
    TOPK, DEVICE, VN100, HERE)

warnings.filterwarnings("ignore")
OUT_CSV = os.path.join(HERE, "dl_vn100_xsec_macro_results.csv")


def conn():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")


def load_macro():
    """economy_* + bonds_* (+cyclical calendar) from unified_vcb, keyed by date."""
    c = conn()
    df = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", c)
    c.close()
    df["date"] = pd.to_datetime(df["date"])
    keep = ["date"] + [col for col in df.columns
                       if col.startswith(("economy_", "bonds_"))
                       or col in ("month_sin", "month_cos", "day_of_week_sin",
                                  "day_of_week_cos", "day_of_year_sin", "day_of_year_cos")]
    return df[keep]


def load_index_state():
    """Full TA of VN100 & VNINDEX, wide by date, prefixed; + index returns."""
    c = conn()
    idx = pd.read_sql(
        "SELECT * FROM gold_schema.indices WHERE ticker IN ('VN100','VNINDEX') "
        "ORDER BY ticker, date", c)
    c.close()
    idx["date"] = pd.to_datetime(idx["date"])
    out = None
    for tk, pre in [("VN100", "idx_vn100_"), ("VNINDEX", "idx_vni_")]:
        g = idx[idx["ticker"] == tk].drop(columns=["exchange", "ticker"]).sort_values("date")
        num = [col for col in g.columns if col != "date" and np.issubdtype(g[col].dtype, np.number)]
        g = g[["date"] + num].rename(columns={col: pre + col for col in num})
        g[f"{pre}ret5"] = g[f"{pre}close"].pct_change(5)
        g[f"{pre}ret20"] = g[f"{pre}close"].pct_change(20)
        out = g if out is None else out.merge(g, on="date", how="outer")
    return out.sort_values("date")


def augment(df):
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    gb = df.groupby("ticker", sort=False)
    c = df["close"].astype(float)
    df["ret5"] = gb["close"].pct_change(5)
    df["ret20"] = gb["close"].pct_change(20)
    df["ret60"] = gb["close"].pct_change(60)
    df["turnover"] = c * df["volume"].astype(float)
    df["volratio"] = df["volume"].astype(float) / gb["volume"].transform(
        lambda s: s.rolling(20, min_periods=5).mean())
    if "natr_14" in df.columns:
        df["natr14x"] = df["natr_14"]
    else:
        df["natr14x"] = gb.apply(lambda g: pd.Series(
            talib.NATR(g["high"].astype(float).values, g["low"].astype(float).values,
                       g["close"].astype(float).values, 14), index=g.index)).reset_index(level=0, drop=True)

    # index state + relative strength
    idx = load_index_state()
    df = df.merge(idx, on="date", how="left")
    df["relstr5"] = df["ret5"] - df["idx_vn100_ret5"]
    df["relstr20"] = df["ret20"] - df["idx_vn100_ret20"]

    # cross-sectional rank / z across the VN100 universe, per date
    xs_base = ["ret5", "ret20", "ret60", "natr14x", "volratio", "turnover", "relstr5", "relstr20"]
    gd = df.groupby("date")
    for col in xs_base:
        df["xs_rank_" + col] = gd[col].rank(pct=True)
    for col in ["ret5", "ret20", "natr14x"]:
        df["xs_z_" + col] = gd[col].transform(lambda s: (s - s.mean()) / (s.std() + 1e-9))

    # macro
    df = df.merge(load_macro(), on="date", how="left")
    return df


def main():
    print(f"device: {DEVICE}")
    df = augment(load_panel())
    print(f"augmented columns: {df.shape[1]} (was ~910 in gold.stocks)")
    X, y, d, tk, cutoff, feat = build(df)
    del df
    print(f"feature matrix: {X.shape[1]} numeric features")

    lab = ~np.isnan(y)
    test_mask = (d.values >= np.datetime64(cutoff)) & lab
    val_cut = pd.Series(d[d.values < np.datetime64(cutoff)]).quantile(0.9)
    train_mask = (d.values < np.datetime64(val_cut)) & lab
    val_mask = (d.values >= np.datetime64(val_cut)) & (d.values < np.datetime64(cutoff)) & lab
    print(f"train {train_mask.sum():,} / val {val_mask.sum():,} / test {test_mask.sum():,} "
          f"| cutoff {pd.Timestamp(cutoff).date()} | test base {y[test_mask].mean():.1%}")

    topk = select_topk(X, y, train_mask)
    new = [c for c in topk if c.startswith(("xs_", "idx_", "relstr", "economy_", "bonds_"))]
    print(f"top-{TOPK}: {', '.join(topk[:12])} ...")
    print(f"  -> {len(new)} of top-{TOPK} are NEW context features: {', '.join(new[:10])}")

    Xf = X.values.astype(np.float32)
    gbm = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
        max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
    gbm.fit(Xf[train_mask | val_mask], y[train_mask | val_mask].astype(int))
    yte = y[test_mask].astype(int)
    rows = [metrics("GBM-full (baseline)", yte, gbm.predict_proba(Xf[test_mask])[:, 1])]
    print(f"  GBM-full     test_auc={rows[0]['test_auc']}")
    del Xf

    seq = make_windows(X[topk], tk)
    Xtr, ytr = seq[train_mask], y[train_mask].astype(np.float32)
    Xva, yva = seq[val_mask], y[val_mask].astype(int)
    Xte = seq[test_mask]
    pos_weight = (ytr == 0).sum() / max((ytr == 1).sum(), 1)

    dl_probs = []
    for name, build_m in {"MLP": lambda: MLP(TOPK), "LSTM": lambda: RNN(TOPK, "lstm"),
                          "GRU": lambda: RNN(TOPK, "gru"), "CNN1D": lambda: CNN1D(TOPK),
                          "Transformer": lambda: TransformerNet(TOPK)}.items():
        import torch
        torch.manual_seed(0)
        p = train_eval(build_m(), Xtr, ytr, Xva, yva, Xte, pos_weight)
        dl_probs.append(p); rows.append(metrics(name, yte, p))
        print(f"  {name:12s} test_auc={rows[-1]['test_auc']}")

    rows.append(metrics("Ensemble (DL mean)", yte, np.mean(dl_probs, axis=0)))
    res = pd.DataFrame(rows).sort_values("test_auc", ascending=False).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)
    print(f"\nSaved -> {os.path.basename(OUT_CSV)}\n")
    print(res.to_string(index=False))


if __name__ == "__main__":
    main()
