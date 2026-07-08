"""
Experiment 3, Part B - cross-sectional VN30 long-short walk-forward backtest.

Instead of timing one stock, rank the VN30 each day by the model's predicted
P(next-5d >= +5%) and trade the spread: long the top names, short the bottom.
This is the framing the thesis favours (relative/cross-sectional return is far
more tradable than a single stock's absolute return).

Walk-forward (pooled, purged, retrained yearly):
  * train one model on all VN30 stock-days before the fold date (label overlap
    purged), predict the next year of stock-days -> stitched OOS probabilities.
Strategy (daily rebalanced, dollar-neutral):
  * long the top N_SIDE stocks, short the bottom N_SIDE by predicted prob
  * spread return = mean(long next-day ret) - mean(short next-day ret) - costs
Benchmark: VN30 equal-weight long-only ("the market").

Outputs: figures/B_longshort_equity.png, vn30_longshort_metrics.csv.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.ensemble import HistGradientBoostingClassifier

warnings.filterwarnings("ignore")
HORIZON, GAIN = 5, 0.05
STEP_DAYS = 252                 # retrain cadence (~1y)
MIN_TRAIN_ROWS = 8000
PURGE_CAL_DAYS = 9              # ~5 trading days
N_SIDE = 6                      # long top 6 / short bottom 6 of 30
COST_SIDE = 0.0015
ANN = 252
HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures"); os.makedirs(FIG, exist_ok=True)
ID_COLS = {"exchange", "ticker", "date", "target"}

VN30 = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG","MBB","MSN",
        "MWG","PLX","POW","SAB","SHB","SSB","SSI","STB","TCB","TPB","VCB","VHM",
        "VIB","VIC","VJC","VNM","VPB","VRE"]


def load():
    load_dotenv(os.path.join(HERE, "..", "..", ".env"))
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT * FROM gold_schema.stocks WHERE exchange='HOSE' "
                     "AND ticker = ANY(%s) ORDER BY ticker, date", conn, params=(VN30,))
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def prep(df):
    feat = [c for c in df.columns if c not in ID_COLS]
    for c in feat:
        if df[c].dtype == bool:
            df[c] = df[c].astype(float)
    feat = [c for c in feat if np.issubdtype(df[c].dtype, np.number)]
    parts = []
    for tk, g in df.groupby("ticker", sort=False):
        g = g.sort_values("date").copy()
        c = g["close"].astype(float).values
        g["ret"] = np.concatenate([[0], c[1:] / c[:-1] - 1])
        yy = np.full(len(c), np.nan)
        yy[:-HORIZON] = (c[HORIZON:] / c[:-HORIZON] - 1.0 >= GAIN).astype(float)
        g["y"] = yy
        parts.append(g)
    out = pd.concat(parts, ignore_index=True)
    return out, feat


def walk_forward(df, feat):
    df = df.sort_values("date").reset_index(drop=True)
    dates = df["date"].values
    uniq = np.array(sorted(pd.unique(dates)))
    oos_p = np.full(len(df), np.nan)
    # fold start dates: every STEP_DAYS trading days once we have enough history
    starts = uniq[MIN_TRAIN_ROWS // len(VN30)::STEP_DAYS]
    folds = 0
    for i, fs in enumerate(starts):
        fe = starts[i + 1] if i + 1 < len(starts) else uniq[-1] + np.timedelta64(1, "D")
        purge = fs - np.timedelta64(PURGE_CAL_DAYS, "D")
        tr = (dates < purge) & (~np.isnan(df["y"].values))
        if tr.sum() < MIN_TRAIN_ROWS:
            continue
        te = (dates >= fs) & (dates < fe)
        if te.sum() == 0:
            continue
        clf = HistGradientBoostingClassifier(max_iter=150, learning_rate=0.05,
            max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
        clf.fit(df.loc[tr, feat], df.loc[tr, "y"].astype(int))
        oos_p[te] = clf.predict_proba(df.loc[te, feat])[:, 1]
        folds += 1
    df["p"] = oos_p
    return df, folds


def perf(name, ret):
    ret = np.asarray(ret, float)
    eq = np.cumprod(1 + ret)
    yrs = len(ret) / ANN
    return {"strategy": name, "total_return": eq[-1] - 1, "CAGR": eq[-1] ** (1/yrs) - 1,
            "ann_vol": ret.std() * np.sqrt(ANN),
            "Sharpe": ret.mean() / (ret.std() + 1e-12) * np.sqrt(ANN),
            "max_drawdown": (eq / np.maximum.accumulate(eq) - 1).min()}, eq


def main():
    print("Loading VN30 panel from gold.stocks ...")
    df, feat = prep(load())
    print(f"  {len(df):,} stock-days, {len(feat)} features. Walk-forward ...")
    df, folds = walk_forward(df, feat)

    oos = df.dropna(subset=["p"]).copy()
    p_w = oos.pivot(index="date", columns="ticker", values="p").sort_index()
    r_w = oos.pivot(index="date", columns="ticker", values="ret").sort_index()
    nextret = r_w.shift(-1)                       # tomorrow's return
    print(f"  folds={folds} | OOS {p_w.index[0].date()}..{p_w.index[-1].date()} "
          f"({len(p_w)} days, avg {p_w.notna().sum(1).mean():.0f} names/day)\n")

    ls_gross, mkt, w_prev = [], [], None
    cols = p_w.columns
    for dt in p_w.index[:-1]:
        nr = nextret.loc[dt]
        p = p_w.loc[dt].dropna()
        p = p.loc[[t for t in p.index if pd.notna(nr.get(t))]]   # need a valid next return
        mkt.append(np.nanmean(nr.values) if nr.notna().any() else 0.0)
        if len(p) < 2 * N_SIDE:
            ls_gross.append(0.0); continue
        longs = p.nlargest(N_SIDE).index
        shorts = p.nsmallest(N_SIDE).index
        w = pd.Series(0.0, index=cols)
        w[longs] = 1.0 / N_SIDE
        w[shorts] = -1.0 / N_SIDE
        gross = nr[longs].mean() - nr[shorts].mean()
        turn = float(np.abs(w - (w_prev if w_prev is not None else 0.0)).sum())
        ls_gross.append(gross - turn * COST_SIDE)
        w_prev = w

    m_ls, eq_ls = perf(f"VN30 long-short (top/bot {N_SIDE}, net {COST_SIDE*1e4:.0f}bps)", ls_gross)
    m_mk, eq_mk = perf("VN30 equal-weight (market)", mkt)
    res = pd.DataFrame([m_ls, m_mk])
    res.to_csv(os.path.join(HERE, "vn30_longshort_metrics.csv"), index=False)

    show = res.copy()
    for c in ["total_return", "CAGR", "ann_vol", "max_drawdown"]:
        show[c] = (show[c] * 100).round(1).astype(str) + "%"
    show["Sharpe"] = show["Sharpe"].round(2)
    print("=== Cross-sectional VN30 results (OOS) ===")
    print(show.to_string(index=False))

    dts = p_w.index[:-1]
    plt.figure(figsize=(12, 6))
    plt.plot(dts, eq_ls, color="#ff7f0e", lw=1.6, label=m_ls["strategy"])
    plt.plot(dts, eq_mk, color="#1f77b4", lw=1.6, label="VN30 equal-weight (market)")
    plt.yscale("log"); plt.ylabel("growth of 1 (log)")
    plt.title("VN30 cross-sectional long-short vs market — walk-forward OOS")
    plt.legend(loc="upper left"); plt.grid(alpha=.3, which="both")
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "B_longshort_equity.png"), dpi=130); plt.close()
    print("\nSaved -> figures/B_longshort_equity.png, vn30_longshort_metrics.csv")


if __name__ == "__main__":
    main()
