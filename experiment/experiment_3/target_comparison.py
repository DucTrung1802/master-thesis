"""
Experiment 3, Part C - which short-horizon TARGET is actually tradable?

experiment_3 showed the "5d >= +5%" binary is a volatility proxy: it ranks stocks
by how much they move, not whether they OUTPERFORM -> not tradable. Here we compare
several 1-2 week target definitions on the pooled VN30 panel and ask, for each:
  * predictive skill  -> daily cross-sectional rank-IC (Spearman of pred vs realised)
  * tradability       -> LONG-ONLY top-6 portfolio (VN allows no easy shorting),
                         daily rebalanced, net 15 bps, vs the VN30 equal-weight market.

Candidate targets (per stock/day):
  ret5    raw forward 5-day return
  ret10   raw forward 10-day return
  rel5    market-RELATIVE 5d return  (fwd5 - cross-sectional mean that day)
  rel10   market-relative 10d return
  volscaled5  fwd5 / trailing-20d volatility   (a Sharpe-like target)
  bin5    forward 5d >= +5%  (the old baseline, classifier)

Walk-forward (purged, retrained every STEP days); gradient boosting reg/clf on a
fixed top-feature subset (for speed). Output: target_comparison_results.csv.
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier

warnings.filterwarnings("ignore")
H5, H10, GAIN = 5, 10, 0.05
STEP_DAYS, MIN_TRAIN_ROWS, PURGE_CAL = 378, 12000, 16
N_LONG, COST_SIDE, ANN = 6, 0.0015, 252
TOPFEAT = 80
HERE = os.path.dirname(os.path.abspath(__file__))
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
        f5 = np.full(len(c), np.nan); f5[:-H5] = c[H5:] / c[:-H5] - 1
        f10 = np.full(len(c), np.nan); f10[:-H10] = c[H10:] / c[:-H10] - 1
        g["fwd5"], g["fwd10"] = f5, f10
        g["vol20"] = pd.Series(g["ret"].values).rolling(20, min_periods=5).std().values
        parts.append(g)
    d = pd.concat(parts, ignore_index=True)
    d["rel5"] = d["fwd5"] - d.groupby("date")["fwd5"].transform("mean")
    d["rel10"] = d["fwd10"] - d.groupby("date")["fwd10"].transform("mean")
    d["volscaled5"] = d["fwd5"] / (d["vol20"] + 1e-6)
    d["bin5"] = (d["fwd5"] >= GAIN).astype(float)
    d.loc[d["fwd5"].isna(), "bin5"] = np.nan
    return d.sort_values("date").reset_index(drop=True), feat


def walk_forward(d, feat, target, binary):
    dates = d["date"].values
    uniq = np.array(sorted(pd.unique(dates)))
    starts = uniq[MIN_TRAIN_ROWS // len(VN30)::STEP_DAYS]
    oos = np.full(len(d), np.nan)
    tcol = d[target].values
    for i, fs in enumerate(starts):
        fe = starts[i + 1] if i + 1 < len(starts) else uniq[-1] + np.timedelta64(1, "D")
        tr = (dates < fs - np.timedelta64(PURGE_CAL, "D")) & ~np.isnan(tcol)
        te = (dates >= fs) & (dates < fe)
        if tr.sum() < MIN_TRAIN_ROWS or te.sum() == 0:
            continue
        M = (HistGradientBoostingClassifier if binary else HistGradientBoostingRegressor)(
            max_iter=150, learning_rate=0.05, max_leaf_nodes=31,
            l2_regularization=1.0, random_state=0)
        M.fit(d.loc[tr, feat], (tcol[tr].astype(int) if binary else tcol[tr]))
        oos[te] = (M.predict_proba(d.loc[te, feat])[:, 1] if binary else M.predict(d.loc[te, feat]))
    return oos


def evaluate(d, pred, horizon_fwd):
    """rank-IC vs realised fwd return + long-only top-N daily-rebalanced backtest."""
    t = d.assign(pred=pred).dropna(subset=["pred", horizon_fwd])
    ic = t.groupby("date").apply(
        lambda g: spearmanr(g["pred"], g[horizon_fwd]).correlation if len(g) > 4 else np.nan).mean()

    pw = t.pivot(index="date", columns="ticker", values="pred").sort_index()
    rw = d.pivot(index="date", columns="ticker", values="ret").reindex(pw.index)
    nxt = rw.shift(-1)
    port, mkt, prev = [], [], None
    for dt in pw.index[:-1]:
        nr = nxt.loc[dt]; p = pw.loc[dt].dropna()
        p = p.loc[[x for x in p.index if pd.notna(nr.get(x))]]
        mkt.append(np.nanmean(nr.values) if nr.notna().any() else 0.0)
        if len(p) < N_LONG:
            port.append(0.0); continue
        longs = p.nlargest(N_LONG).index
        w = pd.Series(0.0, index=pw.columns); w[longs] = 1.0 / N_LONG
        turn = float(np.abs(w - (prev if prev is not None else 0.0)).sum())
        port.append(nr[longs].mean() - turn * COST_SIDE); prev = w

    def stats(r):
        r = np.asarray(r); eq = np.cumprod(1 + r); yrs = len(r) / ANN
        return eq[-1] ** (1 / yrs) - 1, r.mean() / (r.std() + 1e-12) * np.sqrt(ANN)
    p_cagr, p_sh = stats(port); m_cagr, m_sh = stats(mkt)
    return ic, p_cagr, p_sh, m_sh, p_sh - m_sh


def main():
    print("Loading VN30 panel ...")
    d, feat = prep(load())
    # fixed top-feature subset (corr with rel5 on an early slice) for speed
    early = d[d["date"] < d["date"].quantile(0.4)].dropna(subset=["rel5"])
    corr = early[feat].apply(lambda s: np.abs(np.corrcoef(
        s.fillna(s.median()), early["rel5"])[0, 1]) if s.std() > 0 else 0)
    feat = list(corr.sort_values(ascending=False).head(TOPFEAT).index)
    print(f"  {len(d):,} stock-days | using top-{len(feat)} features\n")

    # (display name, target column to fit, realised fwd column for IC, binary?)
    specs = [("ret5", "fwd5", "fwd5", False), ("ret10", "fwd10", "fwd10", False),
             ("rel5", "rel5", "fwd5", False), ("rel10", "rel10", "fwd10", False),
             ("volscaled5", "volscaled5", "fwd5", False), ("bin5", "bin5", "fwd5", True)]
    rows = []
    for name, tcol, fwd, binary in specs:
        oos = walk_forward(d, feat, tcol, binary)
        ic, pc, ps, ms, ex = evaluate(d, oos, fwd)
        rows.append({"target": name, "rank_IC": round(ic, 4),
                     "top6_CAGR": round(pc, 4), "top6_Sharpe": round(ps, 3),
                     "market_Sharpe": round(ms, 3), "excess_Sharpe": round(ex, 3)})
        print(f"  {name:11s} IC={ic:+.3f}  top6 CAGR={pc:+.1%} Sharpe={ps:.2f}  "
              f"excess={ex:+.2f}")

    res = pd.DataFrame(rows).sort_values("excess_Sharpe", ascending=False)
    res.to_csv(os.path.join(HERE, "target_comparison_results.csv"), index=False)
    print("\n=== Target comparison (long-only top-6 vs VN30 market, net 15bps) ===")
    print(res.to_string(index=False))
    best = res.iloc[0]
    print(f"\nBest tradable target: {best['target']} "
          f"(IC {best['rank_IC']:+.3f}, excess Sharpe {best['excess_Sharpe']:+.2f})")


if __name__ == "__main__":
    main()
