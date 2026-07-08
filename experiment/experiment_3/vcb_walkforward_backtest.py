"""
Experiment 3, Part A - VCB walk-forward COSTED backtest.

Turns the 0.77-AUC signal into an actual trading rule and tests whether it
survives walk-forward out-of-sample evaluation and Vietnamese trading costs.

Walk-forward (no look-ahead, purged):
  * expanding train window, retrained every STEP days
  * label horizon overlap purged (drop last HORIZON train rows before each test)
  * per-fold signal threshold = 90th percentile of the fold's TRAIN probabilities
  * out-of-sample probabilities stitched across the whole 2012-2026 history

Strategy (long / flat, 5-day hold):
  enter long for the next 5 trading days whenever the model fires a top-decile
  signal; daily return = position * daily_return - cost on every position change.

Benchmarks on the same OOS window:
  * Buy & hold VCB
  * Momentum rule: long while 20-day momentum > 0 (a plain trend baseline)

Costs charged per side (tax + fees + slippage); a sensitivity table is printed.
Outputs: figures/A_equity_curve.png, vcb_backtest_metrics.csv.
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
MIN_TRAIN, STEP, PURGE = 750, 126, HORIZON
HOLD = 5                       # trading days held per signal
TOP_Q = 0.90                   # top-decile signal
COST_SIDES = [0.0005, 0.0015, 0.0030]   # per-side cost levels (5 / 15 / 30 bps)
BASE_COST = 0.0015
ANN = 252
HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures"); os.makedirs(FIG, exist_ok=True)
ID_COLS = {"exchange", "ticker", "date", "target"}


def load():
    load_dotenv(os.path.join(HERE, "..", "..", ".env"))
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def walk_forward(X, y):
    n = len(X)
    oos_p = np.full(n, np.nan)
    start = MIN_TRAIN
    folds = 0
    while start < n:
        tr = np.where(~np.isnan(y))[0]
        tr = tr[tr < start - PURGE]
        if len(tr) < 300:
            start += STEP; continue
        clf = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.05,
            max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
        clf.fit(X.iloc[tr], y[tr].astype(int))
        te = np.arange(start, min(start + STEP, n))
        oos_p[te] = clf.predict_proba(X.iloc[te])[:, 1]
        start += STEP; folds += 1
    return oos_p, folds


def causal_top_signal(p):
    """Top-decile signal vs a CAUSAL trailing-252d 90th percentile (no look-ahead)."""
    sig = np.zeros(len(p), bool)
    for i in range(len(p)):
        if i >= 60 and not np.isnan(p[i]):
            t = np.nanquantile(p[max(0, i - 252):i], TOP_Q)
            sig[i] = p[i] >= t
    return sig


def hold_position(signal):
    """1 while within HOLD days after a signal (entered next day)."""
    pos = np.zeros(len(signal))
    hold = 0
    for d in range(len(signal)):
        if hold > 0:
            pos[d] = 1; hold -= 1
        if signal[d]:                       # decided at close d -> hold next HOLD days
            hold = HOLD
    return pos


def metrics(name, pos, ret, cost_side):
    pos = np.asarray(pos, float)
    turn = np.abs(np.diff(np.concatenate([[0], pos])))
    strat = pos * ret - turn * cost_side
    eq = np.cumprod(1 + strat)
    yrs = len(strat) / ANN
    cagr = eq[-1] ** (1 / yrs) - 1
    sharpe = strat.mean() / (strat.std() + 1e-12) * np.sqrt(ANN)
    dd = (eq / np.maximum.accumulate(eq) - 1).min()
    trades = int(turn.sum())            # entries+exits
    return {"strategy": name, "total_return": eq[-1] - 1, "CAGR": cagr,
            "ann_vol": strat.std() * np.sqrt(ANN), "Sharpe": sharpe,
            "max_drawdown": dd, "pct_in_market": pos.mean(), "trades": trades}, eq, strat


def main():
    df = load()
    close = df["close"].astype(float).values
    ret = np.concatenate([[0], close[1:] / close[:-1] - 1])
    y = np.full(len(close), np.nan)
    y[:-HORIZON] = (close[HORIZON:] / close[:-HORIZON] - 1.0 >= GAIN).astype(float)

    feat = [c for c in df.columns if c not in ID_COLS]
    X = df[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float)

    print("Walk-forward (expanding, retrain every %d days)..." % STEP)
    oos_p, folds = walk_forward(X, y)
    oos = ~np.isnan(oos_p)
    oos[-HORIZON:] = False                      # no realized return at the very end
    sl = slice(int(np.argmax(oos)), len(oos))   # OOS span
    d = df["date"].values[sl]
    r = ret[sl]
    sig = causal_top_signal(oos_p[sl]) & oos[sl]
    # momentum baseline, position decided at PRIOR close (1-day lag -> no look-ahead)
    mom_full = np.zeros(len(close), bool)
    mom_full[21:] = close[20:-1] > close[:-21]   # close[d-1] > close[d-21]
    mom = mom_full[sl]
    print(f"  folds={folds} | OOS {pd.Timestamp(d[0]).date()}..{pd.Timestamp(d[-1]).date()} "
          f"({len(d)} days) | signal days {int(sig.sum())} ({sig.mean():.1%})\n")

    pos_ml = hold_position(sig)
    pos_mom = mom.astype(float)
    pos_bh = np.ones(len(r))

    # ── cost sensitivity ──────────────────────────────────────────────────────
    print("=== Net Sharpe / CAGR by per-side cost ===")
    print(f"{'cost/side':>10} | {'ML long-flat':>22} | {'Momentum':>18} | {'Buy&Hold':>12}")
    for cs in COST_SIDES:
        m_ml, _, _ = metrics("ML", pos_ml, r, cs)
        m_mo, _, _ = metrics("MOM", pos_mom, r, cs)
        m_bh, _, _ = metrics("BH", pos_bh, r, 0.0)
        print(f"{cs*1e4:7.0f}bps | Sharpe {m_ml['Sharpe']:.2f} CAGR {m_ml['CAGR']:+.1%}"
              f"  | Sharpe {m_mo['Sharpe']:.2f} {m_mo['CAGR']:+.1%}"
              f" | Sharpe {m_bh['Sharpe']:.2f} {m_bh['CAGR']:+.1%}")

    # ── full metrics at base cost ─────────────────────────────────────────────
    rows, eqs = [], {}
    for name, pos, cs in [("ML long-flat (top-decile, 5d hold)", pos_ml, BASE_COST),
                          ("Momentum (20d>0)", pos_mom, BASE_COST),
                          ("Buy & Hold", pos_bh, 0.0)]:
        m, eq, _ = metrics(name, pos, r, cs)
        rows.append(m); eqs[name] = eq
    res = pd.DataFrame(rows)
    res.to_csv(os.path.join(HERE, "vcb_backtest_metrics.csv"), index=False)
    print(f"\n=== Full metrics (base cost {BASE_COST*1e4:.0f} bps/side, OOS) ===")
    show = res.copy()
    for c in ["total_return", "CAGR", "ann_vol", "max_drawdown", "pct_in_market"]:
        show[c] = (show[c] * 100).round(1).astype(str) + "%"
    show["Sharpe"] = show["Sharpe"].round(2)
    print(show.to_string(index=False))

    # ── equity curve ──────────────────────────────────────────────────────────
    dts = pd.to_datetime(d)
    plt.figure(figsize=(12, 6))
    colors = {"ML long-flat (top-decile, 5d hold)": "#ff7f0e",
              "Momentum (20d>0)": "#2ca02c", "Buy & Hold": "#1f77b4"}
    for name, eq in eqs.items():
        plt.plot(dts, eq, label=f"{name}", color=colors[name], lw=1.6)
    plt.yscale("log")
    plt.ylabel("growth of 1 (log scale)")
    plt.title(f"VCB walk-forward OOS equity — net of {BASE_COST*1e4:.0f} bps/side")
    plt.legend(loc="upper left"); plt.grid(alpha=.3, which="both")
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "A_equity_curve.png"), dpi=130); plt.close()
    print(f"\nSaved -> figures/A_equity_curve.png, vcb_backtest_metrics.csv")


if __name__ == "__main__":
    main()
