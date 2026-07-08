"""
Visualisations for the best VCB model (point-in-time GBM, test ROC-AUC ~= 0.77)
on its held-out TEST set. Reproduces the model (unified_vcb, all features, label
next-5d >= +5%, chronological 80/20 split) and writes PNGs to ./figures/:

  1_roc_curve.png            ROC curve with AUC
  2_pr_curve.png             precision-recall curve vs base rate
  3_prob_distribution.png    predicted prob by true class (separation)
  4_return_by_decile.png     mean forward-5d return by predicted-prob decile
  5_price_and_signals.png    VCB price over test period + actual events + top-decile signals
  6_probability_timeline.png predicted probability over test time, positives shaded
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
import matplotlib.dates as mdates
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_curve, roc_auc_score, precision_recall_curve, average_precision_score

warnings.filterwarnings("ignore")
HORIZON, GAIN = 5, 0.05
HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures")
os.makedirs(FIG, exist_ok=True)
ID_COLS = {"exchange", "ticker", "date", "target"}


def main():
    load_dotenv(os.path.join(HERE, "..", "..", "..", ".env"))
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2")
    df = pd.read_sql("SELECT * FROM unified_schema.unified_vcb ORDER BY date", conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])

    close = df["close"].astype(float).values
    fwd = np.full(len(close), np.nan)
    fwd[:-HORIZON] = close[HORIZON:] / close[:-HORIZON] - 1.0
    y = (fwd >= GAIN).astype(float); y[np.isnan(fwd)] = np.nan

    feat = [c for c in df.columns if c not in ID_COLS]
    X = df[feat].copy()
    for c in feat:
        if X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.select_dtypes(include=[np.number]).astype(float)

    m = ~np.isnan(y)
    Xv = X[m].reset_index(drop=True)
    yv = y[m].astype(int)
    dates = df["date"].values[m]
    closes = close[m]
    fwdv = fwd[m]

    split = int(len(Xv) * 0.80)
    clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
        max_leaf_nodes=31, l2_regularization=1.0, random_state=0)
    clf.fit(Xv.iloc[:split], yv[:split])
    p = clf.predict_proba(Xv.iloc[split:])[:, 1]

    yte = yv[split:]; dte = pd.to_datetime(dates[split:]); cte = closes[split:]; fte = fwdv[split:]
    auc = roc_auc_score(yte, p); ap = average_precision_score(yte, p)
    base = yte.mean()
    thr = np.quantile(p, 0.90)                     # top-decile signal threshold
    print(f"VCB test: n={len(yte)} | base {base:.1%} | ROC-AUC {auc:.3f} | PR-AUC {ap:.3f}")

    GREEN, ORANGE, GREY = "#2ca02c", "#ff7f0e", "#888888"

    # 1. ROC
    fpr, tpr, _ = roc_curve(yte, p)
    plt.figure(figsize=(6, 6))
    plt.plot(fpr, tpr, color=ORANGE, lw=2, label=f"GBM (AUC = {auc:.3f})")
    plt.plot([0, 1], [0, 1], "--", color=GREY, label="chance")
    plt.xlabel("False positive rate"); plt.ylabel("True positive rate")
    plt.title("VCB test ROC — next-5d ≥ +5%"); plt.legend(loc="lower right"); plt.grid(alpha=.3)
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "1_roc_curve.png"), dpi=130); plt.close()

    # 2. PR
    prec, rec, _ = precision_recall_curve(yte, p)
    plt.figure(figsize=(6, 6))
    plt.plot(rec, prec, color=ORANGE, lw=2, label=f"GBM (PR-AUC = {ap:.3f})")
    plt.axhline(base, ls="--", color=GREY, label=f"base rate = {base:.1%}")
    plt.xlabel("Recall"); plt.ylabel("Precision")
    plt.title("VCB test Precision–Recall"); plt.legend(loc="upper right"); plt.grid(alpha=.3)
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "2_pr_curve.png"), dpi=130); plt.close()

    # 3. predicted prob by true class
    plt.figure(figsize=(7, 5))
    bins = np.linspace(0, max(p.max(), 0.5), 30)
    plt.hist(p[yte == 0], bins=bins, alpha=.6, color=GREY, density=True, label="no move (y=0)")
    plt.hist(p[yte == 1], bins=bins, alpha=.6, color=GREEN, density=True, label="≥+5% move (y=1)")
    plt.axvline(thr, ls="--", color=ORANGE, label="top-decile threshold")
    plt.xlabel("Predicted P(next-5d ≥ +5%)"); plt.ylabel("density")
    plt.title("VCB test — predicted probability by true class"); plt.legend(); plt.grid(alpha=.3)
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "3_prob_distribution.png"), dpi=130); plt.close()

    # 4. mean forward-5d return by predicted-prob decile
    dec = pd.qcut(p, 10, labels=False, duplicates="drop")
    g = pd.DataFrame({"dec": dec, "fwd": fte}).groupby("dec")["fwd"].mean() * 100
    plt.figure(figsize=(7, 5))
    colors = [GREEN if v > 0 else "#d62728" for v in g.values]
    plt.bar(g.index + 1, g.values, color=colors)
    plt.axhline(fte.mean() * 100, ls="--", color=GREY, label=f"all-days mean = {fte.mean()*100:+.2f}%")
    plt.xlabel("Predicted-probability decile (10 = highest signal)")
    plt.ylabel("mean forward-5d return (%)")
    plt.title("VCB test — actual forward-5d return by signal decile")
    plt.xticks(range(1, 11)); plt.legend(); plt.grid(alpha=.3, axis="y")
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "4_return_by_decile.png"), dpi=130); plt.close()

    # 5. price + actual events + top-decile signals
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dte, cte, color="#1f77b4", lw=1.2, label="VCB close")
    ev = yte == 1
    ax.scatter(dte[ev], cte[ev], s=18, color=GREEN, alpha=.7, label="actual ≥+5% launch day", zorder=3)
    sig = p >= thr
    ax.scatter(dte[sig], cte[sig], s=60, facecolors="none", edgecolors=ORANGE, lw=1.4,
               label="model top-decile signal", zorder=4)
    ax.set_title(f"VCB test set — price, actual events & model signals  (ROC-AUC {auc:.3f})")
    ax.set_ylabel("close"); ax.legend(loc="upper left"); ax.grid(alpha=.3)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "5_price_and_signals.png"), dpi=130); plt.close()

    # 6. probability timeline with positives shaded
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(dte, p, color=ORANGE, lw=1.1, label="predicted probability")
    ax.axhline(thr, ls="--", color=GREY, label="top-decile threshold")
    ax.fill_between(dte, 0, p.max() * 1.05, where=ev, color=GREEN, alpha=.18,
                    label="actual ≥+5% day")
    ax.set_ylim(0, p.max() * 1.05)
    ax.set_title("VCB test — predicted probability over time (green = actual move days)")
    ax.set_ylabel("P(next-5d ≥ +5%)"); ax.legend(loc="upper left"); ax.grid(alpha=.3)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    plt.tight_layout(); plt.savefig(os.path.join(FIG, "6_probability_timeline.png"), dpi=130); plt.close()

    print("Saved 6 PNGs ->", FIG)
    for f in sorted(os.listdir(FIG)):
        print("  ", f)


if __name__ == "__main__":
    main()
