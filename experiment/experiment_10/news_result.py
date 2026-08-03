"""news_result.py — does an ARTICLE'S TEXT predict what the price did next?

Input  : `headline` + `content` of one Vietnamese news article (silver.cafef_news)
Output : the article's effect on the price — the MARKET-EXCESS forward return over
         h ∈ {1, 5, 10} trading sessions, bucketed into 5 ordered levels.

This is the **paper 63 branch**, kept deliberately separate from the forecasting
pipeline. Souma, Vodenska & Aoyama (2019) ran exactly this task on 375,367 Thomson
Reuters articles with millisecond tick labels, reached **97.5% training accuracy**, and
landed at **50.4% out-of-sample on a balanced binary target**. They reported both numbers
without spin, and that honesty is the only reason the paper is citable. This script is
built to produce the same kind of report on VN data.

⚠️ **What this is NOT.** The label comes from the price, so a model that learns it is a
distilled return predictor, not an opinion measure. Feeding its output forward as a
feature to predict returns is paper 46's circularity, which `experiment_10/CONTEXT.md`
records as disqualifying. Framed as "predict the reaction" — as paper 63 does, and as
this file does — the task is legitimate and the result is publishable either way.

Design choices and the paper behind each:

| choice | why |
|---|---|
| **market-EXCESS** return, not raw | raw return makes every article "positive" in a rising market — paper 49's recall-1.00 pathology and paper 58's accuracy-rises-with-horizon |
| **quintile** bands, not fixed thresholds | base rate 20% per class BY CONSTRUCTION (paper 53); paper 56 proves fixed bands let you drive accuracy to 1.00 by widening the neutral one |
| **editorials only** by default | the other 327k rows are filing stubs averaging 288 chars; a model separating those from journalism would score well and mean nothing |
| **purged + embargoed walk-forward** | the label reads h sessions ahead |
| **QWK beside macro-F1** | the 5 levels are ORDERED — confusing level 4 with 3 is not the same error as with 0 |
| **a shuffled-label control** | the only way to see what this pipeline scores on noise |

Run:
    python experiment/experiment_10/news_result.py                 # h=1,5,10, editorials
    python experiment/experiment_10/news_result.py --quick         # 8k articles, fast
    python experiment/experiment_10/news_result.py --include-disclosures
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
os.chdir(REPO)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import cohen_kappa_score, confusion_matrix, f1_score

OUT = Path(__file__).resolve().parent
CACHE = OUT / "_cache"
CACHE.mkdir(exist_ok=True)

# ── palette (dataviz skill reference instance) ───────────────────────────────────────
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
SEQ_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
DIVERGE_NEG, DIVERGE_MID, DIVERGE_POS = "#e34948", "#f0efec", "#2a78d6"

LEVELS = ["rất tiêu cực", "tiêu cực", "trung tính", "tích cực", "rất tích cực"]
HORIZONS = (1, 5, 10)

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "axes.edgecolor": BASELINE,
        "axes.labelcolor": INK_2,
        "axes.titlecolor": INK,
        "text.color": INK,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "font.family": "sans-serif",
        "font.sans-serif": ["Segoe UI", "DejaVu Sans"],
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


# ── data ─────────────────────────────────────────────────────────────────────────────


def _conn():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname="database_main_v2",
    )


def load_frames(include_disclosures: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """News (with text) + the daily price panel. Cached — embedding is the slow part but
    a 2.4M-row price read is not free either."""
    news_cache = CACHE / f"news_{'all' if include_disclosures else 'ed'}.parquet"
    px_cache = CACHE / "prices.parquet"

    if news_cache.exists() and px_cache.exists():
        return pd.read_parquet(news_cache), pd.read_parquet(px_cache)

    with _conn() as c, c.cursor() as cur:
        where = "" if include_disclosures else "WHERE is_editorial"
        cur.execute(
            f"""SELECT row_id, exchange, ticker, trading_date, category, is_editorial,
                       headline, content_clean, relevance_score::double precision, url
                FROM silver_schema.cafef_news {where}"""
        )
        news = pd.DataFrame(
            cur.fetchall(),
            columns=["row_id", "exchange", "ticker", "trading_date", "category",
                     "is_editorial", "headline", "content", "relevance", "url"],
        )
        cur.execute(
            """SELECT ticker, date, close_adjust::double precision
               FROM silver_schema.stocks_basic
               WHERE close_adjust IS NOT NULL AND date >= '2012-06-01'
               ORDER BY ticker, date"""
        )
        px = pd.DataFrame(cur.fetchall(), columns=["ticker", "date", "close"])

    news["trading_date"] = pd.to_datetime(news["trading_date"])
    px["date"] = pd.to_datetime(px["date"])
    news.to_parquet(news_cache)
    px.to_parquet(px_cache)
    return news, px


def build_reactions(
    news: pd.DataFrame, px: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attach the market-excess forward return at each horizon, then quintile it.

    ⚠️ The benchmark is the **equal-weight cross-section of every stock trading that day**,
    so a market-wide move cancels. Without it, "the article was positive" and "the whole
    market rose that week" are the same number — which is how papers 49 and 58 ended up
    with a model that predicts "up" for nearly everything.
    """
    px = px.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = px.groupby("ticker", sort=False)["close"]
    for h in HORIZONS:
        px[f"fwd{h}"] = g.shift(-h) / px["close"] - 1.0
        # market benchmark for that date, over every stock that has the same window
        px[f"bench{h}"] = px.groupby("date")[f"fwd{h}"].transform("mean")
        px[f"exc{h}"] = px[f"fwd{h}"] - px[f"bench{h}"]

    keep = ["ticker", "date"] + [f"exc{h}" for h in HORIZONS] + [f"fwd{h}" for h in HORIZONS]
    df = news.merge(
        px[keep], left_on=["ticker", "trading_date"], right_on=["ticker", "date"], how="inner"
    ).drop(columns=["date"])

    # ── ⚠️ price-sanity screen, and it is NOT optional ───────────────────────────────
    #
    # `close_adjust` carries broken adjustment factors. Worked example, found by reading
    # the very first row of the examples file this script writes:
    #
    #   BNA 2021-10-06  close_raw 67,900  close_adjust 11,320
    #   BNA 2021-10-07  close_raw 38,700  close_adjust 23,950   ← raw −43%, adjusted +112%
    #
    # The raw series takes a bonus issue correctly; the adjusted series jumps instead of
    # staying continuous. Panel-wide, 1,002 rows show a 5-session move above +61% — more
    # than HNX's ±10% band allows even with five consecutive ceilings.
    #
    # It is only 0.04% of rows, which is exactly why it matters: they all sit in the TAILS,
    # and the tails are where the "very positive"/"very negative" classes are defined. Left
    # in, they would set the quintile cut points off broken numbers.
    from sentiment.price_reaction_labels import exchange_limit

    lim = df["exchange"].map(lambda e: exchange_limit(str(e)))
    sane = pd.Series(True, index=df.index)
    for h in HORIZONS:
        ceiling = (1 + lim) ** h - 1
        sane &= df[f"fwd{h}"].abs() <= ceiling * 1.05  # 5% slack for the benchmark leg
    n_insane = int((~sane).sum())
    df = df[sane].copy()

    # ⚠️ Quintiles are cut per CALENDAR YEAR, not globally: volatility regimes differ
    # enormously (2020 vs 2023), and a global cut would label an entire year "positive".
    df["year"] = df["trading_date"].dt.year
    for h in HORIZONS:
        df[f"lab{h}"] = (
            df.groupby("year")[f"exc{h}"]
            .transform(lambda s: pd.qcut(s, 5, labels=False, duplicates="drop"))
        )
    out = df.dropna(subset=[f"lab{h}" for h in HORIZONS]).reset_index(drop=True)
    out.attrs["n_insane"] = n_insane
    return out, px


# ── model ────────────────────────────────────────────────────────────────────────────


def get_embeddings(df: pd.DataFrame, tag: str) -> np.ndarray:
    """Frozen mean-pooled PhoBERT over `headline. content-lead`.

    ⚠️ Cached **by `row_id`**, not by row count. Keying on `len(df)` looked simpler and was
    wrong: any change to the filters — the price-sanity screen below, a different horizon
    set — silently invalidates the whole cache and re-pays 13 GPU-minutes. Keyed by
    row_id, a filter is free and only genuinely new articles are embedded.
    """
    cache = CACHE / f"emb_{tag}.npz"
    want = df["row_id"].to_numpy()

    have_ids, have_vecs = np.array([], dtype=object), None
    if cache.exists():
        z = np.load(cache, allow_pickle=True)
        have_ids, have_vecs = z["ids"], z["vecs"]

    known = {rid: i for i, rid in enumerate(have_ids)}
    missing = [rid for rid in want if rid not in known]

    if missing:
        from sentiment.text_reaction_model import build_text, embed_texts

        sub = df[df["row_id"].isin(set(missing))]
        texts = build_text(sub["headline"], sub["content"])
        print(f"  embedding {len(texts):,} new articles (PhoBERT, frozen)…")
        new_vecs = embed_texts(texts).astype(np.float32)
        have_ids = np.concatenate([have_ids, sub["row_id"].to_numpy()])
        have_vecs = new_vecs if have_vecs is None else np.vstack([have_vecs, new_vecs])
        np.savez(cache, ids=have_ids, vecs=have_vecs)
        known = {rid: i for i, rid in enumerate(have_ids)}
    else:
        print(f"  embeddings: {len(want):,} articles served from cache")

    return have_vecs[[known[rid] for rid in want]]


def walkforward(
    df: pd.DataFrame, emb: np.ndarray, horizon: int, n_folds: int = 5
) -> dict:
    """Purged, embargoed, chronological. Returns metrics + out-of-sample predictions.

    ⚠️ Split on the DATE, never on the row. `experiment_10/CONTEXT.md` records paper 61
    failing exactly here: 19,736 headlines from a few hundred days, split at random, and
    same-day articles carrying the same label landed on both sides — the model only had
    to recognise which day an article came from. This corpus averages 58 articles a day.
    """
    y = df[f"lab{horizon}"].to_numpy(dtype=int)
    dates = df["trading_date"].to_numpy()
    order = np.argsort(dates, kind="mergesort")
    emb, y, dates = emb[order], y[order], dates[order]
    idx = df.index.to_numpy()[order]

    uniq = np.unique(dates)
    edges = np.linspace(len(uniq) * 0.4, len(uniq), n_folds + 1).astype(int)
    embargo = np.timedelta64(horizon + 2, "D")

    folds, preds = [], []
    for k in range(n_folds):
        t0, t1 = uniq[edges[k]], uniq[min(edges[k + 1], len(uniq) - 1)]
        train = dates < (t0 - embargo)
        test = (dates >= t0) & (dates < t1)
        if train.sum() < 2000 or test.sum() < 300:
            continue

        model = HistGradientBoostingClassifier(
            max_iter=150, learning_rate=0.06, max_depth=4,
            l2_regularization=1.0, random_state=0,
        ).fit(emb[train], y[train])
        p = model.predict(emb[test])
        proba = model.predict_proba(emb[test])

        rng = np.random.default_rng(k)
        shuffled = rng.permutation(y[train])
        p_null = (
            HistGradientBoostingClassifier(max_iter=60, max_depth=3, random_state=0)
            .fit(emb[train], shuffled)
            .predict(emb[test])
        )

        folds.append(
            {
                "fold": k,
                "n_train": int(train.sum()),
                "n_test": int(test.sum()),
                "train_acc": float((model.predict(emb[train]) == y[train]).mean()),
                "acc": float((p == y[test]).mean()),
                "base": float(pd.Series(y[test]).value_counts(normalize=True).max()),
                "macro_f1": float(f1_score(y[test], p, average="macro", zero_division=0)),
                "qwk": float(cohen_kappa_score(y[test], p, weights="quadratic")),
                "qwk_shuffled": float(cohen_kappa_score(y[test], p_null, weights="quadratic")),
                "period": f"{pd.Timestamp(t0):%Y-%m}→{pd.Timestamp(t1):%Y-%m}",
            }
        )
        preds.append(
            pd.DataFrame(
                {
                    "src": idx[test],
                    "y": y[test],
                    "pred": p,
                    "p_top": proba[:, -1],
                    "p_bot": proba[:, 0],
                    "conf": proba.max(axis=1),
                }
            )
        )
    return {"folds": folds, "preds": pd.concat(preds) if preds else pd.DataFrame()}


# ── charts ───────────────────────────────────────────────────────────────────────────


def _style(ax, title, sub=None, xlab=None, ylab=None):
    ax.set_title(title, fontsize=11, fontweight="600", loc="left", pad=14 if sub else 8)
    if sub:
        ax.text(0, 1.02, sub, transform=ax.transAxes, fontsize=8.5, color=INK_2, va="bottom")
    ax.set_xlabel(xlab or "", fontsize=9)
    ax.set_ylabel(ylab or "", fontsize=9)
    ax.grid(axis="y", alpha=0.9, zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=8.5, length=0)


def chart_does_news_move_prices(
    df: pd.DataFrame, px: pd.DataFrame, news_all: pd.DataFrame, path: Path
):
    """Panel 1 — the prior question. If news days are not more volatile than ordinary
    days, nothing downstream can work.

    ⚠️ The news-day set comes from the FULL news frame, never from a `--quick` subsample:
    a sampled set would mark real news days as quiet and wash the contrast out.
    """
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.4))

    news_days = set(zip(news_all["ticker"], news_all["trading_date"]))
    sample = px.dropna(subset=["exc5"]).copy()
    sample["has_news"] = [
        (t, d) in news_days for t, d in zip(sample["ticker"], sample["date"])
    ]
    sample["abs_exc"] = sample["exc5"].abs()

    # ⚠️ Normalise WITHIN ticker before comparing. News concentrates in large liquid
    # names, which are structurally less volatile than the small caps that dominate the
    # quiet rows — so the raw comparison measures the UNIVERSE MIX, not the effect of
    # news. Dividing by each ticker's own median removes that composition entirely.
    med = sample.groupby("ticker")["abs_exc"].transform("median")
    sample["rel_vol"] = sample["abs_exc"] / med.replace(0, np.nan)
    stats = sample.groupby("has_news").agg(
        mad=("abs_exc", "mean"), rel=("rel_vol", "median"), count=("abs_exc", "size")
    )

    ax = axes[0]
    x = np.arange(2)
    raw = [stats.loc[False, "mad"] * 100, stats.loc[True, "mad"] * 100]
    rel = [stats.loc[False, "rel"], stats.loc[True, "rel"]]
    ax.bar(x - 0.19, raw, width=0.36, color=MUTED, zorder=3, label="thô (%)")
    ax.bar(x + 0.19, [r * raw[0] for r in rel], width=0.36, color=SERIES[0], zorder=3,
           label="chuẩn hoá trong từng mã (×)")
    for xi, v in zip(x - 0.19, raw):
        ax.text(xi, v, f"{v:.2f}%", ha="center", va="bottom", fontsize=9,
                fontweight="600", color=INK_2)
    for xi, r in zip(x + 0.19, rel):
        ax.text(xi, r * raw[0], f"{r:.2f}×", ha="center", va="bottom", fontsize=9,
                fontweight="600", color=SERIES[0])
    ax.set_xticks(x, [f"Không có tin\nn={int(stats.loc[False, 'count']):,}",
                      f"Có tin\nn={int(stats.loc[True, 'count']):,}"], fontsize=9)
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    _style(ax, "Ngày có tin có biến động mạnh hơn không?",
           "|lợi suất vượt trội 5 phiên| — thô vs chuẩn hoá theo mã", ylab="%")

    ax = axes[1]
    cat = (
        df.assign(a=df["exc5"].abs() * 100)
        .groupby("category")["a"].agg(["mean", "size"])
        .sort_values("mean")
    )
    cat.index = [c.replace("_", " ")[:34] for c in cat.index]
    ax.barh(cat.index, cat["mean"], color=SERIES[0], height=0.6, zorder=3)
    for i, (v, n) in enumerate(zip(cat["mean"], cat["size"])):
        ax.text(v + 0.05, i, f"{v:.2f}%  (n={int(n):,})", va="center", fontsize=8, color=INK_2)
    ax.set_xlim(0, cat["mean"].max() * 1.45)
    _style(ax, "…và loại tin nào đi kèm biến động lớn nhất?",
           "|lợi suất vượt trội 5 phiên| trung bình theo category", xlab="%")
    ax.grid(axis="x", alpha=0.9)
    ax.grid(axis="y", visible=False)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    return stats


def chart_model(results: dict, df: pd.DataFrame, path: Path):
    """Panels 2-4 — can the text predict the level, and does the model degenerate?"""
    fig = plt.figure(figsize=(13.5, 8.6))
    gs = fig.add_gridspec(2, 3, hspace=0.45, wspace=0.32)

    # ── per-fold accuracy vs base rate, by horizon ───────────────────────────────────
    ax = fig.add_subplot(gs[0, :2])
    width = 0.26
    for i, h in enumerate(HORIZONS):
        f = pd.DataFrame(results[h]["folds"])
        x = np.arange(len(f)) + (i - 1) * width
        ax.bar(x, f["acc"] * 100, width=width * 0.9, color=SERIES[i], zorder=3,
               label=f"h = {h} phiên")
    f0 = pd.DataFrame(results[HORIZONS[0]]["folds"])
    ax.axhline(20, color=DIVERGE_NEG, lw=2, ls="--", zorder=4)
    ax.text(len(f0) - 0.4, 20.6, "tỷ lệ nền 20% (5 lớp phân vị)", ha="right",
            fontsize=8.5, color=DIVERGE_NEG, fontweight="600")
    ax.set_xticks(np.arange(len(f0)))
    ax.set_xticklabels(f0["period"], fontsize=8)
    ax.set_ylim(0, 34)
    ax.legend(frameon=False, fontsize=8.5, ncol=3, loc="upper left")
    _style(ax, "Accuracy ngoài mẫu theo từng fold, so với tỷ lệ nền",
           "một con số gộp che mất điều paper 49 đo được (0,90 → 0,56 khi test set lớn dần)",
           ylab="accuracy (%)")

    # ── train vs test collapse — paper 63's headline ─────────────────────────────────
    ax = fig.add_subplot(gs[0, 2])
    f = pd.DataFrame(results[5]["folds"])
    ax.plot([0] * len(f), f["train_acc"] * 100, "o", ms=9, color=SERIES[1], zorder=3)
    ax.plot([1] * len(f), f["acc"] * 100, "o", ms=9, color=SERIES[0], zorder=3)
    for a, b in zip(f["train_acc"] * 100, f["acc"] * 100):
        ax.plot([0, 1], [a, b], color=BASELINE, lw=1.5, zorder=2)
    ax.axhline(20, color=DIVERGE_NEG, lw=2, ls="--", zorder=4)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["train", "test"], fontsize=9.5)
    ax.set_xlim(-0.35, 1.35)
    ax.text(1.05, f["acc"].mean() * 100, f"{f['acc'].mean() * 100:.1f}%", fontsize=10,
            fontweight="600", color=SERIES[0], va="center")
    ax.text(-0.05, f["train_acc"].mean() * 100, f"{f['train_acc'].mean() * 100:.1f}%",
            fontsize=10, fontweight="600", color=SERIES[1], va="center", ha="right")
    _style(ax, "Sụp đổ train → test (h=5)",
           "paper 63: 97,5% → 50,4%", ylab="accuracy (%)")

    # ── confusion matrix ─────────────────────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, 0])
    p = results[5]["preds"]
    cm = confusion_matrix(p["y"], p["pred"], labels=range(5), normalize="true") * 100
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list("seq", SEQ_BLUE)
    im = ax.imshow(cm, cmap=cmap, vmin=0, vmax=max(60, cm.max()))
    for i in range(5):
        for j in range(5):
            ax.text(j, i, f"{cm[i, j]:.0f}", ha="center", va="center", fontsize=8,
                    color="white" if cm[i, j] > cm.max() * 0.55 else INK)
    ax.set_xticks(range(5), [str(i) for i in range(5)], fontsize=8.5)
    ax.set_yticks(range(5), [f"{i} {LEVELS[i][:9]}" for i in range(5)], fontsize=8)
    ax.grid(False)
    _style(ax, "Ma trận nhầm lẫn (h=5), % theo hàng",
           "một cột sáng = mô hình chỉ đoán một lớp", xlab="dự đoán", ylab="thực tế")
    ax.grid(False)

    # ── QWK vs shuffled-label control ────────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, 1])
    xs = np.arange(len(HORIZONS))
    q = [np.mean([f["qwk"] for f in results[h]["folds"]]) for h in HORIZONS]
    qs = [np.mean([f["qwk_shuffled"] for f in results[h]["folds"]]) for h in HORIZONS]
    ax.bar(xs - 0.18, q, width=0.34, color=SERIES[0], zorder=3, label="mô hình")
    ax.bar(xs + 0.18, qs, width=0.34, color=MUTED, zorder=3, label="nhãn xáo trộn")
    for x, v in zip(xs - 0.18, q):
        ax.text(x, v, f"{v:+.3f}", ha="center", va="bottom" if v >= 0 else "top",
                fontsize=8.5, fontweight="600", color=INK)
    ax.axhline(0, color=BASELINE, lw=1.4)
    ax.set_xticks(xs, [f"h={h}" for h in HORIZONS], fontsize=9)
    ax.legend(frameon=False, fontsize=8.5, loc="upper right")
    _style(ax, "QWK — có hơn nhiễu không?",
           "0 = không đồng thuận ngoài ngẫu nhiên", ylab="QWK")

    # ── decile plot: the real test ───────────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, 2])
    pp = results[5]["preds"].copy()
    pp["exc"] = df.loc[pp["src"], "exc5"].to_numpy() * 100
    pp["dec"] = pd.qcut(pp["p_top"], 10, labels=False, duplicates="drop")
    d = pp.groupby("dec")["exc"].mean()
    colors = [DIVERGE_POS if v >= 0 else DIVERGE_NEG for v in d]
    ax.bar(d.index, d.values, color=colors, width=0.72, zorder=3)
    ax.axhline(0, color=BASELINE, lw=1.4)
    _style(ax, "Decile theo P(rất tích cực) → lợi suất thực tế",
           "có tín hiệu thì cột phải dốc lên từ trái sang phải",
           xlab="decile xác suất mô hình", ylab="lợi suất vượt trội TB (%)")

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)


# ── examples ─────────────────────────────────────────────────────────────────────────


def write_examples(df: pd.DataFrame, results: dict, path: Path, n: int = 6):
    """The eyeball test. Six buckets, each answering a different question."""
    p = results[5]["preds"].copy()
    ex = df.loc[p["src"]].copy()
    ex["y"] = p["y"].to_numpy()
    ex["pred"] = p["pred"].to_numpy()
    ex["conf"] = p["conf"].to_numpy()
    ex["correct"] = ex["y"] == ex["pred"]

    def block(title, why, rows):
        out = [f"\n### {title}\n\n_{why}_\n"]
        for _, r in rows.iterrows():
            out.append(
                f"\n**{r['ticker']}** · {r['trading_date']:%Y-%m-%d} · `{r['category']}`  \n"
                f"> {str(r['headline']).strip()}\n\n"
                f"{str(r['content'])[:260].strip()}…\n\n"
                f"| exc 1p | exc 5p | exc 10p | thực tế | dự đoán | tin cậy |\n"
                f"|---|---|---|---|---|---|\n"
                f"| {r['exc1']:+.2%} | **{r['exc5']:+.2%}** | {r['exc10']:+.2%} | "
                f"{int(r['y'])} {LEVELS[int(r['y'])]} | {int(r['pred'])} "
                f"{LEVELS[int(r['pred'])]} | {r['conf']:.2f} |\n"
                f"\n<{r['url']}>\n"
            )
        return "".join(out)

    parts = [
        "# Ví dụ tiêu biểu — text bài báo → phản ứng giá\n",
        "Sinh bởi `news_result.py`. Nhãn = **lợi suất vượt trội thị trường** sau ngày bài "
        "báo có thể tác động (`trading_date`), chia ngũ phân vị theo từng năm.\n\n"
        "⚠️ Mọi dòng dưới đây đều **ngoài mẫu** — mô hình chưa từng thấy khi huấn luyện.\n",
        block("1. Phản ứng dương mạnh nhất", "Text có nói gì báo trước không?",
              ex.nlargest(n, "exc5")),
        block("2. Phản ứng âm mạnh nhất", "Tin xấu có đọc ra được là xấu không?",
              ex.nsmallest(n, "exc5")),
        block("3. Mô hình TỰ TIN và ĐÚNG",
              "Nếu có tín hiệu thật thì nó nằm ở đây — kiểm tra xem có phải chỉ là "
              "nhận ra tên mã / câu khuôn mẫu không (bẫy của paper 61).",
              ex[ex["correct"]].nlargest(n, "conf")),
        block("4. Mô hình TỰ TIN và SAI",
              "Chi phí của việc tin vào mô hình.",
              ex[~ex["correct"]].nlargest(n, "conf")),
        block("5. Trực giác ngược — tin nghe TÍCH CỰC nhưng giá GIẢM mạnh",
              "Chia cổ tức / kết quả kinh doanh mà giá vẫn rơi. Đây là chỗ một scorer "
              "sắc thái tổng quát sẽ sai — và cũng là lý do scorer hiện tại chấm "
              "'VCB: chi trả cổ tức 2025' = −0,97.",
              ex[ex["category"].isin(["dividends_and_record_date",
                                      "business_results_and_analysis"])].nsmallest(n, "exc5")),
        block("6. Cùng ngày, cùng mã, phản ứng khác nhau",
              "Nếu nhiều bài cùng một mã-ngày mà nhãn giống hệt nhau thì bài toán là "
              "'đoán xem hôm đó là ngày nào', không phải đọc hiểu — chính là lỗi "
              "paper 61 mắc.",
              ex[ex.duplicated(["ticker", "trading_date"], keep=False)].nlargest(n, "conf")),
    ]
    path.write_text("".join(parts), encoding="utf-8")


# ── main ─────────────────────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true", help="8k articles, for iteration")
    ap.add_argument("--include-disclosures", action="store_true")
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument(
        "--min-relevance",
        type=float,
        default=0.0,
        help=(
            "Keep only articles naming the ticker at least this often per 1,000 chars. "
            "⚠️ Reading the examples file showed why this matters: the extreme-labelled "
            "cases are dominated by market ROUND-UPS — 'Lịch chốt quyền nhận cổ tức bằng "
            "tiền của 8 doanh nghiệp' is tagged to CPH but is about VGC, NFC and six "
            "others. Paper 57 excludes multi-company stories and filters relevance below "
            "35% for exactly this; paper 49's company-relevance flag measurably helped. "
            "1.0 is a reasonable cut."
        ),
    )
    args = ap.parse_args()

    print("Loading…")
    news, px = load_frames(args.include_disclosures)
    print(f"  news {len(news):,} · price rows {len(px):,}")

    df, px = build_reactions(news, px)
    df_full = df
    if args.min_relevance > 0:
        before = len(df)
        df = df[df["relevance"] >= args.min_relevance].reset_index(drop=True)
        print(f"  relevance ≥ {args.min_relevance}: {before:,} → {len(df):,} "
              f"({len(df) / before:.1%} kept) — drops market round-ups mis-tagged to a ticker")
    if args.quick:
        df = df.sample(8000, random_state=0).sort_values("trading_date").reset_index(drop=True)
    df = df.reset_index(drop=True)
    print(f"  articles with a usable reaction: {len(df):,} "
          f"({df['trading_date'].min():%Y-%m} → {df['trading_date'].max():%Y-%m})")
    print(f"  ⚠️ dropped by the price-sanity screen (close_adjust breaks at some "
          f"corporate actions): {df.attrs.get('n_insane', 0):,}")

    tag = ("all" if args.include_disclosures else "ed") + ("_q" if args.quick else "")
    emb = get_embeddings(df, tag)

    results = {}
    for h in HORIZONS:
        print(f"  walk-forward h={h}…")
        results[h] = walkforward(df, emb, h, n_folds=args.folds)

    print("\n" + "=" * 94)
    print(f"{'h':>3}{'fold':>6}{'period':>18}{'n_test':>9}{'train':>9}{'test':>9}"
          f"{'base':>8}{'macroF1':>9}{'QWK':>9}{'QWK shuf':>10}")
    print("-" * 94)
    for h in HORIZONS:
        for f in results[h]["folds"]:
            print(f"{h:>3}{f['fold']:>6}{f['period']:>18}{f['n_test']:>9,}"
                  f"{f['train_acc']:>9.3f}{f['acc']:>9.3f}{f['base']:>8.3f}"
                  f"{f['macro_f1']:>9.3f}{f['qwk']:>9.3f}{f['qwk_shuffled']:>10.3f}")
        m = pd.DataFrame(results[h]["folds"]).mean(numeric_only=True)
        print(f"{h:>3}{'MEAN':>6}{'':>18}{'':>9}{m['train_acc']:>9.3f}{m['acc']:>9.3f}"
              f"{m['base']:>8.3f}{m['macro_f1']:>9.3f}{m['qwk']:>9.3f}"
              f"{m['qwk_shuffled']:>10.3f}")
        print("-" * 94)

    print("\nCharts…")
    stats = chart_does_news_move_prices(
        df, px, df_full, OUT / "news_result_1_impact.png"
    )
    chart_model(results, df, OUT / "news_result_2_model.png")
    write_examples(df, results, OUT / "news_result_examples.md")

    mad_news, mad_quiet = stats.loc[True, "mad"], stats.loc[False, "mad"]
    rel_news, rel_quiet = stats.loc[True, "rel"], stats.loc[False, "rel"]
    acc5 = np.mean([f["acc"] for f in results[5]["folds"]])
    qwk5 = np.mean([f["qwk"] for f in results[5]["folds"]])
    print(
        f"\nVERDICT\n"
        f"  news-day |excess 5d| {mad_news:.3%} vs quiet-day {mad_quiet:.3%} "
        f"(raw {mad_news / mad_quiet:.2f}×) — but the raw ratio is a universe-mix\n"
        f"  artifact; normalised WITHIN ticker it is "
        f"{rel_news:.3f} vs {rel_quiet:.3f} ({rel_news / rel_quiet:.2f}×)\n"
        f"  h=5 test accuracy {acc5:.3f} against a 0.200 base rate; QWK {qwk5:+.3f}\n"
        f"  Reference: paper 63 reached 97.5% train / 50.4% test on a BALANCED BINARY\n"
        f"  task with Reuters text and millisecond tick labels. On 5 ordered classes the\n"
        f"  base rate is 0.200 and QWK 0 means no agreement beyond chance.\n"
        f"\n  → experiment_10/news_result_1_impact.png\n"
        f"  → experiment_10/news_result_2_model.png\n"
        f"  → experiment_10/news_result_examples.md"
    )


if __name__ == "__main__":
    main()
