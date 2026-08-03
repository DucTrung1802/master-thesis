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
    python experiment/experiment_10/news_result.py --min-relevance 1.0
    python experiment/experiment_10/news_result.py --include-disclosures

────────────────────────────────────────────────────────────────────────────────────────
## RESULTS AS RUN — 2026-08-03, 58,660 editorials, FULL content, 2 label schemes

Four arms on identical rows, folds and labels. Representation (`sentiment/doc_encoder.py`):
`lead` = the first 254-token window ≈ the old 256-token slice, which measurement showed
saw **38.7%** of the average article; `full` = the whole document, 3.44 chunks, pooled as
mean ‖ max ‖ lead. Label scheme: equal quantile bands at k = 3 and k = 5.

| k | rep | h | train | test | base | **lift** | QWK | **MCC** | MCC shuf |
|---|---|---|---|---|---|---|---|---|---|
| 3 | full | 1 | 0.549 | 0.350 | 0.341 | 1.028 | +0.003 | +0.026 | −0.011 |
| 3 | full | 5 | 0.581 | 0.351 | 0.341 | 1.029 | +0.018 | +0.026 | 0.000 |
| 3 | full | 10 | 0.610 | 0.352 | 0.342 | 1.029 | +0.024 | +0.029 | −0.009 |
| 5 | full | 1 | 0.521 | 0.224 | 0.205 | **1.093** | +0.008 | +0.031 | +0.001 |
| 5 | full | 5 | 0.496 | 0.224 | 0.207 | **1.082** | +0.020 | +0.030 | −0.002 |
| 5 | full | 10 | 0.485 | 0.214 | 0.207 | 1.034 | +0.014 | +0.018 | −0.002 |

### A — does reading the WHOLE article help? (full minus lead, same folds)

| k | h | Δacc | ΔQWK | ΔMCC | folds won |
|---|---|---|---|---|---|
| 3 | 1 | −0.0039 | −0.0160 | −0.0058 | 0/5 |
| 3 | 5 | +0.0051 | +0.0049 | +0.0079 | 3/5 |
| 3 | 10 | +0.0030 | +0.0068 | +0.0047 | 2/5 |
| 5 | 1 | +0.0049 | +0.0031 | +0.0062 | 3/5 |
| 5 | 5 | **+0.0092** | **+0.0157** | **+0.0115** | 4/5 |
| 5 | 10 | +0.0024 | +0.0017 | +0.0033 | 3/5 |

**Yes, consistently, and marginally** — at most +0.9 pp of accuracy. Note it is cleaner at
k=5 (3/5, 4/5, 3/5) than at k=3 (0/5, 3/5, 2/5): **the extra information from reading the
body lives in the TAILS, and three bands merge exactly those.**

### B — 3 bands vs 5 bands (full doc, same folds)

| h | Δacc | Δlift | ΔQWK | ΔMCC | folds won |
|---|---|---|---|---|---|
| 1 | **+0.1259** | −0.0643 | −0.0052 | −0.0046 | **1/5** |
| 5 | **+0.1272** | −0.0533 | −0.0015 | −0.0033 | **1/5** |
| 10 | **+0.1380** | −0.0050 | +0.0102 | +0.0109 | 4/5 |

⚠️ **Accuracy jumps 12.6–13.8 pp and every point of it is the base rate**, which moves
0.205 → 0.341 on its own. This is paper 56's result in its mildest form: accuracy rises
by relabelling, with no change to the model. Reporting "3 classes lifted accuracy from
22.4% to 35.1%" without the base rate beside it would be meaningless.

**Corrected for that, three bands are NOT better — they are slightly worse at two of three
horizons** (ΔMCC −0.005, −0.003, +0.011; 1/5 folds won at h=1 and h=5). And **lift falls
from 1.08–1.09 to 1.03**: relative to its own base rate the 5-band model does more.

Both remain null. MCC ≈ 0.026–0.031 either way, against a shuffled-label control at ≈ 0
and paper 51's 0.069 on 8.5M articles — about half the literature's own ceiling.

### Why equal bands, and not 25/50/25

Paper 56 gives the accuracy of guessing from the prior as `Σ Pₖ²`, which for k classes is
minimised exactly when every band is 1/k: **0.333 for tertiles against 0.375 for
25/50/25**. Choosing the uneven split would hand out four free points of accuracy. Paper 56
picked its own threshold at that minimum for the same reason.

### Everything else, all negative
* `--min-relevance 1.0` (only articles naming the ticker; 48% of the corpus) makes it
  WORSE — test 0.220 against a base rate of 0.222, i.e. at or below chance.
* The shuffled-label control sits at ≈ 0 and BEATS the model at h=1 on QWK.
* Train 0.55–0.61 at k=3 against 0.49–0.52 at k=5 — the easier task is fitted better and
  still lands on the base rate. The train→test gap moves; it does not close.

The one positive result is not about direction at all: normalised WITHIN ticker, news days
carry **1.15×** the absolute excess move of quiet days. News predicts MAGNITUDE, not SIGN.
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
from sklearn.metrics import (
    cohen_kappa_score,
    confusion_matrix,
    f1_score,
    matthews_corrcoef,
)

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

HORIZONS = (1, 5, 10)
REP_LEAD, REP_FULL = "lead (256 tok)", "full doc (chunked)"

#: Label schemes. Both are EQUAL-SIZED quantile bands, so the base rate is 1/k by
#: construction and known before a model is fitted (paper 53).
#:
#: ⚠️ **Equal bands are also the least flattering choice, deliberately.** Paper 56 derives
#: the accuracy of guessing from the prior as `Σ Pₖ²`; for k classes that is minimised
#: exactly when every band is 1/k. Tertiles therefore sit at the MINIMUM of the trivial
#: curve — 0.333 — where a 25/50/25 split would hand out 0.375 for free, and a wider
#: neutral band more still. Paper 56 picked its own threshold at that minimum for the same
#: reason.
#:
#: ⚠️ **Accuracy is NOT comparable across k.** Going 5 → 3 raises the base rate 0.200 →
#: 0.333, so accuracy must rise whatever the model does. Only the chance-corrected
#: metrics — QWK and MCC — and the lift over base rate can be read across schemes.
LEVEL_NAMES = {
    3: ["tiêu cực", "trung tính", "tích cực"],
    5: ["rất tiêu cực", "tiêu cực", "trung tính", "tích cực", "rất tích cực"],
}
LEVELS = LEVEL_NAMES[5]  # back-compat for the examples writer's default

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

    # ⚠️ Bands are cut per CALENDAR YEAR, not globally: volatility regimes differ
    # enormously (2020 vs 2023), and a global cut would label an entire year "positive".
    df["year"] = df["trading_date"].dt.year
    label_cols = []
    for k in LEVEL_NAMES:
        for h in HORIZONS:
            col = f"lab{k}_{h}"
            df[col] = (
                df.groupby("year")[f"exc{h}"]
                .transform(lambda s: pd.qcut(s, k, labels=False, duplicates="drop"))
            )
            label_cols.append(col)
    out = df.dropna(subset=label_cols).reset_index(drop=True)
    out.attrs["n_insane"] = n_insane
    return out, px


# ── model ────────────────────────────────────────────────────────────────────────────


def get_embeddings(df: pd.DataFrame, tag: str) -> dict[str, np.ndarray]:
    """→ `{"lead": (n, 768), "full": (n, 2304)}` — the WHOLE article, chunk-pooled.

    ⚠️ `lead` is kept deliberately: it is (near enough) the old 256-token representation,
    which measurement showed saw **38.7%** of the average article. Carrying both makes the
    lead-vs-full comparison an ablation on identical rows, folds and labels rather than a
    comparison against a number from a previous run.

    ⚠️ Cached **by `row_id`**, not by row count. Keying on `len(df)` looked simpler and was
    wrong: any change to the filters silently invalidated the cache and re-paid the GPU
    time. Keyed by row_id, a filter is free and only genuinely new articles are encoded.
    """
    cache = CACHE / f"docemb_{tag}.npz"
    want = df["row_id"].to_numpy()

    have_ids = np.array([], dtype=object)
    have = {"lead": None, "full": None}
    if cache.exists():
        z = np.load(cache, allow_pickle=True)
        have_ids, have["lead"], have["full"] = z["ids"], z["lead"], z["full"]

    known = {rid: i for i, rid in enumerate(have_ids)}
    missing = [rid for rid in want if rid not in known]

    if missing:
        from sentiment.doc_encoder import build_document_text, encode_documents

        sub = df[df["row_id"].isin(set(missing))]
        print(f"  encoding {len(sub):,} new articles — FULL content, chunked…")
        new = encode_documents(build_document_text(sub["headline"], sub["content"]))
        have_ids = np.concatenate([have_ids, sub["row_id"].to_numpy()])
        for k in ("lead", "full"):
            have[k] = new[k] if have[k] is None else np.vstack([have[k], new[k]])
        np.savez(cache, ids=have_ids, lead=have["lead"], full=have["full"])
        known = {rid: i for i, rid in enumerate(have_ids)}
    else:
        print(f"  document embeddings: {len(want):,} articles served from cache")

    take = [known[rid] for rid in want]
    return {k: have[k][take] for k in ("lead", "full")}


def walkforward(
    df: pd.DataFrame, emb: np.ndarray, horizon: int, n_classes: int = 5, n_folds: int = 5
) -> dict:
    """Purged, embargoed, chronological. Returns metrics + out-of-sample predictions.

    ⚠️ Split on the DATE, never on the row. `experiment_10/CONTEXT.md` records paper 61
    failing exactly here: 19,736 headlines from a few hundred days, split at random, and
    same-day articles carrying the same label landed on both sides — the model only had
    to recognise which day an article came from. This corpus averages 58 articles a day.
    """
    y = df[f"lab{n_classes}_{horizon}"].to_numpy(dtype=int)
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
                # ⚠️ MCC is the metric that survives a change of k: it is corrected for the
                # class prior, so 3-class and 5-class runs can be put side by side. Raw
                # accuracy cannot — the base rate moves 0.200 → 0.333 on its own.
                "mcc": float(matthews_corrcoef(y[test], p)),
                "mcc_shuffled": float(matthews_corrcoef(y[test], p_null)),
                "lift": float((p == y[test]).mean()
                              / pd.Series(y[test]).value_counts(normalize=True).max()),
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


def chart_class_schemes(results: dict, path: Path):
    """3 bands vs 5 bands, on the metrics where that comparison is legitimate.

    ⚠️ The whole point of this figure is that **raw accuracy cannot be compared across k**.
    Cutting five bands into three moves the base rate from 0.200 to 0.333, so accuracy
    rises whatever the model does — paper 56's result, that widening bands buys accuracy
    with no change to the model, in its mildest form. Panel 1 shows accuracy WITH its own
    base rate so the reader sees the gap rather than the level; panels 2-3 show the
    chance-corrected metrics that survive the change.
    """
    ks = sorted(LEVEL_NAMES)
    fig, axes = plt.subplots(1, 3, figsize=(14.5, 4.5))
    xs = np.arange(len(HORIZONS))
    width = 0.34

    ax = axes[0]
    for i, k in enumerate(ks):
        acc = [np.mean([f["acc"] for f in results[(k, REP_FULL, h)]["folds"]]) * 100
               for h in HORIZONS]
        base = [np.mean([f["base"] for f in results[(k, REP_FULL, h)]["folds"]]) * 100
                for h in HORIZONS]
        pos = xs + (i - 0.5) * width
        ax.bar(pos, acc, width=width * 0.9, color=SERIES[i], zorder=3, label=f"{k} lớp")
        ax.scatter(pos, base, marker="_", s=340, color=DIVERGE_NEG, zorder=5,
                   linewidths=2.4, label="tỷ lệ nền" if i == 0 else None)
        for x, a, b in zip(pos, acc, base):
            ax.text(x, a + 0.7, f"+{a - b:.1f}", ha="center", fontsize=8,
                    fontweight="600", color=INK_2)
    ax.set_xticks(xs, [f"h={h}" for h in HORIZONS], fontsize=9)
    ax.legend(frameon=False, fontsize=8.5, ncol=3, loc="upper left")
    ax.set_ylim(0, 46)
    _style(ax, "Accuracy — KHÔNG so sánh được giữa 3 và 5 lớp",
           "cột = accuracy · gạch đỏ = tỷ lệ nền của chính nó · số = khoảng cách",
           ylab="%")

    for j, (metric, shuf, title, sub) in enumerate(
        [
            ("qwk", "qwk_shuffled", "QWK — đã hiệu chỉnh ngẫu nhiên",
             "so sánh được giữa 3 và 5 lớp"),
            ("mcc", "mcc_shuffled", "MCC — hiệu chỉnh theo tỷ lệ lớp",
             "paper 51 đạt 0,069 với 8,5 triệu bài"),
        ]
    ):
        ax = axes[j + 1]
        for i, k in enumerate(ks):
            v = [np.mean([f[metric] for f in results[(k, REP_FULL, h)]["folds"]])
                 for h in HORIZONS]
            pos = xs + (i - 0.5) * width
            ax.bar(pos, v, width=width * 0.9, color=SERIES[i], zorder=3, label=f"{k} lớp")
            for x, val in zip(pos, v):
                ax.text(x, val, f"{val:+.3f}", ha="center",
                        va="bottom" if val >= 0 else "top", fontsize=8, color=INK_2)
        sh = [np.mean([f[shuf] for f in results[(ks[-1], REP_FULL, h)]["folds"]])
              for h in HORIZONS]
        ax.plot(xs, sh, "o--", color=MUTED, ms=6, lw=1.6, zorder=4, label="nhãn xáo trộn")
        ax.axhline(0, color=BASELINE, lw=1.4)
        ax.set_xticks(xs, [f"h={h}" for h in HORIZONS], fontsize=9)
        # ⚠️ Headroom before the legend, not after: the value labels sit on the bar tops in
        # DATA coordinates, so without this the tallest bar's label lands under the legend.
        lo, hi = ax.get_ylim()
        ax.set_ylim(min(lo, min(sh) * 1.4), hi * 1.42)
        ax.legend(frameon=False, fontsize=8, ncol=3, loc="upper left")
        _style(ax, title, sub, ylab=metric.upper())

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)


def chart_model(results: dict, df: pd.DataFrame, path: Path, n_classes: int = 5):
    """Panels 2-4 — can the text predict the level, and does the model degenerate?"""
    fig = plt.figure(figsize=(13.5, 8.6))
    gs = fig.add_gridspec(2, 3, hspace=0.45, wspace=0.32)

    # ── per-fold accuracy vs base rate, by horizon ───────────────────────────────────
    ax = fig.add_subplot(gs[0, :2])
    width = 0.26
    for i, h in enumerate(HORIZONS):
        f = pd.DataFrame(results[(n_classes, REP_FULL, h)]["folds"])
        x = np.arange(len(f)) + (i - 1) * width
        ax.bar(x, f["acc"] * 100, width=width * 0.9, color=SERIES[i], zorder=3,
               label=f"h = {h} phiên")
    f0 = pd.DataFrame(results[(n_classes, REP_FULL, HORIZONS[0])]["folds"])
    base_pct = 100.0 / n_classes
    ax.axhline(base_pct, color=DIVERGE_NEG, lw=2, ls="--", zorder=4)
    ax.text(len(f0) - 0.4, base_pct + 0.6, f"tỷ lệ nền {base_pct:.1f}% ({n_classes} lớp phân vị)", ha="right",
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
    f = pd.DataFrame(results[(n_classes, REP_FULL, 5)]["folds"])
    ax.plot([0] * len(f), f["train_acc"] * 100, "o", ms=9, color=SERIES[1], zorder=3)
    ax.plot([1] * len(f), f["acc"] * 100, "o", ms=9, color=SERIES[0], zorder=3)
    for a, b in zip(f["train_acc"] * 100, f["acc"] * 100):
        ax.plot([0, 1], [a, b], color=BASELINE, lw=1.5, zorder=2)
    base_pct = 100.0 / n_classes
    ax.axhline(base_pct, color=DIVERGE_NEG, lw=2, ls="--", zorder=4)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["train", "test"], fontsize=9.5)
    ax.set_xlim(-0.35, 1.35)
    ax.text(1.05, f["acc"].mean() * 100, f"{f['acc'].mean() * 100:.1f}%", fontsize=10,
            fontweight="600", color=SERIES[0], va="center")
    ax.text(-0.05, f["train_acc"].mean() * 100, f"{f['train_acc'].mean() * 100:.1f}%",
            fontsize=10, fontweight="600", color=SERIES[1], va="center", ha="right")
    _style(ax, f"Sụp đổ train → test (h=5, {n_classes} lớp)",
           "paper 63: 97,5% → 50,4%", ylab="accuracy (%)")

    # ── confusion matrix ─────────────────────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, 0])
    p = results[(n_classes, REP_FULL, 5)]["preds"]
    names = LEVEL_NAMES[n_classes]
    cm = confusion_matrix(p["y"], p["pred"], labels=range(n_classes), normalize="true") * 100
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list("seq", SEQ_BLUE)
    im = ax.imshow(cm, cmap=cmap, vmin=0, vmax=max(60, cm.max()))
    for i in range(n_classes):
        for j in range(n_classes):
            ax.text(j, i, f"{cm[i, j]:.0f}", ha="center", va="center", fontsize=8,
                    color="white" if cm[i, j] > cm.max() * 0.55 else INK)
    ax.set_xticks(range(n_classes), [str(i) for i in range(n_classes)], fontsize=8.5)
    ax.set_yticks(range(n_classes), [f"{i} {names[i][:11]}" for i in range(n_classes)], fontsize=8)
    ax.grid(False)
    _style(ax, f"Ma trận nhầm lẫn (h=5, {n_classes} lớp), % theo hàng",
           "một cột sáng = mô hình chỉ đoán một lớp", xlab="dự đoán", ylab="thực tế")
    ax.grid(False)

    # ── QWK vs shuffled-label control ────────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, 1])
    xs = np.arange(len(HORIZONS))
    q = [np.mean([f["qwk"] for f in results[(n_classes, REP_FULL, h)]["folds"]]) for h in HORIZONS]
    qs = [np.mean([f["qwk_shuffled"] for f in results[(n_classes, REP_FULL, h)]["folds"]]) for h in HORIZONS]
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
    pp = results[(n_classes, REP_FULL, 5)]["preds"].copy()
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


def write_examples(
    df: pd.DataFrame, results: dict, path: Path, n: int = 6, n_classes: int = 5
):
    """The eyeball test. Six buckets, each answering a different question."""
    LEVELS = LEVEL_NAMES[n_classes]
    p = results[(n_classes, REP_FULL, 5)]["preds"].copy()
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

    # ⚠️ No `_q` suffix: the cache is keyed by row_id, so a --quick subsample is served
    # from the same store as the full run instead of re-encoding its own copy.
    tag = "all" if args.include_disclosures else "ed"
    emb = get_embeddings(df, tag)
    print(f"  representations: lead {emb['lead'].shape} · full {emb['full'].shape}")

    # ⚠️ Two arms on identical rows, folds and labels. `lead` ≈ the old 256-token slice
    # (38.7% of the average article); `full` is the whole document, chunk-pooled.
    arms = {REP_LEAD: emb["lead"], REP_FULL: emb["full"]}
    ks = sorted(LEVEL_NAMES)
    results = {}
    for k in ks:
        for rep, mat in arms.items():
            for h in HORIZONS:
                print(f"  walk-forward  {k} lớp · {rep:<20} h={h}…", flush=True)
                results[(k, rep, h)] = walkforward(
                    df, mat, h, n_classes=k, n_folds=args.folds
                )

    print("\n" + "=" * 112)
    print(f"{'k':>2}{'representation':<21}{'h':>4}{'train':>8}{'test':>8}{'base':>8}"
          f"{'lift':>7}{'macroF1':>9}{'QWK':>8}{'QWKshuf':>9}{'MCC':>8}{'MCCshuf':>9}")
    print("-" * 112)
    for k in ks:
        for rep in arms:
            for h in HORIZONS:
                m = pd.DataFrame(results[(k, rep, h)]["folds"]).mean(numeric_only=True)
                print(f"{k:>2}{rep:<21}{h:>4}{m['train_acc']:>8.3f}{m['acc']:>8.3f}"
                      f"{m['base']:>8.3f}{m['lift']:>7.3f}{m['macro_f1']:>9.3f}"
                      f"{m['qwk']:>8.3f}{m['qwk_shuffled']:>9.3f}"
                      f"{m['mcc']:>8.3f}{m['mcc_shuffled']:>9.3f}")
        print("-" * 112)

    print("\n⭐ PAIRED A — full doc minus lead, same folds (does reading it all help?):")
    print(f"  {'k':>2}{'h':>4}{'Δacc':>10}{'ΔQWK':>10}{'ΔMCC':>10}{'folds won':>12}")
    for k in ks:
        for h in HORIZONS:
            a = pd.DataFrame(results[(k, REP_FULL, h)]["folds"])
            b = pd.DataFrame(results[(k, REP_LEAD, h)]["folds"])
            n = min(len(a), len(b))
            d_q = a["qwk"][:n] - b["qwk"][:n]
            print(f"  {k:>2}{h:>4}{(a['acc'][:n] - b['acc'][:n]).mean():>+10.4f}"
                  f"{d_q.mean():>+10.4f}{(a['mcc'][:n] - b['mcc'][:n]).mean():>+10.4f}"
                  f"{f'{int((d_q > 0).sum())}/{n}':>12}")

    print("\n⭐ PAIRED B — 3 bands minus 5 bands, full doc, same folds:")
    print("  ⚠️ Δacc is MEANINGLESS here — the base rate moves 0.200 → 0.333 on its own.")
    print("     Read ΔQWK and ΔMCC; both are corrected for the class prior.")
    print(f"  {'h':>4}{'Δacc':>10}{'Δlift':>9}{'ΔQWK':>10}{'ΔMCC':>10}{'folds won (MCC)':>18}")
    for h in HORIZONS:
        a = pd.DataFrame(results[(3, REP_FULL, h)]["folds"])
        b = pd.DataFrame(results[(5, REP_FULL, h)]["folds"])
        n = min(len(a), len(b))
        d_m = a["mcc"][:n] - b["mcc"][:n]
        print(f"  {h:>4}{(a['acc'][:n] - b['acc'][:n]).mean():>+10.4f}"
              f"{(a['lift'][:n] - b['lift'][:n]).mean():>+9.4f}"
              f"{(a['qwk'][:n] - b['qwk'][:n]).mean():>+10.4f}{d_m.mean():>+10.4f}"
              f"{f'{int((d_m > 0).sum())}/{n}':>18}")

    # ⚠️ Persist the fitted results BEFORE drawing. 120 walk-forward fits cost ~18 minutes;
    # a label collision in a chart should not cost that again.
    import pickle

    with open(CACHE / f"results_{tag}.pkl", "wb") as fh:
        pickle.dump({"results": results, "df": df, "ks": ks}, fh)

    print("\nCharts…")
    stats = chart_does_news_move_prices(
        df, px, df_full, OUT / "news_result_1_impact.png"
    )
    for k in ks:
        chart_model(results, df, OUT / f"news_result_2_model_{k}class.png", n_classes=k)
    chart_class_schemes(results, OUT / "news_result_3_class_schemes.png")
    write_examples(df, results, OUT / "news_result_examples.md", n_classes=3)

    mad_news, mad_quiet = stats.loc[True, "mad"], stats.loc[False, "mad"]
    rel_news, rel_quiet = stats.loc[True, "rel"], stats.loc[False, "rel"]
    acc5 = np.mean([f["acc"] for f in results[(5, REP_FULL, 5)]["folds"]])
    qwk5 = np.mean([f["qwk"] for f in results[(5, REP_FULL, 5)]["folds"]])
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
        + "".join(f"  → experiment_10/news_result_2_model_{k}class.png\n" for k in ks)
        + f"  → experiment_10/news_result_3_class_schemes.png\n"
        f"  → experiment_10/news_result_examples.md"
    )


if __name__ == "__main__":
    main()
