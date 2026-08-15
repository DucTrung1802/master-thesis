"""Feature engineering + point-in-time splitting for the sentiment→price model.

Pure pandas (no DB): the runner reads the tables and hands frames in. Two jobs:

1. **`build_event_panel`** — turn per-article sentiment + daily prices into a
   per-`(ticker, date)` modelling panel with (a) trailing-window **sentiment
   features**, (b) **price / TA / foreign-flow** features, and (c) the two
   **targets** — `close_fwd` = `close_adjust[t+H]` (the actual N+H close) and
   `up_fwd` = 1{close_fwd > close_adjust}. Only rows that have news in the trailing
   window are kept (the premise is "predict from this week's news").

2. **`purged_walkforward_folds`** — yield (train, test) index masks for a purged,
   embargoed, walk-forward split. ⚠️ This is the whole point-in-time story: the
   target at day `t` peeks `H` days ahead, so training must use only events whose
   forward window **closed before** the fold cut, an **embargo** of `H` days is
   dropped around the cut, and the test set is strictly after it. Sliding the cut
   across several dates and averaging is what makes the out-of-sample number
   trustworthy (a single split can be lucky).

⚠️ Predicting an **absolute price level** N days out is a random-walk problem —
`close[t+H] ≈ close[t]`. The honest benchmark is therefore `close[t+H] = close[t]`
(carried as `close_adjust` on every test row); a model earns its keep only by
beating that. This module just builds inputs — `price_predictor` does the scoring
against those baselines.
"""

from __future__ import annotations

from typing import Iterator, List, Tuple

import numpy as np
import pandas as pd

# The 5 sentiment levels ↔ ordinal intensity, shared with the scorer's bins.
LEVEL_INTENSITY = {
    "VERY_NEGATIVE": -2,
    "NEGATIVE": -1,
    "NEUTRAL": 0,
    "POSITIVE": 1,
    "VERY_POSITIVE": 2,
}
# Signed-score cut points → intensity (same thresholds the 5-level labelling uses).
_SCORE_BINS = [-1.01, -0.6, -0.2, 0.2, 0.6, 1.01]
_SCORE_INTENSITY = [-2, -1, 0, 1, 2]

DEFAULT_HORIZON = 5  # trading days → close[N+5]
DEFAULT_TRAIL_WEEK = 5  # trailing trading days that count as "this week's news"

# The feature blocks the panel exposes (also used by price_predictor for ablation).
SENTIMENT_FEATURES = [
    "sent_mean_week",
    "sent_sum_week",
    "news_week",
    "very_pos_week",
    "very_neg_week",
]
PRICE_FEATURES = [
    "close_adjust",
    "ret1",
    "ret5",
    "ret20",
    "vol20",
    "sma5_gap",
    "sma20_gap",
    "hl_range",
    "rsi14",
    "mom10",
    "vol_ratio",
    "foreign_net_val_ratio",
    "foreign_buy_ratio",
    "order_imbalance",
]


def score_to_intensity(score: pd.Series) -> pd.Series:
    """Signed sentiment score in [-1, 1] → ordinal intensity in {-2..+2}."""
    binned = pd.cut(score, _SCORE_BINS, labels=_SCORE_INTENSITY)
    return binned.astype("float").astype("Int64")


def _daily_sentiment(sent: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-article intensity to one row per `(ticker, calendar-day)`."""
    sent = sent.copy()
    sent["intensity"] = score_to_intensity(
        pd.to_numeric(sent["sentiment_score"], errors="coerce")
    ).astype(float)
    sent["day"] = pd.to_datetime(sent["timestamp"], errors="coerce").dt.normalize()
    return (
        sent.dropna(subset=["day"])
        .groupby(["ticker", "day"])
        .agg(
            sent_mean=("intensity", "mean"),
            sent_sum=("intensity", "sum"),
            news_n=("intensity", "size"),
            very_pos=("intensity", lambda s: float((s == 2).mean())),
            very_neg=("intensity", lambda s: float((s == -2).mean())),
        )
        .reset_index()
    )


def _price_technical(
    g: pd.DataFrame, horizon: int, jump_threshold: float = 0.05
) -> pd.DataFrame:
    """Per-ticker price/TA/foreign features + the forward targets. `g` is one ticker's
    daily rows, ascending by date. `jump_threshold` defines the `jump_fwd` label."""
    g = g.sort_values("date").reset_index(drop=True)
    c = g["close_adjust"]

    # ── targets ──
    g["close_fwd"] = c.shift(-horizon)  # close[t+H]
    fwd_ret = g["close_fwd"] / c - 1.0
    g["up_fwd"] = (g["close_fwd"] > c).astype("float")
    # The headline target: 1 if the close rises >= threshold within `horizon` days.
    # NaN where the forward window is not yet mature (so it drops, never a false 0).
    g["jump_fwd"] = np.where(fwd_ret.isna(), np.nan, (fwd_ret >= jump_threshold).astype(float))

    # ── returns / trend / momentum ──
    g["ret1"] = c.pct_change(1)
    g["ret5"] = c.pct_change(5)
    g["ret20"] = c.pct_change(20)
    g["vol20"] = c.pct_change().rolling(20).std()
    g["sma5_gap"] = c.rolling(5).mean() / c - 1
    g["sma20_gap"] = c.rolling(20).mean() / c - 1
    g["hl_range"] = (g["high"] - g["low"]) / c
    g["mom10"] = c / c.shift(10) - 1

    # RSI(14)
    delta = c.diff()
    up = delta.clip(lower=0).rolling(14).mean()
    down = (-delta.clip(upper=0)).rolling(14).mean()
    g["rsi14"] = 100 - 100 / (1 + up / down.replace(0, np.nan))

    # volume / foreign / microstructure — computed only when the source columns are
    # present, so a caller needing just the target (e.g. the sentiment-only jump model)
    # can pass a minimal price set without the foreign/order columns.
    if "volume_matched" in g:
        vol = g["volume_matched"]
        g["vol_ratio"] = vol / vol.rolling(20).mean()
    if {"foreign_net_value", "value_matched"} <= set(g.columns):
        # ⚠️ `value_matched` is BILLIONS of VND, `foreign_net_value` is plain VND — a
        # CafeF API inconsistency carried faithfully through bronze and silver. This
        # divided one by the other until 2026-08-16 and was 1e9 too large; the same
        # bug, found first in `ta.ta_functions.add_foreign_net_val_ratio`, which
        # carries the measurement. `foreign_buy_ratio` below needs no fix — both its
        # terms are foreign VND, so the unit cancels.
        #
        # ⚠️ The 1e9 is INLINED rather than imported from `ta.ta_functions`, which
        # owns the canonical `VALUE_MATCHED_VND_SCALE`: that module imports TA-Lib,
        # and this one is deliberately importable by a sentiment-only model that has
        # no TA dependency. If the scale ever changes, both places change.
        val = (g["value_matched"] * 1e9).replace(0, np.nan)
        g["foreign_net_val_ratio"] = g["foreign_net_value"] / val
    if {"foreign_buy_value", "foreign_sell_value"} <= set(g.columns):
        fb, fs = g["foreign_buy_value"], g["foreign_sell_value"]
        g["foreign_buy_ratio"] = fb / (fb + fs).replace(0, np.nan)
    if {"n_buy_orders", "n_sell_orders"} <= set(g.columns):
        nb, ns = g["n_buy_orders"], g["n_sell_orders"]
        g["order_imbalance"] = (nb - ns) / (nb + ns).replace(0, np.nan)
    return g


def build_event_panel(
    sent: pd.DataFrame,
    px: pd.DataFrame,
    horizon: int = DEFAULT_HORIZON,
    trail_week: int = DEFAULT_TRAIL_WEEK,
    jump_threshold: float = 0.05,
) -> pd.DataFrame:
    """Assemble the per-`(ticker, date)` modelling panel.

    `sent` needs `ticker`, `timestamp`, `sentiment_score`; `px` needs `ticker`,
    `date`, and the OHLC/volume/foreign/order columns of `silver.stocks_basic`.
    Returns rows that have news in the trailing `trail_week` days and a mature
    forward target, with `SENTIMENT_FEATURES + PRICE_FEATURES`, the targets
    `close_fwd`/`up_fwd`/`jump_fwd` (the last = 1 if close rises >= `jump_threshold`
    within `horizon` days), and `ticker`/`date`."""
    daily = _daily_sentiment(sent)
    px = px.copy()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    for col in px.columns:
        if col not in ("ticker", "date"):
            px[col] = pd.to_numeric(px[col], errors="coerce")

    out: List[pd.DataFrame] = []
    for ticker, g in px.groupby("ticker"):
        g = _price_technical(g, horizon, jump_threshold=jump_threshold)
        d = daily[daily["ticker"] == ticker].drop(columns="ticker")
        gg = g.merge(d, left_on="date", right_on="day", how="left")

        # Trailing-window ("this week") sentiment. Sums over news-days only; a day
        # with no news contributes 0 to the sum/count and NaN to the mean.
        gg["sent_sum_week"] = gg["sent_sum"].fillna(0).rolling(trail_week, min_periods=1).sum()
        gg["news_week"] = gg["news_n"].fillna(0).rolling(trail_week, min_periods=1).sum()
        gg["sent_mean_week"] = gg["sent_mean"].rolling(trail_week, min_periods=1).mean()
        gg["very_pos_week"] = gg["very_pos"].fillna(0).rolling(trail_week, min_periods=1).mean()
        gg["very_neg_week"] = gg["very_neg"].fillna(0).rolling(trail_week, min_periods=1).mean()
        gg["ticker"] = ticker
        out.append(gg)

    panel = pd.concat(out, ignore_index=True)
    panel = panel[panel["news_week"] > 0]  # premise: has this-week news
    panel = panel.dropna(subset=["close_fwd", "close_adjust"]).reset_index(drop=True)

    # Fill/keep only the feature columns that were actually produced (a minimal price
    # set omits the foreign/order-book features — fine for a sentiment-only model).
    feats = [c for c in SENTIMENT_FEATURES + PRICE_FEATURES if c in panel.columns]
    panel[feats] = panel[feats].astype(float).fillna(0.0)
    keep = ["ticker", "date", "close_fwd", "up_fwd", "jump_fwd"] + feats
    return panel[keep]


def purged_walkforward_folds(
    panel: pd.DataFrame,
    cut_dates: List[str],
    horizon: int = DEFAULT_HORIZON,
    test_window_days: int = 120,
) -> Iterator[Tuple[pd.Timestamp, np.ndarray, np.ndarray]]:
    """Yield `(cut, train_mask, test_mask)` for each cut in `cut_dates`.

    Train = events whose forward window closed on/before the cut minus the embargo
    (`cut − horizon` days); test = events in `(cut, cut + test_window_days]`. The
    `horizon`-day embargo between them is dropped (neither train nor test), so a
    training label never peeks past the cut. Cuts that leave too little data are
    still yielded — the caller decides the minimum."""
    dates = pd.to_datetime(panel["date"])
    embargo = pd.Timedelta(days=horizon)
    for cut in cut_dates:
        cut_ts = pd.Timestamp(cut)
        train_mask = (dates <= (cut_ts - embargo)).to_numpy()
        test_mask = (
            (dates > cut_ts) & (dates <= cut_ts + pd.Timedelta(days=test_window_days))
        ).to_numpy()
        yield cut_ts, train_mask, test_mask
