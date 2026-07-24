"""Price-reaction sentiment labels — define a news item's sentiment by **what the price
did after it**, not by a language model.

Motivation: the PhoBERT 3-class scorer in `sentiment_functions` is general-domain and
miscalibrated for finance (it reads dividend/bond filings as strongly negative). Grounding
the label in the market's own reaction removes that bias and makes "sentiment" mean
"news that preceded an up-move / down-move".

For each news event mapped to its trading day `t`, the forward return
`fwd = close_adjust[t+H]/close_adjust[t] - 1` is bucketed into 5 ordinal levels:

    VERY_NEGATIVE   fwd <  -limit
    NEGATIVE        -limit ≤ fwd < -neutral
    NEUTRAL         |fwd| ≤ neutral
    POSITIVE         neutral < fwd ≤  limit
    VERY_POSITIVE   fwd >  limit

⚠️ **The `limit` is exchange-specific** — the daily price band (biên độ): HOSE ±7 %,
HNX ±10 %, UPCoM ±15 %. Over an H-day window `limit` marks "more than one full limit day
of net move", a principled, microstructure-anchored cut for the extreme classes. `neutral`
(default ±2 %) is the noise dead-band. A news whose forward window has not matured yet
(the last H days) gets a NaN label and is excluded — never a false NEUTRAL.

⚠️ This label peeks H days ahead, so any model trained on it MUST use a purged,
point-in-time split (see `sentiment_features.purged_walkforward_folds`).
"""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

LEVELS = ["VERY_NEGATIVE", "NEGATIVE", "NEUTRAL", "POSITIVE", "VERY_POSITIVE"]
LEVEL_TO_INT = {l: i for i, l in enumerate(LEVELS)}  # 0..4, ordinal

# Exchange daily price limit (biên độ) — the "VERY" threshold over the horizon.
EXCHANGE_DAILY_LIMIT = {
    "HOSE": 0.07,
    "HSX": 0.07,   # alias sometimes used for HOSE
    "HNX": 0.10,
    "UPCOM": 0.15,
    "UPCOM_": 0.15,
    "UPCoM": 0.15,
}
DEFAULT_LIMIT = 0.07  # fall back to HOSE if the exchange is unknown
DEFAULT_NEUTRAL_BAND = 0.02  # ±2% dead-band for NEUTRAL
DEFAULT_HORIZON = 5


def exchange_limit(exchange: str) -> float:
    """Daily price band for an exchange (defaults to HOSE's ±7%)."""
    return EXCHANGE_DAILY_LIMIT.get(str(exchange).upper().strip(), DEFAULT_LIMIT)


def build_price_reaction_labels(
    news: pd.DataFrame,
    px: pd.DataFrame,
    horizon: int = DEFAULT_HORIZON,
    neutral_band: float = DEFAULT_NEUTRAL_BAND,
) -> pd.DataFrame:
    """Attach a 5-level price-reaction label to each news row.

    `news` needs `row_id`, `exchange`, `ticker`, `timestamp` (+ any text columns the
    caller keeps); `px` needs `ticker`, `date`, `close_adjust`. Each news is mapped to
    the FIRST trading day `>=` its calendar day (news after the close reacts next
    session), and labelled by the exchange-aware bands on the H-day forward return.

    Returns the input rows (news order preserved) plus:
      `event_date`  — the trading day the reaction is measured from,
      `fwd_return`  — close[t+H]/close[t] - 1,
      `price_level` — one of LEVELS (NaN-dropped where the window is immature),
      `price_level_id` — the ordinal 0..4.
    Rows with no mature forward return or no matching trading day are dropped."""
    px = px.copy()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    px["close_adjust"] = pd.to_numeric(px["close_adjust"], errors="coerce")
    px = px.sort_values(["ticker", "date"]).reset_index(drop=True)

    news = news.copy()
    news["news_date"] = pd.to_datetime(news["timestamp"], errors="coerce").dt.normalize()

    out = []
    for ticker, g in px.groupby("ticker"):
        g = g.reset_index(drop=True)
        g["fwd_return"] = g["close_adjust"].shift(-horizon) / g["close_adjust"] - 1.0
        dates = g["date"].to_numpy()
        if len(dates) == 0:
            continue
        n = news[news["ticker"] == ticker].copy()
        if n.empty:
            continue
        # first trading day on/after the news day
        idx = np.searchsorted(dates, n["news_date"].to_numpy(), side="left")
        keep = idx < len(dates)
        n = n[keep].copy()
        idx = idx[keep]
        n["event_date"] = dates[idx]
        n = n.merge(
            g[["date", "fwd_return"]], left_on="event_date", right_on="date", how="left"
        ).drop(columns="date")
        out.append(n)

    if not out:
        return news.iloc[0:0].assign(
            event_date=pd.NaT, fwd_return=np.nan, price_level=None, price_level_id=np.nan
        )

    ev = pd.concat(out, ignore_index=True).dropna(subset=["fwd_return"]).reset_index(
        drop=True
    )

    # Per-row bands (exchange-aware VERY threshold), then bucket the forward return.
    limit = ev["exchange"].map(exchange_limit).astype(float)
    fwd = ev["fwd_return"].astype(float)
    level = np.select(
        [
            fwd < -limit,
            (fwd >= -limit) & (fwd < -neutral_band),
            fwd.abs() <= neutral_band,
            (fwd > neutral_band) & (fwd <= limit),
            fwd > limit,
        ],
        LEVELS,
        default=None,
    )
    ev["price_level"] = level
    ev["price_level_id"] = ev["price_level"].map(LEVEL_TO_INT).astype("Int64")
    return ev


def label_distribution(ev: pd.DataFrame) -> pd.Series:
    """Count per level, in ordinal order (for a quick sanity print)."""
    return ev["price_level"].value_counts().reindex(LEVELS).fillna(0).astype(int)
