"""Tier-1 builder for `gold.news_weekly_panel` — one row per `(exchange, ticker, week)`.

Pure pandas, **no database**: the ingest does the two GROUP BYs in SQL (a 2.4M-row daily
table must never be pulled into pandas whole) and hands the aggregates in.

## Why WEEKLY, and not daily

Two independent reasons that happen to agree, which is why this is not a taste call:

* **Paper 57 (Heston & Sinha, FAJ 2017).** Daily news predicts 1-2 days — Day 1 +0.17%
  (t=9.8), Day 2 +0.04% (t=2.5), Day 3 t=1.2 and gone. WEEKLY news predicts **13 weeks**
  (+2.15%, t=8.2). Their Fig. 3 gives the mechanism: the daily cross-sectional sentiment
  percentiles swing too wildly to rank on, the weekly ones are stable.
* **This corpus.** Measured on `silver.cafef_news`, 2015 onward: editorials cover
  **1.6% of ticker-DAYS** against **8.7% of ticker-WEEKS** (top-30 tickers: 12.2% → 51.7%).
  A daily feature would be NaN on 98.4% of the panel — not useless, absent.

## Why the spine is PRICE, not news

Every `(ticker, week)` the stock traded gets a row, with `if_news = 0` where there was no
article. Paper 28 dropped no-news days and broke series continuity; worse, dropping them
throws away the thing paper 57 actually measures — covered stocks outperform uncovered
ones by **2.24%/week** in small caps *regardless of tone*. `if_news` is a feature, not a
missingness indicator.

## What is deliberately NOT here

No sentiment. This is the minimal panel that guidance.md §8 step 5-6 calls for: it exists
so the costed walk-forward can be run on `if_news`/`n_docs` alone, **before** any NLP work,
to establish the baseline that a sentiment block would later have to beat.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

#: `category` → the event-count column it feeds. The disclosure stream is not sentiment
#: (mean length 288 chars of filing boilerplate) but it IS an event calendar, and paper 57
#: Table 7 finds the delayed news response concentrates in earnings weeks (5.57%, t=2.66)
#: and is insignificant outside them.
CATEGORY_COLUMNS = {
    "business_results_and_analysis": "n_earnings",
    "major_and_insider_shareholder_transactions": "n_insider_txn",
    "dividends_and_record_date": "n_dividend",
    "personnel_changes": "n_personnel",
    "capital_increase_and_treasury_shares": "n_capital",
    "general_uncategorized": "n_uncategorized",
}

#: Momentum lookbacks in WEEKS. `mom_12w` is not optional: paper 57's news strategy
#: correlates **0.80** with momentum (their Table 5A), so without it no one can tell a
#: news effect from a momentum effect.
MOMENTUM_WEEKS = (1, 4, 12, 26)

#: ⚠️ The panel starts here, and it is a DATA-QUALITY cut, not a preference.
#:
#: `bronze.cafef_news` has a **six-month hole at ~98% dropout**, 2012-06 → 2012-11:
#: 37 / 35 / 24 / 23 / 24 / 20 rows a month against 600-1,600 either side, across 458 of
#: the 464 tickers that had news in 2011. A hole in the news feed reads as `if_news = 0`,
#: which is exactly the feature this panel exists to measure — so those months would not
#: look like missing data, they would look like *evidence that nothing was published*.
#:
#: The pre-hole years are thin as well (441-464 tickers with any news against 700+ from
#: 2017), so the cut is placed after it rather than around it.
#:
#: ⚠️ Applied AFTER the momentum columns are computed, so the first weeks of 2013 carry a
#: full 26-week lookback instead of NaN. Filtering the input instead would hand the
#: baseline model a half-year of missing controls and quietly understate it.
PANEL_START = "2013-01-01"

NEWS_FEATURES = [
    "if_news", "if_editorial", "n_docs", "n_days", "n_editorial", "n_disclosure",
    "n_docs_named", "relevance_max",
    "n_earnings", "n_insider_txn", "n_dividend", "n_personnel", "n_capital",
    "n_uncategorized", "if_earnings_week",
]
CONTROL_FEATURES = ["ret_w", "log_value_w", "sessions"] + [f"mom_{w}w" for w in MOMENTUM_WEEKS]


def _week_start(dates: pd.Series) -> pd.Series:
    """ISO week → its Monday, as a date. Matches Postgres `date_trunc('week', …)`."""
    d = pd.to_datetime(dates)
    return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()


def aggregate_news_weekly(news: pd.DataFrame) -> pd.DataFrame:
    """`silver.cafef_news` → per `(exchange, ticker, week)` counts.

    ⚠️ Grouped on **`trading_date`**, never on `timestamp`. `trading_date` already answers
    "which session can this article first act on" (09:00 ICT boundary, date-only stamps
    rolled forward); grouping on the raw timestamp would put Friday-evening news into the
    week that has already finished trading.
    """
    df = news.copy()
    df["week_start"] = _week_start(df["trading_date"])
    df["is_editorial"] = df["is_editorial"].astype(bool)
    df["has_ticker"] = df["has_ticker"].astype(bool)
    df["relevance_score"] = pd.to_numeric(df["relevance_score"], errors="coerce").fillna(0.0)

    for category, column in CATEGORY_COLUMNS.items():
        df[column] = (df["category"] == category).astype(int)

    agg = (
        df.groupby(["exchange", "ticker", "week_start"])
        .agg(
            n_docs=("row_id", "size"),
            n_days=("trading_date", "nunique"),
            n_editorial=("is_editorial", "sum"),
            n_docs_named=("has_ticker", "sum"),
            relevance_max=("relevance_score", "max"),
            **{col: (col, "sum") for col in CATEGORY_COLUMNS.values()},
        )
        .reset_index()
    )
    agg["n_disclosure"] = agg["n_docs"] - agg["n_editorial"]
    return agg


def build_news_weekly_panel(
    price_weekly: pd.DataFrame,
    news_weekly: pd.DataFrame,
    start: str | None = PANEL_START,
) -> pd.DataFrame:
    """Price spine LEFT JOIN news counts, + momentum controls.

    `price_weekly` — one row per `(exchange, ticker, week_start)` from `stocks_basic`,
    carrying `sessions`, `close_last`, `close_first`, `value_w`.
    `news_weekly` — the output of `aggregate_news_weekly`.
    """
    panel = price_weekly.copy()
    panel["week_start"] = pd.to_datetime(panel["week_start"]).dt.normalize()
    for col in ("close_last", "close_first", "value_w"):
        panel[col] = pd.to_numeric(panel[col], errors="coerce")
    panel["sessions"] = pd.to_numeric(panel["sessions"], errors="coerce").fillna(0).astype(int)

    news_weekly = news_weekly.copy()
    news_weekly["week_start"] = pd.to_datetime(news_weekly["week_start"]).dt.normalize()

    panel = panel.merge(news_weekly, on=["exchange", "ticker", "week_start"], how="left")

    count_cols = [
        "n_docs", "n_days", "n_editorial", "n_disclosure", "n_docs_named",
        *CATEGORY_COLUMNS.values(),
    ]
    for col in count_cols:
        panel[col] = panel[col].fillna(0).astype(int)
    panel["relevance_max"] = panel["relevance_max"].fillna(0.0).astype(float)

    # ⚠️ if_news = 0 is a FEATURE (paper 57's publication effect), not missing data.
    panel["if_news"] = (panel["n_docs"] > 0).astype(int)
    panel["if_editorial"] = (panel["n_editorial"] > 0).astype(int)
    panel["if_earnings_week"] = (panel["n_earnings"] > 0).astype(int)

    # ── controls, all computed from CLOSED weeks only ────────────────────────────────
    panel = panel.sort_values(["exchange", "ticker", "week_start"], kind="mergesort")
    grp = panel.groupby(["exchange", "ticker"], sort=False)["close_last"]
    panel["ret_w"] = grp.pct_change(1)
    for weeks in MOMENTUM_WEEKS:
        panel[f"mom_{weeks}w"] = grp.pct_change(weeks)
    panel["log_value_w"] = np.log1p(panel["value_w"].clip(lower=0))

    # ⚠️ The cut goes here, AFTER momentum — see PANEL_START.
    if start is not None:
        panel = panel[panel["week_start"] >= pd.Timestamp(start)]

    return panel.reset_index(drop=True)


def grain_violations(panel: pd.DataFrame) -> int:
    """Duplicate `(exchange, ticker, week_start)` rows. Must be 0 — a feature build adds
    COLUMNS, never ROWS, and a silently fanned-out merge looks like a bigger, better table.
    """
    return int(panel.duplicated(subset=["exchange", "ticker", "week_start"]).sum())
