"""Tier-1 cleaning for `bronze.cafef_news` → the shape `silver.cafef_news` stores.

Pure pandas, **no database**: the ingest reads the tables and hands frames in, exactly
like `sentiment_features`. The trading calendar is a PARAMETER, never a query from here.

What this does, and why each step exists (see
`experiment/experiment_10/guidance.md` §4.2 for the full rationale):

1. **Drop `type='error'`** (581 rows) and empty content — they are scrape failures, not news.
2. **De-duplicate** on `(ticker, trading_date, normalised headline)`. CafeF republishes the
   same story; the URL differs so the bronze `row_id` does not catch it.
3. **Strip boilerplate** — Word-export residue (`Normal 0 false false false EN-US …`), the
   `- File đính kèm: x.pdf` stub and the `Theo HOSE` sign-off. On disclosures that residue
   is most of the "content".
4. **Resolve the timestamp.** 22.2% of bronze rows are stamped exactly `00:00:00` — date
   only, no time (89,639 disclosures, 137 errors, **59 editorials**). They are flagged, not
   guessed.
5. **⚠️ Assign `trading_date` — the look-ahead guard, and the point of the whole module.**
   An article belongs to the first session whose OPEN comes after it, on a 09:00 ICT
   boundary. 65.5% of this corpus publishes outside 09:00-15:00 (the mode is 17:00), so
   the calendar-day assignment used by papers 9/28/43/44/46/48 would drop post-close news
   into the same row as that day's close. That is the defect that disqualifies papers 46,
   47 and 50. A date-only timestamp is treated as **end of day** → next session, which is
   the conservative direction: it can only ever delay information, never advance it.
6. **Relevance** — how often the ticker is actually named in the text. 45% of editorials
   are `general_uncategorized` and CafeF tags market-wide pieces to individual tickers.
   Paper 49 found this flag measurably helps; paper 57 filters relevance below 35%.

Text scoring inputs (segmentation, lead extraction) are deliberately NOT built here —
they need VnCoreNLP and belong with the scorer, which does not exist yet.
"""

from __future__ import annotations

import re
import unicodedata

import numpy as np
import pandas as pd

# ── constants ────────────────────────────────────────────────────────────────────────

#: Session open, ICT. An article at or after this on a session day cannot inform that
#: session's open, so it rolls to the next one.
SESSION_OPEN_HOUR = 9

#: ⚠️ Maximum calendar days an article may wait for its session before it is DROPPED.
#:
#: Without this, `searchsorted` maps every article published before the calendar starts
#: onto the calendar's FIRST day. Measured on the live tables: `silver.stocks_basic`
#: begins 2009-01-02 while the news goes back to 2007-02-23, and the first run of this
#: module piled **4,841 articles onto 2009-01-02** — a fake news spike on day one, in a
#: column (`n_docs`) whose whole purpose is to count news.
#:
#: 15 days is chosen against the real distribution, not by taste. The legitimate long
#: gaps are **Tết**, which shuts HOSE for up to ~9 days and produces the genuine
#: pile-ups (2026-02-02: 761 articles, 2025-02-04: 726, 2020-01-30: 627). The longest
#: observed legitimate gap is 11 days; the illegitimate ones sit at 155-302. Anything
#: past a fortnight is a calendar hole, not a holiday.
MAX_SESSION_GAP_DAYS = 15

#: Rows carrying no usable signal at all.
DROP_TYPES = ("error",)

#: Word-export residue and filing sign-offs. Ordered: the widest pattern first.
_BOILERPLATE = [
    # "Normal 0 false false false EN-US X-NONE X-NONE MicrosoftInternetExplorer4"
    re.compile(r"Normal\s+0\s+false\s+false\s+false.*?(?:MicrosoftInternetExplorer\d*|X-NONE)\s*", re.I | re.S),
    re.compile(r"<!--.*?-->", re.S),
    re.compile(r"-?\s*File\s+đính\s+kèm\s*:.*?\.pdf", re.I),
    re.compile(r"\b(?:Theo\s+)?(?:HOSE|HNX|UPCOM|HSX)\s*$", re.I),
    re.compile(r"\s+"),  # ⚠️ must stay LAST — collapses the gaps the others leave
]

#: Bare 3-4 char ticker as a standalone token, e.g. "VCB" but not "VCBS" or "aVCB".
_TICKER_RE_CACHE: dict[str, re.Pattern] = {}


# ── text helpers ─────────────────────────────────────────────────────────────────────


def strip_boilerplate(text: str | float) -> str:
    """Remove Word-export residue, attachment stubs and exchange sign-offs."""
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return ""
    out = str(text)
    for pat in _BOILERPLATE:
        out = pat.sub(" ", out)
    return out.strip()


def normalise_headline(text: str | float) -> str:
    """Dedup key: strip accents, punctuation and case so republished variants collide."""
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return ""
    s = unicodedata.normalize("NFD", str(text))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^0-9a-zA-Z]+", " ", s.lower())
    return s.strip()


def _ticker_pattern(ticker: str) -> re.Pattern:
    if ticker not in _TICKER_RE_CACHE:
        _TICKER_RE_CACHE[ticker] = re.compile(rf"(?<![0-9A-Za-z]){re.escape(ticker)}(?![0-9A-Za-z])")
    return _TICKER_RE_CACHE[ticker]


def count_ticker_mentions(text: str, ticker: str) -> int:
    """How many times the bare ticker code appears as a standalone token.

    ⚠️ Case-SENSITIVE on purpose. VN codes are 3 uppercase letters and several collide
    with ordinary lowercase words once case is folded (`bid`, `sam`, `tip`, `has`).
    This measures naming, not topicality — a piece can be about a company without
    printing its code, and that is a known limitation of the flag.
    """
    if not text or not ticker:
        return 0
    return len(_ticker_pattern(ticker).findall(text))


# ── timestamp / calendar ─────────────────────────────────────────────────────────────


def resolve_timestamps(ts: pd.Series) -> tuple[pd.Series, pd.Series]:
    """→ (`ts_resolved`, `ts_is_date_only`). Nothing is invented: a midnight stamp is
    flagged, and downstream it is treated as end-of-day rather than as 00:00."""
    resolved = pd.to_datetime(ts, errors="coerce")
    date_only = (
        (resolved.dt.hour == 0) & (resolved.dt.minute == 0) & (resolved.dt.second == 0)
    ).fillna(False)
    return resolved, date_only


def assign_trading_date(
    ts_resolved: pd.Series,
    is_date_only: pd.Series,
    sessions: np.ndarray,
    open_hour: int = SESSION_OPEN_HOUR,
    max_gap_days: int = MAX_SESSION_GAP_DAYS,
) -> pd.Series:
    """First session whose OPEN falls after the article. The look-ahead guard.

    `sessions` — sorted unique trading dates (`datetime64[D]`), from `silver.stocks_basic`.

    An article published at `08:00` on a session day can inform that day's open, so it
    maps to that day. One at `10:00`, `17:00`, or on a weekend maps to the NEXT session.
    A date-only stamp is treated as end-of-day → next session (conservative).

    Returns **NaT** in two cases, and the caller must drop both:

    * past the END of the calendar — no forward window exists yet (1,753 rows: the news
      scrape runs ahead of the price scrape);
    * more than `max_gap_days` before the next session — see `MAX_SESSION_GAP_DAYS`.
      This is what stops pre-calendar articles from being swept onto day one.
    """
    sessions = np.asarray(sessions, dtype="datetime64[D]")
    day = ts_resolved.dt.normalize().to_numpy(dtype="datetime64[D]")

    # `left`  → a session ON this day still qualifies (article precedes the open)
    # `right` → the open has passed (or the time is unknown); roll to the next session
    before_open = (ts_resolved.dt.hour < open_hour).to_numpy(dtype=bool) & ~is_date_only.to_numpy(dtype=bool)

    idx = np.where(
        before_open,
        np.searchsorted(sessions, day, side="left"),
        np.searchsorted(sessions, day, side="right"),
    )
    out = np.full(len(day), np.datetime64("NaT"), dtype="datetime64[D]")
    ok = idx < len(sessions)
    out[ok] = sessions[idx[ok]]

    # ⚠️ Reject sessions the article had to wait too long for. Tết (~9 days) survives;
    # a two-year jump onto the first day of the calendar does not.
    gap = (out - day).astype("timedelta64[D]").astype(float)
    out[np.isfinite(gap) & (gap > max_gap_days)] = np.datetime64("NaT")

    return pd.Series(pd.to_datetime(out), index=ts_resolved.index)


# ── the entry point ──────────────────────────────────────────────────────────────────


def clean_news(news: pd.DataFrame, sessions: np.ndarray) -> pd.DataFrame:
    """`bronze.cafef_news` → the `silver.cafef_news` shape. Pure; no I/O.

    `sessions` — sorted unique trading dates from `silver.stocks_basic`.
    """
    df = news.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for col in ("headline", "content", "type", "category", "ticker", "exchange"):
        if col in df:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # 1 ── drop scrape failures
    n_in = len(df)
    df = df[~df["type"].isin(DROP_TYPES)]
    n_error = n_in - len(df)

    # 3 ── boilerplate (before dedup: it is part of what makes rows look different)
    df["content_clean"] = df["content"].map(strip_boilerplate)
    df["headline"] = df["headline"].map(strip_boilerplate)
    df["content_len"] = df["content_clean"].str.len()

    # 1b ── an article with neither headline nor body carries nothing
    before = len(df)
    df = df[(df["content_len"] > 0) | (df["headline"].str.len() > 0)]
    n_empty = before - len(df)

    # 4 ── timestamps
    df["ts_resolved"], df["ts_is_date_only"] = resolve_timestamps(df["timestamp"])
    df = df.dropna(subset=["ts_resolved"])

    # 5 ── ⚠️ the look-ahead guard
    df["trading_date"] = assign_trading_date(
        df["ts_resolved"], df["ts_is_date_only"], sessions
    )
    before = len(df)
    df = df.dropna(subset=["trading_date"])
    n_no_session = before - len(df)
    max_gap = (
        (df["trading_date"] - df["ts_resolved"].dt.normalize()).dt.days.max()
        if len(df)
        else 0
    )

    # 2 ── de-duplicate republished stories
    df["headline_key"] = df["headline"].map(normalise_headline)
    df = df.sort_values(["ticker", "ts_resolved", "news_order"], kind="mergesort")
    before = len(df)
    df = df.drop_duplicates(subset=["ticker", "trading_date", "headline_key"], keep="first")
    n_dup = before - len(df)

    # 6 ── relevance
    df["ticker_hits"] = [
        count_ticker_mentions(f"{h} {c}", t)
        for h, c, t in zip(df["headline"], df["content_clean"], df["ticker"])
    ]
    df["has_ticker"] = df["ticker_hits"] > 0
    # per 1,000 characters, so a long market wrap that names the ticker once scores low
    df["relevance_score"] = (
        df["ticker_hits"] / ((df["content_len"] + df["headline"].str.len()) / 1000.0).clip(lower=1.0)
    ).astype(float)

    df["is_editorial"] = df["type"].eq("editorial")

    out = df[
        [
            "row_id", "exchange", "ticker", "news_order",
            "timestamp", "ts_resolved", "ts_is_date_only", "trading_date",
            "type", "category", "is_editorial",
            "headline", "content_clean", "content_len",
            "ticker_hits", "has_ticker", "relevance_score",
            "url",
        ]
    ].reset_index(drop=True)

    out.attrs["dropped"] = {
        "error_rows": n_error,
        "empty_rows": n_empty,
        "no_session_or_gap": n_no_session,
        "duplicates": n_dup,
    }
    out.attrs["max_session_gap_days"] = int(max_gap)
    return out


def leakage_violations(clean: pd.DataFrame, open_hour: int = SESSION_OPEN_HOUR) -> pd.DataFrame:
    """Rows whose `trading_date` is the SAME day the article published while the article
    landed at/after the open. Must be empty — the ingest asserts on it.

    This is the check that papers 46, 47 and 50 fail.
    """
    same_day = clean["trading_date"].dt.normalize() == clean["ts_resolved"].dt.normalize()
    late = (clean["ts_resolved"].dt.hour >= open_hour) | clean["ts_is_date_only"]
    return clean[same_day & late]
