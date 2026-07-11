# Experiment 5 — VCB shares-outstanding (KLCP) point-in-time history

**Goal.** Recover VCB's **listed / outstanding share count** ("KLCP đang niêm yết") as a
point-in-time series, 2009 → present, so raw price can be joined to **market cap, turnover
and free-float** without look-ahead.

## ⚠️ The original method was wrong — and this is why

The obvious approach (what this experiment did first): anchor on today's share count and walk
CafeF's corporate-action log (`LichSuKien.ashx`) **backwards**, undoing each stock dividend /
bonus / rights issue / placement.

**That log is incomplete.** For VCB it omits **three 2010–2012 capital increases**. Once
experiment_7 gave us VCB's own filed balance sheets, the error was obvious:

| date | **filed** (truth) | old reconstruction | error |
|---|---:|---:|---|
| pre-2010 base | 1,210,086,026 | 1,742,397,291 | **+44%** |
| mid-2011 | 1,758,754,000 | 2,317,388,397 | **+31.8%** |

The old series silently **inflated any pre-2013 market cap by ~30–45%**. (It happened to be
right from mid-2014 on, which is why a spot-check against post-2016 filings passed.)

## The method now: filed charter capital

The authoritative source is the company's own **charter capital** ("Vốn điều lệ",
balance-sheet code `411`), read straight from the quarterly financial statements via the same
CafeF BCTC API experiment_7 uses:

```
shares_outstanding = charter_capital / 10,000        (10,000 VND par — the VN standard)
```

Complete and filing-backed, for all **65 filed quarters** (Q1-2009 → Q1-2026).

The corporate-action log is still fetched, but **only to date and label** each step — an
ex-date is more precise than "sometime in this quarter".

**Dating rule (no look-ahead).** If an action's factor matches the observed jump and its
ex-date falls within ~15 months before the quarter-end, use that exact **ex-date** (charter
capital registers only once shares are actually issued, which lags the ex-date — the
2010-12-13 rights issue first appears in the Q1-2011 balance sheet). Otherwise fall back to
the **quarter-end** on which the new charter capital first appears — conservative, since the
increase is applied no earlier than the first date it is provably true.

## Result

| effective | period | shares outstanding | change | event | dating |
|---|---|---:|---:|---|---|
| 2009-03-31 | Q1-2009 | 1,210,086,026 | — | baseline | quarter-end |
| 2010-09-30 | Q3-2010 | 1,322,371,500 | +9.3% | **unlogged** capital increase | quarter-end |
| 2010-12-13 | Q1-2011 | 1,758,754,000 | +33.0% | rights issue 100:33 | ex-date |
| 2011-09-30 | Q3-2011 | 1,969,804,500 | +12.0% | **unlogged** capital increase | quarter-end |
| 2012-03-31 | Q1-2012 | 2,317,417,100 | +17.6% | **unlogged** (Mizuho placement) | quarter-end |
| 2014-06-17 | Q3-2014 | 2,665,020,300 | +15.0% | bonus 100:15 | ex-date |
| 2016-09-08 | Q3-2016 | 3,597,768,600 | +35.0% | bonus 100:35 | ex-date |
| 2019-01-03 | Q1-2019 | 3,708,877,400 | +3.1% | private placement (GIC/Mizuho) | ex-date |
| 2021-12-21 | Q1-2022 | 4,732,516,600 | +27.6% | stock dividend 1000:276 | ex-date |
| 2023-07-24 | Q3-2023 | 5,589,091,300 | +18.1% | stock dividend 1000:181 | ex-date |
| 2025-03-11 | Q1-2025 | **8,355,675,094** | +49.5% | stock dividend 1000:495 | ex-date |

The three **`unlogged_capital_increase`** rows are exactly the events CafeF's action log is
missing — they are real (the filings prove them), just undated beyond their quarter.

## Files

- `scrape_vcb_shares_outstanding.py` — one stdlib script, no login.
- `vcb_shares_outstanding.csv` — the step series: `effective_date, period,
  shares_outstanding, charter_capital, prev_shares, delta_shares, pct_change, event_type,
  event_text, dating`.
- `vcb_corporate_actions.csv` — CafeF's raw action log (kept for the dating/labelling, and as
  evidence of what it does *not* contain).
- `vcb_shares_milestones.csv` — optional exact-count pins, keyed by `period`. Charter capital
  is filed in **millions** of VND, so `charter / 10,000` is only good to ~±50 shares; the
  current count is pinned here to CafeF's exact KLCP (8,355,675,094).

## Point-in-time lookup

`shares_outstanding(d)` = the last row with `effective_date ≤ d`. Then

```
market_cap(d) = raw_close(d) × shares_outstanding(d)
```

Use the **raw** (unadjusted) close — the share count already carries the dilution.

> For the three quarter-end-dated steps, the true effective date is somewhere inside that
> quarter, so pre-2013 daily market cap can be off for up to ~90 days around each. Levels are
> correct from each quarter-end onward.
