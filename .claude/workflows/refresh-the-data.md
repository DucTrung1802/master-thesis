# Workflow — refresh the data

> **Goal:** move fresh prices from CafeF all the way to something a model reads, and *prove* it
> arrived. **Cost: ~1 h 05 m for the scrape at 780 tickers, plus the carry-up.**
>
> ⚠️ **A scrape that stops at `raw_data/` changes nothing a model reads, and nothing raises.**
> `CLAUDE.md` §5 rule 11. Bronze once sat a full day behind a completed scrape — 5 countries
> against 19 on disk. **Steps 4-6 are the workflow; step 3 is just the download.**

---

## 1. Find out whether it is actually stale — **O3**, **O4**

`python -m pipeline.freshness --layer silver`, then the per-ticker query.

⚠️ **`MAX(date)` cannot answer this and has lied at full scale.** It read 2026-08-19 from **five**
tickers while 757 of 781 were frozen; 599 stopped dead on one date. Every narrowly-scoped
re-scrape pushes that scalar further from the truth.

**Read the shape:** a **cliff** (many tickers on ONE date) is a scrape scope and needs this
workflow; **scatter** (largest group ~0.6 %) is delistings and needs nothing. Both regimes are
measured — 77 % against 0.6 % — and they are separated by two orders of magnitude of *share*,
never by a count.

## 2. Write `refresh.yaml` — the one line that decides whether anything happens

```yaml
ops:
  raw__cafef_price:        {config: {skip_existing: false, incremental: true}}
  raw__cafef_order_stats:  {config: {skip_existing: false, incremental: true}}
  raw__cafef_foreign:      {config: {skip_existing: false, incremental: true}}
  raw__cafef_prop_trading: {config: {skip_existing: false, incremental: true}}
```

⚠️ **`incremental: true` NEEDS `skip_existing: false` beside it.** `skip_existing` is checked
first and returns before the resume is ever reached — so `incremental: true` alone refreshes
**nothing** and still goes green. That is §5 rule 10's exact failure mode, in the config file.

**Why `incremental` at all:** a full 4-tab refetch of ONE ticker is **615 s** (~67 h for the
universe at the old 2-worker pool). Resuming from each CSV's own last date is **2.9-5.2 s**, and
the resumed file reproduces the full scrape **cell for cell**.

## 3. Scrape — **D1**

`Clear-Content logs\app.log` first.

⚠️ **Every scrape runs through Dagster — never a script, never ad-hoc code**, even for a one-off
backfill and even when the scraper class already has a batch method. A run outside Dagster leaves
no materialisation, no metadata and no partition status, so a later session cannot tell what was
fetched or with what scope. **If a run needs a knob the asset does not have, the work is adding
the knob to the asset.**

⚠️ **Never `Materialize all` / `*` / a bare backfill.** With every partition live that takes
`raw/cafef_pdfs` (**~555 GiB**), `raw/trading_view@stocks` (~10 h) and `raw/cafef_financials`
(~2.4 h/ticker).

## 4. Read the restatement warnings — **D3**

⚠️ **A restatement is a WARNING, not a failure, and it is the most important line in the log.**

`close_adjust` is not a fact about a day; it is a fact about a day **as seen from today**. A split
or dividend re-bases the WHOLE history, so appending fresh rows to stored ones splices two price
bases into one series — a step change at the join that looks exactly like a real price move and
that **no freshness check can see**: the row count is right, the date range is right, the last
date is today.

The resume refetches a **45-day overlap**, compares cell-by-cell and falls back to a full refetch
on any disagreement. ⚠️ **It fired on 304 of 780 tickers — 39 %** — because the corpus had stood
still through dividend season. **Those 304 are exactly what a naive incremental scrape would have
corrupted, invisibly.**

⚠️ A high restatement rate is a property of how STALE the corpus is, not of the mechanism.

## 5. Carry it up — **D2**, layer by layer

```
bronze/cafef_*  →  silver/*  →  silver/stocks_basic  →  gold/*  →  filter/universe  →  unified/pool__basic,pool__targets
```

⚠️ **Re-running a screen does NOT rebuild the unified schema.** The edge is deliberately not
declared (the two are partitioned on different sets and a Dagster dep is per-ASSET), so a fresh
screen against a stale schema raises nothing. This bit exactly as documented once: after
re-running `filter/universe`, three `unified_schema_*` still read the old date with **5 names on
the last session**.

⚠️ **A screen re-measures on fresh data, and that is the filter layer working** — one screen went
480 → 461 members and two others gained names. It changes which basket, not the fact that **a
screen is not point-in-time**.

## 6. Rebuild the single-name schemas — **D4**

⚠️ **Rule 14 from the other side, and it has been counted: 28 of the 30 single-name schemas were
found stale**, in three layers that were a fossil record of every scoped re-scrape the repo had
run. **None of it is visible to `MAX(date)`**, which read fresh on the schemas that *had* been
rebuilt and said nothing about the other 28.

⚠️ **D4 rebuilds 2 pools of 25.** `pool__bonds` and the 19 `pool__economy_*` stay on the old
calendar — `status_data` reports that as `pools_behind`, and a wide join over such a schema
INNER-joins back down to theirs.

## 7. Verify — **O3 again**, and per-ticker

⚠️ **Verify with the distribution, per ticker, never with one date.** The check that missed a
two-month freeze for two months was a scalar.

---

## Done when

- [ ] **O3** reports a **scatter**, not a cliff, and you can name why each straggler stopped
- [ ] the last session carries the number of names you expect — not 5, not 24
- [ ] gold and silver agree on their max date (a 30- and a 54-session gap have both happened)
- [ ] every single-name schema you care about has been through **D4**
- [ ] the measurement is written down — see [record-a-finding.md](record-a-finding.md)

## Traps

⚠️ **A green asset is NOT evidence of fresh data.** `landed()` answers *"is this folder empty?"*,
not *"did THIS run produce anything"*. A scrape can go green in 500 ms having fetched nothing — or
worse, having fetched *some* things: one forex run refreshed 29 series and left 328 stale.

⚠️ **`insider_txn` accepts `incremental` and ignores it** — it is paginated by event index with no
date to resume from.

⚠️ **`--rescrape` in `pipeline` is opt-in and scoped to `--ticker` with `skip_existing=False`.**
Without both it either costs 781 tickers or fetches nothing.

⚠️ **`SCRAPER_MAX_WORKERS` (CafeF, `requests`) is not `SCRAPER_MAX_CONCURRENT_BROWSERS`
(TradingView, Selenium).** They are unrelated; confusing them buys nothing.
