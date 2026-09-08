# State of the data — freshness, the carry-up, the filings, the audit

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§6-2`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **`CLAUDE.md` §6-2 through §6-3 in full** — the `pool__ta` prune, what exists right now, the
> 2026-08-23 re-scrape and its carry-up, the freshness distribution, `DEP-1`, the filings/OCR
> summary, and the data audit. ⚠️ **If a number here disagrees with the database, the database is
> right and this file is the bug.**

---

### ⚠️ 6-2. `pool__ta` REDUCED 711 → 145, LABEL-FREE — and the prune exposed `SKW-1` as a NUMBER

Measured 2026-08-20 on a 295,193-row / 538-date sample of `unified_schema_all.pool__ta`.
The question was "which technical indicators are worth keeping?", and the answer had to be
reached **without the label**, because ranking channels by their correlation with the
target would build `PRF-7`'s look-ahead into the candidate set before any null could see
it (`prune.py`'s own argument).

**What `pool__ta`'s 922 columns actually are**: 3 keys + **208 BOOLEAN flags**
(`close_gt_ema_50`, `rsi_14_gt_70`, `macd_…_cross_above` — each a thresholded copy of a
numeric channel that is already present) + **711 numeric**. Of the 711, **278 (39 %) are
moving-average machinery** across 15 MA types and **143 are pairwise MA-vs-MA
combinatorics** (`close_wma_7_14_dist`, `_direction`, `_crossover_up`, `_bars_since`).
Only **164 distinct indicator roots** exist, and **122 of them carry ≤2 columns**.

| step, in order | left | why it is label-free |
|---|---|---|
| all columns | 922 | |
| − 208 booleans − 3 keys | **711** | `prune.numeric_channels` already excludes them |
| − coverage < 0.95 | 596 | `COV-1` |
| − 133 pairwise MA-vs-MA − 26 `_dist_abs` | 437 | **construction, not movement** — a distance between two MAs is a deterministic function of two channels the pool already carries, and `\|x\|` of a present channel is not a new one. Correlation cannot make this drop: at \|ρ\| ≥ 0.50 **24 pairwise columns still survive** |
| − \|Spearman\| ≥ 0.70 redundancy | 152 | same operation `FeatureSelector` runs internally, moved earlier |
| − 7 measured duplicates of `pool__basic` | **145** | see below |

**The measured curve** (statistical prune alone vs semantic-then-statistical):

| \|ρ\| | stat only | semantic + stat | roots |
|---|---|---|---|
| 0.95 | 448 | 303 | 117 |
| **0.90** | **369** | 258 | 107 |
| 0.85 | 308 | 220 | 98 |
| 0.80 | 263 | 197 | 89 |
| **0.70** | 196 | **152** | 76 |
| 0.60 | 155 | 120 | 66 |
| 0.50 | 115 | 94 | 61 |

⚠️ **The 0.90 row reproduces `python -m feature_selection.prune`'s 369 EXACTLY**, and that
equality is the check that the fast reimplementation is the same procedure. It earned its
keep: a first numpy version returned **587** because `R.T @ R` propagates NaN where pandas
`.corr()` uses pairwise-complete observations, so every `|corr| >= threshold` comparison
silently evaluated False and nothing was pruned.

### ⚠️ `SKW-1` IS NOW A NUMBER: the same stock-day, two answers, measured on VN30

The last step above is new evidence, not hygiene. Joining `pool__basic` to `pool__ta`
carries **STA-1's 13 legacy column names**, and they are not new measurements — they are
the same measurements, taken before the `OUT-1` flow screen:

| `pool__ta` | `pool__basic` | Pearson | Spearman | median ratio |
|---|---|---|---|---|
| `val_matched_bn` | `value_matched` | **+1.000000** | **+1.000000** | **1** |
| `val_negotiated_bn` | `value_negotiated` | **+0.113** | **+0.988** | 1 |
| `f_buy_val` | `foreign_buy_value` | +0.867 | +0.897 | 1 |
| `f_net_val` | `foreign_net_value` | +0.862 | +0.973 | 1 |
| `foreign_room` | `foreign_room_left` | +0.993 | +0.967 | ~1 |
| **`close`** | `close_adjust` | +0.989 | **+0.997** | ~1 |

⚠️ **`val_matched_bn` is an EXACT duplicate.** ⚠️ **The high-Spearman / low-Pearson pairs
are `SKW-1` itself**: identical ordering, different extremes, because `pool__basic` was
rebuilt with the `OUT-1` screen on 2026-08-16 and `pool__ta` could not be (`STA-1`). A run
offering both hands the ranker one measurement twice and disagrees with itself about the
outliers. ⚠️ And `pool__ta` carries a **price LEVEL** (`close`, ρ +0.997 with
`close_adjust`) — in a cross-sectional rank problem that is the size proxy
`cross_sectional.py` §3 exists to remove.

### What exists right now

| | VCB | BANK | **ALL (top-150)** |
|---|---|---|---|
| selection runs | **31** run folders in `reports/feature_selection/` | (shared) | 2 of the 31 |
| `final_features` | `close_adjust_5day__final__d20_h5` 4,266 × 39 (35 ch) · `return_5day__final__d20_h5` 4,235 × 70 (66 ch) | `rank_5day__final__d20_h5` 53,921 × 18 · `…__basic` 54,528 × 16 | **`rank_20day__final__d20_h20` 624,448 × 17 (13 ch)** |
| datasets on disk | 3 | 1 | **1** |
| model runs | 2 | 0 | **31** — 1 single-split + 10 PRF-1 folds + 20 PRF-8 folds (2 arms × 10) |

### ✅ 6-2-bis. `FRZ-1` CLOSED 2026-08-23 — the price universe is FRESH, and the fix was a SCRAPE MODE

The audit below (§6-3) is what this answers, and its headline number is inverted:
**771 of 784 tickers now carry data to 2026-08-21**, against 5 producing the max date the
day before. The cross-section holds **771-783 names on EVERY session** from 2026-06-22 to
2026-08-21 — the old `779 → 627 → 28 → 5` cliff is gone.

| | before (2026-08-22) | after (2026-08-23) |
|---|---|---|
| `silver.stocks_basic` rows | 2,389,137 | **2,428,227** |
| tickers at the max date | **5** of 781 | **771** of 784 |
| names on the last session | **5** | **771** |
| tickers stale (< 2026-08-01) | **757** | **13** |

⚠️ **THE 13 STRAGGLERS ARE REAL, AND THEIR SHAPE IS HOW YOU KNOW.** IHK 2026-05-21, DDG
06-23, VNE 06-26, SSN/STL 07-06, DSE/KOS/SIP/VPI/DZM 07-08, TCD/VE2 07-14, CYC 07-30 —
**SEVEN distinct dates**, the largest group being **5 on 2026-07-08**, which is the
signature of individual delistings and suspensions. A scrape failure clusters on ONE
date; that is exactly what the old 599-tickers-all-on-2026-06-26 cliff was.

⚠️ **THAT SENTENCE READ "thirteen distinct dates" UNTIL 2026-08-23, AND ITS OWN LIST
DISPROVED IT** — `SSN/STL` share a date and `DSE/KOS/SIP/VPI/DZM` share another, so the
prose was counting tickers while claiming to count dates. Found by `pipeline.freshness`
(TODO `P1`) on its first run, and `ISSUES.md`'s `FRZ-1` carried the same wrong number.
✅ **The CONCLUSION survives and was re-verified a second way**: each of the 13 raw CafeF
price CSVs ends on exactly the date `silver.stocks_basic` holds, so the incremental scrape
did attempt every one of them and the source returned nothing after. ⚠️ **But the
diagnostic that separates a delisting from a scrape failure had been stated with a number
that was wrong by 6, which is the whole argument for measuring the distribution rather
than describing it.**

#### ⚠️ The scrape could only refetch from 2009, and that is why it had not been done

Measured 2026-08-22 before changing anything: a full 4-tab refetch of ONE ticker is
**615 s** (price 200.5 / order_stats 157.7 / foreign 138.3 / prop 118.9). At the
then-current `SCRAPER_MAX_WORKERS = 2` the universe was **~67 h** — which is the real
reason `FRZ-1` sat open for two months while being a one-command fix in principle.

Two changes made it a 65-minute job, and only the second is interesting:

1. **`SCRAPER_MAX_WORKERS` 2 → 12**, on a measurement: per-ticker cost is **flat** at
   200.5 s alone, 203.8 s with 8 concurrent, 206.8 s with 16 — CafeF was never
   rate-limiting, and the old 2 left ~6× on the table. ⚠️ **This is the CafeF knob and it
   is NOT the browser budget** — CafeF is `requests` against JSON `.ashx` endpoints and
   opens no Chrome at all; `SCRAPER_MAX_CONCURRENT_BROWSERS` (also 12 now) is TradingView's
   Selenium cap and is unrelated. Confusing the two is easy and buys nothing.
2. ⭐ **A third scrape mode, `incremental=True`** — resume each CSV from its OWN last date
   and merge, instead of refetching from `start_year`. **2.9-5.2 s** per stale ticker
   against 615 s, and the resumed file reproduces the full scrape **cell for cell** (PNJ,
   4,344 rows × 12 columns, zero differing cells).

#### ⚠️ AND IT NEEDS A RESTATEMENT GUARD, WHICH FIRED ON 304 OF 780 TICKERS

`close_adjust` **is not a fact about a day; it is a fact about a day as seen from today.**
A split or dividend re-bases the WHOLE history, so appending fresh rows to stored ones
splices two price bases into one series — a step change at the join that looks exactly
like a real price move, and that **no freshness check can see**: the row count is right,
the date range is right, the last date is today.

So the resume refetches a **45-day overlap** behind the last stored date, compares it
cell-by-cell against what is stored, and falls back to the full refetch on any
disagreement. ⚠️ **It fired on 304 of 780 price tickers — 39 %** — because June-August is
VN dividend season and the corpus had stood still for two months. **Those 304 series are
exactly what a naive incremental scrape would have corrupted**, and the corruption would
have been invisible. It fired on **0** of `order_stats`, `foreign` and `prop_trading`,
which carry no adjusted column — the guard is specific, not trigger-happy.

⚠️ **THE GUARD IS ALSO WHY `price` WAS THE SLOW TAB** (39 % paying 200 s each), while
`foreign` and `order_stats` finished 780 tickers in ~2 minutes. **A high restatement rate
is a property of how STALE the corpus is, not of the mechanism** — refreshed weekly it
would approach zero and the whole run would be minutes.

✅ **Verified 2026-08-23 by four checks on a throwaway folder before it touched
`raw_data/`**: equivalence (cell-for-cell), cheapness (38.3×), restatement (halving
`close_adjust` across 4,304 stored rows IS detected, and the fallback repairs history far
outside the overlap), and no-false-positive (an honest stale CSV resumes in 2.9 s).
⚠️ **An earlier version of check 3 corrupted a cell in 2017 and "failed" — the TEST was
wrong, not the guard**: a date outside the overlap is never refetched, so no incremental
scheme could see it. Recorded because the distinction is the whole design.

⚠️ **`insider_txn` ACCEPTS `incremental` AND IGNORES IT** — it is paginated by event index
with no date to resume from, and a row is amended in place upstream, so "rows after X" is
not well defined. ⚠️ **`incremental` helps only where a CSV EXISTS**: 348 of 780 tickers
have no prop-desk history at all and correctly take the full path every time.

**Run:** 1h 05m, **0 errors**, all four tabs at 780/780. ⚠️ **`incremental: true` needs `skip_existing: false` beside it** or
`skip_existing` returns first and the run refreshes nothing while going green.

⚠️ **THIS CLOSED THE SCRAPE AND NOT THE CARRY-UP** — see §6-2-ter, which ran the same
session. Gold was 30/54 sessions behind BEFORE this scrape and further behind after it.
⚠️ **Both were `P1` and `P2` when this was written, and both left the list the same day** —
as did `STA-1` a few hours later (§6-2-quater). TODO's codes shifted **down by 3** in total
on 2026-08-23, so a `P<n>` written before that date means something else; the file's
2026-08-23 crosswalk resolves it. ⚠️ **That was the LAST shift** — the numbers were frozen
as permanent names later the same day, priority moved to the row order, and the two
crosswalks are now the last two that will ever be needed.

### ✅ 6-2-ter. THE CARRY-UP TO GOLD AND UNIFIED — 2026-08-23, same session

A scrape that stops at `raw_data/` changes nothing a model reads (§5 rule 11). Every layer
downstream of `silver.stocks_basic` was rebuilt in the same session, and **verified
per-ticker, never by `MAX(date)`**:

| layer | result |
|---|---|
| `bronze.cafef_price` / `_order_stats` / `_foreign` / `_prop_trading` | 2,428,227 / 2,549,544 / 1,810,336 / 76,368 rows |
| `silver.stocks_basic` | **2,428,227 × 38**, 771 of 784 tickers at 2026-08-21 |
| `gold.stocks` | **771 of 784 at 2026-08-21** — was 2026-07-08, 30 sessions behind |
| `gold.market_breadth`, `gold.news_daily_panel`, `gold.news_weekly_panel` | rebuilt |
| `filter_schema.universe__*` | ⚠️ **membership MOVED** — see below |
| `unified_schema_all` | `pool__basic` + `pool__targets`, **2,428,227 rows, 771 names on the last session** |
| `unified_schema_{price10k,liquid,quality}` | 1,454,674 / 710,683 / 688,466 rows, all to 2026-08-21 |

⚠️ **THE SCREENS RE-MEASURED DIFFERENTLY ON FRESH DATA, AND THAT IS THE FILTER LAYER
WORKING**: `PRICE10K` **480 → 461**, `LIQUID` **206 → 228**, `QUALITY` **200 → 222`. A
screen is a window over data, so extending the data moves the membership — `PRICE10K` lost
19 names that dipped below 10,000 VND in the newly-arrived June-August sessions, while
`LIQUID` and `QUALITY` GAINED names that now clear the 200-session minimum. ⚠️ **They are
still not point-in-time** (§3a-bis point 1); this changes which basket, not that caveat.

⚠️ **RULE 14 BIT EXACTLY AS DOCUMENTED, AND IT IS WORTH SEEING ONCE.** After re-running
`filter/universe` the three `unified_schema_*` still read **2026-08-19 with 5 names on the
last session** — a fresh screen against a stale schema, with nothing raising. Re-running a
screen does **not** mark its unified schema stale; the rebuild is a separate command and
was issued explicitly.

⚠️ **`gold.stocks_ta` WAS DELIBERATELY NOT REBUILT** and is now the widest gap in the repo:
**2026-06-26 against silver's 2026-08-21**. That is `STA-1` — its own decision and never a
side effect of the carry-up, because rebuilding it renames 13 legacy columns and moves
~289k rows, and `pool__ta` inherits all of it. ✅ **It was taken and executed a few hours
later the same day — see §6-2-quater**, which is also why the *"any `basic + ta` INNER join
now truncates ~40 trading sessions"* warning that stood here is no longer true.

### ✅ 6-2-quater. `STA-1` CLOSED 2026-08-23 — `gold.stocks_ta` is rebuilt, and it cost 40 minutes

The table on disk had never been built by the builder that owns it: it was the
2026-08-03 rename of a pre-2026-07-19 `gold.stocks`, carrying **13 legacy column names**
that no Python in the repo produces. §6-2-ter deliberately left it, and the re-scrape had
widened the gap to **56 calendar days**, which is what put it at the head of the list.

**Rebuilt, and all three of `STA-1`'s signatures are gone:**

| signature | before | after |
|---|---|---|
| rows | **2,678,167** over 777 tickers | **2,428,227** over **784** — matches `silver.stocks_basic` EXACTLY |
| `MAX(date)` | **2026-06-26** | **2026-08-21**, same day as silver, **771 of 784** tickers producing it |
| column names | 13 legacy (`val_matched_bn`, `f_net_val`, `vol_matched`, …) | **0 legacy**; silver's own names carried through, 946 columns |

✅ **AND IT CLOSES `SKW-1` AS A NUMBER**: on VCB's 4,276 shared stock-days, `gold.stocks_ta`
and `silver.stocks_basic` now disagree on `value_matched` for **0 rows**. Before the rebuild
the same stock-day gave two answers — `pool__ta` carried pre-`OUT-1` flow values while
`pool__basic` had been rebuilt with the screen — so a run offering both handed the ranker one
measurement twice and disagreed with itself about the outliers.

⚠️ **THE BLAST RADIUS WAS MEASURED BEFORE THE REBUILD, NOT ASSUMED, AND IT IS SMALLER THAN
`STA-1` FEARED.** Querying `information_schema` for the 13 names across every table:

- ✅ **The headline cross-sectional chain names NONE of them** — `rank_20day__final__d20_h20`,
  `rank_10day__final__d20_h10` and both `__wide` variants carry **0 legacy columns**. The
  result §6-0 quotes is untouched.
- ⚠️ **Exactly two artefacts break**, both in the VCB `return_5day` chain that §6 already
  marks *"do not quote it"*: `pool__shortlist__return_5day__d20_h5` (2 legacy columns) and
  `return_5day__final__d20_h5` (1). They are stale now and must be rebuilt before reuse.
- The other `information_schema` hits (`bronze.trading_view_*`, `silver.funds/indices`) are
  the unrelated column `volume`, not this defect.

⚠️ **"~11 GB and hours of compute" WAS AN OVERESTIMATE — it took 40 minutes** (10:49 → 11:29,
784/784 tickers, 0 errors) and disk did not move measurably. The estimate had been carried in
`STA-1` and TODO since 2026-08-16 without anyone running it, which is its own small lesson
about unmeasured costs deterring work: **the item sat open for a week on a number that was
wrong by an order of magnitude.**

⚠️ **`pool__ta` INHERITS ALL OF IT AND MUST BE REBUILT PER PARTITION** — five exist
(`all`, `vcb`, `bank`, `vn30`, `acb`), and a `pool__ta` not rebuilt still carries the legacy
names, the extra ~250k rows and the pre-`OUT-1` flow values. Rule 14 again: gold moving does
not mark the unified layer stale.

### ✅ 6-2-quinquies. `P1` CLOSED 2026-08-23 — freshness is a DISTRIBUTION now, and it is queryable

`FRZ-1` was fixed by a scrape; **the check that missed it for two months was not**, and
that was `P1`. `pipeline.freshness` (new, 22 tests) replaces the scalar with a per-ticker
distribution, in three places: `python -m pipeline.freshness`, the views
`health_schema.session_calendar` / `health_schema.ticker_freshness`, and three new columns
in `pipeline.status_data` (`tickers`, `tickers_current`, `tickers_stale`).

```powershell
python -m pipeline.freshness --install        # (re)create the two views, ~0.1 s
python -m pipeline.freshness --layer silver   # one layer, 1.0 s
```
```sql
SELECT * FROM health_schema.ticker_freshness WHERE layer='silver' AND NOT is_current;
```

⚠️ **THE DESIGN DECISION IS WHOSE CALENDAR, AND IT IS NOT OPTIONAL.** `sessions_behind` is
counted against a reference calendar taken from the price spine, **never** against the
measured table's own dates — a table's own dates cannot contain the sessions it is
missing, so a **completely frozen table would report every ticker 0 behind**. That is the
scalar's lie one level down, wearing a per-ticker shape.

⚠️ **A CLIFF IS A SCRAPE SCOPE; SCATTER IS DELISTING — and the alarm is a SHARE, measured
from this repo's own two regimes.** 599 of 781 on one date is **77 %**; the post-re-scrape
stragglers' largest group is **5 of 784 = 0.6 %**. Only a cliff makes the stage
`not ready`, or 13 permanently-delisted names hold the gate red forever. ⚠️ An absolute
floor of 5 tickers was written first and **fired immediately on the real corpus**, calling
five genuine delistings a failure — the regimes are separated by two orders of magnitude
of share, not by a count.

⚠️ **AND ITS FIRST RUN FOUND 28 SINGLE-NAME UNIFIED SCHEMAS STALE — 28 OF THE 30 THAT EXIST**, in three layers that
are a fossil record of every scoped re-scrape this repo has run:

| stuck at | sessions behind | schemas |
|---|---|---|
| **2026-08-19** | 2 | FPT, HPG, SSI, STB, VIC — the `SSK-1` single-stock track |
| **2026-08-07** | 10 | BID, CTG, HDB, MBB, SHB, SSB, TCB, TPB, VIB, VPB — the bank re-scrape |
| **2026-06-25** | 41 | BCM, BVH, GAS, GVR |
| **2026-06-26** | 40 | MSN, MWG, PLX, POW, SAB, VHM, VJC, VNM, VRE |

Only `unified_vcb` and `unified_acb` were current among the single names; the six
multi-name schemas (`all`, `bank`, `vn30`, `price10k`, `liquid`, `quality`) all were.

✅ **ALL 28 REBUILT THE SAME DAY (`SCH-1`) — `pool__basic` + `pool__targets`, 28 of 28,
0 errors, and the tool that found them now reports `STALE: 0`.** Measured **21 s per
schema** end to end, of which the two Dagster steps are **1.7 s**; the rest is process
start-up, so the whole job was ~10 minutes. ⚠️ **Nothing downstream had been wrong**: all
28 hold pools only — **0 non-pool tables** — so no `__final__` table or dataset was ever
built from one. ⚠️ **The other 23 pools per schema were deliberately NOT rebuilt**
(`pool__bonds`, the 19 `pool__economy_*`): they stay on the old calendar, which
`status_data` already reports as `pools_behind`, and a wide join over one of these schemas
would INNER-join back down to theirs.

**Rule 14 from the other side, counted for the first time** — ⚠️ **and the first count was
WRONG BY ONE**: this section said *"27"* for an hour on 2026-08-23 while its own table
listed 28 names, because the prose was counted by eye and the table was not. Re-counted
by query, which is the only reason it is right now — and none of it is visible to `MAX(date)`, which
reads 2026-08-21 on the fresh schemas and says nothing about the other 27.

⚠️ **FILTER THE VIEW BY `layer`** — it is a `UNION ALL` over 39 layers and `gold.stocks_ta`
alone costs **26.5 s** (17 GB, 946 columns) against silver's 1.0 s. ✅ `EXPLAIN` verifies
`WHERE layer='silver'` prunes the other 38 branches on the constant. ⚠️ The first draft of
the per-ticker query **timed out at 5 minutes** — a correlated count per ticker; ranking
the calendar once with `ROW_NUMBER()` is **1.4 s** on the same table.
`.claude/context/pipeline.md` §1a-bis.

### ⚠️ 6-2-sexies. `DEP-1` — THE MONITOR BLOCKED THE REPAIR, and it was live for one hour

Opened and closed 2026-08-23, an hour after §6-2-quinquies shipped. It is the most
reusable thing this session produced, because the failure is structural rather than a
typo.

`pipeline.freshness` first installed `health_schema.ticker_freshness` as a **VIEW**. The
next rebuild died:

> `psycopg2.errors.DependentObjectsStillExist: cannot drop table`
> `unified_schema_vnm.pool__basic because other objects depend on it`
> `DETAIL: view health_schema.ticker_freshness depends on table ...`

⚠️ **PostgreSQL records a view's dependency on the tables beneath it, and EVERY BUILDER IN
THIS REPO OPENS WITH `DROP TABLE IF EXISTS`** — `_ingest_unified_pool_basic` at
`preprocessor.py:7994`, and the silver and gold builders identically. So the view did not
break one asset; **it blocked the entire write path**, including the `gold.stocks_ta`
rebuild that had finished hours earlier and every future carry-up. **A monitor that has to
be uninstalled before the system can be repaired is worse than no monitor**, and it fails
in the worst possible direction: the more there is to fix, the harder it is to fix it.

✅ **Fixed by making all three health objects `plpgsql` FUNCTIONS.** A function body is not
parsed for dependencies, so `DROP TABLE` is unaffected — verified directly by creating and
dropping a table with the functions installed. Two things fell out for free:

1. **The layer list is discovered at CALL time**, so a schema built later appears on its
   own. The view had to freeze its list at install time and say so in a `COMMENT`.
2. **The layer filter is an ARGUMENT**, so one layer means one table. The view relied on
   the planner pruning `UNION ALL` branches on a constant.

⚠️ **The cost, and it is real: `ticker_freshness(NULL)` walks EVERY layer.** A
`WHERE layer = 'silver'` written AFTER the call cannot push into the function — 32.9 s
against **0.25 s** for `ticker_freshness('silver')`. **Pass the layer; do not filter the
result.**

⚠️ **The general lesson is not about views.** Anything that observes a table takes a lock
or a dependency on it, and the observer is written by someone who is not thinking about
the writer. Ask of any new monitor: *what does this stop the repair path from doing?*
`.claude/context/pipeline.md` §1a-bis; `ISSUES.md` `DEP-1`.

### ⚠️ 6-2. THE FILINGS/OCR PARSER — the state summary

#### What the filings/OCR work established

| | |
|---|---|
| **the rule** | §5 rule 24 — a financial statement value comes from the filing PDF and nothing else. A quarter no readable PDF can produce is `missing`, and `missing` is the correct answer |
| **`BND-1`** | ⚠️ `pdf_ocr_job` REPAIRS a ticker that already has history and cannot BOOTSTRAP one that does not: `seed_history` rebuilds `sane`'s band from the `pdf` rows on disk, an empty band fails open, and the merge then refuses every statement it produced — the loop closes on itself. ⚠️ **BROKEN 2026-09-06 for a quarter whose filing produced ALL THREE statements**: both writers in `pdf_ocr_batch` write it band or no band, so a ticker bootstraps itself — but the rows are UNGUARDED and are recorded as such (`band: 0` per decision). Two of three is still refused. `FORCE_EMPTY_BAND` is the escape and it **lifts a real guard**, so the arithmetic screens (`web_scraper/statement_screens.py`) are what replaces it |
| **`CRP-1`** | ⚠️ **NOTHING FROM A NON-BANK TEMPLATE MAY BE QUOTED AS A FUNDAMENTAL.** `C_LIABILITIES` does not map on the `corp` chart, so `reconcile` tests `assets == resources` — true by construction on any page that reads both totals — and never `A = L + E` |
| **`SET-2`** | ⚠️ a SETTLED absence is a verdict on the page CLASSIFIER wearing the words of one on the document. Re-test every settled cell after any `_page_kind` change; six of FPT's eight turned out winnable that way |
| **`TPX-1`** | `templates.csv` names only ACB, BID, VCB — every other ticker resolves its template by a NETWORK call, and a ticker absent from **both** `CAFEF_FINANCIALS_TICKERS` and `orchestration/config.json` is silently unmaterialisable |
| **the recurring shape** | ⚠️ **`SLD-1`'s family: a WRONG FIGURE that passes every gate.** Recorded six-plus times — a slid row, a lost bracket, a comparative column, a merged label, a seal over the digits. `reconcile` and `sane` are the only gates, and on a cash flow accepted at a STRICT layer the arithmetic identity never runs at all (`CFV-1`) |
| **the method that keeps working** | ⚠️ **a VAS filing prints several of its figures twice, and checking one against the other is free** — the cash flow's closing balance is the balance sheet's cash line, a Q1 income statement's two columns are the same three months, a balance sheet's two grand totals are one number, and a cumulative cash flow prints one opening per year. Four of the 2026-09-05 defects were found that way, with no OCR and no network |

#### Coverage, as last measured

Measured **2026-09-06** by `RUN__pdf_ocr_summary.ipynb` over `statements/**/*.csv` against
`documents(allow_parent=True)` and `settled_absences` — no OCR, no network, seconds to recompute.
**Ten tickers now**, against the eight §6-2-septsexagies tabulated on 2026-09-05:

| 2026-09-06 | CTG | VCB | FPT | ACB | VIC | TCB | BID | GAS | BSR | MSN | total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `pdf` / cells | **210/210** | **210/210** | 210/213 | 202/219 | 192/216 | 180/204 | 182/210 | 123/180 | 40/51 | 21/27 | **1,570/1,740** |
| missing | 0 | 0 | 3 | 17 | 24 | 24 | 28 | 57 | 11 | 6 | **170** |
| `complete` | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | 7 of 10 |

⚠️ **`complete` IS CONTINUITY FROM THE START OF THE FILING CHAIN, NOT COVERAGE, AND THE TWO
DISAGREE HERE** — CTG reads ✅ at 210/210 and BSR reads ✅ at 40/51, while FPT reads ❌ at
210/213. Read the cell count for coverage and `complete` for continuity.

⚠️ **THE `pdf` COLUMN ALONE INVERTS THE RANKING**, and the 2026-09-05 measurement is why: of that
day's 130 missing cells, **66 (51 %) were quarters the company never filed** — where `missing` is
the correct answer and no run can change it — against only **56 winnable**, 46 of them one ticker
(VIC), and 27 of those income statements waiting on a de-cumulation operand rather than on OCR.
**Do not read a missing count as work available.** TODO `P65`.

⚠️ **GAS AND MSN ARE MID-BOOTSTRAP, AND THEIR NUMBERS MEAN SOMETHING ELSE.** MSN's 21/27 is 9
quarters parsed of **65 filed** — the ticker has barely started, so its `missing` column counts
rows that exist, not quarters that failed. `BND-1`: a first run has an EMPTY `sane` band, so both
were written under `FORCE_EMPTY_BAND` with the arithmetic screens standing in for the guard.

⚠️ **AND ONE FILING IS IN THE INDEX WITH NO PDF ON DISK** — ACB `2009-Q3`, which still carries
`pdf` rows parsed from a file that has since gone. It cannot be re-parsed; the notebook WARNs on
it. **24 cells are SETTLED** (`SET-2` — a verdict on the page classifier, so re-test them after
any `_page_kind` change), and **17 quarters have only a standalone filing**, reachable only with
`allow_parent=True`.

⚠️ **AND THE STANDING REQUEST-SHAPE IS §8's, NOT THE LOG'S**: *"OCR ticker `<SYM>` LOCAL|KAGGLE"*
is a request for a PREPARED NOTEBOOK that then WAITS — never for a run. `.claude/docs/PDF_OCR.md` §1a.
### ⚠️ MBB, 2026-09-08 — a BOOTSTRAP that finished, and the CSV that did not open: `MRG-2`

**HOSE_MBB, template `bank`, no statement CSV on disk, 0 `pdf` cells of 186.** Kaggle T4,
`ONNX_ONLY = True`, `ISOLATE_DOCUMENTS = True`, `OVERWRITE = False`, `FORCE_EMPTY_BAND = False`
(the bootstrap no longer needs it — a filing that produced all three statements is written band
or no band since 2026-09-06). Account chosen by `auto` on remaining hours: `lyductrung` (30.00 h)
over `ductrung180200` (3.59 h), which is `ACC-1`'s guard doing its job against a ~4.0 h estimate.

| the run | |
|---|---|
| status · wall clock | **COMPLETE** · **158.3 min**, 66 files pulled |
| documents | **62** parsed of 66 quarters in the span Q4-2009..Q1-2026 |
| engine errors / `VCR-1` | **0** — no document refused for a raised layer |
| accepted | balance_sheet **57**, income_statement **61**, cash_flow **58** → **176 of 186 cells** |
| filings with all three | **53** · two of three **8** · one of three **1** |
| **statements that reached `raw_data/`** | **0**, until the sweep was run by hand the next command |

⚠️ **THAT LAST ROW IS THE FINDING, AND IT IS `MRG-2`.** The parse was excellent and the three
CSVs were never opened: `MERGE_INTO_CSV = False` (as the notebook itself advised), so the pull
merged nothing, and the process holding the notebook died after the pull, so §9 never ran. The
`K5` recovery — rebuild the `JobConfig`, assert its id against the running kernel, `runner.wait`
then `runner.pull` — brought the folder home and merged nothing either. **`BND-1`'s third face:
the parse is durable in the run folder, the CSV was never opened, and a green run says nothing
about which.** The fix is in `MRG-2`; the measurement is this row.

**Run by hand afterwards** (`merge_batch`, apply, one period at a time, oldest first):

| the merge | |
|---|---|
| written | **152** — bs **53/53**, cf **53/53**, is **46/53** |
| refused | **34** = 15 empty band (a filing that produced two of three) + 10 absent + 9 de-cumulation |
| already on disk | 0 — nothing had ever been written for this ticker |

⚠️ **EVERY ONE OF THE 152 PASSED NO MAGNITUDE GUARD** (`BND-1`): this ticker had no `pdf` history
for `seed_history` to rebuild a band from, so `sane` failed open on all 62 documents. Screen by
arithmetic — `statement_screens.py`, and PDF_OCR.md §6 — before quoting any MBB figure.

#### What MBB is still missing, and the two kinds are not the same work

**4 quarters have no filing on disk at all** — `Q1-2010`, `Q2-2010`, `Q3-2010`, `Q1-2011`, i.e.
**12 cells** no OCR can reach. They are the two holes at the start of the chain, and they are why
`complete` will read **False** for MBB for ever. ⚠️ Read the cell count, not `complete`.

**10 cells had a filing and were refused**, with the reason each recorded:

| quarter | statement | recorded refusal (last layer) |
|---|---|---|
| Q2-2014 | balance_sheet | assets 188,570,294,373,690 ≠ L+E **7,387,243,988,483** |
| Q4-2014 | balance_sheet | assets 200,489,173,221,701 ≠ L+E **4,604,174,657,397** |
| Q2-2015 | balance_sheet | assets 204,409,466,000,000 ≠ L+E **7,695,968,000,000**; then `1 figure split across two boxes` at onnx@400 |
| Q1-2020 | balance_sheet | assets 406,802,682,000,000 ≠ L+E **17,651,000,000** |
| Q4-2018 | balance_sheet | `no total assets` |
| Q2-2017 | income_statement | `only 2 rows parsed` (onnx@200), `only 1 rows parsed` (onnx@300) |
| Q3-2017 · Q1-2018 · Q1-2024 | cash_flow | `no closing cash balance` |
| Q4-2018 | cash_flow | `1 figure(s) split across two boxes` |

⚠️ **THE FOUR UNBALANCED BALANCE SHEETS LOOK LIKE ONE DEFECT, NOT FOUR.** In three of them the
`liabilities + equity` side is the size of **equity alone** (7.4 / 4.6 / 7.7 e12 against total
assets of 1.9-2.0 e14), i.e. the **total-liabilities line was not captured** and the gate summed
one vee. That is `GCW-1`'s shape exactly — *33 of GAS's 56 open cells were ONE wording* — so the
next move is to render Q2-2014's balance-sheet page and read the label, not to spend another
cascade. Q1-2020 is a different animal: its L+E reads **17.6 e9**, four orders of magnitude out.

⚠️ **AND THE 9 REFUSED INCOME STATEMENTS ARE A KNOCK-ON, NOT A PARSE FAILURE.** Each parsed
fine and is cumulative, and the prior it must subtract is one of the 10 cells above — so the
refusal names that prior, every time:

| refused | waiting on | | refused | waiting on |
|---|---|---|---|---|
| Q4-2014 | Q2-2014 | | Q4-2020 | Q1-2020 |
| Q4-2015 | Q2-2015 | | Q2-2024 | Q1-2024 |
| Q4-2017 | Q2-2017 | | Q4-2024 | Q1-2024 |
| Q2-2018 · Q4-2018 | Q1-2018 | | Q2-2020 | Q1-2020 |

⚠️ **AND THE PRIOR IS NOT MISSING FROM THE RUN — IT IS ONE OF THE 15 THE MERGE HELD.** Q1-2018's
income statement was ACCEPTED; its filing produced two statements of three, so the
all-three-statements gate did not lift the empty band for that quarter and none of its three
cells was written, which is what makes it `absent` on disk. The same is true of Q2-2014, Q2-2015,
Q2-2017, Q1-2020 and Q1-2024. **So a second §9 pass with `FORCE_EMPTY_BAND = True` would write
those 15 held cells and unblock most of these nine at the same time — and it lifts a real guard
on a ticker that has no band at all**, so it is a judgement about those eight filings and stays
the operator's (`BND-1`, PDF_OCR.md §6). Not done: the arithmetic screens have not been run over
the 152 rows already written.

#### ⚠️ MSN, 2026-09-07 — the gaps were REFUSALS, not documents, and two of them are now fixed

⚠️ **THE ROW ABOVE READS MSN 21/27 AND IS A MID-BOOTSTRAP MEASUREMENT.** A 60-document run on
2026-09-07 took the ticker to **165 `pdf` cells of 195** (317.8 min = 5.30 h, **0 engine errors**,
155 of 180 statements accepted), which is where this section starts. `source` is only `pdf` or
`missing` throughout — §5 rule 24 holds, 0 violations.

⚠️ **THE 30 CELLS STILL OPEN WERE NOT 30 UNREADABLE FILINGS.** Every one was classified from its
own recorded refusal, off the run folders, at no OCR cost:

| cause | cells | status |
|---|---|---|
| `reconcile: operating profit does not close` | **14** | ✅ `JVW-2`, fixed 2026-09-07 |
| the VAS `Mã số` code column read as a figure | **4** | ✅ `MSO-5`, fixed 2026-09-07 |
| de-cumulation with no operand (Q2/Q4-2017, Q2/Q4-2022) | 4 | downstream of the 14 |
| `sane` compares a 12-month figure to a 3-month band | 2 | ⚠️ `SPN-2`, open |
| `reconcile: no total assets` (Q1-2011, Q2-2018) | 2 | ⚠️ `BSP-1`, open |
| no `sane` band at all (Q1-2010 cash flow) | 1 | a judgement, not a defect |
| genuine (Q4-2008 cf, Q2-2011 is, Q1-2017 cf) | 3 | `missing` is the correct answer |

⚠️ **THE LARGEST CAUSE WAS A LABEL VOCABULARY, AND THE STATEMENTS BEHIND IT ARE CORRECT.**
`JVW-1` had already recorded the mechanism — VAS line 24 is an OPTIONAL term of `OP_IDENTITY`'s
corp entry, and "optional" protects only the filing that does NOT print the line; one that PRINTS
it and cannot map it fails the identity **by exactly that figure**. MSN spells it four ways
(`lai_tu_cac_cong_ty_lien_ket` ×9, `phan_lai_tu_cac_cong_ty_lien_ket` ×5,
`loi_nhuan_tu_cac_cong_ty_lien_ket` ×1, one with a note reference glued on), all scoring 0.56-0.70
against a bar of 0.80. **Q1-2024 is the proof and it needed no OCR**: the refusal read *"components
give 1.22796e+13 (or -6.21906e+11 …) against a printed 6.26631e+11"*, and both branches are short
of exactly that line —

```
5,254,838 + 574,011 - 1,899,341 + 1,248,537 - 3,579,977 - 971,437 = 626,631   (millions)
```

— the printed operating profit to the đồng, on a statement whose PBT (626,631 + 7,032 = 633,663)
and after-tax (633,663 - 294,738 + 139,926 = 478,851) identities close exactly too.

⚠️ **AND THE SECOND FIX ASKS THE COLUMN WHERE FOUR EARLIER ONES ASKED THE PAGE.** Every `MSO` fix
to date widens how the words "Mã số" are READ; a filing that prints no readable heading answers
none of them. MSN's four refused balance sheets say the code out loud — `assets 270,000,000 !=
liabilities + equity 440,000,000`, and **270 IS the code for TỔNG CỘNG TÀI SẢN, 440 for TỔNG CỘNG
NGUỒN VỐN** — with every row's column 0 its own code scaled (`tien` 111000000, `hang_ton_kho`
140000000). `_code_column_by_value` requires **every entry exactly 3 digits and never descending**,
which is a VAS balance sheet's 100→270 / 300→440 numbering and is not something a period column
(4-9 digits in Triệu VND, 10-13 in đồng) can be. Five new layers at positions **102-106 of 107** —
last, which is the whole safety argument: only a statement every strict read and all four `MSO`
widenings already refused can reach them.

⚠️ **BOTH WERE MEASURED END TO END BEFORE THE FULL RE-RUN, AND THE LAYER NAMES ARE THE EVIDENCE.**
A 4-document probe (29.8 min) took 165 `pdf` cells to 168:

| quarter | result |
|---|---|
| Q3-2012 | `balance_sheet=43 items` **[onnx@200+codecol]** |
| Q1-2015 | `balance_sheet=55 items` **[onnx@200+codecol]** |
| Q1-2024 | `income_statement=15 items` **[onnx@200+equity]** |
| Q1-2010 | `balance_sheet` ABSENT after all 105 layers |

Fix 2's two land on the layer written for them; fix 1's lands on `+equity`, the layer where
`equity_wording` turns `ACCOUNT_WORDING` on and therefore the only one its new aliases live in.
Neither could have been won by an unrelated layer.

⚠️ **AND THE FREE CROSS-CHECK CLEARS THE TWO BALANCE SHEETS THE NEW LAYER WROTE** — the check
`reconcile` and `sane` cannot do (§6 of the OCR workflow, `SLD-1`'s class). The cash flow's closing
balance **is** the balance sheet's cash line, and both filings' cash flows were accepted at a
DIFFERENT layer (`onnx@200+merged`, `onnx@200+tail+relax`):

| quarter | bs cash line | its own `i.1 + i.2` | cf closing |
|---|---|---|---|
| Q3-2012 | 7,459,429,000,000 | 7,459,429,000,000 | 7,459,429,000,000 |
| Q1-2015 | 4,750,081,000,000 | 4,750,081,000,000 | 4,750,081,000,000 |

Three-way agreement to the đồng, from two independent parses. **A wrongly-dropped column cannot
produce a cash line that matches an independently-parsed cash flow exactly.**

⚠️ **FIX 2 CAME IN AT 2 OF ITS 4 TARGETS, AND THE MISS IS A LESSON ABOUT THE VERIFICATION
METHOD.** Before the run, a replay over the recorded `absent_rows` — reading column 1 where the
gate had been shown column 0 — closed the balance-sheet identity on **three** of the four
(Q1-2010, Q3-2012, Q1-2015) and was reported as three verified. Measured: **Q3-2012 and Q1-2015
were recovered; Q1-2013 refused as predicted; and Q1-2010 refused as well.** The replay asked
*would the identity close* and therefore tested the DATA; it never asked *would the detector fire*,
which is a question about the column's SHAPE. Q1-2010's data was sound — 7,190,076,000,000 on both
sides — and its ragged code column defeated the detector anyway. **A replay over recorded rows can
verify a figure and can never verify a detector.** The honest pre-run figure was 2-3, not 3.

⚠️ **Q1-2010 IS A THIRD SHAPE AND NOT A FAILURE OF FIX 2.** The `+codecol` layers ran (101-105 of
105) and the refusal list gained no new entry, so the reason at each was a duplicate of `assets
270,000,000 != …` — the assets figure never changed and the column was never dropped: the detector
ABSTAINED. Its rows say why: that filing merges part of the numbering into the LABELS
(`tong_cong_tai_san_270_100_200`, `tong_cung_nguon_von_440_300_4004449`), leaving a ragged code
column whose stray token fails the all-3-digit rule. **That is the fail-safe behaviour working as
designed** (§5 rule 2), and the cost of it is one cell. Relaxing the rule to tolerate a few
non-code entries is a different decision and needs its own measurement.

⚠️ **A FIX FOR ONE DEFECT CLOSED HALF OF ANOTHER, MEASURED ON A ROW IT WROTE.** `PBT-1` records
that 41 of 44 accepted MSN income statements reach disk with the PBT cell EMPTY — `reconcile`
accepts them on a figure `get`'s TEXT fallback found, which `mapped` never contained — and that
this starves `seed_history`'s band to **3 probes at every quarter from Q1-2017 to Q1-2026**, while
the balance sheet's grows 2 → 7 → 8 → 14 → 25 and the cash flow's reaches 23. **But the alias that
answers MSN's label already exists**; it is simply unreachable from `onnx@200`. Q1-2024, recovered
by `JVW-2` at `onnx@200+equity`, arrived carrying `15_tong_loi_nhuan_ke_toan_truoc_thue =
633,663,000,000` **and** `phan_lai_lo_trong_cong_ty_lien_doanh_lien_ket = 1,248,537,000,000`. So
every cell `JVW-2` recovers carries its PBT and widens the band for the quarters after it.

⚠️ **THIS WAS STATED, WITHDRAWN AND RESTATED IN ONE SESSION, AND THE MIDDLE VERSION WAS WRONG.**
The withdrawal argued that a recovered cell reaches disk only at §9's end-of-run sweep, so it
cannot widen the band inside its own run. That was inferred from disk being unchanged after
Q1-2017 and Q2-2017 — both unwritable for their OWN reasons (2 of 3 statements; and a cumulative
income statement that `ISOLATE_DOCUMENTS` gives no prior to subtract). **`MERGE_EACH` does write
during the run**, and each isolated document then re-seeds from the fuller disk. Measured, one
line per document:

| document | income band | | document | income band |
|---|---|---|---|---|
| Q1-2017 … Q1-2022 | **3** (nothing written yet) | | Q4-2023 | 8 |
| Q2-2022 | **4** — the first writable quarter | | Q4-2024 | 12 |
| Q4-2022 | 5 | | Q4-2025 | 14 |
| Q2-2023 | 6 | | Q1-2026 | **14** |

**The band went 3 → 14 inside one run**, and that is why Q4-2023, Q4-2024 and Q4-2025 all cleared
`sane` where Q4-2021 had not: by Q4-2023 it held 8 probes including Q4-2022's own 12-month figure,
so the median rose and the 20× window moved with it. ⚠️ **The lesson is about the inference, not
the mechanism** — "disk did not change after two documents" is not evidence that nothing writes,
when both documents had their own reason not to. **The 41 rows already on disk are still untouched
and still need a re-parse or a write-back.**

⚠️ **THE OUTCOME: 165 → 184 `pdf` CELLS OF 195, AND 30 OPEN CELLS BECAME 11.** 29 documents over
310.7 min = **5.18 h** (probe 4 + re-run 25), mean 10.7 min/doc, **0 engine errors**, `source` still
only `pdf` or `missing`. Per fix, against what each was predicted to reach:

| | targeted | recovered | |
|---|---|---|---|
| `JVW-2` (the JV term) | 14 | **13** | Q1-2026 alone missed — a second defect, below |
| de-cumulation, downstream of those | 4 | **4** | Q2/Q4-2017, Q2/Q4-2022 |
| `MSO-5` (the code column) | 4 | **2** | Q3-2012, Q1-2015; Q1-2010 and Q1-2013 abstained |
| **total** | 22 | **19** | against a stated honest range of **18-22** |

The eleven still open: `BSP-1` 2 (Q1-2011, Q2-2018 balance sheets) · `MSO-5` abstentions 2 (Q1-2010,
Q1-2013 balance sheets) · `SPN-2` 2 (Q4-2011, Q4-2021 income statements) · genuine 3 (Q4-2008 and
Q1-2017 cash flows, Q2-2011 income statement) · Q1-2010's cash flow, still bandless · Q1-2026's
income statement.

⚠️ **AND ALL THREE CUMULATIVE Q4s THAT WERE FLAGGED AS `SPN-2` RISKS CLEARED** — Q4-2023, Q4-2024
and Q4-2025 — for the reason the band table above gives: by Q4-2023 the band held 8 probes rather
than 3. The pre-run warning was right about the mechanism and too pessimistic about the outcome.

⚠️ **Q1-2026 IS A SECOND DEFECT ON THE SAME LINE, AND IT IS ORDER-SHAPED WHERE `JVW-2` WAS
WORDING-SHAPED.** Its income statement parses perfectly and the identity closes to the đồng —
`7,906,027 + 425,056 - 1,894,801 + 1,341,193 - 4,089,684 - 1,369,051 = 2,318,740` (millions), the
printed operating profit — and the missing term is `lai_tu_cac_cong_ty_lien_ket = 1,341,193,000,000`,
**one of the four spellings `JVW-2` added an alias for**. Reversing the refusal's `as_expense`
branch confirms the term was simply never added: `(7,906,027 + 425,056) - (1,894,801 + 4,089,684 +
1,369,051) = 977,547`, exactly the 9.77547e+11 reported. ⚠️ **The SCORE is not the blocker** — that
alias scores **0.808** against this label, above the 0.80 bar, measured. What differs is POSITION:
the corp chart puts the JV line at order 9, between `trong_do_chi_phi_lai_vay` and
`9_chi_phi_ban_hang`, and MSN's 2026 layout prints it **after** `chi_phi_quan_ly_doanh_nghiep` —
two lines later than the ordered walk expects. Only two distinct refusals were recorded across all
105 layers, so the `+equity` layers produced one of them too, i.e. the alias never got the row.
**Likely the ordered walk and NOT PROVEN**; a containment fallback or another account claiming the
row are not excluded, and settling it needs `map_to_schema` traced on that statement.

⚠️ **THE RUNTIME MODEL FROM THE 60-DOCUMENT RUN DOES NOT TRANSFER, AND THE 2.2 h ESTIMATE BUILT ON
IT WAS WRONG.** That run averaged 5.30 min/document because most documents were accepted early and
stopped. **Every document of a gap run reaches the end of the cascade by definition** — it is there
because a statement failed — so the population is different: eight measured documents (the probe's
four plus the first four of the re-run) average **10.2 min**, with an annual filing at 31.4 min and
the oldest at 24.3. Estimate a gap run from gap-run documents.

### ⚠️ GVR, 2026-09-07 — a BOOTSTRAP from zero, and the merge still refuses two-of-three

**HOSE_GVR had no statement CSV at all** — 0 `pdf` cells of 102, and `seed_history` returned an
empty band for **102 of 102** (quarter, report) pairs, measured per cell before the run. One T4
kernel, `ONNX_ONLY`, `OVERWRITE=False`, `SPAN_OPERANDS=False`, **`FORCE_EMPTY_BAND=False`**.

| | |
|---|---|
| wall clock | **9 h 44 min** (07:02 → 16:46), exit 0, under Kaggle's 12 h kernel ceiling by 2 h 16 |
| documents | 34 filings, 3,076 pages, 163.0 MB — all `hop_nhat`, one entity |
| parsed | **88 of 102 cells `pdf`**, 14 `absent` |
| **written** | **62** — balance_sheet 22, income_statement 18, cash_flow 22 |
| refused | **40** — 14 `absent`, **26 for want of a magnitude band** |
| coverage | **62 / 102 = 60.8 %** of cells; 22 quarters on disk of 34 openable |
| `source` audit (**D6**) | **0 rows** anything but `pdf` or `missing`, on all three CSVs — §5 rule 24 held |

⚠️ **THE 22 QUARTERS WRITTEN ARE EXACTLY THE 22 THAT PRODUCED ALL THREE STATEMENTS.** The
2026-09-06 change unlocked the all-three branch and **only** that branch: **12 quarters produced one
or two of three and were refused whole**, taking 22 already-parsed cells down with them. So a
bootstrap now starts on its own, and `FORCE_EMPTY_BAND` is still the flag that decides the
two-of-three remainder — `BND-1`'s loop, narrowed rather than closed. The other 4 refused cells are
a de-cumulation problem, not a band problem (Q4-2025's income statement cannot subtract a Q3-2025
that is `absent`).

⚠️ **THE RUNTIME ESTIMATE BUILT FROM PAGE COUNT WAS WRONG IN THE USUAL DIRECTION.** 3,076 pages
× 7 distinct passes gave a ceiling of 21,532 page-reads ≈ 5.1 h, and the run took 9 h 44. **Four
documents carried 3.4 h of it** — Q4-2025 63.5 min, Q1-2026 59.5, Q2-2025 43.3, Q3-2025 39.2 — and
`ISOLATE_DOCUMENTS=True` pays the ONNX load 34 times, which the page count does not price. Estimate a
bootstrap from its largest documents, not from its page total.

#### The arithmetic screens are the whole guard here, and two of the four are unimplemented

**62 of 62 written statements passed no magnitude guard.** `statement_screens.screen_run` over the
run folder flagged **one** statement, on a quarter that was refused and never reached disk. Run by
hand, the cross-document identities found what it could not:

| check | result |
|---|---|
| cash flow closing == balance sheet cash line | **16 EXACT, 1 DIFF, 5 no closing row read** |
| cash flow opening == prior period's balance sheet cash | **15 EXACT, 1 DIFF, 6 not on disk** |

Both DIFFs land on the **same boundary**, Q4-2016 → Q2-2017. The first is adjudicated and is
`GVR-1`; the second is not, and needs the page rendered. ⚠️ **The second identity is the only one
that reaches the five Q4 filings (2019, 2020, 2021, 2022, 2024) whose closing row the parser never
read at all** — a parser gap, not a merge gap, and four of the five are confirmed exact through it.
Neither identity is in `statement_screens`, which is keyed per report; that, and the tolerance that
hid `GVR-1`, are `SCR-1`.

⚠️ **NOTHING HERE MAY BE QUOTED AS A FUNDAMENTAL** — `corp` template, so `CRP-1` / `TPL-1` apply
in full whatever the parse quality.

#### The second pass, same day: `FORCE_EMPTY_BAND = True`, and it cost two rows

The 22 held cells were recovered **at zero GPU cost** — `ENVIRONMENT = LOCAL`, `EXECUTE = False`,
so §9's sweep re-planned the run folder that was already on disk against the CSVs. **22 written,
62 already on disk unchanged, 18 refused.**

| | before | after |
|---|---|---|
| quarters on disk | 22 | **34** (Q4-2016 … Q1-2026, every openable filing) |
| cells `pdf` | 62 / 102 | **84 / 102 = 82.4 %** (bs 32, is 24, cf 28) |
| still refused | 40 | **18** — 14 `absent`, 4 de-cumulation. **No band refusal is left** |

⚠️ **AND THE ARITHMETIC SCREEN IMMEDIATELY FOUND TWO WRONG ROWS AMONG THE 22.** `GVR-2`:
Q4-2023's balance sheet reads the literal **`5` in 32 of 95 cells**. `GVR-3`: Q1-2026's income
statement carries **`unit = 1000000`**, the only row of 24 that does, so every figure in it is 1e6
too large. Both were written `band: 0`. **That is the price of the flag, measured rather than
argued** — and it is the case for lifting it anyway on a bootstrap: **the band is no longer empty**,
so a re-parse of those two filings is now guarded by 84 `pdf` rows that did not exist this morning.
⚠️ **`GVR-3` is the one to remember**: a unit error passes every identity a statement can check
on itself, so `unit` must be screened as its own column.

### ⚠️ GAS, 2026-09-07 — 33 of 59 open cells were ONE WORDING, and it is now fixed: `GCW-1`

⚠️ **NO OCR AND NO NETWORK WERE SPENT ON THIS.** Every figure below was read off the two run
folders already on disk — `20260905-204946__hose_gas__pdf_ocr` (61 quarters) and
`20260907-064620__hose_gas__pdf_ocr` (45 quarters, 2 h 36 m on a T4, 0 engine errors).

**Where GAS stands:** template `corp`, **61 quarters filed, 16 complete, 124 `pdf` cells of 183**,
**59 open**, 0 settled — 17 balance_sheet, 6 income_statement, 33 cash_flow, plus the 3 cells of
`2022-Q2`, which is filed and carries no row in any of the three CSVs. The filing chain runs
2008-Q4 → 2026-Q1 with 9 slots absent (2009-Q1..Q3, 2010-Q1..Q3, 2011-Q1..Q3); GAS listed in 2012
and filed annually before that, so `missing` is the correct answer for those nine.

⚠️ **THE TWO T4 RUNS RETURNED THE IDENTICAL VERDICT ON ALL 59 OPEN CELLS** — 56 `absent in this
run`, 3 parsed with no `pdf` row to compare. Not one cell differs between them.

#### The 59 open cells, classified from their own recorded refusals

| cause | cells | what answers it |
|---|---|---|
| `reconcile: no closing cash balance` | **33** | ✅ **`GCW-1`, fixed 2026-09-07** — below |
| `reconcile: N figures split across two boxes` | 21 | ⚠️ open — OCR damage, 15 bs · 4 is · 2 cf |
| `reconcile: totals do not close` | 2 | ✅ `MSO-5` — **never tried on this ticker** |
| `reconcile: operating profit does not close` | 1 | ◐ `JVW-2` aliases — never tried; 1 of 5 confirmed |
| `no such statement on any page` | 2 | ⚠️ open — 2022-Q2 bs + is |

⚠️ **AND THE PARSER THE RUN USED IS NOT THE PARSER ON DISK. A FIRST READING OF THIS GOT IT
WRONG, MEASURED FROM THE WRONG BASELINE.** `git diff 69ea448a..HEAD -- src/web_scraper/` is empty
and was quoted as *"no parser change since the run"* — but `metadata.json` records the run at
**`38bc1873+dirty`** and it finished at 09:22, while `a812fe2b` (the MSN fixes) landed at 18:32
the same day. **The cascade was 100 layers then and is 112 now**, and twelve have never been tried
on GAS: `tesseract@200` / `tesseract@400+relax` (4, 7), `+codecol` (103-107, `MSO-5`) and
`+cashword` (108-112, `GCW-1`). Two of GAS's five refusal classes are what those answer.

#### `GCW-1` — the wording, and why it cost 33 cells

The corp chart of accounts names VAS codes 60 and 70 *"Tiền và tương đương tiền đầu kỳ (60)"* and
*"… cuối kỳ (70 = 50+60+61)"*. **GAS prints "Tiền tồn đầu năm" and "Tiền tồn cuối năm"** — an
older B03 phrasing for the same two lines. They share almost no characters:

```
tientoncuoinam  vs  tienvatuongduongtiencuoiky   ->  0.550     (SCHEMA_MATCH = 0.80)
```

So the closing line never maps; `reconcile` **requires** a closing balance — the fix that closed
the 27 hollow cash flows — so the entire statement is refused. `tien_ton_cuoi_nam` is the printed
label on **28 of 35** documents and 33 of 35 carry the refusal, **9 of them at every one of the
100 layers tried.** This is `JVW-1`'s shape one level down, at the cash flow.

✅ **FIXED ADDITIVELY, AND NOTHING EXISTING CHANGED**: `CASH_WORDING` (keyed on the WHOLE account,
never a substring — `NST-1`), a `cash_wording` ParseLayer flag, and **five new layers at 108-112
of 112, LAST**, so only a statement every one of the 107 layers before them refused can reach
them. `is_strict` counts the flag and the last strict layer is still position **49**. The flag
defaults False on every path; the full suite is **1,199 passed**.

⚠️ **THE OPENING BALANCE IS THE ONE THING THE CLOSING ALIAS MUST NEVER ANSWER, AND IT VERY NEARLY
DOES.** `tientoncuoinam` scores **0.815** against the OPENING row's `tien_ton_dau_nam` — over the
bar. That is `ANNUAL_WORDING`'s BID Q4-2016 failure (0.804, which handed the closing slot the
opening figure and was caught only by `sane`) arriving by a second route, so **the period word is
a HARD GATE in the new branch and never a score.**

✅ **REPLAYED OVER ALL 35 REAL ROW DUMPS, NO OCR:**

| | before | after |
|---|---|---|
| closing balance MAPPED | 2 / 35 | **32 / 35** |
| `reconcile` passes it did not before | — | **30** |
| OPENING figure landing in the CLOSING slot | — | **0** |

⚠️ **30 RECONCILE PASSES IS NOT 30 CELLS — it is one gate opening, not a statement accepted**
(§5 rule 21). Each still faces `sane`'s band, `_closing_breakdown` and `CBS-1`, and several
layer-1 figures behind them are visibly damaged (Q1-2016's closing reads `190`). **What the fix
removes is a false refusal that stopped the cascade ever reaching those questions.** How many
cells actually land is **UNMEASURED**, and is what the run measures (§5 rule 2).

⚠️ **The 3 it does not reach are OCR damage, not vocabulary**, and are recorded as untried rather
than failed: Q2-2020's closing row is correctly labelled with its current-period figure read as
`None`; Q3-2012's page dump ends at the FX line with no closing row at all; Q4-2010's reads
`rot_duong_tien_cuoi_nam`. All three are a layer-1 read, and 111 other layers get their own go.

⚠️ **NOTHING FROM THIS TICKER MAY BE QUOTED AS A FUNDAMENTAL** — `corp` template, so `CRP-1` /
`TPL-1` apply in full whatever the parse quality. ⚠️ **And bronze holds NO GAS ROW AT ALL**: the
124 cells on disk have never been ingested (§5 rule 11).

### ⚠️ GAS, 2026-09-07 (second pass) — the "59 open cells" was 56, and 19 of 20 "split boxes" were ONE box

⚠️ **TWO OF THE NUMBERS THE SECTION ABOVE USES ARE WRONG, AND BOTH WERE WRONG IN THE SAME
DIRECTION — they overstate the gap.** Measured off the same run folder plus the CSVs on disk,
still at no OCR cost:

| the claim | measured |
|---|---|
| "59 open cells" | **56.** Three of the 59 refusals land on a cell whose row on disk already reads `source='pdf'` — a refusal on a re-parse is not a gap. ⚠️ **A REFUSAL IS A VERDICT ON ONE READING, NOT ON THE CELL** |
| the gaps by statement | **34 cash flow · 18 balance sheet · 4 income statement.** Only **Q2-2022** has no row at all; the other 55 carry `source='missing'`, which is the correct answer until a filing produces one (§5 rule 24) |

**And the fragmentation class is not what its message says.** `reconcile` refuses with `N
figure(s) split across two boxes` on 45 of the 56, **24 of them at exactly ONE figure**. The
pairs were dumped for 14 cells at their minimum-fragment layer — one page of OCR each, no
cascade — and the raw pre-splitter boxes read:

| what the gate counts | what the recogniser actually emitted |
|---|---|
| `'304'` + `'809.430'` | `'304 809.430 862'` — ONE box, a printed 304.809.430.862 |
| `'3.170.949.624'` + `'222'` | `'3.170.949.624 222'` |
| `'334.970'` + `'244'` | `'334.970 244'` — and `'334.970.244'` reads WHOLE in the next column of the same row |
| `'74.826'` + `'437'` | `'74.826'` + `'437 216,601'` — the one genuine two-box split of the twenty |

⚠️ **19 OF 20 COUNTED PAIRS ARE ONE BOX THAT LOST A THOUSANDS SEPARATOR**, split into pieces by
`_split_number_runs`, which apportions by character offset and leaves the pieces
`box_width / len(text)` apart — **measured at 3.58 to 4.47pt against a `SPLIT_MAX_GAP` of 4.5**.
The gate is reading the splitter's own output. It is still refusing a genuinely damaged reading,
so this is not a false positive to be deleted — but **the escalation it asks for is the wrong
one**: raising DPI cannot rejoin a separator the recogniser never saw (GAS Q4-2010's balance
sheet counts 2 → 21 → 1 fragments at 200/300/400, which is noise, not convergence). The repair
that answers it is `join_lost_separator`, and it is already in the cascade at positions 54-62.

⚠️ **`GTR-1` — A LOST BOX TRUNCATES A GRAND TOTAL, AND `SEAL-2`'s REPAIR IS LOCKED OUT BY
DESIGN.** GAS Q2-2021 reads `TỔNG CỘNG NGUỒN VỐN` as **216,601 against a printed
74,826,437,216,601**; Q2-2020 as **47,956,383 against 67,147,956,383,291** — the tail in the
first, the MIDDLE in the second. `total_from_section` requires the rebuilt sum to be within
`_equal` of the damaged reading, because a SEAL covers digits and the magnitude survives; a lost
box removes them, eight orders out, so the lock that makes `SEAL-2` safe is exactly what refuses
this at all 112 layers. Fixed additively (`_total_from_counterpart`, five layers at 113-117 of
117, `max(strict)` still 48). **The evidence is taken BEFORE the write**: at `onnx@300`
`c_no_phai_tra` 26,981,135,438,346 + `d_von_chu_so_huu` 47,845,301,778,255 = **74,826,437,216,601**,
equal to `tong_cong_tai_san` on the facing page **to the đồng**.

| measured end to end on the real PDFs | verdict |
|---|---|
| Q2-2021 balance sheet | refused at every layer → **OK** at 300 and 400 dpi, 216,601 → 74,826,437,216,601 |
| Q2-2020 balance sheet | refused at every layer → **OK** at all five, 47,956,383 → 67,147,956,383,291 |
| Q2-2016 balance sheet | **still refused, correctly** — `77,021,885` is not a run of `59,526,287,021,889`'s digits |

⚠️ **TWO CELLS, NOT EIGHTEEN**, and what `reconcile` says afterwards is trivially true by
construction (§5 rule 21) — the claim is the three-way agreement above, not the verdict.

⚠️ **AND THE OBVIOUS SECOND HALF OF THE IDEA WAS MEASURED AND REFUSED.** Eight balance sheets
fail on `section sum does not close`, which looks like the same defect one level down — a
truncated COMPONENT, rebuildable as `total − other part`. **It is not.** All eight were dumped
at 200/300/400 dpi and every component came back a FULL 14-digit figure with the containment
signature absent:

| | A + B | printed total | gap |
|---|---|---|---|
| Q2-2013 | 48,541,572,943,966 | 49,122,286,048,449 | 580,713,104,483 |
| Q2-2014 | 48,857,592,309,313 | 49,376,259,275,628 | 518,666,966,315 |
| Q3-2013 | 47,795,326,318,852 | 48,356,717,253,633 | 561,390,934,781 |
| Q4-2014 | 53,311,895,757,929 | 53,791,407,348,105 | 479,511,590,176 |

The gaps are all ~1.1 % of the total, which is a shape, not a truncation. ⚠️ **The identity is
not in doubt** — all **60** GAS balance sheets on disk close `A + B = T` exactly — so one of the
three figures is misread in a way this pass did not identify. **Recorded as UNIDENTIFIED, not as
absent and not as unfixable** (§5 rule 2). ⚠️ Q4-2008 is the lead: its two grand totals AGREE
with each other and both move across DPI (…262… at 200, …362… at 300) while `A + B` stays fixed
at 16,507,862,292,018 — so there the TOTAL is damaged and `_total_from_counterpart` cannot see
it, because the counterpart is damaged identically.

⚠️ **THE CODE COLUMN IS A THIRD CLASS AND IS UNTOUCHED**: Q3-2020 refuses with `assets 270 !=
liabilities + equity 440` and Q1-2026 with `280 != 440` — the `Mã số` column read as the value
(`MSO-4`). `_is_truncation`'s 3-digit floor is what keeps `GTR-1` from silently "repairing" one
of these.

### ⚠️ 6-3. THE DATA AUDIT — 2026-08-22, and the cross-section ENDS 2026-06-25

Measured across every ticker-keyed table in all three schemas. Full tables and the
resulting program is in **[TODO.md](../current_state/TODO.md)** (⚠️ renumbered again 2026-08-23 when the first two items closed); three numbers belong
here because they change how any current result is read.

**1. ⚠️ `MAX(date)` SAYS 2026-08-19 AND FIVE TICKERS PRODUCE IT.** Names per session at
the tail of `silver.stocks_basic`: **779** on 2026-06-25, **627** on 2026-06-26, then
**28**, then **24**, then **5** from 2026-08-10. **757 of 781 tickers are stale**; 599 stop
dead on 2026-06-26. `FRZ-1` recorded this as *"143 frozen tickers"* and that
understates it — **the whole universe stops in late June** and a 24-name tail was refreshed
after. §5 rule 10 at full scale: a 24-name cross-section looks like a working pipeline to
anything reading one number.

**2. ⚠️ GOLD IS BEHIND SILVER AND NOTHING SAYS SO.** `gold.stocks` stops **2026-07-08**
(30 sessions), `gold.stocks_ta` **2026-06-26** (54 sessions, and `STA-1` on top). §5 rule
11 — *"re-scraped" never implies "re-ingested"* — with a measured size. **`filter_schema`
and every `unified_schema_*` sit downstream of both and re-materialise themselves never.**

**3. ⚠️ FUNDAMENTALS ARE 2 OF 781 AND TWO WALLS STAND IN FRONT OF THE OTHER 779 —
WAS THREE** — ✅ **the DISK wall fell on 2026-08-23 and it fell by being MEASURED, not by buying
hardware** (§6-2-septies). It read *"PDFs for 112 tickers = 100 GB, median 906 MB each →
~700 GB for the universe, against 144 GB free"*; counted from CafeF the whole universe is
**555 GiB**, and phasing it by filing year makes the first half **286 GiB**. What remains:
**time** (~2.4 h/ticker of OCR → **~78 days**), and **schema** — **761 of 781 names are
not banks** (230 industrials, 117 materials, 93 consumer staples; only 20 are GICS
401010), against a parser that has never once met a corporate filing. ⚠️ **The schema
wall is the real one and is now the ONLY structural one**: with infinite disk and time
the current parser reaches 20 names.

⚠️ **BUT ITS DIAGNOSIS WAS WRONG UNTIL 2026-08-25, AND THE CORRECTION IS §6-2-quaterdecies.**
This paragraph read *"`raw_data/cafef/financials/statements/` holds one template family,
`bank`"* and was taken to mean a corporate template does not exist. ⚠️ **`statements/` is
the parser's OUTPUT** — it holds one family because one family has been RUN. All **four**
charts of accounts exist (12 files, 871 rows) and the parser is template-generic; what is
bank-shaped is **seven hardcoded reconcile anchors**, two of which hand a non-bank cash
flow the OPENING balance as its closing one. That is `TPL-1`, and it makes `P5` cheaper
and its failure mode worse at the same time.

⚠️ **AND THE ROUTE AROUND IT WAS CLOSED BY DECISION ON 2026-08-23: balance-sheet lines come
from the CafeF PDFs.** `P3` had been a one-day gate — ask whether `api.simplize.vn` or
`vnstock` returns balance-sheet lines for a non-bank, since a positive answer cancels the
whole OCR program. It is archived **UNMEASURED**, so nothing may cite it as evidence that a
JSON source does not work (§5 rule 2 — an absent measurement is absent, never inferred).
What the decision changes is the ORDER: nothing gates the OCR program now, **`P6` (OCR the
≤2020 corpus) is the top item of the whole backlog**, and `P5` — the non-bank template — is
what decides whether that run reaches **20 names or 784**, rather than a task running
beside it.

⚠️ **AND THE DECISION WAS WIDENED ON 2026-08-24 INTO A STANDING RULE — §5 rule 24.** It
is not only the JSON route that is closed: **the PDF is the ONLY permitted source for a
financial statement, and every HTML/web transcription is forbidden, including CafeF's own
tabs.** A quarter no PDF can produce is `missing`, and `missing` is the correct answer.
⚠️ **This is retroactive and there is a bill**: 34 of the 456 report-rows on disk carry
`source='cafef'` — 27 ACB, 7 VCB — because the fallback runs whenever a period is absent
from the parse, without checking whether a PDF exists. ⚠️ **Only 4 of the 34 can be retried
from a document on disk, all of them VCB**: `documents()` keeps `consolidated == "True"`
only, and ACB filed no consolidated statement before 2010. That is `FIN-1`; §6-2-octies has
the quarter-by-quarter table.

⚠️ **Three gaps are deliberate and must NOT be filled**: `cafef_news_sentiment` (3 of 781 —
§2a measured tone making models *worse*), `cafef_prop_trading` (431 of 781 — starts 2023,
and §6-1 says EXCLUDE `prop_*` at this timescale, not extend it), `trading_view_stocks`
(571 of 781 — ⚠️ **not in the price spine at all**; `silver.stocks_basic` is CafeF only,
verified in `_ingest_silver_stocks_basic`).

⚠️ **AND THREE SCREENED UNIVERSES SINCE 2026-08-22** — `unified_schema_price10k`
(480 tickers, 1,503,958 rows), `unified_schema_liquid` (206, 657,892),
`unified_schema_quality` (200, 635,919), each holding `pool__basic` + `pool__targets`
only. Nothing has been SELECTED or MODELLED on any of them; they are universes, not
results. §3a-bis.

⚠️ **The 16 pre-2026-08-16 model runs were deleted on request** and archived to
`D:\GIT\_archive\master-thesis\model_runs_2026-08-16.zip` (2.2 MB, outside the repo,
untracked). `src/model/runs/*/` is gitignored (`RPR-1`), so **that zip is the only copy**
of the numbers §5c and §5d quote.

### The two VCB chains, and which to start new work on

| target | evidence | verdict |
|---|---|---|
| `close_adjust_5day` *(still the `chain.py` default)* | `failed_null=1` | ❌ a price **LEVEL**. Its LSTM: R² **−85.6**, MASE **21.36** (21× worse than a random walk), ROC AUC **undefined**, and the whole test range sits **above** the training maximum |
| **`return_5day`** | `cleared_p95_not_a_pass` | ⚠️ layer 2 "clears" at z = +4.48 — **do not quote it**, see TODO **P0-1** |

### ⚠️ The 2026-08-17 return_5day sweep — six pools, real nulls, all six FAIL

The first time this chain has ever run layer 1 with nulls on a **return** target.

| pool | kept | `ic_mean` | null p95 | null MAX | z | p |
|---|---|---|---|---|---|---|
| `pool__fa` | 137 | +0.0564 | +0.0592 | +0.0794 | +1.24 | 0.182 |
| `pool__ta` | 473 | +0.0434 | +0.0603 | +0.0673 | +0.78 | 0.364 |
| `pool__stock_market` | 125 | +0.0386 | +0.0631 | +0.0672 | +0.48 | 0.364 |
| `pool__news_daily` | 69 | +0.0285 | +0.0443 | +0.0469 | +0.53 | 0.455 |
| `pool__bonds` | 99 | +0.0121 | +0.0339 | +0.0373 | +0.19 | 0.545 |
| `pool__market_breadth` | 64 | +0.0196 | +0.0645 | +0.0745 | **−0.22** | 0.727 |

⚠️ **In all six the null MAX exceeds the observed** — rule 3 applies to every row.
⚠️ `pool__market_breadth` lands **below its null's mean**: its 8 channels were picked by
measuring 7 candidates and keeping 3, and under a real null the advantage is gone. The
selection-on-the-same-data lesson, demonstrated by a pool built to demonstrate something
else. ⚠️ `pool__news_daily` did **not** fail for want of data (z = +0.53, mid-pack) — §2d's
third lever is now measured and it says nothing.

**Layer 2** then reports `ic_mean +0.1369` vs a p95 bar of +0.0428, z = +4.48. Four
measured reasons that is not a result — the null does not price in layer 1, `p = 0.0909`
is the 1/(n+1) floor, the fold trend is rule 23's data-arrival signature, and 9 of 66
channels are constant in train — are in **TODO P0-1**, with a written prediction that a
two-layer null will not clear it.

⚠️ **`STA-1` costs this chain its last 31 sessions.** `pool__ta` stops 2026-06-26, and the
INNER join drops the whole `return_5day` chain from 4,266 to **4,235 rows** — table and
dataset both end 2026-06-25 rather than 2026-08-07.

⚠️ **`--scope` is still the only thing keeping two experiments off one table name.**
`final_features` groups on `(schema, target, setup)` — **no term for which pools** — so a
`pool__basic`-only run and a `basic + X` run are ONE group and get unioned.

**Open issues live in [ISSUES.md](../current_state/ISSUES.md)** — **97 open**, 38 resolved, codes permanent.
⚠️ *(This count is a SCAN of the tables, not a running decrement, and it has been wrong before —
it read 96 earlier on 2026-09-06, 70 until 2026-09-05, 22 until 2026-08-28. A stale count is what
a session budgets against.)* ⚠️ **Several FIXED rows deliberately sit inside the Open table rather
than moving** (`WFO-1`, `VRM-1`, `PNL-2`, `PRB-1`, `SCH-1`, `DEP-1`), each marked `✅ FIXED <date>`
in words — **strikethrough was removed from the whole corpus on 2026-08-23**, so a row's status is
read from its text and never from damaged type.

⚠️ **SEVEN CHANGE HOW A NUMBER MAY BE READ, AND THEY ARE THE ONES TO OPEN BEFORE QUOTING ANYTHING:**

| code | what it does to a number |
|---|---|
| **`NUL-1`** | no null anywhere in this repo prices in the feature selection, the architecture search, or the choice of horizon/universe/target |
| **`NUL-3`** | the evaluator's panel null is **not label-neutral** — its centre moves with the MODEL. **On a panel quote the daily-IC t-stat, never `ic_clears`** |
| **`RPR-1`** | 29 run folders were deleted 2026-08-10 and are unrecoverable — §5c and §5d are citations without their evidence |
| **`OUT-1`** | one corrupt source cell (VCB 2026-01-05, `prop_buy_val` 4.001e17) manufactured a **+0.266** forward correlation. Check the extremes before selecting on any `foreign_*` or `prop_*` channel |
| **`CFB-1`** | ⚠️ **before quoting a BID fundamental** — a cash-flow anchor can hold the wrong ACCOUNT with every gate passing: 7 BID quarters carry the 1-Jan opening in the CLOSING slot, and Q3-2011 holds a **negative** closing cash balance |
| **`TPL-1` / `CRP-1`** | ⚠️ **before any non-bank financials parse** — two of the seven reconcile anchors return the OPENING cash balance as the closing one on `corp` and `insurance`, and a `corp` balance sheet reconciles on the trivial `assets == resources` |
| **`FLT-1` / `SHP-1`** | bound what forex data can EXIST: 19 of 47 broker filters fail open, and a `value`-only filter silently discarded **71 % of the forex folder** on every run until 2026-08-14 — the same filter sits unchecked on `bonds`/`funds`/`economy`/`indices` |

⚠️ **The OCR/filings issue codes** (`SLD-1`, `PAR-1`, `QUO-1`, `MSO-*`, `SEAL-*`, `SET-*`, `BND-1`,
`VCR-1`, `CWD-1`, `GPU-1`, `SGN-*`, `NST-*`, `LSP-1`, …) **are in `ISSUES.md` in full.** A 27k-character inline digest of
them stood here until 2026-09-06 — ⚠️ **read the register, not the snapshot.**

Also worth knowing without opening the file: `EVD-1` the missing nulls are ~1,000 CPU-hours;
`DRF-1` 18 channels put 100 % of test beyond 5 train-sigmas; `COV-1` 248 of 952 shortlisted rows
sit below 0.95 coverage; `RPR-1` datasets and run folders are git-ignored.
---
