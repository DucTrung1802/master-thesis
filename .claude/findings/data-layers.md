# The data pipeline, layer by layer — bronze to unified

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§3a`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **`CLAUDE.md` §3 and §3a-bis in full** — the 83 Dagster assets, the ten `pool__*` tables and
> what each one measured, the forex ingest, and the FILTER layer. Open
> [`../context/orchestration.md`](../context/orchestration.md) for the code behind it.

---

## 3. The pipeline, end to end

### 3a. Data — Dagster, 83 assets, `src/orchestration/`

```
raw_data/<source>/            19 landing assets   scrapers: TradingView (universe+OHLCV,
      │                                           Selenium), CafeF (price/flow/news/PDFs),
      │                                           Simplize, GICS
      ▼
bronze_schema                 20 assets → 25 tables     raw-faithful, one per source/tab
      ▼
silver_schema                 20 assets                 canonical, cross-source merged,
      │                                                 GICS tree attached
      ▼
gold_schema                   11 assets                 + features (TA battery ~900 cols,
      │                                                 returns, vol, as-of macro)
      ▼
filter_schema                  1 asset × 3 SCREENS      ⚠️ NEW 2026-08-22 — universe__<screen>,
      │                                                 the MEMBERSHIP table. No time series:
      │                                                 one row per (exchange, ticker) with
      │                                                 every condition's value and verdict
      ▼
unified_schema_<universe>     12 assets × 3 partitions  pool__basic (⚠️ 38 silver +
                                                        58 drv_*, 2026-08-16) / __targets /
                                                        __economy_<country>×19 / __forex /
                                                        __funds / __bonds /
                                                        __stock_market / __basic_bank /
                                                        __ta / __fa /
                                                        __news_daily / __market_breadth
                                                        (last two NEW 2026-08-17, VCB only)
                              partitions: VCB | BANK | ALL | VN30 | 29 single names
                                          | ⚠️ PRICE10K | LIQUID | QUALITY (screens)
      ▼
reports/feature_selection/     1 asset × 19 partitions   analysis/feature_selection_economy
                              partitions: the 19 countries — ⚠️ writes NO table
```

⚠️ **FOUR DATE-BROADCAST POOLS ARE NEW (2026-08-13)**, built for **VCB** so far and
generated from ONE spec table (`DATE_SPINE_POOLS`). All four are the `pool__economy`
shape — a `date`-only gold source LEFT JOINed and BROADCAST across tickers, so the row
count must EQUAL the spine's — not the `pool__ta` one. Each: PK verified from
`pg_index`, **0 round-trip mismatches** against gold, 0 unaligned keys.

| pool | source | VCB | col coverage | source ends |
|---|---|---|---|---|
| `pool__forex` | `gold.forex`, 357 broker pairs | 4,266 × 360 | median **67.0%** | 29 of 357 in Aug, **328 at 2026-06-08/09** |
| `pool__funds` | `gold.funds`, 21 HOSE ETFs × ≤19 measures | 4,266 × 392 | median **17.7%** | 2 of 21 in Aug, **19 at 2026-06-26** |
| `pool__bonds` | `gold.bonds`, 9 tenors × 13 measures | 4,266 × 120 | median **75.9%** | **all 9 at 2026-06-08** |
| `pool__stock_market` | `gold.stock_market`, 6 indices × 27 measures | 4,266 × 165 | median **83.1%** | 2026-07-30 (CafeF chain) |

### ⚠️ TWO MORE POOLS, 2026-08-17 — and both REFUSE to pivot the market into columns

Built for **VCB only** so far. Together they are the answer to "how do I feed one stock
more than that stock?", and the answer is **compression, not width** — VCB has
`n_eff = 852` independent observations, and §5c measured 202 channels at test IC −0.011
against 724 at **−0.072** on the same splits.

| pool | shape | source | what it is |
|---|---|---|---|
| `pool__news_daily` | 4,266 × 17 | `gold.news_daily_panel` | **14 event channels** — `n_docs`, `n_editorial`, `n_docs_named`, `n_earnings`, `relevance_max`, `if_news`, `if_editorial`, each at 5d and 10d. §2d's third-ranked lever, wired in at last |
| `pool__market_breadth` | 4,266 × 11 | `gold.market_breadth` (NEW) | **8 channels compressing all 781 names to one row per session** — dispersion (`xs_disp5/skew5/kurt5/mean5`), concentration (`hhi_turnover`), flow (`log_turnover`, `turnover_z`), width (`n_names`) |

⚠️ **`pool__news_daily` IS NOT THE SENTIMENT THREAD.** §2a's negative was about PhoBERT
tone scores; this carries no tone at all, only counts. Nine price columns are dropped
because `pool__basic` owns them — and `ret_5d` was verified **trailing** (corr +1.000000
against the trailing 5-day return, −0.006 against the forward one), so it is excluded as a
DUPLICATE, not as a leak. ⚠️ Coverage **78.6%** (corpus starts 2013 against a 2009 spine)
and **31 trailing NULL sessions** — rule 22 says read both.

⚠️ **THE PIVOT IS IMPOSSIBLE, NOT MERELY UNWISE**: 781 tickers × 27 measures is **21,087
columns against PostgreSQL's 1,600** (`WID-1`). ⚠️ `pool__market_breadth`'s channel set was
chosen by MEASUREMENT over 826 non-overlapping observations — the dispersion/flow family
kept (`xs_skew5` t = −2.29), the breadth family dropped (t = +0.21…+0.34, and it restates
the index level `pool__stock_market` already carries). **Not one clears Bonferroni**
(|t| > 2.69), and at layer 1 the whole pool landed **below its null's mean** (§6).
⚠️ `mkt_n_names` rises 380 → 771 across the sample and is a **calendar proxy** — exclude it
before ranking (TODO P0-4).

⚠️ **THE THREE TRADINGVIEW SOURCES ARE FROZEN AND `MAX(date)` HIDES IT** — the
`skip_existing=True` scrape of 2026-08-05, which queued **0 bond data tasks at all**.
`stock_market` is a CafeF chain and 6 days short for a different reason.

⚠️ **`pool__stock_market` CONTAINS THE TARGET'S OWN BENCHMARK.**
`hose__vnindex__close_adjust` is `UNIFIED_BENCHMARK_COLUMN`, the series `pool__targets`
subtracts for `return_rel_{h}day`. The pool carries **`bm[t]` and trailing history,
never `bm[t+h]`** — verified, 0 rows hold a future value — so there is **no leakage**;
what is true is that the target's own denominator is now a feature. Its order-flow and
foreign-flow measures are the closest anything in this database gets to §2d's top lever.
⚠️ `pool__funds` is **31.7% NULL by construction** and its widest column IS the VN30
index. ⚠️ On `pool__bonds` **the slope is the signal and it is not a column**:
`vn10y − vn02y` must be derived. `.claude/context/orchestration.md` §"`pool__forex`" /
§"`pool__funds`" / §"`pool__bonds`" / §"`pool__stock_market`".

⚠️ **`unified/pool__basic_bank` (2026-08-14) is the fifth, and the only one with NO
TABLE BEHIND IT.** `silver.stocks_basic` filtered to GICS 401010 and **pivoted on the
fly** to `{exchange}__{ticker}__{measure}` — 20 banks × 27 measures = **540 channels**,
VCB 4,266 × 543, 0 mismatches against silver. ⚠️ **It found that `pool__basic` was 12
columns behind its own source**: silver has 38, the pool on disk had 26, the missing
twelve all flow (`foreign_*` ×8, `prop_*` ×4) — ✅ **fixed 2026-08-16, all three
partitions rebuilt.** ⚠️ **The schema's own ticker is one
of the channels** — `hose__vcb__*` IS `pool__basic`'s own columns (asserted, 0 mismatches
on 15 mirrored measures), so joining both holds each VCB measure twice. ⚠️ Membership is
derived from current GICS and is **not point-in-time** — survivors only.
**`pool__basic_vn30` was deferred** for the same reason in its worst form (`vn30.csv` is
today's list with no history), and **`pool__financials` was not built — it is
`pool__fa`.**

### ⚠️ `pool__basic` CARRIES DERIVED FEATURES NOW (2026-08-16) — it is not a copy

It was `SELECT *` over `silver.stocks_basic` for its whole life. It is now that **plus
58 trailing `drv_*` channels** computed in SQL in the same CTAS — **63 on a universe
partition**, where 5 cross-sectional ones are added. VCB **4,266 × 96**, BANK
**54,528 × 101**, ALL **2,388,975 × 101** (11m8s). The surviving contract is the SUBSET
one and it is still asserted: every silver column, silver's type, silver's value. The
derived set is asserted as an **equality**, so a leaked CTE helper raises.

Seven blocks, chosen against `gold.stocks_ta`'s 935 columns to avoid duplicating it:
**bar shape** (7 — incl. `gap_open_pct`, the only overnight information a daily bar
has), **range volatility** (9 — Parkinson / Garman-Klass / Rogers-Satchell, none of
which existed anywhere), **normalisation** (13 — `close_z_*`, `close_pos_*`,
`dist_from_high_*`, skew/kurt), **order flow** (10 — §2d's top lever at daily grain;
`pool__ta` has 0 hits for "order" or "imbalance"), **foreign/prop** (8), **liquidity**
(7 — Amihud, VWAP), **cross-sectional** (5, universe only — per §2b the one block
anything has ever survived a null in).

⚠️ **Five traps, all measured** (`.claude/context/orchestration.md`): silver's `open/high/low`
are **RAW** and track `close_raw` (4,266/4,266 vs 248 on VCB), so the bar is
split-adjusted first; **`value_matched` is BILLIONS of VND** while `foreign_*_value` /
`prop_*_val` are plain VND (the first draft reported a participation ratio of
215,150,099); **bigint/bigint is integer division** (a channel returned a flat 0);
`STDDEV_SAMP` over bigint returns `numeric` → the rule-15 `Decimal`→`object` trap; and
**PostgreSQL computes PARTIAL frames by default**, so a 252-day channel was a 10-day
channel for every series' first year — 188,737 rows of `ALL`. ⚠️ **pandas could not see
that last one**: `rolling(w)` defaults to `min_periods=w`, so the cross-check compared
only where both were defined.

✅ **Verified**: 20 channels against an independent pandas recomputation (16 at ≤9.5e-13;
skew/kurt are the **population** estimators, matching `scipy…(bias=True)` at 2e-15 and
differing from pandas' sample-corrected form by design), **0 name collisions** with
`pool__ta`/`pool__fa`, **0 history bleed** across all 781 `ALL` series, and a causality
test — the whole block rebuilt on data truncated at 2026-06-15 reproduces all 58 columns
on 4,227 shared rows at **max abs diff exactly 0.0**.

⚠️ **`OUT-1`: one corrupt source cell manufactured a finding — FIXED 2026-08-16 in
silver.** `silver.stocks_basic` VCB **2026-01-05** carried `prop_buy_val = 4.001e17`
against that day's whole turnover of 2.06e11 — an implied 5.7e11 VND/share. That single
cell drove `corr(drv_prop_net_value_ratio, drv_prop_participation)` to **exactly +1.0**
and manufactured a **+0.266** correlation against the forward 5-day return.

`_helper_screen_flow_outliers` NULLs a flow value/volume pair (never winsorises — the
corruption factor is not constant, so there is nothing to divide out) on **three** rules,
and it took three because each of the first two has a blind spot: **implied price** off
`close_raw` by >100× (99.5% of flow rows sit within **2×**, 99.98% within 100×);
**flow volume** >100× the day's total, for rows where value *and* volume are corrupt
together so the price looks fine (`STB` carries 1.5e13 shares); and **flow value** >100×
turnover, the only rule that works when the volume is NULL — `SHB 2025-10-30` slipped
past the first two that way. **611 of 2,388,975 rows (0.0256%)**, row count unchanged.
On VCB the two symptoms go **+1.000000 → +0.3270** and **+0.2658 → +0.0280**.

⚠️ **A second defect, in the derived block itself, found in the same pass**: the flow
ratios divided by MATCHED turnover, but flow trades in the negotiated channel too
(`ABB 2026-06-26`: 19 bn matched against **393 bn negotiated**). The denominator is now
matched + negotiated, taking `drv_foreign_net_value_ratio` from **[−239.6, +75.0]** to
**[−4.87, +2.27]**. ⚠️ **Two classes remain unscreened and reported**: 2,844 pairs with
a real volume and a ZERO value (mixed — 305 of 857 `prop_sell` imply ≥1 BN VND, so *not*
just rounding), and 196 rows with flow on a no-trade day.

⚠️ **NEW ISSUE `STA-1`: `gold.stocks_ta` was not built by the current builder.** It
carries **13 legacy column names** (`val_matched_bn`, `f_net_val`, `vol_matched`, …) and
**zero** of silver's, holds **2,678,167 rows against silver's 2,388,975**, and stops at
2026-06-26. Nothing in the repo produces those names. So rebuilding it is not
maintenance — it renames 13 columns and moves 289 k rows, and `pool__ta` inherits all of
it. That is why the 1e9 fix below shipped as code **without** the rebuild.

⚠️ **The same 1e9 unit bug was found in `gold.stocks_ta.foreign_net_val_ratio`** —
`ta_functions.py:2773` and `sentiment_features.py:142` divided VND by billions of VND.
median(stored ÷ honest) = **999,999,998.1** over 769,188 rows; 46% of 1.67 M rows hold a
"ratio" above 10. **Both fixed.** It changed no result and that is worth stating: an
exact constant multiplier is rank-preserving (`Spearman = 1.0000000000`), every ranker
here is rank-based, and `StandardScaler` removes a constant scale — wrong **units**, not
wrong **ordering**. The table on disk still holds the old values (STA-1).

### ✅ FOREX: 357 → 3,129 series, ingested end to end (2026-08-14)

`bronze 13,662,058 rows / 3,129 series / 48 exchanges / 2000-01-02 → 2026-08-14` →
`silver.forex` (same) → **`gold.forex_<exchange>`, 48 panels summing to 3,129** →
**`unified_schema_vcb.pool__forex_<exchange>`, 48 pools × 4,266 spine rows**.
Round-tripped at every hop, 0 mismatches; the pre-split `gold.forex` and `pool__forex`
are dropped, on success only.

⚠️ **`gold.forex` IS MANY TABLES NOW (`WID-1`, resolved same day).** 3,129 series is
3,130 columns against PostgreSQL's 1,600, so it took the split `gold.economy` makes per
country — widest panel `forex_fx_idc` at **648 columns**. `gold/forex` left the
`WIDE_PANELS` spec table (that builder asserts one row count, one column count, one date
range — true of a table, false of a family) and `pool__forex` followed to
`pool__forex_<exchange>`. **Anything naming `gold.forex` or `pool__forex` is naming a
dropped table.** ⚠️ Unlike economy these panels do **not** share a calendar and it is not
asserted: brokers quote what they quote (B2PRIME starts 2015, SAXO 2000).

⚠️ **AND CLEARING IT EXPOSED `SHP-1`: 71% OF THE FOREX FOLDER HAD BEEN SILENTLY
DISCARDED ON EVERY PREVIOUS RUN.** The scraper writes two file shapes — OHLCV or
`value`, **4,402 files against 1,787** — and every clean layer filters on `value`, so the
old all-files `pd.concat` produced a `value` column from the *other* files and dropped
every OHLC row without a word. That is the whole reason bronze held 357 series. `value`
is now coalesced from `close`, justified by the extraction JS rather than by overlap
(no series carries both shapes): it pushes `v[4]` as `close` in one branch and `v[4]` as
`value` in the other — **the same slot of the same array**. ⚠️ **`bonds`, `funds`,
`economy` and `indices` have the same filter and have never been counted.**

⚠️ **The bronze forex ingest reads in BATCHES of 300 files** (6,189 files / 2.19 GB /
29.6 M rows would need 10-15 GB against 3.6 GB free). That also inverts which duplicate
wins: the old `keep="first"` in glob order let the **stale** file win, batched upserts in
name order let the **newest** win.

### ⚠️ How the forex got there — the scrape (2026-08-14)

Re-scraped in two runs, both green, 0 ERROR lines: **links 47 of 47 brokers (1h09m)**,
**data 10 brokers (2h15m, 668 fetched + 229 skipped)**. Verified symbol-by-symbol
afterwards rather than from the green run — **897 of 897, 0 missing, 0 empty**, each
folder single-exchange, fresh to 2026-08-13.

**`parameters.data_only` is new and is the one place links and data may disagree** —
links enumerate everything, the fetch is restricted to a subset. Data may never enable
what links does not (the adder reads the links CSV its own leaf wrote); `validate()`
raises on that, on an empty list and on an unknown class.

| | |
|---|---|
| brokers enumerated | 47 of 47 — **27 clean, 19 contaminated, 1 empty** |
| symbols fetched | 897 of 1,722 addressable = **52%** (10 of 27 clean brokers) |
| series on disk | 6,189 files / 2.19 GB → **3,129 series, 48 exchanges** (all ingested) |

⚠️ **`FLT-1` — the forex broker filter fails OPEN for 19 of 47 brokers**, returning a
49-exchange `FX_IDC` default list; six returned byte-identical ~16,700-symbol lists.
**37 of 47 brokers' own books are unreachable until it is fixed**, and the folder name in
`data/forex/` tells you nothing — the filename does. The 2,177 series that came from
those mixed folders are real data under wrong folder names, and they ingested fine:
bronze splits `symbol` on `:`, so the exchange is always correct.

⚠️ **Never sum rows across a leaf's links CSVs.** A broker folder accumulates one dated
CSV per run (5 now) and the data adder reads only the NEWEST. Summing reads as growth
that is not there — saxo "269 → 438" where the newest snapshot holds 169 and the union
of all five also holds 169.

⚠️ **One asset writes no database table.** `analysis/feature_selection_economy`
(2026-08-10) runs the selection over `pool__basic + pool__economy_<country>` and
archives a run folder; `feature_selection` is read-only by design. It defaults to a
**20-draw null** (the 18 hand-launched country runs all used 0) and **raises** both when
the country pool is behind `pool__basic`'s calendar and when its fitted cost estimate
exceeds `budget_minutes` — `usa` is 1,458 channels, 7.2 h with no null and **6.3 days**
at 20 draws. `.claude/context/feature_selection.md` §15.

### ⚠️ 3a-bis. THE FILTER LAYER — a universe is a DECLARED SCREEN now (2026-08-22)

`filter_schema.universe__<screen>` sits between gold and unified and answers the one
question the unified layer could not previously express: **which tickers are allowed
in.** Until now that had exactly three answers hard-coded in `UNIFIED_MEMBER_FILTERS` —
everything (`ALL`), one GICS industry (`BANK`), one frozen index list (`VN30`) — and a
fourth meant writing SQL inside a class constant.

A **screen** is a named list of **conditions**; a condition measures ONE number per
`(exchange, ticker)` and compares it to a threshold. Both live in
`src/orchestration/preprocessor/filters.py`, which is **pure** — no I/O — so
`test_filters.py` pins the definition half without a database (**30 tests**).

```powershell
dagster asset materialize -f src/orchestration/definitions.py `
  --select "filter/universe" --partition PRICE10K    # the membership table (~1 s)
dagster asset materialize -f src/orchestration/definitions.py `
  --select "group:unified"  --partition PRICE10K     # the schema it gates
```

**Built and verified 2026-08-22.** ⚠️ Every one is `pool__basic` + `pool__targets` only;
the other pools are unbuilt for these partitions.

| screen | conditions | selected | first failure | `unified_schema_*` |
|---|---|---|---|---|
| **`PRICE10K`** | 1 — `close_raw` never below 10,000 VND since 2026-01-01 | **480 / 781** | `close_raw_min_10k` 301 | **1,503,958 × 101** |
| **`LIQUID`** | 4 — 1 bn/session median matched turnover, 80 % traded days, 200+ sessions, still quoted | **206 / 781** | `turnover_median_1bn` 545, `sessions_min_200` 30 | **657,892 × 101** |
| **`QUALITY`** | 6 — `LIQUID` + a 5,000 VND median price floor + a debt/equity ceiling | **200 / 781** | `turnover_median_1bn` 426, `close_raw_median_5k` 125 | **635,919 × 101** |

✅ **Verified against silver with code sharing nothing with the builder**: `PRICE10K`'s
480 members are exactly the 480 pairs with `MIN(close_raw) >= 10000` since 2026-01-01 —
**0 extra, 0 missing** — and the built schema holds **0 rows below 10,000 VND after
2026-01-01**. Row counts match silver-filtered-to-members exactly on all three, and **0
`QUALITY` members sit outside `LIQUID`**.

Four things worth carrying forward:

1. ⚠️ **A SCREEN IS NOT POINT-IN-TIME.** Membership is decided from a window and applied
   to the WHOLE history — `PRICE10K` picks names that traded above 10,000 VND *in 2026*
   and carries that back to 2009. §2c's defect in its purest form: **benign for a
   within-date shuffle null** (every draw sees the same basket, so a `z` is protected)
   and **fatal for any CAGR** read off one of these schemas. Every window is JSON-encoded
   into the table `COMMENT`, and the asset refuses to finish if that comment's
   fingerprint disagrees with the definition that built it.
2. ⚠️ **`gold.stocks_financials_bank_fa` HOLDS TWO TICKERS OF 781** (ACB, VCB), so the
   debt/equity condition is `on_missing="keep"` and **abstains on 779 names**, reporting
   a **100 % pass rate against a 0.3 % measurement rate**. That is rule 22 at the filter:
   a condition everything cleared and a condition nothing was measured for look
   identical. `measured` sits beside `passed` in the metadata and the asset WARNs. ⚠️ Its
   threshold is **12×, not 3** — ACB is 9.44 and VCB 9.90, because the only fundamentals
   here are bank fundamentals.
3. ⚠️ **THE EDGE INTO `unified` IS NOT DECLARED**, because the two are partitioned on
   different sets and a Dagster dep is per-ASSET. `_helper_unified_member_filter` raises
   with the materialize command instead. **The cost is rule 14 from the other side:
   re-running a screen does NOT mark the unified schema stale** — rebuild it yourself.
4. ⚠️ **EVERY CANDIDATE IS WRITTEN, NOT ONLY THE SURVIVORS** — 781 rows with
   `val__<cond>`, `pass__<cond>`, `passes` and `first_failed`. A survivor list answers
   "who is in"; this answers "why is HPG out", which is the question a threshold change
   actually raises.

⚠️ **Building the first screen found a latent defect in `_ingest_unified_pool_basic`**:
its log-scope string was `predicate.replace('%s', repr(*params))`, correct for all three
original sentinels (each binds exactly one value) and `TypeError: repr() takes exactly
one argument (0 given)` for a screen, whose sub-select binds none. **A display helper
took down a build.** Fixed and pinned. `.claude/context/orchestration.md` §"FILTER".
