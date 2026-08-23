# CLAUDE.md — master-thesis

@docs/INDEX.md

> **ONE FILE, WHOLE PROJECT.** This is the map. The twelve `CONTEXT.md` files are the
> evidence behind it (**~227k tokens** — re-measured 2026-08-22, up from the ~145k this
> line claimed for months) — **open one only when you touch that package**, and §7 says
> which. Hub written 2026-08-10 against the state at commit `fcac8904`.
>
> ⚠️ **ALL PROSE DOCUMENTATION LIVES IN [`docs/`](docs/) SINCE 2026-08-22**, and the line
> above this paragraph is why: `@docs/INDEX.md` imports the documentation map into every
> session automatically. **[docs/INDEX.md](docs/INDEX.md) routes all 127 `.md` files with
> a measured token cost each** — read it before opening anything, and add a row to it when
> you write a new doc (`python docs/check_index.py` fails if you forget). ⚠️ **The corpus
> is ~511k tokens, ~2.5× a context window, so it can never be bulk-loaded** — the index is
> deliberately routing and not content. `CLAUDE.md` itself stays at the repo root because
> that is the only place Claude Code auto-loads it from.
>
> ⚠️ **Everything here is a claim that was MEASURED.** This repo's convention is that a
> number without a null is descriptive, not evidence. Keep it: verify before acting, and
> when you measure something new, write the measurement down where it was made.
>
> ### The four registers — one job each, no overlap
>
> | file | answers | when you touch it |
> |---|---|---|
> | **[CLAUDE.md](CLAUDE.md)** | *what is this, and what has it PROVED?* | auto-loaded every session; the map and the verdict. **Root, not `docs/`** |
> | **[docs/RUNBOOK.md](docs/RUNBOOK.md)** | *how do I RUN it?* | commands, stage order, the flags that destroy things |
> | **[docs/ISSUES.md](docs/ISSUES.md)** | *what is BROKEN?* | permanent codes; a code is never renumbered or reused |
> | **[docs/TODO.md](docs/TODO.md)** | *what is NEXT?* | one list, `P1` first. ⚠️ **A bare `P<n>` is LIVE; a HYPHENATED code (`P1-9`, `PRF-8`, `M-3`) is RETIRED** — renumbered 2026-08-21, crosswalk at the top of that file |
>
> Movement between them is one-way and worth knowing: a TODO item that turns out to be a
> defect **graduates to ISSUES.md with a code**; an ISSUES entry that gets fixed keeps its
> row and is struck through; a TODO item that gets done leaves its measurement in CLAUDE.md
> or a `CONTEXT.md` and is **deleted, not ticked**. Anything a future session must not
> rediscover belongs in CLAUDE.md, not here.

---

## 1. What this project is

A master's thesis on **predicting Vietnamese stock prices**. It is one repo carrying two
things: a **production data pipeline** (scrape → PostgreSQL medallion layers → feature
pools → model → scored run) and a **research record** of what that pipeline has been able
to prove, which is mostly negative and deliberately so.

| | |
|---|---|
| database | PostgreSQL `database_main_v2`, schemas `bronze_schema` / `silver_schema` / `gold_schema` / `unified_schema_<universe>`. Creds in repo `.env` (`POSTGRES_*`) |
| orchestrator | **Dagster, 83 assets** — `src/orchestration/` is THE entry point |
| universe | 781 VN tickers (HOSE/HNX/UPCOM); VCB is the single-name focus, VN30/VN100/LIQUID301/ALL/BANK the cross-sections |
| model | LSTM (2×128, ~276k params) and **CNN** (`Conv1d` over time) + the chain in §3b. `model/common/engine.py` is the shared engine — a model package is a `model.py` + a ~30-line binding, **never a copy of `train.py`** |
| interpreter | `mt_env` venv (`d:/GIT/master-thesis/mt_env`), Python 3.12.10, Windows, RTX 3050 4 GB |

---

## 2. ⚠️ THE VERDICT — read this before proposing any modelling work

**Single-stock short-horizon prediction does not work here, and this has been established
four independent times.** The one thing that survives its own null is the
**cross-sectional relative rank** at ~100+ names.

### 2a. What failed, and how it was measured

| thread | target | result | where |
|---|---|---|---|
| LSTM sweeps, 9 lookbacks × 3 targets (27 runs) | `return_5day`, `direction_5day`, `probability_gain_5pct_5day` | **no lookback beats the zero-baseline**; `dir_auc` ≈ 0.5 throughout | `model` §10 |
| feature selection, VCB, 5 configs | `return_5day` / `return_rel_5day` × 3 representations | **every one inside its own shuffled-label null** | `feature_selection` §6b–6d |
| news sentiment, 3 tickers | price level / direction / ≥5% jump / price-defined sentiment | no signal — and adding sentiment **makes price/TA models worse** (QWK 0.175 → 0.045) | `sentiment` §6, §6a |
| news EVENTS (not tone), 777 tickers | `rel5` / `rel10` / 1-13 weeks, paired by fold | **7 paired tests, every \|t\| < 1.3**, folds won 2-4 of 6, signs mixed. §6a reproduced at 259× the width | `orchestration/todo.md` (retired 2026-08-17) |
| **single stock, h=10, 5 tickers (2026-08-19)** | **`return_10day`** on `pool__basic`, HPG/SSI/FPT/VIC/STB | **pooled excess over each run's own null +0.0332 ± 0.0229, t = +1.45, p = 0.220**; rule 3 fires on 4 of 5 | §6-1 below |
| literature, 23 papers | others' claims | **not one reports a naive baseline**; reported skill tracks test-set size (0.90@10d → 0.56@100d in one paper's own table); best honestly-run paper gets MCC 0.069 | `experiment_10` |

### ⚠️ 2a-bis. THE HORIZON IS THE VARIABLE NOBODY CONTROLLED FOR — and `h=5` is the worst of it

Measured 2026-08-03 on the weekly/daily news panels and **recovered 2026-08-17 from a
retired todo file, where it was the only copy.** The `controls` block — momentum
1/4/12/26 weeks plus liquidity, no text at all — is the one thing in that study that
worked, and whether it works **depends entirely on the horizon**:

| horizon | universe | `controls` CAGR | benchmark | verdict |
|---|---|---|---|---|
| **rel5** (5 sessions) | top-100 | **−2.78%** | 9.75% | ❌ loses |
| **rel5** | top-30 | **2.68%** | 16.48% | ❌ loses |
| rel10 | top-100 | 9.86% | 9.98% | ❌ ties |
| rel10 | top-30 | 7.69% | 16.74% | ❌ loses |
| **4 weeks** | top-30 | **30.39%** | 18.07% | ✅ Sharpe 1.10 |
| **13 weeks** | top-30 | **28.63%** | 19.34% | ✅ Sharpe 1.06 |

**`controls` only beats its benchmark from 4 weeks out.** At 5-10 sessions it loses even
on the most liquid names — so at `h=5` it is not only news that dies, **momentum dies
too**. MCC on the full universe was +0.052…+0.061, positive in **30 of 30 folds**, and it
cost nothing: no corpus, no labelling, no fine-tune.

⚠️ **This bears directly on §2's verdict and on everything run at `d=20, h=5`.** Four
independent threads failed at a 5-day horizon; this is the one measurement that asked
whether the HORIZON was the problem rather than the features, and its answer is that VN
gives signal at 4-13 weeks and not at 5-10 sessions. Two independent sources predicted
it — the `project-vcb-forecasting-conclusion` memory, and paper 57 (daily news predicts
1-2 days then stops at t = 1.2; weekly news predicts 13 weeks). **Nothing in this repo
has yet been run end to end at a 4-week horizon.**

### 2b. What survived — the width ladder

Same pipeline, same target, same `d=20, h=5`, only the number of names changes. **The
observed IC barely moves; the noise floor collapses.**

| universe | N | daily-IC sd | observed IC | null p95 bar | z | clears |
|---|---|---|---|---|---|---|
| VCB | 1 | ~1.0 | +0.0559 | +0.0556 | +1.56 | ❌ |
| BANK (GICS 401010) | 20 | 0.244 | +0.0087 | +0.0249 | +0.11 | ❌ |
| **BANK re-measured 2026-08-10** | **20** | **0.230–0.280** | **−0.0106** | **+0.0216** | **−1.71** | ❌ **below its null's MEAN** |
| VN30 | 30 | 0.205 | +0.0233 | +0.0248 | +1.42 | ❌ |
| **VN100** | **100** | 0.130 | +0.0289 | +0.0117 | **+6.09** | ✅ |
| **LIQUID301** | **301** | — | +0.0768 | +0.0245 | **+18.45** | ✅ |
| ALL | 780 | — | +0.109 | *never ran* | — | ⚠️ unverified |

Three things this ladder means:

1. **The practical threshold is ~100 names, not ~30.** VN30 fails at `z = +1.42`,
   indistinguishable from one stock. A 20-name sector is worse.
2. **The mechanism is `1/√N` precision, not more observations.** `n_eff` is `n_dates/h`,
   **not** `n_rows/h` — 100 stocks on one Tuesday are one observation of the market. The
   cross-section buys precision per observation and nothing else.
3. **The rank target is the whole result.** Swapping `cs_rank_5day` for raw `return_5day`
   on the same panel and folds drops the IC 4× and pushes the hit rate below a coin.

### ⚠️ 2b-bis. THE WIDTH LADDER MEETS THE HORIZON — `cs_rank_20day`, top-150, **z = +9.09**

Measured 2026-08-18 on a Kaggle T4, **6 h 07 m**, the first time this repo has run a
cross-section at the horizon §2a-bis pointed to. `unified_schema_all`, top **150** names
by median matched turnover **before 2014-01-01**, 624,448 rows × 4,368 sessions,
`d=20 h=20`, `n_eff = 218`, 90 channels → 61 kept.

| | |
|---|---|
| observed `ic_mean` | **+0.1075** — folds +0.060 / +0.124 / +0.153 / +0.104 / +0.097, **all five positive** |
| `ic_trend_per_fold` | +0.0054 — flat, not decaying (rule 5) |
| null: 20 draws, `date_block`, **the whole selection re-run inside each** | mean **+0.0291**, sd 0.0086 |
| p95 BAR | **+0.0388** |
| null MAX | **+0.0410** — *below* the observed, so **rule 3 does not fire** |
| **z** | **+9.09** · p = 0.0476 (the 1/21 floor — read z) · 0 failed draws |

**This is the strongest selection-stage evidence in the repo**, and the first at a 4-week
horizon. Four things it does **not** establish, and they are the reason it is filed here
rather than in a conclusion:

1. ⚠️ **THE NULL'S MEAN IS +0.0291, NOT ZERO.** The procedure earns +0.029 on a shuffled
   label, so the excess over chance is **+0.078, not +0.1075** — quoting the raw IC as
   "the skill" overstates it by 37 %.
2. ⚠️ **NO HOLDOUT.** §2c records exactly this trap: the VN100 result cleared its bar and
   then the holdout gave +0.011 against a shuffled control of +0.0071. `HOLDOUT_START` was
   `None` here, so this is cross-validation with an honest bar, **not** an untouched tail.
3. ⚠️ **The null prices in FEATURE selection, not EXPERIMENT selection.** The universe, the
   horizon and the target were each chosen using prior evidence (§2a-bis, §2b). That is
   `NUL-1`'s shape one level up, and no null here prices it.
4. ⚠️ **A cleared selection bar has never yet survived downstream in this repo** — §5d and
   TODO P2-3 both. The model stage is the next question and not a formality.

⚠️ **Do not paste this row into §2b's ladder.** That table is `h=5` at one lookback; this
is `h=20`, a different label, a different `n_eff`, and a universe fixed on pre-2014
liquidity (**358 of 781 tickers could never enter it**, and it is not point-in-time).

### 2c. ⚠️ And the honest caveats on the survivor

- **The holdout does NOT confirm it.** +0.011 against a shuffled control of +0.0071, with
  SE ≈ 0.013. The uncontaminated estimate of what this earns out of sample is **+0.011**,
  not +0.029. (`feature_selection` §9g)
- **The universe is 100% survivors** — `silver.stocks_basic` holds no delisted name. This
  biases the overall result *toward* zero, **except** through `close_adjust`, where the
  truncation itself manufactures the "cheap stocks outperform" pattern.
- **It has never been traded profitably.** Costed walk-forward: AUC ceiling ≈ 0.52–0.53,
  65–78%/leg weekly turnover, net@20bps **+1.46 Sharpe (2017-20) vs −0.51 (2022-26)**, dead
  at 40bps. (`model` §11, `experiment` exp_3)
- **The current end-to-end chain shows no skill on either universe** — see §6.

### 2d. The one lever left

**New information, not more modelling.** Ranked: active/aggressor buy-sell imbalance
(needs intraday tick), fundamentals/earnings surprise, news+disclosure event dates,
**point-in-time index membership** (kills the survivorship bias above), order-book L2.
`model` §11 has the full ranked list with why each matters.

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
`vn10y − vn02y` must be derived. `orchestration/CONTEXT.md` §"`pool__forex`" /
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

⚠️ **Five traps, all measured** (`orchestration/CONTEXT.md`): silver's `open/high/low`
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
at 20 draws. `feature_selection/CONTEXT.md` §15.

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
took down a build.** Fixed and pinned. `orchestration/CONTEXT.md` §"FILTER".

### 3b. Model — EIGHT stages, each `python -m <pkg>`, dry-run by default

```
raw_data/ → bronze → silver → unified pool__*     data               ⚠️ NEW 2026-08-10, ⚠️ the network
   ▼  python -m feature_selection.run --pools pool__basic --null-draws 20
reports/feature_selection/<run>/outstanding.csv   selection          ⚠️ MANUAL for the WIDE pools
   ▼  python -m final_features --apply --shape shortlist
unified_schema_<t>.pool__shortlist__<tgt>__d<d>_h<h>  shortlist_pool ⚠️ writes the DB — §3c
   ▼  python -m feature_selection.run --pools pool__shortlist__<tgt>__d<d>_h<h>
reports/feature_selection/<run>/outstanding.csv   selection_2        ⚠️ MANUAL — the ONE run where
   ▼  python -m final_features --apply                                  the channels compete
unified_schema_<t>.<target>__final__d<d>_h<h>     final_features     ⚠️ writes the DB (same package)
   ▼  python -m train_test_creator --save
src/train_test_set/<dataset>/                     train_test_creator  X/y tensors + scalers + metadata
   ▼  python -m model.lstm --config <cfg>   |   python -m model.cnn --config <cfg>
src/model/runs/<run_id>/                          model.<arch>        config, checkpoints, predictions
   ▼  python -m result_evaluator
results/metrics.json + runs/index.csv             result_evaluator    scored vs a block-shuffled null
```

`python -m pipeline` prints which stage is stale and runs the stale ones. **It passes no
data between stages** — each already reads the previous one's output; the module only
*checks* that what the next stage will read exists and agrees.

⚠️ **The `data` stage's status is a DATE, never a green asset** (§5 rule 10). It compares
`MAX(date)` in `pool__basic` against the newest date in the raw CafeF CSV, and on its
first run that caught a real 31-session gap. `--rescrape` is opt-in and is scoped to
`--ticker` with `skip_existing=False` — without both, a "re-scrape" either costs 781
tickers or fetches nothing.

⚠️ **`--root` + `--scope` run a NARROWER experiment without dropping the wider table.**
`final_features` groups on `(schema, target, setup)`, a key with **no term for which
pools** — so a `pool__basic`-only run archived into the default root silently widens
`return_5day__final__d20_h5` and triggers the STL-1 domino. `--root` keeps the run out of
that group; `--scope basic` names its table `…__d20_h5__basic`. Both are needed.
`src/pipeline/CONTEXT.md` §5c.

⚠️ **`d` and `h` come from the source TABLE NAME**, never a parameter. They flow
`return_5day__final__d20_h5` → dataset `metadata.json` → asserted against the model config.

⚠️ **THE NOTEBOOK AND `feature_selection.run` PRODUCE THE SAME ARTEFACT SINCE
2026-08-16 — AND DID NOT BEFORE.** `RUN__feature_importance_report.ipynb` recorded no
`input.columns_by_table` (so the next stage GUESSED each channel's pool, which returns
`unknown` for every pool built since 2026-08-10 and silently names `pool__ta` for a forex
channel), no `execution` block, and **no `outstanding.csv` at all** — and a run folder
without one is skipped by `final_features.plan_from_reports` **without a word**. Measured
2026-08-15: the two newest runs, both produced through `kaggle_gpu`, were in exactly that
state while `final_features` planned 19 runs and reported no error. The notebook now
writes and validates the shortlist (`contract.validate_shortlist`) and prints
`contract.describe()` — the handoff as the next stage will see it.
`feature_selection/CONTEXT.md` §18.

⚠️ **EVERY ENTRY POINT PRINTS ITS GPU AND ITS RUNTIME NOW** (`utils/runtime.py`, one
clock and one GPU probe, GMT+7). `pipeline` and `kgpu` were the two that were left and
they are the two that run longest. **`pipeline` needed a SECOND clock, per stage**: it
calls the stages IN-PROCESS, so each module's own banner lives in its `main()` and never
fires — a `--apply` run printed no per-stage timing at all. Its `runtime` column is
**empty, never `0`**, for a skipped or planned stage. **`kgpu` prints no GPU on purpose**:
the card that matters is a Kaggle T4, and its clock is the ROUND TRIP, not the selection.
`feature_selection/CONTEXT.md` §18a.

### 3c. ⚠️ TWO selection layers, and the pool between them (2026-08-13, REBUILT 2026-08-16)

⚠️ **THE CODE BELOW WAS DOCUMENTED HERE FOR THREE DAYS WITHOUT EXISTING.** It was
written and RUN on 2026-08-13 — `pool__shortlist__close_adjust_5day__d20_h5` (4,266 ×
892) sat in the database with its source runs named in its own `COMMENT` — and **never
committed**: `git log --all -S"Pre-final shortlist"` finds no commit, `final_features`
on disk had no `--shape` flag, and the `final_features/CONTEXT.md §8` this section
cites did not exist. Rewritten 2026-08-16 from that table's `COMMENT` and this section.
**A documented feature is not a shipped one, and the check that catches it is `grep`.**

**The layer-1 union is not a consensus.** 725 of 750 channels in the old VCB table were
chosen by exactly one run — arithmetic, not agreement: each run saw `pool__basic + one`
macro block, so a macro channel *could not* be a candidate twice. `final_features`
§6 called the fix "ONE selection run over the joined pool"; the pre-final pool is the
cheap version — the one run over the 889 **survivors** instead of ~3,000 candidates.

- **`pool__shortlist__<target>__d<d>_h<h>`** — `--shape shortlist`. Keys + channels,
  **no label column** (`pool__targets` is joined by whoever reads it, as for any pool).
  The `pool__` prefix is load-bearing: `--pools`, `UnifiedSchemaReader.pools()`, the
  pipeline calendar check and the run-folder scope all key on it, so it needed no new code.
- **The target is IN the name** because this pool, unlike every raw one, is
  **target-conditioned** — its channels were kept *using* that label at that window.
  Selecting over it for another target is leakage.
- ⚠️ **Never name a shape with a `__final__` SEGMENT.** `FINAL_TABLE`'s target group
  permits underscores, so `…__pre__final__d20_h5` parses, yielding `target='…__pre'` — a
  column that exists nowhere, found stages later. `__prefinal__` was rejected for this.
- **A run is layer 2 iff its `outstanding.csv` says `source_table=pool__shortlist__*`** —
  a fact about the run, not a flag. `final_features --shape final` then builds from
  layer 2 **whenever any exists**, else unions everything as before. The switch shows up
  in the printed plan, in the table `COMMENT` (`Selection layer N:`) and in the
  fingerprint, so a stale table reports STALE rather than being accepted.
- ⚠️ **`--apply` stops at `shortlist_pool`**: the layer-2 run must exist before
  `final_features` can use it, and that run is manual. `selection_2` reports
  `MANUAL — cannot be produced here`. `final_features/CONTEXT.md` §8.

**First build, 2026-08-13**: `pool__shortlist__close_adjust_5day__d20_h5`, **4,266 × 892**
(889 channels + 3 keys), all 20 source pools on one calendar so the INNER join loses 0
rows, `evidence=no_null=20`.

**And the first layer-2 run through it, same day — 58.5 min on the GPU, `--null-draws 0`:**

| | layer 1 (20 runs, unioned) | layer 2 (1 run, competing) |
|---|---|---|
| shortlisted | **889** | **59** |
| `ic_mean` selected | +0.2505 … −0.2618 per run | **+0.0324** |
| `ic_trend_per_fold` | negative in 15 of 20 | **−0.1594** |
| evidence | `no_null=20` | `no_null` |

⚠️ **The mechanism works — 889 collapse to 59 once they compete — and the result is still
nothing.** The +0.0324 is two folds out of five (**+0.489, +0.527, −0.677, −0.265,
+0.088**), `close_adjust` ranks 1st again (the price level "predicting" the price level at
ρ 0.996), `hit_rate` is 1.000 and R² −8.11 because the target is a price LEVEL, and no
null was paid for. The 59 spread over 17 countries with 2 price channels — what a
selection returns when there is nothing to find. **Fixing the union did not fix the
target**; the next run worth doing is this one on `return_5day`. `final_features/CONTEXT.md`
§8f.

⚠️ **The cost model in `feature_selection/CONTEXT.md` §15c was re-fitted on this run**
(§15c-refit): across 21 GPU runs, `minutes ≈ 0.364 × channels^0.77`, R² 0.87 — **not**
the CPU-era `k = 2.00`, which under-predicts narrow runs ~5× and over-predicts wide ones
~35%. A wide run with no null is ~1 h, not 3.

### 3d. ⚠️ THE SELECTION CAN RUN ON A KAGGLE T4 NOW — `src/kaggle_gpu/` (2026-08-15)

`kgpu` runs **an unmodified repo notebook** on Kaggle's free GPUs (30 GPU-h/week,
2×T4 **15 GiB** against this machine's 4 GiB) and merges the run folder back into
`reports/feature_selection/`, where `outstanding`, `final_features` and `pipeline`
already look for it. `RUN__feature_importance_report.ipynb` is not edited — the
copy in `.build/` is.

```powershell
cd src\kaggle_gpu
python -m kgpu plan     feature-selection   # what would run; touches nothing
python -m kgpu data     feature-selection   # DB -> parquet -> private dataset
python -m kgpu rehearse feature-selection   # the WORKER side, locally, no quota
python -m kgpu run      feature-selection   # push, wait, download, merge
```

⚠️ **The worker cannot reach `database_main_v2`**, so the pools travel as parquet
in a private dataset and `UnifiedSchemaReader` is swapped for a **subclass** whose
`read`/`column_types`/`tables`/`overview` come from that payload. **`join()` is
inherited, not re-implemented** — the one-to-one validation and the `join_log` in
the report are the same code that runs here.

⚠️ **Parameters are rewritten IN PLACE in the notebook's own parameter cell, never
appended after it.** That cell ends with `EXCLUDE = IDENTITY + [c for c in
ALL_TARGETS if c != TARGET]`; an override cell placed after it leaves `EXCLUDE`
excluding the OLD target, which hands the run's own label to `FeatureSelector` as
a candidate feature. It does not raise — it reports an IC near 1.

⚠️ **Two things measured on the first real round trip (2026-08-15), both of which
had passed every local check:** Kaggle **unpacks `source.zip` into `source/` and
deletes the archive**, so the uploaded payload and the mounted payload are
different shapes; and `dataset_status` returns **`ready` immediately** on a new
version of an existing dataset, so a kernel pushed on that answer mounts the
PREVIOUS version and completes normally on stale data. `wait_ready` now requires
the version NUMBER to move. `rehearse` runs both payload shapes.

⚠️ **A T4 run does not reproduce an RTX 3050 run, and the LIBRARY STACK differs
too.** XGBoost subsamples from a different RNG stream per device
(`feature_selection/CONTEXT.md` §16), and Kaggle's image is **xgboost 3.2.0 /
sklearn 1.6.1 / numpy 2.0.2** against `mt_env`'s **2.1.1 / 1.7.2 / 2.2.6**
(measured 2026-08-15 from the two `environment` blocks) — a major version of the
ranker itself. A Kaggle run is a different **procedure**, not the same one on
faster hardware. The commit is pinned into the report from the export, because the
worker has no repo and `report._git_commit()` would otherwise write `null`.

**First run through it, 2026-08-15** — `pool__basic + pool__targets`, `return_5day`,
`d=20 h=5`, `device=cuda`, **20 null draws, 3.7 min end to end** (the same pool with
no null was ~390 s locally). 7 of 15 channels kept; `ic_mean` **+0.0494** against a
p95 bar of **+0.0510** — **does not clear**, z = +1.60, p = 0.1429, null max
**+0.0714** (above the observed, so rule 3 applies). §2 stands: the affordable null
is now the point, not the result.

**Measured round trip, `smoke` job, 2026-08-17** — 8m 15s end to end, of which
**5.2 min was QUEUED**. ⚠️ **The queue is the floor, not the compute.** A job that
runs in 90 s still costs ~7 min wall clock, so batching one large run beats
several small ones, and `rehearse` (the worker side, locally, no quota) is where
iteration belongs.

### ⚠️ 3d-bis. PANEL MODE — SHIPPED 2026-08-17, and it is `CSP-1` in a SECOND form

**A cross-sectional target cannot run on Kaggle through the pool payload at all**, and
the reason is structural rather than a missing parameter. `feature_selection.
cross_sectional.read_universe_panel` builds `pool__basic ⋈ pool__targets` with **one
hand-written SQL statement** and derives `cs_rank_{h}day` from it, so it reaches for
`reader.driver._cursor_ctx()` — and `ParquetSchemaReader.driver` raises *"there is no
database on a Kaggle worker"*. The cross-sectional read **bypasses every abstraction the
parquet payload replaces**. No `--pools` value, no notebook parameter and no config key
routes around it.

**So the join moves to where the database is.** `DataConfig.panel` (`kgpu/config.py`) and
`export._export_panel` rank the universe by median `value_matched`, call
`read_universe_panel` **here**, and ship one finished `panel.parquet` — `cs_rank` already
derived. `resolved_tables()` returns `["panel"]` in this mode, because naming pools would
promise the worker a shape it never sees.

| measured 2026-08-17 | |
|---|---|
| payload, top-300 × h=20 | **1,247,098 × 104**, 4,388 dates, 300 tickers, `cs_rank_20day` ✅ |
| parquet | **477 MB** (88 s to read, 5 s to write) |
| in memory | **1.57 GB** |
| this machine | **4.0 GiB VRAM ❌**, 7.1 GB RAM free ❌ — the local pilot **CUDA-OOMed** asking for 1.01 GiB |
| Kaggle T4 | RAM ~29 GB ✅ — but **14.6 GiB VRAM is ❌ TOO, measured 2026-08-17**: OOM asking for 4.98 GiB with 10.70 GiB already in use (see below) |

⚠️ **`liquidity_before` is REQUIRED and has no default.** Ranking turnover over the whole
sample is look-ahead — it picks the names that *turned out* to be liquid, the same defect
§2c records for non-point-in-time index membership. A silent default would make that
invisible in the artefact, so the exporter raises instead.

⚠️ **The shipped `cs_rank` is a rank within the 300 shipped names, not within all 781.**
That is the intended experiment (a tradeable liquid cross-section) and it is written into
the manifest so a later reader cannot mistake it for the full-universe rank.

**The worker side is `RUN__cross_sectional_panel.ipynb` + the `cross-sectional` job, and
it does NOT re-implement a selection.** It loads `panel.parquet` and hands it to
`feature_selection.run.run_selection` through a new `provided_panel` argument
(`run.ProvidedPanel`), which replaces **the read and nothing else** — the selector, the
by-date purged CV, the panel-aware `date_block` null, `write_report` and the
`outstanding.csv` handoff are the same code `python -m feature_selection.run` runs here.
That is what makes a T4 panel run comparable with a local one at all. ⚠️ It is refused for
a non-`cs_` target: the single-series path checks `pool__targets` against
`information_schema` for labels `ALL_TARGETS` does not name, and a provided panel cannot
run that check.

⚠️ **`ProvidedPanel` CARRIES PROVENANCE, NOT JUST A FRAME**, because a worker cannot
re-derive any of it: the schema, the `database`, the **channel→pool map** and the
**universe**. The map is the one that bites — without it `outstanding` cannot fill
`source_table`, `contract.validate_shortlist` refuses the shortlist, and the run comes
home **invisible to `final_features`** (the measured 2026-08-15 defect). It is captured at
export while a cursor is open and travels in `manifest.json`.

| measured 2026-08-17, before any quota was spent | |
|---|---|
| `kgpu export cross-sectional` | **2m 04s** → 1,247,098 × 104, **477.4 MB**, 300 tickers, 4,388 dates |
| `kgpu rehearse cross-sectional` | **16.0 s**, both mount layouts, `n_eff = 218` |
| the notebook's OWN cells, end to end | ✅ on a 30-name / 48,521-row cut, through the real bootstrap: 2m 11s, 60 kept, shortlist 22, **`source_table from metadata`** |

⚠️ **THE REHEARSAL DRIVES CELL 0 AND THEN RE-CREATES THE PANEL PATH ITSELF** — it never
runs the notebook's own load or run cells. That gap is why the last row above exists: the
built notebook was executed against a cut-down payload with `KGPU_INPUT_DIR` /
`KGPU_WORK_DIR` set, which is the same seam `rehearse` uses, so discovery, the source
unpack, the stubs and the reader swap were the shipping code. **A cut-down panel is a
smoke test and never a measurement** — its `cs_rank` is still the rank over the 300
exported names.

⚠️ **AND THE FIRST REHEARSAL OF THE EXISTING JOB FAILED IN 3.6 s — `KGP-1`.** The payload
never shipped `src/utils`, because `kgpu_bootstrap` **stubs** `utils` for
`utils.constants.DATABASE_MAIN_V2` alone; `feature_selection/report.py` gained
`from utils import runtime` on 2026-08-15, **after this integration's only green round
trip**, and the stub's `__path__ = []` made that raise. So the `feature-selection` job had
been broken on the worker for two days with nothing saying so. Fixed both ways: `src/utils`
is in `source_dirs`, and a stub is now installed **only when the real module is not
importable** — a stub that shadows a shipped package fails at the import that needs it,
not at the one that is absent.

### ⚠️ AND THE FIRST REAL T4 RUN OOMed — "it fits on a T4" was never measured either

Pushed 2026-08-17, `RUN_NULL=false`. **Panel mode itself worked end to end on the
worker**: payload mounted, 34 source files unpacked, reader swapped, `panel.parquet`
loaded (1,247,098 × 104, 1.57 GB, 300 tickers, `n_eff = 218`), `run_selection` dispatched
on the panel path, `prepare + coverage` 6.4 s, **`window design` 189.3 s**. Then, at
**3m 28s**, inside `gpu.spearman_vector → _average_ranks_torch → torch.sort`:

> `CUDA out of memory. Tried to allocate 4.98 GiB. GPU 0 has 14.56 GiB of which 3.86 GiB
> is free; this process has 10.70 GiB in use.`

⚠️ **THE STEP NEEDS ~4× THE DESIGN IN VRAM, NOT 1×.** The design is ~536 float64 columns
over 1.247 M rows ≈ **4.98 GiB**, and that function holds `values` + `filled` + the mask
(10.58 GiB) before `torch.sort` asks for its own output and an int64 `order` — another
~10 GiB. 14.6 GiB was never going to be enough, and **the "~12.8 GB, fits" line in TODO
P2-1 v2 was an estimate of the DESIGN, not of this step**. Two claims corrected by one
run: it is `MEM-1` on the device side again, one card larger.

⚠️ **This is not a panel-mode defect and must not be read as one.** Everything `kgpu`
adds ran; what failed is a ranker step whose memory is quadratic in nothing and linear in
`rows × columns`, at a width no single-ticker run has ever reached. `float32` would halve
it and is **forbidden** for a number that will be quoted (P0-3). The fix is to rank in
COLUMN BLOCKS — ranks are per-column independent, so a chunked loop is exact, not an
approximation. TODO **P2-1 v2**.

---

## 4. Run it

```powershell
# once per shell
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"     # ⚠️ MUST be absolute, MUST be set
Clear-Content logs\app.log                              # before any pipeline/ingest run

# --- data pipeline ---
dagster dev                                             # UI at localhost:3000
dagster asset materialize -f src/orchestration/definitions.py --select "group:bronze"
dagster asset materialize -f src/orchestration/definitions.py --select "+bronze/trading_view_economy"
dagster asset materialize -f src/orchestration/definitions.py --select "group:unified" --partition VCB
dagster definitions validate                            # sanity check: 83 assets, no run

# --- model chain ---
python -m pipeline                                      # what's stale; writes nothing
python -m pipeline --apply                              # run the stale stages
python -m pipeline --ticker bank --table rank_5day__final__d20_h5 \
                   --config bank__rank_5day__final__d20_h5.yaml
python -m result_evaluator                              # the leaderboard
python -m result_evaluator --rescore                    # recompute every metric, no GPU

# --- the country feature-selection sweep (Dagster, 19 partitions) ---
# ⚠️ THE POOLS FIRST — the economy pools lag pool__basic and the join is INNER.
dagster asset materialize -f src/orchestration/definitions.py `
  --select "unified/pool__economy" --partition VCB
dagster asset materialize -f src/orchestration/definitions.py `
  --select "analysis/feature_selection_economy" --partition vietnam
```

⚠️ **`usa` raises at the default budget and is meant to.** ~1,458 channels is ~3.1 h
with no null and ~2.7 days at 20 draws; override `budget_minutes` for that ONE
partition rather than lowering the default. `feature_selection/CONTEXT.md` §15c.

⚠️ **THE COST MODEL HAS NO TERM FOR THE TARGET AND IT IS WORTH 13.7×** (2026-08-14).
`minutes ≈ 0.364 × channels^0.77` was fitted on 21 runs, **20 of them on the price
LEVEL `close_adjust_5day`**. The same 357-channel `pool__forex` panel took **2,016 s**
on that target and **146 s** on `return_5day`, because `lasso` — the dominant cost —
zeroes every coefficient on a return and converges at once. **A 20-draw null on a wide
pool is affordable for a return target** (357 channels + 20 draws = 41 min, measured),
where the fitted model implies ~12 h. `evidence=no_null` on a return run is now a
choice, not a budget. `feature_selection/CONTEXT.md` §15c-target.

⚠️ **AND THE 13.7× WAS `lasso`, WHICH IS OUT OF THE DEFAULT ENSEMBLE SINCE 2026-08-16.**
The six rankers were chosen in 2026-08-03 for what each one SEES and **had never been
measured against each other**. They now have been (`feature_selection/CONTEXT.md` §19 —
two targets × two widths, each method's own top-k scored out of sample against a 40-draw
random-k control), and **`METHODS` is now `spearman, xgb_shap, permutation`**:

| dropped from the default | measured |
|---|---|
| **`lasso`** | **87.2 % of the average archived run's wall clock** (90-96 % of every country run) — and on BOTH measured targets it zeroed every coefficient, so its column is a CONSTANT and the ensemble was bit-identical without it. Removed on cost and inertness; its standalone score is withdrawn, not low (§19b) |
| **`mutual_info`** | the **worst standalone ranker measured** (42.5th percentile, min 7.5th — below chance), and the dearest once lasso is gone (46 % of ranking time) |
| **`xgb_gain`** | ρ = **0.864** with `xgb_shap` **from the same fit** — one model held 2 of 6 votes; second worst standalone (46.2nd percentile, also below chance) |

⚠️ **`permutation` is load-bearing and the only member that is:** every other
leave-one-out subset scored at or ABOVE the full six (80.0); dropping this one put the
blend at **56.2** against chance's 50, in all four cells. ⚠️ **Nothing was deleted** —
`ALL_METHODS` still holds all six and `methods=ALL_METHODS` reproduces an older run; all
19 archived shortlists were verified to rebuild identically. ⚠️ **An mRMR member was
tested and REJECTED** — 100th percentile on one target, 50th on the other (§19f).

✅ **`MTH-1` RESOLVED the same day.** `methods` was recorded in `metadata.json` but not
in `contract.SETUP_KEYS`, so a pre- and a post-2026-08-16 run could be unioned into one
table with nothing saying so. It is in `SETUP_KEYS` now, and **the archive did not have
to be invalidated to get there** — which is what had blocked the fix, since an absent
SETUP_KEY raises. `contract.LEGACY_SETUP_DEFAULTS` gives a run that recorded no ensemble
the reading **`"unrecorded"`**, so the 19 archived runs still validate and still plan
while grouping apart from both the three-ranker default and a deliberate
`methods=ALL_METHODS` reproduction. ⚠️ **`"unrecorded"` is deliberately not read as "the
six"** even though those runs used six — §5 rule 2, an absent measurement is absent, not
inferred. ⚠️ **Grouping is order-insensitive** (`contract.canonical_methods` sorts): the
ensemble is a MEAN over its members, so order cannot change the answer and must not
split a table. ⚠️ **Expect this on the next run** — a new three-ranker `vcb__basic` run
and the 19 archived runs are two groups that still want ONE table name, so
`plan_from_reports` **raises on the collision rather than unioning**. Pass
`--scope basic`. Pinned by `feature_selection/tests/test_contract.py` (**23 tests**).

⚠️ **`SETUP_KEYS` GREW TWICE MORE, 2026-08-17 — `design_dtype` and `env_fingerprint`.**
Both exist because a run's number now depends on something `methods` does not capture.
**`design_dtype`**: `float32` does *not* reproduce `float64` — measured at a **52%
relative change in `ic_mean`** on one panel (TODO P0-3), so the two dtypes must never
union. **`env_fingerprint`** (`report.FINGERPRINTED_LIBRARIES` — numpy/scipy/sklearn/
xgboost/torch, hashed to 12 chars; this machine is `cf51e65b3a15`): §3d already records
that Kaggle ships **xgboost 3.2.0 / sklearn 1.6.1** against `mt_env`'s **2.1.1 / 1.7.2** —
a major version of the ranker itself — so a Kaggle run and a local run were two procedures
that could silently land in ONE table. `LEGACY_SETUP_DEFAULTS` reads both as
`"unrecorded"` for the archive, on the same rule-2 grounds as `methods`.

⚠️ **THE SELECTION RUNS ON THE GPU NOW, AND `device` IS PART OF THE SETUP** (2026-08-10).
Every one of the 22 archived runs recorded `device="cpu"` — the GPU had never been used —
and simply forcing `cuda` made a run **6.8× SLOWER**, because sklearn's
`permutation_importance` copied a whole DataFrame per draw and the feature↔target
Spearman built a 584 MB p×p matrix to keep one column. Both were rewritten and `lasso`
was reimplemented (FISTA, same objective, same purged folds, identical selected alpha);
the same run is now **1,046 s → 66.9 s** and the default is `device="auto"`. ⚠️ **A GPU
re-run does NOT reproduce an archived one** — XGBoost subsamples from a different RNG
stream per device, so the kept set moves (58 of 62 in common). `feature_selection/CONTEXT.md`
§16, and §16d for the one step that was converted, verified exact, measured 4-8× SLOWER
and deliberately left on the host.

⚠️ **NEVER `Materialize all` / `*` / a bare backfill.** With every partition live that
takes `raw/cafef_pdfs` (100 tickers × ~1-1.7 GB), `raw/trading_view@stocks` (777 tickers,
~10 h) and `raw/cafef_financials` (~2.4 h/ticker). Materialise a group or a named asset.

---

## 5. ⚠️ Standing rules — the cross-cutting ones, learned expensively

**Evidence**

1. **Every selection needs its own null, re-run whenever the pool or representation
   changes.** `zscore` moved its own bar 43% with no change to the data. A bar computed
   for one configuration says nothing about another.
2. **An absent null is recorded as absent, never implied to be a pass.** `"null": null`
   means no bar was computed. `evidence=no_null` is an *unknown*; `failed_null` is a
   *measurement*.
3. **`clears_bar` is the wrong summary whenever the null MAX exceeds the observed** —
   quote the max beside it. `pool__ta` cleared its p95 and one of 20 shuffled draws still
   beat the real data.
4. **Every single-score holdout needs a shuffled-label control.** With one score there is
   no fold spread, so the control IS the error bar — and it has reached +0.169 against
   real results of at most +0.071.
5. **Report the IC trend beside the mean.** A mean built from folds decaying to negative
   is not a signal.

⚠️ **HOW MANY DRAWS: 10 to FAIL something, 20 to PASS it** (decided 2026-08-18, on the
measurements below — not on taste). `p` is **not** the criterion: until a draw beats the
observed it is pinned at the `1/(n+1)` floor either way, 0.0909 at 10 draws and 0.0476 at
20, which says "no draw beat it" and never "p is small". **`z` is the statistic, and its
denominator is the null's `sd`.**

| measured | 10 draws | 20 draws |
|---|---|---|
| the p95 BAR | stable — P0-1's two independent 10-draw seeds gave **+0.0573 and +0.0565** | — |
| **`SE(sd)`** — the denominator of `z` | **0.0083** | **0.0051** |

So a 10-draw bar is already trustworthy; what 20 buys is a `z` you can defend. The rule
follows from the asymmetry: **when the observed lands below or near the null's mean, 10
draws settle it** — the 2026-08-17 six-pool sweep failed all six on 10 and needed nothing
more. **When it lands far above, the whole claim is *how far*, and that is `z`.** Rule 3
points the same way: quoting the null MAX beside the bar is a statement about the tail,
and 10 draws sample the tail half as well.

**Leakage & sample size**

6. **THE PURGE GAP IS `d + h − 1`, NOT `h`.** At `d=20, h=5` that is 24 rows. Purging only
   `h` leaves 19 rows of the test sample's own input window in training — the easiest way
   to make a windowed model look predictive. `feature_selection.PurgedWalkForward.gap`
   computes it and `train_test_creator` applies the same one at each split boundary.
7. **`n_eff` is `n/h` for a series and `n_dates/h` for a PANEL.** 635 test samples ≈ 127
   independent observations; 13,028 panel rows over 653 dates ≈ 130.6, not 2,606. Both
   figures are still optimistic — they price in label overlap, not input overlap.
8. **Imputation is the TRAIN-slice median, never `ffill().bfill()`.** `bfill` fills a
   leading gap with the first *future* observation. And the "train slice" is the rows
   carried by a train SAMPLE — the cut *minus* the purge gap, not `date < val_start`.
9. **Never standardise a 0/1 label.** A classification dataset must be built with
   `scale_target=False`; `_verify` raises otherwise.

**The pipeline lies in specific ways**

10. **A green asset is NOT evidence of fresh data.** `landed()` answers "is this folder
    empty?", not "did THIS run produce anything". `skip_existing=True` means a scrape can
    go green in 500 ms having fetched nothing — or worse, having fetched *some* things
    (one forex run refreshed 29 series and left 328 stale). **The per-series max date is
    the only honest freshness check.**
11. **A scrape and its ingests are separate assets, so "re-scraped" never implies
    "re-ingested".** Bronze once sat a full day behind a completed scrape — 5 countries
    against 19 on disk — with nothing raising. The check is one query:
    `COUNT(DISTINCT ticker)` in bronze vs the file count in `raw_data/`.
12. **Absent = OFF in `config.json`, and every asset must be listed** — the loader
    **raises** on an unlisted asset, so silence is never how something gets disabled. A
    group gate (`{"enabled": false, …}`) switches off a layer while modules stay visible.
13. **Selection IS the run plan.** `--select` is the whole mechanism; there is no switch
    file left to veto with — see §5a for what was retired and what replaced it.
14. **Disabling an asset does NOT disable downstream** — Dagster keeps the edge and the
    downstream still reads the folder from disk. To stop a chain, disable the downstream.

**Data types & I/O**

15. **`CREATE TABLE AS`, never a pandas round-trip.** psycopg2 returns `numeric` as
    `Decimal` → DataFrame dtype `object` → writer maps `object` to VARCHAR. A read-then-write
    silently turns every price column into TEXT. This trap is documented in three places
    because it has nearly fired in three directions.
16. **`driver.select` used to swallow exceptions and return an empty DataFrame.** It raises
    now, but prefer `UnifiedSchemaReader.read`, which also raises on empty.
17. **SQL `LIKE '_'` is a single-char wildcard** — `'pool__%'` also matches `poolXX`.
18. **Windows/cp1252**: open `metadata.json` and friends with `encoding="utf-8"` (the
    provenance comments carry `⚠️`); PowerShell 5.1's `Out-File -Encoding utf8` writes a
    BOM; and **never put `⚠️` in matplotlib chart text** — Segoe UI has no glyph and it
    renders as a box.
19. **Ephemeral scripts must load `.env` by explicit path** — they live in the scratchpad
    outside the repo, so `find_dotenv()` misses it and `PostgreSQLConnectionDto` raises
    "Password cannot be empty". Use `load_dotenv(os.path.abspath(".env"), override=True)`
    with cwd = repo root.
20. **Line-buffer anything long and write each unit to disk as it finishes.** A 4-hour null
    run was lost entirely to a `TextIOWrapper` that re-buffered on top of `python -u`.

**A finished run is not a run that checked itself** (added 2026-08-14, all three
measured on one `pool__forex` selection — `feature_selection/CONTEXT.md` §17)

21. **A metric that CANNOT FAIL is not a pass — withdraw it.** `hit_rate` is
    `sign(pred) == sign(y)`, and on a price-LEVEL target (`close_adjust_{h}day`) every
    label is positive, so it is **1.0 by construction**. One run reported `+1.0000`
    beside `ic_mean −0.1638`. It is `NaN` → `—` now. The same target makes R² −24.9,
    which is a *measurement* of a bad target and stays.
22. **Coverage is a scalar and a scalar cannot see a FROZEN SOURCE.** A late starter and
    a channel dead since June both score 0.67. Read `trailing_null_sessions` beside it —
    328 of 357 forex channels had carried no value for 40 sessions. This is §5 rule 10's
    per-series max date, one level down, at the feature.
23. **An all-NaN train slice is imputed to the constant `0.0` and then RANKED.** There is
    no median to take, so `_impute` invents one in a unit the channel never had. **197 of
    357 channels in fold 1**, and **44 of the 66 SELECTED**. `validation.csv` carries
    `n_dead_train`/`n_dead_test` per fold now. ⚠️ A rising `ic_trend_per_fold` on a ragged
    pool measures **data arrival**, not a strengthening signal.

### 5a. ⚠️ RETIRED — do not go looking for these, and do not follow old advice about them

Phase 5 (2026-08-05/06) removed the second way to run things. **All four are gone from
disk**, verified 2026-08-10:

| gone | was | replaced by |
|---|---|---|
| `src/main.py` | the run plan — 8 `scraper.scrape()` calls + 3 ingest entry points | `--select` |
| `src/switch_config.json` | 676 flag keys gating every stage | `config.json`'s `parameters` (295 leaves) for what a scrape *enumerates*; `--select` for what *runs*. **A leftover copy now RAISES** |
| `src/data_preprocessor/` | the ETL library | **moved** to `src/orchestration/preprocessor/` — same code, now inside its only caller |
| `src/data_postprocessor/` | 652 lines joining macro + market columns | `gold.economy`, `gold.stock_market`, the unified schema |
| `DataPreprocessor.ingest_{bronze,silver,gold}_data`, `_run_layer` | hard-coded leaf lists that **deliberately did not raise** | one asset per `_ingest_*`, and a failed asset fails the run |

⚠️ **`SwitchHandler` the CLASS still exists and is still used** — but with an explicit
`switches` dict handed in, and **no default path**. Seeing it in the code is not evidence
that a config file drives anything.

⚠️ **Two CONTEXT.md files gave deleted-file instructions until 2026-08-10** — both §5s,
in `web_scraper` and `orchestration/preprocessor`, opened with *"edit
`src/switch_config.json`, then `python src\main.py`"*. Both are now rewritten with the
old text quoted as history. If you find a third, fix it the same way rather than deleting
it: the old mechanism explains the shape of what replaced it.

### 5b. Folders removed 2026-08-10 — ~3.1 GB

| removed | was | recoverable? |
|---|---|---|
| `charts/`, `pdfs/` | 2025-era chart PNGs and report PDFs — outputs of the viz notebooks below | ✅ tracked, `git checkout` |
| `src/visualization/`, `src/visualizer/`, `src/test/` | 4 viz notebooks + their helper + `compare.ipynb`. **Verified dead**: `visualizer` was imported only by those notebooks; they were imported by nothing | ✅ tracked |
| `ocr_env8/`, `ocr_env9/` | the two OCR venvs for experiments 8/9 (1.9 GB) | ❌ rebuild — recipe in each experiment's README. Production parsing is unaffected (`CAFEF_OCR_ENGINE=onnx` in `mt_env`) |
| `raw_data/_archive/` | the pre-2026-08-05 TradingView CSVs (803 MB) | ❌ **re-scrape only** — see `orchestration/CONTEXT.md`. Budget ~2 h for forex |
| `src/logs/`, 38 × `__pycache__` | stale duplicate log dir; bytecode | n/a — regenerable |

⚠️ **`src/utils/constants.py` still defines `{SILVER,GOLD,ARIMA}_VISUALIZATION_LOG_FILE_BASE`.**
Now dead — the notebooks that used them are gone. Harmless, unreferenced, left alone
because this was a folder cleanup and not a code change.

---

### 5c. The `pool__basic` prototype chain — run end to end 2026-08-10

Six stages, network to scored metric, one ticker, one pool. `pool__basic` re-scraped
(`skip_existing=False`, VCB only, 3m24s) and rebuilt to **4,266 rows / 2026-08-07**, from
4,235 / 2026-06-25.

**ELEVEN models, 0 to 4,961 parameters, identical dataset / splits / purge / null:**

| model | params | test IC | test bar | p | R² | RMSE |
|---|---|---|---|---|---|---|
| `BASELINE_ZERO` | **0** | — (constant) | — | — | −0.001 | **0.03721** ← the floor |
| `BASELINE_AR` | 6 | +0.0557 | +0.0585 ❌ | 0.070 | −0.002 | 0.03723 |
| `BASELINE_RIDGE_STATS` | 25 | +0.1005 | +0.1247 ❌ | 0.095 | −5.19 | 0.09252 |
| `BASELINE_RIDGE_FLAT` | 81 | −0.0397 | +0.0785 ❌ | 0.577 | −3.26 | 0.07677 |
| `MLP` | 257 | **−0.1001** | +0.0908 ❌ | 0.910 | −5.87 | 0.09751 |
| `LSTM` (h=8) | 473 | **+0.0346** | +0.0956 ❌ | 0.249 | −0.111 | 0.03920 |
| `GRU` | 1,105 | −0.0766 | +0.0786 ❌ | 0.726 | −0.031 | 0.03776 |
| **`GBT`** | 1,319 | **+0.1263** | +0.1121 ⚠️ **✅** | **0.035** | −2.11 | 0.06562 |
| `CNN` | 3,745 | −0.0332 | +0.1107 ❌ | 0.657 | −0.008 | 0.03734 |
| `LSTM` (h=32) | 4,961 | −0.0345 | +0.1348 ❌ | 0.726 | −0.059 | 0.03826 |
| *wide, 724 ch — LSTM* | ~276k | −0.0721 | +0.118 ❌ | 0.88 | −0.90 | — |

⚠️ **THE WHOLE SPREAD IS ONE ERROR BAR.** IC ranges −0.100 … +0.126 over 9 scored
models — a span of 0.227 against `SE(IC) = 0.197` at `n_eff = 26.7` (window overlap) and
0.089 at `n_eff = 128` (label overlap). The largest |t| on the board is **+1.42**.
Ranking these architectures is reading noise.

⚠️ **Two runs clear a bar; expectation was 1.1.** `GBT` on `ic` (p = 0.035) and
`BASELINE_AR` on `dir_auc` (p = 0.040), from **11 runs × 2 nulled metrics = 22 tests**.
The null prices in no architecture search (**NUL-1**) and the sweep *is* one.

⚠️ **NOT ONE OF ELEVEN BEATS THE ZERO PREDICTOR ON RMSE**, and the models that rank
best are the ones whose magnitudes are most wrong — `GBT` R² = −2.11, `ridge_stats`
−5.19. Ranking and calibration are separate questions here.

⚠️ **Capacity is real but not the whole story.** The LSTM *flips sign* on capacity alone
(h=32 → −0.0345, h=8 → **+0.0346**), yet the 257-parameter MLP is the worst on the board
on the same design a 25-parameter ridge does best on. `model/CONTEXT.md` §14–§15.

⚠️ **Nothing here changes §2 and everything strengthens it.** Six model families
spanning 0 to 276k parameters all land inside their own nulls.

### 5d. The BANK panel, re-scraped and re-run 2026-08-10

All 20 banks refreshed to 2026-08-07 (`pool__basic` 53,921 → **54,528 rows**), so the
panel has one uniform as-of date. Dataset **27,348 / 12,629 / 13,135 × 20 × 10**, scored
at **panel grain** — `n_eff = n_dates/h` = **131.6**, which is FEWER independent
observations than VCB's 128-per-640-rows, not more.

**Selection: `z = −1.71`, observed −0.0106 BELOW its null's mean of +0.0052.** Decisive
fail, and §13d's reading stands — a sector co-moves, so there is less to rank.

| baseline | params | daily-IC t | evaluator says |
|---|---|---|---|
| `BASELINE_AR` | 6 | **+0.23** | ⚠️ "clears" `ic` at p=0.005, **against a NEGATIVE bar** |
| `BASELINE_RIDGE_STATS` | 61 | **+1.15** | ❌ fails (p=0.254) — the highest t on the panel |
| `BASELINE_RIDGE_FLAT` | 201 | +0.77 | ❌ fails |

⚠️ **NEW ISSUE NUL-3: the evaluator's panel null is not label-neutral.** Its centre moved
with the MODEL (−0.0171 / +0.0076 / +0.0109) and it got both ends wrong — manufacturing a
clear for the weakest model and failing the strongest. **On a panel, quote the daily-IC
t-stat, not `ic_clears`.** `model/CONTEXT.md` §16.

⚠️ **The selection cleared its bar; the model did not clear its own.** `z = +2.15` bought
nothing downstream — which is the two bars working, and the most useful thing the run
measured. `feature_selection/CONTEXT.md` §10d has why `z = +2.15` on 20 draws is weak in
its own right: `p = 0.0476` is the `1/(n+1)` floor, the null is fat-tailed, and this is
the **third** measurement of this pool (§6b `z = +1.56` ❌, §10b `z = +1.46` ❌) under a
third procedure.

⚠️ **The narrow chain is LESS BAD, and it is the STL-1 argument from the other side.**
R² −0.90 → −0.059 on the same ticker, target and splits, at 4,961 parameters instead of
~276k. **Neither shows skill**; "less negative" is not a result.

⚠️ **Re-materialising two pools left 21 siblings on the OLD calendar.** Harmless for a
`pool__basic` build; a rebuild of the 750-channel table would INNER-join back down to
2026-06-25 **and look unchanged**. `status_data` reports it as `pools_behind`.

## 6. State today (2026-08-23)

⚠️ **If a number here disagrees with the database, the database is right and this section
is the bug.** It was 7 days stale once already.

### ⚠️ 6-0. THE HEADLINE, and what a new session should read first

**The cross-sectional chain works out of sample, over TEN expanding folds, after costs.**
§2's verdict is about SINGLE-STOCK SHORT-HORIZON prediction and is untouched; this is the
other thing, at the grain and horizon §2b and §2a-bis pointed to.

⚠️ **QUOTE THE WALK-FORWARD, NOT THE SINGLE SPLIT.** Until 2026-08-19 this section
headlined one train/val/test split whose test window happened to be a +20.2 %/yr VNINDEX
bull market — Sharpe +1.484 over **32 periods**, `se` 0.256. That number is still on disk
and still useful (see the leak check below), but it is not the evidence any more.

| | the walk-forward (PRF-1) | the single split |
|---|---|---|
| folds | **10**, test = calendar 2017…2026 | 1 |
| periods | **118** non-overlapping | 32 |
| `se_sharpe` | **0.155** | 0.256 |
| Sharpe @20/30/50 bps | **+2.026 / +1.991 / +1.921** | +1.551 @50 |
| CAGR @30 bps | **+47.5 %** vs market +14.6 % | +32.3 % |
| daily IC | **+0.1097**, `ic_t` +6.90, 80.6 % of days positive | +0.0863, t +3.47 |
| null, 200 within-date shuffles | **z = +12.18 / +12.28 / +12.46**, null MAX below observed at all three | z = +4.29 |
| shape | IC positive **9 of 10 folds**; beats the equal-weight universe **10 of 10** | — |

⚠️ **AND THE SAME SWEEP AT h=10 SCORES HIGHER ON EVERY ROW — 2026-08-20, §6-0-bis-3.**
Sharpe@30 **+2.531** over **236** periods, IC **+0.1412**, `ic_t` **+16.05**, z = **+18.58**,
IC positive **10 of 10** folds.

✅ **AND THE TWO WERE PAIRED THE SAME DAY (`P2-4`), WHICH SPLIT THE ANSWER IN HALF.**
`walkforward.pair` pairs them on the CALENDAR — both hold a book on all 2,360 shared
sessions, ρ = **0.723** — because they cannot be paired period by period (236 vs 118 periods
over different holding intervals). At 30 bps:

| estimand | h=10 − h=20 | Newey-West | block bootstrap 95 % CI | verdict |
|---|---|---|---|---|
| **mean return/yr** | **+17.0 pp** | t = **+3.53**, p = 0.0004 | **[+8.6, +25.7]** | ✅ significant |
| **Sharpe** | +0.44 | — | **[−0.079, +1.041]** | ❌ **not established** |

⚠️ **h=10 IS A HIGHER-RETURN, HIGHER-VOLATILITY TRACK, and the +0.54 that looked clean is
not resolvable at 2,360 sessions.** Zero sits inside the Sharpe CI at 20, 30 and 50 bps, and
p rises 0.067 → 0.141 as costs do — `backtest` §3's identity showing through, since h=10 pays
double the fee drag. ⚠️ **A non-significant ΔSharpe is not evidence of equality**: the CI
reaches +1.04, so this is underpowered, not settled. **The chain stays at h=20** — not
because h=10 lost, but because it has not won the test that matters.
`walkforward/CONTEXT.md` §10.

**The chain that produced it**, and every stage is reproducible from `RUNBOOK.md` §3a:

| stage | artefact | number |
|---|---|---|
| 2 · selection | `2026-08-18_072323__all__basic__cs_rank_20day` | `ic_mean` **+0.1075**, 20-draw bar +0.0388, null max +0.0410 (below observed), **z = +9.09** — §2b-bis |
| 5 · final_features | `unified_schema_all.rank_20day__final__d20_h20` | 624,448 × 17, **150 names**, 621,448 labelled |
| 6 · train_test_creator | `all__rank_20day__final__d20_h20__tr70_val15_test15__std` | 422,251 / 91,462 / 93,224 windows × 20 × 13 |
| 7 · model | `lstm__all__rank_20day__final__d20_h20__20260818-195738` | 4m 23s; test IC +0.0863, **t = +3.47** (`ICT-1` fixed) |
| 8 · result_evaluator | scored + indexed | clears its block-shuffled bar on `ic` and `dir_auc` — ⚠️ a floor, not a result (`NUL-1`) |
| 9 · backtest | `src/model/runs/<run_id>/results/backtest_test.csv` ⚠️ **inside the RUN FOLDER, which is gitignored (`RPR-1`)** — not repo-root `results/` | top-15 of 150, 50 bps, **ceiling-screened by default since 2026-08-19**: **+1.5512** test / **+1.7385** val |
| **W · walkforward** *(NEW 2026-08-19)* | `results/walkforward/` + 10 run folders | the table above. `walkforward/CONTEXT.md` |
| **W · walkforward @ h=10** *(NEW 2026-08-20)* | `results/walkforward_h10/` + 10 run folders | Sharpe@30 **+2.531**, 236 periods, z = +18.58 — §6-0-bis-3. ⚠️ `--out` is REQUIRED or it overwrites the row above |

### ⚠️ 6-0-a. FOUR THINGS CLOSED ON 2026-08-19, AND WHAT EACH ONE SETTLED

| | verdict |
|---|---|
| **PRF-1** — is it one lucky split? | **No.** 10 folds, 118 periods, z = +12.3. ⚠️ Sharpe@30 DOES decay: slope −0.100/fold, first five +2.775 → last five +1.564. But 2023/24/25 are **+2.64 / +0.90 / +1.39**, all above their market; **2022 is the only bad fold and it is bad for everyone** (the universe itself ran −0.94 that year) |
| **PRF-7** — were the 13 channels fitted to the test folds? | **Bounded, and MILD.** Re-running the identical selection on dates < 2017 keeps **51 of 61** channels (Jaccard 0.761, **5.8 sd** above chance), shortlists 8 of 13 against a chance of 2.17, picks the same top two. ⚠️ It bounds the bias, it does not remove it |
| **PRF-8** — would a different architecture do better? | **Not at h=20.** 205,441 params, **2,033 params** and a **1,400-node GBT** all tie on the identical folds — paired \|t\| < 1 at every cost level. §6-0-ter. ⚠️ **RE-QUALIFIED 2026-08-21 when `P1-9` shipped**: that `\|t\|` tests MEAN RETURN, not Sharpe, and h=20 has **not** been re-scored. At h=10 the Sharpe test says **one** arm loses (`cnn`, p = 0.001), not three, and `gbt` GAINS at a nominal p = 0.044 that does not survive six arms (§6-0-ter-2) |
| **PRF-0** — is it buying names nobody could sell? | **Marginally, and removing it HELPS.** The model picks ceiling names 1.33× as often as chance (against 2.14× for a 5-day screen); excluding them takes test +1.484 → **+1.551**. ✅ Now applied BY DEFAULT in `backtest.build_panel`, not by a probe |

### ⚠️ 6-0-b. ✅ AND IT BEATS "PREDICT NO CHANGE" — the first thing here that does

`P4-12` fixed 2026-08-19: block B (`mase`, `rmsse`, `skill_score`, `beats_naive`) was
computed in `metrics.evaluate` only, so **every cross-sectional run carried `test_mase =
NaN`** — an absence, never a pass (§5 rule 2). `evaluate_panel` now calls a panel-aware
version and the column is filled:

| run | grain | `naive_kind` | `mase` | beats naive? |
|---|---|---|---|---|
| `lstm__vcb__close_adjust_5day` | series | `lag_h` | **21.36** | ❌ 21× worse than a random walk |
| `lstm__vcb__return_5day` | series | `zero` | **1.068** | ❌ — `P2-3`'s *"line to quote"* |
| **`lstm__all__rank_20day`** | panel | `zero` | **0.9937** | ✅ **the first in this repo** |
| the 30 PRF-8 fold runs | panel | `zero` | mean 0.988-0.991 | ✅ `lstm_small` and `gbt` in **10/10** folds, `lstm` in 9/10 |

⚠️ **AND THE MARGIN IS 0.6 %, WHICH IS THE POINT.** `mase = 0.9937` means the model's mean
absolute error is six parts in a thousand below "always predict the mean rank". That is
what an R² of **+0.0003** looks like from the other side. **It clears the line and the
size of the clearance confirms that the magnitudes carry nothing — the result is the
ORDER.** ⚠️ I wrote in TODO P4-12, before measuring, that it would *not* beat the naive on
magnitude. That was wrong, and the wrong prediction is left in the register.

⚠️ **RE-SCORING A PANEL RUN TAKES TWO COMMANDS.** `--rescore` rewrites each run FOLDER's
`results/metrics.{csv,json}`; `index.csv` is written **only** by `--rebuild-index`
(~8 min each over 33 runs). A run scored before 2026-08-19 and not put through **both**
carries no `mase` and, if scored before 2026-08-18, an `ic_t` overstated by `√h`
(`ICT-1`).

### ⚠️ 6-0-bis-2. PRF-2 — THE CHAIN AT h=10, AND IT ANSWERS THE REGIME QUESTION

Run 2026-08-19: the same chain, same universe, same architecture, **only the horizon
moved**. Selection cleared at **z = +13.78** (`ic_mean` +0.1201, bar +0.0355, null max
below observed, `n_eff`/fold **76.6** against h=20's 38.1). Test window 2023-11 → 2026-07,
63 periods, top-20 of 150, buyable only:

| h=10, one panel | CAGR@30 | Sharpe@30 | null z |
|---|---|---|---|
| **the model** | **+43.8 %** | **+2.442** (se 0.251) | **+8.99** ✅ |
| the 3-channel HAND rule | −5.1 % | **−0.263** | −1.72 ❌ |
| equal-weight universe | +4.0 % | +0.329 | — |

Paired (ρ 0.74): **ΔSharpe +2.71, `t` = +5.94**. Model run: IC +0.1393, `ic_t` +8.19,
85.8 % of days positive, `mase` **0.9874** ✅.

⚠️ **THE BREAK AFTER 2022 IS IN THE FEATURES, NOT IN THE MARKET.** `model/CONTEXT.md` §11
and `backtest` §8g both found the edge dying post-2022 and read it as a regime wall. Hold
the window, the universe and the horizon fixed and move only the FEATURE SET: 19 selected
channels return +2.44 where three hand-picked ones return −0.26. **The market did not stop
being predictable; those three columns stopped predicting it.** That is `PRF-3`'s
hypothesis (2), and it re-opens §2d's ladder as the answer rather than a longer training
window. ⚠️ The hand rule's −0.26 is not a contradiction of its own +0.652 — that figure is
over 2018-2026, and §8g itself measured +0.011 for 2022-2026.

⚠️ **h=10 BEATS h=20 WHILE PAYING DOUBLE THE FEES** (8.8 %/yr against 4.4 % at τ=0.70,
50 bps) — +2.442 against +1.441 on the same universe, architecture and window. ⚠️ **One
split each**, `se_sharpe` ~0.25 — ✅ **and the walk-forward below settled it 2026-08-20.**

### ⚠️ 6-0-bis-3. THE h=10 WALK-FORWARD — 2026-08-20, 10 folds, and it holds

The run §6-0-bis-2 called for. Same geometry as `PRF-1`: top 150 by pre-2014 turnover,
`--first-test 2017-01-01`, expanding 12-month folds `oos2017…oos2026`, top-20, ceiling
screen. **33m 26s** sweep + **8m 59s** scoring, local RTX 3050, no Kaggle quota.

| pooled, top-20, buyable only | **h=10 (NEW)** | h=20 (`PRF-1`) |
|---|---|---|
| periods | **236** | 118 |
| `se_sharpe` | **0.128** | 0.155 |
| Sharpe @20/30/50 bps | **+2.601 / +2.531 / +2.391** | +2.026 / +1.991 / +1.921 |
| CAGR@30 | **+74.0 %** vs market +13.9 % | +47.5 % vs +14.6 % |
| daily IC | **+0.1412**, `ic_t` **+16.05**, 86.5 % of days positive | +0.1097, +6.90, 80.6 % |
| null, 200 within-date shuffles | **z = +18.42 / +18.58 / +18.86**, null MAX below observed at all three | +12.18 / +12.28 / +12.46 |
| shape | IC positive **10 of 10** folds; beats the universe **10 of 10** on Sharpe AND CAGR | IC positive 9/10 |

⚠️ **THE DECAY IS THE SAME AT BOTH HORIZONS, AND THE SLOPE ALONE SAYS OTHERWISE.** h=10's
Sharpe@30 slope is **−0.219/fold** against h=20's −0.100 — 2.2× steeper — but the
proportional fall is **−45.8 %** against **−43.6 %** (first five folds vs last five). The
absolute slope is steeper only because the level is higher. **Both horizons lose ~45 % of
their Sharpe across the sweep**; that is a shared property, not an h=10 defect.

⚠️ **THE TWO HORIZONS ARE NOT PAIRED AND CANNOT BE.** `walkforward.compare` pairs ARMS
inside one sweep — same dates, same panel, ρ = 0.88, which is what makes a paired `t`
meaningful. Two horizons give **236 and 118 periods over different holding intervals**, so
no period-wise correspondence exists. The +0.54 gap is two independent estimates with `se`
~0.13-0.16 each: **suggestive, and not the paired test §6-0-ter insisted on**. §5c is why
that matters — eleven architectures once spread IC over 0.227 and the whole spread was one
error bar.

✅ **No mechanical leak.** Restricting the h=10 track to the single split's own test window
(`2023-11-28 → 2026-07-24`, read from the dataset's `metadata.json`) gives **63 periods on
both sides**, IC +0.1307 against `PRF-2`'s +0.1393, Sharpe@30 **+2.257 against +2.442** —
a gap of **0.8 SE**. ⚠️ Its SIGN is opposite to the h=20 check's and at 0.8 SE that is not
resolvable; both horizons agree with their single split within noise.

✅ **THE SELECTION LOOK-AHEAD IS BOUNDED AT h=10 TOO — measured 2026-08-20, 9m 46s on a
T4.** Re-running the identical selection on dates < 2017 keeps **51 of 61** channels
(Jaccard **0.750**, chance 39.3 ± 2.1 → **+5.48 sd**), shortlists **7** against a chance of
1.90 (**+4.37 sd**), and puts **`drv_order_vol_imb` at #1 in both** — the same channel h=20's
probe put first. **10 of the 12 shortlist misses are in the early KEPT set**; only
`drv_close_z_21` and `n_sell_orders` are absent outright, and `n_sell_orders` was one of
h=20's misses too. ⚠️ The early run shortlists **9 against 19** because `n_eff_per_fold` is
**28.9 against 76.6** — that is POWER, not the market. ⚠️ **It bounds the optimism, it does
not remove it**: these 19 channels were still chosen over 2009-2026 including every test
fold. `walkforward/CONTEXT.md` §9e.
⚠️ Survivorship protects `z = +18.6` and **not** +74.0 %/yr (§2c). `walkforward/CONTEXT.md` §9.

⚠️ **`--out` IS LOAD-BEARING AND OMITTING IT DESTROYS `PRF-1`.** `walkforward`'s
`DEFAULT_OUT` is `results/walkforward/`, which holds the h=20 track; every artefact is
written by basename, so the RUNBOOK §3 command run at h=10 **silently overwrites it**. The
h=10 track lives in `results/walkforward_h10/`.

⚠️ **`PRB-1`, found and fixed in the same session**: two Kaggle PROBE runs had been merged
into the CHAIN's report root, where `final_features` groups them with the real runs — the
`PRF-7` window probe **silently** (the data window is not a `SETUP_KEY`) and the `FNM-1`
representation probe as a hard collision that blocked planning entirely. Probes now write
to `reports/feature_selection_probes/`. **A run that measures the SELECTION is not a run
that feeds the CHAIN, and only the ROOT separates them.**

### ⚠️ 6-0-ter-2. SEVEN ARCHITECTURES AT h=10 — 2026-08-21, and §6-0-ter does NOT reproduce

The same test as `PRF-8`, one horizon down, **224× of capacity** (1,398 decision nodes to
313,153 parameters) against §6-0-ter's 101×. Four arms were written for it (`bilstm`,
`cnnlstm`, `tcn`, `transformer`); all seven trained on ONE build of each of 10 folds.
**2h 48m sweep + 22m scoring, 0 errors.**

⚠️ **THE TABLE BELOW WAS RE-SCORED ON 2026-08-21 WHEN `P1-9` SHIPPED, AND THE VERDICT
COLUMN SPLIT IN TWO.** `t_ret` is the old `t_paired` — a test of the mean period-RETURN
gap. `d_sharpe` now carries its own block-bootstrap interval, and **the two disagree about
three of the six arms.** Both are paired against `lstm` over the same 236 periods (ρ
0.91-0.94); at 30 bps, 2,000 circular block draws:

| arm | capacity | Sharpe@30 | IC | `t_ret` (MEAN) | **`d_sharpe` [95 % CI]** | `p_sharpe` | null z |
|---|---|---|---|---|---|---|---|
| **`gbt`** | **1,398 nodes** | **+2.891** | **+0.1460** | −1.02 | **+0.360 [+0.013, +0.721]** | **0.044** ⚠️ | +22.57 |
| `transformer` | 68,417 | +2.622 | +0.1433 | −0.33 | +0.091 [−0.171, +0.385] | 0.537 | +20.08 |
| `tcn` | 18,113 | +2.622 | +0.1426 | −0.20 | +0.091 [−0.119, +0.339] | 0.406 | +20.25 |
| `lstm` *(ref)* | 208,769 | +2.531 | +0.1412 | — | — | — | +18.58 |
| `bilstm` | 313,153 | +2.474 | +0.1419 | **−2.09** ❌ | −0.058 [−0.279, +0.161] | **0.612** ✅ tie | +17.55 |
| `cnnlstm` | 30,369 | +2.367 | +0.1308 | **−2.15** ❌ | −0.164 [−0.472, +0.157] | **0.295** ✅ tie | +16.80 |
| **`cnn`** | 5,185 | +2.133 | +0.1171 | **−3.37** ❌ | **−0.398 [−0.678, −0.135]** | **0.001** ❌ | +15.37 |

⚠️ **"THREE LOSE SIGNIFICANTLY" WAS A CLAIM ABOUT MEAN RETURN. ON SHARPE, ONE DOES.**
`bilstm` and `cnnlstm` earn less per period at *lower volatility*, so their risk-adjusted
gap is indistinguishable from zero (p = 0.61 and 0.30). Only `cnn` loses on both.

⚠️ **AND `gbt` BEATS THE LSTM ON SHARPE AT A NOMINAL p = 0.044, WHICH DOES NOT SURVIVE THE
SIX ARMS THAT WERE TRIED.** Bonferroni over one reference and six challengers is
**0.05/6 = 0.0083**: `cnn`'s 0.001 clears it, `gbt`'s 0.044 does not. That is `NUL-1` one
level up and the same shape §6-1 point 3 records for the five-ticker search — so the honest
statement is *"`gbt` is the best arm measured and its advantage is not established"*, never
*"`gbt` wins"*. ⚠️ Its `p_sharpe` also **rises with cost** (0.040 → 0.044 → 0.051 at
20/30/50 bps), which is `backtest` §3's identity showing through: `gbt` trades more.

⚠️ **SO THE SENTENCE THAT SURVIVES IS NARROWER THAN BOTH EARLIER ONES.** Not *"the
architecture is worth nothing"* (§6-0-ter, h=20, and also read off the MEAN column) and not
*"three lose"*. It is: **one architecture is measurably worse risk-adjusted (`cnn`, which
pools the sequence away), the rest tie, and the best-looking one cannot be separated from
the reference once the search is priced.**

✅ **AND THE SEED FLOOR UNDER THIS WHOLE TABLE WAS MEASURED 2026-08-21 — it is
`|d_sharpe| ≈ 0.09`.** Every config in the repo is `seed: 42` (32 of 32) and every row above
is ONE fit per arm per fold, so five `gbt` arms differing only in the seed were run over the
identical folds (13m 16s; `walkforward/CONTEXT.md` §15). Pooled Sharpe@30 came back
**2.845 … 2.979, sd 0.054**, max paired `d_sharpe` **0.088**. Reading the table against it:

| arm | `d_sharpe` | × the seed floor | verdict |
|---|---|---|---|
| `cnn` | −0.398 | **4.5×** | ✅ real |
| `gbt` | +0.360 | **4.1×** | ✅ **not seed luck** — and still not established (Bonferroni) |
| `cnnlstm` | −0.164 | 1.9× | tie |
| `transformer` / `tcn` | +0.091 | **1.0×** | ⚠️ **the whole gap is one seed** |
| `bilstm` | −0.058 | **0.7×** | ⚠️ **inside** the floor |

⚠️ **SO THE ORDERING OF THE FOUR MIDDLE ARMS IS NOT AN ORDERING.** *"Tied"* and *"the
measured gap is the size of a reseed"* are different statements and only the second closes
it. ⚠️ **A PER-FOLD CELL IS 4.4× NOISIER STILL** — mean per-fold Sharpe range over five
seeds **0.593** against the pooled **0.134**, worst fold **1.079**. Never compare two arms
in one fold. ✅ **The DECAY is not a seed artefact** (slope −0.308 ± 0.027, the first-to-second
half fall −55 % in all five). ⚠️ **One architecture, and the cheapest one**: `gbt` resamples
only rows and columns, while an LSTM also varies its initialisation — the reference arm's
floor could be larger and is unmeasured.

⚠️ **`ac1` IS THE REASON THE BOOTSTRAP IS TRUSTED HERE**: the lag-1 autocorrelation of every
arm's difference series is **−0.09 … +0.06**, so the periods really are near-independent and
`block=2` is not doing hidden work. Measured, not assumed — it is printed per row.

⚠️ **AND IT IS NOT A CAPACITY STORY.** The best arm is the SMALLEST (1,398 nodes); the
largest (313 k) sits *below* the 209 k reference; the second-worst is 5,185 parameters.
What separates them is inductive bias — `cnn` pools the sequence away and loses 0.40
Sharpe, while the two arms keeping a per-timestep view of the whole window tie. §6-0-ter's
reading that **the sequence inside the lookback is worth nothing** reproduces and is the
better explanation of the whole table.

✅ **`P1-9` SHIPPED 2026-08-21 AND THIS IS WHAT IT MEASURED.** `compare.paired()` computed
its `t` on the mean period-RETURN difference while the table printed `d_sharpe` beside it
bare, so both this section and §6-0-ter were read off the wrong column. It now reports
**both estimands**, each with its own interval, by reusing `walkforward.pair`'s
`block_bootstrap_diff` rather than a second implementation. ✅ **The h=20 `PRF-8` sweep was
re-scored the same day and ITS ties hold on both** (§6-0-ter) — so the disagreement is a
property of h=10, not of the fix.

⚠️ **AND `gbt` CHANGES SIGN BETWEEN THE HORIZONS — measured 2026-08-21, and it is the
strongest argument against promoting it.** Same tool, same `k`, same universe, same
reference arm: `d_sharpe` vs `lstm` is **+0.360 [+0.013, +0.721]** at h=10 and **−0.016
[−0.299, +0.291]** at h=20 (`p_sharpe` 0.044 against **0.941**). Two estimates that
disagree in SIGN across a neighbouring horizon are what a null effect looks like. ⚠️ It is
**not** a paired test across horizons — only `walkforward.pair` can do that and it has not
been run on the arms — so read it as two independent estimates.

⚠️ **AND "BEST EPOCH IS 1" IS AN LSTM PROPERTY, NOT A PROPERTY OF THE PROBLEM.** That
sentence has been quoted four times here as evidence capacity is worthless. Across these 70
runs only **43** stop by epoch 2: `cnn` averages **7.7** (max 20) and `tcn` **5.7** (max 13).
Attach it to an architecture from now on. `walkforward/CONTEXT.md` §11.

### ⚠️ 6-0-ter-3. AND NEITHER DO THE DATASET SETTINGS — 2026-08-21

Six full walk-forward tracks at h=10, one per setting, `gbt` throughout, scored **paired**
against a baseline that reproduces §6-0-ter-2's `gbt` row to every digit.

| setting | Sharpe@30 | Δ | paired `t` | ρ |
|---|---|---|---|---|
| validation 12 → **6** months | +2.9655 | +0.075 | +0.33 | 0.972 |
| validation 12 → **24** months | +2.7562 | −0.135 | −1.32 | 0.946 |
| refold every **6** months (**20 folds**) | +2.8977 | +0.007 | −0.09 | 0.989 |
| `scale_target` off | +2.8910 | **0.0000** | **NaN** | **1.0000** |

**Every setting ties; no `|t|` reaches 1.4.** ⚠️ **`step6` is the one to read**: retraining
**twice as often** moves nothing (ρ 0.989), so the ~45 % Sharpe decay across the sweep
(§6-0-bis-3) is **not staleness** — refitting does not arrest it.

⚠️ **`scale_target` is bit-identical because trees are scale-invariant, not because the
knob is inert.** Splits depend on the ORDERING of `y`, standardising is affine, and
`engine._write_predictions` inverse-transforms — end to end, the identity. **For a neural
net it would not be**, and choosing `gbt` for speed made that one setting unanswerable for
the family that uses it. ⚠️ `rank_min_width` was REFUSED by `compare` as a different
experiment (349,371 rows against 349,581) — it moves the LABEL, not the split.
⚠️ **`lookback` was never swept and is the one that would matter**: `d` comes from the
source TABLE NAME, so each value needs its own selection run. `walkforward/CONTEXT.md` §12.

### ⚠️ 6-0-quater. PRF-9 — MORE FEATURES DO NOT PAY EITHER, AND THAT CLOSES THE SECOND LEVER

Run 2026-08-19. `pool__ta` is the only widening available at all — `PRF-9`'s survey found
**71 of 76 gold tables are date-only**, and a column identical for every ticker on a date
has a constant within-date rank, so ~4,500 channels are *structurally* incapable of ranking
a cross-section. `pool__ta`'s 711 numeric channels are the exception.

**They were offered, and they did not pay.** 120-channel selection (90 `pool__basic` + 30
`pool__ta`, the latter chosen LABEL-FREE by `feature_selection.prune`), 22 shortlisted of
which 6 from `pool__ta`, built into `rank_20day__final__d20_h20__wide` and trained with the
architecture, schedule, seed and universe copied unchanged. Priced against the narrow chain
on the **intersection** of their rows (646 dates, 32 periods, top-15):

| | daily IC, same rows | Sharpe@30 | null z |
|---|---|---|---|
| wide (22 ch) | **+0.1053** | +1.496 | +4.53 |
| narrow (13 ch) | +0.0927 | **+1.623** | +5.42 |

Paired, ρ **0.90**: ΔSharpe **−0.126**, `t` = **−0.29**. **The extra channels moved the
shortlist and not the money.**

⚠️ **REPRODUCED AT h=10, 2026-08-21, AND IT HOLDS.** 162 candidates (90 `pool__basic`
+ 72 pruned `pool__ta`) against `PRF-9`'s 120, a GBT instead of an LSTM, selection on a T4
in 44m 12s: **21 shortlisted, 18 `pool__basic` and 3 `pool__ta`**, top NINE all
`pool__basic`, `drv_order_vol_imb` #1 again. Priced downstream on the 340,183 rows the two
chains SHARE — daily IC **+0.1520 vs +0.1484**, Sharpe@30 **+2.8136 vs +2.8910**, paired
over 236 periods at ρ 0.943: **`t` = +0.46**. ⚠️ **IC up, Sharpe down — the same split
`PRF-9` found**, and the paired test separates neither from zero.
`walkforward/CONTEXT.md` §13.

⚠️ **WITH `PRF-8`, TWO OF THE THREE OBVIOUS LEVERS ARE NOW CLOSED BY MEASUREMENT.** A model
101× smaller ties; 30 more candidate channels tie. **The 13 original channels are the
result.** What remains is honest execution (`PRF-4`/`PRF-5`) and NEW INFORMATION (`PRF-6`,
§2d) — not a better model and not more of this data.

⚠️ **`VRM-1` bounds how much of `pool__ta` could be tried.** Only **30 of 405** pruned
channels were offered, because the width ceiling on a T4 is **VRAM** — `xgb_shap`'s SHAP
contributions, `(n_rows, channels × 6 + 1)` — and not host RAM, which survived 24.5 GB at
140 channels. So this is *"these 30 did not pay"*, never *"`pool__ta` is useless"*. ⚠️ All
three of `MEM-1`, `P3-2` and `PRF-9` predicted the host wall and all three were wrong about
which one binds.

### ⚠️ 6-0-c. What the headline still does NOT say

**(1)** ⚠️ **It ranks, it does not price.** R² test **+0.0003**, RMSE 0.29065 against a
constant-predictor 0.29070. Only the ORDER carries — see §6-0-b.
**(2)** `long_short = +0.0635` is a **rank** spread, not money — the label is a rank.
**(3)** `NUL-1` — no null anywhere in this chain prices in the feature selection, the
architecture search, the early stopping, or the choice of `h=20`, `k=20` and the universe.
**(4)** ✅ `FNM-1` **MEASURED 2026-08-19 and it holds.** The selection scored those 13
channels under `feature_normalize=cs_rank` while the dataset feeds them globally
standardised — two representations, and §5 rule 1 says a bar computed for one says nothing
about another. Re-running the identical selection under `feature_normalize=none` (22m 04s
on a T4; every other setup key including `env_fingerprint` unchanged) keeps **12 of the 13**,
kept-set overlap **53 of 61**, Jaccard 0.779, **+5.90 sd** above chance, and the **same
channel at #1** (`drv_order_vol_imb`). ⚠️ **What that establishes is narrower than "it
passed": the CHANNEL SET is representation-invariant, but the BAR does not transfer** — the
`none` run carries no null, so **`z = +9.09` remains a `cs_rank` number**. ⚠️ And `none`
scores *higher* on the same folds (`ic_mean` **+0.1215** vs +0.1075), which says the
within-date feature ranking is not what is doing the work — read it as a channel-set fact,
not as a result, because it has no bar. TODO P1-6.
**(5)** **Survivorship protects the `z` and not the CAGR** (§2c). Every shuffled draw picks
from the same survivor basket, so +12.3 stands and **+47.5 %/yr does not**.
**(6)** **No slippage, no ADV cap, no floor-day exclusion on the SELL side** — `PRF-4`.

### ⚠️ 6-0-bis. AND STAGE 9 ANSWERED THE TRADABILITY QUESTION — portfolio yes, ONE STOCK no

`src/backtest/` (2026-08-18) is the first thing here that charges costs. Two results, and
the second one matters more for anyone who wants a buy/sell signal on a single name.

**The portfolio works, at this horizon, after costs.** Top-15 of 150, rebalanced every 20
sessions, 50 bps, long-only (no shorting — HOSE does not offer it), clearing a 200-draw
within-date shuffle null at **z = +4.29** (test) and **+6.10** (val), null MAX below
observed in all four cells.

⚠️ **THE NUMBERS MOVED UP ON 2026-08-19 AND THE REASON IS `PRF-0`, NOT A BETTER MODEL.**
`backtest.build_panel` now drops names sitting at their exchange's daily ceiling on the
entry date — they have no sellers, so buying them was fiction — and the stage prints how
many it dropped. It used to be a probe a reader had to remember to run:

| 50 bps, top-15 | as first reported | **screened, the default now** | rows dropped |
|---|---|---|---|
| test | +1.4845 / CAGR +30.5 % | **+1.5512** / **+32.3 %** | 1,708 (1.83 %) |
| val | +1.7367 / CAGR +69.9 % | **+1.7385** / **+70.0 %** | 3,437 (3.76 %) |

⚠️ Both reproduce §8h's probe EXACTLY, which is how the change was verified rather than
assumed. ⚠️ **And it is the walk-forward, not this split, that should be quoted** — §6-0. `k` is not a
knife-edge — Sharpe decays monotonically 1.53 (k=10) → 0.81 (k=75).

⚠️ **This CONTRADICTS §11's regime wall in the direction §2a-bis predicts.** §11 measured
net@20bps **−0.51 in 2022-26** and called the recent regime unlearnable; this window IS
2023-26 and returns +1.48 at **50** bps. The two studies differ in the HORIZON — 5 days
against 20 — which is the variable §2a-bis says nobody controlled for.

⚠️ **VCB, SPECIFICALLY: ZERO TRADES IN 33 PERIODS.** Its median percentile among the 150
is **0.273**; the maximum it ever reached is **0.826**, so it never touches the 0.90 entry
band. That is a correct call, not a failure — VCB returned **+1.45 % CAGR** over the test
window while the universe made 5.96 % and VNINDEX **20.2 %**. Lowering the band until it
trades gives Sharpe −0.65…+0.38 on 1-12 periods, and none of it beats holding the stock.
**A correct "do not hold this" is not a tradable signal for that stock.**

⚠️ **THE HORIZON IS ALSO THE COST VARIABLE, and this had never been written down.** At
turnover 0.70 and 50 bps the annual fee drag is **17.6 % at h=5**, 8.8 % at h=10, **4.4 %
at h=20** — against a top-100 benchmark CAGR of 9.75 % (§2a-bis). **At h=5 the fees alone
exceed the market's entire return.** Four single-stock defeats at `h=5` were never going
to be rescued by a better model. `backtest/CONTEXT.md` §3.

⚠️ **What stage 9 does NOT establish**: `NUL-1` in full force (the null prices in the
universe, cost, schedule and `k` — never the feature selection, the architecture search or
the choice of window); **32 periods**, `se_sharpe` 0.256; survivorship protects the z but
**not** the +30.5 % (§2c); `val` chose the early-stopping epoch; one split, no
walk-forward.

⚠️ **It cannot give you a price for one stock, and that is structural.** The label removes
the market factor by construction, so inverting it needs a 20-day market forecast plus a
cross-sectional dispersion forecast — the two things §2 has failed at four times. What it
answers is *"where will VCB sit among these 150 over the next 20 sessions"*, and reading
that requires scoring all 150 on the same date.

### ⚠️ 6-0-ter. AND THE ARCHITECTURE IS WORTH NOTHING — PRF-8, 2026-08-19

`src/walkforward/` runs the h=20 chain over **ten expanding folds** (PRF-1, same day), and
PRF-8 then ran three architectures over the **identical folds and one build of each fold's
tensors** — 15m 03s. Pooled over 118 non-overlapping periods, top-20 of 150, buyable only:

| arm | capacity | IC | `ic_t` | Sharpe@30 | `z` (200 draws) |
|---|---|---|---|---|---|
| `lstm` | **205,441 params** | +0.1097 | 6.90 | **+1.991** | +12.28 ✅ |
| **`lstm_small`** | **2,033 params — 101×** | +0.1239 | 9.49 | **+1.997** | +12.43 ✅ |
| `gbt` | **1,400 decision nodes** | +0.1249 | 8.90 | **+1.975** | +11.88 ✅ |

⚠️ **PAIRED, because the arms share the market factor** (ρ = 0.88 between their period
returns, so `se_sharpe = 0.155` is the error bar on the wrong quantity): `lstm_small`
`t = +0.87…+0.88`, `gbt` `t = +0.42…+0.47` at 20/30/50 bps. **Every |t| < 1.**

✅ **RE-SCORED 2026-08-21 THROUGH THE FIXED `compare`, AND THE TIES HOLD ON BOTH
ESTIMANDS.** That `t` tested the MEAN RETURN (`P1-9`); the run that settles it took
**1m 29s** and reproduced §6-0-ter to every digit — Sharpe@30 **1.9913 / 1.9970 /
1.9750**, `se_sharpe` 0.1553 — while adding the risk-adjusted column: `lstm_small`
**`d_sharpe` +0.006 [−0.289, +0.381], p = 0.903**; `gbt` **−0.016 [−0.299, +0.291],
p = 0.941**. ⚠️ **So *"a 101× capacity span ties at h=20"* is now a risk-adjusted result
and not only a return one, and NO caveat is left on this row.** ⚠️ At h=10 the same code
found the two columns disagreeing about three of six arms (§6-0-ter-2), so the fix is not
biased toward finding disagreement — it finds it where it is.

⚠️ **THE RESULT LIVES IN THE 13 CHANNELS, NOT IN THE ARCHITECTURE**, and this is the fourth
independent measurement pointing that way — after §5c's eleven architectures inside one
error bar, `P2-3`'s *"best epoch 1 of 21"*, and PRF-1's nine-of-ten folds stopping at epoch
1. It is the first that moved capacity DELIBERATELY. ⚠️ **The sequence inside the lookback
is worth nothing either**: `model.gbt` compresses each (20, 13) window to **78 window
statistics where the LSTM sees 260 numbers**, and it ties.

⚠️ **So "try a bigger model" is closed as an answer to anything in this repo** — ⚠️ **but
"any model will do" is NOT, and 2026-08-21 measured the difference** (§6-0-ter-2): at h=10 a
CNN loses **0.40 Sharpe** to the LSTM, and it is the ONE arm that loses on the
risk-adjusted test as well as on mean return (**p = 0.001**, the only one surviving a
correction for six arms). The bidirectional LSTM's `t` = −2.09 is a MEAN-RETURN loss whose
Sharpe gap is a tie (p = 0.61). Bigger buys nothing; the WRONG INDUCTIVE BIAS costs. What is
left is FEATURES (TODO `PRF-9`, 90 → 800 candidates), the HORIZON (`PRF-2`), honest
EXECUTION (`PRF-4`/`PRF-5`) and new DATA (`PRF-6`). ⚠️ It also makes `PRF-7`'s bounded
selection look-ahead close to the WHOLE story about where this Sharpe comes from rather
than part of it — the only other candidate has been ruled out. ⚠️ **Not a claim that the
small model should replace the big one**: nothing was re-tuned for it, and a tie under one
schedule is not an optimum. `walkforward/CONTEXT.md` §8.

⚠️ **TWO CONCURRENT `walkforward` SWEEPS SILENTLY CORRUPT EACH OTHER** (measured on this
run, and it cost the first attempt). Every fold writes
`train_test_set/<ticker>__<table>__…__<tag>` — a name derived from the DATA with no term
for which process built it — saved with `replace=True` and deleted once its arms are done.
The loud half is a `FileNotFoundError` in the second sweep; **the silent half is the first
sweep reading tensors the second is mid-`np.save` on**. `run.namespace_lock` refuses the
second sweep now, by pid, taking over a lock whose holder is dead.

### ⚠️ 6-1. THE SINGLE-STOCK TRACK AT h=10 — FIVE TICKERS, AND IT DOES NOT CLEAR

Run 2026-08-19, and it is the **fifth independent failure** of single-stock short-horizon
prediction in this repo — the first at a horizon other than 5 days, and the first on more
than one name. Motivation was §2a-bis: four defeats had all been at `h=5`, so the HORIZON
had never been controlled for on a single stock. It has now.

**Setup.** `pool__basic` alone (90 numeric channels), `return_10day`, `d=20 h=10`, purge
`d+h-1 = 29`, 5 expanding folds (`n_train` 500/1267/2034/2801/3568, `n_test` 767,
`n_eff_test` **76.7**), `date_block` null with the whole selection re-run inside each draw,
GPU, 5m25s-5m32s per ticker. ⚠️ `pool__ta` was deliberately EXCLUDED — `FRZ-1`'s re-scrape
moved `pool__basic` 37 sessions past `gold.stocks_ta` (`STA-1`), so a `basic + ta` join
would have truncated the very rows the re-scrape was run to obtain.

| ticker | kept | `ic_mean` | trend | null mean | p95 bar | **null MAX** | **z** | p | verdict |
|---|---|---|---|---|---|---|---|---|---|
| HPG | 55 | +0.0169 | +0.0072 | +0.0109 | +0.0877 | +0.0958 | **+0.11** | 0.545 | ❌ |
| SSI | 54 | +0.0420 | +0.0184 | +0.0068 | +0.0632 | +0.0814 | **+0.96** | 0.182 | ❌ |
| FPT | 54 | +0.0720 | +0.0509 | +0.0051 | +0.0690 | +0.0794 | +1.74 | 0.182 | ⚠️ rule 3 |
| VIC | 60 | −0.0214 | −0.0458 | +0.0150 | +0.0785 | +0.0792 | **−0.69** | 0.727 | ❌ below the null's mean |
| **STB, 20 draws** | 61 | **+0.1014** | −0.0179 | +0.0047 | +0.0830 | +0.0866 | **+1.83** | 0.0476 | ⚠️ |

**Pooled over the 5 independent names** (excess over each run's own null mean):
**+0.0332, sd 0.0512, se 0.0229 → t = +1.45, p = 0.220.**

Four things this measured, and each one is reusable:

1. ⚠️ **RULE 3 FIRES ON FOUR OF FIVE.** HPG, SSI, FPT and VIC each had a shuffled draw beat
   the real data. FPT's `cleared_p95_not_a_pass` is the label working as designed.
2. ⚠️ **MORE DRAWS MOVED `z` DOWN, NOT UP.** STB at 10 draws was `z = +2.13`; the identical
   run at 20 draws is **`z = +1.83`** — the observed is deterministic and unchanged at
   +0.1014, and only the null moved, its **sd growing 0.0444 → 0.0528 (+19 %)**. That is
   within one `SE(sd)` and so CONFIRMS the "10 to fail, 20 to pass" rule rather than
   contradicting it: a 10-draw `z` on a result that lands far above is optimistic, which is
   exactly why the rule asks for 20 before anything is promoted.
3. ⚠️ **THE NULL PRICES THE FEATURE SEARCH, NEVER THE TICKER SEARCH.** Five names were
   tried. P(z > 1.83) once is 0.0336; **P(at least one of five) = 0.157**. STB's survival is
   about as surprising as nothing at all. This is `NUL-1` one level up, the same shape §2b-bis
   point 3 records for the universe/horizon/target choice.
4. ⚠️ **RULE 23 EXPLAINS BOTH APPARENT CLEARANCES, AND `validation.csv` STILL CANNOT SHOW IT**
   (`P4-2` confirmed open — the file carries `n_train`/`n_test`/`ic`/`r2`/`hit_rate` and **no**
   `n_dead_train`). Counting all-NaN-in-train channels externally, per fold, of each shortlist:

   | ticker | dead in train, folds 1-5 | folds' selected IC |
   |---|---|---|
   | HPG | 6, 3, 3, 3, **0** | −0.062 +0.198 −0.086 −0.074 +0.110 |
   | SSI | 2, 2, 2, 2, **0** | −0.059 +0.094 +0.143 −0.097 +0.129 |
   | **FPT** | **9**, 3, 3, 3, **0** | **−0.137** +0.122 +0.148 +0.097 +0.130 |
   | VIC | 6, 3, 3, 3, **0** | +0.026 +0.066 +0.024 −0.108 −0.115 |
   | **STB** | **8**, 5, 5, 5, **0** | +0.028 **+0.240 +0.191** −0.021 +0.069 |

   **FPT is the textbook case**: 9 of 20 shortlisted channels are constants in fold 1's
   training slice, fold 1 scores −0.137, every later fold +0.10…+0.15, and
   `ic_trend_per_fold` is **+0.0509** — the steepest of the five. Rule 23 says a rising trend
   on a ragged pool measures **data arrival**, not a strengthening signal.
   **STB is weak differently**: +0.1014 is carried by folds 2-3 alone, and fold 5 — the only
   fold where all 21 channels are alive in train — scores **+0.069**. At `n_eff_test = 76.7`,
   `SE(IC) ≈ 0.114`, so only fold 2 exceeds 2 sd and the other four sit inside 1 sd of zero
   (across its own folds, `t = +2.05`, p = 0.109).

⚠️ **`prop_*` MUST BE EXCLUDED FROM ANY RUN AT THIS TIME SCALE, AND IT WAS NOT.** Proprietary
flow starts **2023-01-03** — coverage **0.197-0.203** — yet `prop_sell_vol` and `prop_buy_val`
were shortlisted by **4 of 5** tickers and `drv_prop_participation` by 3. They are all-NaN in
the training slice of folds 1-4, imputed to the constant `0.0`, and then RANKED. Flow that
IS usable: **order stats from 2010-01-04** (≥0.95) and **foreign from 2012-01-03** (~0.70) —
§2d's top lever survives, the prop block does not.

⚠️ **What this does NOT establish.** It is one pool. `pool__ta`'s ~900 channels, the 19
`pool__economy_*` blocks and the 47 `pool__forex_*` blocks were **not** offered — and the
date-only blocks are the one thing that is structurally DEAD for a cross-section (a column
constant within a date has a constant within-date rank, `PRF-9`) and perfectly valid for a
single stock. **71 of 76 gold tables are date-only.** So the honest statement is *"the stock's
own 90 channels carry nothing at h=10 on these five names"*, not *"nothing does"*.

### ⚠️ 6-1-bis. THE SAME QUESTION ON 30 VN30 NAMES — and the POOLED answer flips

Run 2026-08-20. `pool__basic` (90 numeric channels), `return_10day`, `d=20 h=10`, 5
expanding folds, a `date_block` null **per name** with the whole selection re-run inside
each draw. 30 names, ~5.5 min each.

| | |
|---|---|
| observed `ic_mean` | mean **+0.0635**, sd 0.0571 |
| each run's own null | mean **+0.0023**, sd 0.0140 — well centred (se of a 10-draw null mean is 0.0175) |
| pooled excess | **+0.0611**, sd 0.0637 |
| naive `t` (29 df) | **+5.25** |
| cross-name dependence | ρ̄ = **+0.032** over the 5-fold IC vectors → `n_eff` **15.5** |
| **dependence-adjusted `t`** | **+3.77, p = 0.002** |

**This is the first time in this repo that a single-stock short-horizon selection has
beaten its own null in aggregate.** It does not overturn §2, and four measured reasons say
why:

1. ⚠️ **NO INDIVIDUAL NAME IS CONVINCING.** `z > +1.645` on **8 of 30** (chance says 1.5 —
   above chance, but it is 8), **5 names are NEGATIVE**, and **rule 3 fires on 22 of 30**:
   a shuffled draw beat the real data for 73 % of the names. The per-name picture is the
   same as §6-1's five; what changed is that 30 draws of a small effect resolve it and 5
   do not.
2. ⚠️ **THE EFFECT IS SMALL.** Mean selected IC **+0.064** at `n_eff_test = 76.7` per fold
   — about 1.3 SE for one name. It is detectable in aggregate and useless one name at a
   time, which is the same shape §2b found from the other direction.
3. ⚠️ **`NUL-1` IN FULL FORCE.** These nulls price the FEATURE search inside each run.
   Nothing prices the choice of `h=10` over `h=5`, of `return_10day` over three other
   labels, of VN30, or the fact that this is the fifth thread tried. §6-1 measured that
   cost directly at one level up: `P(at least one of five names above z=1.83) = 0.157`.
4. ⚠️ **A CLEARED SELECTION BAR HAS NEVER SURVIVED DOWNSTREAM HERE** — §5d (`z = +2.15`
   bought nothing) and TODO `P2-3`. Selection IC is not model skill and certainly not money.

⚠️ **The five-name result in §6-1 is superseded for the POOLED question and stands for
every per-name one.** `t = +1.45` on five names and `t = +3.77` on thirty are the same
effect at two sample sizes — the five happened to include VIC (`z −0.69`) and HPG
(`z +0.11`), two of the weakest. **That is what an underpowered sample of a small effect
looks like, and recording it is the point: the 5-name run was not wrong, it was small.**

⚠️ **The fold pattern rises monotonically** — mean IC per fold across the 30 names is
**+0.020 / +0.045 / +0.088 / +0.071 / +0.093**. Fold 1 trains on 500 rows and carries the
most dead channels (§6-1's rule-23 table). The null re-runs the selection inside every
draw, so it *does* price a ragged pool — but the trend is still the reason not to read
+0.064 as a stationary edge.

### ⚠️ 6-1-ter. WIDENING A SINGLE STOCK HURTS; WIDENING THE CROSS-SECTION HELPS

Two measurements on 2026-08-20, opposite in sign, and the contrast is the finding.

**Single stock** — HPG, `return_10day`, identical folds, `pool__basic` versus
`pool__basic + pool__stock_market + pool__bonds + pool__market_breadth +
pool__economy_vietnam`:

| | channels | kept | `ic_mean` | null mean | null MAX | **z** |
|---|---|---|---|---|---|---|
| narrow | 90 | 55 | **+0.0169** | +0.0109 | +0.0958 | **+0.11** |
| **wide** | **470** | 218 | **−0.0064** | +0.0119 | +0.0893 | **−0.40** |

⚠️ **This is the first time the date-only macro blocks have been offered to a RETURN target
on a single stock** — §6-1 named it as the one structurally-new lever left, because
`PRF-9` proved those blocks cannot rank a cross-section while they are perfectly valid for
one name. Offered, they take HPG **below its null's mean**. 19m58s for one name.

**Cross-section** — VN30, `cs_rank_10day`, 65,763 rows × 30 names × 2,288 dates
(2017-04-21 → 2026-06-26):

| | channels | kept | `ic_mean` | null mean | p95 | null MAX | **z** |
|---|---|---|---|---|---|---|---|
| `pool__basic` | 100 | 62 | +0.0204 | +0.0190 | +0.0370 | +0.0435 | **+0.10** ❌ |
| **+ the 145 reduced `pool__ta` channels** | 245 | 204 | **+0.0333** | +0.0136 | +0.0332 | +0.0342 | **+1.62** ⚠️ |

⚠️ **Neither is a pass** — 5b's null MAX (+0.0342) still exceeds its observed (+0.0333), so
rule 3 fires and `cleared_p95_not_a_pass` is the right label. What is measured is the
DIRECTION: width helps a panel and hurts a series, on the same data, in the same week.

### ⚠️ 6-1-quater. THE WIDTH LADDER RE-RUN AT h=10 — VN30 is z = +0.10 against top-150's +13.78

| | `PRF-2`, top-150 | **VN30** |
|---|---|---|
| N | 150 | **30** |
| dates | 4,368 (2009→) | **2,288 (2017→)** |
| `ic_mean` | **+0.1201** | **+0.0204** |
| null `sd` | ≈0.0065 | ≈0.014 |
| **z** | **+13.78** ✅ | **+0.10** ❌ |

Two things it establishes and one it does not:

1. ✅ **The `1/√N` mechanism reproduces exactly.** The null sd ratio is **2.15** against
   `√(150/30) = 2.24`. §2b's account of *why* width works is confirmed at a second horizon.
2. ⚠️ **But the OBSERVED IC collapsed too, 6×, and §2b says it should not have.** §2b's
   claim is *"the observed IC barely moves; the noise floor collapses"*. Here both moved.
   The extra suspect is §5d's: **a co-moving basket has less to rank**, and VN30 is the 30
   most co-moving names in the market — possibly the WORST 30-name cross-section available,
   not the best.
3. ⚠️ **It is NOT a single-variable comparison and must not be quoted as one.** N, the date
   window (4,368 → 2,288 sessions, because VN30 only reaches 20 listed members in 2017) and
   the universe RULE (pre-2014 liquidity vs today's index membership) all moved together,
   and all three push the same way. It bounds the cost of narrowing; it does not price N.

⚠️ **VN30 membership is `vn30.csv`, i.e. TODAY'S list with no history** (`UNIFIED_VN30` in
`preprocessor.py` carries the warning). Before 2017 the "VN30" here is 9-16 names that
happen to still be in the index in 2026. A within-date shuffle null is protected — every
draw sees the same basket — and any CAGR read off this universe is not.

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

**Run:** 1h 05m, **0 errors**, all four tabs at 780/780. `RUNBOOK.md` has the command and
the config; ⚠️ **`incremental: true` needs `skip_existing: false` beside it** or
`skip_existing` returns first and the run refreshes nothing while going green.

⚠️ **THIS CLOSED THE SCRAPE AND NOT THE CARRY-UP** — see §6-2-ter, which ran the same
session. Gold was 30/54 sessions behind BEFORE this scrape and further behind after it.
⚠️ **Both were `P1` and `P2` when this was written, and both left the list the same day** —
as did `STA-1` a few hours later (§6-2-quater). TODO's codes shifted **down by 3** in total
on 2026-08-23, so a `P<n>` written before that date means something else; the file's
2026-08-23 crosswalk resolves it.

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

⚠️ **AND ITS FIRST RUN FOUND 27 SINGLE-NAME UNIFIED SCHEMAS STALE**, in three layers that
are a fossil record of every scoped re-scrape this repo has run:

| stuck at | sessions behind | schemas |
|---|---|---|
| **2026-08-19** | 2 | FPT, HPG, SSI, STB, VIC — the `SSK-1` single-stock track |
| **2026-08-07** | 10 | BID, CTG, HDB, MBB, SHB, SSB, TCB, TPB, VIB, VPB — the bank re-scrape |
| **2026-06-25/26** | 40-41 | BCM, BVH, GAS, GVR, MSN, MWG, PLX, POW, SAB, VHM, VJC, VNM, VRE |

`unified_{all,vcb,bank,vn30,acb,price10k,liquid,quality}` are current. **Rule 14 from the
other side, counted for the first time** — and none of it is visible to `MAX(date)`, which
reads 2026-08-21 on the fresh schemas and says nothing about the other 27.

⚠️ **FILTER THE VIEW BY `layer`** — it is a `UNION ALL` over 39 layers and `gold.stocks_ta`
alone costs **26.5 s** (17 GB, 946 columns) against silver's 1.0 s. ✅ `EXPLAIN` verifies
`WHERE layer='silver'` prunes the other 38 branches on the constant. ⚠️ The first draft of
the per-ticker query **timed out at 5 minutes** — a correlated count per ticker; ranking
the calendar once with `ROW_NUMBER()` is **1.4 s** on the same table.
`pipeline/CONTEXT.md` §1a-bis.

### ⚠️ 6-3. THE DATA AUDIT — 2026-08-22, and the cross-section ENDS 2026-06-25

Measured across every ticker-keyed table in all three schemas. Full tables and the
resulting program is in **[TODO.md](docs/TODO.md)** (⚠️ renumbered again 2026-08-23 when the first two items closed); three numbers belong
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

**3. ⚠️ FUNDAMENTALS ARE 2 OF 781 AND THREE WALLS STAND IN FRONT OF THE OTHER 779** —
none of them a code problem: **disk** (PDFs for 112 tickers = 100 GB, median 906 MB each →
~700 GB for the universe, against **144 GB free**), **time** (~2.4 h/ticker of OCR →
**~78 days**), and **schema** — `raw_data/cafef/financials/statements/` holds **one**
template family, `bank`, while **761 of 781 names are not banks** (230 industrials, 117
materials, 93 consumer staples; only 20 are GICS 401010). ⚠️ **The schema wall is the real
one**: with infinite disk and time the current parser reaches 20 names. So `P3` prices a
JSON source (`api.simplize.vn`, `vnstock`) *before* anything else is built — §2d's
second-ranked lever is a data-acquisition question, not an OCR question.

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

**Open issues live in [ISSUES.md](docs/ISSUES.md)** (**14 open**, 38 resolved, codes permanent — ⚠️ **`STA-1` CLOSED 2026-08-23**: `gold.stocks_ta` rebuilt, 0 of 13 legacy names left, matching silver exactly, and the `basic + ta` join no longer truncates — which also closed **`SKW-1`** (§6-2-quater); ⚠️ **`FRZ-1` CLOSED 2026-08-23**: the price universe is fresh again, 771 of 784 tickers at 2026-08-21 against 5, and the fix was an `incremental` scrape mode whose restatement guard fired on 304 of 780 price tickers (§6-2-bis); **`SCP-1` opened-and-closed 2026-08-22** (a log-only helper assumed one bound parameter and took down a build) and **`FRZ-1` re-measured the same day**: 757 of 781 tickers stale, the cross-section ending 2026-06-25 while `MAX(date)` reads 2026-08-19 from five names; `WFO-1` closed and `BOO-1` opened-and-closed 2026-08-21; `PNL-2`/`PRB-1` closed and `VRM-1`/`FRZ-1` opened 2026-08-19). ⚠️ Counts here are a SCAN of the tables, not a running decrement — the previous "36 resolved" was one ahead of the file, and four fixed rows (`WFO-1`, `VRM-1`, `PNL-2`, `PRB-1`) deliberately sit struck-through in the Open table rather than moving.
Short version: ⚠️ **`SHP-1`** the forex scraper writes two file shapes and only one was
ever ingested — 71% of the folder was silently discarded until 2026-08-14, and **the
same `value`-only filter sits unchecked on `bonds`/`funds`/`economy`/`indices`**;
`FLT-1` the TradingView forex broker filter fails open for 19 of 47 brokers
(**`WID-1` — the 1,600-column block — was opened and cleared the same day**);
`EVD-1` the missing
nulls are ~1,000 CPU-hours, `NUL-1` the evaluator's null is structurally weak, `DRF-1` 18
channels put 100% of test beyond 5 train-sigmas, `COV-1` 248 of 952 shortlisted rows sit
below 0.95 coverage, `RPR-1` datasets/runs are git-ignored.

---

## 7. Where to read more — open ONE, only when you touch it

⚠️ **[docs/INDEX.md](docs/INDEX.md) is the complete map and it is already in your context**
(`@docs/INDEX.md`, top of this file). This table is the same routing for the packages,
kept here because the *when you are…* column is the part worth reading twice.
⚠️ **The token costs below were re-measured 2026-08-22 and every one of the 16 had gone
stale as the files grew** — `walkforward` was listed at 6k against a true 16.0k, and
`feature_selection` at 25k against 45.0k. A stale cost is worse than none, because it is
what a session budgets against.

| open this | ~tokens | when you are… |
|---|---|---|
| [src/orchestration/CONTEXT.md](src/orchestration/CONTEXT.md) | **47.0k** | touching Dagster, `config.json`, any asset, any bronze/silver/gold table, the browser budget, a scrape, or ⚠️ **the FILTER layer** (§"FILTER" — screens, `filter_schema`, and why a screen is not point-in-time) |
| [src/orchestration/preprocessor/CONTEXT.md](src/orchestration/preprocessor/CONTEXT.md) | **25.8k** | changing HOW a table is built — the `_ingest_*` / `_helper_*` transform library the assets wrap |
| [src/web_scraper/CONTEXT.md](src/web_scraper/CONTEXT.md) | **28.7k** | touching a scraper, the PDF/OCR statement parser, or `raw_data/` layout |
| [src/feature_selection/CONTEXT.md](src/feature_selection/CONTEXT.md) | **45.0k** | running or reading a selection, or quoting any IC / null / bar number. **§15a is the STEP-BY-STEP UI GUIDE** for the country sweep (§15a-cli is the same in PowerShell); §15b-§15d the two guards and the cost table; **§16 is the GPU conversion** — what moved, what was measured slower and left alone; §14c is the measured cut that replaced `max_features=12` |
| [src/feature_selection/docs/RANKER_COMPARISON.md](src/feature_selection/docs/RANKER_COMPARISON.md) | **4.5k** | asking which ranker to keep, drop or add, or quoting any per-ranker cost. The full scorecard behind `feature_selection` §19 — advantage vs a random-k control, both cost regimes, the ρ=0.864 duplicate pair, the REJECTED mRMR addition, and the two errors the measurement had to correct |
| [src/final_features/CONTEXT.md](src/final_features/CONTEXT.md) | **6.8k** | building or rebuilding a `__final__` table |
| [src/train_test_creator/CONTEXT.md](src/train_test_creator/CONTEXT.md) | **4.9k** | building a dataset, or asking about the purge/impute/scale/window steps |
| **[src/model/CONTEXT.md](src/model/CONTEXT.md)** | **12.0k** | training, adding a model type, or quoting any run's numbers. **§1a is the RUN STANDARD** (naming/input/output, enforced); §7 the new-model recipe; **§13–§16 are today's results** — CNN, Tier 1, Tier 2, the bank panel; §10–§11 the older research log ⚠️ now a citation without its evidence (RPR-1) |
| **[src/walkforward/CONTEXT.md](src/walkforward/CONTEXT.md)** | **16.0k** | asking whether a result survives more than ONE split, or which MODEL to use. §3 the 10-fold h=20 result (pooled Sharpe **+1.991**, IC positive 9/10 folds, beats the market 10/10); §4 the recorded prediction that was half wrong; §5 the no-mechanical-leak check; **§8 is PRF-8 — three architectures from 205 k params to 1,400 tree nodes, all tied**, and §8c the concurrency trap that voided a whole sweep |
| **[src/backtest/CONTEXT.md](src/backtest/CONTEXT.md)** | **8.1k** | asking whether a signal is TRADABLE — stage 9, the costed non-overlapping backtest. §3 is the cost identity that decides the horizon (h=5 pays **17.6 %/yr** in fees, above the top-100 benchmark's entire return); §4 the first result here to clear a costed null (top-15, z = **+4.29** test / +6.10 val); **§5 is the single-stock answer and it is "no trade"** |
| [src/result_evaluator/CONTEXT.md](src/result_evaluator/CONTEXT.md) | **4.1k** | scoring, the metric set, or panel-vs-series grain. ⚠️ **STALE — it predates `index.py`, the `rebuild_index` schema change and issue NUL-3.** Nothing in it is false; it is silent about all three |
| [src/pipeline/CONTEXT.md](src/pipeline/CONTEXT.md) | **7.0k** | the **six**-stage chain, staleness, `--root`/`--scope`, `--rescrape`, adding a stage or a second target |
| [src/sentiment/CONTEXT.md](src/sentiment/CONTEXT.md) | **3.4k** | anything news/text/PhoBERT |
| [src/kaggle_gpu/README.md](src/kaggle_gpu/README.md) | **6.4k** | running a repo notebook on a Kaggle T4 — the payload dataset, the parameter patcher, `rehearse`, **§7b PANEL MODE** (the one job that ships no pools), and §7's five measured traps (all five are "a green step is not evidence"; the fifth, `KGP-1`, had no green step at all) |
| [experiment/CONTEXT.md](experiment/CONTEXT.md) | **9.2k** | the 9 exploratory experiments — signal discovery, tradability, point-in-time data, VN OCR |
| [experiment/experiment_10/CONTEXT.md](experiment/experiment_10/CONTEXT.md) | **44.0k** | writing the literature chapter. **§"Combined reading" (line 2877) is the distillate** — read that alone unless you need a specific paper |

⚠️ **[ISSUES.md](docs/ISSUES.md) (~4k) is the second file to open, not an afterthought.** Sixteen
open issues. **SHP-1** is the one to read first — a `value`-only filter silently
discarded 71% of the forex folder for as long as that ingest existed, and the same
filter sits unchecked on four sibling ingests. **Four** change how a number may be read:
**NUL-3** (the panel null is not label-neutral — on a panel quote the daily-IC t-stat,
never `ic_clears`), **NUL-1** (no null here prices in selection or architecture search),
**RPR-1** (29 run folders were deleted 2026-08-10 and are unrecoverable), and
**OUT-1** (one corrupt cell — VCB 2026-01-05, `prop_buy_val` 4.001e17 — manufactures a
+0.266 forward correlation, and ~0.1% of `foreign_*` rows carry the same defect: check
the extremes before selecting on any foreign or prop channel). **FLT-1**
bounds what forex data can exist at all: 19 of 47 broker filters fail open, so 37
brokers' books are unreachable.

### The other files in `docs/`

⚠️ **These moved out of the repo root on 2026-08-22.** The filenames did not change, so a
prose citation like *"`RUNBOOK.md` §8 rule 1"* — including the ~12 in Python docstrings —
still resolves; only the PATH gained a `docs/` prefix.

| file | what it is | read it when |
|---|---|---|
| **[RUNBOOK.md](docs/RUNBOOK.md)** | the operating guide — 8 stages with MEASURED runtimes, the two flags that destroy things, the target-switch leakage trap, and §10's list of what is deliberately not standardized | you are about to run something |
| **[ISSUES.md](docs/ISSUES.md)** | 14 open / 38 resolved, permanent codes | before quoting any number — four of them change how a number may be READ |
| **[TODO.md](docs/TODO.md)** | the one backlog — ⚠️ **DATA FIRST.** `P1` … `P36` in six lettered groups — **A data `P1`-`P2`, B OCR→Kaggle `P3`-`P6`**, C output `P7`-`P8`, D model `P9`-`P17`, E honesty `P18`-`P21`, F backlog `P22`-`P36`. ⚠️ **RENUMBERED TWICE ON 2026-08-23** — the re-scrape, the gold carry-up and `STA-1` all closed that day, so every code moved **DOWN BY 3**. A `P<n>` written earlier means a different item; the file's 2026-08-23 crosswalk resolves it. ⚠️ **A HYPHENATED code is retired** (`PRF-4` is now `P11`, `P4-2` is now `P21`, …). ⚠️ **AND THAT RENUMBERING WAS BARE → BARE**, so a `P<n>` written before 2026-08-22 evening resolves to a DIFFERENT item — `P1` was live scoring and is now `P7`. **TODO.md's 2026-08-22 crosswalk is the bridge**; the live pointers in this file, RUNBOOK.md, pipeline.md and PIPELINE_h10_CAGR74.md were rewritten with it | deciding what to do next |
| **[pipeline.md](docs/pipeline.md)** | ⚠️ **what the chain OUTPUTS — `(date, ticker, weight)`** — 4,720 picks across 236 dated books, with the measured statistics: 65.1 % turnover, **UPCOM over-picked 2.20×**, one book is a coin flip (60.2 % of picks in the top half). ⚠️ **§6 is why there is no book for TODAY**: after 2026-06-11 only **7 of 150** names carry data | asking *"which ticker, on which date"* |
| **[PIPELINE_h10_CAGR74.md](docs/PIPELINE_h10_CAGR74.md)** | ⚠️ **how ONE number gets made, end to end** — the h=10 cross-sectional chain that returns **CAGR +74.0 %/yr** (Sharpe@30 +2.531, z = +18.58). Raw scrape → pools → the 19 channels → the LSTM → the costed walk-forward, with every artefact id and every measured runtime. **§12 is the caveat section and is the reason the file exists** | explaining the result to anyone, or reproducing it |
| `README.md` | the front door; routes here — ⚠️ **stays at the repo ROOT** | — |
| `docs/thesis/THESIS_PROGRESS_2026*.md`, `docs/thesis/THESIS_SUMMARY_2026_VI.md` | deliverable write-ups (EN + VI) | writing the thesis, not running the pipeline |
| `docs/feature_groups.md` | canonical feature taxonomy | naming a feature group |
| `vn30.csv` / `vn100.csv` | index membership — ⚠️ **current, not point-in-time**; ⚠️ **repo ROOT, they are data not docs** | never as a historical universe |
| **[docs/INDEX.md](docs/INDEX.md)** | ⚠️ **the auto-loaded map** — all 127 `.md` files routed with a measured token cost each, in four tiers. `CLAUDE.md` pulls it in via `@docs/INDEX.md`; `python docs/check_index.py` fails if a doc is unrouted | before opening ANY file below, and whenever you add one |

⚠️ **`TODO.md` absorbed `src/orchestration/todo.md` on 2026-08-17** (28 items, Vietnamese).
If an older message or `CONTEXT.md` points at that path, it is a history reference —
`git show 6059c183^:src/orchestration/todo.md` is the file.

**Working preferences** (test.py usage, notebook DataFrame display, the paper-analysis
workflow, log truncation) live in the auto-loaded memory index and are not duplicated here.

---

## 8. Conventions that hold across the repo

- ⚠️ **BEFORE YOU COMMIT, RECORD THE STATE.** Run **`python docs/state_check.py`** and
  resolve what it reports. A commit that changes what this project KNOWS must also change
  where that knowledge is read: a new measurement lands in `CLAUDE.md` (§6 "State today",
  and its date is bumped) or in the package's own `CONTEXT.md`; a new defect gets a
  permanent code in `docs/ISSUES.md`; a finished item leaves its number behind and is
  deleted from `docs/TODO.md`; a new `.md` file gets a row in `docs/INDEX.md`. ⚠️ **The
  script REPORTS and never rewrites** — the counts here are a SCAN, not a decrement
  (`ISSUES.md` keeps four fixed rows struck-through inside its Open table, so a row-counter
  returns 17 where the truth is 16), and a confidently wrong number is worse than none.
  ⚠️ **Nothing enforces this at commit time by choice** (2026-08-22): no git hook, so
  running it is the discipline. `RUNBOOK.md` §8c is the procedure.
- **`⚠️` marks a claim that cost something to learn.** Do not strip them; add them when you
  measure a new one.
- **Dates on findings, always.** A number without a date cannot be told from a stale one.
- **Record what was measured, not what was concluded** — the tables in these files are
  reproducible checks, which is why they are still trusted months later.
- **Nothing in `feature_selection` writes to the database**; `final_features` is the only
  stage that does. That boundary is enforced by the package split, not a comment.
- **Notebooks named `RUN__*.ipynb` are meant to be run.** Everything else (`study_*`,
  legacy `lstm_*`) is a finished write-up kept for the record.
- **A run folder is immutable.** Re-scoring rewrites metrics from `predictions_*.csv`;
  editing a built dataset's `metadata.json` in place is how a folder stops describing its
  own tensors.
- **git**: `src/model/runs/*/` and `src/train_test_set/` are ignored (only `index.csv` is
  tracked), so a fresh checkout has 27 runs stripped to `results/`. `reports/feature_selection/`
  IS tracked. `raw_data/` is ignored except `raw_data/cafef/financials/`.
