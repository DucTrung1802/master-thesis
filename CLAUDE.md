# CLAUDE.md — master-thesis

@docs/INDEX.md

> **ONE FILE, WHOLE PROJECT.** This is the map. The twelve `CONTEXT.md` files are the
> evidence behind it (**~212k tokens** — re-measured 2026-08-27; the ~227k this line carried
> since 2026-08-22 was counted over a wider set) — **open one only when you touch that package**, and §7 says
> which. Hub written 2026-08-10 against the state at commit `fcac8904`.
>
> ⚠️ **ALL PROSE DOCUMENTATION LIVES IN [`docs/`](docs/) SINCE 2026-08-22**, and the line
> above this paragraph is why: `@docs/INDEX.md` imports the documentation map into every
> session automatically. **[docs/INDEX.md](docs/INDEX.md) routes all 127 `.md` files with
> a measured token cost each** — read it before opening anything, and add a row to it when
> you write a new doc (`python docs/check_index.py` fails if you forget). ⚠️ **The corpus
> is ~583k tokens (re-measured 2026-08-27), ~3× a context window, so it can never be bulk-loaded** — the index is
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
takes `raw/cafef_pdfs` (⚠️ **784 tickers since 2026-08-23, was 100** — and with NO year
window that is **~555 GiB**), `raw/trading_view@stocks` (777 tickers, ~10 h) and
`raw/cafef_financials` (~2.4 h/ticker). Materialise a group or a named asset.

⚠️ **AND EVERY SCRAPE RUNS THROUGH DAGSTER — never a script, never ad-hoc code**, even for
a one-off backfill and even when the scraper class already has a batch method that would
do it. A run outside Dagster leaves no materialisation, no metadata and no partition
status, so a later session cannot tell what was fetched or with what scope. If a run needs
a knob the asset does not have, **the work is adding the knob to the asset** (a `Config`
class), not writing a wrapper around it. `RUNBOOK.md` §3e.

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
    renders as a box. ⚠️ **AND A ONE-OFF EDIT SCRIPT THAT `print`s THE TEXT IT IS EDITING
    DIES ON THE PRINT, HALFWAY THROUGH** — measured three times in one session on
    2026-08-23, each time after some replacements had been made and before the file was
    written, so the edit was silently partial. Start such a script with
    **`sys.stdout.reconfigure(encoding="utf-8")`**, or print counts rather than content.
    The write is the work; the print is the thing that kills it.
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

**⚠️ WHERE A NUMBER IS ALLOWED TO COME FROM** (added 2026-08-24, a standing DECISION,
not a measurement)

24. ⚠️ **FINANCIALS COME FROM THE FILING PDF AND FROM NOTHING ELSE. EVER.** Every
    balance-sheet, income-statement and cash-flow value must be OCR-parsed out of the
    company's own filed PDF in `raw_data/cafef/pdfs/`. **An HTML tab, a JSON endpoint, a
    web table or any other transcription is FORBIDDEN as a source** — not as a fallback,
    not "for the quarters OCR cannot read", not to fill a gap. **A quarter with no
    readable PDF is recorded as `missing`, and `missing` is the correct answer.** A
    transcription is somebody else's parse of the document, with their rounding, their
    omissions and their sentinels (CafeF's "not reported" is a literal `-1`, which reads
    as −1 dong in a column of billions), and once it is in the table nothing downstream
    can tell it from the filing. ⚠️ **THE CODE STILL DISAGREES WITH THIS RULE**:
    `CafefFinancialsBuilder` takes `use_api: bool = True` (`cafef_financials.py:485`,
    `:1629`) and its `from_api` docstring argues the opposite in as many words — *"This is
    not a lesser source … it is a BETTER one"*. That default is why **34 report-rows on
    disk today carry `source='cafef'`** rather than `pdf` (`FIN-1`). ⚠️ The `source`
    column of `bronze.cafef_financial_reports` is what makes this auditable at all —
    **keep it, and read it before quoting any fundamental.**

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

## 6. State today (2026-08-31)

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
`pipeline/CONTEXT.md` §1a-bis.

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
`pipeline/CONTEXT.md` §1a-bis; `ISSUES.md` `DEP-1`.

### ✅ 6-2-septies. THE FILING ARCHIVE — 784 TICKERS, PHASE 1 DONE 2026-08-23 in 74 min

`P2`'s input exists now for every listed code up to and including 2020. It had sat open on
a number that was never measured, and measuring it is most of the story.

**The count came before the download, and cost 123 seconds.** `FileBCTC.ashx` lists a
ticker's documents without serving one, so the universe can be SIZED without fetching a
byte — 784 codes, 0 errors, every ticker returns filings:

| | |
|---|---|
| universe | **784 codes · 84,076 PDF documents** |
| size model | **7.02 MB/doc**, from the 15,217 PDFs already on disk whose index CSVs record `bytes`; rising 2.75 MB (2008) → 9.32 MB (2025) |
| whole corpus | **≈ 555 GiB** — not the ~700 GB carried since 2026-08-22 |
| **phase 1 (`≤2020`)** | **50,382 docs ≈ 286 GiB** |
| phase 2 (`2021+`) | 33,694 docs ≈ 269 GiB |

**The run**: one Dagster run, `--partition-range HNX_ADC...UPCOM_XMC` with
`year_max: 2020`, **74 minutes**, 784/784 materialisations, 0 errors. On disk now
**56,351 PDFs / 366.8 GiB**; `D:` (extended 318 → 636 GiB) went 461 → **197 GiB** free.

⚠️ **THE COST IS THE YEAR WINDOW, NOT THE TICKER COUNT — and that inverts `P2`.** The item
had read *"choose N tickers and justify it"* for as long as it existed, on the belief that
the universe could not fit. The universe fits; **every filing year at once** does not.
Nothing about the ticker list was ever the constraint.

⚠️ **AN EXTRAPOLATION FROM THE EXISTING SAMPLE WAS WRONG BY 2×, AND THE REASON IS NOT
"SMALL SAMPLE".** The 112 tickers on disk are **96 % HOSE**, and their 5 non-HOSE members
average 52 MB — which read as *"HNX/UPCOM are ~18× smaller"* and gave 240 GiB. Those five
are **partial scrapes, not small companies**: `HNX_AMV` held 9 files where CafeF lists
**160**, `UPCOM_CMT` held **1 of 112**. Measured properly, HNX averages 102 docs/ticker and
UPCOM 75 against HOSE's 134. *"The sample is small"* and *"the sample is a different
thing"* are different failures and only the first is fixed by more data.

✅ **VERIFIED PER TICKER AGAINST THE PRE-RUN COUNT, not off the green run** (§5 rule 10):
**50,345 of 50,382** expected documents landed. The 37 absent ones are **CafeF's dead
links** — sampled 6 across 3 tickers, all **404 on BOTH hosts**. ⚠️ **The first check said
4 missing on one ticker and 2 of them downloadable; it was matching by FILENAME**, which
misses documents stored under the collision-hash suffix. Matching by URL is the honest
comparison and gives 2, both dead.

**Three defects the work exposed, all measured:**

1. ⚠️ **`link.endswith(".pdf")` was silently skipping 1,408 of 84,076 documents (1.7 %)** —
   CafeF appends a cache-buster (`…_31072026105556.pdf?v=1785470157744`), VCB's own
   Q2-2026 filing among them. Fixed by testing `urlsplit(link).path`.
2. ⚠️ **A dead link vanished without a word** — 37 documents, and the log carried **0
   warnings**. A ticker missing files and a complete one looked identical (§5 rule 22 at
   the document). The count is now in the summary line and a WARNING names the first three.
3. ⚠️ **The year-scoped index merge had never been run.** `years` had existed for months
   and was never passed; the first scoped run over an existing index raised `int + str`
   from `csv.DictReader`. The quiet half would not have raised: **`"False"` is truthy**, so
   `consolidated` and `half_year` would have counted every carried row.

⚠️ **AND THE PHASE BOUNDARY HAD TO BE MADE LEAKPROOF ON PURPOSE.** CafeF files 10 of the
84,076 documents with a `Year` that is not a year (eight `0`, one `202`, one `203`).
`year_max` keeps them, `year_min` does not — so the two phases partition the corpus exactly
and no document falls between them. Verified after the run: the 5,865 index rows dated
2021+ sit in exactly the 112 tickers that were already complete, and the merge preserved
them rather than erasing them. **21 tests** pin both pure decisions without a network.

⚠️ **THE SCRAPE RUNS THROUGH DAGSTER AND ONLY THROUGH DAGSTER** (2026-08-23, standing
rule). `raw/cafef_pdfs` is partitioned over all 784 codes and carries
`BackfillPolicy.single_run()` — **not a speed tweak: it is the only way the concurrency
exists.** One partition per run would pay ~19 s of process start-up each (~4 h over the
universe) *and* scrape one ticker at a time, discarding the scraper's 12-way thread
manager. A run outside Dagster leaves no materialisation, no metadata and no partition
status. If a run needs a knob the asset lacks, **the work is adding the knob to the
asset**. `RUNBOOK.md` §3e.

⚠️ **What this does NOT buy.** Phase 2 is unrun and should stay unrun until the OCR has
been through phase 1 — `D:` has 197 GiB free against phase 2's ~269 GiB, so the disk
question returns exactly when it is supposed to. And **none of this touches the schema
wall**: 761 of 781 names are not banks, so these PDFs feed a parser that has never once
been run against a corporate filing (`P5`). ⚠️ **The clause *"and the non-bank template
still does not exist"* stood here until 2026-08-25 and was WRONG** — all four charts of
accounts exist; the blocker is `TPL-1`, seven bank-shaped reconcile anchors.
§6-2-quaterdecies.

### ⚠️ 6-2-octies. WHAT HAD ACTUALLY BEEN PARSED ON 2026-08-24 — 2 TICKERS, and 34 rows came from HTML

Measured 2026-08-24, three independent ways that agree: `statements/` on disk holds one
family (`bank`) with two files per report; `bronze.cafef_financials_bank_*` holds **152
rows / 2 tickers**; `gold.stocks_financials_bank_fa` holds **2 tickers**. **The parsed
universe is `HOSE_ACB` and `HOSE_VCB` and nothing else** — 782 of 784 tickers have PDFs on
disk (55,996 files) and **zero** parse output. ⚠️ **BID JOINED THEM ON 2026-08-25 (§6-2-quindecies),
so the parsed universe is THREE**; everything else in this section is the 2026-08-24 state and
the `FIN-1` bill it describes was settled the same day.

⚠️ **BUT "PARSED" IS NOT THE SAME AS "PARSED FROM A PDF", AND `bronze.cafef_financial_reports.source`
IS THE ONLY PLACE THAT CAN TELL YOU.** Per §5 rule 24 the PDF is now the only permitted
source, which makes this a bill rather than a footnote:

| ticker | quarters | report-rows | from **PDF** | from **HTML** (`cafef`) | `missing` |
|---|---|---|---|---|---|
| ACB | 74 (Q1-2008 → Q2-2026) | 222 | **195** | **27** | 0 |
| VCB | 78 (Q4-2006 → Q1-2026) | 234 | **209** | **7** | **18** |

⚠️ **THE FALLBACK FIRES ON ANY ABSENT PERIOD WITHOUT CHECKING WHETHER A PDF EXISTS**, so a
document the OCR merely failed on is filled from the web instead of being retried or
recorded as `missing`. That is the defect. ⚠️ **But only FOUR of the 34 rows can actually
be retried, and all four are VCB** — a correction to a claim made earlier the same day
from the PDF index alone.

⚠️ **`documents()` KEEPS `consolidated == "True"` AND NOTHING ELSE, AND ACB FILED NO
CONSOLIDATED STATEMENT BEFORE 2010.** Counted from `raw_data/cafef/pdfs/index/`: ACB
2003/2004/2007/2008/2009 are **parent-company-only** (`cons=0`), consolidated filings start
in 2010 with 5. So `FinancialsBuilder.documents("HOSE","ACB")` returns **65 documents
covering 2010-2026**, and every one of ACB's 27 HTML rows sits in a year the parser cannot
see. VCB is different — it has a consolidated filing from **2006** (1/yr for 2006-08, 5 in
2009), so `documents()` returns **72 covering 2006-2026**.

| | ACB | VCB |
|---|---|---|
| years with a document the parser will open | 17 (2010-2026) | 21 (2006-2026) |
| skipped by `skip_existing=True` (all `pdf`) | **17 — all of them** | 17 |
| **re-opened** | **0** | **4 — 2006, 2007, 2008, 2009** (7 documents) |
| HTML rows that get a real retry | **0 of 27** | **4 of 7** |

**The 4 retryable rows: VCB 2008Q4 IS, 2009Q1 BS, 2009Q2 BS, 2009Q2 CF.** Three `missing`
rows are retried alongside them (2006Q4 IS + CF, 2007Q4 IS). The remaining 30 rows have no
document the parser will open — ACB's 27, and VCB's 2008Q3 (all three, no Q3-2008 filing
exists at all). ⚠️ **Reaching ACB's 27 means deciding to accept a PARENT-ONLY statement
when no consolidated one exists — a change of which ENTITY the numbers describe, not a
parser fix.** `FIN-1`.

### ⚠️ 6-2-nonies. A THIRD WALL IN FRONT OF THE OCR PROGRAM — 273 TICKERS FILE NO CONSOLIDATED STATEMENT

Measured 2026-08-24 over all 784 PDF index files, no OCR and no network. `P5`'s framing —
*"the non-bank template decides whether `P6` reaches 20 names or 784"* — is **incomplete**,
and this is the missing term.

`FinancialsBuilder.documents()` keeps `consolidated == "True"` and nothing else. Counted
across the whole archive:

| | consolidated only (the rule until today) | with a parent fallback |
|---|---|---|
| documents the parser OPENS | **13,912 of 55,998 on disk — 24.8 %** | **26,280 — ×1.89** |
| **tickers that yield NOTHING AT ALL** | **273 of 784 (34.8 %)** | **22** |
| ACB | 65 | **73** |
| VCB | 72 | **75** |

⚠️ **THIS IS NOT A COMPANY FILING BADLY — IT IS A COMPANY WITH NO SUBSIDIARIES.** A
single-entity issuer files no `hợp nhất` report at all, so its standalone statement is not
a lesser version of a consolidated one, **it is the only statement that exists and it IS
the company**. `HNX_ADC` files 51 documents, every one standalone; `HNX_BAX` 30, likewise.
Reading them is a correction, not a loosening.

✅ **`allow_parent` SHIPPED THE SAME DAY, OFF BY DEFAULT**, on
`FinancialsBuilder.documents/build` and on the Dagster asset's `FinancialsConfig`.
**Consolidated still wins wherever both exist** — the entity is the FIRST sort key, ahead
of assurance, so no quarter may change entity to buy a better-assured document. ✅ Verified
across all 784 tickers: **0 of 13,912 consolidated periods move.**

⚠️ **AND THE VERIFICATION CAUGHT A REAL DEFECT ON ITS FIRST RUN — `best.update(annual)`
MOVED 86 OF 13,912.** The line implementing *"the audited annual stands in for Q4"* was a
bare `dict.update`, correct while both sides were consolidated-only and **silently
entity-changing** once they were not: a STANDALONE annual displaced a CONSOLIDATED Q4
quarterly, buying a better-produced document with a change of which company the row
describes. The merge now compares the entity rank first. **10 tests** pin both invariants
without a network (`test_cafef_financials_documents.py`).

⚠️ **EVERY ROW NOW CARRIES A `consolidated` COLUMN**, and that is the load-bearing half:
two entities in one column with nothing saying which is which is the same defect as
sourcing a figure from a web tab (§5 rule 24). It is **blank for a `missing` row** — no
filing was read, so no entity was chosen, and defaulting it to `True` would assert a fact
about a quarter nothing was parsed for.

⚠️ **IT ROUGHLY DOUBLES THE OCR BILL** (13,912 → 26,280 documents), which is why it is
opt-in. ⚠️ **And it does NOT touch `P5`**: these are still 761 non-bank names against one
`bank` template. The two walls are independent and both must fall for `P6` to reach the
universe.

### ⚠️ 6-2-decies. THE VCB REPAIR RAN, AND IT RECOVERED NOTHING — the quarters do not fail on OCR

Run 2026-08-24 through `raw/cafef_financials` with the new `periods` knob: VCB Q4-2006,
Q4-2007, Q4-2008, Q1-2009, Q2-2009. **45.5 min, 0 errors, and 0 of 234 cells changed.**

| document | winning layers | wall |
|---|---|---|
| Q4-2006 | BS + IS `onnx@200`, **CF absent** | **29.7 min** |
| Q4-2007 | all three `onnx@200` | 2.9 min |
| Q4-2008 | all three `onnx@200` | 2.7 min |
| Q1-2009 | IS **`onnx@200+loose`**, CF `onnx@200`, **BS absent** | 9.4 min |
| Q2-2009 | IS `onnx@200`, **BS + CF absent** | 0.8 min |

⚠️ **THE COST OF A DOCUMENT IS BIMODAL AND THE RATIO IS 10×, measured on two consecutive
filings of one ticker.** `_parse_cascaded` breaks only when **all three** statements are
accepted (`cafef_financials.py:378`), so one unresolvable statement makes the other two pay
the whole 21-layer cascade: Q4-2006 took **29.7 min** with both its readable statements won
at layer 1, against Q4-2007's **2.9 min**. The driver is `document size × number of
distinct OCR passes` (the cascade caches per engine/dpi/crop/join/title/loose, so 21 layers
collapse to ~10 passes) — which is why Q2-2009 cost 47 s with TWO statements absent: it is a
small filing.

⚠️ **THIS DOMINATES THE `P6` ESTIMATE.** Over 26,280 documents the failure RATE, not OCR
speed, is the whole number — 10 % of documents carrying one unreadable statement is
~1,300 h on its own. And the rate for a non-bank filing has never been measured (`P5`).

#### ⚠️ FIVE NEW PARSE CONFIGS WERE TRIED AND **NONE IS KEPT** — the failure is arithmetic

`LAYERS` has no combination of `title_over_form` with `loose_form_code`, and no
`loose_form_code` above 200 dpi, so five genuinely untried configs were probed against the
three unresolved statements:

| new config | Q1-2009 BS | Q2-2009 BS |
|---|---|---|
| `onnx@200+title+loose` | no total assets | assets != liabilities + equity |
| `onnx@300+loose` | no total to balance against | assets != liabilities + equity |
| **`onnx@400+loose`** | **assets != liabilities + equity** — the furthest any config gets | assets != liabilities + equity |
| `onnx@{200,300,400}+loose+pad6` | unchanged | unchanged |
| `onnx@400+loose+pad3` | unchanged | unchanged |

**Not one accepts, so not one is added.** The file's own design rule is that a layer
recovering zero quarters is pure cost, and these are the most expensive layers there are.

⚠️ **AND THE REASON THEY CANNOT WORK IS THE POINT: THE DIGITS ARE BEING READ CORRECTLY.**
The totals are stable across 200/300/400 dpi and every crop setting, and the gap is an
accounting one:

| | Q1-2009 | Q2-2009 |
|---|---|---|
| TOTAL ASSETS | 220,493,455,515,290 | 215,651,790,234,750 |
| **A − (L + E)** | **9,887,489,311,808 — 4.48 % of A** | **9,333,624,197,467 — 4.33 % of A** |

A stable ~4.4 % shortfall in both quarters, at every resolution. ⚠️ Q1-2009 also shows
`TOTAL RESOURCES` mapped to the **equity** figure — the exact fuzzy-match hazard the
`SCHEMA_MATCH = 0.80` comment documents (*"TỔNG VỐN CHỦ SỞ HỮU scores 0.75 against TỔNG NỢ
PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU"*). **The lever is the SCHEMA MAPPING for the 2009-era
consolidated VAS bank presentation, not a `ParseLayer`** — which puts it beside `P5`, not
inside `P6`.

#### ⚠️ AND `periods` HAS A HAZARD THE DOCSTRING DID NOT PREDICT: A SUBSET RUN CAN FAIL A QUARTER A FULL RUN WOULD ACCEPT

VCB **Q2-2009 cash flow** reconciles CLEANLY on its own — `reconcile` and `sane` both return
`None` at `onnx@200`, with and without the previous quarter's closing balance as `open_ref`
— and the run still logged it `absent`. By elimination the difference is `history`:
`sane` judges magnitude against the quarters accumulated **in this run**, and a run holding
only 2006-2009 Q4 annuals plus two 2009 quarters gives it a misleading neighbourhood.

⚠️ **So the known weakness is worse than recorded.** `build`'s docstring says a subset run
makes `sane` *"fail open"*; measured here, a subset run can also make it **fail CLOSED** —
rejecting a statement the document supports. **Use `periods` to probe, never to produce.**
An authoritative grid still needs `skip_existing=false` with no `periods`.

⚠️ **A merging run also cannot DELETE a stale row** — it upserts only what it produced, so
all 7 VCB `cafef` rows survived a run that re-attempted their exact quarters and failed.
That confirms `FIN-1`'s note rather than contradicting it.

### ✅ 6-2-duodecies. ACB REBUILT PDF-ONLY — 27 HTML rows gone, 4 NEW pdf rows, 0 lost

Run 2026-08-24, `raw/cafef_financials` partition `HOSE_ACB`, `skip_existing=false`
`allow_parent=true` `period_min=Q1-2008`. **3 h 36 m, RUN_SUCCESS**, 69 documents.

| | before | after |
|---|---|---|
| `pdf` | 195 | **199** |
| `cafef` (HTML transcription) | **27** | **0** ✅ |
| `missing` | 0 | 20 |
| cells | 222 | 219 (Q2-2026 has no document at CafeF) |

⚠️ **THE FOUR NEW ROWS ALL CAME FROM `allow_parent`, AND NONE FROM A NEW OCR LAYER** —
Q2-2009 BS `onnx@200+relax`, Q3-2009 IS `onnx@200`, Q4-2009 BS and CF `onnx@200`, every one
`consolidated=False`. They are the first PDF-sourced rows this repo has gained in the whole
`FIN-1` effort, and what unlocked them was **a filter, not a parser**: ACB filed no
consolidated statement before 2010, so `documents()` had never opened those filings.

⚠️ **THE SIX NEW PARSE LAYERS WON NOTHING — 0 of 219 cells.** `onnx@400+loose`,
`onnx@300+loose`, `onnx@200+title+loose` and their `+relax` variants were added the same day
to fill real gaps in the cascade and not one of them accepted a statement. They stay because
they sit last and only a statement that defeated all 20 predecessors can reach them, **but
the honest line is that they have returned nothing so far.**

✅ **0 cells LOST and 0 cells changed LAYER** — all 195 pre-existing `pdf` rows reproduced on
the same layer they had before, which is what makes this build reproducible rather than
merely successful, and it is how `SAN-1` was confirmed fully repaired.

⚠️ **COVERAGE FELL, AND THAT IS THE POINT.** 27 transcribed cells became 20 honest `missing`
ones plus 4 real parses. §5 rule 24: a quarter no readable PDF can produce is `missing`.

### ✅ 6-2-terdecies. THE CARRY-UP — `FIN-1` CLOSED, and three defects stood in the way

Run 2026-08-24 straight after the two rebuilds. **`bronze/cafef_financials` →
`silver/cafef_financials_bank` → `silver/stocks_basic_financials_bank(_fa)` →
`gold/stocks_financials_bank_fa`**, all green, ~4 minutes end to end.

| | |
|---|---|
| **the rule-24 test** | `bronze.cafef_financial_reports`: **405 `pdf` / 24 `missing` / 0 anything else** ✅ |
| bronze statement tables | **143 periods** × 3, Q1-2008 .. Q4-2025 — equal to the CSVs, 0 stale |
| `gold.stocks_financials_bank_fa` | **8,669 rows** (was 8,265), to **2026-08-21** (was 2026-06-25) |

⚠️ **THREE DEFECTS BLOCKED IT, AND ONLY ONE WAS THE SESSION'S OWN:**

1. **`GLB-1` — `glob` was the MODULE, not the function, and all 11 call sites in
   `preprocessor.py` were broken.** `from glob import glob` at line 6 is overwritten by
   `from utils.enums import *` / `from utils.utils import *` at lines 35 and 37, because
   **neither declares `__all__`** and so `import *` carries their own `import glob` along.
   Pre-existing — the file was last touched at `82fea06` and is not in this session's diff.
   Fixed with a re-bind after the star imports; **the root cause survives** and a future star
   import re-breaks it. ⚠️ *`import *` from a module without `__all__` exports that module's
   IMPORTS, not only its definitions.*
2. **The new `consolidated` column was absent from `CAFEF_FINANCIAL_META_COLS`**, so it fell
   through to `line_cols`, was coerced to numeric, and raised `ValueError: Unable to parse
   string "False"`. ⚠️ **`DATA_COLS` (the writer) and `CAFEF_FINANCIAL_META_COLS` (the
   reader) are two halves of one contract with NOTHING enforcing the match.**
3. **`BRZ-1` — a row deleted at the source is never deleted from bronze.** The ingest
   UPSERTS, so after `period_min` removed 8 VCB quarters and ACB lost Q2-2026, bronze still
   held **152 periods against 143**. ⚠️ **§5 rule 11 in a sharper form**: that rule is about
   staleness, this is a **deletion that cannot propagate**, and no freshness check can see
   it — the row count goes UP and `MAX(date)` stays right. Worked around with `DROP TABLE`;
   **every `_ingest_bronze_*` in the repo has the same property.**

⚠️ **AND ONE DEFECT IS FIXED IN CODE BUT NOT IN THE DATA — 2 of 429 cells.** `_decumulate`
drops a cumulative Q4 income statement when the run lacks its Q1..Q3 priors, but it does not
clear `meta`, so the row reports `source='missing'` while still carrying the `document`, the
OCR layer and the **entity** of a parse that was thrown away: ACB Q4-2009 IS
(`consolidated='false'`) and VCB Q4-2008 IS (`consolidated='true'`). That is a fact asserted
about a quarter nothing was written for, which is exactly what the blank exists to prevent.
`_write` now takes provenance only from a period that produced a row.

✅ **AND THE TWO CELLS WERE CORRECTED WITHOUT RE-PARSING — the rule was REPLAYED over the
files already on disk**, which is worth recording as a technique rather than as a fix. A
5.5 h re-parse would have recomputed 429 cells to change 2, and the 427 others would have
been recomputed with the same code that produced them. Instead the six fields `_write` now
sources from `prov` were blanked wherever `source <> 'pdf'` — the identical predicate, applied
to the output instead of during it. ⚠️ **`publish_date`, `assurance` and the share counts were
NOT touched**: they are facts about the DOCUMENT, which `_write` keeps whether or not any
statement of it reconciled. **Dry run first, 2 rows; applied; re-run 0 rows (idempotent); git
diff 2 lines in 2 files; all six CSVs verified non-ragged**; re-ingested in 2 minutes.
Afterwards `bronze.cafef_financial_reports` holds **0 rows carrying provenance without having
produced one**, and `consolidated` reads ACB 195 true / 4 false / 20 null and VCB 206 true /
4 null — matching the CSVs exactly.

⚠️ **This is only legitimate because the repair is the CODE'S OWN RULE, not a judgement about
the data.** Replaying a deterministic post-condition over an artefact is not the same as
editing a figure by hand, and the difference is that this one is reproducible: running it
again changes nothing.

### ⚠️ 6-2-undecies. `SAN-1` — THE MAGNITUDE GUARD ADOPTED THE CORRUPTION AS ITS BASELINE

Created and fixed 2026-08-24 within the hour, by the `allow_parent` change two sections up.
It is the most reusable thing that happened that day and it is `DEP-1`'s lesson from the
other side.

`sane` is the guard that catches what reconciliation cannot — a units error, a cumulative
column — by comparing a statement's probe against `history[report]`, the probes of quarters
already accepted **in the same run**, using the **median** with a ±20× band. Turning on
`allow_parent` let ACB's **Q3-2009 STANDALONE** filing into the run. Its income statement
mapped **2 line items**, reconciled, and its probe became the **only** entry in
`history[income_statement]`.

**The median of one value is that value.** The band became `that/20 … that×20`, and
**ACB's Q1-2010 income statement — `pdf` at `onnx@200` for as long as the file has
existed — was rejected as a magnitude outlier.**

⚠️ **NOTHING RAISED.** The symptom is the word `absent` in a log line, which is exactly
what a genuinely unreadable document prints. It was caught only by diffing the run against
a **pre-run backup**, and the diff was run because a 12.5-minute quarter looked wrong, not
because anything reported a problem.

⚠️ **AND THE RUN WRITES NON-MERGING PROGRESS SNAPSHOTS**, so killing it left ACB's three
CSVs at **9 rows against 74**. Restored byte-identical from the backup, **0 rows lost** —
but *"take a backup before an authoritative run"* is now a rule, not a courtesy.

✅ **Fixed two ways, both structural rather than a threshold nudge:**

1. **`history` is keyed by ENTITY as well as report.** A standalone company is not the
   consolidated group; its profit and its balance sheet are legitimately smaller, so pooling
   them makes the band meaningless in both directions. This became reachable the moment
   `allow_parent` existed, because a ticker can now file standalone early and consolidated
   later — **exactly ACB** (standalone 2008-09, consolidated 2010+).
2. **`MIN_ITEMS_FOR_HISTORY = 8`.** A thin statement is still WRITTEN but may not become a
   REFERENCE, and a WARNING names its item count. **Accepting a statement and trusting it
   are different decisions**, and they had been the same decision.

**5 tests**, including one that reproduces the poisoning itself so the fix cannot be quietly
undone.

⚠️ **THE GENERAL LESSON, and it generalises past this repo:** `DEP-1` was a monitor that
blocked the repair path; this is a **validator that learns its baseline from its own input**
and therefore adopts the first corruption it sees as normal. **Ask of any guard that learns
from what it is guarding: what happens when the first thing it learns is wrong?** Here the
answer was *"it rejects everything correct that follows, silently"*.

### ⚠️ 6-2-quaterdecies. THE NON-BANK WALL IS NOT A MISSING TEMPLATE — it is SEVEN RECONCILE ANCHORS, and two of them LIE

Measured 2026-08-25 from the charts of accounts and the parser's own matching code — **no
OCR, no network, no GPU.** It corrects a claim this file, `TODO.md` `P5` and the 2026-08-22
data audit all carried, and none of the three had ever measured it.

**The claim that was wrong:** *"`raw_data/cafef/financials/statements/` holds one template
family, `bank`, so a corporate template does not exist in this repo."* ⚠️ **`statements/` is
the parser's OUTPUT.** It holds one family because one family has been RUN. What exists is:

| | |
|---|---|
| `financials/schema/` | **12 files, 871 rows** — `bank`, `corp`, `securities`, `insurance`, x 3 statements |
| `cafef_schema.TEMPLATES` | all four, reference tickers `VCB` / `FPT` / `SSI` / `BVH` |
| `detect_template()` | **fingerprints the filing's own chart of accounts**, never GICS — *"HVA sits in the securities industry group and files on the CORPORATE template"* |
| `schema_of` / `map_to_schema` | template is an ARGUMENT; any of the 12 loads |
| the bronze ingest | *"one wide table per (template, report) that has been parsed"* — `_bank` is a consequence, not a scope |

#### ⚠️ What IS bank-shaped: `C_ASSETS … C_CASH_CLOSE`, seven exact dict-key lookups

On a miss, `reconcile` and `sane._probe` fall through to `Statement.find` — substring, then
`SequenceMatcher >= NAME_MATCH = 0.85`. Replaying that exact algorithm over each chart's own
`as_printed` labels (**clean labels — the OPTIMISTIC case, before any OCR damage**):

| anchor | bank | corp | securities | insurance |
|---|---|---|---|---|
| `C_ASSETS` | ✅ canonical | ✅ canonical | ✅ canonical | ✅ canonical |
| `C_RESOURCES` | ✅ canonical | ✅ canonical | ⚠️ text, fuzzy **0.962** | ✅ canonical |
| `C_LIABILITIES` | ✅ canonical | ❌ both | ⚠️ text — hits the GRAND TOTAL | ❌ both |
| `C_EQUITY` | ✅ canonical | ✅ canonical | ✅ canonical | ✅ canonical |
| `C_PBT` | ✅ canonical | ⚠️ text | ⚠️ text | ⚠️ text |
| `C_NET_CF` | ✅ canonical | ⚠️ text | ❌ both | ⚠️ text |
| `C_CASH_CLOSE` | ✅ canonical | ⚠️ **WRONG ROW** | ❌ both | ⚠️ **WRONG ROW** |

⚠️ **THE CASH FLOW DOES NOT MISS — IT LIES.** `CASH_CLOSE`'s needle fuzzy-matches the
**OPENING** balance at **0.885 (corp)** and **0.902 (insurance)**, above the 0.85 threshold,
and `find` scans in statement order where `đầu kỳ` is printed BEFORE `cuối kỳ` — first hit
wins. So a corp or insurance cash flow would be **reconciled and magnitude-probed on the
opening balance labelled as the closing one**. A wrong figure, not a refusal, and nothing
raises.

⚠️ **`bank` IS PROTECTED ONLY BY AN ACCIDENT OF ORDERING** — its own opening line scores
**0.930** on the same needle, and the only reason it never bites is that the canonical column
is present, so the fallback is never reached. The defect has been latent in the bank path
since the fallback existed.

⚠️ **`securities` FAILS THE OPPOSITE WAY AND IS THEREFORE THE SAFE ONE**: opening 0.789,
closing 0.831, **both below** the threshold, so every securities cash flow is refused with
`no closing cash balance`. **A refusal is the correct failure mode; corp and insurance get
the dangerous one.** That asymmetry is the same one the `CASH_COMPONENT` comment already
records — *"a marker that is too narrow makes the sum fall SHORT and refuses a sound
statement (recoverable), while one that is too wide makes it OVERSHOOT"*.

⚠️ **AND CafeF's INSURANCE CASH-FLOW CHART HAS NO CLOSING-BALANCE LINE AT ALL** — it ends at
`HDTC_39 … đầu kỳ` and `HDTC_40` (FX effect). Even with the anchor fixed there is **nowhere
to store the figure**: that one is a schema repair, not a tuple edit.

⚠️ **Two further bank-only sets, both silent:** `_cash_flow_identity`'s `C_CASH_OPEN` /
`C_CASH_FX` / `C_FLOW_SECTIONS` are **0 of 3 present on every non-bank chart**, so the
relaxed-layer verification cannot run outside `bank`; and `TOTAL_ALIASES`' two recovery
columns exist in **no** non-bank chart, so the `_anchor` recovery has nowhere to write.

⚠️ **NOTHING ON DISK IS WRONG TODAY** — only ACB, VCB and BID are parsed and all three are banks — so
this is `TPL-1`, a wall in front of `P5`/`P38`, and **not** a defect in any number this repo
has published. ⚠️ **It also makes `P5` cheaper and MORE dangerous at once**: cheaper because
the charts of accounts are written, more dangerous because the failure it removes is a silent
wrong value rather than an error. TODO `P5` has the six-step repair; `ISSUES.md` `TPL-1`.

⚠️ **THE GENERAL LESSON, and it is `SAN-1`'s from a third side:** *"the folder holds one
family"* is a fact about OUTPUT that was read as a fact about CAPABILITY, and it stood
unchallenged for as long as it did because it was **plausible and never measured**. §5 rule 2
covers the absent null; this is the absent CHECK — an inference recorded in the same
typeface as a measurement.

### ✅ 6-2-quindecies. BID PARSED — the third ticker, and the OCR COST MODEL IS NOW A FUNCTION

Run 2026-08-25 through `raw/cafef_financials` partition `HOSE_BID`, `skip_existing=false`
`allow_parent=true`. **7 h 23 m, RUN_SUCCESS**, 62 documents, 70 quarters Q1-2009 .. Q4-2025.

| | ACB | VCB | **BID (new)** |
|---|---|---|---|
| cells `pdf` / total | 199 / 219 = **90.9 %** | 206 / 210 | **168 / 210 = 80.0 %** |
| `cafef` (HTML) | 0 | 0 | **0** ✅ |
| entity | 195 cons + 4 parent | 206 cons | **60 / 57 / 51 all `consolidated=True`** |
| dominant layer | `onnx@200` | `onnx@200` | `onnx@200` (52 of 60 BS) |

✅ **Rule 24 holds on the third ticker with no special handling** — `use_api` now defaults to
`False`, so the run produced **0 rows from any HTML tab** without anyone having to remember.
✅ **ACB and VCB were verified byte-identical against a pre-run backup** — 6 of 6 CSVs — which
is the check `SAN-1` exists to force and the only one that would have caught it.

⚠️ **`allow_parent` CHANGED NOTHING FOR BID — 62 documents either way**, because BID files a
consolidated statement for every period it files at all. That is the opposite pole from
`TPB` (9 consolidated against 55 with the fallback, a 6.1x difference), so the flag's value
is a property of the ISSUER and cannot be budgeted from an average.

⚠️ **8 QUARTERS LOST ALL THREE STATEMENTS AND NONE OF IT IS AN OCR FAILURE** — Q1-Q3 2009,
Q1-Q3 2010, Q1-Q2 2011. Counted from the PDF index, **BID filed only an ANNUAL report for
2008, 2009 and 2010** (1 document each) and its first quarterly is Q3-2011; 12 documents a
year start in 2012. `missing` is the correct answer and 80.0 % is the wrong denominator:
excluding those 24 unfileable cells the ticker is **168 / 186 = 90.3 %**, level with ACB.

#### ⚠️ THE COST IS 7.15 MIN/DOCUMENT, 3x THE FIGURE `P38` WAS BUDGETED ON — and the driver is now measured

`P38` estimated the VN30 batch at ~63 h from **2.37 min/document**, the ACB+VCB average.
BID came in at **7.15**. §6-2-decies had already named the mechanism — `_parse_cascaded`
breaks only when **all three** statements are accepted, so one unreadable statement makes the
other two pay the whole 21-layer cascade — and three tickers now turn that into a line:

| ticker | quarters needing the full cascade | min/document |
|---|---|---|
| VCB | 3 of 70 = **4 %** | **1.63** |
| ACB | 8 of 73 = **11 %** | **3.13** |
| **BID** | **25 of 70 = 36 %** | **7.15** |

**min/doc ≈ 0.94 + 0.173 x (% of quarters with at least one unreadable statement)** — it
predicts ACB at 2.84 against a measured 3.13. ⚠️ **THREE POINTS, SO THIS IS A SHAPE AND NOT A
FITTED MODEL**; what it establishes is that **OCR speed is not the variable — the FAILURE RATE
is**, exactly as §6-2-decies predicted before there was a second point to test it on.

⚠️ **RE-BUDGET `P38` AND `P6` ON IT.** At BID's rate the 1,598 remaining VN30 documents are
**~190 h**, not 63; at the three-ticker mean failure rate (17 %) they are ~103 h. ⚠️ **And the
failure rate for a NON-BANK filing has never been measured at all** — `TPL-1` says two of the
three non-bank templates cannot even reconcile a cash flow, which would put their rate near
100 % and the cascade cost at its ceiling. **`P5` is therefore a COST item as well as a
correctness one.**

#### ⚠️ WHY BID'S 13 STATEMENTS FAILED — probed 2026-08-25, and the guards were RIGHT

11 of the 13 are CASH FLOW and 2 are balance sheets; no income statement fails on OCR at all.
Separately, **8 quarters lost all three because BID filed no quarterly report before 2012** —
absent documents, not failures. Probing the two cheapest cash flows found two different causes
and **no defect**:

| quarter | what the cascade did | verdict |
|---|---|---|
| **Q3-2016** | read closing = **55,968,854,000,000** — equal **to the đồng** to Q1-2016's already-accepted closing | ✅ `sane` refused it: *"probe exactly equals an already-accepted quarter (comparative column read as the current one?)"* |
| **Q1-2012** | OCR merged the labels (`nop_trong_ky_luu_chuyen_tien_thuan_...`); no row matches the closing balance at all | ✅ `reconcile`: *"no closing cash balance"* |

⚠️ **THIS IS THE FIRST FIELD EVIDENCE THAT `sane`'s EQUALITY GATE CATCHES A REAL ERROR.** Until
now its only witness was ACB's Q4-2022 in a docstring. Two quarters agreeing on a 14-digit
figure to the last unit is not something a going concern does, and here it was a whole
statement taken from the wrong column.

⚠️ **AND NO LAYER READS Q3-2016 CORRECTLY** — all 26 were tried: `onnx@400` gives 100 bn,
`tesseract@200` gives 0, and the two `+relax` layers give **53,361 bn and 53,261 bn**, which
disagree with EACH OTHER by 100 bn and are refused for an unmapped FX line. The true figure is
~55,261 bn (Q2-2016 is 59,066, Q1-2017 is 65,522). **`missing` is the correct answer**, and it
is rule 24 working rather than failing.

⚠️ **A PROBE THAT OMITS ONE LAYER PARAMETER INVERTS ITS OWN CONCLUSION — measured the hard
way.** A first probe set `crop_pad=0` where `ParseLayer("onnx@200", "onnx", 200)` leaves it
`None`, read a closing of 13,161 bn, saw `reconcile` and `sane` both pass, and concluded that
`sane`'s ±20x band was too wide to catch a 4.5x error. **All of that came from a layer that
does not exist**: at the real setting the same document reads 55,969 bn and is refused. The
cascade already keys its parse cache on `(engine, dpi, crop_pad, join_digits, title_over_form,
loose_form_code)` precisely because *"two layers that share an engine and DPI but crop
differently produce different text"* — a probe claiming to describe real behaviour must rebuild
**every** one of those, not the two obvious ones.

#### ✅ AND THE LOG NOW SAYS WHY, WHICH IS THE ONLY CHANGE SHIPPED

Recovering the reason above took **four probe runs**, because `_parse_cascaded` computed a
refusal reason at every layer and threw all of them away — the sole trace was the word `absent`
in the period line, which is exactly what a filing with no such page prints. That is
§6-2-undecies' complaint restated, and it is how `SAN-1` came to be found by diffing against a
backup rather than by reading a log.

`_parse_cascaded` now keeps each refusal and prints the DISTINCT reasons, each attributed to the
first layer that gave it:

```
    cash_flow absent after 26 layer(s):
      [onnx@200]        sane: probe 5.6e+13 exactly equals an already-accepted quarter ...
      [onnx@400]        sane: magnitude 1e+11 vs typical 4.16e+13 ...
      [tesseract@200]   sane: magnitude 5.51e+07 vs typical 4.16e+13 ...
      [onnx@200+relax]  reconcile: cash flow unverifiable — fx not mapped
      [onnx@300+relax]  reconcile: cash flow unverifiable — opening, fx not mapped
```

⚠️ **THE SHORT-CIRCUIT IS PRESERVED EXACTLY** — `sane` still runs only where `reconcile` passed,
so no OCR pass, no gate, no threshold and no accept ordering changes. Verified on Q3-2016:
`accepted` is `[balance_sheet, income_statement]` before and after. **Distinct reasons only**,
because 26 layers usually fail the same two or three ways and printing all 26 buries the one
that matters.

#### ⚠️ AND PROBING ALL 13 FOUND ONE BOTTLENECK — `FXM-1`, with a fix that is WRITTEN and UNMEASURED

Each of BID's 13 genuinely failed statements was put through the full 26-layer cascade and the
refusal reasons read off the log the change above added. **All 13 completed — 3 h 56 m — and
they do not spread out, they converge:**

| the LAST reason the cascade gave | periods |
|---|---|
| **`cash flow unverifiable — fx not mapped`** | **8** — 6 on that alone, 2 with `opening` beside it |
| `no closing cash balance` | 1 — Q1-2012 |
| **`cash flow does not close`** | 1 — Q4-2010 |
| `sane: magnitude 5.45e+08 vs typical 1.19e+14` | 1 — Q1-2026, **five orders out** |
| `no total assets` (a balance sheet) | 1 — Q1-2021 |
| `no total to balance against` (a balance sheet) | 1 — Q3-2025 |

⚠️ **THE THREE BALANCE-SHEET FAILURES AND Q1-2026 ARE A SEPARATE PROBLEM** and no FX change
touches them: two cannot find a total to balance against and one is out by five orders of
magnitude. **The FX bottleneck is 8 of 13, not 11 of 13.**

⚠️ **AND THE PROBE PRICED A FAILED DOCUMENT: 18.2 min against 4.2 for a clean one, 4.3x.**
13 documents took 236 minutes here; BID's whole run was 443 minutes over 62 documents, so the
49 that succeeded averaged 4.2. That is §6-2-decies' bimodal cost measured directly rather than
inferred from a failure share, and it is the number `P6` should be budgeted on.

⚠️ **THESE QUARTERS ARE NOT LOST TO UNREADABLE PIXELS.** In the 8, the relaxed layers **already
recover the opening and closing balances** positionally and are then refused for want of a
FOURTH TERM: `_cash_flow_identity` needs `opening + movement + fx == closing`, and `VI. Điều
chỉnh ảnh hưởng của thay đổi tỷ giá` does not reach `SCHEMA_MATCH = 0.80` on these scans. The
`fx = 0` stand-in already in the code cannot help — it applies only when `open_ + net == close`
to the đồng, which is exactly what is not true here.

⚠️ **AND Q4-2010 IS THE REASON NOT TO EXPECT 8 RECOVERIES.** It reached the identity with all
four terms and **still failed to close** — which is what a genuinely mis-read statement looks
like, and may be what several of the other 8 are. The gate works; the question is how many of
these quarters deserve to pass it.

**Two changes are written for this and NEITHER HAS TOUCHED REAL DATA** (TODO `P39` is the
measurement, and it carries a mandatory ACB + VCB regression):

1. **The FX line, recovered by POSITION.** The chart prints `HDTC_48` opening, `HDTC_49` FX,
   `HDTC_50` closing, adjacent; when the recovered pair sits **exactly two rows apart**, the row
   between them is claimed. ⚠️ **It loosens no gate** — it lets a gate that was skipping for
   want of a term actually run, and the identity then rejects a wrong row rather than writing
   it. This is the same positional guess the IV recovery beside it has always made, and the
   repo's own justification for that one applies unchanged.
2. **The unread ALTERNATE filing.** `documents` returns ONE document per quarter, so a quarter
   whose every layer refused it was recorded `missing` **while a second CONSOLIDATED filing of
   the same quarter sat on disk** — BID has 4 (Q4-2015, Q4-2016, Q2-2017, Q4-2017), each the
   unaudited quarterly beside the audited annual `documents` preferred. ⚠️ **The entity is
   FIXED, not preferred** (verified: 0 alternates change entity across BID's whole index), so a
   fallback can never quietly change which company a row describes; `allow_parent` remains the
   only way to a standalone filing. ⚠️ **Provenance follows the row** — `meta` takes
   `origin[report]`, not the chosen document, or the row would name a filing it did not come
   from, which is §6-2-terdecies' defect exactly. ⚠️ **An income statement from an alternate is
   REFUSED when the cumulative flag differs**, because `_decumulate` would subtract earlier
   quarters from a figure that never contained them.

⚠️ **Write nothing about what these recover until `P39` has run.** Six parse layers added on
2026-08-24 have returned **0 cells** so far; being well-argued is not evidence.

#### BID's COVERAGE MAP — 70 quarters x 3 statements, and only 13 cells are a FAILURE

`B` balance sheet · `I` income statement · `C` cash flow · `·` missing · `—` no such quarter

| year | Q1 | Q2 | Q3 | Q4 | | year | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|---|---|---|---|---|---|
| 2008 | — | — | — | `B·C` | | 2018 | `BIC` | `BIC` | `BI·` | `BIC` |
| 2009 | `···` | `···` | `···` | `B·C` | | 2019 | `BIC` | `BIC` | `BIC` | `BIC` |
| 2010 | `···` | `···` | `···` | `B··` | | 2020 | `BIC` | `BIC` | `BIC` | `BIC` |
| 2011 | `···` | `···` | `B·C` | `B·C` | | 2021 | `·IC` | `BIC` | `BIC` | `BIC` |
| 2012 | `BI·` | `BIC` | `BIC` | `BIC` | | 2022 | `BIC` | `BIC` | `BIC` | `BIC` |
| 2013 | `BIC` | `BIC` | `BIC` | `BIC` | | 2023 | `BIC` | `BIC` | `BIC` | `BIC` |
| 2014 | `BIC` | `BIC` | `BIC` | `BIC` | | 2024 | `BIC` | `BIC` | `BIC` | `BIC` |
| 2015 | `BIC` | `BIC` | `BI·` | `BI·` | | 2025 | `BIC` | `BIC` | `·IC` | `BIC` |
| 2016 | `BIC` | `BIC` | `BI·` | `BI·` | | 2026 | `BI·` | — | — | — |
| 2017 | `BIC` | `BI·` | `BI·` | `BI·` | | | | | | |

| | parsed | absent | rate |
|---|---|---|---|
| balance sheet | 60 | 10 | 85.7 % |
| income statement | 57 | 13 | 81.4 % |
| **cash flow** | **51** | **19** | **72.9 %** |
| total | 168 | 42 | 80.0 % |

⚠️ **THE 42 ABSENT CELLS ARE THREE DIFFERENT THINGS AND ONLY ONE IS A DEFECT:**

| | cells | what it is |
|---|---|---|
| **A · no document exists** | **24** (8 quarters x 3) | Q1-Q3 2009, Q1-Q3 2010, Q1-Q2 2011. BID filed only an ANNUAL report for 2008-2010; its first quarterly is Q3-2011 and 12 documents a year start in 2012. `missing` is the correct answer |
| **B · `_decumulate` has no priors** | **5** | the Q3/Q4 income statements of 2008-2011. An interim IS is CUMULATIVE and the quarter is what is left after subtracting Q1..Q(q-1) — quarters that do not exist here. **The PDF read fine**: the same documents yielded their BS and CF |
| **C · the PDF genuinely failed** | **13** | 11 cash flows + 2 balance sheets — the only cells a better parser could win |

**Excluding class A the ticker is 168 / 186 = 90.3 %, level with ACB.**

### ⚠️ RE-SCOPED 2026-08-26 — MEASURE BID FROM 2012, AND EVERY FAILURE LEFT IS A CASH FLOW

**BID files 12 documents a year only from 2012.** Before that it filed an ANNUAL report and
nothing else — 1 document each for 2008, 2009 and 2010, first quarterly Q3-2011 — so the table
above was measuring the FILING CALENDAR as much as the parser. ⚠️ **Re-enumerated live from
CafeF on 2026-08-26 and the absence is CafeF's, not our snapshot's**: 2011 lists exactly two
documents, and **Q1-2011 / Q2-2011 do not exist at all**. On the 2012 denominator:

| from Q1-2012 · 57 quarters | `pdf` | absent |
|---|---|---|
| balance sheet | **57 / 57** ✅ | 0 |
| income statement | **57 / 57** ✅ | 0 |
| **cash flow** | **47 / 57** | **10** |
| total | **161 / 171 = 94.2 %** | 10 |

⚠️ **THAT IS THE FINDING: with the filing calendar out of the denominator, BID's balance sheet
and income statement are COMPLETE, and every remaining failure in the ticker is a CASH FLOW.**
Eight of the ten end at `fx not mapped`, so `FXM-1`'s written-but-unmeasured fix targets
**169 / 171 = 98.8 %** on its own. TODO `P40`.

⚠️ **THE FLOOR IS A DENOMINATOR, NOT A `period_min`.** Measured: **9 real parsed `pdf` cells sit
before it** — five balance sheets and four cash flows, all from audited annual reports — and a
`period_min: Q1-2012` run DELETES them from the CSVs while `BRZ-1` leaves them stranded in
bronze. Quote the 2012 figure; keep the default floor.

⚠️ **AND THE B/C SPLIT ABOVE IS WRONG IN BOTH TERMS, INDEPENDENTLY OF THE RE-SCOPE.**
**Q3-2011's income statement is NOT a `_decumulate` drop**: its filing carries `half_year=False`
and `annual=False`, so `_decumulate`'s `if q == 1 or not half_year.get(period): continue` skips
it outright — it was refused by a gate. Class B is the four ANNUAL reports alone. Class C fell
13 → 11 as `SLD-1` and `PGB-1` recovered the two balance sheets. ⚠️ **The miscount stood because
*"the Q3/Q4 income statements of 2008-2011"* reads as one family and was never checked against
the code path** — an inference recorded in the same typeface as a measurement, §6-2-quaterdecies'
lesson again.

⚠️ **AND CafeF PUBLISHED Q2-2026 AFTER THE PARSE RUN.** Re-enumerating BID's index through
`raw/cafef_pdfs` on 2026-08-26 (3.7 s, RUN_SUCCESS) added **4 documents** — the parent and
consolidated Q2-2026 filings, each unaudited and reviewed, filed 2026-08-01 and 2026-08-20. The
index goes 176 → 180 rows and `documents()` **62 → 63**, picking the consolidated reviewed one.
⚠️ **A ticker's archive is not static once parsed**, and nothing in the pipeline notices: the
statement CSVs still end at Q1-2026 and no freshness check reads a filing index.

⚠️ **CASH FLOW IS THE EXPENSIVE STATEMENT EVEN WHEN IT SUCCEEDS.** Of the 51 that parsed,
**15 needed a layer past `onnx@200`** — nine distinct configurations including
`onnx@200+pad6+relax+components` — against 8 of 60 for the balance sheet and 4 of 57 for the
income statement. That is where BID's 7 h 23 m went.

⚠️ **AND CASH FLOW IS THE ONLY THING BREAKING CONTINUITY.** Longest unbroken runs:

| | with all three | **BS + IS only** |
|---|---|---|
| longest | Q2-2021 .. Q2-2025 (**17 quarters**) | **Q1-2012 .. Q4-2020 (36 quarters)** |
| next | Q2-2012 .. Q2-2015 (13) | Q2-2021 .. Q2-2025 (17) |
| next | Q4-2018 .. Q4-2020 (9) | — |

**A model that does not need the cash-flow statement gets 36 consecutive quarters from this
ticker, not 17** — so 80.0 % understates what is usable by a wide margin, and which statements
a feature actually needs is worth deciding before more OCR is bought.

### ⚠️ 6-2-sexdecies. `SLD-1` — A STATEMENT CAN BE PARSED ONE ROW OUT AND PASS EVERY GATE

Found 2026-08-26 while making BID Q1-2021's balance sheet parse, and it is the more important
half of that day's work — the recovery is the smaller one.

**The mechanism.** On some scans the OCR emits each numeric box a constant distance ABOVE the
text box of its own printed line: BID Q1-2021 by **7pt**, against `Y_TOL = 4.0`. The two never
group. `table_rows` emits a row only when it holds a figure and turns a label-only line into
`carry` — so a SECTION HEADING (`A. TÀI SẢN`, which the filing prints with no figure) leaves a
label waiting, and every figure is handed to the label ABOVE it. The whole statement slides by
one row **with every digit read correctly**. ⚠️ The existing recovery for exactly this shape
(the ACB Q1-2024 branch, *"a label sits BELOW its own figures"*) is disabled precisely when it
is needed, because its condition is `not words_` and a heading guarantees `carry` is not empty.

⚠️ **AND BOTH GRAND TOTALS CAN STILL LAND CORRECTLY, SO `reconcile` PASSES.** That is what
makes this a correctness issue rather than a coverage one: the row is written as `pdf`, the
totals are right, and every line item between them is its neighbour's.

**Verified against the filing's own text**, which is the only reason it is stated as fact: VCB
Q1-2021 page 4 prints `Tiền mặt, vàng bạc, đá quý` **12,277,634** and `Tiền gửi tại NHNN`
**24,008,360**. The row on disk holds **24,008,360** and **203,604,117** — each label carrying
the next line's figure.

#### The audit — 201 filings re-OCR'd, 69.9 min

| | |
|---|---|
| statement-rows measured | **596** across ACB + VCB + BID (571 `pdf`, 25 `missing`) |
| non-zero offset | **20** (3.4 %), of which **18 stored as `pdf`** |
| **benign** | **0** — re-pairing changes **2 to 22 line items in every single one** |
| convicted | **5** |

Judging the six flagged BALANCE SHEETS against the prior Q4 already accepted on disk — every
interim balance sheet prints 31/12 of the prior year beside the current column, so that column
must reproduce it:

| | stored | realigned | |
|---|---|---|---|
| BID Q2-2019 | 15/45 = **33 %** | 46/46 = **100 %** | ❌ |
| BID Q1-2022 | 26/44 = 59 % | 45/46 = 98 % | ❌ |
| VCB Q1-2021 | 31/47 = 66 % | 45/46 = 98 % | ❌ |
| BID Q2-2014 | 24/36 = 67 % | 47/52 = 90 % | ❌ |
| VCB Q1-2026 | 42/48 = 88 % | 56/56 = **100 %** | ❌ |
| VCB Q3-2023 | 47/51 = **92 %** | 52/61 = 85 % | ⚠️ inconclusive, probably sound |

⚠️ **FIVE IS A LOWER BOUND, NOT A TOTAL.** The other 12 are income statements and cash flows
whose comparative column is the prior YEAR's period — not a stored quarter — so this method
cannot judge them, and **3 of them sit in a filing whose balance sheet is already convicted**
(BID Q1-2021 changes 9 items, BID Q2-2019 13, VCB Q1-2026 5). Worst unjudged: **BID Q4-2024
income statement, offset 11.5, 22 items**.

⚠️ **OFFSET SIZE DOES NOT PREDICT DAMAGE**, and I assumed it did before measuring: BID
Q2-2019's income statement carries the SMALLEST offset in the whole list (2.0) and changes 13
items, inside a filing already proven broken.

⚠️ **TWO DISK-ONLY DETECTORS WERE TRIED FIRST AND BOTH FAILED**, which is why the audit costs
70 minutes of OCR: quarter-on-quarter continuity is blind to a slide that persists across
adjacent quarters (they are continuous with each other), and *sum of level-1 items vs the grand
total* is defeated by coverage — only 8-10 of 12 items are ever present, so VCB Q1-2021 (slid)
scored **−0.96 %** against Q4-2020 (sound) at **−20.2 %**, the wrong way round.

#### What shipped, and what it does NOT do

`realign_rows` is a `ParseLayer` flag (`PdfParser._value_row_offset`). The offset is chosen by
maximising CO-LOCATION — the number of lines holding both a label and a figure — a criterion
that never looks at what the figures ARE, so it cannot be pulled toward a total that happens to
reconcile; and it is discarded unless it beats the unshifted page by half again.
⚠️ **It returns the CENTRE of the maximal band, not its first point**: `Y_TOL` is a tolerance,
so a true 7pt offset reads as a flat maximum over 3..11, and taking the first scored 3.0 and
left every figure on the edge of the tolerance. That bug survived the real-data check (BID's
maximum is a single point at 7) and was caught only by a synthetic fixture.

✅ **BID Q1-2021 recovered** — `missing` → `pdf` at `onnx@200+realign`, total assets
**1,558,887,407 mn**, the figure printed on page 2. Written through Dagster, 12m36s, and diffed
against a backup: **only that period changed** in the balance sheet, **zero rows** changed in
the other two statements.

⚠️ **IT REPAIRS NONE OF THE 5.** The five layers sit LAST, so only a statement that defeated
all 26 earlier layers reaches them — and every convicted row reconciles at layer 1, which stops
the cascade. It can reach exactly **2 of the 25 `missing` statement-rows** (BID Q1-2021, done;
VCB Q1-2009, untried); the other 23 measure offset 0. Repairing the five needs a decision to
re-judge accepted statements, which is a different change with a much larger blast radius.

✅ **The default path is unchanged, and that is proven rather than asserted**: against HEAD on
the same OCR output with the flag off, **12 statements / 643 rows of ACB, VCB and BID filings
reproduce row for row**. Adding the recovered quarter to `history` changes no later verdict —
0 of BID's 19 subsequent quarters move. 7 tests, no PDF or network needed.

### ⚠️ 6-2-septdecies. AND `periods` WROTE GARBAGE THE SAME DAY — the warning is not decorative

BID Q3-2025's balance sheet was diagnosed first, at `onnx@200`: **22 pages classified as the
balance sheet, 316 rows**, and the anchors taken from a NOTE table — `tong_tai_san` =
**115,110 mn** for a bank whose real total is **3,071,970,196 mn**. ⚠️ **`reconcile` PASSES on
that**, because assets and resources are the same piece of garbage; **`sane` is the only gate
that refuses it.**

It was then run through Dagster with `periods: ["Q3-2025"]` anyway — the one mode that makes
`sane` fail open. **RUN_SUCCESS in 48.3 s, and 115,110 mn was written to disk as `pdf`**, along
with an overwrite of the Q3-2025 cash flow that was already `pdf`. Nothing warned. Restored
byte-identical from a backup taken beforehand.

⚠️ **This is §6-2-decies' rule firing exactly as written — *"use `periods` to PROBE, never to
PRODUCE"* — and the diagnosis that predicted it was sitting in the run's own config file.** The
same knob was safe for Q1-2021 only because that statement had first been checked against the
FULL 41-quarter history in a read-only probe, where `sane` returned `None`. **A `periods` run
may only write what a full-history probe has already cleared.**

⚠️ **Q3-2025 REMAINS UNPARSED and `realign` cannot help it** (measured offset **0.0**). It is
page OVER-INCLUSION — `_drop_islands` failing to prune 22 pages to 3 — a separate defect from
`SLD-1`. ⚠️ **And the reason recorded in §6-2-quindecies (`no total to balance against`) is the
LAST layer's**; `onnx@200` gets further and is stopped by `sane` instead.

⚠️ **THE EIGHT OTHER `missing` BALANCE SHEETS CANNOT BE PARSED BY ANY CHANGE**: Q1-Q3 2009,
Q1-Q3 2010 and Q1-Q2 2011 have **no filing at all** — BID filed only an annual report for 2008,
2009 and 2010, and its first quarterly is Q3-2011. `documents()` returns nothing for all eight.
`missing` is the correct answer, and §6-2-quindecies' class A already said so.

### ⚠️ 6-2-octodecies. `PGB-1` — BID Q3-2025, and a `periods` RUN WROTE GARBAGE TWICE

The last `missing` balance sheet in BID's grid that had a filing at all. Recovering it took
three fixes and three write attempts, and **the two failed writes are worth more than the fix.**

#### The parse — three defects, each hiding the next

| | |
|---|---|
| **1. no form code survives OCR** | `from_form = False` on all 37 pages, so `_drop_islands` — which prunes by distance from a form-coded page — returns early with nothing to measure from |
| **2. the notes are swept in** | pages 12-13 and 18-34 score against the balance-sheet title, `_fill_continuations` absorbs every numbered table after them: **22 pages / 316 rows**, and the anchors come from a NOTE table — `tong_tai_san` **115,110 mn** against a real **3,071,970,196 mn** |
| **3. a merged label answers the EQUITY anchor** | the filing prints "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" and "I. Các khoản nợ Chính phủ và NHNN" as one row; `_anchor` scored `row.key` RAW where `map_to_schema`'s ordered walk has always scored `_split_merged(...)`, so equity took the government-debt figure — 215,823,611, **7.0 % of assets** against BID's usual 5.2-5.4 % |

⚠️ **`reconcile` PASSES on defect 2**, because assets and resources are the same piece of
garbage. **`sane` is the only gate that refuses it** — which is the whole reason the quarter
read `missing` rather than wrong, and the reason the two bad writes below were possible.

⚠️ **DEFECT 3 IS ON DISK IN THREE UNCHECKED QUARTERS**: BID Q1-2024 E/A **2.4 %**, Q3-2024
**3.7 %**, Q1-2026 **7.4 %**. None was verified; they reconcile at layer 1 and never reach the
new layers, exactly as `SLD-1`'s five do not.

**Fixed** by two ParseLayer flags, off by default, last in the cascade: `notes_boundary`
(`_drop_after_notes`) and `relax_merged_seam` (`MERGED_SEAM_RE_LOOSE` + `_anchor` scoring the
split key). ✅ Default path unchanged and proven: **15 statements of ACB, VCB and BID reproduce
row for row against HEAD**, page classification included.

⚠️ **AND THE LAYER ORDER WAS WRONG ON THE FIRST TRY.** `onnx@200+notes` alone ACCEPTS the
statement — both totals are then correct, which is all `reconcile` and `sane` inspect — while
equity stays wrong. A layer that passes the gates ends the cascade, so **a half-right layer
placed before a fully-right one wins and nothing says so**. `+notes+seam` now runs first and
bare `+notes` is the last fallback. That is `SLD-1`'s shape one level up: the gates cannot see
the thing the layer fixes.

#### ⚠️ THE TWO BAD WRITES — and why the rule I wrote after the first one did not hold

| attempt | how | result |
|---|---|---|
| 1 | `periods: ["Q3-2025"]`, no check | ❌ **RUN_SUCCESS in 48 s**, wrote `tong_tai_san = 115,110,000,000`, and overwrote the Q3-2025 cash flow that was already `pdf`. Restored from backup |
| 2 | `periods: ["Q3-2025"]` **after** a full-history probe cleared all three statements | ❌ **RUN_SUCCESS in 50 s, the same garbage**. Restored from backup |
| 3 | `periods: ["Q4-2024","Q1-2025","Q2-2025","Q3-2025"]` | ✅ 22m23s, `onnx@200+notes+seam`, **only Q3-2025 changed** |

⚠️ **THE RULE WRITTEN AFTER ATTEMPT 1 — *"a `periods` run may only write what a full-history
probe has already cleared"* — IS WRONG, and attempt 2 is the measurement that killed it.** A
probe and a `periods` run take DIFFERENT PATHS through the cascade: with 59 quarters of history
`sane` rejects `onnx@200`'s garbage and the cascade escalates to `+notes+seam`; with no history
`sane` fails open, `onnx@200` passes at layer 1, and the cascade **stops there**. The probe
cleared a layer the run never reaches.

✅ **The rule that does hold: the RUN's history must match the PROBE's, and the only way to get
it is to put the preceding quarters in `periods`.** Three of them give a median of ~2.86e15 by
the time Q3-2025 is read, so the band is [1.4e14 .. 5.7e16] and 115,110 mn falls outside it.
⚠️ The three earlier quarters are re-parsed and re-written; here they reproduced identically
(E/A 5.2 / 5.2 / 5.4 %), **verified by diff, not assumed**.

⚠️ **`CLAUDE.md` §6-2-decies is sharper than it reads.** It says a subset run makes `sane`
"fail open"; what it does not say is that the run may therefore **stop at an earlier layer and
write something no probe ever judged**. That is the sentence to carry forward.

#### What is left in BID's balance sheet: nothing parseable

**62 `pdf` / 8 `missing`**, and the eight are Q1-Q3 2009, Q1-Q3 2010 and Q1-Q2 2011 — BID filed
only an annual report for 2008, 2009 and 2010, and its first quarterly is Q3-2011.
`documents()` returns **nothing** for all eight. `missing` is the correct answer and no code
change can alter it.

### ✅ 6-2-noviesdecies. BID Q1-2012 — THE TAIL PAGE WAS TWO NUMBERS SHORT, AND THE LABEL WRAPPED

Recovered 2026-08-27. Two independent defects, and the second is the one worth carrying
forward: **it produced a wrong figure that passed every gate.**

#### Defect 1 · the statement's LAST page was thrown away

The cash flow runs **pages 5-7**. `_fill_continuations` gives an unidentifiable page to the
statement running through it only when the page holds `MIN_TABLE_WORDS = 15` figures — the rule
that keeps a signature page out. **Page 7 holds 13**, and it carries codes 53/54/55: opening
**48,919,272,456,242**, closing **43,180,157,643,381**, every digit read correctly at
`onnx@200`. The page was dropped and the quarter recorded `missing` for `no closing cash
balance`.

⚠️ **A STATEMENT'S FINAL PAGE IS SPARSE FOR THE SAME REASON THE THRESHOLD EXISTS** — a few
closing rows and then the signature block. For a cash flow that is the one page that must not
be lost. **The threshold is NOT lowered**: `tail_continuation` admits a page on POSITIVE
evidence, by carrying the statement's own closing line (`PdfParser.TAIL`), and the run ENDS at
the page it admits.

#### Defect 2 · the label wrapped AROUND its figures — and the wrong figures RECONCILED

The filing prints the label's first half, then the item code on its own baseline, then the
second half beside the figures. `table_rows` builds a label only from the lines ABOVE, so the
code line cleared the pending label and the row came out keyed `thoi_diem_dau_ky` — and the
suffix `đầu kỳ`/`cuối kỳ` is **the only thing that tells the opening balance from the closing
one**.

⚠️ **WITH PAGE 7 RECOVERED BUT THE LABELS STILL TORN, THE MEASURED RESULT WAS:**
`close = 48,919,272,456,242` (the OPENING figure), `fx = 43,180,157,643,381` (the CLOSING
figure), **`reconcile` PASS and `sane` PASS**. Both cash figures on the wrong account, written
as `pdf`. That is `SLD-1`'s shape again and it is why the fix was not stopped at defect 1: a
test asserting only *"the quarter is no longer `missing`"* goes green on it.

⚠️ **AND THE ITEM CODE IS THE ONLY THING ON THE PAGE THAT SAYS WHERE AN ITEM BEGINS.** Keeping
the carry across a code line is not enough on its own — the FX adjustment (code 54) prints NO
figure, so its label is still pending when 55's figures arrive, and it would prefix the closing
label and push the discriminating suffix past `slug`'s 60-character cap. **That failure was
measured, not foreseen**: it is where the first attempt at this fix landed. Text after the code
is preferred; the carry is the fallback.

#### What shipped, and what it is verified against

Two `ParseLayer` flags, off by default, in four layers at the END of the cascade
(`onnx@200+tail`, `+relax`, `onnx@300+tail`, `+relax+components`).

| | |
|---|---|
| the run | `raw/cafef_financials` partition `HOSE_BID`, `periods=[Q4-2010, Q3-2011, Q4-2011, Q1-2012]`, **1h45m, RUN_SUCCESS** |
| the result | `Q1-2012 cash_flow=20 items [onnx@200+tail]` — `missing` → `pdf`, 9 filled cells → 35. Cash flow **51 → 52 parsed** |
| the anchors | opening **48,919,272,456,242**, closing **43,180,157,643,381**, FX blank (the filing prints none) — the right accounts, not swapped |
| an independent figure | `thu nhập lãi và các khoản thu nhập tương tự nhận được` = **12,225,174,001,132**, matching the figure read off the filing by hand |
| ⚠️ **the diff** | **exactly ONE period changed across all 9 CSVs.** ACB and VCB untouched; BID's balance sheet and income statement untouched; the three re-parsed neighbouring quarters reproduce byte-identically — verified by diff against a pre-run backup, not assumed |
| the default path | **24 statements** across 5 filings of ACB/VCB/BID, both `realign` settings, reproduce **row for row** against HEAD |
| tests | **13**, no PDF, no network — including one that reproduces the swapped-balances failure so the fix cannot be quietly undone |

⚠️ **The cash-flow IDENTITY is not verified for this row**, because `verify_cash` is tied to
`relax_totals` and this statement was accepted at a strict layer — the same as every other
strictly-accepted statement in the repo. **20 of 34 line items mapped.**

#### ⚠️ THE COST MODEL HAS NO TERM FOR DOCUMENT SIZE, AND IT IS WORTH 3.6x

Predicted 58-64 min from `P39`'s *"a failed document costs 18.2 min"*; the run took **105 min**.
Measured per quarter:

| quarter | document | size | wall clock |
|---|---|---|---|
| Q4-2010 | FY-2010 annual | 6.9 MB | **73 min** — 9.1 min/MB |
| Q3-2011 | Q3-2011 quarterly | 4.0 MB | 18 min — 2.5 min/MB |
| Q4-2011 | FY-2011 annual | 6.6 MB | **1.5 min** — accepted at layer 1 |
| Q1-2012 | Q1-2012 quarterly | 1.5 MB | 13 min — recovered at layer 37 of 40 |

**3.6x between two documents that both ran the full cascade.** §6-2-decies named the driver
(*"document size × number of distinct OCR passes"*) and `P38`'s `min/doc ≈ 0.94 + 0.173 x
%failing` has no term for it. ⚠️ **Re-budget `P38` and `P6` on SIZE, not only on failure rate** —
an annual report is the expensive document, and it is one quarter in four.

⚠️ **AND A LONG RUN REPORTS NO PROGRESS UNTIL A QUARTER COMPLETES.** `logs/app.log` sat at 3
lines for **73 minutes** while Dagster's compute log and the task output stayed at **0 bytes**
(stdout is fully buffered in the subprocess — §5 rule 20). The only live progress signal was
the **`LastAccessTime` of the filing PDFs**, which shows which document is open. That works and
is worth reusing; a 7-hour `P38` ticker will be just as blind without it.

### ⚠️ 6-2-vicies. BID Q3-2015 — THE FOURTH TERM IS A MERGER, NOT FX, AND `FXM-1` MISFIRES ON IT

Recovered 2026-08-27, and **no code was written for it**. The value of this quarter is the two
things it measured on the way.

#### It was already parseable — `801a88b`'s `_anchor` seam split did it

The filing's tail merges each label with the next item's numeral, so the closing balance came
out keyed `nhan_sap_nhap_mhb_viii_tien_va_cac_khoan_tuong_duong_tien_`. `MERGED_SEAM_RE` — the
**strict** one, no flag needed — splits that at `_viii_` to `tien_va_cac_khoan_tuong_duong_tien_`,
which answers `C_CASH_CLOSE`. `_anchor` only gained that split on 2026-08-26 (`PGB-1`), so the
2026-08-25 run could not map it. **`onnx@200` now passes `reconcile` AND `sane`**; the run took
3m39s and the quarter needed no new layer. Cash flow **52 → 53 parsed**; from 2012 BID is
**163 / 171 = 95.3 %**, with the balance sheet and income statement complete.

#### ⚠️ THE FOURTH TERM IS THE MHB MERGER — BID absorbed MHB in 2015

The tail reads **V** opening, **VII** `Tiền và các khoản tương đương tiền từ việc nhận sáp nhập
MHB`, **VIII** closing. There is **no FX line at all**, and the identity closes to the đồng on
the merger line instead:

| | |
|---|---|
| opening (V) | **50,202,708** mn — equal to Q4-2014's closing, as a cumulative-from-1-Jan statement requires |
| net for the period | 16,160,895 mn |
| **VII · cash acquired with MHB** | **1,477,340** mn |
| closing (VIII) | **67,840,943** mn |
| **residual** | **0** |

✅ **The merger line is correctly left UNMAPPED** — the bank chart of accounts has no column for
it, and inventing one is not this run's business. The two balances are on disk and right.

#### ⚠️ AND THAT IS WHY `FXM-1`'s WRITTEN RECOVERY IS UNSAFE — measured, before it ever ran

`_recover_totals` claims *"when the recovered pair sits exactly two rows apart the row between
them is the FX line"*, and defends itself with *"`_cash_flow_identity` tests opening + IV + fx ==
closing to the đồng immediately afterwards, so a wrong row is REJECTED, not written"*.

⚠️ **THAT ARGUMENT IS FALSE, AND THIS FILING IS THE COUNTEREXAMPLE.** The row between the two
balances **is** the genuine fourth term — it is simply not FX. The identity therefore closes
*because the row is right about the arithmetic and wrong about the account*, and it cannot
reject what it confirms. Measured at `onnx@200+relax`: **`hdtc_vi_dieu_chinh_anh_huong_cua_thay_doi_ty_gia`
= 1,477,340,000,000**, i.e. merger cash written into the FX column. Nothing reached disk only
because that layer was refused for an unrelated reason (`opening not mapped`).

⚠️ **This is `SLD-1`'s shape a third time: a wrong figure that passes every gate.** `P39` must not
be run until the recovery names what it claims — the positional guess needs the row's own label
to be consistent with FX, or the term must be written to a column that says "unidentified fourth
term". **A structural defect, not a threshold.**

#### ⚠️ AND A `periods` RUN SILENTLY DOWNGRADED THE QUARTER IT WAS GIVEN FOR HISTORY

`PGB-1`'s remedy is *"put the preceding quarters in `periods` so the run's history matches the
probe's"*. This run did that — `[Q1-2015, Q2-2015, Q3-2015]` — and **Q1-2015 came back worse**:

| | before | after |
|---|---|---|
| winning layer | `onnx@200+pad6+components` | **`onnx@200`** |
| `hddt_mua_sam_tai_san_co_dinh` | −424,367 mn | **lost** |
| `hdkd_4_chenh_lech_...` | −11,922 mn | **−21,922 mn** — the two runs disagree |
| `publish_date` (all 3 statements) | 2015-09-16 | **lost** |

⚠️ **THE MECHANISM IS `sane`, PROVEN BY ELIMINATION.** Layer 1 is deterministic — the
2026-08-26 regression reproduced 24 statements row for row — so the parse and `reconcile` were
identical in both runs. The full run held a complete history there and **refused layer 1**; this
run held almost none, `sane` failed open, and the earlier, worse layer won.

⚠️ **SO `PGB-1`'s REMEDY CARRIES A COST NOBODY HAD MEASURED: the history-supplying quarters are
themselves re-parsed with a THINNER history than they originally had, and can be silently
downgraded.** The rule that survives is narrower — **a `periods` run may only WRITE the quarter
it is repairing.** Q1-2015 was restored from the pre-run backup and the final diff is **one
period changed across all nine CSVs**. ⚠️ Taking that backup is what made this visible at all;
the log said `RUN_SUCCESS` and named `onnx@200` without a word about the downgrade.

### ✅ 6-2-unvicies. BID Q1-2026 — THE STATEMENT NAMED NO UNIT, AND `unit_of` CANNOT SAY SO

Recovered 2026-08-27 at a new layer, `onnx@200+unit+tail`. Three defects, and the third is an
ordering trap this repo has now hit twice.

#### ⚠️ Defect 1 · "printed in đồng" and "did not say" were ONE answer

`unit_of` returns `1` both for a statement that names đồng and for one that names nothing.
BID's Q1-2026 cash flow prints **"Triệu VNĐ" on NEITHER of its two pages** while the balance
sheet of the same filing does, so every figure was read as đồng — a uniform 10⁶ error which,
as `unit_of`'s own docstring says, **reconciles perfectly against itself**. `sane` was the only
gate that saw it (`magnitude 5.45e+08 vs typical 1.19e+14`), and refusing the quarter was
CORRECT behaviour, not a failure.

`declared_unit()` now returns `None` for silence, and `document_unit()` supplies the unit the
rest of the filing declares — consulted only when the statement itself is silent, and only when
the filing does not contradict itself. ⚠️ **Notes pages do not vote**: a note is printed in
whatever unit the note wants.

#### Defect 2 · the label wrapped around its figures, in a THIRD shape

| | |
|---|---|
| `y=405.4` | `Tiền và các khoản tương đương tiền tại t` **+ 530,277,690** ← opening |
| `y=412.9` | `V điểm đầu kỳ` — the half that names the account, **7.5pt** below |
| `y=426.6` | `VI Tiền và các khoản tương đương tiền tạ` ← closing's first half |
| `y=432.7` | `BẮT TR` at **x=594** — the round company stamp |
| `y=437.8` | `điểm cuối kỳ` **+ 544,528,992** ← closing |

Two things `label_wrap` could not yet reach: the value line **carries half its own label** (so
`not label` was still too strict), and **the stamp** sits between the closing label's halves.
Widened on both — the forward branch now always runs under the flag, with **distance** as the
separator (a wrapped half is inside 8pt where ordinary rows on this page are 15-32pt apart);
and a line that contributed **neither a label nor a figure** no longer ends a pending label,
bounded by `CARRY_GAP = 24pt`. ⚠️ `_only_item_code` was the narrow version of this and is
**deleted** — the generalisation subsumes it.

**opening 530,277,690 + net 14,251,302 = closing 544,528,992** (Triệu VNĐ), residual **0**.

#### ⚠️ Defect 3 · I ORDERED THE NEW LAYERS WRONG, AND THE GATES COULD NOT SEE IT

With the unit corrected but the labels still torn, **`onnx@200+unit` passes `reconcile` AND
`sane`** while writing the **OPENING** balance into the closing slot — 530,277,690 where the
filing prints 544,528,992. Both gates see one plausible cash balance and cannot tell which line
it came from. A layer that passes ends the cascade, so the half-right layer must never run
first; `+unit+tail` now precedes it and bare `+unit` is the last fallback.

⚠️ **`PGB-1` recorded this exact trap for `+notes` vs `+notes+seam`. This is the SECOND
instance, so it is a rule and not an anecdote: when two new layers differ by a LABEL REPAIR,
the repair goes first.** Caught here only because the values were checked against the filing
rather than the gates' verdict.

#### ⚠️ AND THE HISTORY-PROVIDER DOWNGRADE REPRODUCED — a second time, same shape

§6-2-vicies measured a `periods` run silently downgrading Q1-2015, a quarter included only to
give `sane` a band. It happened again: **Q3-2025's cash flow went `onnx@200+relax` →
`onnx@200`, 38 filled cells → 37.** Restored from the pre-run backup, after which the diff is
**one period changed across all nine CSVs**.

⚠️ **So this is now reproduced, not anecdotal, and the procedure is fixed**: a `periods` run
must be diffed against a pre-run backup and **every non-target quarter that moved must be
restored.** The log says `RUN_SUCCESS` and names a layer; it says nothing about the downgrade.

| | |
|---|---|
| run | 4 periods, **41m 53s**, `Q1-2026 cash_flow=19 items [onnx@200+unit+tail]` |
| BID cash flow | 53 → **54** parsed |
| from 2012 | **164 / 171 = 95.9 %**, balance sheet **57/57** and income statement **57/57** |
| default path | **24 statements** across 5 filings reproduce row for row against HEAD |
| tests | **64** — 5 new, no PDF and no network |

⚠️ **Seven cash flows remain, all `fx not mapped`**: Q4-2015, Q3-2016, Q4-2016, Q2-2017,
Q3-2017, Q4-2017, Q3-2018. `FXM-1`'s written fix is measured WRONG (§6-2-vicies) and must not
be run as it stands. ✅ **ALL SEVEN CLOSED THE SAME DAY** — six at STRICT layers with no FX
change at all (§6-2-duovicies) and Q4-2016 at a new one (§6-2-quatervicies). ⚠️ **And the label
`fx not mapped` was itself the artefact**: it is the LAST layer's reason, and the last layer is
always the most relaxed one.

### ⚠️ 6-2-duovicies. THE SEVEN `fx not mapped` QUARTERS — SIX WERE NEVER AN FX PROBLEM, AND THE SEVENTH WAS WRONG

Run 2026-08-27, 15 periods, **1h41m**, 0 errors. ⚠️ **A prediction was recorded before it ran**
— *"several will parse at a STRICT layer, because `fx not mapped` is the RELAXED path's
symptom and the real defect is the labels"* — and it holds.

| target | layer that won | new layer needed? |
|---|---|---|
| Q4-2015 | `onnx@200+relax` | no |
| Q3-2016 | **`onnx@200`** | no |
| Q4-2016 | `onnx@200` | ⚠️ **the value is WRONG — see below** |
| Q2-2017 | `onnx@200+relax` | no |
| Q3-2017 | **`onnx@200`** | no |
| Q4-2017 | `onnx@300` | no |
| Q3-2018 | **`onnx@200`** | no |

⚠️ **NOT ONE NEEDED `tail_continuation`, `label_wrap` OR `unit_from_document`.** Four won at
plain `onnx@200`. What actually unblocked them was `PGB-1`'s `_anchor` seam split, shipped
2026-08-26 — the same change that recovered Q3-2015 — and **nobody had re-run these quarters
since.** ⚠️ **So `FXM-1` is far smaller than its "8 of 13" record claims**: at a strict layer
`verify_cash` is off and the identity is never demanded, so the FX line is not needed at all.
The 2026-08-25 probe read the LAST layer's reason, and the last layer is always the most
relaxed one. **A cascade's final refusal names the hardest path tried, not the blocking
defect.**

#### ⚠️ AND ONE RECOVERED VALUE WAS WRONG — caught by a check no gate performs

BID's cash flow is cumulative from 1 January, so **every quarter of a year must share one
opening balance**, and it must equal the prior year's closing. Applied across 2014-2018, every
recovered OPENING is corroborated by an independent quarter — and two closings are not:

| | on disk | the filing's own printed figure |
|---|---|---|
| **Q4-2016 closing** | **61,575,636** | **65,521,789** ❌ |
| Q4-2017 closing | 100,455,652 | plausible (0.28 % from 2018's opening — a restatement) |

Re-reading FY-2016 directly: *cash at start of year* **55,806,145**, *cash received with MHB*
**3,004,011**, *cash at end of year* **65,521,789** — and 65,521,789 is exactly the opening
that Q2-2017, Q3-2017 and Q4-2017 independently agree on. **Q4-2016 was reverted to `missing`.**
⚠️ Its identity residual was **exactly +1,000,000,000,000**, a suspiciously round number that
`reconcile` and `sane` both accepted. ✅ **PARSED CORRECTLY LATER THE SAME DAY — §6-2-quatervicies**,
and these three figures are what it was checked against. ⚠️ **The 61,575,636 did not come from
this filing**: it is the UNAUDITED quarterly's figure, reached through `alternates`, i.e. a
RESTATEMENT rather than an OCR error — which is a hazard `alternates` still has no guard for.

⚠️ **THE CHECK THAT FOUND IT IS REUSABLE AND CHEAP**: a cumulative cash flow's opening balance
is printed four times a year and once more as the prior year's close, so **five independent
readings of one number** exist in the corpus already. No OCR, no network — and it caught what
43 layers and two gates did not.

#### ⚠️ BID HAS THREE MERGERS IN THIS DATA, AND EACH ADDS AN UNMAPPED FOURTH TERM

MHB in 2015 (**1,477,340**) and again in 2016 (**3,004,011**), LVB in 2017 (**1,540,994**).
Each is a real cash source with no column in the bank chart of accounts, so it is correctly
left unmapped — and each shows up as a **constant identity residual across that year's
quarters**, which is how they were told apart from OCR noise. ⚠️ **This is why `FXM-1`'s
positional guess must not ship**: on any of these quarters it would claim the merger line as
FX and the identity would confirm it.

| | |
|---|---|
| BID cash flow | 54 → **60** parsed |
| **from 2012** | **170 / 171 = 99.4 %** — balance sheet **57/57**, income statement **57/57**, cash flow **56/57**. ✅ **171/171 later the same day** — §6-2-quatervicies |
| the one gap | **Q4-2016**, and the correct closing is known: **65,521,789** mn. ✅ **CLOSED 2026-08-27**, and that known figure is what the recovery was checked against |
| collateral | **5 history-provider downgrades**, all restored — the pattern is now measured **three times** |

### ⚠️ 6-2-tervicies. BID Q4-2016 STAYS `missing`, AND NOW THE REASON IS EXACT

Worked 2026-08-27. The label defect was found and fixed; **the quarter is still not writable,
and that is the correct outcome.** Both halves are worth carrying.

#### What was wrong with the label, and what fixes it

FY-2016's closing balance comes out keyed
`ty_con_khi_hop_nhat_tien_vi_cac_khoan_tuong_duong_tien_cuo` while the line ITSELF reads
"Tiền vì các khoản tương đương tiền cuối năm" and carries the right figures. Three separate
things had to be true at once:

1. **Somebody else's words are stuck on the front.** The previous item's label wrapped onto its
   own line and was still pending when this row's figures arrived. ⚠️ **`table_rows` cannot fix
   it**: on that page a wrapped continuation is **11.8pt** below its line and the next ordinary
   row is **15.1pt** below, so widening the forward branch is tuning on a 3pt margin.
2. **`slug` caps a key at 60 characters** and that is exactly where "cuối năm" lived, so no
   amount of trimming recovers it — the trim must run on the re-slugged FULL label.
3. ⚠️ **An ANNUAL report dates its balances by the YEAR where the chart of accounts names the
   PERIOD** — "cuối năm" against "tại thời điểm cuối kỳ". A wording difference in the FILING,
   like `CASH_TAIL`'s, so it does not improve with resolution: the trimmed label scores
   **0.765** against the schema wording and **0.944** against the annual one.

⚠️ **AND THE FIRST VERSION OF THE FIX SWAPPED THE TWO BALANCES.** Rewriting the account to the
annual wording drops "tại thời điểm" — text the two lines SHARE — which raises the relative
weight of everything else until the OPENING row scores **0.804** against the CLOSING account,
over the bar. The walk then put the opening figure in the closing slot and **only `sane`'s
equality gate caught it.** So the period word is now a **hard discriminator**: a row saying
"đầu" is refused for the closing account outright, whatever it scores. Measured after:
opening row **0.851 / 0.000**, closing row **0.000 / 0.840**.

#### ⚠️ AND THE QUARTER IS STILL NOT WRITABLE — one line is misread at every DPI

⚠️ **SUPERSEDED THE SAME DAY — the quarter IS writable, and this sub-section's own headline is
the reason it took a second pass. "Misread at every DPI" is true of the DEFAULT CROP and false
at `crop_pad=6`, which was never tried. §6-2-quatervicies.** Everything else below stands and
the four verified figures it leaves behind are exactly what the recovery was checked against.

With the labels fixed, opening **55,806,145** and closing **65,521,789** are both correct. The
statement still does not close:

| | |
|---|---|
| net cash flow as parsed | **671,163** |
| net cash flow REQUIRED | **6,711,633** |
| what the OCR actually read | **`6.711.6.3`** — `parse_num` strips the dots and gets a **10x** error |
| the 2015 comparative column | 50,199,476 + 4,129,329 + 1,477,340 = **55,806,145** ✅ closes exactly |

⚠️ **A STRICT LAYER WOULD HAVE WRITTEN THAT.** `verify_cash` rides with `relax_totals`, so
`onnx@200+annual` accepts a statement whose two balances are right and whose net is out by a
factor of ten. **Only the `+relax` variant ships** — a layer that can recover a label must not
also be a layer that skips the arithmetic. That is the third ordering lesson of the day and the
sharpest: **the earlier lessons were about which layer runs first; this one is about which
layer may exist at all.**

⚠️ **So `missing` is the correct answer for Q4-2016**, and it is now `missing` for a reason that
is written down rather than unknown: **opening 55,806,145 · net 6,711,633 · MHB 3,004,011 ·
closing 65,521,789**, all four verified against the filing and against 2017's opening. A future
session can check any fix against those four numbers in seconds.

⚠️ **`annual_tail` HAS RECOVERED ZERO QUARTERS SO FAR** and is kept on the same terms as the six
layers added 2026-08-24: it sits last, behind the identity check, and the annual wording is
universal — every `FY-*` filing in the archive words its cash tail this way, so it will matter
at `P38` scale. **That is an argument, not a measurement, and it is labelled as one.**
✅ **It stopped being an argument within hours**: `annual_tail` is half of the layer that
recovered this quarter, and the layer WITHOUT it maps 21 items where the layer with it maps 24.

### ✅ 6-2-quatervicies. BID Q4-2016 PARSED — the digit was outside the CROP, and the fourth term had no column

Recovered 2026-08-27, hours after §6-2-tervicies concluded the quarter was correctly `missing`.
⚠️ **That conclusion rested on one sentence nobody had tested — *"one line is misread at every
DPI"*.** It is true at every DPI **with the default crop**, and false at `crop_pad=6` — a knob
the file's own docstring names for exactly this defect and that section never tried. Two
independent blockers, and the second is the reusable one.

#### Defect 1 · the detector box ended INSIDE the number, so no DPI could help

Measured on page 12 of the FY-2016 consolidated filing, every config run twice, identical both
times. The filing prints `6.711.633`:

| layer | what OCR returns | `parse_num` |
|---|---|---|
| `onnx@200` | `6.711.6.3` | **671,163** — 10× low |
| `onnx@300` | `6.711.610` | **6,711,610** — wrong by **23** |
| `onnx@400` | `6.711.63)` | **671,163** |
| **`onnx@200+pad6`** | **`6.711.633`** | **6,711,633** ✅ |

⚠️ **THIS IS `crop_pad`'s OWN DOCUMENTED DEFECT AT THE OTHER END OF THE BOX.** ACB's Q3-2023
loses its LEADING digit because the box starts inside the number; here the box ENDS inside it
and the last digit is never shown to the recogniser. Both are invisible to resolution — the
pixels are outside the crop at 200, 300 and 400 alike — which is why the three default-crop
layers agree with each other and all three are wrong.

⚠️ **AND THREE OF THE FOUR MISREADS ARE WELL-FORMED THOUSANDS GROUPS**, so no grouping check
could ever catch them: `parse_num` returns a plausible number and only the identity disagrees.
The 300 dpi read is wrong by **23 đồng in 6.7 million** — which is why the new check below is
held to EXACT equality and not to `_equal`'s tolerance.

⚠️ **A PROBE THAT DOES NOT SET THE ENGINE IS NOT PROBING THE PIPELINE.** `PdfParser()` defaults
to **tesseract** (`OCR_ENGINE = os.environ.get("CAFEF_OCR_ENGINE", "tesseract")`, and the var is
NOT in `.env`), so the first two probes of this session measured the wrong engine and produced a
different garbled token (`1/M4,011`) that sent the diagnosis sideways. Production is unaffected
— every `ParseLayer` names its engine — but a probe must name it too.

#### Defect 2 · the statement has a FOURTH term and the chart of accounts has no column for it

BID's FY-2016 cash flow prints **five** lines where the bank chart has four: IV movement, V
opening, *"…từ việc nhận sáp nhập MHB"*, *"…nhận từ các công ty con khi hợp nhất"*, VIII closing.
**Both columns close only with the extra term:**

| Triệu VND | 2016 | 2015 |
|---|---|---|
| opening | 55,806,145 | 50,199,476 |
| movement | 6,711,633 | 4,129,579 |
| **merger cash** | **3,004,011** | **1,477,340** |
| **closing** | **65,521,789** | **55,806,145** |

So the quarter was refused for `fx not mapped` while every figure on the page was correct. This
is `FXM-1`'s real shape, and §6-2-vicies had already measured why the written fix is unsafe: the
positional guess claims that row as FX, and the identity then **confirms** the wrong account
because the arithmetic is right.

#### ✅ What shipped — `cash_extra_terms`, and the term is COUNTED, never WRITTEN

A `ParseLayer` flag, off by default, on three layers at the very end of the cascade. It sums
what the filing printed **between** the two balance rows and lets that stand in for `fx`, which
it already contains. Three properties carry the whole design:

1. ⚠️ **COUNTED, NEVER WRITTEN.** The figure is admitted to the CHECK and the FX column is left
   empty — §5 rule 2 for a number nothing can attribute. For the same reason the flag **stops
   `_recover_totals`' positional FX guess claiming a row whose own label does not say FX**,
   which is the safe half of `FXM-1` that §6-2-vicies asked for. Nothing is lost: BID's FY-2015
   has ONE row between its balances and the guess DOES fire there, and the span counts it
   instead (50,199,476 + 4,129,579 + 1,477,340 = 55,806,145).
2. ⚠️ **THE CURRENT-PERIOD CELL ONLY, never `_first_value`.** The 2016 column leaves the MHB
   line blank and prints 1,477,340 beside it in the 2015 comparative; the fall-through would add
   a prior-year figure to this year's identity and break a sum that closes exactly without it.
3. ⚠️ **POSITION IS THE WHOLE DEFINITION.** Matching by label would mean guessing which words
   name a reconciling item, and filings word them differently every time. Between the two
   balances a cash flow prints nothing else — so the span needs no vocabulary, and whatever it
   returns is tested to the đồng immediately.

⚠️ **THE FIRST OF THE THREE LAYERS COSTS NO OCR AT ALL.** `crop_pad` is part of the parse-cache
key and `annual_tail` / `cash_extra_terms` are not, so `onnx@200+pad6+annual+extra` re-maps the
pages `onnx@200+pad6+components` already rendered. ⚠️ **And the label repair goes FIRST**, per
the rule `PGB-1` and §6-2-unvicies each measured independently: the bare `+extra` layer also
accepts, with **21 items against 24**, so ordering it first would have cost three line items and
ended the cascade.

#### The run

| | |
|---|---|
| command | `raw/cafef_financials` partition `HOSE_BID`, `skip_existing=false` `allow_parent=true` `periods=[Q1-2016 .. Q4-2016]` |
| result | **35m 20s, RUN_SUCCESS**, `Q4-2016 cash_flow=24 items [onnx@200+pad6+annual+extra]` |
| on disk | `pdf`, from the **audited consolidated annual**, IV **6,711,633** · V **55,806,145** · VI **empty** · VII **65,521,789** — identity exact |
| BID cash flow | 60 → **61** parsed |
| **from Q1-2012** | **171 / 171 = 100 %** — balance sheet 57/57, income statement 57/57, **cash flow 57/57**. Every row of the ticker reads `pdf` or `missing` and nothing else |
| the diff | pre-run backup, and **exactly one period changed across all nine CSVs** |
| tests | **79** in `src/web_scraper/`, of which **15 new**, no PDF and no network |

⚠️ **THE HISTORY-PROVIDER DOWNGRADE REPRODUCED A THIRD TIME, and it is subtler than the first
two.** Q3-2016's balance sheet and income statement each came back with `publish_date` **blank**
where they held `2017-05-01` — not a changed layer, not a changed figure, one lost DOCUMENT
FACT per statement. Restored from the backup. §6-2-vicies and §6-2-unvicies both lost a whole
OCR layer and several figures; this lost a single metadata cell in a quarter whose numbers were
untouched, so **a diff that only compares the figures would have called this run clean.**

⚠️ **AND A FUTURE FULL RUN WILL NOW REFUSE Q1-2017 — deliberately, and it looks correct.**
That quarter holds `VII = 65,521,789` on disk, which is exactly the closing this run accepted
for Q4-2016, so `sane`'s equality gate will fire (*"probe exactly equals an already-accepted
quarter"*). It is almost certainly right to: Q1-2017's own `IV` is −12,018,572 and Q2-2017's
opening is 65,521,789, so 65,521,789 is Q1-2017's OPENING balance sitting in its closing slot.
**A gate firing on a neighbouring quarter is the expected consequence of fixing this one.**

⚠️ **WHAT THIS DOES NOT DO.** It recovers no other quarter today: the three new layers sit last,
so only a statement that defeated all 44 before them can reach them, and no other BID quarter
does. **`FXM-1` is not closed** — the seven cash flows §6-2-duovicies recovered did so at strict
layers, and the unsafe positional guess is only guarded on the three layers carrying this flag,
not removed. ⚠️ And BID's eight remaining `missing` quarters (Q1-Q3 2009, Q1-Q3 2010, Q1-Q2
2011) have **no filing at all** — no code change can alter them.

⚠️ **ONE HAZARD THE MEASUREMENT EXPOSED AND NOBODY HAS FIXED: the ALTERNATE filing disagrees
with the audited one, and `alternates` cannot see it.** BID's unaudited Q4-2016 quarterly
reports a closing balance around **62.6 tn** against the audited annual's **65,521,789** — a
restatement, not an OCR error, and the two later quarters that carry 65,521,789 as their opening
say which one the company stands behind. That unaudited figure is the likely source of the
**61,575,636** §6-2-duovicies had to revert. Reconcile and `sane` both pass on a restated
statement, so nothing in the parser separates them; here it is moot only because the chosen
document now parses first. **`alternates` trades assurance for coverage and has no guard for a
restatement.**

### ✅ 6-2-quinvicies. `P39` — A GUARD WIRED TO A FLAG IS A GUARD THAT IS OFF, and it had already written two cells

Closed 2026-08-27, in its FX half. The defect is one boolean, the measurement around it is
the reusable part, and a prediction I recorded and then had to withdraw is the third.

#### The defect · the guard existed and forty-four layers could not see it

`cash_extra_terms` shipped on 2026-08-27 (§6-2-quatervicies) with a guard beside it:
`_recover_totals` may claim the row between the two cash balances as the FX adjustment only
when that row's own label says FX. ⚠️ **It was gated on the flag** —
`if v is not None and (named_fx or not extra_terms)` — so it was live on the **three** layers
carrying `cash_extra_terms` and absent on the other **forty-four**, `onnx@200+relax` among
them, which is **layer 5 of 47**.

⚠️ **READ OFF `cf_HOSE_BID.csv` THE NEXT DAY, THE UNGUARDED CLAIM HAD ALREADY WRITTEN MERGER
CASH INTO THE FX COLUMN TWICE, FROM TWO DIFFERENT DOCUMENTS**, and the identity confirmed
both to the đồng:

| | on disk | what it actually is | closes |
|---|---|---|---|
| **Q4-2015** | `1,477,340` | MHB merger cash, FY-2015 audited annual | 50,202,708 + 4,288,806 + 1,477,340 = 55,968,854 |
| **Q2-2017** | `1,540,994` | LienVietPostBank merger cash, Q2-2017 reviewed quarterly | 65,521,789 + 2,648,425 + 1,540,994 = 69,711,208 |

§6-2-vicies had predicted exactly this shape — *"the identity closes because the arithmetic is
right and the account is wrong, and it cannot reject what it confirms"* — but recorded it as a
reason **not to run** `FXM-1`'s written fix. **The fix was already running**, on 44 layers,
and had been since the FX recovery was added.

#### The fix, and the two dead parameters it exposed

Dropping `or not extra_terms` made `extra_terms` that branch's only reader, so it left
`_recover_totals`; `cash_extra_terms` then became dead in `map_to_schema` too and left that.
⚠️ **The wiring WAS the defect, so the parameter had to go rather than be set differently** —
a knob deciding whether a guard applies is a knob that turns a guard off. A test now asserts
the signature structurally, because the failure was never a wrong threshold.

⚠️ **AND THE TEST SUITE WAS PINNING THE DEFECT AS EXPECTED BEHAVIOUR.**
`test_a_positional_fx_guess_is_refused_when_the_row_does_not_say_fx` asserted
`loose.get(C_FX) == MHB_PRIOR` under the comment *"today's behaviour, and it is the defect"*.
**A test that pins a defect is how the defect survives a rewrite.**

#### ⚠️ THE BLAST RADIUS WAS MEASURED, NOT ARGUED — 32 candidates, 7.0 minutes

Every parsed cash flow whose FX column is non-empty **and** whose winning layer carries
`relax_totals` is exactly the set the guess could have written. Each was re-parsed at ITS OWN
recorded layer (every `ParseLayer` field reproduced — §6-2-quatervicies) and re-mapped:

| | |
|---|---|
| candidates | **32** — 30 ACB, 2 BID |
| **unchanged** | **30** — every ACB quarter's FX came from the ordered walk, not the guess |
| **dropped** | **2** — precisely the two merger cells |

⚠️ **This is `P39`'s *"0 cells may be lost and 0 may change layer"* met in the strong form**,
and at a fraction of the cost of the authoritative three-ticker re-parse the item asks for. It
is narrower: it covers the changed code path exactly and nothing else.

#### ⚠️ A PREDICTION I RECORDED AND THE MEASUREMENT KILLED

All three `cash_extra_terms` layers carry `crop_pad=6.0`, which BID's FY-2016 needed for an
unrelated reason. Both repaired quarters read correctly at the DEFAULT crop, so I added three
default-crop `+extra` layers arguing the span must be reachable wherever the guess was.
**Driven through the real cascade, neither fired:**

| | layer that won | items |
|---|---|---|
| Q4-2015 | `onnx@300+pad6+annual+extra` | 27 |
| Q2-2017 | `onnx@200+pad6+annual+extra` | 19 |

**All three were removed the same day.** The file's own rule is that a layer recovering zero
quarters is pure cost — six layers added 2026-08-24 are kept only on an argument and have
returned nothing since; these had a measurement available and it went against them. A test
records the disproven reasoning so it is not re-made.

#### The repair, and what it cost

One Dagster run, `HOSE_BID`, `skip_existing: false` `allow_parent: true`, **7 periods** — the
two targets plus five that exist only to give `sane` a magnitude band (`PGB-1`). **1 h 26 m,
RUN_SUCCESS**, and both targets landed on the layer the probe predicted, to the item count.

| | before | after |
|---|---|---|
| Q4-2015 FX | 1,477,340 | **empty**; `open + IV` unchanged, the fourth term 1,477,340 now unattributed |
| Q2-2017 FX | 1,540,994 | **empty**; likewise 1,540,994 |
| BID rows carrying an FX value | 6 | **4** |

⚠️ **THE HISTORY-PROVIDER DOWNGRADE REPRODUCED A FOURTH TIME**, and one of its forms is new:
Q1-2015's cash flow fell `onnx@200+pad6+components` → `onnx@200` (2 line items lost, 2 values
changed) and **four statements across Q1-2015 and Q3-2016 silently lost their `publish_date`**.
All five non-target periods were restored from the pre-run backup, after which the diff is
**exactly two periods, both targets**. ⚠️ A diff comparing only the FIGURES would have called
this run clean — §6-2-quatervicies made the same point about one lost metadata cell, and this
is the second instance, so it is the rule: **diff every column, not the numbers.**

#### ⚠️ What is NOT closed

1. **`P39`'s second half is untouched and is now measured to be BLOCKED.** Guarding
   `alternates` against a restatement cannot be done as the item describes: BID's Q4-2016
   unaudited quarterly and audited annual share the SAME opening balance, so the backward
   check the item proposes cannot separate them. What separates them is the opening balance
   of the **2017** quarters — a forward reading, i.e. `P43`'s machinery.
2. ⚠️ **FOUR BID ROWS STILL CARRY A WRONG FX VALUE AND `P39` DOES NOT REACH THEM** — Q4-2011
   `48,919,272` and Q2-2012 `40,110,402` are cash BALANCES sitting in the FX column, written
   at **strict** layers by `_align`/`_anchor`, not by the positional guess. A different
   mechanism, and `P43`/`P44`'s.
3. **The `P43` screen is unchanged by this work** — 7 BID quarters still hold the 1-Jan
   opening in the closing slot, 2 in the movement slot, 1 negative cash balance. Correctly so:
   `P39` was never about them.

---

### ✅ 6-2-sexvicies. THE PDF PARSE RUNS ON A KAGGLE T4 NOW — and it reproduces this machine cell for cell

Shipped 2026-08-28. `src/web_scraper/pdf_ocr_job.py` is the OCR cascade with the WRITE removed
and the machine made a parameter, `RUN__pdf_ocr.ipynb` is its ~7-cell binding, and
`kgpu`'s payload gained a third mode — **`documents`**, which ships filings instead of parquet
and opens no database at all.

```powershell
cd src ; python -m web_scraper.pdf_ocr_job --symbol VCB --periods Q1-2026   # here
cd src\kaggle_gpu ; python -m kgpu rehearse pdf-ocr ; python -m kgpu run pdf-ocr   # on a T4
```

**The test case was VCB Q1-2026**, chosen because all three of its statements already read
`pdf` at `onnx@200` on this machine — so the run has an EXACT baseline and scores itself
against it rather than being assumed to agree. Four runs of the same filing:

| run | card | onnxruntime | DETECTION on | RECOGNITION on | parse | verdict |
|---|---|---|---|---|---|---|
| local | RTX 3050 | — *(unrecorded)* | — | — | 100.6 s | **REPRODUCED** |
| Kaggle 1 | Tesla T4 | 1.29.0 | ⚠️ **CPU** | cuda | 83.8 s | **REPRODUCED** |
| **Kaggle 2** | **Tesla T4** | **1.22.0** | **CUDA** | cuda | **69.0 s** | **REPRODUCED** |
| local | RTX 3050 | 1.20.1 | CUDA | cuda | 113.3 s | **REPRODUCED** |

**REPRODUCED is the strong form and it is asserted per CELL**: 59 + 22 + 17 = **98 of 98 line
items identical**, the same winning LAYER (`onnx@200`), the same `unit`, the same
`publish_date`, and no column present on one side and absent on the other. ⚠️ **That was not
the expected answer.** Kaggle ships **torch 2.10.0+cu128** against this machine's **2.5.1+cu121**
and its own onnxruntime, and §3d records the library stack as a difference in the PROCEDURE
rather than in the hardware. For the ranker that mattered; for this OCR stack, on this
document, it did not — detection is a fixed convolution and recognition is an argmax, so
neither has the RNG the XGBoost result turns on.

⚠️ **ONE DOCUMENT, AND AN EASY ONE.** VCB Q1-2026 is accepted at layer 1 of 47. It says nothing
about a filing that escalates — the `+relax`, `+realign` and `+pad6` layers were never reached,
and a statement decided by a fuzzy threshold has more room to move between stacks than one
decided by a reconciliation.

⚠️ **THE SPEEDUP IS ~1.5× AND THE ERROR BAR IS VISIBLE IN THE TABLE.** Two local runs of the
identical document differ by **12.7 s (12 %)**, so `107 ± 6 s` local against **69.0 s** on a T4
is one measurement each, not a benchmark.

### ⚠️ THE HARD DOCUMENT — BID Q4-2016, and it INVERTED the prediction written above it

I wrote here, before running one, that an easy document is *"a WEAK case for the T4"* and that
the failing documents *"are the ones a GPU is for"*. **Measured 2026-08-28, and the advantage
gets SMALLER, not larger.**

`HOSE_BID Q4-2016` is the hardest filing on disk: the FY-2016 audited consolidated **annual**
report, 5.0 MB, whose balance sheet and income statement accept at `onnx@200` while its
**cash flow needs `onnx@200+pad6+annual+extra` — layer 45 of 47**, three stacked defects deep
(§6-2-quatervicies). `_parse_cascaded` breaks only when all three statements are accepted, so
one unresolvable statement makes the whole document pay the entire cascade.

| | card | parse | balance sheet | cash flow |
|---|---|---|---|---|
| local | RTX 3050 | **32.9 min** | REPRODUCED, `onnx@200` | REPRODUCED, `onnx@200+pad6+annual+extra` |
| **Kaggle** | **Tesla T4** | **26.4 min** | REPRODUCED, `onnx@200` | REPRODUCED, **the same layer 45** |

| | easy document (VCB Q1-2026) | **hard document (BID Q4-2016)** |
|---|---|---|
| local | 107 ± 6 s | 1,975 s |
| T4 | 69.0 s | 1,587 s |
| **speedup** | **1.55×** | **1.24×** |

✅ **REPRODUCING THE LAYER IS THE STRONGER RESULT, AND IT IS WHAT THIS RUN WAS FOR.** The
document does not merely read the same digits — it has to **lose 44 layers and win on the
45th**, which means `reconcile`, `sane`, the per-`(engine, dpi, crop, …)` parse cache and the
escalation order all behaved identically on Kaggle's **torch 2.10.0+cu128 / onnxruntime 1.22**
as on this machine's **2.5.1+cu121 / 1.20.1**. `sane` got its band from disk (24/19/23 probes)
and `open_ref = 55,968,854 mn`, the 1-Jan-2016 opening this repo verified in §6-2-vicies.

⏭ **The INCOME STATEMENT was refused, not scored, and that is the design working.** An annual
filing prints a CUMULATIVE P&L (19 mapped items) where the row on disk has been de-cumulated
(12). `compare()` abstains and says so.

⚠️ **WHY THE ADVANTAGE SHRINKS — a hypothesis, labelled as one, not a measurement.** A hard
document runs ~10 distinct OCR passes (the cache keys on engine/dpi/crop/join/title/loose/…)
but **47 MAPPING rounds**, and `map_to_schema` / `reconcile` / `sane` / `table_rows` are host
work no GPU touches. So the GPU-bound share falls as a document gets harder, which is the
opposite of what "the GPU is for the hard ones" assumes. **Nothing here measures that split**
— it would need per-phase timing inside `_parse_cascaded`, which does not exist.

### ⚠️ AND THEN BOTH SPEEDUPS WERE WITHDRAWN — the local clock moves 2.25× on its own

Two more runs of VCB Q1-2026 on 2026-08-28, hours after the first two, on the identical
document, the identical code and the identical machine:

| run | started | parse |
|---|---|---|
| local #1 | 01:04:51 | 100.6 s |
| local #2 | 01:26:09 | 113.3 s |
| **local #3** | **06:46:09** | **50.8 s** |
| **local #4** | **06:48:08** | **50.3 s** |
| Kaggle, CUDA detection | — | 69.0 s |

⚠️ **TWO TIGHT CLUSTERS FIVE HOURS APART, AND THE T4 SITS BETWEEN THEM.** Within a cluster the
spread is 1 % and 13 %; between them it is **2.25×**. Against the idle machine the T4 is
**1.37× SLOWER**; against the busy one it is 1.55× faster. **Nothing recorded in the artefacts
distinguishes the clusters** — same torch, same onnxruntime, same providers, same
`vram_free_mb`, same 12 of 53 pages read. I am not going to invent a cause.

⚠️ **SO EVERY CROSS-MACHINE TIMING NUMBER FROM THIS WORK IS WITHDRAWN**: the 1.55× above, and
the 1.24× on BID Q4-2016 (32.9 min local against 26.4 on a T4) — one pair each, against a
local clock that moves 2.25× unexplained. The rule this leaves: **to compare two machines,
INTERLEAVE the runs; do not take them hours apart.** §5 rule 1 in a new place — a measurement
taken under one configuration says nothing about another, and "the same machine, later" turns
out to be another configuration.

✅ **WHAT SURVIVES IS THE CORRECTNESS, AND IT IS THE PART THAT MATTERED**: 98 of 98 cells on
the easy document, **76 of 76 plus the same layer 45 of 47** on the hard one, across two torch
majors and two onnxruntime versions. That is deterministic, was reproduced four times, and
does not depend on a clock.

⚠️ **`P38`/`P6` CANNOT BE RE-BUDGETED ON A SPEEDUP AT ALL** — not on 1.5×, not on 1.25×. What a
T4 buys is measured differently: **a second machine, running in parallel with this laptop, at
no cost and without occupying it.** That is worth having and it is not a multiplier.

### ⚠️ AND THE THREE CONTRACTS WERE STANDARDISED — input, log, output

Same day, after the two runs above. `pdf_ocr_job`'s three callers (the CLI, the notebook,
`kgpu`) now build ONE frozen `JobSpec` and hand it to `run()`; `JobSpec.prepare()` resolves the
data root, the models, the TEMPLATE and the document list and **raises** on any of them,
before a page is rendered.

⚠️ **THE TEMPLATE WAS THE DANGEROUS ONE, AND IT WAS DEFAULTED.** `plan()` ended
`builder.template_of(symbol) or "bank"` — a silent wrong answer for the **761 of 781** listed
names that are not banks, which would map a corporate filing against the bank chart of
accounts and reject every statement as unreconcilable, hours later, reported as a parse
failure. `resolve_template()` tries an explicit override, then `templates.csv`, then CafeF's
own fingerprint, and RAISES if all three are silent. ⚠️ **Which route answered is recorded in
the artefact**, because *"read off templates.csv"* and *"guessed from a line-item count over
the network"* are not the same claim.

**The log now names the denominator of every percentage**, which `kaggle_gpu/README.md` §3 had
already established as this repo's rule for progress readouts:

```
[doc 1/1 100% of DOCUMENTS, not of time] HOSE_VCB__Q1-2026  …  8.1 MB  bank/consolidated
  band: balance 67, income 68, cash 68   open_ref=541688802000000
  [layer 1/47   2% of POSITIONS] onnx@200
    [ocr page 8/53  15% of PAGES — the only fraction here that predicts time]  ~76 s left
```

⚠️ **ONLY THE PAGE FRACTION PREDICTS TIME** — pages of one document cost ~0.87 s each, while a
document count spans 4.2 to 18.2 min and a layer index spans a full OCR pass to a cached
re-map. ⚠️ **And the page ETA is an UPPER BOUND**: `scan` stops at the notes boundary once the
three statements are behind it, so VCB Q1-2026 reads **12 of 53 pages** and finishes while the
last line printed says *"~61 s left"*. Said out loud rather than hidden — the denominator is
not knowable until the page that ends the scan.

⚠️ **THE FIRST VERSION OF THE PAGE HOOK REPORTED NOTHING, AND IT LOOKED LIKE IT WORKED.**
`run()` set `on_page` on `builder._parsers.values()`, which on a fresh builder holds the
ENV-DEFAULT parser only; the onnx parser that does the work is built LAZILY by `_parser_for` on
the first layer that needs it, and never got the hook. Zero page lines, no error. The hook is
owned by the BUILDER now and handed to every parser it creates. **A progress reporter that
reports nothing fails exactly like a fast run.**

**The output is a declared schema** — `metadata.json` (with `schema_version`, the resolved
spec, `template_how`, `environment.ocr`), `summary.csv`, `documents/<key>.json`, and **`run.log`
written line-buffered as the run goes** (§5 rule 20). **25 tests**, no PDF and no network.

### ⚠️ AND THE FILTER GAINED A `years` FORM — 2026-08-29

`plan()`, `JobSpec`, the CLI (`--years 2014 2015`), `kgpu`'s `data.documents.years` and the
notebook's `YEARS` all carry it, and **empty or absent means every year the ticker files** —
which is `documents()`'s own answer rather than a list any of the five recomputes. The unit is
a YEAR because that is the unit the statement build already skips in (`orchestration` §2a:
`_decumulate` needs Q1..Q(q-1) of the same year, so a partial skip deletes the very quarter a
run exists to fix). ⚠️ **The year of a document is read from its PERIOD, never from the index's
`year` column** — `documents()` folds a quarter-5 annual onto that year's Q4 and rewrites
`period`, and CafeF files 10 of 84,076 documents with a `Year` of `0`, `202` or `203`.
`years` and `periods` INTERSECT and each raises when it matches nothing.

⚠️ **AND `kgpu` NOW CHECKS THE TWO COPIES OF THE FILTER AGAINST EACH OTHER.**
`data.documents` decides which filings are UPLOADED and `parameters` decides which the worker
OPENS — a job naming different years in the two ships one set and parses another, and the
worker reports the shortfall as `missing`, the same word a genuinely unreadable filing gets.
`config._validate` refuses that, and refuses a year written as a string (`"2014"` filters
nothing, so the payload would quietly grow to the whole ticker). Free here; a round trip to
find. **9 tests**, no PDF and no network.

⚠️ **The notebook has TWO MODES and states which**: `MODE = "auto" | "local" | "kgpu"`. `auto`
resolves from `$CAFEF_DATA_ROOT` — the variable the bootstrap sets — and prints what it chose;
an explicit `"kgpu"` with no payload mounted **raises**, because falling back would parse the
repo's own `raw_data/` and report a successful Kaggle run that never touched the payload. Both
modes are exercised: `kgpu rehearse` drives the worker side, and the notebook was run
end-to-end locally (50.8 s, 98 of 98 REPRODUCED).

### ⚠️ `ORT-1` — A GREEN GPU RUN CAN BE HALF ON THE CPU, AND THAT IS WHAT KAGGLE RUN 1 WAS

The 21 % between the two T4 rows is the whole finding, and nothing in the first run said so.
`pip install onnxruntime-gpu` resolves to **1.29.0**, which needs **cuDNN 9 with CUDA 13**;
Kaggle's image is **CUDA 12.8**. `get_available_providers()` still listed
`CUDAExecutionProvider` — and `InferenceSession` then failed to create it and fell back to
CPU, so **DeepDoc detection ran on the worker's CPU while VietOCR ran on the T4**. The only
trace was one warning inside a wall of ANSI-coloured onnxruntime noise.

⚠️ **`get_available_providers()` IS AN ADVERTISEMENT; `session.get_providers()` IS THE
MEASUREMENT.** This is §5 rule 10 in a new place — a green step is not evidence the step did
what it said — and `_DbTextDetector` had been choosing its provider list from the
advertisement since it was written. Three changes, none of them a version bump alone:
`onnx_ocr` calls `ort.preload_dlls()` where it exists (1.21+ stopped adding the `nvidia-*`
wheels to the loader path itself, which is the underlying cause on a pip-only CUDA image);
`pdf_ocr_job.engine_report()` reads the SESSION back and records it in every run's
`metadata.json`; and the notebook pins the **line** `onnxruntime-gpu>=1.19,<1.23`.
⚠️ **An `==1.20.1` pin was tried first and cost a run** — this repo's own version is not
published for Kaggle's cp312 Linux (the index offers 1.20.0 and 1.20.2), so an exact pin fails
the INSTALL. ⚠️ **The two OCR halves fail independently** — detection is onnxruntime,
recognition is torch — so *"the GPU is being used"* is two questions, and
`runtime.gpu_report()` answers neither.

### ⚠️ VIC Q3-2014 ON A T4 — the first cross-machine check on a quarter that FAILS

Run 2026-08-29, and it is the first time this repo has scored a T4 against a quarter where the
cascade REFUSES most of what it opens. Every earlier comparison (VCB Q1-2026, BID Q4-2016, VIC
Q1-2026) ended with three accepted statements, so what they measured was reproduction of an
ACCEPT. On disk this quarter reads `missing` for its balance sheet and income statement and
`pdf` for its cash flow.

| statement | on disk | the T4 | verdict |
|---|---|---|---|
| cash flow | `pdf` `onnx@200` | `pdf` `onnx@200`, 19 items | ✅ **REPRODUCED** — 19/19 cells, same layer, unit and `publish_date` |
| balance sheet | `missing` | absent after 45 layers, first reason `assets != liabilities + equity` | ✅ **the REFUSAL reproduces**, same reason as local |
| **income statement** | `missing` | **`pdf` `onnx@300`, 16 items** | ⚠️ **RECOVERED — and nothing scored it** |

⚠️ **THE WORKER IS DETERMINISTIC, MEASURED BY RE-RUNNING IT.** The identical job was pushed a
second time against the identical payload: **every statement, every layer, every value and the
`rows_sha` over EVERY parsed row — mapped or not — match** (24.4 min vs 23.4 min, same stack
fingerprint `88df8ef02c08`). So the recovery is not a coin that came up heads. ⚠️ `rows_sha` is
the stronger comparison than §6-2-sexvicies' 98 cells, which covered only the MAPPED minority.

✅ **AND THE RECOVERED FIGURE PASSES AN INDEPENDENT ARITHMETIC CHECK.** The filing prints three
columns; the run took the first as Q3-2014. Against the ONE 2014 quarter already on disk, using
the cumulative identity `P43` proposes: `2,821,804,892,483 + (−1,225,516,938,300) +
3,210,714,392,300 = 4,807,002,346,483`, which is the filing's own 9-month column **to the
đồng**. Q2-2014 is not this run's data, so the check is genuinely external.

⚠️ **AND IT STILL MAY NOT BE MERGED WITHOUT READING THE FILING, BECAUSE THE GATE SAW A
DIFFERENT POPULATION.** The full local run REFUSED `onnx@300` here with *"sane: probe exactly
equals an already-accepted quarter"*. **Nothing about the machine differed** — the cash flow
reproduced bit for bit and the balance sheet was refused identically. What differed is the
magnitude band: `seed_history` rebuilds it from the `pdf` rows on DISK (**12** income-statement
probes for VIC — only 13 of 27 quarters map a PBT anchor at all) while a full run accumulates
it IN THE RUN, over more quarters and over pre-de-cumulation figures. ⚠️ **Which quarter caused
the local collision is NOT established** — resolving it needs the same document run locally
against the disk-seeded band, and that has not been done.

⚠️ **`kgpu run` REPORTED `FAILED` ON A KERNEL THAT HAD COMPLETED, AND 24 MINUTES OF GPU NEARLY
WENT WITH IT.** The Kaggle client writes the kernel log with a bare `open(outfile, "w")`, which
resolves to **cp1252** on Windows against a log carrying `⚠️` — `UnicodeEncodeError` inside
`pull`, after the compute was spent. §5 rule 18 in a dependency, where the file mode is not
ours to pass; `PYTHONUTF8=1` fixes it only from the NEXT process, which is no use to a notebook
already running. `runner._utf8_text_files()` scopes a UTF-8 `open` to that one call.

### ⚠️ AND THE MERGE INTO `raw_data/` IS A SECOND MODULE NOW — 2026-08-29

`pdf_ocr_job` still writes no statement CSV. `web_scraper/pdf_ocr_merge.py` takes a finished
run folder and upserts it — and **since the same day, by request, it WRITES BY DEFAULT**:
`MERGE_INTO_CSV = True` in the control notebook, `merge_statements=True` on the job, and
`kgpu merge <job>` writes unless given `--dry-run`. ⚠️ **The refusals are what makes an
automatic write defensible, not the extra command**, and the merge touches only the quarters
the run produced.

⚠️ **THE MERGE RUNS ON THIS MACHINE AND COULD NOT RUN ANYWHERE ELSE.** A Kaggle kernel writes
`/kaggle/working` and exits; the CSVs are here. *"The Kaggle run upserts the CSV"* is
necessarily *"the pull does"* — which is what makes a pre-merge backup and a printed diff
possible. It calls `FinancialsBuilder._write(merge=True)`, the same upsert `build()` uses, so
only the quarters the run PRODUCED are rewritten.

**Three refusals, each a measurement, each with a `force_*` escape**: a **cumulative income
statement** (the filing prints the year to date, the column holds the quarter, and a
one-document run has no priors to de-cumulate with); an **empty `sane` band** (the guard failed
open, so nothing judged the figure); and a figure that **DIFFERS from a good `pdf` row**
(`compare()` scored it already — two runs disagreeing is not settled by preferring the newer).

⚠️ **AND THE SECOND REFUSAL HAD A FIELD CASE THE SAME DAY.** VIC Q3-2014: the T4 ACCEPTED an
income statement at `onnx@300` that the full local run had REFUSED with *"probe exactly equals
an already-accepted quarter"*. **Nothing about the machine differed** — the cash flow reproduced
bit for bit at the same layer, the balance sheet was refused for the same reason on both. What
differed is the POPULATION the gate compares against: `seed_history` rebuilds the band from the
`pdf` rows on DISK (12 income-statement probes for VIC) while a full run accumulates it IN THE
RUN, over more quarters and over pre-de-cumulation figures. **A statement a worker accepts is
not a statement a full run would accept**, and no code can tell the two apart — which is why
`apply=False` is now the explicit look rather than the default. `SAN-1` from a fifth side.
**15 tests.**

✅ **FIRST REAL MERGE, 2026-08-29 — one period, one statement, and the other two files did not
move.** VIC Q3-2014's income statement went in (21 → 22 parsed); diffed against the pre-merge
backup **column by column across all three CSVs**, exactly 22 columns of one period changed and
the balance sheet and cash flow moved **not one cell**, even though `_write` rewrote all three.
⚠️ **And that diff found a defect in the merge itself**: `BACKUP_ROOT` was a RELATIVE path, so
the backup landed under `src/kaggle_gpu/` — `kgpu merge` runs from there — and the one thing
that makes a merge reversible went where nobody looks. Anchored to the repo root. *A safety net
placed by the caller's working directory is a safety net you cannot find when you need it.*

### ⚠️ What the module deliberately does NOT do

1. ⚠️ **IT WRITES NO STATEMENT CSV.** The artefact is a run folder. This repo has measured
   **four** separate builds in which a `periods` run silently DOWNGRADED a quarter it was given
   only for history while the log said `RUN_SUCCESS` (§6-2-vicies, §6-2-unvicies,
   §6-2-quatervicies, §6-2-quinvicies); a run whose output is an artefact cannot do that.
   Merging a recovered quarter stays a deliberate Dagster act with a pre-run backup.
2. ⚠️ **`sane` IS SEEDED FROM DISK AND THAT IS A RECONSTRUCTION.** `seed_history` rebuilds the
   magnitude band from the `pdf` rows already on disk, in `build()`'s own entity split, with
   `MIN_ITEMS_FOR_HISTORY` applied and restricted to periods BEFORE the target — VCB Q1-2026
   got 67/68/68 probes. It is not the run's own history and cannot be: it holds what disk
   records. An empty band is what makes `sane` fail open (§6-2-octodecies), so the rehearsal
   WARNs when one is empty.
3. **No alternate-filing retry and no de-cumulation.** Both are `build()`'s, and both need
   state a one-document run does not have. `compare()` therefore REFUSES to score a cumulative
   income statement rather than reporting every cell as changed.
4. ⚠️ **The comparison reads EVERY column, not the figures** — layer, unit and `publish_date`
   beside the values, because a run that lost one `publish_date` and nothing else read as clean
   to a figures-only diff, twice (§6-2-quatervicies, §6-2-quinvicies).

⚠️ **Two defects were found in `kgpu` itself and both were on paths `rehearse` cannot reach**:
the dataset version-note read `manifest['schema']` unconditionally and died with a bare
`KeyError` on a payload that has no schema, and `_stage_layout` extracted only `source.zip`
where Kaggle extracts EVERY zip. The first is the upload path, which a rehearsal never walks;
the second would have left the nested rehearsal testing a shape the worker never sees. **17
tests** pin the module's four decisions without a PDF, a network or an OCR engine.

---

### ⚠️ 6-2-septvicies. VIC Q1-2026 — THE FIRST NON-BANK PARSE, and it wrote a wrong cash figure

Run 2026-08-28 as a prototype, **31.1 min**, the full 47-layer cascade, 12 distinct OCR passes
over 71 pages. It is the first filing this repo has ever parsed against a chart of accounts
other than `bank`, and it produced exactly the evidence `P5` needs — including two things
`TPL-1` did not predict.

| | |
|---|---|
| template | **`corp`**, resolved by CafeF's own fingerprint over the network — ⚠️ **and `resolve_template` is the only reason that is visible**: the previous `or "bank"` would have read a corporate filing against the bank chart in silence |
| balance sheet | ✅ accepted at `onnx@200`, **79 items** — more than any bank statement here |
| cash flow | ⚠️ **accepted at `onnx@200` AND WRONG** — see below |
| income statement | ❌ **`no such statement on any page of this filing`, at all 47 layers** |
| magnitude band | **EMPTY** — VIC has no accepted quarter on disk, so `sane` failed open, and the run WARNED that it would |

#### ⚠️ THE CLOSING CASH BALANCE WENT INTO THE FX COLUMN — proven three ways, no PDF needed

The corp chart prints `(60)` opening, `(61)` FX effect, `(70) = 50+60+61` closing. The run put
**54,750,360 mn into the FX cell** and left the closing cell **empty**:

| | mn VND |
|---|---|
| opening `(60)` | 72,226,561 |
| net for the period `(50)` | −17,476,201 |
| **opening + net** | **54,750,360** |
| **what is in the FX row** | **54,750,360** — identical |
| the same filing's BALANCE SHEET, `i_tien_va_cac_khoan_tuong_duong_tien` | **54,750,360** |
| its two components, 47,992,026 + 6,758,334 | **54,750,360** |

A 54.75 tn FX effect on 72 tn of cash is not a number a company reports. ⚠️ **AND IT PASSED
BOTH GATES**: accepted at `onnx@200`, a STRICT layer where `verify_cash` rides with
`relax_totals` and is therefore OFF, so `_cash_flow_identity` never ran — and `sane` had no
band. This is `FXM-1`/`P39`'s shape from the other direction: there merger cash was written
INTO the FX column, here the closing balance is.

#### ⚠️ AND THE BALANCE SHEET RECONCILED ON THE TRIVIAL IDENTITY

Assets **1,178,694,748 mn** equals resources **1,178,694,748 mn** — which is
`TỔNG CỘNG TÀI SẢN` against `TỔNG CỘNG NGUỒN VỐN`, true by construction on any page that reads
both. `C_LIABILITIES` maps to **nothing** on corp (`TPL-1`), so **assets == liabilities +
equity was never checked**. 79 items mapped and the cash decomposition is exact to the đồng, so
the corp template *reads* well — **the check that passed is simply not the check that matters.**

⚠️ **THE NON-BANK WALL IS AT LEAST FOUR THINGS, NOT ONE.** `TPL-1` named the seven bank-shaped
reconcile anchors; this run adds the page classifier missing the P&L outright, and the cash tail
mis-assigning a column. Both are now `CRP-1`. **Nothing from this run may be quoted as a
fundamental**, and `P5` is larger than the schema files made it look.

✅ **What the prototype DID establish is that the machinery around the parse works on a ticker
it has never seen**: the template resolved and said how, the empty band was WARNED rather than
silently accepted, the 47-layer escalation and its parse cache behaved, and the artefact
carries enough to convict the cash flow **from the artefact alone** — no PDF, no re-run.

---

### ⚠️ 6-2-duodetricies. THE TWO MACHINES WERE ALIGNED — and aligning them BROKE the worker first

Done 2026-08-28 on instruction, after §6-2-septvicies. `src/web_scraper/requirements-ocr.txt`
is now the single source of truth for the OCR stack: `mt_env` installs it by hand and
`RUN__pdf_ocr.ipynb` installs **the same file** on a Kaggle worker. ⚠️ **Every line in it
changes pixels, boxes or characters** — the rasteriser (`pymupdf`), the DB detector
(`onnxruntime`), the recogniser (`vietocr`), the resize/normalise (`opencv`), the polygon
unclip (`shapely`/`pyclipper`) and the arrays between them (`numpy`).

#### ⚠️ THE FIRST ATTEMPT PINNED BOTH MACHINES TO ONE VERSION, AND THE WORKER STOPPED PARSING

`onnxruntime-gpu 1.20.1` — this machine's version — **is not published on PyPI** (the index
offers 1.20.0 and 1.20.2 and no 1.20.1), so aligning meant moving BOTH sides. ✅ The local move
to **1.20.2** was guarded rather than assumed: VCB Q1-2026 re-parsed and reproduced **98 of 98**
cells written under 1.20.1. The 750 MB 1.20.1 tree is archived at `D:\GIT\_archive\`, because
pip cannot fetch it back.

**On the worker the same version did the opposite.** Four T4 runs of that one filing, opencv
varied deliberately to exonerate it:

| onnxruntime | opencv on the worker | winning layer | verdict |
|---|---|---|---|
| **1.22.0** | Kaggle's own 4.13.0.92 | `onnx@200` | ✅ **REPRODUCED 98/98** |
| 1.20.2 | headless upgraded to 5.0.0.93 | `onnx@300` | ❌ DIFFERS |
| 1.20.2 | `opencv-python` upgraded to 5.0.0.93 | `onnx@300` | ❌ DIFFERS |
| 1.20.2 | **both left at Kaggle's 4.13.0.92** | `onnx@300` | ❌ DIFFERS — **opencv exonerated** |

⚠️ **AND THE FALLBACK LAYER WROTE WRONG NUMBERS THAT BOTH GATES ACCEPTED.** `onnx@300` produced
a **row-slid income statement**: `x_chi_phi_du_phong_rui_ro_tin_dung` holding
`ix_loi_nhuan_thuan…`'s **14,295,755 mn**, four cells each carrying their neighbour's figure —
`SLD-1`'s shape, reached by a library downgrade rather than by a scan. `reconcile` passed and
`sane` passed. **Only `compare()` against the 98 cells on disk caught it**, which is the whole
argument for scoring a run against a baseline instead of trusting a green finish.

⚠️ **BOTH VERSIONS ARE INSIDE THE CUDA 12 / cuDNN 9 LINE `ORT-1` PRESCRIBES.** "Supported" does
not mean "equivalent". That is `ORT-2`.

#### ✅ THE CONFIGURATION THAT SHIPS, AND THE RULE IT EARNED

**Pinned exactly, verified reproducing on both:** `pymupdf==1.28.0`, `vietocr==0.3.13`,
`shapely==2.1.2`, `pyclipper==1.4.0`, `numpy==2.2.6`, `einops==0.2.0`.
**Pinned as a LINE:** `onnxruntime-gpu>=1.19,<1.23` — Windows takes 1.20.2, Linux 1.22.0, and
each is the version measured to reproduce there.
**Recorded, not pinned:** `opencv` (Kaggle ships BOTH distributions at 4.13.0.92 and this
machine has both at 5.0.0.93, so a pin moves one and leaves the other — a collision, and
nothing has verified that moving either REPRODUCES), `torch`, the Python patch level, the OS.

⚠️ **"SAME VERSION" IS NOT THE GOAL; "SAME OUTPUT" IS.** Aligning the version number is what
made the outputs diverge. **Pin only what is verified to reproduce on BOTH machines, pin the
supported LINE where a version is not, and let the fingerprint carry the rest.**

`pdf_ocr_job.engine_report()` now records the whole stack plus a 12-character
`stack_fingerprint` and `pin_violations` into every `metadata.json` — **reported, never
enforced**: a worker that could not honour a pin has still done work worth collecting, and what
must not happen is the mismatch going unnoticed. **Final state, interleaved and verified:**
local `f5103a6f5ae2`, T4 `88df8ef02c08`, **both REPRODUCED 98/98 at `onnx@200`**, zero pin
violations, and four packages still differing — all four in the fingerprint.

⚠️ **Two of my own defects on the way, and both are the same shape.** `source.zip` shipped only
`.py`, so the pins file would not have reached the worker at all (fixed: `.txt` joined the
list, exactly one 8 KB file). And the install cell reconstructed `==` pins from the file and
installed only those — so the moment `onnxruntime-gpu` became a RANGE it was **silently never
installed**, and the run died at the import check **blaming a missing internet connection it
had**. It now hands pip every line of the file. *A tool that reconstructs its input instead of
using it will disagree with it the first time the input changes shape.*

### ⚠️ AND THE T4 IS SLOWER THAN THIS LAPTOP — measured twice, interleaved

§6-2-sexvicies withdrew every timing claim because the local clock moved 2.25× unexplained
between runs taken hours apart. The fix was to INTERLEAVE, and two pairs have now been run
that way:

| document | local (RTX 3050) | Kaggle (T4) | |
|---|---|---|---|
| **VIC Q1-2026** (hard, 47/47 layers) | 31m 53s | 28m 03s | ⚠️ **but the T4 entered 45 layers, not 47** |
| **VCB Q1-2026** (easy, aligned stack) | **48.0 s** | **68.3 s** | **1.42× — the laptop** |

⚠️ **THE VIC WALL CLOCK FLATTERS THE T4 BECAUSE THE WORKER SKIPPED WORK**: Tesseract is not
installed there, so `tesseract@200` and `tesseract@400+relax` — which this machine ran at 1.57
and 2.90 s/page — never happened. Inverting `Progress.page`'s own ETA formula
(`rate = left / (total − i − 1)`) recovers the per-page rate of every pass from the logs, and
on the **20 onnx passes both machines actually ran**: local **0.62-0.68 s/page** against T4
**0.78-0.95**, median **local/T4 = 0.80×**.

✅ **So the T4 is ~1.25-1.4× SLOWER per page for this workload**, measured on two documents,
interleaved, once with the stacks aligned. It is a 2018 Turing part (sm_75) against an Ampere
laptop chip (sm_86); what the T4 has more of is **VRAM**, and VietOCR batching 24 crops does not
need it. ⚠️ **This does not make the Kaggle route worthless — it re-prices it.** What it buys is
**a second machine running in parallel with this laptop, free, without occupying it**, and
§6-2-sexvicies already said a multiplier was the wrong thing to budget on.

---

### ✅ 6-2-undetricies. THE TWO ENVIRONMENTS, ALIGNED 9 OF 10 — and the tenth is measured-impossible

Done 2026-08-28, immediately after §6-2-duodetricies, and it **withdraws that section's
range-pin**. Final state, verified by an interleaved pair that both REPRODUCE VCB Q1-2026's
98 cells at `onnx@200`:

| | local | Kaggle | |
|---|---|---|---|
| `onnxruntime-gpu` · `pymupdf` · `vietocr` · `opencv-python` · `opencv-python-headless` · `shapely` · `pyclipper` · `numpy` · `einops` | | | ✅ **identical, 9 of 9** |
| **`torch`** | 2.5.1+cu121 | 2.10.0+cu128 | ❌ **cannot be aligned — measured** |

⚠️ **THE FIRST ATTEMPT FAILED BECAUSE I ONLY TRIED ONE DIRECTION.** §6-2-duodetricies concluded
*"pin the LINE, each platform keeps its own version"* after moving Kaggle DOWN to this
machine's 1.20.2 broke the worker's parse. **Moving this machine UP to Kaggle's 1.22.0 was
never tried.** It works: VCB Q1-2026 still reproduces 98 of 98 here under 1.22.0, so both
machines now hold **one** version. ⚠️ **The finding is about the method, not the number: *"we
cannot align"* was an inference drawn from a single direction, and the other direction cost
four minutes.** The same treatment then aligned opencv — BOTH distributions to Kaggle's
4.13.0.92, because Kaggle ships both and either can win the `cv2` import, so pinning one and
leaving the other is a collision rather than an alignment.

#### ❌ `torch` CANNOT BE ALIGNED, AND THAT IS NOW A MEASUREMENT

Pushed once with `ALIGN_TORCH=True` and `torch==2.5.1+cu121 / torchvision==0.20.1+cu121` from
`requirements-ocr-torch.txt`: pip reported success, **the very next cell still printed
`torch 2.10.0+cu128`** — a running interpreter cannot be handed a different torch, its shared
objects are already mapped — and the kernel then **died with `DeadKernelError` after 4.7
minutes**. Doing it properly would need the install before any import *and a restart*, which a
papermill batch run does not offer. The file is kept, **off**, so the next person does not
spend a run learning it.

### ⚠️ SO: CAN THE TWO ENVIRONMENTS BE MADE COMPLETELY IDENTICAL? **NO — and four things remain**

| residue | why |
|---|---|
| **torch** 2.5.1+cu121 vs 2.10.0+cu128 | measured above. Aligning UP would also invalidate every model run in this repo, which records `env_fingerprint` for exactly that reason |
| **Python** 3.12.10 vs 3.12.13 | Kaggle's image, not choosable |
| **OS** Windows vs Linux | not choosable |
| **GPU** RTX 3050 (sm_86) vs Tesla T4 (sm_75) | not choosable, and the deepest of the four |

⚠️ **THE GPU IS THE ONE THAT WOULD SURVIVE EVEN A BYTE-IDENTICAL LIBRARY SET.** cuDNN and
onnxruntime select different kernels per architecture, and a different reduction order is a
different floating-point result. So "identical environment" is not merely unreached here — it
is **unreachable** while the two machines have different silicon.

#### ⚠️ AND CHASING IT IS THE WRONG INVARIANT — both halves are now measured

**Version identity is neither necessary nor sufficient for output identity**, and this session
produced one measurement of each:

| | |
|---|---|
| **not necessary** | ort **1.20.1 local vs 1.22.0 Kaggle** — mismatched — **REPRODUCED 98/98** (§6-2-sexvicies) |
| **not sufficient** | ort **1.20.2 on both** — identical — **DIVERGED**, and the fallback layer wrote a row-slid income statement both gates accepted (`ORT-2`) |

✅ **The invariant that holds is VERIFIED-EQUAL OUTPUT, and this repo already has the machinery
for it**: `compare()` scores every parsed cell against the statement CSV on disk, and
`stack_fingerprint` makes any residual difference visible rather than absent. Alignment is
worth doing — it removes whole classes of drift, and 9 of 10 is a much narrower place to look
when something moves — but it is a **narrowing of the search space, not a proof**. The proof is
the 98 cells.

---

### ⚠️ 6-2-tricies. IS THE VCB OUTPUT IDENTICAL ON BOTH MACHINES? **NO — one diacritic, and it is harmless**

Asked and measured 2026-08-28, after §6-2-undetricies aligned 9 of 10 libraries. ⚠️ **The
question could not be answered by anything already recorded**: `compare()` scores each run
against the statement CSV on DISK, so two runs both reading `REPRODUCED` agree on the **98
mapped cells** and say nothing about the rest of the statement. `pdf_ocr_job` now records
`rows`, `rows_sha` and a `row_dump` per statement — every line the OCR read, mapped or not.

| statement | rows read | mapped | identical? |
|---|---|---|---|
| balance sheet | 72 | 59 | ✅ |
| income statement | 29 | 22 | ✅ |
| **cash flow** | **32** | 17 | ❌ **one row** |

**The whole difference, located from the artefacts alone:**

| | |
|---|---|
| local | `Các kho**ản** tiền gửi của khách hàng` |
| Kaggle | `Các kho**àn** tiền gửi của khách hàng` |

⚠️ **ONE TONE MARK, ON AN UNMAPPED LINE, AND NO FIGURE DIFFERS ANYWHERE.** Every `values` list
of every row is identical on both machines; only that label's text moved. So the honest answer
is *"not byte-identical, and identical in every number"*.

✅ **AND THE REASON IT CANNOT PROPAGATE IS STRUCTURAL, NOT LUCK.** A row is matched on its
`key`, which is `slug(label)` — accent-stripped ASCII — and both labels slug to
`cac_khoan_tien_gui_cua_khach_hang`. **A tone-mark misread cannot move a figure**, because
nothing downstream reads the accented text. That is worth knowing before anyone tries to
"fix" it.

⚠️ **IT IS A RECOGNITION DIFFERENCE, AND THE REMAINING RESIDUE EXPLAINS IT — but which part of
the residue is not established.** Detection is onnxruntime, now the same version on both;
recognition is VietOCR under **torch**, which cannot be aligned (§6-2-undetricies), running on
**different silicon** (sm_86 against sm_75). Either would do it and this measurement does not
separate them.

⚠️ **THE ARTEFACT COULD NOT LOCATE THIS UNTIL IT WAS MADE TO.** The first cross-machine check
carried only `rows_sha`, so it reported *that* the cash flow differed and not *where* — and
finding out cost another run on each machine. §6-2-quindecies' lesson, one stage over: the
parser computed the rows and the artefact threw them away. `row_dump` is ~10 KB per document
against a re-run measured in minutes on two machines.

---

### ✅ 6-2-untricies. THE `corp` ANCHORS — `TPL-1` FIXED, and the defect was DUPLICATION

Done 2026-08-28. `CRP-1` recorded the first non-bank parse writing a wrong cash figure that
every gate passed; this is what was actually wrong. ⚠️ **It is bigger than the seven `C_*`
tuples `TPL-1` named**, and the extra term is the one that explains why nobody noticed.

⚠️ **`ANCHORS` WAS A HAND-WRITTEN LITERAL DUPLICATING THE ROLE TUPLES.** `_anchor` — the
position-independent re-match that exists because *"the ordered walk drifts"* — filters
`if c in self.ANCHORS`, and that list held **nine bank column names**. So on the `corp` chart
it re-matched **2 of 7 roles** (only the two grand totals, whose names happen to be shared)
and on `insurance` the same 2. Total liabilities, equity, PBT, the net cash movement and the
closing balance had **no position-independent recovery at all** on any non-bank filing.
`ANCHORS` is now DERIVED from the role tuples, so a column added to a role cannot be missed
here again.

**Three defects, each measured on VIC Q1-2026 before and after:**

| | before | after |
|---|---|---|
| `_anchor` roles resolved on `corp` | **2 of 7** | **7 of 7** |
| `_cash_flow_identity` | *"opening, movement, fx, closing not mapped"* — **not one of the four names it looks for exists in the corp chart** | **runs, and CLOSES**: 72,226,561 − 17,476,201 + 0 = 54,750,360 to the đồng |
| the closing cash balance | **54,750,360 mn in the FX column**, closing column empty | **54,750,360 in the closing column**, FX empty |
| `_probe` (what `sane` bands on) | **72,226,561 — the OPENING balance** | **54,750,360** |
| `find(*CASH_CLOSE)` on the opening row | returns **72,226,561** | returns **None** — a refusal |

✅ **CONFIRMED ON THE PRODUCTION PATH, NOT ONLY IN THE REPLAY** — `20260828-172722__hose_vic__pdf_ocr`,
the full 47-layer cascade, **31m 57s** on the RTX 3050. Cash flow accepted at `onnx@200`, 23
items: opening **72,226,561**, movement **−17,476,201**, **FX EMPTY**, closing **54,750,360**,
identity residual **exactly 0** — and the same filing's balance-sheet cash line reads
**54,750,360**, which is the independent corroboration. ⚠️ **It costs nothing in time**: the
same document took 31.1 and 31.9 min before the change, because the income statement still
forces every layer. ⚠️ **And the run's magnitude band was EMPTY on all three statements** (VIC
has no accepted quarter on disk), so `sane` failed open throughout and the job WARNed that it
would — the parse is right, but it was not GUARDED, and that is why nothing here may be
written to a statement CSV yet.

⚠️ **THE CASH DEFECT WAS A MERGED LABEL, NOT A MISSING ANCHOR, AND THE ANCHOR FIX ALONE DOES
NOT REACH IT.** The filing prints the FX line with **no figure**, so `table_rows` carried its
label forward and the closing balance's figures arrived under both labels joined — and `slug`
caps a key at 60 characters, which is exactly where *"…tương đương tiền cuối kỳ"* was cut off.
The row read as a pure FX line and mapped there correctly. ⚠️ **So the repair had to go in the
DEFAULT PATH**: VIC's cash flow is accepted at `onnx@200`, **layer 1 of 47**, and a late
cascade layer is never reached. `PGB-1` and §6-2-unvicies each recorded that trap from the
other side (*a half-right layer that passes the gates ends the cascade*); this is the first
time the conclusion was **when the gates cannot see the defect, the repair cannot be an
escalation**.

⚠️ **AND ADDING THE ANCHORS EXPOSED A LATENT HAZARD IN `_anchor` THAT COST TWO CELLS AT ONCE.**
Equity's account text is *"vốn chủ sở hữu"* — **11 characters, one over `MIN_CONTAINS`** — so
containment awards a flat 0.95 to any line that merely MENTIONS it, and the length-ratio
tie-break then prefers the SHORT impostor: *"Quỹ khác thuộc vốn chủ sở hữu"* (117,845 mn,
ratio 0.48) beat the real equity row (153,703,820 mn, ratio 0.32, its label polluted by page
header text OCR merged onto it), and `_claim` then evicted the account the impostor came from.
The guard is not a threshold: **an anchor may not take a row that fits another account of the
same chart STRICTLY better.** Strictly, because that is what preserves `_claim`'s own
documented case — ACB's Q1-2022 merges *"Dự phòng rủi ro khác"* with *"TỔNG NỢ PHẢI TRẢ"*, both
score 0.95, and the anchor must still win the tie.

✅ **THE BANK REGRESSION IS THE REASON THIS SHIPS: 15 statements across 5 filings of ACB, VCB
and BID re-map IDENTICALLY, under EVERY ONE of the 6 distinct mapping-flag combinations the 47
layers use — 90 of 90 mappings** (every value, the item count, both `reconcile` verdicts and
the `sane` probe). ⚠️ **The strict default alone was not enough to check**: `relax_totals`,
`relax_split_tail`, `relax_merged_seam` and `annual_tail` each change `map_to_schema`, and a
filing that escalates takes a path a strict-only regression never measured. The **6** that do
change are one statement — VIC's cash flow — under all six combinations. ⚠️ **And it cost minutes, not hours, because the ROWS were replayed rather
than re-parsed**: a mapping change cannot alter what the OCR read, so re-mapping a stored
`row_dump` (§6-2-tricies) or a single-layer probe measures the blast radius exactly. That is
the second thing `row_dump` has now paid for. **203 tests pass**, 96 of them new and none
needing a PDF, a network or an engine.

### ⚠️ What this does NOT fix — and one recorded cause that was wrong

1. ⚠️ **THE INCOME STATEMENT IS STILL ABSENT, AND `CRP-1`'s DIAGNOSIS OF IT WAS WRONG.** That
   entry read *"it is a `_page_kind` classification failure and not an OCR one"*. Dumping the
   page scan: pages 9 and 10 — the P&L, sitting between the balance sheet on 6-9 and the cash
   flow on 12-13 — come back with **25 and 5 words** against **94-169** on every neighbouring
   page, headers reading `I s / E / 3 / co`. **The OCR read almost nothing there, so the
   classifier had nothing to classify.** The pages are structurally identical to their
   neighbours (612×792, rotation 0, four DeviceGray strips each), so it is not geometry — and
   no further cause is established, so none is offered.
2. ⚠️ **`C_LIABILITIES` STILL MISSES ON `corp` IN THE FIELD, so a corp balance sheet still
   reconciles on the TRIVIAL identity** (`assets == resources`, true by construction). The row
   IS on the page — VIC's is **1,024,990,928 mn**, and 1,024,990,928 + 153,703,820 =
   1,178,694,748 = total assets **exactly** — but its label carries page-header text merged
   onto it and its account text `no_phai_tra` is **9 characters, below `MIN_CONTAINS = 10`**,
   so containment cannot reach it. Lowering that floor would re-open the hazard the new guard
   was just added for, so this is recorded rather than patched.
3. `insurance` still has **no closing-cash line in its chart of accounts** — §6-2-quaterdecies
   called that a schema repair and it still is.
4. ⚠️ **THE RELAXED CASH-TAIL RECOVERY IS INERT ON `corp`, AND SAFE ONLY BY ACCIDENT.**
   `_recover_totals` claims into `CASH_BALANCES`, which are BANK column names, so on a corp
   chart it would write a column the chart does not have — straight into `_write`'s `extra`.
   Measured on VIC's real rows: it never fires, because `CASH_TAIL`
   (`tienvacackhoantuongduongtientai`) and `CASH_PHRASE` are the BANK wording and corp prints
   *"tiền và tương đương tiền cuối kỳ"* — no *"các khoản"*, no *"tại"* — so the scan finds
   **0 rows** and all three of `onnx@200`, `+relax` and `onnx@300+relax` emit **0 columns
   outside the corp chart**. That is protection by vocabulary mismatch, the same shape
   §6-2-quaterdecies found protecting `bank`'s own cash tail. **The cost is coverage**: a corp
   cash flow that NEEDS the relaxed recovery gets nothing from it.
5. ⚠️ **ONE FILING, ONE QUARTER, ONE TEMPLATE.** `securities` and `insurance` anchors are
   verified against their CHARTS and have never met a filing. **Nothing from VIC may be quoted
   as a fundamental yet** — `CRP-1` stays open on point 1 alone.

### ⚠️ AND ADDING A TICKER TO THE PARSE TAKES **TWO** REGISTRATIONS, NOT ONE

Measured 2026-08-28 by two failed launches, ~40 s apart, with two DIFFERENT errors:

| step | what is missing | the error |
|---|---|---|
| 1 | `CAFEF_FINANCIALS_TICKERS` in `utils/constants.py` | `DagsterInvalidSubsetError: All selected assets must have a PartitionsDefinition containing the passed partition key` |
| 2 | `partitions -> raw/cafef_financials` in `orchestration/config.json` | `DagsterUnknownPartitionError: Could not find a partition with key` |

⚠️ **`dagster definitions validate` PASSES AFTER STEP 1 AND THE RUN STILL CANNOT START** —
the definitions are valid, the partition simply is not in the set. §5 rule 12 is why:
`enabled.register` FILTERS the constant's list through `config.json`, and **absent = OFF**,
so a ticker present in the constant and absent from the config is silently unaddressable.
That is the same shape the `unified` block's own comment warns about (*"a new ticker must
appear here AND in UNIFIED_PARTITIONS or it is silently unmaterialisable"*), and it applies
to `raw/cafef_financials` too. ⚠️ **The constant's comment says *"Add, never substitute"***
— `build_templates_index` REWRITES `templates.csv` from exactly that list, so a ticker
dropped from it loses its template mapping while its statement CSVs survive.

⚠️ **VIC's statements land under `statements/corp/`, a directory that did not exist**, so the
parse feeds NO existing silver asset: `silver/cafef_financials_bank` and the three after it
are bank-shaped by name AND by chart of accounts. Parsing VIC changes no existing table, and
carrying `corp` up is a separate ingest that has not been written.

⚠️ **THE LESSON, and it is `SAN-1`'s and `DEP-1`'s in a third shape: a check that CANNOT RUN
is not a check that passed.** `_cash_flow_identity` reported *"opening, movement, fx, closing
not mapped"* on a statement that had mapped three of the four — the names it was looking for
simply did not exist in that chart — and that message is indistinguishable from a genuinely
unreadable statement. §5 rule 2 is written for an absent NULL; this is an absent GATE, wearing
the same words as a failed one.

---

### ⚠️ 6-2-duotricies. THE FIRST AUTHORITATIVE NON-BANK RUN — VIC, STOPPED AT 27 OF 72, and the BALANCE SHEET only reads an ASSURED filing

Launched 2026-08-28 20:01 through `raw/cafef_financials` partition `HOSE_VIC`,
`skip_existing: false` `allow_parent: true`, **no `periods`** — the full authoritative shape,
which is what makes `sane`'s magnitude band the run's own rather than a subset one's. **Stopped
by hand on 2026-08-29 at 08:39**, after **12 h 00 m** and **27 of 72 consolidated quarters
(37.5 %)**, Q2-2008 … Q4-2014.

⚠️ **STOPPING IT COST NOTHING, AND THAT WAS DESIGNED IN.** `_write` snapshots after every
quarter on a full run, so the three CSVs hold 27 complete rows each, written at 08:02:26 with
the last quarter that finished. **A killed run is inspectable; it is not a partial row.**

| | balance sheet | income statement | cash flow |
|---|---|---|---|
| `pdf` | **13** | **21** | **20** |
| `missing` | 14 | 6 | 7 |
| `cafef` (HTML) | **0** | **0** | **0** ✅ |
| line items | 66 | 22 | 34 |

**54 of 81 cells = 66.7 %**, and rule 24 holds with no special handling — `use_api` now defaults
to `False`, so not one row came from a web tab. 8 quarters triggered the alternate-filing retry
and **4 recovered** by it. Winning layers are cheap: `onnx@200` took 42 of the 54, `onnx@300`
7, `tesseract@200` 2, and one income statement needed `onnx@300+pad6+annual+extra` — layer 45.

#### ⚠️ THE BALANCE SHEET SPLITS ON ASSURANCE AND THE OTHER TWO STATEMENTS DO NOT

This is the finding, and the control is what makes it one: if the quarterly filings were simply
worse scans, all three statements would fail on them together.

| parse rate | audited / reviewed | **unaudited (self-prepared quarterly)** |
|---|---|---|
| **balance sheet** | **12 / 13 = 92 %** | **1 / 14 = 7 %** |
| income statement | 10 / 13 = 77 % | 11 / 14 = 79 % |
| cash flow | 7 / 9 = 78 % | 13 / 18 = 72 % |

**The income statement and the cash flow do not care. The balance sheet cares completely.** The
13 that parsed are every FY annual and every Q2 half-year review from Q4-2008 on; the one
exception each way is `Q1-2011` (unaudited, parsed) and `Q4-2010` (audited, missing).

#### ⚠️ AND THE CAUSE IS THE COMPARATIVE COLUMN — evidence, not inference

`sane`'s equality gate fires on the quarterly filings and **repeats one figure across
consecutive quarters**, which is not something a going concern's balance sheet does:

| quarters refused | probe read |
|---|---|
| Q1-2009, Q3-2009, Q4-2009 | **6.02e+12 — the same number three times** |
| Q1-2010, Q3-2010 | **1.43e+13 — the same number twice** |

A balance sheet prints the prior year-end beside the current period, and that prior column is a
quarter this run had **already accepted** — so the gate recognised its own earlier figure coming
back. The remaining refusals are `assets != liabilities + equity` (11, the most common of all)
and `no total assets` (3), both consistent with two columns being mixed. ⚠️ **The two columns
were not read off the PDF page to confirm it**, so the mechanism is strongly indicated and not
verified; what IS measured is the assurance split, the repeated probes and the refusal mix.

⚠️ **`sane` IS THE ONLY GATE THAT CAUGHT THIS.** `reconcile` passed several of these — a
comparative column is internally consistent, so assets equal resources within it. This is the
second field case (after BID Q3-2016) of the equality gate catching a whole statement taken from
the wrong column, and the first where it fires systematically rather than once.

#### ⚠️ WHAT THE NEXT RUN MUST NOT DO

**Resume is not available and must not be improvised.** Re-running with `skip_existing: true`,
or with `periods`, makes it a SUBSET run — and this repo has measured four separate builds where
that flipped `sane` to failing open and silently downgraded a quarter (§6-2-vicies,
§6-2-unvicies, §6-2-quatervicies, §6-2-quinvicies). The 27 quarters on disk cost 12 h; **the
correct next action is the same full run again, from Q2-2008, once `P5`'s remaining half is
fixed** — otherwise it re-earns the same 14 missing balance sheets at the same price.

⚠️ **This changes `P5`'s shape, not only its size.** §6-2-untricies fixed the anchors on one
quarter of one filing; 27 quarters say the anchors were never the balance sheet's problem — the
column choice is. And `P38`'s cost model gains a data point: **26.7 min/quarter** averaged over
the run, **38.3 min** over the last eleven, on a ticker whose documents grow with the years.

⚠️ **Nothing here may be quoted as a fundamental.** VIC has no accepted quarter behind it, so
`sane` built its band inside this run from the first quarters it accepted — and those are the
2008-2009 filings, the least reliable in the set. The `corp` template still reconciles a balance
sheet on the trivial `assets == resources` (`CRP-1` point b), and the income statement's own
structural failure is untouched.

---

### ✅ 6-2-tretricies. VIC Q3-2014 PARSED — the "Mã số" column was read as a period, and the FIGURES were split in two

Asked and done 2026-08-29. One quarter, **two independent defects**, and the second is the more
dangerous — it produces WRONG NUMBERS that every gate passes. The balance sheet had been refused
on all 45 layers, twice on a T4 and once here, with one reason: `assets != liabilities + equity`.

#### ⚠️ `MSO-1` · assets read **270** and resources **440** — the VAS item codes

The standard corporate form **B01-DN** prints `Chỉ tiêu | Mã số | Thuyết minh | Số cuối kỳ | Số
đầu năm`, and `value_columns` separates a note column from a period column by MEDIAN DIGIT COUNT
(`NOTE_MAX_DIGITS = 2`). An item code is **3 digits, sometimes 4** (3131, 3161) — a note is 1-2
and a figure 4-14 — so the code sits **exactly in the overlap and no threshold can cover it**.
Measured on page 4: the code column clusters at **x=279.7 of a 595pt page, 47 %, inside the
right-60 % value zone**, 86 numbers of which 79 three-digit. It became column 0 and
`_first_value` returned every row's item code as its figure.

⚠️ **AND THIS IS NOT A QUIRK OF ONE FILING — IT IS THE FORM 761 OF 781 LISTED NAMES FILE ON.**

⚠️ **THE GATES CATCH IT ON A BALANCE SHEET AND NOT ON THE OTHER TWO.** An income statement only
has to present a PBT line and `50` is one; a cash flow only a closing balance and `70` is one.
Both are then left to `sane`, which **fails open on a ticker with no accepted history — which is
every non-bank ticker on its first run**. ⚠️ **It is already on disk**: VIC **Q1-2011** carries
`a_tai_san_ngan_han = 100`, `b_tai_san_dai_han = 200`, `i_no_ngan_han = 310` and
`ii_no_dai_han = 330` **as figures**, because both its grand totals happened to read correctly
and `reconcile` passed.

✅ **Fixed in the DEFAULT path, by reading the HEADING** — §6-2-untricies' rule, *when the gates
cannot see the defect the repair cannot be an escalation*, and the same thing `parse` already
does for the quarter column rather than counting columns. `'Mã số'` is read cleanly at
x0=261.0-286.2 with the column's right edge at 279.7, inside it. Three conditions, **each failing
SAFE**: a whole word box normalising to *maso*, a detected column under it, and that column being
the LEFTMOST — the form's own layout, so a mis-read heading cannot reach past a figure column.

#### ⚠️ `SPL-1` · and then `onnx@200` handed back **60 figures in two boxes each**

Dropping the code column made the statement reconcile — with `i_1_tien` reading **158,154**
against a printed **945,186,158,154**. The detector had split one figure into two boxes:
`'5.209.108'` ending at x=405.7 and `'954.978'` starting at x=409.5, **3.8pt apart**. The left
half lands on no column and is dropped; where enough left halves line up they instead form a
SPURIOUS COLUMN (x=498.2, n=34) and are kept as a period of their own.

⚠️ **BOTH GRAND TOTALS SURVIVED WHOLE**, so `reconcile` passed and `sane` probed a correct total.
**`SLD-1`'s shape a fourth time: a wrong figure that passes every gate** — and had I merged the
first run, 42 corrupted line items would have gone to disk as `pdf`.

⚠️ **IT IS RESOLUTION-DEPENDENT, WHICH IS THE WHOLE FIX.** The identical document at **onnx@300
splits NOTHING** — 2 clean columns instead of 3, identical grand totals. So accepting at layer 1
was `PGB-1`'s *"a half-right layer that passes the gates ends the cascade"* for the third time.

✅ **Fixed by REFUSING, not repairing.** `PdfParser.split_figures` counts split pairs from
geometry and text alone — deliberately **NOT** keyed on the detected columns, because the
fragments create their own column and any test asking *"is this box on a column?"* answers yes
for the very fragments it is hunting. `reconcile` then returns *"N figure(s) split across two
boxes"*, and the cascade escalates.

| the gap, measured before it was chosen | statements | split pairs at **4.5pt** | at 6pt |
|---|---|---|---|
| VCB Q1-2021, VCB Q1-2026, ACB Q1-2024, BID Q4-2016 — **all parse today** | 12 | **0** | 1-2 each |
| **VIC Q3-2014 @ onnx@200** | 2 of 3 | **60 + 27** | same |
| **VIC Q3-2014 @ onnx@300** | 3 | **0** | 0 |

#### ✅ AND THE MERGE SHIPPED TOO, ONCE IT HAD A MEASUREMENT — same day, on request

The refusal was the whole change for about an hour, on the argument that a merge *"rewrites the
OCR text of every page of every filing in the corpus, and four bank filings is too thin a base
for that"*. **The base was widened to 19 filings and the argument did not survive it.**

`PdfParser._merge_split_figures` re-joins a pair at the OCR seam when the gap is under 4.5pt, the
right box begins with a **full three-digit group** (a continuation always does; a fresh figure
usually does not) and the join is a well-formed thousands-grouped figure. It is confined to the
VALUE ZONE — not because a label cannot hold two adjacent numbers, but because that is the half
of the page the measurement covers, and shipping wider than the measurement is how a rule with
0 false positives acquires some.

| measured over 19 filings / 53 statements | |
|---|---|
| statements untouched | **47** |
| statements whose cells moved | **6 — every one of them VIC** |
| **across 21 BANK statements** (VCB ×5, ACB ×4, BID ×4, three reports each) | the merge fires **once** and changes **no mapped cell** |

✅ **AND THE REPAIR IS SCORED AGAINST A GROUND TRUTH RATHER THAN ARGUED.** onnx@300 reads VIC
Q3-2014 with 0 splits, so it is what 200 dpi ought to agree with. Raw, they agree on **29 of 45**
balance-sheet cells; with the merge, **43** — and the column that matters is the other one:

| VIC Q3-2014, 200 dpi vs the 300 dpi truth | repaired | **broken** |
|---|---|---|
| balance sheet | 14 | **0** |
| income statement | 3 | **0** |
| cash flow | 0 | **0** |

⚠️ **THE MERGE DOES NOT RETIRE THE GATE, AND THAT IS THE DESIGN.** VIC Q3-2014's balance sheet
still carries **6 unmergeable splits** at 200 dpi — three-way breaks the pair rule cannot prove —
so it still fails the gate and still escalates to onnx@300. Repair what is provable, refuse the
rest: the two together are why the run below settles on the clean reading rather than a plausible
one.

⚠️ **ORDER: MERGE FIRST, THEN SPLIT, AND IT IS NOT COSMETIC.** `_split_number_runs` apportions a
multi-figure box by CHARACTER OFFSET, so the gap it leaves between two pieces is
`box width / len(text)` — **5.7pt** on the measured case and therefore outside `MERGE_MAX_GAP`,
but **4.3pt at a 100pt box**, inside it, at which point a merge running afterwards joins the
splitter's own pieces straight back together. ⚠️ **I wrote the test for this the other way round
first — asserting the pieces are contiguous — and the measurement said 5.7pt.** The claim was
wrong and the ordering is right anyway; the test now pins the ratio.

#### The result, and every check it passed

| | |
|---|---|
| run | `pdf_ocr_job --symbol VIC --quarters 2014-Q3 --template corp`, **1m 52s** with the merge (3m 42s without it), RTX 3050 — against **23 min** before, because the cascade now stops early |
| layers won | balance sheet **`onnx@300`** · income statement `onnx@300` · cash flow `onnx@200` |
| **the two statements already on disk** | **REPRODUCED** — 16/16 and 19/19 cells, same layer, same unit, same `publish_date` |
| balance sheet | **45 items**, `source='pdf'`, unit đồng, consolidated, `publish_date` 2014-11-14 |
| the merge | `pdf_ocr_merge` planned **1 WRITE, 2 skips**; pre-merge backup taken; diffed **column by column** across all three CSVs — **exactly one period changed**, 0 columns lost |
| VIC balance sheet | 13 → **14** parsed of 27 |

✅ **Five internal identities close to the đồng, and none of them is the one `reconcile` tested**:

```
tiền 945,186,158,154 + tương đương 4,263,922,796,824 = 5,209,108,954,978
TSCĐ hữu hình:  7,702,724,753,030 − 1,386,768,347,720 = 6,315,956,405,310
TSCĐ thuê TC:     573,019,861,690 −    86,380,449,572 =   486,639,412,118
tồn kho:       13,612,624,455,291 −     5,413,420,000 = 13,607,211,035,291
A 36,550,263,468,338 + B 46,241,674,807,211 = 82,791,938,275,549 = TỔNG TÀI SẢN = TỔNG NGUỒN VỐN
```

⚠️ **What this does NOT do.** **VIC Q1-2011 is still wrong on disk** — repairing it needs a
re-parse and `force_differs`, and was not done. `CRP-1` point 1 (the income statement's pages
come back with 25 and 5 words) and point 2 (`C_LIABILITIES` unreachable on `corp`, so a corp
balance sheet still reconciles on the trivial `assets == resources`) are untouched. And the other
44 VIC quarters the stopped run never reached are unaffected — ⚠️ **but the next authoritative VIC
run will now read them differently, and that is the point**: `MSO-1` was silently corrupting the
non-bank corpus before any of it had been carried up.

#### ⚠️ AND THE BATCH FILTER IS A QUARTER NOW, NOT A YEAR — `YYYY-QQ`

`pdf_ocr_job.plan`, `JobSpec`, the CLI (`--quarters 2014-Q3`), `kgpu`'s `data.documents.quarters`,
both notebooks and the payload/parameter cross-check all moved together. The unit was a YEAR on
one argument — `orchestration` §2a records that the statement BUILD skips complete years, because
`_decumulate` needs Q1..Q(q-1) of the same year — and ⚠️ **that argument never bound this module**:
nothing here de-cumulates and nothing here writes, so the wider unit only ever bought extra OCR.
Asking for 16 quarters no longer opens the 27 filings of the seven years they fall in.

⚠️ **`YYYY-QQ` AND NOT THE REPO-NATIVE `QQ-YYYY`, AND THE OTHER FORM IS REFUSED RATHER THAN
ACCEPTED.** They name the same quarter and only one of them SORTS. A lenient parser would be easy
to write — and then a caller who used the wrong form would never find out, while a caller who made
a TYPO would get *"files no document for [...]"* and go looking at CafeF for a filing that is
sitting there. The form is checked before the corpus is, in `plan` **and** in `config._validate`,
where it costs nothing and saves a Kaggle round trip.

⚠️ **AND IT IMMEDIATELY EARNED ITS KEEP TWICE.** Asked for VIC's 17 quarters that still hold a
`missing` cell, `plan` **refused 2008-Q3** — VIC files no document for it at all, so those three
`missing` cells are the correct answer and a year filter would have opened Q2 and Q4 of 2008
silently instead. And the first rehearsal of the first `quarters` job exposed **`RHS-1`**:
`rehearse` passed only `periods_requested` to `plan`, never the batch filter, so it compared a
filtered shipment against an UNFILTERED `documents()` and tripped its own assertion. ⚠️ **That was
latent for as long as the batch filter existed** — `years` had the identical defect and nothing
ever exercised it, because every documents job rehearsed until now was narrowed by `periods`
alone. **A check that reconstructs its subject from a SUBSET of the inputs that built it will
disagree with it the first time an unused input is used.**

**258 tests pass** in `src/web_scraper/`, of which **28 new**, none needing a PDF, a network or an
OCR engine.

---

### ✅ 6-2-quattuortricies. THE TWO RUNS THAT PRICED THE FIX — VIC on a T4, TCB locally

Both on 2026-08-29, after `MSO-1` and `SPL-1` shipped. The first says the fix was worth it; the
second says what the OCR job is NOT for, and that is the more reusable half.

#### ✅ VIC, 16 quarters, Kaggle T4 — 3h 55m, SIX balance sheets recovered

The 16 quarters that still held a `missing` cell and for which CafeF has a filing.
⚠️ **`plan` refused a seventeenth — 2008-Q3, which VIC does not file at all** — so its three
`missing` cells are the correct answer, where the old year-granular filter would have opened Q2
and Q4 of 2008 instead. Every recovery is a balance sheet, which is exactly `MSO-1`'s damage
class:

| quarter | layer | items | | quarter | layer | items |
|---|---|---|---|---|---|---|
| Q3-2011 | `onnx@300+notes+seam` | 47 | | Q1-2013 | **`onnx@400`** | 47 |
| Q1-2012 | `onnx@300` | 45 | | Q3-2013 | `onnx@200` | 47 |
| Q3-2012 | `onnx@300` | 45 | | Q1-2014 | `onnx@200` | 44 |

Diffed column by column against the pre-merge backup: **6 periods changed, 0 columns emptied, 0
columns lost**; the income statement moved one cell (Q1-2011's `method`, `onnx@200` → `onnx@300`,
**no figure changed**) and the cash flow moved nothing. VIC's balance sheet is **14 → 20 of 27**;
quarters holding a `missing` cell **17 → 14**.

⚠️ **FOUR OF THE SIX CLOSE `A + B = TỔNG TÀI SẢN = TỔNG NGUỒN VỐN` EXACTLY; TWO ARE OUT BY ONE
ĐỒNG** (Q3-2011, Q3-2012, on 33-51 tn) — the filing's own rounding, inside `_equal`'s tolerance.
Said rather than rounded away.

⚠️ **Q1-2011's balance sheet was REFUSED and the refusal was right.** It differs from the disk row
in 61 columns, and the disk row is the one `MSO-1` corrupted (`a_tai_san_ngan_han = 100`). The new
parse replaces those item codes with real trillions — **and still does not close**:
`A + B = 27,618,524,857,111` against a total of 26,146,849,247,419, out by 1.47 tn. **Better is not
correct, and `force_differs` must not be used here.** The reason it passes every gate is `CRP-1`
point 2: `C_LIABILITIES` misses on `corp`, so `reconcile` only ever tests the trivial
`assets == resources` and never sums the components.

#### ⚠️ TCB, 59 filings, locally — 5h 21m, 95.5 %… and 9 of those cells were WRONG

The first ticker parsed since the two fixes, and the first NEW ticker in this repo since VIC.
59 consolidated filings, Q4-2009 → Q2-2026, template `bank`. **169 of 177 cells** — balance sheet
58/59, income statement **59/59**, cash flow 52/59 — with `onnx@200` winning **155** of them.

⚠️ **AND THE HEADLINE IS THE TRAP.** `seed_history` rebuilds the magnitude band from the `pdf`
rows ON DISK and re-seeds **per document** (`pdf_ocr_job.py:1047`); TCB had no CSV, so the band was
EMPTY for all 59 and **`sane` failed open from start to finish**. Two screens run over the
artefact afterwards — the work the band would have done — convicted **9 of the 169**:

| screen | what it found |
|---|---|
| **unit** — a statement whose `unit` is the minority for its report | **8 statements read `unit=1`** against the ticker norm of 1,000,000. TCB Q1-2014 PBT read **673,136** where the company earned **673 tỷ**: a uniform 10⁶ error, which `unit_of`'s own docstring says *reconciles perfectly against itself* |
| **continuity** — total assets quarter on quarter | **Q1-2013 = 17,586,290,323 tr** against ~178,000 tr either side, with the EQUITY line holding that same figure. Won at `tesseract@200` |

✅ **FIVE OF THE NINE WERE REPAIRED, NOT DISCARDED — 8m 58s.** The three layers carrying
`unit_from_document` sit at **41-43 of 47**, and these statements accept at **layer 1**, so the
cascade never reaches them: `PGB-1`'s half-right-layer trap, with the gate that would have refused
switched off by the empty band. Re-run with the cascade restricted to those three layers, all five
income statements came back at **exactly ×10⁶** — asserted as a ratio, because a genuine re-read
would not divide cleanly:

```
Q1-2014  673,136 -> 673,136,000,000 = 673 tỷ    Q3-2015  519,896 -> 519,896,000,000
Q3-2014  214,245 -> 214,245,000,000             Q1-2016  582,428 -> 582,428,000,000
Q1-2015  408,204 -> 408,204,000,000
```

⚠️ **Q3-2013 is NOT repairable that way and stays `missing`** — all three of its statements read
`unit=1` and the unit layers did not move them, so the filing declares no unit anywhere the parser
can find. With Q1-2013's balance sheet that is **4 cells deliberately withheld**.
✅ **THE FIRST HALF OF THAT SENTENCE HELD AND THE SECOND WAS WRONG — SUPERSEDED 2026-08-30,
§6-2-septtricies.** The unit layers indeed could not move it, because `document_unit` had nothing
to offer: **the filing declares `Đơn vị tính: triệu đồng` on page 2, and `_drop_islands` was
discarding page 2** (`ISL-1`). The quarter is on disk now, all three statements, at
`onnx@200` / `onnx@300+unit+tail` / `onnx@300+unit`. *"The filing declares no unit"* was an
inference from the parser's own output, and it read as a fact about the DOCUMENT.

**Written to `statements/bank/` on request**, with `force_empty_band=True` (unavoidable) and the
four convicted cells held back by a `periods`/`reports` filter rather than by editing the artefact:

| | quarters | `pdf` | `missing` |
|---|---|---|---|
| balance sheet | 67 | **56** | 11 |
| income statement | 67 | **58** | 9 |
| cash flow | 67 | **51** | 16 |

✅ **165 `pdf`, 36 `missing`, nothing else** — rule 24 holds. ✅ **0 jumps above 70 % across all 56
written balance sheets**, 92.6 → 1,273.1 nghìn tỷ from Q4-2009 to Q2-2026. ⚠️ **24 of the 36
`missing` are 8 quarters with NO FILING AT ALL** (Q1-Q3 2010, Q1-Q3 2011, Q1 & Q3 2012 — TCB filed
only annuals then), 8 are real parse failures, 4 are the withheld cells.

#### ⚠️ THE LESSON, AND IT IS ABOUT WHICH TOOL — `BND-1`

`build()` appends to `history` **as its own run proceeds** (`cafef_financials.py:2835`);
`pdf_ocr_job` re-seeds from disk per document and never accumulates. So **`pdf_ocr_job` is the
tool for REPAIRING a quarter on a ticker that already has history, and the wrong tool for
BOOTSTRAPPING a new one** — and the loop closes on itself, because `pdf_ocr_merge` refuses an
empty-band statement, so nothing is written, so the band stays empty. The authoritative path for a
new ticker is a full Dagster `raw/cafef_financials` run. ⚠️ **This is not a small caveat: 95.5 %
"parsed" concealed a 5.3 % wrong-figure rate, and every one of the nine was the shape `sane`
exists to catch.**

⚠️ **AND `templates.csv` IS STALE — `TPX-1`.** It holds ACB, BID and VCB and **not VIC**, which has
been in `CAFEF_FINANCIALS_TICKERS` since 2026-08-28 and is parsed; `build_templates_index` has not
been re-run since. So every other ticker resolves its template by a NETWORK call to CafeF —
harmless locally, already handled on Kaggle (`kgpu` resolves at export), and an offline route that
is quietly gone. ⚠️ **TCB is in neither register**, so its Dagster asset cannot be materialised and
its statements feed no silver ingest — parsing it changed no table.

---

### ✅ 6-2-quinquatricies. ONE NOTEBOOK, TWO MACHINES — and the run now survives being stopped

Shipped 2026-08-29. The OCR path had **two** entry notebooks and a write that happened only at
the end; it now has one entry point and a write that happens per quarter. Nothing about the
cascade, the gates or the 47 layers moved.

| | before | after |
|---|---|---|
| the notebook you open | `RUN__pdf_ocr_control.ipynb` (Kaggle) **or** `RUN__pdf_ocr.ipynb` (local) | **`RUN__pdf_ocr_control.ipynb` only** — one uppercase parameter cell, `ENVIRONMENT = "LOCAL" \| "KAGGLE"` |
| the batch filter's spelling | `2014-Q3` | `2014-Q3` **or** the zero-padded `2014-03`, folded at the edge |
| a quarter already parsed | re-OCR'd, then refused at the merge | **dropped before any OCR** unless `OVERWRITE` |
| the statement CSVs | written once, after the whole run | **upserted per quarter, as each finishes** |
| `src/web_scraper/` tests | 258 | **283** |

⚠️ **THE INTERRUPTION GUARANTEE IS THE POINT, AND IT IS A PROPERTY OF `_write`, NOT A NEW
WRITER.** `pdf_ocr_job.run` calls `pdf_ocr_merge.merge_run(periods=[this quarter])` between
documents, which calls `FinancialsBuilder._write(merge=True)` — the upsert `build()` itself
uses, rendering to a `.tmp` and `os.replace`-ing it. So **a 12-hour run stopped at hour 6 keeps
every quarter that finished** and can lose at most the one in flight. The backup is taken ONCE,
by the first quarter that actually writes something; seventy timestamped copies of three CSVs
answer *"what did this run change?"* worse than one.

⚠️ **AND THE THREE REFUSALS ARE WHAT MAKES AN AUTOMATIC WRITE DEFENSIBLE, NOT THE PER-QUARTER
SCHEDULE.** A cumulative income statement, an empty `sane` band and a figure that DIFFERS from a
good `pdf` row are still skipped and SAID. §6-2-sexvicies' four measured silent downgrades are
why the module's own default is still to write nothing: `--merge` is opt-in on the CLI and on in
the notebook, where a person has read the plan first.

⚠️ **`OVERWRITE` DECIDES AT BOTH ENDS, WHICH IS WHY IT IS ONE WORD.** `False` fills the GAPS —
`partition_by_disk` drops a quarter already reading `pdf` in all three statements before any OCR
**and before it is uploaded**, and the merge refuses to replace a `pdf` row that disagrees.
`True` re-parses and lets the merge write. ⚠️ **It reaches the merge on the Kaggle side too**:
without that, a re-parse asked for explicitly would come home and be refused, so the run would
do the work and disk would keep the old figure — the worst of both answers. `config._validate`
refuses a job whose `data.documents.overwrite` disagrees with `parameters.OVERWRITE`, the same
way it already refuses a quarters mismatch.

⚠️ **THE SKIP IS PER QUARTER HERE AND PER YEAR IN `build()`, AND THE DIFFERENCE IS MEASURED.**
`_skippable_years` keeps a year whole because `_decumulate` needs THAT run's Q1..Q(q-1) — so
dropping Q1..Q3 while keeping Q4 would delete the quarter the run exists to fix. **Nothing in
`pdf_ocr_job` de-cumulates**, and both `seed_history` and `open_reference` read from DISK, so no
quarter depends on another quarter of the same run. At 4-18 min a document, a year held whole
for one missing cash flow costs three filings that had nothing left to win.

⚠️ **A WORKER MAY NOT UPSERT, AND `run()` REFUSES RATHER THAN TRUSTING ITS CALLER.** On Kaggle
`CAFEF_DATA_ROOT` is an unpacked payload that dies with the kernel, so the write would edit a
copy and report success. `run()` compares the resolved root against the repo's own and turns the
flag off with a line in the log — **so on KAGGLE the per-quarter guarantee is the PULL's, not
the run's**, and that is stated in the notebook rather than implied.

**Tidy, same session**: the two `Logger`-shaped sinks became one (`Progress(CollectingLogger)` —
four `log_*` methods had two implementations, i.e. two places for one change); `plan_merge` now
reads a disk row through `pdf_ocr_job._line_items`, the same rule `compare()` scored it with;
`_documents(periods=…)` filters by FILENAME before parsing, so a 70-quarter run reads its own
folder once per quarter instead of 70 times; one dead import; one f-string with no placeholder.
**5.6 GB of `kgpu` staging deleted** — `.payload`, `.rehearsal`, `results`, `.build`, all four
named in `src/kaggle_gpu/.gitignore` as *"rebuilt by a command"*, plus four
`.tmp_dagster_home_*`, `.pytest_cache` and 107 `__pycache__`.

#### ⚠️ AND THE FIRST REAL RUN THROUGH IT WROTE WRONG FIGURES — `VCR-1`, found and contained the same hour

The verification run was chosen to be safe: VCB **Q1-2026**, the quarter §6-2-sexvicies measured
at **98 of 98 cells reproducing** at `onnx@200`, re-parsed with `--overwrite --merge` so that a
correct run would decide *"identical to the row already on disk"* and write nothing. It wrote
**13 changed columns across all three statements** instead.

⚠️ **THE CAUSE IS NOT IN THIS SESSION'S CODE AND IT IS NOT IN THE PARSER.**
`vietocr.tool.config.Cfg.load_config_from_name` fetches `base.yml` and `<arch>.yml` from
**`https://vocr.vn`** on EVERY `Predictor` construction and caches neither — a bare
`requests.get` — and **that host's TLS certificate has expired.** So all three `onnx@*` layers
raised `SSLError`, and the cascade did what a cascade does: it went on, and **`tesseract@200`
won a document that has read `onnx@200` for as long as it has been parsed.**

| | |
|---|---|
| what the log said | three `WARNING: … parse failed — SSLError` lines, then a normal-looking accept |
| what the gates said | `reconcile` ✅ and `sane` ✅ — a tesseract parse of a real filing is a real parse |
| what disk got | `i_cac_khoan_no_chinh_phu_va_nhnn` 198,629,540 mn → blank; `viii_1_a_von_dieu_le` 4,995,389 mn → 83,556,751 mn; 11 more |
| what saved it | **the automatic pre-merge backup** — restored, and all three md5s match the pre-run hashes exactly |

⚠️ **SO THE "OFFLINE" CLAIM WAS INCOMPLETE FOR AS LONG AS IT STOOD.** `use_models` ships the
DET model and the VietOCR **weights**; the recogniser's **config** was never local, on this
machine or in a Kaggle payload. Every onnx parse this repo has ever run depended on that host
being reachable — it simply always was.

✅ **CONTAINED TWO WAYS, AND THE FIRST IS THE GENERAL ONE.** `_parse_cascaded` now records every
layer whose parse RAISED (`layer_errors`), `run_document` carries them into the run folder as
`engine_errors`, and **`pdf_ocr_merge` refuses such a document WHOLE** — because **a refusal
measures the DOCUMENT and an exception measures the MACHINE**, and whatever wins after an
exception wins by default rather than on merit. Proven by re-running the exact command that did
the damage: three `skip` lines, **0 writes, md5 unchanged**. Second, `onnx_ocr` will load a
merged base+arch yaml from `CAFEF_ONNX_VIETOCR_CONFIG` when one exists and otherwise raises with
the cause named. ⚠️ **That file is NOT in the repo**: obtaining it needs a working certificate on
vocr.vn or a deliberate decision to fetch it once over unverified TLS, which is a judgement
about trust rather than a code change. **Until it exists, no onnx layer can run on this machine
at all** — the guard turns silent corruption into a loud stop, which is the right failure and
not a fix.

⚠️ **AND IT REPRICES `OVERWRITE`.** The knob is safe in the sense that every refusal still
applies to it — `force_differs` was never meant to be, and now is not, a way past a broken
engine. But the incident is the argument for `OVERWRITE = False` being the default and for
reading the plan before setting it: the run did exactly what it was told.

⚠️ **WHAT THIS DOES NOT CHANGE.** `BND-1` stands: `pdf_ocr_job` REPAIRS a ticker that already
has history and cannot BOOTSTRAP one that does not — an empty `sane` band fails open, and the
merge then refuses every statement it produced, so the loop closes on itself. `CRP-1` stands:
nothing from a non-bank template may be quoted as a fundamental. And **no figure already on disk
was re-measured or moved** — the one run that changed anything was reverted from its own backup,
verified by md5.

---

### ✅ 6-2-sextricies. `VCR-1` CLOSED — the config was in this repo all along, and the payload shipped TWO of THREE model files

The Kaggle control notebook raised on its first cell that spends nothing, and behind that one
line were three defects, each hiding the next. Found 2026-08-29 by running
`RUN__pdf_ocr_control.ipynb` at `ENVIRONMENT="KAGGLE"` for `HOSE_TCB 2013-Q3`.

| | what it was |
|---|---|
| **1 · the rehearsal had nothing to rehearse** | the notebook went `plan` → `rehearse` → `run`, and `rehearse` reads `.payload/<job>/`, which only `export` writes. **Every FIRST run of any job raised `FileNotFoundError: no staged payload`** — and the fix the message names, `python -m kgpu data <job>`, cannot resolve a job this notebook COMPUTED rather than wrote into `kaggle_config.json`. ⚠️ `PDF_OCR.md` §7's own Python snippet carried the same order and the same defect |
| **2 · the payload was one model file short** | `use_models` gained a third file — the recogniser's CONFIG — when `VCR-1` was contained hours earlier, and `kgpu.export._export_documents` is a SECOND hand-written list that nobody told. The rehearsal's own assertion was the only thing that said so |
| **3 · that config did not exist anywhere** | so `VCR-1` was live: with vocr.vn's certificate still expired (re-measured, still `CERTIFICATE_VERIFY_FAILED`), **every `onnx@*` layer raises on both machines** — and a Kaggle image has no tesseract to fall through to, so the run would have returned nothing after ~30 min of quota |

⚠️ **THE THIRD ONE WAS RECORDED AS A DECISION ABOUT TRUST AND IT WAS NOT ONE.** `VCR-1` and
`ISSUES.md` both said obtaining the file needed *"a working certificate on vocr.vn or a
deliberate decision to fetch it once over unverified TLS"*. There was a third route neither
considered: **vietocr's `base.yml` and `vgg-seq2seq.yml` are public on the project's own GitHub
over verified TLS — and a copy has been sitting in this repo since experiment 9**
(`experiment/experiment_9/vendor/deepdoc_vietocr/vietocr/config/`). The two are merged exactly
as `Cfg.load_config_from_name` merges them into
**`src/web_scraper/models/vietocr_vgg_seq2seq.yml`, which is TRACKED** — 3 KB, unlike the two
gitignored binaries beside it — so a fresh checkout parses without reaching vocr.vn at all.

✅ **VERIFIED BY BEHAVIOUR, NOT BY BYTES, WHICH IS THE ONLY CHECK THAT COULD SETTLE IT.** The
vendored copy differs from upstream in `pretrain`/`weights` — the checkpoint's download URL,
which `onnx_ocr` overwrites with the local `.pth` before use — and in nothing that touches
recognition. So the question *"is this the config the parsed corpus was read with?"* was
answered the way `ORT-2` says it must be: **re-parse VCB Q1-2026 and score it against disk —
98 of 98 cells REPRODUCED at `onnx@200`**, same layer, same unit, same `publish_date`, 1m 14s.

**What shipped**, and defect 2 is the reusable one:

- ⚠️ **`pdf_ocr_job.MODEL_FILES` names the three files ONCE**, and `use_models` and the
  exporter both read it. The exporter also RAISES when it would stage fewer than the constant
  names, so a fourth model file is a build error rather than a silent shortfall. *A payload
  built from a second copy of a list disagrees with the reader the first time the list moves.*
- the control notebook's rehearse cell **stages the payload first** (`export`, local, no
  upload — the RUN cell still re-exports and uploads), and says why in the cell.
- **3 tests** (290 in `src/web_scraper/`, none needing a PDF, a network or an engine): the
  constant is what `use_models` resolves, the tracked yaml is a COMPLETE base+arch config
  (`load_config_from_file` starts from an EMPTY dict, so an arch-only file would load and
  recognise with no vocab), and the exporter names every key of the constant.

✅ **Both mount layouts of the TCB payload now rehearse green** — 21 files, 85.9 MB,
`models: OK (deepdoc_det.onnx, vgg_seq2seq.pth, vietocr_vgg_seq2seq.yml)`.

⚠️ **WHAT THIS DOES NOT DO.** It spends no quota and parses nothing on Kaggle — the run cell is
the user's. It does not touch `BND-1`: TCB's band for Q3-2013 is **5/7/5 probes** from the
quarters before it, thin but not empty. And TCB Q3-2013 is the quarter `P46` is about — all
three of its statements read `unit=1` and the unit repair sits at layer 41 of 47 — so a green
round trip is not the same as a recovered quarter.

---

### ✅ 6-2-septtricies. TCB Q3-2013 — ONE QUARTER, FOUR DEFECTS, AND THREE OF THEM WROTE A WRONG FIGURE

Asked on 2026-08-29: make this quarter parse locally. It had read `missing` in all three
statements since the ticker was first parsed, and §6-2-quattuortricies had recorded why —
*"all three of its statements read `unit=1` … the filing declares no unit anywhere the parser
can find"*. ⚠️ **That sentence was wrong**, and the four defects behind it are the section.

**The filing DOES declare its unit.** Page 2 prints `Đơn vị tính: triệu đồng` above the table,
and the OCR reads it — verified by rendering the page and by reading it back out of the cached
OCR text. What no statement saw was the PAGE.

| | the defect | what it did |
|---|---|---|
| **`ISL-1`** | `_drop_islands` measured the gap in a **±1 window** around the form-coded pages | the balance sheet runs pages 2-4, only page 4 kept a readable form code, so **page 2 was dropped as an island while page 3, between them, was kept** — and page 2 is the only place in the filing that prints the unit |
| **`TCG-1`** | "TỔNG CỘNG TÀI SẢN CÓ" scores **0.769** against the chart's "TỔNG TÀI SẢN" | total assets did not map; and the grand total, which scores 0.929 for its own anchor, was taken by the **EQUITY** anchor on a flat 0.95 containment — 165,878,786 mn against a real 13,857,834 |
| **`MEN-1`** | a merged section header + line item, and the account is only MENTIONED in it | equity then fell to "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU \| II. Tiền gửi và vay các TCTD khác" and read **24,686,177 mn of interbank deposits**, while `_claim` evicted the deposits line |
| **`DEC-1`** | `parse_num` stripped `.` and `,` alike, and TCB's 2012 filings print `1,630,428.99` | **TCB Q2-2012's PBT is on disk as 163,042,899 mn** — 163 tn for a bank with 180 tn of assets — and that one row poisoned `sane`'s band until it refused Q3-2013's own income statement |

⚠️ **EVERY ONE OF THE FIRST THREE PASSES BOTH GATES.** `reconcile` falls through to
`Statement.find`, which reads the right rows out of the OCR text, so the grand totals are sound
while the written ROW carries the wrong figures — `SLD-1`'s shape for the fifth time. Only
`sane` refused this quarter, and only because the unit error is 10^6 rather than subtle.

#### What shipped, and what each fix was priced against

| | |
|---|---|
| `_drop_islands` walks the statement's **own contiguous run** before applying the ±1 tolerance | VCB Q2-2023's islands are still dropped — page 8 there belongs to no report, so the walk stops |
| `ABBREV` normalises `tongcong` → `tong` on both sides | **0 new account collisions** across all 12 charts (31 before, 31 after) |
| containment may not award its flat 0.95 to an account buried **inside** a merged row | **ANCHORS ONLY**: gating the ordered walk the same way changed **23 of 228** archived statements and lost sound cells, against **4** confined to `_anchor` |
| `parse_num` reads a 1-2 digit tail after the last separator as a DECIMAL | the character cannot be trusted — OCR confuses `.` and `,`, so "1,234.567" must stay 1,234,567 |
| two layers added, `onnx@300+unit+tail` and `onnx@300+unit`, and `unit_from_document` now demands the cash identity | **a layer that multiplies every figure by a million may not be the layer that skips the arithmetic** — at 200 dpi this cash flow reads its net movement as **205** where the page prints 2,989,205, with both gates passing |

**Two regressions, and both had to be run because the changes are in the DEFAULT path:**

1. **228 archived statements re-mapped** from their stored `row_dump` — a mapping change cannot
   alter what the OCR read, so this prices it exactly. **4 changed, every one a repair**: the two
   TCB Q3-2013 readings, TCB Q1-2013 (a corrupt equity cell moved off the equity column), and
   **TCB Q3-2017, where `viii_von_chu_so_huu` = 2,000,000,000,000 on disk against assets of
   233.5 tn and liabilities of 214.1 tn** — a wrong cell this change removes.
2. **6 filings re-parsed end to end** against disk (`--overwrite`, no merge): VCB Q1-2026, ACB
   Q1-2024, TCB Q1-2014, TCB Q4-2013, VIC Q3-2014 (`corp`), BID Q4-2016. **15 statements
   REPRODUCED, same layer, same unit, same `publish_date`**; 2 abstained by design (a cumulative
   income statement is not scored against a de-cumulated row); **1 refused — and it is `PAR-1`,
   proven pre-existing.**

⚠️ **TCB Q1-2014 is the one that mattered**: its income statement still wins at
`onnx@200+unit+tail`, the layer whose gate this work changed.

#### ⚠️ AND THE REGRESSION FOUND A FIFTH DEFECT ON A TICKER NOBODY WAS LOOKING AT — `PAR-1`

BID Q4-2016's cash flow — the quarter §6-2-quatervicies spent a day recovering — came back
**`absent`**. It is not this work: the identical three split pairs appear at the identical gaps
on stashed HEAD, and the cause is `SPL-1`'s own guard, shipped 2026-08-29 on a document last
parsed 2026-08-28. **The guard is right.** The detector returns `'(1.029 827)'` as ONE box for a
printed (1.029.827) — a thousands separator read as a space inside a negative figure —
`_split_number_runs` cuts it on the space, and **the row keeps the RIGHT half as a POSITIVE
number**: BID Q4-2016 is on disk with `hddt_mua_sam_tai_san_co_dinh` = **616 mn** for a printed
(2.298.616) and dividends paid of **383 mn** for a printed (2.940.383).

✅ Fixed in the default path: **a box the parentheses SPAN is one figure**, and two figures boxed
together each close their own.

⚠️ **AND THE REGRESSION FOR THAT FIX FOUND THE SAME DEFECT ON A THIRD FILING — one that had
REPRODUCED an hour earlier.** ACB Q1-2024's income statement came back `DIFFERS` in exactly one
cell, `6_chi_phi_hoat_dong_khac` **+907 mn → −109,907 mn**, and **the filing's own subtotal
settles which is right**: other income 172,323 + (−109,907) = **62,416**, the "Lãi/lỗ thuần từ
hoạt động khác" the statement prints. With the disk value it comes to 173,230. So the printed
figure is (109.907), OCR boxed it as `'(109 907)'`, the splitter cut it, and the row kept
**"907" as a positive expense**. ✅ Repaired on disk — one cell, one period, diffed.

⚠️ **SO `PAR-1` IS NOT ONE FILING'S QUIRK: three statements across two tickers are measured, and
the corpus has never been screened for it.** Any parenthesised figure whose thousands separator
OCR read as a space is a candidate, and the reading is a positive number where the filing prints
a negative one. ✅ **BID Q4-2016 WAS RE-PARSED AND REPAIRED ON 2026-08-30 — §6-2-quadragies**, and the
filing's own printed subtotals, not the newer run, are what adjudicated it.

⚠️ **`SPL-1`'s own note — *"0 hits across 12 statements … BID Q4-2016"* — did not cover this
layer.** That quarter wins at `onnx@200+pad6+annual+extra`, and `crop_pad` changes the
recogniser's text; the measurement was taken at the default crop. **A guard measured at one
layer is not measured at the layer the document actually wins on.**

#### ⚠️ AND THE BAND HAD TO BE REPAIRED BEFORE THE QUARTER COULD BE WRITTEN — `DEC-1` ON DISK

Fixing `parse_num` does not move a figure already written. `sane` bands the income statement on
the MEDIAN of the quarters already accepted, and one of TCB's seven was the 163 tn row:

```
before   probes: [0.01, 0.397, 1.018, 2.253, 2.744, 4.221, 163.043] tn
         median 2.253 -> band [0.113, 45.1]   probe 0.097 tn   REFUSED
after    probes: [0.01, 0.397, 1.018, 1.630, 2.253, 2.744, 4.221] tn
         median 1.630 -> band [0.082, 32.6]   probe 0.097 tn   PASS
```

So TCB Q2-2012 and Q1-2013 were re-parsed first (**88.8 min**, `--overwrite`, no merge) and
inspected before anything was written. Q2-2012's income statement came back on the **same layer
with the same 17 items**, every figure divided by exactly 100:
`xi_tong_loi_nhuan_truoc_thue` **163,042,899 mn → 1,630,429 mn**. ⚠️ **The new figure is
corroborated to the đồng by a DIFFERENT filing**: Q3-2013's income statement prints
9M-2012 = 2,233,858 and Q3-2012 = 603,429, and 2,233,858 − 603,429 = **1,630,429**. That is
what makes `force_differs` defensible here rather than "the newer run wins".

✅ Merged and diffed column by column against the pre-merge backup: **exactly 3 periods changed**
— Q2-2012's income statement (17 cells) and cash flow (21), and Q1-2013's cash flow (17) —
**0 balance-sheet rows touched, 67 periods before and after, 0 columns lost**. ⚠️ Q1-2013's
income statement came back **REPRODUCED** and was skipped by the merge, which is the check that
the change is confined to figures printed with decimals.

#### ✅ THE RESULT — TCB Q3-2013 IS ON DISK, AND EVERY LAYER IT USES IS ONE OF THE FIXES

`python -m web_scraper.pdf_ocr_job --symbol TCB --quarters 2013-Q3 --merge`, **39.1 min**:

| | layer | items | |
|---|---|---|---|
| balance sheet | **`onnx@200`** | 47 | ⚠️ the CHEAPEST layer — with page 2 back in the statement the filing declares its own unit and no repair is needed at all |
| income statement | **`onnx@300+unit+tail`** | 15 | 7 split figures at 200 dpi (`SPL-1`), so it needs the resolution AND the document's unit — the pair no layer offered before |
| cash flow | **`onnx@300+unit`** | 20 | the label repair HURTS this one (it puts the movement in the opening slot), which is why the bare variant ships last |

**7 of 7 checks pass, and three of them are arithmetic the parser never tested:**

```
total assets            165,878,786 mn   = the figure printed on page 3
assets == resources     165,878,786 mn   the filing's own identity
total liabilities       152,030,952 mn
PBT                          97,315 mn   the QUARTER column, not the 9M cumulative (749,886)
cash identity           22,621,969 + 2,989,205 = 25,611,174   exactly
the comparative column  179,933,598 mn   = Q4-2012's total assets as already stored
```

⚠️ **`viii_von_chu_so_huu` is deliberately ABSENT.** This filing prints "Vốn và các quỹ" where
the chart of accounts says "VỐN CHỦ SỞ HỮU", and after `MEN-1` no anchor will take a row that
merely mentions it. 13,857,834 mn is a figure the statement prints; it is not one this chart can
name, and §5 rule 2 says absent beats a plausible wrong column.

✅ Diffed column by column against the pre-merge backup: **exactly one quarter changed, in all
three CSVs, `source missing -> pdf`**; 67 periods before and after, 0 columns lost. TCB's parsed
counts go **balance sheet 56 → 57, income statement 58 → 59, cash flow 51 → 52**.

---

### ✅ 6-2-duodequadragies. `BND-1`'s LOOP HAS AN ESCAPE NOW — and a green 14-document run had created NO CSV

Found 2026-08-30 from a user report that two cloned control notebooks *"ran on Kaggle and did
not create or write the .csv"*. ⚠️ **Neither notebook was at fault, and neither was `kgpu`.**
Both had `MERGE_INTO_CSV = True`, both pulled a complete run folder, and both wrote nothing —
because both were parsing a ticker that had never been parsed before.

**The loop, measured on `HOSE_BSR`** (14 documents, Q4-2016 .. Q4-2020, template `corp`,
run folder `20260829-230930__hose_bsr__pdf_ocr`):

| | |
|---|---|
| `seed_history` rebuilds `sane`'s band from | the `pdf` rows **already on disk** |
| BSR's rows on disk | **none** — no statement CSV existed |
| so the band was | **EMPTY on all 14 documents**, and the run WARNed each time |
| `pdf_ocr_merge`'s second refusal | an empty-band statement is not written |
| statements written | **0 of 42** |
| and the band next time | **still empty** |

⚠️ **THE ARTEFACT SAID SO AND NOTHING ELSE DID.** `metadata.json` recorded
`"merge_into_csv": false` — correct, and not the cause: `run()` turns the upsert off on a
worker because the payload root is not the repo's `raw_data/`. The merge was supposed to
happen at the PULL, it did, and it refused every statement silently enough that a green run
and a run that wrote nothing look identical from the notebook's output.

✅ **`FORCE_EMPTY_BAND` breaks the loop, and it is one knob that lifts one guard.**
`JobSpec.force_empty_band` (LOCAL, per quarter) and `JobConfig.merge_force_empty_band`
(KAGGLE, at the pull) both reach `pdf_ocr_merge.merge_run`'s existing escape;
`RUN__pdf_ocr_control.ipynb` carries it, and so do `--force-empty-band` on the CLI and on
`kgpu merge`. ⚠️ **It is a JobConfig field and deliberately NOT a worker parameter** — the
worker does not merge, and `notebook.patch_parameters` RAISES on a parameter the worker
notebook does not declare, so putting it there would have broken every push.

⚠️ **The other three refusals are untouched** — a cumulative income statement, a figure that
DIFFERS from a good `pdf` row, and a document whose engine RAISED (`VCR-1`) are still skipped
and SAID. On BSR that mattered: **7 income statements (every Q2 and Q4) were refused as
cumulative** and stay `missing`, which is correct — this module has no Q1..Q(q-1) to
de-cumulate with.

#### ⚠️ AND THE PRICE IS THE GUARD, SO THE SCREENS WERE RUN BY HAND FIRST

`BND-1`'s whole lesson is that TCB's **95.5 %** concealed a **5.3 % wrong-figure rate**, and
every one of those nine cells was the shape `sane` catches. So before BSR was written, the two
screens that convicted TCB were run over the artefact — no PDF, no OCR, no network:

| screen | BSR |
|---|---|
| **unit**, per report, minority is the suspect | `{1: 10}` / `{1: 13}` / `{1: 13}` — **uniform, no outlier** |
| **continuity**, total assets quarter on quarter | 62.7 → 61.9 → 53.2 → … → 42.9 → 55.9 tn VND, **no step above 1.7×** |

**Written**: `corp/{bs,is,cf}_HOSE_BSR.csv`, **28 `pdf` / 23 `missing` / 0 `cafef`** — rule 24
holds with no special handling. Backup at `raw_data/_backup/statements/20260830-015130__HOSE_BSR`.

⚠️ **NOTHING FROM IT MAY BE QUOTED AS A FUNDAMENTAL.** BSR is a `corp` filing, so `CRP-1`
stands: `C_LIABILITIES` misses on that chart and the balance sheet reconciles on the TRIVIAL
`assets == resources`, true by construction on any page that reads both. And these 28 rows
passed no magnitude guard at all — that is what the knob costs, and it is why the two screens
above are a procedure and not a courtesy.

⚠️ **`P47` IS NOT CLOSED BY THIS.** It asks for two other things, and the second is worth more
than anything here: **ship those screens as CODE.** They were an ad-hoc script this time, they
are free, they need no OCR, and they also run on tickers that DO have history — where `sane` is
on but sees only its own entity's band. `P47`(a) — `plan()` warning ONCE, up front, before any
GPU is spent, rather than per document after the cost — is also untouched.

### ✅ 6-2-undequadragies. ONE PROGRESS LINE, BOTH MACHINES — and the denominator is OCR PASSES

Shipped 2026-08-30 on request. The OCR run reported itself as THREE nested percentages on
three differently-indented lines — `[doc 2/3 67% of DOCUMENTS]`, `[layer 12/47 26% of
POSITIONS]`, `[ocr page 40/96 42% of PAGES]` — each honest about its own denominator and
**none of them answering the only question a progress readout exists to answer**: how far
through the whole thing am I? It is now one line, on both machines:

```
 33.7% - doc 2/3 HOSE_TCB Q3-2013 - layer 12/47 onnx@300 - page 40/96  ~76 s left   <- the OCR
 42.5% - step 4/6 HOSE_TCB 2013-Q3 - wait kernel - [ 1.5 min] RUNNING  25% of last  <- the control
```

`xx.x% - task - sub-task - detail`, formatted by **`src/utils/progress.py`** — one module, so
the machine that does the OCR and the machine that drives Kaggle cannot drift apart. The three
denominators did not go away; they moved into the segments, where each still names its own
(`doc 2/3`, `layer 12/47`, `page 40/96`) and only the last predicts time.

⚠️ **THE WITHIN-DOCUMENT DENOMINATOR IS OCR PASSES, NOT CASCADE POSITIONS, AND THAT IS THE
DIFFERENCE BETWEEN A USEFUL NUMBER AND A STUCK ONE.** Measured 2026-08-30: the cascade's **49
layers share 24 distinct parse keys** (`cafef_financials.parse_key`, now a function precisely
so a second reader cannot re-type it), because a layer that changes only the MAPPING or the
GATE re-maps a cached parse in milliseconds. On a layer index the first — and usually only —
OCR pass is 1/49 of a document, so the bar would sit at 2 % through the most expensive thing
the run does. A cached layer now moves nothing, and the line says `cached parse, re-map only`.

⚠️ **THE NUMBER IS A POSITION IN THE PLAN AND A LOWER BOUND ON REAL PROGRESS.** A filing
accepted at layer 1 is ~1 min and one that defeats the cascade was 33 min (§6-2-noviesdecies),
so no fraction of documents is a fraction of time. Low is the honest direction: a run finishes
early, it does not stall at 99 %. ⚠️ Passes are counted EQUAL and are not — a 400 dpi pass
costs more — and a layer whose engine is missing here (no tesseract) never runs at all.

⚠️ **ON KAGGLE THE OVERALL % STANDS STILL THROUGH `wait kernel` UNLESS THAT EXACT JOB HAS
COMPLETED ONCE BEFORE, AND THAT IS DELIBERATE.** `kernels_status` returns QUEUED / RUNNING /
COMPLETE and **no completion fraction**, so the only honest clock is the job's own last
duration — and a PDF-OCR job is named after its ticker and quarters, so its FIRST run has
none. `runner.RUN_STAGES`' six weights (export 10, upload 15, push 5, wait 50, download 10,
merge 10) are ⚠️ **NOMINAL**: they say which step is the long one and measure nothing. Anything
else would be §5 rule 2 wearing a progress bar.

⚠️ **ANYTHING THAT PARSED `run.log` IS BROKEN BY THIS AND ONE THING WAS.** Every line now
begins with the percentage, so a filter anchored at the start of a line
(`startswith("WRITE ")`) matches nothing — the control notebook's own *"what was refused"*
cell was the first casualty. `progress.detail_of(line)` is the segment that used to BE the
line.

✅ **`progress=None` LEAVES `python -m kgpu run` BYTE FOR BYTE AS IT WAS** — the reporter is
opt-in and only the PDF-OCR control notebook passes one, because a formatting change that
reaches a command nobody asked to change is a change nobody consented to. **345 tests pass in
`src/web_scraper` + `src/utils` (15 of them new, none needing a PDF, a network or an engine),
688 across `src/`.**

⚠️ **AND `src/utils` GAINED AN `__init__.py`, WHICH IS NOT COSMETIC.** `utils/` also holds a
module called `utils.py`, and pytest's default import mode puts a test file's own directory
first on `sys.path` — so the first test ever placed in `src/utils/` made `import utils`
resolve to `utils/utils.py` and every `from utils.constants import …` under it fail with
*"'utils' is not a package"*, which reads as a missing dependency rather than a shadowed name.
9 of the 12 packages under `src/` already carried one.

### ✅ 6-2-quadragies. BID Q4-2016 REPAIRED — and the merge that wrote it had been reading a disk that was not there

Run 2026-08-30 on request, and it did two things: it closed `PAR-1`'s last open cell, and on the
way to writing it, it exposed **`CWD-1`** — a refusal that had been silently unable to run.

#### ✅ The repair, and what adjudicated it

`pdf_ocr_job --symbol BID --quarters 2016-Q4 --overwrite --allow-parent`, **64.6 min**, the full
49-layer cascade over the 4.8 MB FY-2016 audited consolidated annual. It landed on
**`onnx@200+pad6+annual+extra` — the same layer the row on disk was written at**, which is the
first thing worth stating: the cascade, the gates and the escalation order all behaved as before,
and only the two cells `PAR-1` addresses moved.

| statement | verdict against disk |
|---|---|
| balance sheet | **REPRODUCED**, 52/52 cells, same layer |
| income statement | **abstained** — the filing is cumulative and the row on disk is de-cumulated, so `compare()` refuses to score it (by design) |
| **cash flow** | **DIFFERS in exactly 2 of 24 cells**, same layer, same unit, same `publish_date` |

```
hddt_mua_sam_tai_san_co_dinh                      616.000.000  ->  -2.298.616.000.000
hdtc_4_co_tuc_tra_cho_co_dong_loi_nhuan_da_chia   383.000.000  ->  -2.940.383.000.000
```

⚠️ **`force_differs` IS ONLY DEFENSIBLE BECAUSE THE FILING'S OWN SUBTOTALS DECIDE IT, and both
were read off the run's `row_dump` without another OCR pass:**

| check | before | after |
|---|---|---|
| the five mapped INVESTING lines, summed | **+1.296.779 tr** — off by **+2.299.232** | **−1.002.453 tr** = the section II total the filing PRINTS, **exactly** |
| the dividend vs the section III total | 383 vs −2.940.383 | **equal to the đồng** — 2016 has no other financing line |
| the 2015 comparative column, as a control | — | `I + II + III = IV` **exactly**, and there the financing section HAS two lines |
| sign | both POSITIVE, i.e. a purchase of fixed assets and a dividend paid reported as cash IN | both negative |

⚠️ **A 300 tr RESIDUAL REMAINS BETWEEN `I + II + III` AND `IV`, AND IT IS NOT THIS.** `IV` is
vouched for exactly by the closing-balance identity (`V + IV + MHB(3.004.011) = VII`, to the
đồng, on three independently-read figures), and the five investing components now sum to the
printed section II exactly — so the gap sits in `I`, it was on disk before this run, and all of
`I`'s cells are among the 22 that reproduced. **Recorded rather than rounded away.**

✅ **Diffed column by column against the automatic pre-merge backup: one period, two columns,
across all three CSVs.** BID's cash flow stays 61 parsed; nothing else moved.

#### ⚠️ `CWD-1` — the merge planned TWO unguarded writes, and `absent` is a legitimate answer

The dry run said it would write the balance sheet and the cash flow, giving the reason
*"recovers a quarter disk records as `absent`"* — for a quarter that reads `pdf` in all three
statements. `statement_path()` reads `fin.STATEMENTS_DIR` at call time and the module default is
**RELATIVE**; `pdf_ocr_merge.plan_merge` never re-pointed it, so from `src\` it read an empty
directory.

| the identical call | `on_disk` | refusal 3 | plans |
|---|---|---|---|
| from `src\` | **`absent`** | **never ran** | **2 writes** |
| from the repo root | `pdf` | ran | **0 writes** — DIFFERS, correctly refused |

⚠️ **NOTHING LOOKED WRONG, AND THAT IS THE WHOLE DEFECT: `absent` IS THE CORRECT ANSWER FOR A
TICKER BEING BOOTSTRAPPED** (`BND-1`, and §6-2-duodequadragies wrote 28 such rows for BSR three
days earlier). A mislocated data root and a genuinely new ticker produce the same word, so the
guard that stands between a merge and a wrong figure on disk was simply skipped. ⚠️ `_write`
would have put the row under the wrong root too. ⚠️ **`kgpu merge` runs from
`src\kaggle_gpu\`** — the same cwd that mislocated `BACKUP_ROOT` on 2026-08-29, which was
anchored then while this was not.

✅ Fixed by anchoring to `pdf_ocr_job.DEFAULT_DATA_ROOT` — which is itself resolved from
`__file__` and carries the comment *"resolved from this file rather than from the CWD"*, so the
reasoning already existed one module away and was never applied here.

⚠️ **AND THE FIRST VERSION OF THE FIX WAS WORSE THAN THE DEFECT.** An unconditional
`use_data_root` overrules the `root` fixture in `test_pdf_ocr_merge.py`, which monkeypatches
`STATEMENTS_DIR` into a `tmp_path` — so **every `apply=True` test would have written into the
real `raw_data/`**. Caught by reading the fixture before running the suite, not by running it.
The anchor is now conditional on the path still being **relative**, and that predicate is the
defect's own definition rather than a proxy for it: a relative `STATEMENTS_DIR` is exactly one
that resolves against the cwd, and the module default is the only relative value there is.
Anything absolute was put there deliberately — by `pdf_ocr_job.run`, by an experiment harness
(`statement_path`'s docstring records that contract), or by a fixture — and overruling a
deliberate root would move the WRITE as well as the read. **2 tests, one per direction; 335 pass.**

⚠️ **THE GENERAL SHAPE IS `DEP-1`'s AND `SAN-1`'s FOR THE SIXTH TIME — a check that CANNOT RUN
reports the same thing as a check that PASSED.** What is new here is why it survived: the
absent-disk state was made legitimate on 2026-08-29 by `BND-1`'s bootstrap escape, and a
legitimate `absent` is indistinguishable from a mislocated one. **Widening what an answer is
allowed to mean can silently disarm a guard that reads it.**

#### ⚠️ The cost, and why it is not comparable to §6-2-quatervicies' 35m 20s

Simulating the parse cache offline reproduces the run's log exactly — **9 fresh OCR passes in
layers 1-19**, the other 10 being `cached parse, re-map only` — so the honest denominator for
this document is **23 fresh passes to reach layer 47**, at ~2.8 min each. ⚠️ **Two of those 23
did not exist on 2026-08-27**: `onnx@300+unit+tail` and `onnx@300+unit` shipped with
§6-2-septtricies and are both 300-dpi passes. So the four-quarter run that first recovered this
quarter and this one-quarter run are not one measurement repeated, and §6-2-sexvicies has already
withdrawn cross-run timings on this machine taken hours apart.

### ✅ 6-2-unquadragies. BSR Q4-2016 — THE ROW WAS UNWRITEABLE BECAUSE THE CSV HAD NO WAY TO SAY "TWELVE MONTHS"

Asked 2026-08-30: make `is_HOSE_BSR.csv` Q4-2016 parse. ⚠️ **It already parsed, and had for
days** — 16 line items at `onnx@200`, layer 1 of 49, on page 9 of the FY-2016 audited
consolidated filing, with **all five of the filing's own subtotals closing to the đồng**
(net revenue, gross profit, operating profit, PBT, PAT). Nothing about the OCR was wrong. The
blocker was one refusal in `pdf_ocr_merge`, and behind it a limitation of the CSV itself.

**The FY-2016 annual prints the YEAR.** The Q4 income-statement column holds the standalone
quarter, so Q4 = FY − (Q1+Q2+Q3) — and **BSR filed no quarterly report for 2016 at all**
(CafeF lists exactly two 2016 documents, the parent and consolidated FY annuals). So the
subtraction has no operands and never will. `_decumulate` dropped the row, the merge refused
it, and both were right: **a 12-month total in a 3-month column with nothing saying so is the
error this whole module exists to prevent**, and it cost ACB Q4-2010 once already.

⚠️ **SO THE FIX IS NOT A PARSER CHANGE — IT IS A COLUMN.** `months` says how many months of
activity a row covers, and it is the same move `consolidated` made on 2026-08-24 for the same
reason: *two things in one column with nothing saying which is which is the same defect as
sourcing a figure from a web tab*. With the span on the row the choice stops being "an
unmarked wrong-span figure or nothing".

| statement | `months` | why |
|---|---|---|
| balance sheet | **blank** | a STOCK read at a date. A span against it is a category error, not a missing value |
| cash flow | **3·q** | cumulative from 1 January in every quarter — §6-2-vicies used exactly that to convict a mis-read closing balance. **A Q3 row has always been nine months; this is the first time the CSV says so** |
| income statement | **3**, or **3·q** when it could not be split | and `quarter_column` outranks the index: VCB's Q2-2014 prints "Quý II" beside "Lũy kế", so it IS the quarter |

#### ⚠️ WHICH OF "DROP" AND "KEEP" IS RIGHT DEPENDS ON A FACT NOBODY WAS CONSULTING — the PDF index

`_decumulate` and the merge now ask the same question: **were the quarters that would be
subtracted ever FILED?**

* **filed, and merely not parsed in this run** → DROP, exactly as before. Every subset run is
  in this case; the quarter is recoverable and writing the YTD figure now pre-empts a better
  answer with a worse one.
* **never filed** → KEEP, labelled. Nothing will ever subtract quarters that were not
  reported, so the choice is not *"cumulative now or a quarter later"*, it is **"cumulative
  now or nothing, ever"**.

⚠️ **MEASURED ACROSS EVERY PARSED TICKER BEFORE THE RULE WAS WRITTEN — 9 quarters are in the
second case and 7 in the first**, so the distinction is load-bearing rather than theoretical:

| ticker | never de-cumulatable | a full `build()` could still split |
|---|---|---|
| **BSR** | Q4-2016, Q4-2017, Q2-2018, Q4-2018 — it filed no quarterly report before H2-2018 | Q2-2019, Q4-2019, Q2-2020, Q4-2020 |
| **BID** | Q4-2008, Q4-2009, Q4-2010, Q4-2011 — annual reports only, as §6-2-quindecies recorded | — |
| **VCB** | Q4-2008 | — |
| VIC | — | Q4-2010, Q2-2013, Q4-2013 |
| ACB, TCB | — | — |

⚠️ **`filed` MUST BE THE TICKER'S WHOLE DOCUMENT SET, NEVER THE RUN'S.** Passing the subset
would read every subset run's own narrowing as *"never filed"* and write YTD figures over
quarters that are perfectly recoverable — so `build()` captures it from `documents()` BEFORE
the `periods` filter, and the merge reads the PDF index directly at `allow_parent=True`, the
widest possible set. ⚠️ **With no index on disk nothing is kept**: the claim "this was never
filed" cannot be made without the evidence for it (§5 rule 2).

#### ⚠️ AND THE OVER-REFUSAL IT EXPOSED: the merge was refusing statements `build()` never did

Refusal 1 tested the INDEX's `cumulative` flag. `build()` has always let the DOCUMENT overrule
it — a half-year filing printing its own quarter column *is* the quarter — so a run of VCB
Q2-2014 through `pdf_ocr_merge` would have been refused where the authoritative path accepts.
The refusal now reads the span, which is that override already applied. Latent, never fired,
and it only became visible because the span had to be computed anyway.

#### The result

| | |
|---|---|
| the run | `pdf_ocr_job --symbol BSR --quarters 2016-Q4 --merge --force-empty-band`, **1m 17s**, accepted at `onnx@200` |
| written | `is_HOSE_BSR.csv` Q4-2016 — `source=pdf`, `months=12`, `unit=1`, 16 line items, publish 2017-03-17. BSR's income statement **6 of 17** parsed, was 5 |
| the arithmetic | **5 of 5** of the filing's own subtotals close to the đồng — revenue 73,686,050,815,612, PAT 4,435,734,601,163, and 4,483,216,556,061 + (−47,481,954,898) = PAT exactly |
| the diff | pre-merge backup, **every column compared**: exactly ONE period changed in ONE statement, `months` added to all three files, **0 columns lost**, no other ticker touched |
| tests | **359** in `src/web_scraper/` (24 new), **717** across `src/`, none needing a PDF, a network or an engine |

⚠️ **THE TTM SUMS ARE GUARDED, AND THAT GUARD IS THE PRICE OF THE COLUMN.** A row with
`months = 12` entering `_helper_build_bank_fundamental_indicators`' trailing-4-quarter sum
would count a year twice, and nothing downstream could catch it — so a P&L row whose span is
present and ≠ 3 has its flow columns NaN'd, which makes every window touching it NULL rather
than wrong. ⚠️ **A BLANK span is left alone and has to be**: every income-statement row written
before the column existed was either de-cumulated or read from an ordinary quarterly filing —
both 3 months — so blanking them would delete the whole bank history to guard against a case
none of them is in. ⚠️ **It is reviewed and NOT measured**: no bank row carries a non-blank
`months` today, so the branch has never executed on real data.

⚠️ **WHAT THIS DOES NOT DO.** It writes no figure that was not already parsed — the OCR
cascade, the gates and the 49 layers are untouched. It does not make BSR quotable: `CRP-1`
stands (`C_LIABILITIES` misses on `corp`, so a corp balance sheet still reconciles on the
trivial `assets == resources`), and this quarter passed **no magnitude guard at all** —
BSR Q4-2016 is the ticker's earliest period, so `seed_history` can never build a band for it
and `force_empty_band` is unavoidable rather than convenient (`BND-1`). And the eight other
never-de-cumulatable quarters are **not** written: reaching them needs a run each.

### ✅ 6-2-duoquadragies. THE HARD FILING COSTS 9.5 MIN INSTEAD OF 64.6 — and every character it reads is unchanged

Done 2026-08-30 on request: make the OCR module cheaper without moving its output. `P41` and
`P42` were already on the list as *"the cost `P38`/`P6` are budgeted on"*, and both are now
measured rather than estimated. **Nothing about the parse, the gates, the layer order or the
49 layers moved.**

| document | before | after | |
|---|---|---|---|
| **BID Q4-2016** — the hardest filing on disk, cash flow at layer 47 of 49 | **64.6 min** | **9.5 min** | **6.8x** |
| **VIC Q1-2026** — `corp`, all three statements refused | 34.8 min | 5.2 min | **6.7x** |
| **TCB Q3-2013** — three different layers, two at 300 dpi | 39.1 min | 7.9 min | **5.0x** |
| VCB Q1-2026 — accepted at layer 1, ONE OCR pass | 1.4 min | 1.4 min | **1.0x** |

⚠️ **THE FLAT ROW IS THE CONTROL, NOT A DISAPPOINTMENT.** A filing that stops at layer 1 pays
one OCR pass and has no repetition to remove. What was removed is what a filing pays when it
does **not** stop there — 17 % of quarters at the three-ticker rate (§6-2-quindecies), and all
of the tail that `P38`'s 190 h and `P6`'s "days of GPU" are made of.

#### Three defects, and the first two are the same shape: work repeated for an answer nobody reads

1. ⚠️ **`P42` — THE PARSE CACHE KEYS ON ELEVEN FIELDS AND THE OCR DEPENDS ON THREE.**
   `_parse_cascaded` caches a whole parse under `parse_key`, correctly, because every
   `ParseLayer` flag can change the ROWS. **None of them can change a recognised character** —
   `join_digits`, `title_over_form`, `loose_form_code`, `realign_rows`, `notes_boundary`,
   `tail_continuation`, `label_wrap` and `unit_from_document` all run AFTER `scan` has read the
   page. Counted over the 49 layers: **24 distinct `parse_key` against 7 distinct `ocr_key`**,
   so a filing that defeats the cascade was reading every page of itself **24 times to produce
   7 answers**. `PdfParser._ocr_cache` now memoises the page under `(engine, dpi, crop_pad)`,
   scoped to one document.
2. ⚠️ **`P41` — THE CAPITAL-NOTE SCAN WAS 69-77 % OF A PARSE AND HAS NEVER APPEARED IN A LOG.**
   `share_capital` walks from the last statement page to the END of the filing and calls
   `_ocr_page` directly rather than through `scan`, so the page-progress hook never saw it.
   Profiled: BID's FY-2016 annual **50 pages / 84.8 s / 68.8 % of one `parse()`**, VIC's
   Q1-2026 **58 pages / 81.9 s / 76.6 %** — and **both returned nothing**. That is where the
   missing time was: 23 passes of ETA-inverted page rates sum to 16 min against a 64.6 min run,
   and the gap was this scan, invisible. `parse()` gained `want_shares` and `_parse_cascaded`
   passes `not facts["publish_date"]` — **the same condition the value is READ under two lines
   later**, so it is provably output-identical.
3. **The recogniser bucketed its crops AFTER chunking them.** A vietocr batch is one
   autoregressive decode and must share a padded width; the code sorted by ASPECT RATIO and
   chunked by `batch_size`, and `predict_batch` then re-grouped each chunk by exact width —
   **542 crops over 44 widths**, so a 24-crop chunk fragmented into a dozen decode loops of one
   or two images. Bucketing first is **1.11-1.22x on recognition over four interleaved pairs,
   with 0 of 542 crops changed**. ⚠️ **A first measurement said 1.37x and was measuring a
   different thing** — it pre-computed each crop's tensor and skipped a resize the shipped
   path still does. ⚠️ **The 2.35x version is not available from here**: bucketing across the
   whole document is that much better, and taking it means recognising pages in blocks, while
   `scan` reads each page's TEXT to decide whether to read the next. That would change which
   pages are read — a change to the parse, not to its cost.
   ⚠️ **Recognition is 85 % of an OCR pass** (render 1.2 %, detection 12.7 %, crop 1.1 %), so
   this is the only part of the pass worth attacking.

#### ⚠️ TWO FASTER THINGS WERE MEASURED AND REJECTED, and the first is the reason to trust the third

- **Padding every crop in a chunk to a common width** is 2.0x faster again and changes
  **70 of 542 crops** — `'Deloitte'` -> `'Deloitte.'`, `'ĐÃ ĐƯỢC KIỂM TOÁN TH'` ->
  `'ĐÃ ĐƯỢC KIỂM TOÁN TRUNG'`. The recogniser is width-sensitive, so a fuller batch buys a
  different answer. **The shipped change is bucketing precisely BECAUSE padding was tried
  first**, and the 0-of-542 is what separates them.
- **A rewritten greedy decode** — no per-step `.to('cpu')`, no `topk(5)` for a top-1, no
  O(steps²) numpy rebuild of the token history — is **not faster** (3.00x against 3.07x) and
  changed one crop. The decode is bound by the RNN step, not by the host work around it.

#### ⚠️ VERIFIED ON `rows_sha`, NOT ON THE MAPPED CELLS

`compare()` scores the cells that map to a chart of accounts — 76 for BID Q4-2016 — and says
nothing about the rest of the statement. `rows_sha` digests **every row the OCR read**: label,
the filing's own numbering, every figure. **BID, TCB and VCB all reproduce IDENTICAL `rows_sha`
on all three statements, at the same winning layers**, BID's cash flow included — a statement
that has to lose 46 layers and win on the 47th. **731 tests pass** across `src/`, 15 of them
new (`test_cafef_ocr_cache.py`, no PDF, no network, no engine).

⚠️ **AND ONE APPARENT REGRESSION WAS PROVEN NOT TO BE MINE, WHICH COST A 34.8-MINUTE RE-RUN
AND WAS WORTH IT.** VIC Q1-2026 parsed at `onnx@200` on 2026-08-28 and is refused now. HEAD
was stashed back in and run against **today's** disk: the same three absences, the same
reasons, layer for layer. The cause is `sane` — every VIC balance sheet on disk is 2008-2014,
and six more small quarters were merged on 2026-08-29, pulling the band's median to 5.11e13
against Q1-2026's 1.18e15. **"The output changed" and "my change did it" are different claims,
and only a re-run on the old code with today's data separates them.**

#### ⚠️ WHAT IS NOT CLOSED

`P41` is **reduced, not closed**. A filing whose pages carry no signing date leaves `facts`
open, so every layer still asks for the counts — TCB Q3-2013 is exactly that, and it still came
down 5.0x only because the page cache makes the repeats cheap. Two further reductions are
measured-available and **deliberately not taken**, because both change behaviour rather than
cost: `SHARE_NOTE_ANCHOR` is *"phat hanh cua ngan hang"*, so on `corp`/`securities`/`insurance`
it can never match (**0 of 91 `corp` rows on disk carry a share count against 201 of 753
`bank` rows**), and the walk has no page budget. ⚠️ **And `P38`/`P6` should NOT simply be
divided by 6.8** — the multiplier applies to the failing tail and not to the 83 % of statements
that win at layer 1. `web_scraper/CONTEXT.md` §3c.

### ⚠️ 6-2-trequadragies. CTG — 8h 40m ON A T4, 201 CELLS, AND THE FREE SCREENS CONVICTED 43

Merged 2026-08-30 from `20260829-232042__hose_ctg__pdf_ocr` — a Kaggle T4 run of **every**
quarter CTG files, 70 periods Q4-2008 … Q1-2026, **8 h 40 m**, template `bank`, 0 engine
errors, **201 of 210 cells accepted**. It is the second bootstrap ticker after BSR, the first
BANK one, and unlike BSR it did **not** come out clean.

⚠️ **THE PULL WROTE NOTHING, AND `BND-1` IS EXACTLY WHY.** CTG had no statement CSV, so
`seed_history` built an EMPTY magnitude band for all 70 documents, `sane` failed open from
start to finish, and `pdf_ocr_merge` then refused **every** statement it produced. The plan
was `0 writes` from **168 empty-band refusals + 33 cumulative income statements + 9 absent**.
A green 8-hour run and a run that wrote nothing look identical from the notebook's output —
the artefact's `history_sizes` is the only place that says so.

#### ⚠️ SO THE SCREENS DID THE WORK `sane` COULD NOT — and 43 of 201 did not survive

| screen · no PDF, no OCR, no network | held |
|---|---|
| **`A = L + E` to 0.5 %**, and `E/A` inside 3-12 % | **31 balance sheets** |
| total assets continuity vs the verified spine (±15 %/quarter) | 5 more balance sheets |
| PAT ≤ PBT · PBT/assets inside 0.05-2 %/quarter | 2 income statements |
| cash: one opening per year, positive balances, `opening + net + fx = closing` | 5 cash flows |

⚠️ **THE EQUITY ANCHOR IS THE DOMINANT FAILURE AND IT IS `MEN-1`/`TCG-1` AT SCALE.** Twenty
quarters spread across 2012-2024 read `viii_von_chu_so_huu` at **0.03 – 1.7 % of assets** with
a residual of **+5 … +9 %** — a small sub-line claimed as total equity, `A` and `L` both
correct beside it. Q4-2013 is the clean specimen: A **576.4 tn**, L **522.1 tn**, and equity
read **0.147 tn** where the filing's own arithmetic gives **54.3 tn**. Five more go the other
way and take the GRAND TOTAL — Q3-2011's equity is **414.99 tn** against liabilities of 390.5.

⚠️ **AND THE `unit` SCREEN THAT CONVICTED TCB IS A RED HERRING HERE — the correction is worth
more than the merge.** §6-2-quattuortricies convicted 8 TCB statements by taking the MINORITY
`unit` for a report. The same screen flags **32** CTG statements (`unit=1` against a norm of
1,000,000, every one a Q1 or Q3 of 2009-2014) and **every one of them is correct**: those
interim filings genuinely print đồng, and Q3-2014's balance sheet reads **621.0 tn** between
neighbours of 597.6 and 661.1. ⚠️ **`accepted.values` are ALREADY scaled** — the parser applies
`unit` before writing them — so the minority unit is a *fact about the filing*, never evidence
about the figure. **What convicts is the MAGNITUDE**, and on CTG the two disagree 32 times out
of 32. TCB's nine were caught because their figures were 10⁶ small, not because their unit was
odd; the screen was reading the right cases for the wrong reason.

#### What went to disk, and what did not

Three `merge_run` calls, one per report, each carrying only the periods that survived —
**held back by a `periods`/`reports` filter and never by editing the artefact**, which is
§6-2-quattuortricies' procedure.

| | quarters | `pdf` | `missing` |
|---|---|---|---|
| balance sheet | 70 | **30** | 40 |
| income statement | 70 | **35** | 35 |
| cash flow | 70 | **61** | 9 |

✅ **126 `pdf` / 84 `missing` / 0 `cafef`** — rule 24 holds with no special handling. The
30 written balance sheets close `A = L + E` to the đồng at a median **E/A of 6.2 %**, and the
series runs 193.6 tn (Q4-2008) → **2,924.2 tn (Q1-2026)** with no step above 1.15×/quarter.

⚠️ **32 Q4 INCOME STATEMENTS WERE REFUSED AS CUMULATIVE AND THAT REFUSAL WAS NOT OVERRIDDEN.**
CTG filed Q1..Q3 of every one of those years, so a full `build()` can subtract them — this is
precisely the case §6-2-unquadragies says must DROP rather than be written as a 12-month figure
in a 3-month column. `force_cumulative` was not passed and must not be.

⚠️ **`months` IS BLANK ON ALL 126 ROWS, AND THAT IS THE HONEST READING.** The run folder was
written before the field shipped (§6-2-unquadragies, the same day), so it records no span and
`_write` defaults nothing — §5 rule 2 at the column.

#### ⚠️ What this does NOT establish

1. ⚠️ **NOT ONE OF THE 126 ROWS PASSED `sane`.** `force_empty_band=True` is unavoidable for a
   bootstrap ticker and the screens are what replaced the guard — they are four identities and
   a continuity test, not the run's own accumulated magnitude band. **The authoritative path
   for a new ticker is still a full Dagster `raw/cafef_financials` run** (`BND-1`), which
   accumulates history as it goes and would decide these 43 itself.
2. ⚠️ **The screens are still an ad-hoc script, which is `P47`(b)** — and this run is the
   argument for shipping them as code: they cost seconds, need no OCR, and here they stood
   between 43 wrong figures and a CSV.
3. ⚠️ **The 36 held balance sheets are not lost, they are unparsed.** Recovering them means
   fixing the equity anchor on CTG's chart wording, not re-running the OCR: `A` and `L` are
   already correct in 20 of them.
4. ⚠️ **CTG is registered in `CAFEF_FINANCIALS_TICKERS` and NOT in `config.json`**, so its
   Dagster asset cannot be materialised (§6-2-untricies' two-registration rule) — and
   `templates.csv` still does not name it (`TPX-1`).

### ✅ 6-2-quaterquadragies. THE ARTEFACT RECORDED THE REQUEST, NOT THE OUTCOME — `MRG-1`

Found 2026-08-30 from the plainest possible report: *"the control notebook runs, and it does
not create the folder or write the .csv."* Three defects sat behind it, and the first is the
one worth carrying forward, because **no amount of reading the artefact or the log could have
told a run that wrote 126 statements from a run that wrote none.**

#### ⚠️ 1 · `merged_into_csv` WAS `false` ON EVERY KAGGLE RUN EVER, AND NOTHING COULD HAVE SET IT

`metadata.json` is written by the process that PARSES. On a Kaggle round trip that process is
a worker in `/kaggle/working` with no path to this disk — `pdf_ocr_job.run` turns its own
upsert off for exactly that reason, and the merge happens later, here, on the pull. **The pull
wrote nothing back.** So the field recorded what was INTENDED at parse time and was read as
what HAPPENED, and the notebook printed `upserted : False` over the CTG run that had just been
merged by hand.

⚠️ **THE LOCAL PATH HAD THE MIRROR DEFECT**: there the field was `spec.merge_into_csv`, the
REQUEST, so a run whose every statement was refused recorded **`true`**. One field, two
readings, neither of them the outcome.

#### ⚠️ 2 · A FOLDER ALREADY IN THE REPORT ROOT TOOK THE CSVs OUT OF THE ROUND TRIP, IN SILENCE

`pull` offers the upsert only the folders it COPIED this time — deliberately, so that a
re-download does not re-upsert a run the user did not just fetch. But `merge_statements`
opened `if not folders: return`, so a second push of the same job, or any re-pull, printed one
line about the run FOLDER (`already exists — skipped`) and **never mentioned that the statement
CSVs had not been opened at all.**

#### ⚠️ 3 · AND THE NOTEBOOK'S "WHAT WAS WRITTEN" CELL GREPPED THE WRONG MACHINE'S LOG

It filtered the WORKER's `run.log` for `WRITE `/`skip `. Those lines are written by the merge,
which runs HERE — so on a Kaggle run the filter matches nothing, and finding nothing it printed
**"no refusals — every statement was accepted"**. That is a false success claim, and it was
printed over an 8 h 40 m run that wrote **0 of 201 accepted cells**.

#### ✅ What shipped

| | |
|---|---|
| a **`merge` block** in the run folder, schema **v3** | one event per upsert plus their union, keyed by `(period, report)` so a later event supersedes an earlier one. Written by `pdf_ocr_merge.record_merge` on the pull and by `run()` itself on the LOCAL path — **one shape, one reader, both machines** |
| `merged_into_csv` | means what HAPPENED. ⚠️ **True only if something was actually written**: an applied merge that every refusal turned away is not a merge, and recording it as one re-creates the defect one level down |
| `merge_results` | returns `(copied, already_present)`, so *"there was nothing to merge"* and *"the thing you are waiting for was already here"* stop being the same silence. The skip now names `python -m kgpu merge <job>` |
| every merge | ends with one line saying how many statements reached `raw_data/`, and 0 points at `FORCE_EMPTY_BAND` and `BND-1` |
| notebook cells 8-10 | 8 reads the block; 9 separates **the PARSE refused** (the worker's log) from **the MERGE decided** (the block); **10 is new and reads the three CSVs themselves** |

⚠️ **EVENTS, NOT A FLAG, and the CTG bootstrap is why.** That merge took three calls — one per
report, each carrying only the periods an external screen had cleared — and a single
overwritten block would have kept the last and lost the other two. A LOCAL run appends one
event per QUARTER for the same reason.

⚠️ **`pdf_rows_on_disk`, NOT `written`.** `_write` returns the number of `pdf` rows in the
WHOLE csv after the upsert, and the first version of this summed it as *"statements written"* —
which reports a ticker's entire history as one run's work. The count that means what it says is
over the DECISIONS. Caught by reading the function, and pinned by a test.

#### ⚠️ AND CELL 10 WALKED STRAIGHT INTO `CWD-1`, FOUR DAYS AFTER IT WAS FIXED ELSEWHERE

The first version reported **`NO FILE`** for all three of CTG's CSVs — files that exist and
that this repo wrote the day before. `statement_path()` reads `fin.STATEMENTS_DIR` at call
time and its module default is RELATIVE, while cell 3 `os.chdir`s to `src/kaggle_gpu` because
that is where `kgpu` stages its payload. **`CWD-1` was fixed in `pdf_ocr_merge` on 2026-08-30
and the notebook is a second caller of the same function.** The cell now re-points at
`DEFAULT_DATA_ROOT` and **prints the directory it read**, which is the part that would have
made the original defect visible in seconds.

#### The proof, on real artefacts

| check | |
|---|---|
| LOCAL, end to end | `pdf_ocr_job --symbol VCB --quarters 2026-Q1 --overwrite --merge`, **48.7 s**, reproduced at `onnx@200` (59/22/17 items). `metadata.json` now carries `schema_version: 3`, `merge.statements_written: 2`, `merge.statements_skipped: 1` |
| the diff, every column | against the automatic pre-merge backup: **2 cells changed**, both `months` on Q1-2026 (`unrecorded -> 3`), **0 columns lost, 0 periods moved, 0 figures touched** |
| KAGGLE-side recording | `record_merge` on a copy of the real BSR run folder: block written, 3 refusals recorded with their reason, **the repo's own folder untouched** |
| the notebook | cells 8-10 driven against the CTG (v2, no block) and VCB (v3) folders — the v2 folder correctly reports *"schema v2 predates the `merge` block"* rather than *"nothing was written"* |
| tests | **14 new** (6 for `record_merge`/`merge_block`, 8 for the runner — the first tests `kgpu` has ever had), **745 across `src/`**, none needing a PDF, a network or an engine |

⚠️ **WHAT THIS DOES NOT DO.** It writes no figure and lifts no guard: the four refusals are
untouched, `BND-1` still means a new ticker's first run is unguarded and needs
`FORCE_EMPTY_BAND`, and `CRP-1` still means nothing from a non-bank template may be quoted.
What changed is that a run which writes nothing now says so, in three places, one of which is
the CSV itself.

### ✅ 6-2-quinquadragies. BID Q3-2011 — THE INCOME STATEMENT WAS A LANDSCAPE PAGE, AND THE FAILURE WAS THE EXPENSIVE ONE

Recovered 2026-08-30. `is_HOSE_BID.csv` had read `missing` for this quarter since the ticker
was first parsed, and the reason turned out to be three defects stacked on one page — two of
which write nothing, and one of which writes a **wrong figure every gate passes**.

⚠️ **THE CASCADE'S OWN REFUSAL NAMED THE SYMPTOM AND NOT THE CAUSE, AGAIN.** A T4 run of this
quarter that morning reported `no such statement on any page of this filing` at `onnx@200` and
`only 8 rows parsed` at two later layers. §6-2-duovicies' rule — *a cascade's final refusal
names the hardest path tried, not the blocking defect* — applies to its FIRST one too when the
statement is never found at all: 47 layers, three reasons, and none of them was *"page 6 is
sideways"*.

#### Defect 1 · the page is turned 90° and the PDF says `/Rotate 0`

`BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH HỢP NHẤT` is page 6 of 32, a landscape table scanned
into a portrait page. Its `/Rotate` is 0, like every other page of that scan — every page of
the file is 585×842 with one 812×1170 image — so nothing in the PDF says so, and the OCR
returned `I I I I | I I I I I I I T` where the header should be.

⚠️ **AND THE COST RUNS THE WRONG WAY ROUND FROM WHAT YOU WOULD EXPECT: A PARSE THAT FAILS IS
THE EXPENSIVE ONE.** `scan` stops as soon as all three statements are behind it, so a filing
whose statements are found reads a fraction of itself, once. This one was never found, so
every layer re-asked and the scan ran to page 32:

| | pages read | OCR passes | wall clock |
|---|---|---|---|
| before | **32 of 32** | 7 | **2 m 30 s on a T4, and no income statement** |
| after | **7 of 32** | **1** — accepted at layer 1 of 49 | **29 s on the RTX 3050** |

✅ **THE DETECTOR ALREADY KNEW, AND ASKING IT COSTS AN UPRIGHT PAGE NOTHING.** A text LINE is
far wider than it is tall; a turned one is far taller than it is wide. Measured over all 32
pages of this filing, the share of boxes taller than wide:

| | share tall | median w/h |
|---|---|---|
| the 20 upright pages | **0-19 %** | 4.32 … 7.79 |
| the 12 rotated pages | **92-100 %** | 0.16 … 0.25 |

Two orders of magnitude apart with nothing between, so `VERTICAL_LINES_SHARE = 0.70` is a cut
with room on both sides and not a tuned threshold. ⚠️ **The boxes are the ones the upright read
has ALREADY returned** — `_page_rotation` is handed `words`, not a page to re-render — so a
document with no turned pages pays not one extra pixel, and the answer is cached for the life
of the filing.

⚠️ **THE DETECTOR CANNOT TELL 90 FROM 270** — both lay the lines flat — so the direction is
decided by reading the page each way at `ROT_PROBE_DPI = 100` and counting the tokens that
parse as NUMBERS. Upside down, digits stop being digits: **100 against 7** on this page.
⚠️ **AND IT IS A PER-PAGE DECISION, NOT A PER-DOCUMENT ONE.** In this one filing the income
statement needs **90** and all eleven rotated NOTES pages need **270** — measured before the
code was written, and the only reason the probe reads both ways instead of deciding once and
reusing it. Deciding per document would have been cheaper and wrong.

#### ⚠️ Defect 2 · and then the CASH FLOW was deleted for being in the wrong order

Only reachable once defect 1 landed, which is why it had never been seen. **BID prints its
balance sheet (pages 1-2), then its CASH FLOW (3-5), then its income statement (6)** — the
canonical order is a convention, not a rule. `_enforce_order` cleared the cash-flow pages for
preceding the income statement, and `_fill_continuations` then handed those two pages to the
balance sheet running above them: **55 balance-sheet rows became 55 balance-sheet-plus-cash-flow
rows.** The guard now drops out-of-order pages only when the statement SURVIVES somewhere else
— which is the DUPLICATE it was written for, a page that merely NAMES a statement early while
the statement itself is printed further on. A report whose only pages are the early ones is a
different thing, and deleting it is not a repair.

#### ⚠️ Defect 3 · the opening bracket read as a quote, and THAT one wrote a wrong figure

With the page readable, two of the 20 mapped cells were the **prior-period column**. The
recogniser returns `"9,797,589,605,016)` and `"299,126,415,190)` — a `(` printed tight against
a digit is a thin arc — with the CLOSING bracket intact, so nothing about the sign is
ambiguous. `parse_num` refused both, `table_rows` left column 0 empty, and `_first_value` fell
through to the next column:

| | on the page | what was written |
|---|---|---|
| `2_chi_phi_lai_va_cac_chi_phi_tuong_tu` | **(9,797,589,605,016)** | −5,417,947,722,487 — *"Kỳ trước"* |
| `6_chi_phi_hoat_dong_khac` | **(299,126,415,190)** | −210,714,047,566 — *"Kỳ trước"* |

⚠️ **AND IT RECONCILED.** An income statement is anchored only on PBT, and that cell was
sound. `SLD-1`'s shape for the sixth time: a wrong figure every gate passes. ✅ Fixed in the
default path and deliberately narrowly — the mark stands in for `(` **only where the matching
`)` proves a bracket was printed**, in `NUM_RE` (so `_numbers` counts the figure when the
columns are clustered) and in `parse_num` (so it carries the sign). A token that merely starts
with a quote is not a number, then or now. `QUO-1`.

#### ✅ What the recovered statement is checked against — the filing's own arithmetic

Not the gates, which passed either way. Read off the page and closed to the đồng:

```
II  = 3 + 4              578,283,710,824 − 177,445,466,169 =   400,838,244,655   ✅
VI  = 5 + 6              390,827,119,279 − 299,126,415,190 =    91,700,704,089   ✅
IX  = I + … + VIII                                          = 1,690,050,075,012   ✅
XI  = IX + X           1,690,050,075,012 − 1,117,498,904,751 =   572,551,170,261  ✅
XIII = XI + XII          572,551,170,261 − 125,536,323,592 =   447,014,846,669   ✅
I   = 1 + 2           12,879,817,911,898 − 9,797,589,605,016 = 3,082,228,306,882
                                          against a printed  3,082,228,306,881   ⚠️ 1 đồng
```

⚠️ The one đồng is the **filing's own** rounding: both operands were read verbatim off the
page image. ✅ **An external check the run did not use**: the 9-month cumulative column reads
3,181,692,576,552 against BID's FY-2011 PBT of 4,219,873,000,000 already on disk, leaving
Q4-2011 at 1,038 bn — and the quarter's own 572 bn sits inside the 2012-2013 quarterly range
of −42 bn … 4,060 bn.

#### ✅ The regression — and BOTH divergences were proven to be HEAD's

Six filings re-parsed with `--overwrite`, no merge:

| | verdict |
|---|---|
| VCB Q1-2026 · ACB Q1-2024 · TCB Q1-2014 · **VIC Q3-2014 (`corp`)** | **12 statements REPRODUCED**, same winning layer each |
| TCB Q4-2013 | 6 cells DIFFER |
| VCB Q1-2021 | cash flow DIFFERS, `tesseract@200` on disk against `onnx@200` now |

⚠️ **"THE OUTPUT CHANGED" AND "MY CHANGE DID IT" ARE DIFFERENT CLAIMS.** Both divergent
filings were re-run against **stashed HEAD** and came back the same way, and the two builds
agree on `rows_sha` — every row the OCR read, mapped or not — for **all six statements**. So
they are pre-existing (`PAR-1`'s residue and a disk row written by an engine this machine no
longer has), not this change.

✅ **AND VERIFYING TCB Q4-2013 AGAINST THE FILING WAS WORTH THE TWO MINUTES IT COST.** Four of
its six cells were read off the page: `Dự phòng giảm giá chứng khoán kinh doanh` is printed
**(1.427)** where disk holds **+427**, and the filing's own subtotal settles it
(921.035 − 1.427 = 919.608, the printed IV); `Phát hành giấy tờ có giá` is **(4.807.548)**
against disk's **+548**; `Mua sắm bất động sản đầu tư` **(129.902)** against **+902**; `Tiền
chi đầu tư góp vốn` **(35.800)** against **+800**. **Six wrong cells sit on disk today in a
ticker nobody was looking at**, which is `PAR-1`'s *"the corpus has never been screened for
it"* with four more instances attached.

#### The merge, and the one thing it cost

`pdf_ocr_merge` refused the balance sheet and the cash flow as `DIFFERS` and wrote **only the
income statement** — the correct default, and no `force_differs` was passed. It needed
`force_empty_band=True`: BID has no income-statement `pdf` row BEFORE Q3-2011, so `seed_history`
can never build a band for it, which is `BND-1` at the quarter level. **The five closing
identities above are what replaced the guard**, and they are a stronger check than a ±20× band.

| | |
|---|---|
| written | `is_HOSE_BID.csv` Q3-2011 — `pdf`, `onnx@200`, `months=3`, `consolidated=True`, 20 line items |
| BID income statement | **58 → 59 parsed of 70**; from Q1-2012 the ticker is unchanged at 57/57 |
| the diff | pre-merge backup, **every column compared**: exactly ONE period in ONE statement, 27 columns, all newly filled; 0 columns lost, the other two CSVs untouched by md5 |

⚠️ **AND THE COLUMN-BY-COLUMN DIFF EARNED ITS KEEP: `publish_date` CAME BACK EMPTY.** The scan
now stops at page 7, and this filing prints its signing date in a MIDDLE note — **page 18,
`ngày 1 tháng 10 năm 2011`, the only page of the whole filing carrying one** — which neither
`scan` nor `_tail_date`'s last-4-pages window reaches. Re-measured by a probe that read every
page at its own rotation, then restored from the backup. ⚠️ **That is pre-existing behaviour
for every normally-parsed filing** — the early stop is why `_tail_date` exists at all — not a
new defect, and it is unfixed: a filing that signs in a middle note loses its date, and
`publish_date` is what makes the fundamentals point-in-time safe. ⚠️ **A diff that compared only
the FIGURES would have called this merge clean**, which is the third instance of that lesson
(§6-2-quatervicies, §6-2-quinvicies).

#### ⚠️ What this does NOT do

1. **BID's cash flow is still wrong on disk and was deliberately left there.** Its closing
   balance reads **−23,457,326,032,339** — a negative cash balance, `CFB-1`/`P43`, the interest
   paid sitting in the closing slot — and the re-parse recovers three more cells while leaving
   that one. `DIFFERS` refused the whole statement, correctly, and forcing it would have written
   a better statement that is still wrong.
2. **`QUO-1` and `PAR-1` are one family and the corpus is unscreened for both.** Any
   parenthesised figure whose bracket OCR damaged reads as a positive number, or as the wrong
   column, and reconciles.
3. **Nothing here touches the 8 quarters BID never filed** (Q1-Q3 2009, Q1-Q3 2010, Q1-Q2
   2011); `missing` remains the correct answer and no code change can alter it.
4. ⚠️ **`_page_rotation` has met exactly ONE filing.** Its threshold is measured on 32 pages of
   it, the direction rule on 12, and no other document in the corpus has been screened for
   turned pages — so what is established is that this filing's are handled, not that the corpus
   has none left.

### ✅ 6-2-sexquadragies. THE COVERAGE NOTEBOOK — and a start mark that is not CONTIGUOUS inflates its own denominator

`src/kaggle_gpu/RUN__pdf_ocr_summary.ipynb` (read-only — no OCR, no network, writes nothing)
answers the question a run folder cannot: **across every parsed ticker, which quarters are still
missing?** It reads every `statements/**/*.csv` and joins them to the PDF index through
`FinancialsBuilder.documents()` itself — one row per ticker, six columns (`exchange`, `complete`,
`first_report`, and the quarter each statement has been CONTIGUOUSLY read from), quarters
printed as `2008-Q4`, which is the `--quarters` spelling `pdf_ocr_job` takes.

⚠️ **THE THREE STATEMENT COLUMNS BECAME A START MARK, NOT A "LATEST OUTSTANDING QUARTER"
(2026-08-31).** Each now answers *"from which quarter on is this statement complete"* — every
FILED quarter from there to the ticker's newest filing carries a `pdf` row — and `first_report`
is the LATEST of the three, i.e. the quarter from which all three are complete at once. ACB is
the case that forced it: `bs_HOSE_ACB.csv` is unbroken `pdf` from **2009-Q2** while the old
column read **2008-Q1**, because that lone 2008 quarter was the latest one still outstanding. ACB
now reads bs **2009-Q2** / is **2009-Q2** / cf **2009-Q4** / `first_report` **2009-Q4** — the cash
flow opens a quarter later because 2009-Q2 and Q3 are CBTT-03 condensed forms carrying no cash
flow at all (§6-2-sesquadragies), so `missing` there is correct and permanent. ⚠️ **The mark is
anchored at the NEWEST filing, so a hole at the top makes the column `—`** — BID (missing only
2026-Q2) and VIC (45 quarters after 2014-Q4) read `—` on all three; anchoring at the newest
quarter READ instead would print VIC `2011-Q1` and hide the 45-quarter hole. ⚠️ **The table names no
missing quarter, and since 2026-08-31 nothing else does either** — the per-statement listing under
it was removed on request; the `outstanding` grid it was built from is still computed.

⚠️ **THE DENOMINATOR HAS TO COME FROM OUTSIDE THE STATEMENTS CSV.** A `missing` row carries no
`document` (§6-2-terdecies removed provenance from rows nothing produced), so inside the CSV
*"the company never filed"* and *"a filing exists and the parse failed"* are the same word.
Outstanding cells come in two shapes and both count: `missing` (tried, refused) and `absent` (no
row at all — VIC has 45 from the run that was stopped half way, BID has 2026-Q2).

⚠️ **AND `first_report` IS NOW READ OFF THE PDF FILES ON DISK (2026-08-31), NOT THE INDEX AND NOT
THE CSVs.** The index is what CafeF advertises; the files are what an OCR run can open. Measured
over the 7 parsed tickers, exactly **1 quarter has a filing in the index and no PDF on disk** —
ACB 2009-Q3, still carrying `pdf` rows parsed from the file that has since gone; the notebook
WARNs on it. The three statement columns became *"the furthest-back quarter read without a
break"*, anchored at each statement's own newest quarter READ, so a hole at the TOP no longer
empties them — BID reads bs **2008-Q4** / is **2011-Q3** / cf **2011-Q3** while missing only 2026-Q2.
`complete` is `all three marks <= first_report`: every statement's read run reaches back at least to
where the filing chain starts. ⚠️ **It therefore measures the START of the chain and not its TOP,
which is the one thing it cannot see — BID reads `True` while 2026-Q2 is unparsed** (ACB and BID
`True`, the other five `False`).

⚠️ **THE FILING-CHAIN MARK — the `first_report` COLUMN throughout, though its SOURCE moved from the
index to the files on 2026-08-31 — WAS TWO RULES, EACH MEASURED SEPARATELY.** The CONTIGUITY half is
still live, and is also how the three statement columns are built. It comes from the INDEX and
never from the parse — a mark taken from the first quarter that *parsed* pushes every early
failure out of its own denominator, which is `SAN-1`'s shape one level up, and it had BID reading
`complete = True` while its Q3-2011 income statement was `missing` on a filing whose other two
statements read at `onnx@200`. **And, since 2026-08-30, the chain must be CONTIGUOUS walking back
from the ticker's NEWEST filing**: touching a quarter with no filing ends it, and an isolated old
filing opens no chain at all.

| ticker | the PDF index says | before | **after** | cells leaving the denominator |
|---|---|---|---|---|
| **ACB** | 2008-Q1, then **four empty quarters**, then 2009-Q2 → 2026-Q1 unbroken | 2008-Q1 | **2009-Q2** | 3 — all of one lone quarter |
| **BSR** | 2016-Q4, 2017-Q3, 2017-Q4, **no 2018-Q1**, then 2018-Q2 → 2020-Q4 | 2017-Q3 | **2018-Q2** | 3 |
| BID · TCB · VIC · VCB · CTG | no break after the mark | — | **unchanged** | 0 |

⚠️ **CONTIGUITY IS MEASURED OVER EVERY FILING WHILE THE MARK IS STILL A QUARTERLY ONE, AND ALL
THREE VARIANTS WERE RUN BEFORE ONE WAS CHOSEN.** `documents()` folds the audited annual onto Q4,
so demanding contiguity over *quarterly* filings alone breaks at **every Q4** and the longest
chain is three quarters; taking the chain's own first quarter as the mark instead pulls VCB and
CTG back to 2008-Q4 — a year that filed nothing but its annual — and hands VCB one more blocking
cell, the cumulative Q4-2008 income statement that can never be split (§6-2-unquadragies).
**Filter for contiguity first, then take the earliest QUARTERLY filing still inside the chain.**

⚠️ **`complete = False` is *not proved continuous*, never *the parser is broken*** — a filing may
not contain that statement, and a cumulative income statement with no Q1..Q(q-1) to subtract is
refused by the merge by design (CTG has 32 such quarters). ⚠️ **And the PDF index is not in git**
(`raw_data/` is ignored except `financials/`), so a fresh checkout cannot prove which quarters
were filed: those tickers read `False` with a warning rather than a guess (§5 rule 2).
`kgpu/PDF_OCR.md` §8.

### ✅ 6-2-sesquadragies. ACB 2009 — A PRE-UNICODE UNIT, A FOUR-LINE P&L, AND TWO CASH FLOWS THAT DO NOT EXIST

Asked 2026-08-30: the ACB control notebook had been run twice on `2009-Q2 / Q3 / Q4` and the
three statement CSVs still read `missing`. **Neither notebook nor `kgpu` was at fault.** Four
outstanding cells, three distinct causes, and only two of them are fixable — which is the
finding, because the third was being retried as though it were a parser failure.

⚠️ **BOTH 2009 FILINGS ARE `BÁO CÁO TÀI CHÍNH TÓM TẮT` — Mẫu CBTT-03, a CONDENSED DISCLOSURE
FORM, NOT A FINANCIAL STATEMENT SET.** Three pages each: a condensed balance sheet on pages 1-2
and a **four-line** profit-and-loss on page 3 (Tổng thu nhập, Tổng chi phí, LNTT, LNST). Q4-2009
looks complete only because its rows come from `FY-2009_…da_kiem_toan.pdf`, a different
document. Read off the PDFs directly, no OCR needed for Q3-2009 — it carries a real text layer.

| | before | cause | after |
|---|---|---|---|
| Q2-2009 balance sheet | `pdf`, 33 items | — | untouched ✅ |
| **Q2-2009 income statement** | `missing` — *"only 5 rows parsed"* | `MIN_ROWS` = 12 against a form that HAS four lines | ✅ **`pdf`**, `onnx@200+unit+condensed` |
| **Q3-2009 balance sheet** | `missing` — *"sane: magnitude 1.7e+08 vs typical 1.3e+14"* | the unit, below | ✅ **`pdf`**, `onnx@200`, 20 items |
| **Q3-2009 income statement** | ⚠️ **`pdf` AND WRONG** — PBT **641,749 đồng** | the unit, and `sane`'s band was empty so nothing refused it | ✅ **repaired**, ×10⁶ |
| Q2/Q3-2009 cash flow | `missing` | **the form contains no cash flow at all** | ⚠️ **`missing` is correct and permanent** |

#### ⚠️ `LGU-1` · the unit is declared in VNI-Times, and `norm` does not know VNI

Q3-2009's text layer prints **`ÑVT : Trieäu ñoàng`** — pre-Unicode VNI, where a tone mark is a
separate character after the base vowel and `đ` is the codepoint `ñ`. `PdfParser.norm` strips
accents, so it normalises to **`trieaunoang`**, and `_declares_millions` tested only
`trieudong` / `trieuvnd`. Every figure of the filing was therefore read as đồng.

⚠️ **AND THE TWO STATEMENTS OF ONE FILING WENT DIFFERENT WAYS, WHICH IS THE WHOLE LESSON ABOUT
`sane`.** The balance sheet had a band (from Q2-2009) and was **refused** — the quarter read
`missing`, which looks like an OCR failure and is a gate working. The income statement had an
**empty** band, `sane` failed open, and the same 10⁶ error **reached disk as `pdf`**: a pre-tax
profit of 641,749 đồng for a bank holding 169 trillion. `BND-1` at the level of one statement.

✅ **Fixed in the DEFAULT path**, per the rule §6-2-untricies drew — *when the gates cannot see
the defect, the repair cannot be an escalation* — because the statement is accepted at layer 1
and no later layer is ever reached. ⚠️ **The blast radius was measured in both directions before
it shipped**: across the **1,196 filings of the seven parsed tickers, 366 carry a text layer and
exactly FOUR carry this spelling** (ACB Q1-2007, Q2-2007, Q1-2008, Q3-2009); the first two are
before `FINANCIALS_PERIOD_MIN` and never opened, and all of ACB 2008 reads `missing`, so **no
existing `pdf` row could be damaged.** ⚠️ **TCVN3/ABC is deliberately NOT matched** — it writes
`TriÖu ®ång` → `triouang` and has **0 hits**, so adding it would be an unmeasured needle
(§5 rule 2); a test records the decision so a later reader does not "fix" it.

#### The four-line P&L — admitted on EVIDENCE, and the obvious marker is the one that fails

`MIN_ROWS = 12` exists to keep a page that is not a statement out. Mẫu CBTT-03's P&L is four
printed lines, so it threw away a statement that is complete. New `ParseLayer.condensed_income`,
**last in the cascade**, lowers the floor to the form's own length — but only when the parser
has recorded the evidence, and **both halves are required**: the flag alone is a slackened
threshold, the evidence alone would widen acceptance at layer 1 where nothing has yet failed.

⚠️ **THE EVIDENCE IS THE P&L'S OWN WORDING, NOT THE "Mẫu CBTT-03" MARKER, AND THAT CHOICE IS A
MEASUREMENT.** The marker looks like the obvious key and it is boilerplate quoting the
circular — **VIC carries it on the statement pages of eleven 24-32 page FULL filings**, one of
which (Q3-2008) classifies pages [8, 13, 30, 32] as its income statement (8 rows) and [14, 15]
as its **cash flow (2 rows)**. Keyed on the marker the floor would have been lowered for exactly
that junk. Keyed on the two summary lines a condensed P&L prints and a full one never does —
"Tổng thu nhập" and "Tổng chi phí", **scoped to the statement's own pages** — it matches **2 of
1,196 filings**, both genuine ACB condensed forms. ⚠️ **The scope is half the measurement**:
searched over whole documents the same two words also hit VCB's 44-page Q4-2009 filings, where
they sit in a note. ⚠️ **And it can only ever license an income statement**, because it is a P&L
line — VIC's 2-row cash flow is unreachable by construction, not by threshold.

⚠️ **`+unit` RIDES WITH IT AND THERE IS NO BARE `+condensed`.** The condensed form prints its
unit once, in the page-1 header, while the P&L is on page 3 — so Q2-2009's income statement
declares nothing itself and a bare layer would have accepted a pre-tax profit of **868,056
đồng**. These are a ticker's earliest quarters, where the band is empty by construction, so the
one gate that could catch it is guaranteed off. Same rule as `annual_tail` (§6-2-tervicies):
**a layer that widens acceptance may not also be the layer that reads the wrong unit.**

#### ⚠️ AND THE CASH FLOWS ARE NOT A PARSER PROBLEM — the form has none

*"`no such statement on any page of this filing`"* was the cascade's answer for both quarters at
all 49 layers, and it is **correct**. A CBTT-03 summary carries a balance sheet and a P&L and
nothing else. §5 rule 24: a quarter no readable PDF can produce is `missing`, and `missing` is
the answer. ⚠️ **This is the interpretive trap the run costs money to fall into** — the refusal
reads identically to a scan the OCR could not handle, and it had been retried twice.

#### ⚠️ `OVERWRITE = True` WOULD HAVE DAMAGED A GOOD ROW WHILE REPAIRING THE BAD ONE

Q3-2009's income statement was on disk as `pdf` and wrong, so the merge refused it as `DIFFERS`
— correctly. The obvious move is `OVERWRITE = True`, which sets `force_differs` for the whole
run. **Measured, that would have cost a good row in the same run**: ACB Q2-2009's balance sheet
is **33 items at `onnx@200+relax`** on disk and **19 items at `onnx@200`** in this run, and
`DIFFERS` is the only thing that saved it.

⚠️ **THE CAUSE IS `seed_history`, AND IT GENERALISES TO EVERY `pdf_ocr_job` RUN.** That module
rebuilds `sane`'s band from the `pdf` rows ON DISK; a full `build()` accumulates one as it goes.
**The two runs therefore escalate differently, and the seeded one can win on an earlier, poorer
layer** — here the thinner band let layer 1 pass where the full run's band had refused it and
sent the cascade on to `+relax`. So a `pdf_ocr_job` run is not a re-run of the run that wrote
the rows, and `OVERWRITE` is not a repair tool. ✅ **Both notebooks gained a scoped `REPAIR`
list instead** — explicit `(quarter, statement)` pairs through `merge_run`'s own
`periods`/`reports` filter, so nothing outside it can move even by accident.

#### The result, and every check it passed

| | |
|---|---|
| the run | `pdf_ocr_job --symbol ACB --quarters 2009-Q2 2009-Q3 --allow-parent --merge --force-empty-band`, **81 s** for both documents |
| the repair | one scoped `merge_run(periods=['Q3-2009'], reports=['income_statement'], force_differs=True)`, adjudicated by the FILING and never by the newer run |
| ACB balance sheet | 67 → **68** parsed · income statement 67 → **68** · cash flow 66, unchanged |
| **the diff, every column** | against a pre-run backup: **exactly 3 periods changed**, 73 periods and every column count identical before and after, **0 columns lost**, the cash-flow CSV **not one cell** |
| the arithmetic | Q3-2009 total assets **169,512,664,000,000** = the filing's printed 169,512,664 triệu; five line items match ×10⁶; the filing's own TSCĐ identity closes **exactly** (1,269,466 − 422,585 = 846,881) |
| the adjudication | Q3-2009's filing prints Q2/2009 = **868,056** as its comparative column, and Q2-2009's **separate** filing independently wrote **868,056,000,000** — two documents, one figure, same unit |
| regression | **425 tests** in `src/web_scraper` (23 new, no PDF/network/engine); **two filings re-parsed end to end, 6 of 6 statements REPRODUCED at their own layers** — ACB Q1-2024 (44/20/28 items, `onnx@300+relax` cash flow) and **TCB Q1-2014, whose income statement wins at `onnx@200+unit+tail`**, i.e. the `unit_from_document` path this change touches. ⚠️ That second one is the control that matters: a statement whose unit is resolved from the DOCUMENT is exactly what a wider needle set could have perturbed, and it did not move |

⚠️ **ONE TEST WAS RESTATED RATHER THAN SATISFIED.**
`test_the_new_layers_are_last_and_relaxed` asserted that the `cash_extra_terms` layers are
literally the tail of `LAYERS`. Appending any legitimate layer breaks that while changing
nothing about when the span runs — **a test that pins a POSITION fails the first time something
is appended**; the guard is the ORDER relative to the strict layers, which is what the flag's
own docstring claims. It now asserts contiguity plus "after every strict layer".

⚠️ **What this does NOT do.** It parses no cash flow for either quarter and never will.
`P46` is untouched — `unit_from_document` still sits at layers 43-45 of 50 and cannot reach a
statement that reconciles at layer 1; this fixes the *needle*, not the *reachability*, and the
two are independent. ⚠️ **ACB Q1-2008 is now probably recoverable and was NOT run** — it is the
fourth VNI filing, in range, and every one of its cells reads `missing` today. ⚠️ And these
figures passed **no magnitude guard**: `force_empty_band` was unavoidable (`BND-1`), so the
arithmetic above is what replaced it.

### ✅ 6-2-septquadragies. THE SAME TWO QUARTERS, A FOURTH AND FIFTH TIME — and the artefact never said WHY

Asked 2026-08-31 to make ACB's Q2-2009 and Q3-2009 cash flows parse. **They cannot, they never
could, and §6-2-sesquadragies had already measured that** — the filings are three-page
`BÁO CÁO TÀI CHÍNH TÓM TẮT` forms (Mẫu CBTT-03) carrying a condensed balance sheet and a
four-line P&L and no cash flow at all. The value of the session is the second half: **why that
finding did not stop the re-runs**, and what now does.

**Re-measured before anything was changed, three independent ways plus a live check:**

| | |
|---|---|
| Q3-2009's own TEXT LAYER | 3 pages — `I.B. BẢNG CÂN ĐỐI KẾ TOÁN` (1-2), `II.B. KẾT QUẢ HOẠT ĐỘNG KINH DOANH` (3, four lines, then the signature). **0 occurrences** of `LƯU CHUYỂN` or `TIỀN TỆ` |
| Q2-2009 (a scan) through `PdfParser.scan` itself | `balance_sheet, balance_sheet, income_statement`; page 1 reads `Mẫu CBTT - 03 … (Quý 02/2009)` |
| the cascade | **all 50 layers, ONE reason**: `no such statement on any page of this filing` |
| `documents()` | exactly one document per quarter, **no alternate** |
| a LIVE re-enumeration through Dagster (`raw/cafef_pdfs`, `HOSE_ACB`, `year_min/max: 2009`) | index **byte-identical**; CafeF still lists 4 PDFs for 2009 |

⚠️ **THE REFUSAL THAT IS PERMANENT AND THE REFUSAL THAT IS RECOVERABLE PRINT THE SAME WORD.**
`absent` covers both *"the filing does not CONTAIN this statement"* — a verdict on the DOCUMENT,
which no layer, engine or DPI can overturn, and where `missing` is correct and permanent (§5
rule 24) — and *"every layer refused the one it found"*, which a later layer might still take.
**The reason lived in `run.log` prose alone**, so the notebook, the merge and every reader had
to match a sentence to learn which kind they had. The run folders record **four full 50-layer
runs of these two quarters on 2026-08-30**, and this request was the fifth.

✅ **So the reason is DATA now** — by the argument `layer_errors` was already made data for,
*a WARNING is prose and the decision downstream must not depend on matching a sentence*:
`NO_SUCH_STATEMENT` is a constant, `_parse_cascaded` publishes `self.refusals`, each document
JSON carries `absent_reasons` (**artefact schema v3 → v4**), and
`pdf_ocr_job.settled_absences()` reads it back → `{YYYY-QQ: {report: run_id}}`. Both control
notebooks print it **before any OCR is spent**. ⚠️ **No gate, threshold or layer ordering
moved**, and `raw_data/` did not change by a byte — verified by `git status` after two full
notebook runs with `MERGE_INTO_CSV = True`, where the existing refusals correctly wrote nothing.

⚠️ **IT ANSWERS ONLY FROM RECORDED REASONS.** A run folder older than v4 carries none and
contributes **nothing** — never *"nothing was permanently absent"* (§5 rule 2). A quarter it
returns has been MEASURED unproducible; a quarter it omits has not been measured either way.
⚠️ **And it reports rather than filters**: a filing can be re-uploaded (§6-2-quindecies), so a
settled absence is settled about the DOCUMENT that was read, not about the ticker forever.

⚠️ **THE GENERAL LESSON, and it is `DEP-1`/`SAN-1`/`CWD-1`'s for the seventh time: a measurement
that exists only as prose is a measurement the next session cannot act on.** §6-2-sesquadragies
stated this conclusion in as many words on 2026-08-30 and it did not prevent one re-run, because
nothing a person or a notebook reads at the moment of deciding carried it. **The register that
stops a repeat is the ARTEFACT, not the write-up.**

⚠️ **One finding turned up on the way and is NOT acted on: ACB Q1-2009 is `missing` in all three
statements because its only filing is a `.rar`** (`ACB_09Q1_BCTC.rar`), dropped by
`_is_pdf_link`. Not an OCR failure, and §5 rule 24 is silent on an archive the company itself
filed. `web_scraper/CONTEXT.md` §3e.

### ✅ 6-2-duodequinquagies. `LSP-1` — THE PARSER MANUFACTURED THE SPLIT IT THEN REFUSED THE STATEMENT FOR

Found 2026-08-31 while making BSR's four `missing` balance sheets parse. The cascade's reason
line said `N figure(s) split across two boxes` — `SPL-1`, the DETECTOR emitting one printed
figure as two boxes — and that diagnosis was wrong in the most useful way: **the detector had
emitted ONE box, and `_split_number_runs` cut it.**

BSR's Q3-2018 balance sheet returns `'9.964.924.167 838'` as a single box for a printed
9.964.924.167.838 — a thousands separator recognised as a space. `_split_number_runs`, built
for the opposite case (one box holding TWO period figures), splits it; the left half lands on
no column and is dropped, so the row would read **838** for a cash line of 9.96 nghìn tỷ.
`split_figures` counts the fragments and `reconcile` refuses the whole statement, which is
correct behaviour on a defect it cannot otherwise see — and is why raising the DPI rescues
nothing: `onnx@300` fixes some rows and breaks others, and no layer ever reaches zero.

⚠️ **THE TWO DEFECTS ARE OPPOSITE IN MECHANISM AND IDENTICAL IN SYMPTOM**, because one guard
fires on both. Reading the reason as `SPL-1` sends you to the detector; the box is sitting in
the OCR output, whole.

#### The ground truth, and the discriminator that was measured and DISPROVEN

✅ **The same box comes back WHOLE at onnx@300 and onnx@400**, over the identical x-range
[347.8, 423.4]. That, and not the arithmetic, is what settles that it is one figure. Across
four BSR filings: **76 whitespace runs, 69 of which join into one well-formed grouped
figure**; the other 7 are OCR damage (`'25 1961177.684.364'`, `'697 188.266,449'`) and must
stay split.

⚠️ **AND NOTHING INSIDE THE BOX SEPARATES IT FROM THE OPPOSITE CASE.** ACB's Q1-2025 cash flow
really does box two period figures together — `'135.272.610 126.501.216'` — and that joins
just as well-formed. A character-density test was tried FIRST, on the theory that a lost
separator costs one character where an inter-column gap costs none:

| measured 2026-08-31 | pt/char, against that page's own median |
|---|---|
| BSR's lost separator, `'9.964.924.167 838'` | **1.00x** — and the clean box beside it 1.00x |
| **ACB's genuine two-figure box** | **1.03x** |

**The hypothesis is dead**: the two populations are indistinguishable by text and by geometry.
A test records the disproven reasoning so it is not re-made.

#### ✅ So the separation is CASCADE POSITION, and the blast radius is a COUNT

`join_lost_separator` is a `ParseLayer` flag, off by default, on **five layers at positions
51-55 of 55**. ACB Q1-2025's cash flow is accepted at **layer 6** and can never reach them.

⚠️ **AND THE SAFETY CLAIM IS MEASURED, NOT ARGUED**: across all **1,023 `pdf` rows on disk**
the latest layer any of them was won at is position **50**, and the cascade stops at the first
acceptance — so **no row now on disk can move**, and the reach of the new block is exactly the
**159 `missing` rows**. ✅ It costs **no extra OCR pass**: `ocr_key` stays at 7 distinct
values, because the flag is a per-layer post-step on words the page cache already holds (the
run log says `cached parse, re-map only`).

#### ✅ BSR Q3-2018 recovered — and checked against the FILING, not against the gates

`onnx@300+joinlost`, **46 items**, 6m 30s. ⚠️ `reconcile` on the `corp` chart only ever tests
`assets == resources`, which is true by construction on any page that reads both (`CRP-1`), so
the acceptance proves little on its own. **Nine internal identities close to the đồng**, four
of them over figures that were being split:

```
A 30,151,613,550,063 + B 29,536,913,206,485 = 59,688,526,756,548 = TAI SAN = NGUON VON
tien 2,058,124,724,783 + tuong duong 7,906,799,443,055 = 9,964,924,167,838   <- the split figure
nguyen gia 46,968,470,120,703 - hao mon 19,578,257,630,264 = TSCD huu hinh 27,390,212,490,439
ton kho 10,982,779,849,642 - du phong 3,190,773,155 = 10,979,589,076,487     <- a THREE-part split
cash + ST inv + receivables + inventory + other = A, exactly
```

Total assets 59,688.5 bn sits between Q2-2018's 61,875.8 and Q4-2018's 53,211.6.

⚠️ **The same run's cash flow came back DIFFERS in one cell and the merge refused it, correctly
— and it is NOT this change.** `join_lost_separator` is off at `onnx@300`, where that statement
accepts. The `rows_sha` of ALL THREE statements differs between the Kaggle run that wrote the
disk row and this local one — including the income statement, which nonetheless REPRODUCED
every mapped cell — so it is the cross-machine recognition difference §6-2-tricies measured,
one cell wide.

#### ⚠️ What it does NOT fix — two of the four balance sheets

**BSR Q3-2017 and Q4-2017 are still absent**, and their refusals are total-LABEL ones
(`no total assets`, `no total to balance against`) rather than fragmentation. Their figures are
joinable — all 22 of Q3-2017's balance-sheet runs are — so this is a SECOND defect stacked on
the first, in `CRP-1`/`TPL-1`'s territory (the corp chart's grand totals) and not in the
splitter. `LSP-1` was necessary for those two and is not sufficient.

#### ✅ WHAT WENT TO DISK — 4 of the 8 recoverable cells, and every one checked

BSR's three CSVs held **17 `missing` cells**, of which **9 are three quarters CafeF never
filed** (Q1-2017, Q2-2017, Q1-2018 — `missing` is correct and permanent, §5 rule 24), leaving
**8** a parser could win. Four now read `pdf`:

| | recovered by | checked against |
|---|---|---|
| balance sheet **Q3-2018** | `LSP-1` — `onnx@300+joinlost`, 46 items | **9 of 9** internal identities exact |
| income statement **Q2-2019** | the merge's de-cumulation (H1 − Q1) | `LN gop = DTT − GV` exact |
| income statement **Q2-2020** | de-cumulation (H1 − Q1) | `LNTT = LNT + LN khac` exact; PBT **−1,908.7 bn**, the COVID/oil-crash quarter |
| income statement **Q4-2020** | de-cumulation (FY − Q1..Q3) | `LNTT` exact; and the four 2020 quarters sum to **−2,852,427,438,530**, the pre-tax loss the audited annual prints |

⚠️ **Diffed column by column against git HEAD, not against the figures**: **10 rows changed**,
**0 columns lost, 0 periods lost**, and the only figure altered on an existing row is the one
adjudicated cell below. The other six changed rows are `months` span-fills that move nothing.

⚠️ **ONE EXISTING CELL WAS OVERRULED, AND ONLY BECAUSE THE FILING DECIDES IT.** Q3-2020's
income statement carried `loi_nhuan_sau_thue_cua_co_dong_cua_cong_ty_me` =
**1,281,417,324,785** against a total PAT of **162,863,303,784** — the parent's share 7.9x the
whole company's. The run reads **173,033,101,904**, which implies a −10.2 bn minority interest
and is ordinary. That is `PAR-1`'s *"the corpus has never been screened for it"* with another
instance attached, in a ticker nobody was looking at. `force_differs` was scoped to that ONE
(quarter, statement) pair.

⚠️ **AND THE OPPOSITE CALL WAS MADE ON Q3-2019, FROM THE SAME EVIDENCE STANDARD.** There the
local reading comes from `tesseract@200` and is demonstrably worse
(`i_6_chi_phi_phai_tra_ngan_han` 361,884,738,267 → **361,884,738**), so disk was left alone.
Recency decided neither.

#### ⚠️ THE FOUR THAT REMAIN, and each has a NAMED reason

⚠️ **PROBED AT `onnx@200+joinlost`, AND ALL THREE BALANCE SHEETS NOW REPORT
`split_figures = 0`** — so `LSP-1` did its whole job on them and what is left is a DIFFERENT
defect class, the VAS **`Mã số` item-code column** and the corp grand totals (`MSO-1`,
`CRP-1`/`TPL-1`). That is the useful finding: these three were never one problem with the
Q3-2018 one.

| | what the probe actually shows |
|---|---|
| balance sheet **Q4-2019** | ⚠️ **`MSO-1` outright** — 3 columns detected and column 0 is the item code, so `TỔNG TÀI SẢN` reads **270**, `TỔNG NGUỒN VỐN` **440**, `A` **100**, `B` **200**, `D` **410**. §6-2-tretricies' heading-based `Mã số` detection did not fire on this filing |
| balance sheet **Q4-2017** | the item code is merged INTO the label — `tong_cong_tai_san_270_1004200` — and the `TỔNG CỘNG NGUỒN VỐN` row lost its current-period figure entirely (`[None, 63,181,382,122,907]`). ⚠️ It is one cell short of parsing: `A + B = 63,260,794,448,651` equals the total assets it did read, **exactly** |
| balance sheet **Q3-2017** | neither grand-total row is produced at all — `A` and `D` map, and no row's label mentions a total |
| income statement **Q4-2019** | the de-cumulation is blocked on Q3-2019's span, and **this machine cannot record it**: Q3-2019's income statement is not reproducible here at any onnx layer (§6-2-quinquagies), so writing it would mean overruling a disk cell that differs by 200,000 dong on 90 billion — below anything the statement's own identities can adjudicate. **The remedy is a Kaggle run**, where the cascade and the reading match the row on disk |

⚠️ **NONE OF THE THREE IS ATTEMPTED HERE, DELIBERATELY.** `MSO-1`'s repair lives in the DEFAULT
path — it decides what a column IS on every corporate filing — so it needs its own blast-radius
measurement across the corp corpus, which is a second piece of work and not a follow-on to this
one.

### ✅ 6-2-undequinquagies. AND THE MERGE DE-CUMULATES NOW — a refusal that left a row `missing` until somebody ran a full `build()`

Same day, and it is the other half of BSR's gap: **four income statements parsed cleanly and
could not be written.** `pdf_ocr_merge`'s refusal 1 turned away a cumulative Q2/Q4 P&L whose
priors WERE filed, on the correct ground that `pdf_ocr_job` cannot de-cumulate — and the
consequence was that the row stayed `missing` until somebody ran a multi-hour authoritative
`build()` over the whole ticker. BSR's Q2-2019, Q4-2019, Q2-2020 and Q4-2020 sat in exactly
that state with their figures already in a run folder.

The merge now does the subtraction itself, with `_decumulate`'s arithmetic, on operands it can
already reach: **the `pdf` rows on disk, plus the quarters this same pass has just recovered**
— which is what lets a Q4 be split once the Q2 it needs has been written (`_documents` yields
oldest first, and BSR's Q4-2019 needs a Q2-2019 that is `missing` on disk). **No OCR.**

⚠️ **IT STILL REFUSES, ON THE TWO GROUNDS THAT COULD MAKE THE SUBTRACTION WRONG.** A prior must
be a `pdf` row — subtracting a blank silently returns the year-to-date figure unchanged — and
its span must be a KNOWN three months. There are exactly two ways to know it: `months == 3` on
the row, or the prior is **Q1**, which is three months by construction. ⚠️ **A blank `months`
is UNRECORDED, not 3** (§5 rule 2 at the column): most of the corpus predates that field, and
subtracting a six-month row from a twelve-month one yields a number that is neither, with
nothing downstream able to tell. Re-parsing the prior records the span — through the merge's
own `fills_span` branch, which moves no figure — and the Q4 becomes splittable.

Measured on BSR's existing run folder before anything was written:

| | |
|---|---|
| Q2-2019, Q2-2020 | ✅ de-cumulated — each needs only its Q1 |
| Q4-2019, Q4-2020 | ❌ refused, naming Q3-2019 / Q3-2020 as `months=unrecorded` |

⚠️ **A quarter this pass de-cumulated becomes a later quarter's OPERAND only if it survives
refusals 2-4.** They run after the subtraction, so a Q2 that was split and then refused — an
empty band, or a figure that DIFFERS from disk — must not be subtracted from Q4: disk would
still hold the other value, and the two would disagree about the same year. The fallback is
the disk read, which is what a reader can check.

**8 new tests** on the de-cumulation alone; **457 pass** in `src/web_scraper/`, none needing a
PDF, a network or an OCR engine.

### ⚠️ 6-2-quinquagies. AND THE TWO MACHINES DO NOT RUN THE SAME CASCADE — `tesseract@200` IS LAYER 4 HERE AND ABSENT THERE

Measured 2026-08-31, on the way to the above, and it is the sharpest cross-machine finding yet.
§6-2-duodetricies recorded that Kaggle has no Tesseract and priced it as WALL CLOCK — *"the T4
entered 45 layers, not 47"*. What nobody had recorded is the consequence for WHICH LAYER WINS.

`tesseract@200` sits at **position 4 of 55**, ahead of every relaxed onnx layer. So on a
machine that has Tesseract it can accept a statement that the onnx layers would have read
better twenty layers later — and on Kaggle it is skipped and those layers are reached.

**BSR Q3-2019, the same document, the same commit:**

| | winning layer | reads |
|---|---|---|
| Kaggle (wrote the disk row) | **`onnx@300+tail`** | `hdtc_4_tien_chi_tra_no_goc_vay` = −1,070,886,323,063 |
| this laptop | **`tesseract@200`** | **−11,070,886,323,063** — a leading digit invented |

11 of 56 balance-sheet cells and 11 of 24 cash-flow cells differ. ✅ **The merge refuses all of
it as DIFFERS, which is refusal 3 doing exactly its job** — but the honest reading is that **a
local re-parse of a Kaggle-parsed ticker is not a re-run of that parse**, and the difference is
structural rather than the diacritic-level drift §6-2-tricies measured.

⚠️ **THE PRACTICAL COST IS REAL AND IT LANDED IMMEDIATELY.** BSR's Q4-2019 and Q4-2020 income
statements can only be de-cumulated once Q3-2019 and Q3-2020 carry a recorded `months=3`, and
the way to record it is to re-parse them — which locally lands on `tesseract@200` and is
refused. **The remedy is to restrict the cascade to the onnx layers** (`--layers`, 53 of the
55), which reproduces the cascade the disk rows were produced under, or to run it on Kaggle.
Cherry-picking a single layer would not be the same claim.

⚠️ **This is `PGB-1`'s *"a half-right layer that passes the gates ends the cascade"* in a new
place** — the half-right layer is an ENGINE that exists on one machine and not the other, so
the cascade's shape, and not only its arithmetic, is part of a run's provenance. `stack_
fingerprint` records the library versions; it does not record which ENGINES were reachable.

✅ **CLOSED THE SAME DAY — AND THE ENGINE WAS NEVER MISSING. §6-2-unquinquagies.** The library half of that last sentence is fixed too: every run now records `environment.ocr.tesseract`.

### ✅ 6-2-unquinquagies. THE TWO MACHINES RUN THE SAME 55-LAYER CASCADE NOW — and the engine was never missing

Asked 2026-08-31, straight after §6-2-quinquagies measured the divergence: *why does Kaggle not
have tesseract, and can it be installed?* ⚠️ **The premise was wrong, and it had been written
down three times without ever being measured** — `kgpu/export.py`, §6-2-duodetricies (*"Tesseract
is not installed there"*) and this file. **Kaggle has had a Tesseract all along.**

| measured 2026-08-31 | |
|---|---|
| `mupdfcpp64.dll`, the wheel this machine runs | carries `thirdparty/tesseract`, leptonica, `TESSDATA_PREFIX`, *"Tesseract language initialisation failed"* |
| **`libmupdf.so.29.0`** in `pymupdf-1.28.0-cp310-abi3-manylinux_2_28_x86_64.whl` — the wheel `requirements-ocr.txt` pins on BOTH machines | **carries the same strings** |
| probe here: `C:\Program Files\Tesseract-OCR` off PATH (`which tesseract` → `None`), a tessdata folder holding **only** `vie.traineddata` | `get_textpage_ocr` returned **1,594 characters of correct Vietnamese** |

**So pymupdf STATICALLY EMBEDS Tesseract and Leptonica**: no apt package, no binary, no
`osd.traineddata`. What a worker lacked was a **12.4 MB language model** and one line of
environment — and, behind that, a probe shaped like Windows.

#### What was actually skipping the layers — four mechanisms, and only the last is a defect

1. `tesseract@200` is layer **4 of 55** and `tesseract@400+relax` layer **7**.
2. `cafef_financials.py:1027` drops a layer whose engine is not ready with a bare `continue`.
3. `ocr_ready` for tesseract asks ONE question: is there a `vie.traineddata` in `TESSDATA_DIR`.
4. ⚠️ **`TESSDATA_DIR` was `$TESSDATA_PREFIX` or `%LOCALAPPDATA%\tessdata`** — on Linux both
   are absent, so `os.path.join("", "tessdata")` yields the **relative** path `tessdata` and the
   answer is False, whatever the image holds.

⚠️ **AND THE EVIDENCE WAS ALREADY ON DISK, UNREAD**: the Kaggle BSR run of 2026-08-31 has **0**
lines containing `tesseract` in its `run.log`; the local run of the same ticker the same day has
**40**.

#### What shipped, and the two decisions inside it

`vie.traineddata` travels in the payload as `pdf_ocr_job.MODEL_FILES["tessdata"]` — the fourth
file, beside the two onnx binaries and the recogniser config — so `kgpu.export` stages it (and
RAISES rather than shipping fewer, the guard `VCR-1` earned), `kgpu_bootstrap` points
`TESSDATA_PREFIX` at it, and `use_models` **rebinds the module global as well as the env var**,
because `TESSDATA_DIR` is read at IMPORT time and `cafef_pdf_parser` is always imported first.

1. ⚠️ **`apt-get install tesseract-ocr-vie` IS REFUSED, AND THAT IS THE ALIGNMENT RULE, NOT A
   PREFERENCE.** Ubuntu's package is a different build of the model under the same name: it
   would align the package and diverge the characters, which is exactly the failure
   §6-2-duodetricies measured on onnxruntime — *same version is not the goal; same output is*.
   The payload ships **our bytes**, and `tessdata_candidates()` deliberately scans **no system
   directory** for the same reason.
2. ⚠️ **A DATA FILE HAS NO VERSION, SO ITS PIN IS A DIGEST.** `pdf_ocr_job.TESSDATA_MD5 =
   d4d5bd4f9864702c44d059205ba82270` (12,435,550 bytes) is the model every tesseract-won `pdf`
   row in this repo was read with, and `engine_report()` records the md5 of whatever actually
   ran plus whether it matched — **reported, never enforced**, the rule `pin_violations`
   already follows.

⚠️ **`environment.ocr.tesseract` IS THE HALF OF THE ARTEFACT THAT WAS MISSING.**
`stack_fingerprint` records the pip stack and said nothing about which ENGINES were reachable,
so two runs of one filing ran 55 and 53 layers and neither artefact could say why. Two runs may
now only be compared on their cascade if that block matches.

#### Verified — and every check is local, because no quota was spent

| | |
|---|---|
| the worker CONDITION, simulated (no `LOCALAPPDATA`, no `TESSDATA_PREFIX`) | **before**: `TESSDATA_DIR = 'tessdata'`, ready **False** — the defect, reproduced. **after** `use_models(payload)`: ready **True**, and one real page of the shipped VCB filing OCR'd to **200 word boxes** of correct Vietnamese |
| the payload | 22 files / 103.8 MB, `models/vie.traineddata` present, manifest names four models |
| `kgpu rehearse`, **both** mount layouts | `tesseract layers : READY (md5 d4d5bd4f9864702c44d059205ba82270)`, `cascade: 55 layers` |
| ⚠️ the LOCAL default path, which must not move | VCB Q1-2026 re-parsed end to end: **3 of 3 statements REPRODUCED at `onnx@200`**, 59/22/17 items, 45.8 s |
| tests | **487** across `src/web_scraper`, `src/kaggle_gpu`, `src/utils` — 6 new, none needing a PDF, a network or an engine |

⚠️ **NO KAGGLE RUN WAS MADE.** The T4 side is verified by rehearsal and by a simulated worker
environment, not by quota — so *"the worker will now run 55 layers"* is a prediction with the
inputs checked, and the first real round trip is what confirms it. Its `metadata.json` will say
so in one field.

#### ⚠️ What this does NOT do

1. **It repairs no row on disk.** BSR Q3-2019 was written by a 53-layer Kaggle cascade and a
   local re-parse still lands on `tesseract@200` with a different figure; `pdf_ocr_merge` still
   refuses that as DIFFERS, correctly. Re-parsing it is a separate decision.
2. ⚠️ **IT LETS KAGGLE WIN ON TESSERACT TOO — which on that quarter invented a leading digit.**
   Matching cascades is not the same as a better cascade, and the other way to match them is
   still open and is one argument long: `--layers` with the 53 onnx names. **This closes a
   PROVENANCE gap, not a correctness one.**
3. **The payload grows 12.4 MB** (103.8 MB for one filing), and a document that defeats the
   cascade can now pay **7 OCR passes instead of 5** on the worker — the two tesseract passes
   are 200 and 400 dpi, and the 400 is the dearest pass there is.
4. ⚠️ **The two environments are still not identical, and the residue is unchanged**: torch
   (2.5.1+cu121 here, 2.10.0+cu128 there — measured unalignable on a running kernel,
   §6-2-undetricies), the Python patch level, the OS and the silicon. What IS now aligned is the
   whole OCR stack: nine pinned packages, plus a tesseract that is identical by construction
   (one pymupdf version is one mupdf build is one embedded tesseract) reading a model that is
   identical by digest. Measured against the last T4 run's recorded stack: **9 of 10 packages
   equal, `torch` the only difference.**

   ⚠️ **AND CLOSING THAT LAST ONE WAS CONSIDERED AND DECIDED AGAINST, 2026-08-31.** Both routes
   were priced: moving the WORKER down needs an untried subprocess trick plus ~2.5 GB per run,
   and moving THIS machine up invalidates `env_fingerprint` for the whole modelling archive
   (`feature_selection.contract.SETUP_KEYS`) — while **the silicon differs either way**
   (sm_86 against sm_75), so byte-identical output is unreachable and the residue §6-2-tricies
   actually measured is ONE diacritic on an unmapped row with every figure equal. **What
   protects correctness is `compare()`, `rows_sha` and the fingerprint, not the version
   number.** Recorded as a decision, not as a thing nobody got to.

⚠️ **AND THE REHEARSAL'S FIRST RUN PRINTED *"NOT the pinned model"* OVER THE PINNED MODEL.** A
rehearsal executes the PAYLOAD's copy of the repo, and that payload had been staged minutes
before `TESSDATA_MD5` existed — so `matches_pin` was ABSENT and the line read it as False. §5
rule 2 at a progress line, and it cost nothing only because the md5 was printed beside it. Three
states now: matched, not matched, and *"this payload's code predates the pin"*.

#### ⚠️ AND CHECKING THE DIFF FOUND `PIN-1`: THE ALIGNMENT FILE ITSELF WAS NOT IN THE REPO

`.gitignore`'s blanket `*.txt` had swallowed **`src/web_scraper/requirements-ocr.txt`** — the
file that calls itself *"THE OCR STACK, PINNED ONCE, FOR BOTH MACHINES"*, that the Kaggle
worker installs, and that `pdf_ocr_job.requirement_pins()` reads — and
`requirements-ocr-torch.txt` with it. ⚠️ **The failure is silent in the worst direction**:
`requirement_pins` returns `{}` for an absent file, so `pin_violations()` returns `{}` too and
every run records *no violations*, which is indistinguishable from a machine honouring every
pin. **A check that cannot run must not look like one that passed** — `DEP-1`, `SAN-1`, `CWD-1`,
and now this. ⚠️ **The identical trap had already bitten and been patched for ONE file**:
`.gitignore` carries `!src/kaggle_gpu/requirements.txt` with a comment recording that `git add`
reported success and committed nothing. Both OCR files are un-ignored now, and they land with
the next commit.

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

**Open issues live in [ISSUES.md](docs/ISSUES.md)** (**48 open**, 38 resolved, codes permanent — ⚠️ **`LGU-1` OPENED-AND-CLOSED 2026-08-30**: a filing old enough to predate Unicode declares its unit in **VNI-Times** — `ÑVT : Trieäu ñoàng` — which `norm` reduces to `trieaunoang`, so `_declares_millions` could not see it and every figure was read as đồng. **A uniform 10⁶ error reconciles perfectly against itself**, and the two statements of ACB's Q3-2009 went opposite ways: `sane` had a band for the balance sheet and REFUSED it (the quarter read `missing`, which looks like an OCR failure and is a gate working), and had an EMPTY band for the income statement, which reached disk as `pdf` with a pre-tax profit of **641,749 đồng for a bank holding 169 trillion**. Fixed in the DEFAULT path — the statement is accepted at layer 1, so no later layer is ever reached — with the blast radius measured BOTH ways first: **4 of 1,196 filings carry the spelling, 2 are before the period floor, and every ACB 2008 cell reads `missing`**, so no existing `pdf` row could be damaged (§6-2-sesquadragies) — ⚠️ **`ROT-1` AND `QUO-1` OPENED-AND-CLOSED 2026-08-30, BOTH IN ONE PAGE OF ONE FILING**: BID Q3-2011's income statement is a LANDSCAPE page scanned into a portrait one, `/Rotate 0` like every other page, so 47 layers reported `no such statement on any page of this filing` — and **a statement that is never found is the EXPENSIVE failure**, because the scan then runs to the last page and every layer re-asks (2 m 30 s and nothing, against 29 s and all three statements once the page is turned). `QUO-1` is the dangerous half: the opening bracket of a negative figure comes back as a quote, `parse_num` refuses it, and `_first_value` takes the **PRIOR-PERIOD column** instead — which reconciles, because an income statement is anchored only on PBT (§6-2-quinquadragies) — ⚠️ **`CWD-1` OPENED-AND-CLOSED 2026-08-30 AND IT IS THE ONE TO READ BEFORE ANY MERGE**: `pdf_ocr_merge` resolved the statement CSVs through a RELATIVE default, so from any cwd but the repo root — `kgpu merge` runs from `src/kaggle_gpu/` — it read an empty directory and reported every quarter `on_disk="absent"`. **That is a legitimate state for a ticker being bootstrapped (`BND-1`), so nothing looked wrong**, and the refusal that stands between a merge and a wrong figure on disk simply could not fire: the BID Q4-2016 repair planned **2 writes from `src/` and 0 from the repo root**. ⚠️ *Widening what an answer is allowed to mean can silently disarm a guard that reads it* (§6-2-quadragies) — ⚠️ **`VCR-1` OPENED 2026-08-29 AND IT IS THE ONE TO READ BEFORE ANY OCR RUN**: `vietocr` fetches its CONFIG from `vocr.vn` on every `Predictor` build and caches nothing, so when that host's TLS certificate expired every `onnx@*` layer RAISED and the cascade fell through to `tesseract@200` — **VCB Q1-2026, which had read `onnx@200` with 98 of 98 cells reproducing, came back with 13 different columns and both gates passing**. A degraded engine does not look like a failure, it looks like a different answer. ✅ Contained the same day, and ✅ **CLOSED 2026-08-29**: a layer that RAISES is recorded as an `engine_error` and `pdf_ocr_merge` refuses that document whole, and the local config **is in the repo now** — `src/web_scraper/models/vietocr_vgg_seq2seq.yml`, tracked, merged from vietocr's own public base+arch yamls over verified TLS, and proven equal to what vocr.vn served by reproducing VCB Q1-2026's 98 cells. ⚠️ *"A decision about trust, not a code change"* was written here before anyone looked for a third route; there was one, and a copy was already in this repo (§6-2-sextricies) — ⚠️ **`MSO-1` and `SPL-1` opened-and-closed 2026-08-29, both found in ONE quarter of ONE non-bank filing, and both write WRONG FIGURES rather than refusing**: `MSO-1` is the VAS `Mã số` item-code column read as a period (3-4 digits, exactly the overlap `NOTE_MAX_DIGITS` cannot cover) — it is already on disk in VIC Q1-2011 — and `SPL-1` is one printed figure returned as TWO detector boxes 3.8pt apart, 60 of them in one statement, with both grand totals whole so every gate passed (§6-2-tretricies) — ⚠️ **`FXM-1` OPENED 2026-08-25 with a fix that is WRITTEN AND UNMEASURED**: the FX adjustment line cannot be mapped, and it is the single bottleneck behind **8 of the 11 probed BID cash-flow refusals** — the balances are already recovered and then discarded for want of a fourth term. TODO `P39` is the measurement and it is not optional (§6-2-quindecies) — ⚠️ **`TPL-1` OPENED 2026-08-25 and it is the one to read before any non-bank parse**: the non-bank wall is NOT a missing template — all four charts of accounts exist — it is seven hardcoded reconcile anchors, and on `corp` and `insurance` the cash-flow one **fuzzy-matches the OPENING balance and returns it as the closing one** (0.885 / 0.902 against a 0.85 threshold, first hit wins in statement order). A wrong figure, not a refusal; `securities` fails safely instead, below the threshold at both ends (§6-2-quaterdecies) — ⚠️ **`FIN-1` CLOSED 2026-08-24** — no financials row anywhere reads `source='cafef'`; ⚠️ **`GLB-1` and `BRZ-1` opened the same day**, both found by the carry-up: `GLB-1` is a star import rebinding `glob` from the function to the MODULE, breaking all 11 call sites in `preprocessor.py`; **`BRZ-1` is the sharper one — a row deleted at the SOURCE is never deleted from bronze**, because every `_ingest_bronze_*` upserts, and no freshness check can see it (§6-2-terdecies) — ⚠️ **`SAN-1` opened-and-closed 2026-08-24 and is the one to read**: the magnitude guard `sane` learns its baseline from the quarters accepted in its own run, so one 2-line statement became the whole reference population and silently rejected every correct quarter after it (§6-2-undecies) — ⚠️ **`FIN-1` OPENED 2026-08-24 and it is the one to read if you touch fundamentals**: 34 financial report-rows on disk were transcribed from CafeF's HTML tabs rather than parsed from the filing PDF — the fallback fires on any absent period without checking whether a PDF exists. ⚠️ **Only 4 can be retried, all VCB**: `documents()` keeps `consolidated == "True"` only and ACB filed no consolidated statement before 2010. §5 rule 24 now forbids the source outright; the code still defaults `use_api=True` (§6-2-octies) — ⚠️ **`SCH-1` and `DEP-1` opened-and-closed 2026-08-23, both found by `pipeline.freshness` on its first run**: `SCH-1` is **28 of the 30 single-name unified schemas stale**, their dates a fossil record of every scoped re-scrape (§6-2-quinquies); **`DEP-1` is the sharper one — a MONITORING VIEW BLOCKED EVERY REPAIR IT RECOMMENDED**, because a PostgreSQL view records a dependency on its tables and every builder here opens with `DROP TABLE`. Fixed by making the health objects `plpgsql` FUNCTIONS, whose bodies are not parsed for dependencies. ⚠️ **`STA-1` CLOSED 2026-08-23**: `gold.stocks_ta` rebuilt, 0 of 13 legacy names left, matching silver exactly, and the `basic + ta` join no longer truncates — which also closed **`SKW-1`** (§6-2-quater); ⚠️ **`FRZ-1` CLOSED 2026-08-23**: the price universe is fresh again, 771 of 784 tickers at 2026-08-21 against 5, and the fix was an `incremental` scrape mode whose restatement guard fired on 304 of 780 price tickers (§6-2-bis); **`SCP-1` opened-and-closed 2026-08-22** (a log-only helper assumed one bound parameter and took down a build) and **`FRZ-1` re-measured the same day**: 757 of 781 tickers stale, the cross-section ending 2026-06-25 while `MAX(date)` reads 2026-08-19 from five names; `WFO-1` closed and `BOO-1` opened-and-closed 2026-08-21; `PNL-2`/`PRB-1` closed and `VRM-1`/`FRZ-1` opened 2026-08-19). ⚠️ Counts here are a SCAN of the tables, not a running decrement — the previous "36 resolved" was one ahead of the file. ⚠️ **Several FIXED rows deliberately sit inside the Open table rather than moving** (`WFO-1`, `VRM-1`, `PNL-2`, `PRB-1`, and now `SCH-1`/`DEP-1`), each marked `✅ FIXED <date>` in words — **strikethrough was removed from the whole corpus on 2026-08-23**, so a row's status is read from its text and never from damaged type.
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
| [src/web_scraper/CONTEXT.md](src/web_scraper/CONTEXT.md) | **48.5k** | touching a scraper, the PDF/OCR statement parser, or `raw_data/` layout |
| [src/feature_selection/CONTEXT.md](src/feature_selection/CONTEXT.md) | **45.0k** | running or reading a selection, or quoting any IC / null / bar number. **§15a is the STEP-BY-STEP UI GUIDE** for the country sweep (§15a-cli is the same in PowerShell); §15b-§15d the two guards and the cost table; **§16 is the GPU conversion** — what moved, what was measured slower and left alone; §14c is the measured cut that replaced `max_features=12` |
| [src/feature_selection/docs/RANKER_COMPARISON.md](src/feature_selection/docs/RANKER_COMPARISON.md) | **4.5k** | asking which ranker to keep, drop or add, or quoting any per-ranker cost. The full scorecard behind `feature_selection` §19 — advantage vs a random-k control, both cost regimes, the ρ=0.864 duplicate pair, the REJECTED mRMR addition, and the two errors the measurement had to correct |
| [src/final_features/CONTEXT.md](src/final_features/CONTEXT.md) | **6.8k** | building or rebuilding a `__final__` table |
| [src/train_test_creator/CONTEXT.md](src/train_test_creator/CONTEXT.md) | **4.9k** | building a dataset, or asking about the purge/impute/scale/window steps |
| **[src/model/CONTEXT.md](src/model/CONTEXT.md)** | **12.0k** | training, adding a model type, or quoting any run's numbers. **§1a is the RUN STANDARD** (naming/input/output, enforced); §7 the new-model recipe; **§13–§16 are today's results** — CNN, Tier 1, Tier 2, the bank panel; §10–§11 the older research log ⚠️ now a citation without its evidence (RPR-1) |
| **[src/walkforward/CONTEXT.md](src/walkforward/CONTEXT.md)** | **16.0k** | asking whether a result survives more than ONE split, or which MODEL to use. §3 the 10-fold h=20 result (pooled Sharpe **+1.991**, IC positive 9/10 folds, beats the market 10/10); §4 the recorded prediction that was half wrong; §5 the no-mechanical-leak check; **§8 is PRF-8 — three architectures from 205 k params to 1,400 tree nodes, all tied**, and §8c the concurrency trap that voided a whole sweep |
| **[src/backtest/CONTEXT.md](src/backtest/CONTEXT.md)** | **8.1k** | asking whether a signal is TRADABLE — stage 9, the costed non-overlapping backtest. §3 is the cost identity that decides the horizon (h=5 pays **17.6 %/yr** in fees, above the top-100 benchmark's entire return); §4 the first result here to clear a costed null (top-15, z = **+4.29** test / +6.10 val); **§5 is the single-stock answer and it is "no trade"** |
| [src/result_evaluator/CONTEXT.md](src/result_evaluator/CONTEXT.md) | **4.1k** | scoring, the metric set, or panel-vs-series grain. ⚠️ **STALE — it predates `index.py`, the `rebuild_index` schema change and issue NUL-3.** Nothing in it is false; it is silent about all three |
| [src/pipeline/CONTEXT.md](src/pipeline/CONTEXT.md) | **7.6k** | the **six**-stage chain, staleness, `--root`/`--scope`, `--rescrape`, adding a stage or a second target |
| [src/sentiment/CONTEXT.md](src/sentiment/CONTEXT.md) | **3.4k** | anything news/text/PhoBERT |
| [src/kaggle_gpu/README.md](src/kaggle_gpu/README.md) | **7.5k** | running a repo notebook on a Kaggle T4 — the payload dataset, the parameter patcher, `rehearse`, **§7b PANEL MODE** (the one job that ships no pools), and §7's five measured traps (all five are "a green step is not evidence"; the fifth, `KGP-1`, had no green step at all) |
| [experiment/CONTEXT.md](experiment/CONTEXT.md) | **9.2k** | the 9 exploratory experiments — signal discovery, tradability, point-in-time data, VN OCR |
| [experiment/experiment_10/CONTEXT.md](experiment/experiment_10/CONTEXT.md) | **44.0k** | writing the literature chapter. **§"Combined reading" (line 2877) is the distillate** — read that alone unless you need a specific paper |

⚠️ **[ISSUES.md](docs/ISSUES.md) (~33.8k) is the second file to open, not an afterthought.**
**48** open issues — ⚠️ *(this line read "42" and "40" earlier on 2026-08-30, "34" and "28" earlier on 2026-08-29, "22" until 2026-08-28 and "(~4k)"/"Sixteen" until
2026-08-25; a stale count is what a session budgets against)*. ⚠️ **CFB-1 IS THE ONE TO READ
BEFORE QUOTING A BID FUNDAMENTAL** (opened 2026-08-28): a cash-flow anchor can hold the wrong
ACCOUNT and every gate passes — **7 BID quarters carry the 1-Jan opening in the CLOSING slot**,
2 in the movement slot, 4 hold a wrong FX value, and Q3-2011 holds a **negative** closing cash
balance of −23,457,326,032,339. All 7 were accepted at `onnx@200`, a STRICT layer where
`verify_cash` is off so the identity never ran; `reconcile` has no sign test and `sane`
compares `abs()`. ACB is clean; VCB's mismatches are mixed OCR and restatement, so it is a
SCREEN, not a verdict. TODO `P43` is the fix and is the top row. **SHP-1** is the next to read — a `value`-only filter silently discarded 71% of the forex folder for as long as that
ingest existed, and the same filter sits unchecked on four sibling ingests. ⚠️ **TPL-1**
before any non-bank financials parse: two of the seven reconcile anchors return the OPENING
cash balance as the closing one on `corp` and `insurance`. **Four** change how a number may
be read:
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
| **[ISSUES.md](docs/ISSUES.md)** | 20 open / 38 resolved, permanent codes | before quoting any number — four of them change how a number may be READ |
| **[TODO.md](docs/TODO.md)** | the one backlog — ⚠️ **DATA FIRST.** Seven groups — ⭐ **the TOP ROW is `P43`, in the NEW group `0 · PARSER`** (added 2026-08-27: `P41`-`P45` plus `P39`, from a review of `cafef_pdf_parser.py` + `cafef_financials.py` against the three parsed tickers ON DISK. ✅ **`P39` DONE 2026-08-27** — its positional FX guess was guarded behind a FLAG, so the guard was live on 3 of 47 layers and had already written MERGER CASH into BID Q4-2015 and Q2-2017 with the identity confirming both; fixed, blast radius measured at 30-unchanged/2-dropped over 32 candidates, both cells repaired, and **two leftovers moved into `P43`** rather than to a new code (§6-2-quinvicies). ⚠️ **`P43` is now the top row and it is wrong numbers every gate passed** — its free cumulative-cash invariant flags **10 BID rows**, 7 of them the 1-Jan opening sitting in the CLOSING slot, plus 4 FX cells written at strict layers and the `alternates` restatement guard `FXM-1` still needs. ⚠️ **Two are the cost `P38`/`P6` are budgeted on** — `P41` is the unbounded, 22×-repeated, bank-only share-capital note scan, which is the `document size` term §6-2-noviesdecies could not find); **A data `P2`, B OCR `P38`/`P6`/`P5`/`P4`** (⚠️ `P3`, the JSON gate, is CLOSED BY DECISION and archived UNMEASURED), C output `P7`-`P8`, D model `P9`-`P17`, E honesty `P18`-`P21`, F backlog `P22`-`P36`. ✅ **`P1` DONE 2026-08-23** (§6-2-quinquies). ⚠️ **THE NUMBERS ARE FROZEN AS OF 2026-08-23 AND WILL NOT MOVE AGAIN** — a `P<n>` is a permanent NAME, exactly as an `ISSUES.md` code is, and **PRIORITY IS THE ROW ORDER**, so read the list top-down and cite the number. The list starts at `P2` and the numbers need not stay monotonic; that is the price of a code that means one thing forever. ⚠️ **A HYPHENATED code is retired** (`PRF-4` is now `P11`, `P4-2` is now `P21`, …). ⚠️ **A `P<n>` written BEFORE 2026-08-23 still resolves to a different item** — three renumbers in two days preceded the freeze — so take the DATE of what you are reading, then TODO.md's two crosswalks, which are the last two that will ever be needed | deciding what to do next |
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
- ⚠️ **NO STRIKETHROUGH, ANYWHERE — 108 markers were removed on 2026-08-23.** A closed
  item is marked with words (`✅ FIXED <date>`, `DONE`, `SUPERSEDED`, `RETIRED`) and its
  text stays in ordinary type. Struck-out text renders as damaged and reads as *"ignore
  this"*, which is the opposite of what a closed row is for here: **the measurement it
  leaves behind is the point**, and `ISSUES.md` rows are cited BY CODE from this file and
  from the `CONTEXT.md` files. Nothing was lost in the removal — every struck row already
  carried its status in words beside the markup.
- ⚠️ **A `TODO.md` number is a permanent NAME and priority is the ROW ORDER** (frozen
  2026-08-23). Codes are never renumbered or reused, exactly as in `ISSUES.md`; the list
  therefore starts at `P2` and need not stay monotonic. **Read the order, cite the
  number.** Three renumbers in two days preceded the freeze, and each one silently
  repointed every `P<n>` written before it.
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
