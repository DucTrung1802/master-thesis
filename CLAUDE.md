# CLAUDE.md — master-thesis

@.claude/current_state/INDEX.md
@.claude/rules/common.md

> **THE MAP, AND NOTHING ELSE.** Every claim here was MEASURED; the evidence behind it lives in
> [`.claude/`](.claude) and is named in §7. ⚠️ **A number without a null is descriptive, not
> evidence** — verify before acting, and write a new measurement down where it was made (§8).
>
> ⚠️ **THIS FILE WAS 2,549 LINES UNTIL 2026-09-06 — the map had become the territory.** §2-§6's
> evidence moved VERBATIM to [`.claude/findings/`](.claude/findings/) and §5's rule bodies to
> [`.claude/rules/standing-rules.md`](.claude/rules/standing-rules.md), **each keeping its original
> heading**, so a `§2b-bis` / `§6-1` / `§5 rule 24` citation still resolves — to those files.
> [`common.md`](.claude/rules/common.md) **R2 caps this file at 300 lines** and
> `python .claude/tools/state_check.py` fails when it is over. ⚠️ **~196 `§6-2-*` citations name
> sections deleted the same day** — `git show ea07d4aa`.
>
> ### The four registers — one job each, no overlap
>
> | file | answers | when you touch it |
> |---|---|---|
> | **CLAUDE.md** | *what is this, and what has it PROVED?* | auto-loaded; the map and the verdict. **Root, not `.claude/`** — the only place Claude Code auto-loads from |
> | **[.claude/runbook/RUNBOOK.md](.claude/runbook/RUNBOOK.md)** | *how do I RUN it?* | ~40 command templates, each with what it writes and its measured cost |
> | **[.claude/current_state/ISSUES.md](.claude/current_state/ISSUES.md)** | *what is BROKEN?* | permanent codes; never renumbered or reused |
> | **[.claude/current_state/TODO.md](.claude/current_state/TODO.md)** | *what is NEXT?* | one list, priority is the ROW ORDER. ⚠️ A bare `P<n>` is a permanent NAME; a HYPHENATED code (`P1-9`, `PRF-8`) is RETIRED |
>
> Movement is one-way: a TODO item that turns out to be a defect **graduates to ISSUES.md with a
> code**; a fixed ISSUES entry keeps its row; a done TODO item **leaves its measurement in
> `.claude/findings/` and is deleted, not ticked.** ⚠️ `.claude/current_state/` holds SNAPSHOTS —
> what a command printed — and is not a fifth register.

---

## 1. What this project is

A master's thesis on **predicting Vietnamese stock prices** — one repo carrying a **production data
pipeline** (scrape → PostgreSQL medallion layers → feature pools → model → scored run) and the
**research record** of what it has been able to prove, which is mostly negative and deliberately so.

| | |
|---|---|
| database | PostgreSQL `database_main_v2`, schemas `bronze_schema` / `silver_schema` / `gold_schema` / `filter_schema` / `unified_schema_<universe>`. Creds in repo `.env` (`POSTGRES_*`) |
| orchestrator | **Dagster, 83 assets** — `src/orchestration/` is THE entry point |
| universe | 781 VN tickers (HOSE/HNX/UPCOM); VCB the single-name focus, VN30/VN100/LIQUID301/ALL/BANK the cross-sections |
| model | LSTM (2×128, ~276k params), CNN, GBT + the chain in §3. `model/common/engine.py` is the shared engine — a model package is a `model.py` + a ~30-line binding, **never a copy of `train.py`** |
| interpreter | `mt_env` venv (`d:/GIT/master-thesis/mt_env`), Python 3.12.10, Windows, RTX 3050 4 GB |

---

## 2. ⚠️ THE VERDICT — read this before proposing any modelling work

📂 **[.claude/findings/verdict.md](.claude/findings/verdict.md)** — §2a-§2d in full, with every table.

**Single-stock short-horizon prediction does not work here, established FIVE independent times**
(LSTM sweeps · feature selection · news sentiment · news events · five tickers at h=10). The
literature does not contradict it: of 23 papers, **not one reports a naive baseline**.

**What survives is the CROSS-SECTIONAL RELATIVE RANK at ~100+ names**, and only there:

| universe | N | observed IC | null p95 bar | z | clears |
|---|---|---|---|---|---|
| VCB · BANK · VN30 | 1 · 20 · 30 | +0.056 · −0.011 · +0.023 | +0.056 · +0.022 · +0.025 | +1.56 · −1.71 · +1.42 | ❌ all three |
| **VN100 · LIQUID301** | 100 · 301 | +0.029 · +0.077 | +0.012 · +0.025 | **+6.09 · +18.45** | ✅ |

Three things that ladder means: **the threshold is ~100 names, not ~30**; the mechanism is **`1/√N`
precision** (`n_eff` is `n_dates/h`, **not** `n_rows/h`); and **the rank target is the whole
result** — swapping `cs_rank_5day` for raw `return_5day` drops the IC 4×.

⚠️ **THE HORIZON IS THE VARIABLE NOBODY CONTROLLED FOR.** A momentum-only `controls` block loses at
rel5/rel10 (**−2.78 %** against a 9.75 % benchmark) and wins from **4 weeks out** (30.39 %, Sharpe
1.10). Four of the five defeats above were at `h=5`, where **fees alone (17.6 %/yr at τ=0.70,
50 bps) exceed the market's entire return.**

⚠️ **Honest caveats on the survivor** (§2c): the holdout gives **+0.011**, not +0.029; the universe
is **100 % survivors**; the costed walk-forward was net **+1.46 Sharpe (2017-20) vs −0.51 (2022-26)**
at 20 bps, dead at 40. **The one lever left is NEW INFORMATION, not more modelling** (§2d): intraday
aggressor imbalance → fundamentals → news event dates → point-in-time index membership.

---

## 3. The pipeline, end to end

📂 **[data-layers.md](.claude/findings/data-layers.md)** — the 83 assets, all ten `pool__*` tables
and what each measured, the forex ingest, the FILTER layer. 📂 **[model-chain.md](.claude/findings/model-chain.md)**
— the eight stages, the two selection layers, Kaggle panel mode, and §4's commands with their why.

```
raw_data/<source>/  19 landing assets   TradingView (Selenium) · CafeF · Simplize · GICS
   ▼ bronze_schema   20 assets/25 tables   raw-faithful, one per source/tab
   ▼ silver_schema   20 assets              canonical, cross-source merged, GICS attached
   ▼ gold_schema     11 assets              + features (TA ~900 cols, returns, vol, as-of macro)
   ▼ filter_schema    1 asset × 3 SCREENS   universe__<screen> — MEMBERSHIP, no time series
   ▼ unified_schema_<universe>  12 assets   pool__basic / __targets / __ta / __fa / __forex
       partitions: VCB BANK ALL VN30 29     / __economy_<country> / __funds / __bonds / __basic_bank
       single names PRICE10K LIQUID QUALITY / __stock_market / __news_daily / __market_breadth
── then eight stages, each `python -m <pkg>`, dry-run by default ──
   data → selection → shortlist_pool → selection_2 → final_features
        → train_test_creator → model.<arch> → result_evaluator → backtest → walkforward
```

⚠️ **`python -m pipeline` prints which stage is stale and runs the stale ones — it passes NO data
between stages.** ⚠️ **`d` and `h` come from the source TABLE NAME**, never a parameter. ⚠️ **Both
selection layers are MANUAL**; `--apply` stops at `shortlist_pool`. ⚠️ **`--root` + `--scope` are the
only things keeping two experiments off one table name** — `final_features` groups on
`(schema, target, setup)`, a key with **no term for which pools**.

---

## 4. Run it

⚠️ **[.claude/runbook/RUNBOOK.md](.claude/runbook/RUNBOOK.md) IS THE COMMAND SOURCE** — ~40
templates as one table (`O`/`C`/`W`/`D`/`K`/`F` row IDs), each with what it writes, its measured
cost, and the step that must follow it. **Cite a row ID; never copy a flag into another file.**

```powershell
.\mt_env\Scripts\Activate.ps1                        # once per shell
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"  # ⚠️ MUST be absolute, MUST be set
Clear-Content logs\app.log                           # before any pipeline/ingest run
python -m pipeline            # O1 — what is stale; writes nothing
python -m pipeline.freshness  # O3 — whether the DATA is fresh, per ticker
```

⚠️ **NEVER `Materialize all` / `*` / a bare backfill.** With every partition live that takes
`raw/cafef_pdfs` (**784 tickers, ~555 GiB with no year window**), `raw/trading_view@stocks` (777
tickers, ~10 h) and `raw/cafef_financials` (~2.4 h/ticker). Materialise a group or a named asset.
⚠️ **EVERY SCRAPE RUNS THROUGH DAGSTER** — never a script, never ad-hoc code, even for a one-off
backfill: a run outside it leaves no materialisation, no metadata and no partition status. If a run
needs a knob the asset lacks, **the work is adding the knob** (a `Config` class), not a wrapper.

---

## 5. ⚠️ Standing rules — learned expensively, one line each

📂 **[.claude/rules/standing-rules.md](.claude/rules/standing-rules.md)** — all 24 with the evidence
behind each, plus §5a RETIRED mechanisms and §5b removed folders. **Read the entry before leaning on
a rule.** The numbering is permanent: `§5 rule 24` resolves here and there.

**Evidence** — 1 every selection needs its OWN null, re-run when the pool or representation changes ·
2 an absent null is recorded as absent, **never implied to be a pass** · 3 `clears_bar` is wrong
whenever the null MAX exceeds the observed — **quote the max beside it** · 4 every single-score
holdout needs a shuffled-label control · 5 report the IC trend beside the mean. ⚠️ **10 draws to
FAIL something, 20 to PASS it** — `p` is pinned at the `1/(n+1)` floor either way; **`z` is the
statistic**, and 20 draws halve `SE(sd)` (0.0083 → 0.0051).

**Leakage & sample size** — 6 the purge gap is **`d + h − 1`, not `h`** · 7 `n_eff` is `n/h` for a
series and **`n_dates/h` for a PANEL** · 8 imputation is the TRAIN-slice median, **never
`ffill().bfill()`** · 9 never standardise a 0/1 label.

**The pipeline lies in specific ways** — 10 **a green asset is NOT evidence of fresh data**; the
per-series max date is the only honest check · 11 **"re-scraped" never implies "re-ingested"** ·
12 absent = OFF in `config.json`, and an unlisted asset RAISES · 13 selection IS the run plan
(`--select`) · 14 **disabling an asset does NOT disable downstream**, and a fresh screen leaves a
stale schema looking current.

**Data types & I/O** — 15 **`CREATE TABLE AS`, never a pandas round-trip** (`Decimal` → `object` →
VARCHAR) · 16 prefer `UnifiedSchemaReader.read`, which raises on empty · 17 SQL `LIKE '_'` is a
single-char wildcard · 18 **Windows/cp1252**: `encoding="utf-8"`, `sys.stdout.reconfigure(...)` at
the top of any one-off script, no `⚠️` in matplotlib text · 19 ephemeral scripts must load `.env` by
explicit path · 20 line-buffer anything long and write each unit to disk as it finishes.

**A finished run is not a run that checked itself** — 21 **a metric that CANNOT FAIL is not a pass**
(`hit_rate` = 1.0 on a price level) · 22 **coverage is a scalar and cannot see a FROZEN SOURCE** —
read `trailing_null_sessions` beside it · 23 **an all-NaN train slice is imputed to the constant
`0.0` and then RANKED**; a rising `ic_trend_per_fold` on a ragged pool measures **data arrival**.

**Where a number may come from** — 24 ⚠️ **FINANCIALS COME FROM THE FILING PDF AND FROM NOTHING
ELSE.** No HTML tab, no JSON endpoint, no transcription — not as a fallback, not to fill a gap.
**A quarter with no readable PDF is `missing`, and `missing` is the correct answer.** ⚠️ The code
still disagrees (`CafefFinancialsBuilder(use_api=True)`) — that is `FIN-1`, and the `source` column
of `bronze.cafef_financial_reports` is what makes it auditable. **Read it before quoting one.**

---

## 6. State today (2026-09-09)

⚠️ **If a number here disagrees with the database, the database is right and this section is the
bug.** It was 7 days stale once already.
📂 **[cross-sectional.md](.claude/findings/cross-sectional.md)** — the headline chain, seven
architectures, the dataset sweep, every caveat. 📂 **[single-stock.md](.claude/findings/single-stock.md)**
— the five defeats, the BANK panel, the VN30 run whose pooled answer flips.
📂 **[data-state.md](.claude/findings/data-state.md)** — freshness, the carry-up, the filings/OCR
summary, the data audit, and what exists right now.

**THE HEADLINE: the cross-sectional chain works out of sample, over TEN expanding folds, after
costs.** §2's verdict is about SINGLE-STOCK SHORT-HORIZON prediction and is untouched.

| pooled walk-forward, top-20 of 150, buyable only | **h=10** | h=20 (`PRF-1`) |
|---|---|---|
| periods · `se_sharpe` | **236** · 0.128 | 118 · 0.155 |
| Sharpe @20/30/50 bps | **+2.601 / +2.531 / +2.391** | +2.026 / +1.991 / +1.921 |
| CAGR@30 vs market | **+74.0 %** vs +13.9 % | +47.5 % vs +14.6 % |
| daily IC · `ic_t` | **+0.1412** · **+16.05** | +0.1097 · +6.90 |
| null, 200 within-date shuffles | **z = +18.58**, null MAX below observed | +12.28 |
| IC positive · beats the universe | **10/10 · 10/10** | 9/10 · 10/10 |

⚠️ **AND h=10 IS NOT ESTABLISHED OVER h=20**: paired on 2,360 shared sessions, mean return
**+17.0 pp/yr (t = +3.53)** but ΔSharpe **+0.44 [−0.079, +1.041]** — zero is inside the CI at every
cost level. **The chain stays at h=20**; h=10 has not won the test that matters.

**Four levers are now CLOSED by measurement** — a 101×-smaller model ties (`PRF-8`); 30 more
candidate channels tie (`PRF-9`); refitting twice as often moves nothing; and the ~45 % Sharpe decay
across the sweep is shared by both horizons. ⚠️ **The 13 channels ARE the result** — what remains is
honest execution and NEW DATA (§2d). ⚠️ **But "any model will do" is FALSE**: at h=10 a CNN loses
**0.40 Sharpe** (p = 0.001, the only arm surviving a six-arm correction), and the measured **seed
floor is `|d_sharpe| ≈ 0.09`** — four arms' ordering sits inside it.

**What the headline still does NOT say**: it **ranks, it does not price** (R² +0.0003, `mase` 0.9937
— a 0.6 % margin); `NUL-1` prices no selection, architecture, horizon, `k` or universe search
anywhere in this repo; **survivorship protects the `z` and not the CAGR**; no slippage, no ADV cap,
no floor-day exclusion on the SELL side.

**The data**: ✅ `FRZ-1` closed 2026-08-23 — **771 of 784 tickers fresh to 2026-08-21**, carried up
through gold and unified, `gold.stocks_ta` rebuilt (`STA-1`, `SKW-1` both closed), freshness is a
**DISTRIBUTION** now (`pipeline.freshness`, `health_schema.ticker_freshness('<layer>')`), and ⚠️
**fundamentals are 11 of 781** — the wall is **schema** (761 not banks, `TPL-1`/`CRP-1`), not disk.
⚠️ **A GAP IS A REFUSAL MORE OFTEN THAN AN UNREADABLE DOCUMENT** — off run folders, no OCR: MSN 30 →
11; **33 of GAS's 56 were ONE WORDING** (`GCW-1`, 2/35 → 32/35), 2 more a truncated total (`GTR-1`),
**19 of 20 "split boxes" are ONE box**, its "59 open" was **56**. ⚠️ **A pass is a gate opening, not
a cell** (§5 rule 21). ⚠️ **`BND-1` broke 2026-09-06**: 3-of-3 writes UNGUARDED. ⚠️ **AND A FINISHED
PARSE IS NOT A WRITTEN CSV** (`MRG-2`): MBB, **176 of 186 accepted, 0 written**. ⚠️ **SIX PARSER DEFECTS TOOK HPG 170 → 192 of 195, income statement 65/65** (2026-09-09) — `VAS-4` `SGB-1` `SGB-2` `EQU-1` `NSB-1` `TSM-1`, and ⚠️ **one of the 22 new cells is WRONG** (`DPC-2`).
[ISSUES.md](.claude/current_state/ISSUES.md) — **117 open**, 44 resolved, codes permanent. ⚠️
**SEVEN change how a number may be READ and are the ones to open before quoting anything**: `NUL-1`
(no null prices in any search) · `NUL-3` (**on a panel quote the daily-IC t-stat, never
`ic_clears`**) · `RPR-1` (29 run folders deleted) · `OUT-1` (one corrupt cell → a +0.266
correlation) · `CFB-1` (before quoting a BID fundamental) · `TPL-1`/`CRP-1` (before any non-bank
parse) · `FLT-1`/`SHP-1` (what forex can exist). ⚠️ **No strikethrough anywhere** — a closed row's
status is read from its words, never from damaged type.

---

## 7. ⚠️ Lazy loading — open ONE file, when you touch that thing

⚠️ **NEVER BULK-LOAD.** 131 `.md` files, **~529k tokens** — ~2.5× a context window.
[INDEX.md](.claude/current_state/INDEX.md) is the complete map with a measured cost per row **and is
already in your context**; this is the routing worth reading twice. **THE MECHANICAL RULE: before
you touch `src/<pkg>/`, open `.claude/context/<pkg>.md`** — one file per `src/` subfolder, named
after it, and a package folder no longer advertises its own evidence.

| touching this module | open this | ~tokens |
|---|---|---|
| `src/web_scraper/` — any scraper, the PDF/OCR statement parser, `raw_data/` layout | [.claude/context/web_scraper.md](.claude/context/web_scraper.md) | 64.9k |
| `src/orchestration/` — Dagster, `config.json`, any asset, any bronze/silver/gold table, the FILTER layer | [.claude/context/orchestration.md](.claude/context/orchestration.md) | 47.6k |
| `src/orchestration/preprocessor/` — HOW a table is built, the `_ingest_*`/`_helper_*` library | [.claude/context/orchestration-preprocessor.md](.claude/context/orchestration-preprocessor.md) | 26.1k |
| `src/feature_selection/` — running or reading a selection, or quoting any IC / null / bar | [.claude/context/feature_selection.md](.claude/context/feature_selection.md) | 45.4k |
| `src/walkforward/` — does a result survive more than ONE split? which MODEL? | [.claude/context/walkforward.md](.claude/context/walkforward.md) | 16.0k |
| `src/model/` — training, adding a model type, quoting a run's numbers (§1a is the RUN STANDARD) | [.claude/context/model.md](.claude/context/model.md) | 12.0k |
| `src/backtest/` — is a signal TRADABLE? (§3 the cost identity, §5 the single-stock "no trade") | [.claude/context/backtest.md](.claude/context/backtest.md) | 8.1k |
| `src/pipeline/` — the chain, staleness, `--root`/`--scope`, `--rescrape`, `pipeline.freshness` | [.claude/context/pipeline.md](.claude/context/pipeline.md) | 7.4k |
| `src/final_features/` — building or rebuilding a `__final__` table | [.claude/context/final_features.md](.claude/context/final_features.md) | 6.8k |
| `src/train_test_creator/` — a dataset, or the purge/impute/scale/window steps | [.claude/context/train_test_creator.md](.claude/context/train_test_creator.md) | 5.0k |
| `src/result_evaluator/` — scoring, the metric set, panel-vs-series grain. ⚠️ **STALE** | [.claude/context/result_evaluator.md](.claude/context/result_evaluator.md) | 4.1k |
| `src/sentiment/` — anything news / text / PhoBERT | [.claude/context/sentiment.md](.claude/context/sentiment.md) | 3.4k |
| `src/kaggle_gpu/` — a repo notebook on a T4: payload, `rehearse`, **§7b PANEL MODE** | [.claude/docs/kaggle_gpu.md](.claude/docs/kaggle_gpu.md) | 8.1k |
| `src/dtos/` — any dataclass. ⚠️ **a mixed-age attic**: `ModelConfigDto`/`ConfigDto` have 0 refs | [.claude/context/dtos.md](.claude/context/dtos.md) | 3.2k |

| doing this JOB | open this |
|---|---|
| **the EVIDENCE behind a number in §2-§6** — six files, by subject, ~2-10k each | [.claude/findings/](.claude/findings/) — the 📂 links in §2/§3/§6 point into it; `standing-rules.md` holds §5's |
| starting a session · running the chain · a selection · refreshing data · OCR a ticker · quoting a number · recording a finding · committing | [.claude/workflows/](.claude/workflows/README.md) — **8 guides, ~1.5k each; `/wf-list` names the slash command that runs each. A workflow is the ORDER; the runbook is the COMMANDS** |
| you want the COMMAND | [.claude/runbook/RUNBOOK.md](.claude/runbook/RUNBOOK.md) — 5.2k |
| **running the FILING OCR on a T4** (§1a is what *"OCR ticker `<SYM>`"* means; §8 what is missing) | [.claude/docs/PDF_OCR.md](.claude/docs/PDF_OCR.md) — 14.4k |
| which ticker, on which date? · how ONE number gets made end to end | [.claude/docs/pipeline.md](.claude/docs/pipeline.md) 4.8k · [PIPELINE_h10_CAGR74.md](.claude/docs/PIPELINE_h10_CAGR74.md) 7.2k |
| which ranker to keep or drop · how many null draws · fundamental definitions · feature groups · writing the thesis | [RANKER_COMPARISON.md](.claude/docs/RANKER_COMPARISON.md) · [NULL_DRAWS.md](.claude/docs/NULL_DRAWS.md) · [FUNDAMENTAL_INDICATORS.md](.claude/docs/FUNDAMENTAL_INDICATORS.md) · [feature_groups.md](.claude/docs/feature_groups.md) · [THESIS_PROGRESS_2026.md](.claude/docs/THESIS_PROGRESS_2026.md) + two `_VI` |
| about to run one of `src/model/lstm/configs/_legacy/` — ⚠️ **no purge, `ffill()`, no null** | [.claude/docs/model_lstm_legacy_configs.md](.claude/docs/model_lstm_legacy_configs.md) |
| writing a measured SNAPSHOT (freshness, coverage, what exists) | [.claude/current_state/README.md](.claude/current_state/README.md) — the contract for one |
| ⚠️ `vn30.csv` / `vn100.csv` — at the repo ROOT because **they are data, not docs** | **current membership, NOT point-in-time.** Never use one as a historical universe |

---

## 8. Conventions that hold across the repo

- ⚠️ **BEFORE YOU COMMIT, RUN `python .claude/tools/state_check.py`** (row `O5`) and resolve what it
  reports. A commit that changes what this project KNOWS must change where that knowledge is read:
  a measurement lands in `.claude/findings/` (or the package's `.claude/context/` file) and §6's date
  is bumped; a defect gets a permanent code in `ISSUES.md`; a finished item is DELETED from
  `TODO.md`; a new `.md` gets a row in `INDEX.md`. ⚠️ **The script REPORTS and never rewrites** — its
  counts are a SCAN, and a confidently wrong number is worse than none. Nothing enforces it at commit
  time by choice, so running it is the discipline.
- ⚠️ **THIS FILE STAYS ≤ 300 LINES** ([R2](.claude/rules/common.md), checked by that script) **and
  stays at the repo root** — the only place Claude Code auto-loads it from. When it grows, the prose
  goes to `.claude/findings/` and a pointer stays here.
- **`⚠️` marks a claim that cost something to learn.** Do not strip them; add one when you measure a
  new one. **Dates on findings, always** — a number without a date cannot be told from a stale one.
  **Record what was MEASURED, not what was concluded**: the tables in these files are reproducible
  checks, which is why they are still trusted months later.
- ⚠️ **"OCR ticker `<SYM>` LOCAL|KAGGLE" IS A REQUEST FOR A PREPARED NOTEBOOK, NEVER FOR A RUN** —
  clone the control notebook, edit **cell 2 only**, resolve the parameters read-only, report what
  they resolved to, and **stop**. The wait is the point. [ocr-a-ticker.md](.claude/workflows/ocr-a-ticker.md)
- ⚠️ **ANYTHING THAT REPORTS PROGRESS PRINTS `xx.x% - task - sub-task - detail`**, leading with the
  fraction of the WHOLE thing the reader started — formatted by **`src/utils/progress.py` and by
  nothing else**. The number is a POSITION IN A PLAN, never a fraction of the time left.
- ⚠️ **NO STRIKETHROUGH, ANYWHERE.** A closed item is marked in words (`✅ FIXED <date>`, `DONE`,
  `SUPERSEDED`) in ordinary type — **the measurement it leaves behind is the point**, and struck-out
  text reads as *"ignore this"*.
- **Nothing in `feature_selection` writes to the database**; `final_features` is the only stage that
  does — enforced by the package split, not a comment. **A run folder is immutable** (re-scoring
  rewrites metrics from `predictions_*.csv`), and **notebooks named `RUN__*.ipynb` are meant to be
  run** — everything else is a finished write-up.
- **git**: `src/model/runs/*/` and `src/train_test_set/` are ignored (only `index.csv` is tracked);
  `reports/feature_selection/` IS tracked; `raw_data/` is ignored except `raw_data/cafef/financials/`.
