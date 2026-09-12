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

## 6. State today (2026-09-13)

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
PARSE IS NOT A WRITTEN CSV** (`MRG-2`): MBB, **176 of 186 accepted, 0 written**. ⚠️ **SIX PARSER DEFECTS TOOK HPG 170 → 192 of 195, income statement 65/65** (2026-09-09) — `VAS-4` `SGB-1` `SGB-2` `EQU-1` `NSB-1` `TSM-1`, and ⚠️ **one of the 22 new cells is WRONG** (`DPC-2`). ⚠️ **AND THE COVERAGE NUMBER WAS TAKEN OVER THE SUCCESSES** (`GRD-1`, fixed 2026-09-10): the OCR path wrote no row for a quarter it FAILED on, so **1,014 quarter-cells had no row of any kind** and the same 3,193 statements read **92.5 %** of what was listed and **71.5 %** of the real 4,464-cell grid. The grid is contiguous on all 72 CSVs now. ⚠️ **AND ITS TOP EDGE STILL SAID NOTHING** (`GRD-2`, fixed 2026-09-11): the grid ran first-filed to last-filed, so a STALE SCRAPE and a company that stopped filing were one sentence — **91 tickers stop at Q1-2026 while 4 carry Q2-2026**, and BSR/MCH/TCX stop in 2019-2020. `corpus_ceiling()` extends every grid to the newest quarter that has ENDED *and* that some issuer has filed for (+261 rows, no rate change). ⚠️ **AND A RELATIVE `out_root` NAMED TWO DIRECTORIES** (`CWD-2`), so MSN's whole run merged nothing and said only `exit 0 and NO run folder`; ⚠️ **and a CPU-only cascade queued 120 s per document for VRAM it never touches** (`VRW-1`, 15 full timeouts in 158 documents). ⚠️ **AND THE REMAINING GAP IS UNSPENT GPU, NOT AN UNFIXED PARSER** (`RAT-1`): of the 765 cells with a filing and no `pdf` row, **332 were never opened**, **214 met only a SHORTER cascade**, **216 had PARSED and were never written**, and **3 lost to today's 117 layers**. ✅ The 216 came back with no GPU at all — `merge_batch` runs the arithmetic screens itself now and withholds only what fails the filing's own identities (177 released, 51 held), taking the corpus **71.5 % -> 75.5 %** and SHB **39.0 % -> 72.4 %**. ⚠️ **AND THAT SENTENCE EXPIRED ON 2026-09-11**: the 332 and the 214 have been run, and of the **241** cells still open **184 have already MET the full 115-layer cascade and lost**. The corpus is **3,723 of 3,958 winnable = 94.1 %**. ⚠️ **AND ON 2026-09-11 THE ONNX CASCADE RAN OUT OF QUESTIONS**: GVR's 18 and MSN's 11 have been asked, MSN's ten documents costing **2.51 h of GPU for 0 cells** with **16 of 16 accepted statements reproducing rows already on disk**, and **exactly ONE document in the corpus** holds a cell no full cascade has met — ACB 2009-Q3, a three-page Mẫu CBTT-03 summary form that cannot contain a cash flow. **There is no unspent GPU left**, so the 36 cells between here and 95 % each need a NEW READING. ✅ **A THIRD ENGINE IS NOW IN THE CASCADE** (`EOC-1`): `easyocr` had been implemented since the beginning with NO `ParseLayer` using it, on a note written while its branch skipped the split repair (`TSM-2`); measured on 8 open cells it won **2, both on SCANS** — and of the 205 open cells with a found page **89 are TEXT, where no OCR engine runs at all**, so its ceiling is 116. ⚠️ **AND THE PARSER DEFECT BEHIND THE TEXT HALF IS NAMED NOW** (`GLU-1`): the table's HEADER BAND is glued onto its first data row, so `a_tai_san_ngan_han` is emitted as `tai_san_a_tai_san_ngan_han` and the identity is evaluated against a role nothing answers — **62 of 1,131 refused statements**, and on VIC Q1-2009, whose face closes to the đồng, the cascade's own `code_column_by_value` repair leaves it refused unchanged. ⚠️ `SPR-1`'s repair is real and its reach is **about zero** — SHB 0 cells for 18 documents and 2.2 h, GAS 1 for 14 and 1.0 h — because `join_lost` already repairs that at **layer 55 of 115**. ⚠️ **AND EVERY REFUSAL RANKING ON DISK RANKS LAYER 1** (`RSN-1`): the cascade stores one reason per KIND from the FIRST layer that gave it, which is how a sweep aimed at 89 cells returned one. ⚠️ **AND A SHALLOW RUN COULD RETIRE A WINNABLE CELL AND RAISE THE RATE BY DOING IT** (`SET-3`, fixed 2026-09-11): a 2-layer tesseract cascade settled six SHB balance sheets that 115-layer runs were still refusing, reading 94.15 % against the honest 94.01 %. ⚠️ **AND A THIRD ENGINE IS NOT A LEVER ON A PAGE NOBODY FOUND** (VHM, 2026-09-12): 14 of its 15 winnable documents are SCANS and all 15 had lost to the 115 onnx layers, yet `ENGINES = ["easyocr"]` took **6 open cells to 0 in ~90 min** — four refusals reading `no such statement on any page`, which is a verdict on the page CLASSIFIER (`SET-2`). ⚠️ **FIVE GPUs FOR A DAY TOOK VN30 56.4 % -> 74.3 %** (2026-09-12): one RTX 3050 plus **four Kaggle T4 kernels** (2 accounts x **2 sessions each**, a hard API limit), a ticker per clone — **ten tickers from a blank CSV**, VPB 83.6 % and BCM 80.0 % down to **PLX 5.6 %**, and the yield does NOT follow the template (bank TPB 38.2 % < corp VRE 68.4 %). ⚠️ **RE-RUNNING A TICKER THE CASCADE HAS SEEN RETURNS ~0** — VPB 24.5 min, VHM 90 min, GAS 92.2 min, all for 0 quarters: a **2-of-3** quarter is HELD by the merge, disk still reads `missing` on all three, and `exhausted_quarters` cannot see it because it records only what was REFUSED. ⚠️ **AND THE LOCAL BOTTLENECK IS ONE CPU CORE, NOT THE CARD** — 540 cached re-map layers against 38 OCR passes, GPU median **0 %**, 96 % of ONE core of 20. ⚠️ **AND THE PLAN KEPT RE-OPENING WHAT IT COULD NEVER WRITE** (`OPB-1`): a cumulative Q4 needs `FY − (Q1+Q2+Q3)`, `span_operands` pulls in only a prior already reading `pdf`, and `ASK-1`'s skip withheld the Q3s — **19.1 min of GPU, 0 cells, same five next run**; `plan_batch` and `run_batch` now both ask the merge's own `_quarter_priors`. ✅ **AND 46 % OF THE OPEN CELLS TURNED OUT TO BE ALREADY PARSED** (`HLD-1`): `BND-1`'s lift is gated on `complete_periods`, the FILING, so a **2-of-3** filing's two good statements keep the empty-band refusal, the band never grows, and the next run holds them again — while `exhausted_quarters` records only what was REFUSED, so nothing could name them. **VN30, 2026-09-12: 358 of 779 open cells parsed and unwritten, 119 quarters holding all three in a run folder** (PLX: one 54-document run, 130 accepted, **52 written**). `unwritten_cells` names them and `release_batch` wrote **246 with no GPU at all** — cells **85.1 % -> 89.7 %**, quarters **74.3 % -> 76.0 %** — with **122 withheld** by the arithmetic screens and the remainder cumulative income statements whose operand was never WON. ⚠️ **AND VN30 HAS NO UNSPENT GPU EITHER, MEASURED THE SAME DAY**: of the **536** cells still open **414 have already met the full 115-layer cascade and lost**, and `unasked_quarters` — the one GPU test that assumes nothing about the parser — finds **ONE** never-asked cell in the whole universe (ACB 2009-Q3, whose PDF is not on disk). So the 311-document gap plan is ~13 h of GPU for what the PARSER has learned since, and **the parser is the only lever left**: at the DEEPEST layer the refusals rank `fragmented reading` 119 cells · `cash: no closing balance` 52 · `is: operating profit does not close` 42 (**23 give only that reason**) · `bs: assets != liab+equity` 41 · `no such statement on any page` 41 (**23**, and that is the page CLASSIFIER, `SET-2`) · `bs: no total assets` 29 (**15**). ⚠️ **A GAP PLAN COULD NOT SEE ANY OF THAT UNTIL `FPR-1`**: `ASK-1`'s skip needs the parser to be the same FILE and read that off `git_commit`, so a dirty tree was unreadable — **258 of 1,472 folders had a usable fingerprint**, PLX's run of the same day not among them. `parser_digest()` is sha256 of the parser BYTES, written into every run folder from now on, and needs no git (a worker has none). ✅ **THE FLEET IS CODE NOW** (`FLT-2`): `kgpu/fleet.py` + the notebook's §1b partition a universe across 1 local card and every Kaggle session — 2 per account, so two accounts are FOUR lanes — one subprocess per lane because `activate` is process-wide, longest-first, asserted to be a PARTITION, local preferred on a tie because local GPU is free. ⚠️ **AND CONTIGUITY IS THE NUMBER NOBODY HAD**: cells 89.7 % and quarters 75.9 % hide **310 HOLES** — non-solid quarters sitting BETWEEN two solid ones — with only **5 of 30 tickers one unbroken band** (ACB BID CTG TCB VCB; ⚠️ **and the TOTAL is the wrong denominator for a contiguity goal** — `BND-2`, 2026-09-13: closing one of SHB's 39 holes changes no band, closing MWG's ONE makes one, so **ranked by holes remaining seven tickers sit within FOUR of whole** (MWG 1 · SSB 2 · BCM 3 · BVH 3 · FPT 3 · HPG 3 · VPB 4) — **19 holes needing 20 cells, 18 of them a single cell**, which would take one-band **5 of 30 -> 12 of 30**. Four are convicted correctly by the screens, and ⚠️ **BVH Q4-2025 is convicted on its NEIGHBOUR's bad reading** (Q1-2025 reads 255.8 tn, so a correct Q2 makes Q4's 291.9 tn a 1.08x step): **one wrong figure convicts the next**, so read the EARLIEST member of a run of continuity failures, never the loudest. ⚠️ **And several of the 15 have met only a SHALLOW cascade** — MWG Q4-2011 records ONE layer with `absent_deepest: None`, so `ASK-1` had nothing to reuse). SHB 41 holes, VNM 39, TPB 25, PLX 22 on a band of 2. ⚠️ **AND 24 % OF THE GAP WAS A QUESTION NOBODY ASKED** (`ALT-2`): `_alternate_retry` re-reads an absent statement from the other filings of the same period, finds them with `os.path.exists`, and a documents payload ships ONE filing per quarter — so on a worker every alternate was skipped by a `continue` **placed above its own log line**, silently. **168 of the 536 open cells have an alternate on disk and 127 were NEVER retried**, and the split names the machine: every Kaggle-bootstrapped ticker reads 100 % never-tried (BVH 27/27, PLX 24/24, POW 13/13) while the local ones read VNM 18 → 2, SHB 14 → 4, MSN 3 → 0. `unasked_quarters` could not see it — it keys on the QUARTER, and a DIFFERENT FILING of an opened quarter is a new question. Fixed both ends; **this is the one block where GPU still buys cells.** ⚠️ **THE REST OF THE GAP IS NOT GPU-SHAPED**: **104** open income statements are BLOCKED on a de-cumulation operand (82 distinct roots, ~1.3 dependents each — they parse perfectly and the merge can never write them), **58** sit on filings of ≤10 pages where `missing` is correct (TPB Q1-2016 is five pages with no cash flow, VJC Q2-2023 is ONE page — **17 of the 23 `no such statement` cells are cash flows on condensed forms**), and the **122** the screens withheld are convicted correctly (BVH total assets 220.8 tn → 18.3 tn in one quarter). ⚠️ **SO THE HONEST CEILING IS ~5,155 CELLS, NOT 5,214** — 4,678 of 5,155 = **90.7 % of what is winnable** — and **95 % of the grid needs +276 cells**, which this repo's own record prices at a dozen multi-defect parser efforts (six took HPG 170 → 192). ⚠️ **AND THE `months` LABEL CAME FROM THE WRONG FILING, WHICH IS WHY THE ALTERNATE'S INCOME STATEMENT WAS BEING THROWN AWAY** (`MTH-1`, fixed 2026-09-13): `run_document` wrote every statement's span from `task.cumulative`, a property of the CHOSEN document, so a standalone quarterly recovered from an alternate under a reviewed half-year would be labelled `months = 6` and de-cumulated against priors it never contained — and the guard against that hazard REFUSED the statement instead of fixing the term, taking the cell with it. The origin dict carries the alternate's own `_cumulative` now; **both directions of the mismatch are pinned by test, because only one was ever safe by accident.** ✅ **VALIDATED ON FOUR DOCUMENTS, 22.3 min of RTX 3050**: POW Q4-2025 and PLX Q4-2013/Q4-2014 balance sheets **written from an alternate no run had ever opened**, and POW Q2-2019's income statement now reads `[onnx@200] 16 items` where the refusal used to be — held only by `OPB-1`'s missing Q1-2019 operand, which is a different and honest block. VN30 **4,681/5,214 = 89.8 %**, quarters 76.0 %, **holes 311** (a recovered quarter can CREATE a hole by putting an existing gap between two solid ones — that is the number behaving correctly). ⚠️ **AND `SPB-1`'s RULE HAD BEEN TAUGHT TO ONLY ONE OF THE TWO FUNCTIONS THAT MUST AGREE** (`SPB-2`, fixed 2026-09-13): the split GATE refuses a pair whose left box CLOSES with `)`, and the split REPAIR refused only the mirror image, so `(1.234)` and `567` 2.0pt apart merged to `(1.234.567` — **a complete negative figure glued to the next PERIOD COLUMN.** ⚠️ **That is the direction that costs a FIGURE and not a cell**: a gate refusing a good statement is visible in the run folder, which is how `SPB-1` was found, while the repair writes a well-formed negative nothing downstream can tell from a reading. `_joinable` is the shared predicate now; all three measured split shapes still merge, `SPR-1`'s three-box run included. **The reach cannot be measured from the artefacts** — a run folder stores the row dump, not the word boxes. ⚠️ **AND THE ALTERNATES PLAN PROPOSED 101 DOCUMENTS WHERE 59 COULD NEVER BE BANKED** (`FLT-3`, fixed 2026-09-13): a quarter whose only open report is a CUMULATIVE income statement waits on its Q1..Q(q-1) operands, and re-reading a different FILING of it changes nothing — `OPB-1` by a second route, into a NEW plan builder that did not ask the merge's own `_quarter_priors`. **The first alternates fleet walked into it while running**: 21 PLX documents planned, 11 skipped at run time. The honest plan is **42 documents / 50 cells**, and no GPU was wasted because `OPB-1`'s run-time half caught every one — a plan that over-promised, not a run that overspent. ⚠️ **AND A REPLAY NAMED TWO MORE DEFECTS ON ONE CELL** (`JVW-2`, fixed 2026-09-13): of the 24 income statements whose every deepest-layer reason is `operating profit does not close`, **19 have a residual that IS one row the reading produced and 17 of those rows are ONE account** — the associates/JV share, `OP_IDENTITY`'s optional corp term. A trailing NOTE REFERENCE (`V.4(c)`, `VI.6b`) was scored as part of the name while `_prefix_trims` trimmed only LEADING words, and behind it sat `JVW-1`'s wording gap: every spelling says **chia** where the chart says *phần lãi (lỗ) trong*. **The trim alone leaves all 13 keys short and six aliases alone leave 9 short; together all 13 clear 0.849-0.902** — two defects on one cell, HPG's lesson. ⚠️ **`NST-1` made the report scoping load-bearing** (a 0.820 rival on the corp BALANCE SHEET, 0.667 once scoped), the trim's floor is THREE words because two let containment invent a match, and **17 is not a cell count and 15 of them are VNM.** ⚠️ **The replay's first answer was `0 of 24` and was a TYPO** — `absent_rows` stores dicts where `row_dump` stores lists — the confident kind of wrong. ⚠️ **AND THE REACH CLAIM WAS WRONG TOO, BECAUSE A SCORE OVER THE BAR IS NOT A MAPPED COLUMN**: on the DEFAULT path 23 of 24 still do not map, and the reason is a gate nobody had named — **`ACCOUNT_WORDING` is consulted ONLY under `equity_wording`**, a table with a generic name reachable only through a flag with a specific one, so `JVW-1` has always been conditional on it as well. That flag is **layers 66-69 of 115**, which the full cascade reaches, and replayed there **18 of 24 map AND the identity CLOSES** (VNM 17, VHM 1). ⚠️ **18 is a population, not cells**: the rows replayed are `RSN-1`'s layer-1 reading and `sane` and the merge still have to pass — ✅ **and the re-run is done: 14 CELLS BANKED at `onnx@200+equity` and NOT ONE at any other layer** — 8 from a 62.1-min RTX 3050 run and **6 from a Kaggle T4 lane parsing VNM concurrently**, since `source.zip` ships the parser. ⚠️ **Two processes merged one ticker's CSVs at once and it was CHECKED for a lost update, not assumed safe** (`merge_run` is read-modify-write, and a dropped row leaves no duplicate and no count change): all 17 periods read `pdf`, 74 rows each). ⚠️ **AND VERIFYING THAT RE-WRITE EXPOSED A HOLLOW CELL NOBODY HAD COUNTED** (`CCB-1`, 2026-09-13, NOT FIXED): **113 of 1,702 `pdf` cash-flow rows = 6.6 % carry NO closing balance in any closing column** — BVH 41, FPT 13, VIC 13, BSR 12, PLX 10 — because `reconcile` can satisfy its closing-balance requirement on a term that fills no SCHEMA COLUMN. **This is `GCW-1`'s and `replay_cash_close.py`'s predicted hazard measured in the data**, and it qualifies every rate taken off these files (§5 rule 21): the cell is not `missing`, it is **present and hollow**, which is the harder kind to see. ⚠️ **A blank is not a zero** — `opening + net + fx` would compute a plausible figure and computing it is a TRANSCRIPTION (§5 rule 24); the route is `GCW-1`'s `cash_wording` alias). ⚠️ **AND THE SAME AUDIT FOUND A SECOND HOLLOW CLASS WHOSE QUARTER SPLIT NAMES ITS MECHANISM** (`HOL-1`, NOT FIXED): **64 of 1,667 `pdf` income statements carry ≤6 figures against a median of 19** — BVH Q4-2012 holds FOUR, no revenue and no operating profit — and the split is Q1 **0.2 %**, Q3 **0.5 %**, Q2 **5.0 %**, Q4 **9.2 %**, exactly the CUMULATIVE quarters, with **59 of 64 carrying `months=3`** and the never-de-cumulated BALANCE SHEET at **0 of 1,679**. `_decumulate` fills a column only where the year-to-date AND every prior held a value, so **one sparse prior propagates sparsity forward** and Q4 is worst because it subtracts three. ⚠️ **The run log has printed the ratio per document all along** (`14 of 18 columns`) **and nothing summed it.** Together with `CCB-1` that is **177 cells present and hollow that every rate counts as parsed.** ⚠️ **And this measurement first came back CLEAN and was wrong** — `symbol`/`method`/`n_columns`/`document` are always populated, scoring every hollow row four higher than its content. ⚠️ **SO VN30's HONEST NUMBER IS 87.8 % AND NOT 90.8 %, measured 2026-09-13 with the fleet finished**: of the **4,736** `pdf` cells on disk, **100 are `CCB-1`** (a cash flow with no closing balance) and **58 are `HOL-1`** (≤6 figures), leaving **4,578 cells with real content = 87.8 % of the 5,214-cell grid**. **Three points of the headline are cells that parsed and say almost nothing**, and both blocks are NOT FIXED. This is `GRD-1`'s lesson at the CELL level rather than the grid's: that fix made a hole admit it was a hole, and a row can still be present and empty — layer 66, the layer the replay named, not one cell at any other. ⚠️ **The other 10 are `OPB-1` MEETING `HLD-1`, not a failed prediction**: five more Q1 roots had their income statement ACCEPTED and the merge held the whole quarter because the FILING gave 2 of 3, so the dependent Q4 in the same batch was skipped for an operand just won — `Q1-2018 is 'missing' on disk` printed minutes after `Q1-2018 HELD`. A second pass after the release found 7 dependents already solid and 3 still blocked on roots no target list contained. **A de-cumulation chain needs its own closure sweep.** ⚠️ **AND OLDEST-FIRST IS NOT COSMETIC** — Q1-2025 won, then `Q2-2025 = 6-month − Q1` and `Q4-2025 = 12-month − Q1,Q2,Q3` became writable in the same pass.
[ISSUES.md](.claude/current_state/ISSUES.md) — **141 open**, 44 resolved, codes permanent. ⚠️
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
| `src/web_scraper/` — any scraper, the PDF/OCR statement parser, `raw_data/` layout | [.claude/context/web_scraper.md](.claude/context/web_scraper.md) | 70.7k |
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
| `src/kaggle_gpu/` — a repo notebook on a T4: payload, `rehearse`, **§7b PANEL MODE** | [.claude/docs/kaggle_gpu.md](.claude/docs/kaggle_gpu.md) | 10.6k |
| `src/dtos/` — any dataclass. ⚠️ **a mixed-age attic**: `ModelConfigDto`/`ConfigDto` have 0 refs | [.claude/context/dtos.md](.claude/context/dtos.md) | 3.2k |

| doing this JOB | open this |
|---|---|
| **the EVIDENCE behind a number in §2-§6** — six files, by subject, ~2-10k each | [.claude/findings/](.claude/findings/) — the 📂 links in §2/§3/§6 point into it; `standing-rules.md` holds §5's |
| starting a session · running the chain · a selection · refreshing data · OCR a ticker · summarising the OCR backlog · quoting a number · recording a finding · committing | [.claude/workflows/](.claude/workflows/README.md) — **9 guides, ~1.5k each; `/wf-list` names the slash command that runs each. A workflow is the ORDER; the runbook is the COMMANDS** |
| you want the COMMAND | [.claude/runbook/RUNBOOK.md](.claude/runbook/RUNBOOK.md) — 5.2k |
| **running the FILING OCR on a T4** (§1a is what *"OCR ticker `<SYM>`"* means; §8 what is missing) | [.claude/docs/PDF_OCR.md](.claude/docs/PDF_OCR.md) — 17.8k |
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
