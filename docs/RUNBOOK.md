# RUNBOOK — how to run this chain

> Every runtime below was **measured**, not estimated — §8 says why that distinction matters.
> Registers: [CLAUDE.md](../CLAUDE.md) *what is PROVED* · **RUNBOOK.md** *how to RUN* ·
> [ISSUES.md](ISSUES.md) *what is BROKEN* · [TODO.md](TODO.md) *what is NEXT*.
> ⚠️ **When another register disagrees with this file about a COMMAND, this file wins** — it is the
> one whose commands were actually run.

---

## 1. TL;DR

```powershell
.\mt_env\Scripts\Activate.ps1
cd src
python -m pipeline            # what is stale — writes NOTHING
python -m pipeline --apply    # run the stale stages
```

`python -m pipeline` is safe any time: writes nothing, ~5 s, one row per stage saying
`ready` / `would run` and **why**. Start every session with it.

## 2. Once per shell

```powershell
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"   # ⚠️ absolute, and required
Clear-Content logs\app.log                            # before any pipeline/ingest run
cd src                                                # every `python -m` below runs from src\
```

⚠️ **`cd src` matters** — every stage is a package under `src\`, and from the repo root
`python -m pipeline` raises `ModuleNotFoundError`.

---

## 3. The chain, and what each stage costs

The chain's DEFAULT is **`vcb` / `close_adjust_5day` / `d=20, h=5`**, named in exactly one place:
[src/utils/chain.py](../src/utils/chain.py). ⚠️ **The default is not the live experiment:**

| target | table | evidence | verdict |
|---|---|---|---|
| `close_adjust_5day` *(the default)* | `…__final__d20_h5`, 35 ch | `failed_null=1` | ❌ **a price LEVEL — R² −85.6, MASE 21.4, ROC AUC undefined.** Do not start new work here |
| **`return_5day`** | `…__final__d20_h5`, 66 ch | `cleared_p95_not_a_pass` | ⚠️ layer 2 "clears" but see TODO `P0-1` — four measured reasons the bar is too low |

Use `--table return_5day__final__d20_h5` explicitly, or move `DEFAULT_TARGET`. The default was left
alone only because moving it is a behaviour change nobody asked for.

| # | stage | command (from `src\`) | writes | measured |
|---|---|---|---|---|
| **0** | **filter** *(optional)* | `dagster asset materialize -f orchestration/definitions.py --select "filter/universe" --partition PRICE10K` | `filter_schema.universe__price10k` | **~1 s** |
| 1 | data | `dagster asset materialize -f orchestration/definitions.py --select "group:unified" --partition VCB` | `unified_schema_vcb.pool__*` | minutes-hours |
| 2 | selection | `python -m feature_selection.run --pools pool__basic --null-draws 10` | `reports/feature_selection/<run>/` | **~1 min** per country pool |
| 3 | shortlist_pool | `python -m final_features --apply --shape shortlist` | `pool__shortlist__<target>__d20_h5` | seconds |
| 4 | selection_2 | `python -m feature_selection.run --pools pool__shortlist__<target>__d20_h5 --null-draws 10` | another run folder | **29m 44s** at 644 ch |
| 5 | final_features | `python -m final_features --apply` | `<target>__final__d20_h5` | **0.8 s** |
| 6 | train_test_creator | `python -m train_test_creator --save` | `src/train_test_set/<dataset>/` | **0.5 s** |
| 7 | model | `python -m model.lstm` *or* `model/lstm/RUN__lstm.ipynb` | `src/model/runs/<run_id>/` | minutes |
| 8 | result_evaluator | `python -m result_evaluator` | `results/metrics.json` — ⚠️ **`runs/index.csv` only via `--rebuild-index`** | **41.6 s** `--rescore`, **42.7 s** `--rebuild-index` |
| 9 | **backtest** | `python -m backtest --run <run_id> --ticker VCB --top-k 15` | `src/model/runs/<run_id>/results/backtest_<split>.csv` ⚠️ **inside the RUN FOLDER** (gitignored, `RPR-1`) | **1m 14s** with a 200-draw null |

**Four tools that are not stages — each answers a question the chain cannot ask about itself:**

| tool | command | answers | measured |
|---|---|---|---|
| **walk-forward** | `python -m walkforward --ticker all --table <T> --config <C> --first-test 2017-01-01 --out <dir>` then `python -m walkforward.evaluate --top-k 20 --draws 200 --universe all --out <dir>` | *is this one lucky split?* 10 expanding folds, one OOS track. ⚠️ `--horizon` is DERIVED from the track; to re-read a FINISHED track use `--draws 0` (§7b) | **~35 min** |
| **arm sweep** | `python -m walkforward --out <dir> --arm <pkg>:<cfg>.yaml …` then `python -m walkforward.compare --top-k 20 --horizon <h> --draws 200 a=<dir>/a b=<dir>/b …` | *does the ARCHITECTURE matter?* Every arm trains on ONE build of each fold | **2h 49m** for 7 arms × 10 folds; scoring **22m 25s** |
| **pair** (`P2-4`) | `python -m walkforward.pair --top-k 20 --draws 2000 h10=<dirA>:10 h20=<dirB>:20` | *does one HORIZON beat another?* Pairs on the CALENDAR — the only tool that can compare two horizons | **48 s** |
| **settings sweep** | `python -m walkforward … --out <dir>/<tag> [--val-months N] [--step-months N] [--no-scale-target] [--rank-min-width N]` then `compare` | *does the SPLIT or the DATASET setting matter?* Use the cheapest arm — the model must be the constant | ~**20 min** per `gbt` track |
| **hand baseline** | `python -m backtest.handscreen --run <run_id> --top-k 20 --draws 200` | *does the model beat three ranked columns?* | **1m 53s** |
| **head to head** | `python -m backtest.head2head --a <run_id> --b <run_id> --top-k 15 --draws 200` | *does chain A beat chain B?* Priced on the INTERSECTION, paired | **2m 18s** |
| **pool prune** | `python -m feature_selection.prune --ticker ALL --pool pool__ta --universe-from <table> --budget 30 --out <json>` | *which channels can a wide pool even OFFER?* ⚠️ LABEL-FREE by construction | ~1 min |

⚠️ **A wide-vs-narrow walk-forward comparison must be on the INTERSECTION** of the two tracks'
`(date, ticker)` — `walkforward.compare` REFUSES two tracks with different row coverage, correctly,
and a wider pool changes coverage.

### The model packages, and what each one costs to add

Ten architectures are wired to the shared engine. **A new one is `model.py` + a ~30-line binding,
never a copy of `train.py`** (`model/CONTEXT.md` §7) — the four added 2026-08-21 took about half an
hour each.

| package | at h=10, 19 channels |
|---|---|
| `gbt` | **1,398 nodes** — the best arm measured |
| `cnn` · `tcn` · `cnnlstm` | 5,185 · 18,113 (dilated CAUSAL convolutions) · 30,369 (Conv1d then LSTM) |
| `transformer` | 68,417 — needs positional encoding or it is a set function |
| `lstm` · `bilstm` | 208,769 (the reference) · 313,153 (reads `h_n`, NOT `out[:, -1, :]`) |
| `gru` / `mlp` / `baseline_*` | the older arms |

⚠️ **Every arm in one sweep must inherit the reference's optimiser schedule, batch size, patience
and seed** — that is what makes them comparable, and a difference in schedule shows up as a
difference in architecture. It also means a LOSS may be a schedule mismatch (`cnn` wanting 20 epochs
under a patience of 15 is the visible case).
⚠️ **`--arm <pkg>:<cfg>` requires each arm's `run_name` to start with a DIFFERENT segment**, because
`Arm.label` is `run_name.split("__")[0]` and the arms share one output directory.

✅ **`WFO-1` FIXED — A SECOND SWEEP INTO AN OCCUPIED DIRECTORY IS REFUSED.** `run.main` **claims**
the directory before a single fold is built: `manifest.json` records the experiment and a mismatch
raises, naming the offending field, in **under a second** — verified against the real command that
nearly destroyed `PRF-1`. ⚠️ **The five pre-manifest tracks are guarded on their TABLE only**
(`folds.csv` records no knobs, and §5 rule 2 forbids inferring an absent measurement), so a legacy
directory is protected against the horizon collision that actually happened and not against a
knob-only one; re-running any legacy track once closes that gap. ⚠️ **`--force-out` overrides.**

**One experiment, one `--out` is still the practice** — the refusal is a backstop, not a licence:

```powershell
# h=20 — PRF-1, the default
python -m walkforward --ticker all --table rank_20day__final__d20_h20 `
    --config lstm__all__rank_20day__final__d20_h20.yaml --first-test 2017-01-01
python -m walkforward.evaluate --top-k 20 --draws 200 --horizon 20 --universe all

# h=10 — measured 2026-08-20: 33m 26s sweep + 8m 59s scoring
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --config lstm__all__rank_10day__final__d20_h10.yaml --first-test 2017-01-01 `
    --out ../results/walkforward_h10
python -m walkforward.evaluate --top-k 20 --draws 200 --horizon 10 --universe all `
    --out ../results/walkforward_h10
```

✅ **`--horizon` IS DERIVED FROM THE TRACK AND THE FLAG IS OPTIONAL.** It sets the holding interval
the periods are cut at AND the `return_{h}day` column scored, and it used to default to **20** — so
an h=10 track scored without it silently scored the wrong label against the right predictions.
`evaluate` and `compare` read it from the manifest (else `folds.csv`) and **raise** when an explicit
`--horizon` disagrees. ⚠️ `compare` also refuses arms built at DIFFERENT horizons and points at
`walkforward.pair`, the only tool that can compare two.

⚠️ **`compare` and `head2head` PAIR the difference, and that is not a nicety** — every arm trades the
same dates out of the same panel, so their period returns correlate at **ρ 0.74-0.90** and
`se_sharpe` ≈ 0.16-0.25 is the error bar on the wrong quantity. CLAUDE.md §5c is the cautionary case.

⚠️ **Stages 2, 4 and W are MANUAL** — `--apply` stops and prints `MANUAL — cannot be produced here`,
because each is an expensive artefact that must be a deliberate act (a selection run is GPU-hours; a
walk-forward sweep is ~35 GPU minutes and ten run folders). Everything else `--apply` will do.
⚠️ **`pipeline` knows about stages 9 and W**, so it is the gate §8 rule 1 always claimed it was: ten
rows, and on a cross-sectional chain stages 3-4 report `n/a` rather than proposing a pool nothing can
read.

---

## 3a. ⚠️ THE CROSS-SECTIONAL CHAIN — every command as actually run, 2026-08-18

The chain that produced the repo's first out-of-sample model skill.

```powershell
cd src
# 2 — the selection is MANUAL and ran on a Kaggle T4 (6 h 07 m with a 20-draw null); see §7a.

# 5 — the final table.  ⚠️ NO --scope.
python -m final_features --apply                                   # 7.3 s
#    -> unified_schema_all.rank_20day__final__d20_h20   624,448 x 17  (13 channels)

# 6 — tensors.  ⚠️ --ticker all IS NOT OPTIONAL.
python -m train_test_creator --ticker all --table rank_20day__final__d20_h20 --save
#    -> all__rank_20day__final__d20_h20__tr70_val15_test15__std     10.9 s
#       train 422,251 | val 91,462 | test 93,224   x 20 x 13

# 7 — train (config must be written FIRST; see below)
python -m model.lstm --config configs/lstm__all__rank_20day__final__d20_h20.yaml   # 4m 23s

# 8 — score it.  ⚠️ TWO commands: the first rewrites the run FOLDER, the second index.csv.
python -m result_evaluator --rescore         # 41.6 s
python -m result_evaluator --rebuild-index   # 42.7 s

# 9 — does the ranking pay for its own trading?  ⚠️ PANEL RUNS ONLY.
python -m backtest --run lstm__all__rank_20day__final__d20_h20__20260818-195738 `
    --ticker VCB --top-k 15 --draws 200          # 1m 14s
```

⚠️ **`--ticker all` is not optional** — `train_test_creator` defaults to `vcb` and would look in the
wrong schema. ⚠️ **NO `--scope`, and an earlier draft of this runbook was wrong to suggest one**:
`--scope` names EVERY table in the plan, so `--scope liquid150` was measured planning two junk
duplicates of VCB tables that already exist. A scope separates two groups that COLLIDE on a name;
nothing collides here.

✅ **`python -m pipeline` covers this chain** — measured on the h=20 chain: **10 stages, 5.8 s, every
row `up to date`.**

```powershell
python -m pipeline --ticker all --table rank_20day__final__d20_h20 `
    --config lstm__all__rank_20day__final__d20_h20.yaml
```

`shortlist_pool` and `selection_2` report **`n/a — CROSS-SECTIONAL chain … there is no layer 2
(CSP-1)`** and are `ready`, so `--apply` skips them (and `apply_shortlist_pool` **raises** if forced
with `--only`, because `--only` ignores `ready`).

⚠️ **The chain is detected from the SHORTLISTS, never from the table name** — `final_features` drops
the `cs_` prefix when naming a table, so the name genuinely cannot say whether the selection was
cross-sectional; `pipeline.selected_for` reads `outstanding.csv`'s `target` column under the same
`(schema, d, h)` filter `TrainTestCreator` uses. ⚠️ **`walkforward` is `manual` and has NO `apply`.**
⚠️ **`--config` is still not optional** — the `model` row keys on it, not on `--table`, so without
one it reports the DEFAULT chain's run as up to date and the `backtest` row scores that run too.
⚠️ **The model config cannot be written before stage 6 exists**: `n_features` is an ASSERTION
`engine._verify` raises on, and the surviving channel count is only known once the dataset is built.
Write it between 6 and 7, filename **equal to `run_name`**.

## 3b. ⚠️ A PROBE MUST NOT LAND IN THE CHAIN'S REPORT ROOT — `PRB-1`

`plan_from_reports` groups **every run under a root** by `(schema, target, SETUP_KEYS)`. **The data
WINDOW is not a setup key, and neither is which POOLS a run saw**, so a probe merged into
`reports/feature_selection/` is treated as a chain input and fails one of two ways: with the same
setup keys it is ⚠️ **SILENTLY UNIONED**; with a different one it raises and **nothing plans at all**,
including unrelated chains. ⚠️ **`--scope` fixes NEITHER** (it suffixes both groups identically).
**Only `--root` separates them.**

| root | holds |
|---|---|
| `reports/feature_selection/` | runs that FEED the chain |
| `reports/feature_selection_probes/` | runs that MEASURE the selection — `PRF-7` (window), `FNM-1` (representation), the `PRF-9` pilots |
| `reports/feature_selection_wide/` | the `PRF-9` wide run, promoted to a chain input for its downstream test |

**A run that measures the SELECTION is not a run that feeds the CHAIN.** ⚠️ To build a table from a
probe you need `--root` AND `--scope` — the root to stop it grouping, the scope to stop it claiming a
name the chain already holds (`--replace` would destroy that table):

```powershell
python -m final_features --apply --root ../reports/feature_selection_wide --scope wide
#    -> unified_schema_all.rank_20day__final__d20_h20__wide   (not …__d20_h20)
```

## 3c. ⚠️ THE CHAIN AT h=10, as run 2026-08-19 (`PRF-2`)

```powershell
cd src
# 2 — selection on a Kaggle T4, 20 draws.  6 h 04 m.  See §7a.
python -m final_features --apply                                            # 5.9 s
python -m train_test_creator --ticker all --table rank_10day__final__d20_h10 --save
python -m model.lstm --config configs/lstm__all__rank_10day__final__d20_h10.yaml  # 4m 31s
python -m result_evaluator --rebuild-index                                  # 8m 51s
python -m backtest --run <run_id> --ticker VCB --top-k 20 --draws 200
python -m backtest.handscreen --run <run_id> --top-k 20 --draws 200         # 1m 53s
```

⚠️ **Run `handscreen` beside the backtest, not instead of it** — §5 rule 4's shape: a model that does
not beat three ranked columns has not earned its complexity. At h=10 the model returns **+2.442**
against the hand rule's **−0.263** (paired `t` = +5.94).

## 3d. ⚠️ SCREENING A UNIVERSE — the filter layer, as run 2026-08-22

Stage 0 is optional and exists only for a universe that is **not** `ALL`, `BANK`, `VN30` or one
company. A **screen** is a named list of conditions in
[filters.py](../src/orchestration/preprocessor/filters.py); materialising it writes the membership
table, and the matching `unified` partition builds against it.

```powershell
# 0. the membership table — ~1 s, writes filter_schema.universe__price10k
dagster asset materialize -f src/orchestration/definitions.py `
  --select "filter/universe" --partition PRICE10K

# 1. the schema it gates. pool__basic FIRST — every other pool joins to it as the spine.
dagster asset materialize -f src/orchestration/definitions.py `
  --select "unified/pool__basic"   --partition PRICE10K     # 7m 36s at 480 tickers
dagster asset materialize -f src/orchestration/definitions.py `
  --select "unified/pool__targets" --partition PRICE10K     # 40 s
```

| partition | what it means | tickers | `pool__basic` |
|---|---|---|---|
| `PRICE10K` | `close_raw` never below **10,000 VND** on any session since 2026-01-01 | **480 / 781** | 1,503,958 × 101 |
| `LIQUID` | ≥1 bn VND/session median matched turnover, ≥80 % traded days, ≥200 sessions, still quoted | **206 / 781** | 657,892 × 101 |
| `QUALITY` | `LIQUID` + a 5,000 VND median price floor + a debt/equity ceiling | **200 / 781** | 635,919 × 101 |

⚠️ **`--select "group:unified"` builds ALL TWELVE pools** — hours on a 480-ticker screen. Name the
two you need.

```sql
SELECT ticker, val__turnover_median_1bn, first_failed
FROM filter_schema.universe__quality WHERE NOT passes ORDER BY 2 DESC NULLS LAST;
```

⚠️ **EVERY CANDIDATE IS IN THE TABLE, not only the survivors** — 781 rows with each condition's
measured value, its verdict, `passes` and `first_failed`, which is what makes *"why is this ticker
out"* answerable without re-running anything. ⚠️ **A PASS RATE IS NOT A COVERAGE RATE**:
`COUNT(val__<cond>)` is how many names the condition could be MEASURED on, and a condition everything
cleared looks identical to one nothing was measured for. `debt_to_equity_max_12` is the live case —
`gold.stocks_financials_bank_fa` holds **2 tickers of 781**, so it abstains on 779 and the asset logs
a WARNING naming its 0.3 % coverage (rule 22 at the filter).

**Two things that will bite.** (1) **Re-running a screen does NOT rebuild the unified schema** — the
edge is deliberately not declared, so a changed threshold leaves the old universe on disk looking
current; rebuild `pool__basic` and `pool__targets` yourself. (2) **A screen is NOT point-in-time**:
membership is decided from a window and applied to the whole history, so **a `z` against a
within-date shuffle is protected and a CAGR read off one of these universes is not**. Every window is
in the table `COMMENT`.

**Adding a condition**: one `register(Condition(...))`, one entry in `SCREENS`, and the new key under
**both** `filter` and `unified` in `src/orchestration/config.json`. Then
`python -m pytest src/orchestration/preprocessor/test_filters.py -q` (30 tests, no database) and
`dagster definitions validate -f src/orchestration/definitions.py`.

## 3e. ⚠️ SCRAPING THE FILING PDFs — one Dagster run, scoped BY YEAR (2026-08-23)

⚠️ **EVERY SCRAPE RUNS THROUGH DAGSTER. There is no supported script path**, and that is a rule about
the RUN, not the code: a scrape outside Dagster leaves no materialisation, no metadata and no
partition status, so nothing downstream can say what was fetched or with what scope.

`raw/cafef_pdfs` is partitioned over **all 784 listed codes** and carries
`BackfillPolicy.single_run()`, so a partition RANGE is **one run, one process** and the scraper
batches tickers through its own 12-way thread manager. One partition per run would pay ~19 s of
start-up each (~4 h over the universe) and scrape one ticker at a time.

```powershell
# one ticker
dagster asset materialize -f src/orchestration/definitions.py `
  --select "raw/cafef_pdfs" --partition HNX_AMV --config phase1.yaml

# PHASE 1 — every listed code, filings up to and including 2020 (~286 GiB)
dagster asset materialize -f src/orchestration/definitions.py `
  --select "raw/cafef_pdfs" --partition-range "HNX_ADC...UPCOM_XMC" --config phase1.yaml
# PHASE 2 — only after the OCR program has run on phase 1: year_min: 2021 (~269 GiB)
```

⚠️ **THE COST IS THE YEAR WINDOW, NOT THE PARTITION COUNT.** Counted from CafeF without downloading
anything — 784 codes, **84,076 documents ≈ 555 GiB**: everything does not fit; **`year_max: 2020` is
50,382 docs ≈ 286 GiB** (phase 1) and `year_min: 2021` is 33,694 ≈ 269 GiB.

⚠️ **An undated document lands in the `year_max` phase, never the `year_min` one** — CafeF files 10
of the 84,076 with a `Year` that is not a year — so the two phases partition the corpus exactly.
⚠️ **`skip_existing` defaults to `true` and that is what makes the run resumable**; a year-scoped run
MERGES its rows into the existing index rather than replacing it, so phase 2 does not erase phase 1.
⚠️ **`--partition-range` REQUIRES the single-run backfill policy** and fails loudly without it.

### ⚠️ 3e-ter. `--config-json '{...}'` IS BROKEN IN POWERSHELL 5.1 — measured 2026-08-24

**PowerShell 5.1 STRIPS THE DOUBLE QUOTES** when it hands a single-quoted string to a native
executable, so Dagster receives `{ops:{raw__cafef_pdfs:{config:{year_max:2020}}}}` and dies with
`JSONDecodeError: Expecting property name enclosed in double quotes`. Reproduced without Dagster, so
it is the shell and not the CLI:

```powershell
& .\mt_env\Scripts\python.exe -c "import sys; print(sys.argv[1])" '{"ops":{"x":1}}'
# ARG: {ops:{x:1}}          <-- quotes gone
```

| | |
|---|---|
| ✅ **`--config <file.yaml>`** | a run-config FILE. Readable, diffable, and the only form that survives copy-paste into any shell |
| ✅ `'{\"ops\":…}'` | backslash-escaped inline — correct but unreadable, and one missed backslash is a silent shape change |
| ❌ `--%` | **measured: still `{ops:{x:1}}`** — the stop-parsing token passes the argument verbatim to CreateProcess, but the quotes are already gone |

```yaml
# repair.yaml
ops:
  raw__cafef_financials:
    config:
      skip_existing: false
      periods: ["Q4-2006", "Q1-2009"]
```
```powershell
dagster asset materialize -f src/orchestration/definitions.py `
  --select "raw/cafef_financials" --partition "HOSE_VCB" --config repair.yaml
```

⚠️ **This does not mean the phase-1 scrape did not run** (784 partitions, 0 errors — it was issued
some other way). What it means is that **the command as WRITTEN here does not work in this repo's
primary shell**, which is worth more than knowing which shell was used on the day.

### ⚠️ 3e-quater. THE FINANCIALS CARRY-UP — parsing is not ingesting

A rebuilt statement CSV changes nothing a model reads until it is carried up (§5 rule 11). Five
assets, in this order, ~4 minutes for two tickers:

```powershell
foreach ($a in @("bronze/cafef_financials",
                 "silver/cafef_financials_bank",
                 "silver/stocks_basic_financials_bank",
                 "silver/stocks_basic_financials_bank_fa",
                 "gold/stocks_financials_bank_fa")) {
  dagster asset materialize -f src/orchestration/definitions.py --select $a
}
```

⚠️ **`BRZ-1`: DROP THE BRONZE TABLES FIRST IF ANY PERIOD WAS REMOVED UPSTREAM.** The ingest UPSERTS,
so it can add and update but never delete — after `period_min` cut 8 VCB quarters, bronze still held
**152 periods against the CSVs' 143**, and nothing reported it: **the row count goes UP and
`MAX(date)` stays correct.**

⚠️ **A NEW COLUMN MUST BE ADDED IN TWO PLACES** — `FinancialsBuilder.DATA_COLS` writes it and
`DataPreprocessor.CAFEF_FINANCIAL_META_COLS` reads it; a column in the first and not the second falls
through to the line items and is coerced to numeric (`ValueError: Unable to parse string "False"`).
Nothing enforces the match.

✅ **Verify with the queries that can see the whole point**, never off the green run:

```sql
SELECT ticker, source, COUNT(*) FROM bronze_schema.cafef_financial_reports
GROUP BY 1, 2 ORDER BY 1, 3 DESC;      -- expect only 'pdf' and 'missing'

SELECT COUNT(*) FROM bronze_schema.cafef_financial_reports
WHERE source <> 'pdf'
  AND (COALESCE(method,'') <> '' OR consolidated IS NOT NULL
       OR COALESCE(document,'') <> '' OR unit IS NOT NULL OR n_columns IS NOT NULL);
-- expect 0: a row that produced NO statement must carry NO parse provenance
```

⚠️ **`publish_date`, `assurance` and the share counts are NOT provenance** — they are facts about the
DOCUMENT, kept whether or not any statement reconciled. Do not blank them. **If that second count is
ever non-zero, REPLAY the rule instead of re-parsing**: blanking those six fields wherever
`source <> 'pdf'` is the same predicate `_write` applies, so it is reproducible and idempotent, where
a 5.5 h re-parse to change 2 of 429 cells would recompute the other 427 with the code that already
produced them. Dry-run, apply, re-run to confirm 0, then re-ingest.

#### ⚠️ 3e-quinquies. THE `periods` REPAIR PROCEDURE — four steps, and step 4 is the one people skip

Measured four times. A subset run is the only affordable way to repair a quarter, and it has two
properties that make an unguarded one dangerous:

1. ⚠️ **PUT THE PRECEDING QUARTERS IN `periods` TOO.** `sane` judges magnitude against the quarters
   accepted **in this run**, so a subset run holding only the target gives it no band and it fails
   open — and the cascade may then stop at an EARLIER layer and write something no probe ever judged
   (`PGB-1`: `onnx@200`'s garbage passed with no history and was refused with it). Three or four
   neighbouring quarters are enough; they need not be the expensive annuals.
2. ⚠️ **BACK THE NINE CSVs UP FIRST** — an authoritative run writes NON-MERGING progress snapshots,
   so an interrupted one truncates the file (`SAN-1`: ACB's three CSVs left at 9 rows against 74).
3. **Diff against that backup afterwards** — `git diff` is not enough, because the run may have
   merged into rows that were already committed.
4. ⚠️ **RESTORE EVERY NON-TARGET PERIOD THAT MOVED. A `periods` RUN MAY ONLY WRITE THE QUARTER IT IS
   REPAIRING.** The history-providing quarters are re-parsed with a THINNER history than they
   originally had and can be silently downgraded. Measured forms: a whole OCR layer lost (taking two
   line items with it), figures changed, and — ⚠️ **the form to design against** — **`publish_date`
   blanked on four statements and nothing else**, in quarters whose numbers were untouched. **A diff
   that compares only the FIGURES would have called that run clean.** Compare every column.

⚠️ **AND A `periods` RUN CANNOT DELETE** — `build` sets `merge = bool(periods)`, so a quarter it
attempts and FAILS keeps whatever the file already held. Use it to ATTEMPT a repair, never to prove
one is impossible.

### ⚠️ 3e-bis. AND THE PDF IS THE ONLY SOURCE A FINANCIAL STATEMENT MAY COME FROM

**Standing rule — CLAUDE.md §5 rule 24.** Every balance-sheet, income-statement and cash-flow value
must be OCR-parsed out of the company's own filed PDF. **An HTML tab, a JSON endpoint, a web table or
any other transcription is FORBIDDEN** — not as a fallback, not "for the quarters OCR cannot read",
not to close a gap. **A quarter no readable PDF can produce is `missing`, and `missing` is correct.**

⚠️ **THE BUILDER STILL DEFAULTS THE OTHER WAY, SO CHECK BEFORE YOU QUOTE** — `use_api: bool = True`
(`cafef_financials.py:485`, `:1629`) fills any period the PDF pass missed from CafeF's web tabs. The
audit is the `source` query above and it is the only way to tell. ⚠️ **Any `source` other than `pdf`
or `missing` is a defect, not a data point** — and `-1` is CafeF's "not reported" sentinel, so a
transcribed row can carry −1 dong in a column of billions and take part in no subtotal, which means
no reconciliation catches it.

---

## 4. The two flags that decide whether you destroy something

| flag | on | means |
|---|---|---|
| `--apply` | `final_features`, `pipeline` | actually write. Without it: plan only |
| `--replace` | `final_features` | **DROP the existing table** and rebuild. Without it, a table whose fingerprint does not match is a hard **error**, not a silent overwrite |
| `--replace` | `train_test_creator` | overwrite the dataset folder. ⚠️ **Any model run referencing its `dataset_hash` stops verifying** |

⚠️ **`final_features --apply` alone will refuse** if the table exists and its fingerprint moved.
That refusal is the feature: rebuilding drops the table and orphans every dataset below it. Read the
fingerprints it prints before adding `--replace`.

---

## 5. A full run, end to end — copy this

⚠️ The block below is the `close_adjust_5day` chain as run 2026-08-16, kept because every command in
it was executed. **For a new experiment substitute `return_5day`** and read §6 first — the shortlist
pool is target-conditioned and cannot be reused.

```powershell
cd src
# 5 — rebuild the final table from the layer-2 run (drops the old 885-channel one)
python -m final_features --apply --replace
#    -> unified_schema_vcb.close_adjust_5day__final__d20_h5   4,266 x 39  (35 channels)
# 6 — tensors, scalers, metadata
python -m train_test_creator --save
#    -> vcb__close_adjust_5day__final__d20_h5__tr70_val15_test15__std
#       train 2,939 | val 615 | test 640   x 20 x 35
python -m model.lstm --dry-run     # 7 — validate the config without training anything
python -m model.lstm               # 7 — train (or open model/lstm/RUN__lstm.ipynb)
python -m result_evaluator         # 8 — score against a block-shuffled null
```

Then `python -m pipeline` again: every row should read `up to date`.

## 6. Changing the target — the one place with a trap

```powershell
# (a) one-off: leave the chain's default alone, name the table explicitly
python -m train_test_creator --table return_5day__final__d20_h5 --save
python -m model.lstm --config configs/lstm__vcb__return_5day__final__d20_h5.yaml
# (b) move the whole chain: edit DEFAULT_TARGET in src/utils/chain.py, once
```

⚠️ **YOU CANNOT REUSE `pool__shortlist__close_adjust_5day__d20_h5` FOR ANOTHER TARGET.** That pool is
**target-conditioned** — its 644 channels were kept *using* the `close_adjust_5day` label at
`d=20, h=5` — so selecting over it for `return_5day` is leakage. A new target restarts at stage 2:

```powershell
# stage 2, once per pool — the layer-1 sweep
python -m feature_selection.run --ticker VCB --pools pool__basic --target return_5day `
    --lookback 20 --horizon 5 --null-draws 10 --device cuda
# ... repeat per macro pool, then:
python -m final_features --apply --shape shortlist --scope basic
python -m feature_selection.run --pools pool__shortlist__return_5day__d20_h5 --null-draws 10
python -m final_features --apply
```

⚠️ **Pass `--scope` when two experiments would land on one table name.** `final_features` groups on
`(schema, target, setup)` — **no term for which pools** — so a `pool__basic`-only run and a
`basic + economy_x` run are ONE group and get unioned. `--scope basic` names it `…__d20_h5__basic`.

## 7. The LSTM notebook

`model/lstm/RUN__lstm.ipynb` — open, **Run All**. It trains the same `train()` the CLI does, so the
notebook and `python -m model.lstm` are the same run. Retarget it by editing **the parameter cell
(cell 4)** and nothing else — `TICKER`, `TARGET`, `D, H`, `MODEL`; the config path, dataset folder
and run name all derive from those four names, and the cell **asserts the config file exists**.

⚠️ **Rewrite that cell in place; never add an override cell below it.** The same mistake in the
feature-selection notebook fed a run its own label as a feature and reported an IC near 1 without
raising.

---

## 7a. Running on a Kaggle T4 — when the local card is too small

```powershell
cd src\kaggle_gpu
python -m kgpu plan     feature-selection   # what would run; touches nothing
python -m kgpu data     feature-selection   # DB -> parquet -> private dataset
python -m kgpu rehearse feature-selection   # the WORKER side, locally, NO QUOTA
python -m kgpu run      feature-selection   # push, wait, download, merge
```

**Iterate in `rehearse`, not in `run`.** Measured on the `smoke` job: 8m 15s end to end, of which
**5.2 min was QUEUED** — ⚠️ **the queue is the floor, not the compute**, so batch one large run
rather than several small ones. This machine has **4.0 GiB VRAM / ~7 GB RAM free**; a T4 has **14.6
GiB / ~29 GB**, 30 GPU-h/week.

⚠️ **THE WIDTH CEILING WAS RE-MEASURED 2026-08-21 AND IT MOVED TWICE.** Three memory fixes shipped
(all verified exact at `0.000e+00`): `gpu.tree_shap` blocks its `pred_contribs` call over ROWS,
`UnifiedSchemaReader.read` filters tickers in SQL, and the design is no longer materialised three
times over. **VRAM is no longer the wall** — it held at 6.1 of 14.9 GB across four attempts; what
binds now is HOST RAM inside the ranker ensemble:

| width | outcome |
|---|---|
| 120 channels | ✅ completed (`PRF-9`, before any of the fixes) |
| **162 channels** | ✅ **completed 2026-08-21, 44m 12s** — the current known-good |
| 233 channels | ❌ four attempts, `DeadKernelError` in the ranker phase every time |

⚠️ **Read the phase profile, not the outcome.** `selector._tick` prints `rss=` and `peak=` per phase;
`peak` decides whether a run survives, and on attempt 3 `rss` ROSE while `peak` held — a fix that
looked like a regression on the wrong column. *(The earlier ceiling, kept because the reasoning is
the evidence: at 140 channels the host peaked at 24.5 GB and survived while XGBoost died in
`XGBoosterPredictFromDMatrix` — the SHAP allocation `(n_rows, channels × 6 + 1)`, linear in the
width, invisible to `_tick` because XGBoost allocates through its own CUDA allocator — `VRM-1`.)*

⚠️ **The status poller survives a network blip now, and it did not before.** A six-hour run lost its
watcher at **355 min** to one `ConnectionError` while the kernel was still RUNNING, so nothing pulled
the results. `_fetch_status` retries a NETWORK failure for 30 min but never retries an ANSWER (a
`ValueError` means the kernel is missing; retrying repeats a wrong request). If it does give up, the
kernel is probably still alive: `kgpu status <job>` → `kgpu wait <job>` → `kgpu pull <job>`.

⚠️ **ADDING A TOP-LEVEL IMPORT TO A SHIPPED MODULE CAN KILL THE WORKER, AND `rehearse` IS THE ONLY
THING THAT CATCHES IT.** The payload ships `src/feature_selection` and `src/utils` while
`kgpu_bootstrap` **stubs** `dtos` for the single DTO it needs, so a new top-level
`from dtos…import Condition` imports perfectly here and dies on the worker at cell 0 — **1m 29s into
a 233-channel run that had nothing else wrong**, quota already spent. **The rule: an import a worker
cannot satisfy belongs INSIDE the branch that needs it** (a ticker filter needs a database; a worker
has none). And **`kgpu export` + `kgpu rehearse` before `kgpu run`, every time a shipped module
changed** — about four minutes against a wasted kernel launch. This is `KGP-1` from the other
direction: that was a stub SHADOWING a shipped package, this is a shipped package reaching for
something never shipped.

⚠️ **A Kaggle run is a different PROCEDURE, not the same run on a faster card** — its image ships
**xgboost 3.2.0 / sklearn 1.6.1 / numpy 2.0.2** against `mt_env`'s **2.1.1 / 1.7.2 / 2.2.6**, and
XGBoost subsamples from a different RNG stream per device. `contract.SETUP_KEYS` carries
`env_fingerprint`, so the two **cannot be unioned by accident** — expect a group collision instead,
and pass `--scope`.

### A CROSS-SECTIONAL target — the `cross-sectional` job (panel mode)

```powershell
python -m kgpu export   cross-sectional   # DB -> ONE panel.parquet   measured 2m 04s / 477 MB
python -m kgpu rehearse cross-sectional   # both mount layouts        measured 16.0 s
python -m kgpu data     cross-sectional   # the same export, then UPLOAD it
python -m kgpu run      cross-sectional
```

⚠️ **The join happens HERE, not on the worker, and that is structural** — `read_universe_panel` is
one hand-written SQL statement reaching for `reader.driver`, so `ParquetSchemaReader` answers *"there
is no database on a Kaggle worker"* whatever parameters it is given (`CSP-1` in its second form). The
worker gets a finished panel with `cs_rank` already derived. ⚠️ **`data.panel.liquidity_before` is
REQUIRED and has no default** — ranking turnover over the whole sample picks the names that *turned
out* to be liquid, so the exporter raises rather than defaulting silently; and the shipped `cs_rank`
is a rank **within the shipped names**. ⚠️ **`rehearse` does NOT run the notebook's own cells** — it
drives cell 0 and re-creates the panel path itself; to exercise the notebook, point `PANEL_PAYLOAD`
at a staged payload and cut it down first, treating the result as a smoke test and never a number.

### OCR-ing a FILING — the `pdf-ocr` job (documents mode)

⚠️ **It writes a run folder and never a statement CSV** — merging a recovered quarter back stays a
deliberate act with a pre-run backup (§3e-quater).

```powershell
# locally first — this is the baseline the Kaggle run is scored against
cd src
python -m web_scraper.pdf_ocr_job --symbol VCB --periods Q1-2026   # measured 1m 41s

cd src\kaggle_gpu
python -m kgpu export   pdf-ocr    # filings + schema + statements + models   (5.3 s, 92.4 MB)
python -m kgpu rehearse pdf-ocr    # both mount layouts, every input the parse reads  (9.7 s)
python -m kgpu data     pdf-ocr    # the same export, then UPLOAD it          (1m 50s)
$env:PYTHONUTF8 = "1"              # ⚠️ REQUIRED — see below
python -m kgpu run      pdf-ocr    # push, wait, download, merge              (2m 36s)
```

⚠️ **`PYTHONUTF8=1` IS NOT OPTIONAL ON THIS JOB.** `kernels_output` writes the run log with the
process's default encoding — cp1252 on Windows — and this parse logs Vietnamese account labels, so
the log can never be ASCII. Without it the run COMPLETES on Kaggle and the DOWNLOAD raises
`UnicodeEncodeError`; the run is not lost, re-pull it with the variable set.
⚠️ **The job needs `enable_internet: true`, and it is the PACKAGES, not the data** — Kaggle's image
has no vietocr, pymupdf, pyclipper or onnxruntime. The models travel in the payload precisely so that
no weight is fetched at run time.
⚠️ **READ `metadata.json`'s `environment.ocr` BEFORE QUOTING A RUNTIME** — the two halves of the OCR
run on different devices and fail independently (detection is onnxruntime, recognition is torch). The
first Kaggle run had VietOCR on the T4 and DETECTION ON THE CPU: correct output, 21 % slower, one
warning inside a wall of noise (`ORT-1`).

**Measured on VCB Q1-2026** — all three statements already `pdf` at `onnx@200` on disk, so the run
has an exact baseline: **98 of 98 cells identical every time**, same layer, unit and `publish_date`.
**And on the HARD document (BID Q4-2016, cash flow at layer 45 of 47): 32.9 min local against 26.4 on
a T4**, both REPRODUCED on the same layer.

⚠️ **DO NOT QUOTE A SPEEDUP FROM ANY OF THIS.** Four runs of the identical easy document on this
machine came in at 100.6, 113.3, **50.8 and 50.3 s** — a **2.25× swing** in two clusters five hours
apart, with the T4's 69.0 s between them and nothing recorded to tell the clusters apart. **To
compare two machines, INTERLEAVE the runs.** What a T4 buys is a second machine running in parallel
with this laptop, free — not a multiplier.

**Reading the log.** Three percentages, three denominators, and the line says which: `of DOCUMENTS,
not of time` (4.2 min against 18.2 for a failing filing), `of POSITIONS` in the cascade (one layer
re-OCRs every page, the next re-maps a cache), and `of PAGES` — the only one that predicts time, and
an UPPER bound, because `scan` stops at the notes boundary. **`MODE`** takes
`"auto" | "local" | "kgpu"`; `auto` resolves from `$CAFEF_DATA_ROOT` and prints what it chose, while
an explicit `"kgpu"` with no payload mounted **raises** rather than parsing the repo's own
`raw_data/` and reporting a Kaggle run that never touched the payload.

---

## 7b. Re-reading a FINISHED track without re-running it

⚠️ **The pooled row is printed, never stored.** `evaluate` writes `per_fold.csv` but the POOLED line
— the Sharpe, CAGR and `se_sharpe` every register quotes — only ever went to stdout. So *"check the
number against the artefact"* needs a re-run, and the re-run is cheap **only if you drop the null**:

```powershell
# the pooled row alone, from the predictions already on disk. NO GPU, NO training. ~2 min each.
python -m walkforward.evaluate --top-k 20 --draws 0 --universe all --out ../results/walkforward
python -m walkforward.evaluate --top-k 20 --draws 0 --universe all --out ../results/walkforward_h10
```

⚠️ **`--draws 200` is what costs the ~9 minutes**, not the scoring — drop it when re-reading a level,
keep it when claiming the level clears a bar. ⚠️ **`--horizon` is derived from the track.**
⚠️ **It REWRITES `per_fold.csv`** with identical content; harmless, but the file's mtime is then not
evidence of when the sweep ran (`folds.csv` and `manifest.json` are).

⚠️ **AND IT IS NOT HARMLESS AT A DIFFERENT `--top-k`.** The rewrite keys on the basename, not on `k`,
so `--top-k 5` **overwrites the published k=20 fold table** and nothing says so. To score a finished
track at another `k`, copy `predictions_oos.csv` + `folds.csv` elsewhere and point `--out` at the
copy — `manifest.horizon_for` recovers the horizon from `folds.csv`, so the copy scores identically.
✅ Verified: scoring the copy at k=20 reproduces CAGR@30 **0.7398** / Sharpe **2.5310** / 236 periods,
and the published `per_fold.csv` md5 was unchanged. ✅ Both tracks reproduce CLAUDE.md §6-0 to every
digit this way.

### 7c. ⚠️ SHARPE AND CAGR RANK THE ARMS DIFFERENTLY — say which one you are quoting

The h=10 pooled table at 30 bps over 236 periods:

| arm | `sharpe@30` | `cagr@30` |
|---|---|---|
| **`gbt`** | **+2.891** ← best | +69.8 % |
| `tcn` / `transformer` | +2.622 | +73.4 % / +72.9 % |
| **`lstm`** | +2.531 | **+74.0 %** ← best |

**The highest-Sharpe arm is not the highest-CAGR arm**, and the gap is not rounding: `gbt` earns
**4.2 pp/yr less** while scoring **0.36 more** Sharpe — a *lower mean return at lower volatility*,
the same fact `P1-9` separated into two columns. ⚠️ **So "the best model" is not a well-formed
question without an estimand**: *best risk-adjusted* is `gbt`, *best return* is `lstm`, and the
paired test says neither advantage survives a correction for the six arms tried.

---

## 8. Before you quote any number this chain produces

1. **`python -m pipeline` must show `up to date` for every stage below the one you are quoting.** A
   green model run on a stale table is a number about a table that no longer exists.
2. **Read `dataset.meta["evidence"]`**, printed by the notebook and by `train_test_creator`. It is
   generated from the source table's own `COMMENT`, so it cannot go stale. Today it reads
   `failed_null=1` — ⚠️ that run was measured against a shuffled-label null AND DID NOT CLEAR IT.
3. **`close_adjust_5day` is a price LEVEL.** `hit_rate` and `dir_accuracy` are 1.0 by construction
   and R² goes deeply negative. **`ic` against its null is the only readable metric on this target.**
4. **A cleared bar is not a result.** The model-stage null shuffles the outcome against a fixed score
   vector and prices in neither the feature selection nor the architecture search (`NUL-1`). *A run
   that fails it is dead; a run that clears it is not yet alive.*
5. **On a PANEL, quote the daily-IC t-stat, never `ic_clears`** — the evaluator's panel null is not
   label-neutral (`NUL-3`).
6. ✅ **`ICT-1` FIXED — that t-stat is now honest.** `metrics.csv`'s `ic_t` divided by `n_dates`
   rather than `n_eff = n_dates/h`, overstating it by **exactly `√h`** (15.50 reported against +3.47
   at h=20). ⚠️ **A run folder scored before 2026-08-18 carries the old number until re-scored, and
   re-scoring takes TWO commands** — `--rescore` (rewrites each run FOLDER, 41.6 s) **and**
   `--rebuild-index` (rewrites `index.csv`, 42.7 s); neither needs a GPU, and `--rescore` alone left
   the folder reading +3.47 while the leaderboard still read 15.50. Only PANEL runs move.
7. ⚠️ **`mase` DOES NOT EXIST ON A PANEL — do not read a blank as a pass.** Block B is computed in
   `metrics.evaluate` only, so `test_mase` is **NaN** for every cross-sectional run. Rule 4 and
   `P2-3` both say `mase ≥ 1` is the line to quote; on a panel that line is **not measured yet**.
8. ✅ **`P1-9` FIXED — `walkforward.compare` REPORTS BOTH ESTIMANDS, AND THEY ARE NOT SUMMARIES OF
   EACH OTHER.** The column formerly `t_paired` is **`t_ret`** (the mean period-RETURN gap) and
   **`d_sharpe` carries its own CI and `p_sharpe`** from a paired circular block bootstrap. They
   disagreed about **three of six arms** at h=10: `bilstm` and `cnnlstm` lose on return (−2.09 /
   −2.15) and TIE on Sharpe (p = 0.61 / 0.30), while `gbt` gains +0.36 Sharpe at p = 0.044 against
   `t_ret` = −1.02. ⚠️ **Read `p_sharpe` against the number of ARMS, not against 0.05** — six
   challengers is Bonferroni **0.0083**, which `cnn` (0.001) clears and `gbt` (0.044) does not.
   ⚠️ **`ac1` is printed per row** — the lag-1 autocorrelation of the difference series, which is what
   `--block` has to cover; it ran −0.09…+0.06, so the default `block=2` was doing no hidden work.
   ⚠️ **`gbt`'s h=10 advantage does NOT reproduce at h=20** (`d_sharpe` +0.360 against **−0.016**):
   two estimates disagreeing in sign across a neighbouring horizon are what a null effect looks like.
9. **A rank target's `long_short` is NOT money** — the metric is "in return units", which holds when
   the label is a return; on `cs_rank_*` it is a spread of RANKS. `+0.0635` is not 6.35 %.
10. ⚠️ **AN ARM GAP BELOW `|d_sharpe| ≈ 0.09` IS A RESEED, NOT A RESULT.** Every config is `seed: 42`,
    so every arm row is one fit per fold; five `gbt` seeds over identical h=10 folds gave pooled
    Sharpe@30 **2.845 … 2.979, sd 0.054**, max paired `d_sharpe` **0.088**. That leaves `cnn` (4.5×)
    and `gbt` (4.1×) above the floor and puts `transformer`, `tcn` and `bilstm` at or inside it.
    ⚠️ **One architecture, and the cheapest one** — `gbt` resamples rows and columns while a net also
    varies its initialisation, so the `lstm` floor is unmeasured and could be larger.
11. ⚠️ **NEVER COMPARE TWO ARMS IN ONE FOLD** — a per-fold Sharpe is **4.4× more seed-sensitive** than
    the pooled one (mean per-fold range **0.593** against **0.134**, worst fold 1.079). ✅ The DECAY
    across folds survives (slope −0.308 ± 0.027 over five seeds), so it is not a seed artefact.
12. ⚠️ **WHEN TWO ARMS DIFFER, CHECK THE IC BEFORE BELIEVING THE SHARPE.** Pooled IC moves **0.5 %**
    across seeds against Sharpe's **1.9 %** — `long_only_top_k` is a THRESHOLD (top 20 of ~150), so a
    hair's change in the ranking swaps which names are held and the portfolio inherits a
    discretisation the IC never sees.
13. ⚠️ **A FIVE-ARM SWEEP IS NOT FIVE TRACKS** — `--arm` builds each fold's tensors ONCE, so five
    `gbt` arms cost **13m 16s**, not the ~1 h 40 m that `5 × 20 min` predicts. **Cost an arm sweep
    from the fold BUILD, not from the fit.**
14. ⚠️ **`--top-k` IS THE CAP ON HOW MANY STOCKS MAY BE BOUGHT, AND LOWERING IT BUYS LESS TRADABLE
    NAMES.** CAGR@30 rises monotonically as `k` falls — +74.0 % (k=20) → +181.6 % (k=5) → +217.9 %
    (k=3) — but so does the concentration into names nobody can buy: median matched turnover of a
    picked row goes 0.30 bn → **0.03 bn**, and the share under 0.1 bn/day 38.6 % → **61.4 %**.
    **Quote `k` beside any CAGR from this chain.** ⚠️ **And at k=5 the width guard stops protecting
    the track** — `long_only_top_k` skips a date on `len(day) < k`, which excluded the three frozen
    2026 books at k=20 and does not at k=5 (7 ≥ 5), giving **239 periods instead of 236**; on a LIVE
    book it would print five bank tickers off a seven-name panel.
15. ⚠️ **UNDER A TRADABILITY GATE THE LEVELS COLLAPSE, AND `k=20` BEATS `k=5` ON EVIDENCE.** Gating on
    trailing 60-session median `value_matched` (`shift(1)`, no look-ahead) takes h=10 k=5 from
    **+181.3 %** to **+36.5 %** (ADV ≥ 1 bn) and **+19.9 %** (≥ 5 bn); daily IC falls 0.1412 → 0.0816
    → 0.0667 and max drawdown WORSENS to −50.7 % / −64.0 %. ✅ All eight gated cells still clear a
    200-draw within-date null with MAX below observed — but **`z` is higher at k=20 in every one of
    them**. **On a buyable basket, cutting `k` weakens the evidence and adds almost nothing.**
    ⚠️ The gated null MEAN is +0.22…+0.34, not zero, and this is a post-hoc filter on a model trained
    over all 150 names — the screened chain is the real test.
16. ⚠️ **CHECK THAT THE IC DECAYS WITH THE HORIZON BEFORE BELIEVING ANY CAGR LADDER.** The h=10
    model's IC is **FLAT from h=1 to h=30** (+0.1403 … +0.1328) when it should peak at its own label,
    and with a constant IC `CAGR ∝ 1/√h` is arithmetic, not skill — which is why the same predictions
    "earn" **+1416.9 %/yr** rebalanced daily. The cause is measured: **51.2 %** of rows with ADV60 <
    0.1 bn have a forward 1-day return of **exactly zero**, and a frozen price ranks the same at every
    horizon. ✅ Under the ADV ≥ 1 bn gate the ladder flattens.

## 8a. ⚠️ CURRENT STATE — the chain CANNOT emit a pick list

| rebalance date | names scored & buyable |
|---|---|
| 2026-05-27 · **2026-06-10** | 143 of 150 · **147 ← the last real book** |
| 2026-06-24 · 07-08 · 07-22 | **7** each |

⚠️ **`MAX(date)` on `pool__basic` reports 2026-08-07 and conceals this completely** — §8 rule 1's
mechanism at the row level. Mean names scored per session runs **145-147 for 2017-2025** and **113.3
in 2026**, so the annual average hides a cliff.

⚠️ **THE 2026-06-11 CLIFF IS IN THE LABEL, NOT IN THE PRICE.** This file used to say *"after
2026-06-11 only 7 of 150 names carry data"*: the date is right and the mechanism was wrong — **all
150 names carry a close through 2026-06-25**. What ends on 2026-06-11 is `return_10day`, because it
needs a close ten sessions later. Last session with ≥100 names LABELLED: **2026-06-18** at h=5,
**2026-06-11** at h=10, **2026-05-28** at h=20. So the horizon decides where a track ends, and a
live-scoring module (`P7`) has about ten more sessions of usable FEATURES than that sentence implied
— **ranking a book and scoring a book fail on different dates.**

✅ **The published +74 %/yr is NOT affected** — `long_only_top_k` skipped those three stub dates
(239 rebalance dates − 3 = 236 periods, exactly the `n_periods` on the artefact). ⚠️ **But the skip
is SILENT — a data freeze SHORTENS the track instead of failing it.** Check the width per rebalance
date before reading any recent book:

```powershell
python -c "import pandas as pd; d=pd.read_csv('../results/walkforward_h10/predictions_oos.csv'); print(d.groupby('date').size().tail(40))"
```

### ⚠️ REFRESHING THE PRICE UNIVERSE — use `incremental`, not `skip_existing: false`

The four CafeF daily tabs resume each ticker from **its own last date** instead of refetching from
2009, which is what made a universe refresh affordable at all: **615 s per ticker** for a full 4-tab
refetch (~67 h for 781 tickers at the old 2-worker pool) against **2.9-5.2 s** to resume one.

```powershell
dagster asset materialize -f src/orchestration/definitions.py `
  --select "raw/cafef_price,raw/cafef_order_stats,raw/cafef_foreign,raw/cafef_prop_trading" `
  --config refresh.yaml
```
```yaml
# refresh.yaml — all four tabs, every ticker
ops:
  raw__cafef_price:        {config: {skip_existing: false, incremental: true}}
  raw__cafef_order_stats:  {config: {skip_existing: false, incremental: true}}
  raw__cafef_foreign:      {config: {skip_existing: false, incremental: true}}
  raw__cafef_prop_trading: {config: {skip_existing: false, incremental: true}}
```

⚠️ **`incremental: true` NEEDS `skip_existing: false` BESIDE IT** — `skip_existing` is checked first
and returns before the resume is reached, so `incremental: true` alone refreshes **nothing** and
still goes green, the exact failure mode §8 rule 1 is about.

⚠️ **A RESTATEMENT IS A WARNING, NOT A FAILURE, AND YOU SHOULD READ THEM.** CafeF re-bases a whole
history when a stock splits or pays a dividend, so the resume refetches a 45-day overlap and
compares it; a ticker that disagrees falls back to the full refetch on its own. Count them
afterwards — **13 fired within the first two minutes of the 2026-08-23 run**, each a series a naive
append would have spliced across two price bases:

```powershell
Select-String -Path logs\app.log -Pattern "RESTATED" | Measure-Object
```

⚠️ **The four assets run CONCURRENTLY**, so a universe refresh is 4 × `SCRAPER_MAX_WORKERS` threads
against CafeF (48 at the current 12). `foreign` and `order_stats` finish in ~2 minutes;
**`prop_trading` is the long pole**, because 350 of 781 tickers have no prop history and correctly
take the full path every time.

⚠️ **Then carry it up** — a scrape that stops at `raw_data/` changes nothing a model reads:
`bronze/cafef_*` → `silver/cafef_*` → `silver/stocks_basic`, then `gold/*` → `filter/universe` →
`group:unified`. §8 rule 1's cousin — *"re-scraped" never implies "re-ingested"* — is why they are
two commands and not one. ⚠️ **`--rescrape` is still opt-in and still scoped to `--ticker` with
`skip_existing=False`**: without both it either costs 781 tickers or fetches nothing.
⚠️ **And there is still no LIVE-SCORING path** — every stage writes predictions for a dataset's
*test split*, so the chain cannot score today's cross-section even with fresh data (`P7`).

### ✅ CHECKING FRESHNESS AFTERWARDS — `MAX(date)` is a scalar and it lies

⚠️ **A refresh is not verified by one date.** `MAX(date)` cannot say how many tickers produce it: it
once read 2026-08-19 from **five** tickers while 757 of 781 were frozen, and every narrowly-scoped
re-scrape pushed it further from the truth. Verify with the distribution instead:

```powershell
python -m pipeline.freshness                  # every layer, ~33 s (39 layers)
python -m pipeline.freshness --layer silver   # one layer, ~1 s
python -m pipeline.freshness --install        # (re)create the three SQL functions
```
```sql
-- which names are behind, and by how many SESSIONS (not calendar days)
SELECT ticker, last_date, sessions_behind
FROM health_schema.ticker_freshness('silver')
WHERE NOT is_current ORDER BY sessions_behind DESC;
```

⚠️ **PASS THE LAYER AS THE ARGUMENT; DO NOT FILTER THE RESULT** — a `WHERE layer='silver'` written
after the call cannot push into the function: **32.9 s** against **0.25 s**, and `gold.stocks_ta`
alone costs 26.5 s.
⚠️ **THEY ARE FUNCTIONS, NOT VIEWS, AND THE REASON MATTERS IF YOU EVER ADD A MONITOR HERE**
(`DEP-1`): a PostgreSQL view records a dependency on its tables and every builder here opens with
`DROP TABLE IF EXISTS`, so the first version blocked the whole write path until it was uninstalled.
**Ask of any new monitor: what does this stop the repair path from doing?**
**Read the shape, not the count** — a **cliff** (many tickers stopping on ONE date) is a scrape scope
and the tool warns; **scattered** dates are delistings and it says so. Both measured: 599 of 781 on
one date (77 %) against 5 of 784 (0.6 %). ✅ **No re-install after building a new schema** — the
layer list is a query against `information_schema`, evaluated at call time.

⚠️ **AND RE-RUN THE CARRY-UP FOR ANY SINGLE-NAME SCHEMA** — rule 14 means a fresh silver does not
mark them stale, and `MAX(date)` on the schemas that *were* rebuilt says nothing about the others
(`SCH-1`: 28 were stale, rebuilt at a measured **21 s per schema**):

```powershell
dagster asset materialize -f src/orchestration/definitions.py `
  --select "unified/pool__basic,unified/pool__targets" --partition VNM
```

⚠️ **That rebuilds the two pools the price data feeds, and NOT the other 23** — `pool__bonds` and the
19 `pool__economy_*` stay on the old calendar, which `status_data` reports as `pools_behind`, and a
wide join over such a schema INNER-joins back down to theirs.

## 8b. Reading the output as a PICK LIST — three rules

1. ⚠️ **ONE BOOK IS CLOSE TO A COIN FLIP.** Over 236 books the mean realised rank is **+0.0688** and
   **81.8 %** are positive (`t` = +14.0), but individual picks land in the top half only **60.2 %**
   of the time and **43 of 236 books were negative**. The +74 %/yr is a 60/40 edge compounding over
   236 periods — never a single book.
2. ⚠️ **THE MODEL OVER-PICKS THE BOARDS YOU CAN LEAST TRADE.** UPCOM is **2.20×** its share of scored
   rows, HNX 1.31×, HOSE **0.76×**; the most-selected names are `DCT` (108 of 236 books), `DCS`
   (106), `EFI` (87) while `VCB` appears in 30. **With no ADV cap and no slippage modelled (`P11`),
   this is the biggest open threat to the levels.**
3. **Turnover is 65.1 % per rebalance** (median 65.0 %, range 20-90 %) → **8.2 %/yr** at 50 bps.
   ✅ That confirms `backtest/CONTEXT.md` §3's assumed `τ = 0.70` from the data.

## 8c. ⚠️ BEFORE YOU COMMIT — record the state

```powershell
python docs/state_check.py      # about two seconds
```

⚠️ **It REPORTS and never rewrites.** Every finding is handed back as a decision, because these
numbers cannot be derived mechanically without getting them wrong — `ISSUES.md` keeps FIXED rows
inside its Open table on purpose, so a naive row-counter disagrees with the headings. **A
confidently wrong number is worse than no number**, since it is what the next session budgets against.

| the check | what a failure means, and where the fix goes |
|---|---|
| **`CLAUDE.md` §6 date** | the heading is older than the `.md` files in this commit. If the commit changes what the project KNOWS, bump the date and write the measurement in; if it is a typo fix, ignore the row |
| **`CONTEXT.md` ↔ `CLAUDE.md`** | a package `CONTEXT.md` changed and `CLAUDE.md` did not. ⚠️ **A measurement that never reaches the hub is invisible** — every session loads `CLAUDE.md`, almost none open a given `CONTEXT.md`. "The detail stays local" is a valid answer, but decide it |
| **issue counts** | `CLAUDE.md`'s *"N open, M resolved"* disagrees with `ISSUES.md`'s own headings. Re-SCAN the tables — do not decrement |
| **`INDEX.md` completeness** | a `.md` file exists that the index does not route. **A file missing from the index is a file no session knows exists** |
| **`INDEX.md` token costs** | a claimed cost drifted >20 % from measured. ⚠️ **This check exists because all 16 of `CLAUDE.md` §7's costs had gone stale** |
| **relative links** | a markdown link points at nothing. ⚠️ **11 are known-broken and pre-date the docs move**, so read the count, not just the colour |

| you changed | it goes in |
|---|---|
| a new measurement, or one that moves a verdict | `CLAUDE.md` §6 (+ bump the date), or the package's `CONTEXT.md` |
| a new defect | `docs/ISSUES.md`, with a **permanent** code |
| a finished backlog item | its number moves to `CLAUDE.md` / `CONTEXT.md`; the item is **deleted from `docs/TODO.md`, not ticked** |
| a new `.md` file | a row in `docs/INDEX.md` (`python docs/check_index.py` fails without one) |
| a new command or stage | this file |

⚠️ **NOTHING ENFORCES THIS AT COMMIT TIME, AND THAT IS A CHOICE.** A hook that blocks an unrelated
commit costs more than the drift it prevents, and `--no-verify` would train the reflex to bypass it.
`state_check.py` exits **1** on drift, so it can be wired in later if that trade changes.

---

## 9. Traps that have actually bitten, and the fix

| symptom | cause | fix |
|---|---|---|
| `ModuleNotFoundError: pipeline` | run from repo root | `cd src` |
| `does not exist. Available: [...]` from `train_test_creator` | the stage defaulted to a different target than `pipeline` planned | pass `--table`, or edit `utils/chain.py` |
| `UnicodeEncodeError` at the end of a long run | Windows console is cp1252 and the text has `⚠️` | handled in the stages; in **your own** scripts call `sys.stdout.reconfigure(errors="replace")` |
| `Password cannot be empty` in an ad-hoc script | scratchpad scripts are outside the repo, so `find_dotenv()` misses `.env` | `load_dotenv(os.path.abspath(".env"), override=True)` with cwd = repo root |
| a scrape goes green in 500 ms | `skip_existing=True` — `landed()` asks "is the folder empty?", not "did this run fetch anything?" | check the **per-series max date**, never the asset colour |
| a rebuilt pool silently loses years of rows | sibling pools on an older calendar, and the join is INNER | `pipeline`'s `data` row reports `pools_behind`; rebuild the siblings first |
| a run folder is skipped by `final_features` with no message | it has no `outstanding.csv` | `plan_from_reports` ignores such folders |
| `final_features` raises *"two different setups both want …"* and plans NOTHING | a PROBE run is sitting in the chain's report root | move it to `reports/feature_selection_probes/` — §3b. ⚠️ `--scope` does **not** fix this |
| a rebuild quietly gains channels nobody selected | same cause, silent half: the probe shares setup keys, so it is UNIONED rather than colliding | §3b, and check the table's `obj_description` for its `Source runs:` |
| a Kaggle job "finishes" but no run folder appears | the local WATCHER died on a network error; the kernel is probably still running | `kgpu status <job>`, then `kgpu wait` + `kgpu pull` — §7a |
| `XGBoostError: … cudaErrorMemoryAllocation` mid-selection | more than ~120 channels on a T4 — the ceiling is VRAM, not host RAM (`VRM-1`) | prune the pool first: `python -m feature_selection.prune` |
| a Kaggle kernel ends `ERROR` with `DeadKernelError` and no traceback | **OOM-kill.** A Python exception leaves a traceback; a dead kernel was terminated by the OS | `kgpu logs <job>` and read the PHASE lines — `rss=` says where it settled, `peak=` says what killed it |
| a memory fix lands and `peak` does not move | you fixed a real allocation that was not the BINDING one | measured: row-blocking `window_design` moved a 23.3 GB cube and the peak by **0.1 GB**, because the panel path allocates elsewhere. ⚠️ `rss` went UP while `peak` held — **read `peak`, never `rss` alone** |
| `walkforward.compare` raises *"arm X covers N rows against …"* | the two tracks do not span the same panel — a wider pool changes coverage, a different `rank_min_width` changes the label | **the refusal is correct.** Price them on the INTERSECTION, never by comparing two unpaired Sharpes |
| `final_features --scope X` plans a table you did not ask for | ⚠️ **a scope names EVERY table in the plan**, and a report root holding two experiments plans both | give the second experiment its own `--root` — **and add its `.gitignore` negation pair in the same commit**, or its CSVs are silently dropped |
| a long background run's log holds only the banner, then the process is gone | **stdout buffering** (§5 rule 20) — redirecting to a file re-buffers, so nothing lands until exit and an interim `tail` reads as a crash | run it as `python -u -m ...` and read the exit status before concluding it died. Measured: a 200-draw null that looked dead had finished in 8m 40s |
| a scratchpad script raises `KeyError: 'exchange'` in `portfolio.mark_ceiling` | the `pool__basic` read dropped a column the screen needs | select `date, exchange, ticker, close_adjust` plus a `day_ret` |
| `Unknown top-level keys in kaggle_config.json` | a comment or note added at the top level; the schema is closed | put prose in `kaggle_gpu/README.md`, not in the config |

## 10. What is NOT standardized yet

- **Stages 2 and 4 stay manual by design** — there is no `--apply` that will spend an hour of GPU for
  you.
- **The cost estimator is wrong.** Both models (`0.364 × ch^0.77` and the Dagster guard's
  `1.1 × (ch/113)² × (1+draws)`) were fitted with `lasso`, dropped 2026-08-16. The guard
  over-predicts by **4-13×** — 393 min for a run that took **29m 44s** — so `budget_minutes` raises
  on runs you can afford. Treat the raise as advisory and read §3's measured column (`P23`).
- **`hit_rate` on level targets** — ✅ fixed: `sign_hit_rate` returns NaN when every non-zero label
  shares a sign and the README prints **`—`**. That dash is the deliberate absence, not a defect.
- **The 19 archived country runs group as `methods="unrecorded"`.** They still plan and validate but
  will not union with a new three-ranker run — `MTH-1` working, not a bug. Pass `--scope`.
