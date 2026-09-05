# `.claude/runbook/RUNBOOK.md` — the command templates

> **This file is a LOOKUP TABLE, not a guide.** One row per thing you can run: the command with
> its placeholders, what it writes, what it costs, and the steps that must happen **before** and
> **after** it. Copy a row, fill the `<PLACEHOLDERS>`, run it.
>
>
> ⚠️ **A step is not optional because it is boring.** Nearly every row's "after" column exists
> because skipping it once produced a green run that had done nothing — `CLAUDE.md` §5 rules 10,
> 11 and 14 are three separate forms of that failure, and they are why this table has a steps
> column at all.
>
> **Related:** [../workflows/README.md](../workflows/README.md) routes the step-by-step guides for
> whole JOBS (a job uses several rows from here); [../current_state/README.md](../current_state/README.md)
> is where a measured snapshot gets written down.

---

## 0. Every shell, before anything else

```powershell
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"   # ⚠️ absolute, and required
Clear-Content logs\app.log                            # before any pipeline/ingest run
cd src                                                # every `python -m` below runs from src\
```

⚠️ **`cd src` matters** — every stage is a package under `src\`; from the repo root
`python -m pipeline` raises `ModuleNotFoundError`. ⚠️ **`dagster asset materialize` is the
exception** — its `-f` path in this table is written from the REPO ROOT
(`src/orchestration/definitions.py`).

---

## 1. The table

`ID` is a handle for citing a row in a conversation or a commit message. **Measured** is a real
runtime that was MEASURED, never an estimate — an unmeasured cell reads `—`.

### A · Orientation — writes NOTHING, run these first

| ID | you want to… | command template | measured | before → after |
|---|---|---|---|---|
| **O1** | know what is stale in the chain | `python -m pipeline` | ~5 s | — → read the `why` column, not just the colour |
| **O2** | know what is stale for ONE experiment | `python -m pipeline --ticker <TICKER> --table <TABLE> --config <CFG>.yaml` | 5.8 s | — → ⚠️ `--config` is NOT optional; without it the `model` row scores the DEFAULT chain's run |
| **O3** | know whether the DATA is fresh | `python -m pipeline.freshness --layer <LAYER>` | ~1 s (silver) · ~33 s (all 39) | — → read the SHAPE: a cliff is a scrape scope, scatter is delistings |
| **O4** | see which tickers are behind | `SELECT ticker, last_date, sessions_behind FROM health_schema.ticker_freshness('<LAYER>') WHERE NOT is_current ORDER BY sessions_behind DESC;` | 0.25 s | O3 → ⚠️ **pass the layer as the ARGUMENT**; a `WHERE layer = …` written after the call costs 32.9 s instead |
| **O5** | check the docs before committing | `python ../tools/state_check.py` | ~2 s | — → resolve every row it reports; it REPORTS and never rewrites |
| **O6** | check a new `.md` is routed | `python ../tools/check_index.py` | ~1 s | wrote a `.md` → add its row to `../current_state/INDEX.md` |
| **O7** | re-read what a finished track scored | `python -m walkforward.evaluate --top-k 20 --draws 0 --universe all --out <DIR>` | ~2 min | — → ⚠️ **it REWRITES `per_fold.csv`**, and at a different `--top-k` it OVERWRITES the published table |

### B · The chain — stages 0-9, in order

| ID | stage | command template | writes | measured | before → after |
|---|---|---|---|---|---|
| **C0** | 0 · filter *(optional)* | `dagster asset materialize -f src/orchestration/definitions.py --select "filter/universe" --partition <SCREEN>` | `filter_schema.universe__<screen>` | ~1 s | — → ⚠️ **C1 does NOT follow by itself.** Rule 14: a fresh screen leaves a stale schema looking current |
| **C1** | 1 · data | `dagster asset materialize -f src/orchestration/definitions.py --select "unified/pool__basic,unified/pool__targets" --partition <PART>` | `unified_schema_<part>.pool__*` | 7m 36s at 480 tickers | C0 → O3. ⚠️ **`--select "group:unified"` builds all TWELVE pools** — hours. Name the two you need |
| **C2** | 2 · selection ⚠️ MANUAL | `python -m feature_selection.run --ticker <T> --pools <POOL> --target <TGT> --lookback <D> --horizon <H> --null-draws <N> --device cuda` | `reports/feature_selection/<run>/` | ~1 min per country pool · 29m 44s at 644 ch | C1 → §2.C2. ⚠️ **10 draws to FAIL, 20 to PASS.** A PROBE takes `--root ../reports/feature_selection_probes` |
| **C3** | 3 · shortlist_pool | `python -m final_features --apply --shape shortlist --scope <SCOPE>` | `pool__shortlist__<tgt>__d<d>_h<h>` | seconds | C2 → ⚠️ the pool is **target-conditioned**; reusing it for another target is leakage |
| **C4** | 4 · selection_2 ⚠️ MANUAL | `python -m feature_selection.run --pools pool__shortlist__<TGT>__d<D>_h<H> --null-draws <N>` | another run folder | 29m 44s at 644 ch | C3 → C5. ⚠️ reports `n/a` on a cross-sectional chain (`CSP-1`) |
| **C5** | 5 · final_features | `python -m final_features --apply` | `<target>__final__d<d>_h<h>` | 0.8 s · 7.3 s at 624k rows | C2/C4 → ⚠️ read the fingerprints it prints **before** adding `--replace`; `--replace` DROPS the table and orphans every dataset below it |
| **C6** | 6 · train_test_creator | `python -m train_test_creator --ticker <T> --table <TABLE> --save` | `src/train_test_set/<dataset>/` | 0.5 s · 10.9 s at 624k rows | C5 → **write the model config now**; `n_features` is an assertion and is only knowable here |
| **C7** | 7 · model | `python -m model.<ARCH> --config configs/<RUN_NAME>.yaml` | `src/model/runs/<run_id>/` | 4m 23s (h=20 LSTM) | C6 → ⚠️ the config filename must **equal `run_name`**. `--dry-run` validates without training |
| **C8** | 8 · result_evaluator | `python -m result_evaluator --rescore` **then** `python -m result_evaluator --rebuild-index` | `results/metrics.json` · `runs/index.csv` | 41.6 s + 42.7 s | C7 → ⚠️ **TWO commands.** `--rescore` alone once left the folder reading +3.47 while the leaderboard still read 15.50 |
| **C9** | 9 · backtest ⚠️ PANEL ONLY | `python -m backtest --run <RUN_ID> --ticker <T> --top-k <K> --draws 200` | `runs/<run_id>/results/backtest_<split>.csv` | 1m 14s | C8 → ⚠️ the CSV lands **inside the gitignored run folder** (`RPR-1`), not in repo-root `results/` |

### C · Beyond one split — the tools that are not stages

| ID | answers | command template | measured | before → after |
|---|---|---|---|---|
| **W1** | *is this one lucky split?* | `python -m walkforward --ticker all --table <TABLE> --config <CFG>.yaml --first-test 2017-01-01 --out <DIR>` then `python -m walkforward.evaluate --top-k 20 --draws 200 --universe all --out <DIR>` | ~35 min | C7 → ⚠️ **`--out` IS LOAD-BEARING** — omitting it silently overwrote the published h=20 track once |
| **W2** | *does the ARCHITECTURE matter?* | `python -m walkforward --out <DIR> --arm <PKG>:<CFG>.yaml …` then `python -m walkforward.compare --top-k 20 --horizon <H> --draws 200 a=<DIR>/a b=<DIR>/b` | 2h 49m (7 arms × 10 folds) + 22m 25s scoring | W1 → ⚠️ read **both** `t_ret` and `d_sharpe`; they disagreed about 3 of 6 arms |
| **W3** | *does one HORIZON beat another?* | `python -m walkforward.pair --top-k 20 --draws 2000 h10=<DIR_A>:10 h20=<DIR_B>:20` | 48 s | two finished tracks → the ONLY tool that can compare two horizons |
| **W4** | *does a SPLIT or DATASET setting matter?* | `python -m walkforward … --out <DIR>/<TAG> [--val-months N] [--step-months N] [--no-scale-target]` then `compare` | ~20 min per `gbt` track | W1 → use the cheapest arm; the model must be the constant |
| **W5** | *does it beat three ranked columns?* | `python -m backtest.handscreen --run <RUN_ID> --top-k 20 --draws 200` | 1m 53s | C9 → run it **beside** the backtest, never instead of it |
| **W6** | *does chain A beat chain B?* | `python -m backtest.head2head --a <RUN_A> --b <RUN_B> --top-k 15 --draws 200` | 2m 18s | two runs → priced on the INTERSECTION, paired |
| **W7** | *which channels can a wide pool even OFFER?* | `python -m feature_selection.prune --ticker ALL --pool <POOL> --universe-from <TABLE> --budget 30 --out <JSON>` | ~1 min | before C2 on a wide pool → ⚠️ LABEL-FREE by construction, and that is the point |

### D · Data — scrape, carry up, verify

| ID | you want to… | command template | measured | before → after |
|---|---|---|---|---|
| **D1** | refresh the price universe | `dagster asset materialize -f src/orchestration/definitions.py --select "raw/cafef_price,raw/cafef_order_stats,raw/cafef_foreign,raw/cafef_prop_trading" --config refresh.yaml` | 1h 05m (780 tickers) | §2.D1 → D2. ⚠️ **`incremental: true` NEEDS `skip_existing: false` beside it** |
| **D2** | carry a scrape up to the model | `dagster asset materialize -f src/orchestration/definitions.py --select "<ASSET>"`, layer by layer: `bronze/cafef_*` → `silver/*` → `gold/*` → `filter/universe` → `unified/pool__basic,unified/pool__targets` | 40 min (`gold.stocks_ta`) · 21 s per single-name schema | D1 → O3. ⚠️ **"re-scraped" never implies "re-ingested"** (rule 11) |
| **D3** | count the restatement warnings | `Select-String -Path logs\app.log -Pattern "RESTATED" \| Measure-Object` | — | D1 → a restatement is a WARNING, not a failure; 13 fired in the first two minutes once |
| **D4** | rebuild ONE single-name schema | `dagster asset materialize -f src/orchestration/definitions.py --select "unified/pool__basic,unified/pool__targets" --partition <TICKER>` | 21 s | D2 → ⚠️ this rebuilds **2 pools of 25**; the other 23 stay on the old calendar |
| **D5** | scrape filing PDFs, scoped BY YEAR | `dagster asset materialize -f src/orchestration/definitions.py --select "raw/cafef_pdfs" --partition-range "<FIRST>...<LAST>" --config phase1.yaml` | — (~286 GiB at `year_max: 2020`) | ⚠️ **`--config-json` is BROKEN in PowerShell 5.1** — it strips the quotes; use a YAML file |
| **D6** | check a fundamental's provenance | `SELECT source, COUNT(*) FROM bronze.cafef_financial_reports GROUP BY 1;` | — | before quoting ANY fundamental → ⚠️ anything but `pdf` or `missing` is a defect (`FIN-1`, §5 rule 24) |

### E · Kaggle T4 — when the local card is too small

| ID | you want to… | command template | measured | before → after |
|---|---|---|---|---|
| **K1** | see what would run | `cd src\kaggle_gpu` then `python -m kgpu plan <JOB>` | seconds | — → touches nothing |
| **K2** | build the payload | `python -m kgpu export <JOB>` | 2m 04s / 477 MB (panel) · 5.3 s / 92.4 MB (pdf-ocr) | K1 → K3 |
| **K3** | run the WORKER side locally | `python -m kgpu rehearse <JOB>` | 16.0 s (panel) · 9.7 s (pdf-ocr) | K2 → ⚠️ **every time a shipped module changed.** It is the only thing that catches an import the worker cannot satisfy |
| **K4** | upload, run, merge | `python -m kgpu data <JOB>` then `python -m kgpu run <JOB>` | 8m 15s minimum — **5.2 min of it QUEUED** | K3 → ⚠️ `$env:PYTHONUTF8 = "1"` is REQUIRED on `pdf-ocr` or the DOWNLOAD raises |
| **K5** | recover a run whose watcher died | `python -m kgpu status <JOB>` → `python -m kgpu wait <JOB>` → `python -m kgpu pull <JOB>` | — | a job that "finished" with no run folder → the kernel is probably still alive |

### F · Filings / OCR

| ID | you want to… | command template | measured | before → after |
|---|---|---|---|---|
| **F1** | OCR one quarter locally | `python -m web_scraper.pdf_ocr_job --symbol <SYM> --periods <Q>-<YYYY>` | 1m 41s (VCB Q1-2026) · 32.9 min (BID Q4-2016) | — → this is the baseline a Kaggle run is scored against |
| **F2** | OCR a whole ticker | ⚠️ **NOT a command.** Clone `src/kaggle_gpu/RUN__pdf_ocr_control.ipynb` → `RUN__pdf_ocr_control_<sym>.ipynb`, edit **cell 2 only**, resolve the parameters read-only, then **WAIT** | 185 min (HOSE_FPT, 71 filings, T4) | [../workflows/ocr-a-ticker.md](../workflows/ocr-a-ticker.md) |
| **F3** | recompute parser coverage | `RUN__pdf_ocr_summary.ipynb` — no OCR, no network | seconds | — → ⚠️ `complete` is CONTINUITY, not coverage; read the cell count for coverage |

---

## 2. The rows whose steps do not fit in a cell

### D1 · Refreshing the price universe

`refresh.yaml`, at the repo root:

```yaml
ops:
  raw__cafef_price:        {config: {skip_existing: false, incremental: true}}
  raw__cafef_order_stats:  {config: {skip_existing: false, incremental: true}}
  raw__cafef_foreign:      {config: {skip_existing: false, incremental: true}}
  raw__cafef_prop_trading: {config: {skip_existing: false, incremental: true}}
```

1. Write the YAML. ⚠️ **`incremental: true` alone refreshes NOTHING and still goes green** —
   `skip_existing` is checked first and returns before the resume is ever reached.
2. `Clear-Content logs\app.log`, then run **D1**.
3. **D3** — count the `RESTATED` lines. Each one is a series a naive append would have spliced
   across two price bases; the guard fell back to a full refetch on its own.
4. **D2** — carry it up, layer by layer. A scrape that stops at `raw_data/` changes nothing a
   model reads.
5. **O3** — verify by DISTRIBUTION. `MAX(date)` once read 2026-08-19 from **five** tickers while
   757 of 781 were frozen.
6. **D4** for every single-name schema that matters — rule 14 means a fresh silver does not mark
   them stale. 28 were found stale exactly this way.

### C2 / C4 · A selection run

1. **C1 first.** The pools join INNER, so a sibling on an older calendar silently truncates the
   run — `python -m pipeline`'s `data` row reports it as `pools_behind`.
2. Decide the draw count: **10 to FAIL something, 20 to PASS it.** `SE(sd)` is 0.0083 at 10 draws
   and 0.0051 at 20, and `z` is the statistic — `p` sits at the `1/(n+1)` floor either way.
3. Decide the ROOT. A run that measures the SELECTION is not a run that feeds the CHAIN:
   `--root ../reports/feature_selection_probes`. ⚠️ **`--scope` does not fix this** — it suffixes
   both groups identically, and a probe left in the chain's root is either silently unioned or
   blocks all planning (`PRB-1`).
4. Run it. A wide pool goes to Kaggle (**K1**→**K4**); this machine has 4.0 GiB of VRAM against a
   T4's 14.6.
5. Read `ic_mean`, the p95 bar, **and the null MAX**. ⚠️ **When the null MAX exceeds the observed,
   `clears_bar` is the wrong summary** — quote the max beside it (rule 3).

### C7 · Writing a model config

⚠️ **The config cannot be written before C6 exists.** `n_features` is an assertion `engine._verify`
raises on, and the surviving channel count is only known once the dataset is built. Write it
**between C6 and C7**, with the filename **equal to `run_name`**.

### O5 · What `state_check.py` reports, and where each fix goes

| you changed | it goes in |
|---|---|
| a new measurement, or one that moves a verdict | `CLAUDE.md` §6 (+ bump the date), or the package's `CONTEXT.md` |
| a new defect | `../current_state/ISSUES.md`, with a **permanent** code |
| a finished backlog item | its number moves to `CLAUDE.md` / `CONTEXT.md`; the item is **deleted from `../current_state/TODO.md`, not ticked** |
| a new `.md` file | a row in `../current_state/INDEX.md` |
| a new command or stage | **this file** |

---

## 3. Adding a row

1. **Run the command first.** A template nobody has executed is a guess, and this table's whole
   value is that its runtimes were measured. An unmeasured cost is `—`, never a number.
2. **Fill the `before → after` column.** If it comes out empty, ask what a green run of this
   command would fail to change — that answer is the "after" step.
   That file is the authority; this one is the index into it.
4. **Date anything you measured.** A number without a date cannot be told from a stale one.
