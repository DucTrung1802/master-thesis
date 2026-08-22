# RUNBOOK — how to run this chain

> Written 2026-08-16, extended 2026-08-19 (`PRF-1`/`PRF-2`/`PRF-8`/`PRF-9` and the
> tools they needed). Every runtime below was **measured**, not estimated — see §8 for
> why that distinction matters here.
>
> ### The four root registers — one job each, no overlap
>
> | file | answers | when you touch it |
> |---|---|---|
> | **[CLAUDE.md](../CLAUDE.md)** | *what is this, and what has it PROVED?* | auto-loaded every session; the map and the verdict |
> | **[RUNBOOK.md](RUNBOOK.md)** | *how do I RUN it?* | commands, stage order, the flags that destroy things |
> | **[ISSUES.md](ISSUES.md)** | *what is BROKEN?* | permanent codes; a code is never renumbered or reused |
> | **[TODO.md](TODO.md)** | *what is NEXT?* | one list, `P1` first. ⚠️ **A bare `P<n>` is LIVE; a HYPHENATED code (`P1-9`, `PRF-8`, `M-3`) is RETIRED** — renumbered 2026-08-21, crosswalk at the top of that file |
>
> Movement between them is one-way and worth knowing: a TODO item that turns out to be a
> defect **graduates to ISSUES.md with a code**; an ISSUES entry that gets fixed keeps its
> row and is struck through; a TODO item that gets done leaves its measurement in CLAUDE.md
> or a `CONTEXT.md` and is **deleted, not ticked**. Anything a future session must not
> rediscover belongs in CLAUDE.md, not here.
>
> ⚠️ **When another register disagrees with this file about a COMMAND, this file wins** —
> it is the one whose commands were actually run.

---

## 1. TL;DR

```powershell
.\mt_env\Scripts\Activate.ps1
cd src
python -m pipeline            # what is stale — writes NOTHING
python -m pipeline --apply    # run the stale stages
```

`python -m pipeline` is safe to run any time. It writes nothing, takes ~5 s, and prints
one row per stage saying `ready` / `would run` and **why**. Start every session with it.

---

## 2. Once per shell

```powershell
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"   # ⚠️ absolute, and required
Clear-Content logs\app.log                            # before any pipeline/ingest run
cd src                                                # every `python -m` below runs from src\
```

⚠️ **`cd src` matters.** Every stage is a package under `src\`. From the repo root
`python -m pipeline` raises `ModuleNotFoundError`.

---

## 3. The chain, and what each stage costs

The chain's DEFAULT is **`vcb` / `close_adjust_5day` / `d=20, h=5`**, named in exactly one
place: [src/utils/chain.py](../src/utils/chain.py). Every stage derives from there.

⚠️ **The default is not the live experiment, and knowing which is which matters.** Both
chains are built in the database as of 2026-08-17:

| target | table | evidence | verdict |
|---|---|---|---|
| `close_adjust_5day` *(the default)* | `…__final__d20_h5`, 35 ch | `failed_null=1` | ❌ **a price LEVEL — R² −85.6, MASE 21.4, ROC AUC undefined.** Do not start new work here |
| **`return_5day`** | `…__final__d20_h5`, 66 ch | `cleared_p95_not_a_pass` | ⚠️ layer 2 "clears" but see **TODO P0-1** — four measured reasons the bar is too low |

Use `--table return_5day__final__d20_h5` explicitly, or move `DEFAULT_TARGET`. The default
was left on `close_adjust_5day` only because moving it is a behaviour change nobody asked
for yet.

| # | stage | command (from `src\`) | writes | measured |
|---|---|---|---|---|
| **0** | **filter** *(NEW 2026-08-22, optional)* | `dagster asset materialize -f orchestration/definitions.py --select "filter/universe" --partition PRICE10K` | `filter_schema.universe__price10k` | **~1 s** |
| 1 | data | `dagster asset materialize -f orchestration/definitions.py --select "group:unified" --partition VCB` | `unified_schema_vcb.pool__*` | minutes–hours |
| 2 | selection | `python -m feature_selection.run --pools pool__basic --null-draws 10` | `reports/feature_selection/<run>/` | **~1 min** per country pool |
| 3 | shortlist_pool | `python -m final_features --apply --shape shortlist` | `pool__shortlist__<target>__d20_h5` | seconds |
| 4 | selection_2 | `python -m feature_selection.run --pools pool__shortlist__<target>__d20_h5 --null-draws 10` | another run folder | **29m 44s** at 644 ch |
| 5 | final_features | `python -m final_features --apply` | `<target>__final__d20_h5` | **0.8 s** |
| 6 | train_test_creator | `python -m train_test_creator --save` | `src/train_test_set/<dataset>/` | **0.5 s** |
| 7 | model | `python -m model.lstm` *or* `model/lstm/RUN__lstm.ipynb` | `src/model/runs/<run_id>/` | minutes |
| 8 | result_evaluator | `python -m result_evaluator` | `results/metrics.json` — ⚠️ **`runs/index.csv` only via `--rebuild-index`** | **41.6 s** `--rescore`, **42.7 s** `--rebuild-index`, 3 runs |
| 9 | **backtest** | `python -m backtest --run <run_id> --ticker VCB --top-k 15` | `src/model/runs/<run_id>/results/backtest_<split>.csv` + `backtest_null_<split>.csv` ⚠️ **inside the RUN FOLDER** (gitignored, `RPR-1`) — repo-root `results/` holds only the walk-forward tracks | **1m 14s** with a 200-draw null |

**Four tools added 2026-08-19, none of them a stage — each answers a question the chain
cannot ask about itself:**

| tool | command | answers | measured |
|---|---|---|---|
| **walk-forward** | `python -m walkforward --ticker all --table <T> --config <C> --first-test 2017-01-01 --out <dir>` then `python -m walkforward.evaluate --top-k 20 --draws 200 --universe all --out <dir>` | *is this one lucky split?* 10 expanding folds, one OOS track. ⚠️ `--horizon` is DERIVED from the track since 2026-08-21; to re-read a FINISHED track's pooled row use `--draws 0` (§7b, ~2 min) | **~35 min**, 10 folds |
| **arms** (`PRF-8`) | `python -m walkforward --out <dir> --arm lstm:<cfgA>.yaml --arm gbt:<cfgB>.yaml` then `python -m walkforward.compare --top-k 20 --draws 200 a=<dirA> b=<dirB>` | *does the ARCHITECTURE matter?* All arms train on ONE build of each fold | **15m 03s**, 10 folds × 2 arms |
| **pair** (`P2-4`) | `python -m walkforward.pair --top-k 20 --draws 2000 h10=<dirA>:10 h20=<dirB>:20` | *does one HORIZON beat another?* Pairs on the CALENDAR, not on periods — the only tool that can compare two horizons | **48 s** |
| **arm sweep, N arms** | `python -m walkforward --out <dir> --arm <pkg>:<cfg>.yaml ...` then `python -m walkforward.compare --top-k 20 --horizon <h> --draws 200 a=<dir>/a b=<dir>/b ...` | *does the ARCHITECTURE matter?* Every arm trains on ONE build of each fold | **2h 49m** for 7 arms × 10 folds; scoring **22m 25s** |
| **settings sweep** | `python -m walkforward … --out <dir>/<tag> [--val-months N] [--step-months N] [--no-scale-target] [--rank-min-width N]` then `walkforward.compare` | *does the SPLIT or the DATASET setting matter?* Use the cheapest arm — the model must be the constant | ~**20 min** per track with `gbt` |
| **hand baseline** (`PRF-2`) | `python -m backtest.handscreen --run <run_id> --top-k 20 --draws 200` | *does the model beat three ranked columns?* | **1m 53s** |
| **head to head** (`PRF-9`) | `python -m backtest.head2head --a <run_id> --b <run_id> --top-k 15 --draws 200` | *does chain A beat chain B?* Priced on the INTERSECTION, paired | **2m 18s** |
| **wide vs narrow, walk-forward** | intersect the two tracks on `(date, ticker)`, then `compare.net_series` + `compare.paired` | *does a wider chain PAY?* ⚠️ `walkforward.compare` REFUSES two tracks with different row coverage — correctly; a wider pool changes coverage, so the comparison must be on the INTERSECTION | ~3 min |
| **pool prune** (`PRF-9`) | `python -m feature_selection.prune --ticker ALL --pool pool__ta --universe-from <table> --budget 30 --out <json>` | *which channels can a wide pool even OFFER?* ⚠️ LABEL-FREE by construction | ~1 min |

### The model packages, and what each one costs to add

Ten architectures are wired to the shared engine. **A new one is `model.py` + a ~30-line
binding, never a copy of `train.py`** (`model/CONTEXT.md` §7) — the four added 2026-08-21
took about half an hour each.

| package | `model_type` | at h=10, 19 channels |
|---|---|---|
| `gbt` | GBT | **1,398 nodes** — the best arm measured |
| `cnn` | CNN | 5,185 params |
| `tcn` | TCN | 18,113 — dilated CAUSAL convolutions |
| `cnnlstm` | CNNLSTM | 30,369 — Conv1d then LSTM |
| `transformer` | TRANSFORMER | 68,417 — needs positional encoding or it is a set function |
| `lstm` | LSTM | 208,769 — the chain's reference |
| `bilstm` | BILSTM | 313,153 — reads `h_n`, NOT `out[:, -1, :]` |
| `gru` / `mlp` / `baseline_*` | — | the older arms |

⚠️ **Every arm in one sweep must inherit the reference's optimiser schedule, batch size,
patience and seed.** That is what makes them comparable; a difference in schedule shows up
as a difference in architecture. It also means a LOSS may be a schedule mismatch — `cnn`
wanting 20 epochs under a patience of 15 is the visible case (`walkforward/CONTEXT.md` §11d).

⚠️ **`--arm <pkg>:<cfg>` requires each arm's `run_name` to start with a DIFFERENT segment**,
because `Arm.label` is `run_name.split("__")[0]` and the arms share one output directory.

✅ **`WFO-1` FIXED 2026-08-21 — A SECOND SWEEP INTO AN OCCUPIED DIRECTORY IS NOW REFUSED.**
`DEFAULT_OUT` is still `results/walkforward/` and the artefacts are still written by
basename, but `run.main` now **claims** the directory before a single fold is built:
`walkforward/manifest.py` writes a `manifest.json` recording the experiment
(`ticker`, `table`, `first_test`, `step_months`, `val_months`, `scale_target`,
`rank_min_width`) and raises on a mismatch, naming the offending field. Verified against
the real command that nearly destroyed `PRF-1` — it now exits in **under a second**, before
any GPU time.

⚠️ **The five tracks that predate the manifest are protected too**, by recovering the table
from `folds.csv`'s run names. ⚠️ **But only the TABLE is checked there** — `folds.csv`
records no knobs, and §5 rule 2 says an absent measurement is absent rather than inferred.
So a legacy directory is guarded against the horizon collision that actually happened and
**not** against a knob-only one. Re-running any legacy track once writes its manifest and
closes that gap.

⚠️ **`--force-out` overrides the refusal** and is the only way to overwrite on purpose.

**One experiment, one `--out` is still the practice** — the refusal is a backstop, not a
licence to share a directory:

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

✅ **`--horizon` IS NOW DERIVED FROM THE TRACK AND THE FLAG IS OPTIONAL (2026-08-21).** It
sets the holding interval the periods are cut at AND the `return_{h}day` column that is
scored, and it used to default to **20** — so an h=10 track scored without it silently
scored the wrong label against the right predictions. `evaluate` and `compare` both read
the horizon out of the track's manifest (else its `folds.csv`) and **raise** when an
explicit `--horizon` disagrees. The commands above keep it only because they document what
was run. ⚠️ `compare` additionally refuses arms built at DIFFERENT horizons and points at
`walkforward.pair`, which is the only tool that can compare two (`P2-4`).

⚠️ **`walkforward.compare` and `backtest.head2head` PAIR the difference, and that is not a
nicety.** Every arm trades the same dates out of the same panel, so their period returns
correlate at **ρ 0.74-0.90** and `se_sharpe` ≈ 0.16-0.25 is the error bar on the wrong
quantity — unpaired, it cannot resolve the gaps these tools exist to measure. CLAUDE.md §5c
is the cautionary case.

⚠️ **Stages 2, 4 and W are MANUAL.** `python -m pipeline --apply` stops before each of them
and prints `MANUAL — cannot be produced here`, because each is an expensive artefact that
must be a deliberate act: a selection run is GPU-hours, and a walk-forward SWEEP is ~35 GPU
minutes and ten run folders. Everything else `--apply` will do for you.

⚠️ **`pipeline` KNOWS ABOUT STAGES 9 AND W SINCE 2026-08-21**, so it is now the gate rule 1
in §8 always claimed it was. Ten rows, and on a cross-sectional chain stages 3-4 report
`n/a` rather than proposing a pool nothing can read — §3a.

---

## 3a. ⚠️ THE CROSS-SECTIONAL CHAIN — every command as actually run, 2026-08-18

The chain that produced the repo's first out-of-sample model skill. Copy it; every runtime
below was measured on this machine, and the two traps in it each cost something today.

```powershell
cd src
# 2 — the selection is MANUAL and ran on a Kaggle T4 (6 h 07 m with a 20-draw null).
#     See §7a. Its run folder is already merged into reports/feature_selection/.

# 5 — the final table.  ⚠️ NO --scope.
python -m final_features --apply                                   # 7.3 s
#    -> unified_schema_all.rank_20day__final__d20_h20   624,448 x 17  (13 channels)

# 6 — tensors.  ⚠️ --ticker all IS NOT OPTIONAL.
python -m train_test_creator --ticker all --table rank_20day__final__d20_h20 --save
#    -> all__rank_20day__final__d20_h20__tr70_val15_test15__std     10.9 s
#       train 422,251 | val 91,462 | test 93,224   x 20 x 13

# 7 — train (config must be written FIRST; see below)
python -m model.lstm --config configs/lstm__all__rank_20day__final__d20_h20.yaml   # 4m 23s

# 8 — score it.  ⚠️ TWO commands: the first rewrites the run FOLDER, the second
#     rewrites index.csv.  Neither needs a GPU.
python -m result_evaluator --rescore         # 41.6 s
python -m result_evaluator --rebuild-index   # 42.7 s

# 9 — does the ranking pay for its own trading?  ⚠️ PANEL RUNS ONLY.
python -m backtest --run lstm__all__rank_20day__final__d20_h20__20260818-195738 `
    --ticker VCB --top-k 15 --draws 200          # 1m 14s
```

⚠️ **`--ticker all` is not optional.** `train_test_creator` defaults to
`chain.DEFAULT_TICKER`, which is `vcb`, and would look for the table in the wrong schema.

⚠️ **NO `--scope`, and an earlier draft of this runbook was wrong to suggest one.**
`--scope` names EVERY table in the plan: `--scope liquid150` was measured planning
`close_adjust_5day__final__d20_h5__liquid150` and `return_5day__final__d20_h5__liquid150`
as well — two junk duplicates of VCB tables that already exist. Plain `--apply` reports
those two as `exists=True, fingerprint matches` and skips them. A scope separates two
groups that COLLIDE on a name; nothing collides here.

✅ **`python -m pipeline` COVERS THIS CHAIN AS OF 2026-08-21 — the two warnings that used
to sit here are fixed, not worked around.** Measured on the h=20 chain: **10 stages, 5.8 s,
every row `up to date`.**

```powershell
python -m pipeline --ticker all --table rank_20day__final__d20_h20 `
    --config lstm__all__rank_20day__final__d20_h20.yaml
```

| what used to be wrong | now |
|---|---|
| `shortlist_pool` said *"would run"*, and `--apply` would build a `pool__shortlist__rank_20day__d20_h20` that **nothing can ever select over** | both it and `selection_2` report **`n/a — CROSS-SECTIONAL chain … there is no layer 2 (CSP-1)`** and are `ready`, so `--apply` skips them. `apply_shortlist_pool` also **raises** if forced with `--only`, because `--only` ignores `ready` |
| `selection_2` reported another chain's runs as `up to date` | ✅ `P4-11`, fixed 2026-08-21 (the detection is scoped) |
| stages 9 and W were invisible to the gate | **`backtest` and `walkforward` are stages now** — the two tools that produce every headline in §6-0 |

⚠️ **The chain is detected from the SHORTLISTS, never from the table name.**
`final_features` drops the `cs_` prefix when it names a table (`cs_rank_20day` →
`rank_20day__final__…`), so the name genuinely cannot say whether the selection was
cross-sectional. `pipeline.selected_for` reads `outstanding.csv`'s `target` column under
the same `(schema, d, h)` filter `TrainTestCreator` uses — one rule, not two.

⚠️ **`walkforward` is `manual` and has NO `apply`.** A sweep is ~35 GPU minutes and ten run
folders; `--apply` reports MANUAL rather than spending that for you, exactly as
`selection_2` does on the single-series chain.

⚠️ **`--config` is still not optional.** The `model` row keys on `--config`, not on
`--table`, so without one it reports the DEFAULT chain's run as up to date — and the
`backtest` row then scores that run too.

⚠️ **The model config cannot be written before stage 6 exists.** `n_features` is an
ASSERTION `engine._verify` raises on, and the surviving channel count is only known once
the dataset is built. Write it between 6 and 7, filename **equal to `run_name`**.

## 3b. ⚠️ A PROBE MUST NOT LAND IN THE CHAIN'S REPORT ROOT — `PRB-1`

`final_features.plan_from_reports` groups **every run under a root** by
`(schema, target, SETUP_KEYS)` and builds one table per group. **The data WINDOW is not a
setup key, and neither is which POOLS a run saw.** So a probe merged into
`reports/feature_selection/` is treated as a chain input, and it fails one of two ways:

| | |
|---|---|
| same setup keys | ⚠️ **SILENTLY UNIONED.** `PRF-7`'s pre-2017 probe (44 % of the dates) grouped with the two full-sample runs and would have merged its 15 channels into `rank_20day__final__d20_h20` on the next rebuild, **reporting success** |
| a different setup key | LOUD — two groups want one table name, `plan_from_reports` **raises**, and nothing plans at all, including unrelated chains |

⚠️ **`--scope` fixes NEITHER.** The table name is
`(schema, target, lookback, horizon, scope, shape)` — a scope suffixes *both* groups
identically. **Only `--root` separates them.**

**So there are three report roots now, and the rule is one sentence:**

| root | holds |
|---|---|
| `reports/feature_selection/` | runs that FEED the chain |
| `reports/feature_selection_probes/` | runs that MEASURE the selection — `PRF-7` (window), `FNM-1` (representation), the `PRF-9` memory pilots |
| `reports/feature_selection_wide/` | the `PRF-9` wide run, promoted to a chain input for its downstream test |

**A run that measures the SELECTION is not a run that feeds the CHAIN.** Every probe job
sets `results_into` **and** its `REPORT_ROOT` parameter to the probes root.

⚠️ **To build a table from a probe you need `--root` AND `--scope`** — the root to stop it
grouping with the others, the scope to stop it claiming a table name the chain already
holds (`--replace` would destroy that table):

```powershell
python -m final_features --apply --root ../reports/feature_selection_wide --scope wide
#    -> unified_schema_all.rank_20day__final__d20_h20__wide   (not …__d20_h20)
```

---

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

⚠️ **Run `handscreen` beside the backtest, not instead of it.** §5 rule 4's shape: a model
that does not beat three ranked columns has not earned its complexity. At h=10 the model
returns **+2.442** against the hand rule's **−0.263** on one panel (paired `t` = +5.94).

---

## 3d. ⚠️ SCREENING A UNIVERSE — the filter layer, as run 2026-08-22

Stage 0 is optional and only exists if you want a universe that is **not** `ALL`, `BANK`,
`VN30` or one company. A **screen** is a named list of conditions defined in
[src/orchestration/preprocessor/filters.py](../src/orchestration/preprocessor/filters.py);
materialising it writes the membership table, and the matching `unified` partition then
builds against it.

```powershell
cd D:\GIT\master-thesis
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"
Clear-Content logs\app.log

# 0. the membership table — ~1 s, writes filter_schema.universe__price10k
dagster asset materialize -f src/orchestration/definitions.py `
  --select "filter/universe" --partition PRICE10K

# 1. the schema it gates. pool__basic FIRST — every other pool joins to it as the spine.
dagster asset materialize -f src/orchestration/definitions.py `
  --select "unified/pool__basic"   --partition PRICE10K     # 7m 36s at 480 tickers
dagster asset materialize -f src/orchestration/definitions.py `
  --select "unified/pool__targets" --partition PRICE10K     # 40 s
```

**The three screens shipped, measured 2026-08-22:**

| partition | what it means | tickers | `pool__basic` |
|---|---|---|---|
| `PRICE10K` | `close_raw` never below **10,000 VND** on any session since 2026-01-01 | **480 / 781** | 1,503,958 × 101 |
| `LIQUID` | ≥1 bn VND/session median matched turnover, ≥80 % traded days, ≥200 sessions, still quoted | **206 / 781** | 657,892 × 101 |
| `QUALITY` | `LIQUID` + a 5,000 VND median price floor + a debt/equity ceiling | **200 / 781** | 635,919 × 101 |

⚠️ **`--select "group:unified"` builds ALL TWELVE pools** — `pool__ta`, `pool__fa`, 19
`pool__economy_*` and 48 `pool__forex_*` among them. On a 480-ticker screen that is
hours. Name the two pools you need, as above.

### Reading the screen table before you trust the universe

```sql
SELECT ticker, val__turnover_median_1bn, first_failed
FROM filter_schema.universe__quality WHERE NOT passes ORDER BY 2 DESC NULLS LAST;
```

⚠️ **EVERY CANDIDATE IS IN THE TABLE, not only the survivors** — 781 rows with each
condition's measured value, its verdict, `passes` and `first_failed`. That is what makes
"why is this ticker out" answerable without re-running anything.

⚠️ **A PASS RATE IS NOT A COVERAGE RATE.** `SELECT COUNT(val__<cond>)` is how many names
the condition could be MEASURED on; a condition everything cleared and a condition
nothing was measured for both report 100 % passing. `debt_to_equity_max_12` is the live
case: `gold.stocks_financials_bank_fa` holds **2 tickers of 781**, so it abstains on 779
(`on_missing="keep"`) and the asset logs a WARNING naming its 0.3 % coverage. Rule 22 at
the filter, and §8 rule 1's gate applies here too.

### ⚠️ Two things that will bite

1. **Re-running a screen does NOT rebuild the unified schema.** The edge is deliberately
   not declared (the two assets are partitioned on different sets), so a changed
   threshold leaves the old universe on disk looking current. Rebuild `pool__basic` and
   `pool__targets` for that partition yourself.
2. **A screen is NOT point-in-time.** Membership is decided from a window and applied to
   the whole history — `PRICE10K` picks names on 2026 prices and carries that back to
   2009. **A `z` against a within-date shuffle is protected; a CAGR read off one of these
   universes is not.** Every window is recorded in the table `COMMENT`:
   `SELECT obj_description('filter_schema.universe__price10k'::regclass, 'pg_class');`

### Adding a condition

One `register(Condition(...))` in `filters.py`, one entry in `SCREENS`, and the new key
under **both** `filter` and `unified` in `src/orchestration/config.json`. Then
`python -m pytest src/orchestration/preprocessor/test_filters.py -q` (30 tests, no
database) and `dagster definitions validate -f src/orchestration/definitions.py`.

## 4. The two flags that decide whether you destroy something

| flag | on | means |
|---|---|---|
| `--apply` | `final_features`, `pipeline` | actually write. Without it: plan only. |
| `--replace` | `final_features` | **DROP the existing table** and rebuild. Without it, a table whose fingerprint does not match is a hard **error**, not a silent overwrite. |
| `--replace` | `train_test_creator` | overwrite the dataset folder. ⚠️ **Any model run referencing its `dataset_hash` stops verifying.** |

⚠️ **`final_features --apply` alone will refuse** if the table exists and its fingerprint
moved. That refusal is the feature: rebuilding drops the table and orphans every dataset
below it. Read the fingerprints it prints before adding `--replace`.

---

## 5. A full run, end to end — copy this

⚠️ The block below is the `close_adjust_5day` chain as run 2026-08-16, kept because every command in it was executed. **For a new experiment substitute `return_5day`** and read §6 first — the shortlist pool is target-conditioned and cannot be reused.

```powershell
cd src

# 5 — rebuild the final table from the layer-2 run (drops the old 885-channel one)
python -m final_features --apply --replace
#    -> unified_schema_vcb.close_adjust_5day__final__d20_h5   4,266 x 39  (35 channels)

# 6 — tensors, scalers, metadata
python -m train_test_creator --save
#    -> vcb__close_adjust_5day__final__d20_h5__tr70_val15_test15__std
#       train 2,939 | val 615 | test 640   x 20 x 35

# 7 — validate the config without training anything
python -m model.lstm --dry-run

# 7 — train for real
python -m model.lstm
#    or open model/lstm/RUN__lstm.ipynb and run all

# 8 — score it against a block-shuffled null
python -m result_evaluator
```

Then `python -m pipeline` again: every row should read `up to date`.

---

## 6. Changing the target — the one place with a trap

**Two ways, and they mean different things:**

```powershell
# (a) one-off: leave the chain's default alone, name the table explicitly
python -m train_test_creator --table return_5day__final__d20_h5 --save
python -m model.lstm --config configs/lstm__vcb__return_5day__final__d20_h5.yaml

# (b) move the whole chain: edit DEFAULT_TARGET in src/utils/chain.py, once
```

⚠️ **YOU CANNOT REUSE `pool__shortlist__close_adjust_5day__d20_h5` FOR ANOTHER TARGET.**
That pool is **target-conditioned** — its 644 channels were kept *using* the
`close_adjust_5day` label at `d=20, h=5`. Selecting over it for `return_5day` is leakage.
A new target restarts at stage 2:

```powershell
# stage 2, once per pool — the layer-1 sweep
python -m feature_selection.run --ticker VCB --pools pool__basic --target return_5day `
    --lookback 20 --horizon 5 --null-draws 10 --device cuda
# ... repeat per macro pool, then:
python -m final_features --apply --shape shortlist --scope basic
python -m feature_selection.run --pools pool__shortlist__return_5day__d20_h5 --null-draws 10
python -m final_features --apply
```

⚠️ **Pass `--scope` when two experiments would land on one table name.** `final_features`
groups on `(schema, target, setup)` — there is **no term for which pools** — so a
`pool__basic`-only run and a `basic + economy_x` run are ONE group and get unioned into
one table. `--scope basic` names it `…__d20_h5__basic` instead.

---

## 7. The LSTM notebook

`model/lstm/RUN__lstm.ipynb` — open, **Run All**. It trains the same `train()` the CLI
does, so the notebook and `python -m model.lstm` are the same run.

Retarget it by editing **the parameter cell (cell 4)** and nothing else:

```python
TICKER = "vcb"
TARGET = "close_adjust_5day"
D, H   = 20, 5
MODEL  = "lstm"
```

The config path, dataset folder and run name all derive from those four names, and the
cell **asserts the config file exists** and lists what is present if it does not.

⚠️ **Rewrite that cell in place; never add an override cell below it.** The same mistake
in the feature-selection notebook fed a run its own label as a feature and reported an IC
near 1 without raising.

---

## 7a. Running on a Kaggle T4 — when the local card is too small

```powershell
cd src\kaggle_gpu
python -m kgpu plan     feature-selection   # what would run; touches nothing
python -m kgpu data     feature-selection   # DB -> parquet -> private dataset
python -m kgpu rehearse feature-selection   # the WORKER side, locally, NO QUOTA
python -m kgpu run      feature-selection   # push, wait, download, merge
```

**Iterate in `rehearse`, not in `run`.** Measured 2026-08-17 on the `smoke` job: 8m 15s end
to end, of which **5.2 min was QUEUED**. ⚠️ **The queue is the floor, not the compute** — a
90-second job still costs ~7 minutes, so batch one large run rather than several small ones.

| | this machine | Kaggle T4 |
|---|---|---|
| VRAM | **4.0 GiB** | **14.6 GiB** |
| RAM free | ~7 GB | ~29 GB |
| quota | — | 30 GPU-h/week |

⚠️ **THE WIDTH CEILING WAS RE-MEASURED 2026-08-21 AND IT MOVED TWICE.** Three memory fixes
shipped (all verified exact at `0.000e+00`): `gpu.tree_shap` blocks its `pred_contribs`
call over ROWS, `UnifiedSchemaReader.read` filters tickers in SQL, and `window_design` /
`panel_window_design` stopped materialising the design three times over. **VRAM is no
longer the wall** — it held at 6.1 of 14.9 GB across four attempts. What binds now is HOST
RAM inside the ranker ensemble, and the measured state is:

| width | outcome |
|---|---|
| 120 channels | ✅ completed (`PRF-9`, before any of the fixes) |
| **162 channels** | ✅ **completed 2026-08-21, 44m 12s** — the current known-good |
| 233 channels | ❌ four attempts, `DeadKernelError` in the ranker phase every time |

⚠️ **Read the phase profile, not the outcome.** `selector._tick` prints `rss=` and `peak=`
per phase; `peak` is the number that decides whether a run survives, and on attempt 3 `rss`
ROSE while `peak` held — a fix that looked like a regression on the wrong column.

⚠️ **The historical note, kept because the reasoning is the evidence:**

⚠️ **THE WIDTH CEILING ON A T4 IS ~120 CHANNELS, AND IT IS VRAM — `VRM-1`.** Measured
2026-08-19: at **140 channels** over 624 k rows the host peaked at **24.5 GB and survived**,
while XGBoost died in `XGBoosterPredictFromDMatrix` (*free 3.00 GB, requested 3.15 GB*).
The allocation is `xgb_shap`'s SHAP contributions, `(n_rows, channels × 6 + 1)` — linear in
the width. **120 channels COMPLETE in 32.6 min; 140 fails.** ⚠️ `selector._tick` cannot see
it: it reports torch's VRAM (6.2 GB) while XGBoost allocates through its own CUDA allocator.
Host RAM, for reference: `peak_GB ≈ 1.54 + 0.164 × channels`, fitted on two measured points.

⚠️ **The status poller survives a network blip now, and it did not before.** A six-hour run
lost its watcher at **355 min** to one `ConnectionError` from `api.kaggle.com` while the
kernel was still RUNNING — so nothing pulled the results. `_fetch_status` retries a NETWORK
failure for 30 min but never retries an ANSWER (a `ValueError` means the kernel is missing;
retrying repeats a wrong request). If it does give up, the kernel is probably still alive:

```powershell
python -m kgpu status <job>              # is it still RUNNING?
python -m kgpu wait <job>                # resume the watch
python -m kgpu pull <job>                # then fetch and merge
```

⚠️ **ADDING A TOP-LEVEL IMPORT TO A SHIPPED MODULE CAN KILL THE WORKER, AND `rehearse`
IS THE ONLY THING THAT CATCHES IT — measured again 2026-08-21.** The payload ships
`src/feature_selection` and `src/utils`; `kgpu_bootstrap` **stubs** `dtos` for the single
connection DTO it needs. A new `from dtos…tabular_database_driver_dtos import Condition`
at the top of `unified_reader.py` imports perfectly here and dies on the worker at cell 0
with *"'dtos.tabular_database_driver_dtos' is not a package"* — **1m 29s into a
233-channel run that had nothing else wrong**, and the quota was already spent.

**The rule this leaves:** an import a worker cannot satisfy belongs INSIDE the branch that
needs it. A ticker filter needs a database; a worker has none; so the import goes where the
database does. And **`kgpu export` + `kgpu rehearse` before `kgpu run`, every time a
shipped module changed** — `export` restages the payload without uploading, `rehearse`
drives the worker path locally, and together they cost about four minutes against a wasted
kernel launch. This is `KGP-1` a second time, from the other direction: that one was a
stub SHADOWING a shipped package, this one is a shipped package reaching for something
never shipped.

⚠️ **A Kaggle run is a different PROCEDURE, not the same run on a faster card.** Its image
ships **xgboost 3.2.0 / sklearn 1.6.1 / numpy 2.0.2** against `mt_env`'s **2.1.1 / 1.7.2 /
2.2.6**, and XGBoost subsamples from a different RNG stream per device. Since 2026-08-17
`contract.SETUP_KEYS` carries `env_fingerprint`, so the two **cannot be unioned into one
table by accident** — expect a group collision instead, and pass `--scope`.

### A CROSS-SECTIONAL target — the `cross-sectional` job (panel mode, 2026-08-17)

```powershell
cd src\kaggle_gpu
python -m kgpu export   cross-sectional   # DB -> ONE panel.parquet   measured 2m 04s / 477 MB
python -m kgpu rehearse cross-sectional   # both mount layouts        measured 16.0 s
python -m kgpu data     cross-sectional   # the same export, then UPLOAD it
python -m kgpu run      cross-sectional
```

⚠️ **The join happens HERE, not on the worker, and that is structural.**
`read_universe_panel` is one hand-written SQL statement reaching for `reader.driver`, so
`ParquetSchemaReader` answers *"there is no database on a Kaggle worker"* whatever
parameters it is given (`CSP-1` in its second form). The worker gets a finished panel with
`cs_rank` already derived — CLAUDE.md §3d-bis.

⚠️ **`data.panel.liquidity_before` is REQUIRED and has no default.** Ranking turnover over
the whole sample picks the names that *turned out* to be liquid — the same look-ahead §2c
records for non-point-in-time index membership — so the exporter raises rather than
defaulting silently. ⚠️ The shipped `cs_rank` is a rank **within the shipped names**.

⚠️ **`rehearse` does NOT run the notebook's own cells** — it drives cell 0 and then
re-creates the panel path itself. To exercise the notebook, point `PANEL_PAYLOAD` at a
staged `.payload/<job>/` folder and run it locally; on a full 1.25 M-row panel that needs
more than this machine's 4 GiB card, so cut the payload down first and treat the result as
a smoke test, never a number.

---

## 7b. Re-reading a FINISHED track without re-running it — measured 2026-08-21

⚠️ **The pooled row is printed, never stored.** `walkforward.evaluate` writes `per_fold.csv`
(the fold series) but the POOLED line — the Sharpe, CAGR and `se_sharpe` every register
quotes — only ever went to stdout. So *"check the number against the artefact"* needs a
re-run, and the re-run is cheap **only if you drop the null**:

```powershell
# the pooled row alone, from the predictions already on disk. NO GPU, NO training.
python -m walkforward.evaluate --top-k 20 --draws 0 --universe all `
    --out ../results/walkforward          # h=20 — ~2 min
python -m walkforward.evaluate --top-k 20 --draws 0 --universe all `
    --out ../results/walkforward_h10      # h=10 — ~2 min
```

⚠️ **`--draws 200` is what costs the ~9 minutes**, not the scoring. Drop it when you are
re-reading a level and keep it when you are claiming the level clears a bar.
⚠️ **`--horizon` is no longer needed** — it is derived from the track (`WFO-1`, §3).
⚠️ **It REWRITES `per_fold.csv`** with identical content; harmless, but it means the file's
mtime is not evidence of when the sweep ran. `folds.csv` and `manifest.json` are.

⚠️ **AND IT IS NOT HARMLESS AT A DIFFERENT `--top-k`** (measured 2026-08-22). The rewrite
keys on the basename, not on `k`, so `--top-k 5` **overwrites the published k=20 fold table**
with a k=5 one and nothing says so. To score a finished track at another `k`, copy the two
inputs somewhere else and point `--out` at the copy — `manifest.horizon_for` recovers the
horizon from `folds.csv`, so the copy scores identically:

```powershell
mkdir $env:TEMP\wf_k5
copy ..\results\walkforward_h10\predictions_oos.csv $env:TEMP\wf_k5\
copy ..\results\walkforward_h10\folds.csv           $env:TEMP\wf_k5\
python -m walkforward.evaluate --top-k 5 --draws 200 --universe all --out $env:TEMP\wf_k5
```

✅ Verified the same day: scoring the copy at k=20 reproduces CAGR@30 **0.7398** / Sharpe
**2.5310** / 236 periods, and the published `per_fold.csv` md5 was unchanged afterwards.

**Verified this way on 2026-08-21** — both tracks reproduce CLAUDE.md §6-0 to every digit
(h=20: `ic` 0.1097, `ic_t` 6.8956, `sharpe@30` 1.9913, `cagr@30` 0.4753, 118 periods,
`se_sharpe` 0.1553). That is `RUNBOOK` §8 rule 1's spirit applied to a result rather than to
a stage.

### 7c. ⚠️ SHARPE AND CAGR RANK THE ARMS DIFFERENTLY — say which one you are quoting

The h=10 pooled table, read off disk the same day, at 30 bps over 236 periods:

| arm | `sharpe@30` | `cagr@30` |
|---|---|---|
| **`gbt`** | **+2.891** ← best | +69.8 % |
| `tcn` | +2.622 | +73.4 % |
| `transformer` | +2.622 | +72.9 % |
| **`lstm`** | +2.531 | **+74.0 %** ← best |

**The highest-Sharpe arm is not the highest-CAGR arm**, and the gap is not rounding:
`gbt` earns **4.2 pp/yr less** while scoring **0.36 more** Sharpe. It is the same fact
`P1-9` separated into two columns (§8 rule 8) — a *lower mean return at lower volatility* —
and it is visible in the pooled levels, not only in the paired test.

⚠️ **So "the best model" is not a well-formed question here without an estimand.** Name it:
*best risk-adjusted* is `gbt`, *best return* is `lstm`, and the paired test says neither
advantage survives a correction for the six arms that were tried.

---

## 8. Before you quote any number this chain produces

1. **`python -m pipeline` must show `up to date` for every stage below the one you are
   quoting.** A green model run on a stale table is a number about a table that no longer
   exists.
2. **Read `dataset.meta["evidence"]`**, printed by the notebook and by
   `train_test_creator`. It is generated from the source table's own `COMMENT`, so it
   cannot go stale. Today it reads:
   > `failed_null=1` — ⚠️ that run was measured against a shuffled-label null AND DID NOT
   > CLEAR IT.
3. **`close_adjust_5day` is a price LEVEL.** `hit_rate` and `dir_accuracy` are 1.0 by
   construction (every label is positive) and R² goes deeply negative. **`ic` against its
   null is the only readable metric on this target.**
4. **A cleared bar is not a result.** The model-stage null shuffles the outcome against a
   fixed score vector; it prices in neither the feature selection nor the architecture
   search (`NUL-1`). *A run that fails it is dead; a run that clears it is not yet alive.*
5. **On a PANEL (`bank`, `vn30`, …), quote the daily-IC t-stat, never `ic_clears`** — the
   evaluator's panel null is not label-neutral (`NUL-3`).
6. ✅ **`ICT-1` FIXED 2026-08-18 — that t-stat is now honest, and you no longer compute
   it by hand.** `metrics.csv`'s `ic_t` divided by `n_dates` rather than
   `n_eff = n_dates/h`, overstating it by **exactly `√h`** (15.50 reported against +3.47
   at h=20). `panel_core_metrics` takes the horizon now and `evaluate_panel` passes it.
   ⚠️ **A run folder scored BEFORE that date still carries the old number until it is
   re-scored, and re-scoring takes TWO commands:**

   ```powershell
   python -m result_evaluator --rescore        # rewrites each run FOLDER   41.6 s
   python -m result_evaluator --rebuild-index  # rewrites index.csv         42.7 s
   ```

   ⚠️ **`--rescore` alone does NOT touch `index.csv`** — measured the same day: the
   folder read +3.47 while the leaderboard still read 15.50. Neither needs a GPU.
   ⚠️ Only PANEL runs move; a single-series run's `ic_t` comes from `_ic_uncertainty`,
   which had `n_eff` right all along (the two VCB runs are unchanged at 5.50 / 0.96).
7. ⚠️ **`mase` DOES NOT EXIST ON A PANEL — do not read a blank as a pass.** Block B is
   computed in `metrics.evaluate` only, so `evaluate_panel` returns no `mase`, `rmsse`,
   `skill_score` or `beats_naive`, and `test_mase` is **NaN** for every cross-sectional
   run. Rule 4 above and P2-3 both say `mase ≥ 1` is the line to quote; on a panel that
   line is simply **not measured yet** (TODO **P4-12**).
8. ✅ **`P1-9` FIXED 2026-08-21 — `walkforward.compare` NOW REPORTS BOTH ESTIMANDS, AND
   THEY ARE NOT SUMMARIES OF EACH OTHER.** The column formerly called `t_paired` is now
   **`t_ret`** (a test of the mean period-RETURN gap) and **`d_sharpe` carries its own
   `sh_ci_lo` / `sh_ci_hi` / `p_sharpe`** from a paired circular block bootstrap. They
   disagreed about **three of six arms** on the h=10 sweep: `bilstm` and `cnnlstm` lose on
   return (`t_ret` −2.09 / −2.15) and TIE on Sharpe (p = 0.61 / 0.30), while `gbt` gains
   +0.36 Sharpe at p = 0.044 against `t_ret` = −1.02.
   ⚠️ **Read `p_sharpe` against the number of ARMS, not against 0.05.** Six challengers
   against one reference is Bonferroni **0.0083**; `cnn` (0.001) clears it and `gbt`
   (0.044) does not. `NUL-1` one level up.
   ⚠️ **`ac1` is printed per row** — the lag-1 autocorrelation of the difference series,
   which is what `--block` has to cover. It ran −0.09…+0.06 on that sweep, so the default
   `block=2` was doing no hidden work. If it comes back large, raise `--block`.
   ✅ **The h=20 `PRF-8` sweep WAS re-scored 2026-08-21** — **1m 29s**, and its ties hold
   on BOTH estimands (`lstm_small` `p_sharpe` 0.903, `gbt` 0.941). It reproduced the
   published pooled row to every digit, which is what licensed reading the new column.
   ⚠️ **`gbt`'s h=10 advantage does NOT reproduce there**: `d_sharpe` **+0.360** at h=10
   against **−0.016** at h=20. Two estimates disagreeing in sign across a neighbouring
   horizon are what a null effect looks like — `walkforward/CONTEXT.md` §8a-ter.
9. **A rank target's `long_short` is NOT money.** The metric is documented "in return
   units", which holds when the label is a return; on `cs_rank_*` it is a spread of
   RANKS. The 2026-08-18 run's `+0.0635` is not 6.35 %.

10. ⚠️ **AN ARM GAP BELOW `|d_sharpe| ~ 0.09` IS A RESEED, NOT A RESULT** (measured
    2026-08-21, `walkforward/CONTEXT.md` §15). Every config in the repo is `seed: 42`, so
    every arm row is one fit per fold. Five `gbt` seeds over the identical h=10 folds gave
    pooled Sharpe@30 **2.845 … 2.979, sd 0.054**, max paired `d_sharpe` **0.088**. At h=10
    that leaves `cnn` (4.5x) and `gbt` (4.1x) above the floor and puts `transformer`,
    `tcn` and `bilstm` **at or inside** it.
    ⚠️ **One architecture, and the cheapest one.** `gbt` resamples rows and columns; a net
    also varies its initialisation and batch order, so the `lstm` floor is unmeasured and
    could be larger.

11. ⚠️ **NEVER COMPARE TWO ARMS IN ONE FOLD.** The same sweep measured a per-fold Sharpe
    as **4.4x more seed-sensitive** than the pooled one — mean per-fold range **0.593**
    against **0.134** pooled, worst fold **1.079**. The pooled row is the quotable one, and
    that convention is now measured rather than assumed. ✅ The DECAY across folds survives
    (slope -0.308 +/- 0.027 over five seeds), so §9a's finding is not a seed artefact.

12. ⚠️ **WHEN TWO ARMS DIFFER, CHECK THE IC BEFORE BELIEVING THE SHARPE.** Pooled IC moves
    **0.5 %** across seeds against Sharpe's **1.9 %** — `long_only_top_k` is a THRESHOLD
    (top 20 of ~150), so a hair's change in the ranking swaps which names are held and the
    portfolio inherits a discretisation the IC never sees.

13. ⚠️ **A FIVE-ARM SWEEP IS NOT FIVE TRACKS.** `--arm` builds each fold's tensors ONCE and
    every arm trains off that build, so five `gbt` arms cost **13m 16s**, not the ~1 h 40 m
    that `5 x 20 min` predicts. Cost an arm sweep from the fold BUILD, not from the fit.

14. ⚠️ **`--top-k` IS THE CAP ON HOW MANY STOCKS MAY BE BOUGHT, AND LOWERING IT BUYS LESS
    TRADABLE NAMES** (measured 2026-08-22, `pipeline.md` §9). CAGR@30 rises monotonically as
    `k` falls — +74.0 % (k=20) → +181.6 % (k=5) → +217.9 % (k=3) — but so does the
    concentration into names nobody can buy: the median matched turnover of a picked row goes
    0.30 bn (k=20) → **0.03 bn** (k=5), and the share of picks under 0.1 bn/day goes 38.6 % →
    **61.4 %**. **Quote `k` beside any CAGR from this chain.**
    ⚠️ **And at k=5 the width guard stops protecting the track.** `long_only_top_k` skips a
    date on `len(day) < k`; at k=20 that excluded the three frozen 2026 books, at k=5 it does
    not (7 ≥ 5) and the track becomes **239 periods instead of 236**. Worth ~0.35 pp of CAGR
    here — and on a LIVE book it would print five bank tickers off a seven-name panel.

15. ⚠️ **UNDER A TRADABILITY GATE THE LEVELS COLLAPSE, AND `k=20` BEATS `k=5` ON EVIDENCE.**
    Gating on trailing 60-session median `value_matched` (`shift(1)`, no look-ahead) takes
    h=10 k=5 from **+181.3 %** to **+36.5 %** (ADV ≥ 1 bn) and **+19.9 %** (ADV ≥ 5 bn); daily
    IC falls 0.1412 → 0.0816 → 0.0667 and max drawdown WORSENS to −50.7 % / −64.0 %.
    ✅ All eight gated cells still clear a 200-draw within-date null with MAX below observed —
    but **`z` is higher at k=20 than at k=5 in every one of them** (h=10, ADV ≥ 1 bn: +7.64 vs
    +5.23), and at ADV ≥ 5 bn the two CAGRs are within a point. **On a buyable basket, cutting
    `k` weakens the evidence and adds almost nothing.** ⚠️ The gated null MEAN is +0.22…+0.34,
    not zero. ⚠️ This is a post-hoc filter on a model trained over all 150 names; the screened
    chain in `plan.md` is the real test.

16. ⚠️ **CHECK THAT THE IC DECAYS WITH THE HORIZON BEFORE BELIEVING ANY CAGR LADDER.** The
    h=10 model's IC is **FLAT from h=1 to h=30** (+0.1403 … +0.1328) when it should peak at
    its own label. With a constant IC, `CAGR ∝ 1/√h` is arithmetic, not skill — which is why
    the same predictions "earn" **+1416.9 %/yr** rebalanced daily. The cause is measured:
    **51.2 %** of rows with ADV60 < 0.1 bn have a forward 1-day return of **exactly zero**, and
    a frozen price ranks the same at every horizon. ✅ Under the ADV ≥ 1 bn gate the ladder
    flattens (h=1 +44.4 % vs h=10 +36.5 %). `pipeline.md` §9e.

---

## 8a. ⚠️ CURRENT STATE — 2026-08-21: the chain CANNOT emit a pick list

**Measured that day while documenting the output** (`pipeline.md`), and it is the first
thing to check before running anything that reads `pool__basic`:

| rebalance date | names scored & buyable |
|---|---|
| 2026-05-27 | 143 of 150 |
| **2026-06-10** | **147** ← the last real book |
| 2026-06-24 | **7** |
| 2026-07-08 | **7** |
| 2026-07-22 | **7** |

⚠️ **`MAX(date)` on `pool__basic` reports 2026-08-07 and conceals this completely**, which
is §8 rule 1's mechanism at the row level. Mean names scored per session runs **145-147 for
2017-2025** and **113.3 in 2026** — the annual average hides a cliff.

⚠️ **CORRECTED 2026-08-22 — THE 2026-06-11 CLIFF IS IN THE LABEL, NOT IN THE PRICE.** This
file used to say *"after 2026-06-11 only 7 of 150 names carry data"*. The date is right and
the mechanism was wrong: **all 150 names carry a close through 2026-06-25** (98 on 06-26, then
7 from 06-29). What ends on 2026-06-11 is `return_10day`, because it needs a close ten sessions
later. Last session with ≥100 names LABELLED: **2026-06-18** at h=5, **2026-06-11** at h=10,
**2026-05-28** at h=20. So the horizon decides where a track ends — and a live-scoring module
(`P10`) has about ten more sessions of usable FEATURES than the old sentence implied, because
**ranking a book and scoring a book fail on different dates**. `pipeline.md` §6.1-bis.

✅ **The published +74 %/yr is NOT affected.** `long_only_top_k` does
`if len(day) < k: continue`, so those three stub dates were skipped: **239 rebalance dates
− 3 = 236 periods**, exactly the `n_periods` on the artefact.

⚠️ **But the skip is SILENT — a data freeze SHORTENS the track instead of failing it.**
Check the width per rebalance date before reading any recent book:

```powershell
# names scored per rebalance date, straight off the track
python -c "import pandas as pd; d=pd.read_csv('../results/walkforward_h10/predictions_oos.csv'); print(d.groupby('date').size().tail(40))"
```

**To get back to a usable chain**: re-scrape the 143 frozen tickers (`--rescrape` is
opt-in and scoped to `--ticker` with `skip_existing=False` — **without both it either costs
781 tickers or fetches nothing**), rebuild `pool__basic` → `rank_10day__final__d20_h10`,
then re-run the sweep. TODO **`P1`**.

⚠️ **And there is still no LIVE-SCORING path** — every stage writes predictions for a
dataset's *test split*, so the chain cannot score today's cross-section even with fresh
data. TODO **`P10`**. `pipeline.md` §6 has both.

---

## 8b. Reading the output as a PICK LIST — three rules

`pipeline.md` is the full statistics; these are the three that change how a book is read.

1. ⚠️ **ONE BOOK IS CLOSE TO A COIN FLIP.** Over 236 books the mean realised rank is
   **+0.0688** and **81.8 %** are positive (`t` = +14.0), but individual picks land in the
   top half only **60.2 %** of the time and **43 of 236 books were negative**. The
   +74 %/yr is a 60/40 edge compounding over 236 periods — never a single book.
2. ⚠️ **THE MODEL OVER-PICKS THE BOARDS YOU CAN LEAST TRADE.** UPCOM is **2.20×** its share
   of scored rows, HNX 1.31×, HOSE **0.76×**. The most-selected names are `DCT` (108 of 236
   books), `DCS` (106), `EFI` (87); `VCB` appears in 30, `HPG` and `VNM` in 12. **With no
   ADV cap and no slippage modelled (`P14`), this is the biggest open threat to the
   levels.**
3. **Turnover is 65.1 % per rebalance** (median 65.0 %, range 20-90 %) → **8.2 %/yr** at
   50 bps. ✅ That confirms `backtest/CONTEXT.md` §3's assumed `τ = 0.70` from the data.

---

## 8c. ⚠️ BEFORE YOU COMMIT — record the state

**One command, and it takes about two seconds:**

```powershell
python docs/state_check.py
```

⚠️ **It REPORTS and never rewrites.** Every finding is handed back as a decision, because
the numbers here cannot be derived mechanically without getting them wrong — `ISSUES.md`
keeps four FIXED rows struck-through *inside* its Open table on purpose, so a naive
row-counter reports **17** where the truth is **16**. A confidently wrong number is worse
than no number, since it is what the next session budgets against.

| the check | what a failure means, and where the fix goes |
|---|---|
| **`CLAUDE.md` §6 date** | `## 6. State today (YYYY-MM-DD)` is older than the `.md` files in this commit. If the commit changes what the project KNOWS, bump the date and write the measurement in. If it is a typo fix, ignore the row |
| **`CONTEXT.md` ↔ `CLAUDE.md`** | a package `CONTEXT.md` changed and `CLAUDE.md` did not. ⚠️ **A measurement that never reaches the hub is invisible** — every session loads `CLAUDE.md`, almost none open a given `CONTEXT.md`. Decide deliberately; "the detail stays local" is a valid answer |
| **issue counts** | `CLAUDE.md`'s *"N open, M resolved"* disagrees with `ISSUES.md`'s own `## Open (n)` headings. Re-SCAN the tables — do not decrement |
| **`INDEX.md` completeness** | a `.md` file exists that `docs/INDEX.md` does not route. **A file missing from the index is a file no session knows exists.** Add it to a tier |
| **`INDEX.md` token costs** | a claimed cost drifted >20 % from measured. ⚠️ **This check exists because all 16 of `CLAUDE.md` §7's costs had gone stale** — `walkforward` was listed at 6k against a true 16.0k |
| **relative links** | a markdown link points at nothing. ⚠️ **11 are known-broken and pre-date the docs move** (stale `#L` anchors in `NULL_DRAWS.md` and two `CONTEXT.md` files) — the row is a watermark, so read the count, not just the colour |

**Where each kind of change is recorded** — this is the whole convention in one table:

| you changed | it goes in |
|---|---|
| a new measurement, or one that moves a verdict | `CLAUDE.md` §6 (+ bump the date), or the package's `CONTEXT.md` |
| a new defect | `docs/ISSUES.md`, with a **permanent** code — never renumbered, never reused |
| a finished backlog item | its number moves to `CLAUDE.md` / `CONTEXT.md`; the item is **deleted from `docs/TODO.md`, not ticked** |
| a new `.md` file | a row in `docs/INDEX.md` (`python docs/check_index.py` fails without one) |
| a new command or stage | this file |

⚠️ **NOTHING ENFORCES THIS AT COMMIT TIME, AND THAT IS A CHOICE** (2026-08-22). There is no
git hook and no `PreToolUse` hook: a hook that blocks an unrelated commit costs more than
the drift it prevents, and `--no-verify` would train the reflex to bypass it. Running the
command is the discipline. `state_check.py` exits **1** on drift, so it *can* be wired into
`.git/hooks/pre-commit` later if that trade changes.

---

## 9. Traps that have actually bitten, and the fix

| symptom | cause | fix |
|---|---|---|
| `ModuleNotFoundError: pipeline` | run from repo root | `cd src` |
| `does not exist. Available: [...]` from `train_test_creator` | ⚠️ **fixed 2026-08-16** — the stage defaulted to a different target than `pipeline` planned | pass `--table`, or edit `utils/chain.py` |
| `no config at ...configs\vcb__...yaml` | the `lstm__` prefix was missing from the notebook's default | ⚠️ **fixed 2026-08-16**; the cell now asserts and lists |
| `UnicodeEncodeError` at the end of a long run | Windows console is cp1252 and the text has `⚠️` | already handled in the stages; in **your own** scripts call `sys.stdout.reconfigure(errors="replace")` |
| `Password cannot be empty` in an ad-hoc script | scratchpad scripts are outside the repo, so `find_dotenv()` misses `.env` | `load_dotenv(os.path.abspath(".env"), override=True)` with cwd = repo root |
| a scrape goes green in 500 ms | `skip_existing=True` — `landed()` asks "is the folder empty?", not "did this run fetch anything?" | check the **per-series max date**, never the asset colour |
| a rebuilt pool silently loses years of rows | sibling pools on an older calendar, and the join is INNER | `pipeline`'s `data` row reports `pools_behind`; rebuild the siblings first |
| a run folder is skipped by `final_features` with no message | it has no `outstanding.csv` | `plan_from_reports` ignores such folders — check the folder has one |
| `final_features` raises *"two different setups both want …"* and plans NOTHING | a PROBE run is sitting in the chain's report root | move it to `reports/feature_selection_probes/` — §3b. ⚠️ `--scope` does **not** fix this |
| a rebuild quietly gains channels nobody selected | same cause, silent half: the probe shares setup keys, so it is UNIONED rather than colliding | §3b, and check the table's `obj_description` for its `Source runs:` |
| a Kaggle job "finishes" but no run folder appears | the local WATCHER died on a network error; the kernel is probably still running | `kgpu status <job>`, then `kgpu wait` + `kgpu pull` — §7a |
| `XGBoostError: … cudaErrorMemoryAllocation` mid-selection | more than ~120 channels on a T4 — the ceiling is VRAM, not host RAM (`VRM-1`) | prune the pool first: `python -m feature_selection.prune` — §7a |
| a Kaggle kernel ends `ERROR` with `DeadKernelError: Kernel died` and no traceback | **OOM-kill.** A Python exception leaves a traceback; a dead kernel was terminated by the OS | `kgpu logs <job>` and read the PHASE lines — `rss=` says where it settled, `peak=` says what killed it, and the two disagree |
| a memory fix lands and `peak` does not move | you fixed a real allocation that was not the BINDING one | measured 2026-08-21: row-blocking `window_design` moved a 23.3 GB cube and the peak by **0.1 GB**, because the panel path allocates in `panel_window_design` instead. ⚠️ `rss` went UP while `peak` held — **read `peak`, never `rss` alone** |
| `walkforward.compare` raises *"arm X covers N rows against the reference arm's M"* | the two tracks do not span the same panel — a wider pool changes coverage, a different `rank_min_width` changes the label | **the refusal is correct.** Price them on the INTERSECTION (`backtest/CONTEXT.md`, and §3's wide-vs-narrow recipe), never by comparing two unpaired Sharpes |
| `final_features --scope X` plans a table you did not ask for | ⚠️ **a scope names EVERY table in the plan**, and a report root holding two experiments plans both | give the second experiment its own `--root` — **and add its `.gitignore` negation pair in the same commit**, or its CSVs are silently dropped |
| a long background run's log holds only the banner, then the process is gone | **stdout buffering** — CLAUDE.md §5 rule 20. Redirecting to a file re-buffers, so nothing lands until the flush at exit, and an interim `tail` reads as a crash | run it as `python -u -m ...`, and read the exit status before concluding it died. Measured 2026-08-21: a 200-draw null that looked dead had in fact finished in 8m 40s |
| a scratchpad script raises `KeyError: 'exchange'` inside `portfolio.mark_ceiling` | the `pool__basic` read dropped a column the screen needs | `mark_ceiling` needs `date, exchange, ticker, close_adjust` plus a `day_ret`; select all four |
| `Unknown top-level keys in kaggle_config.json` | a comment or note added at the top level; the schema is closed | put prose in `kaggle_gpu/README.md`, not in the config |

---

## 10. What is NOT standardized yet

Honest list, so you do not go looking for a switch that is not there:

- **Stages 2 and 4 stay manual by design.** There is no `--apply` that will spend an hour
  of GPU for you.
- **The cost estimator is wrong.** Both models in the repo (`utils`-side
  `0.364 × ch^0.77` in `feature_selection/CONTEXT.md` §15c, and the Dagster guard's
  `1.1 × (ch/113)² × (1+draws)`) were fitted with `lasso` in the ensemble, which was
  dropped 2026-08-16. The guard over-predicts by **4–13×** — it predicted 393 min for a
  run that took **29m 44s** — so `budget_minutes` now raises on runs you can afford.
  Until it is re-fitted, treat the raise as advisory and read §3's measured column.
- ~~**`hit_rate` is not withdrawn on level targets.**~~ ✅ **Fixed 2026-08-17** (TODO
  P0-2). `selector.py` calls `evaluation.sign_hit_rate`, which returns NaN when every
  non-zero label shares a sign, and `report.py`'s formatter now tests `v != v` so the
  README prints **`—`** rather than a bare `nan`. Verified end to end on a fresh
  level-target run. A `—` in that cell is the deliberate absence, not a defect.
- **The 19 archived country runs group as `methods="unrecorded"`.** They still plan and
  still validate, but they will not union with a new three-ranker run — that is `MTH-1`
  working, not a bug. Pass `--scope` if you hit the collision.
