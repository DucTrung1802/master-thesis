# RUNBOOK — how to run this chain

> Written 2026-08-16, extended 2026-08-19 (`PRF-1`/`PRF-2`/`PRF-8`/`PRF-9` and the
> tools they needed). Every runtime below was **measured**, not estimated — see §8 for
> why that distinction matters here.
>
> ### The four root registers — one job each, no overlap
>
> | file | answers | when you touch it |
> |---|---|---|
> | **[CLAUDE.md](CLAUDE.md)** | *what is this, and what has it PROVED?* | auto-loaded every session; the map and the verdict |
> | **[RUNBOOK.md](RUNBOOK.md)** | *how do I RUN it?* | commands, stage order, the flags that destroy things |
> | **[ISSUES.md](ISSUES.md)** | *what is BROKEN?* | permanent codes; a code is never renumbered or reused |
> | **[TODO.md](TODO.md)** | *what is NEXT?* | priority-ordered, every item with a measured cost |
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
place: [src/utils/chain.py](src/utils/chain.py). Every stage derives from there.

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
| **walk-forward** | `python -m walkforward --ticker all --table <T> --config <C> --first-test 2017-01-01` then `python -m walkforward.evaluate --top-k 20 --draws 200 --horizon <h> --universe all` | *is this one lucky split?* 10 expanding folds, one OOS track | **~35 min**, 10 folds |
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

⚠️ **`walkforward` WRITES TO ONE DEFAULT DIRECTORY AND WILL OVERWRITE THE LAST SWEEP.**
`DEFAULT_OUT` is `results/walkforward/`, and `folds.csv` / `per_fold.csv` /
`predictions_oos.csv` are all written by basename — so the row above, run at a second
horizon, **silently destroys the first**. One experiment, one `--out`:

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

⚠️ **`--horizon` on `evaluate` is NOT cosmetic** — it sets the holding interval the periods
are cut at and the `return_{h}day` column that is scored. Passing the default 20 against an
h=10 track silently scores the wrong label.

⚠️ **`walkforward.compare` and `backtest.head2head` PAIR the difference, and that is not a
nicety.** Every arm trades the same dates out of the same panel, so their period returns
correlate at **ρ 0.74-0.90** and `se_sharpe` ≈ 0.16-0.25 is the error bar on the wrong
quantity — unpaired, it cannot resolve the gaps these tools exist to measure. CLAUDE.md §5c
is the cautionary case.

⚠️ **Stages 2 and 4 are MANUAL.** `python -m pipeline --apply` stops before each of them
and prints `MANUAL — cannot be produced here`, because a selection run is the expensive
artefact and must be a deliberate act. Everything else `--apply` will do for you.

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

⚠️ **DO NOT use `python -m pipeline --apply` for this chain.** Its `shortlist_pool` row
says *"would run"* and `--apply` would build a `pool__shortlist__rank_20day__d20_h20` that
**nothing can ever select over** — a cross-sectional selection reads `pool__basic ⋈
pool__targets` only (`CSP-1`), so there is no layer 2 and stages 3-4 do not exist here.
Its `selection_2` row also reports another chain's runs as `up to date` (`P4-11`).

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
8. ⚠️ **`walkforward.compare`'s `t_paired` TESTS THE MEAN RETURN, NOT the `d_sharpe`
   printed beside it** (`compare.py:110`). They can disagree in sign — the h=10 sweep's
   `gbt` arm shows `d_sharpe` **+0.36** against `t` **−1.02**, i.e. a lower mean return at
   lower volatility. **Quote `t_paired` as a return test and leave `d_sharpe`
   unqualified** until **P1-9** ships. `walkforward.pair` already bootstraps both
   estimands and is the model for the fix.
9. **A rank target's `long_short` is NOT money.** The metric is documented "in return
   units", which holds when the label is a return; on `cs_rank_*` it is a spread of
   RANKS. The 2026-08-18 run's `+0.0635` is not 6.35 %.

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
