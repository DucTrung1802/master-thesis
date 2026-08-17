# RUNBOOK — how to run this chain

> Written 2026-08-16 against commit `af348b78`+. Every runtime below was **measured on
> this machine today**, not estimated — see §8 for why that distinction matters here.
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
| 8 | result_evaluator | `python -m result_evaluator` | `results/metrics.json`, `runs/index.csv` | seconds |

⚠️ **Stages 2 and 4 are MANUAL.** `python -m pipeline --apply` stops before each of them
and prints `MANUAL — cannot be produced here`, because a selection run is the expensive
artefact and must be a deliberate act. Everything else `--apply` will do for you.

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
- **`hit_rate` is not withdrawn on level targets.** CLAUDE.md §5 rule 21 says it is; the
  code still computes a bare mean of sign matches, so every README on a level target
  prints `+1.0000`. Ignore that cell.
- **The 19 archived country runs group as `methods="unrecorded"`.** They still plan and
  still validate, but they will not union with a new three-ranker run — that is `MTH-1`
  working, not a bug. Pass `--scope` if you hit the collision.
