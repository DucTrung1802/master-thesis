# The model chain — eight stages, two selection layers, and the T4

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§3b`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **`CLAUDE.md` §3b-§3d-bis and §4 in full** — the stage chain, the shortlist pool between the two
> selection layers, Kaggle panel mode, and the run commands with their measured costs.
> ⚠️ **The commands are also in [`../runbook/RUNBOOK.md`](../runbook/RUNBOOK.md)**, which is the one
> that gets maintained; this file keeps the reasoning around them.

---

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
`.claude/context/pipeline.md` §5c.

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
`.claude/context/feature_selection.md` §18.

⚠️ **EVERY ENTRY POINT PRINTS ITS GPU AND ITS RUNTIME NOW** (`utils/runtime.py`, one
clock and one GPU probe, GMT+7). `pipeline` and `kgpu` were the two that were left and
they are the two that run longest. **`pipeline` needed a SECOND clock, per stage**: it
calls the stages IN-PROCESS, so each module's own banner lives in its `main()` and never
fires — a `--apply` run printed no per-stage timing at all. Its `runtime` column is
**empty, never `0`**, for a skipped or planned stage. **`kgpu` prints no GPU on purpose**:
the card that matters is a Kaggle T4, and its clock is the ROUND TRIP, not the selection.
`.claude/context/feature_selection.md` §18a.

### 3c. ⚠️ TWO selection layers, and the pool between them (2026-08-13, REBUILT 2026-08-16)

⚠️ **THE CODE BELOW WAS DOCUMENTED HERE FOR THREE DAYS WITHOUT EXISTING.** It was
written and RUN on 2026-08-13 — `pool__shortlist__close_adjust_5day__d20_h5` (4,266 ×
892) sat in the database with its source runs named in its own `COMMENT` — and **never
committed**: `git log --all -S"Pre-final shortlist"` finds no commit, `final_features`
on disk had no `--shape` flag, and the `.claude/context/final_features.md §8` this section
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
  `MANUAL — cannot be produced here`. `.claude/context/final_features.md` §8.

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
target**; the next run worth doing is this one on `return_5day`. `.claude/context/final_features.md`
§8f.

⚠️ **The cost model in `.claude/context/feature_selection.md` §15c was re-fitted on this run**
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
(`.claude/context/feature_selection.md` §16), and Kaggle's image is **xgboost 3.2.0 /
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
partition rather than lowering the default. `.claude/context/feature_selection.md` §15c.

⚠️ **THE COST MODEL HAS NO TERM FOR THE TARGET AND IT IS WORTH 13.7×** (2026-08-14).
`minutes ≈ 0.364 × channels^0.77` was fitted on 21 runs, **20 of them on the price
LEVEL `close_adjust_5day`**. The same 357-channel `pool__forex` panel took **2,016 s**
on that target and **146 s** on `return_5day`, because `lasso` — the dominant cost —
zeroes every coefficient on a return and converges at once. **A 20-draw null on a wide
pool is affordable for a return target** (357 channels + 20 draws = 41 min, measured),
where the fitted model implies ~12 h. `evidence=no_null` on a return run is now a
choice, not a budget. `.claude/context/feature_selection.md` §15c-target.

⚠️ **AND THE 13.7× WAS `lasso`, WHICH IS OUT OF THE DEFAULT ENSEMBLE SINCE 2026-08-16.**
The six rankers were chosen in 2026-08-03 for what each one SEES and **had never been
measured against each other**. They now have been (`.claude/context/feature_selection.md` §19 —
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
stream per device, so the kept set moves (58 of 62 in common). `.claude/context/feature_selection.md`
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
class), not writing a wrapper around it.

---
