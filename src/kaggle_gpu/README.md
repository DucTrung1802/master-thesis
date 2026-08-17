# kgpu — run a repo notebook on a Kaggle T4, with its data

Author notebooks where they live (`src/feature_selection/RUN__*.ipynb`), run them on
Kaggle's free GPUs (**30 GPU-h/week, 2×T4 15 GiB** against this machine's RTX 3050
4 GiB), and get the **run folder back into `reports/feature_selection/`** where
`outstanding`, `final_features` and `pipeline` already look for it.

> ⚠️ **The notebook is never edited.** `RUN__feature_importance_report.ipynb` stays
> exactly as it is on disk and keeps working locally against PostgreSQL. What goes
> to Kaggle is a patched copy in `.build/`.

---

## 1. Why this is not "point a kernel at the repo"

Kaggle exposes **no endpoint for attaching a local Jupyter client to a remote GPU
kernel**. The supported mechanism is the Kernels API: upload a notebook, Kaggle runs
it headless on a worker, you collect `/kaggle/working`.

Which lands you on the real problem: **the worker cannot reach `database_main_v2`,
and it does not have this repo.** Every selection notebook opens with
`UnifiedSchemaReader(TICKER)` and a SQL join. So three things have to travel, and
one thing has to be swapped:

```
 LOCAL                                          KAGGLE WORKER
 ─────                                          ─────────────
 unified_schema_vcb  ──┐                          /kaggle/input/datasets/<owner>/<slug>/
  (PostgreSQL)         │  kgpu export                          ├ pool__*.parquet
                       ├──► .payload/  ──► private dataset ──► ├ manifest.json
 src/feature_selection ┘      (flat)                           ├ source/src/… (unzipped
                                                               │   by Kaggle, see §7)
 RUN__…ipynb ──► kgpu build ──► .build/ ──► kernel version ──► └ kgpu_bootstrap.py
                 (params patched,                                        │
                  bootstrap cell)                                        │
                                                                         ▼
                                                        cell 0: setup()
                                                          unpack src -> /kaggle/working
                                                          stub the DB modules
                                                          UnifiedSchemaReader
                                                            -> ParquetSchemaReader
                                                          pin the git commit
                                                                         │
 reports/feature_selection/<run_id>/ ◄── kgpu pull ◄── /kaggle/working/reports/…
```

`ParquetSchemaReader` **subclasses** `UnifiedSchemaReader` and overrides only the four
methods that spoke SQL — `read`, `column_types`, `tables`, `overview`. **`join()` is
the local one**, so the key intersection, the one-to-one validation and the
`join_log` that go into the report are the same code that runs on this machine.

The trade-off against a live remote kernel: runs are **batch, not interactive**. You
get logs and artefacts, not a cell-by-cell REPL. Which is why `rehearse` exists.

---

## 2. Setup, once

```powershell
.\mt_env\Scripts\Activate.ps1
python -m pip install -r src/kaggle_gpu/requirements.txt
```

⚠️ **Into `mt_env`, not a second venv.** The export step imports
`feature_selection.unified_reader`, which needs psycopg2 and the repo. The old
`src/kaggle_gpu/test_env/` (137 MB) is from when this was a standalone demo —
**delete it**, nothing points at it any more.

Token from <https://www.kaggle.com/settings/api> → *Generate New Token*, into
`src/kaggle_gpu/.env` (gitignored):

```
KAGGLE_API_TOKEN=KGAT_xxxxxxxxxxxx
```

The repo root `.env` is read too, so `POSTGRES_*` and the Kaggle token may live in
either file. Verify:

```powershell
cd src\kaggle_gpu
python -m kgpu quota
```

Then set your Kaggle username in the two `id` fields of `kaggle_config.json`.

---

## 3. The loop

```powershell
cd src\kaggle_gpu

python -m kgpu plan      feature-selection   # what would run — touches nothing
python -m kgpu data      feature-selection   # DB -> parquet -> private dataset
python -m kgpu rehearse  feature-selection   # the WORKER side, locally, no quota
python -m kgpu run       feature-selection   # push, wait, download, merge
```

`data` and `run` are separate on purpose: exporting a wide pool is minutes of
PostgreSQL and tens of MB of upload, while parameters change every run. `run`
**refuses to start** if what is staged is not what was uploaded — a content hash,
not a "did it look green".

### ⚠️ `rehearse` is the cheapest test in the loop — use it

It **executes the built notebook's own first cell**, in a clean subprocess, against a
fake `/kaggle/input` holding the staged payload: discovers the payload, unpacks the
source, installs the stubs, swaps the reader, joins the pools, checks the report root
resolves inside the collected output, constructs a `FeatureSelector`.

**Twice, in the two shapes Kaggle actually mounts** — flat with `source.zip`, and
`datasets/<owner>/<slug>/` with `source/` already unpacked. That is not paranoia:
every defect this integration has had lived in exactly that path, and each one cost a
real run (§7).

```
  payload discovered           : OK (datasets\lyductrung\mt-unified-vcb)
  report root under output dir : OK (…/nested/work/reports/feature_selection)
  git commit pinned            : OK (fc7868b6+dirty)
  pool__basic                    4,266 rows  2009-06-30 .. 2026-08-07
  pool__targets                  4,266 rows  2009-06-30 .. 2026-08-07
  one calendar                 : OK
  joined panel                 : 4,266 x 32 (2009-06-30 -> 2026-08-07)
  FeatureSelector constructs   : OK
```

The seam is two environment variables, `KGPU_INPUT_DIR` / `KGPU_WORK_DIR`, read by
the injected cell and by `kgpu_bootstrap`. Unset — on a worker — they are
`/kaggle/input` and `/kaggle/working`, so the rehearsal drives the shipping code
rather than a re-creation of it.

⚠️ The payload is a **snapshot** of `kgpu/remote/*.py`, not a link to it. Edit those
and `rehearse` refuses until you re-`export` — otherwise it would rehearse code that
is not the code that gets uploaded.

### ⚠️ What the percentages mean — each has a different denominator

Three progress readouts, and **only one of them predicts time**:

| where | line | denominator |
|---|---|---|
| `wait` | `[  1.2 min] RUNNING    32% of last` | the **last COMPLETE run of this job**, recorded in `.state/`. Kaggle's `kernels_status` returns QUEUED/RUNNING/COMPLETE and **no completion fraction**, so any "42% done" would be invented. With no baseline yet it prints `no baseline` and the elapsed time only. |
| `export` | `[1/2  50%] pool__basic …` | **tables**, not bytes. One wide pool can be 100× another. |
| the null loop | `draw   7/20  35% … [2.1 min elapsed, ~3.9 min left]` | **the real one.** Every draw is the same procedure on the same panel, so the fraction is a fraction of the work and the ETA is a genuine extrapolation. |
| a selection | `[4/9 phases  44%] rank (6 methods)   2.1s` | **phases completed**, and the line says so. The phases are wildly unequal — `permutation` alone is 12,255 s at 1,458 channels — so this is a position in the run, never a time estimate. |

In a terminal `wait` rewrites one line in place; redirected to a pipe or a file it
prints only when the state changes or the percentage crosses a 5-point step, so a
12-hour poll does not leave 2,880 lines in a log.

Cost, measured rather than asserted: nine `_tick` calls per selection are
**0.010 ms**, or 4.3e-8 of the 3.7-minute run and 7.8e-10 of one wide-pool
permutation step.

### Commands

| Command | Use it when |
| --- | --- |
| `run` | Normal case. Push, wait, download, merge. The default if you omit the command. |
| `run --data` | Also re-export and re-upload the payload first. |
| `plan` | Check the parameter patches and the payload state. Touches nothing. |
| `data` / `export` | Refresh the payload (`export` stages it, `data` also uploads). |
| `rehearse` | Before every first run of a new pool or notebook. |
| `push` | Start a long run and close the laptop. |
| `wait` | Reattach after `push` or Ctrl-C. |
| `status` / `logs` | One-shot state; the traceback when it fails. |
| `pull` | Re-download and re-merge without re-running. `--force` overwrites a merged run folder. |
| `build` | Stage `.build/` and see the patched notebook. Pushes nothing. |
| `jobs` / `quota` | What is configured; hours left this week. |

Ctrl-C during `wait` **does not stop the Kaggle run**. Reattach, or cancel it in the
web UI.

---

## 4. Configuration — `kaggle_config.json`

`defaults` merge underneath every job. ⚠️ **An unknown key raises** — a key that
changes nothing is worse than an error when it decides what runs on a paid GPU.

```json
{
  "jobs": {
    "feature-selection": {
      "id": "<user>/mt-feature-selection",
      "notebook": "src/feature_selection/RUN__feature_importance_report.ipynb",
      "results_into": "reports/feature_selection",
      "data": {
        "id": "<user>/mt-unified-vcb",
        "ticker": "VCB",
        "source_dirs": ["src/feature_selection"]
      },
      "parameters": { "POOLS": ["pool__basic", "pool__targets"],
                      "TARGET": "return_5day", "DEVICE": "cuda",
                      "RUN_NULL": true, "N_NULL": 20 }
    }
  }
}
```

| key | meaning |
| --- | --- |
| `notebook` | **Relative to the repo root**, not to this folder. Any notebook in the repo. |
| `parameters` | Rewritten into the notebook's own parameter cell — see §5. |
| `data.tables` | What to export. **Defaults to `parameters.POOLS`** + `pool__targets`, so the pools are named once. |
| `data.source_dirs` | Repo directories whose `*.py` travel in `source.zip`. Add `src/model` for a training notebook. |
| `results_into` | Repo-relative directory that downloaded run folders are merged into. Omit and the download stays in `results/`. |
| `accelerator` | `NvidiaTeslaT4` (default), `Tpu1VmV38`, or `null`. **Case-sensitive** — §7. |
| `enable_internet` | `false` by default. Only needed for `pip install` inside the notebook. |
| `max_wait_minutes` | Poll ceiling. An unbounded poll on a hung worker never returns. |

---

## 5. ⚠️ How parameters are patched, and why not the obvious way

`kgpu` rewrites the **top-level assignment where it already stands**, in the
notebook's own parameter cell, preserving its trailing comment:

```python
TARGET = 'return_5day'
HORIZON = 5  # h — ⚠️ MUST match the target's own horizon (the `5` in its name)
RUN_NULL = True  # ⚠️ the BAR. Slow. See the warning above.
```

**Not an override cell appended after it.** That parameter cell ends with

```python
EXCLUDE = IDENTITY + [c for c in ALL_TARGETS if c != TARGET]
```

— a *derived* value. Override `TARGET` in a later cell and `EXCLUDE` still excludes
the old one, so the run's own label is handed to `FeatureSelector` as a candidate
feature. That does not raise. It reports an IC near 1 and looks like a discovery.

Consequences worth knowing:

- The patcher works on the **AST**, not on text — this notebook is two-thirds prose
  and a regex for `^TARGET = ` matches inside it.
- A parameter that is **not found raises and refuses to push**, rather than letting
  the notebook run its own defaults under your config's name.
- A name bound by tuple unpacking (`N_SPLITS, MIN_TRAIN = 5, 500`) gets its override
  inserted on the following line — still ahead of anything derived from it.
- Only **module-level** assignments in code cells are candidates.

---

## 6. What comes back, and where it goes

`kgpu pull` does two things and only one of them deletes:

1. **`results/`** — a scratch mirror of `/kaggle/working`, **wiped on every pull**.
   Keep nothing here.
2. **the merge** — each `<run_id>/` holding a `metadata.json` is copied into
   `results_into` (`reports/feature_selection/`). ⚠️ **Never overwritten**: a run
   folder is immutable by repo convention, so a collision is reported and skipped;
   `--force` is for re-downloading the same run, not for a second one.

After the merge the run is a normal archived run and `final_features` sees it like any
local one.

⚠️ **IT DID NOT USED TO CARRY `outstanding.csv`, AND NOTHING SAID SO.** The notebook
wrote a report folder and stopped, one file short — and `final_features.plan_from_reports`
**skips a folder with no shortlist without a word**. Measured 2026-08-15: the two runs
produced through this command were both in that state, and `final_features` planned 19
runs and reported no error (`feature_selection/contract.py` §2). The notebook writes and
validates the shortlist itself since 2026-08-16 (`feature_selection/CONTEXT.md` §18), so
`merge_results` now **checks** what came home rather than printing a reminder to go and
do it by hand — a merged run without one is named as a WARNING.

The mirrored source is deleted from `/kaggle/working` by an injected final cell, so
the download is the report and nothing else.

### ⚠️ Provenance, and the one number that moves

`feature_selection.report` shells out to `git` for the commit in every
`metadata.json`, and a Kaggle worker has no repo — so the bootstrap **pins the
commit the payload was cut at** (`fc7868b6+dirty`), which is the code that actually
ran. `manifest.json` carries the export timestamp beside it.

⚠️ **A KAGGLE RUN AND A LOCAL RUN ARE TWO RUNS, NOT ONE REPEATED — FOR TWO REASONS,
BOTH MEASURED.**

1. **The device.** XGBoost subsamples from a different RNG stream per device
   (`feature_selection/CONTEXT.md` §16), so the kept set moves between a T4, an
   RTX 3050 and a CPU even at the same seed.
2. **The library stack is not the same one.** Measured 2026-08-15, same run,
   both `environment` blocks:

   | | this machine (`mt_env`) | Kaggle image |
   |---|---|---|
   | python | 3.12.10, Windows-11 | 3.12.13, Linux-6.12.90 |
   | **xgboost** | **2.1.1** | **3.2.0** ← a major version |
   | sklearn | 1.7.2 | 1.6.1 |
   | numpy | 2.2.6 | 2.0.2 |
   | scipy | 1.17.1 | 1.16.3 |

   XGBoost is one of the six rankers and sklearn provides `mutual_info` and the
   lasso path, so this is a difference in the **procedure**, not just the hardware.

Every one of those numbers is in the run's own `metadata.json` under `environment`,
which is what makes the comparison checkable after the fact — `platform` alone tells
you Linux from Windows, and therefore Kaggle from local. ⚠️ The **GPU name is not**
in the report; it is in the run log, which `pull` brings back beside it.

---

## 7. Traps that cost a run

### ⚠️ The three measured on the first round trip, 2026-08-15

Each of these passed every local check before it fired, and each burned a run.

1. **The mount is `/kaggle/input/datasets/<owner>/<slug>/`, not
   `/kaggle/input/<slug>/`.** Every tutorial says the second and this package
   assumed it. The payload is now found by **content** — a directory holding a
   `manifest.json` beside our own reader — searched to depth 4, and the rehearsal
   runs both layouts.
2. **Kaggle unpacks `source.zip` into `source/` and deletes the archive.** The
   payload that is *uploaded* and the payload that is *mounted* are different
   shapes. Both are handled; only one can be tested by looking at `.payload/`.
3. **`dataset_status` says `ready` while the new version is still processing.**
   Measured: a fresh version returned `ready v2` on the first poll, with v3 the one
   just uploaded. A kernel pushed on that answer mounts v2, runs to COMPLETE, and
   reports on stale data. `wait_ready` now requires the version **number** to move.

And one found by reading the client rather than by losing a run: **`kernels_push`
returns `invalidDatasetSources` and succeeds anyway**, leaving a kernel that starts
with nothing mounted. `push` raises on it.

### ⚠️ The fourth, measured 2026-08-17 — `KGP-1`, and it had been live for two days

**A stub that shadows a package which was never shipped is worse than a missing one.**
`_install_stubs` fakes `utils` for one constant (`utils.constants.DATABASE_MAIN_V2`), and
a stub module has `__path__ = []`, so every real submodule under it is unreachable.
`feature_selection/report.py` gained `from utils import runtime` on **2026-08-15 — the day
after this integration's only green round trip** — and from that moment every job would
have died on the worker at `import feature_selection.report`, after the queue and the
upload. Nothing detected it because nothing had rehearsed since. The first rehearsal of the
panel work reproduced it in **3.6 s**.

Fixed two ways, both needed: `src/utils` is in both jobs' `source_dirs`, and a stub is
installed **only when `importlib.util.find_spec` cannot find the real module**. ⚠️ The
lesson is the section's own: a green step is not evidence — and here there was not even a
green step, only two days of nobody looking. **`rehearse` before every run, not only
before the first one.**

### ⚠️ A fifth, and it fired AFTER a 23-minute run had COMPLETED

`kgpu pull` raised `UnicodeEncodeError: 'charmap' codec can't encode character '⋈'`
(2026-08-18). The run was fine; the DOWNLOAD was not. `kernels_output` writes the run log
to disk with the process's default encoding — cp1252 on Windows — and the log carries the
worker's stdout, which included a `⋈` this package had put in the panel's provenance note.

⚠️ **CLAUDE.md §5 rule 18, one step further out: it is not enough for OUR writers to use
utf-8, because a third party's writer handles this text too.** The note is ASCII now. If
you meet it on an existing artefact, `PYTHONUTF8=1 python -m kgpu pull <job>` gets the run
home — the fix is in the text, not in the retry.

### ⚠️ Two more from the first panel push, both "Kaggle substitutes and carries on"

Measured 2026-08-17, adding the `cross-sectional` job. Both are now rejected locally by
`config._validate`, for the reason the accelerator already was.

1. **A dataset title must be 6-50 characters, and Kaggle only says so after the upload
   call.** A 62-character title cost a whole `kgpu data` — **1m 51s** of export and
   parquet write — before `dataset_create_version` raised.
2. **⚠️ THE KERNEL SLUG COMES FROM THE TITLE, NOT FROM `id`.** Title
   `MT cross-sectional selection (top-300 panel)` with id `…/mt-cross-sectional`
   created `…/mt-cross-sectional-selection-top-300-panel`. `kernels_push` **warns in
   prose and pushes anyway**, so the push reported success and the kernel ran on a T4 —
   while `status`, `wait` and `pull` every one raised *"Kaggle has no kernel you can
   read"* against the id that was asked for. The run is not lost; it is simply
   unreachable until the id is corrected. `config.kaggle_slug()` reproduces Kaggle's
   rule and `_validate` refuses a job whose two halves disagree.

The shape of all four is the same, and it is the repo's standing rule 10 in a new
place: **a green step is not evidence that the step did what it said.**

### The rest

| Symptom | Cause |
| --- | --- |
| `no payload dataset is mounted` | The job ships data and `/kaggle/input` is empty or unrecognised. The error lists the whole tree — read it, then check `dataset_sources` and the dataset's status. ⚠️ This used to fall through to the local branch and fail six cells later complaining about a repo root. |
| `no kernel image is available for execution on the device` | Landed on a P100 (`sm_60`); Kaggle's torch ships `sm_70`+. Use `NvidiaTeslaT4`. **Kaggle silently ignores an unrecognised accelerator** and hands you the default, so `nvidiaTeslaT4` gets you a P100 — `kgpu` rejects a bad value locally. The bootstrap also checks the arch up front. |
| Report built on last week's data | A dataset version still `processing` when the kernel starts mounts the **previous** version and completes normally. `kgpu data` waits for `ready`; do not push around it. |
| `ModuleNotFoundError` on the worker | A `source_dirs` gap. `rehearse` catches it in seconds. |
| Run completes, `pull` downloads nothing | The notebook's `REPORT_ROOT` resolved outside `/kaggle/working`. `rehearse` asserts against exactly this. |
| Panel silently truncated | The pools do not share a calendar. Checked at **export**, before anything is spent, as well as in the notebook. |
| `CUDA out of memory` | The T4 is 15 GiB, ~4× this machine. If it still fails, the pool is too wide for the design matrix, not for the card. |
| A dataset holding only loose files | Something staged a **subdirectory** into `.payload/`. `dir_mode="skip"` drops those without a word — the payload is flat on purpose. |

**Limits**: 30 GPU-h/week · 12 h per GPU session · `/kaggle/working` capped at 20 GB ·
`QUEUED` before `RUNNING` is normal and can take minutes.

---

## 7b. ⚠️ PANEL MODE — the one job that does NOT ship pools

`cross-sectional` ships **one `panel.parquet`**, not a set of pools, and the reason is
structural rather than a missing parameter. Every other notebook reaches the database
through `UnifiedSchemaReader`, which is exactly why `ParquetSchemaReader` can stand in
for it. `feature_selection.cross_sectional.read_universe_panel` does not: it is one
hand-written SQL statement reaching for `reader.driver`, so on a worker it hits *"there
is no database on a Kaggle worker"* whatever it is given. **No `--pools` value, no
notebook parameter and no config key routes around that** — the cross-sectional read
bypasses every abstraction the payload replaces (`CSP-1` in its second form).

So the join runs at EXPORT time, here, and the worker receives the finished panel with
`cs_rank_{h}day` already derived:

```jsonc
"data": {
  "id": "<user>/mt-panel-top300",
  "ticker": "ALL",
  "source_dirs": ["src/feature_selection", "src/utils"],
  "panel": { "top_n": 300, "liquidity_before": "2014-01-01",
             "horizons": [20], "min_width": 5 }
}
```

`resolved_tables()` returns `["panel"]` in this mode — naming pools would promise the
worker a shape it never sees. ⚠️ **`liquidity_before` is REQUIRED and has no default**:
ranking turnover over the whole sample picks the names that *turned out* to be liquid,
and a silent default would make that invisible in the artefact. ⚠️ The derived `cs_rank`
is a rank **within the shipped names**, recorded in the manifest as such.

The worker notebook (`src/feature_selection/RUN__cross_sectional_panel.ipynb`)
re-implements nothing: `kgpu_remote_reader.load_panel()` returns a
`feature_selection.run.ProvidedPanel` and `run_selection(provided_panel=…)` does the
rest. ⚠️ **The provenance travels with the frame** — schema, database, universe and the
**channel→pool map**, without which `outstanding` cannot fill `source_table`,
`contract.validate_shortlist` refuses the shortlist, and the run comes home invisible to
`final_features`.

⚠️ **`rehearse` DOES NOT RUN THE NOTEBOOK'S OWN CELLS.** It executes cell 0 — where every
defect this integration has had actually lived — and then drives the panel path itself.
To cover the notebook, cut a payload down (a handful of tickers) and run the built copy
with `KGPU_INPUT_DIR` / `KGPU_WORK_DIR` pointed at it, which is the same seam. Measured
2026-08-17: 30 names × 48,521 rows, 2m 11s end to end, `source_table from metadata`. **A
cut-down panel is a smoke test and never a measurement.**

Measured on the real payload, 2026-08-17: `export` **2m 04s** → 1,247,098 × 104,
**477.4 MB**, 300 tickers, 4,388 dates; `rehearse` **16.0 s**, both layouts, `n_eff = 218`.

## 8. Adding another notebook

1. Add a job to `kaggle_config.json`: `id`, `notebook` (repo-relative), the
   `parameters` you want to pin, and `results_into` if it writes a run folder.
2. Widen `data.source_dirs` to whatever the notebook imports.
3. `python -m kgpu rehearse <job>` until it passes.
4. `python -m kgpu run <job>`.

A notebook that needs no database omits the `data` block entirely — `smoke`
(`notebooks/train.ipynb`) is that case, and is worth a 2-minute run whenever you want
to prove auth and the GPU path without touching the pipeline.

⚠️ A notebook reading anything **other** than `unified_schema_*` needs its own
payload shape; the reader swap only covers `UnifiedSchemaReader`. Export the frames
and read them by path.
