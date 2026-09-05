# Workflow — run the chain end to end

> **Goal:** take a target from pools to a scored, costed model run. Stages 1→9 of `CLAUDE.md` §3b.
> **Cost: minutes of compute per stage, plus GPU-hours at stage 2 — which is why stage 2 is a
> separate workflow ([run-a-selection.md](run-a-selection.md)) and this one starts after it.**
>
> ⚠️ **`python -m pipeline` passes no data between stages.** Each stage already reads the previous
> one's output; the module only *checks* that what the next stage will read exists and agrees. So
> a stage can be run alone, and a stage can be skipped by accident.

---

## 0. Before anything — decide the experiment, in writing

Four names decide everything downstream, and three of them are encoded into table names rather
than passed as flags:

| | example | where it lives |
|---|---|---|
| universe / partition | `all`, `vcb`, `PRICE10K` | the schema — `unified_schema_<part>` |
| target | `cs_rank_20day` → table `rank_20day` | the table NAME |
| `d`, `h` | `d20_h20` | ⚠️ **the table NAME, never a parameter** — it flows `…__final__d20_h20` → dataset `metadata.json` → asserted against the model config |
| architecture | `lstm`, `gbt`, `cnn`, … | the config file, whose name must equal `run_name` |

⚠️ **The chain's DEFAULT is `vcb` / `close_adjust_5day` / `d=20 h=5`, and the default is not the
live experiment.** That target is a price LEVEL — R² −85.6, MASE 21.36, `hit_rate` 1.0 by
construction. **Do not start new work on it.** Name your table explicitly with `--table`.

## 1. Orient — **O1**, **O3**

`python -m pipeline` with your `--ticker` / `--table` / `--config` (**O2**). ⚠️ **`--config` is not
optional**: the `model` row keys on it, so without one the tool reports the DEFAULT chain's run as
up to date and the `backtest` row scores that run too.

## 2. Stage 1 — the pools — **C1**

⚠️ **Name the two pools you need.** `--select "group:unified"` builds all twelve — hours on a wide
partition.

⚠️ **Sibling pools on an older calendar silently truncate everything below.** The joins are INNER,
so a rebuild of a wide table INNER-joins back down to the oldest sibling **and looks unchanged**.
`pipeline`'s `data` row reports this as `pools_behind`. This is how a chain lost 31 sessions to a
`pool__ta` that stopped two months earlier.

## 3. Stage 2/4 — selection — **STOP, see [run-a-selection.md](run-a-selection.md)**

Stages 2 and 4 are **MANUAL by design** — `--apply` prints `MANUAL — cannot be produced here` and
stops, because a selection run is GPU-hours and must be a deliberate act. On a cross-sectional
chain stages 3-4 report `n/a` (`CSP-1`) and are skipped, not failed.

## 4. Stage 5 — the final table — **C5**

`python -m final_features --apply`.

⚠️ **Read the fingerprints it prints before you reach for `--replace`.** Without `--replace`, a
table whose fingerprint moved is a hard **error** — and that refusal is the feature: rebuilding
DROPS the table and orphans every dataset below it.

⚠️ **`final_features` groups on `(schema, target, setup)` — there is NO term for which pools a run
saw.** So a `pool__basic`-only run and a `basic + X` run are ONE group and get **unioned**. Pass
`--scope <name>` when two experiments would land on one table name, and `--root` when one of them
is a probe rather than a chain input.

## 5. Stage 6 — the tensors — **C6**

⚠️ **`--ticker` is not optional on a cross-section** — it defaults to `vcb` and would read the
wrong schema entirely.

What this stage guarantees, and why each matters:

- **the purge gap is `d + h − 1`, not `h`** — at `d=20 h=5` that is 24 rows. Purging only `h`
  leaves 19 rows of the test sample's own input window in training.
- **imputation is the TRAIN-slice median, never `ffill().bfill()`** — `bfill` fills a leading gap
  with the first *future* observation.
- **a 0/1 label is never standardised** — build with `scale_target=False`; `_verify` raises.

## 6. Write the model config — **between stage 6 and stage 7**

⚠️ **It cannot be written earlier.** `n_features` is an assertion `engine._verify` raises on, and
the surviving channel count is only known once the dataset exists. Filename **equal to
`run_name`**.

⚠️ **In an arm sweep every arm inherits the reference's optimiser schedule, batch size, patience
and seed** — that is what makes them comparable, and a schedule difference shows up as an
architecture difference.

## 7. Stage 7 — train — **C7**

`--dry-run` first validates the config without training anything.

## 8. Stage 8 — score — **C8, and it is TWO commands**

`--rescore` rewrites each run FOLDER's metrics; `index.csv` is written **only** by
`--rebuild-index`. Running one without the other once left a folder reading `ic_t` +3.47 while the
leaderboard still read 15.50.

## 9. Stage 9 — does it pay for its own trading? — **C9** (panel runs only)

⚠️ **The output CSV lands inside the run folder, which is gitignored** (`RPR-1`) — not in
repo-root `results/`. 29 run folders were deleted once and are unrecoverable.

Then **W5** (`handscreen`) **beside** it, never instead of it: a model that cannot beat three
ranked columns has not earned its complexity.

## 10. Is it one lucky split? — **W1**

A single train/val/test split is not evidence. ⚠️ **`--out` is load-bearing** — every artefact is
written by basename, so a second track run without it silently overwrites the first.

⚠️ **Two concurrent `walkforward` sweeps corrupt each other.** The loud half is a
`FileNotFoundError`; the silent half is one sweep reading tensors the other is mid-`np.save` on.
`run.namespace_lock` refuses the second sweep now — do not force past it.

---

## Done when

- [ ] `python -m pipeline` reads `up to date` for every stage at or below the one you quote
- [ ] the run folder holds `metrics.json` **and** `index.csv` agrees with it
- [ ] you have either a walk-forward track or an explicit written note that this is ONE split
- [ ] every number you intend to publish has been through [quote-a-number.md](quote-a-number.md)

## Traps

⚠️ **`--scope` does not separate a probe from the chain — only `--root` does.** A probe in the
chain's report root is either silently unioned (same setup keys) or blocks ALL planning, including
unrelated chains.

⚠️ **A cleared selection bar has never yet survived downstream in this repo.** `z = +2.15` at
stage 2 bought nothing at stage 7. The model stage is the next question, not a formality.

⚠️ **A run folder is immutable.** Re-scoring rewrites metrics from `predictions_*.csv`; editing a
built dataset's `metadata.json` in place is how a folder stops describing its own tensors.
