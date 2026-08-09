# Context — `src/pipeline`

> The five stages as one chain, and the one question none of them could answer:
> **which stage is stale?** Built 2026-08-09.

```
python -m pipeline                      # what exists, what is stale — writes nothing
python -m pipeline --apply              # run every stage that is not ready
python -m pipeline --only model --apply # force one stage
python -m pipeline --ticker bank --table rank_5day__final__d20_h5
```

## 1. The chain

```
reports/feature_selection/<run>/outstanding.csv        feature_selection
        ↓
unified_schema_<t>.<target>__final__d<d>_h<h>          final_features   ⚠️ writes the DB
        ↓
src/train_test_set/<dataset>/                          train_test_creator
        ↓
src/model/runs/<run_id>/                               model.lstm
        ↓
results/metrics.json + runs/index.csv                  result_evaluator
```

Every stage is `python -m <package>`, dry-run by default, `--apply`/`--save` to write.
That uniformity is most of what "end to end" means here — before this, two stages were
notebooks with a parameter cell and no headless entry point.

## 2. ⚠️ This module does NOT pass data between stages

It would be the obvious design and it is the wrong one. Each stage already reads its
input from the previous stage's output; what was missing was that some of those reads
were **unverified strings typed twice**. Those were fixed *in the stages*, not
papered over here:

| seam | was | now | enforced by |
|---|---|---|---|
| table → dataset | `LOOKBACK_DAY` a free notebook parameter, against an `lb20` view | `d`,`h` parsed from the table name | `train_test_creator.parse_final_table` |
| dataset → run | a `dataset:` string in a YAML, believed | asserted against the dataset's own `metadata.json` | `model.lstm.train._verify` |
| run → score | metrics computed inside the training notebook | recomputed from `predictions_*.csv` | `result_evaluator.evaluate_run` |
| run → provenance | the run recorded its dataset, not the dataset's source | `lineage` carries table → `COMMENT` → dataset → run | `model.lstm.train._verify` |

So `stages.py` runs each stage and **checks** that what the next one will read exists
and agrees. `status()` is that check, and it is the whole value: `python -m pipeline`
with no flags answers "which stage is stale" without touching the database, the GPU or
a single file.

## 3. ⚠️ A ready stage is SKIPPED, not re-run

`--apply` runs only what is not ready, because re-running is not free in either
direction:

* re-running `final_features` needs `--replace`, which **drops the table**, and every
  dataset hash downstream changes with it;
* re-running `train_test_creator` changes the dataset hash, and `load_dataset(
  expected_hash=…)` in a past run's config then fails to verify — correctly;
* re-running `model` appends a **second** run folder rather than replacing the first.

`--only <stage>` forces one, which is explicit about which of those you are asking
for. `--from <stage>` starts partway down the chain.

## 4. ⚠️ What the pipeline does not do

**Run a feature selection.** A selection is hours of GPU time and a judgement about
which pools to join — `feature_selection/RUN__feature_importance_report.ipynb` is the
entry point and it stays manual (`Stage.manual = True`). This stage only refreshes
`outstanding.csv` from the runs that already exist.

**Vouch for anything.** A green pipeline means the five stages agree with each other.
It says nothing about whether the features are worth having, and the answer to that is
recorded at every level: **18 of 19 source runs computed no null and the 19th failed
its own** (`feature_selection/CONTEXT.md` §14b), 725 of 750 channels were chosen by
exactly one run (`final_features/CONTEXT.md` §6), and the run trained on them shows
**no skill on either split** (`result_evaluator/CONTEXT.md` §6).

⚠️ **`status()` cannot see any of that.** It compares fingerprints over
`(source_table, channel)`, so a run gaining a null moves its `evidence` and no
channel — the chain stayed green through the 2026-08-09 EVD-1 measurement while the
stored provenance sentence went stale. Green means the five stages agree, and that is
all it has ever meant.

## 5. State today (2026-08-09)

| stage | VCB chain | BANK chain |
|---|---|---|
| `selection` | 20/20 archived runs carry `outstanding.csv` | (shared) |
| `final_features` | `return_5day__final__d20_h5` — 4,235 × **754** (750 ch), `505fbe21a1f0` | `rank_5day__final__d20_h5` — 53,921 × 18 (14 ch), `f5615a68f556` |
| `train_test_creator` | 2,918 / 610 / 635 × 20 × **724** | 26,964 / 12,524 / 13,028 × 20 × 13 |
| `model` | `lstm__vcb__…__20260809-130032`, best epoch **7** | `lstm__bank__…__20260809-130054`, best epoch **1** |
| `result_evaluator` | series grain — **no skill** | panel grain — **no skill** |

**29/29 runs scored**, and both new runs sit inside their own null:

| | val IC | val bar | test IC | test bar | test R² |
|---|---|---|---|---|---|
| VCB, series grain | +0.1012 | 0.1507 ❌ | **−0.0721** | 0.118 ❌ (p 0.88) | **−0.90** |
| BANK, panel grain | +0.0073 | 0.0206 ❌ | **−0.0209** | 0.0158 ❌ (p 0.84) | −0.018 |

⚠️ **The counts above are post-STL-1 and this table was stale until 2026-08-09** — it
still read `4,235 × 207` and `× 202`, the widths from before the measured cut replaced
`max_features=12`. The table is now 750 channels and the dataset keeps **724** of them
(26 dropped as constant across the train slice, `train_test_creator/CONTEXT.md` §5).
Both fingerprints currently match their shortlists, so nothing in the chain is stale.

## 6. Adding a stage or a second target

A stage is a `Stage(name, describe, status, apply)` — a status function that reports
whether its output exists, and an apply function that produces it. Nothing else. A
second target is `--ticker`/`--table`/`--config`, not an edit: the defaults at the top
of `stages.py` are arguments everywhere below them.

Both chains run today (issue **BNK-1**, closed 2026-08-09):

```
python -m pipeline                                                          # VCB
python -m pipeline --ticker bank --table rank_5day__final__d20_h5 \
                   --config bank__rank_5day__final__d20_h5.yaml             # BANK
```

⚠️ **The bank table is named `rank_5day` and stores `return_5day`.** A rank's value
depends on which other names are in the panel, so `final_features` refuses to freeze
one into a table (`final_features/CONTEXT.md` §5). `train_test_creator.resolve_target`
reads the column the table actually has; the *cross-sectional* reading is recovered at
the other end by `result_evaluator`, which scores per date and averages
(`result_evaluator/CONTEXT.md` §3c). Nothing in the chain ever materialises a rank.

⚠️ **A 20-ticker panel changes the scoring, not just the data.** `n_eff` becomes
`n_dates/h` rather than `n/h` (130.6, not 2,606), the IC becomes per-date, and the
null moves whole dates. The grain is detected from the `ticker` column in
`predictions_*.csv`, so it cannot be misdeclared.
