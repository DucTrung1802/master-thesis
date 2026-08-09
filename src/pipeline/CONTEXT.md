# Context — `src/pipeline`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

> The five stages as one chain, and the one question none of them could answer:
> **which stage is stale?** Built 2026-08-09.

```
python -m pipeline                      # what exists, what is stale — writes nothing
python -m pipeline --apply              # run every stage that is not ready
python -m pipeline --apply --rescrape   # ⚠️ and re-fetch from the network first
python -m pipeline --only model --apply # force one stage
python -m pipeline --ticker bank --table rank_5day__final__d20_h5
```

## 1. The chain — SIX stages as of 2026-08-10

```
raw_data/ → bronze → silver → unified_schema_<t>.pool__*   data   ⚠️ NEW, ⚠️ the network
        ↓
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

### 1a. ⚠️ The `data` stage, and the two rules it is built out of

The chain used to start at `selection` and treat the unified pools as given. It now
starts at the network, because "end to end" that begins after the data is already
correct is not end to end. Two of the standing rules are the whole design:

**Its status is a DATE, never a green asset** (CLAUDE.md §5 rule 10). `landed()` asks
"is this folder empty?", and with `skip_existing=True` a CafeF scrape returns at an
`os.path.exists` check before one request goes out — green, fast, every series left
where it was. So `status_data` reads `MAX(date)` from `pool__basic` and compares it with
the newest `date` in the raw CafeF price CSV for that ticker. **On the first run of this
stage that comparison fired immediately**: raw reached `2026-08-07` while the table
stopped at `2026-06-25`.

**It always re-ingests, even when it does not re-scrape** (rule 11). A scrape and its
ingests are separate assets, so "re-scraped" never implies "re-ingested" — bronze once
sat a full day behind a completed scrape with nothing raising. `apply_data` therefore
runs the bronze → silver → unified half unconditionally and the network half only under
`--rescrape`.

⚠️ **The asset list is EXPLICIT, and `+unified/pool__targets` would have been wrong.**
An upstream selection resolves to the whole landing layer, including
`raw/trading_view_data@stocks` (777 tickers, ~10 h) and `raw/cafef_financials` (~2.4 h
per ticker) — neither of which `pool__basic` reads. `DATA_SCRAPE_ASSETS` is exactly
`silver.stocks_basic`'s six sources (`orchestration/assets/silver.py::STOCKS_BASIC_SOURCES`)
and stops there.

⚠️ **`--rescrape` is scoped to `--ticker` and is honest about it.** The four CafeF tab
assets now take a `CafeFTabConfig`, so the stage passes `skip_existing=False` (or the
fetch does not happen at all) together with `tickers=[<TICKER>]` (or it costs 781 names
instead of one). A ticker matching nothing **raises** rather than queueing zero tasks.

⚠️ **`apply_selection` used to call `outstanding.main([])`** — passing `[]` as the report
ROOT. It never fired because the selection stage is always `ready`, so `--apply` skipped
it. Fixed while threading `--root` through.

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

⚠️ **`Stage.manual` was set and never read until 2026-08-09** (issue **PIP-1**): the
plan decided "MANUAL" from `stage.apply is None`, and the selection stage *has* an
apply, so the flag did nothing and the plan printed a bare `ran`. A reader could
reasonably take `--apply` for a cold rebuild. The plan now carries a **`manual`
column**, a manual stage's apply reports **`ran (refresh only)`**, and a manual stage
that is not ready reports **`MANUAL — cannot be produced here`**.

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

## 5d. The `--scope basic` prototype chain, run end to end 2026-08-10

The first run of all six stages from the network to a scored metric. One ticker, one
pool, one command per stage.

| stage | what it did | cost |
|---|---|---|
| `data` | VCB re-fetched with `skip_existing=False`, then bronze ×6 → silver → unified | scrape **3m24s**, bronze ~5m, silver **6m54s**, unified 1.1s |
| `selection` | `pool__basic` alone, `d=20 h=5`, **20-draw null** | **6m31s** |
| `final_features` | `return_5day__final__d20_h5__basic`, 4,266 × 10 | seconds |
| `train_test_creator` | 2,939 / 615 / 640 × 20 × **4** | seconds |
| `model` | LSTM 1×32, **4,961 parameters**, early stop @30, best epoch 10 | ~1 min |
| `result_evaluator` | 30/30 runs scored | ~2 min |

**The panel moved**: `pool__basic` went 4,235 rows / 2026-06-25 → **4,266 / 2026-08-07**.

| | selection IC | selection bar | test IC | test bar | test R² | hit rate |
|---|---|---|---|---|---|---|
| **basic, 4 channels** | +0.0783 | +0.0562 ⚠️ **clears, z=+2.15** | **−0.0345** | +0.1348 ❌ (p 0.73) | **−0.059** | 0.486 |
| *wide, 724 channels* | — | — | −0.0721 | +0.118 ❌ (p 0.88) | −0.90 | 0.491 |

⚠️ **The narrow chain is LESS BAD, and that is the STL-1 argument arriving from the other
direction.** `R²` goes −0.90 → −0.059 and test IC −0.072 → −0.034 on the same ticker,
target and split ratios. Handing an LSTM 724 channels on 2,918 windows was the
explanation offered for the wide result; cutting to 4 channels and 4,961 parameters
removes most of the damage. **Neither shows skill** — both sit inside their own null, and
"less negative" is not a result.

⚠️ **The selection cleared its bar and the model did not clear its own.** `z = +2.15` at
the selection stage bought nothing downstream. That is the two bars doing their job, and
it is the single most useful thing this prototype measured — see
`feature_selection/CONTEXT.md` §10d for why `z = +2.15` on 20 draws is weak anyway.

⚠️ **Four defects surfaced by running it, all fixed, all instances of documented rules:**

| what | why it survived until now |
|---|---|
| `apply_selection` passed `[]` as the report ROOT | the selection stage is always `ready`, so `--apply` never reached it |
| `status_model` matched runs by bare PREFIX | no two configs shared a prefix until `…__d20_h5` and `…__d20_h5__basic` both existed — the wide config then reported **2 runs** and named the basic one as its latest |
| `train_test_creator`'s CLI died on `UnicodeEncodeError` | it prints `⚠️` per dropped channel; the notebook's stdout is UTF-8 and the CLI's is cp1252, so only the CLI could fail and only when a channel WAS dropped (§5 rule 18) |
| `status_data` called an EMPTY table current | `"2026-08-07" > "none"` is False — the placeholder sorts above every digit |

⚠️ **And one that is not fixed, because it is a decision**: materialising
`unified/pool__basic` + `pool__targets` alone left **21 sibling pools on the old
calendar**. Harmless for `--scope basic`, and a rebuild of the 750-channel table would
INNER-join straight back down to 2026-06-25 while looking unchanged. `status_data`
reports it as a `pools_behind` count rather than failing, because failing would make
`--apply` re-materialise `pool__ta` for a chain that never reads it.

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

## 5c. ⚠️ `--root` and `--scope` — running a NARROWER experiment without breaking the wide one

`final_features.plan_from_reports` groups every run under one root by
`(schema, target, setup)`, and **that key has no term for "which pools"**. So a
`pool__basic`-only run at `d=20, h=5` is the same group as the 19 runs behind
`return_5day__final__d20_h5`: archiving it into the default root silently widens that
table's channel set, moves its fingerprint, and makes every dataset and run below it
stale — the STL-1 domino, for a run that was meant to be a separate experiment.

Two flags keep them apart, and both are needed:

| flag | decides | without it |
|---|---|---|
| `--root` | which runs are in the group at all | the new run joins the archive's union |
| `--scope` | the table's name suffix | both builds want `return_5day__final__d20_h5`, and the narrow one can only be built with `--replace`, which DROPS the wide one |

```powershell
python -m feature_selection.run --pools pool__basic --null-draws 20 `
    --root reports/feature_selection_basic
python -m pipeline --apply `
    --root reports/feature_selection_basic --scope basic `
    --table return_5day__final__d20_h5__basic `
    --config vcb__return_5day__final__d20_h5__basic.yaml
```

⚠️ **A scoped root needs its own `.gitignore` negation.** `!reports/feature_selection/**`
does not match `reports/feature_selection_basic/`, so its CSVs fall straight back into
the blanket `*.csv` — the same trap as issue GIT-1, one path over.

⚠️ **`--scope` is not part of the grouping key and must not become one.** It is chosen
per build, beside the `--root` that already decided which runs are in scope. Making it a
setup key would put "which pools" into a fingerprint that is deliberately over
`(source_table, channel)`.

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
