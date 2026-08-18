# TODO — the one backlog

> Consolidated 2026-08-17 from this file and the retired `src/orchestration/todo.md`
> (28 numbered items, deleted in the same commit — `git show HEAD~1:src/orchestration/todo.md`
> brings it back). **Everything here carries a measured cost or a measured symptom.**
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
> ⚠️ Costs are measured on this machine (RTX 3050 4 GB, 15.6 GB RAM), three rankers, GPU
> — not the stale fitted models in `feature_selection/CONTEXT.md` §15c, which **P1-1** is
> about.

**Priority rule used below:** a thing that makes a number you ALREADY HAVE wrong outranks
a thing that would give you a new number; a thing that unblocks hours of other work
outranks a thing that is only itself; structural code comes last because it only pays off
for runs that are currently blocked anyway.

---

## P0 — a number you already have is wrong or unreadable until this is done

### ✅ P0-1 · DONE 2026-08-17 — the two-layer null CLEARS, and **my recorded prediction was wrong**

`feature_selection/studies/two_layer_null.py`, **20 draws across two seeds**, 3 h 39 m total (1h50m + 1h48m). Each draw shuffles the
label ONCE and re-runs **both** layers on it — six layer-1 selections, the union of their
survivors, one layer-2 selection.

| | layer-2 only *(the run's own null)* | **BOTH layers, 20 draws** *(honest)* |
|---|---|---|
| draws | 10 | **20** (seeds 18 + 19, pooled) |
| null mean | +0.0023 | +0.0156 |
| null sd | 0.0300 | 0.0314 |
| **p95 BAR** | **+0.0428** | **+0.0574** |
| null MAX | +0.0577 | +0.0676 |
| z | +4.48 | **+3.86** |
| p | 0.0909 | **0.0476** (the 1/21 floor) |
| observed | +0.1369 | +0.1369 |

⚠️ **The two seeds agree to the third decimal on the bar** — +0.0573 (seed 18) vs +0.0565
(seed 19) — which is the reassuring part, and more useful than the p-value. `SE(sd)` fell
from 0.0083 to 0.0051, so `z = +3.86` now rests on a stable dispersion estimate.

⚠️ **20 draws does not escape the floor, it moves it.** `p = 0.0476` is exactly `1/21`:
**0 of 20** draws reached the observed, so the p-value is again pinned at its minimum and
says "no draw beat it", not "p is small". Real resolution needs 50 draws (floor 0.0196,
~9 h) or 100 (0.0099, ~18 h). **Read `z`, which has no floor.**

**The criticism was right about the direction and wrong about the outcome.** Pricing in
layer 1 raises the bar **34%** — so the run's own null *was* too easy, exactly as argued.
But the observed IC is **2.4× the honest bar**, **0 of 10 draws** reach it, and the null
MAX (+0.0676) stays below it so rule 3 does not fire.

⚠️ **I wrote "Prediction, recorded now so it cannot be revised afterwards: this will not
clear." It cleared.** The prediction is left in the git history rather than quietly edited.

⚠️ **The bar is CONSERVATIVE, which makes clearing it stronger.** On a shuffled label
layer 1 keeps **~705-713 channels** against 208 on the real label — nothing dominates, so
the correlation prune removes less. Layer 2 in each draw therefore selects from a 3.4×
wider candidate set and has *more* room to overfit noise, pushing the null up.

**What this does NOT settle** — three of the four original objections are untouched:

1. `p = 0.0476` is **still the floor**, now at 1/21. Raising 10 → 20 draws bought a
   stable `sd` (SE 0.0083 → 0.0051) and a trustworthy `z = +3.86`; it did **not** buy
   p-value resolution, and 50-100 draws (9-18 h) is what would.
2. The fold trend `+0.125 / −0.017 / +0.142 / +0.127 / +0.306` is untested here, and rule
   23's data-arrival reading still fits a pool whose news channels are NULL before 2013.
3. 9 of 66 channels are constant across the train slice.

⚠️ **And a cleared SELECTION bar has never yet survived downstream in this repo** — §5d:
"The selection cleared its bar; the model did not clear its own." **P2-3** is now worth
running, and that is a change from this morning.

### ✅ P0-2 · DONE 2026-08-17 — rule 21 shipped, **and the report had to be fixed too**

`selector.py:1021` and `:1117` now call `evaluation.sign_hit_rate`, which returns **NaN
when every non-zero label shares a sign**. Implemented in `feature_selection` rather than
imported from `result_evaluator`, because the dependency runs the other way.

⚠️ **Shipping the metric was not enough, and this is the part that nearly slipped.** A
withdrawn `hit_rate` is NaN, and `report.py`'s formatter tested only `v is None`:
`abs(nan) < 10` is False, so it fell through to `f"{v:.1f}"` and the README printed a
bare **`nan`** — which reads as a defect rather than as the deliberate absence it is.
Both the summary table and the holdout table now test `v != v` and print `—`.

**Verified end to end, not just by unit test**, on a fresh level-target run
(`vcb__basic+market_breadth__close_adjust_5day`, 48.6 s):

```
README.md:47   | `hit_rate` | — | — |
validation.csv  hit_rate NaN in all 10 rows
```

5 unit tests besides, including the two edge cases that matter: an **all-negative** label
must withdraw too, and a single unchanged day (`0.0`) must **not** make a return series
single-signed — otherwise one flat session silently deletes the metric for a whole run.

### ✅ P0-3 · DONE 2026-08-17 — `float32` does **NOT** reproduce `float64`

Measured on two panels, both dtypes, **both on CPU** so XGBoost's per-device RNG could not
confound the dtype:

| panel | kept | shared | Jaccard | `ic_mean` float64 → float32 |
|---|---|---|---|---|
| `basic + market_breadth` | 64 vs 64 | 59 | 0.855 | +0.0275 → +0.0317 |
| `basic + stock_market` | 123 vs 123 | 115 | 0.878 | +0.0322 → **+0.0490** |

The second is a **52% relative change in the measured IC** — the same order as the effects
this package exists to detect. ⚠️ **The docstring shipped on 2026-08-16 claimed the
opposite** ("the precision loss is nominal…"); that claim is withdrawn and the measurement
is in its place.

⚠️ **What it swaps is not random, and that is the useful half.** Every differing channel
trades for its NEAR-TWIN — `foreign_sell_value`↔`foreign_sell_volume`,
`prop_sell_val`↔`prop_sell_vol`, `drv_parkinson_21`↔`drv_garman_klass_21`,
`drv_close_pos_63`↔`drv_close_z_63`, `…volume_negotiated`↔`…value_negotiated`. `float32`
is **breaking ties the correlation prune considers interchangeable**, not scrambling the
selection.

**Standing rule now:** never use `float32` for a run whose number will be quoted. It is
for the case where the alternative is not running at all (`MEM-1`'s universe panel), and
`contract.SETUP_KEYS` already carries `design_dtype` so the two can never be unioned.

### ✅ P0-4 · DONE 2026-08-17 — `mkt_n_names` blocked from the pool, kept in gold

`UNIFIED_MARKET_BREADTH_NOT_FEATURES` blocks it at the pool builder;
`pool__market_breadth` is now **4,266 × 10 (7 channels)**, and `gold.market_breadth` still
carries the column because a reader needs to know how wide each date's cross-section was.
A candidate FEATURE and a DIAGNOSTIC are different things.

⚠️ **It never bit**: on the 2026-08-17 `return_5day` chain **no `mkt_*` channel survived
layer 2 at all** — 4 of 208 reached the shortlist pool, 0 of 66 reached the final table.
This is a guard against the next run, not a repair of that one. ⚠️ The block-list raises
if it names a column the source does not have — a guard that silently matches nothing is
how an excluded column comes back after an upstream rename.

---

### ✅ P0-5 · DONE 2026-08-18 — `RNK-1`, the label is reconstituted at dataset build

**The model is trained on a label the selection never scored.** `final_features` stores
`return_{h}day` because a rank belongs to a run and not to a row (its §5), on the stated
understanding that *"the reader re-ranks"* — and no reader does. `train_test_creator`
builds `y` from the stored column and merely records `selected_for`.

⚠️ **§2b already measured the cost of exactly this swap, on the same panel and folds: the
IC drops 4× and the hit rate falls below a coin.** So this outranks any new number — it is
the priority rule's first clause, *a thing that makes a number you already have wrong*.

**Fix**: in `train_test_creator`, when the table's target is `derived`, recompute
`cross_sectional_rank` within each date over the rows the table holds, and record the
universe it ranked over in the dataset metadata. ⚠️ Do **not** reach for
`cross_sectional.cross_sectional_rank` and stop there — `min_width` is part of the label's
definition and must travel with it.

### ✅ P0-6 · DONE 2026-08-18 — `UNI-1`, the universe travels and cannot union

`RNK-1`'s sibling: RNK-1 is the wrong COLUMN, this is the wrong POPULATION. The run folder
already records `input.universe` (150 tickers); `final_features` never reads it and would
build over `unified_schema_all`'s **781**. Filter the build to it, and put it in the table
`COMMENT` so a dataset built later cannot silently widen it.

✅ **Both shipped, 151 tests passing across the three affected packages.** `RNK-1`: one
definition of the label, asserted equal to `cross_sectional_rank` at **atol = 0**; thin
dates dropped and counted; `metadata.json → target.column` now means what `y` IS.
`UNI-1`: the universe is a GROUP KEY, so two populations collide on the table name and
**raise** instead of unioning; `build_sql` emits `WHERE base.ticker IN (…)`; the COMMENT
carries eight names and a sha1. `final_features` had **no tests at all** and now has seven.

⚠️ **P1-5 is unblocked and has NOT been run** — that was the instruction.

---

## P1 — unblocks hours of other work

### ⚠️ P1-5 · IN PROGRESS — stages 5 and 6 DONE 2026-08-18, the model is next

**`final_features --apply`** built `unified_schema_all.rank_20day__final__d20_h20` in
**7.3 s**: 624,448 × 17, **150 tickers**, 2009-01-02 → 2026-08-07, 621,448 labelled. The
two VCB tables were reported `exists=True, fingerprint matches` and skipped, which is why
no `--scope` was needed. ⚠️ **First real exercise of `UNI-1`'s fix**: the plan carried all
150 names and the DDL emitted `WHERE base.ticker IN (…)` — 624,448 rows and not 2.39 M is
the proof it fired. The `COMMENT` carries the universe and its sha1 `301aeb491d`.

**`train_test_creator --ticker all --save`** built the dataset in **10.9 s**:

| split | windows | dates |
|---|---|---|
| train | 422,251 | 2009-02-05 → 2021-02-01 |
| val | 91,462 | 2021-04-05 → 2023-09-20 |
| test | 93,224 | 2023-11-15 → 2026-07-10 |

13 features kept, **0 dropped**; 3,000 unlabelled rows dropped — exactly `150 × 20`, the
h=20 tail of each ticker, which is the arithmetic check that the tail is per-ticker and
not global. **0 rows too thin to rank.**

⚠️ **AND THIS IS `RNK-1` PROVEN ON THE ARTEFACT RATHER THAN IN CODE.** The banner printed
*"y is 'cs_rank_20day', RE-RANKED within each date from 'return_20day'"* — the first time
that path ran through `read()`, which the unit tests could not reach — and the saved
tensors settle it: **excess kurtosis −1.199 (train) and −1.200 (test)**, the theoretical
value for a UNIFORM distribution, bounded at ±1.720 after standardisation. A 20-day
return is strongly leptokurtic. Before the fix `y` would have been that return.

⚠️ Read at training time: `evidence = cleared_p95_not_a_pass=1, no_null=1`, and **drift —
2 of 13 channels put >1 % of the test set beyond 5 train-sigmas** (0 put all of it there).

**Left: `n_features: 13`** into a new `configs/lstm__all__rank_20day__final__d20_h20.yaml`
— that key is an ASSERTION `train.py` raises on, which is why it could not be written
before the dataset existed — then `model.lstm`, then `result_evaluator`.

### ~~P1-5 · Run the model chain on the top-150 shortlist~~ ⏱ ~1 h, ~~blocked on P0-5 + P0-6~~

**The question this repo has never answered.** Twice now a selection has cleared an honest
bar and the model below it has shown nothing (§5d, P2-3) — and `RNK-1` says that on a
cross-section the model was aimed at the wrong label both times, so those two data points
do not settle it. This is the first chance to ask it properly: a shortlist of 13 channels
behind **z = +9.09**, the strongest selection evidence in the repo.

```powershell
python -m final_features --apply                        # -> rank_20day__final__d20_h20
python -m train_test_creator --table rank_20day__final__d20_h20 --save
python -m model.lstm --config configs/lstm__all__rank_20day__final__d20_h20.yaml
python -m result_evaluator
```

⚠️ **NO `--scope`, and an earlier draft of this item was WRONG to say otherwise.**
`--scope` names EVERY table in the plan, not the one you meant: `--scope liquid150` was
measured on 2026-08-18 planning `close_adjust_5day__final__d20_h5__liquid150` and
`return_5day__final__d20_h5__liquid150` as well — two junk duplicates of VCB tables that
already exist. Plain `--apply` builds only what is missing: the plan reports the two VCB
tables as `exists=True` and skips them. A scope is for separating two runs that COLLIDE
on a name, and nothing collides here — `unified_schema_all.rank_20day__final__d20_h20` is
a name no other group wants.

⚠️ **On a panel, quote the daily-IC t-stat, never `ic_clears`** — `NUL-3`, the evaluator's
panel null is not label-neutral. ⚠️ And read `mase` beside it: P2-3's model cleared nothing
and lost to "predict no change" at `mase 1.068`, which is the line that mattered.

### ✅ P1-4 · DONE 2026-08-18 — the VRAM half is fixed; **the next wall is HOST RAM**

Shipped: `gpu.rank_block_columns` + a blocked `_average_ranks_torch` + a blocked
`_spearman_vector_cuda`. ⚠️ **Chunking the rank helper alone would not have been enough**
— the old path also handed a full `n × p` rank matrix to `_pearson_against_last`, which
builds five more `n × p` tensors. Every stage is `O(n × p)`, so every stage had to become
`O(n × block)`.

| verified three ways | |
|---|---|
| 4 new tests (11 in the file) | blocked == dense at **0.0**, blocks of 1/2/5/36/37/100 and the whole vector path at 1/3/16 |
| the 30-name smoke run, **through 2 blocks** | reproduces exactly: 60 kept, `ic_mean +0.0263`, trend +0.0297, shortlist 22 — and 0.6 s against 0.7 s, so blocking is free |
| `rank_block_columns(4_266, 600)` | **1 block** — every archived run keeps the dense path |

**On the T4 it worked**: phase 3 went from an OOM to **12.3 s**. Then the kernel **died in
phase 4 with no traceback** — `DeadKernelError: Kernel died`, which is a SIGKILL from the
cgroup, not a catchable CUDA error. So the binding constraint moved from VRAM to **host
RAM**, which is `MEM-1`'s original half (**P3-2**).

⚠️ **AND NOTHING IN THE RUN SAID HOW MUCH MEMORY IT WAS USING**, so the diagnosis was an
inference — which §5 rule 2 forbids leaving as one. `selector._tick` now prints
`rss=… vram=…` per phase (one `psutil` call per phase, nine per run). Measured on the
30-name smoke panel, 48,521 rows: RSS **0.8 → 2.3 GB**, peaking at `stability`.

### P1-4b · Cut the host-side peak so the top-300 panel fits ⏱ see P3-2

⚠️ **THE FIRST EXTRAPOLATION HERE WAS WRONG AND THE SECOND MEASUREMENT KILLED IT.** It
read *"~1.5 GB of the smoke run's RSS is data over 48,521 rows, so the top-300 panel is
25.7× the rows → ~39 GB"*. The top-150 run then ended phase 4 at **11.0 GB on 624,448
rows**, and a straight line through both points predicts **20.6 GB** for top-300 — under
the box, not 10 GB over it. **One point does not fit a line**, and scaling a peak from a
tiny panel treats a large fixed cost as if it were per-row.

⚠️ **AND THE SECOND FIT IS NOT TO BE TRUSTED EITHER, FOR A DIFFERENT REASON: `rss` is
sampled BETWEEN phases.** The top-300 run died *inside* phase 4, so whatever killed it
was never printed. `selector._tick` now also reports `peak=` — the OS high-water mark
(`peak_wset` / `ru_maxrss`) — which is the number that decides whether a run survives.
✅ **AND THE 2026-08-18 NULL RUN REPORTED IT.** On top-150 the phases read `rss=11.2G`
but **`peak=16.3G`** — the high-water mark is **45 % above where the run settles**, and it
is reached inside `rank (the ensemble's methods)`, which is exactly where the top-300 run
died and exactly what an end-of-phase sample could never see. `window design` has the same
shape: 7.3 G settled, **10.8 G peak**. Doubling the rows puts top-300's peak at
**~28-30 GB against a ~29-30 GB box**, so that kill is now explained by a measurement.

⚠️ **Both earlier extrapolations were wrong, in opposite directions, and for the same
reason: each scaled a quantity that was not the binding one** (~39 GB from one tiny panel,
then ~20.6 GB from settled RSS). **top-300 needs the streaming design (P3-2), not a trim.**

### ~~P1-4 · Chunk the GPU rank step~~ — **promoted from P3-2 by measurement** ⏱ done

**The one thing between this repo and P2-1 v2 / P2-2.** It was structural code under P3
until 2026-08-17, when the T4 run made it the binding constraint on the only two
measurements left worth making. The priority rule at the top of this file promotes it:
*a thing that unblocks hours of other work outranks a thing that is only itself.*

`gpu._average_ranks_torch` holds `values` + `filled` + a mask (**10.58 GiB** at ~536
float64 columns × 1.247 M rows) and then `torch.sort` asks for its own output plus an
int64 `order` — **~4× the design**, against a T4's 14.56 GiB. Rank in **column blocks**:
ranks are per-column independent, so the result is bit-identical, which `float32` is not
(P0-3, 52% relative change in `ic_mean`).

⚠️ **Do not promise that this alone unblocks the run.** The T4 died in phase 3 of 9; the
next phase, `rank (the ensemble's methods)`, has never been reached at this width and
`permutation` is the member that puts the design on the device. Expect to chunk twice.
Verify against the 30-name smoke payload — a chunked ranking that changes a number is a
bug, and that run's `ic_mean +0.0263 / 60 kept` is the reference.

### P1-1 · Re-fit the cost model into ONE function ⏱ ~2 h

Two models exist, disagree, and were both fitted with `lasso` — dropped 2026-08-16:

| model | predicted the 644-ch / 10-draw run at | actual |
|---|---|---|
| Dagster guard `1.1 × (ch/113)² × (1+draws)` | **393 min** | **29.7 min** |
| `CONTEXT` §15c `0.364 × ch^0.77` | ~53 min/pass | ~3 min/pass |

Needs a **draw coefficient** (draws skip `stability` and the holdout, so `(1 + draws)` is
wrong) and a **raggedness term** — exponent ~0.83 fits the well-behaved runs while the
1,406-channel `usa` run sits **6× off**, likely rule 23's all-NaN slices rather than width.

⚠️ **The guard's premise is falsified**: CLAUDE.md says `usa` is "7.2 h with no null"; it
ran **35 min 12 s**. Rewrite the raise message with the measured number.

**Payoff:** a 20-draw null on each of the 19 country pools becomes **~2-3 hours**, not the
~1,000 CPU-hours `EVD-1` is scoped at. This is what makes EVD-1 closable.

### P1-2 · Fix `PNL-2` ⏱ half a day

Cheapest fix in the issue register. Derive `cross` from the panel's own `ticker` count, as
resolved `PNL-1` already made the SCORER do. No chicken-and-egg: the read happens before
`build()`.

⚠️ **It partly dissolves `CSP-1` for free** — once grain comes from the data, the `else`
branch reads via `reader.join(pools)`, so `--ticker ALL --pools pool__basic,pool__X
--target return_5day` becomes a real cross-sectional multi-pool run. `daily_ic` is
Spearman per date, so ranking `return_5day` within a date is the same metric as
`cs_rank_5day`; only the ranker *fit* differs.

### ✅ P1-3 · DONE 2026-08-17 — panel mode runs, and the FIRST rehearsal found the existing job broken

The worker side is `src/feature_selection/RUN__cross_sectional_panel.ipynb` + the
`cross-sectional` job. It re-implements no selection: it loads `panel.parquet` and calls
`feature_selection.run.run_selection` through a new `provided_panel` argument
(`run.ProvidedPanel`), which replaces **the read and nothing else**.

| measured, before any quota was spent | |
|---|---|
| `kgpu export cross-sectional` | **2m 04s** → 1,247,098 × 104, **477.4 MB**, 300 tickers, 4,388 dates |
| `kgpu rehearse cross-sectional` | **16.0 s**, both mount layouts, `n_eff = 218` |
| the notebook's OWN cells, end to end | ✅ 30 names / 48,521 rows, through the real bootstrap: **2m 11s**, 60 kept, shortlist 22, `source_table from metadata` |
| `feature_selection` tests | 113 passed |

⚠️ **THE REHEARSAL NEVER RUNS THE NOTEBOOK'S OWN CELLS** — it drives cell 0 and then
re-creates the panel path itself, so a defect in the notebook would have surfaced only
after the queue. Hence the third row: the built notebook was executed against a cut-down
payload with `KGPU_INPUT_DIR` / `KGPU_WORK_DIR` set — the same seam `rehearse` uses. ⚠️ A
cut-down panel is a **smoke test and never a measurement**; its `cs_rank` is still the
rank over the 300 exported names.

⚠️ **`KGP-1`, found in 3.6 s by the first rehearsal and fixed:** the payload never shipped
`src/utils` because `kgpu_bootstrap` **stubs** `utils`, and `report.py` gained
`from utils import runtime` on 2026-08-15 — **after this integration's only green round
trip**. The `feature-selection` job had been broken on the worker for two days with
nothing saying so. A stub is now installed only when the real module is **not importable**.

⚠️ **One more, unrelated to Kaggle and found by the test suite:** `ranker_eval.ALL_TARGETS`
never received the three `*_20day` labels `run.ALL_TARGETS` gained with the 4-week horizon
(`e87a3fa7`), so a scorecard run could have offered `return_20day` as a candidate FEATURE.
The two lists are one list now.

**What is left is the RUN, not the mode:** `kgpu data cross-sectional` (477 MB upload),
then `kgpu run cross-sectional`. That is P2-1 v2.

---

## P2 — new measurements worth having

### ⚠️ P2-1 · REWRITTEN 2026-08-17 — the first version was a bad experiment

**What it said:** "run the chain at a 4-week horizon", where *the chain* is the VCB
single-stock chain. **That does not reproduce the evidence it cites.** CLAUDE.md §2a-bis
measured `controls` on **top-30 / top-100 CROSS-SECTIONS rebalanced weekly**, not on one
stock as a time series.

And it makes the binding constraint worse. `n_eff = n/h` on VCB:

| horizon | `n_eff` | purge gap |
|---|---|---|
| h=5 | 852 | 24 rows |
| h=10 | 426 | 29 rows |
| **h=20** | **213** | 39 rows |

`feature_selection` §6d priced the single-ticker study at ~850 independent observations
and said **1,500 were needed**. Running VCB at h=20 takes the constraint that is already
binding and tightens it **4×**, to test a result measured under a different design.

### ⚠️ P2-1 (v2) · `cs_rank_20day` on the top ~300 by turnover — **NOT RUN**, blocked on P1-3

At h=20 the independent count is 213 **at either grain** — what differs is the QUALITY of
each observation:

| | one observation is | its sd |
|---|---|---|
| VCB, h=20 | one stock's ±1 sign | ~1.0 |
| top-300 cross-section, h=20 | an IC over ~300 names on one date | **~0.06** |

That is §2b's mechanism exactly: width buys **precision per observation**, `1/√N`, not
more observations. So h=20 kills the single-stock study and does **not** kill the
cross-section.

It combines the four things separate measurements have each pointed at:

- the **horizon** §2a-bis says works (4-13 weeks, not 5-10 sessions);
- the **grain** §2b says is the only one that ever cleared a null (≥100 names);
- the **liquidity tier** the 2026-08-17 reversal probe says is the real variable
  (t = −18.60 all names → −10.43 top 300 → **−1.96** top 100);
- and `cs_rank_20day` carries the `cs_` prefix, so it takes the correct path regardless of
  `PNL-2`.

⚠️ **The universe must be chosen from data available BEFORE the evaluation window**, or
"top 300 by turnover" is look-ahead: picking today's liquid names and applying them to
2010 is the same defect as a point-in-time index list, which §2c already records.

⚠️ **THE FOURTH CLAIM — "it fits in memory today" — IS FALSIFIED, and it was checked
wrong.** I checked RAM. **VRAM is the binding constraint**: the local pilot CUDA-OOMed in
`gpu.spearman_vector`, asking for **1.01 GiB on a 4.00 GiB card**. That is `MEM-1` on the
device side, and it is why this item now routes through Kaggle:

| | this machine | Kaggle T4 |
|---|---|---|
| VRAM | 4.0 GiB ❌ | **14.6 GiB** ✅ |
| RAM free | ~7 GB ❌ | ~29 GB ✅ |
| the design, 1.25 M × 104, float64 | ~12.8 GB | fits |

### ⚠️ P2-1 v2 · RUN AND FAILED 2026-08-17 — the T4 OOMed, and **the fifth claim was checked wrong too**

Uploaded (477 MB, dataset v1) and pushed with `RUN_NULL=false`. It reached the GPU and
died at **3m 28s**:

| phase, on the T4 | |
|---|---|
| payload mount + source unpack + reader swap | ✅ |
| `panel.parquet` loaded | 1,247,098 × 104, 1.57 GB, 300 tickers, **`n_eff = 218`**, density 0.947 |
| `prepare + coverage` | 6.4 s |
| **`window design`** | **189.3 s** |
| `spearman vs target` | ❌ **CUDA OOM: tried to allocate 4.98 GiB, 3.86 GiB free, 10.70 GiB already in use of 14.56** |

⚠️ **"The design is ~12.8 GB, so a 14.6 GiB T4 fits" priced the DESIGN and not the STEP.**
`gpu._average_ranks_torch` holds `values` + `filled` + a mask — 10.58 GiB at ~536 float64
columns × 1.247 M rows — and *then* `torch.sort` asks for its own output plus an int64
`order`, roughly another 10 GiB. **The step needs ~4× the design, so no card this side of
an A100 80 GB runs it as written.** That is the same error as the RAM-not-VRAM one
recorded above, one level down: I checked the wrong quantity again.

**The fix is exact, not approximate: rank in COLUMN BLOCKS.** Ranks are per-column
independent, so chunking `spearman_vector` changes no number — unlike `float32`, which
P0-3 measured at a **52% relative change in `ic_mean`** and which is forbidden here. It is
`MEM-1` on the device side and it is now the only thing between this repo and the
measurement §2a-bis has been pointing at since 2026-08-03. ⏱ ~2 h to chunk and re-verify
against the smoke run, then one more T4 round trip.

⚠️ **Panel mode is NOT what failed and must not be re-opened.** Everything `kgpu` adds ran
on the worker; what died is a ranker step at a width no single-ticker run has ever reached.

### ⚠️ SECOND ATTEMPT, 2026-08-18 — one wall further, and a DECISION was needed

With P1-4's chunking, the same job on the same payload got **past** the step that killed
it: `spearman vs target` **12.3 s** where it had OOMed. `window design` 198.1 s. Then
phase 4 of 9 — `rank (the ensemble's methods)` — took the kernel down with
`DeadKernelError` and no traceback: a host-RAM kill, ~39 GB wanted against ~29-30 GB.

**Two honest ways forward, and they are not equivalent:**

| | cost | what it costs the RESULT |
|---|---|---|
| **(a) top-150 by turnover**, everything else identical | one round trip, today | `n_eff` stays **218** — dates are unchanged. Daily-IC sd goes ~0.058 → ~0.082, so **z scales by ~0.71**. Still above §2b's ~100-name threshold |
| **(b) fix `P3-2` first**, then run top-300 | ~a day of streaming work | nothing — full power, `z` as designed |

⚠️ **Halving the DATES instead would cost exactly the same `z` (both scale it by √½), and
would cost `n_eff` as well** — 218 → 109. Prefer cutting names; they are the axis that
buys precision, not independence.

### ⚠️ THIRD ATTEMPT, 2026-08-18 — **IT RAN**, top-150, and the result has NO BAR

`2026-08-17_235146__all__basic__cs_rank_20day`, Tesla T4, **22m 53s**, 624,448 × 104,
150 tickers, 4,368 sessions, `n_eff = 218`, 90 channels → **61 kept**, shortlist 13.

| fold | 1 | 2 | 3 | 4 | 5 | mean |
|---|---|---|---|---|---|---|
| IC (selected) | +0.060 | +0.124 | **+0.153** | +0.104 | +0.097 | **+0.1075** |
| R² | −0.038 | +0.007 | +0.021 | +0.007 | +0.012 | — |

`ic_trend_per_fold` **+0.0054** (flat, not decaying — §5 rule 5), `hit_rate` 0.536,
`ic_fold_sd` 0.0342. **All five folds positive**, and R² is positive in four of five —
which §5c's eleven single-stock models never managed once.

⚠️ **AND `null: None`.** Rule 2: `evidence=no_null` is an **unknown, not a pass**, and
§2b's whole finding is that the observed IC barely moves while the noise floor collapses
— so the bar is the entire question and this run does not answer it. ⚠️ **Do not read
+0.1075 against §2b's bars** (VN100 +0.0117, LIQUID301 +0.0245): those were measured at
`h=5`, and at `h=20` each fold carries `n_eff = 38.1`, so this run's own null will be
**wider**. It must be its own.

**The null is now priced, which is what this run bought.** One pass 1,355 s, of which
`stability` 187 s is skipped by a draw → **~19.5 min per draw, ~6.5 h for 20**. Against
Kaggle's 12 h session cap and 27.6 h of weekly quota, that is **affordable in one
session** — the first time a 20-draw null on a real cross-section has been.

⚠️ **`permutation` is 726 s, 54 % of the run**, and §19 measured it as the one
load-bearing ensemble member, so it cannot be dropped to buy the null.

### ✅ THE 20-DRAW NULL — DONE 2026-08-18, **z = +9.09**, and my prediction was half right

`RUN_NULL=true, N_NULL=20` on the same payload. Each draw shuffles the label in **date
blocks** (`cross_sectional.shuffle_dates`, `mode="date_block"` — each stock keeps its own
returns, moved to a different fortnight) and re-runs the whole selection on it.

**RESULT — 6 h 07 m on a T4, 0 failed draws:** null mean **+0.0291**, sd **0.0086**, p95
bar **+0.0388**, null MAX **+0.0410** (below the observed, so rule 3 does not fire),
**z = +9.09**, p = 0.0476 (the 1/21 floor — read z). **It clears.**

⚠️ **THE PREDICTION WAS RIGHT ON THE BAR AND WRONG ON `z`, and the wrong half is left
here rather than edited.** Predicted bar +0.02 … +0.05 → actual **+0.0388** ✅. Predicted
z +2 … +6 → actual **+9.09** ❌ — I hedged the range upward "for safety" while my own
reasoning in the same paragraph implied a fold noise of ~0.013 and therefore a much
tighter null. **Padding a prediction is not conservatism; it is a worse prediction.** The
null sd came in at 0.0086.

⚠️ **And the number to quote is not +0.1075.** The null's mean is +0.0291, so the excess
over a shuffled label is **+0.078**. See CLAUDE.md §2b-bis for the four things this does
not settle — chiefly that there is **no holdout**, and that §2c records the VN100 result
clearing its bar and then failing exactly that test.

*(The original prediction, kept verbatim: "it WILL clear, with a p95 bar around
+0.02 … +0.05 and z between +2 and +6.")* The reasoning, so
that being wrong is informative:

- a daily IC over 150 names has sd ≈ `1/√150` ≈ **0.082**, and each fold averages ~760 of
  them, so even at 20× inflation for label overlap a fold's IC noise is ~0.013 — an order
  below the observed +0.1075;
- §2b's ladder is the precedent: LIQUID301 observed +0.0768 against a bar of +0.0245;
- the selection here keeps **61 of 90** channels, far less aggressive than the two-layer
  funnel P0-1 had to price in, so there is less selection for the null to absorb.

**What would falsify that reading, and it is not nothing:** rule 23's data-arrival
signature (this panel starts in 2009 and `drv_*` channels need 252 sessions of history),
and the fat-tailed nulls §10d records — `p = 1/21` is the floor either way, so **read `z`,
not `p`**.

⚠️ **A SEPARATE THING TO CHECK IN THAT RUN'S OUTPUT**: `ic_summary.se_ic_per_fold` reads
**0.1642** ≈ `1/√38.1`, which is the SINGLE-SERIES formula — it assumes a daily IC of sd
1.0, i.e. one stock's ±1 sign. On a 150-name cross-section the sd is ~0.082, so that
column appears to over-state the error bar by ~√N ≈ 12×. If so it is `PNL-1`'s family at
the summary instead of the scorer, and it is **conservative** (too wide), which is why it
has never manufactured a result — but it should not be quoted as this run's error bar.

### P2-2 · `cs_rank_5day` on the top ~300 by turnover ⏱ ~1 h

`read_universe_panel` already takes a `tickers` list and filters in SQL, so this is a CLI
flag, not a new schema. ~1.3 M rows — ⚠️ **the same width as P2-1 v2, so assume the same
4 GiB VRAM ceiling and the same P1-3 dependency** until measured otherwise. Puts a number
against §2b's `ALL` row, which reads **"never ran — ⚠️ unverified"** at IC +0.109.
⚠️ Today's measurement says liquidity is the variable: the 5-day cross-sectional reversal
runs `t = −18.60` over all names, `−10.43` at top 300, **`−1.96` at top 100**.

### ✅ P2-3 · DONE 2026-08-17 — the selection cleared its bar; **the model did not**

`lstm__vcb__return_5day__final__d20_h5__20260817-205952`, 6.0 s, 228,225 parameters,
57 features × 20 × 2,921 training windows.

| | val | test |
|---|---|---|
| `ic` | +0.0611 | **+0.0858** |
| `ic_bar` (200 draws) | +0.1124 | **+0.1232** ❌ |
| `ic_p` | 0.184 | 0.109 |
| **`dir_auc`** | 0.5276 | **0.4974** — a coin |
| **`mase`** | 1.073 | **1.068** — LOSES to the naive |
| `skill_score` | −0.073 | −0.029 |
| `r2` | −0.081 | −0.031 |
| `calibration_slope` | 0.252 | 0.408 |
| best epoch | **1 of 21** | val loss rose from the first epoch on |

**This answers the question the run existed to ask.** The shortlist behind it cleared a
bar that priced in BOTH selection layers (P0-1, z = +3.86) — the strongest selection
evidence this repo has produced — and the model built on it shows **no skill on either
split**, loses to a zero-return naive on `mase`, and never beat its own first epoch.
CLAUDE.md §5d's sentence reproduced on better evidence: *"The selection cleared its bar;
the model did not clear its own."*

⚠️ **Two things ARE better than the `close_adjust_5day` chain, and both are about the
TARGET, not the model.** Both splits now agree in SIGN (+0.061 / +0.086) where the level
target gave −0.459 / +0.488 — one error bar straddling zero. And `dir_auc`, `hit_rate`
and `mase` are all **readable** here because a return is two-signed; on the level target
ROC AUC did not exist at all.

⚠️ **`mase > 1` on both splits is the line to quote.** A model trained on the best
shortlist this project has assembled does not beat "predict no change".

## P3 — structural code, only pays off for runs currently blocked

| item | what | note |
|---|---|---|
| **P3-1** | `CSP-1` — give `read_universe_panel` the `UnifiedSchemaReader.join()` the single-ticker path uses | ⚠️ makes `MEM-1` worse by the width joined; `pool__ta` at 922 channels is ~10× the design |
| **P3-2** | `MEM-1`, **host half only** — stop materialising the whole design; window per fold or per ticker-chunk, never holding the blocks and the `pd.concat` result at once | 4.03 GB per million rows, measured. ⚠️ **The DEVICE half was promoted to P1-4**; and on a T4's ~29 GB of RAM this host half did **not** bite — the 1.25 M-row design built in 189.3 s |
| **P3-3** | `_ingest_gold_stocks` still carries a legacy column (`close`/`volume` check: 1 of 2 present) | ⚠️ Fixing it redefines `gold.stocks`' OHLC as adjusted and commits to a ~2.4 M × ~900 col rebuild. Its own decision, not a side effect. Related to `STA-1` |

---

## P4 — hygiene, each item distorts one number or hides one failure

| item | what | status |
|---|---|---|
| **P4-1** | **`STA-1` costs the chain its last 31 sessions** — `pool__ta` stops 2026-06-26, and the INNER join drops the whole chain 4,266 → **4,235 rows**. The `return_5day` table and dataset end 2026-06-25 | measured 2026-08-17 |
| **P4-11** | ⚠️ **`pipeline`'s `selection_2` ROW DESCRIBES A DIFFERENT EXPERIMENT AND CALLS IT `up to date`.** Measured 2026-08-18: `python -m pipeline --ticker all --table rank_20day__final__d20_h20` reports *"2 layer-2 run(s) over `pool__shortlist__rank_20day__d20_h20`"* and then names `2026-08-17_011642__vcb__shortlist__return_5day__d20_h5__return_5day` — a different schema, a different target and a different pool. The layer-2 detection is not scoped to the chain being asked about, so a stage that has never run for this chain reads green. ⚠️ Related trap in the same table, working as designed but worth knowing: the **`model` row keys on `--config`, not on `--table`**, so without one it reports the DEFAULT chain's run as up to date — pass `--config` or `--apply` will skip the model stage while saying everything is fine. | measured 2026-08-18 |
| **P4-2** | Confirm `validation.csv` emits **`n_dead_train` / `n_dead_test`** and read them for `pool__news_daily` — rule 23, its channels are entirely NULL before 2013 | the 2026-08-17 layer-2 `validation.csv` showed no such column |
| **P4-3** | **262 rows in `bronze.cafef_price` have `high < low`** (e.g. ACB 2018-07-31: high 35,800 low 36,500). CafeF's defect, surfaces in gold as a negative `range_hl`. Needs a bronze data-quality screen, not a gold patch | ⚠️ **re-verified 2026-08-17, still 262.** Probably deserves an ISSUES.md code |
| **P4-4** | XGBoost warns in **every** run: *"Falling back to prediction using DMatrix due to mismatched devices — running on cuda:0, input data on cpu"* | if the design is copied host→device per prediction, the GPU conversion is leaving speed on the table |
| **P4-5** | `landed()` cannot answer "did THIS run produce anything" — it rglobs a folder where the previous run's dated files still sit. 140 header-only CSVs went green (2026-07-31) | this is §5 rule 10's mechanism; fix is to compare against the run's own outputs |
| **P4-6** | `logs/app.log` has many writers now — the executor is multiprocess and every step appends, so records interleave | fix is per-process filenames in `Logger`, **not** going back to sequential |
| **P4-7** | `raw/trading_view` partitions `crypto` and `options` are permanently red — both `true` in config, folders never existed, `landed(require=True)` fails them | choose `require=False` or accept two red partitions |
| **P4-8** | Decide the fate of `raw/trading_view_collected_links` — nothing reads it | it is a leaf, not a hub |
| **P4-9** | ⚠️ If ever backfilling TradingView, use a **single-run backfill**. `tag_concurrency_limits` is per-RUN, so 9 partitions the default way is 9 runs × 8 browsers = **72 Chrome** | `.dagster/dagster.yaml` is empty |
| **P4-10** | Four heavy assets have never been observed running end to end through Dagster: `trading_view_links`/`data`, the 5 CafeF stock tabs + news, `cafef_pdfs` (100 partitions), `cafef_financials` (~2.4 h each) | *"built is not run"* |

---

## Closed — recorded so they are not reopened

| what | why closed |
|---|---|
| **News sentiment scorer** (old items 7-16: annotation, LLM labelling, PhoBERT fine-tune, LIME gate, full panel) | ⛔ **Decided against 2026-08-03 and confirmed 2026-08-17.** 7 paired tests, every \|t\| < 1.3; adding news costs 2-8 pp CAGR for ΔMCC ±0.003. The one reason to continue — coverage — was tested on the top-30 most-covered tickers and did not survive. The event-count half is now `pool__news_daily` and it measured `z = +0.53` at layer 1 |
| **Silver leaf assets** (old item 17: bonds, forex, funds, indices, gics) | ✅ all five exist |
| **Gold leaf assets** (old item 18: bonds, forex, funds) | ✅ all three exist |
| **`switch_config.json` cleanup** (old items 22, 23) | ✅ moot — the file is gone (§5a); a leftover copy now RAISES |
| **`execution.finished_at = None`** in every `metadata.json` | ✅ **working as designed** ([runtime.py:329](src/utils/runtime.py#L329)) — `summary()` is called mid-run because `write_report` writes the file, and waiting for `stop()` would record a runtime of zero. `None` "rather than a guess" is §5 rule 2 at the clock. I called it a bug on 2026-08-16 and was wrong |
