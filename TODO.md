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

## P1 — unblocks hours of other work

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

---

## P2 — new measurements worth having

### P2-1 · ⚠️ Run the chain at a 4-WEEK horizon ⏱ ~2 h

**The highest-value untried experiment in the repo, and it was buried in a todo file.**
CLAUDE.md §2a-bis: the `controls` block loses to its benchmark at `rel5`/`rel10` and beats
it by **30.39% vs 18.07% CAGR at 4 weeks (Sharpe 1.10)**, positive in 30 of 30 folds. Four
independent threads have failed at `h=5`; **nothing has ever been run end to end at
`h≈20`.** Needs `pool__targets` to carry a 20-session horizon (`UNIFIED_TARGET_HORIZONS`).

### P2-2 · `cs_rank_5day` on the top ~300 by turnover ⏱ ~1 h

Fits today with **none of C fixed** — `read_universe_panel` already takes a `tickers` list
and filters in SQL, so this is a CLI flag, not a new schema. ~1.3 M rows. Puts a number
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
| **P3-2** | `MEM-1` — stop materialising the whole design; window per fold or per ticker-chunk, never holding the blocks and the `pd.concat` result at once | 4.03 GB per million rows, measured |
| **P3-3** | `_ingest_gold_stocks` still carries a legacy column (`close`/`volume` check: 1 of 2 present) | ⚠️ Fixing it redefines `gold.stocks`' OHLC as adjusted and commits to a ~2.4 M × ~900 col rebuild. Its own decision, not a side effect. Related to `STA-1` |

---

## P4 — hygiene, each item distorts one number or hides one failure

| item | what | status |
|---|---|---|
| **P4-1** | **`STA-1` costs the chain its last 31 sessions** — `pool__ta` stops 2026-06-26, and the INNER join drops the whole chain 4,266 → **4,235 rows**. The `return_5day` table and dataset end 2026-06-25 | measured 2026-08-17 |
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
