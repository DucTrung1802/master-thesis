# Context — `src/event_chain`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — read that first; this file is the depth
> behind one package. Built 2026-09-16.

> **The EVENT chain: WHICH NAMES DO I BUY TOMORROW MORNING so that they are +g % h sessions
> later?** ONE setup since 2026-09-18 (`--setup basket`, §7): the LIQUID panel of 228 names, at
> most X of them per session, scored after the close of N and bought at the OPEN of N+1.
> ⚠️ **The three close-to-close setups (`window`, `tabular`, `tabular_mbb`) and the close-priced
> basket were DELETED the same day** — §6 is what they proved and where to recover them. A binary label, parameterised in ONE place (`src/utils/event_target.py`), run
> end to end on the repo's own stages — Dagster for the data, `feature_selection` for the
> selection, `final_features` for the table, `train_test_creator` for the tensors,
> `model.<arch>` for the fits — with an event-specific report on top.

```
python -m event_chain                                   # the plan — writes nothing
python -m event_chain --apply                           # every stage, skipping what already ran
python -m event_chain --apply --stages select --pools pool__basic,pool__event_features
python -m event_chain --apply --stages final,dataset,train,report
python -m event_chain.trial                             # the TRIAL LOG — every attempt, one row each
python -m event_chain.trial --show <trial_id prefix>    # one attempt's record
python -m pytest utils/test_event_target.py event_chain/test_trial.py -q   # no database
```

## 0. The parameters — `src/utils/event_target.py`

| constant | default | meaning |
|---|---|---|
| `EVENT_GAIN_PCT` | `5.0` | the rise, in percent |
| `EVENT_HORIZON` | `5` | trading sessions ahead |
| `EVENT_RULE` | `"close"` | `close`: `close[t+h] >= (1+g)·close[t]` → column `up_5pct_5day`; `any`: `max(close[t+1..t+h]) >= (1+g)·close[t]` → `upany_5pct_5day` |

⚠️ **Changing one means re-materialising three assets, in order**, and nothing else:

```
dagster asset materialize -f src/orchestration/definitions.py --select "unified/pool__targets" --partition VCB
dagster asset materialize -f src/orchestration/definitions.py --select "gold/stocks_event_features"
dagster asset materialize -f src/orchestration/definitions.py --select "unified/pool__event_features" --partition VCB
python -m event_chain --apply
```

Every consumer READS the module — the SQL label (`EventTarget.sql`), the columns
`feature_selection.run.is_label` refuses as features (by list AND by pattern, so an older
parameter set's column is still refused), the table name, `train_test_creator`'s
classification switch and the model configs. ⚠️ **Adjusted close only** — `pool__basic`'s
`open/high/low` are RAW prices (VCB 2009-06-30: high 60,000 vs `close_adjust` 9,060), so a
touch-the-high rule would read every split as a crash.

⚠️ **The label is NULL exactly where `close[t+h]` does not exist**, for BOTH rules. The `any`
rule could be decided early by a touch on day t+1 and deliberately is not: a tail that is
labelled for some rows and not others makes the SAMPLE SET depend on the future path.

## 1. What each stage does, and the package that owns it

| stage | owner | writes | the rule it enforces here |
|---|---|---|---|
| data | Dagster: `unified/pool__targets`, `gold/stocks_event_features` → `unified/pool__event_features` | the 0/1 label; ~40 trailing event features | label tail = `h` rows exactly (asserted in the asset AND the ingest) |
| select | `feature_selection.run.run_selection` — **one pool per run** | `reports/feature_selection_event/<run>/` | `holdout_start` = the dataset's VAL start, so the ranking never reads a val/test row |
| final | `final_features.builder.build_all(root=reports/feature_selection_event)` | `unified_schema_vcb.up_5pct_5day__final__d20_h5` | `MAX_TABLE_COLUMNS = 1600` checked on the PLAN before any DROP |
| dataset | `train_test_creator.TrainTestCreator` | `src/train_test_set/vcb__up_5pct_5day__final__d20_h5__tr70_val15_test15__std/` | an event target resolves `scale_target=False`, `task=classification`; purge `d+h-1`; refuses a val start earlier than the selection holdout |
| train | `model.{baseline,gbt,forest,lstm,gru,cnn,mlp,tcn}.train` | `src/model/runs/<run_id>/` + a config per run in each package's `configs/` | estimators must expose `predict_logit` (the engine sigmoids ONCE) |
| report | `event_chain.report` | `reports/event_chain/vcb__up_5pct_5day__final__d20_h5/{report.md,leaderboard.csv,selection.csv,walkforward.csv}` | the best model is chosen on VAL ROC-AUC; its test row is read once |

⚠️ **A ROOT OF ITS OWN** (`reports/feature_selection_event`). `final_features` unions every
run under a root that shares `(schema, target, setup)`, so the event runs must not live in
`reports/feature_selection` beside other targets' shortlists (`PRB-1`'s shape).

⚠️ **`--root` IS ANCHORED AT THE REPO ROOT, NOT THE CWD.** `run_selection` resolves a relative
root against `report.REPO_ROOT`, so the runbook's `--root ../reports/feature_selection_probes`
typed from `src/` lands in `D:\GIT\reports\…`, OUTSIDE the repo — measured 2026-09-16 and
removed; `D:\GIT\reports\feature_selection_wide` (2026-08-20) is the same accident, older.
The chain passes an absolute path.

## 2. ⚠️ `result_evaluator`'s `dir_auc` is not the event's AUC

For a classifier, `result_evaluator.evaluate_run` measures its core block against the
realised RETURN read from `pool__targets` — so `dir_auc` is the AUC for `return_5day > 0`,
not for the +5 % event. That keeps a classifier on the regression leaderboard; it answers a
different question. `event_chain.report.event_metrics` computes the event's own ROC-AUC,
PR-AUC, precision in the top 10 %/5 % and Brier skill against the train base rate, with a
1,000-draw BLOCK shuffle (block `d + h`) as the AUC's null.

## 3. The new feature group — `gold.stocks_event_features` / `pool__event_features`

~40 channels per `(exchange, ticker, date)`, every one TRAILING (windows end on the row's
own date), parameterised by the event's `(g, h)`:

| prefix | channels |
|---|---|
| `evt_` | trailing event rate over 60/120/250 sessions, the down-move rate, realised vol 10/20/60/120, **the threshold in σ·√h units** (`evt_thr_z_20/60`), vol ratio, last h-session return, 60-session max/min h-return, h-range, distance to the 20-session high/low, turnover z, sessions since the last event |
| `sec_` | the GICS industry group (the 20 banks for VCB), **leave-one-out**: h-return, relative h-return, event breadth and its 60-session mean, dispersion |
| `mkt_` | the whole cross-section: event up/down breadth, median h-return, dispersion, 60-session breadth |
| `cal_` | weekday and month (sin/cos), days to month end, days since quarter end, gap days, days to/since Tết, days to the VN30F expiry |

⚠️ **Two columns were built and REMOVED before the first run**: `sec_n` (a sector's width
grows with listings — a tree splitting on it reads the calendar, `mkt_n_names`' trap,
TODO `P0-4`) and a foreign-flow ratio whose numerator and denominator were in different
units (−2.8e8 on VCB). ⚠️ **The 0/1 flags are cast to `double precision` before `AVG`**: a
`1.0` literal is NUMERIC in PostgreSQL, AVG over it is numeric, and psycopg2 returns that as
`Decimal` → pandas `object` (§5 rule 15) — caught on the first test build.

## 4. Two silver defects the refresh hit, both the same shape (fixed 2026-09-16)

`bronze.cafef_financials_*` was ingested BEFORE the parser wrote the `months` column
(`MTH-1`), and the silver code assumed it:

| asset | failure | fix |
|---|---|---|
| `silver/cafef_financials_bank` | `KeyError: 'months'` in `_helper_cast_columns` | cast `months` only when present |
| `silver/stocks_basic_financials_bank_fa` | `AttributeError: 'numpy.float64' object has no attribute 'notna'` — `pd.to_numeric(q.get(absent))` is a scalar | an absent column is an all-NaN Series, which the code's own rule already leaves alone |

⚠️ Neither was visible until a silver rebuild ran on a bronze that predates `months`: a
silver table built BEFORE the column existed kept loading, so rule 11 hid it.

## 5. ⚠️ THE TRIAL LOG — one row per model run of a COMPLETE trial (2026-09-17)

```
reports/event_chain/
    trials.csv                      THE LOG: one row per MODEL RUN (derived — see below)
    trials/<YYYYmmdd-HHMMSS>__<ticker>__<table>/
        trial.json                  THE RECORD of one trial (schema_version 2)
        leaderboard.csv selection.csv walkforward.csv report.md
        predictions.csv             every model's val/test `date, y_true, y_prob` (6 sig. digits)
```

⚠️ **ONLY COMPLETE TRIALS ARE LOGGED** (the user's decision, 2026-09-17): a trial is the
`report` stage run on a dataset with trained models, and `python -m event_chain --apply` records
it with no flag (`--notes` says why; `--no-trial` only for debugging). A probe, a crashed attempt
or a data refresh has no data/feature/target/split/model/result and is NOT a row — the first
version of this log carried six such rows and they were deleted. ⚠️ **`trials.csv` IS DERIVED**:
`trial.rebuild_log()` regenerates it from every `trials/*/trial.json`, so a row cannot disagree
with its folder and deleting a folder deletes its rows.

**`trials.csv` holds only these columns** (`trial.LOG_COLUMNS`):

| group | columns |
|---|---|
| which stock — ⚠️ FIRST (2026-09-17, the log holds VCB and MBB) | `exchange` (read from the labelled rows, oldest first, `HNX>HOSE` for a mover), `ticker` |
| key | `run_id`, `trial_id`, `started_at` |
| data | `data_table`, `data_from`, `data_to` |
| features | `feature_pools`, `n_features`, `lookback_d`, `feature_selection` |
| target | `target`, `target_definition` |
| split | `split_ratio`, `purge_gap`, `train/val/test_period`, `train/val/test_samples`, `train/val/test_positive_rate` |
| model | `model`, `model_variant`, `hyperparameters` (JSON: the `model:` block minus `type`, plus `train:`), `n_params`, `best_epoch`, `seed` |
| run time, hardware | `run_seconds`, `fit_seconds`, `device`, `gpu` |
| results | `val_auc`, `test_auc`, `test_auc_null_p95`, `test_auc_z`, `test_beats_null`, `test_auc_refit_train_val` (+ p95), `test_auc_rolling_refit` (+ p95, §7b), `test_pr_auc`, `test_precision_top10pct`, `test_brier_skill`, `test_precision/recall_at_val_threshold`, `chosen_on_val` |

⚠️ **The five VCB `trial.json` files were BACKFILLED with `universe.exchange = "HOSE"`**
(2026-09-17), read from `unified_schema_vcb.pool__targets` — the only edit to a logged trial.

⚠️ **`run_seconds` has two sources**: `model.common.engine` records `timing: {fit_seconds,
run_seconds}` in every run's `metadata.json` since 2026-09-17; the two trials before that read it
off the run folder's file times (`config.yaml` → `results/metrics.json`), and `fit_seconds` is
blank for them. ⚠️ **`trial.json` keeps what the CSV leaves out** — git commit, dirty files and
`code.digest` (SHA-256 of the 36 files that decide the numbers), the environment, pool freshness,
the label's base rate by year, every selection run with its null, the dataset's drift, each run's
config and `result_evaluator` block, the blend and the walk-forward. ⚠️ **`predictions.csv` lets a
trial outlive its run folders** (`RPR-1`): re-scored from the file alone, the debug trial's
ExtraTrees test AUC is **0.5644**, the logged value.

**The three complete trials, 2026-09-16/17** — 36 model runs, one event (`up_5pct_5day`),
the same split (train 2,946 / val 617 / test 641 samples; positive rate 0.134 / 0.083 / 0.051):

| trial | features | best on val | val AUC | test AUC | test null p95 |
|---|---|---|---|---|---|
| `20260916-235618` debug (6 models) | 82: basic, economy_vietnam, event_features, news_daily, stock_market | ExtraTrees leaf50 | 0.700 | 0.564 | 0.661 |
| `20260917-010551` compact | 11: event_features only | ExtraTrees leaf20 | 0.719 | 0.520 | 0.646 |
| `20260917-013926` full | 183: the 6 pools that cleared their null (+ basic_bank) | RandomForest leaf30 | 0.694 | 0.549 | 0.661 |
| `20260917-032538` **tabular** (§6) | 211 at d = 1: all channels of `event_features`, `basic`, `market_context`, `market_breadth` | **`ensemble_geo3`** (fixed) | 0.799 | **0.660** | 0.644 (max 0.795) |
| `20260917-041045` **tabular v2** (§6d) | the same 211, + 20 BANK names as training rows for one member | **`ensemble_geo4`** (fixed) | 0.803 | **0.670** | 0.639 (max 0.785) |

**Not one val-chosen model clears its test null**, and the val→test drop is 0.15-0.20 AUC in
every trial. Across all 36 runs the only test AUC above its own p95 is the debug LSTM's
0.638 vs 0.635 — one of 36, which is what chance gives at a 95th-percentile bar (`NUL-1`).
⚠️ **The full table is WORSE for the networks and the logistic baselines** (val AUC 0.45-0.54
against 0.59-0.68 on 11 channels): its 101 `pool__basic_bank` channels include peer price
LEVELS (`hose__acb__close_adjust`, `hose__ssb__close_raw`, …) — measured on its dataset, **108 of 183
channels put >1 % of TEST beyond 5 train-sigmas and 3 put ALL of it there** (`train_test_creator` §6) — which saturates a
linear or neural score — `baseline_logistic_stats_c01` scores a constant (AUC exactly 0.500) and
`mlp_h16` a Brier skill of −15.9. Trees are scale-free and survive it.

## 6. What the three CLOSE-TO-CLOSE setups proved, and why they were deleted (2026-09-18)

⚠️ **THE `window`, `tabular`, `tabular_mbb` AND CLOSE-PRICED `basket` SETUPS WERE DELETED ON
2026-09-18** — their configs, model configs, datasets, reports and trials. Every one of them was
trained on `up_5pct_5day`, a label that BUYS AT THE CLOSE OF SESSION N, and a decision taken from
session N's own close can first be traded the next morning: the picks gap up **+1.72 %** overnight
(**+4.44 %** on the names at their ceiling) and the close-priced track's Sharpe of 1.28 became
**0.26** when it was re-priced at the open of N+1. The label was the defect, so the flow was
replaced rather than patched (§7). **Everything below is recoverable with
`git show c845f440 -- <path>`** — that is the last commit before the deletion.

| deleted setup | what it measured | the verdict it leaves behind |
|---|---|---|
| `window` (VCB, d=20) | 5 trials, 45 model runs, eleven architectures | val-chosen test AUC **0.520-0.564** against a block-shuffled null p95 of 0.646-0.661 — no pass |
| `tabular` (VCB, d=1, all channels) | linear kinds + a BANK-panel member | best ensemble test AUC **0.670** vs null p95 0.639 (refit 0.680), ⚠️ null MAX **0.785**, z +1.93, 2024 inverted — above the bar, NOT a pass |
| `tabular_mbb` (MBB, d=1) | a grid tuned on CV (0.665 → 0.739; ~330 candidates never passed 0.741) | val 0.743, **test 0.630** vs p95 0.616, max 0.719 — not a pass |
| `basket` (LIQUID, 228 names, ≤ 5 per session) | the first flow that WORKED: `ensemble_boost_eq`, test AUC **0.662** (within-session 0.638, quarterly refit 0.668), hit@5 **0.262** on a 0.121 base, lift 1.9-2.2× in every walk-forward year, and with a val-chosen P(event) cut of 0.32 precision **0.378**, Sharpe@50 **1.28**, CAGR +72 % | ⚠️ **priced at a fill nobody can get.** Its Sharpe also depended on ceiling names (`CLF-1`): without them 0.06 uncut, 0.55 at the cut |

⚠️ **The two `NUL-1` charges against those numbers stand**: the test split was read once per trial
and the second basket trial's grid was chosen on a CV of ~25 candidates, with a choice rule changed
after the first trial's test read. They are quoted here as history, not as a live result.

**What carried over into §7**, because it was measured and not assumed: the panel is the only place
this label is predictable at all (a single ticker never cleared its null, six times); `d=1` tabular
beats a window; the boosted ensemble beats one XGBoost beats trees beats linear kinds; a basket of
at most X names with a P(event) cut beats a fixed X; and a name at its ceiling on N is bought
(`BASKET_EXCLUDE_CEILING = False`, the user's decision 2026-09-17) because the screen lost on val.

## 7. ⚠️ THE FLOW TODAY — the basket a morning order can buy (2026-09-18)

**The question, in the user's own execution:** session N closes, the data is scraped and scored that
evening, and the order goes in the NEXT MORNING. So the label is
`upopen_5pct_5day` — **1 when `close_adjust[t+6] >= 1.05 × open_adjust[t+1]`** — bought at the open
of N+1, held 5 sessions, sold at the close of N+6, and the money metrics are priced the same way
(`config.BASKET_ENTRY = "next_open"`). ⚠️ **The table is named `__d1_h6__`**: the `h` in a table
name is the label's SPAN, because every purge in this repo is `d + h - 1` read off that name
(§5 rule 6). The holding is 5 sessions; `event.describe()` is where that is stated.

| stage | what it did | cost |
|---|---|---|
| `pool__targets` (Dagster, LIQUID) | the three event rules + `return_open_{h}day` | 1.5 min |
| `select` | 2 pools, within-date IC, 10 null draws each — `pool__event_features` 67 → **59** (IC 0.1238, null p95 0.0500, z **24.6**), `pool__basic` 84 → **57** (0.1268, 0.0433, z **21.1**) | 37 min |
| `final` → `dataset` | `unified_schema_liquid.upopen_5pct_5day__final__d1_h6__bsk` → `liquid__upopen_5pct_5day__final__d1_h6__bsk__tr70_val15_test15__std` (hash `53228aec2e679487`, **151 channels**, purge 6 sessions) | 4 min |
| `train` + `report` | the 9-model grid and 6 fixed ensembles, then `event_chain.basket` (200-draw nulls, quarterly rolling refit, yearly walk-forward, the P(event) cut) | 56 min |

### ⚠️ What the GPU is worth here, measured rather than assumed (2026-09-19)

The card is an RTX 3050 Laptop, 4 GB. **Feature selection has been on CUDA all along** (its own
`gpu.py`: spearman 1.9 s, gain+SHAP 5.9 s, permutation 43.8 s of a 24.7-min pool run — the rest is
the 10 null re-selections). What was NOT is the model grid, and the answer is one model deep:

| fit, 419,923 x 151 -> a 906-column design | cpu | cuda | |
|---|---|---|---|
| `gbt_d4` | 138.8 s | **42.0 s** | **3.3x** — flipped to `cuda` |
| `forest` as `xgbrf` (200 x depth 12) | 372.6 s | **371.6 s** | ⚠️ **1.00x — the card buys nothing** |
| `forest` as `et` (sklearn ExtraTrees, what it replaced) | 1,103.8 s | — | no CUDA path exists (cuML is Linux-only) |
| `event_boost_xgb_d8`, `event_boost_mag_d8` | — | — | ✅ already `cuda` since they were added |
| `event_linear` x3 | 1.5-5.2 s | — | sklearn; a port would buy ~10 s |
| the 200-draw nulls | ~4.4 min per report | — | numpy `lexsort` + `bincount`, already vectorised |

⚠️ **THE FOREST'S 3x IS THE ESTIMATOR AND NOT THE DEVICE** — 1,103.8 s -> 371.6 s is ExtraTrees
giving way to XGBoost's random forest, and that same forest is 372.6 s on the host. ⚠️ **AND THE
FIRST MEASUREMENT OF IT SAID 20x AND WAS WRONG**: it fed the raw 151 columns to XGBoost, while
every caller goes through `window_statistics` first, which at `d = 1` returns **906** columns. A
benchmark that skips a transform the production path always runs is timing a different model.

⚠️ **The lever that is left is not a device at all** (`WST-1`): of those 906 columns only **152 are
distinct** — `last`/`mean`/`min`/`max` are four identical copies and `slope`/`sd` are 302 constant
zeros — so every tree model pays 6x, and `max_features` / `colsample_bytree` are drawing their
decorrelation from a set that is five-sixths duplicate. Fixing it changes what the models ARE, so
it belongs to a re-run and not to a speed-up.

Splits: train 419,503 rows (base **0.182**, 2009-01-02 → 2021-04-19), val 139,699 (**0.209**), test
147,527 (**0.134**, 2023-12-14 → 2026-08-13). Trial
`20260918-125131__liquid__upopen_5pct_5day__final__d1_h6__bsk`.

⚠️ **THE MODEL IS CHOSEN ON THE WITHIN-SESSION AUC** (`BASKET_CHOOSE_ON = "val_daily_auc"`,
2026-09-18): the ROC-AUC computed INSIDE each session and averaged. ⚠️ **THE FIRST VERSION OF THIS
PARAGRAPH JUSTIFIED IT BY SAYING A BASKET CANNOT TRADE TIMING, AND THAT IS FALSE** — the reason is
real and the argument was not, so it is corrected here rather than quietly rewritten. **A basket
with a cut DOES trade timing**: a session none of whose names clears `min_prob` is not traded at
all, which is a decision about the SESSION and not about a name.

**Measured the same day** (`scratchpad/which_auc.py`, read-only, over the chosen row's 659 test
sessions): the session's own score level predicts that session's base rate at Spearman **+0.250**
for the top-5 mean (+0.183 for the max), against a 200-draw permutation of the sessions' base
rates — **p95 +0.068, z +6.43**. And the cut spends it: at `min_prob` 0.36 the 200 traded sessions
carry a base rate of **0.166 against 0.134** over all 659, while the basket hits 0.330. **So of the
+0.196 total edge, +0.032 is choosing the SESSION (16 %) and +0.164 is choosing the NAME (84 %)**
— at the old 0.25 cut timing was +0.006, and at no cut it is 0 by construction.

**The real reason the pooled AUC cannot be the choice metric is that it BLENDS the two** in
proportions nobody sets: it scores name-sessions against each other across sessions, so its excess
over 0.5 is part selection and part timing, and the mixing weights depend on how many names a
session holds and how much base rates vary — properties of the UNIVERSE, not of the model. ⚠️ **Its
null sits at 0.548, not 0.5**, and that 0.048 IS the blend showing itself: a score that knew only
which sessions were eventful would read ~0.55 pooled and exactly 0.500 within-session
(`test_basket.py` pins both ends). A number you cannot attribute cannot be optimised.

⚠️ **THE MODEL-SELECTION EVIDENCE IS WEAK AND POINTS THE SAME WAY** — over the 13 models that rank
at all, Spearman(val metric → test outcome) is **daily 0.702 vs pooled 0.570 for basket return**,
0.602 vs 0.562 for Sharpe, 0.864 vs 0.645 for test daily AUC — **but 0.446 vs 0.749 for hit@5**,
and with n=13 CORRELATED models (six are ensembles of the other nine) `SE(Spearman) ≈ 0.29`, so
**not one of those gaps is established**. What the rules actually picked: daily AUC →
`ensemble_boost` (+1.00 % per basket, Sharpe 0.55), pooled AUC and val hit → `ensemble_boost_eq`
(+0.76 %, 0.39), and **the best test row, `event_boost_xgb_d8` at +1.15 % and Sharpe 1.01, was
found by no rule at all.** The choice metric is not what limits this chain.

⚠️ **NEITHER AUC SEES THE THING THE CUT ACTUALLY DEPENDS ON** — both are rank metrics and the cut
is a level. The proof is in this run: swapping an arithmetic ensemble for a geometric one left the
ranking almost identical (val daily AUC 0.6348 → 0.6357) and moved the val-chosen cut **0.25 →
0.36**. So the metric set is three, not one: **within-session AUC for the name, a session-level
correlation for the timing, Brier/reliability for the cut.**

⚠️ **This is the FOURTH choice rule** (`val_hit` → `val_auc` → `val_auc` → `val_daily_auc`) and it
was changed after test numbers had been read, which is `NUL-1`'s shape: every earlier row is still
in `trials.csv`.

**Chosen on val within-session AUC: `ensemble_boost`** (XGBoost, the event logit, the tree
magnitude model — geometric mean). ⚠️ **The top three are inside 0.001** (`ensemble_boost` 0.6357,
`ensemble_xl` 0.6357, `ensemble_boost_eq` 0.6348, the previous winner), so the flip is a TIE-BREAK
— pooled AUC 0.650 vs 0.646 — and not a demonstrated difference. What separates them is the money:
+1.01 % per basket against +1.04 % and +0.76 %.

| | val | **test** | refit (train+val) | rolling (quarterly) |
|---|---|---|---|---|
| **within-session AUC** (the choice metric) | 0.636 | **0.632** (within-session shuffle null p95 0.505, **z +44.7**) | 0.631 | 0.632 |
| pooled AUC (⚠️ not tradeable) | 0.650 | **0.655** (null p95 0.548, z +58.0) | 0.658 | 0.658 |
| hit@5 (base 0.134) | 0.352 | **0.266** (null p95 0.141, max 0.150, z +25.3, lift **1.99**) | 0.256 | **0.273** (lift 2.04) |

**With the val-chosen cut `min_prob = 0.36`** (at most 5 names, cash when none clears it):

| test basket | sessions traded | names | precision (null p95 · z) | basket ≥ 5 % | basket ret | EV/session | Sharpe@50 (worst offset) | CAGR | max DD | without ceiling names |
|---|---|---|---|---|---|---|---|---|---|---|
| frozen | 24.4 % | 2.34 | **0.379** (0.231 · +9.4) | 0.373 | +4.37 % | +0.95 % | 0.92 (0.38) | +46.7 % | −36.2 % | 0.378 · +4.03 % · 0.77 |
| **rolling (quarterly)** | 30.3 % | 2.29 | **0.408** (0.227 · +11.8) | 0.385 | **+3.02 %** | +0.76 % | **0.73 (0.32)** | **+30.6 %** | −45.9 % | **0.417 · +2.92 % · 0.58** |

⚠️ **THE CUT MOVED 0.25 → 0.36 WITH THE MODEL, AND THAT IS THE CUT DOING ITS JOB, NOT A BETTER
ONE**: `ensemble_boost` is a GEOMETRIC mean and `ensemble_boost_eq` an arithmetic one, so the same
ranking carries a different probability SCALE — the cut is chosen on val EV either way and lands
where that scale puts it. ⚠️ **It now trades 200 of 659 sessions, so `n_eff` is ~40 independent
baskets and the z falls 23.0 → 11.8** even though the precision rises 0.305 → 0.408; the EV per
session is **unchanged at +0.76 %**. ⚠️ **And the track is CONCENTRATED**: 117 of those 200 rolling
sessions are 2025 (+5.08 % per basket) against 2026's 54 at **−0.29 %** and 2024's 24 at −0.06 %,
so the +30.6 % CAGR is one good year plus cash.

✅ **TRAINING ON THE TRADEABLE LABEL IS WORTH ~3× THE MONEY**: the deleted close-priced model's own
picks, re-priced at the open of N+1, gave precision 0.308 but **+0.78 % per basket, Sharpe 0.26,
CAGR +2.4 %** (§6, and `EXE-1` in ISSUES.md). The new model ranks slightly worse on paper and earns
**+3.02 %, Sharpe 0.73, CAGR +30.6 %** — it stops paying for names whose move happens overnight.
✅ **AND `CLF-1` NO LONGER BINDS**: dropping every name that closed at its ceiling on N leaves
precision 0.417 and Sharpe 0.58 (against 0.408 / 0.73 with them), because the ceiling gap is now
OUTSIDE the label. The old flow lost 1.28 → 0.13 the same way.

**Walk-forward, yearly expanding refits of the chosen row** — lift over the buyable base in every
one of six years, and the return is not: hit 0.439 / 0.343 / 0.300 / 0.250 / 0.310 / 0.235 on bases
0.265 / 0.206 / 0.170 / 0.124 / 0.150 / 0.122 (**lift 1.66-2.07, z +10.1 … +16.4**), basket return
+5.20 / +0.19 / +1.67 / +0.80 / +1.59 / **−0.35 %** and Sharpe@50 5.76 / −0.03 / 1.53 / 0.73 / 0.65
/ **−0.41** (2021 … 2026), within-session AUC **0.612-0.652 in all six**. ⚠️ **2026 is negative on
the money in every read of this chain** while the universe is −0.43 %.

⚠️ **What still qualifies every number here**: `NUL-1` — the grid, the ensembles and the cut rule
were carried over from a search done on the deleted close-priced flow, so this test split is not
the first read of that search; the universe is survivors-only and not point-in-time; the max
drawdown of the cut track is ~46 %; the cut trades 30 % of sessions, which is ~40 independent
baskets; and the cut's own grid on test is descriptive — **only the val-chosen 0.36 is the
result**.

## 8. ⚠️ THE N+5 LABEL — what the user actually trades (2026-09-19)

⚠️ **§7's `upopen_5pct_5day` SELLS ONE SESSION LATER THAN THE USER DESCRIBED.** Asked to confirm
*"check data Friday, buy Monday's open, sell Friday's close"*, the arithmetic on disk said
otherwise: `upopen`'s `h` counts sessions AFTER THE ENTRY, so a Friday signal buys Monday and sells
**the following Monday** — six sessions and a second weekend. Verified on PNJ 2025-01-10: N+5
(Friday 01-17) returns **+1.60 %** and N+6 (Monday 01-20) **+2.55 %**, and 2.55 % is what
`pool__targets` stored. **The extra session was 0.96 pp of the move.**

**`uphold_5pct_5day` is the rule that holds exactly `h` sessions** — `close[t+h] >= (1+g) ·
open_adjust[t+1]`, bought at the OPEN of N+1 and sold at the CLOSE of N+5. Both labels are carried
in `pool__targets` (the user asked to ADD, not replace), and `EVENT_RULE` picks the one the chain
trains on. Base rate over the panel **0.159 against `upopen`'s 0.177** — one fewer session to reach
+5 %. Table `uphold_5pct_5day__final__d1_h5__bsk`, purge 5, trial
`20260919-033720__liquid__uphold_5pct_5day__final__d1_h5__bsk`.

Splits: train 419,923 (base **0.164**, → 2021-04-22), val 139,928 (**0.188**), test 147,536
(**0.117**, 2023-12-15 → 2026-08-14). Both pools clear their null unchanged (`pool__event_features`
59 channels, IC +0.1271, z +26.65; `pool__basic` 56, +0.1325, z +14.59).

**Chosen on val within-session AUC: `ensemble_xl`** (XGBoost twice + the event logit).

| | val | **test** | refit (train+val) | rolling (quarterly) |
|---|---|---|---|---|
| **within-session AUC** | 0.643 | **0.644** (null p95 0.506, **z +43.8**) | 0.642 | 0.645 |
| pooled AUC (⚠️ not tradeable) | 0.656 | 0.662 (null p95 0.553, z +56.5) | 0.664 | 0.665 |
| hit@5 (base 0.117) | 0.342 | **0.232** (null p95 0.126, max 0.131, z +21.7, lift **1.99**) | 0.234 | 0.236 |

**With the val-chosen cut `min_prob = 0.30`:**

| test basket | sessions traded | names | precision (null p95 · z) | basket ret | EV/session | Sharpe@50 (worst) | CAGR | max DD |
|---|---|---|---|---|---|---|---|---|
| **frozen** | 42.2 % | 3.09 | **0.289** (0.170 · +12.5) | **+1.98 %** | +0.62 % | **0.65 (0.43)** | **+24.9 %** | −43.6 % |
| refit | 45.4 % | 3.17 | 0.283 (0.168 · +11.8) | +1.54 % | +0.47 % | 0.56 (−0.06) | +19.6 % | −39.0 % |
| rolling | 49.0 % | 3.04 | 0.291 (0.172 · +12.3) | +1.45 % | +0.47 % | 0.53 (0.27) | +16.8 % | −44.0 % |

Walk-forward, six yearly expanding refits: lift **1.74-2.15 in every year**, within-session AUC
0.617-0.664, basket return +4.71 / +0.72 / +1.53 / +0.76 / +1.26 / **−0.25 %** (2021…2026). ⚠️
**2026 is negative again**, as it is on every read of this chain, and at the chosen cut 2025 carries
169 of the rolling track's 323 traded sessions.

⚠️ **THIS IS NOT COMPARABLE TO §7's NUMBERS AND THE DIFFERENCE IS TWO CHANGES, NOT ONE.** The label
moved (N+6 → N+5) and the DESIGN moved the same day (`WST-1`: 906 columns → 151), so the gap
between `upopen`'s +3.02 % per basket and `uphold`'s +1.45 % cannot be attributed to either. What
can be said: the within-session AUC is **higher** (0.644 vs 0.632) while the money is **lower**, and
selling a session earlier gives up the second weekend on a label whose base rate falls 0.177 →
0.159. Whether the shorter hold is worth it is a question for a paired comparison nobody has run.

### ⚠️ What `WST-1` and the GPU were worth, measured on this run

| model | before (906 cols, cpu) | after (151 cols, cuda) | |
|---|---|---|---|
| `forest` (ExtraTrees → `xgbrf`) | 1,103.8 s | **34.1 s** | **32.3x** |
| `gbt_d4` | 92.6 s | **6.1 s** | 15.1x |
| `gbt_d2` | 63.5 s | **5.1 s** | 12.4x |
| `event_boost_xgb_d8` | 24.0 s | 21.6 s | 1.1x — already `cuda`, no `window_statistics` |
| `event_linear` x3 | 1.5 / 5.2 / 3.8 s | 1.6 / 5.1 / 3.2 s | 1.0x — neither applies |
| **whole grid** | **1,320.2 s** | **76.9 s** | **17.2x** |

⚠️ **THE SPLIT IS THE POINT: every model that sped up is one that goes through
`window_statistics`, and every model that did not is one that does not.** The card alone was worth
3.3x on `gbt_d4` and **1.00x on the forest** (371.6 s cuda against 372.6 s cpu at 906 columns) — so
`WST-1` is most of this, and "move it to the GPU" was the wrong diagnosis honestly tested. Stage
times: select 52 min (unchanged — it never had the defect), train **8 min** (was 32), report
**21 min** (was 59).

**Run it**: `python -m event_chain --setup basket --apply` (runbook `G8`),
`python -m event_chain.basket --pick YYYY-MM-DD` for one session (`G9`), `--min-prob-study` to
re-choose the cut without refitting (`G10`). ⚠️ **The report stage alone is ~59 min** (2026-09-18):
the leaderboard is cheap and the refits are not.

## 9. ⚠️ ONE LOSS, AND WHAT EVERY RUN NOW RECORDS (2026-09-19, code only — not yet run)

⚠️ **THE GRID CARRIED THREE LOSS FUNCTIONS AND ITS RUN FOLDERS DESCRIBED THEM WITH ONE
SENTENCE.** `training.criterion` read `"train log-loss, single fit (no training loop)"` for a
`binary:logistic` classifier, a `reg:squarederror` regressor and a `rank:pairwise` ranker
alike, because it names the SHAPE of the fit and not the function. The user asked for one
loss; the answer below is **binary cross-entropy on the 0/1 event label, unweighted, with no
class weighting**, and every member now writes the objective it actually minimised.

**Why log-loss and not a ranking loss**, which matches the top-5-in-a-session decision on its
face: the decision has two halves and both are measured (`measure_auc_split.py`) — **84 % of
the edge is choosing the NAME inside a session** (a rank) and **16 % is choosing the SESSION**
through the `min_prob` cut (a LEVEL). Log-loss is a proper scoring rule, so one number serves
both. `rank:pairwise` is invariant to any monotone transform inside a query, so its scores do
not travel across sessions and the cut cannot be made at all. ⚠️ **The one thing that would
flip this is dropping the cut** and trading a fixed basket every session.

| member | was | is |
|---|---|---|
| `event_linear_mag_har_a100_hl4` | ridge on `log\|log(1+r_h)\|`, 4-year half-life, + a 1-D logistic | **`event_linear_har_c003`** — the same `har_` channels, log-loss on the event, uniform weights |
| `event_linear_dir_c001` | log-loss on a DIFFERENT label (`1{r>0}`, moves ≥ 3 %) | **removed** — and it had already measured nothing: val within-session AUC **0.5007**, test 0.5021, **z +0.68** |
| `event_boost_mag_d8` | `reg:squarederror` on the same volatility proxy | **removed** (see `BRD-1`: it had been silently absent from the board anyway) |
| `ensemble_geo3`, `ensemble_boost`, `ensemble_boost_eq` | fixed geometric means | **removed with their members** — a fixed ensemble whose member left is a different ensemble, not an adjustable one |
| `gbt_d2`/`gbt_d4`/`event_boost_xgb_d8` | `n_estimators` 300 / 300 / 800 | a **cap of 2,000** with `early_stopping_rounds = 50` |

⚠️ **EARLY STOPPING IS ON VAL, WHICH NOW CARRIES THREE JOBS** (the user's decision, taken over
carving the stop block out of train): val chooses the model (`BASKET_CHOOSE_ON`), chooses
`min_prob`, and stops the boosting. **So `val_daily_auc` is a selection score and no longer an
out-of-sample estimate of anything** — TEST is the only honest read. ⚠️ **And the refit paths
have no val at all** (`refit_test` fits on train+val, the rolling refit on everything before
its block), so they cannot stop again: each **reuses the round the frozen fit chose**,
`training.best_epoch + 1`, with stopping switched off. One mode, two paths.

⚠️ **THREE FAMILIES CANNOT EARLY-STOP AND THAT IS A PROPERTY OF THE FAMILY, NOT A GAP**: a
forest is ONE boosting round (`xgbrf`: `num_parallel_tree` at full step size), a logistic is
convex with `C`/`alpha` as its capacity knob, and the prior does not fit. They write a
**one-row** `loss_history.csv`, because a missing file must always mean a bug.

### 9a. The artefacts a chart is drawn from

Every run folder now writes three more files, and `trial.json` copies them into
`trials/<id>/curves/` — **run folders are gitignored and `RPR-1` deleted 29 of them once.**

| file | columns | note |
|---|---|---|
| `results/loss_history.csv` | `step, train_loss, val_loss` | `step` is an EPOCH on the torch path, a BOOSTING ROUND under XGBoost, `0` for a single fit |
| `results/feature_importance.csv` | `feature, importance` | ⚠️ gain for a tree, the SIGNED standardised coefficient for a linear model — **not one scale** |
| `results/calibration.csv` | `split, bin, n, mean_pred, observed` | 10 **equal-count** bins; the cut is a level, and no AUC can see whether the level means anything |

`trial.json` is **schema 3**: a `loss` block (the grid's rule, why, what would flip it, and
each run's own recorded objective), `setup.declared_models` beside `setup.scored_models`
(`BRD-1`), `selection.runs[].columns_in_design` — the **feature → pool mapping** the flat
151-name list never carried — and `predictions_wide.csv.gz` now holds a column per ENSEMBLE,
which is the one model the file used to omit because it has no run folder.

### 9b. ⚠️ `d` LEFT 1, AND THE BILL IS NOT THE DL TRAINING

`BASKET_LOOKBACK = 10` (was 1) so LSTM/TCN have something to read — at `d = 1` a sequence
model is an MLP with extra machinery. Members added: `mlp_h32`, `lstm_h32`, `tcn_c32`, and
`ensemble_xl_seq` declared **now**, before any `d > 1` row is scored, because adding it after
would be `NUL-1`. What it costs, all of it re-run:

- the window tensor is `d`× bigger — 420k × 10 × 151 is 2.5 GB as float32 and the engine casts
  to **float64**;
- `WST-1` short-circuits `d == 1` **only**, so `gbt`/`forest` go back to a legitimate
  906-column design and the **1,320 s → 77 s that fix bought is spent again**;
- the selection layer builds six statistics per channel instead of collapsing to one;
- the purge becomes `d + h − 1 = 14` (§5 rule 6), not 5.

⚠️ **10 is a choice, not a standard**: the repo's documented pairing is `d = 20, h = 5`,
measured on a ONE-TICKER series of ~4k rows, not a 228-name panel of 420k. ⚠️ **And the
windowed grid these members come from LOST** — 15 models over 3 VCB trials, val-chosen test
AUC 0.520-0.564 against a null p95 of 0.646-0.661 — **on one ticker. That difference is the
whole hypothesis, and nothing here has been measured on this panel.**

### 9c. ⚠️ THE FIRST `d = 10` TRIAL — worse on every read, and `d` is not why (2026-09-19)

Trial `20260919-192250__liquid__uphold_5pct_5day__final__d10_h5__bsk`, on `2b429834`: **10 models
declared, 14 rows scored (10 + 4 ensembles), none missing** (`BRD-1`'s guard). Chosen on val:
**`ensemble_xl`** again, cut `min_prob = 0.28`.

| same label `uphold`, chosen `ensemble_xl` | val dAUC | test dAUC (z) | hit@5 | frozen: active · prec · basket · EV · Sharpe@50 · CAGR · maxDD | rolling: basket · Sharpe · CAGR |
|---|---|---|---|---|---|
| `d = 1` (03:37) | 0.6433 | **0.6436** (+43.8) | 0.232 | 42.2 % · 0.289 · **+1.98 %** · +0.62 % · **0.65** · **+24.9 %** · −43.6 % | +1.45 % · 0.53 · +16.8 % |
| `d = 10` (19:22) | 0.6356 | 0.6384 (+41.9) | 0.226 | 49.6 % · 0.274 · **+0.24 %** · −0.13 % · **−0.17** · **−13.2 %** · −59.3 % | +1.11 % · 0.34 · +6.6 % |

⚠️ **THE CHOSEN MODEL READS NO WINDOW, SO `d` CANNOT BE WHY IT FELL.** `ensemble_xl` is
`event_boost_xgb_d8`² · `event_linear_evt_c003`, and both read the LAST ROW only. What changed
for it: the **boosting round count** — early stopping on val log-loss took `event_boost` from
800 fixed rounds to **209 (best 159)** — and ~0.9 % fewer train rows (the purge is 14 and each
name's first 9 sessions drop). ⚠️ **Plausibly the loss/metric mismatch §9 warned about**: val
LOG-LOSS bottoms out at round 159 because it prices the probability LEVEL, while the
within-session RANK may keep improving past it. **Untested** — a paired `event_boost` with
early stopping off on this dataset (~3 min) is the measurement that would settle it.

⚠️ **THE DEEP-LEARNING MEMBERS LEARNED IN ONE EPOCH AND THEN ONLY MEMORISED** — the hypothesis
behind `d = 10`, measured:

| member | best epoch | val loss: first → best → last | train loss | val dAUC | test dAUC |
|---|---|---|---|---|---|
| `lstm_h32` | **1** | 0.4730 → 0.4730 → **0.5613** | 0.4159 → 0.3474 | **0.6090** (lowest) | 0.6279 |
| `tcn_c32` | **1** | 0.4646 → 0.4646 → 0.5288 | 0.4198 → 0.3749 | 0.6294 | 0.6344 |
| `mlp_h32` | 3 | 0.4767 → 0.4678 → 0.4844 | 0.4169 → 0.3795 | 0.6185 | 0.6179 |
| trees, for scale | 123-220 rounds | best **0.4606-0.4619** | | 0.6245-0.6317 | 0.6299-0.6387 |

Even their BEST val loss is worse than any tree's, and the two lowest non-baseline rows on the
board are two of the three. **The windowed grid lost on one VCB ticker and it loses on a
228-name panel too** — the difference in rows was the whole hypothesis, and it did not hold.

⚠️ **`ensemble_xl_seq` HAS THE BEST TEST ROW AND IS NOT THE ANSWER**: test dAUC **0.6420** and
basket **+0.93 %**, but val **0.6306** — it was not chosen, and quoting its test row is
choosing on test (`NUL-1`). Walk-forward of the chosen row: lift 1.71-1.92 in all six years,
basket −0.04 % in 2022 and **−0.59 % in 2026**, Sharpe@50 negative 2024-2026.

**Costs, measured** (`MEM-2` fixed first): select **4 h 34** (2 pools) · final 1 min · dataset
3 min · train **35 min** (10 models, the DL three stopping at epoch 16-18) · report **17 min**.

⚠️ **TWO LABELS IN THIS TRIAL'S ARTEFACTS ARE WRONG, AND THE NUMBERS ARE NOT**: `gbt_d2`/`gbt_d4`
record `early_stopping: none` beside `best_round` 220/123 of a 2,000 cap (the family did not
implement `early_stopping_rule`, and the engine's fallback is `none`), and the torch curves
count steps from 0 while `training.best_epoch` counts from 1. Both fixed in code for the next
trial; this trial's files stay as written (§8: a run folder is immutable).
