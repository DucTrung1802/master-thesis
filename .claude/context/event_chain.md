# Context — `src/event_chain`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — read that first; this file is the depth
> behind one package. Built 2026-09-16.

> **The EVENT chain: which sessions is a ticker about to rise by at least g % within h
> sessions?** A binary label, parameterised in ONE place (`src/utils/event_target.py`), run
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
| key | `run_id`, `trial_id`, `started_at` |
| data | `ticker`, `data_table`, `data_from`, `data_to` |
| features | `feature_pools`, `n_features`, `lookback_d`, `feature_selection` |
| target | `target`, `target_definition` |
| split | `split_ratio`, `purge_gap`, `train/val/test_period`, `train/val/test_samples`, `train/val/test_positive_rate` |
| model | `model`, `model_variant`, `hyperparameters` (JSON: the `model:` block minus `type`, plus `train:`), `n_params`, `best_epoch`, `seed` |
| run time, hardware | `run_seconds`, `fit_seconds`, `device`, `gpu` |
| results | `val_auc`, `test_auc`, `test_auc_null_p95`, `test_auc_z`, `test_beats_null`, `test_pr_auc`, `test_precision_top10pct`, `test_brier_skill`, `test_precision/recall_at_val_threshold`, `chosen_on_val` |

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

**Not one val-chosen model clears its test null**, and the val→test drop is 0.15-0.20 AUC in
every trial. Across all 36 runs the only test AUC above its own p95 is the debug LSTM's
0.638 vs 0.635 — one of 36, which is what chance gives at a 95th-percentile bar (`NUL-1`).
⚠️ **The full table is WORSE for the networks and the logistic baselines** (val AUC 0.45-0.54
against 0.59-0.68 on 11 channels): its 101 `pool__basic_bank` channels include peer price
LEVELS (`hose__acb__close_adjust`, `hose__ssb__close_raw`, …) — measured on its dataset, **108 of 183
channels put >1 % of TEST beyond 5 train-sigmas and 3 put ALL of it there** (`train_test_creator` §6) — which saturates a
linear or neural score — `baseline_logistic_stats_c01` scores a constant (AUC exactly 0.500) and
`mlp_h16` a Brier skill of −15.9. Trees are scale-free and survive it.
