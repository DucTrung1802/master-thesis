# Context — `src/event_chain`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — read that first; this file is the depth
> behind one package. Built 2026-09-16.

> **The EVENT chain: which sessions is a ticker about to rise by at least g % within h
> sessions?** Three setups (`--setup window|tabular|tabular_mbb`, §6-§7), two tickers (VCB, MBB). A binary label, parameterised in ONE place (`src/utils/event_target.py`), run
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

## 6. ⚠️ THE TABULAR SETUP — d = 1, all channels, three linear kinds (2026-09-17)

```
python -m event_chain --setup tabular --apply --notes "<why>"      # RUNBOOK G5
```

| knob | `window` (§5's trials) | `tabular` |
|---|---|---|
| `d` | 20 | **1** — every model reads the last row |
| pools | 11 raw groups | `pool__event_features` (now +27: `har_` log volatility, `px_` range, `flow_` scaled foreign flow), `pool__basic`, **`pool__market_context`** (new, US lagged one session), `pool__market_breadth`, `pool__news_daily` |
| table | the selection's SHORTLISTS unioned | **every numeric channel** of the pools whose selection cleared its null (`final_features` `channels="all"`, scope `tab`) |
| dataset | `y` only | `y` + the auxiliary target `return_5day` (`aux_*.npy`, [train_test_creator.md](train_test_creator.md) §12) |
| models | 15 (baselines, GBT, forests, 5 networks) | `model.event_linear` — `magnitude_ridge`, `event_logit`, `direction_logit` ([model.md](model.md) §18) — `model.event_panel` (XGBoost on the 20 BANK names, [model.md](model.md) §19, from v2), plus prior, GBT d2, ExtraTrees |
| ensembles | the top-3-on-val blend (never eligible) | **fixed before the run**: `ensemble_geo3` = geometric mean of magnitude, event and direction; `ensemble_geo2` = magnitude and event; from v2 `ensemble_geo4` = geo3 + the bank panel, `ensemble_geo3p` = geo2 + the bank panel |
| report | train-only fit | + **refit on train+val**, scored on test once; the walk-forward also refits every ensemble member |

### 6a. How the setup was chosen — a research harness that never chose on a test row

A scratch harness over the same VCB rows (2009-07-27 → 2026-08-14) cut **rolling-origin folds**: each
fold validates one calendar year (2014 … 2023, the 2023 fold ending at the chain's val end
2023-12-05), trains on every row before it, and purges 24 rows. Choices were made on the fold mean
(`CV10`; an early scan used the 2019-2023 folds, `CV5`). Measured:

| what | CV AUC | note |
|---|---|---|
| any model on the 183 channels of §5's full table | mean over 9 models 0.581 (CV5) | the level channels drift (`EVD-1`) |
| `pool__ta` alone | mean over 9 models 0.565 (CV5) | |
| last-row **event logit**, `evt_`+`drv_` | **0.684** CV5 (C 0.1) · 0.671 CV10 (C 0.03) | val range 0.787 |
| + the levels of 6 macro/market pools as 250-day z-scores | mean over 9 models 0.597, best 0.626 (CV5) | more channels, less AUC |
| event logit fitted on a denser label (`up3`, `up4`, `any5`) | ≤ 0.663 (CV5) | no gain |
| channels chosen inside each fold by within-year AUC stability | ≤ 0.631 (CV5) | no gain |
| **magnitude ridge** on `log|r_5|`, `evt_`+`drv_` (± VCB range/flow) | **0.705-0.716** CV5 (fold min 0.615-0.650) · 0.653 CV10 | the most STABLE single model; weak in 2014-2016 |
| + HAR log-volatility (`har_`) and a 4-year half-life | 0.671 CV10 | |
| direction logit (P(up \| ≥ 3 % move)) | 0.638 CV10 → **0.607** once US series were lagged (`TZL-1`) | the same-date join had been helping |
| Student-t distributional regression, quantile transform, splines, L1, bagged XGB | ≤ 0.665 | no gain |
| panel training scored on VCB: 20 banks (XGB d2) · VN30 (logit) · 228 liquid names (XGB d4) | 0.672 CV10 · 0.664 CV5 · 0.632 CV5 | wider is worse past the sector |
| geometric mean of the family bests (magnitude, event, direction, bank panel) | **0.695** CV10 (0.698 without the event logit) | 0.689 without the bank panel |
| the same three kinds re-measured on the PIPELINE's channels | geo3 **0.682**, geo2 0.683; magnitude 0.667, event 0.670, direction 0.607 | the numbers `config.TABULAR_*` cite |

⚠️ **THE BANK PANEL WAS LEFT OUT FOR COST, NOT ON EVIDENCE** (−0.006 CV10): it needs a second
schema's table and dataset aligned to VCB's split. ⚠️ **It went in with v2** (§6c), without a
second table: `model.event_panel` reads the peers from `unified_schema_bank` at fit time.
⚠️ **THE RESEARCH HARNESS DID PRINT TEST AUCs** for a handful of blends after they were chosen on CV
(train+val refit 0.64-0.67, walk-forward 0.61-0.64, every one with 2024 inverted at 0.14-0.43). No
configuration was changed on them; the one decision taken after they were read is the bank panel's
exclusion above. **So the chain's test number below is not a first read of 2024-2026** (`NUL-1`).

### 6b. The first tabular trial — `20260917-032538__vcb__up_5pct_5day__final__d1_h5__tab`

**Selection at d = 1** (10 draws, rows before 2021-06-22): `event_features` IC **+0.1126** (p95
+0.0354, max +0.0439, z **+4.73**), `market_context` +0.1190 (z +3.86), `basic` +0.0839 (z +3.03),
`market_breadth` +0.0553 (z +2.69) cleared; `news_daily` **−0.0218 FAILED** and is not in the table.
Table 218 channels, dataset 211 (7 constant in train), train 2,984 / val 636 / test 641 samples.

| model | val AUC | **test AUC** | test null p95 / max | test z | test AUC, refit on train+val (p95) |
|---|---|---|---|---|---|
| **`ensemble_geo3`** — chosen on val | **0.799** | **0.660** | 0.644 / 0.795 | +1.84 | 0.659 (0.641) |
| `ensemble_geo2` | 0.790 | 0.669 | 0.645 / 0.802 | +1.93 | 0.664 (0.645) |
| `event_linear_evt_c01` | 0.795 | 0.614 | 0.645 / 0.774 | +1.31 | 0.620 (0.641) |
| `event_linear_evt_c003` | 0.789 | 0.615 | 0.642 / 0.781 | +1.33 | 0.625 (0.640) |
| `event_linear_mag_a10_hl4` | 0.771 | 0.669 | 0.639 / 0.816 | +1.91 | 0.635 (0.647) |
| `event_linear_mag_har_a100_hl4` | 0.760 | **0.705** | 0.641 / 0.786 | **+2.35** | 0.688 (0.645) |
| `gbt_d2` | 0.735 | 0.623 | 0.638 / 0.784 | +1.42 | 0.637 (0.639) |
| `event_linear_dir_c001` | 0.702 | 0.401 | 0.639 / 0.721 | −1.27 | 0.475 (0.630) |
| `forest_et_leaf30` | 0.689 | 0.565 | 0.653 / 0.767 | +0.75 | 0.610 (0.650) |

⚠️ **THE VAL-CHOSEN MODEL CLEARS ITS PER-RUN p95 FOR THE FIRST TIME — AND THAT IS NOT A PASS.**
0.660 > 0.644, but the null MAX is **0.795** (§5 rule 3), `z = +1.84`, the grid is 10 runs plus
the research harness's search (`NUL-1`), and the test set holds **33 positives in a handful of
episodes**. It is +0.096 over the windowed chain's best val-chosen 0.564.
⚠️ **2024 IS INVERTED FOR EVERY KIND** — walk-forward AUC 2021 0.681 · 2022 0.798 · 2023 0.838 ·
**2024 0.292** · 2025 0.725 · 2026 0.592 (`ensemble_geo3`); 2024 had 7 events at a base rate of
0.029. ⚠️ **The best single test number is not the chosen one**: the HAR magnitude ridge scored
0.705 and was fifth on val. ⚠️ Brier skill of the chosen ensemble is **−0.30**: a geometric mean of
three probabilities ranks, it does not calibrate.

### 6c. The second research round — ~100 candidates on CV10, after the first tabular trial (2026-09-17)

Same harness, same folds, the pipeline's own channel names; every candidate wrote its test folds
to disk and **none was printed**. Measured (CV10 mean · worst fold):

| what | CV10 | verdict |
|---|---|---|
| the frozen members, for reference: magnitude · event · direction · `geo3` | 0.667 · 0.670 · 0.607 · **0.682** (0.504) | — |
| the three kinds trained on the **20 BANK names** (`unified_schema_bank` rows ≤ the fold's cut) | magnitude ridge 0.624-0.636 · event logit 0.656-0.663 · direction 0.579-0.609 | worse than VCB-only |
| **XGBoost on the BANK panel**, 17 settings (depth 1-3, 300-1,500 trees, blocks, half-life, VCB weight ×5, labels `up3`/`dir3`) | 0.600-**0.679**; d2 n600 on `har_`+`evt_`+`drv_`+`px_`/`flow_`+`mctx_`+`glb_`/`bond_` **0.675** (0.525) | the best single model; its weak years are not the linear kinds' |
| **`geo4`** = `geo3` + that XGBoost · **`geo3p`** = `geo2` + it | **0.695** (0.534) · 0.697 (0.532) — 7 of 10 folds up; every XGBoost setting gives 0.689-0.697 | ✅ went into the chain |
| greedy forward selection over the whole pool (with replacement) | 0.703 in-sample, **0.647 leave-one-year-out** | ❌ selection overfits by 0.056 — no weights were fitted |
| the kinds bagged over their neighbours (alpha, C, blocks) | `geo3` 0.682 → 0.682 | no gain |
| new blocks in each kind: USD/VND and CNY/VND changes (lagged one session, `TZL-1`), VN-Index/VN30 order imbalance, HNX/UPCOM state, bank-sector medians (leave-VCB-out) | every one −0.013 … +0.003 | no gain |
| log Rogers-Satchell/Garman-Klass vol (`lvol_`) | event 0.674, magnitude 0.670 | noise |
| vol-only designs (HAR + index vol, no `drv_`/`evt_` rates) | magnitude 0.605-0.622 · event 0.540-0.558 | ❌ the non-vol channels carry ~0.05 |
| other training labels for a ranking model: `soft` sigmoid of the move, upper partial moment, `up3`, XGBoost regression of `log|r_5|` | 0.617-0.659 | no gain |
| event and direction logits with a recency half-life (2/4/8 y), balanced class weight, C 0.01; direction at 0/2/4 % | 0.568-0.666 | no gain |
| **calendar bumps** — pre-Tet (event rate **0.34** in the 20 days before Tet vs 0.10, 12 of 14 years ≥ 0.15, mean 5-session return +2.9 % vs +0.2 %), post-Tet, month dummies — inside each kind, or as a calendar-only member | inside: −0.016 … +0.002; alone 0.569-0.594; as a member −0.006 | ❌ a real effect the CV cannot use: ~13 sessions a year |
| causal EWM smoothing of the scores (span 2-10) | ≤ the raw score | no gain |
| the best single channel, sign fixed per fold | `drv_rogers_satchell_21` 0.640 | the ensemble adds ~0.055 |

⚠️ **THIS ROUND STARTED AFTER THE FIRST TABULAR TRIAL'S TEST NUMBERS WERE READ** (§6b: geo3
0.660, geo2 0.669, magnitude 0.705, direction 0.401). The panel member and both new ensembles
are CV choices, but `geo3p` also drops the member that was worst on test — so **v2 is the SECOND
read of 2024-2026, not an independent one** (`NUL-1`). The chain's own val picked `geo4`, which
keeps it.

### 6d. The second tabular trial — `20260917-041045__vcb__up_5pct_5day__final__d1_h5__tab`

`python -m event_chain --setup tabular --apply --stages train,report`: the dataset and its hash
(`8cae1d16d1530360`) are unchanged, so **8 runs are reused from §6b by run id** and one is new;
the BANK pools were materialised first (RUNBOOK G6, 13 s + 12 s + 12 s).

| model | val AUC | **test AUC** | test null p95 / max | test z | test PR-AUC (lift) | refit on train+val (p95) |
|---|---|---|---|---|---|---|
| **`ensemble_geo4`** — chosen on val | **0.803** | **0.670** | 0.639 / 0.785 | +1.93 | 0.130 (2.53×) | **0.680** (0.642) |
| `ensemble_geo3` | 0.799 | 0.660 | 0.644 / 0.795 | +1.84 | 0.104 (2.01×) | 0.659 (0.641) |
| `ensemble_geo3p` | 0.796 | 0.676 | 0.645 / 0.788 | +2.00 | 0.135 (2.63×) | 0.683 (0.644) |
| `event_linear_evt_c01` | 0.795 | 0.614 | 0.645 / 0.774 | +1.31 | 0.109 | 0.620 |
| `ensemble_geo2` | 0.790 | 0.669 | 0.645 / 0.802 | +1.93 | 0.126 | 0.664 |
| `event_panel_xgb_d2_n600` | 0.759 | 0.637 | 0.644 / 0.788 | +1.53 | 0.126 (P@10 % 3.34×) | 0.654 (0.645) |

Walk-forward, `ensemble_geo4`: 2021 0.688 · 2022 0.788 · 2023 0.836 · **2024 0.333** · 2025 0.742 ·
2026 0.641; the panel member alone 0.514 · 0.730 · 0.771 · **0.523** · 0.642 · 0.652.

⚠️ **+0.010 ON TEST AND +0.021 REFITTED, AND STILL NOT A PASS.** The val-chosen number rose
0.660 → 0.670 and the refit 0.659 → 0.680, but the null MAX (0.785) is above both, `z < 2`, the
test holds 33 positives, and this is the second read (§6c). A +0.013 CV gain is inside the test's
standard error (~0.05). ⚠️ **2024 is still inverted** (0.333); the panel member is the only kind
above 0.5 there. ⚠️ **The peer rows are not in the dataset hash** (`PEH-1`): a refit reads the
BANK pools as they are on that day.

## 7. ⚠️ MBB — the same chain on a second ticker, and a grid tuned for it (2026-09-17)

```
python -m event_chain --setup tabular --ticker MBB --apply --notes "<why>"   # VCB's grid on MBB
python -m event_chain --setup tabular_mbb --apply --notes "<why>"           # RUNBOOK G7 — MBB's grid
```

**Data**: `unified_schema_mbb` had `pool__basic`/`pool__targets` only; `pool__targets`,
`pool__event_features`, `pool__market_context`, `pool__market_breadth`, `pool__news_daily` and
(for a probe) `pool__ta` were materialised through Dagster for partition `MBB`, 12-14 s each.
MBB trades on HOSE from **2011-11-01**, so its split is its own: train 2011-11-01 → 2022-02-28
(2,575 samples, 288 events, base 0.112), val 2022-03-08 → 2024-05-17 (548, 55, 0.100), test
2024-05-27 → 2026-08-14 (553, **49**, 0.089). The selection holdout is 2022-03-08.

**Selection at d = 1** (10 draws): `event_features` z **+5.14**, `market_context` **+7.86**,
`market_breadth` +2.19 cleared; ⚠️ **`basic` FAILED (z −0.18) and so did `news_daily` (+0.54) and
`pool__ta` (+0.42, a probe)** — VCB's `basic` cleared, so MBB's table has **no `drv_` block**: 133
channels against VCB's 211. ⚠️ Two defects the second ticker exposed, both fixed before it logged
a trial: `RSC-1` (the selection lookup ignored the ticker) and `LBS-1` (a leaderboard scored every
run on the dataset hash, so two setups on one table would have been one search).

### 7a. The tuning CV — train+val rows only, validation years 2015-2024

A scratch harness cut one fold per calendar year (train = every row ending `d + h − 1` before
the year) plus the chain's own train→val split, and fitted the repo's OWN estimators
(`model.event_linear`, `model.event_panel`), so a config moves into the chain unchanged. It
reproduced the first trial's val AUCs to the third digit. ⚠️ Its 2022-2024 folds overlap the
chain's val split, so **every val AUC below is optimistic for the configuration chosen on it**.

| candidate | CV10 · worst fold · val |
|---|---|
| VCB's grid as it is: `event_panel_xgb_d2_n600` · `ensemble_geo4` | 0.665 · 0.443 · 0.698 — 0.665 · 0.453 · 0.677 |
| own-ticker linear kinds, ~150 settings (blocks, alpha/C, half-life, `min_move`) | best 0.664 (event logit on `har_ evt_ px_ flow_`, C 0.03, 4 y); magnitude 0.648; direction ≤ 0.60 |
| ⚠️ **the panel WITHOUT `mctx_`/`glb_`/`bond_`** (`har_ evt_ sec_ mkt_ cal_ px_ flow_`) | **0.723 · 0.491 · 0.742** — `PDL-1` |
| ... without `cal_` · without the two Tet channels · without `mkt_` · without `sec_`/`flow_`/`har_` | 0.613 · 0.688 · 0.705 · 0.717-0.722 |
| ... + VN-Index volatility · + its returns/position · + VIX | 0.708 · 0.687-0.702 · 0.728 |
| ... depth 1-5, 300-1,800 trees, `own_weight` 3-10, half-life 2-8 y, colsample, min_child_weight | 0.702-0.733; depth 3 **0.733**, its seeds 1-3 **0.719-0.729** |
| ... peers VN30 · BANK+VN30 · BANK without the state banks · without EVF/ABB/NAB · 7 oldest · 12 newest | 0.689 · 0.683-0.691 · 0.711 · 0.722 · 0.701 · 0.667 |
| ... with `pool__basic`'s `drv_` (a `--keep-failed` table, `…__tabk`, dropped afterwards) | 0.726 — noise, and it failed its null |
| **L2 logit on the same panel rows** (`kind: logit`), C 0.03, 4-year half-life | 0.718 · **0.559** · 0.73 — the best worst fold |
| geometric mean: depth-3 tree + logit (`ensemble_pxl`) · + the depth-2 tree (`ensemble_px2l`) | **0.739** · 0.565 · 0.74 — 0.735 · 0.536 · 0.74 |
| ... + the own-ticker linear kinds · + `gbt_d2` · + VCB's panel | 0.709-0.722 · 0.700 · 0.701 |

⚠️ **WHY THE MARKET BLOCKS HURT HERE AND NOT FOR VCB'S OWN LINEAR KINDS**: a channel that is the
same for all 20 banks on a day lets a tree isolate DATES and learn their event rate; a linear
score cannot. `cal_` is date-level too and is the exception, because a season recurs. ⚠️ **VCB's
panel member was not re-measured without them** (`PDL-1`).

### 7b. The two MBB trials

| trial | grid | val-chosen | val AUC | **test AUC** | null p95 / max · z | refit train+val | rolling refit (21 sessions) |
|---|---|---|---|---|---|---|---|
| `20260917-102945` | `tabular` (VCB's) | `event_panel_xgb_d2_n600` | 0.698 | **0.608** | 0.613 / 0.731 · +1.58 | 0.691 | — (not computed then) |
| `20260917-112502` | `tabular_mbb` | **`ensemble_px2l`** | **0.743** | **0.630** | 0.616 / 0.719 · +1.85 | **0.675** | 0.648 (p95 0.612) |

Trial 2, every row (val · test · refit · rolling): `ensemble_pxl` 0.743 · 0.639 · 0.676 · 0.650 —
`xgb_pan_d2_n600` 0.742 · 0.612 · 0.662 · 0.640 — `logit_pan_c003_hl4` 0.737 · 0.586 · 0.634 · 0.620 —
`xgb_pan_d3_n600` 0.730 · **0.665** · 0.689 · 0.659 — `evt_hep_c003_hl4` 0.698 · 0.604 · 0.551 · 0.517 —
VCB's panel 0.698 · 0.608 · 0.691 · 0.663 — `mag_hepm_a100_hl4` 0.689 · 0.541 · 0.558 · 0.555 —
`forest_et_leaf30` 0.659 · 0.635 · 0.653 · 0.641 — `gbt_d2` 0.630 · 0.649 · 0.662 · 0.639.
Walk-forward, `ensemble_px2l`: 2022 0.823 · 2023 0.685 · **2024 0.600** · 2025 0.686 · 2026 0.735.

⚠️ **THE TUNING MOVED VAL +0.045 AND TEST +0.022 — AND NOTHING CLEARS ITS NULL MAX.** 0.630 is
above the per-run p95 (0.616) and below the max (0.719), `z < 2`, the test holds 49 events, and
the search behind it is ~250 CV candidates plus 20 chain runs (`NUL-1`). **The 0.75 the tuning
aimed at was reached on CV (0.739) and val (0.743), not on test.** The val→test drop is 0.11, as
for VCB; 2024 is again the weakest year. ⚠️ **The best test number is not the chosen one**: the
depth-3 tree scored 0.665 and was fifth on val. ⚠️ **A ROLLING REFIT SCORES BELOW ONE REFIT**
(0.648 vs 0.675): pooling 27 blocks pools 27 base-rate levels, and a pooled AUC pays for the
drift between them. `ROLLING_REFIT_SESSIONS` (21) was fixed before any rolling number was read.
⚠️ **Test was read three times for MBB** — trial 1, trial 2 and its re-report with the rolling
column (the first trial-2 folder, `20260917-112154`, was replaced by `112502`: same runs, same
numbers, one column more). No configuration was changed after a test number was read.

### 7c. ⚠️ The push for 0.8 — five more rounds, ~80 candidates, NOTHING above 0.741 (2026-09-17)

Same harness, same folds, no test row read; the chain's grid was NOT changed.

| round | candidates | CV10 · worst fold |
|---|---|---|
| other panel learners on `_PANEL` | HistGradientBoosting 0.721 · 0.552; RandomForest 0.697; ExtraTrees 0.686; spline logit 0.693; MLP 0.657 | none above the XGBoost/logit pair |
| threshold augmentation (each row stacked at +3…+7 %, the threshold a feature, scored at +5 %) | depth 2 0.712, depth 3 0.728 | no gain |
| market-RELATIVE channels (own `har_`/`px_` minus VN-Index's) | XGBoost 0.715, logit 0.714 · **0.596** | best worst fold, no gain in the mean |
| the market blocks with `min_child_weight` 500 | 0.611 | regularising does not stop the date memorisation (`PDL-1`) |
| scale-free `pool__ta` channels (RSI, stochastics, ROC, …; a `--keep-failed` probe table, dropped) | 0.714-0.718; TA alone 0.686 | no gain — and TA failed its null |
| a DATE-LEVEL market component: ridge on the BANK sector's event RATE (19 labels a day) | 0.677 alone (alpha 3); in the ensemble 0.714-0.730 | the panel already carries it |
| the touch rule `upany_5pct_5day` as label (base 0.136) | panel 0.703, pair 0.708 | the other reading of the phrase is NOT easier |
| ensembles of the above with `ensemble_pxl` | 0.714-**0.741** (`+` market-relative logit) | +0.002, inside the noise |

⚠️ **THE CEILING IS THE LABEL, NOT THE MODEL.** Every family lands at 0.70-0.74 and the fold
standard error is ~0.02. A +5 % close five sessions out is a MAGNITUDE question (predictable from
volatility, the Tet calendar and breadth) times a DIRECTION question (the repo's verdict, §2), and
no channel here moved the direction part. **0.8 was not reached on CV, val or test**, and no
configuration was changed on a test number.
