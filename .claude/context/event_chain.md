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

Splits: train 419,503 rows (base **0.182**, 2009-01-02 → 2021-04-19), val 139,699 (**0.209**), test
147,527 (**0.134**, 2023-12-14 → 2026-08-13). Trial
`20260918-125131__liquid__upopen_5pct_5day__final__d1_h6__bsk`.

**Chosen on val AUC: `ensemble_boost_eq`** (the same three members as the deleted flow's winner —
XGBoost, the event logit, the tree magnitude model).

| | val | **test** | refit (train+val) | rolling (quarterly) |
|---|---|---|---|---|
| pooled AUC | 0.650 | **0.654** (within-session shuffle null p95 0.547, **z +57.5**) | 0.656 | 0.657 |
| within-session AUC | 0.635 | **0.631** (null p95 0.505, z +44.1) | 0.629 | 0.630 |
| hit@5 (base 0.134) | 0.357 | **0.260** (null p95 0.141, max 0.150, z +24.2, lift **1.94**) | 0.255 | **0.270** (lift 2.02) |

**With the val-chosen cut `min_prob = 0.25`** (at most 5 names, cash when none clears it):

| test basket | sessions traded | names | precision (null p95 · z) | basket ≥ 5 % | basket ret | EV/session | Sharpe@50 (worst offset) | CAGR | max DD | without ceiling names |
|---|---|---|---|---|---|---|---|---|---|---|
| frozen | 91.8 % | 3.99 | **0.286** (0.152 · +21.7) | 0.246 | +1.01 % | +0.47 % | 0.46 (0.10) | +12.9 % | −50.1 % | 0.281 · +0.91 % · 0.37 |
| **rolling (quarterly)** | 88.9 % | 3.81 | **0.305** (0.159 · +23.0) | 0.276 | **+1.35 %** | +0.76 % | **0.69 (0.25)** | **+28.1 %** | −49.6 % | **0.306 · +1.38 % · 0.67** |

✅ **TRAINING ON THE TRADEABLE LABEL IS WORTH ~3× THE MONEY**: the deleted close-priced model's own
picks, re-priced at the open of N+1, gave precision 0.308 but **+0.78 % per basket, Sharpe 0.26,
CAGR +2.4 %** (§6, and `EXE-1` in ISSUES.md). The new model ranks slightly worse on paper and earns
**+1.35 %, Sharpe 0.69, CAGR +28.1 %** — it stops paying for names whose move happens overnight.
✅ **AND `CLF-1` NO LONGER BINDS**: dropping every name that closed at its ceiling on N leaves
precision 0.306 and Sharpe 0.67 (against 0.305 / 0.69 with them), because the ceiling gap is now
OUTSIDE the label. The old flow lost 1.28 → 0.13 the same way.

**Walk-forward, yearly expanding refits of the chosen row** — lift over the buyable base in every
one of six years, and the return is not: hit 0.452 / 0.347 / 0.300 / 0.237 / 0.301 / 0.235 on bases
0.265 / 0.206 / 0.170 / 0.124 / 0.150 / 0.122 (**lift 1.68-2.00, z +10.1 … +15.4**), basket return
+5.63 / +0.24 / +1.51 / +0.55 / +1.46 / **−0.54 %** and Sharpe@50 5.21 / −0.18 / 1.26 / 0.55 / 0.56
/ **−1.25** (2021 … 2026). ⚠️ **2026 is negative on both** while the universe is −0.43 %.

⚠️ **What still qualifies every number here**: `NUL-1` — the grid, the ensembles and the cut rule
were carried over from a search done on the deleted close-priced flow, so this test split is not
the first read of that search; the universe is survivors-only and not point-in-time; the max
drawdown of the cut track is ~50 %; and the cut's own grid on test is descriptive (a 0.35 cut
trades 37 % of sessions at precision 0.399, Sharpe 0.80 — chosen on val it was 0.25).

**Run it**: `python -m event_chain --setup basket --apply` (runbook `G8`),
`python -m event_chain.basket --pick YYYY-MM-DD` for one session (`G9`; 2026-08-21 → **PNJ** alone
clears the cut, the other four of the top five sit at 0.21-0.24), `--min-prob-study` to re-choose
the cut without refitting (`G10`).
