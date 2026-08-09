# Context — `src/model` (referenced dataset → scored run)

> Handoff notes for a new session. **Rebuilt 2026-08-09** — §1–§7, §9 and §12 describe
> the current pipeline; §10–§11 are the research log and are unchanged. Verify anything
> before acting on it — the DB, `src/train_test_set/`, and
> `src/model/runs/index.csv` are the sources of truth.

## 1. Big picture / pipeline

```
reports/feature_selection/<run>/outstanding.csv        feature_selection  (manual runs)
   ▼   python -m final_features --apply
unified_schema_<t>.<target>__final__d<d>_h<h>          ⚠️ the one stage that writes the DB
   ▼   python -m train_test_creator --save
src/train_test_set/<dataset>/                          X/y tensors + scalers + metadata
   ▼   python -m model.lstm --config <cfg>             ← THIS package
src/model/runs/<run_id>/                               config, checkpoints, TB, predictions
   ▼   python -m result_evaluator
results/metrics.json + runs/index.csv                  scored against a shuffled-label null
```

`python -m pipeline` prints the state of all five and runs the stale ones —
`src/pipeline/CONTEXT.md` is the chain-level document. Each stage's own CONTEXT.md
covers its internals.

- **One sample** = a `(d, n_features)` window ending on day *t*; **label** = the target
  at day *t* (for `return_5day`, `close[t+5]/close[t] − 1`).
- ⚠️ **`d` and `h` come from the source TABLE NAME**, never from a parameter. They flow
  `return_5day__final__d20_h5` → dataset `metadata.json` → asserted against the config.
- **DB**: PostgreSQL `database_main_v2`, schema `unified_schema_<ticker>`. Creds in the
  repo `.env` (`POSTGRES_*`).

## 2. Directory layout

```
src/model/
├── CONTEXT.md            ← this file
├── __init__.py           makes `model` importable as a package (python -m model.lstm)
├── common/               ← shared framework lib (used by every model)
│   ├── data.py           load_dataset (reference + hash), Dataset dataclass
│   ├── run_dir.py        RunDir: immutable run folder, config/metadata, git sha, env
│   ├── trainer.py        Trainer (train loop, early stop, TensorBoard, checkpoints),
│   │                     TrainConfig, set_seed, resolve_device, to_loaders;
│   │                     criterion is configurable (MSE / BCEWithLogits)
│   ├── metrics.py        ⚠️ a SHIM over result_evaluator.metrics — kept only so the two
│   │                     legacy notebooks still run. No metric is defined here.
│   └── registry.py       append_run → runs/index.csv; MIGRATES the header when the
│                         column set grows, instead of misaligning rows under it
├── lstm/                 ← ONE model = code only (no run outputs live here)
│   ├── model.py          LSTMRegressor + build_model + arch_dict (last Linear → scalar;
│   │                     the return for regression / the logit for a classifier)
│   ├── train.py          config → run folder → trained model → scored result
│   ├── configs/          vcb__return_5day__final__d20_h5.yaml  ← the current one
│   │                     return_5day_lb*.yaml, direction_5day_lb*.yaml  ← legacy sweeps
│   ├── RUN__lstm.ipynb            the notebook meant to be run (calls train.py)
│   ├── lstm_return_5day.ipynb     legacy; reads the pre-2026-08-09 dataset names
│   └── lstm_direction_5day.ipynb  legacy; same
└── runs/                 ← ALL runs from ALL models AND tasks (shared)
    ├── index.csv         cross-run leaderboard (TRACKED in git)
    └── <run_id>/         one immutable run (git-IGNORED: .gitignore src/model/runs/*/)
```

⚠️ **`runs/*/` being git-ignored has a consequence worth knowing before you rely on a
run**: 27 of the 28 folders in a fresh checkout hold only `results/` and `logs/` —
no `metadata.json`, no checkpoints. `result_evaluator` is built to score them anyway
(its CONTEXT §1); nothing else can.

## 3. The run folder (`src/model/runs/<run_id>/`)

`run_id = <run_name>__<YYYYmmdd-HHMMSS>`, and by convention
`run_name = <model>__<target>__lb<L>__final` (model first). Example:
`lstm__return_5day__lb20__final__20260703-204219`.

```
config.yaml         the exact input config for this run
metadata.json       git sha, env/CUDA, dataset ref + hash, model, training summary, metrics
model/  best.pt      best-val state_dict (inference)
        last.pt      state_dict + optimizer + scheduler + epoch + RNG (exact resume)
        arch.json    class + kwargs to rebuild the module (model.build_model)
tensorboard/        TB event files (scalars: train/val loss, lr; HPARAMS: config vs metrics)
results/  metrics.json  predictions_{val,test}.csv (date,y_true,y_pred)  loss_history.csv  *.png
logs/
```

**Data is referenced, not cloned** — the run stores only `dataset_name` +
`dataset_hash` in `metadata.json`; the tensors stay in `src/train_test_set/`.

## 4. Create a dataset — `python -m train_test_creator --save`

`src/train_test_creator/CONTEXT.md` is the full document. What matters here:

- The dataset folder **names its input**:
  `vcb__return_5day__final__d20_h5__tr70_val15_test15__std`.
- ⚠️ `d` and `h` are parsed from the source table's name, never passed in.
- ⚠️ **The splits are purged by `d + h - 1` = 24 samples** at each boundary —
  `feature_selection.PurgedWalkForward.gap`, the same purge the channels were selected
  under. The pre-2026-08-09 notebook had none: it started each split `d-1` rows early
  and let train labels reach `h` days into the val window.
- ⚠️ Imputation is the **train-slice median**, matching `FeatureSelector._impute`. The
  old `ffill().bfill()` filled leading gaps with future values (3,382 of 4,230 rows on
  one channel).
- `metadata.json` carries the source table's `COMMENT`, the purge gap, the dropped
  channels and a drift summary. `train.py` reads all of it into the run's `lineage`.

> `train_test_set/` is git-ignored (`*.npy` + the dir).

## 5. Train a model — `python -m model.lstm`

```
python -m model.lstm                                       # the default config
python -m model.lstm --config configs/<name>.yaml
python -m model.lstm --config <path> --dry-run             # print the plan only
```

`RUN__lstm.ipynb` is the same thing with figures — it calls the same `train()`, so a
sweep is a shell loop rather than nine edited copies of a notebook.

`train()` does: `load_dataset` → `_verify` → `RunDir.create` → `Trainer.fit` →
write `predictions_{val,test}.csv` → `result_evaluator.evaluate_run` → `append_run`.

⚠️ **`_verify` raises, it does not warn.** A config whose `lookback`/`n_features`
disagree with the dataset, or a classification task on a dataset that has a target
scaler, stops there. Training a `d=20` architecture on `d=5` windows produces a run
that looks finished, lands in `index.csv` beside comparable runs, and is not one.

⚠️ **No metric is computed here.** `evaluate_run` reads the prediction files, so
`python -m result_evaluator --rescore` can add or correct a metric across every past
run without a GPU — see `result_evaluator/CONTEXT.md` §1.

Runs go to the shared `src/model/runs/`. GPU is auto (`device: auto`). Compare:
`tensorboard --logdir src/model/runs`, `src/model/runs/index.csv`, or
`python -m result_evaluator`.

## 6. Config schema

```yaml
run_name: lstm__vcb__return_5day__final__d20_h5     # <model>__<ticker>__<target>__<setup>
dataset: vcb__return_5day__final__d20_h5__tr70_val15_test15__std
task: regression                                    # regression | classification

lookback: 20        # ⚠️ ASSERTIONS, not settings — the dataset is the authority
n_features: 724     # 724 on the VCB dataset, 13 on the bank one

model:   {type: LSTM, hidden_size: 128, num_layers: 2, dropout: 0.2}
train:   {batch_size: 64, lr: 0.001, weight_decay: 0.00001,
          max_epochs: 150, patience: 20, grad_clip: 1.0,
          lr_factor: 0.5, lr_patience: 5, log_every: 10}
null_draws: 200     # draws in result_evaluator's block-shuffled null
seed: 42
device: auto        # auto | cuda | cpu
```

## 7. End-to-end recipe: a new `(target, setup)`

1. **Selection** — run `feature_selection/RUN__feature_importance_report.ipynb` for
   each pool combination. ⚠️ Manual: hours of GPU and a judgement about which pools to
   join. Then `python -m feature_selection.outstanding` to refresh the shortlists.
2. **Table** — `python -m final_features` to see the plan, `--apply` to build
   `<target>__final__d<d>_h<h>`. One table per (schema, target, setup).
3. **Dataset** — `python -m train_test_creator --ticker <t> --table <table> --save`.
4. **Config** — copy `configs/vcb__return_5day__final__d20_h5.yaml`; set `run_name`,
   `dataset`, `lookback`, `n_features`, and `task` if it is a classifier.
5. **Train** — `python -m model.lstm --config configs/<name>.yaml`.
6. **Score** — happens automatically; `python -m result_evaluator` for the board.

Or `python -m pipeline --apply`, which does 2–6 and skips whatever is already there.

⚠️ **A binary target must be built with `scale_target=False`** — never standardise a
0/1 label. `_verify` raises if a classification config points at a dataset that has a
target scaler.

**New model** (GRU/Transformer/TCN): create `src/model/<name>/` with a `model.py` (an
`nn.Module` mapping `(batch, d, n) → (batch,)`, plus `build_model`/`arch_dict`), a
`configs/`, and a `train.py` copying `lstm/train.py`. Everything in `common/` and all
of `result_evaluator` is reused unchanged — the `Trainer` is model-agnostic and the
core metric block reads a score, not an architecture. Prefix `run_name` with the model
so runs stay distinct in the shared `runs/` and `index.csv`.

## 8. GPU & performance (RTX 3050 Laptop, 4 GB, CUDA 12.7)

- Training uses the GPU automatically (`device: auto` → `cuda`). Torch 2.5.1+cu121.
- **Feature selection** (upstream) is the expensive part. XGBoost gain + SHAP run
  on the GPU; **LASSO/ElasticNet (sklearn) are CPU-only** and their CV is
  pathologically slow on the wide *flattened-window* design matrices
  (`n_features × lookback`). Measured: `ta` lb20 full ensemble ≈ **6.6 h**;
  `macro` lb25 (3725 cols) hung > 60 min. Fix = `tree_only=True` (XGB+SHAP only,
  GPU): `ta` lb20 → **87 s**, `macro` lb25 → seconds.
- **Rule**: `ta` always `tree_only`; `macro` `tree_only` at `L ≳ 25`; narrow
  pools (`basic`/`calendar`) always full ensemble. `max_features` does NOT change
  fit cost (the fit is on all `n_features × lookback` columns).

## 9. Metrics — see `result_evaluator/CONTEXT.md`

⚠️ **No metric is defined in this package any more.** `common/metrics.py` is a shim
kept so the two legacy notebooks still run; the definitions live in
`result_evaluator/metrics.py`, and `train.py` calls `evaluate_run`.

The short version. Every model type reports the **same four core metrics**, computed
from `(per-sample score, realised forward return)` and nothing else — which is exactly
what lets a regressor, a classifier and a ranker share one leaderboard:

| | question |
|---|---|
| `ic` (Spearman) | does a higher score mean a higher return? |
| `dir_auc` | does a higher score mean *up* more often? |
| `dir_accuracy` | hit rate at the score's own median |
| `long_short` | what does the top quintile minus the bottom pay, in return units? |

Each of `ic` and `dir_auc` carries `_p`, `_bar` and `_clears` from **200
block-shuffled draws** (block = `d + h`, so the label's autocorrelation survives).
Task extras are additive: `RMSE`/`RMSE_zero_baseline`/`r2` for regression,
`log_loss`/`brier`/`pr_auc`/`base_rate` for classification.

⚠️ That null does **not** price in feature selection, architecture search or early
stopping — it cannot, because it sees a finished prediction vector. **A run that fails
it is dead; a run that clears it is not yet alive.**

⚠️ `n_eff = n/h` is reported beside `n`: 635 overlapping test samples carry about 127
independent observations, and that figure is itself optimistic.

Metrics remain **re-computable from `predictions_{val,test}.csv` without retraining** —
`python -m result_evaluator --rescore`. That is how `dir_auc` was backfilled across all
runs once, and how a p-value bug was corrected across all 28 during the 2026-08-09
rebuild (`result_evaluator/CONTEXT.md` §3a).

## 10. Current state (as of this handoff)

- **Model**: LSTM (2 layers, hidden 128, LayerNorm+Dropout head, ~276k params),
  Adam + ReduceLROnPlateau, early stopping on val loss. Regression = MSE on the
  scaled return; classification = BCE on the 0/1 label (same Trainer, swapped loss).
- **`return_5day` sweep done**: lookbacks **1, 2, 3, 5, 10, 15, 20, 25, 30** (9 runs).
  **No lookback beats the zero-baseline** (test RMSE ≈ 0.0357); `dir_auc` all ≈ 0.5
  (best 0.56 @ lb2); ICs near 0.
- **`direction_5day` (classification) sweep done**: lookbacks **1, 2, 3, 5, 10, 15,
  20, 25, 30** (9 runs; the classification pathway + all views/datasets built this
  session). **Negative across the board**: `test_dir_auc` mean 0.519 (range 0.47–0.55,
  0.5 = no skill); only lb5 nominally clears the majority baseline (acc 0.574 vs 0.564,
  `dir_auc` 0.549) and it is uncorroborated by its neighbours (lb3 0.528, lb10 0.500) —
  i.e. noise, not signal. Most runs stop at best epoch 1 (never learn past init; train
  loss falls while val rises = overfitting to noise). Structural at every lookback:
  train up-rate ≈0.498 vs test ≈0.436 (mild label drift), majority baseline acc 0.564.
- **`probability_gain_5pct_5day` (classification) sweep done**: lookbacks **1, 2, 3, 5,
  10, 15, 20, 25, 30** (9 runs). **Strongly imbalanced** target (1 = close gains ≥5%
  within 5 days): base rate 0.153 overall, and only **0.071 in the test window vs 0.176
  in train** (heavy label drift) → majority-baseline acc 0.929, so accuracy is
  uninformative and `beats_majority` is `False` everywhere. Read on AUC/PR-AUC instead:
  `test_dir_auc` (ROC-AUC) mean 0.545 (range 0.41–0.66), `test_pr_auc` mean 0.106 vs
  base rate 0.071 (~1.5× lift). A *whisper* more life than `direction_5day`, but **not
  trustworthy**: val and test ROC-AUC are decorrelated/anti-correlated (e.g. lb15 has the
  worst val AUC 0.33 but the best test AUC 0.66), so the apparent test edge isn't
  selected-for and doesn't reproduce across lookbacks = noise, not signal.
- **Takeaway**: absolute-return regression, direction classification, and the +5%-gain
  classification all confirm single-stock *absolute* 5-day outcomes are ~unpredictable
  here — the tradable signal is the cross-sectional *relative* return.
- **Single-ticker LSTM stage is EXHAUSTED** (return + direction + prob_gain all negative
  OOS). `return_rel_5day` was deliberately NOT swept: a single-ticker time-series model
  cannot capture a *relative* edge (it is inherently cross-sectional). The live research
  thread moved to the cross-sectional strategy — see §11.

## 11. Cross-sectional investigation & tried methods (2026-07-05)

The research pivoted from single-ticker LSTM to a **cross-sectional** study on the
`gold_schema.stocks` panel (777 tickers, 2000-2026; ~150-200 liquid names used). Target
= **5-day forward return, cross-sectionally demeaned** (universe- or sector-neutral) —
"which stocks beat the cross-section over 5 days", traded market-neutral. This work lives
in **session scratch scripts** (not committed) + [[project-cross-sectional-strategy]] in
memory; recreate from that if needed. Key results:

- **Foreign flow is the one signal that survives.** Price factors (rev/mom/vol) had a real
  cross-sectional IC in 2016-2020 but **reversed post-2021**; foreign-flow features
  (`foreign_net_value` etc., 100% populated historically in `gold_schema.stocks`) stayed positive.
- **Best OOS classifier**: XGBoost on flow (+ decomposition: gross participation, foreign
  order-imbalance, foreign-room accumulation, block-trade ratio) + liquidity + short-
  reversal + within-sector z-scores, **sector-neutral target**, walk-forward + H-day
  embargo. **AUC ceiling ≈ 0.52 all-names / 0.53 tercile** — sector-neutralisation + flow
  decomposition added only +0.003; ensembling and recency-weighting did not help.
- **Tradability**: GROSS L/S Sharpe ~1.3-1.6, but 65-78%/leg weekly turnover kills it.
  Turnover control (EWMA span-10 + hysteresis enter 0.90/exit 0.75) → ~8%/day, flips
  net@20bps from Sharpe -1.5 to +0.46. BUT by regime: net@20bps **+1.46 (2017-20) vs
  −0.51 (2022-26)**; long-only (VN-executable) same story; dead at 40bps.
- **Rolling-window training does NOT beat the regime wall** (tested expand vs 12mo vs 24mo,
  6-mo test blocks, val-6mo early-stop): rolling *lowered* AUC (0.513 vs 0.520) and recent
  net stayed negative — the recent regime simply has little learnable signal, not stale
  training data.
- **The edge does NOT concentrate in any single name — including VCB.** Trained the panel
  model, then pulled out VCB's own OOS predictions: VCB *sector-relative* 5-day AUC = **0.491**
  (worse than chance; recent 0.450, only 40% of years >0.5), VCB *absolute* direction AUC =
  0.518 (marginal, matches the LSTM sweeps), VCB relative-value trade net Sharpe 0.20 overall
  but **−0.12 in 2022-26**. The cross-sectional edge is a weak per-name average (~0.52) that
  only becomes money spread across 20-40 names; for one ticker it drowns in idiosyncratic
  noise. **⇒ VCB has no trustworthy single-name 5-day signal; tradability is portfolio-level.**
- **Verdict**: on current data (price + foreign flow) there is **no robustly-tradable 5-day
  edge in the current regime** — a quantified alpha-decay story (robust to target, feature,
  model, split, and turnover choices). The only real lever left is **new information** (below),
  NOT more modeling.
- **Tried & shelved**: single-ticker LSTM (all 4 targets); cross-sec targets {universe-
  demeaned, sector-demeaned, sign vs tercile-extremes}; features {price factors, foreign-
  flow + decomposition, liquidity/Amihud, within-sector z}; models {Ridge, Logistic, XGB,
  logit+xgb ensemble, recency-weighting}; splits {expanding & rolling walk-forward, H-day
  embargo}; turnover control {EWMA smoothing, hysteresis bands, long-only}; costs {0/20/40
  bps}. AUC plateaus ~0.52-0.53; net edge is pre-2021 only.

**Data availability audit (2026-07-05).** Confirmed at the RAW layer: the only buy/sell data
anywhere in the DB is **FOREIGN** (`f_buy/f_sell_*`, `foreign_buy_pressure`) — present in
`bronze_schema.{cafef,simplize}_stocks`, `gold_schema.stocks`, and the VCB `__final` views, and
already fully exploited. **Active/aggressor buy-sell (all-investor buyer- vs seller-initiated) is
NOT ingested** (CafeF/Simplize/TradingView never captured it; `volume_matched` is total matched
volume, not side-split). `mfi_14`/`bop` are price-derived proxies, not order flow. No fundamentals,
no sentiment, no intraday/tick, no point-in-time index membership in the DB.

**New information needed (ranked by expected impact on a 5-day VCB signal):**
1. **Active/aggressor buy-sell imbalance** — derivable only from **intraday tick / order-book**
   data (trade hits ask = aggressive buy, hits bid = aggressive sell). Same underlying feed as (6).
   Forward-collect only (broker/exchange API); `src/money_flow/daily_flow_collector.py` targets this.
2. **Fundamentals / earnings** — highest 5-day value = **earnings surprise + estimate revisions**;
   bank-specific (VCB): P/B, ROE, NIM, credit/deposit growth, NPL/LLR, CAR, CASA. Need point-in-time
   filing dates (no look-ahead). User to consider collecting.
3. **News / disclosure / sentiment** — VN news headlines, HOSE/HNX filings (event dates), analyst
   rating/target changes, forum sentiment. User to consider collecting.
4. **Point-in-time VN30/VN100 membership** — historical constituent lists w/ effective dates; kills
   the survivorship bias in all backtests above (universe currently = "survived to 2026").
5. **Intraday / order-book (Level-2) for VCB** — sub-daily ticks + bid/ask depth → order-book
   imbalance, spread, and the genuine single-name microstructure edge. Real-time feed, heavy storage.

## 12. Gotchas

- **`src/model/runs/*/` is git-ignored** (checkpoints/TB/plots — reproducible);
  only `runs/index.csv` is tracked (`.gitignore`: `src/model/runs/*/` +
  `!src/model/runs/index.csv`). ⚠️ In practice that means a fresh checkout has 27
  runs with **no `metadata.json`** — `result_evaluator` infers what it needs and
  records that it inferred it; nothing else can read them.
- **Dataset is referenced by name + hash**; rebuilding a dataset changes the hash and
  `load_dataset(expected_hash=...)` flags the mismatch. That is the intended
  behaviour, not a nuisance — a run whose data moved under it is not reproducible.
- **`index.csv`'s header is migrated, not assumed.** `csv.DictWriter` writes fields in
  the code's column order; appending under a header with a *different* order silently
  misaligns every new row. `append_run` compares the two and rewrites the file with
  the union when they differ — which is what happened when the core metric block
  replaced `val_RMSE`/`test_spearman_ic` on 2026-08-09.
- **`driver.select` swallows exceptions** and returns an empty DataFrame — a bad
  table/column silently yields 0 rows. `UnifiedSchemaReader.read` raises on empty
  instead; prefer it.
- **SQL `LIKE '_'` is a single-char wildcard**: `'pool__%'` also matches `poolXX`.
  `UnifiedSchemaReader.tables` filters in Python for this reason. Watch for it in any
  table-name discovery.
- **Batch runs are a shell loop now, not an ephemeral script.** `train.py` and every
  other stage have a `python -m` entry point, so a sweep is
  `for c in configs/*.yaml; do python -m model.lstm --config $c; done`. The pre-rebuild
  note about scratchpad helper scripts no longer applies.
- **The legacy notebooks** (`lstm_return_5day.ipynb`, `lstm_direction_5day.ipynb`)
  and the 27 legacy configs still **work** — all 33 pre-2026-08-09 dataset folders are
  on disk and every config resolves. ⚠️ But they write runs scored by the OLD path
  (metrics computed in-notebook, no null), so a run produced that way lands in
  `index.csv` with the core columns blank until `python -m result_evaluator --rescore`
  fills them. `RUN__lstm.ipynb` + `train.py` is the path that scores as it goes.
- The `mt_env` virtualenv is the interpreter for all notebooks/scripts
  (`d:/GIT/master-thesis/mt_env`).
- **Ephemeral scripts must load `.env` by explicit path**: they live in the session
  scratchpad (outside the repo), so `load_dotenv()` / `find_dotenv()` (which walk up
  from the *script's* location) miss the repo `.env` and env vars come back empty →
  `PostgreSQLConnectionDto` raises "Password cannot be empty." Use
  `load_dotenv(os.path.abspath(".env"), override=True)` with cwd = repo root.
- **A binary target must be built with `SCALE_TARGET=False`** in
  `train_test_creator` (never standardize a 0/1 label); the classification notebook
  asserts `dataset.target_scaler is None`. The dataset dir still ends in `_std`
  (features are still standardized) — the `task`/`target` fields disambiguate.
