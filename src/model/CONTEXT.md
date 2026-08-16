# Context — `src/model` (referenced dataset → scored run)

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

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

## 1a. ⚠️ RUN STANDARD — naming, input, output (2026-08-10)

Every run on the shared leaderboard obeys this. It is **checked, not documented**:
`engine.load_config` raises on a config whose filename is not its `run_name`.

### Naming — one name, four places

```
run_name  = <model>__<universe>__<target>__final__d<d>_h<h>[__<scope>]
run_id    = <run_name>__<YYYYmmdd-HHMMSS>          the run FOLDER
config    = <run_name>.yaml                        ⚠️ enforced by load_config
model_type= <MODEL>                                index.csv, set by the binding
```

| segment | is | examples |
|---|---|---|
| `<model>` | the package under `src/model/` | `lstm`, `cnn`, `baseline_ridge_stats` |
| `<universe>` | the `unified_schema_<t>` the table came from | `vcb`, `bank`, `all` |
| `<target>` | the label | `return_5day`, `rank_5day` |
| `d`,`h` | ⚠️ **parsed from the source TABLE NAME**, never chosen | `d20_h5` |
| `<scope>` | the feature BLOCK; absent = the archive's union | `basic`, `ta`, `fa` |

⚠️ **The filename must equal `run_name`, and that is load-bearing rather than tidy.**
Configs live in per-model directories and `pipeline._config_path` resolves a bare
`--config` name across `model/*/configs/` — so two packages holding one filename is
ambiguous. Since `run_name` starts with the model, `filename == run_name` makes the
collision *impossible* instead of merely detectable. That is issue **CFG-1**, which
happened. `_legacy/` is exempt by directory.

⚠️ **`model_type` is passed by the binding, never read from `config["model"]["type"]`.**
The YAML field is a label a person edits; the shared leaderboard's key should not be.

### Input — one shape, one source

```
src/train_test_set/<universe>__<table>__tr70_val15_test15__std/
    X_{train,val,test}.npy    (n, d, n_features) float, STANDARDISED on the train slice
    y_{train,val,test}.npy    (n,)  SCALED for regression, raw 0/1 for classification
    dates_{split}.npy         (n,)  the label date
    tickers_{split}.npy       (n,)  ⚠️ present only on a PANEL
    feature_scaler.pkl  target_scaler.pkl  metadata.json
```

A model is any callable over `(n, d, n_features) → (n,)`. Nothing else is passed in:
no dates, no ticker, no raw table. ⚠️ `lookback` and `n_features` in a config are
**assertions**, and `_verify` raises — the dataset is the authority.

### Output — one contract, one scorer

```
src/model/runs/<run_id>/
    config.yaml  metadata.json  model/  tensorboard/  logs/
    results/predictions_{val,test}.csv    ← the ONLY file result_evaluator reads
            metrics.json  metrics.csv  loss_history.csv  verdict.txt
```

`predictions_*.csv` is `date[,ticker],y_true,y_pred` for regression and
`date[,ticker],y_true,y_prob` for classification. Two properties are load-bearing:

1. ⚠️ **`y_pred` is in RETURN units, not scaled units.** Scoring the standardised
   target would make RMSE depend on the train slice's variance and stop two datasets
   being comparable.
2. ⚠️ **The `ticker` column is what declares a PANEL.** Without it an N-ticker run is
   scored as one series — `n_eff` counts 20 banks on a date as 20 observations rather
   than one (issue **PNL-1**). It is written from the dataset, never from a config flag.

**Every run is scored by `result_evaluator`, never by the training code**, and lands in
`src/model/runs/index.csv` through `result_evaluator.index.index_row` — one schema for
the training path and the `--rebuild-index` path alike. That is why a constant, a ridge,
an LSTM and a CNN can be read off the same table.

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
│   ├── features.py       window → vector reductions (the six stats), shared by every
│   │                     model that cannot eat a sequence: baseline, gbt, mlp
│   ├── test_features.py  6 tests pinning the numpy and torch reductions to agree
│   ├── engine.py         ⭐ THE TRAINING ENGINE, model-agnostic (2026-08-10): config →
│   │                     run folder → trained model → scored result. _verify, the
│   │                     prediction writer, the lineage block, evaluate_run and the
│   │                     registry row live here ONCE and are shared by every model
│   ├── metrics.py        ⚠️ a SHIM over result_evaluator.metrics — kept only so the two
│   │                     legacy notebooks still run. No metric is defined here.
│   └── registry.py       append_run → runs/index.csv; MIGRATES the header when the
│                         column set grows, instead of misaligning rows under it
├── lstm/                 ← ONE model = code only (no run outputs live here)
│   ├── model.py          LSTMRegressor + build_model + arch_dict (last Linear → scalar;
│   │                     the return for regression / the logit for a classifier)
│   ├── train.py          the LSTM BINDING — names the model module, the model_type
│   │                     string and its configs/. No training logic (see common/engine)
│   ├── configs/          lstm__vcb__close_adjust_5day__final__d20_h5.yaml ← the DEFAULT
│   │                     lstm__vcb__return_5day__final__d20_h5.yaml
│   │                     lstm__vcb__return_5day__final__d20_h5__basic.yaml
│   │                     lstm__bank__rank_5day__final__d20_h5.yaml     ← the panel chain
│   │                     _legacy/  27 pre-2026-08-09 sweep configs
│   └── RUN__lstm.ipynb            the ONE notebook meant to be run (calls train.py)
├── cnn/                  ← second architecture, added 2026-08-10 (§13)
│   ├── model.py          CNNRegressor — Conv1d over the TIME axis + global avg pool
│   ├── train.py          the CNN binding, same shape as lstm/train.py
│   └── configs/          cnn__vcb__return_5day__final__d20_h5__basic.yaml
├── baseline/             ← Tier-1 baselines, added 2026-08-10 (§14). NOT torch.
│   ├── model.py          zero | mean | ridge_stats | ridge_flat | ar — 0 to 81 params
│   ├── train.py          binding onto engine.train_estimator (no training LOOP)
│   └── configs/          one per kind, baseline_<kind>__vcb__…__basic.yaml
├── gbt/                  ← Tier 2 (§15). XGBoost on the window statistics — the
│   │                     estimator feature_selection already ranks with. NOT torch.
├── mlp/                  ← Tier 2. One hidden layer over the same 24-column design.
│   │                     ⚠️ holds a TORCH copy of window_statistics — pinned by
│   │                     common/test_features.py, see §12
├── gru/                  ← Tier 2. Three gates to the LSTM's four.
└── runs/                 ← ALL runs from ALL models AND tasks (shared)
    ├── index.csv         cross-run leaderboard (TRACKED in git)
    └── <run_id>/         one immutable run (git-IGNORED: .gitignore src/model/runs/*/)
```

⚠️ **`runs/` was cleared to today's work on 2026-08-10.** The 29 pre-existing folders —
the 27-run lookback sweep of §10 plus the 2026-08-09 wide-VCB and BANK-panel runs — were
**deleted on request**. They were git-ignored, so they are not recoverable: their METRICS
survive as text in §10, in `CLAUDE.md` §2a and in `index.csv`'s git history, but no run
among them can ever be rescored or re-verified again. §10's research log is now a
citation without its evidence, and should be read that way.

⚠️ **A config filename must be UNIQUE ACROSS model packages.** `pipeline._config_path`
resolves a bare `--config` name by globbing `model/*/configs/`, so two packages holding
the same filename is ambiguous — and it happened on the first try: a CNN config named
`vcb__return_5day__final__d20_h5__basic.yaml` sat beside the LSTM one, `sorted()` put
`cnn` first, and the LSTM config became unreachable through that function while still
resolving through `model.lstm.train.CONFIG_DIR`. Two ways to name one file that
disagree is the STL-1 shape. `_config_path` now **raises** on ambiguity instead of
resolving alphabetically, and a config is prefixed with its model — as `run_name`
already is.

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
python -m model.cnn  --config cnn__vcb__return_5day__final__d20_h5__basic.yaml
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
#        {type: CNN, channels: 32, kernel_size: 3, num_layers: 2, dropout: 0.1}
train:   {batch_size: 64, lr: 0.001, weight_decay: 0.00001,
          max_epochs: 150, patience: 20, grad_clip: 1.0,
          lr_factor: 0.5, lr_patience: 5, log_every: 10}
null_draws: 200     # draws in result_evaluator's block-shuffled null
seed: 42
device: auto        # auto | cuda | cpu
```

⚠️ **The `model:` block minus `type` IS the `arch_dict` kwargs** (`engine.model_spec`),
so each model owns its own knobs and no module has to ignore another's. `type` is the
label; the *binding* decides which module builds the architecture.

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

**New model** (GRU/Transformer/TCN): create `src/model/<name>/` with

1. **`model.py`** — an `nn.Module` mapping `(batch, d, n) → (batch,)`, plus
   `build_model(n_features, **kwargs)` and `arch_dict(n_features, **kwargs)`. The
   `arch_dict` kwargs are exactly the `model:` block of a config minus `type`, which is
   how a CNN config carries `channels`/`kernel_size` while an LSTM config carries
   `hidden_size`/`num_layers` without either module knowing about the other.
2. **`train.py`** — a ~30-line binding: `engine.train(config, model_module=…,
   model_type=…)` and `engine.run_cli(…)`. Copy `cnn/train.py`.
3. **`configs/`** — ⚠️ filenames prefixed with the model (see §2).
4. **`__main__.py`** — `from model.<name>.train import main`.

⚠️ **DO NOT COPY `lstm/train.py`. That was this section's advice until 2026-08-10 and
it was wrong.** The file was 346 lines of which **eight** were model-specific — the
import, the `arch_dict` call, and the string `"LSTM"` twice. Copying it would duplicate
`_verify`, `_write_predictions`, the lineage block and `_registry_row` per model, which
is the shape of issue **TGT-1** (one rule implemented "in a second place that could
drift from it"). They now live in **`common/engine.py`**, once.

⚠️ **`_write_predictions` is the part that had to stop being copyable.** Two of its
behaviours are load-bearing and neither is visible from a call site: regression
predictions are inverse-transformed to the RETURN scale (scoring the standardised target
makes RMSE depend on the train slice's variance, so two datasets stop being comparable),
and a missing `ticker` column makes an N-ticker panel score as one series — `n_eff`
counting 20 banks on a date as 20 observations is issue **PNL-1**. A per-model copy that
drifted on either would produce a run that looks finished and is not comparable to the
runs beside it. **Two model types now land in `index.csv` scored by identical code,
which is the only reason their rows may be read against each other.**

Everything in `common/` and all of `result_evaluator` is reused unchanged — the
`Trainer` is model-agnostic and the core metric block reads a score, not an
architecture. Prefix `run_name` with the model so runs stay distinct in the shared
`runs/` and `index.csv`.

⚠️ **`model_type` is passed by the binding, not read from `config["model"]["type"]`.**
The YAML field is a label a person edits; what lands in the shared leaderboard should
not be changeable that way.

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

## 13. The `pool__basic` prototype — two architectures, same answer (2026-08-10)

Both trained on `vcb__return_5day__final__d20_h5__basic__tr70_val15_test15__std`
(2,939 / 615 / 640 × 20 × **4**, hash `08c7a0498ab2c934`), from the chain in
`pipeline/CONTEXT.md` §5d. **Same dataset, splits, purge, seed and a byte-identical
`train:` block — only the `model:` block differs**, so any gap is architecture.

| | LSTM | CNN |
|---|---|---|
| architecture | 1×32, last timestep | 2× `Conv1d(32, k=3)` over TIME + global avg pool |
| parameters | 4,961 | **3,745** |
| best epoch | 10 (of 30) | **2** (of 22) |
| best val loss | 0.6113 | 0.6073 |
| **test IC** | **−0.0345** | **−0.0332** |
| test IC bar / p | +0.1348 / 0.726 | +0.1107 / 0.657 |
| test dir_auc (bar) | 0.4743 (0.5685) | 0.4912 (0.5544) |
| test hit_rate | 0.4859 | 0.5078 |
| test RMSE / zero-baseline | 0.0383 / 0.0372 ❌ | 0.0373 / 0.0372 ❌ |
| test R² | −0.0585 | **−0.0081** |

**Verdict on all four splits: NO SKILL DEMONSTRATED.** Both test ICs are negative, both
sit deep inside their own null, and both lose to predicting a flat zero.

⚠️ **The CNN's `best_epoch 2` is the tell.** It stopped improving on validation almost
immediately; the other twenty epochs were early-stopping patience. That is the §10
`direction_5day` signature — "most runs stop at best epoch 1, never learn past init" —
now reproduced on a regression target with a different architecture.

⚠️ **Two architectures with different inductive biases converging on ≈ −0.033 is a
statement about the DATA, not about either model.** The LSTM consumes the window
sequentially and predicts from the last hidden state; the CNN learns width-3 shape
detectors and averages them over the window. They disagree about almost everything
except the answer.

⚠️ **AND TWO ARCHITECTURES ON ONE DATASET IS TWO DRAWS AT THE SAME QUESTION.** The
evaluator's null prices in neither the architecture choice nor the feature selection
(issue **NUL-1**) — it sees a finished prediction vector. Had one of them cleared, that
would have been a second attempt, not a discovery. **A run that fails the null is dead;
a run that clears it is not yet alive.**

## 14. ⚠️ TIER 1 — THE BASELINES, AND THEY BEAT BOTH NETWORKS (2026-08-10)

Five estimators over the same dataset, same splits, same purge, same 200-draw null,
scored by the same code. `index.csv` had held **31 runs and not one linear model or
constant** — the study went straight to sequence models. This is the missing rung.

| model | params | test IC | bar | p | clears | dir_auc | bar | p | clears | RMSE | R² |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `BASELINE_ZERO` | **0** | — | — | — | — | 0.5000 | 0.5000 | 0.995 | ❌ | **0.03721** | −0.001 |
| `BASELINE_MEAN` | 1 | — | — | — | — | 0.5000 | 0.5000 | 0.995 | ❌ | 0.03727 | −0.005 |
| `BASELINE_AR` | 6 | +0.0557 | +0.0585 | 0.070 | ❌ | **0.5258** | 0.5247 | **0.040** | ⚠️ **✅** | 0.03723 | −0.002 |
| **`BASELINE_RIDGE_STATS`** | **25** | **+0.1005** | +0.1247 | 0.095 | ❌ | 0.5329 | 0.5592 | 0.194 | ❌ | 0.09252 | **−5.19** |
| `BASELINE_RIDGE_FLAT` | 81 | −0.0397 | +0.0785 | 0.577 | ❌ | 0.4746 | 0.5440 | 0.597 | ❌ | 0.07677 | −3.26 |
| `CNN` | 3,745 | −0.0332 | +0.1107 | 0.657 | ❌ | 0.4912 | 0.5544 | 0.567 | ❌ | 0.03734 | −0.008 |
| `LSTM` | 4,961 | −0.0345 | +0.1348 | 0.726 | ❌ | 0.4743 | 0.5685 | 0.816 | ❌ | 0.03826 | −0.059 |

**Three findings, and the first is the one that matters.**

⚠️ **1. A 25-PARAMETER RIDGE HAS THE BEST TEST IC ON THE BOARD — `+0.1005`, three times
either network, both of which are NEGATIVE.** The two smallest fitted models (25 and 6
parameters) are the only ones with a positive test IC at all. That is §6d's capacity
argument measured directly: train carries 2,939 windows but `n_eff` is **588** on label
overlap and **122** on window overlap, and a 4,961-parameter model on 122 independent
observations is not underfitting.

⚠️ **2. NOTHING BEATS THE ZERO PREDICTOR ON RMSE, INCLUDING THE MODEL WITH THE BEST
IC.** `ridge_stats` posts `RMSE 0.0925` against zero's `0.0372` and **`R² = −5.19`** —
it ranks well and its magnitudes are wrong by a factor of 2.5. `beats_zero_baseline` is
False for all seven. Rank and calibration are separate questions and this dataset
separates them.

⚠️ **3. `BASELINE_AR` CLEARS ITS `dir_auc` BAR — AND IT IS ALMOST CERTAINLY NOISE.**
0.5258 against a bar of 0.5247 at `p = 0.040`, from a 6-parameter autoregression on
`close_adjust`'s own last five values. **Seven runs × two nulled metrics is 14 tests**;
at a 95th-percentile bar, ~0.7 false positives are expected and exactly one appeared, in
the most trivial model, by 0.0011. `NULL_DRAWS = 200` prices in none of that. This is
what a multiple-comparison artefact looks like, and it is recorded rather than promoted.

⚠️ **`BASELINE_ZERO` and `BASELINE_MEAN` have NO IC, and that is correct.** A constant
prediction has zero variance, so Spearman is undefined. Their value is `RMSE`, and it is
the reference every other row is read against.

⚠️ **The train MEAN return is a worse forecast than zero** (`RMSE 0.03727` vs
`0.03721`). Small, but it is why `zero` had to be built to emit a genuine zero return
rather than 0 in the scaled space — see §12.

**What this changes about §10's verdict: nothing, and it strengthens it.** Seven models
spanning 0 to 4,961 parameters and four model families all land inside their own nulls.
A linear model reaching the same answer as an LSTM is a far stronger statement about the
data than another network reaching it.

## 15. ⚠️ TIER 2 — ELEVEN MODELS, AND THE WHOLE SPREAD IS ONE ERROR BAR (2026-08-10)

Four more models on the identical dataset, splits, purge and 200-draw null. The board,
ordered by capacity:

| model | params | best ep | test IC | bar | p | clears | dir_auc | bar | p | clears | RMSE | R² |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `BASELINE_ZERO` | **0** | — | — | — | — | — | 0.5000 | 0.5000 | 0.995 | ❌ | **0.03721** | −0.001 |
| `BASELINE_MEAN` | 1 | — | — | — | — | — | 0.5000 | 0.5000 | 0.995 | ❌ | 0.03727 | −0.005 |
| `BASELINE_AR` | 6 | — | +0.0557 | +0.0585 | 0.070 | ❌ | 0.5258 | 0.5247 | **0.040** | ⚠️ ✅ | 0.03723 | −0.002 |
| `BASELINE_RIDGE_STATS` | 25 | — | +0.1005 | +0.1247 | 0.095 | ❌ | 0.5329 | 0.5592 | 0.194 | ❌ | 0.09252 | −5.19 |
| `BASELINE_RIDGE_FLAT` | 81 | — | −0.0397 | +0.0785 | 0.577 | ❌ | 0.4746 | 0.5440 | 0.597 | ❌ | 0.07677 | −3.26 |
| **`MLP`** | 257 | 3 | **−0.1001** | +0.0908 | 0.910 | ❌ | 0.4613 | 0.5422 | 0.776 | ❌ | 0.09751 | −5.87 |
| **`LSTM` (h=8)** | 473 | 41 | **+0.0346** | +0.0956 | 0.249 | ❌ | 0.4888 | 0.5450 | 0.597 | ❌ | 0.03920 | −0.111 |
| **`GRU`** | 1,105 | 10 | −0.0766 | +0.0786 | 0.726 | ❌ | 0.4463 | 0.5393 | 0.791 | ❌ | 0.03776 | −0.031 |
| **`GBT`** | 1,319† | — | **+0.1263** | +0.1121 | **0.035** | ⚠️ **✅** | 0.5246 | 0.5593 | 0.244 | ❌ | 0.06562 | −2.11 |
| `CNN` | 3,745 | 2 | −0.0332 | +0.1107 | 0.657 | ❌ | 0.4912 | 0.5544 | 0.567 | ❌ | 0.03734 | −0.008 |
| `LSTM` (h=32) | 4,961 | 10 | −0.0345 | +0.1348 | 0.726 | ❌ | 0.4743 | 0.5685 | 0.816 | ❌ | 0.03826 | −0.059 |

† decision NODES, not weights — a boosted ensemble has none. See `gbt/model.py`.

### ⚠️ 15a. The finding: the spread IS the error bar

| | |
|---|---|
| IC range over 9 scored models | **−0.1001 … +0.1263**, a span of 0.227 |
| SE(IC) at `n_eff = 128` (label overlap) | **0.089** |
| SE(IC) at `n_eff = 26.7` (window overlap) | **0.197** |
| largest \|t\| on the board (`GBT`) | **+1.42** / **+0.64** |

**Every model on this board is within 1.5 standard errors of zero, and the entire
eleven-model spread is barely wider than ONE window-overlap standard error.** Ranking
these architectures is reading noise. That is the same arithmetic
`feature_selection/CONTEXT.md` §6d gives from the other direction: separating an IC of
0.05 from zero needs ~1,500 independent observations and the test split carries 27.

### ⚠️ 15b. Two runs clear a bar. Expectation was 1.1.

`BASELINE_AR` on `dir_auc` (p = 0.040) and `GBT` on `ic` (p = 0.035). **Eleven runs × two
nulled metrics = 22 tests; at a 95th-percentile bar ~1.1 false positives are expected and
2 appeared.** Neither survives as evidence: the null prices in no architecture search
(**NUL-1**), and the sweep IS an architecture search. Recorded, not promoted.

### ⚠️ 15c. Capacity is real but it is not the whole story

**The LSTM flipped sign on capacity alone**: `h=32` (4,961 params) scores **−0.0345**,
`h=8` (473 params) scores **+0.0346**, same family, same data, same everything else. It
also trained far longer before stopping (best epoch 41 against 10) — a smaller model on
122 independent observations has something to learn for longer.

**But smaller is not simply better**, and the `MLP` is the counterexample: 257 parameters
and the **worst IC on the board** (−0.1001), on the same 24-column design that the
25-parameter ridge scores +0.1005 on. Family, design and capacity all move the number by
about one error bar each, which is what "no signal" looks like from the inside.

### ⚠️ 15d. Ranking and calibration disagree, hard

The three models with the best IC — `GBT` (+0.126), `ridge_stats` (+0.101), `AR`
(+0.056) — post RMSEs of **0.0656, 0.0925 and 0.0372** against the zero predictor's
**0.03721**. `GBT`'s R² is **−2.11**, `ridge_stats`'s is **−5.19**. **Not one of eleven
models beats the zero predictor on RMSE.** The models that rank best are the ones whose
magnitudes are most wrong; the models with sane magnitudes (`CNN`, `GRU`, both LSTMs)
rank at or below zero.

### 15e. What Tier 2 was for, and what it settled

`GBT` was the highest-value run in the tier because **it is the estimator
`feature_selection` has been ranking with all along** — `xgb_gain`, `xgb_shap` and
`permutation` are XGBoost fits on this same design. Its `+0.1263` is therefore the
closest thing on the leaderboard to the selection's own `+0.0783`, reached
independently, and it is the strongest single number any model here has produced.
**It is still 0.64 window-SE from zero, and it still cannot predict a magnitude.**

## 16. ⚠️ THE BANK PANEL — TIER 1 AT PANEL GRAIN, AND A NULL THAT LIES (2026-08-10)

The data was extended to `unified_schema_bank` — all **20 banks re-scraped** to
2026-08-07, so the panel has one uniform as-of date instead of VCB running 31 sessions
ahead of the other 19. `pool__basic` went **53,921 → 54,528 rows**.

| | VCB (series) | **BANK (panel)** |
|---|---|---|
| dataset | 2,939 / 615 / 640 × 20 × **4** | **27,348 / 12,629 / 13,135 × 20 × 10** |
| test rows / dates / tickers | 640 / — / 1 | 13,135 / **658** / **20** |
| `n_eff` | 128 (`n/h`) | **131.6** (`n_dates/h`) |
| selection | +0.0783 vs bar +0.0562, **z = +2.15** | **−0.0106** vs bar +0.0216, **z = −1.71** ❌ |

⚠️ **THE SELECTION FAILED DECISIVELY, AND ITS OBSERVED IC IS BELOW ITS NULL'S MEAN**
(−0.0106 against +0.0052) — the same signature `pool__fa` showed at z = −0.25. Twenty
names is far below the ~100 the width ladder puts the threshold at, and §13d's reading
stands: **a sector CO-MOVES, so there is less to rank.**

### 16a. The five baselines, and why `ic_clears` must be ignored here

| model | params | test IC | evaluator bar | p | `clears` | **daily-IC t** | days + |
|---|---|---|---|---|---|---|---|
| `BASELINE_ZERO` | 0 | — | — | — | — | — | — |
| `BASELINE_MEAN` | 1 | — | — | — | — | — | — |
| `BASELINE_AR` | 6 | +0.0056 | **−0.0059** | 0.005 | ⚠️ **YES** | **+0.23** | 51.4% |
| `BASELINE_RIDGE_STATS` | 61 | **+0.0230** | +0.0383 | 0.254 | ❌ | **+1.15** | 54.6% |
| `BASELINE_RIDGE_FLAT` | 201 | +0.0156 | +0.0307 | 0.249 | ❌ | +0.77 | 53.6% |

⚠️ **THE EVALUATOR'S PANEL NULL GOT BOTH ENDS WRONG** (issue **NUL-3**). It reported
`BASELINE_AR` as clearing `ic` **against a NEGATIVE p95 bar of −0.0059**, at `p = 0.005`
— for a model whose honest daily-IC t-stat is **+0.23**. And it FAILED
`BASELINE_RIDGE_STATS`, the highest t on the panel. The null's centre moved with the
MODEL — −0.0171 for AR, +0.0076 for ridge_flat, +0.0109 for ridge_stats — which a
label-shuffle against a fixed score vector must not do. AR predicts from `close_adjust`,
a price LEVEL that is near-static per ticker, so its score is close to "which bank is
this" and the shuffle interacts with it.

**On a panel, quote the daily-IC t-stat. Nothing here reaches |t| = 1.2.**

⚠️ **`sqrt(n_days)` IS THE WRONG DENOMINATOR AND IT NEARLY DOUBLES EVERY t.** At `h=5`
consecutive daily ICs share four of their five label days, so the independent count is
`n_dates/h` = **131.6**, not 658 (§5 rule 7). Computed naively, `ridge_stats` reads
**t = +2.56**; correctly, **+1.15**. The first number was on screen before the second.

⚠️ **The daily-IC sd cross-checks the width ladder.** 0.230–0.280 measured here against
§13's **0.244** for the same 20-name universe, and `1/√N` predicts ~0.22 from VN100's
0.130. The panel is behaving exactly as the ladder says; there is simply nothing to rank.

⚠️ **`close_adjust` is index 2 on this dataset, not 0.** The VCB `ar` config pinned
`target_channel: 0`; had that carried over, the bank AR baseline would have silently
autoregressed on `avg_vol_per_buy_order`. `ARPredictor` now resolves by NAME and raises
on an unknown one.

## 12. Gotchas

- ⚠️ **A baseline in the SCALED space is not the baseline you meant.** Every estimator
  returns a scaled target and `engine._write_predictions` inverts it on the way out, so
  a `ZeroPredictor` emitting `0.0` produces the **train mean return**, not zero. It
  shipped that way and was caught only because its `RMSE` (0.03726) disagreed in the
  fourth decimal with the `RMSE_zero_baseline` column (0.037212) it should have equalled
  exactly. It now asks the target scaler which scaled value maps to a zero return.
  **The two numbers being identical is the standing self-check.**
- ⚠️ **`metrics.verdict` puts a `⚠️` in its CLEARING branch and nowhere else**, and a
  Windows console is cp1252. So the only path that could raise `UnicodeEncodeError` was
  the one reporting a POSITIVE result, and it had never fired because no run had ever
  cleared. The first that did (`baseline_ar`) crashed the process *after* `append_run` —
  the run was recorded, the report was not. `engine._console_safe` now reconfigures both
  streams with `errors="replace"`.
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
- ⚠️ **`src/train_test_set/` HOLDS ONE DATASET** (checked 2026-08-10):
  `vcb__return_5day__final__d20_h5__basic__…`, built by the prototype chain. This
  section claimed "all 33 pre-2026-08-09 dataset folders are on disk and every config
  resolves" — **that has not been true since before 2026-08-09**. The folder is
  git-ignored (issue **RPR-1**), so a fresh checkout has none at all. Consequences: the
  27 legacy configs and the two legacy notebooks reference datasets that must be
  **rebuilt** with `python -m train_test_creator --save` before they run, and every past
  run's `dataset_hash` is currently unverifiable. **31 run folders DO survive** under
  `src/model/runs/`, and `result_evaluator --rescore` reads them from
  `predictions_*.csv` without any dataset.
- ⚠️ **The two legacy notebooks were DELETED 2026-08-16** — `lstm_return_5day.ipynb` and
  `lstm_direction_5day.ipynb`. They are tracked, so `git checkout f12ef091 --
  src/model/lstm/lstm_return_5day.ipynb` brings either back. The text they replaced read:

  > *"The legacy notebooks … and the 27 legacy configs still **resolve**. ⚠️ But they
  > write runs scored by the OLD path (metrics computed in-notebook, no null), so a run
  > produced that way lands in `index.csv` with the core columns blank until
  > `python -m result_evaluator --rescore` fills them."*

  That was the argument for deleting them: a second way to produce a run folder, which
  scores it by a path that carries **no null**. `RUN__lstm.ipynb` + `train.py` scores as
  it goes, and is now the only way in. **The 27 legacy configs still resolve** and were
  left alone.

  ⚠️ **Two functions in `model/common/metrics.py` are now dead** — its docstring says
  they are "kept so the existing notebooks keep running unchanged", and those notebooks
  are gone. Left in place deliberately: this was a notebook removal, not a code change,
  and `model/common/trainer.py:81` still cites one of them in a comment.
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
