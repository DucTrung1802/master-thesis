# Context — `src/model` (dataset creation → model training)

> Handoff notes for a new session. Describes how datasets and model training runs
> are produced under `src/model`. Verify anything before acting on it — the DB,
> `src/train_test_set/`, and `src/model/runs/index.csv` are the sources of truth.

## 1. Big picture / pipeline

```
unified_schema_<ticker>.<target>__lb<L>__final     ← a VIEW of pre-selected features
   │   (built upstream by src/train_test_creator/unified_schema_creator.ipynb)
   │
   │   src/train_test_creator/train_test_creator.ipynb
   │   (clean → chronological split → scale train-fit → sliding windows → save)
   ▼
src/train_test_set/<dataset_name>/                 ← X/y .npy tensors + scalers + dates + metadata
   │
   │   a model notebook REFERENCES the dataset by name + content hash (never clones it)
   ▼
src/model/<model>/  (config-driven notebook)  →  src/model/runs/<run_id>/
                                                  (config, checkpoints, TensorBoard, metrics)
```

- **Upstream** (feature selection + the `__final` view) is documented in
  `src/train_test_creator/CONTEXT.md`. This file starts from the view onward.
- **One sample** = a `(LOOKBACK_DAY, n_features)` window of all view features
  ending on day *t*; **label** = `target` at day *t* (for `return_5day`, the
  5-day-ahead return `close[t+5]/close[t] − 1`).
- **DB**: PostgreSQL `database_main_v2`, schema `unified_schema_<ticker>` (e.g.
  `unified_schema_vcb`). Creds in repo `.env` (`POSTGRES_*`).

## 2. Directory layout

```
src/model/
├── CONTEXT.md            ← this file
├── common/               ← shared framework lib (used by every model)
│   ├── data.py           load_dataset (reference + hash), Dataset dataclass
│   ├── run_dir.py        RunDir: immutable run folder, config/metadata, git sha, env
│   ├── trainer.py        Trainer (train loop, early stop, TensorBoard, checkpoints),
│   │                     TrainConfig, set_seed, resolve_device, to_loaders;
│   │                     criterion is configurable (default MSE; BCE for classifiers)
│   ├── metrics.py        regression_metrics + classification_metrics (both share the
│   │                     dir_accuracy/dir_auc keys so reg vs clf compare directly)
│   └── registry.py       append_run → runs/index.csv leaderboard (has a `task` col)
├── lstm/                 ← ONE model = code only (no run outputs live here)
│   ├── model.py          LSTMRegressor + build_model + arch_dict (last Linear → scalar;
│   │                     that scalar is the return for regression / the logit for a clf)
│   ├── configs/          return_5day_lb{1,2,3,5,10,15,20,25,30}.yaml
│   │                     direction_5day_lb20.yaml (classification; task: classification)
│   ├── lstm_return_5day.ipynb      regression notebook (MSE, regression_metrics)
│   └── lstm_direction_5day.ipynb   classification notebook (BCEWithLogitsLoss, sigmoid
│                                    eval, classification_metrics, ROC plot)
└── runs/                 ← ALL runs from ALL models AND tasks (shared)
    ├── index.csv         cross-run leaderboard (TRACKED in git)
    └── <run_id>/         one immutable run (git-IGNORED: see .gitignore src/model/runs/*/)
```

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

## 4. Create a dataset — `src/train_test_creator/train_test_creator.ipynb`

Set the params cell and Run All (or run headless with nbconvert). Key params:

- `TICKER = "vcb"`, `TARGET = "return_5day"`, `LOOKBACK_DAY = <L>`
- `LOOKBACK_DAY` **must match** the view's lb — it both picks the source view
  (`<TARGET>__lb<L>__final`) and sets the window length.
- Split: chronological `TRAIN_RATIO=0.70`, `VAL_RATIO=0.15` (test = rest).
- `SCALE_TARGET=True` (StandardScaler, saved for inverse-transform).

Output → `src/train_test_set/<ticker>_<target>_lb<L>_h5_final_tr70_val15_test15_std/`
containing `X/y_{train,val,test}.npy`, `dates_{train,val,test}.npy` (per-window
label date = window's last day), `feature_scaler.pkl`, `target_scaler.pkl`,
`metadata.json`.

**Leak-free windowing**: features scaled with **train-only** statistics; val/test
windows start `LOOKBACK_DAY−1` rows early so their first window is complete
without borrowing label rows across splits; labels only look forward.

Headless: `cd src/train_test_creator && python -m nbconvert --to notebook
--execute --inplace train_test_creator.ipynb` (uses the `mt_env` env).

> The `train_test_set/` folder is git-ignored (`*.npy` + the dir). `LOOKBACK_DAY`
> is a manual param — during a lookback sweep it is edited per run and left
> **uncommitted** (transient), so the committed notebook default is not churned.

## 5. Train a model — `src/model/lstm/lstm_return_5day.ipynb`

Config-driven and thin (≈ load config → `RunDir.create` → `load_dataset` →
build model → `Trainer.fit` → evaluate → save + registry). To run:

1. Pick/edit a config in `lstm/configs/` (see §6 for the schema).
2. Interactive: set `CONFIG_PATH` (top of notebook, or the `CONFIG_PATH` env var;
   default `configs/return_5day_lb20.yaml`) and **Run All**.
3. Headless for config `X`:
   ```
   cd src/model/lstm
   CONFIG_PATH=configs/return_5day_lb<L>.yaml \
     python -m nbconvert --to notebook --execute \
     --output <scratch>/executed.ipynb lstm_return_5day.ipynb
   ```
   Use `--output <scratch>` (not `--inplace`) to keep the committed notebook
   clean; the run folder + `index.csv` row are written as side effects.

Runs go to the shared `src/model/runs/` (`RUNS_DIR = ../runs`). GPU is auto
(`device: auto`). Compare runs: `tensorboard --logdir src/model/runs` (HPARAMS
tab = config vs metrics), or open `src/model/runs/index.csv`.

## 6. Config schema (`lstm/configs/<target>_lb<L>.yaml`)

```yaml
run_name: lstm__return_5day__lb20__final          # <model>__<target>__lb<L>__final
dataset: vcb_return_5day_lb20_h5_final_tr70_val15_test15_std   # folder under src/train_test_set/
model:   {type: LSTM, hidden_size: 128, num_layers: 2, dropout: 0.2}
train:   {batch_size: 64, lr: 0.001, weight_decay: 0.00001,
          max_epochs: 150, patience: 20, grad_clip: 1.0,
          lr_factor: 0.5, lr_patience: 5, log_every: 10}
seed: 42
device: auto                                       # auto | cuda | cpu
```

## 7. End-to-end recipe: new `(target, lookback)`

1. **Selection tables + view** (upstream, DB) — for each group build
   `<target>__lb<L>__<group>__<n>`, then the `<target>__lb<L>__final` VIEW.
   Use `src/train_test_creator/unified_schema_creator.ipynb`
   (`build_selected_table(group, target, max_features, lookback=L, tree_only=?)`
   then `build_final_view(target, L)`). Groups: `calendar`, `basic`, `macro`,
   `ta`. **Use `tree_only=True` (GPU) for wide pools** — always `ta`, and
   `macro` at `L ≳ 25` (see §8).
2. **Dataset** — set `LOOKBACK_DAY=L` in `train_test_creator.ipynb`, run → dataset
   folder (§4).
3. **Config** — copy an existing `lstm/configs/return_5day_lb*.yaml`, set
   `run_name`, `dataset`, lookback.
4. **Train** — run the LSTM notebook with that `CONFIG_PATH` (§5).
5. **Result** — new `src/model/runs/<run_id>/` + a row in `index.csv`.

New target (e.g. `return_rel_5day`): same steps, just change `TARGET`/`target`
throughout (the pool + `pool__targets` column already exist upstream).

New model (e.g. GRU/Transformer/TCN): create `src/model/<name>/` with a
`model.py` (an `nn.Module` mapping `(batch, lookback, n_features) → (batch,)`,
plus `build_model`/`arch_dict`), a `configs/`, and a notebook copying
`lstm_return_5day.ipynb`. Everything in `common/` is reused unchanged (the
`Trainer` is model-agnostic). Prefix `run_name` with the model so runs stay
distinct in the shared `runs/` + `index.csv`.

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

## 9. Metrics (`common/metrics.regression_metrics`)

Computed on the **original return scale** (predictions are inverse-transformed
before scoring), for `val` and `test`, into `results/metrics.json` and
`index.csv`:

- `RMSE`, `MAE`, `r2`
- `RMSE_zero_baseline` — error of always predicting 0; **the bar to beat** for any
  absolute-return edge (`beats_zero_baseline` flag)
- `dir_accuracy` — sign hit-rate at the 0 threshold
- `dir_auc` — ROC-AUC ranking up-days vs down-days using the predicted return as
  the score (threshold-free direction skill; 0.5 = none)
- `spearman_ic` — rank correlation of predictions vs outcomes
- `hit_rate_pos` — precision of the "predict up" calls

Metrics are **re-computable from `predictions_{val,test}.csv` without retraining**
(that is how `dir_auc` was backfilled across all runs).

**Classification** (`classification_metrics`, for binary targets like `direction_5day`):
scored on the predicted probability `P(up) = sigmoid(logit)` (`predictions_*.csv` has
`y_true`,`y_prob`), for `val`/`test`:

- `dir_accuracy` — accuracy at 0.5 (SAME index.csv column the regressor fills from the
  sign of its predicted return, so classifier vs regressor compare directly)
- `majority_baseline_acc` — accuracy of always predicting the majority class; the bar to
  beat (`beats_majority` flag). `base_rate` = share of up-days
- `dir_auc` — ROC-AUC of `P(up)` (threshold-free skill; comparable to the regressor's)
- `pr_auc`, `precision`/`recall`/`f1` (at 0.5), `log_loss`, `brier`

**Adding a classification target** (already wired for `direction_5day`,
`probability_gain_5pct_5day`): build the `__final` view (§7), build the dataset with
**`SCALE_TARGET=False`** (a 0/1 label is never scaled → `target_scaler=None`), copy
`configs/direction_5day_lb20.yaml` (set `task: classification`), and run
`lstm_direction_5day.ipynb` (it passes `criterion=nn.BCEWithLogitsLoss()` to the
otherwise-identical `Trainer`; the model emits a logit — no model change needed).
The `index.csv` header is a superset over both tasks (`task` col disambiguates;
`best_val_loss` = MSE for regression / BCE for a classifier).

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
- **Next experiments**: `return_rel_5day` regression sweep. Note a single-ticker
  time-series model is not expected to capture the *relative* edge — that is inherently
  cross-sectional (rank many names against each other, not one name over time). For an
  imbalanced target like `probability_gain_5pct_5day`, a `pos_weight` on BCE would fix
  the degenerate 0.5-threshold predictions but would NOT change the AUC skill verdict.

## 11. Gotchas

- **`src/model/runs/*/` is git-ignored** (checkpoints/TB/plots — reproducible);
  only `runs/index.csv` is tracked (`.gitignore`: `src/model/runs/*/` +
  `!src/model/runs/index.csv`).
- **Dataset is referenced by name + hash**; if you rebuild a dataset the hash
  changes and `load_dataset(expected_hash=...)` will flag a mismatch.
- **`driver.select` swallows exceptions** and returns an empty DataFrame — a bad
  view/column silently yields 0 rows.
- **SQL `LIKE '_'` is a single-char wildcard**: discovering `<target>__lb2__%`
  tables also matched `lb20` (duplicate columns → `CREATE VIEW` error). Fixed in
  `unified_schema_creator.build_final_view` by filtering with Python
  `startswith(prefix)`. Watch for this in any table-name discovery.
- **Batch helper scripts are ephemeral** (they lived in the session scratchpad,
  not the repo): one looped `build_selected_table`+`build_final_view` over
  lookbacks; one replicated `train_test_creator`'s dataset stage to build several
  datasets without editing the notebook per lookback; one backfilled metrics from
  predictions. The **committed notebooks are the source of truth** — recreate a
  script from them if you need batch runs.
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
