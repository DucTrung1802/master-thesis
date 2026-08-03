# Context — `src/feature_selection`

> Reads a per-ticker `unified_schema_<ticker>` schema, joins its `pool__*` tables on
> the keys they share, and ranks every feature against one target. Built 2026-08-03
> against `unified_schema_vcb.pool__basic ⋈ pool__targets`, target `return_5day`.
>
> **Entry point: [feature_selection.ipynb](feature_selection.ipynb).** The three
> modules hold nothing notebook-specific, so the same run scripts.

## 1. What is here

| file | does |
|---|---|
| [unified_reader.py](unified_reader.py) | connect, introspect, read with the right dtypes, join on `(exchange, ticker, date)` ∩ |
| [selector.py](selector.py) | six rankers → ensemble → correlation prune → walk-forward validation |
| [gpu.py](gpu.py) | the CUDA paths, the size heuristic, and which steps have no GPU path |
| [plots.py](plots.py) | the figures — one theme, one palette, applied by the job each colour does |

**This replaces `feature_selector.FeatureSelector`, which does not exist on this
branch.** `train_test_creator/unified_schema_creator.ipynb` cell 23 still imports
it; that import fails here. The old class ranked with an XGB+SHAP+LASSO+ElasticNet
blend and wrote `<target>__lb<N>__<group>__<n>` tables — see
`orchestration/CONTEXT.md` §UNIFIED for what happened to those (all 135 dropped
2026-08-03). **Nothing in this package writes to the database**; a selection is a
result object and a set of figures, not a table.

## 2. The three things that make the output mean anything

**1. The label looks forward, so the CV is purged.** `return_5day` at day `t` is
computed from the close at `t+5`. A random K-fold puts `t+1` in train and `t` in
test and the model reads its own answer — the usual way a feature-selection
notebook reports an R² of 0.4 on a series that is mostly noise. `PurgedWalkForward`
is expanding-window, in date order, and drops the `horizon` training rows
immediately before each test block.

**2. Overlapping labels mean the effective sample is n/horizon.** Consecutive
`return_5day` values share 4 of their 5 days. Nothing here fixes that; what it does
is refuse to report a single averaged number as if it were one, and show the
per-fold spread instead.

**3. Imputation is fitted on train only.** Inside the CV every fold takes its
median from its own train slice. The whole-sample ranking uses a whole-sample
median and says so — that ranking is descriptive, and the walk-forward numbers are
the ones carrying a generalisation claim.

## 3. The join, and why it is not on `date + ticker`

The request was "join on date + ticker". **`pool__targets` has no `ticker`
column** — it is `(date, return_5day)`, because a one-company label table has
nowhere to put one. So `join()` uses the INTERSECTION of `(exchange, ticker, date)`
present in both frames, requires `date` to be in it, and records what it used in
`join_log`. For `["pool__basic", "pool__targets"]` that is `["date"]`; add a pool
that does carry `ticker` and the same call joins on `(exchange, ticker, date)` with
no change.

⚠️ **Every merge is validated one-to-one on both sides first.** A duplicated key
turns a 4,235-row panel into a longer one that still looks like a panel, and the
selection would then be fitting on repeats.

⚠️ **`numeric` comes back as `Decimal`.** psycopg2 maps PostgreSQL `numeric` to
Python `Decimal`, which pandas carries as dtype `object` — `close_adjust` would
arrive looking like a string column. `read()` casts by the SQL type read from
`information_schema`, not by guessing from the values. This is the same trap
`orchestration/assets/unified.py` documents for the other direction (a pandas
round-trip turning every price column into VARCHAR).

## 4. The six rankers

| method | sees | blind to | GPU |
|---|---|---|---|
| `spearman` | monotone rank association | interactions, non-monotone shapes | ✅ |
| `mutual_info` | any dependence | direction | ❌ |
| `xgb_gain` | interactions, thresholds | correlated features split credit arbitrarily | ✅ |
| `xgb_shap` | the same, attributed per sample | the same, less arbitrarily | ✅ |
| `lasso` | linear signal, redundancy priced in | non-linearity | ❌ |
| `permutation` | **out-of-sample** contribution | features the model never used | ✅ |

`permutation` is the only one measured out of sample and is the one to believe when
it disagrees. The ensemble is a **rank** average, not a score average — `xgb_gain`
routinely spans three orders of magnitude and would otherwise decide the blend
alone after min-max.

⚠️ **A method that separated nothing is reported, not hidden.** On `pool__basic`,
`LassoCV` zeroes every coefficient, so `lasso` contributes a constant to the blend.
That is a finding about the data (no linear signal survives cross-validated
shrinkage), and `result.dead_methods` names it rather than leaving "an ensemble of
six" quietly an ensemble of five.

## 5. ⚠️ The device can change the ANSWER — because of stochastic sampling

Measured, same data, `cuda` vs `cpu`:

| quantity | `subsample/colsample = .8` | both `= 1.0` |
|---|---|---|
| both Spearman passes | 0.0 | 0.0 |
| `xgb_gain` / `xgb_shap` / `permutation` | up to **0.82** | **8.6e-08** |
| ensemble mean rank | up to **2.3 places** | **0.0** |
| the kept feature set | **different** | **identical** |

**Not the histogram sketch** — that was the obvious suspect and it is wrong.
Isolated by elimination: with sampling off the two devices grow bit-identical trees
(same features, same thresholds) at the default 256 bins, and raising `max_bin` 64×
changes nothing. With sampling on, 4,189 of 8,280 nodes pick a different feature —
XGBoost's row/column subsampling draws from a different RNG stream on the GPU, so
the same `random_state` selects different rows, and it compounds over 300 rounds.

`FeatureSelector(subsample=1.0, colsample_bytree=1.0)` therefore buys exact
cross-device reproducibility, at the cost of the regularisation subsampling
provides. Otherwise `device` is part of the experimental setup and is recorded on
every `SelectionResult` as `result.device`. Pin one or the other before quoting a
selection.

⚠️ **And on a narrow pool the GPU is SLOWER**: 21.2 s against 12.3 s on
4,235 × 27 (RTX 3050 Laptop, 4 GB) — 27 columns is too little work per kernel
launch. `device="auto"` therefore stays on the host below
`gpu.AUTO_CUDA_MIN_FEATURES` (200) and says so in `device_report()["reason"]`;
`device="cuda"` forces the GPU everywhere a path exists and raises if there is
none. Full tables in [gpu.py](gpu.py).

⚠️ **The correlation-matrix win is the ALGORITHM, not the GPU.** Five matmuls
instead of pandas' pairwise Cython loop is worth 46-80×; the GPU adds 1.0-2.0× on
top and the margin *shrinks* with width, because a consumer GeForce runs float64
at 1/32 of its fp32 rate. float64 is kept anyway — it is what makes the two devices
agree to 0.0 on that step.

⚠️ **SHAP comes from the booster, not the `shap` package.** XGBoost's own
`pred_contribs=True` runs the same exact tree-SHAP algorithm inside the booster, so
on CUDA it runs there with the model already resident. Verified equal to
`shap.TreeExplainer` at **0.0**, and faster.

## 6. What the first run found (VCB, `pool__basic`, `return_5day`)

| | |
|---|---|
| panel | 4,235 × 39, 2009-06-30 → 2026-06-25, joined on `date` |
| labelled | 4,230 + a 5-row NULL tail (exactly the horizon) |
| candidates | 27 numeric; identity + the 8 GICS columns are VARCHAR and constant for one ticker |
| pruned as redundant | 12 at \|ρ\| ≥ 0.9 — OHLC are four views of one price, every flow appears as both a volume and a value |
| out-of-sample IC | mean **+0.086** (selected) / +0.070 (all), but **fold 5 is negative** |
| strongest \|Spearman\| vs target | **≈ 0.07** |

⚠️ **Read the scale before the ranking.** Nothing in `pool__basic` clears |ρ| ≈ 0.1
against the forward 5-day return, and the IC is carried by the early folds and
turns negative in the last. The honest reading is that this pool does not predict
this target, which is the same conclusion `model/CONTEXT.md` and memory
`project-cross-sectional-strategy` already point at — the ranking below that is
ordering noise.

⚠️ **The price LEVELS top the ranking, and that is the artefact to watch.** `low`,
`close_raw` and `close_adjust` lead on `mutual_info` and `permutation` while
contributing almost nothing on `spearman`: a level rises over seventeen years, so a
tree can use it to identify the ERA rather than predict the return. The notebook
has `EXCLUDE_PRICE_LEVELS = True` to re-run without them and compare fold ICs.

## 7. Extending it

* **Another pool** — add it to `POOLS`. `pool__ta` (~900 columns) does not exist as
  an asset yet; `orchestration/CONTEXT.md` §UNIFIED has the status. At that width
  `device="auto"` moves to the GPU on its own.
* **Another ticker** — change `TICKER`. The schema name is a template and the
  reader validates it as an identifier.
* **Another target** — ⚠️ `selector.py` is **regression-only**. `direction_5day` and
  `probability_gain_5pct_5day` are binary and would be treated as continuous
  labels: the tree rankers would still run and the numbers would still look
  plausible. Classifier variants are the change to make first.
