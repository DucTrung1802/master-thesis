# Context — `src/feature_selection`

> Reads a per-ticker `unified_schema_<ticker>` schema, joins its `pool__*` tables on
> the keys they share, and ranks every feature against one target. Built 2026-08-03
> against `unified_schema_vcb.pool__basic ⋈ pool__targets`.
>
> **Three notebooks. Read the third one first.**
>
> | notebook | one sample is | for |
> |---|---|---|
> | [feature_selection.ipynb](feature_selection.ipynb) | one row → `y_N` | a per-row model. `lookback=1` |
> | [windowed_selection.ipynb](windowed_selection.ipynb) | a `(d, n)` window → `y_N` | **a sequence model.** `d=20`, `h ∈ {5, 10}` |
> | **[evaluation_study.ipynb](evaluation_study.ipynb)** | — | **whether either result is real.** It is not: see §6b-6d |
>
> ⚠️ **The first two both produce a positive out-of-sample IC. So does the same
> pipeline run on shuffled labels.** The third notebook is what tells them apart, and
> it is the one whose conclusion governs the other two. The rankings, kept sets and
> stat profiles there are internally consistent descriptions of noise.
>
> The modules hold nothing notebook-specific, so the same runs script.

## 1. What is here

| file | does |
|---|---|
| [unified_reader.py](unified_reader.py) | connect, introspect, read with the right dtypes, join on `(exchange, ticker, date)` ∩ |
| [windows.py](windows.py) | daily panel → windowed samples; scoring CHANNELS, not columns |
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

## 1a. ⚠️ The windowed setup — one sample, and the gap it forces

```
input   rows N-d+1 … N of the feature panel        a (d, n) matrix
output  y_N = pool__targets.return_{h}day[N]        = close[N+h]/close[N] − 1
```

The label already looks forward and lives **on row `N`**; nothing is shifted here.

A tree takes a vector, so `windows.window_design` reduces each channel's window to
six statistics — `last, mean, slope, sd, min, max`. `(4211, 20, 27)` becomes a
`(4211, 162)` design matrix. **`last` is the raw value at day `N`**, so `lookback=1`
reduces to the un-windowed selector exactly (verified: 4,230 samples, gap 5, 27
design columns, same as before the windowing existed).

⚠️ **Not one column per (feature, lag).** That is what `unified_schema_creator.ipynb`
did, it is 540 columns here and **18,000** on `pool__ta`, and it answers *which lag
matters* when the model reads every lag anyway. The six stats answer the question
that is useful — level, direction, or dispersion — at a third of the width, and
`plot_stat_profile` turns the answer into a chart.

⚠️ **THE PURGE GAP IS `d + h − 1`, NOT `h`.** Training sample `M` and test sample `N`
share nothing only if `M + h < N − d + 1`. At `d=20, h=5` that is **24 rows, not 5** —
purging only `h` would leave 19 rows of the test sample's own input window inside
training. This is the easiest way to make a windowed model look predictive when it is
not, and it gets worse as `d` grows. `PurgedWalkForward.gap` computes it; `lookback=1`
recovers exactly `horizon`.

⚠️ **Usable samples are `L − d − h + 1`, so the horizons differ**: 4,211 at `h=5`
against 4,206 at `h=10`. Each target's own tail is dropped, never a shared one.

⚠️ **The unit of selection is a CHANNEL.** The model consumes all `d` days of a
feature or none, so scoring, pruning and validation all happen at channel level —
scores are aggregated over a channel's six stats by **MAX** (`mean` and `last` are
near-duplicates on a slow series, so a SUM would score a channel twice for saying one
thing). Selecting `close_adjust` gives the model all six of its columns.

## 2. The three things that make the output mean anything

**1. The label looks forward, so the CV is purged.** `return_5day` at day `t` is
computed from the close at `t+5`. A random K-fold puts `t+1` in train and `t` in
test and the model reads its own answer — the usual way a feature-selection
notebook reports an R² of 0.4 on a series that is mostly noise. `PurgedWalkForward`
is expanding-window, in date order, and drops `lookback + horizon − 1` training rows
immediately before each test block (see §1a).

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

⚠️ **`permutation` is scored on Spearman IC, not R² (changed 2026-08-04).** It was
`scoring="r2"`, which ranked features by their contribution to a calibration §6 of
this file explicitly says not to decide on. The importance metric and the decision
metric are now the same one.

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

## 6a. The windowed run — `d = 20`, `h = 5` and `h = 10` (2026-08-04)

| | h = 5 | h = 10 |
|---|---|---|
| samples / purge gap | 4,211 / **24** | 4,206 / **29** |
| design matrix | 162 columns from 27 channels | same |
| IC by fold (selected) | +.097 +.072 +.153 −.005 **−.037** | +.166 +.047 +.033 +.026 **−.070** |
| mean IC — selected / all channels | **+0.056** / +0.034 | **+0.041** / +0.086 |
| IC trend per fold | **−0.034** | **−0.049** |
| **hit rate** | **0.477** | **0.460** |
| R² | −1.19 | −1.03 |

**8 of 12 channels are kept at BOTH horizons** — `close_adjust`, `foreign_own`,
`foreign_room_left`, `foreign_net_value`, `value_negotiated`, `buy_order_vol`,
`avg_vol_per_buy_order`, `avg_vol_per_sell_order`.

⚠️ **The stat profile is what the windowing bought.** `close_adjust` is carried by
`last` (a level — the era proxy again), `volume_negotiated` by `slope`, `low` and
`high` by `sd`, `sell_order_vol` by `max`. A channel that only ever wins on `last`
never needed a window.

## 6b. ⚠️ THE NULL TEST — and it does not clear it (2026-08-04)

The whole pipeline was re-run **20 times on BLOCK-SHUFFLED labels** (blocks of
`d + h`, so the label's own autocorrelation survives and the null is not made
artificially tight). Selection included, because selection is what inflates.

| | |
|---|---|
| null mean IC — **mean** | **+0.0167** |
| null mean IC — sd / p95 / max | 0.0252 / **+0.0556** / **+0.0606** |
| observed h=5 mean IC | **+0.0559** |
| empirical p-value `P[null ≥ observed]` | **0.050** (1 of 20) |
| z vs null | **+1.56** |

⚠️ **The null is centred on +0.017, not on zero.** Picking 12 of 27 channels by their
fit to the labels earns a positive IC from noise alone. So `IC > 0` is not a bar —
**`IC > ~0.017` is the floor before anything has been said**, and it will rise with
the number of candidates and configurations tried.

⚠️ **Noise beat the real data once in 20 tries** (null max +0.0606 > observed
+0.0559). The observed result sits at the 95th percentile of its own null: it is not
distinguishable from what this procedure produces on shuffled labels.

⚠️ **The hit rate is BELOW 0.5 at both horizons** (0.477, 0.460) while the IC is
positive. A model that ranks days but gets the direction wrong more often than right
is reading MAGNITUDE, not direction — the same conclusion `experiment_10` reached for
news, and consistent with memory `project-vcb-forecasting-conclusion`.

⚠️ **Effective sample size, which is what makes all of this fragile.** Overlapping
labels mean the independent count per fold is ~`n_test / h`: **147** at h=5, **74** at
h=10, against 737 rows. SE(IC) per fold is then ~0.083 and ~0.117 — larger than every
fold IC observed. The window overlap (`d−1` of `d` input days) makes even that
optimistic.

⚠️ **At h=10 the selection is WORSE than keeping everything** (+0.041 vs +0.086), while
at h=5 it is better (+0.056 vs +0.034). A selection that helps at one horizon and hurts
at the other is not selecting.

**The pipeline itself is deterministic** — three identical runs give bit-identical fold
ICs and kept sets, and `stability=True/False` changes nothing. So the variation above is
the data, not the code.

## 6c. The four-step study (2026-08-04) — what each step changed

Run at `d=20, h=5`, holdout `2024-06-01` onward (487 samples), 20-draw block-shuffled
null per configuration.

### Step 3 — the null, built into the package ([evaluation.py](evaluation.py))

`null_distribution` re-runs the WHOLE pipeline (selection included) on block-shuffled
labels. `ic_summary` reports mean **and trend** with an `n_eff = n/h` error bar.
`NullResult.bar` is the p95 — the number to beat instead of zero.

### Step 1 — the purged holdout, and the control that broke it

`FeatureSelector(holdout_start=…)` removes the holdout in `_prepare`, before any
ranker sees it, and purges `lookback + horizon − 1` rows from the development side of
the boundary too. `score_holdout` trains on all development data and scores once —
against **`all channels`** and against a **shuffled-label control**.

⚠️ **The control is what makes the holdout readable, and it says the holdout cannot
decide anything.** A model trained on RANDOMLY PERMUTED labels scored **+0.169** on
the holdout — higher than any real configuration in the study. At 487 rows and `h=5`,
`n_eff` is 97 and SE(IC) ≈ 0.102, so every holdout number here is inside one standard
error of zero. **A single holdout score with no control is unreadable**, and this is
the demonstration.

### Step 2 — normalisation made it WORSE

| representation | dev IC | null bar (p95) | clears? | hit rate |
|---|---|---|---|---|
| `none` (raw levels) | **+0.046** | +0.053 | ❌ p=0.19 | 0.480 |
| `zscore` | +0.038 | **+0.076** | ❌ p=0.38 | 0.500 |
| `window_relative` | +0.000 | +0.059 | ❌ p=0.71 | 0.485 |

⚠️ **Removing the era proxy removed the apparent signal**, which is the diagnosis
confirming itself: the raw-level result was substantially the level acting as a date.
What is left after normalising is nothing.

⚠️ **`zscore` raised its own bar** (null mean +0.029 vs +0.013 for raw). Standardised
windows give the selector *more* room to overfit, so a fair comparison had to re-run
the null per representation — which is exactly why the bar is not a constant.

⚠️ **Adding the holdout also lowered the dev IC** (+0.056 → +0.046): less training
data, and the removed period is the recent one, where the signal had already decayed.

### Step 4 — the market-relative target did not help either

`pool__targets` gained `return_rel_{h}day` = stock return − VNINDEX return over the
same window (`gold_schema.stock_market.hose__vnindex__close_adjust`, **not** the
retired `gold.indices`). Its `sd` is smaller than the absolute target's, which is the
market factor being removed — visible before any model is fitted.

### The whole grid, `d=20, h=5`, holdout 2024-06-01, 20-draw null each

| target / representation | dev IC | null bar | clears? | p | trend | hit |
|---|---|---|---|---|---|---|
| `return_5day` / none | **+0.046** | +0.053 | ❌ | 0.19 | +0.008 | 0.480 |
| `return_5day` / zscore | +0.038 | +0.076 | ❌ | 0.38 | −0.012 | 0.501 |
| `return_5day` / window_relative | +0.000 | +0.059 | ❌ | 0.71 | +0.024 | 0.485 |
| `return_rel_5day` / none | +0.013 | +0.044 | ❌ | 0.29 | −0.016 | 0.495 |
| `return_rel_5day` / zscore | −0.031 | +0.057 | ❌ | 0.90 | +0.013 | 0.484 |

⚠️ **NOTHING CLEARS ITS OWN NULL.** Five configurations, three representations, two
targets — every one inside what shuffled labels produce. Hit rates are 0.48-0.50
throughout.

> The table above is the **20-draw** run. `evaluation_study.ipynb` re-runs the same
> grid at `N_NULL = 10` to keep its execution under ~20 minutes, so its bars differ
> in the third decimal. The verdict is identical in every cell — which is itself the
> point: the result is not close enough to the bar for draw count to matter.

⚠️ **The holdout cannot separate them, and the control proves it.** Shuffled-label
controls scored **−0.189 to +0.169**; the real configurations scored **−0.090 to
+0.071**. The control range strictly CONTAINS the real range.

## 6d. ⚠️ Why more features cannot fix this

`n_eff` is the binding constraint. At `h=5`, VCB's 4,235 sessions carry roughly
**850 independent observations** (`n/h`), the development folds ~126 each, and the
holdout **97**. Separating an IC of 0.05 from zero at conventional confidence needs
on the order of 1,500. **A wider feature pool buys no observations — it only raises
the null**, which `zscore` demonstrated by moving its own bar from +0.053 to +0.076.

The way out is more **cross-section**, not more features, more history or a better
model: N stocks × T days at the same `h` multiplies the independent count by N.
`unified_schema_<ticker>` cannot express that by construction — it is one company —
which is the same conclusion memory `project-cross-sectional-strategy` reached from
the other direction.

## 7. Extending it

* **Another pool** — add it to `POOLS`. `pool__ta` (~900 columns) does not exist as
  an asset yet; `orchestration/CONTEXT.md` §UNIFIED has the status. At that width
  `device="auto"` moves to the GPU on its own.
* **Another ticker** — change `TICKER`. The schema name is a template and the
  reader validates it as an identifier.
* **Another horizon** — add it to `DataPreprocessor.UNIFIED_TARGET_HORIZONS` and
  re-materialise `unified_vcb/pool__targets`. The label definition lives there, in
  one place, on one calendar.
* **Another target** — ⚠️ `selector.py` is **regression-only**. `direction_5day` and
  `probability_gain_5pct_5day` are binary and would be treated as continuous
  labels: the tree rankers would still run and the numbers would still look
  plausible. Classifier variants are the change to make first.

## 8. ⚠️ Before this feeds a sequence model

**Normalisation has to be decided here, not later.** Whatever the model eats —
per-window z-score, divide-by-last-close, differences — the selection must run on that
same representation, or it has selected features for a different problem. Both runs to
date are on **raw levels**, which is exactly why `last` ranks `close_adjust` first: it
is measuring how well a price level identifies the era.

**And stage 2 is not built.** What is here is a screen: a surrogate tree model over
window summaries, which cuts the candidate set cheaply and without needing the LSTM to
exist. The faithful measurement — permute one channel's whole window across samples and
read the drop in the *actual* model's out-of-sample IC — needs that model, and belongs
beside it in `src/model/`.
