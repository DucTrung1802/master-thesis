# Context — `src/feature_selection`

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

> Ranks every feature of a `unified_schema_*` pool against one target, and refuses to
> report the ranking until it has beaten the same pipeline run on shuffled labels.
> Built 2026-08-03 against one ticker; extended 2026-08-04 to the cross-section.
>
> ## ⚠️ THE VERDICT, IN TWO HALVES (2026-08-04)
>
> **Half one — the single-ticker study found nothing, and that stands.** Five
> configurations on VCB (three representations × two targets, `d=20, h=5`, purged CV,
> an uncontaminated holdout and a 20-draw null each), and **every one sits inside what
> the same pipeline scores on shuffled labels**. §6b-6d has the tables.
>
> **Half two — the SAME pipeline on the CROSS-SECTION clears its null by 6 sigma at
> VN100, and by 18 at 301 names.** §7 said the way out was N stocks × T days with a
> per-date target, not a wider feature pool. Built (§9): `unified_schema_all`,
> `d=20, h=5`, `cs_rank_5day`.
>
> | | VCB, time series | VN100, cross-section |
> |---|---|---|
> | observed mean IC | +0.056 | **+0.029** |
> | null mean / p95 bar | +0.017 / +0.056 | **+0.004 / +0.012** |
> | null MAX over 20 draws | **+0.061 — above the observed** | **+0.012 — 2.4× below it** |
> | z vs null | +1.56 | **+6.09** (and **+12.16** vs the `within_date` null) |
> | clears? | ❌ | ✅ |
> | hit rate | 0.477 — **below** a coin | 0.511, and **52.7-68.3 % of DAYS positive** |
>
> ⚠️ **At VN100 the signal did not get bigger — the BAR got smaller.** The observed IC
> actually *fell*, from +0.056 to +0.029. What collapsed is the null: from a mean of
> +0.017 to +0.004. On 4,211 single-ticker samples, picking 12 of 27 channels earns
> +0.017 from noise alone; on 2,860 dates × 100 names it earns +0.004. **That is the
> entire argument of §9b, measured** — the cross-section does not buy independent
> observations, it buys precision, and precision is what shrinks the bar.
>
> ⚠️ **Past ~300 names the observed IC rises too, and §9b does NOT explain that.**
> +0.023 (N=30) → +0.031 (100) → **+0.077 (301)** → +0.109 (780). A falling bar
> cannot raise an observed value. Either less-liquid names are genuinely less
> efficiently priced, or thin trading makes their ranks mechanically predictable —
> §9j screens out the worst of the second and **+0.0768 still clears a bar of
> +0.0245 by 18σ**, which is evidence for the first without being proof.
>
> ⚠️ **THE HOLDOUT DOES NOT CONFIRM ANY OF IT** (§9g): +0.011 against a shuffled
> control of +0.0071, with SE ≈ 0.013. Read §9g before quoting a magnitude.
>
> ## ⚠️ HALF THREE — THE WIDE POOLS, RUN 2026-08-04 ON THE SINGLE TICKER
>
> | pool | ch | observed | p95 bar | null max | z | verdict |
> |---|---|---|---|---|---|---|
> | `pool__basic` §6b | 27 | +0.0559 | +0.0556 | +0.0606 | +1.56 | ❌ |
> | `pool__fa` §11 | 162 | +0.0157 | +0.0740 | +0.0896 | −0.25 | ❌ **below its null's mean** |
> | **`pool__ta` §12** | **918** | **+0.1121** | **+0.0754** | **+0.1189** | **+2.52** | ⚠️ **clears the bar; noise still beat it once in 20** |
>
> ⚠️ **`pool__ta` is the only single-ticker run to clear its p95 bar, and it is still
> not a pass.** One of twenty shuffled-label runs scored +0.1189, above the observed
> +0.1121, so `p` sits at its `1/(n+1)` floor. Its 12 kept channels are eleven
> moving-average crossover/slope channels and one cycle period — **trend-following** —
> but **nine of the twelve are in raw price units** and §6c showed that removing the
> price level removes the apparent signal. §12a is the caveat list; §12d is what
> would settle it.
>
> ⚠️ **The bar rose from 27 to 162 channels and then stopped** (+0.0556 → +0.0740 →
> +0.0754 at 918). §6d's "a wider pool only raises the null" is real but saturates.
>
> ## ⚠️ HALF FOUR — THE BANK SECTOR, 2026-08-05 (§13)
>
> `unified_schema_bank` — 20 GICS `401010` names, the §9c protocol otherwise
> unchanged. **Observed +0.0087 against a null MEAN of +0.0073: `z = +0.11`, and
> 11 of 20 shuffled draws beat the real data.** The holdout is −0.0262 and its
> shuffled control won. **A single-sector cross-section is not resolvable in this
> market** — banks are VN's biggest GICS industry at 20 names, and §9h's threshold
> is ~100.
>
> ⚠️ **It did confirm the mechanism it failed on.** Daily-IC sd **0.244** against
> the **0.251** that `1/√N` predicts from VN30 — §9b now holds across four widths.
> But the observed IC fell while the bar did not move, so §13d offers a second
> reading worth testing: **a sector CO-MOVES, so there is less to rank.**
>
> ## ⚠️ ONE NOTEBOOK IS MEANT TO BE RUN. THE OTHER FOUR ARE WRITE-UPS.
>
> **[RUN__feature_importance_report.ipynb](RUN__feature_importance_report.ipynb)** —
> the `RUN__` prefix is the whole point of the name. Set the parameter cell, Run All,
> get an archived run folder. §10.
>
> **[run.py](run.py) is the same thing as a command** (added 2026-08-10):
>
> ```
> python -m feature_selection.run --pools pool__basic --null-draws 20 \
>                                 --root reports/feature_selection_basic
> ```
>
> It re-implements nothing — same `FeatureSelector`, same
> `evaluation.null_distribution`, same `report.write_report`, and it writes the
> `outstanding.csv` before returning. Two differences from the notebook, both
> deliberate:
>
> 1. ⚠️ **`--null-draws` defaults to 20, where `RUN_NULL` defaults to `False`.** A
>    scripted run has no human at the keyboard to read the "no bar was computed"
>    warning, and `evidence=no_null` then travels into the table `COMMENT`, the dataset
>    `metadata.json` and the run `lineage`. `--null-draws 0` still records the absence
>    as an absence.
> 2. ⚠️ **`--root` exists because `final_features` groups on
>    `(schema, target, setup)`** — a key with no term for *which pools*. A new
>    `pool__basic` run archived into the default root joins the 19 runs behind
>    `return_5day__final__d20_h5` and silently widens that table. See
>    `final_features/CONTEXT.md` §0.
>
> ⚠️ **The wide pools stay manual regardless.** This makes the CHEAP case scriptable —
> 27 channels is minutes. `basic+economy_usa` spends 12,255 s on `permutation` per pass
> at 1,458 channels, so one 20-draw null there is ~68 CPU-hours (issue **EVD-1**), and a
> CLI does not change that arithmetic.
>
> The four `study_*` notebooks are finished experiments kept for the record. They are
> the evidence behind everything above; **re-running one reproduces a result that is
> already written down here**, and none of them writes an archived report.
>
> | study notebook | one sample is | established |
> |---|---|---|
> | [study_1__vcb_unwindowed.ipynb](study_1__vcb_unwindowed.ipynb) | one row → `y_N` | a per-row model. `lookback=1`. §6 |
> | [study_2__vcb_windowed.ipynb](study_2__vcb_windowed.ipynb) | a `(d, n)` window → `y_N` | **a sequence model.** `d=20`, `h ∈ {5, 10}`. §6a |
> | [study_3__null_and_holdout.ipynb](study_3__null_and_holdout.ipynb) | — | **whether either result is real.** It is not: §6b-6d |
> | **[study_4__cross_sectional.ipynb](study_4__cross_sectional.ipynb)** | a `(d, n)` window → **`y` = the stock's RANK on day `N`** | **the one that works.** §9 |
>
> **Read study 4, then study 3.**
>
> ⚠️ **The first two both produce a positive out-of-sample IC. So does the same
> pipeline run on shuffled labels.** The third notebook is what tells them apart, and
> it governs the first two: their rankings and kept sets are internally consistent
> descriptions of noise. The fourth changes the QUESTION — not *will VCB rise* but
> *which of these stocks beats the others* — and is the first thing here to survive
> its own null.
>
> The modules hold nothing notebook-specific, so the same runs script.

## 1. What is here

| file | does |
|---|---|
| [unified_reader.py](unified_reader.py) | connect, introspect, read with the right dtypes, join on `(exchange, ticker, date)` ∩ |
| [windows.py](windows.py) | daily panel → windowed samples; scoring CHANNELS, not columns |
| [selector.py](selector.py) | six rankers → ensemble → correlation prune → purged walk-forward → holdout |
| **[cross_sectional.py](cross_sectional.py)** | **N × T panels** — per-date target, per-date IC, date-grouped CV, panel-aware null |
| [evaluation.py](evaluation.py) | **the BAR** — the shuffled-label null, `n_eff`, and the IC summary that reports trend |
| [gpu.py](gpu.py) | the CUDA paths, the size heuristic, and which steps have no GPU path |
| [plots.py](plots.py) | the figures — one theme, one palette, applied by the job each colour does |
| **[report.py](report.py)** | **one run → one self-describing folder** — CSVs, PNGs and a `metadata.json` that records what may be compared with what (§10) |
| **[outstanding.py](outstanding.py)** | **one run → its final feature list** — kept channels only, ties broken, each mapped back to the pool table it must be read from (§14) |
| **[selection_cut.py](selection_cut.py)** | **how many channels a run supports** — a shuffled-methods null + a per-method knee, replacing `max_features=12` (§14c) |

### Downstream of here

```
THIS  →  final_features  →  train_test_creator  →  model.lstm  →  result_evaluator
```

`python -m pipeline` prints the state of all five and runs the stale ones
(`src/pipeline/CONTEXT.md`). ⚠️ **The runs in this package stay MANUAL** — a selection
is hours of GPU and a judgement about which pools to join, so the pipeline only
refreshes `outstanding.csv` from the runs that already exist.

Three things here are reused verbatim downstream rather than reimplemented:
`evaluation.block_shuffle` and `evaluation.effective_sample` are what
`result_evaluator` builds its null and its `n_eff` from; `plots.py`'s theme is the
only palette in the repo and `result_evaluator/plots.py` imports it; and
`PurgedWalkForward.gap` (`d + h - 1`) is the purge `train_test_creator` applies at
each split boundary, so the CV here and the split there agree about what a leak is.

⚠️ **§14b's "no surviving run clears anything" propagates all the way down.** It is
copied into the table `COMMENT` by `final_features`, into the dataset's
`metadata.json` by `train_test_creator`, and into each run's `lineage` by
`model/lstm/train.py`.
| [test_selection_cut.py](test_selection_cut.py) | **13 tests, no database, ~15 s** — one per way the cut could manufacture a list |
| **[test_cross_sectional.py](test_cross_sectional.py)** | **13 tests, no database, ~2 min** — one per way of faking a cross-sectional result |

⚠️ **`cross_sectional.py` re-implements NO ranker.** `CrossSectionalSelector`
overrides six hooks on `FeatureSelector` — `_design`, `_splits`, `_ic`,
`_effective_n`, `_purge_boundary`, `_on_development` — and inherits the six rankers,
the rank-average ensemble, the correlation prune, the stability pass and the holdout
protocol unchanged. That is deliberate: **the two studies have to be the same
procedure on differently-shaped panels, or §9's numbers cannot be set beside §6's.**
The refactor that extracted those hooks was verified behaviour-preserving — the VCB
run gives bit-identical fold ICs before and after.

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

## 3. The join — now on all three key columns

**Every `unified_schema_<ticker>` table is keyed `(date, exchange, ticker)`** as of
2026-08-04 (`DataPreprocessor.UNIFIED_PRIMARY_KEY`), so `join()` on
`["pool__basic", "pool__targets"]` uses all three.

⚠️ **It did not used to.** `pool__targets` was `(date, return_5day)` — a one-company
label table has nowhere to put a ticker — so the join ran on `date` alone and every
join to it was a special case. `join()` still intersects `KEY_COLS` per pair rather
than assuming all three, requires `date` in the result, and records what it used in
`join_log`: a pool that predates the change joins correctly instead of silently
wrongly.

⚠️ **`date` leads the key**, matching the layer's contract — every access pattern
here is time-ordered, and only a leading `date` lets the PK's index serve a range
scan.

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

> The table above is the **20-draw** run. `study_3__null_and_holdout.ipynb` re-runs the same
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
model. `unified_schema_<ticker>` cannot express that by construction — it is one
company — which is the same conclusion memory `project-cross-sectional-strategy`
reached from the other direction.

> ⚠️ **CORRECTION (2026-08-04), and it matters for how §9 is read.** This section
> originally said "N stocks × T days at the same `h` **multiplies the independent
> count by N**". **That is wrong.** The cross-sectional mean IC is an average over
> `T` daily ICs, consecutive days still overlap by `h−1` label days, and so `n_eff`
> stays `T / h` — VN100 has 2,860 sessions and therefore **572** independent
> observations, not 57,200. Widening the panel adds no dates.
>
> The conclusion survives for a *different* reason, which §9b sets out and §9c
> measures: a day's IC is an average over `N` stocks, so its noise falls like
> `1/√N` — the daily IC's own sd is ~0.13 instead of ~1.0. The cross-section buys
> **precision per observation**, not observations. That is why the null's mean fell
> from +0.017 to +0.004 while the observed IC also fell.

## 7. ✅ THE CROSS-SECTIONAL STEP — DONE 2026-08-04, see §9

This section was the plan. Steps 1-3 are built and run; step 4 is still open.

| | plan | what happened |
|---|---|---|
| 1. multi-ticker panel | VN30 ≈ 25 stocks | ✅ **`unified_schema_all`** — 781 tickers, 2.39 M rows, built by `_ingest_unified_pool_basic("ALL")`. Studies run on VN30 / VN100 / all of it |
| 2. cross-sectional rank target | day `t`'s ranking | ✅ **`cs_rank_{h}day`**, uniform on `[-0.5, +0.5]` per date, built in `read_universe_panel` |
| 3. reuse the package, group the CV by date | | ✅ **six hook overrides**, nothing re-implemented. `PurgedWalkForwardByDate` purges `d+h−1` **sessions** |
| 4. widen to `pool__ta` / `pool__macro` / `pool__calendar` | | ⏳ **run on the SINGLE TICKER only** (2026-08-04): `pool__fa` §11, `pool__ta` §12. Neither has been run against a cross-sectional target, which is what step 4 actually asked for |

⚠️ **The order in the original plan was right and is worth keeping.** Widening the
pool first would have produced "a longer list of nothing, more slowly, and with a
higher bar". Widening it *now*, against a target that has already beaten its null, is
a different proposition — there is something for the extra features to add to.

⚠️ **`unified_schema_<ticker>` still cannot express any of this.** It is one company
by construction and `pool__basic` asserts `COUNT(DISTINCT ticker) = 1`.
`unified_schema_all` is that assertion's sibling, not its replacement — see
`data_preprocessor/CONTEXT.md` §"ticker = ALL" for the sentinel and the three
assertions that had to become series-aware.

### 7a. The other two pools now exist — `pool__ta` and `pool__fa` (2026-08-04)

`unified_schema_vcb` holds four tables, all 4,235 rows on one calendar, all keyed
`(date, exchange, ticker)`:

| pool | columns | is |
|---|---|---|
| `pool__basic` | 38 | the price/flow panel |
| `pool__targets` | 7 | the labels |
| **`pool__ta`** | **924** | the technical block — ~226 moving-average columns, 121 oscillator, 120 price-transform, 90 Hilbert, 58 volatility, 50 volume, 45 RSI, 28 SAR, 26 stochastic, 23 Bollinger, 20 MACD |
| **`pool__fa`** | **207** | the fundamental block — 93 balance-sheet, 50 cash-flow, 29 income-statement line items, 17 ratios (`roe`, `roa`, `nim`, `pe_ttm`, `pb`, `ldr`, …), 4 YoY growth, 4 per-share/TTM, 4 share counts |

Full listing: [unified_schema_vcb__pool_columns.md](../../reports/feature_selection/unified_schema_vcb__pool_columns.md).

⚠️ **BOTH HAVE NOW BEEN RUN ON THE SINGLE TICKER, AND §7 WAS RIGHT ABOUT THE BAR.**
The prediction was that a wider pool buys no independent observations and **raises
its own null** — `zscore` moved its bar 43 % on 27 channels. Measured, same ticker,
same target, same folds, same device:

| pool | channels | observed IC | **null p95 BAR** | null max | z | § |
|---|---|---|---|---|---|---|
| `pool__basic` | 27 | +0.0559 | +0.0556 | +0.0606 | +1.56 | §6b |
| `pool__fa` | 162 | +0.0157 | +0.0740 | +0.0896 | −0.25 | §11 |
| **`pool__ta`** | **918** | **+0.1121** | **+0.0754** | **+0.1189** | **+2.52** | **§12** |

⚠️ **The bar rose 27 → 162 channels and then STOPPED (+0.0556 → +0.0740 → +0.0754).**
§6d's mechanism is real but it is not unbounded — what a selector can earn from
shuffled labels saturates once the pool is wide enough that the top of it is already
noise. The observed value is what separates the three, not the bar.

These tables were built for step 4, against a **cross-sectional** target. Both
single-ticker runs are the control for that, not the destination.

⚠️ **`pool__fa` exists for VCB and ACB only** — it is built from the CafeF *bank*
chart of accounts. A cross-sectional FA study is not possible from this table.

⚠️ **`pool__fa` is safe only because of `publish_date`**, which the ingest asserts
(0 rows published after their own date). The lag reaches **0 days**, so shift it
forward one session before trusting any FA-driven result — see
`data_preprocessor/CONTEXT.md`.

⚠️ **207 of `pool__ta`'s columns are BOOLEAN** and `_prepare` drops bool dtypes, so
a naive run scores **717 of the 921**, not all of them. §12 casts them to 0/1 in the
panel before the selector sees it — the flags (`rsi_14_gt_70`,
`macd_12_26_9_cross_above`, `close_bb_20_above_upper`, …) are real signals, and a
0/1 column is a perfectly good feature for all six rankers. **Cast them, or say in
the report that 204 indicators were never scored.** One more column,
`ht_dcphase_quadrant`, is VARCHAR and is excluded by name rather than by accident.

⚠️ **BOTH HAVE NOW BEEN RUN — `pool__fa` is §11, `pool__ta` is §12.** They came
back at opposite ends: FA scored *below* its own null's mean (z = −0.25), TA cleared
its p95 bar (z = +2.52) and is the only single-ticker configuration in this package
to do so. Neither is a pass; §12's null max still exceeds its observed. Read both
before pointing the selector at a wide pool again.

### Smaller extensions, if the study continues as-is

* **Another ticker** — change `TICKER`; the schema name is a template and the reader
  validates it as an identifier. ⚠️ Each ticker is a *separate* single-stock study
  with the same `n_eff` problem, not a bigger one.
* **Another horizon** — add it to `DataPreprocessor.UNIFIED_TARGET_HORIZONS` and
  re-materialise `unified_vcb/pool__targets`. The label definition lives there, in one
  place, on one calendar. ⚠️ A longer `h` *lowers* `n_eff`.
* **Another target** — ⚠️ `selector.py` is **regression-only**. `direction_5day` and
  `probability_gain_5pct_5day` are binary and would be treated as continuous labels:
  the tree rankers would still run and the numbers would still look plausible.
  Classifier variants are the change to make first.

## 8. ⚠️ Standing rules, learned the hard way here

**Every selection needs its own null, re-run whenever the pool or the representation
changes.** `zscore` moved its own bar from +0.053 to +0.076 without any change to the
data. A bar computed for one configuration says nothing about another.

**Every single-score holdout needs a shuffled-label control.** With one score there is
no fold spread to read, so the control IS the error bar — and here it reached +0.169
against real results of at most +0.071.

**Report the IC trend beside the mean.** A mean built from folds decaying to negative
is not a signal that averages that mean.

**Normalisation is chosen before selection, not after.** The selection must run on the
representation the model will eat, or it has selected features for a different
problem — and if the answer changes when you normalise, the original answer was the
era proxy.

**Stage 2 is still not built.** What is here is a screen: a surrogate tree over window
summaries, cheap and not needing the LSTM to exist. The faithful measurement — permute
one channel's whole window and read the drop in the *actual* model's out-of-sample
IC — needs that model and belongs beside it in `src/model/`. ⚠️ On the current
evidence there is nothing for it to measure; build it after §7 step 1.


## 9. THE CROSS-SECTIONAL STUDY (2026-08-04) — the first result that survives

Read [cross_sectional.py](cross_sectional.py)'s module docstring before changing
anything here; it names the three specific mistakes that manufacture a
cross-sectional result, and all three produce numbers that look *better* than these.

### 9a. The panel

`unified_schema_all` — `_ingest_unified_pool_basic(DataPreprocessor.UNIFIED_UNIVERSE)`
— is `unified_schema_vcb`'s shape with every ticker in it: **2,388,368 rows × 38
columns, 781 tickers, 4,366 sessions, 2009-01-02 → 2026-07-08**, same
`(date, exchange, ticker)` key, built in 57 s. Verified against the single-ticker
schema: 4,235 VCB rows compared, **0 disagreeing `return_5day` values**.

| universe | tickers | rows | sessions | median width | label density |
|---|---|---|---|---|---|
| VN30 | 30 | 65,737 | 2,283 | 30 | 0.958 |
| **VN100** (the headline) | 100 | 251,110 | 2,860 | 92 | 0.876 |
| ALL | 780 | 1,853,043 | 2,862 | 675 | 0.828 |

All three are `2015-01-01 → 2026-06-26`, `min_width=20`.

⚠️ **Two data facts that the universe build exposed, both of which change what may
be claimed:**

1. **`close_adjust` is NEGATIVE for 968 sessions on `VNX`** (−10.0, then 7,800),
   which makes `unified_schema_all.return_5day` reach **−781.0** against a VN30 range
   of −0.33 … +0.55. One ticker, no other column affected. `read_universe_panel`
   excludes it by default; **the fix belongs in silver.**
2. **⚠️ THE UNIVERSE IS 100 % SURVIVORS.** Every one of the 781 tickers has data
   through 2026 — `silver.stocks_basic` holds no delisted name. See §9e for which
   part of the result this threatens and which part it does not.

### 9b. The arithmetic — what the cross-section does and does not buy

**`n_eff` is `n_dates / h`, NOT `n_rows / h`.** A hundred stocks on one Tuesday are
one observation of the market, not a hundred. VN100 is 2,860 sessions, so **572**
independent observations at `h=5` — against VCB's 847. *Fewer.* The panel is 59×
wider and has **no more independent observations at all.**

What it buys is precision *within* an observation:

| | VCB, time series | VN100, cross-section |
|---|---|---|
| one observation is | one day's ±1 outcome | one day's IC over ~92 stocks |
| sd of that observation | ~1.0 | **~0.13** (measured, §9c) |
| observations | 847 | 572 |
| SE of the fold mean | ~0.083 | **~0.006** |

`selector._effective_n` is the hook that says so, `_validate` carries `n_eff_test`
per fold, and `ic_summary` prefers that column over its own `n_rows / h` — because
that function cannot tell the two panel shapes apart from a row count.

⚠️ **`ic_summary`'s `se_ic_per_fold` is the WRONG error bar for a cross-sectional
run** and is kept only for comparability with §6's tables. It is the standard error
of *one* rank correlation (`1/√(n_eff−1)` = 0.107); the fold mean is an average of
~440 of them. Read `CrossSectionalSelector.daily_ic_by_fold`'s `t_stat` instead.

### 9c. The headline run — VN100, `d=20`, `h=5`, `cs_rank_5day`, `cs_rank` features

**Every fold positive, no decay, and a hit rate above a coin for the first time.**

| fold | IC | daily-IC sd | days | t | days positive |
|---|---|---|---|---|---|
| 1 | +0.0158 | 0.129 | 442 | 1.15 | 55.9 % |
| 2 | **+0.0558** | 0.125 | 442 | **4.21** | **68.3 %** |
| 3 | +0.0344 | 0.135 | 442 | 2.40 | 59.3 % |
| 4 | +0.0267 | 0.121 | 442 | 2.07 | 57.5 % |
| 5 | +0.0238 | 0.135 | 444 | 1.66 | 52.7 % |
| **mean** | **+0.0313** | | 2,212 | | **hit rate 0.511** |

`ic_trend_per_fold` **−0.0013** — flat, against −0.034 and −0.049 for the two
single-ticker horizons. **R² is −0.01**, against −1.19: the rank target is a
quantity a regressor can actually fit, which the raw forward return was not.

### 9d. ⚠️ THE NULL — and this one clears it

Two nulls, 20 draws each, the **selection re-run inside every draw**, and the
observed run at the same `permutation_repeats=3` as its own null (a bar computed
from a cheaper pipeline than the number it judges is a different procedure).

| | observed | null mean | null sd | **p95 BAR** | null MAX | z | clears |
|---|---|---|---|---|---|---|---|
| `date_block` | **+0.0289** | +0.0044 | 0.0040 | +0.0117 | +0.0119 | **+6.09** | ✅ |
| `within_date` | **+0.0289** | +0.0008 | 0.0023 | +0.0042 | +0.0063 | **+12.16** | ✅ |
| *(VCB, §6b, for contrast)* | +0.0559 | +0.0167 | 0.0252 | +0.0556 | **+0.0606** | +1.56 | ❌ |

⚠️ **Not one of the 40 null draws reached even half the observed value.** The
maximum over both nulls is +0.0119, and the observed is **2.4× that**. `p = 0.048`
is the floor `1/(n+1)`, not a measurement — 20 draws cannot distinguish p = 0.05
from p = 0.001, which is why the z is the number to quote.

⚠️ **The two nulls differ, and the weaker-looking one is the honest one to quote.**
`date_block` pivots the label to `date × ticker` and permutes blocks of rows, so each
stock keeps its **own** labels and the label's autocorrelation survives — but on a
ragged panel a donor date has no label for a name that had not listed, and a draw
keeps **230,685 of 250,610 labelled rows (92.0 %)**. Less training data means a
weaker null model, a lower bar, and a bias *toward* a false positive. `within_date`
is exactly lossless (250,610 of 250,610) and gives a far lower bar anyway. **Quote
`date_block`'s +6.09.**

⚠️ **The kept set barely moves with `permutation_repeats`.** At 10 repeats and at 3,
**11 of 12 channels agree and the top five agree in order**; the twelfth is
`value_negotiated` against `volume_negotiated`, which are each other's correlated
twin. The wall-clock knob is not deciding the answer.

### 9e. ⚠️ What the ranking says — and which parts of it to distrust

Ensemble over six rankers, aggregated to channels by MAX, pruned at |ρ| ≥ 0.9.
**No dead methods**: LASSO separated features here, where on VCB's `pool__basic` it
zeroed every coefficient (§4).

| rank | channel | ensemble | signed ρ vs target | carried by | reads as |
|---|---|---|---|---|---|
| **1** | **`avg_vol_per_buy_order`** | **3.83** | **+0.0373** | `last` | mean buy-order SIZE — big tickets relative to today's cross-section |
| 2 | `close_adjust` | 5.33 | **−0.0286** | `min` | price LEVEL relative to the cross-section — ⚠️ see below |
| 3 | `n_sell_orders` | 7.67 | −0.0121 | `last` | sell-side order COUNT |
| 4 | `avg_vol_per_sell_order` | 11.50 | +0.0151 | `mean` | the sell-side twin of #1 |
| 5 | `foreign_net_value` | 11.83 | −0.0121 | `last` | foreign net buying |
| … | | | | | |
| **27 (last)** | `foreign_own` | 23.33 | −0.0007 | `mean` | ⚠️ **top-3 in the single-ticker study** |

⚠️ **`avg_vol_per_buy_order` tops four of the six rankers outright** — `spearman`,
`xgb_gain`, `xgb_shap` and `permutation` all score it 1.000, including the only
out-of-sample one. Nothing in §6 was ever agreed on by four methods. It is also the
largest |ρ| in the table by 30 %.

⚠️ **`close_adjust` at #2 is the one to distrust, and §9a.2 is why.** Its sign says
*cheap stocks outperform*, and a universe containing no delisted names is exactly the
sample that manufactures that: the stocks that were cheap and then went to zero are
missing, so only the ones that recovered are in the panel. **Survivorship bias hits
this channel directly.** It does not obviously hit #1 — an order-size effect has no
comparable path from "the failures are absent" to "big buy orders precede
outperformance" — but the honest statement is that **the study cannot separate them
until a delisted-inclusive universe exists.**

⚠️ **Survivorship biases the OVERALL result toward zero, not away from it.** The
target is a rank *within* the surviving names, so a uniform survivor premium cancels;
what is missing is the left tail, and features predicting disaster — usually the
strongest ones — have nothing left to predict. The `+0.029` is therefore a *lower*
bound on what a complete universe would show, **except** through channels like
`close_adjust` where the truncation itself creates the pattern.

⚠️ **`last` carries the top channel**, which means the 20-day window bought nothing
for it — `avg_vol_per_buy_order` on day `N` alone would do. §1a's rule applies: a
channel that only ever wins on `last` never needed a window.

### 9f. ⚠️ What moves the answer — and it is the TARGET, not the features

Four one-line changes off the §9c baseline, same panel, same CV, same rankers.

| configuration | IC | trend | hit | daily-IC sd | fold t-stats |
|---|---|---|---|---|---|
| **VN100 / cs_rank feats / `cs_rank_5day`** (baseline) | **+0.0313** | −0.0013 | 0.511 | 0.130 | 1.15 4.21 2.40 2.07 1.66 |
| VN100 / **RAW** feats / `cs_rank_5day` | **+0.0326** | −0.0016 | 0.509 | 0.132 | 2.58 2.88 1.58 3.24 1.47 |
| VN100 / cs_rank feats / **`return_5day`** | **+0.0074** | −0.0023 | **0.489** | 0.129 | 0.28 1.39 1.03 0.11 **−0.08** |
| VN100 / cs_rank feats / `cs_rank_10day`, `h=10` | +0.0236 | −0.0015 | 0.505 | 0.135 | 0.38 2.27 1.00 2.29 **−0.13** |
| **VN30** / cs_rank feats / `cs_rank_5day` | +0.0218 | −0.0046 | 0.507 | **0.205** | 1.07 1.06 0.99 1.06 0.11 |

⚠️ **THE RANK TARGET IS THE WHOLE RESULT.** Swapping `cs_rank_5day` for the raw
`return_5day` — same features, same panel, same folds — drops the IC **4×, from
+0.031 to +0.0074**, and pushes the hit rate to **0.489**: back below a coin, the
exact §6b signature of a model reading MAGNITUDE rather than direction. Everything
§9d claims rests on asking *which stock beats the others today*, not *what will this
stock return*.

⚠️ **THE CROSS-SECTIONAL FEATURE NORMALISATION IS NOT what makes this work.** RAW
features score `+0.0326` against cs-ranked features' `+0.0313`, with *steadier* fold
t-stats. That directly contradicts §6c, where normalising **destroyed** the
single-ticker result — and the reason is worth keeping: there, the raw level was
acting as a date proxy and the "signal" *was* the era. **A per-date target
immunises against that automatically** — a feature that identifies the year cannot
help rank stocks *within* a day — so the representation stops mattering once the
target is cross-sectional.

⚠️ **RAW got its OWN null, and it clears — but the point is that its BAR did not
move.** Per §8 a bar computed for one representation says nothing about another, so
this was run rather than assumed:

| representation | observed | null mean | **null p95 bar** | null max | z | clears |
|---|---|---|---|---|---|---|
| cs_rank features (20 draws) | +0.0289 | +0.0044 | **+0.0117** | +0.0119 | +6.09 | ✅ |
| RAW features (10 draws) | +0.0326 | +0.0034 | **+0.0115** | +0.0153 | **+5.37** | ✅ |

**The two bars agree to the third decimal (+0.0117 vs +0.0115).** That is the direct
contrast with §6c, where `zscore` moved its own bar from +0.053 to +0.076 — a 43 %
swing — purely by giving the selector more to overfit. Here the representation
changes nothing about how much the pipeline can earn from noise, because **the
per-date target caps that regardless of what the features look like.** §8's rule
still stands (re-run the null when the representation changes); this is a case where
obeying it produced a null result, which is the useful kind.

⚠️ **VN30 vs VN100 IS §9b's precision argument, measured.** The two ICs are similar
(+0.022 vs +0.031) but VN30's **daily-IC sd is 0.205 against VN100's 0.130** — a
ratio of 1.58 against the `1/√N` prediction of `√(92/30) = 1.75`. So VN30's fold
t-stats are all ≈1.0 while VN100's reach 2-4 **on a comparable IC**. Widening the
cross-section did not raise the signal; it lowered the noise on each day's estimate,
which is the entire mechanism.

⚠️ **`h=10` is worse, and for the documented reason**: `n_eff` halves to 44 days per
fold, and the last fold goes negative.

### 9g. ⚠️ THE HOLDOUT DOES NOT CONFIRM IT — read this before quoting §9d

Scored **once**, `2024-06-01` onward: 50,662 holdout rows, 48,762 scored, **24
sessions (2,352 rows) purged** from the development side of the boundary.

| configuration | dev IC | **holdout IC** | **shuffled control** | margin | hit rate |
|---|---|---|---|---|---|
| VN100 / cs_rank, selected | +0.0273 | **+0.0110** | **+0.0071** | **+0.0039** | 0.504 |
| VN100 / cs_rank, all channels | — | +0.0083 | −0.0027 | +0.0110 | 0.503 |
| VN100 / RAW, selected | +0.0341 | **+0.0124** | **+0.0033** | +0.0091 | 0.508 |
| VN100 / RAW, all channels | — | +0.0119 | +0.0049 | +0.0070 | 0.507 |

⚠️ **The holdout IC is a THIRD of the development IC, and its margin over the
shuffled control is inside the error bar.** The holdout spans 489 sessions, so
`n_eff` is **97.8** and `SE ≈ 0.130/√97.8 ≈ 0.013`. A margin of +0.0039 against
that is nothing. **The holdout neither confirms nor refutes §9d; it cannot resolve
an effect this small at this length.**

⚠️ **The gap between +0.027 and +0.011 is not mysterious — it is the selection.**
The walk-forward ranks features on the whole development sample, including the
periods it then tests on; the holdout's selection never saw the holdout. **The
uncontaminated estimate of what this earns out of sample is +0.011, not +0.029.**

⚠️ **That does NOT invalidate §9d**, and the distinction matters. The null re-runs
the *whole* pipeline, selection included, on shuffled labels — so the observed
+0.0289 and the null's +0.0044 carry the *same* contamination, and the 6σ gap
between them is still a statement about signal rather than about selection. What
the holdout revises is the **magnitude to expect**, not the existence.

**The honest summary of §9 in one line: the effect is real (6σ past a
selection-inclusive null), small (+0.011 uncontaminated), and not yet confirmed on
data the study never touched.**

### 9h. ⚠️ THE WIDTH LADDER — the single most useful table here

Same pipeline, same target, same `d=20, h=5`, same 20-draw `date_block` null.
**Only the number of names changes.**

| universe | N | daily-IC **sd** | observed IC | null mean | **null p95 BAR** | z | clears |
|---|---|---|---|---|---|---|---|
| VCB (§6b) | **1** | ~1.0 | +0.0559 | +0.0167 | **+0.0556** | +1.56 | ❌ |
| **BANK, GICS (§13)** | **20** | **0.244** | **+0.0087** | +0.0073 | **+0.0249** | **+0.11** | ❌ **on the null's mean** |
| VN30 | **30** | 0.205 | +0.0233 | +0.0110 | **+0.0248** | **+1.42** | ❌ |
| **VN100** | **100** | 0.130 | +0.0289 | +0.0044 | **+0.0117** | **+6.09** | ✅ |
| **LIQUID301** (§9j, `d=1`) | **301** | — | **+0.0768** | +0.0216 | **+0.0245** | **+18.45** | ✅ |
| ALL (§9i) | **780** | — | **+0.109** | — | *(never ran, §9k)* | — | ⚠️ |

⚠️ **THE BAR FALLS WITH `1/√N` AND THAT IS THE WHOLE STORY.** +0.0556 → +0.0248 →
+0.0117. From VN30 to VN100 the predicted factor is `√(100/30) = 1.83` and the
measured one is `0.0248 / 0.0117 = 2.12`. The observed IC barely moves (+0.023 →
+0.029). **Nothing is getting more predictable; the noise floor is dropping.**

⚠️ **VN30 FAILS, AND THAT IS THE RESULT THAT MAKES VN100 BELIEVABLE.** A 30-name
cross-section scores `z = +1.42` — statistically indistinguishable from VCB's
`+1.56`, i.e. **a 30-stock panel is no better than one stock at resolving this.**
The pipeline does not hand out a pass to any panel it is shown; §7's original plan
proposed VN30 ≈ 25 stocks, and **that would have failed.**

⚠️ **So the practical threshold is ~100 names, not ~30.** Anyone repeating this on a
narrower index should expect a null they cannot clear, and should not read that as
absence of signal — VN30's observed IC (+0.023) is 80 % of VN100's, it just cannot
be resolved against a bar of +0.025.

⚠️ **BANK (N=20) IS THE PREDICTION CONFIRMED, AND IT GOES FURTHER THAN VN30 DID.**
Its **daily-IC sd is 0.244 against the 0.251 that `1/√N` predicts from VN30's
0.205** — a 3 % error, the cleanest confirmation of §9b's mechanism in the package.
But its *observed* IC also collapsed, +0.0233 → +0.0087, which the precision
argument does **not** predict, and its bar did **not** rise (+0.0248 → +0.0249). So
at 20 names the failure is no longer "a real effect the panel cannot resolve" —
`z = +0.11` means there is nothing there to resolve. §13 has the reason this may be
about SECTORS rather than width.

### 9i. ⚠️ ALL 780 NAMES — the biggest number here, and the least trustworthy

1,853,043 rows, 780 tickers, `d=20`, `h=5`, 61 min.

| fold | IC selected (12 ch) | IC **all 27 ch** | R² | hit rate |
|---|---|---|---|---|
| 1 | +0.1166 | **+0.1408** | +0.014 | 0.537 |
| 2 | +0.1115 | +0.1377 | +0.014 | 0.531 |
| 3 | +0.1139 | +0.1339 | +0.011 | 0.537 |
| 4 | +0.1113 | +0.1382 | +0.014 | 0.531 |
| 5 | +0.0930 | +0.0948 | +0.010 | 0.524 |
| **mean** | **+0.109** | **+0.129** | | **0.532** |

**3.5× VN100's IC, flat across folds, and the only POSITIVE R² anywhere in this
package** (+0.010 … +0.014, against −1.19 for windowed VCB). Three warnings, in
order of how much they should change your reading:

⚠️ **1. IT HAS NO `d=20` NULL, AND IT IS THE CONFIGURATION THAT MOST NEEDS ONE.**
One run is 61 min, so a 10-draw null is ~10 h. §9j runs the same universe at
`lookback=1` — 27 design columns instead of 162, observed *and* null both at `d=1`
so the pair is internally consistent — to answer the only question that matters
here: does a 780-name cross-section clear its own bar at all? **Until §9j, treat
+0.109 as unverified.** §8's standing rule is not suspended because a run is
expensive.

⚠️ **2. THE SELECTION NOW HURTS.** All 27 channels beat the pruned 12 in **every
fold** (+0.129 vs +0.109). By §6b's own rule — *a selection that helps at one
setting and hurts at another is not selecting* — pruning to 12 channels is
discarding information at this width. `max_features=12` was chosen for a 27-channel
single-ticker pool and should not be carried here unexamined.

⚠️ **3. THE RANKING REORDERS TOWARD LIQUIDITY, WHICH IS ALSO WHAT AN ARTEFACT WOULD
DO.** `n_sell_orders` takes #1 (1.000 on both `xgb_shap` and `permutation`) with
signed ρ **−0.0280**: *fewer sell orders → higher forward rank*. VN100's leader
`avg_vol_per_buy_order` falls to #4 but still scores 1.000 on `spearman` (ρ
**+0.0329**). The 780-name universe is mostly UPCOM microcaps, where thin trading
and stale prices generate exactly this shape — a stock that barely trades has an
unchanged `close_adjust`, hence a mid-rank return, predictably. **An order-count
effect and a staleness artefact are not distinguishable from this table.**

⚠️ **`last` carries almost every top channel here** (`n_sell_orders`,
`avg_vol_per_buy_order`, `close_adjust`, `buy_order_vol`, `sell_order_vol` all win
on `last`), so the 20-day window is contributing very little at this width — which
is also why §9j's `lookback=1` null is a fair proxy and not merely a cheap one.

### 9j. LIQUID301 — removing the microcap tail, which answers warning 3

The 780-name run's null was attempted twice at full width and never finished (§9k).
This is the same question asked of a **liquidity-screened** universe instead:
**301 tickers whose MEDIAN daily matched value since 2018 is ≥ 0.5 bn VND** —
711,275 rows, 2,857 sessions, median cross-section width **260**. Half the memory,
and it is *also* the direct test: if +0.109 were the illiquid tail being stale
rather than an effect, screening the tail out should collapse it.

⚠️ **The screen is applied over the WHOLE sample**, so a name's 2015 rows are kept
because of liquidity it had in 2023. That is look-ahead in the strict sense and is
standard for defining an investable universe; it is also the right instrument here,
because identifying "the microcap tail" requires knowing which names are the tail.

| | `d=20` | `d=1` |
|---|---|---|
| mean IC (selected) | **+0.0715** | **+0.0768** |
| mean IC (all 27) | +0.0684 | +0.0782 |
| fold ICs | .086 .081 .081 .047 .063 | .087 .084 .087 .052 .075 |
| trend / hit rate | — / 0.522 | −0.0057 / **0.526** |
| R² | +0.0003 … +0.0064 | −0.0014 … +0.0064 |

⚠️ **IT DOES NOT COLLAPSE.** +0.109 → +0.0715 is a 35 % fall, not a disappearance,
and it stays **2.3× the VN100 result**. So the wide-universe effect is *not* purely
a microcap artefact — though part of it plainly is, and the honest reading is that
roughly a third of the 780-name number came from the tail.

⚠️ **`avg_vol_per_buy_order` RETURNS TO #1** (1.000 on `spearman` *and*
`permutation`), displacing the `n_sell_orders` that led at 780 names. **The VN100
leader survives the widening; the ALL-universe reordering toward order-counts was
substantially the tail.** That is warning 3 of §9i, answered.

⚠️ **The selection stops hurting.** At 780 names all 27 channels beat the pruned 12
in every fold; here the pruned set wins at `d=20` (+0.0715 vs +0.0684). So §9i's
warning 2 localises to the illiquid tail rather than to width as such.

⚠️ **`d=1` ≈ `d=20` (+0.0768 vs +0.0715), which is why the null below runs at
`d=1`.** The 20-day window is worth nothing at this width — consistent with `last`
carrying the top channels — so the cheap configuration is also the faithful one.
Observed and null both run at `d=1`, so the pair is internally consistent.

**The IC ladder, now four points wide:**

| universe | N | mean IC |
|---|---|---|
| VN30 | 30 | +0.0233 |
| VN100 | 100 | +0.0313 |
| **LIQUID301** | 301 | **+0.0715** |
| ALL | 780 | +0.109 |

⚠️ **The IC itself rises with N, which the §9b precision argument does NOT
explain.** Precision predicts a falling *bar*, not a rising *observed* value. Two
readings are consistent with this and the data here cannot separate them: smaller
and less-liquid names are less efficiently priced (a real and well-documented
effect), and thinner trading generates stale prices whose ranks are mechanically
predictable. The liquidity screen removes the worst of the second and leaves
+0.0715 standing, which is evidence for the first — not proof of it.

#### ⚠️ AND IT CLEARS ITS NULL BY THE WIDEST MARGIN IN THE STUDY

10 draws, `date_block`, selection re-run inside every draw, observed and null both
at `d=1, permutation_repeats=3`.

| | observed | null mean | null sd | **p95 BAR** | null max | **z** | clears |
|---|---|---|---|---|---|---|---|
| LIQUID301 / `d=1` | **+0.0768** | +0.0216 | 0.0030 | **+0.0245** | +0.0249 | **+18.45** | ✅ |

⚠️ **The observed is 3.1× the highest of ten null draws**, and the null is
extraordinarily tight — sd **0.0030**, every draw inside +0.016 … +0.025. `p = 0.091`
is the floor `1/(n+1)` at 10 draws, not a measurement; the z is the number.

⚠️ **The null's MEAN is +0.0216 here against VN100's +0.0044** — five times higher.
A wider panel gives the selector more to earn from noise, exactly as §8's rule
anticipates, which is why this configuration needed its own null rather than
VN100's. It got one, and the observed still clears by 18σ.

**So §9i's warning 1 is discharged for the liquid universe and NOT for all 780.**
A 301-name investable cross-section carries a real, large, null-clearing effect.
Whether the extra +0.038 that the 780-name run adds on top is also real remains
**unverified** — see §9k.

### 9k. ⚠️ The 780-name null was attempted twice and never produced a number

| attempt | configuration | outcome |
|---|---|---|
| 1 | ALL 780, `d=20`, 10 draws | abandoned — 61 min/run ⇒ ~10 h |
| 2 | ALL 780, `d=1`, 2 modes × 10 draws | **died at ~4 h with a 0-byte output file** |
| 3 | LIQUID301, `d=1`, 10 draws | ✅ §9j — 49 min, z = +18.45 |

⚠️ **Attempt 2 lost everything, and the cause was a logging bug, not the compute.**
The driver wrapped `sys.stdout` in a `TextIOWrapper`, which **re-buffers on top of
`python -u`**; nothing reached disk, so when the process died (OOM or teardown — it
is not recoverable which) four hours of draws vanished. Attempt 3 used
`sys.stdout.reconfigure(line_buffering=True)` and printed each draw as it completed.

**If you run anything long here, line-buffer it and write each draw to disk as it
finishes.** A null is `n` independent runs; there is no reason for draw 9 to be lost
because draw 10 crashed.

## 10. THE REPORT PIPELINE — one run in, one self-describing folder out

[report.py](report.py) + [RUN__feature_importance_report.ipynb](RUN__feature_importance_report.ipynb).
Set the parameters, Run All, get an archived run. This is the operational half of
the package; §6 and §9 are the studies.

```
reports/feature_selection/<date>_<HHMMSS>__<ticker>__<pools>__<target>/
  metadata.json            what was run, on what, with which knobs, what came out
  README.md                the same, for a human, in ~40 lines
  feature_importance.csv   ⭐ the deliverable — 18 columns, see below
  design_scores.csv        per (channel, stat), the detail the MAX aggregates
  validation.csv           per fold x feature set: IC, R², hit rate, n_eff
  target_correlation.csv   signed Spearman per channel
  channel_correlation.csv  the matrix the redundancy prune used
  stability.csv            per-fold SHAP rank (when stability=True)
  coverage.csv             non-null share per channel
  figures/01..10 *.png     ranking, method heatmap, correlations, stat profile,
                           validation, stability, coverage, target dist, null
```

### 10a. ⚠️ The folder name carries the INPUT and the TARGET — renamed 2026-08-09

`report.default_run_id` builds `<date>_<HHMMSS>__<ticker>__<pools>__<target>`:

```
2026-08-08_103654__vcb__basic+economy_united_kingdom__return_5day
                    ─┬─  ────────────┬────────────────  ─────┬─────
                  schema        the INPUT pools        the TARGET
```

`unified_schema_` and `pool__` are stripped — every folder here would carry them —
and **`pool__targets` is never named**, because it is where the label comes from and
the label is already the last segment.

⚠️ **The old scheme was `<date>__<schema>__<target>__<HHMMSS>` and it hid the input.**
Eighteen of the twenty-two archived runs are VCB / `return_5day` differing only in
which `pool__economy_<country>` was joined — under the old names all eighteen read
`2026-08-0X__unified_schema_vcb__return_5day__<HHMMSS>` and were separable only by
opening `metadata.json`. The whole argument of §8 is that two runs can look
comparable and not be; a folder name that omits the input is that failure mode
built into the filesystem.

⚠️ **The name is a LABEL, not the record.** The pool segment is capped at 60
characters (`MAX_POOL_SEGMENT`, then `_etc`) because Windows still enforces a
260-character path and `figures/09_target_distribution.png` lives underneath.
`metadata.json` holds the untruncated table list, the full schema and the 27 knobs
a folder name cannot fit.

`feature_importance.csv` is one row per CHANNEL, ensemble-sorted, carrying `rank`,
`ensemble`, `kept`, `dropped_for`, **all six method scores**, `spearman_vs_target`
(the SIGN — a ranking without it cannot be read as a strategy) and
`best_stat__<method>` (which window statistic carried the channel).

### ⚠️ Why `metadata.json` is long, and why that is the point

**A ranking is meaningless without the setup beside it.** §8 is a list of ways two
runs look comparable and are not: `zscore` moved its own bar 43 % without touching
the data, `device` changes the kept set outright, and a bar computed for one target
says nothing about another. A bare CSV of feature names is precisely the artefact
that gets quoted a month later against a different configuration. So the file
records the input (schema, tables, **`join_log` — which keys each merge actually
used**, row counts, date range), the target (definition, labelled/unlabelled split,
moments), all 27 setup knobs, the results, the environment (library versions) and
the **git commit**, because a ranking from before the cross-sectional hooks landed
is not comparable with one from after.

⚠️ **`"null": null` MEANS NO BAR WAS COMPUTED, and the README says so in bold.**
An absent null is recorded as absent, never omitted and never implied to be a pass.
`RUN_NULL` defaults to **False** so the notebook finishes in a minute; turn it on
before any number leaves the machine. Same for `RUN_HOLDOUT`.

⚠️ **`compare_reports([...])` exists to make INCOMPARABILITY visible.** It puts
`target`, `normalize`, `feature_normalize`, `lookback_d` and `device` next to
`ic_mean`, so the difference between two runs is seen *before* their ICs are.

⚠️ **Nothing here writes to the database.** §1's rule is unchanged — this writes to
a filesystem path the caller picks. And nothing is re-run: every artefact is read
off an existing `SelectionResult`, so a report costs ~2 s.

### 10b. The VCB prototype (2026-08-04) — checked in as the reference

`unified_schema_vcb.pool__basic ⋈ pool__targets`, `return_5day`, `d=20, h=5`,
executed end-to-end via `nbconvert`: **17 code cells, 0 errors, 18 artefacts,
1.2 MB, report written in 2.2 s.**

| | |
|---|---|
| panel | 4,235 × 42, 2009-06-30 → 2026-06-25 |
| target | 4,230 labelled + a 5-row tail; mean +0.0032, sd 0.0430 |
| kept | 12 of 27 channels, 161 design columns |
| top 5 | `volume_negotiated`, `foreign_own`, `close_adjust`, `open`, `buy_order_vol` |
| IC — selected / all | **+0.0636** / +0.0427, trend −0.0070 / −0.0370 |
| hit rate | **0.492** |
| null | ✅ **computed 2026-08-09 — and it FAILS** (below) |

### ⚠️ Its null, added 2026-08-09 (issue **EVD-1**) — `z = +1.46`, ❌

| | observed | null mean | null sd | **p95 BAR** | null MAX | **z** | p | clears |
|---|---|---|---|---|---|---|---|---|
| **prototype, 27 ch** | **+0.0636** | **+0.0041** | 0.0407 | **+0.0838** | **+0.1026** | **+1.46** | 0.095 | ❌ |
| *§6b, same pool, study grid* | +0.0559 | +0.0167 | 0.0252 | +0.0556 | +0.0606 | +1.56 | 0.05 | ❌ |

**Two of twenty shuffled-label draws beat the real data** (+0.1026 and +0.0828 against
the observed +0.0636), and the observed sits well under its own p95 bar. §6b's verdict
for this pool now has a second, independent measurement behind it at a nearly identical
z — reached from a different run, a different null seed and a different null mean.

⚠️ **The bar here (+0.0838) is HALF AGAIN §6b's (+0.0556), and the null mean is 4×
LOWER (+0.0041 vs +0.0167).** Same pool, same ticker, same `d=20, h=5`. The draws are
fat-tailed — four below −0.03, then +0.083 and +0.103 — so at 20 draws the p95 is
being set by the tail rather than measured. This is §12's "low-mean, fat-tailed"
shape on 27 channels instead of 918, and it is the direct argument for why **a
20-draw bar is not a precise number**; the z, which uses the whole distribution, is
stable across the two runs where the p95 is not.

⚠️ **The prototype was checked in to demonstrate the CONTRACT, and it still does —
it now demonstrates the other half of it.** It used to be the reference for what an
*unverified* run looks like (`"null": null`). It is now the reference for a run that
computed its bar and did not clear it, which is the more useful artefact: `evidence`
in its `outstanding.csv` reads `failed_null`, not `no_null`.

⚠️ **Reproduced bit-identically before the null was trusted.** The 2026-08-04 result
was re-derived at the archived knobs (**`max_features=12`, which is no longer the
default** — §14c) and matched to `0.00e+00` on `ic_mean`, on the kept set, and on the
`ensemble` column across all 27 channels. That mattered because §9d forbids judging a
number with a bar computed from a different procedure — and because the `ensemble`
column is what `outstanding.csv` and the STL-1 fingerprint derive from, so an exact
match is also the proof that nothing downstream moved. Verified after the write:
only `evidence` changed in the shortlist, and `final_features` still reports
`fingerprint 505fbe21a1f0 matches`.

⚠️ **`reports/feature_selection/*/` is still gitignored, but ALL 22 archived runs
were force-added on 2026-08-09** — the whole archive is now in history, not just this
prototype. **A tracked file overrides the ignore rule**, so those 22 keep updating
normally while a NEW run still has to be force-added on purpose.

⚠️ **And "~1 MB per run" was wrong by two orders of magnitude on the wide pools.**
The archive is **119.9 MB across 404 files** — 70 MB CSV, 50 MB PNG — and two files
carry half of it: `channel_correlation.csv` is **40.0 MB** for
`basic+economy_usa` and **16.5 MB** for `ta`, because it is the full N×N pairwise
matrix the redundancy prune used and N runs to the thousands on a wide pool. Every
other folder is 0.6-3.6 MB. **Before force-adding a new wide-pool run, look at that
one file** — the deliverable is `feature_importance.csv`, and `dropped_for` already
names what each channel was pruned into.

### 10d. ⚠️ THE THIRD MEASUREMENT OF `pool__basic` (2026-08-10) — it CLEARS, and that is not a pass

Run headlessly by `python -m feature_selection.run` into its own root
(`reports/feature_selection_basic/2026-08-10_034947__vcb__basic__return_5day`), on the
panel re-scraped the same day: **4,266 rows to 2026-08-07**, against 4,235 to 2026-06-25
for the two runs below it.

| measurement | rows | cut | observed | null mean | null sd | **p95 BAR** | **null MAX** | **z** | p | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| §6b, study grid | 4,235 | `max_features=12` | +0.0559 | +0.0167 | 0.0252 | +0.0556 | **+0.0606 — above observed** | +1.56 | 0.050 | ❌ |
| §10b, prototype | 4,235 | `max_features=12` | +0.0636 | +0.0041 | 0.0407 | +0.0838 | **+0.1026 — above observed** | +1.46 | 0.095 | ❌ |
| **§10d, this run** | **4,266** | **measured (14 kept)** | **+0.0783** | **+0.0013** | 0.0358 | **+0.0562** | **+0.0648 — BELOW observed** | **+2.15** | **0.0476** | ⚠️ **clears p95** |

⚠️ **This is the first `pool__basic` run where no shuffled draw beat the real data**, and
it is why the other two failed: their null MAX exceeded their observed (§5 rule 3), and
this one's does not. `outstanding.csv` records it as **`evidence=cleared_p95_not_a_pass`**
— the archive's own vocabulary, and the right reading. Four reasons not to promote it:

1. ⚠️ **`p = 0.0476` is the `1/(n+1)` FLOOR, not a measurement.** Zero of 20 draws
   reached the observed, so the p-value is pinned at 1/21 and 20 draws cannot tell
   `p = 0.05` from `p = 0.001`. The z is the number to quote — the same argument §9d
   makes for the cross-section, where it supports a z of +6.09 rather than +2.15.
2. ⚠️ **The null is fat-tailed and the p95 is being SET by the tail rather than
   measured.** The 20 draws run −0.0705 … +0.0648 with a mean of +0.0013; two of them
   (+0.0648, +0.0557) sit near the bar and the other eighteen are nowhere near it. §10b
   made exactly this argument about the same pool, and its bar (+0.0838) and this one
   (+0.0562) differ by 49% on the same data and the same 27 candidates.
3. ⚠️ **It is a DIFFERENT PROCEDURE from the two rows above it**, so the table is three
   measurements, not one repeated three times: 31 more sessions, and the measured cut
   (14 kept) in place of the flat `max_features=12`. §8 requires a bar per configuration
   and this run has its own — the observed and its null share `build()` in
   [run.py](run.py), so the comparison is internally valid. It is the comparison ACROSS
   rows that is not.
4. ⚠️ **AND THE MODEL TRAINED ON IT SHOWS NO SKILL.** `lstm__vcb__return_5day__final__
   d20_h5__basic__20260810-035257`: test IC **−0.0345** against a bar of +0.1348
   (p = 0.73), hit rate 0.486 — below a coin, the §6b signature. A selection that clears
   its own bar and a model that does not clear its own is the whole reason both bars
   exist. **`z = +2.15` at the selection stage bought nothing downstream.**

⚠️ **For scale: `pool__ta` cleared at `z = +2.52` (§12) and is likewise not a pass.**
Clearing a 20-draw p95 on a single ticker is the weakest positive result this package
can produce, and §2's verdict is unchanged by it.

⚠️ **The 6 shortlisted channels are not the 12 of §10b.** `sell_order_vol`,
`volume_negotiated`, `close_adjust`, `foreign_sell_value`, `prop_buy_val`,
`prop_sell_val` — and **three carry `PARTIAL` coverage flags**, two of them at **0.206**
(issue **COV-1**, reproduced on a six-row shortlist). `train_test_creator` then drops
both `prop_*` for zero TRAIN-slice coverage, so the model sees **4** channels. A cut
that spends a third of its shortlist on columns that do not exist across training is the
defect COV-1 describes, at a scale small enough to read in one table.

### 10c. ⚠️ The figure specs — and the three faults the first version shipped

[plots.py](plots.py) was rewritten 2026-08-04 after rendering the prototype and
looking at it. The palette was never the problem — it was **computed, not
eyeballed**, and passes every check (all-pairs CVD ΔE **9.2** against a target of
8.0, normal-vision ΔE **24.0** against a floor of 15.0). The faults were in FORM
and MARKS:

**1. A ranking of mean RANKS was drawn as bars from zero.** Every channel sat
between 11 and 19 on a 0-20 axis, so twenty-seven bars were visually identical: the
chart displayed a ranking while hiding every difference in it. A mean rank is an
ordinal position with no meaningful zero, so `plot_ensemble_ranking` is now a **dot
plot**, where a non-zero baseline is honest rather than a lie, and the spread that
decides the selection is the thing you see.

**2. The corner radius was in DATA units, so it was anisotropic.** One radius
applied to both axes means a corner that vanishes along a `ρ` axis (one unit ≈
1,000 px) and swallows the bar along a category axis (one unit ≈ 30 px) — the IC
bars came out shaped like tombstones. `_radius_in_data_units` converts one PIXEL
radius into each axis's own units, which is why bars are drawn as paths rather than
with `ax.bar`. ⚠️ **It reads the data transform, so limits must be set BEFORE the
marks are drawn** — every caller here does that, and a new one must too.

**3. Legends sat on the data, and the subtitle collided with the title.** The
subtitle was positioned in axes fractions while the title pad was in points, so on a
tall figure the two drifted into each other. Both are now in points. Legends are
always **below and outside** the axes: `plot_validation`'s subtitle carries two mean
ICs and a caveat, none of which can be shortened, so the legend is what moves.

Also fixed: bars capped at `_BAR_FRACTION` of their slot (the rest is air) instead
of filling it; the correlation matrix **masks its upper triangle** (it is symmetric
— drawing both halves doubles the ink to say the same thing twice); the target-ρ
chart uses **two flat fills instead of the diverging ramp**, because bar length
already encodes the magnitude and shading by it made the small values invisible;
and cell labels flip to white at 0.45 of the ramp rather than 0.55, where secondary
ink on mid-blue lands near 3:1.

⚠️ **`node` is not installed here, so `scripts/validate_palette.js` cannot be run
directly.** Its checks were ported to Python — same thresholds, same
Machado-Oliveira-Fernandes severity-1.0 transforms, same OKLab ΔE×100 — and the
palette validated against them. If the palette ever changes, re-run those checks;
do not eyeball the result.

## 11. ⚠️ THE FA RUN (2026-08-04) — the observed result is BELOW its own null's MEAN

`unified_schema_vcb.pool__fa ⋈ pool__targets`, `return_5day`, `d=1, h=5`, 162
channels, 20-draw block-shuffled null, holdout `2024-06-01` with a control.
⚠️ **Report folder REMOVED 2026-08-09** — the study now keeps only `d=20, h=5` runs.
The finding below stands as written; the artefacts are recoverable from commit
`5813342` (`git show 5813342 -- reports/feature_selection/2026-08-04_213330__vcb__fa__return_5day`).

| | observed | null mean | null sd | **p95 BAR** | null max | **z** | p | clears |
|---|---|---|---|---|---|---|---|---|
| **`pool__fa`, 162 ch** | **+0.0157** | **+0.0242** | 0.0337 | **+0.0740** | +0.0896 | **−0.25** | 0.62 | ❌ |
| `pool__basic`, 27 ch (§6b) | +0.0559 | +0.0167 | 0.0252 | +0.0556 | +0.0606 | +1.56 | 0.05 | ❌ |

⚠️ **THIRTEEN OF TWENTY SHUFFLED-LABEL RUNS BEAT THE REAL DATA.** The observed IC is
not merely inside the null — it is **below the null's centre**. This is the only
configuration in the package with a negative z, and it is the strongest available
statement that a pool carries nothing: the pipeline does better when the labels are
destroyed than when they are real.

⚠️ **Widening the pool RAISED THE BAR by a third and LOWERED the result by 3.6×.**
27 channels → 162 moved the p95 from +0.0556 to **+0.0740** while the observed fell
from +0.0559 to +0.0157. §6d predicted exactly this ("a wider feature pool buys no
observations — it only raises the null") and §8's rule demanded the re-run that
measured it. This is the number to cite when someone proposes pointing the selector
at `pool__ta`'s 921.

### 11a. Why: a fundamental is a STEP FUNCTION, and there are only 69 steps

**`pool__fa` changes value on 69 publish days out of 4,230 sessions — a channel
moves about once every 64 sessions.** The row count is 4,230; the number of distinct
feature configurations is **69**.

| fold | train rows | **train publishes** | test rows | **test publishes** |
|---|---|---|---|---|
| 1 | 500 | **9** | 745 | 12 |
| 2 | 1,245 | 21 | 745 | 13 |
| 3 | 1,990 | 33 | 745 | 13 |
| 4 | 2,735 | 45 | 745 | 13 |
| 5 | 3,480 | 57 | 745 | 13 |

⚠️ **Fold 1 fits 162 features to 9 distinct feature states.** Its `all channels`
R² is **−5.52**, and folds 2 and 4 reach −6.69 and −3.72 — the signature of a model
with far more parameters than information. The `selected` sets are saner (−0.40 to
−0.001) and still carry no signal.

⚠️ **`n_eff` here is NOT `n/h`.** §6d priced the single-ticker study at `4,230/5 ≈
850` independent observations. That bound applies to the LABEL. The FEATURE side is
bounded by publish events, so the honest count for "does this fundamental predict
the forward return" is **~69**, and ~13 per test fold. `_effective_n` does not know
this — it reports 149 per fold from the row count — so **every error bar in this
run is roughly 3× too small.**

⚠️ **The daily grain is a presentation, not information.** Forward-filling a
quarterly statement across 64 sessions makes 64 rows that say one thing. A run at
`lookback=20` would be worse than useless: over a window where the value never
changes, `slope`=0, `sd`=0 and `min`=`max`=`last`, so five of the six window
statistics collapse and the design matrix becomes ~1,000 constant columns. **That is
why this run is `lookback=1`** — a property of the data, not a shortcut.

### 11b. The holdout agrees, and the control is why it is readable

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | **+0.0232** | 0.528 |
| selected | **shuffled control** | **+0.0322** | 0.443 |
| all channels | real | −0.0111 | 0.528 |
| all channels | shuffled control | −0.0448 | 0.449 |

⚠️ **The shuffled control BEAT the real labels on the selected set** (+0.0322 vs
+0.0232), on 506 holdout rows carrying ~8 publish events. Consistent with §11's
verdict and with §6c's finding that a single holdout score is unreadable without
its control.

### 11c. What the ranking says, and why it should not be quoted

The ensemble is led by `balance_sheet_iv_chung_khoan_kinh_doanh` (trading
securities), `equity_growth_yoy`, `balance_sheet_iii_2_cho_vay_cac_tctd_khac`
(interbank lending) and `income_statement_ii_lai_lo_thuan_tu_hoat_dong_dich_vu`
(net fee income). **Do not use this list.** It is an internally consistent
description of noise — §6's standing warning, now with a negative z behind it. The
figures and CSVs are in the report folder so the run is auditable, not so the
ranking is actionable.

⚠️ **`lasso` scored 0.0 on all of the top 25** — cross-validated shrinkage zeroed
essentially every coefficient, which is itself a finding: no linear fundamental
signal survives a penalty. It was not flagged in `dead_methods` only because a
handful of channels outside the top 25 kept a non-zero coefficient.

⚠️ **`year` and `quarter` were EXCLUDED, and that is not housekeeping.** §6c found
price LEVELS acting as a date proxy, and the apparent signal died when they went.
`year` is not a proxy for the era — it *is* the era, an integer counting up through
the sample. Left in, it would have topped the ranking and meant nothing.

### 11d. What would actually test fundamentals

**Not more features and not another ticker.** The binding constraint is 69 publish
events, and neither adds any.

1. **Go cross-sectional** — the same move that worked in §9. `N` banks × the same
   quarters multiplies publish events by `N`, which is the only thing that changes
   this arithmetic. ⚠️ **`pool__fa` cannot express it**: the source is the CafeF
   *bank* chart of accounts, so it exists for **VCB and ACB only**. Building a
   cross-sectional FA study means parsing more issuers first.
2. **Make the sample the EVENT, not the session.** One row per publish date, with
   the label the return over the days following the announcement. That is ~69 rows
   for VCB — honestly small, rather than 4,230 rows pretending to be.
3. **Shift `publish_date` forward one session** before believing anything: the lag
   reaches 0 days, so a statement released after the close is a half-day leak this
   layer cannot detect (`data_preprocessor/CONTEXT.md`).

## 12. ⚠️ THE TA RUN (2026-08-04) — clears its p95 bar, and STILL is not a pass

`unified_schema_vcb.pool__ta ⋈ pool__targets`, `return_5day`, `d=1, h=5`, **918
channels**, `device="cpu"`, `max_features=12`, `permutation_repeats=10`, 20-draw
block-shuffled null, holdout `2024-06-01` with a control. Every knob matched to §11
so the two pools are the same procedure on different columns.
⚠️ **Report folder REMOVED 2026-08-09** — the study now keeps only `d=20, h=5` runs,
and §12c explains that this run is `lookback=1` by nature rather than by economy. The
finding below stands as written; the artefacts are recoverable from commit `5813342`.

| | observed | null mean | null sd | **p95 BAR** | null max | **z** | p | clears |
|---|---|---|---|---|---|---|---|---|
| **`pool__ta`, 918 ch** | **+0.1121** | +0.0249 | 0.0346 | **+0.0754** | **+0.1189** | **+2.52** | 0.048 | ⚠️ |
| `pool__fa`, 162 ch (§11) | +0.0157 | +0.0242 | 0.0337 | +0.0740 | +0.0896 | −0.25 | 0.62 | ❌ |
| `pool__basic`, 27 ch (§6b) | +0.0559 | +0.0167 | 0.0252 | +0.0556 | +0.0606 | +1.56 | 0.05 | ❌ |

⚠️ **`clears_bar` IS `True` HERE AND THE HONEST VERDICT IS STILL "NOT A PASS".** The
observed +0.1121 beats the p95 bar of +0.0754 — the first single-ticker
configuration in this package to do so — but **draw 10 of 20 scored +0.1189 on
SHUFFLED LABELS, above the observed.** `p = 0.0476` is `1/(n+1)`, the floor.
§6b faced exactly this shape (+0.0559 against a bar of +0.0556 and a max of +0.0606)
and called it ❌; the same reading applies at four times the z. **The boolean
`clears_bar` is a mechanical p95 comparison and it is the wrong summary whenever the
null max exceeds the observed — quote the max beside it.**

⚠️ **The null is LOW-MEAN, FAT-TAILED, and that is the finding.** Draws: four
negative, twelve below +0.04, then +0.055, +0.058, +0.060, +0.073 and +0.119. A mean
of +0.0249 with a max of +0.1189 is a 4.8× ratio, against 3.6× for `pool__basic`. On
918 channels the *typical* shuffled run earns little, but the tail of the selection
distribution reaches the observed value — which is precisely why a mean-based z of
+2.52 overstates the case and 20 draws cannot resolve it. **This configuration needs
100+ draws to be decidable, at ~5.3 min each (~9 h).**

### 12a. What it found — one idea, twelve times

| rank | channel | ensemble | carried by | reads as |
|---|---|---|---|---|
| **1** | **`close_ema_50_100_dist`** | **20.50** | `permutation` 1.000, `spearman` 0.820 | EMA-50 minus EMA-100 |
| 2 | `close_ema_50_200_dist` | 22.83 | — | ⚠️ pruned into #1 |
| 3 | `close_ema_100_200_dist` | 25.33 | `spearman` 0.733 | the same, slower pair |
| 4 | `close_kama_50_200_dist` | 33.67 | `spearman` **1.000** | the same, KAMA |
| … | | | | |
| 9 | `ht_dcperiod_signal_10` | 54.00 | `xgb_shap` **1.000** | dominant-cycle period |

**Eleven of the twelve kept channels are moving-average crossover distances or
long-MA slopes** — `close_{ema,sma,kama,dema}_{50,100,200}_dist[_pct]`,
`close_{sma,kama}_{100,200}_slope`. The twelfth is `ht_dcperiod_signal_10`. The
|ρ| ≥ 0.9 prune kept them as distinct, which they are pairwise; **as a set they are
one hypothesis — trend-following** — and that agrees with memory
`project-vcb-forecasting-conclusion`, reached by a different route.

⚠️ **NINE OF THE TWELVE ARE IN RAW PRICE UNITS, AND §6c IS ABOUT EXACTLY THIS.**
`ta_functions.py` builds `{pair}_dist = ma_fast − ma_slow` in VND and `_dist_pct =
dist / ma_slow` separately — the source comment on the `_pct` variants says
*"removes price-level dominance"*. VCB's close ran ~9× over 2009-2026, so the
absolute channels scale with the era, and §6c found that removing the price level
removed the apparent signal. **Only `close_dema_100_200_dist_pct`,
`close_dema_50_200_dist_pct` and `ht_dcperiod_signal_10` are scale-free.** The run
that decides this is `normalize="zscore"` or a `_pct`-only pool, each with **its own
null** (§8) — not yet done, and until it is, the trend-following reading is a
hypothesis, not a result.

⚠️ **`lasso` is a DEAD METHOD** — cross-validated shrinkage zeroed every coefficient
across all 918, so the "ensemble of six" is five. Same as `pool__basic` (§4) and
stronger than `pool__fa`, where a few coefficients outside the top 25 survived. **No
linear signal survives a penalty in any pool tried on this ticker.**

⚠️ **The hit rate is 0.516 selected and 0.494 for all channels**, against 0.477 in
§6b. Marginally above a coin for the first time on a single ticker, and still inside
noise at `n_eff = 149` per fold. R² stays deeply negative (−0.21 … −1.69).

⚠️ **The IC trend is +0.0165, POSITIVE** — the only single-ticker configuration here
that does not decay across folds (§6b: −0.034; h=10: −0.049). Fold ICs +0.166,
−0.009, +0.094, +0.131, +0.179.

### 12b. The holdout — real beats its control, by about one standard error

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected (12) | real | **+0.1732** | **0.490** |
| selected (12) | shuffled control | +0.0612 | 0.492 |
| all channels (918) | real | **+0.2085** | 0.504 |
| all channels (918) | shuffled control | +0.1076 | 0.490 |

⚠️ **Real beats the control in both rows** (+0.112, +0.101), which §11b could not
manage — there the control *beat* the real labels. But `n_eff` on 506 holdout rows is
**101**, so SE(IC) ≈ 0.10 and a margin of +0.11 is **one standard error**. And a
model trained on permuted labels scored **+0.1076**, which is §6c's demonstration
repeating: a single holdout score is unreadable without its control, and with one it
is merely suggestive.

⚠️ **The selected set's holdout hit rate is 0.490 — below a coin — with an IC of
+0.173.** §6b's signature of a model reading MAGNITUDE, not direction. Note the
holdout beat the development IC here (+0.173 vs +0.112), the reverse of §9g.

### 12c. ⚠️ `lookback=1` — a choice about TA, not about wall clock

A technical indicator **already is a window statistic**: `sma_200` is a 200-day mean,
`bb_20_bandwidth` a rolling sd, `macd` a difference of two EMAs. Wrapping §1a's
20-day `last/mean/slope/sd/min/max` around 918 of them computes windows of windows
and multiplies the design matrix by six to 5,508 columns. §11 reached `lookback=1`
for `pool__fa` for the opposite reason (the values barely change); here the reason is
that the windowing is already inside the feature. §9i/§9j support it from a third
direction — `last` carried nearly every top channel and `d=1 ≈ d=20`.

It is *also* what makes the run affordable, and that is worth stating plainly: one
pass is **332 s** on CPU, of which `permutation` is 62 % and `lasso` 22 %; the null
alone took **105 min**. At `d=20` this run would not have finished in a day, which
memory `project-feature-selection-ta-cost` measured for the retired selector (6.6 h
on the old 18,100-column design).

⚠️ **The `d=20` run is therefore UNTESTED, not dismissed.** If it is ever wanted, use
GPU + a reduced `permutation_repeats` — and re-run the null at the same settings,
because §9d's rule is that a bar computed from a cheaper pipeline than the number it
judges is a different procedure.

### 12d. What would actually settle this

1. **Kill the era proxy.** Re-run on `_dist_pct`-style scale-free channels only, or
   `normalize="zscore"`, each with its own null. §6c is the precedent and it is the
   single largest threat to §12a's ranking.
2. **More draws.** 20 cannot separate this null's tail from the observed. 100 draws
   at ~5.3 min is ~9 h — line-buffered and checkpointed per draw (§9k), which this
   run already did.
3. **Go cross-sectional**, which is what §7 step 4 asked for and what neither §11 nor
   §12 did. `pool__ta` exists for VCB only in this schema; the equivalent panel for
   `unified_schema_all` does not exist yet. That is the run with something to gain —
   §9's target has already beaten its null, so there is something for 918 technical
   channels to add to.

## 13. ⚠️ THE BANK SECTOR (2026-08-05) — a cross-section that is WIDE ENOUGH TO BUILD and not to resolve

`unified_schema_bank.pool__basic ⋈ pool__targets`, `cs_rank_5day`, `d=20, h=5`,
`cs_rank` features, 27 channels, 20-draw `date_block` null, holdout `2024-06-01`
with a control. **The §9c protocol with one thing changed: the universe is a GICS
SECTOR instead of an index.**
Report: `reports/feature_selection/2026-08-05_004241__bank__basic__cs_rank_5day`
→ `unified_schema_bank.rank_5day__final__d20_h5` (`final_features/CONTEXT.md`).

| | observed | null mean | null sd | **p95 BAR** | null max | **z** | p | clears |
|---|---|---|---|---|---|---|---|---|
| **BANK, 20 names** | **+0.0087** | **+0.0073** | 0.0134 | **+0.0249** | +0.0357 | **+0.11** | **0.52** | ❌ |
| *VN30 (§9h)* | +0.0233 | +0.0110 | — | +0.0248 | — | +1.42 | — | ❌ |
| *VN100 (§9d)* | +0.0289 | +0.0044 | 0.0040 | +0.0117 | +0.0119 | +6.09 | 0.048 | ✅ |

⚠️ **ELEVEN OF TWENTY SHUFFLED-LABEL DRAWS BEAT THE REAL DATA** (`p = 0.52`), and the
observed **+0.0087** sits on top of the null's mean **+0.0073**. This is the second
weakest result in the package after `pool__fa`'s `z = −0.25`, and unlike VN30 it
cannot be read as "a real effect too small to resolve" — there is no gap to resolve.

### 13a. The panel

`_ingest_unified_pool_basic(DataPreprocessor.UNIFIED_BANK)` — GICS
`industry_code = '401010'` (Financials → Banks → Banks). **53,921 rows, 20 tickers,
4,358 sessions, 2009-01-02 → 2026-06-26**, built in 1.4 s and verified against the
single-ticker schema: 4,235 VCB rows compared, **0 disagreeing `return_5day`
values** — the same check §9a ran for `unified_schema_all`.

After `min_width=10`: **39,056 rows, 2,201 dates, 2017-08-17 → 2026-06-26, median
width 20, density 0.885.**

⚠️ **`min_width=10`, not §9's 20, and it is a real compromise.** Only 20 banks exist,
so `min_width=20` starts the panel at **2021-03-24** and throws away half the
sessions — and `n_eff` is `dates/h`, so sessions are the scarce thing. At 10 the
panel keeps 2,201 dates at a median width of 20; the cost is ~99 early dates whose
cross-section is 10-11 names.

⚠️ **The bank universe is 20 names and GROWING** — 6 in 2009, 8 in 2015, 11 in 2017,
15 in 2018, 20 from 2021. A sector panel is far more ragged than an index one, and
the ragged part is the early history.

### 13b. ⚠️ `1/√N` CONFIRMED TO 3 %, WHICH MAKES THE FAILURE MORE INTERESTING

| fold | daily-IC sd | t | days positive |
|---|---|---|---|
| 1 | 0.2748 | −0.54 | 47.1 % |
| 2 | 0.2456 | **+1.85** | **59.8 %** |
| 3 | 0.2056 | +0.48 | 49.8 % |
| 4 | 0.2574 | −0.24 | 51.7 % |
| 5 | 0.2373 | +0.04 | 51.8 % |
| **mean** | **0.244** | | **hit rate 0.497** |

**Predicted from VN30's 0.205 by `√(30/20) = 1.22`: 0.251. Measured: 0.244.** §9b's
precision argument now holds across four widths (1 → 20 → 30 → 100) and is the
best-evidenced claim in this file.

⚠️ **But the OBSERVED IC fell too, +0.0233 → +0.0087, and the BAR DID NOT MOVE**
(+0.0248 → +0.0249). Precision predicts a rising bar at lower `N`; it did not rise.
So the BANK result is not VN30's result made harder to see — **it is a different and
weaker result**, and §13d is why that may be about sectors rather than about 20.

### 13c. ⚠️ The holdout is NEGATIVE and its control beat it

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected (12) | real | **−0.0262** | 0.485 |
| selected (12) | **shuffled control** | **+0.0095** | 0.503 |
| all channels (27) | real | −0.0136 | 0.490 |
| all channels (27) | shuffled control | −0.0250 | 0.486 |

⚠️ **Both real holdout ICs are negative, and on the selected set the control WON** —
the §11b signature. Development said +0.0087, the untouched tail says −0.0262. At
`n_eff = 97.8` with daily sd 0.244, SE ≈ 0.025, so every cell here is inside one
standard error of zero: this refutes nothing and confirms nothing, and saying
"banks mean-revert" from it would be reading noise.

⚠️ **The selection HURTS**: all 27 channels beat the pruned 12 in the aggregate
(+0.0147 vs +0.0087) and on the holdout (−0.0136 vs −0.0262). §9i saw the same at
780 names. `max_features=12` was chosen for a 27-channel single-ticker pool and
keeps failing to earn its place anywhere else.

⚠️ **`lasso` scored 0.0 on 26 of 27 channels** — only `avg_vol_per_buy_order` kept a
non-zero coefficient, so it is a hair away from `dead_methods` and should be read as
dead. Same conclusion as §4 and §12a.

### 13d. ⚠️ Why a SECTOR may be the wrong cross-section, not just a small one

The kept set is the §9e family — `value_negotiated`, `close_adjust`,
`sell_order_vol`, `avg_vol_per_sell_order`, `volume_negotiated`,
`avg_vol_per_buy_order` — so the features are not the difference. Two readings, and
this run cannot separate them:

1. **Width.** 20 names is below VN30, which already failed. §9h's threshold stands
   and nothing more needs explaining.
2. **⚠️ A SECTOR CO-MOVES, SO THERE IS LESS TO RANK.** Twenty banks share a common
   factor — rates, credit growth, one regulator — and `cs_rank_5day` asks which of
   them beats the others *today*. Removing the market factor is what made the
   cross-sectional target work (§9f); removing the SECTOR factor as well may leave
   too little dispersion for any feature to explain. **This is testable and was not
   tested**: compare the cross-sectional sd of `return_5day` within banks against
   within a size-matched random 20 names from `unified_schema_all`. If the bank
   dispersion is materially lower, reading 2 rather than 1 is the explanation.

⚠️ **Do not conclude "bank stocks are efficiently priced."** The run establishes that
*this pipeline, on this pool, at this width* cannot distinguish its output from
shuffled labels. §9j found a large effect at 301 names using the same 27 channels.

### 13e. What would test the sector hypothesis properly

1. **Run the dispersion comparison in §13d.2 first** — it is minutes of work and it
   decides which of the two readings to spend a run on.
2. **A sector-neutral target on the WIDE panel.** Rank within sector but score across
   all 780 names, so the cross-section stays wide while the sector factor is removed.
   That separates "sectors have no internal signal" from "20 is too few".
3. **A wider sector.** Banks are the biggest GICS industry in VN at 20 names, so no
   VN sector reaches §9h's ~100 threshold — which is itself the finding: **a
   single-sector cross-sectional study is not resolvable in this market.**
4. ⚠️ **Not more features.** §11 and §12 both widened the pool on an unresolvable
   panel and both raised their own bar. The constraint here is `N` and dispersion,
   and neither `pool__ta` nor `pool__fa` supplies either.

## 14. ⚠️ `outstanding.csv` (2026-08-09) — one final feature list PER RUN

[outstanding.py](outstanding.py) reduces each archived run to the channels it
actually chose, and writes the result **into that run's own folder**:

```
reports/feature_selection/<run>/outstanding.csv   ⭐ the deliverable, 20 of them
```

`python -m feature_selection.outstanding` rebuilds all of them in about a minute;
`--dry-run` prints without writing. **10-236 channels per run, median 40, 952 rows
over 20 runs** (2026-08-09: the two `d=1` runs, `pool__fa` and `pool__ta`, were
removed — the study keeps only `d=20, h=5`).

Three filters, in order: **the per-run cut** (§14c — replaced `max_features=12` on
2026-08-09); **an uncapped |ρ| ≥ 0.9 prune**; and **ties on the ensemble mean rank
broken by `permutation`**, the only out-of-sample ranker (§4), then by |ρ| vs the
target. The tie loser is named in `beat_in_tie` and the prune's victims in
`absorbed_as_redundant`, so nothing leaves the shortlist unrecorded.

⚠️ **THERE IS NO COMBINED FILE, AND THAT IS THE DESIGN.** The 22 runs are not one
experiment — different pools, two targets, two grains, and §8 is a list of ways two
runs look comparable and are not. A single merged shortlist is precisely the artefact
that gets quoted against a configuration it was never computed for. Every file
therefore carries `run_id`, `target`, `horizon_h`, `lookback_d`, `grain` and
`evidence`, so the next module merges **knowingly**, on those columns.

⚠️ **`grain` is the column that decides whether a file can be concatenated at all.**
`date` for the 21 single-ticker runs; **`date+ticker` for the bank run**, which is
cross-sectional — 20 banks on one Tuesday are 20 rows, so folding it into a
date-indexed table collapses the dimension the run was about (§13).

⚠️ **The source table is derived, not looked up.** The pools are disjoint (verified:
`basic ∩ fa = basic ∩ ta = fa ∩ ta = ∅`) and economy columns self-identify by a
`<country>__economy__` prefix, so a channel maps to its `pool__*` **from the archive
alone, with no database connection** — 0 `unknown` across all 952 rows, mapping onto
19 distinct `pool__*` tables. An
unrecognised channel becomes `unknown` rather than a guess: a wrong table fails
loudly when read, a silent wrong guess would not.

⚠️ **`best_stat` is guidance, not a column to fetch.** It names which of
`last/mean/slope/sd/min/max` carried the channel; the RAW column is what gets read
and `windows.window_design` computes the statistics downstream (§1a).

### 14d. ⚠️ `coverage` / `coverage_flag` — 26 % OF THE SHORTLIST BARELY EXISTS (2026-08-09)

Added for issue **COV-1**. Each row now carries the channel's non-null share, read
from the run's own `coverage.csv`, and a `PARTIAL` flag below `COVERAGE_FLOOR = 0.95`.

| | rows |
|---|---|
| shortlisted, full coverage | 704 |
| **shortlisted, `PARTIAL`** | **248 of 952 — 26 %** |

⚠️ **The worst shortlisted channel exists for 2.4 % of the sample.**
`germany__…__deelpc` at **0.024**, then a wall of macro series at 0.031-0.036, and
`prop_buy_vol` at 0.20. These were ranked highly *on the fraction of history where
they exist* — the ranking is computed over the whole panel, so a channel present only
after 2023 is scored against a target it can only see the tail of.

⚠️ **This FLAGS, it does not filter, and the distinction is the honest part.** The
archive cannot see where a downstream train/test cut will fall, so it cannot know
whether a coverage of 0.20 means "untrainable" or merely "ragged" — that depends on
the split, which `train_test_creator` chooses. A coverage of 0.20 is therefore a
**warning**, not a verdict. `train_test_creator`'s `on_untrainable="drop"` remains the
thing that acts, on the 26 channels whose train-slice coverage is actually zero.

⚠️ **Adding a COLUMN cannot make a table stale, and that is why this was safe.** The
STL-1 fingerprint is a digest of the `(source_table, channel)` SET
(`final_features.fingerprint`), so two new columns leave it untouched — verified
across all 20 runs after the rebuild, and `final_features` still reports
`505fbe21a1f0 matches`. **Filtering on the flag would NOT have been safe**: dropping
248 rows changes the set, and that is the full STL-1 domino (§7 of
`final_features/CONTEXT.md`).

### 14a. ⚠️ WHAT COMPARING THE FILES SHOWS — THE INSTABILITY IS IN THE *ORDERING*, NOT THE MEMBERSHIP

Union the 19 `date`-grain files. **All nineteen contain `pool__basic` and all nineteen
saw the SAME 27 channels**, ranked by the same six methods on the same folds; they
differ only in which `pool__economy_<country>` block was joined alongside. A stable
signal would return the same names.

⚠️ **THIS SECTION REVERSED WHEN `max_features=12` WENT (2026-08-09), AND THE REVERSAL
IS THE FINDING.** It used to read "199 of 203 channels appear once" and cite
`foreign_own` surviving 9 of 19 chances as proof the selection was not consistent with
itself. Both numbers were measured on shortlists **truncated to 12**. At the measured
cut the shortlists are 10-236 long, and the same comparison gives the opposite
impression — so the two have to be read side by side, at **matched length**:

| | union | appear once | top repeat-selected |
|---|---|---|---|
| **top 12 of each run** (the old cap) | 204 | **198** | `foreign_own` **11**/19, `volume_negotiated` 7, `close_adjust` 4, `foreign_sell_value` 2 |
| **full shortlist** (10-236, median 40) | 750 | **725** | `volume_negotiated` **19**/19, `avg_vol_per_buy_order` 18, `avg_vol_per_sell_order` 18, `foreign_own` 17, `prop_buy_val` 17 |

⚠️ **At matched length the original finding stands, and it is the one to quote.**
Compare the top 12 and no channel survives more than 11 of 19 chances — adding an
unrelated block of macro columns reshuffles which of the 27 price/flow channels ranks
top. §6b said a selection on shuffled labels is internally consistent; this says the
selection is not even consistent **with itself** across runs sharing the same features
and the same target.

⚠️ **At full length the `pool__basic` core is nearly always PRESENT, and that is a
weaker claim than it looks.** `volume_negotiated` appears in all 19 and four more in
17-18 — but a shortlist of 40-236 has far more room than one of 12, so this measures
*presence in a longer list*, not *agreement on an ordering*. All 27 basic channels
reach the union; only 2 of them appear just once. **Membership stabilised because the
cut got wider, not because the runs started agreeing.**

⚠️ **The 725 singletons are mostly ARITHMETIC, not disagreement** — 723 of them are
economy channels, and each `pool__economy_<country>` block is a candidate in exactly
one run, so it *cannot* be chosen twice. `final_features/CONTEXT.md` §6 makes the same
point about the table this union builds.

⚠️ **`foreign_own` tops that table and §9e ranked it LAST of 27** on the VN100
cross-section. Different studies need not agree — but nothing here lets either
ordering be quoted as *the* ordering.

### 14b. ⚠️ `evidence` — what a row in one of these files is worth

Every row carries the null verdict of the run that produced it:

| evidence | runs | rows | is |
|---|---|---|---|
| `no_null` | **18** | **928** | **no bar was computed at all** — §10 records an absent null as absent, never as a pass |
| `failed_null` | **2** | **24** | inside what shuffled labels produce — `bank` z = +0.11 (§13), `pool__basic` z = +1.46 (§10b) |

⚠️ **Since the `d=1` runs were removed, NOT ONE surviving run clears anything.**
`pool__ta` was the only `cleared_p95_not_a_pass` in the archive and it is gone; what
remains is 18 runs with no bar and 2 that failed their own. **928 of the 952 rows in
these files come from runs that never computed a null.** (Row counts are per §14c's
measured cut; before it they were 220 of 230 at a flat 12 channels per run.)

⚠️ **One run moved from `no_null` to `failed_null` on 2026-08-09**, and the
distinction is the whole point of the column: `no_null` is an *unknown*, `failed_null`
is a *measurement*. The archive is no better than it was — it is better **described**.

⚠️ **`outstanding.csv` is a FETCH LIST, not a green light.** It says which columns to
assemble for the next stage; it does not say they predict anything. **The cheapest
missing null has now been RUN** (2026-08-09): the bare `pool__basic` run,
`2026-08-04_205945__vcb__basic__return_5day`, 27 channels, one ticker — **456 s
end-to-end for reproduction plus 20 draws**, and it failed at `z = +1.46` (§10b). It
changed no channel and no fingerprint, exactly as predicted, so nothing downstream
went stale.

⚠️ **The 18 remaining `no_null` runs are NOT in that price class.** Each is
`pool__basic + one pool__economy_<country>`, and `basic+economy_usa` alone spent
**12,255 s on `permutation` in a single pass** (its own `metadata.json`) at 1,458
channels — so one 20-draw null there is ~68 CPU-hours, and all 18 is over 1,000. That
is the substance of issue **EVD-1**: the gap is not neglect, it is arithmetic. §12c's
levers (`permutation_repeats` 10 → 3, CUDA above `AUTO_CUDA_MIN_FEATURES`, 10 draws)
are what would make even one of them affordable, and §8's rule then requires the
*observed* to be re-run at those same settings.

### 14c. ⚠️ THE COUNT IS NOW MEASURED PER RUN — `max_features=12` is gone (2026-08-09)

[selection_cut.py](selection_cut.py). **Twelve was chosen for a 27-channel
single-ticker pool and then applied to a 1,458-channel one.** §9i and §13c had already
measured what that costs — at 780 names and in the bank sector, *all* channels beat the
pruned 12 in *every* fold. It also truncated the record: `_prune` **broke** at the cap,
so every channel below it carries `kept=False` with an empty `dropped_for` and
"redundant" is indistinguishable from "never examined".

| | before | after |
|---|---|---|
| channels per run | **12, every run** | **10-236, median 40** |
| rows over 20 runs | 230 | **952** |
| `pool__basic` (p=27) | 12 | 10 |
| `basic+economy_usa` (p=1,458) | 12 | **236** |
| `dropped_for` | truncated at the break | complete |

**Two tiers, unioned, then the uncapped prune.** Nothing is re-fitted and no database
is touched — the archived `feature_importance.csv` and `channel_correlation.csv`
determine every number, and the rebuilt method ranks reproduce the stored `ensemble`
to **2.8e-14** across all 20 runs.

**1. CONSENSUS — the ensemble beats a shuffled-METHODS null.** Permute each method's
rank column independently, keeping every marginal exactly (ties, and a dead method's
constant, survive) and destroying only cross-method agreement. Benjamini-Hochberg at
`fdr_q=0.10`.

⚠️ **The independence assumption was measured, not assumed: mean pairwise Spearman
between the six rank columns is +0.071** across the archive (−0.063 to +0.193). That
is also a finding — **the six rankers agree barely more than chance**, which is §14a's
cross-run instability seen *inside* a single run.

⚠️ **This tier keeps 12 rows out of 952** and none at all on `pool__basic`. Correct
for the question it asks, useless as a fetch list on its own.

**2. SPECIALIST — the top of some single method's own score curve.** A mean rank
buries a channel one method is certain about and five have no opinion on;
`aggregate_to_channels` already rejects that logic across stats (§1a) and the same
argument holds across methods. Per live method, sort its normalised scores descending
and cut at the **knee**.

⚠️ **The knee works on SCORES and is meaningless on RANKS.** Measured: a knee on the
`ensemble` column keeps 92 of 113 and **1,313 of 1,458** — a mean of ranks is
near-uniform by construction and has no bend, the same flatness §10c.1 fixed in the
ranking chart. A per-method score curve is long-tailed and bends sharply: "≥ 0.9 of
the best on some method" holds for **5-9 channels whether `p` is 27 or 1,458.**

⚠️ **At `p = 27` the specialist tier degenerates** — six knees cover 18 of 27
channels, so the rule reduces to "the whole non-redundant pool". That is the honest
answer at that width (§9h: a 27-channel single-ticker run resolves nothing) and
`cut_report()["specialist_share"]` reports it as a number so it is not read as a
selection.

⚠️ **`kept_by` and `evidence` are different verdicts.** `kept_by` is the channel
against shuffled *methods*; `evidence` is the run against shuffled *labels*. All 952
rows still sit in runs that failed or never computed the second one.

⚠️ **`FeatureSelector(max_features=...)` now defaults to `None`** — uncapped. Pass an
integer only to reproduce an archived run.
