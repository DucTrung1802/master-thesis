# Context — `src/result_evaluator`

> Scores a finished run **whatever produced it**, against a block-shuffled null.
> Built 2026-08-09. The folder existed and was empty.

```
python -m result_evaluator                  # leaderboard of every run
python -m result_evaluator --run <run_id>   # rescore one run
python -m result_evaluator --rescore        # rescore every run in place
python -m pytest result_evaluator/test_metrics.py -q
```

## 1. Why this is not inside `model/`

A run is finished when it holds `results/predictions_<split>.csv`. Everything after
that — the metric, the bar, the verdict — is a **reading** of that file and needs no
model, no GPU and no training.

The project already cashed that in once: `dir_auc` was backfilled across every
existing run without retraining any of them (`model/CONTEXT.md` §9). It cashed in
again during this build — **27 of the 28 run folders in this checkout have no
`metadata.json` at all**, because `src/model/runs/*/` is git-ignored and only
`results/` and `logs/` survived. A scorer that required the metadata could not read
26 of the 28 runs the project has produced. `run_metadata` returns `{}` instead, and
`_setup` infers task from the score COLUMN (`y_prob` → classification), lookback and
horizon from the run id, and records what it inferred in `inferred_setup`.

`model/common/metrics.py` is now a 60-line shim over this package, kept only so the
two old notebooks still run. There is one definition of every metric and it is here.

## 2. ⚠️ The one-fits-all metric set — and the argument for it

Every model here emits **one score per sample**; every target derives from **one
realised forward return per sample**. That is the entire overlap between a return
regressor, a direction classifier and a cross-sectional ranker, and it is enough:

| metric | question | invariant to |
|---|---|---|
| `ic` (Spearman) | does a higher score mean a higher return? | any monotone rescaling of the score |
| `dir_auc` | does a higher score mean *up* more often? | that, and the base rate |
| `hit_rate` | hit rate at the score's own median | that |
| `long_short` | what does the top quintile minus the bottom pay? | that; in return units |

**Ranking skill, directional skill, economic value.** All four read a *score*, never a
prediction in the target's units — which is exactly why a regressor's return, a
classifier's `P(up)` and a ranker's rank land in one column of one leaderboard.
`test_the_core_metrics_do_not_change_when_the_score_is_rescaled` asserts the property
the whole design rests on.

⚠️ **`ic` and `long_short` are both reported because they can disagree.** An IC can be
earned entirely in the middle of the distribution, where nothing is traded;
`long_short` only pays for the tails. `test_two_scores_with_the_same_ic_can_have_very
_different_long_short` builds exactly that pair.

⚠️ **`hit_rate` is at the score's MEDIAN, not at zero — and it was renamed FROM `dir_accuracy` on 2026-08-09 (issue NAM-1)** because the legacy shim's `dir_accuracy` is the sign hit rate at 0. Two quantities under one name in one leaderboard is how a column stops meaning anything. A classifier's score is a
probability and a ranker's is a rank, so "positive score" is not a shared notion. The
0-threshold version is `sign_accuracy` in the regression extras, and the two differ
whenever predictions are biased — which for a regressor trained on a standardised
target they are.

**The extras are per-task and additive**: RMSE / `RMSE_zero_baseline` / `r2` for
regression, `log_loss` / `brier` / `pr_auc` / `base_rate` for classification. RMSE is
meaningless for a classifier and `log_loss` is meaningless for a regressor, so neither
pretends to be shared.

## 3. ⚠️ Every core metric carries a bar, and the bar is not zero

`feature_selection/CONTEXT.md` §10 and `final_features/CONTEXT.md` §6 make the same
point about selections: a number without a null is descriptive, not evidence. So `ic`
and `dir_auc` are each reported with `<m>_p`, `<m>_bar` (the null's p95) and
`<m>_clears`, from 200 block-shuffled draws.

⚠️ **The shuffle is by BLOCKS of `lookback + horizon` rows** — one sample's whole
footprint, the same block `feature_selection.evaluation` uses. A row-wise permutation
destroys the label's own autocorrelation (consecutive `return_5day` values share 4 of
their 5 days), the null comes out far tighter than reality, and a worthless run clears
a bar that was never there. `test_a_row_wise_null_would_be_too_tight_on_an_overlapping
_label` measures the difference.

### ⚠️ 3a. The bug this shipped with for one hour, and what it cost

`block_shuffle` pads to a whole number of blocks **with NaN**. When the padded partial
block is permuted forward, those NaN land inside the retained slice: on 635 rows at
`block=25`, most draws carried 15 NaN, the rank correlation returned NaN, and the draw
was discarded. **10 of 200 draws survived** — and the p-value was still divided by
`draws + 1 = 201`, understating every p-value by roughly 20×.

Measured on the VCB run: bar `+0.0456` → `+0.1133`, `p = 0.025` → `p = 0.522`. Across
the archive it turned **13 "clears the bar" into 9**. The fix drops the NaN *pair*
rather than the draw (whole blocks still move together, so the structure the null
exists to preserve is untouched) and divides by the usable count. Two tests pin it:
`test_every_draw_is_usable_despite_the_shuffle_padding_with_nan` and
`test_the_p_value_uses_the_usable_draw_count_as_its_denominator`.

### ⚠️ 3b. This null is WEAKER than the one in `feature_selection`

`feature_selection.evaluation.null_distribution` re-runs the whole selection on every
draw, because selection is the step that inflates. This one cannot: by the time it
sees a prediction vector, the features were chosen, the architecture was chosen and
the epoch was early-stopped on val. It answers only:

> given THIS score vector, could its agreement with the target be produced by a target
> with the same autocorrelation and no relation to it?

**A run that fails this bar is dead; a run that clears it is not yet alive.** That
sentence is in `verdict()` and in every chart subtitle, because it is the only
defensible reading.

## 3c. ⚠️ An N-ticker PANEL is scored differently, and the grain is read from the file

Added 2026-08-09 with the bank chain (**BNK-1**). A `ticker` column carrying more than
one name switches `evaluate_run` to `evaluate_panel`, because scoring a panel as one
series is wrong in three separate ways and **each one flatters the model**:

| | series reading | panel reading |
|---|---|---|
| `n_eff` | `n / h` — 13,028 rows → 2,606 | **`n_dates / h`** — 653 dates → **130.6** |
| `ic` | pooled Spearman over all rows | **per-date rank correlation, averaged** |
| null | `block_shuffle` of ~1.25 dates of rows | **whole DATE BLOCKS of the label matrix** |

1. **Twenty banks on one date are one observation of the market, not twenty.** The
   row-wise `n_eff` overstates the evidence twentyfold.
   `feature_selection.evaluation.ic_summary` flags this in a comment and
   `cross_sectional.DailyICSummary` already used `n_dates/h`; this now matches.
2. **A pooled Spearman mixes "which bank beats which today" with "is today a good
   day".** The second is not what a market-neutral book trades. The cross-sectional IC
   is the per-date correlation averaged over dates, and `ic_days_positive` reports the
   share of days it was positive.
3. **The null moves whole dates** (`cross_sectional.shuffle_dates(mode="date_block")`
   in matrix form): each stock keeps its own labels, moved to a different fortnight, so
   cross-sectional dispersion, each name's own volatility and the label's
   autocorrelation all survive — only the day's FEATURES ↔ day's LABELS pairing dies.

⚠️ **The grain is read from the FILE, never from a config flag**, so a run cannot
claim to be one thing and be scored as the other. Every row records `grain`.

⚠️ Calibration was checked before it was used: on the real bank panel a random score
scores `ic = −0.025` at `p = 0.99`, and a score equal to the outcome plus one sigma of
noise scores `ic = 0.479` against a bar of `0.045`. A bar nothing can clear is not a
bar; one everything clears is worse.

⚠️ The panel null costs ~16 s for 200 draws on 653 × 20. It is a Python loop over
dates inside each draw — fine here, and the thing to replace first if a 300-name
universe ever runs through it.

## 4. ⚠️ `n` overstates the evidence, so `n_eff` is reported beside it

Consecutive samples share `h-1` of their `h` label days, so 635 test samples carry
about **127** independent observations. `n_eff = n/h` via
`feature_selection.evaluation.effective_sample`, and it is still optimistic: it prices
in the label overlap but not the input overlap (`d-1` of `d` days), which a windowed
design adds on top.

## 5. ⚠️ A classifier's return comes from `pool__targets`, or the metric is BLANK

The core block is measured against the realised **return**. For a regressor `y_true`
already is it. For a classifier `y_true` is a 0/1 label, so the return has to come
from somewhere else — and the first version read it from the dataset the run
references. **That could never have worked** (issue CMP-1): a classification dataset's
`y_test` *is* the 0/1 label with `target_scaler=None`, so `load_dataset` handed back
the very thing it was called to replace, silently and with no error.

The fix reads `return_{h}day` from **`pool__targets`**, joined on `(date, ticker)` —
the authoritative record of what a return is, the same principle that replaced the
`return_{h}day` name heuristic upstream (TGT-1).

⚠️ **When the schema is unknown, `ic` and `long_short` are NaN — not the label.** 27
of 29 runs have no `metadata.json`, so nothing records which schema they came from.
Substituting the label there put a hit-rate spread and a return spread in one
`long_short` column. `dir_auc` and `hit_rate` survive, because they need only the
up/down label and a classifier's `y_true` *is* that — so those stay comparable with a
regressor's. `return_source` records which path ran; the current board reads
`unavailable (no schema recorded)` on all 18 classification rows.

## 6. What it says today (2026-08-09, 30 runs)

| | |
|---|---|
| runs scored | 30 (3 new, 27 historical with no metadata) |
| split-metrics clearing a block-shuffled bar | **9** — none of them new |

| new run | grain | test `ic` (bar) | test `dir_auc` (bar) | verdict |
|---|---|---|---|---|
| `lstm__vcb__return_5day__final__d20_h5` | series | −0.0112 (+0.1133) | 0.479 (0.557) | **no skill** |
| `lstm__bank__rank_5day__final__d20_h5` | panel | +0.0013 (+0.0285) | 0.512 (0.522) | **no skill** |

Both stop at **best epoch 1** — never improving on their initialisation, train loss
falling while val rose. The VCB run's predicted series has **0.20× the standard
deviation of the realised one**. Visible in `figures_test.png` before any metric is
read.

⚠️ The bank run is the more interesting negative: 20 tickers, 26,964 training windows
and a cross-sectional reading — the direction `model/CONTEXT.md` §11 identified as the
only tradeable one — and it still sits inside its own null. Note also that the panel
null's mean `ic` is **+0.008, not zero**: shuffled labels pay something here, which is
exactly why the bar is not zero.

This reproduces `model/CONTEXT.md` §10–§11 on a completely rebuilt pipeline: freshly
selected channels, a purged split, a corrected null and a panel-aware reading did not
change the answer.

The 9 that do clear are the `probability_gain_5pct_5day` runs at lb1/lb15/lb25 and the
`return_5day` runs at lb1/lb2 — and §11 of `model/CONTEXT.md` already recorded why the
first group is not trustworthy (val and test ROC-AUC are decorrelated across
lookbacks, so the apparent test edge is not selected-for and does not reproduce).
Every one of them also reads `return_source = label`.

## 7. Files

```
metrics.py       the core block, the extras, the null, verdict()
evaluator.py     read a run folder; put every run on one board
plots.py         the four figures, on feature_selection/plots.py's theme
test_metrics.py  15 tests, no run folder, ~5 s
```

⚠️ **`plots.py` defines no palette.** `feature_selection/plots.py` is the one theme in
this repo, so this module imports its `use_theme`, `SERIES`, ink and gridline colours
and its `_titles`/`_legend` helpers. A figure from a selection report and a figure
from a run have to be readable side by side in one document; two palettes would make
the same blue mean two things.

⚠️ The `⚠️` glyph is **not** used in chart text — Segoe UI has no glyph for it and
matplotlib renders a box. Same reasoning as `final_features`' ASCII `NOT_SET`
sentinel. It is used freely in docstrings and Markdown, which render elsewhere.
