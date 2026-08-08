# Feature-importance run — `return_5day`

*Generated 2026-08-08T03:36:57.926401+00:00 at commit `1eae672+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__economy_united_kingdom`, `pool__targets`
- **panel** 4,235 rows x 224 columns
- **range** 2009-06-30 to 2026-06-25 (4235 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,230 labelled, 5 unlabelled
- mean +0.00319, sd 0.04295, range -0.1876 to +0.2914

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 20 |
| `horizon_h` | 5 |
| `normalize` | none |
| `purge_gap_rows` | 24 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `max_features` | 12 |
| `corr_threshold` | 0.9 |
| `device` | cpu |
| `random_state` | 42 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4211 |
| `design_columns` | 1241 |

## Result

- **12 of 207 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `united_kingdom__economy__government__economics__gbtaxr`, `united_kingdom__economy__housing__economics__gbmr`, `united_kingdom__economy__gdp__economics__gbgdpmin`, `united_kingdom__economy__trade__economics__gbed`, `united_kingdom__economy__labor__economics__gbreeb`, `united_kingdom__economy__business__economics__gbcap`, `united_kingdom__economy__prices__economics__gbppimm`, `united_kingdom__economy__housing__economics__gbhpiyy`, `united_kingdom__economy__money__economics__gboiar`, `united_kingdom__economy__prices__economics__gbei`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0665 | +0.0437 |
| `ic_trend_per_fold` | +0.0156 | -0.0080 |
| `hit_rate` | +0.4868 | +0.4763 |
| `n_eff_per_fold` | 147.4 | 147.4 |

## The bar

- ⚠️ **NO NULL WAS COMPUTED FOR THIS RUN.** A positive IC is not a result on its own — see `CONTEXT.md` §6b and §8. Treat the ranking below as descriptive until a shuffled-label null has been run for *this* configuration.

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/05_stat_profile.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `metadata.json`
