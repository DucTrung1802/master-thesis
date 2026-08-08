# Feature-importance run — `return_5day`

*Generated 2026-08-07T03:55:39.953528+00:00 at commit `1eae672+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__economy_japan`, `pool__targets`
- **panel** 4,235 rows x 223 columns
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
| `design_columns` | 1229 |

## Result

- **12 of 205 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `japan__economy__labor__economics__jpmw`, `japan__economy__labor__economics__jpwag`, `japan__economy__housing__economics__jphpi`, `japan__economy__consumer__economics__jpblr`, `japan__economy__prices__economics__jptccpi`, `japan__economy__business__economics__jpcep`, `japan__economy__gdp__economics__jpgfcf`, `close_adjust`, `japan__economy__business__economics__jpcap`, `japan__economy__housing__economics__jphst`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0508 | +0.0723 |
| `ic_trend_per_fold` | +0.0283 | -0.0252 |
| `hit_rate` | +0.5256 | +0.4961 |
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
