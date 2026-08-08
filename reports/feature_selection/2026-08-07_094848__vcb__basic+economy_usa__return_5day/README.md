# Feature-importance run — `return_5day`

*Generated 2026-08-07T02:48:54.248108+00:00 at commit `1eae672+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__economy_usa`, `pool__targets`
- **panel** 4,235 rows x 1480 columns
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
| `design_columns` | 8747 |

## Result

- **12 of 1458 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `usa__economy__government__fred__fyoint`, `usa__economy__money__economics__usfbi`, `usa__economy__government__fred__fygfd`, `usa__economy__trade__fred__expjp`, `usa__economy__government__fred__mvmtd027mnfrbdal`, `usa__economy__business__economics__uscor`, `usa__economy__prices__fred__will5000ind`, `usa__economy__housing__fred__boaaahorusq156n`, `usa__economy__business__fred__ltotalnsa`, `usa__economy__consumer__fred__pcend`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1448 | +0.0100 |
| `ic_trend_per_fold` | -0.0079 | +0.0098 |
| `hit_rate` | +0.5210 | +0.4830 |
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
