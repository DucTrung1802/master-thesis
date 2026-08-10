# Feature-importance run — `return_5day`

*Generated 2026-08-10T13:44:52.889444+00:00 at commit `7e925a52+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__economy_japan`, `pool__targets`
- **panel** 4,266 rows x 223 columns
- **range** 2009-06-30 to 2026-08-07 (4266 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,261 labelled, 5 unlabelled
- mean +0.00315, sd 0.04303, range -0.1876 to +0.2914

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
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4242 |
| `design_columns` | 1230 |

## Result

- **122 of 205 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `japan__economy__labor__economics__jpwag`, `japan__economy__labor__economics__jpmw`, `japan__economy__consumer__economics__jpblr`, `japan__economy__gdp__economics__jpgfcf`, `japan__economy__business__economics__jpcap`, `japan__economy__prices__economics__jptccpi`, `japan__economy__housing__economics__jphpi`, `japan__economy__business__economics__jpcep`, `japan__economy__money__economics__jppgb`, `japan__economy__government__economics__jpgbv`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0509 | +0.0813 |
| `ic_trend_per_fold` | +0.0048 | -0.0087 |
| `hit_rate` | +0.4723 | +0.4900 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **+0.0509** against a p95 bar of **+0.0415** (20 draws) — **CLEARS**
- null mean -0.0016, sd 0.0390, max +0.0916; z = **+1.34**, p = 0.0476

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `null_draws.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/05_stat_profile.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `figures/10_null.png`
- `metadata.json`

## Notes

dagster analysis/feature_selection_economy/japan
