# Feature-importance run — `up_5pct_5day`

*Generated 2026-09-17T11:18:02+07:00 at commit `e201de6d+dirty`.*

## Input

- **schema** `unified_schema_mbb` (database `database_main_v2`)
- **tables** `pool__ta`, `pool__targets`
- **panel** 3,691 rows x 943 columns
- **range** 2011-11-01 to 2026-08-21 (3691 sessions, 1 tickers)

## Target

- **`up_5pct_5day`** — up_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 3,686 labelled, 5 unlabelled
- mean +0.10635, sd 0.30832, range +0.0000 to +1.0000

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 1 |
| `horizon_h` | 5 |
| `normalize` | none |
| `purge_gap_rows` | 5 |
| `window_stats` | last |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | 2022-03-08 |
| `dev_samples` | 2575 |
| `design_columns` | 717 |

## Result

- **412 of 717 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_wma_50_100_dist_abs`, `close_wma_21_50_dist_abs`, `close_sma_100_dist_abs`, `close_dema_50_200_dist_pct`, `mom_10_hist_abs`, `close_dema_100_200_dist_pct`, `close_wma_14_100_dist`, `close_dema_100_200_dist`, `close_dema_50_100_dist_abs`, `close_ema_100_dist_abs`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0115 | +0.0212 |
| `ic_trend_per_fold` | +0.0769 | +0.0478 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 82.8 | 82.8 |

## The bar

- observed **+0.0115** against a p95 bar of **+0.0310** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0037, sd 0.0188, max +0.0355; z = **+0.42**, p = 0.3636

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.1041 | — |
| selected | shuffled control | +0.1049 | — |
| all channels | real | +0.0957 | — |
| all channels | shuffled control | +0.0634 | — |

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `holdout.csv`
- `null_draws.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `figures/10_null.png`
- `metadata.json`

## Notes

event_chain: 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions
