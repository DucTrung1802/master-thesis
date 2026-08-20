# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:45:00+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_tcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,042 rows x 105 columns
- **range** 2018-06-04 to 2026-08-07 (2042 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,032 labelled, 10 unlabelled
- mean +0.00528, sd 0.06858, range -0.2836 to +0.3035

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 20 |
| `horizon_h` | 10 |
| `normalize` | none |
| `purge_gap_rows` | 29 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 2013 |
| `design_columns` | 504 |

## Result

- **58 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb_21`, `drv_negotiated_value_share`, `drv_order_count_imb_z21`, `drv_rogers_satchell_21`, `drv_log_order_size_ratio`, `drv_close_pos_252`, `drv_close_z_21`, `drv_dist_from_low_21`, `drv_dist_from_high_252`, `drv_close_z_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.0123 | +0.0697 |
| `ic_trend_per_fold` | +0.0228 | -0.0563 |
| `hit_rate` | +0.4696 | +0.4818 |
| `n_eff_per_fold` | 29.6 | 29.6 |

## The bar

- observed **-0.0123** against a p95 bar of **+0.1235** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0039, sd 0.0761, max +0.1454; z = **-0.21**, p = 0.5455

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

SSK-1 single-stock VN30 sweep, 2026-08-20. pool__basic only: pool__ta cannot build on a one-company schema (STA-1 -- gold.stocks_ta disagrees with silver for all 30 names, in BOTH directions).
