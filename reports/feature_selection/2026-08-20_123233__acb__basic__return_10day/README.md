# Feature-importance run — `return_10day`

*Generated 2026-08-20T12:32:36+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_acb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,383 rows x 105 columns
- **range** 2009-01-02 to 2026-08-07 (4383 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,373 labelled, 10 unlabelled
- mean +0.00633, sd 0.05871, range -0.2620 to +0.4452

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
| `dev_samples` | 4354 |
| `design_columns` | 504 |

## Result

- **56 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_amihud_63`, `avg_vol_per_sell_order`, `drv_foreign_flow_ratio_21`, `drv_ret_log_1d`, `drv_vwap_raw`, `drv_log_order_size_ratio`, `drv_close_pos_252`, `drv_ret_skew_63`, `drv_ret_1d`, `drv_order_count_imb`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0506 | +0.0649 |
| `ic_trend_per_fold` | +0.0245 | -0.0252 |
| `hit_rate` | +0.5048 | +0.5022 |
| `n_eff_per_fold` | 76.5 | 76.5 |

## The bar

- observed **+0.0506** against a p95 bar of **+0.0653** (10 draws) — **DOES NOT CLEAR**
- null mean -0.0079, sd 0.0451, max +0.0921; z = **+1.30**, p = 0.1818

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

SSK-1 single-stock VN30 sweep, 2026-08-20. pool__basic only (STA-1 blocks pool__ta on a one-company schema).
