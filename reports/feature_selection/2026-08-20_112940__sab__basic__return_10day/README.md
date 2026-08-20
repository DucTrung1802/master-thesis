# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:29:44+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_sab` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,380 rows x 105 columns
- **range** 2016-12-06 to 2026-06-26 (2380 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,370 labelled, 10 unlabelled
- mean +0.00068, sd 0.06083, range -0.2563 to +0.4941

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
| `dev_samples` | 2351 |
| `design_columns` | 504 |

## Result

- **62 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_volume_pos_63`, `value_matched`, `drv_foreign_flow_ratio_5`, `drv_downside_vol_21`, `drv_value_z_21`, `drv_realized_vol_63`, `drv_close_z_21`, `drv_foreign_net_value_ratio`, `drv_foreign_flow_ratio_21`, `drv_order_vol_imb`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1292 | +0.1435 |
| `ic_trend_per_fold` | +0.0163 | +0.0365 |
| `hit_rate` | +0.5357 | +0.5269 |
| `n_eff_per_fold` | 36.4 | 36.4 |

## The bar

- observed **+0.1292** against a p95 bar of **+0.0719** (10 draws) — **CLEARS**
- null mean -0.0039, sd 0.0614, max +0.0994; z = **+2.17**, p = 0.0909

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
