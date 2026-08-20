# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:35:10+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_shb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,315 rows x 105 columns
- **range** 2009-04-20 to 2026-08-07 (4315 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,305 labelled, 10 unlabelled
- mean +0.00774, sd 0.09128, range -0.3247 to +0.8691

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
| `dev_samples` | 4286 |
| `design_columns` | 504 |

## Result

- **56 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_vwap_raw`, `avg_vol_per_buy_order`, `drv_dist_from_low_21`, `drv_order_vol_imb`, `drv_close_z_21`, `foreign_net_value`, `avg_vol_per_sell_order`, `drv_ret_skew_63`, `drv_avg_order_size`, `drv_foreign_own_chg_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0075 | -0.0553 |
| `ic_trend_per_fold` | +0.0427 | +0.0159 |
| `hit_rate` | +0.4583 | +0.4261 |
| `n_eff_per_fold` | 75.1 | 75.1 |

## The bar

- observed **+0.0075** against a p95 bar of **+0.0646** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0135, sd 0.0337, max +0.0906; z = **-0.18**, p = 0.5455

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
