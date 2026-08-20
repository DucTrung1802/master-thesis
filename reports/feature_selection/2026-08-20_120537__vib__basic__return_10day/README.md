# Feature-importance run — `return_10day`

*Generated 2026-08-20T12:05:41+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vib` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,376 rows x 105 columns
- **range** 2017-01-09 to 2026-08-07 (2376 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,366 labelled, 10 unlabelled
- mean +0.01076, sd 0.07186, range -0.3075 to +0.3424

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
| `dev_samples` | 2347 |
| `design_columns` | 510 |

## Result

- **56 of 85 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_adjust`, `drv_upper_shadow`, `drv_close_z_21`, `drv_close_pos_21`, `drv_volume_z_21`, `drv_lower_shadow`, `drv_avg_order_size`, `avg_vol_per_buy_order`, `drv_ret_skew_63`, `drv_log_order_size_ratio`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0738 | +0.1240 |
| `ic_trend_per_fold` | -0.0051 | -0.0225 |
| `hit_rate` | +0.4768 | +0.4867 |
| `n_eff_per_fold` | 36.3 | 36.3 |

## The bar

- observed **+0.0738** against a p95 bar of **+0.1399** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0129, sd 0.0775, max +0.1501; z = **+0.79**, p = 0.2727

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
