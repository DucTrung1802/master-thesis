# Feature-importance run — `return_10day`

*Generated 2026-08-20T10:26:37+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_bcm` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,071 rows x 105 columns
- **range** 2018-02-21 to 2026-06-25 (2071 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,061 labelled, 10 unlabelled
- mean +0.00841, sd 0.08956, range -0.3638 to +0.6440

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
| `dev_samples` | 2042 |
| `design_columns` | 507 |

## Result

- **58 of 85 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_vol_ratio_10_63`, `drv_foreign_participation`, `drv_dist_from_high_252`, `foreign_sell_value`, `foreign_sell_volume`, `prop_buy_val`, `drv_close_z_63`, `drv_order_vol_imb_21`, `low`, `drv_volume_z_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0394 | +0.0756 |
| `ic_trend_per_fold` | +0.0399 | +0.0113 |
| `hit_rate` | +0.5003 | +0.5003 |
| `n_eff_per_fold` | 30.2 | 30.2 |

## The bar

- observed **+0.0394** against a p95 bar of **+0.0473** (10 draws) — **DOES NOT CLEAR**
- null mean -0.0226, sd 0.0493, max +0.0608; z = **+1.26**, p = 0.1818

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
