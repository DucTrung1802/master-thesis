# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:19:29+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_plx` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,289 rows x 105 columns
- **range** 2017-04-21 to 2026-06-26 (2289 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,279 labelled, 10 unlabelled
- mean +0.00351, sd 0.07592, range -0.3097 to +0.5317

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
| `dev_samples` | 2260 |
| `design_columns` | 501 |

## Result

- **58 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_amihud_21`, `drv_foreign_net_value_ratio`, `close_adjust`, `drv_range_hl_pct`, `drv_foreign_flow_ratio_5`, `foreign_sell_value`, `drv_volume_pos_63`, `drv_foreign_own_chg_5`, `drv_ret_1d`, `drv_order_vol_imb_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1315 | +0.1081 |
| `ic_trend_per_fold` | +0.0118 | -0.0016 |
| `hit_rate` | +0.5268 | +0.5228 |
| `n_eff_per_fold` | 34.6 | 34.6 |

## The bar

- observed **+0.1315** against a p95 bar of **+0.0707** (10 draws) — **CLEARS**
- null mean +0.0077, sd 0.0486, max +0.0818; z = **+2.55**, p = 0.0909

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
