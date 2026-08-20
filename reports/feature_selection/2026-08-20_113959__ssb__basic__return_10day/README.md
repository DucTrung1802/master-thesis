# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:40:02+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_ssb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 1,339 rows x 105 columns
- **range** 2021-03-24 to 2026-08-07 (1339 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 1,329 labelled, 10 unlabelled
- mean +0.00309, sd 0.05717, range -0.2242 to +0.4215

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
| `dev_samples` | 1310 |
| `design_columns` | 501 |

## Result

- **60 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_ret_skew_63`, `foreign_sell_value`, `drv_ret_1d`, `drv_foreign_net_value_ratio`, `n_buy_orders`, `drv_foreign_own_chg_21`, `foreign_room_left`, `drv_gap_open_pct`, `drv_foreign_flow_ratio_21`, `drv_avg_order_size`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.2133 | +0.1597 |
| `ic_trend_per_fold` | +0.0471 | +0.0046 |
| `hit_rate` | +0.5148 | +0.5149 |
| `n_eff_per_fold` | 15.6 | 15.6 |

## The bar

- observed **+0.2133** against a p95 bar of **+0.1233** (10 draws) — **CLEARS**
- null mean -0.0023, sd 0.0831, max +0.1273; z = **+2.60**, p = 0.0909

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
