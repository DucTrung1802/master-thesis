# Feature-importance run — `return_10day`

*Generated 2026-08-20T10:48:15+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_gas` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 3,515 rows x 105 columns
- **range** 2012-05-21 to 2026-06-25 (3515 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 3,505 labelled, 10 unlabelled
- mean +0.00719, sd 0.07093, range -0.3252 to +0.4709

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
| `dev_samples` | 3486 |
| `design_columns` | 502 |

## Result

- **60 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_adjust`, `drv_avg_order_size`, `close_raw`, `drv_log_order_size_ratio`, `drv_close_vs_vwap`, `drv_ret_kurt_63`, `drv_foreign_flow_ratio_5`, `foreign_net_value`, `foreign_net_volume`, `drv_foreign_own_chg_5`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0857 | +0.0236 |
| `ic_trend_per_fold` | +0.0386 | +0.0439 |
| `hit_rate` | +0.4934 | +0.4846 |
| `n_eff_per_fold` | 59.1 | 59.1 |

## The bar

- observed **+0.0857** against a p95 bar of **+0.0734** (10 draws) — **CLEARS**
- null mean -0.0179, sd 0.0692, max +0.0829; z = **+1.50**, p = 0.0909

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
