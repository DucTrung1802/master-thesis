# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:09:11+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_msn` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,146 rows x 105 columns
- **range** 2009-11-05 to 2026-06-26 (4146 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,136 labelled, 10 unlabelled
- mean +0.00562, sd 0.07408, range -0.3385 to +0.4806

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
| `dev_samples` | 4117 |
| `design_columns` | 504 |

## Result

- **59 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_gap_open_pct`, `drv_clv`, `drv_foreign_flow_ratio_5`, `low`, `drv_dist_from_high_63`, `open`, `drv_rogers_satchell_5`, `drv_log_order_size_ratio`, `drv_rogers_satchell_21`, `avg_vol_per_buy_order`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0538 | +0.0402 |
| `ic_trend_per_fold` | +0.0009 | -0.0004 |
| `hit_rate` | +0.4916 | +0.4944 |
| `n_eff_per_fold` | 71.7 | 71.7 |

## The bar

- observed **+0.0538** against a p95 bar of **+0.0458** (10 draws) — **CLEARS**
- null mean +0.0013, sd 0.0347, max +0.0475; z = **+1.51**, p = 0.0909

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
