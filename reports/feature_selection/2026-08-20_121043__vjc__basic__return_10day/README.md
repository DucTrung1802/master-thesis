# Feature-importance run — `return_10day`

*Generated 2026-08-20T12:10:46+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vjc` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,325 rows x 105 columns
- **range** 2017-02-28 to 2026-06-26 (2325 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,315 labelled, 10 unlabelled
- mean +0.00497, sd 0.06359, range -0.2505 to +0.3824

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
| `dev_samples` | 2296 |
| `design_columns` | 501 |

## Result

- **59 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_adjust`, `avg_vol_per_sell_order`, `drv_realized_vol_10`, `drv_vol_ratio_10_63`, `drv_value_z_21`, `drv_body_pct`, `buy_order_vol`, `close_raw`, `drv_close_pos_63`, `drv_close_z_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1625 | +0.1118 |
| `ic_trend_per_fold` | -0.0067 | -0.0174 |
| `hit_rate` | +0.4991 | +0.5105 |
| `n_eff_per_fold` | 35.3 | 35.3 |

## The bar

- observed **+0.1625** against a p95 bar of **+0.0436** (10 draws) — **CLEARS**
- null mean -0.0272, sd 0.0516, max +0.0666; z = **+3.67**, p = 0.0909

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
