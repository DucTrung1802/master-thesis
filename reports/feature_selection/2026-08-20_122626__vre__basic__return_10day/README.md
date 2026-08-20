# Feature-importance run — `return_10day`

*Generated 2026-08-20T12:26:29+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vre` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,150 rows x 105 columns
- **range** 2017-11-06 to 2026-06-26 (2150 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,140 labelled, 10 unlabelled
- mean +0.00231, sd 0.07841, range -0.3295 to +0.4333

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
| `dev_samples` | 2121 |
| `design_columns` | 510 |

## Result

- **59 of 85 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_adjust`, `drv_dist_from_high_252`, `drv_ret_skew_63`, `drv_ret_1d`, `drv_amihud_21`, `drv_dist_from_low_63`, `prop_sell_vol`, `prop_sell_val`, `drv_close_z_21`, `drv_order_vol_imb`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1807 | +0.2112 |
| `ic_trend_per_fold` | -0.0058 | -0.0354 |
| `hit_rate` | +0.4838 | +0.4876 |
| `n_eff_per_fold` | 31.8 | 31.8 |

## The bar

- observed **+0.1807** against a p95 bar of **+0.0869** (10 draws) — **CLEARS**
- null mean -0.0111, sd 0.0775, max +0.0945; z = **+2.48**, p = 0.0909

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
