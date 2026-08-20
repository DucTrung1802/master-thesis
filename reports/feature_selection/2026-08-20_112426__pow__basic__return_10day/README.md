# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:24:29+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_pow` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,062 rows x 105 columns
- **range** 2018-03-06 to 2026-06-26 (2062 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,052 labelled, 10 unlabelled
- mean +0.00257, sd 0.06946, range -0.2397 to +0.3134

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
| `dev_samples` | 2033 |
| `design_columns` | 502 |

## Result

- **57 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_raw`, `drv_vwap_raw`, `low`, `drv_ret_skew_63`, `drv_order_count_imb_z21`, `drv_amihud_63`, `drv_order_vol_imb_5`, `close_adjust`, `drv_order_count_imb`, `drv_downside_vol_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0759 | +0.0018 |
| `ic_trend_per_fold` | -0.0154 | -0.0146 |
| `hit_rate` | +0.4981 | +0.4602 |
| `n_eff_per_fold` | 30.0 | 30.0 |

## The bar

- observed **+0.0759** against a p95 bar of **+0.0882** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0083, sd 0.0558, max +0.0889; z = **+1.21**, p = 0.2727

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
