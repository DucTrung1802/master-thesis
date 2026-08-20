# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:49:56+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_tpb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,068 rows x 105 columns
- **range** 2018-04-19 to 2026-08-07 (2068 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,058 labelled, 10 unlabelled
- mean +0.00447, sd 0.06260, range -0.2399 to +0.2836

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
| `dev_samples` | 2039 |
| `design_columns` | 502 |

## Result

- **57 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_dist_from_low_63`, `drv_downside_vol_21`, `drv_amihud_63`, `drv_dist_from_high_21`, `drv_dist_from_low_21`, `close_adjust`, `drv_close_z_21`, `drv_upper_shadow`, `drv_ret_skew_63`, `close_raw`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0071 | +0.0358 |
| `ic_trend_per_fold` | +0.0126 | +0.0362 |
| `hit_rate` | +0.4815 | +0.4868 |
| `n_eff_per_fold` | 30.2 | 30.2 |

## The bar

- observed **+0.0071** against a p95 bar of **+0.1058** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0317, sd 0.0591, max +0.1304; z = **-0.42**, p = 0.7273

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
