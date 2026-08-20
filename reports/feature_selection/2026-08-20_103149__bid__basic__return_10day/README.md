# Feature-importance run — `return_10day`

*Generated 2026-08-20T10:31:53+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_bid` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 3,121 rows x 105 columns
- **range** 2014-01-24 to 2026-08-07 (3121 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 3,111 labelled, 10 unlabelled
- mean +0.00761, sd 0.06989, range -0.2935 to +0.4046

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
| `dev_samples` | 3092 |
| `design_columns` | 502 |

## Result

- **58 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `high`, `drv_negotiated_value_share`, `drv_dist_from_low_63`, `drv_close_pos_252`, `low`, `drv_rogers_satchell_5`, `close_raw`, `drv_order_count_imb_z21`, `drv_amihud_63`, `value_matched`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0303 | +0.0564 |
| `ic_trend_per_fold` | +0.0836 | +0.0925 |
| `hit_rate` | +0.5045 | +0.5017 |
| `n_eff_per_fold` | 51.2 | 51.2 |

## The bar

- observed **+0.0303** against a p95 bar of **+0.0343** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0005, sd 0.0339, max +0.0372; z = **+0.88**, p = 0.2727

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
