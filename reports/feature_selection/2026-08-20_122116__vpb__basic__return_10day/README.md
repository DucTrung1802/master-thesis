# Feature-importance run — `return_10day`

*Generated 2026-08-20T12:21:19+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vpb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,236 rows x 105 columns
- **range** 2017-08-17 to 2026-08-07 (2236 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,226 labelled, 10 unlabelled
- mean +0.00825, sd 0.07486, range -0.2530 to +0.4262

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
| `dev_samples` | 2207 |
| `design_columns` | 504 |

## Result

- **59 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_ret_kurt_63`, `drv_order_vol_imb_21`, `drv_log_order_size_ratio`, `drv_order_fill_ratio`, `drv_close_pos_21`, `drv_amihud_63`, `avg_vol_per_sell_order`, `drv_rogers_satchell_21`, `drv_order_count_imb`, `drv_dist_from_low_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0420 | +0.0144 |
| `ic_trend_per_fold` | +0.1666 | +0.1244 |
| `hit_rate` | +0.5017 | +0.4921 |
| `n_eff_per_fold` | 33.5 | 33.5 |

## The bar

- observed **+0.0420** against a p95 bar of **+0.0468** (10 draws) — **DOES NOT CLEAR**
- null mean -0.0224, sd 0.0588, max +0.0553; z = **+1.09**, p = 0.1818

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
