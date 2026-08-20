# Feature-importance run — `return_10day`

*Generated 2026-08-20T12:00:38+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vhm` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,023 rows x 105 columns
- **range** 2018-05-17 to 2026-06-26 (2023 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,013 labelled, 10 unlabelled
- mean +0.00687, sd 0.07688, range -0.2771 to +0.3973

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
| `dev_samples` | 1994 |
| `design_columns` | 508 |

## Result

- **63 of 85 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_gap_open_pct`, `drv_prop_participation`, `drv_order_fill_ratio`, `drv_dist_from_high_63`, `drv_downside_vol_21`, `drv_order_count_imb_z21`, `drv_close_vs_vwap`, `drv_dist_from_low_21`, `drv_ret_skew_63`, `drv_ret_1d`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0134 | +0.0547 |
| `ic_trend_per_fold` | +0.0448 | +0.0503 |
| `hit_rate` | +0.4485 | +0.4717 |
| `n_eff_per_fold` | 29.3 | 29.3 |

## The bar

- observed **+0.0134** against a p95 bar of **+0.0897** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0066, sd 0.0615, max +0.1085; z = **+0.11**, p = 0.6364

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
