# Feature-importance run — `return_10day`

*Generated 2026-08-20T10:53:11+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_gvr` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 2,054 rows x 105 columns
- **range** 2018-03-21 to 2026-06-25 (2054 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 2,044 labelled, 10 unlabelled
- mean +0.01101, sd 0.09870, range -0.3358 to +0.4757

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
| `dev_samples` | 2025 |
| `design_columns` | 510 |

## Result

- **57 of 85 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `avg_vol_per_sell_order`, `drv_value_z_21`, `drv_amihud_63`, `drv_order_vol_imb_5`, `open`, `drv_ret_kurt_63`, `drv_foreign_own_chg_5`, `avg_vol_per_buy_order`, `drv_parkinson_21`, `drv_avg_order_size`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0492 | +0.0500 |
| `ic_trend_per_fold` | +0.0680 | +0.0532 |
| `hit_rate` | +0.4725 | +0.4619 |
| `n_eff_per_fold` | 29.9 | 29.9 |

## The bar

- observed **+0.0492** against a p95 bar of **+0.1420** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0080, sd 0.0816, max +0.1503; z = **+0.50**, p = 0.2727

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
