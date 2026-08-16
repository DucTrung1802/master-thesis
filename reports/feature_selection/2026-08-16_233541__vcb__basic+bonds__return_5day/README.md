# Feature-importance run — `return_5day`

*Generated 2026-08-16T23:35:48+07:00 at commit `2be85cff+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__bonds`, `pool__targets`
- **panel** 4,266 rows x 219 columns
- **range** 2009-06-30 to 2026-08-07 (4266 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,261 labelled, 5 unlabelled
- mean +0.00315, sd 0.04303, range -0.1876 to +0.2914

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 20 |
| `horizon_h` | 5 |
| `normalize` | none |
| `purge_gap_rows` | 24 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4242 |
| `design_columns` | 1206 |

## Result

- **99 of 201 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_amihud_63`, `drv_close_vs_vwap`, `drv_foreign_flow_ratio_5`, `volume_negotiated`, `drv_order_vol_imb_5`, `drv_ret_skew_63`, `tvc__vn15y__return_log`, `drv_close_pos_63`, `drv_foreign_flow_ratio_21`, `drv_ret_kurt_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0121 | +0.0307 |
| `ic_trend_per_fold` | +0.0138 | +0.0003 |
| `hit_rate` | +0.4844 | +0.4884 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **+0.0121** against a p95 bar of **+0.0339** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0069, sd 0.0277, max +0.0373; z = **+0.19**, p = 0.5455

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

LAYER 1, 2026-08-16: return_5day sweep. pool__basic + pool__bonds. 10-draw null.
