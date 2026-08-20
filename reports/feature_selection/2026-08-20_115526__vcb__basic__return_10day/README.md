# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:55:29+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,266 rows x 105 columns
- **range** 2009-06-30 to 2026-08-07 (4266 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,256 labelled, 10 unlabelled
- mean +0.00628, sd 0.05986, range -0.2742 to +0.3310

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
| `dev_samples` | 4237 |
| `design_columns` | 504 |

## Result

- **57 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_dist_from_low_63`, `drv_order_vol_imb_5`, `drv_ret_skew_63`, `drv_realized_vol_63`, `drv_foreign_flow_ratio_5`, `drv_foreign_participation`, `drv_amihud_63`, `drv_close_vs_vwap`, `drv_close_pos_252`, `drv_close_z_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.0020 | +0.0242 |
| `ic_trend_per_fold` | -0.0456 | -0.0242 |
| `hit_rate` | +0.4545 | +0.4591 |
| `n_eff_per_fold` | 74.1 | 74.1 |

## The bar

- observed **-0.0020** against a p95 bar of **+0.0770** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0210, sd 0.0422, max +0.0887; z = **-0.55**, p = 0.7273

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
