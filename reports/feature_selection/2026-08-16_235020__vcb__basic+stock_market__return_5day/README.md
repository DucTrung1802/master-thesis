# Feature-importance run — `return_5day`

*Generated 2026-08-16T23:50:28+07:00 at commit `2be85cff+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__stock_market`, `pool__targets`
- **panel** 4,266 rows x 264 columns
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
| `design_columns` | 1415 |

## Result

- **125 of 237 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_ret_kurt_63`, `upcom__upcom_index__volume_negotiated`, `upcom__upcom_index__avg_vol_per_buy_order`, `upcom__upcom_index__value_negotiated`, `drv_clv`, `hnx__hnx30_index__avg_vol_per_buy_order`, `drv_order_vol_imb_5`, `drv_foreign_flow_ratio_5`, `drv_close_vs_vwap`, `drv_close_pos_252`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0386 | +0.0387 |
| `ic_trend_per_fold` | -0.0001 | -0.0090 |
| `hit_rate` | +0.4949 | +0.4965 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **+0.0386** against a p95 bar of **+0.0631** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0244, sd 0.0295, max +0.0672; z = **+0.48**, p = 0.3636

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

LAYER 1, 2026-08-16: return_5day sweep. pool__basic + pool__stock_market. 10-draw null.
