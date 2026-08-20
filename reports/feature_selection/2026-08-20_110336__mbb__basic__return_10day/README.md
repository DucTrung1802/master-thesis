# Feature-importance run — `return_10day`

*Generated 2026-08-20T11:03:39+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_mbb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 3,681 rows x 105 columns
- **range** 2011-11-01 to 2026-08-07 (3681 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 3,671 labelled, 10 unlabelled
- mean +0.00900, sd 0.05676, range -0.2280 to +0.3862

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
| `dev_samples` | 3652 |
| `design_columns` | 504 |

## Result

- **55 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `volume_negotiated`, `drv_clv`, `avg_vol_per_buy_order`, `drv_garman_klass_21`, `drv_dist_from_high_252`, `value_negotiated`, `drv_log_order_size_ratio`, `drv_rogers_satchell_5`, `drv_realized_vol_63`, `drv_ret_kurt_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0326 | +0.0495 |
| `ic_trend_per_fold` | +0.0234 | +0.0108 |
| `hit_rate` | +0.5020 | +0.5017 |
| `n_eff_per_fold` | 62.4 | 62.4 |

## The bar

- observed **+0.0326** against a p95 bar of **+0.0586** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0059, sd 0.0416, max +0.0703; z = **+0.64**, p = 0.3636

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
