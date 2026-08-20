# Feature-importance run — `return_10day`

*Generated 2026-08-20T10:37:21+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_bvh` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,239 rows x 105 columns
- **range** 2009-06-25 to 2026-06-25 (4239 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,229 labelled, 10 unlabelled
- mean +0.00556, sd 0.08911, range -0.3704 to +0.6097

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
| `dev_samples` | 4210 |
| `design_columns` | 501 |

## Result

- **60 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_adjust`, `drv_downside_vol_21`, `avg_vol_per_buy_order`, `drv_amihud_21`, `drv_body_pct`, `drv_gap_open_pct`, `drv_close_pos_252`, `drv_order_vol_imb_21`, `high`, `drv_amihud_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0501 | +0.0603 |
| `ic_trend_per_fold` | +0.0095 | -0.0058 |
| `hit_rate` | +0.4985 | +0.5010 |
| `n_eff_per_fold` | 73.6 | 73.6 |

## The bar

- observed **+0.0501** against a p95 bar of **+0.0682** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0202, sd 0.0319, max +0.0758; z = **+0.94**, p = 0.2727

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
