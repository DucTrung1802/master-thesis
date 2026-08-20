# Feature-importance run — `return_10day`

*Generated 2026-08-20T10:42:46+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_ctg` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,255 rows x 105 columns
- **range** 2009-07-16 to 2026-08-07 (4255 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,245 labelled, 10 unlabelled
- mean +0.00550, sd 0.06472, range -0.2525 to +0.3354

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
| `dev_samples` | 4226 |
| `design_columns` | 504 |

## Result

- **56 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `foreign_own`, `drv_foreign_own_chg_21`, `drv_garman_klass_21`, `drv_ret_1d`, `drv_order_vol_imb_5`, `drv_order_vol_imb`, `drv_ret_skew_63`, `drv_close_pos_252`, `drv_avg_order_size`, `drv_dist_from_low_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0916 | +0.0966 |
| `ic_trend_per_fold` | +0.0050 | -0.0022 |
| `hit_rate` | +0.4953 | +0.4861 |
| `n_eff_per_fold` | 73.9 | 73.9 |

## The bar

- observed **+0.0916** against a p95 bar of **+0.0699** (10 draws) — **CLEARS**
- null mean -0.0192, sd 0.0534, max +0.0991; z = **+2.08**, p = 0.1818

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
