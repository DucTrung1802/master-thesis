# Feature-importance run — `return_5day`

*Generated 2026-08-17T00:04:18+07:00 at commit `2be85cff+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__fa`, `pool__targets`
- **panel** 4,235 rows x 295 columns
- **range** 2009-06-30 to 2026-06-25 (4235 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,235 labelled, 0 unlabelled
- mean +0.00320, sd 0.04292, range -0.1876 to +0.2914

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
| `dev_samples` | 4216 |
| `design_columns` | 1452 |

## Result

- **137 of 245 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `income_statement_n4_chi_phi_hoat_dong_dich_vu`, `drv_amihud_63`, `drv_ret_kurt_63`, `balance_sheet_ix_2_von_gop_lien_doanh`, `drv_foreign_flow_ratio_5`, `drv_close_vs_vwap`, `drv_close_pos_63`, `drv_clv`, `drv_close_pos_252`, `drv_foreign_own_chg_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0564 | +0.0490 |
| `ic_trend_per_fold` | -0.0199 | -0.0087 |
| `hit_rate` | +0.4967 | +0.4943 |
| `n_eff_per_fold` | 147.6 | 147.6 |

## The bar

- observed **+0.0564** against a p95 bar of **+0.0592** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0069, sd 0.0400, max +0.0794; z = **+1.24**, p = 0.1818

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

LAYER 1, 2026-08-16: return_5day sweep. pool__basic + pool__fa. 10-draw null.
