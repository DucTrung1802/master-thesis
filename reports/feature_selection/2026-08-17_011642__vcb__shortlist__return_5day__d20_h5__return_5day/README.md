# Feature-importance run — `return_5day`

*Generated 2026-08-17T01:16:50+07:00 at commit `2be85cff+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__shortlist__return_5day__d20_h5`, `pool__targets`
- **panel** 4,235 rows x 217 columns
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
| `design_columns` | 1246 |

## Result

- **199 of 208 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_ema_50_200_dist`, `income_statement_n4_chi_phi_hoat_dong_dich_vu`, `close_tema_30_dist_abs`, `close_midpoint_50_slope`, `upcom__upcom_index__avg_vol_per_buy_order`, `ht_dcphase`, `bop_signal_14_slope`, `aroon_down_25_slope`, `close_kama_50_100_direction`, `atr_14_signal_slope`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1369 | +0.1245 |
| `ic_trend_per_fold` | +0.0507 | +0.0490 |
| `hit_rate` | +0.5230 | +0.5168 |
| `n_eff_per_fold` | 147.6 | 147.6 |

## The bar

- observed **+0.1369** against a p95 bar of **+0.0428** (10 draws) — **CLEARS**
- null mean +0.0023, sd 0.0300, max +0.0577; z = **+4.48**, p = 0.0909

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

LAYER 2, 2026-08-17: the 208 survivors of the 6 layer-1 return_5day runs compete in one pool. 10-draw null.
