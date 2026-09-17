# Feature-importance run — `up_5pct_5day`

*Generated 2026-09-17T10:33:14+07:00 at commit `e201de6d+dirty`.*

## Input

- **schema** `unified_schema_mbb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 3,691 rows x 107 columns
- **range** 2011-11-01 to 2026-08-21 (3691 sessions, 1 tickers)

## Target

- **`up_5pct_5day`** — up_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 3,686 labelled, 5 unlabelled
- mean +0.10635, sd 0.30832, range +0.0000 to +1.0000

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 1 |
| `horizon_h` | 5 |
| `normalize` | none |
| `purge_gap_rows` | 5 |
| `window_stats` | last |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | 2022-03-08 |
| `dev_samples` | 2575 |
| `design_columns` | 78 |

## Result

- **52 of 78 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_realized_vol_10`, `drv_vol_ratio_10_63`, `drv_dist_from_low_63`, `drv_garman_klass_21`, `drv_parkinson_21`, `drv_rogers_satchell_21`, `drv_close_pos_252`, `drv_garman_klass_5`, `drv_close_z_63`, `close_adjust`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.0041 | +0.0048 |
| `ic_trend_per_fold` | +0.0064 | +0.0047 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 82.8 | 82.8 |

## The bar

- observed **-0.0041** against a p95 bar of **+0.0526** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0018, sd 0.0322, max +0.0720; z = **-0.18**, p = 0.6364

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.0637 | — |
| selected | shuffled control | +0.0520 | — |
| all channels | real | +0.0576 | — |
| all channels | shuffled control | +0.0144 | — |

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `holdout.csv`
- `null_draws.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `figures/10_null.png`
- `metadata.json`

## Notes

event_chain: 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions
