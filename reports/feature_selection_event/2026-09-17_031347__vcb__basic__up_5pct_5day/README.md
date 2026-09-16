# Feature-importance run — `up_5pct_5day`

*Generated 2026-09-17T03:13:53+07:00 at commit `4552148b+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,276 rows x 107 columns
- **range** 2009-06-30 to 2026-08-21 (4276 sessions, 1 tickers)

## Target

- **`up_5pct_5day`** — up_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 4,271 labelled, 5 unlabelled
- mean +0.11566, sd 0.31986, range +0.0000 to +1.0000

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
| `holdout_start` | 2021-06-22 |
| `dev_samples` | 2984 |
| `design_columns` | 78 |

## Result

- **55 of 78 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_rogers_satchell_21`, `drv_downside_vol_21`, `drv_garman_klass_21`, `drv_realized_vol_10`, `drv_parkinson_21`, `drv_foreign_flow_ratio_5`, `drv_vol_ratio_10_63`, `drv_order_vol_imb_21`, `drv_foreign_own_chg_5`, `drv_foreign_flow_ratio_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0839 | +0.0675 |
| `ic_trend_per_fold` | +0.0472 | +0.0381 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 99.0 | 99.0 |

## The bar

- observed **+0.0839** against a p95 bar of **+0.0386** (10 draws) — **CLEARS**
- null mean -0.0013, sd 0.0281, max +0.0456; z = **+3.03**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | -0.0489 | — |
| selected | shuffled control | -0.0631 | — |
| all channels | real | -0.0313 | — |
| all channels | shuffled control | -0.0853 | — |

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
