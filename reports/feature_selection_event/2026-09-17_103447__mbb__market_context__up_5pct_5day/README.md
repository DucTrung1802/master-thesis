# Feature-importance run — `up_5pct_5day`

*Generated 2026-09-17T10:34:50+07:00 at commit `e201de6d+dirty`.*

## Input

- **schema** `unified_schema_mbb` (database `database_main_v2`)
- **tables** `pool__market_context`, `pool__targets`
- **panel** 3,691 rows x 73 columns
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
| `design_columns` | 59 |

## Result

- **34 of 59 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `mctx_vn30_ma_gap_200`, `glb_vix`, `mctx_vn30_lrv_22`, `mctx_vn30_fnet_20`, `mctx_vn30_lrv_5`, `mctx_vn30_pos_250`, `mctx_vn_fnet_20`, `glb_djia_ret_5`, `mctx_vn_ret_20`, `mctx_vn_lrv_63`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1474 | +0.1413 |
| `ic_trend_per_fold` | +0.0126 | -0.0007 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 82.8 | 82.8 |

## The bar

- observed **+0.1474** against a p95 bar of **+0.0307** (10 draws) — **CLEARS**
- null mean +0.0083, sd 0.0177, max +0.0354; z = **+7.86**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.0969 | — |
| selected | shuffled control | +0.0334 | — |
| all channels | real | +0.0836 | — |
| all channels | shuffled control | +0.1092 | — |

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
