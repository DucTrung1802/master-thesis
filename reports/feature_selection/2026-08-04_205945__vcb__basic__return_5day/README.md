# Feature-importance run — `return_5day`

*Generated 2026-08-09T16:13:55.551392+00:00 at commit `93fcb967+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,235 rows x 42 columns
- **range** 2009-06-30 to 2026-06-25 (4235 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,230 labelled, 5 unlabelled
- mean +0.00319, sd 0.04295, range -0.1876 to +0.2914

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
| `max_features` | 12 |
| `corr_threshold` | 0.9 |
| `device` | cpu |
| `random_state` | 42 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4211 |
| `design_columns` | 161 |

## Result

- **12 of 27 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `volume_negotiated`, `foreign_own`, `close_adjust`, `open`, `buy_order_vol`, `close_raw`, `foreign_room_left`, `low`, `high`, `n_sell_orders`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0636 | +0.0427 |
| `ic_trend_per_fold` | -0.0070 | -0.0370 |
| `hit_rate` | +0.4920 | +0.4790 |
| `n_eff_per_fold` | 147.4 | 147.4 |

## The bar

- observed **+0.0636** against a p95 bar of **+0.0838** (20 draws) — **DOES NOT CLEAR**
- null mean +0.0041, sd 0.0407, max +0.1026; z = **+1.46**, p = 0.0952

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

Null added 2026-08-09 (issue EVD-1). The 2026-08-04 run recorded no bar; the selection was reproduced bit-identically and the 20-draw block-shuffled null it never had was computed at the same knobs.
