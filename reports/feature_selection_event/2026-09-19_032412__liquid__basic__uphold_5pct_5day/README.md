# Feature-importance run — `uphold_5pct_5day`

*Generated 2026-09-19T03:24:16+07:00 at commit `1593f663+dirty`.*

## Input

- **schema** `unified_schema_liquid` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 710,683 rows x 120 columns
- **range** 2009-01-02 to 2026-08-21 (4398 sessions, 228 tickers)

## Target

- **`uphold_5pct_5day`** — uphold_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 709,537 labelled, 1,146 unlabelled
- mean +0.15865, sd 0.36535, range +0.0000 to +1.0000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 1 |
| `horizon_h` | 5 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 5 |
| `window_stats` | last |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | 2021-05-04 |
| `dev_samples` | 419929 |
| `design_columns` | 84 |

## Result

- **56 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_realized_vol_63`, `drv_parkinson_21`, `drv_realized_vol_10`, `drv_dist_from_low_63`, `drv_dist_from_low_21`, `drv_no_trade_days_21`, `drv_ret_kurt_63`, `close_adjust`, `drv_order_vol_imb`, `drv_close_pos_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1325 | +0.1319 |
| `ic_trend_per_fold` | +0.0161 | +0.0153 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 102.6 | 102.6 |

## The bar

- observed **+0.1325** against a p95 bar of **+0.0455** (10 draws) — **CLEARS**
- null mean +0.0385, sd 0.0064, max +0.0476; z = **+14.59**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.1337 | — |
| selected | shuffled control | +0.0130 | — |
| all channels | real | +0.1309 | — |
| all channels | shuffled control | +0.0136 | — |

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

event_chain: 1 when close_adjust[t+5] >= (1 + 5%) x open_adjust[t+1] — bought at the OPEN of t+1 and sold at the close of t+5, 5 sessions held — else 0; NULL for the last 5 sessions
