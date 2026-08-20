# Feature-importance run — `cs_rank_10day`

*Generated 2026-08-20T10:18:55+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vn30` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`, `pool__ta`
- **panel** 65,763 rows x 249 columns
- **range** 2017-04-21 to 2026-06-26 (2288 sessions, 30 tickers)

## Target

- **`cs_rank_10day`** — cs_rank_10day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **10** sessions; 65,463 labelled, 300 unlabelled
- mean +0.00000, sd 0.29890, range -0.5000 to +0.5000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 20 |
| `horizon_h` | 10 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 29 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 64893 |
| `design_columns` | 1410 |

## Result

- **204 of 235 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_log_order_size_ratio`, `bop_signal_14`, `drv_order_vol_imb`, `stoch_rsi_14_5_3_d_slope`, `close_sma_200_acceleration`, `willr_14_direction`, `trange_14_strength`, `close_adjust`, `close_gt_dema_200`, `close_bb_20_slope_upper`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0333 | +0.0264 |
| `ic_trend_per_fold` | +0.0120 | +0.0093 |
| `hit_rate` | +0.5112 | +0.5074 |
| `n_eff_per_fold` | 34.6 | 34.6 |

## The bar

- observed **+0.0333** against a p95 bar of **+0.0332** (10 draws) — **CLEARS**
- null mean +0.0136, sd 0.0121, max +0.0342; z = **+1.62**, p = 0.1818

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

SSK-1 wide: + pool__ta reduced 711 -> 145 label-free (booleans, coverage, pairwise-MA, |rho|>=0.70, and 7 measured duplicates of pool__basic incl. an EXACT copy of value_matched).
