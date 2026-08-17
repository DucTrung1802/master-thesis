# Feature-importance run — `cs_rank_20day`

*Generated 2026-08-18T06:51:51+07:00 at commit `5201c0ec+dirty`.*

## Input

- **schema** `unified_schema_all` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 624,448 rows x 104 columns
- **range** 2009-01-02 to 2026-08-07 (4388 sessions, 150 tickers)

## Target

- **`cs_rank_20day`** — cs_rank_20day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **20** sessions; 621,448 labelled, 3,000 unlabelled
- mean +0.00000, sd 0.29069, range -0.5000 to +0.5000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 20 |
| `horizon_h` | 20 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 39 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 618598 |
| `design_columns` | 540 |

## Result

- **61 of 90 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_dist_from_high_252`, `drv_log_order_size_ratio`, `drv_order_vol_imb_5`, `drv_realized_vol_63`, `drv_parkinson_5`, `drv_clv`, `drv_vwap_raw`, `drv_rogers_satchell_5`, `drv_range_hl_pct`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1075 | +0.1075 |
| `ic_trend_per_fold` | +0.0054 | +0.0027 |
| `hit_rate` | +0.5363 | +0.5361 |
| `n_eff_per_fold` | 38.1 | 38.1 |

## The bar

- ⚠️ **NO NULL WAS COMPUTED FOR THIS RUN.** A positive IC is not a result on its own — see `CONTEXT.md` §6b and §8. Treat the ranking below as descriptive until a shuffled-label null has been run for *this* configuration.

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/05_stat_profile.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `metadata.json`

## Notes

TODO P2-1 v2: cs_rank_20day over the top 150 by pre-2014 median turnover, on a Kaggle T4. 150 and not 300 because the top-300 design wants ~39 GB of host RAM against the box's ~29-30 (MEM-1/P3-2): n_eff stays 218 since the dates are unchanged, and the daily-IC sd goes ~0.058 to ~0.082, so z scales by ~0.71 and the universe is still above section 2b's ~100-name threshold.
