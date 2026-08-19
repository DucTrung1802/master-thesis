# Feature-importance run — `cs_rank_20day`

*Generated 2026-08-19T19:58:32+07:00 at commit `9914e620+dirty`.*

## Input

- **schema** `unified_schema_all` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`, `pool__ta`
- **panel** 624,238 rows x 134 columns
- **range** 2009-01-02 to 2026-06-26 (4358 sessions, 150 tickers)

## Target

- **`cs_rank_20day`** — cs_rank_20day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **20** sessions; 621,378 labelled, 2,860 unlabelled
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
| `dev_samples` | 618528 |
| `design_columns` | 720 |

## Result

- **90 of 120 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_log_order_size_ratio`, `drv_order_vol_imb_5`, `drv_dist_from_high_252`, `drv_realized_vol_63`, `drv_parkinson_5`, `drv_clv`, `drv_rogers_satchell_5`, `drv_range_hl_pct`, `drv_vwap_raw`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1285 | +0.1269 |
| `ic_trend_per_fold` | +0.0016 | +0.0025 |
| `hit_rate` | +0.5428 | +0.5421 |
| `n_eff_per_fold` | 38.0 | 38.0 |

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

TODO PRF-9, memory pilot ATTEMPT 2 - 90 pool__basic + 30 pool__ta = 120 channels. ATTEMPT 1 at 140 channels FAILED, and it failed on the wall nobody predicted: host RAM peaked at 24.5 GB and SURVIVED, while XGBoost died on VRAM inside XGBoosterPredictFromDMatrix - free 3.00 GB, requested 3.15 GB on a 14.6 GiB T4. The allocation is xgb_shap's SHAP contributions, (n_rows, design_cols+1), and design_cols is channels x 6 window stats - so it scales with the widening exactly. 120 channels puts that request at ~2.7 GB against a smaller torch footprint. LABEL-FREE prune as before (711 -> 405 -> 30). NO NULL: the measurement is whether it fits and whether any pool__ta channel survives selection at all.
