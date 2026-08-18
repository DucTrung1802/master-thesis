# Feature-importance run — `cs_rank_20day`

*Generated 2026-08-19T04:17:06+07:00 at commit `6a86536b+dirty`.*

## Input

- **schema** `unified_schema_all` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 273,367 rows x 104 columns
- **range** 2009-01-02 to 2016-12-30 (1995 sessions, 150 tickers)

## Target

- **`cs_rank_20day`** — cs_rank_20day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **20** sessions; 273,367 labelled, 0 unlabelled
- mean -0.00000, sd 0.29077, range -0.5000 to +0.5000

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
| `dev_samples` | 270517 |
| `design_columns` | 504 |

## Result

- **57 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_dist_from_high_252`, `drv_order_vol_imb`, `drv_dist_from_high_63`, `drv_close_pos_252`, `drv_vwap_raw`, `drv_close_pos_63`, `drv_realized_vol_63`, `drv_order_vol_imb_5`, `volume_matched`, `drv_clv`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0973 | +0.0980 |
| `ic_trend_per_fold` | +0.0244 | +0.0277 |
| `hit_rate` | +0.5315 | +0.5314 |
| `n_eff_per_fold` | 14.3 | 14.3 |

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

TODO PRF-7, the CHEAP half. Identical to the `cross-sectional` job in every respect except the DATA WINDOW: dates < 2017-01-01, i.e. only what the earliest walk-forward fold could have seen. Same universe (liquidity_before stays 2014-01-01), same target, horizon, lookback, min_width, dtype and ensemble, so the ONLY thing that moves is which dates the selection saw. The question is whether the same channels survive: the full-sample run shortlisted 13 of 90 candidates, and two random 13-subsets of 90 would share 1.9 by chance. NO NULL on purpose - the kept SET is the measurement here, not its bar, and dropping the null takes the run from ~6 h to well under one.
