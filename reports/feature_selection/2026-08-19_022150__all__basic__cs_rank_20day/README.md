# Feature-importance run — `cs_rank_20day`

*Generated 2026-08-19T09:21:55+07:00 at commit `5f652961+dirty`.*

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
| `feature_normalize` | none |
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

- **60 of 90 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_order_vol_imb_5`, `drv_dist_from_high_252`, `drv_log_order_size_ratio`, `drv_clv`, `drv_cs_pct_range`, `drv_cs_ret_vs_industry`, `drv_close_pos_252`, `drv_vwap_raw`, `drv_parkinson_5`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1215 | +0.1231 |
| `ic_trend_per_fold` | -0.0018 | -0.0037 |
| `hit_rate` | +0.5387 | +0.5401 |
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

TODO P1-6 / FNM-1. Identical to the `cross-sectional` job in every respect except FEATURE_NORMALIZE: 'cs_rank' -> 'none'. Same payload dataset (the panel is byte-identical), same universe, target, horizon, lookback, min_width, dtype and ensemble, so the only thing that moves is the FEATURE REPRESENTATION the selection scores under. The question: the selection kept its 13 channels while ranking each feature within its date before windowing, but train_test_creator feeds the model those channels GLOBALLY STANDARDISED - two different representations, and CLAUDE.md section 5 rule 1 says a bar computed for one configuration says nothing about another. If the same channels survive under 'none', the mismatch is moot and the sentence 'built on a shortlist that cleared z = +9.09' is safe; if they do not, that sentence is withdrawn. NO NULL on purpose - the kept SET is the measurement here, not its bar, which is what took PRF-7 from ~6 h to 10m 34s. feature_normalize is already in contract.SETUP_KEYS, so this run groups APART from the cs_rank runs and cannot be unioned into one table by accident.
