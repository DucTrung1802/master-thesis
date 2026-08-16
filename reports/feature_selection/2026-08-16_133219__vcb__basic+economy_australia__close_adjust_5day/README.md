# Feature-importance run — `close_adjust_5day`

*Generated 2026-08-16T13:32:22+07:00 at commit `3bb4492f+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__economy_australia`, `pool__targets`
- **panel** 4,266 rows x 262 columns
- **range** 2009-06-30 to 2026-08-07 (4266 sessions, 1 tickers)

## Target

- **`close_adjust_5day`** — close_adjust_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 4,261 labelled, 5 unlabelled
- mean +27692.29054, sd 20542.14818, range +4400.0000 to +76000.0000

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
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4242 |
| `design_columns` | 1440 |

## Result

- **162 of 240 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `australia__economy__business__economics__aucar`, `australia__economy__labor__economics__auemp`, `australia__economy__money__economics__aucbbs`, `australia__economy__money__economics__aufer`, `close_adjust`, `close_raw`, `australia__economy__housing__economics__auhs`, `drv_amihud_21`, `value_matched`, `australia__economy__prices__economics__auppi`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1150 | +0.1070 |
| `ic_trend_per_fold` | -0.0119 | -0.0424 |
| `hit_rate` | +1.0000 | +1.0000 |
| `n_eff_per_fold` | 148.6 | 148.6 |

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

layer-1 re-run 2026-08-16: three rankers on the rebuilt pool__basic (OUT-1 fixed, 58 drv_* channels), for the pool__shortlist__close_adjust_5day__d20_h5 union
