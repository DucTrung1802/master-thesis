# Feature-importance run — `close_adjust_5day`

*Generated 2026-08-16T14:43:20+07:00 at commit `3bb4492f+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__shortlist__close_adjust_5day__d20_h5`, `pool__targets`
- **panel** 4,266 rows x 653 columns
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
| `design_columns` | 3864 |

## Result

- **522 of 644 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `russia__economy__money__economics__rulps`, `taiwan_china__economy__government__economics__twgsp`, `vietnam__economy__trade__economics__vnexp`, `netherlands__economy__money__economics__nlm0`, `india__economy__money__economics__infer`, `united_kingdom__economy__money__economics__gbm0`, `close_raw`, `japan__economy__money__fred__nikkei225`, `australia__economy__money__economics__aucbbs`, `indonesia__economy__money__economics__idfer`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.0608 | +0.0183 |
| `ic_trend_per_fold` | -0.0017 | -0.0518 |
| `hit_rate` | +1.0000 | +1.0000 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **-0.0608** against a p95 bar of **+0.0853** (10 draws) — **DOES NOT CLEAR**
- null mean -0.0123, sd 0.0792, max +0.0867; z = **-0.61**, p = 0.7273

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

LAYER 2, 2026-08-16: the 644 survivors of the 20 layer-1 runs compete in one pool. 10-draw null.
