# Feature-importance run — `close_adjust_5day`

*Generated 2026-08-12T18:47:12.117131+00:00 at commit `a4dd2306+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__economy_germany`, `pool__targets`
- **panel** 4,266 rows x 172 columns
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
| `design_columns` | 966 |

## Result

- **90 of 161 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `germany__economy__consumer__economics__depsc`, `germany__economy__prices__economics__decpi`, `germany__economy__gdp__economics__degdppa`, `germany__economy__prices__economics__deccp`, `germany__economy__housing__economics__dehpi`, `germany__economy__money__economics__dem2`, `germany__economy__money__economics__decbbs`, `germany__economy__gdp__fred__cpmnacscab1gqde`, `germany__economy__labor__economics__delc`, `germany__economy__money__economics__dem3`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.2005 | -0.1761 |
| `ic_trend_per_fold` | +0.0265 | +0.0162 |
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

germany macro pool ONLY (no pool__basic); forward PRICE LEVEL target; no null - evidence=no_null
