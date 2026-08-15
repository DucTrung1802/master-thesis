# Feature-importance run — `close_adjust_5day`

*Generated 2026-08-15T05:26:09.891197+00:00 at commit `5b054227+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,266 rows x 32 columns
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
| `design_columns` | 90 |

## Result

- **7 of 15 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_adjust`, `low`, `close_raw`, `n_sell_orders`, `value_matched`, `high`, `sell_order_vol`, `open`, `avg_vol_per_buy_order`, `n_buy_orders`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.2358 | +0.1040 |
| `ic_trend_per_fold` | -0.1474 | -0.1009 |
| `hit_rate` | +1.0000 | +1.0000 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **+0.2358** against a p95 bar of **+0.0806** (10 draws) — **CLEARS**
- null mean +0.0130, sd 0.0487, max +0.1020; z = **+4.58**, p = 0.0909

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

run on a Kaggle T4 via kgpu; data shipped as a parquet payload
