# Feature-importance run — `return_10day`

*Generated 2026-08-19T22:13:11+07:00 at commit `178f1beb+dirty`.*

## Input

- **schema** `unified_schema_vic` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,393 rows x 105 columns
- **range** 2009-01-02 to 2026-08-19 (4393 sessions, 1 tickers)

## Target

- **`return_10day`** — forward 10-session return on close_adjust
- horizon **10** sessions; 4,383 labelled, 10 unlabelled
- mean +0.01369, sd 0.08283, range -0.3714 to +0.4724

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 20 |
| `horizon_h` | 10 |
| `normalize` | none |
| `purge_gap_rows` | 29 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4364 |
| `design_columns` | 504 |

## Result

- **60 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_ret_skew_63`, `foreign_own`, `drv_dist_from_low_21`, `drv_dist_from_high_63`, `drv_downside_vol_21`, `drv_avg_order_size`, `avg_vol_per_buy_order`, `drv_ret_kurt_63`, `drv_parkinson_21`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.0214 | -0.0432 |
| `ic_trend_per_fold` | -0.0458 | -0.0554 |
| `hit_rate` | +0.4618 | +0.4589 |
| `n_eff_per_fold` | 76.7 | 76.7 |

## The bar

- observed **-0.0214** against a p95 bar of **+0.0785** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0150, sd 0.0525, max +0.0792; z = **-0.69**, p = 0.7273

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

single-stock h=10 track, 2026-08-19; pool__basic only (gold.stocks_ta 37 sessions behind, STA-1)
