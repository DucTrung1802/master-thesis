# Feature-importance run — `return_5day`

*Generated 2026-08-17T01:00:55+07:00 at commit `2be85cff+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__ta`, `pool__targets`
- **panel** 4,236 rows x 1020 columns
- **range** 2009-06-30 to 2026-06-26 (4236 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,236 labelled, 0 unlabelled
- mean +0.00321, sd 0.04292, range -0.1876 to +0.2914

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
| `dev_samples` | 4217 |
| `design_columns` | 4733 |

## Result

- **473 of 794 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_ema_50_200_dist`, `close_midpoint_50_slope`, `close_kama_100_200_dist`, `ht_sine_signal_10`, `ht_dcphase`, `close_sma_100_slope`, `ultosc_7_14_28_hist_abs`, `midprice_50_slope`, `close_tema_30_dist_abs`, `close_ema_200_slope`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0434 | +0.0279 |
| `ic_trend_per_fold` | +0.0150 | +0.0058 |
| `hit_rate` | +0.5064 | +0.4882 |
| `n_eff_per_fold` | 147.6 | 147.6 |

## The bar

- observed **+0.0434** against a p95 bar of **+0.0603** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0244, sd 0.0242, max +0.0673; z = **+0.78**, p = 0.3636

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

LAYER 1, 2026-08-16: return_5day sweep. pool__basic + pool__ta. 10-draw null.
