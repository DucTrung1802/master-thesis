# Feature-importance run — `return_5day`

*Generated 2026-08-16T23:15:34+07:00 at commit `2be85cff+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__market_breadth`, `pool__targets`
- **panel** 4,266 rows x 110 columns
- **range** 2009-06-30 to 2026-08-07 (4266 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,261 labelled, 5 unlabelled
- mean +0.00315, sd 0.04303, range -0.1876 to +0.2914

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
| `design_columns` | 552 |

## Result

- **64 of 92 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_amihud_63`, `drv_ret_kurt_63`, `volume_negotiated`, `drv_close_vs_vwap`, `mkt_xs_disp5`, `drv_close_pos_252`, `drv_dist_from_low_63`, `mkt_n_names`, `drv_close_pos_63`, `drv_clv`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0196 | +0.0441 |
| `ic_trend_per_fold` | -0.0006 | +0.0107 |
| `hit_rate` | +0.4841 | +0.4938 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **+0.0196** against a p95 bar of **+0.0645** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0259, sd 0.0282, max +0.0745; z = **-0.22**, p = 0.7273

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

LAYER 1, 2026-08-16: return_5day sweep. pool__basic + pool__market_breadth. 10-draw null.
