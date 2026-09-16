# Feature-importance run — `up_5pct_5day`

*Generated 2026-09-17T03:18:47+07:00 at commit `4552148b+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__market_breadth`, `pool__targets`
- **panel** 4,276 rows x 21 columns
- **range** 2009-06-30 to 2026-08-21 (4276 sessions, 1 tickers)

## Target

- **`up_5pct_5day`** — up_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 4,271 labelled, 5 unlabelled
- mean +0.11566, sd 0.31986, range +0.0000 to +1.0000

## Setup

| knob | value |
|---|---|
| `selector_class` | FeatureSelector |
| `lookback_d` | 1 |
| `horizon_h` | 5 |
| `normalize` | none |
| `purge_gap_rows` | 5 |
| `window_stats` | last |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | 2021-06-22 |
| `dev_samples` | 2984 |
| `design_columns` | 7 |

## Result

- **7 of 7 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `mkt_xs_kurt5`, `mkt_log_turnover`, `mkt_xs_disp5`, `mkt_xs_skew5`, `mkt_hhi_turnover`, `mkt_turnover_z`, `mkt_xs_mean5`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0553 | +0.0304 |
| `ic_trend_per_fold` | +0.0256 | +0.0247 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 99.0 | 99.0 |

## The bar

- observed **+0.0553** against a p95 bar of **+0.0259** (10 draws) — **CLEARS**
- null mean -0.0057, sd 0.0226, max +0.0334; z = **+2.69**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.0161 | — |
| selected | shuffled control | -0.0486 | — |
| all channels | real | +0.0161 | — |
| all channels | shuffled control | -0.0614 | — |

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `holdout.csv`
- `null_draws.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/09_target_distribution.png`
- `figures/10_null.png`
- `metadata.json`

## Notes

event_chain: 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions
