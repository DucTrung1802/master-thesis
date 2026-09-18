# Feature-importance run — `upopen_5pct_5day`

*Generated 2026-09-18T11:53:47+07:00 at commit `c845f440+dirty`.*

## Input

- **schema** `unified_schema_liquid` (database `database_main_v2`)
- **tables** `pool__event_features`, `pool__targets`
- **panel** 710,683 rows x 85 columns
- **range** 2009-01-02 to 2026-08-21 (4398 sessions, 228 tickers)

## Target

- **`upopen_5pct_5day`** — upopen_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **6** sessions; 709,309 labelled, 1,374 unlabelled
- mean +0.17722, sd 0.38186, range +0.0000 to +1.0000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 1 |
| `horizon_h` | 6 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 6 |
| `window_stats` | last |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | 2021-04-29 |
| `dev_samples` | 419509 |
| `design_columns` | 54 |

## Result

- **59 of 67 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `har_lrv_22`, `evt_rate_up_120`, `har_lrv_63`, `evt_rate_up_250`, `har_lpk_63`, `har_lpk_10`, `evt_dist_low_20`, `har_lrv_125`, `har_lpk_125`, `evt_turnover_z_60`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1238 | +0.1228 |
| `ic_trend_per_fold` | +0.0147 | +0.0158 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 85.3 | 85.3 |

## The bar

- observed **+0.1238** against a p95 bar of **+0.0500** (10 draws) — **CLEARS**
- null mean +0.0455, sd 0.0032, max +0.0514; z = **+24.56**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.1416 | — |
| selected | shuffled control | -0.0058 | — |
| all channels | real | +0.1418 | — |
| all channels | shuffled control | -0.0127 | — |

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
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `figures/10_null.png`
- `metadata.json`

## Notes

event_chain: 1 when close_adjust[t+6] >= (1 + 5%) x open_adjust[t+1] — bought at the OPEN of t+1 and sold 5 sessions later — else 0; NULL for the last 6 sessions
