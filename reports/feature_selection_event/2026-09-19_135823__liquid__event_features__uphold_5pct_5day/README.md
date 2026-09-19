# Feature-importance run — `uphold_5pct_5day`

*Generated 2026-09-19T13:58:27+07:00 at commit `2ead7719+dirty`.*

## Input

- **schema** `unified_schema_liquid` (database `database_main_v2`)
- **tables** `pool__event_features`, `pool__targets`
- **panel** 710,683 rows x 89 columns
- **range** 2009-01-02 to 2026-08-21 (4398 sessions, 228 tickers)

## Target

- **`uphold_5pct_5day`** — uphold_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 709,537 labelled, 1,146 unlabelled
- mean +0.15865, sd 0.36535, range +0.0000 to +1.0000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 10 |
| `horizon_h` | 5 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 14 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | 2021-05-04 |
| `dev_samples` | 416182 |
| `design_columns` | 324 |

## Result

- **58 of 67 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `har_lrv_22`, `evt_range_h`, `har_lrv_63`, `evt_rate_up_120`, `evt_dist_low_20`, `evt_sessions_since_up`, `evt_rate_up_60`, `har_lpk_10`, `px_pos_250`, `har_labs_ret_h`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1232 | +0.1262 |
| `ic_trend_per_fold` | +0.0217 | +0.0193 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 101.4 | 101.4 |

## The bar

- observed **+0.1232** against a p95 bar of **+0.0439** (10 draws) — **CLEARS**
- null mean +0.0399, sd 0.0031, max +0.0455; z = **+26.99**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.1392 | — |
| selected | shuffled control | -0.0143 | — |
| all channels | real | +0.1398 | — |
| all channels | shuffled control | +0.0145 | — |

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
- `figures/05_stat_profile.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `figures/10_null.png`
- `metadata.json`

## Notes

event_chain: 1 when close_adjust[t+5] >= (1 + 5%) x open_adjust[t+1] — bought at the OPEN of t+1 and sold at the close of t+5, 5 sessions held — else 0; NULL for the last 5 sessions
