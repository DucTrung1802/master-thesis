# Feature-importance run — `up_5pct_5day`

*Generated 2026-09-17T00:51:35+07:00 at commit `f1486a41+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic_bank`, `pool__targets`
- **panel** 4,276 rows x 554 columns
- **range** 2009-06-30 to 2026-08-21 (4276 sessions, 1 tickers)

## Target

- **`up_5pct_5day`** — up_5pct_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 4,271 labelled, 5 unlabelled
- mean +0.11566, sd 0.31986, range +0.0000 to +1.0000

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
| `holdout_start` | 2021-06-22 |
| `dev_samples` | 2946 |
| `design_columns` | 2624 |

## Result

- **232 of 443 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `hose__vpb__foreign_own`, `hose__vcb__foreign_own`, `hose__vcb__foreign_buy_volume`, `hose__stb__low`, `hose__mbb__close_raw`, `hose__mbb__foreign_room_left`, `hose__vcb__foreign_room_left`, `hose__hdb__foreign_sell_volume`, `hose__acb__foreign_room_left`, `hose__shb__close_adjust`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0784 | +0.1021 |
| `ic_trend_per_fold` | -0.0167 | -0.0255 |
| `hit_rate` | — | — |
| `n_eff_per_fold` | 96.8 | 96.8 |

## The bar

- observed **+0.0784** against a p95 bar of **+0.0596** (10 draws) — **CLEARS**
- null mean +0.0137, sd 0.0413, max +0.0636; z = **+1.57**, p = 0.0909

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | -0.0134 | — |
| selected | shuffled control | +0.0146 | — |
| all channels | real | +0.0064 | — |
| all channels | shuffled control | -0.0211 | — |

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

event_chain: 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions
