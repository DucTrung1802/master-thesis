# Feature-importance run — `return_5day`

*Generated 2026-08-04T16:50:10.849490+00:00 at commit `ba495b3`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__ta`, `pool__targets`
- **panel** 4,235 rows x 928 columns
- **range** 2009-06-30 to 2026-06-25 (4235 sessions, 1 tickers)

## Target

- **`return_5day`** — forward 5-session return on close_adjust
- horizon **5** sessions; 4,230 labelled, 5 unlabelled
- mean +0.00319, sd 0.04295, range -0.1876 to +0.2914

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
| `max_features` | 12 |
| `corr_threshold` | 0.9 |
| `device` | cpu |
| `random_state` | 42 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4230 |
| `design_columns` | 918 |

## Result

- **12 of 918 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_ema_50_100_dist`, `close_ema_50_200_dist`, `close_ema_100_200_dist`, `close_kama_50_200_dist`, `close_kama_50_100_dist`, `close_dema_100_200_dist_pct`, `close_kama_200_slope`, `close_sma_100_slope`, `ht_dcperiod_signal_10`, `close_kama_100_200_dist`
- ⚠️ **dead methods** (separated nothing): `lasso`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1121 | +0.0871 |
| `ic_trend_per_fold` | +0.0165 | -0.0024 |
| `hit_rate` | +0.5160 | +0.4942 |
| `n_eff_per_fold` | 149.0 | 149.0 |

## The bar

- observed **+0.1121** against a p95 bar of **+0.0754** (20 draws) — **CLEARS**
- null mean +0.0249, sd 0.0346, max +0.1189; z = **+2.52**, p = 0.0476

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.1732 | 0.4901 |
| selected | shuffled control | +0.0612 | 0.4921 |
| all channels | real | +0.2085 | 0.5040 |
| all channels | shuffled control | +0.1076 | 0.4901 |

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

Feature selection over the TECHNICAL pool, run to sit beside the pool__fa run (CONTEXT §11) and the pool__basic run (§6b): same ticker, same target, same horizon, same folds, same device, same max_features. lookback=1 BY DESIGN, not for cost: a technical indicator already IS a window statistic over the price (sma_200 is a 200-day mean, macd a difference of two EMAs, bb_20_bandwidth a rolling sd), so wrapping a 20-day last/mean/slope/sd/min/max window around 922 of them computes windows of windows and multiplies the design matrix by six for no new information — the same reasoning §11 applied to pool__fa for a different reason. ⚠️ THE 207 BOOLEAN COLUMNS ARE CAST TO 0/1 AND SCORED. `FeatureSelector._prepare` drops bool dtypes, so a naive run would have silently scored 717 of the 921 indicators (CONTEXT §7a). The flag columns (rsi_14_gt_70, macd_..._cross_above, close_bb_20_above_upper, …) are real signals and are included; 2 of them are constant and were dropped as such. `ht_dcphase_quadrant` (VARCHAR) is excluded. 40 channels have coverage below 0.9 — the warm-up NaNs of the long moving averages — imputed with the TRAIN median inside every fold.
