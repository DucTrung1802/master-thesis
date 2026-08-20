# Feature-importance run — `cs_rank_10day`

*Generated 2026-08-20T09:10:07+07:00 at commit `6dfb53df+dirty`.*

## Input

- **schema** `unified_schema_vn30` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 65,763 rows x 104 columns
- **range** 2017-04-21 to 2026-06-26 (2288 sessions, 30 tickers)

## Target

- **`cs_rank_10day`** — cs_rank_10day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **10** sessions; 65,463 labelled, 300 unlabelled
- mean +0.00000, sd 0.29890, range -0.5000 to +0.5000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 20 |
| `horizon_h` | 10 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 29 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cuda |
| `random_state` | 18 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 64893 |
| `design_columns` | 540 |

## Result

- **62 of 90 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_log_order_size_ratio`, `drv_order_vol_imb`, `close_adjust`, `drv_foreign_participation`, `avg_vol_per_buy_order`, `drv_close_vs_vwap`, `drv_dist_from_low_21`, `drv_upper_shadow`, `drv_order_count_imb_5`, `drv_order_vol_imb_5`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0204 | +0.0155 |
| `ic_trend_per_fold` | +0.0129 | +0.0120 |
| `hit_rate` | +0.5060 | +0.5020 |
| `n_eff_per_fold` | 34.6 | 34.6 |

## The bar

- observed **+0.0204** against a p95 bar of **+0.0370** (10 draws) — **DOES NOT CLEAR**
- null mean +0.0190, sd 0.0137, max +0.0435; z = **+0.10**, p = 0.5455

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

SSK-1 / width ladder at h=10: same target, horizon and machinery as PRF-2's top-150 (z=+13.78), N moved 150 -> 30. VN30 = today's vn30.csv membership, NOT point-in-time.
