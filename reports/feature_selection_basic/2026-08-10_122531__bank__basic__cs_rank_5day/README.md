# Feature-importance run — `cs_rank_5day`

*Generated 2026-08-10T05:25:35.107471+00:00 at commit `ae2514a3+dirty`.*

## Input

- **schema** `unified_schema_bank` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 54,185 rows x 41 columns
- **range** 2009-07-16 to 2026-08-07 (4255 sessions, 20 tickers)

## Target

- **`cs_rank_5day`** — cs_rank_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 54,085 labelled, 100 unlabelled
- mean -0.00000, sd 0.31253, range -0.5000 to +0.5000

## Setup

| knob | value |
|---|---|
| `selector_class` | CrossSectionalSelector |
| `lookback_d` | 20 |
| `horizon_h` | 5 |
| `normalize` | none |
| `feature_normalize` | cs_rank |
| `panel_col` | ticker |
| `purge_gap_rows` | 24 |
| `window_stats` | last, mean, slope, sd, min, max |
| `n_splits` | 5 |
| `min_train` | 500 |
| `corr_threshold` | 0.9 |
| `device` | cpu |
| `random_state` | 42 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 53705 |
| `design_columns` | 162 |

## Result

- **14 of 27 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `n_sell_orders`, `avg_vol_per_buy_order`, `foreign_net_value`, `n_buy_orders`, `high`, `volume_matched`, `close_raw`, `value_matched`, `sell_order_vol`, `prop_sell_vol`
- ⚠️ **dead methods** (separated nothing): `lasso`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | -0.0106 | -0.0051 |
| `ic_trend_per_fold` | +0.0079 | +0.0001 |
| `hit_rate` | +0.4753 | +0.4777 |
| `n_eff_per_fold` | 148.2 | 148.2 |

## The bar

- observed **-0.0106** against a p95 bar of **+0.0216** (20 draws) — **DOES NOT CLEAR**
- null mean +0.0052, sd 0.0092, max +0.0279; z = **-1.71**, p = 0.9524

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

Bank panel, pool__basic only, on the 2026-08-10 re-scraped data (54,528 rows, 20 tickers to 2026-08-07). Cross-sectional rank target, date_block null.
