# Feature-importance run — `return_5day`

*Generated 2026-08-09T20:49:50.966844+00:00 at commit `690cfe61+dirty`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 4,266 rows x 42 columns
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
| `device` | cpu |
| `random_state` | 42 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 4242 |
| `design_columns` | 162 |

## Result

- **14 of 27 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `close_raw`, `sell_order_vol`, `foreign_own`, `volume_negotiated`, `high`, `foreign_room_left`, `close_adjust`, `n_sell_orders`, `foreign_sell_value`, `low`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0783 | +0.0612 |
| `ic_trend_per_fold` | -0.0215 | -0.0297 |
| `hit_rate` | +0.4822 | +0.4844 |
| `n_eff_per_fold` | 148.6 | 148.6 |

## The bar

- observed **+0.0783** against a p95 bar of **+0.0562** (20 draws) — **CLEARS**
- null mean +0.0013, sd 0.0358, max +0.0648; z = **+2.15**, p = 0.0476

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

Prototype end-to-end chain: VCB, pool__basic only, on the 2026-08-10 re-scraped panel (4,266 rows to 2026-08-07).
