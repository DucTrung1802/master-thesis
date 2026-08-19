# Feature-importance run — `cs_rank_10day`

*Generated 2026-08-19T16:34:08+07:00 at commit `e270c9d3+dirty`.*

## Input

- **schema** `unified_schema_all` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 624,448 rows x 104 columns
- **range** 2009-01-02 to 2026-08-07 (4388 sessions, 150 tickers)

## Target

- **`cs_rank_10day`** — cs_rank_10day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **10** sessions; 622,948 labelled, 1,500 unlabelled
- mean +0.00000, sd 0.29065, range -0.5000 to +0.5000

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
| `dev_samples` | 620098 |
| `design_columns` | 540 |

## Result

- **61 of 90 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_clv`, `drv_order_vol_imb_5`, `drv_close_vs_vwap`, `drv_log_order_size_ratio`, `drv_rogers_satchell_5`, `drv_range_hl_pct`, `drv_parkinson_5`, `drv_cs_pct_ret_1d`, `drv_lower_shadow`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1201 | +0.1211 |
| `ic_trend_per_fold` | +0.0115 | +0.0106 |
| `hit_rate` | +0.5393 | +0.5393 |
| `n_eff_per_fold` | 76.6 | 76.6 |

## The bar

- observed **+0.1201** against a p95 bar of **+0.0355** (20 draws) — **CLEARS**
- null mean +0.0233, sd 0.0070, max +0.0357; z = **+13.78**, p = 0.0476

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

TODO PRF-2. The real chain at h=10 - the one horizon that is measured-to-work and unmeasured-by-a-model. A hand-built 3-channel rank gets Sharpe +0.652 at 30 bps there (backtest/CONTEXT.md 8g); nobody has run the selection + LSTM chain at h=10, so how much a fitted model adds over three ranked columns is unknown at EVERY horizon. It also separates PRF-3's two hypotheses for the post-2022 break by holding the feature pipeline fixed and moving only the horizon. 20 draws and not 10: section 5's rule is 10 to fail and 20 to pass, and the h=20 run cleared at z=+9.09, so a bar this one can be set beside is the point. Everything else is identical to the h=20 job - same universe, lookback, min_width, dtype, ensemble, feature_normalize.
