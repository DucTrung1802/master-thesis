# Feature-importance run — `cs_rank_10day`

*Generated 2026-08-20T14:56:24+07:00 at commit `807999a+dirty`.*

## Input

- **schema** `unified_schema_all` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 273,367 rows x 104 columns
- **range** 2009-01-02 to 2016-12-30 (1995 sessions, 150 tickers)

## Target

- **`cs_rank_10day`** — cs_rank_10day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **10** sessions; 273,367 labelled, 0 unlabelled
- mean -0.00000, sd 0.29072, range -0.5000 to +0.5000

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
| `dev_samples` | 270517 |
| `design_columns` | 504 |

## Result

- **58 of 84 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_dist_from_high_252`, `drv_clv`, `drv_vwap_raw`, `drv_parkinson_5`, `drv_close_vs_vwap`, `drv_rogers_satchell_5`, `drv_close_pos_252`, `drv_range_hl_pct`, `drv_lower_shadow`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1260 | +0.1345 |
| `ic_trend_per_fold` | +0.0219 | +0.0189 |
| `hit_rate` | +0.5392 | +0.5429 |
| `n_eff_per_fold` | 28.9 | 28.9 |

## The bar

- ⚠️ **NO NULL WAS COMPUTED FOR THIS RUN.** A positive IC is not a result on its own — see `CONTEXT.md` §6b and §8. Treat the ranking below as descriptive until a shuffled-label null has been run for *this* configuration.

## Files

- `feature_importance.csv`
- `design_scores.csv`
- `validation.csv`
- `target_correlation.csv`
- `channel_correlation.csv`
- `stability.csv`
- `coverage.csv`
- `figures/01_ensemble_ranking.png`
- `figures/02_method_heatmap.png`
- `figures/03_target_correlation.png`
- `figures/04_channel_correlation.png`
- `figures/05_stat_profile.png`
- `figures/06_validation.png`
- `figures/07_stability.png`
- `figures/08_coverage.png`
- `figures/09_target_distribution.png`
- `metadata.json`

## Notes

TODO PRF-7 at h=10 - the CHEAP half, second horizon. The h=20 walk-forward has its selection look-ahead BOUNDED (pre-2017 probe kept 51 of 61 channels, Jaccard 0.761, 5.8 sd above chance, same top two); the h=10 walk-forward measured 2026-08-20 (pooled Sharpe at 30 bps +2.531 over 236 periods, z=+18.58, positive in 10 of 10 folds) has NO such bound - its 19 channels were selected over 2009-2026, i.e. including every test fold. Identical to the `cross-sectional-h10` job in every respect except the DATA WINDOW: dates < 2017-01-01, exactly what walk-forward fold 0 could have seen (its train ends 2016-01-01, its val is 2016). Same universe (liquidity_before stays 2014-01-01), same target, horizon, lookback, min_width, dtype, feature_normalize and ensemble, so the ONLY thing that moves is which dates the selection saw. Two random 19-subsets of 90 candidates share 4.0 by chance, so that is the bar the overlap is read against. NO NULL on purpose - the kept SET is the measurement here, not its bar, which is what took the h=20 version from ~6 h to 10m 34s. REPORT_ROOT is the probes root: PRB-1 - a run that measures the SELECTION is not a run that feeds the CHAIN, and only the root separates them.
