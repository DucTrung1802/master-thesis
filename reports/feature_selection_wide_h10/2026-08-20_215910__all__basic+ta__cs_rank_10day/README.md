# Feature-importance run — `cs_rank_10day`

*Generated 2026-08-21T04:59:15+07:00 at commit `a6b2052`.*

## Input

- **schema** `unified_schema_all` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`, `pool__ta`
- **panel** 624,238 rows x 176 columns
- **range** 2009-01-02 to 2026-06-26 (4358 sessions, 150 tickers)

## Target

- **`cs_rank_10day`** — cs_rank_10day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **10** sessions; 622,808 labelled, 1,430 unlabelled
- mean +0.00000, sd 0.29064, range -0.5000 to +0.5000

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
| `dev_samples` | 619958 |
| `design_columns` | 972 |

## Result

- **133 of 162 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `drv_order_vol_imb`, `drv_clv`, `drv_order_vol_imb_5`, `drv_log_order_size_ratio`, `drv_close_vs_vwap`, `drv_dist_from_high_252`, `drv_rogers_satchell_5`, `drv_parkinson_5`, `drv_range_hl_pct`, `close_wma_7_slope`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.1495 | +0.1493 |
| `ic_trend_per_fold` | +0.0044 | +0.0047 |
| `hit_rate` | +0.5489 | +0.5483 |
| `n_eff_per_fold` | 76.2 | 76.2 |

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

TODO item 1/2 - THE WIDENED CROSS-SECTIONAL RUN AT THE MEASURED CEILING. 90 pool__basic + 72 pruned pool__ta channels, against PRF-9's 120 which was the T4 limit before three memory fixes shipped on 2026-08-21. WHY NOT 233: four attempts at 233 channels all died with DeadKernelError, and the phase profile tracked the wall moving each time - VRAM 6.1/14.9 GB throughout (the gpu.tree_shap row-blocking held), host peak 26.2 GB -> 26.1 -> 21.7 as the export ticker filter, the window_design cube blocking and the panel reassembly were fixed in turn, and death each time inside phase 4 (the ranker ensemble) on a 12.1 GB resident base. The remaining allocation is the ensemble's own copies of a 1,398-column design, which is a bigger change than this run is worth: PRF-9 already measured pool__ta as changing the SHORTLIST and not the MONEY at 120 channels, so the marginal value of 233 over ~160 is low and the cost of chasing a fifth wall is not. THE CHANNEL SET IS LABEL-FREE: feature_selection.prune ranks pool__ta by coverage and within-pool redundancy at corr 0.70, never by the target, so no look-ahead enters the candidate set. Seven SKW-1 duplicates of pool__basic are removed BY HAND because prune.py prunes WITHIN one pool and cannot see a cross-pool duplicate - PRF-9's own pilot shipped pool__ta.close alongside pool__basic.close_adjust at rho +0.997. pool__fa is REFUSED: on unified_schema_all it holds 2 tickers (VCB, ACB) and 8,265 rows, so an INNER join would collapse a 150-name panel to 2. The 19 pool__economy_* are date-only and cannot rank a cross-section at all. NO NULL on purpose: the question is whether any pool__ta channel survives selection at this width; PRF-9 established that the money question is settled downstream. STA-1: joining pool__ta truncates the panel at 2026-06-26, losing ~31 sessions.
