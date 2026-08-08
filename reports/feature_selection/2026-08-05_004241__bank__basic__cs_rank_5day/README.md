# Feature-importance run — `cs_rank_5day`

*Generated 2026-08-04T17:42:41.166910+00:00 at commit `9d529dd+dirty`.*

## Input

- **schema** `unified_schema_bank` (database `database_main_v2`)
- **tables** `pool__basic`, `pool__targets`
- **panel** 39,056 rows x 44 columns
- **range** 2017-08-17 to 2026-06-26 (2206 sessions, 20 tickers)

## Target

- **`cs_rank_5day`** — cs_rank_5day (see cross_sectional.py / DataPreprocessor for the definition)
- horizon **5** sessions; 38,956 labelled, 100 unlabelled
- mean -0.00000, sd 0.30544, range -0.5000 to +0.5000

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
| `max_features` | 12 |
| `corr_threshold` | 0.9 |
| `device` | cpu |
| `random_state` | 42 |
| `permutation_repeats` | 10 |
| `holdout_start` | — |
| `dev_samples` | 38576 |
| `design_columns` | 162 |

## Result

- **12 of 27 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `value_negotiated`, `close_adjust`, `sell_order_vol`, `avg_vol_per_sell_order`, `volume_negotiated`, `n_sell_orders`, `avg_vol_per_buy_order`, `prop_sell_vol`, `n_buy_orders`, `value_matched`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0087 | +0.0147 |
| `ic_trend_per_fold` | -0.0025 | +0.0035 |
| `hit_rate` | +0.4966 | +0.4974 |
| `n_eff_per_fold` | 66.2 | 66.2 |

## The bar

- observed **+0.0087** against a p95 bar of **+0.0249** (20 draws) — **DOES NOT CLEAR**
- null mean +0.0073, sd 0.0134, max +0.0357; z = **+0.11**, p = 0.5238

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | -0.0262 | 0.4850 |
| selected | shuffled control | +0.0095 | 0.5034 |
| all channels | real | -0.0136 | 0.4896 |
| all channels | shuffled control | -0.0250 | 0.4864 |

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

Cross-sectional feature selection over the GICS BANK sector — the §9c protocol (cs_rank_5day, d=20, h=5, cs_rank feature normalisation, date-grouped purged CV, date_block null with the selection re-run inside every draw) applied to a SECTOR instead of an index. Panel: unified_schema_bank, built by _ingest_unified_pool_basic(DataPreprocessor.UNIFIED_BANK), which selects silver.stocks_basic on GICS industry_code='401010' (Financials -> Banks -> Banks) — 20 tickers, membership DERIVED from the taxonomy rather than listed. min_width=10 rather than §9's 20: only 20 banks exist, and min_width=20 would start the panel at 2021-03-24 and cost half the sessions. ⚠️ READ §9h BEFORE READING THE IC. The width ladder says a 30-name cross-section scores z=+1.42 and does NOT clear its null, statistically indistinguishable from a single ticker; the practical threshold measured there is ~100 names. This panel is 20. A failure here is the predicted outcome and is evidence about the WIDTH, not about banks.
