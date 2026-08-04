# Feature-importance run — `return_5day`

*Generated 2026-08-04T14:33:30.373996+00:00 at commit `9ff2ac5`.*

## Input

- **schema** `unified_schema_vcb` (database `database_main_v2`)
- **tables** `pool__fa`, `pool__targets`
- **panel** 4,235 rows x 211 columns
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
| `design_columns` | 162 |

## Result

- **12 of 162 channels kept** after the |rho| >= 0.9 redundancy prune
- **top 10 by ensemble rank**: `balance_sheet_iv_chung_khoan_kinh_doanh`, `balance_sheet_iv_1_chung_khoan_kinh_doanh`, `equity_growth_yoy`, `balance_sheet_iii_2_cho_vay_cac_tctd_khac`, `income_statement_ii_lai_lo_thuan_tu_hoat_dong_dich_vu`, `balance_sheet_vii_1_cac_khoan_lai_phi_phai_tra`, `balance_sheet_ix_2_von_gop_lien_doanh`, `balance_sheet_xii_1_cac_khoan_phai_thu`, `balance_sheet_viii_chung_khoan_dau_tu`, `cash_flow_hdkd_6_tien_thu_cac_khoan_no_da_duoc_xu_ly_xoa_bu_dap`

| metric | selected | all channels |
|---|---|---|
| `ic_mean` | +0.0157 | +0.0710 |
| `ic_trend_per_fold` | -0.0171 | -0.0209 |
| `hit_rate` | +0.4679 | +0.4752 |
| `n_eff_per_fold` | 149.0 | 149.0 |

## The bar

- observed **+0.0157** against a p95 bar of **+0.0740** (20 draws) — **DOES NOT CLEAR**
- null mean +0.0242, sd 0.0337, max +0.0896; z = **-0.25**, p = 0.6190

## Holdout

| feature set | labels | IC | hit rate |
|---|---|---|---|
| selected | real | +0.0232 | 0.5277 |
| selected | shuffled control | +0.0322 | 0.4427 |
| all channels | real | -0.0111 | 0.5277 |
| all channels | shuffled control | -0.0448 | 0.4486 |

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

Feature selection over the FUNDAMENTAL pool. lookback=1 because a fundamental is a step function: pool__fa changes value on 69 publish days out of 4,230 sessions (~1 in 64), so window statistics collapse. `year`/`quarter` and the *_template/_period/_source metadata are excluded — `year` is not a proxy for the era, it IS the era. See CONTEXT §7a and §11.
