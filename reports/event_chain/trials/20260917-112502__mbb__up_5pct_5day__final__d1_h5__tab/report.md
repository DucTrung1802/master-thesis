# Event report — MBB `up_5pct_5day`

**Event:** 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions. **Window** d=1, **purge** 5 samples. **Table** `unified_schema_mbb.up_5pct_5day__final__d1_h5__tab` → dataset `mbb__up_5pct_5day__final__d1_h5__tab__tr70_val15_test15__std` (hash `08e43324ddc6de7a`).

| split | label dates | samples | positives | base rate |
|---|---|---|---|---|
| train | 2011-11-01 → 2022-02-28 | 2,575 | 288 | 0.112 |
| val | 2022-03-08 → 2024-05-17 | 548 | 55 | 0.100 |
| test | 2024-05-27 → 2026-08-14 | 553 | 49 | 0.089 |

Feature selection read only rows before **2022-03-08** (the val start). Features in the model: **133**.

## Selection, one pool per run (null = block-shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__market_context | 59 | 34 | 0.1474 | 0.0307 | 0.0354 | 7.86 | yes |
| pool__event_features | 67 | 57 | 0.1265 | 0.0418 | 0.0418 | 5.14 | yes |
| pool__market_breadth | 7 | 7 | 0.0581 | 0.0363 | 0.0442 | 2.19 | yes |
| pool__news_daily | 14 | 11 | 0.0097 | 0.0346 | 0.0351 | 0.54 | no |
| pool__ta | 717 | 412 | 0.0115 | 0.0310 | 0.0355 | 0.42 | no |
| pool__basic | 78 | 52 | -0.0041 | 0.0526 | 0.0720 | -0.18 | no |

## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)

| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall | test AUC, refit on train+val (null p95) | test AUC, rolling refit every 21 sessions (null p95) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `blend_top3` | 0.744 | **0.627** | 0.618 / 0.712 | 1.82 | 0.138 (1.55x) | 0.164 (1.85x) | 0.179 | 0.006 | 26 / 0.154 / 0.082 | — | — |
| `ensemble_px2l` | 0.743 | **0.630** | 0.616 / 0.719 | 1.85 | 0.138 (1.56x) | 0.164 (1.85x) | 0.143 | 0.007 | 26 / 0.154 / 0.082 | 0.675 (0.612) | 0.648 (0.612) |
| `ensemble_pxl` | 0.743 | **0.639** | 0.616 / 0.710 | 1.98 | 0.141 (1.59x) | 0.182 (2.05x) | 0.143 | 0.008 | 23 / 0.087 / 0.041 | 0.676 (0.612) | 0.650 (0.614) |
| `event_panel_xgb_pan_d2_n600` | 0.742 | **0.612** | 0.612 / 0.721 | 1.61 | 0.135 (1.53x) | 0.145 (1.64x) | 0.179 | 0.002 | 23 / 0.174 / 0.082 | 0.662 (0.607) | 0.640 (0.608) |
| `event_panel_logit_pan_c003_hl4` | 0.737 | **0.586** | 0.617 / 0.713 | 1.24 | 0.120 (1.35x) | 0.145 (1.64x) | 0.107 | -0.018 | 11 / 0.182 / 0.041 | 0.634 (0.608) | 0.620 (0.607) |
| `event_panel_xgb_pan_d3_n600` | 0.730 | **0.665** | 0.614 / 0.731 | 2.37 | 0.162 (1.83x) | 0.200 (2.26x) | 0.179 | 0.008 | 29 / 0.172 / 0.102 | 0.689 (0.607) | 0.659 (0.607) |
| `event_linear_evt_hep_c003_hl4` | 0.698 | **0.604** | 0.619 / 0.742 | 1.52 | 0.152 (1.71x) | 0.164 (1.85x) | 0.179 | -0.015 | 29 / 0.172 / 0.102 | 0.551 (0.614) | 0.517 (0.614) |
| `event_panel_xgb_d2_n600` | 0.698 | **0.608** | 0.613 / 0.731 | 1.57 | 0.127 (1.43x) | 0.145 (1.64x) | 0.179 | -0.005 | 1 / 0.000 / 0.000 | 0.691 (0.609) | 0.663 (0.611) |
| `event_linear_mag_hepm_a100_hl4` | 0.689 | **0.541** | 0.611 / 0.696 | 0.61 | 0.116 (1.31x) | 0.109 (1.23x) | 0.179 | -0.009 | 3 / 0.333 / 0.020 | 0.558 (0.610) | 0.555 (0.616) |
| `forest_et_leaf30` | 0.659 | **0.635** | 0.613 / 0.695 | 1.91 | 0.129 (1.46x) | 0.091 (1.03x) | 0.071 | -0.010 | 21 / 0.048 / 0.020 | 0.653 (0.618) | 0.641 (0.614) |
| `gbt_d2` | 0.630 | **0.649** | 0.615 / 0.712 | 2.18 | 0.132 (1.49x) | 0.127 (1.44x) | 0.107 | 0.012 | 3 / 0.000 / 0.000 | 0.662 (0.611) | 0.639 (0.615) |
| `baseline_prior` | 0.500 | **0.500** | 0.500 / 0.500 | — | 0.089 (1.00x) | 0.091 (1.03x) | 0.036 | -0.000 | 553 / 0.089 / 1.000 | — | — |

**Best on val:** `ensemble_px2l` — val AUC 0.743, **test AUC 0.630** against a block-shuffled null p95 of 0.616 (max 0.719, z 1.85); test PR-AUC 0.138 on a base rate of 0.089; of the 26 test sessions over the val threshold, 0.154 were followed by the event. Refitted on train+val, its test AUC is 0.675 (null p95 0.612). Refitted every 21 test sessions, 0.648 (null p95 0.612).

**Fixed ensemble** `ensemble_px2l` (geometric mean of `event_panel_xgb_pan_d2_n600`, `event_panel_xgb_pan_d3_n600`, `event_panel_logit_pan_c003_hl4`, chosen before this chain ran): val AUC 0.743, test AUC **0.630** (null p95 0.616, z 1.85); refitted on train+val **0.675** (null p95 0.612).

**Fixed ensemble** `ensemble_pxl` (geometric mean of `event_panel_xgb_pan_d3_n600`, `event_panel_logit_pan_c003_hl4`, chosen before this chain ran): val AUC 0.743, test AUC **0.639** (null p95 0.616, z 1.98); refitted on train+val **0.676** (null p95 0.612).

## Walk-forward — yearly expanding refits over years the selection never read

| model | year | train n | test n | positives | base rate | AUC | PR-AUC |
|---|---|---|---|---|---|---|---|
| `event_panel_xgb_pan_d2_n600` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.816 | 0.503 |
| `event_panel_xgb_pan_d2_n600` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.658 | 0.203 |
| `event_panel_xgb_pan_d2_n600` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.599 | 0.161 |
| `event_panel_xgb_pan_d2_n600` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.679 | 0.249 |
| `event_panel_xgb_pan_d2_n600` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.716 | 0.094 |
| `event_panel_xgb_pan_d3_n600` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.816 | 0.491 |
| `event_panel_xgb_pan_d3_n600` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.673 | 0.326 |
| `event_panel_xgb_pan_d3_n600` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.608 | 0.185 |
| `event_panel_xgb_pan_d3_n600` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.716 | 0.286 |
| `event_panel_xgb_pan_d3_n600` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.733 | 0.099 |
| `event_panel_logit_pan_c003_hl4` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.817 | 0.483 |
| `event_panel_logit_pan_c003_hl4` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.720 | 0.306 |
| `event_panel_logit_pan_c003_hl4` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.571 | 0.156 |
| `event_panel_logit_pan_c003_hl4` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.613 | 0.209 |
| `event_panel_logit_pan_c003_hl4` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.724 | 0.099 |
| `ensemble_px2l` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.823 | 0.484 |
| `ensemble_px2l` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.685 | 0.289 |
| `ensemble_px2l` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.600 | 0.166 |
| `ensemble_px2l` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.686 | 0.251 |
| `ensemble_px2l` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.735 | 0.099 |
| `ensemble_pxl` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.825 | 0.497 |
| `ensemble_pxl` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.700 | 0.351 |
| `ensemble_pxl` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.597 | 0.180 |
| `ensemble_pxl` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.685 | 0.253 |
| `ensemble_pxl` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.742 | 0.101 |

## Read before quoting

- `n_eff` ≈ samples / h: the test split carries ~111 independent observations, and fewer positives.
- The null prices ONE run; the grid is several runs (NUL-1). Quote the val-chosen row.
- The base rate drifts across splits (see the first table), so a fixed probability threshold does not travel; the val-F1 threshold is reported for that reason.
