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
| pool__basic | 78 | 52 | -0.0041 | 0.0526 | 0.0720 | -0.18 | no |

## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)

| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall | test AUC, refit on train+val (null p95) |
|---|---|---|---|---|---|---|---|---|---|---|
| `blend_top3` | 0.705 | **0.585** | 0.612 / 0.713 | 1.24 | 0.141 (1.59x) | 0.218 (2.46x) | 0.179 | 0.010 | 12 / 0.167 / 0.041 | — |
| `event_panel_xgb_d2_n600` | 0.698 | **0.608** | 0.613 / 0.731 | 1.57 | 0.127 (1.43x) | 0.145 (1.64x) | 0.179 | -0.005 | 1 / 0.000 / 0.000 | 0.691 (0.609) |
| `event_linear_evt_c01` | 0.682 | **0.575** | 0.613 / 0.710 | 1.12 | 0.138 (1.56x) | 0.218 (2.46x) | 0.179 | -0.024 | 24 / 0.208 / 0.102 | 0.559 (0.613) |
| `ensemble_geo3p` | 0.679 | **0.605** | 0.618 / 0.721 | 1.52 | 0.142 (1.60x) | 0.218 (2.46x) | 0.214 | 0.016 | 8 / 0.000 / 0.000 | 0.627 (0.613) |
| `ensemble_geo4` | 0.677 | **0.615** | 0.615 / 0.726 | 1.66 | 0.153 (1.73x) | 0.218 (2.46x) | 0.250 | -0.025 | 16 / 0.188 / 0.061 | 0.638 (0.612) |
| `event_linear_mag_a10_hl4` | 0.667 | **0.570** | 0.611 / 0.708 | 1.08 | 0.136 (1.53x) | 0.218 (2.46x) | 0.214 | 0.007 | 24 / 0.208 / 0.102 | 0.550 (0.614) |
| `event_linear_evt_c003` | 0.660 | **0.583** | 0.614 / 0.729 | 1.22 | 0.159 (1.79x) | 0.218 (2.46x) | 0.286 | -0.000 | 11 / 0.182 / 0.041 | 0.564 (0.615) |
| `forest_et_leaf30` | 0.659 | **0.635** | 0.613 / 0.695 | 1.91 | 0.129 (1.46x) | 0.091 (1.03x) | 0.071 | -0.010 | 21 / 0.048 / 0.020 | 0.653 (0.618) |
| `ensemble_geo3` | 0.656 | **0.602** | 0.613 / 0.721 | 1.50 | 0.160 (1.80x) | 0.200 (2.26x) | 0.250 | -0.081 | 16 / 0.250 / 0.082 | 0.597 (0.615) |
| `ensemble_geo2` | 0.653 | **0.590** | 0.615 / 0.737 | 1.31 | 0.146 (1.65x) | 0.218 (2.46x) | 0.214 | 0.014 | 24 / 0.167 / 0.082 | 0.584 (0.615) |
| `event_linear_dir_c001` | 0.643 | **0.623** | 0.604 / 0.720 | 1.89 | 0.172 (1.94x) | 0.182 (2.05x) | 0.214 | -3.687 | 12 / 0.333 / 0.082 | 0.655 (0.602) |
| `event_linear_mag_har_a100_hl4` | 0.639 | **0.586** | 0.615 / 0.722 | 1.27 | 0.141 (1.59x) | 0.182 (2.05x) | 0.179 | 0.010 | 19 / 0.263 / 0.102 | 0.605 (0.613) |
| `gbt_d2` | 0.630 | **0.649** | 0.615 / 0.712 | 2.18 | 0.132 (1.49x) | 0.127 (1.44x) | 0.107 | 0.012 | 3 / 0.000 / 0.000 | 0.662 (0.611) |
| `baseline_prior` | 0.500 | **0.500** | 0.500 / 0.500 | — | 0.089 (1.00x) | 0.091 (1.03x) | 0.036 | -0.000 | 553 / 0.089 / 1.000 | — |

**Best on val:** `event_panel_xgb_d2_n600` — val AUC 0.698, **test AUC 0.608** against a block-shuffled null p95 of 0.613 (max 0.731, z 1.57); test PR-AUC 0.127 on a base rate of 0.089; of the 1 test sessions over the val threshold, 0.000 were followed by the event. Refitted on train+val, its test AUC is 0.691 (null p95 0.609).

**Fixed ensemble** `ensemble_geo3p` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, `event_panel_xgb_d2_n600`, chosen before this chain ran): val AUC 0.679, test AUC **0.605** (null p95 0.618, z 1.52); refitted on train+val **0.627** (null p95 0.613).

**Fixed ensemble** `ensemble_geo4` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, `event_linear_dir_c001`, `event_panel_xgb_d2_n600`, chosen before this chain ran): val AUC 0.677, test AUC **0.615** (null p95 0.615, z 1.66); refitted on train+val **0.638** (null p95 0.612).

**Fixed ensemble** `ensemble_geo3` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, `event_linear_dir_c001`, chosen before this chain ran): val AUC 0.656, test AUC **0.602** (null p95 0.613, z 1.50); refitted on train+val **0.597** (null p95 0.615).

**Fixed ensemble** `ensemble_geo2` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, chosen before this chain ran): val AUC 0.653, test AUC **0.590** (null p95 0.615, z 1.31); refitted on train+val **0.584** (null p95 0.615).

## Walk-forward — yearly expanding refits over years the selection never read

| model | year | train n | test n | positives | base rate | AUC | PR-AUC |
|---|---|---|---|---|---|---|---|
| `event_panel_xgb_d2_n600` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.789 | 0.303 |
| `event_panel_xgb_d2_n600` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.553 | 0.215 |
| `event_panel_xgb_d2_n600` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.557 | 0.120 |
| `event_panel_xgb_d2_n600` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.686 | 0.272 |
| `event_panel_xgb_d2_n600` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.750 | 0.107 |
| `event_linear_mag_har_a100_hl4` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.777 | 0.344 |
| `event_linear_mag_har_a100_hl4` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.501 | 0.106 |
| `event_linear_mag_har_a100_hl4` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.456 | 0.084 |
| `event_linear_mag_har_a100_hl4` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.618 | 0.226 |
| `event_linear_mag_har_a100_hl4` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.589 | 0.082 |
| `event_linear_evt_c003` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.768 | 0.319 |
| `event_linear_evt_c003` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.631 | 0.141 |
| `event_linear_evt_c003` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.461 | 0.089 |
| `event_linear_evt_c003` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.619 | 0.279 |
| `event_linear_evt_c003` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.522 | 0.069 |
| `event_linear_dir_c001` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.730 | 0.344 |
| `event_linear_dir_c001` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.618 | 0.108 |
| `event_linear_dir_c001` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.650 | 0.125 |
| `event_linear_dir_c001` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.626 | 0.221 |
| `event_linear_dir_c001` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.743 | 0.141 |
| `ensemble_geo3p` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.797 | 0.372 |
| `ensemble_geo3p` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.564 | 0.152 |
| `ensemble_geo3p` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.474 | 0.097 |
| `ensemble_geo3p` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.650 | 0.296 |
| `ensemble_geo3p` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.684 | 0.089 |
| `ensemble_geo4` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.787 | 0.376 |
| `ensemble_geo4` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.595 | 0.158 |
| `ensemble_geo4` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.486 | 0.104 |
| `ensemble_geo4` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.656 | 0.320 |
| `ensemble_geo4` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.699 | 0.094 |
| `ensemble_geo3` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.771 | 0.364 |
| `ensemble_geo3` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.605 | 0.149 |
| `ensemble_geo3` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.483 | 0.094 |
| `ensemble_geo3` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.626 | 0.299 |
| `ensemble_geo3` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.621 | 0.081 |
| `ensemble_geo2` | 2022 | 2,570 | 209 | 26 | 0.124 | 0.782 | 0.367 |
| `ensemble_geo2` | 2023 | 2,779 | 249 | 18 | 0.072 | 0.569 | 0.127 |
| `ensemble_geo2` | 2024 | 3,028 | 245 | 21 | 0.086 | 0.460 | 0.089 |
| `ensemble_geo2` | 2025 | 3,273 | 248 | 31 | 0.125 | 0.624 | 0.292 |
| `ensemble_geo2` | 2026 | 3,521 | 150 | 8 | 0.053 | 0.570 | 0.075 |

## Read before quoting

- `n_eff` ≈ samples / h: the test split carries ~111 independent observations, and fewer positives.
- The null prices ONE run; the grid is several runs (NUL-1). Quote the val-chosen row.
- The base rate drifts across splits (see the first table), so a fixed probability threshold does not travel; the val-F1 threshold is reported for that reason.
