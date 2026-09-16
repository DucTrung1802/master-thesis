# Event report — VCB `up_5pct_5day`

**Event:** 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions. **Window** d=1, **purge** 5 samples. **Table** `unified_schema_vcb.up_5pct_5day__final__d1_h5__tab` → dataset `vcb__up_5pct_5day__final__d1_h5__tab__tr70_val15_test15__std` (hash `8cae1d16d1530360`).

| split | label dates | samples | positives | base rate |
|---|---|---|---|---|
| train | 2009-06-30 → 2021-06-14 | 2,984 | 404 | 0.135 |
| val | 2021-06-22 → 2024-01-02 | 636 | 53 | 0.083 |
| test | 2024-01-10 → 2026-08-14 | 641 | 33 | 0.051 |

Feature selection read only rows before **2021-06-22** (the val start). Features in the model: **211**.

## Selection, one pool per run (null = block-shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__event_features | 67 | 57 | 0.1126 | 0.0354 | 0.0439 | 4.73 | yes |
| pool__market_context | 59 | 35 | 0.1190 | 0.0410 | 0.0486 | 3.86 | yes |
| pool__basic | 78 | 55 | 0.0839 | 0.0386 | 0.0456 | 3.03 | yes |
| pool__market_breadth | 7 | 7 | 0.0553 | 0.0259 | 0.0334 | 2.69 | yes |
| pool__news_daily | 14 | 14 | -0.0218 | 0.0142 | 0.0167 | -1.03 | no |

## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)

| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall | test AUC, refit on train+val (null p95) |
|---|---|---|---|---|---|---|---|---|---|---|
| `ensemble_geo4` | 0.803 | **0.670** | 0.639 / 0.785 | 1.93 | 0.130 (2.53x) | 0.141 (2.73x) | 0.062 | -0.070 | 89 / 0.112 / 0.303 | 0.680 (0.642) |
| `ensemble_geo3` | 0.799 | **0.660** | 0.644 / 0.795 | 1.84 | 0.104 (2.01x) | 0.125 (2.43x) | 0.094 | -0.300 | 99 / 0.081 / 0.242 | 0.659 (0.641) |
| `blend_top3` | 0.797 | **0.632** | 0.643 / 0.803 | 1.51 | 0.115 (2.22x) | 0.125 (2.43x) | 0.125 | -0.089 | 102 / 0.078 / 0.242 | — |
| `ensemble_geo3p` | 0.796 | **0.676** | 0.645 / 0.788 | 2.00 | 0.135 (2.63x) | 0.141 (2.73x) | 0.094 | 0.090 | 82 / 0.122 / 0.303 | 0.683 (0.644) |
| `event_linear_evt_c01` | 0.795 | **0.614** | 0.645 / 0.774 | 1.31 | 0.109 (2.12x) | 0.109 (2.12x) | 0.094 | -0.222 | 114 / 0.070 / 0.242 | 0.620 (0.641) |
| `ensemble_geo2` | 0.790 | **0.669** | 0.645 / 0.802 | 1.93 | 0.126 (2.44x) | 0.125 (2.43x) | 0.094 | 0.017 | 106 / 0.085 / 0.273 | 0.664 (0.645) |
| `event_linear_evt_c003` | 0.789 | **0.615** | 0.642 / 0.781 | 1.33 | 0.107 (2.08x) | 0.109 (2.12x) | 0.062 | -0.134 | 98 / 0.082 / 0.242 | 0.625 (0.640) |
| `event_linear_mag_a10_hl4` | 0.771 | **0.669** | 0.639 / 0.816 | 1.91 | 0.110 (2.14x) | 0.125 (2.43x) | 0.156 | 0.021 | 108 / 0.074 / 0.242 | 0.635 (0.647) |
| `event_linear_mag_har_a100_hl4` | 0.760 | **0.705** | 0.641 / 0.786 | 2.35 | 0.121 (2.36x) | 0.125 (2.43x) | 0.125 | 0.074 | 156 / 0.128 / 0.606 | 0.688 (0.645) |
| `event_panel_xgb_d2_n600` | 0.759 | **0.637** | 0.644 / 0.788 | 1.53 | 0.126 (2.45x) | 0.172 (3.34x) | 0.250 | 0.122 | 33 / 0.242 / 0.242 | 0.654 (0.645) |
| `gbt_d2` | 0.735 | **0.623** | 0.638 / 0.784 | 1.42 | 0.141 (2.75x) | 0.094 (1.82x) | 0.125 | 0.022 | 86 / 0.151 / 0.394 | 0.637 (0.639) |
| `event_linear_dir_c001` | 0.702 | **0.401** | 0.639 / 0.721 | -1.27 | 0.041 (0.79x) | 0.016 (0.30x) | 0.000 | -8.281 | 310 / 0.035 / 0.333 | 0.475 (0.630) |
| `forest_et_leaf30` | 0.689 | **0.565** | 0.653 / 0.767 | 0.75 | 0.102 (1.98x) | 0.094 (1.82x) | 0.062 | -0.132 | 89 / 0.101 / 0.273 | 0.610 (0.650) |
| `baseline_prior` | 0.500 | **0.500** | 0.500 / 0.500 | — | 0.051 (1.00x) | 0.062 (1.21x) | 0.125 | 0.000 | 641 / 0.051 / 1.000 | — |

**Best on val:** `ensemble_geo4` — val AUC 0.803, **test AUC 0.670** against a block-shuffled null p95 of 0.639 (max 0.785, z 1.93); test PR-AUC 0.130 on a base rate of 0.051; of the 89 test sessions over the val threshold, 0.112 were followed by the event. Refitted on train+val, its test AUC is 0.680 (null p95 0.642).

**Fixed ensemble** `ensemble_geo4` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, `event_linear_dir_c001`, `event_panel_xgb_d2_n600`, chosen before this chain ran): val AUC 0.803, test AUC **0.670** (null p95 0.639, z 1.93); refitted on train+val **0.680** (null p95 0.642).

**Fixed ensemble** `ensemble_geo3` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, `event_linear_dir_c001`, chosen before this chain ran): val AUC 0.799, test AUC **0.660** (null p95 0.644, z 1.84); refitted on train+val **0.659** (null p95 0.641).

**Fixed ensemble** `ensemble_geo3p` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, `event_panel_xgb_d2_n600`, chosen before this chain ran): val AUC 0.796, test AUC **0.676** (null p95 0.645, z 2.00); refitted on train+val **0.683** (null p95 0.644).

**Fixed ensemble** `ensemble_geo2` (geometric mean of `event_linear_mag_har_a100_hl4`, `event_linear_evt_c003`, chosen before this chain ran): val AUC 0.790, test AUC **0.669** (null p95 0.645, z 1.93); refitted on train+val **0.664** (null p95 0.645).

## Walk-forward — yearly expanding refits over years the selection never read

| model | year | train n | test n | positives | base rate | AUC | PR-AUC |
|---|---|---|---|---|---|---|---|
| `event_linear_evt_c01` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.685 | 0.157 |
| `event_linear_evt_c01` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.788 | 0.290 |
| `event_linear_evt_c01` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.834 | 0.252 |
| `event_linear_evt_c01` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.282 | 0.023 |
| `event_linear_evt_c01` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.734 | 0.218 |
| `event_linear_evt_c01` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.605 | 0.110 |
| `event_linear_mag_har_a100_hl4` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.698 | 0.101 |
| `event_linear_mag_har_a100_hl4` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.724 | 0.204 |
| `event_linear_mag_har_a100_hl4` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.847 | 0.233 |
| `event_linear_mag_har_a100_hl4` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.409 | 0.027 |
| `event_linear_mag_har_a100_hl4` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.700 | 0.271 |
| `event_linear_mag_har_a100_hl4` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.515 | 0.093 |
| `event_linear_evt_c003` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.666 | 0.146 |
| `event_linear_evt_c003` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.786 | 0.283 |
| `event_linear_evt_c003` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.823 | 0.235 |
| `event_linear_evt_c003` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.281 | 0.023 |
| `event_linear_evt_c003` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.739 | 0.211 |
| `event_linear_evt_c003` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.619 | 0.113 |
| `event_linear_dir_c001` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.622 | 0.283 |
| `event_linear_dir_c001` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.673 | 0.235 |
| `event_linear_dir_c001` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.662 | 0.190 |
| `event_linear_dir_c001` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.496 | 0.046 |
| `event_linear_dir_c001` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.477 | 0.054 |
| `event_linear_dir_c001` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.678 | 0.157 |
| `event_panel_xgb_d2_n600` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.514 | 0.068 |
| `event_panel_xgb_d2_n600` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.730 | 0.233 |
| `event_panel_xgb_d2_n600` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.771 | 0.325 |
| `event_panel_xgb_d2_n600` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.523 | 0.033 |
| `event_panel_xgb_d2_n600` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.642 | 0.309 |
| `event_panel_xgb_d2_n600` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.652 | 0.153 |
| `ensemble_geo4` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.688 | 0.091 |
| `ensemble_geo4` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.788 | 0.265 |
| `ensemble_geo4` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.836 | 0.274 |
| `ensemble_geo4` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.333 | 0.024 |
| `ensemble_geo4` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.742 | 0.252 |
| `ensemble_geo4` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.641 | 0.119 |
| `ensemble_geo3` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.681 | 0.148 |
| `ensemble_geo3` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.798 | 0.266 |
| `ensemble_geo3` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.838 | 0.261 |
| `ensemble_geo3` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.292 | 0.023 |
| `ensemble_geo3` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.725 | 0.227 |
| `ensemble_geo3` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.592 | 0.104 |
| `ensemble_geo3p` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.688 | 0.092 |
| `ensemble_geo3p` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.782 | 0.256 |
| `ensemble_geo3p` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.848 | 0.275 |
| `ensemble_geo3p` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.334 | 0.025 |
| `ensemble_geo3p` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.739 | 0.257 |
| `ensemble_geo3p` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.638 | 0.117 |
| `ensemble_geo2` | 2021 | 2,979 | 137 | 7 | 0.051 | 0.697 | 0.113 |
| `ensemble_geo2` | 2022 | 3,116 | 249 | 30 | 0.120 | 0.786 | 0.254 |
| `ensemble_geo2` | 2023 | 3,365 | 249 | 15 | 0.060 | 0.852 | 0.256 |
| `ensemble_geo2` | 2024 | 3,614 | 245 | 7 | 0.029 | 0.298 | 0.023 |
| `ensemble_geo2` | 2025 | 3,859 | 247 | 14 | 0.057 | 0.724 | 0.249 |
| `ensemble_geo2` | 2026 | 4,106 | 150 | 13 | 0.087 | 0.574 | 0.101 |

## Read before quoting

- `n_eff` ≈ samples / h: the test split carries ~128 independent observations, and fewer positives.
- The null prices ONE run; the grid is several runs (NUL-1). Quote the val-chosen row.
- The base rate drifts across splits (see the first table), so a fixed probability threshold does not travel; the val-F1 threshold is reported for that reason.
