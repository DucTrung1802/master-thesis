# Event report — VCB `up_5pct_5day`

**Event:** 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions. **Window** d=20, **purge** 24 samples. **Table** `unified_schema_vcb.up_5pct_5day__final__d20_h5__event` → dataset `vcb__up_5pct_5day__final__d20_h5__event__tr70_val15_test15__std` (hash `7f7422b6815ee9d3`).

| split | label dates | samples | positives | base rate |
|---|---|---|---|---|
| train | 2009-07-27 → 2021-05-18 | 2,946 | 395 | 0.134 |
| val | 2021-06-22 → 2023-12-05 | 617 | 51 | 0.083 |
| test | 2024-01-10 → 2026-08-14 | 641 | 33 | 0.051 |

Feature selection read only rows before **2021-06-22** (the val start). Features in the model: **11**.

## Selection, one pool per run (null = block-shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__news_daily | 14 | 14 | 0.0574 | 0.0250 | 0.0346 | 3.22 | yes |
| pool__event_features | 40 | 37 | 0.1186 | 0.0633 | 0.0701 | 2.58 | yes |
| pool__economy_vietnam | 84 | 45 | 0.0471 | 0.0280 | 0.0297 | 2.53 | yes |
| pool__basic_bank | 443 | 232 | 0.0784 | 0.0596 | 0.0636 | 1.57 | yes |
| pool__basic | 78 | 55 | 0.0637 | 0.0602 | 0.0669 | 1.50 | yes |
| pool__stock_market | 133 | 66 | 0.0657 | 0.0616 | 0.0696 | 1.36 | yes |
| pool__market_breadth | 7 | 7 | 0.0123 | 0.0336 | 0.0477 | 0.90 | no |
| pool__fa | 156 | 78 | 0.0012 | 0.0346 | 0.0391 | 0.14 | no |
| pool__funds | 133 | 58 | -0.0220 | 0.0246 | 0.0277 | -1.23 | no |
| pool__bonds | 117 | 44 | -0.0593 | 0.0605 | 0.0723 | -1.54 | no |

## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)

| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall |
|---|---|---|---|---|---|---|---|---|---|
| `forest_et_leaf20` | 0.719 | **0.520** | 0.646 / 0.727 | 0.25 | 0.057 (1.10x) | 0.047 (0.91x) | 0.062 | -0.089 | 104 / 0.058 / 0.182 |
| `blend_top3` | 0.707 | **0.505** | 0.652 / 0.733 | 0.08 | 0.055 (1.06x) | 0.047 (0.91x) | 0.062 | -0.110 | 126 / 0.071 / 0.273 |
| `forest_et_leaf50` | 0.694 | **0.477** | 0.648 / 0.749 | -0.20 | 0.053 (1.02x) | 0.062 (1.21x) | 0.000 | -0.058 | 79 / 0.051 / 0.121 |
| `forest_rf_leaf30` | 0.688 | **0.514** | 0.651 / 0.756 | 0.15 | 0.057 (1.11x) | 0.031 (0.61x) | 0.062 | -0.207 | 271 / 0.055 / 0.455 |
| `baseline_logistic_stats_c01` | 0.685 | **0.352** | 0.640 / 0.763 | -1.58 | 0.043 (0.83x) | 0.031 (0.61x) | 0.062 | -0.248 | 155 / 0.026 / 0.121 |
| `cnn_c16` | 0.676 | **0.496** | 0.672 / 0.814 | -0.16 | 0.051 (0.98x) | 0.031 (0.61x) | 0.000 | -0.307 | 111 / 0.027 / 0.091 |
| `baseline_logistic_stats_c001` | 0.661 | **0.367** | 0.644 / 0.764 | -1.41 | 0.043 (0.83x) | 0.047 (0.91x) | 0.062 | -0.184 | 63 / 0.048 / 0.091 |
| `gru_h16` | 0.657 | **0.455** | 0.643 / 0.752 | -0.37 | 0.049 (0.96x) | 0.031 (0.61x) | 0.062 | -0.118 | 145 / 0.021 / 0.091 |
| `lstm_h16` | 0.642 | **0.541** | 0.635 / 0.767 | 0.54 | 0.056 (1.08x) | 0.031 (0.61x) | 0.031 | -0.154 | 84 / 0.024 / 0.061 |
| `gbt_d2` | 0.611 | **0.503** | 0.637 / 0.749 | 0.12 | 0.052 (1.01x) | 0.031 (0.61x) | 0.062 | -0.243 | 309 / 0.055 / 0.515 |
| `gbt_d3` | 0.610 | **0.514** | 0.645 / 0.726 | 0.21 | 0.054 (1.04x) | 0.031 (0.61x) | 0.062 | -0.326 | 280 / 0.046 / 0.394 |
| `tcn_c16` | 0.604 | **0.408** | 0.647 / 0.809 | -0.94 | 0.044 (0.85x) | 0.047 (0.91x) | 0.031 | 0.020 | 140 / 0.029 / 0.121 |
| `gbt_d4` | 0.597 | **0.487** | 0.647 / 0.758 | -0.13 | 0.051 (0.99x) | 0.031 (0.61x) | 0.062 | -0.260 | 357 / 0.048 / 0.515 |
| `mlp_h16` | 0.593 | **0.381** | 0.645 / 0.774 | -1.21 | 0.042 (0.81x) | 0.031 (0.61x) | 0.031 | -0.241 | 43 / 0.047 / 0.061 |
| `baseline_logistic_channel` | 0.528 | **0.533** | 0.660 / 0.810 | 0.38 | 0.054 (1.05x) | 0.031 (0.61x) | 0.000 | 0.030 | 151 / 0.046 / 0.212 |
| `baseline_prior` | 0.500 | **0.500** | 0.500 / 0.500 | — | 0.051 (1.00x) | 0.062 (1.21x) | 0.125 | 0.000 | 641 / 0.051 / 1.000 |

**Best on val:** `forest_et_leaf20` — val AUC 0.719, **test AUC 0.520** against a block-shuffled null p95 of 0.646 (max 0.727, z 0.25); test PR-AUC 0.057 on a base rate of 0.051; of the 104 test sessions over the val threshold, 0.058 were followed by the event.

## Walk-forward — yearly expanding refits over years the selection never read

| model | year | train n | test n | positives | base rate | AUC | PR-AUC |
|---|---|---|---|---|---|---|---|
| `forest_et_leaf20` | 2021 | 2,922 | 137 | 7 | 0.051 | 0.666 | 0.120 |
| `forest_et_leaf20` | 2022 | 3,059 | 249 | 30 | 0.120 | 0.641 | 0.178 |
| `forest_et_leaf20` | 2023 | 3,308 | 231 | 14 | 0.061 | 0.727 | 0.242 |
| `forest_et_leaf20` | 2024 | 3,539 | 244 | 6 | 0.025 | 0.214 | 0.018 |
| `forest_et_leaf20` | 2025 | 3,783 | 247 | 14 | 0.057 | 0.581 | 0.089 |
| `forest_et_leaf20` | 2026 | 4,030 | 150 | 13 | 0.087 | 0.485 | 0.086 |
| `baseline_logistic_channel` | 2021 | 2,922 | 137 | 7 | 0.051 | 0.511 | 0.111 |
| `baseline_logistic_channel` | 2022 | 3,059 | 249 | 30 | 0.120 | 0.380 | 0.098 |
| `baseline_logistic_channel` | 2023 | 3,308 | 231 | 14 | 0.061 | 0.619 | 0.082 |
| `baseline_logistic_channel` | 2024 | 3,539 | 244 | 6 | 0.025 | 0.720 | 0.047 |
| `baseline_logistic_channel` | 2025 | 3,783 | 247 | 14 | 0.057 | 0.499 | 0.064 |
| `baseline_logistic_channel` | 2026 | 4,030 | 150 | 13 | 0.087 | 0.298 | 0.063 |

## Read before quoting

- `n_eff` ≈ samples / h: the test split carries ~128 independent observations, and fewer positives.
- The null prices ONE run; the grid is several runs (NUL-1). Quote the val-chosen row.
- The base rate drifts across splits (see the first table), so a fixed probability threshold does not travel; the val-F1 threshold is reported for that reason.
