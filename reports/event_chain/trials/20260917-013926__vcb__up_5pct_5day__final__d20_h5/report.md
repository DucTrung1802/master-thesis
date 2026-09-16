# Event report — VCB `up_5pct_5day`

**Event:** 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions. **Window** d=20, **purge** 24 samples. **Table** `unified_schema_vcb.up_5pct_5day__final__d20_h5` → dataset `vcb__up_5pct_5day__final__d20_h5__tr70_val15_test15__std` (hash `15720a134545481e`).

| split | label dates | samples | positives | base rate |
|---|---|---|---|---|
| train | 2009-07-27 → 2021-05-18 | 2,946 | 395 | 0.134 |
| val | 2021-06-22 → 2023-12-05 | 617 | 51 | 0.083 |
| test | 2024-01-10 → 2026-08-14 | 641 | 33 | 0.051 |

Feature selection read only rows before **2021-06-22** (the val start). Features in the model: **183**.

## Selection, one pool per run (null = block-shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__news_daily | 14 | 14 | 0.0574 | 0.0250 | 0.0346 | 3.22 | yes |
| pool__event_features | 40 | 37 | 0.1186 | 0.0633 | 0.0701 | 2.58 | yes |
| pool__economy_vietnam | 84 | 45 | 0.0471 | 0.0280 | 0.0297 | 2.53 | yes |
| pool__basic_bank | 443 | 232 | 0.0784 | 0.0596 | 0.0636 | 1.57 | yes |
| pool__basic | 78 | 55 | 0.0637 | 0.0602 | 0.0669 | 1.50 | yes |
| pool__ta | 717 | 428 | 0.0579 | 0.0681 | 0.0821 | 1.37 | no |
| pool__stock_market | 133 | 66 | 0.0657 | 0.0616 | 0.0696 | 1.36 | yes |
| pool__market_breadth | 7 | 7 | 0.0123 | 0.0336 | 0.0477 | 0.90 | no |
| pool__fa | 156 | 78 | 0.0012 | 0.0346 | 0.0391 | 0.14 | no |
| pool__funds | 133 | 58 | -0.0220 | 0.0246 | 0.0277 | -1.23 | no |
| pool__bonds | 117 | 44 | -0.0593 | 0.0605 | 0.0723 | -1.54 | no |

## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)

| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall |
|---|---|---|---|---|---|---|---|---|---|
| `forest_rf_leaf30` | 0.694 | **0.549** | 0.661 / 0.747 | 0.54 | 0.088 (1.72x) | 0.031 (0.61x) | 0.062 | -0.411 | 126 / 0.063 / 0.242 |
| `blend_top3` | 0.687 | **0.536** | 0.659 / 0.742 | 0.42 | 0.058 (1.13x) | 0.031 (0.61x) | 0.062 | -0.329 | 132 / 0.061 / 0.242 |
| `forest_et_leaf50` | 0.668 | **0.519** | 0.657 / 0.746 | 0.22 | 0.053 (1.02x) | 0.031 (0.61x) | 0.000 | -0.136 | 69 / 0.029 / 0.061 |
| `forest_et_leaf20` | 0.663 | **0.515** | 0.652 / 0.767 | 0.22 | 0.057 (1.11x) | 0.031 (0.61x) | 0.062 | -0.498 | 412 / 0.061 / 0.758 |
| `gbt_d2` | 0.627 | **0.476** | 0.650 / 0.772 | -0.21 | 0.056 (1.09x) | 0.078 (1.52x) | 0.062 | -0.076 | 84 / 0.083 / 0.212 |
| `gbt_d3` | 0.621 | **0.456** | 0.650 / 0.751 | -0.48 | 0.061 (1.19x) | 0.078 (1.52x) | 0.062 | -0.071 | 70 / 0.071 / 0.152 |
| `gbt_d4` | 0.612 | **0.492** | 0.643 / 0.722 | 0.05 | 0.061 (1.19x) | 0.031 (0.61x) | 0.062 | 0.009 | 75 / 0.027 / 0.061 |
| `lstm_h16` | 0.539 | **0.542** | 0.585 / 0.670 | 0.75 | 0.059 (1.14x) | 0.062 (1.21x) | 0.000 | -4.867 | 564 / 0.059 / 1.000 |
| `gru_h16` | 0.533 | **0.521** | 0.591 / 0.690 | 0.39 | 0.057 (1.11x) | 0.000 (0.00x) | 0.000 | -0.047 | 575 / 0.057 / 1.000 |
| `baseline_logistic_stats_c01` | 0.529 | **0.500** | 0.500 / 0.500 | — | 0.051 (1.00x) | 0.062 (1.21x) | 0.125 | 0.075 | 0 / — / 0.000 |
| `baseline_logistic_channel` | 0.528 | **0.533** | 0.660 / 0.810 | 0.38 | 0.054 (1.05x) | 0.031 (0.61x) | 0.000 | 0.030 | 151 / 0.046 / 0.212 |
| `baseline_logistic_stats_c001` | 0.516 | **0.480** | 0.544 / 0.608 | -0.68 | 0.051 (1.00x) | 0.062 (1.21x) | 0.000 | -0.598 | 24 / 0.000 / 0.000 |
| `baseline_prior` | 0.500 | **0.500** | 0.500 / 0.500 | — | 0.051 (1.00x) | 0.062 (1.21x) | 0.125 | 0.000 | 641 / 0.051 / 1.000 |
| `tcn_c16` | 0.464 | **0.383** | 0.639 / 0.787 | -1.32 | 0.042 (0.81x) | 0.016 (0.30x) | 0.000 | 0.067 | 521 / 0.042 / 0.667 |
| `mlp_h16` | 0.458 | **0.503** | 0.503 / 0.503 | 0.34 | 0.052 (1.01x) | 0.062 (1.21x) | 0.125 | -15.931 | 637 / 0.052 / 1.000 |
| `cnn_c16` | 0.448 | **0.354** | 0.659 / 0.798 | -1.57 | 0.038 (0.73x) | 0.000 (0.00x) | 0.000 | 0.023 | 0 / — / 0.000 |

**Best on val:** `forest_rf_leaf30` — val AUC 0.694, **test AUC 0.549** against a block-shuffled null p95 of 0.661 (max 0.747, z 0.54); test PR-AUC 0.088 on a base rate of 0.051; of the 126 test sessions over the val threshold, 0.063 were followed by the event.

## Walk-forward — yearly expanding refits over years the selection never read

| model | year | train n | test n | positives | base rate | AUC | PR-AUC |
|---|---|---|---|---|---|---|---|
| `forest_rf_leaf30` | 2021 | 2,922 | 137 | 7 | 0.051 | 0.546 | 0.149 |
| `forest_rf_leaf30` | 2022 | 3,059 | 249 | 30 | 0.120 | 0.674 | 0.240 |
| `forest_rf_leaf30` | 2023 | 3,308 | 231 | 14 | 0.061 | 0.573 | 0.222 |
| `forest_rf_leaf30` | 2024 | 3,539 | 244 | 6 | 0.025 | 0.464 | 0.026 |
| `forest_rf_leaf30` | 2025 | 3,783 | 247 | 14 | 0.057 | 0.576 | 0.122 |
| `forest_rf_leaf30` | 2026 | 4,030 | 150 | 13 | 0.087 | 0.559 | 0.097 |
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
