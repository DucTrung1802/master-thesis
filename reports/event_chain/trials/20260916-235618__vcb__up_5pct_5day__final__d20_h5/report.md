# Event report — VCB `up_5pct_5day`

**Event:** 1 when close_adjust[t+5] >= (1 + 5%) x close_adjust[t], else 0; NULL for the last 5 sessions. **Window** d=20, **purge** 24 samples. **Table** `unified_schema_vcb.up_5pct_5day__final__d20_h5` → dataset `vcb__up_5pct_5day__final__d20_h5__tr70_val15_test15__std` (hash `8407b2d3ec0fa04b`).

| split | label dates | samples | positives | base rate |
|---|---|---|---|---|
| train | 2009-07-27 → 2021-05-18 | 2,946 | 395 | 0.134 |
| val | 2021-06-22 → 2023-12-05 | 617 | 51 | 0.083 |
| test | 2024-01-10 → 2026-08-14 | 641 | 33 | 0.051 |

Feature selection read only rows before **2021-06-22** (the val start). Features in the model: **82**.

## Selection, one pool per run (null = block-shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__news_daily | 14 | 14 | 0.0574 | 0.0250 | 0.0346 | 3.22 | yes |
| pool__event_features | 40 | 37 | 0.1186 | 0.0633 | 0.0701 | 2.58 | yes |
| pool__economy_vietnam | 84 | 45 | 0.0471 | 0.0280 | 0.0297 | 2.53 | yes |
| pool__basic | 78 | 55 | 0.0637 | 0.0602 | 0.0669 | 1.50 | yes |
| pool__stock_market | 133 | 66 | 0.0657 | 0.0616 | 0.0696 | 1.36 | yes |
| pool__market_breadth | 7 | 7 | 0.0123 | 0.0336 | 0.0477 | 0.90 | no |

## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)

| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall |
|---|---|---|---|---|---|---|---|---|---|
| `forest_et_leaf50` | 0.700 | **0.564** | 0.661 / 0.795 | 0.69 | 0.061 (1.18x) | 0.031 (0.61x) | 0.031 | -0.078 | 67 / 0.045 / 0.091 |
| `blend_top3` | 0.696 | **0.546** | 0.652 / 0.765 | 0.57 | 0.080 (1.55x) | 0.031 (0.61x) | 0.062 | -0.272 | 81 / 0.025 / 0.061 |
| `baseline_logistic_stats_c001` | 0.676 | **0.406** | 0.648 / 0.774 | -1.00 | 0.044 (0.85x) | 0.031 (0.61x) | 0.031 | -1.286 | 71 / 0.028 / 0.061 |
| `lstm_h16` | 0.665 | **0.638** | 0.635 / 0.808 | 1.61 | 0.083 (1.62x) | 0.125 (2.43x) | 0.062 | -0.489 | 327 / 0.076 / 0.758 |
| `gbt_d2` | 0.611 | **0.559** | 0.643 / 0.759 | 0.70 | 0.063 (1.23x) | 0.078 (1.52x) | 0.031 | -0.233 | 80 / 0.075 / 0.182 |
| `baseline_logistic_channel` | 0.528 | **0.533** | 0.660 / 0.810 | 0.38 | 0.054 (1.05x) | 0.031 (0.61x) | 0.000 | 0.030 | 151 / 0.046 / 0.212 |
| `baseline_prior` | 0.500 | **0.500** | 0.500 / 0.500 | — | 0.051 (1.00x) | 0.062 (1.21x) | 0.125 | 0.000 | 641 / 0.051 / 1.000 |

**Best on val:** `forest_et_leaf50` — val AUC 0.700, **test AUC 0.564** against a block-shuffled null p95 of 0.661 (max 0.795, z 0.69); test PR-AUC 0.061 on a base rate of 0.051; of the 67 test sessions over the val threshold, 0.045 were followed by the event.

## Walk-forward — yearly expanding refits over years the selection never read

| model | year | train n | test n | positives | base rate | AUC | PR-AUC |
|---|---|---|---|---|---|---|---|
| `forest_et_leaf50` | 2021 | 2,922 | 137 | 7 | 0.051 | 0.458 | 0.091 |
| `forest_et_leaf50` | 2022 | 3,059 | 249 | 30 | 0.120 | 0.669 | 0.181 |
| `forest_et_leaf50` | 2023 | 3,308 | 231 | 14 | 0.061 | 0.664 | 0.236 |
| `forest_et_leaf50` | 2024 | 3,539 | 244 | 6 | 0.025 | 0.533 | 0.029 |
| `forest_et_leaf50` | 2025 | 3,783 | 247 | 14 | 0.057 | 0.499 | 0.137 |
| `forest_et_leaf50` | 2026 | 4,030 | 150 | 13 | 0.087 | 0.395 | 0.072 |
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
