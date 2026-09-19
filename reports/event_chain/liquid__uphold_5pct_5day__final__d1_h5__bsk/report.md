# Basket report — LIQUID: the top 5 names per session for `uphold_5pct_5day`

**Question:** at the close of session N, which names — at most **5** — will close session N+5 at least **5 %** higher? **Event:** 1 when close_adjust[t+5] >= (1 + 5%) x open_adjust[t+1] — bought at the OPEN of t+1 and sold at the close of t+5, 5 sessions held — else 0; NULL for the last 5 sessions. **Table** `unified_schema_liquid.uphold_5pct_5day__final__d1_h5__bsk` → dataset `liquid__uphold_5pct_5day__final__d1_h5__bsk__tr70_val15_test15__std` (hash `65a64d2fbb4463b4`), 151 channels, purge 5 sessions. A name at its exchange ceiling on N IS bought (`BASKET_EXCLUDE_CEILING = False`).

| split | label dates | rows | base rate |
|---|---|---|---|
| train | 2009-01-02 → 2021-04-22 | 419,923 | 0.164 |
| val | 2021-05-04 → 2023-12-07 | 139,928 | 0.188 |
| test | 2023-12-15 → 2026-08-14 | 147,536 | 0.117 |

Feature selection read only rows before **2021-05-04** (the val start).

## Selection, one pool per run (within-date IC; null = shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__event_features | 67 | 59 | 0.1271 | 0.0503 | 0.0509 | 26.65 | yes |
| pool__basic | 84 | 56 | 0.1325 | 0.0455 | 0.0476 | 14.59 | yes |

## Leaderboard — chosen on `val_daily_auc` (the other VAL metrics break a tie), test read once

⚠️ **`val daily AUC` / `test daily AUC` are the CHOICE metric** — the ROC-AUC computed inside each session and averaged, which is the only comparison a basket makes. The pooled columns beside them also score names ACROSS sessions and read ~0.02 higher.

`hit` = share of the basket that rose ≥ 5 %; `base` = the same share over all buyable names; null = 200 random baskets per session. Refit = train+val once; rolling = refitted every 63 test sessions.

| model | **val daily AUC** | val AUC (pooled) | val hit | **test daily AUC** | test AUC (pooled) | refit daily · pooled | rolling daily · pooled | **test hit** | test base | null p95 / max · z | lift | all-hit | basket ret | universe ret | Sharpe@50 (CAGR) | refit hit (lift) | rolling hit (lift) | rolling Sharpe@50 (CAGR) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ensemble_xl` | **0.643** | 0.656 | 0.342 | **0.644** | 0.662 | 0.642 · 0.664 | 0.645 · 0.665 | **0.232** | 0.117 | 0.126 / 0.131 · 21.66 | 1.99 | 0.009 | +0.78 % | +0.14 % | 0.51 (+11.3 %) | 0.234 (2.00) | 0.236 (2.02) | 0.04 (-4.1 %) |
| `event_boost_xgb_d8` | **0.639** | 0.650 | 0.327 | **0.644** | 0.659 | 0.643 · 0.663 | 0.645 · 0.661 | **0.237** | 0.117 | 0.126 / 0.131 · 22.58 | 2.03 | 0.015 | +0.90 % | +0.14 % | 0.40 (+8.0 %) | 0.234 (2.00) | 0.237 (2.03) | -0.03 (-7.0 %) |
| `event_linear_evt_c003` | **0.635** | 0.643 | 0.328 | **0.630** | 0.648 | 0.630 · 0.647 | 0.634 · 0.652 | **0.212** | 0.117 | 0.126 / 0.131 · 18.01 | 1.82 | 0.006 | +0.36 % | +0.14 % | -0.20 (-10.4 %) | 0.208 (1.79) | 0.221 (1.89) | -0.09 (-7.5 %) |
| `gbt_d4` | **0.634** | 0.652 | 0.327 | **0.636** | 0.652 | 0.635 · 0.657 | — · — | **0.221** | 0.117 | 0.126 / 0.131 · 19.61 | 1.89 | 0.009 | +0.68 % | +0.14 % | 0.06 (-2.9 %) | 0.221 (1.89) | — (—) | — (—) |
| `ensemble_geo2t` | **0.634** | 0.654 | 0.324 | **0.631** | 0.654 | 0.631 · 0.656 | — · — | **0.225** | 0.117 | 0.126 / 0.131 · 20.35 | 1.93 | 0.008 | +0.43 % | +0.14 % | -0.23 (-11.1 %) | 0.221 (1.89) | — (—) | — (—) |
| `forest_rf200_d12` | **0.632** | 0.651 | 0.320 | **0.637** | 0.657 | 0.637 · 0.661 | — · — | **0.229** | 0.117 | 0.126 / 0.131 · 21.15 | 1.96 | 0.009 | +0.57 % | +0.14 % | 0.11 (-1.8 %) | 0.227 (1.94) | — (—) | — (—) |
| `ensemble_geo3` | **0.630** | 0.644 | 0.329 | **0.626** | 0.645 | 0.627 · 0.647 | — · — | **0.215** | 0.117 | 0.126 / 0.131 · 18.41 | 1.84 | 0.006 | +0.40 % | +0.14 % | -0.20 (-9.8 %) | 0.212 (1.81) | — (—) | — (—) |
| `ensemble_geo2` | **0.630** | 0.648 | 0.320 | **0.626** | 0.649 | 0.626 · 0.650 | — · — | **0.216** | 0.117 | 0.126 / 0.131 · 18.64 | 1.85 | 0.009 | +0.23 % | +0.14 % | -0.16 (-10.1 %) | 0.219 (1.87) | — (—) | — (—) |
| `gbt_d2` | **0.627** | 0.653 | 0.320 | **0.631** | 0.652 | 0.631 · 0.654 | — · — | **0.228** | 0.117 | 0.126 / 0.131 · 20.92 | 1.95 | 0.014 | +0.52 % | +0.14 % | 0.00 (-5.1 %) | 0.227 (1.94) | — (—) | — (—) |
| `event_linear_mag_har_a100_hl4` | **0.618** | 0.645 | 0.318 | **0.614** | 0.641 | 0.615 · 0.643 | — · — | **0.219** | 0.117 | 0.126 / 0.131 · 19.21 | 1.87 | 0.009 | +0.16 % | +0.14 % | -0.40 (-17.6 %) | 0.223 (1.91) | — (—) | — (—) |
| `event_linear_dir_c001` | **0.501** | 0.503 | 0.144 | **0.502** | 0.519 | 0.511 · 0.522 | — · — | **0.070** | 0.117 | 0.126 / 0.131 · -8.81 | 0.60 | 0.002 | +0.59 % | +0.14 % | 0.08 (+0.3 %) | 0.066 (0.57) | — (—) | — (—) |
| `baseline_prior` | **0.500** | 0.500 | 0.149 | **0.500** | 0.500 | — · — | — · — | **0.097** | 0.117 | 0.126 / 0.131 · -3.62 | 0.83 | 0.002 | +0.16 % | +0.14 % | -0.97 (-16.8 %) | — (—) | — (—) | — (—) |

**Chosen on val (`val_daily_auc`):** `ensemble_xl` — val within-session AUC 0.643 (pooled 0.656), hit@5 0.342. **Test within-session AUC 0.644** (within-session label-shuffle null p95 0.506, z 43.81; refitted on train+val 0.642, refitted every 63 sessions 0.645). The POOLED AUC, which also prices knowing WHICH SESSIONS are eventful and is not what a basket trades, reads 0.662 on test (null p95 0.553, z 56.52; refit 0.664, rolling 0.665). **On test, 0.232 of its basket names rose ≥ 5 %** against a buyable base rate of 0.117 (lift 1.99×) and a random-basket null p95 of 0.126 (max 0.131, z 21.66); every name hit on 0.009 of sessions; the basket's mean 5-session return was +0.78 % against the universe's +0.14 %, and the equal-weight basket itself was ≥ 5 % on 0.153 of sessions. Traded every 5 sessions at 50 bps a round trip: Sharpe 0.51, CAGR +11.3 %, max drawdown -31.5 % (universe Sharpe 0.55, CAGR +7.4 %). Refitted on train+val: hit 0.234, Sharpe@50 0.25. Refitted every 63 sessions: hit 0.236 (null p95 0.126), Sharpe@50 0.04, CAGR -4.1 %.

The test baskets themselves — date, rank, ticker, score, outcome — are `baskets_test_{frozen,refit,rolling}.csv`; `python -m event_chain.basket --pick <date>` answers one session.

## At most 5 names — a name under P(event) 0.30 is not bought

The cut is chosen on VAL (`BASKET_MIN_PROB_ON = "ev"`: the mean net return per session at 50 bps, a cash session returning 0, among cuts trading at least 25 val sessions): **0.30** — on val it traded 0.703 of sessions, 3.73 names each, precision 0.381 (base on those sessions 0.206), EV +1.58 % per session. A session none of whose top 5 reaches the cut holds cash. Null: the same number of names per active session, drawn at random (200 draws). Sharpe/CAGR: one basket every 5 sessions, cash when idle, mean over the 5 start offsets (the worst in brackets).

| test basket | cut | active (share) | names | **precision** (null p95 · z) | base on active | p_mean | basket ≥ 5 % (null p95) | loss rate | basket ret (null p95) | universe ret | EV/session | Sharpe@50 (worst) | CAGR | max DD | without ceiling names: precision · ret · Sharpe |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| frozen | 0.00 | 659 (1.000) | 5.00 | **0.232** (0.126 · 21.66) | 0.117 | 0.265 | 0.153 (0.064) | 0.454 | +0.78 % (+0.25 %) | +0.14 % | +0.28 % | 0.40 (0.04) | +9.1 % | -39.7 % | 0.225 · +0.68 % · 0.25 |
| frozen | 0.30 | 278 (0.422) | 3.09 | **0.289** (0.170 · 12.48) | 0.135 | 0.359 | 0.273 (0.122) | 0.442 | +1.98 % (+0.66 %) | +0.30 % | +0.62 % | 0.65 (0.43) | +24.9 % | -43.6 % | 0.287 · +1.95 % · 0.62 |
| refit | 0.00 | 659 (1.000) | 5.00 | **0.234** (0.126 · 22.00) | 0.117 | 0.265 | 0.170 (0.064) | 0.455 | +0.59 % (+0.25 %) | +0.14 % | +0.09 % | 0.13 (-0.31) | -1.4 % | -46.4 % | 0.226 · +0.42 % · -0.11 |
| refit | 0.30 | 299 (0.454) | 3.17 | **0.283** (0.168 · 11.82) | 0.138 | 0.361 | 0.251 (0.120) | 0.475 | +1.54 % (+0.73 %) | +0.37 % | +0.47 % | 0.56 (-0.06) | +19.6 % | -39.0 % | 0.286 · +1.46 % · 0.47 |
| rolling | 0.00 | 659 (1.000) | 5.00 | **0.236** (0.126 · 22.46) | 0.117 | 0.266 | 0.178 (0.064) | 0.452 | +0.69 % (+0.25 %) | +0.14 % | +0.19 % | 0.25 (-0.05) | +3.2 % | -45.6 % | 0.229 · +0.61 % · 0.13 |
| rolling | 0.30 | 323 (0.490) | 3.04 | **0.291** (0.172 · 12.34) | 0.137 | 0.356 | 0.297 (0.118) | 0.455 | +1.45 % (+0.65 %) | +0.36 % | +0.47 % | 0.53 (0.27) | +16.8 % | -44.0 % | 0.285 · +1.47 % · 0.52 |

Per year, at the chosen cut:

- frozen: 2023 11 sessions, precision 0.233, basket +2.01 %, ≥ 5 % on 0.09; 2024 37 sessions, precision 0.184, basket +0.61 %, ≥ 5 % on 0.16; 2025 157 sessions, precision 0.331, basket +2.42 %, ≥ 5 % on 0.32; 2026 73 sessions, precision 0.269, basket +1.70 %, ≥ 5 % on 0.25
- refit: 2023 9 sessions, precision 0.125, basket +0.94 %, ≥ 5 % on 0.00; 2024 60 sessions, precision 0.190, basket +0.42 %, ≥ 5 % on 0.15; 2025 150 sessions, precision 0.350, basket +3.09 %, ≥ 5 % on 0.33; 2026 80 sessions, precision 0.253, basket -0.46 %, ≥ 5 % on 0.20
- rolling: 2023 9 sessions, precision 0.125, basket +0.94 %, ≥ 5 % on 0.00; 2024 66 sessions, precision 0.190, basket +0.63 %, ≥ 5 % on 0.20; 2025 169 sessions, precision 0.351, basket +2.48 %, ≥ 5 % on 0.37; 2026 79 sessions, precision 0.261, basket +0.00 %, ≥ 5 % on 0.27

The grid (⚠️ DESCRIPTIVE on test: only the val-chosen cut is the result) — active share · precision · basket ret · Sharpe:

| cut | val | test_frozen | test_refit | test_rolling |
|---|---|---|---|---|
| 0.00 | 1.00 · 0.342 · +2.0 % · 1.42 | 1.00 · 0.232 · +0.8 % · 0.40 | 1.00 · 0.234 · +0.6 % · 0.13 | 1.00 · 0.236 · +0.7 % · 0.25 |
| 0.20 | 1.00 · 0.340 · +1.9 % · 1.33 | 0.94 · 0.241 · +0.8 % · 0.40 | 0.89 · 0.246 · +0.6 % · 0.06 | 0.90 · 0.252 · +0.9 % · 0.38 |
| 0.25 | 0.93 · 0.355 · +1.9 % · 1.18 | 0.71 · 0.254 · +1.1 % · 0.54 | 0.70 · 0.258 · +0.7 % · 0.13 | 0.71 · 0.263 · +0.9 % · 0.29 |
| 0.30 ⬅ | 0.70 · 0.381 · +2.8 % · 1.48 | 0.42 · 0.289 · +2.0 % · 0.65 | 0.45 · 0.283 · +1.5 % · 0.56 | 0.49 · 0.291 · +1.5 % · 0.53 |
| 0.35 | 0.46 · 0.392 · +3.3 % · 1.28 | 0.20 · 0.319 · +3.2 % · 0.68 | 0.24 · 0.337 · +2.6 % · 0.65 | 0.25 · 0.347 · +2.3 % · 0.47 |
| 0.40 | 0.30 · 0.419 · +2.8 % · 0.88 | 0.09 · 0.368 · +3.6 % · 0.41 | 0.11 · 0.389 · +2.0 % · 0.14 | 0.12 · 0.429 · +3.7 % · 0.50 |
| 0.45 | 0.17 · 0.456 · +3.7 % · 0.78 | 0.03 · 0.385 · +3.5 % · 0.40 | 0.04 · 0.457 · +2.8 % · 0.32 | 0.04 · 0.508 · +1.6 % · 0.25 |
| 0.50 | 0.10 · 0.535 · +6.1 % · 1.03 | 0.02 · 0.405 · +2.2 % · 0.32 | 0.02 · 0.514 · +2.1 % · 0.23 | 0.02 · 0.536 · +5.0 % · 0.44 |

`min_prob.csv` holds the whole grid; `min_prob.json` the cut `--pick` applies.

## Walk-forward — yearly expanding refits of `ensemble_xl`

| year | train rows | sessions | hit | base | lift | null p95 · z | basket ret | universe ret | daily AUC | Sharpe@50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 2021 | 419,923 | 172 | 0.438 | 0.236 | 1.86 | 0.257 · 16.61 | +4.71 % | +1.52 % | 0.648 | 6.38 |
| 2022 | 455,010 | 249 | 0.332 | 0.191 | 1.74 | 0.205 · 15.27 | +0.72 % | -0.74 % | 0.617 | 0.30 |
| 2023 | 508,266 | 244 | 0.287 | 0.148 | 1.94 | 0.164 · 14.88 | +1.53 % | +0.51 % | 0.664 | 1.31 |
| 2024 | 561,165 | 250 | 0.213 | 0.106 | 2.01 | 0.118 · 13.76 | +0.76 % | +0.23 % | 0.645 | 0.55 |
| 2025 | 616,314 | 248 | 0.282 | 0.131 | 2.15 | 0.145 · 16.17 | +1.26 % | +0.28 % | 0.641 | 0.43 |
| 2026 | 672,461 | 150 | 0.205 | 0.111 | 1.85 | 0.131 · 8.58 | -0.25 % | -0.36 % | 0.649 | -0.56 |

## Read before quoting

- `n_eff` ≈ sessions / h: the test split's 659 sessions are ~132 independent baskets, and the trading track 132 periods.
- The null prices ONE run against random baskets; the grid is several runs and the kinds were chosen on VCB (NUL-1). Quote the val-chosen row.
- The universe is survivors-only and its membership is not point-in-time (CLAUDE.md §2c): that protects the z, not the return.
- A hit rate rewards volatile names; read the basket return beside it.
