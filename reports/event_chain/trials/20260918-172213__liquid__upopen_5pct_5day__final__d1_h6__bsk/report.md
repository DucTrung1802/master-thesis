# Basket report — LIQUID: the top 5 names per session for `upopen_5pct_5day`

**Question:** at the close of session N, which names — at most **5** — will close session N+5 at least **5 %** higher? **Event:** 1 when close_adjust[t+6] >= (1 + 5%) x open_adjust[t+1] — bought at the OPEN of t+1 and sold 5 sessions later — else 0; NULL for the last 6 sessions. **Table** `unified_schema_liquid.upopen_5pct_5day__final__d1_h6__bsk` → dataset `liquid__upopen_5pct_5day__final__d1_h6__bsk__tr70_val15_test15__std` (hash `53228aec2e679487`), 151 channels, purge 6 sessions. A name at its exchange ceiling on N IS bought (`BASKET_EXCLUDE_CEILING = False`).

| split | label dates | rows | base rate |
|---|---|---|---|
| train | 2009-01-02 → 2021-04-19 | 419,503 | 0.182 |
| val | 2021-04-29 → 2023-12-05 | 139,699 | 0.209 |
| test | 2023-12-14 → 2026-08-13 | 147,527 | 0.134 |

Feature selection read only rows before **2021-04-29** (the val start).

## Selection, one pool per run (within-date IC; null = shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__event_features | 67 | 59 | 0.1238 | 0.0500 | 0.0514 | 24.56 | yes |
| pool__basic | 84 | 57 | 0.1268 | 0.0433 | 0.0450 | 21.06 | yes |

## Leaderboard — chosen on `val_daily_auc` (the other VAL metrics break a tie), test read once

⚠️ **`val daily AUC` / `test daily AUC` are the CHOICE metric** — the ROC-AUC computed inside each session and averaged, which is the only comparison a basket makes. The pooled columns beside them also score names ACROSS sessions and read ~0.02 higher.

`hit` = share of the basket that rose ≥ 5 %; `base` = the same share over all buyable names; null = 200 random baskets per session. Refit = train+val once; rolling = refitted every 63 test sessions.

| model | **val daily AUC** | val AUC (pooled) | val hit | **test daily AUC** | test AUC (pooled) | refit daily · pooled | rolling daily · pooled | **test hit** | test base | null p95 / max · z | lift | all-hit | basket ret | universe ret | Sharpe@50 (CAGR) | refit hit (lift) | rolling hit (lift) | rolling Sharpe@50 (CAGR) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ensemble_boost` | **0.636** | 0.650 | 0.352 | **0.632** | 0.655 | 0.631 · 0.658 | 0.632 · 0.658 | **0.266** | 0.134 | 0.141 / 0.150 · 25.34 | 1.99 | 0.014 | +1.01 % | +0.19 % | 0.55 (+15.5 %) | 0.256 (1.91) | 0.273 (2.04) | 0.54 (+14.9 %) |
| `ensemble_xl` | **0.636** | 0.646 | 0.355 | **0.633** | 0.655 | 0.632 · 0.658 | 0.635 · 0.658 | **0.259** | 0.134 | 0.141 / 0.150 · 23.95 | 1.93 | 0.015 | +1.04 % | +0.19 % | 0.42 (+9.6 %) | 0.250 (1.87) | 0.264 (1.97) | 0.77 (+25.0 %) |
| `ensemble_boost_eq` | **0.635** | 0.650 | 0.357 | **0.631** | 0.654 | 0.629 · 0.656 | 0.630 · 0.657 | **0.260** | 0.134 | 0.141 / 0.150 · 24.18 | 1.94 | 0.015 | +0.76 % | +0.19 % | 0.39 (+8.0 %) | 0.255 (1.90) | 0.270 (2.02) | 0.37 (+6.9 %) |
| `event_boost_xgb_d8` | **0.632** | 0.639 | 0.338 | **0.632** | 0.651 | 0.631 · 0.656 | 0.634 · 0.654 | **0.253** | 0.134 | 0.141 / 0.150 · 22.96 | 1.89 | 0.017 | +1.15 % | +0.19 % | 1.02 (+37.8 %) | 0.251 (1.87) | 0.274 (2.05) | 1.09 (+41.7 %) |
| `ensemble_geo2t` | **0.628** | 0.646 | 0.345 | **0.624** | 0.648 | 0.622 · 0.651 | — · — | **0.254** | 0.134 | 0.141 / 0.150 · 23.02 | 1.90 | 0.014 | +0.68 % | +0.19 % | 0.28 (+3.3 %) | 0.240 (1.79) | — (—) | — (—) |
| `event_linear_evt_c003` | **0.627** | 0.633 | 0.341 | **0.623** | 0.643 | 0.623 · 0.643 | 0.624 · 0.646 | **0.232** | 0.134 | 0.141 / 0.150 · 18.85 | 1.73 | 0.009 | +0.52 % | +0.19 % | -0.80 (-31.7 %) | 0.232 (1.74) | 0.237 (1.77) | -0.34 (-18.5 %) |
| `gbt_d4` | **0.627** | 0.644 | 0.339 | **0.627** | 0.645 | 0.623 · 0.648 | — · — | **0.253** | 0.134 | 0.141 / 0.150 · 22.96 | 1.89 | 0.014 | +0.98 % | +0.19 % | 0.68 (+21.1 %) | 0.229 (1.71) | — (—) | — (—) |
| `forest_et_leaf200` | **0.626** | 0.647 | 0.349 | **0.628** | 0.650 | 0.627 · 0.652 | — · — | **0.249** | 0.134 | 0.141 / 0.150 · 22.04 | 1.86 | 0.015 | +0.85 % | +0.19 % | 0.40 (+8.4 %) | 0.249 (1.86) | — (—) | — (—) |
| `ensemble_geo2` | **0.624** | 0.639 | 0.345 | **0.619** | 0.644 | 0.619 · 0.646 | — · — | **0.243** | 0.134 | 0.141 / 0.150 · 21.05 | 1.82 | 0.011 | +0.35 % | +0.19 % | -0.23 (-15.7 %) | 0.244 (1.82) | — (—) | — (—) |
| `ensemble_geo3` | **0.624** | 0.634 | 0.349 | **0.619** | 0.641 | 0.620 · 0.643 | — · — | **0.240** | 0.134 | 0.141 / 0.150 · 20.42 | 1.79 | 0.012 | +0.67 % | +0.19 % | -0.08 (-9.9 %) | 0.235 (1.76) | — (—) | — (—) |
| `event_boost_mag_d8` | **0.622** | 0.638 | 0.325 | **0.615** | 0.622 | 0.610 · 0.618 | 0.612 · 0.630 | **0.259** | 0.134 | 0.141 / 0.150 · 24.06 | 1.94 | 0.011 | +0.55 % | +0.19 % | -0.16 (-15.6 %) | 0.258 (1.93) | 0.263 (1.96) | -0.19 (-16.4 %) |
| `gbt_d2` | **0.622** | 0.648 | 0.339 | **0.623** | 0.646 | 0.622 · 0.647 | — · — | **0.255** | 0.134 | 0.141 / 0.150 · 23.19 | 1.90 | 0.014 | +0.73 % | +0.19 % | 0.24 (+2.0 %) | 0.253 (1.89) | — (—) | — (—) |
| `event_linear_mag_har_a100_hl4` | **0.615** | 0.635 | 0.337 | **0.608** | 0.636 | 0.608 · 0.638 | — · — | **0.248** | 0.134 | 0.141 / 0.150 · 21.98 | 1.86 | 0.011 | +0.34 % | +0.19 % | -0.19 (-15.7 %) | 0.244 (1.82) | — (—) | — (—) |
| `event_linear_dir_c001` | **0.503** | 0.506 | 0.161 | **0.506** | 0.527 | 0.515 · 0.529 | — · — | **0.080** | 0.134 | 0.141 / 0.150 · -10.19 | 0.59 | 0.002 | +0.65 % | +0.19 % | -0.01 (-0.7 %) | 0.077 (0.57) | — (—) | — (—) |
| `baseline_prior` | **0.500** | 0.500 | 0.171 | **0.500** | 0.500 | — · — | — · — | **0.112** | 0.134 | 0.141 / 0.150 · -3.95 | 0.84 | 0.003 | +0.23 % | +0.19 % | -0.79 (-17.5 %) | — (—) | — (—) | — (—) |

**Chosen on val (`val_daily_auc`):** `ensemble_boost` — val within-session AUC 0.636 (pooled 0.650), hit@5 0.352. **Test within-session AUC 0.632** (within-session label-shuffle null p95 0.505, z 44.67; refitted on train+val 0.631, refitted every 63 sessions 0.632). The POOLED AUC, which also prices knowing WHICH SESSIONS are eventful and is not what a basket trades, reads 0.655 on test (null p95 0.548, z 58.04; refit 0.658, rolling 0.658). **On test, 0.266 of its basket names rose ≥ 5 %** against a buyable base rate of 0.134 (lift 1.99×) and a random-basket null p95 of 0.141 (max 0.150, z 25.34); every name hit on 0.014 of sessions; the basket's mean 5-session return was +1.01 % against the universe's +0.19 %, and the equal-weight basket itself was ≥ 5 % on 0.229 of sessions. Traded every 5 sessions at 50 bps a round trip: Sharpe 0.55, CAGR +15.5 %, max drawdown -40.3 % (universe Sharpe 0.24, CAGR +2.8 %). Refitted on train+val: hit 0.256, Sharpe@50 0.73. Refitted every 63 sessions: hit 0.273 (null p95 0.141), Sharpe@50 0.54, CAGR +14.9 %.

The test baskets themselves — date, rank, ticker, score, outcome — are `baskets_test_{frozen,refit,rolling}.csv`; `python -m event_chain.basket --pick <date>` answers one session.

## At most 5 names — a name under P(event) 0.36 is not bought

The cut is chosen on VAL (`BASKET_MIN_PROB_ON = "ev"`: the mean net return per session at 50 bps, a cash session returning 0, among cuts trading at least 25 val sessions): **0.36** — on val it traded 0.510 of sessions, 3.44 names each, precision 0.413 (base on those sessions 0.250), EV +1.76 % per session. A session none of whose top 5 reaches the cut holds cash. Null: the same number of names per active session, drawn at random (200 draws). Sharpe/CAGR: one basket every 5 sessions, cash when idle, mean over the 5 start offsets (the worst in brackets).

| test basket | cut | active (share) | names | **precision** (null p95 · z) | base on active | p_mean | basket ≥ 5 % (null p95) | loss rate | basket ret (null p95) | universe ret | EV/session | Sharpe@50 (worst) | CAGR | max DD | without ceiling names: precision · ret · Sharpe |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| frozen | 0.00 | 659 (1.000) | 5.00 | **0.266** (0.141 · 25.34) | 0.134 | 0.287 | 0.229 (0.080) | 0.445 | +1.01 % (+0.33 %) | +0.19 % | +0.51 % | 0.60 (-0.15) | +19.7 % | -48.4 % | 0.260 · +0.94 % · 0.54 |
| frozen | 0.36 | 161 (0.244) | 2.34 | **0.379** (0.231 · 9.39) | 0.169 | 0.417 | 0.373 (0.193) | 0.429 | +4.37 % (+1.28 %) | +0.70 % | +0.95 % | 0.92 (0.38) | +46.7 % | -36.2 % | 0.378 · +4.03 % · 0.77 |
| refit | 0.00 | 659 (1.000) | 5.00 | **0.256** (0.141 · 23.48) | 0.134 | 0.283 | 0.206 (0.080) | 0.448 | +0.70 % (+0.33 %) | +0.19 % | +0.20 % | 0.24 (-0.47) | +2.9 % | -51.2 % | 0.251 · +0.58 % · 0.10 |
| refit | 0.36 | 185 (0.281) | 2.24 | **0.370** (0.225 · 9.15) | 0.167 | 0.412 | 0.368 (0.189) | 0.422 | +2.90 % (+1.15 %) | +0.62 % | +0.67 % | 0.67 (0.55) | +25.5 % | -50.4 % | 0.368 · +2.76 % · 0.58 |
| rolling | 0.00 | 659 (1.000) | 5.00 | **0.273** (0.141 · 26.78) | 0.134 | 0.287 | 0.231 (0.080) | 0.439 | +0.92 % (+0.33 %) | +0.19 % | +0.42 % | 0.49 (-0.02) | +13.6 % | -51.3 % | 0.269 · +0.82 % · 0.36 |
| rolling | 0.36 | 200 (0.303) | 2.29 | **0.408** (0.227 · 11.78) | 0.166 | 0.410 | 0.385 (0.185) | 0.435 | +3.02 % (+1.13 %) | +0.60 % | +0.76 % | 0.73 (0.32) | +30.6 % | -45.9 % | 0.417 · +2.92 % · 0.58 |

Per year, at the chosen cut:

- frozen: 2023 7 sessions, precision 0.182, basket -2.34 %, ≥ 5 % on 0.00; 2024 12 sessions, precision 0.357, basket +3.58 %, ≥ 5 % on 0.42; 2025 93 sessions, precision 0.471, basket +7.30 %, ≥ 5 % on 0.46; 2026 49 sessions, precision 0.285, basket -0.04 %, ≥ 5 % on 0.24
- refit: 2023 5 sessions, precision 0.571, basket +5.22 %, ≥ 5 % on 0.40; 2024 22 sessions, precision 0.314, basket +1.42 %, ≥ 5 % on 0.27; 2025 96 sessions, precision 0.440, basket +5.80 %, ≥ 5 % on 0.49; 2026 62 sessions, precision 0.279, basket -1.26 %, ≥ 5 % on 0.21
- rolling: 2023 5 sessions, precision 0.571, basket +5.22 %, ≥ 5 % on 0.40; 2024 24 sessions, precision 0.323, basket -0.06 %, ≥ 5 % on 0.29; 2025 117 sessions, precision 0.467, basket +5.08 %, ≥ 5 % on 0.44; 2026 54 sessions, precision 0.340, basket -0.29 %, ≥ 5 % on 0.30

The grid (⚠️ DESCRIPTIVE on test: only the val-chosen cut is the result) — active share · precision · basket ret · Sharpe:

| cut | val | test_frozen | test_refit | test_rolling |
|---|---|---|---|---|
| 0.00 | 1.00 · 0.352 · +2.1 % · 1.29 | 1.00 · 0.266 · +1.0 % · 0.60 | 1.00 · 0.256 · +0.7 % · 0.24 | 1.00 · 0.273 · +0.9 % · 0.49 |
| 0.20 | 1.00 · 0.352 · +2.1 % · 1.29 | 0.98 · 0.274 · +1.0 % · 0.51 | 0.98 · 0.264 · +0.8 % · 0.37 | 0.98 · 0.280 · +0.9 % · 0.43 |
| 0.25 | 0.98 · 0.351 · +2.1 % · 1.22 | 0.87 · 0.290 · +1.1 % · 0.58 | 0.80 · 0.274 · +1.0 % · 0.37 | 0.83 · 0.303 · +1.8 % · 0.98 |
| 0.30 | 0.83 · 0.383 · +2.2 % · 1.08 | 0.57 · 0.326 · +2.1 % · 0.80 | 0.55 · 0.301 · +1.4 % · 0.47 | 0.59 · 0.338 · +1.9 % · 0.79 |
| 0.35 | 0.55 · 0.414 · +3.4 % · 1.28 | 0.30 · 0.378 · +4.2 % · 1.00 | 0.32 · 0.357 · +2.2 % · 0.53 | 0.34 · 0.395 · +2.5 % · 0.64 |
| 0.36 ⬅ | 0.51 · 0.413 · +3.9 % · 1.39 | 0.24 · 0.379 · +4.4 % · 0.92 | 0.28 · 0.370 · +2.9 % · 0.67 | 0.30 · 0.408 · +3.0 % · 0.73 |
| 0.40 | 0.33 · 0.427 · +3.6 % · 0.99 | 0.12 · 0.419 · +5.0 % · 0.58 | 0.14 · 0.415 · +4.8 % · 0.64 | 0.15 · 0.452 · +5.7 % · 0.80 |
| 0.45 | 0.18 · 0.493 · +4.4 % · 0.90 | 0.04 · 0.424 · +5.0 % · 0.47 | 0.04 · 0.400 · +1.0 % · 0.05 | 0.05 · 0.500 · +3.9 % · 0.36 |
| 0.50 | 0.11 · 0.561 · +6.6 % · 1.02 | 0.02 · 0.412 · +3.1 % · 0.24 | 0.01 · 0.478 · -0.5 % · -0.04 | 0.02 · 0.609 · +6.8 % · 0.64 |

`min_prob.csv` holds the whole grid; `min_prob.json` the cut `--pick` applies.

## Walk-forward — yearly expanding refits of `ensemble_boost`

| year | train rows | sessions | hit | base | lift | null p95 · z | basket ret | universe ret | daily AUC | Sharpe@50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 2021 | 419,503 | 173 | 0.439 | 0.265 | 1.66 | 0.286 · 12.29 | +5.20 % | +1.85 % | 0.640 | 5.76 |
| 2022 | 454,586 | 249 | 0.343 | 0.206 | 1.66 | 0.223 · 13.34 | +0.19 % | -0.91 % | 0.612 | -0.03 |
| 2023 | 507,839 | 243 | 0.300 | 0.170 | 1.77 | 0.184 · 12.99 | +1.67 % | +0.67 % | 0.652 | 1.53 |
| 2024 | 560,516 | 250 | 0.250 | 0.124 | 2.01 | 0.138 · 14.55 | +0.80 % | +0.30 % | 0.633 | 0.73 |
| 2025 | 615,659 | 248 | 0.310 | 0.150 | 2.07 | 0.165 · 16.40 | +1.59 % | +0.38 % | 0.629 | 0.65 |
| 2026 | 671,804 | 149 | 0.235 | 0.122 | 1.92 | 0.141 · 10.05 | -0.35 % | -0.43 % | 0.641 | -0.41 |

## Read before quoting

- `n_eff` ≈ sessions / h: the test split's 659 sessions are ~132 independent baskets, and the trading track 132 periods.
- The null prices ONE run against random baskets; the grid is several runs and the kinds were chosen on VCB (NUL-1). Quote the val-chosen row.
- The universe is survivors-only and its membership is not point-in-time (CLAUDE.md §2c): that protects the z, not the return.
- A hit rate rewards volatile names; read the basket return beside it.
