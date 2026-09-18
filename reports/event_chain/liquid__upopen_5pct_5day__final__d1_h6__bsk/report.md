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

## Leaderboard — chosen on `val_auc` (the other VAL metrics break a tie), test read once

`hit` = share of the basket that rose ≥ 5 %; `base` = the same share over all buyable names; null = 200 random baskets per session. Refit = train+val once; rolling = refitted every 63 test sessions.

| model | val AUC | val daily AUC | val hit | **test AUC** | test daily AUC | refit AUC · daily | rolling AUC · daily | **test hit** | test base | null p95 / max · z | lift | all-hit | basket ret | universe ret | Sharpe@50 (CAGR) | refit hit (lift) | rolling hit (lift) | rolling Sharpe@50 (CAGR) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ensemble_boost_eq` | 0.650 | 0.635 | 0.357 | **0.654** | 0.631 | 0.656 · 0.629 | 0.657 · 0.630 | **0.260** | 0.134 | 0.141 / 0.150 · 24.18 | 1.94 | 0.015 | +0.76 % | +0.19 % | 0.39 (+8.0 %) | 0.255 (1.90) | 0.270 (2.02) | 0.37 (+6.9 %) |
| `ensemble_boost` | 0.650 | 0.636 | 0.352 | **0.655** | 0.632 | 0.658 · 0.631 | 0.658 · 0.632 | **0.266** | 0.134 | 0.141 / 0.150 · 25.34 | 1.99 | 0.014 | +1.01 % | +0.19 % | 0.55 (+15.5 %) | 0.256 (1.91) | 0.273 (2.04) | 0.54 (+14.9 %) |
| `gbt_d2` | 0.648 | 0.622 | 0.339 | **0.646** | 0.623 | 0.647 · 0.622 | — · — | **0.255** | 0.134 | 0.141 / 0.150 · 23.19 | 1.90 | 0.014 | +0.73 % | +0.19 % | 0.24 (+2.0 %) | 0.253 (1.89) | — (—) | — (—) |
| `forest_et_leaf200` | 0.647 | 0.626 | 0.349 | **0.650** | 0.628 | 0.652 · 0.627 | — · — | **0.249** | 0.134 | 0.141 / 0.150 · 22.04 | 1.86 | 0.015 | +0.85 % | +0.19 % | 0.40 (+8.4 %) | 0.249 (1.86) | — (—) | — (—) |
| `ensemble_xl` | 0.646 | 0.636 | 0.355 | **0.655** | 0.633 | 0.658 · 0.632 | 0.658 · 0.635 | **0.259** | 0.134 | 0.141 / 0.150 · 23.95 | 1.93 | 0.015 | +1.04 % | +0.19 % | 0.42 (+9.6 %) | 0.250 (1.87) | 0.264 (1.97) | 0.77 (+25.0 %) |
| `ensemble_geo2t` | 0.646 | 0.628 | 0.345 | **0.648** | 0.624 | 0.651 · 0.622 | — · — | **0.254** | 0.134 | 0.141 / 0.150 · 23.02 | 1.90 | 0.014 | +0.68 % | +0.19 % | 0.28 (+3.3 %) | 0.240 (1.79) | — (—) | — (—) |
| `gbt_d4` | 0.644 | 0.627 | 0.339 | **0.645** | 0.627 | 0.648 · 0.623 | — · — | **0.253** | 0.134 | 0.141 / 0.150 · 22.96 | 1.89 | 0.014 | +0.98 % | +0.19 % | 0.68 (+21.1 %) | 0.229 (1.71) | — (—) | — (—) |
| `ensemble_geo2` | 0.639 | 0.624 | 0.345 | **0.644** | 0.619 | 0.646 · 0.619 | — · — | **0.243** | 0.134 | 0.141 / 0.150 · 21.05 | 1.82 | 0.011 | +0.35 % | +0.19 % | -0.23 (-15.7 %) | 0.244 (1.82) | — (—) | — (—) |
| `event_boost_xgb_d8` | 0.639 | 0.632 | 0.338 | **0.651** | 0.632 | 0.656 · 0.631 | 0.654 · 0.634 | **0.253** | 0.134 | 0.141 / 0.150 · 22.96 | 1.89 | 0.017 | +1.15 % | +0.19 % | 1.02 (+37.8 %) | 0.251 (1.87) | 0.274 (2.05) | 1.09 (+41.7 %) |
| `event_boost_mag_d8` | 0.638 | 0.622 | 0.325 | **0.622** | 0.615 | 0.618 · 0.610 | 0.630 · 0.612 | **0.259** | 0.134 | 0.141 / 0.150 · 24.06 | 1.94 | 0.011 | +0.55 % | +0.19 % | -0.16 (-15.6 %) | 0.258 (1.93) | 0.263 (1.96) | -0.19 (-16.4 %) |
| `event_linear_mag_har_a100_hl4` | 0.635 | 0.615 | 0.337 | **0.636** | 0.608 | 0.638 · 0.608 | — · — | **0.248** | 0.134 | 0.141 / 0.150 · 21.98 | 1.86 | 0.011 | +0.34 % | +0.19 % | -0.19 (-15.7 %) | 0.244 (1.82) | — (—) | — (—) |
| `ensemble_geo3` | 0.634 | 0.624 | 0.349 | **0.641** | 0.619 | 0.643 · 0.620 | — · — | **0.240** | 0.134 | 0.141 / 0.150 · 20.42 | 1.79 | 0.012 | +0.67 % | +0.19 % | -0.08 (-9.9 %) | 0.235 (1.76) | — (—) | — (—) |
| `event_linear_evt_c003` | 0.633 | 0.627 | 0.341 | **0.643** | 0.623 | 0.643 · 0.623 | 0.646 · 0.624 | **0.232** | 0.134 | 0.141 / 0.150 · 18.85 | 1.73 | 0.009 | +0.52 % | +0.19 % | -0.80 (-31.7 %) | 0.232 (1.74) | 0.237 (1.77) | -0.34 (-18.5 %) |
| `event_linear_dir_c001` | 0.506 | 0.503 | 0.161 | **0.527** | 0.506 | 0.529 · 0.515 | — · — | **0.080** | 0.134 | 0.141 / 0.150 · -10.19 | 0.59 | 0.002 | +0.65 % | +0.19 % | -0.01 (-0.7 %) | 0.077 (0.57) | — (—) | — (—) |
| `baseline_prior` | 0.500 | 0.500 | 0.171 | **0.500** | 0.500 | — · — | — · — | **0.112** | 0.134 | 0.141 / 0.150 · -3.95 | 0.84 | 0.003 | +0.23 % | +0.19 % | -0.79 (-17.5 %) | — (—) | — (—) | — (—) |

**Chosen on val (`val_auc`):** `ensemble_boost_eq` — val AUC 0.650, within-session 0.635, hit@5 0.357. **Test AUC 0.654** (within-session shuffle null p95 0.547, z 57.45; within-session AUC 0.631, null p95 0.505, z 44.05; refitted on train+val 0.656 · 0.629; refitted every 63 sessions 0.657 · 0.630). **On test, 0.260 of its basket names rose ≥ 5 %** against a buyable base rate of 0.134 (lift 1.94×) and a random-basket null p95 of 0.141 (max 0.150, z 24.18); every name hit on 0.015 of sessions; the basket's mean 5-session return was +0.76 % against the universe's +0.19 %, and the equal-weight basket itself was ≥ 5 % on 0.208 of sessions. Traded every 5 sessions at 50 bps a round trip: Sharpe 0.39, CAGR +8.0 %, max drawdown -44.4 % (universe Sharpe 0.24, CAGR +2.8 %). Refitted on train+val: hit 0.255, Sharpe@50 0.19. Refitted every 63 sessions: hit 0.270 (null p95 0.141), Sharpe@50 0.37, CAGR +6.9 %.

The test baskets themselves — date, rank, ticker, score, outcome — are `baskets_test_{frozen,refit,rolling}.csv`; `python -m event_chain.basket --pick <date>` answers one session.

## At most 5 names — a name under P(event) 0.25 is not bought

The cut is chosen on VAL (`BASKET_MIN_PROB_ON = "ev"`: the mean net return per session at 50 bps, a cash session returning 0, among cuts trading at least 25 val sessions): **0.25** — on val it traded 0.998 of sessions, 4.86 names each, precision 0.355 (base on those sessions 0.209), EV +1.67 % per session. A session none of whose top 5 reaches the cut holds cash. Null: the same number of names per active session, drawn at random (200 draws). Sharpe/CAGR: one basket every 5 sessions, cash when idle, mean over the 5 start offsets (the worst in brackets).

| test basket | cut | active (share) | names | **precision** (null p95 · z) | base on active | p_mean | basket ≥ 5 % (null p95) | loss rate | basket ret (null p95) | universe ret | EV/session | Sharpe@50 (worst) | CAGR | max DD | without ceiling names: precision · ret · Sharpe |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| frozen | 0.00 | 659 (1.000) | 5.00 | **0.260** (0.141 · 24.18) | 0.134 | 0.294 | 0.208 (0.080) | 0.466 | +0.76 % (+0.33 %) | +0.19 % | +0.26 % | 0.31 (-0.15) | +4.7 % | -53.4 % | 0.253 · +0.62 % · 0.15 |
| frozen | 0.25 | 605 (0.918) | 3.99 | **0.286** (0.152 · 21.74) | 0.138 | 0.320 | 0.246 (0.104) | 0.471 | +1.01 % (+0.45 %) | +0.27 % | +0.47 % | 0.46 (0.10) | +12.9 % | -50.1 % | 0.281 · +0.91 % · 0.37 |
| refit | 0.00 | 659 (1.000) | 5.00 | **0.255** (0.141 · 23.19) | 0.134 | 0.289 | 0.184 (0.080) | 0.454 | +0.67 % (+0.33 %) | +0.19 % | +0.17 % | 0.21 (-0.17) | +0.9 % | -54.6 % | 0.248 · +0.56 % · 0.06 |
| refit | 0.25 | 576 (0.874) | 3.88 | **0.276** (0.158 · 18.89) | 0.138 | 0.321 | 0.227 (0.106) | 0.470 | +0.88 % (+0.46 %) | +0.27 % | +0.33 % | 0.31 (0.09) | +4.7 % | -54.1 % | 0.273 · +0.89 % · 0.27 |
| rolling | 0.00 | 659 (1.000) | 5.00 | **0.270** (0.141 · 26.20) | 0.134 | 0.288 | 0.219 (0.080) | 0.445 | +0.87 % (+0.33 %) | +0.19 % | +0.37 % | 0.44 (-0.17) | +11.4 % | -50.8 % | 0.267 · +0.79 % · 0.34 |
| rolling | 0.25 | 586 (0.889) | 3.81 | **0.305** (0.159 · 22.99) | 0.136 | 0.321 | 0.276 (0.104) | 0.440 | +1.35 % (+0.39 %) | +0.20 % | +0.76 % | 0.69 (0.25) | +28.1 % | -49.6 % | 0.306 · +1.38 % · 0.67 |

Per year, at the chosen cut:

- frozen: 2023 12 sessions, precision 0.217, basket +0.59 %, ≥ 5 % on 0.17; 2024 217 sessions, precision 0.280, basket +0.89 %, ≥ 5 % on 0.20; 2025 236 sessions, precision 0.321, basket +1.78 %, ≥ 5 % on 0.32; 2026 140 sessions, precision 0.239, basket -0.07 %, ≥ 5 % on 0.20
- refit: 2023 12 sessions, precision 0.250, basket +0.75 %, ≥ 5 % on 0.17; 2024 199 sessions, precision 0.264, basket +0.64 %, ≥ 5 % on 0.20; 2025 232 sessions, precision 0.315, basket +1.78 %, ≥ 5 % on 0.29; 2026 133 sessions, precision 0.228, basket -0.34 %, ≥ 5 % on 0.17
- rolling: 2023 12 sessions, precision 0.250, basket +0.75 %, ≥ 5 % on 0.17; 2024 196 sessions, precision 0.293, basket +1.16 %, ≥ 5 % on 0.24; 2025 233 sessions, precision 0.345, basket +2.27 %, ≥ 5 % on 0.34; 2026 145 sessions, precision 0.258, basket +0.20 %, ≥ 5 % on 0.22

The grid (⚠️ DESCRIPTIVE on test: only the val-chosen cut is the result) — active share · precision · basket ret · Sharpe:

| cut | val | test_frozen | test_refit | test_rolling |
|---|---|---|---|---|
| 0.00 | 1.00 · 0.357 · +2.1 % · 1.23 | 1.00 · 0.260 · +0.8 % · 0.31 | 1.00 · 0.255 · +0.7 % · 0.21 | 1.00 · 0.270 · +0.9 % · 0.44 |
| 0.20 | 1.00 · 0.357 · +2.1 % · 1.23 | 1.00 · 0.265 · +0.7 % · 0.26 | 1.00 · 0.261 · +0.7 % · 0.22 | 1.00 · 0.276 · +0.8 % · 0.37 |
| 0.25 ⬅ | 1.00 · 0.355 · +2.2 % · 1.25 | 0.92 · 0.286 · +1.0 % · 0.46 | 0.87 · 0.276 · +0.9 % · 0.31 | 0.89 · 0.305 · +1.4 % · 0.69 |
| 0.30 | 0.89 · 0.378 · +2.1 % · 1.08 | 0.61 · 0.312 · +1.8 % · 0.73 | 0.59 · 0.298 · +1.4 % · 0.49 | 0.60 · 0.331 · +2.0 % · 0.83 |
| 0.35 | 0.62 · 0.400 · +2.8 % · 1.02 | 0.38 · 0.363 · +3.4 % · 0.94 | 0.37 · 0.352 · +2.0 % · 0.48 | 0.37 · 0.399 · +3.1 % · 0.80 |
| 0.40 | 0.36 · 0.422 · +3.3 % · 0.93 | 0.17 · 0.407 · +3.8 % · 0.61 | 0.16 · 0.405 · +4.6 % · 0.69 | 0.17 · 0.468 · +5.0 % · 0.78 |
| 0.45 | 0.19 · 0.481 · +4.2 % · 0.84 | 0.06 · 0.442 · +10.5 % · 0.74 | 0.05 · 0.419 · +5.7 % · 0.21 | 0.05 · 0.529 · +9.6 % · 0.74 |
| 0.50 | 0.11 · 0.575 · +6.7 % · 0.88 | 0.02 · 0.394 · +2.7 % · 0.26 | 0.02 · 0.545 · +1.4 % · 0.02 | 0.01 · 0.600 · +7.5 % · 0.43 |

`min_prob.csv` holds the whole grid; `min_prob.json` the cut `--pick` applies.

## Walk-forward — yearly expanding refits of `ensemble_boost_eq`

| year | train rows | sessions | hit | base | lift | null p95 · z | basket ret | universe ret | daily AUC | Sharpe@50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 2021 | 419,503 | 173 | 0.452 | 0.265 | 1.70 | 0.286 · 13.18 | +5.63 % | +1.85 % | 0.639 | 5.21 |
| 2022 | 454,586 | 249 | 0.347 | 0.206 | 1.68 | 0.223 · 13.73 | +0.24 % | -0.91 % | 0.610 | -0.18 |
| 2023 | 507,839 | 243 | 0.300 | 0.170 | 1.77 | 0.184 · 12.99 | +1.51 % | +0.67 % | 0.650 | 1.26 |
| 2024 | 560,516 | 250 | 0.237 | 0.124 | 1.90 | 0.138 · 13.07 | +0.55 % | +0.30 % | 0.630 | 0.55 |
| 2025 | 615,659 | 248 | 0.301 | 0.150 | 2.00 | 0.165 · 15.41 | +1.46 % | +0.38 % | 0.627 | 0.56 |
| 2026 | 671,804 | 149 | 0.235 | 0.122 | 1.92 | 0.141 · 10.05 | -0.54 % | -0.43 % | 0.639 | -1.25 |

## Read before quoting

- `n_eff` ≈ sessions / h: the test split's 659 sessions are ~132 independent baskets, and the trading track 132 periods.
- The null prices ONE run against random baskets; the grid is several runs and the kinds were chosen on VCB (NUL-1). Quote the val-chosen row.
- The universe is survivors-only and its membership is not point-in-time (CLAUDE.md §2c): that protects the z, not the return.
- A hit rate rewards volatile names; read the basket return beside it.
