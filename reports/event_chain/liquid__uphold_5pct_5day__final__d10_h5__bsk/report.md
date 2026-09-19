# Basket report — LIQUID: the top 5 names per session for `uphold_5pct_5day`

**Question:** at the close of session N, which names — at most **5** — will close session N+5 at least **5 %** higher? **Event:** 1 when close_adjust[t+5] >= (1 + 5%) x open_adjust[t+1] — bought at the OPEN of t+1 and sold at the close of t+5, 5 sessions held — else 0; NULL for the last 5 sessions. **Table** `unified_schema_liquid.uphold_5pct_5day__final__d10_h5__bsk` → dataset `liquid__uphold_5pct_5day__final__d10_h5__bsk__tr70_val15_test15__std` (hash `d03983287f3e2d68`), 151 channels, purge 14 sessions. A name at its exchange ceiling on N IS bought (`BASKET_EXCLUDE_CEILING = False`).

| split | label dates | rows | base rate |
|---|---|---|---|
| train | 2009-01-15 → 2021-04-08 | 416,135 | 0.164 |
| val | 2021-05-04 → 2023-11-24 | 137,889 | 0.188 |
| test | 2023-12-15 → 2026-08-14 | 147,464 | 0.117 |

Feature selection read only rows before **2021-05-04** (the val start).

## Selection, one pool per run (within-date IC; null = shuffled re-selections)

| pool | channels | kept | CV IC | null p95 | null max | z | clears |
|---|---|---|---|---|---|---|---|
| pool__event_features | 67 | 58 | 0.1232 | 0.0439 | 0.0455 | 26.99 | yes |
| pool__basic | 84 | 57 | 0.1320 | 0.0395 | 0.0401 | 26.93 | yes |

## Leaderboard — chosen on `val_daily_auc` (the other VAL metrics break a tie), test read once

⚠️ **`val daily AUC` / `test daily AUC` are the CHOICE metric** — the ROC-AUC computed inside each session and averaged, which is the only comparison a basket makes. The pooled columns beside them also score names ACROSS sessions and read ~0.02 higher.

`hit` = share of the basket that rose ≥ 5 %; `base` = the same share over all buyable names; null = 200 random baskets per session. Refit = train+val once; rolling = refitted every 63 test sessions.

| model | **val daily AUC** | val AUC (pooled) | val hit | **test daily AUC** | test AUC (pooled) | refit daily · pooled | rolling daily · pooled | **test hit** | test base | null p95 / max · z | lift | all-hit | basket ret | universe ret | Sharpe@50 (CAGR) | refit hit (lift) | rolling hit (lift) | rolling Sharpe@50 (CAGR) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ensemble_xl` | **0.636** | 0.654 | 0.325 | **0.638** | 0.658 | 0.638 · 0.660 | 0.639 · 0.662 | **0.226** | 0.117 | 0.126 / 0.135 · 22.00 | 1.94 | 0.006 | +0.54 % | +0.14 % | -0.27 (-12.7 %) | 0.220 (1.88) | 0.221 (1.89) | -0.42 (-16.8 %) |
| `ensemble_geo2t` | **0.635** | 0.650 | 0.323 | **0.633** | 0.655 | 0.633 · 0.655 | — · — | **0.220** | 0.117 | 0.126 / 0.135 · 20.72 | 1.88 | 0.006 | +0.50 % | +0.14 % | -0.02 (-5.0 %) | 0.219 (1.88) | — (—) | — (—) |
| `ensemble_geo2` | **0.634** | 0.644 | 0.326 | **0.630** | 0.649 | 0.630 · 0.647 | — · — | **0.213** | 0.117 | 0.126 / 0.135 · 19.31 | 1.82 | 0.006 | +0.40 % | +0.14 % | -0.13 (-8.5 %) | 0.209 (1.79) | — (—) | — (—) |
| `event_linear_evt_c003` | **0.634** | 0.643 | 0.328 | **0.629** | 0.647 | 0.629 · 0.645 | 0.633 · 0.651 | **0.209** | 0.117 | 0.126 / 0.135 · 18.58 | 1.79 | 0.006 | +0.29 % | +0.14 % | -0.02 (-5.0 %) | 0.207 (1.77) | 0.214 (1.83) | -0.32 (-12.9 %) |
| `event_linear_har_c003` | **0.634** | 0.643 | 0.316 | **0.631** | 0.650 | 0.630 · 0.648 | — · — | **0.212** | 0.117 | 0.126 / 0.135 · 19.19 | 1.82 | 0.006 | +0.45 % | +0.14 % | -0.12 (-7.5 %) | 0.212 (1.81) | — (—) | — (—) |
| `event_boost_xgb_d8` | **0.632** | 0.654 | 0.320 | **0.639** | 0.658 | 0.639 · 0.662 | 0.639 · 0.662 | **0.231** | 0.117 | 0.126 / 0.135 · 22.98 | 1.98 | 0.012 | +0.59 % | +0.14 % | -0.07 (-7.2 %) | 0.226 (1.94) | 0.222 (1.90) | -0.45 (-17.6 %) |
| `ensemble_xl_seq` | **0.631** | 0.654 | 0.319 | **0.642** | 0.659 | — · — | — · — | **0.239** | 0.117 | 0.126 / 0.135 · 24.51 | 2.04 | 0.015 | +0.93 % | +0.14 % | 0.87 (+25.9 %) | — (—) | — (—) | — (—) |
| `tcn_c32` | **0.629** | 0.650 | 0.314 | **0.634** | 0.653 | — · — | — · — | **0.229** | 0.117 | 0.126 / 0.135 · 22.49 | 1.96 | 0.005 | +0.56 % | +0.14 % | 0.27 (+3.8 %) | — (—) | — (—) | — (—) |
| `gbt_d4` | **0.628** | 0.651 | 0.317 | **0.633** | 0.658 | 0.635 · 0.662 | — · — | **0.223** | 0.117 | 0.126 / 0.135 · 21.45 | 1.91 | 0.014 | +0.38 % | +0.14 % | -0.45 (-18.9 %) | 0.222 (1.90) | — (—) | — (—) |
| `forest_rf200_d12` | **0.626** | 0.649 | 0.321 | **0.635** | 0.657 | 0.635 · 0.662 | — · — | **0.225** | 0.117 | 0.126 / 0.135 · 21.88 | 1.93 | 0.011 | +0.51 % | +0.14 % | -0.06 (-7.6 %) | 0.232 (1.98) | — (—) | — (—) |
| `gbt_d2` | **0.625** | 0.652 | 0.320 | **0.630** | 0.655 | 0.630 · 0.654 | — · — | **0.227** | 0.117 | 0.126 / 0.135 · 22.13 | 1.94 | 0.012 | +0.52 % | +0.14 % | 0.01 (-5.6 %) | 0.226 (1.94) | — (—) | — (—) |
| `mlp_h32` | **0.619** | 0.637 | 0.315 | **0.618** | 0.627 | — · — | — · — | **0.217** | 0.117 | 0.126 / 0.135 · 20.11 | 1.86 | 0.003 | +0.99 % | +0.14 % | 1.30 (+38.6 %) | — (—) | — (—) | — (—) |
| `lstm_h32` | **0.609** | 0.632 | 0.296 | **0.628** | 0.637 | — · — | — · — | **0.219** | 0.117 | 0.126 / 0.135 · 20.47 | 1.87 | 0.014 | +0.80 % | +0.14 % | 0.73 (+19.7 %) | — (—) | — (—) | — (—) |
| `baseline_prior` | **0.500** | 0.500 | 0.150 | **0.500** | 0.500 | — · — | — · — | **0.097** | 0.117 | 0.126 / 0.135 · -3.95 | 0.83 | 0.002 | +0.16 % | +0.14 % | -0.97 (-16.8 %) | — (—) | — (—) | — (—) |

**Chosen on val (`val_daily_auc`):** `ensemble_xl` — val within-session AUC 0.636 (pooled 0.654), hit@5 0.325. **Test within-session AUC 0.638** (within-session label-shuffle null p95 0.506, z 41.92; refitted on train+val 0.638, refitted every 63 sessions 0.639). The POOLED AUC, which also prices knowing WHICH SESSIONS are eventful and is not what a basket trades, reads 0.658 on test (null p95 0.550, z 57.27; refit 0.660, rolling 0.662). **On test, 0.226 of its basket names rose ≥ 5 %** against a buyable base rate of 0.117 (lift 1.94×) and a random-basket null p95 of 0.126 (max 0.135, z 22.00); every name hit on 0.006 of sessions; the basket's mean 5-session return was +0.54 % against the universe's +0.14 %, and the equal-weight basket itself was ≥ 5 % on 0.158 of sessions. Traded every 5 sessions at 50 bps a round trip: Sharpe -0.27, CAGR -12.7 %, max drawdown -44.4 % (universe Sharpe 0.55, CAGR +7.5 %). Refitted on train+val: hit 0.220, Sharpe@50 -0.44. Refitted every 63 sessions: hit 0.221 (null p95 0.126), Sharpe@50 -0.42, CAGR -16.8 %.

The test baskets themselves — date, rank, ticker, score, outcome — are `baskets_test_{frozen,refit,rolling}.csv`; `python -m event_chain.basket --pick <date>` answers one session.

## At most 5 names — a name under P(event) 0.28 is not bought

The cut is chosen on VAL (`BASKET_MIN_PROB_ON = "ev"`: the mean net return per session at 50 bps, a cash session returning 0, among cuts trading at least 25 val sessions): **0.28** — on val it traded 0.794 of sessions, 4.03 names each, precision 0.362 (base on those sessions 0.204), EV +1.30 % per session. A session none of whose top 5 reaches the cut holds cash. Null: the same number of names per active session, drawn at random (200 draws). Sharpe/CAGR: one basket every 5 sessions, cash when idle, mean over the 5 start offsets (the worst in brackets).

| test basket | cut | active (share) | names | **precision** (null p95 · z) | base on active | p_mean | basket ≥ 5 % (null p95) | loss rate | basket ret (null p95) | universe ret | EV/session | Sharpe@50 (worst) | CAGR | max DD | without ceiling names: precision · ret · Sharpe |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| frozen | 0.00 | 659 (1.000) | 5.00 | **0.226** (0.126 · 22.00) | 0.117 | 0.261 | 0.158 (0.064) | 0.454 | +0.54 % (+0.25 %) | +0.14 % | +0.04 % | 0.05 (-0.31) | -3.6 % | -43.1 % | 0.223 · +0.50 % · -0.01 |
| frozen | 0.28 | 327 (0.496) | 3.08 | **0.274** (0.168 · 11.72) | 0.132 | 0.326 | 0.239 (0.116) | 0.483 | +0.24 % (+0.48 %) | +0.21 % | -0.13 % | -0.17 (-0.59) | -13.2 % | -59.3 % | 0.280 · +0.41 % · -0.07 |
| refit | 0.00 | 659 (1.000) | 5.00 | **0.220** (0.126 · 20.72) | 0.117 | 0.264 | 0.143 (0.064) | 0.476 | +0.34 % (+0.25 %) | +0.14 % | -0.16 % | -0.25 (-0.79) | -13.4 % | -48.6 % | 0.212 · +0.26 % · -0.36 |
| refit | 0.28 | 341 (0.517) | 3.24 | **0.278** (0.166 · 12.54) | 0.136 | 0.329 | 0.246 (0.112) | 0.472 | +0.72 % (+0.62 %) | +0.37 % | +0.12 % | 0.11 (-0.21) | -1.4 % | -52.4 % | 0.276 · +0.68 % · 0.09 |
| rolling | 0.00 | 659 (1.000) | 5.00 | **0.221** (0.126 · 20.90) | 0.117 | 0.258 | 0.141 (0.064) | 0.495 | +0.26 % (+0.25 %) | +0.14 % | -0.24 % | -0.33 (-0.72) | -16.8 % | -52.7 % | 0.214 · +0.23 % · -0.36 |
| rolling | 0.28 | 347 (0.527) | 2.90 | **0.289** (0.172 · 12.20) | 0.135 | 0.323 | 0.254 (0.115) | 0.450 | +1.11 % (+0.61 %) | +0.35 % | +0.32 % | 0.34 (-0.00) | +6.6 % | -51.7 % | 0.284 · +1.07 % · 0.30 |

Per year, at the chosen cut:

- frozen: 2023 11 sessions, precision 0.179, basket +0.50 %, ≥ 5 % on 0.09; 2024 52 sessions, precision 0.185, basket -0.96 %, ≥ 5 % on 0.13; 2025 171 sessions, precision 0.313, basket +1.18 %, ≥ 5 % on 0.32; 2026 93 sessions, precision 0.264, basket -0.84 %, ≥ 5 % on 0.17
- refit: 2023 6 sessions, precision 0.120, basket -1.90 %, ≥ 5 % on 0.00; 2024 62 sessions, precision 0.218, basket +0.11 %, ≥ 5 % on 0.19; 2025 169 sessions, precision 0.323, basket +1.82 %, ≥ 5 % on 0.31; 2026 104 sessions, precision 0.255, basket -0.54 %, ≥ 5 % on 0.19
- rolling: 2023 6 sessions, precision 0.120, basket -1.90 %, ≥ 5 % on 0.00; 2024 63 sessions, precision 0.219, basket +0.33 %, ≥ 5 % on 0.21; 2025 176 sessions, precision 0.331, basket +2.37 %, ≥ 5 % on 0.31; 2026 102 sessions, precision 0.267, basket -0.42 %, ≥ 5 % on 0.21

The grid (⚠️ DESCRIPTIVE on test: only the val-chosen cut is the result) — active share · precision · basket ret · Sharpe:

| cut | val | test_frozen | test_refit | test_rolling |
|---|---|---|---|---|
| 0.00 | 1.00 · 0.325 · +1.5 % · 0.92 | 1.00 · 0.226 · +0.5 % · 0.05 | 1.00 · 0.220 · +0.3 % · -0.25 | 1.00 · 0.221 · +0.3 % · -0.33 |
| 0.20 | 1.00 · 0.325 · +1.5 % · 0.92 | 1.00 · 0.232 · +0.5 % · 0.03 | 0.99 · 0.227 · +0.5 % · -0.03 | 0.98 · 0.233 · +0.5 % · -0.01 |
| 0.25 | 0.96 · 0.341 · +1.7 % · 0.98 | 0.72 · 0.250 · +0.5 % · -0.06 | 0.73 · 0.249 · +0.4 % · -0.14 | 0.72 · 0.263 · +0.5 % · 0.02 |
| 0.28 ⬅ | 0.79 · 0.362 · +2.1 % · 1.13 | 0.50 · 0.274 · +0.2 % · -0.17 | 0.52 · 0.278 · +0.7 % · 0.11 | 0.53 · 0.289 · +1.1 % · 0.34 |
| 0.30 | 0.68 · 0.359 · +1.8 % · 0.76 | 0.37 · 0.281 · +0.8 % · 0.10 | 0.40 · 0.282 · +0.7 % · 0.06 | 0.39 · 0.297 · +0.7 % · 0.05 |
| 0.35 | 0.37 · 0.393 · +1.8 % · 0.48 | 0.13 · 0.338 · +2.0 % · 0.22 | 0.19 · 0.358 · +1.8 % · 0.34 | 0.14 · 0.391 · +0.9 % · 0.04 |
| 0.40 | 0.20 · 0.435 · +3.0 % · 0.62 | 0.04 · 0.367 · +3.6 % · 0.17 | 0.07 · 0.439 · +3.6 % · 0.31 | 0.05 · 0.460 · +3.8 % · 0.33 |
| 0.45 | 0.11 · 0.502 · +4.5 % · 0.56 | 0.02 · 0.394 · +0.2 % · -0.03 | 0.01 · 0.458 · +5.8 % · 0.07 | 0.01 · 0.667 · +12.6 % · 0.32 |
| 0.50 | 0.05 · 0.642 · +8.9 % · 0.92 | 0.01 · 0.600 · +8.8 % · 0.37 | 0.00 · 0.600 · +10.6 % · 0.21 | 0.00 · 0.714 · +12.0 % · 0.62 |

`min_prob.csv` holds the whole grid; `min_prob.json` the cut `--pick` applies.

## Walk-forward — yearly expanding refits of `ensemble_xl`

| year | train rows | sessions | hit | base | lift | null p95 · z | basket ret | universe ret | daily AUC | Sharpe@50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 2021 | 416,135 | 172 | 0.409 | 0.236 | 1.74 | 0.257 · 12.90 | +4.02 % | +1.52 % | 0.640 | 5.81 |
| 2022 | 449,296 | 249 | 0.326 | 0.191 | 1.71 | 0.206 · 14.62 | -0.04 % | -0.74 % | 0.610 | 0.15 |
| 2023 | 502,480 | 235 | 0.277 | 0.148 | 1.87 | 0.164 · 13.06 | +1.28 % | +0.49 % | 0.654 | 1.07 |
| 2024 | 554,024 | 250 | 0.198 | 0.106 | 1.87 | 0.121 · 10.32 | +0.28 % | +0.24 % | 0.638 | -0.53 |
| 2025 | 608,418 | 248 | 0.252 | 0.132 | 1.92 | 0.147 · 13.38 | +0.65 % | +0.28 % | 0.637 | -0.44 |
| 2026 | 664,515 | 150 | 0.203 | 0.111 | 1.83 | 0.131 · 8.33 | -0.59 % | -0.36 % | 0.647 | -1.50 |

## Read before quoting

- `n_eff` ≈ sessions / h: the test split's 659 sessions are ~132 independent baskets, and the trading track 132 periods.
- The null prices ONE run against random baskets; the grid is several runs and the kinds were chosen on VCB (NUL-1). Quote the val-chosen row.
- The universe is survivors-only and its membership is not point-in-time (CLAUDE.md §2c): that protects the z, not the return.
- A hit rate rewards volatile names; read the basket return beside it.
