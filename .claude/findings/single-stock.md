# The single-stock track — five defeats and one small effect

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§6-1`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **`CLAUDE.md` §5c, §5d and §6-1 through §6-1-quater in full** — eleven architectures inside one
> error bar, the BANK panel, the five-ticker h=10 run, the 30-name VN30 run whose POOLED answer
> flips, and the width ladder re-run.

---

### 5c. The `pool__basic` prototype chain — run end to end 2026-08-10

Six stages, network to scored metric, one ticker, one pool. `pool__basic` re-scraped
(`skip_existing=False`, VCB only, 3m24s) and rebuilt to **4,266 rows / 2026-08-07**, from
4,235 / 2026-06-25.

**ELEVEN models, 0 to 4,961 parameters, identical dataset / splits / purge / null:**

| model | params | test IC | test bar | p | R² | RMSE |
|---|---|---|---|---|---|---|
| `BASELINE_ZERO` | **0** | — (constant) | — | — | −0.001 | **0.03721** ← the floor |
| `BASELINE_AR` | 6 | +0.0557 | +0.0585 ❌ | 0.070 | −0.002 | 0.03723 |
| `BASELINE_RIDGE_STATS` | 25 | +0.1005 | +0.1247 ❌ | 0.095 | −5.19 | 0.09252 |
| `BASELINE_RIDGE_FLAT` | 81 | −0.0397 | +0.0785 ❌ | 0.577 | −3.26 | 0.07677 |
| `MLP` | 257 | **−0.1001** | +0.0908 ❌ | 0.910 | −5.87 | 0.09751 |
| `LSTM` (h=8) | 473 | **+0.0346** | +0.0956 ❌ | 0.249 | −0.111 | 0.03920 |
| `GRU` | 1,105 | −0.0766 | +0.0786 ❌ | 0.726 | −0.031 | 0.03776 |
| **`GBT`** | 1,319 | **+0.1263** | +0.1121 ⚠️ **✅** | **0.035** | −2.11 | 0.06562 |
| `CNN` | 3,745 | −0.0332 | +0.1107 ❌ | 0.657 | −0.008 | 0.03734 |
| `LSTM` (h=32) | 4,961 | −0.0345 | +0.1348 ❌ | 0.726 | −0.059 | 0.03826 |
| *wide, 724 ch — LSTM* | ~276k | −0.0721 | +0.118 ❌ | 0.88 | −0.90 | — |

⚠️ **THE WHOLE SPREAD IS ONE ERROR BAR.** IC ranges −0.100 … +0.126 over 9 scored
models — a span of 0.227 against `SE(IC) = 0.197` at `n_eff = 26.7` (window overlap) and
0.089 at `n_eff = 128` (label overlap). The largest |t| on the board is **+1.42**.
Ranking these architectures is reading noise.

⚠️ **Two runs clear a bar; expectation was 1.1.** `GBT` on `ic` (p = 0.035) and
`BASELINE_AR` on `dir_auc` (p = 0.040), from **11 runs × 2 nulled metrics = 22 tests**.
The null prices in no architecture search (**NUL-1**) and the sweep *is* one.

⚠️ **NOT ONE OF ELEVEN BEATS THE ZERO PREDICTOR ON RMSE**, and the models that rank
best are the ones whose magnitudes are most wrong — `GBT` R² = −2.11, `ridge_stats`
−5.19. Ranking and calibration are separate questions here.

⚠️ **Capacity is real but not the whole story.** The LSTM *flips sign* on capacity alone
(h=32 → −0.0345, h=8 → **+0.0346**), yet the 257-parameter MLP is the worst on the board
on the same design a 25-parameter ridge does best on. `.claude/context/model.md` §14–§15.

⚠️ **Nothing here changes §2 and everything strengthens it.** Six model families
spanning 0 to 276k parameters all land inside their own nulls.

### 5d. The BANK panel, re-scraped and re-run 2026-08-10

All 20 banks refreshed to 2026-08-07 (`pool__basic` 53,921 → **54,528 rows**), so the
panel has one uniform as-of date. Dataset **27,348 / 12,629 / 13,135 × 20 × 10**, scored
at **panel grain** — `n_eff = n_dates/h` = **131.6**, which is FEWER independent
observations than VCB's 128-per-640-rows, not more.

**Selection: `z = −1.71`, observed −0.0106 BELOW its null's mean of +0.0052.** Decisive
fail, and §13d's reading stands — a sector co-moves, so there is less to rank.

| baseline | params | daily-IC t | evaluator says |
|---|---|---|---|
| `BASELINE_AR` | 6 | **+0.23** | ⚠️ "clears" `ic` at p=0.005, **against a NEGATIVE bar** |
| `BASELINE_RIDGE_STATS` | 61 | **+1.15** | ❌ fails (p=0.254) — the highest t on the panel |
| `BASELINE_RIDGE_FLAT` | 201 | +0.77 | ❌ fails |

⚠️ **NEW ISSUE NUL-3: the evaluator's panel null is not label-neutral.** Its centre moved
with the MODEL (−0.0171 / +0.0076 / +0.0109) and it got both ends wrong — manufacturing a
clear for the weakest model and failing the strongest. **On a panel, quote the daily-IC
t-stat, not `ic_clears`.** `.claude/context/model.md` §16.

⚠️ **The selection cleared its bar; the model did not clear its own.** `z = +2.15` bought
nothing downstream — which is the two bars working, and the most useful thing the run
measured. `.claude/context/feature_selection.md` §10d has why `z = +2.15` on 20 draws is weak in
its own right: `p = 0.0476` is the `1/(n+1)` floor, the null is fat-tailed, and this is
the **third** measurement of this pool (§6b `z = +1.56` ❌, §10b `z = +1.46` ❌) under a
third procedure.

⚠️ **The narrow chain is LESS BAD, and it is the STL-1 argument from the other side.**
R² −0.90 → −0.059 on the same ticker, target and splits, at 4,961 parameters instead of
~276k. **Neither shows skill**; "less negative" is not a result.

⚠️ **Re-materialising two pools left 21 siblings on the OLD calendar.** Harmless for a
`pool__basic` build; a rebuild of the 750-channel table would INNER-join back down to
2026-06-25 **and look unchanged**. `status_data` reports it as `pools_behind`.

### ⚠️ 6-1. THE SINGLE-STOCK TRACK AT h=10 — FIVE TICKERS, AND IT DOES NOT CLEAR

Run 2026-08-19, and it is the **fifth independent failure** of single-stock short-horizon
prediction in this repo — the first at a horizon other than 5 days, and the first on more
than one name. Motivation was §2a-bis: four defeats had all been at `h=5`, so the HORIZON
had never been controlled for on a single stock. It has now.

**Setup.** `pool__basic` alone (90 numeric channels), `return_10day`, `d=20 h=10`, purge
`d+h-1 = 29`, 5 expanding folds (`n_train` 500/1267/2034/2801/3568, `n_test` 767,
`n_eff_test` **76.7**), `date_block` null with the whole selection re-run inside each draw,
GPU, 5m25s-5m32s per ticker. ⚠️ `pool__ta` was deliberately EXCLUDED — `FRZ-1`'s re-scrape
moved `pool__basic` 37 sessions past `gold.stocks_ta` (`STA-1`), so a `basic + ta` join
would have truncated the very rows the re-scrape was run to obtain.

| ticker | kept | `ic_mean` | trend | null mean | p95 bar | **null MAX** | **z** | p | verdict |
|---|---|---|---|---|---|---|---|---|---|
| HPG | 55 | +0.0169 | +0.0072 | +0.0109 | +0.0877 | +0.0958 | **+0.11** | 0.545 | ❌ |
| SSI | 54 | +0.0420 | +0.0184 | +0.0068 | +0.0632 | +0.0814 | **+0.96** | 0.182 | ❌ |
| FPT | 54 | +0.0720 | +0.0509 | +0.0051 | +0.0690 | +0.0794 | +1.74 | 0.182 | ⚠️ rule 3 |
| VIC | 60 | −0.0214 | −0.0458 | +0.0150 | +0.0785 | +0.0792 | **−0.69** | 0.727 | ❌ below the null's mean |
| **STB, 20 draws** | 61 | **+0.1014** | −0.0179 | +0.0047 | +0.0830 | +0.0866 | **+1.83** | 0.0476 | ⚠️ |

**Pooled over the 5 independent names** (excess over each run's own null mean):
**+0.0332, sd 0.0512, se 0.0229 → t = +1.45, p = 0.220.**

Four things this measured, and each one is reusable:

1. ⚠️ **RULE 3 FIRES ON FOUR OF FIVE.** HPG, SSI, FPT and VIC each had a shuffled draw beat
   the real data. FPT's `cleared_p95_not_a_pass` is the label working as designed.
2. ⚠️ **MORE DRAWS MOVED `z` DOWN, NOT UP.** STB at 10 draws was `z = +2.13`; the identical
   run at 20 draws is **`z = +1.83`** — the observed is deterministic and unchanged at
   +0.1014, and only the null moved, its **sd growing 0.0444 → 0.0528 (+19 %)**. That is
   within one `SE(sd)` and so CONFIRMS the "10 to fail, 20 to pass" rule rather than
   contradicting it: a 10-draw `z` on a result that lands far above is optimistic, which is
   exactly why the rule asks for 20 before anything is promoted.
3. ⚠️ **THE NULL PRICES THE FEATURE SEARCH, NEVER THE TICKER SEARCH.** Five names were
   tried. P(z > 1.83) once is 0.0336; **P(at least one of five) = 0.157**. STB's survival is
   about as surprising as nothing at all. This is `NUL-1` one level up, the same shape §2b-bis
   point 3 records for the universe/horizon/target choice.
4. ⚠️ **RULE 23 EXPLAINS BOTH APPARENT CLEARANCES, AND `validation.csv` STILL CANNOT SHOW IT**
   (`P4-2` confirmed open — the file carries `n_train`/`n_test`/`ic`/`r2`/`hit_rate` and **no**
   `n_dead_train`). Counting all-NaN-in-train channels externally, per fold, of each shortlist:

   | ticker | dead in train, folds 1-5 | folds' selected IC |
   |---|---|---|
   | HPG | 6, 3, 3, 3, **0** | −0.062 +0.198 −0.086 −0.074 +0.110 |
   | SSI | 2, 2, 2, 2, **0** | −0.059 +0.094 +0.143 −0.097 +0.129 |
   | **FPT** | **9**, 3, 3, 3, **0** | **−0.137** +0.122 +0.148 +0.097 +0.130 |
   | VIC | 6, 3, 3, 3, **0** | +0.026 +0.066 +0.024 −0.108 −0.115 |
   | **STB** | **8**, 5, 5, 5, **0** | +0.028 **+0.240 +0.191** −0.021 +0.069 |

   **FPT is the textbook case**: 9 of 20 shortlisted channels are constants in fold 1's
   training slice, fold 1 scores −0.137, every later fold +0.10…+0.15, and
   `ic_trend_per_fold` is **+0.0509** — the steepest of the five. Rule 23 says a rising trend
   on a ragged pool measures **data arrival**, not a strengthening signal.
   **STB is weak differently**: +0.1014 is carried by folds 2-3 alone, and fold 5 — the only
   fold where all 21 channels are alive in train — scores **+0.069**. At `n_eff_test = 76.7`,
   `SE(IC) ≈ 0.114`, so only fold 2 exceeds 2 sd and the other four sit inside 1 sd of zero
   (across its own folds, `t = +2.05`, p = 0.109).

⚠️ **`prop_*` MUST BE EXCLUDED FROM ANY RUN AT THIS TIME SCALE, AND IT WAS NOT.** Proprietary
flow starts **2023-01-03** — coverage **0.197-0.203** — yet `prop_sell_vol` and `prop_buy_val`
were shortlisted by **4 of 5** tickers and `drv_prop_participation` by 3. They are all-NaN in
the training slice of folds 1-4, imputed to the constant `0.0`, and then RANKED. Flow that
IS usable: **order stats from 2010-01-04** (≥0.95) and **foreign from 2012-01-03** (~0.70) —
§2d's top lever survives, the prop block does not.

⚠️ **What this does NOT establish.** It is one pool. `pool__ta`'s ~900 channels, the 19
`pool__economy_*` blocks and the 47 `pool__forex_*` blocks were **not** offered — and the
date-only blocks are the one thing that is structurally DEAD for a cross-section (a column
constant within a date has a constant within-date rank, `PRF-9`) and perfectly valid for a
single stock. **71 of 76 gold tables are date-only.** So the honest statement is *"the stock's
own 90 channels carry nothing at h=10 on these five names"*, not *"nothing does"*.

### ⚠️ 6-1-bis. THE SAME QUESTION ON 30 VN30 NAMES — and the POOLED answer flips

Run 2026-08-20. `pool__basic` (90 numeric channels), `return_10day`, `d=20 h=10`, 5
expanding folds, a `date_block` null **per name** with the whole selection re-run inside
each draw. 30 names, ~5.5 min each.

| | |
|---|---|
| observed `ic_mean` | mean **+0.0635**, sd 0.0571 |
| each run's own null | mean **+0.0023**, sd 0.0140 — well centred (se of a 10-draw null mean is 0.0175) |
| pooled excess | **+0.0611**, sd 0.0637 |
| naive `t` (29 df) | **+5.25** |
| cross-name dependence | ρ̄ = **+0.032** over the 5-fold IC vectors → `n_eff` **15.5** |
| **dependence-adjusted `t`** | **+3.77, p = 0.002** |

**This is the first time in this repo that a single-stock short-horizon selection has
beaten its own null in aggregate.** It does not overturn §2, and four measured reasons say
why:

1. ⚠️ **NO INDIVIDUAL NAME IS CONVINCING.** `z > +1.645` on **8 of 30** (chance says 1.5 —
   above chance, but it is 8), **5 names are NEGATIVE**, and **rule 3 fires on 22 of 30**:
   a shuffled draw beat the real data for 73 % of the names. The per-name picture is the
   same as §6-1's five; what changed is that 30 draws of a small effect resolve it and 5
   do not.
2. ⚠️ **THE EFFECT IS SMALL.** Mean selected IC **+0.064** at `n_eff_test = 76.7` per fold
   — about 1.3 SE for one name. It is detectable in aggregate and useless one name at a
   time, which is the same shape §2b found from the other direction.
3. ⚠️ **`NUL-1` IN FULL FORCE.** These nulls price the FEATURE search inside each run.
   Nothing prices the choice of `h=10` over `h=5`, of `return_10day` over three other
   labels, of VN30, or the fact that this is the fifth thread tried. §6-1 measured that
   cost directly at one level up: `P(at least one of five names above z=1.83) = 0.157`.
4. ⚠️ **A CLEARED SELECTION BAR HAS NEVER SURVIVED DOWNSTREAM HERE** — §5d (`z = +2.15`
   bought nothing) and TODO `P2-3`. Selection IC is not model skill and certainly not money.

⚠️ **The five-name result in §6-1 is superseded for the POOLED question and stands for
every per-name one.** `t = +1.45` on five names and `t = +3.77` on thirty are the same
effect at two sample sizes — the five happened to include VIC (`z −0.69`) and HPG
(`z +0.11`), two of the weakest. **That is what an underpowered sample of a small effect
looks like, and recording it is the point: the 5-name run was not wrong, it was small.**

⚠️ **The fold pattern rises monotonically** — mean IC per fold across the 30 names is
**+0.020 / +0.045 / +0.088 / +0.071 / +0.093**. Fold 1 trains on 500 rows and carries the
most dead channels (§6-1's rule-23 table). The null re-runs the selection inside every
draw, so it *does* price a ragged pool — but the trend is still the reason not to read
+0.064 as a stationary edge.

### ⚠️ 6-1-ter. WIDENING A SINGLE STOCK HURTS; WIDENING THE CROSS-SECTION HELPS

Two measurements on 2026-08-20, opposite in sign, and the contrast is the finding.

**Single stock** — HPG, `return_10day`, identical folds, `pool__basic` versus
`pool__basic + pool__stock_market + pool__bonds + pool__market_breadth +
pool__economy_vietnam`:

| | channels | kept | `ic_mean` | null mean | null MAX | **z** |
|---|---|---|---|---|---|---|
| narrow | 90 | 55 | **+0.0169** | +0.0109 | +0.0958 | **+0.11** |
| **wide** | **470** | 218 | **−0.0064** | +0.0119 | +0.0893 | **−0.40** |

⚠️ **This is the first time the date-only macro blocks have been offered to a RETURN target
on a single stock** — §6-1 named it as the one structurally-new lever left, because
`PRF-9` proved those blocks cannot rank a cross-section while they are perfectly valid for
one name. Offered, they take HPG **below its null's mean**. 19m58s for one name.

**Cross-section** — VN30, `cs_rank_10day`, 65,763 rows × 30 names × 2,288 dates
(2017-04-21 → 2026-06-26):

| | channels | kept | `ic_mean` | null mean | p95 | null MAX | **z** |
|---|---|---|---|---|---|---|---|
| `pool__basic` | 100 | 62 | +0.0204 | +0.0190 | +0.0370 | +0.0435 | **+0.10** ❌ |
| **+ the 145 reduced `pool__ta` channels** | 245 | 204 | **+0.0333** | +0.0136 | +0.0332 | +0.0342 | **+1.62** ⚠️ |

⚠️ **Neither is a pass** — 5b's null MAX (+0.0342) still exceeds its observed (+0.0333), so
rule 3 fires and `cleared_p95_not_a_pass` is the right label. What is measured is the
DIRECTION: width helps a panel and hurts a series, on the same data, in the same week.

### ⚠️ 6-1-quater. THE WIDTH LADDER RE-RUN AT h=10 — VN30 is z = +0.10 against top-150's +13.78

| | `PRF-2`, top-150 | **VN30** |
|---|---|---|
| N | 150 | **30** |
| dates | 4,368 (2009→) | **2,288 (2017→)** |
| `ic_mean` | **+0.1201** | **+0.0204** |
| null `sd` | ≈0.0065 | ≈0.014 |
| **z** | **+13.78** ✅ | **+0.10** ❌ |

Two things it establishes and one it does not:

1. ✅ **The `1/√N` mechanism reproduces exactly.** The null sd ratio is **2.15** against
   `√(150/30) = 2.24`. §2b's account of *why* width works is confirmed at a second horizon.
2. ⚠️ **But the OBSERVED IC collapsed too, 6×, and §2b says it should not have.** §2b's
   claim is *"the observed IC barely moves; the noise floor collapses"*. Here both moved.
   The extra suspect is §5d's: **a co-moving basket has less to rank**, and VN30 is the 30
   most co-moving names in the market — possibly the WORST 30-name cross-section available,
   not the best.
3. ⚠️ **It is NOT a single-variable comparison and must not be quoted as one.** N, the date
   window (4,368 → 2,288 sessions, because VN30 only reaches 20 listed members in 2017) and
   the universe RULE (pre-2014 liquidity vs today's index membership) all moved together,
   and all three push the same way. It bounds the cost of narrowing; it does not price N.

⚠️ **VN30 membership is `vn30.csv`, i.e. TODAY'S list with no history** (`UNIFIED_VN30` in
`preprocessor.py` carries the warning). Before 2017 the "VN30" here is 9-16 names that
happen to still be in the index in 2026. A within-date shuffle null is protected — every
draw sees the same basket — and any CAGR read off this universe is not.
