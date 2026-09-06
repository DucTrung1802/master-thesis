# The verdict — what four years of modelling PROVED

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§2a`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **This is `CLAUDE.md` §2 in full.** Read it before proposing any modelling work: §2a is what
> failed and how it was measured, §2a-bis the horizon nobody controlled for, §2b the width ladder
> that survived, §2c its honest caveats, §2d the one lever left.

---

## 2. ⚠️ THE VERDICT — read this before proposing any modelling work

**Single-stock short-horizon prediction does not work here, and this has been established
four independent times.** The one thing that survives its own null is the
**cross-sectional relative rank** at ~100+ names.

### 2a. What failed, and how it was measured

| thread | target | result | where |
|---|---|---|---|
| LSTM sweeps, 9 lookbacks × 3 targets (27 runs) | `return_5day`, `direction_5day`, `probability_gain_5pct_5day` | **no lookback beats the zero-baseline**; `dir_auc` ≈ 0.5 throughout | `model` §10 |
| feature selection, VCB, 5 configs | `return_5day` / `return_rel_5day` × 3 representations | **every one inside its own shuffled-label null** | `feature_selection` §6b–6d |
| news sentiment, 3 tickers | price level / direction / ≥5% jump / price-defined sentiment | no signal — and adding sentiment **makes price/TA models worse** (QWK 0.175 → 0.045) | `sentiment` §6, §6a |
| news EVENTS (not tone), 777 tickers | `rel5` / `rel10` / 1-13 weeks, paired by fold | **7 paired tests, every \|t\| < 1.3**, folds won 2-4 of 6, signs mixed. §6a reproduced at 259× the width | `orchestration/todo.md` (retired 2026-08-17) |
| **single stock, h=10, 5 tickers (2026-08-19)** | **`return_10day`** on `pool__basic`, HPG/SSI/FPT/VIC/STB | **pooled excess over each run's own null +0.0332 ± 0.0229, t = +1.45, p = 0.220**; rule 3 fires on 4 of 5 | §6-1 below |
| literature, 23 papers | others' claims | **not one reports a naive baseline**; reported skill tracks test-set size (0.90@10d → 0.56@100d in one paper's own table); best honestly-run paper gets MCC 0.069 | `experiment_10` |

### ⚠️ 2a-bis. THE HORIZON IS THE VARIABLE NOBODY CONTROLLED FOR — and `h=5` is the worst of it

Measured 2026-08-03 on the weekly/daily news panels and **recovered 2026-08-17 from a
retired todo file, where it was the only copy.** The `controls` block — momentum
1/4/12/26 weeks plus liquidity, no text at all — is the one thing in that study that
worked, and whether it works **depends entirely on the horizon**:

| horizon | universe | `controls` CAGR | benchmark | verdict |
|---|---|---|---|---|
| **rel5** (5 sessions) | top-100 | **−2.78%** | 9.75% | ❌ loses |
| **rel5** | top-30 | **2.68%** | 16.48% | ❌ loses |
| rel10 | top-100 | 9.86% | 9.98% | ❌ ties |
| rel10 | top-30 | 7.69% | 16.74% | ❌ loses |
| **4 weeks** | top-30 | **30.39%** | 18.07% | ✅ Sharpe 1.10 |
| **13 weeks** | top-30 | **28.63%** | 19.34% | ✅ Sharpe 1.06 |

**`controls` only beats its benchmark from 4 weeks out.** At 5-10 sessions it loses even
on the most liquid names — so at `h=5` it is not only news that dies, **momentum dies
too**. MCC on the full universe was +0.052…+0.061, positive in **30 of 30 folds**, and it
cost nothing: no corpus, no labelling, no fine-tune.

⚠️ **This bears directly on §2's verdict and on everything run at `d=20, h=5`.** Four
independent threads failed at a 5-day horizon; this is the one measurement that asked
whether the HORIZON was the problem rather than the features, and its answer is that VN
gives signal at 4-13 weeks and not at 5-10 sessions. Two independent sources predicted
it — the `project-vcb-forecasting-conclusion` memory, and paper 57 (daily news predicts
1-2 days then stops at t = 1.2; weekly news predicts 13 weeks). **Nothing in this repo
has yet been run end to end at a 4-week horizon.**

### 2b. What survived — the width ladder

Same pipeline, same target, same `d=20, h=5`, only the number of names changes. **The
observed IC barely moves; the noise floor collapses.**

| universe | N | daily-IC sd | observed IC | null p95 bar | z | clears |
|---|---|---|---|---|---|---|
| VCB | 1 | ~1.0 | +0.0559 | +0.0556 | +1.56 | ❌ |
| BANK (GICS 401010) | 20 | 0.244 | +0.0087 | +0.0249 | +0.11 | ❌ |
| **BANK re-measured 2026-08-10** | **20** | **0.230–0.280** | **−0.0106** | **+0.0216** | **−1.71** | ❌ **below its null's MEAN** |
| VN30 | 30 | 0.205 | +0.0233 | +0.0248 | +1.42 | ❌ |
| **VN100** | **100** | 0.130 | +0.0289 | +0.0117 | **+6.09** | ✅ |
| **LIQUID301** | **301** | — | +0.0768 | +0.0245 | **+18.45** | ✅ |
| ALL | 780 | — | +0.109 | *never ran* | — | ⚠️ unverified |

Three things this ladder means:

1. **The practical threshold is ~100 names, not ~30.** VN30 fails at `z = +1.42`,
   indistinguishable from one stock. A 20-name sector is worse.
2. **The mechanism is `1/√N` precision, not more observations.** `n_eff` is `n_dates/h`,
   **not** `n_rows/h` — 100 stocks on one Tuesday are one observation of the market. The
   cross-section buys precision per observation and nothing else.
3. **The rank target is the whole result.** Swapping `cs_rank_5day` for raw `return_5day`
   on the same panel and folds drops the IC 4× and pushes the hit rate below a coin.

### ⚠️ 2b-bis. THE WIDTH LADDER MEETS THE HORIZON — `cs_rank_20day`, top-150, **z = +9.09**

Measured 2026-08-18 on a Kaggle T4, **6 h 07 m**, the first time this repo has run a
cross-section at the horizon §2a-bis pointed to. `unified_schema_all`, top **150** names
by median matched turnover **before 2014-01-01**, 624,448 rows × 4,368 sessions,
`d=20 h=20`, `n_eff = 218`, 90 channels → 61 kept.

| | |
|---|---|
| observed `ic_mean` | **+0.1075** — folds +0.060 / +0.124 / +0.153 / +0.104 / +0.097, **all five positive** |
| `ic_trend_per_fold` | +0.0054 — flat, not decaying (rule 5) |
| null: 20 draws, `date_block`, **the whole selection re-run inside each** | mean **+0.0291**, sd 0.0086 |
| p95 BAR | **+0.0388** |
| null MAX | **+0.0410** — *below* the observed, so **rule 3 does not fire** |
| **z** | **+9.09** · p = 0.0476 (the 1/21 floor — read z) · 0 failed draws |

**This is the strongest selection-stage evidence in the repo**, and the first at a 4-week
horizon. Four things it does **not** establish, and they are the reason it is filed here
rather than in a conclusion:

1. ⚠️ **THE NULL'S MEAN IS +0.0291, NOT ZERO.** The procedure earns +0.029 on a shuffled
   label, so the excess over chance is **+0.078, not +0.1075** — quoting the raw IC as
   "the skill" overstates it by 37 %.
2. ⚠️ **NO HOLDOUT.** §2c records exactly this trap: the VN100 result cleared its bar and
   then the holdout gave +0.011 against a shuffled control of +0.0071. `HOLDOUT_START` was
   `None` here, so this is cross-validation with an honest bar, **not** an untouched tail.
3. ⚠️ **The null prices in FEATURE selection, not EXPERIMENT selection.** The universe, the
   horizon and the target were each chosen using prior evidence (§2a-bis, §2b). That is
   `NUL-1`'s shape one level up, and no null here prices it.
4. ⚠️ **A cleared selection bar has never yet survived downstream in this repo** — §5d and
   TODO P2-3 both. The model stage is the next question and not a formality.

⚠️ **Do not paste this row into §2b's ladder.** That table is `h=5` at one lookback; this
is `h=20`, a different label, a different `n_eff`, and a universe fixed on pre-2014
liquidity (**358 of 781 tickers could never enter it**, and it is not point-in-time).

### 2c. ⚠️ And the honest caveats on the survivor

- **The holdout does NOT confirm it.** +0.011 against a shuffled control of +0.0071, with
  SE ≈ 0.013. The uncontaminated estimate of what this earns out of sample is **+0.011**,
  not +0.029. (`feature_selection` §9g)
- **The universe is 100% survivors** — `silver.stocks_basic` holds no delisted name. This
  biases the overall result *toward* zero, **except** through `close_adjust`, where the
  truncation itself manufactures the "cheap stocks outperform" pattern.
- **It has never been traded profitably.** Costed walk-forward: AUC ceiling ≈ 0.52–0.53,
  65–78%/leg weekly turnover, net@20bps **+1.46 Sharpe (2017-20) vs −0.51 (2022-26)**, dead
  at 40bps. (`model` §11, `experiment` exp_3)
- **The current end-to-end chain shows no skill on either universe** — see §6.

### 2d. The one lever left

**New information, not more modelling.** Ranked: active/aggressor buy-sell imbalance
(needs intraday tick), fundamentals/earnings surprise, news+disclosure event dates,
**point-in-time index membership** (kills the survivorship bias above), order-book L2.
`model` §11 has the full ranked list with why each matters.

---
