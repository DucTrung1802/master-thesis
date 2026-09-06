# The cross-sectional result — the one thing that works

> **Moved out of `CLAUDE.md` on 2026-09-06, VERBATIM.** The hub had grown to 2,549 lines while its
> own header called itself a map; [`.claude/rules/common.md`](../rules/common.md) R2 now caps it at
> **300 lines**, so the evidence lives here and the hub routes to it.
>
> ⚠️ **THE SECTION HEADINGS BELOW ARE UNCHANGED ON PURPOSE.** ~196 `§6-2-*` citations across this
> repo were already pointing at sections deleted from the hub earlier the same day; a `§6-0`
> citation written before this move still resolves — **to this file**. Nothing was rewritten but
> the relative links, which climb one directory less.
>
> **`CLAUDE.md` §6-0 through §6-0-ter in full** — the walk-forward that survives costs, the h=10
> track, seven architectures, the dataset-setting sweep, and every caveat that stops it being a
> conclusion. ⚠️ **§6-0-c is the caveat list and is the reason the rest is readable.**

---

### ⚠️ 6-0. THE HEADLINE, and what a new session should read first

**The cross-sectional chain works out of sample, over TEN expanding folds, after costs.**
§2's verdict is about SINGLE-STOCK SHORT-HORIZON prediction and is untouched; this is the
other thing, at the grain and horizon §2b and §2a-bis pointed to.

⚠️ **QUOTE THE WALK-FORWARD, NOT THE SINGLE SPLIT.** Until 2026-08-19 this section
headlined one train/val/test split whose test window happened to be a +20.2 %/yr VNINDEX
bull market — Sharpe +1.484 over **32 periods**, `se` 0.256. That number is still on disk
and still useful (see the leak check below), but it is not the evidence any more.

| | the walk-forward (PRF-1) | the single split |
|---|---|---|
| folds | **10**, test = calendar 2017…2026 | 1 |
| periods | **118** non-overlapping | 32 |
| `se_sharpe` | **0.155** | 0.256 |
| Sharpe @20/30/50 bps | **+2.026 / +1.991 / +1.921** | +1.551 @50 |
| CAGR @30 bps | **+47.5 %** vs market +14.6 % | +32.3 % |
| daily IC | **+0.1097**, `ic_t` +6.90, 80.6 % of days positive | +0.0863, t +3.47 |
| null, 200 within-date shuffles | **z = +12.18 / +12.28 / +12.46**, null MAX below observed at all three | z = +4.29 |
| shape | IC positive **9 of 10 folds**; beats the equal-weight universe **10 of 10** | — |

⚠️ **AND THE SAME SWEEP AT h=10 SCORES HIGHER ON EVERY ROW — 2026-08-20, §6-0-bis-3.**
Sharpe@30 **+2.531** over **236** periods, IC **+0.1412**, `ic_t` **+16.05**, z = **+18.58**,
IC positive **10 of 10** folds.

✅ **AND THE TWO WERE PAIRED THE SAME DAY (`P2-4`), WHICH SPLIT THE ANSWER IN HALF.**
`walkforward.pair` pairs them on the CALENDAR — both hold a book on all 2,360 shared
sessions, ρ = **0.723** — because they cannot be paired period by period (236 vs 118 periods
over different holding intervals). At 30 bps:

| estimand | h=10 − h=20 | Newey-West | block bootstrap 95 % CI | verdict |
|---|---|---|---|---|
| **mean return/yr** | **+17.0 pp** | t = **+3.53**, p = 0.0004 | **[+8.6, +25.7]** | ✅ significant |
| **Sharpe** | +0.44 | — | **[−0.079, +1.041]** | ❌ **not established** |

⚠️ **h=10 IS A HIGHER-RETURN, HIGHER-VOLATILITY TRACK, and the +0.54 that looked clean is
not resolvable at 2,360 sessions.** Zero sits inside the Sharpe CI at 20, 30 and 50 bps, and
p rises 0.067 → 0.141 as costs do — `backtest` §3's identity showing through, since h=10 pays
double the fee drag. ⚠️ **A non-significant ΔSharpe is not evidence of equality**: the CI
reaches +1.04, so this is underpowered, not settled. **The chain stays at h=20** — not
because h=10 lost, but because it has not won the test that matters.
`.claude/context/walkforward.md` §10.

**The chain that produced it**, and every stage is reproducible:

| stage | artefact | number |
|---|---|---|
| 2 · selection | `2026-08-18_072323__all__basic__cs_rank_20day` | `ic_mean` **+0.1075**, 20-draw bar +0.0388, null max +0.0410 (below observed), **z = +9.09** — §2b-bis |
| 5 · final_features | `unified_schema_all.rank_20day__final__d20_h20` | 624,448 × 17, **150 names**, 621,448 labelled |
| 6 · train_test_creator | `all__rank_20day__final__d20_h20__tr70_val15_test15__std` | 422,251 / 91,462 / 93,224 windows × 20 × 13 |
| 7 · model | `lstm__all__rank_20day__final__d20_h20__20260818-195738` | 4m 23s; test IC +0.0863, **t = +3.47** (`ICT-1` fixed) |
| 8 · result_evaluator | scored + indexed | clears its block-shuffled bar on `ic` and `dir_auc` — ⚠️ a floor, not a result (`NUL-1`) |
| 9 · backtest | `src/model/runs/<run_id>/results/backtest_test.csv` ⚠️ **inside the RUN FOLDER, which is gitignored (`RPR-1`)** — not repo-root `results/` | top-15 of 150, 50 bps, **ceiling-screened by default since 2026-08-19**: **+1.5512** test / **+1.7385** val |
| **W · walkforward** *(NEW 2026-08-19)* | `results/walkforward/` + 10 run folders | the table above. `.claude/context/walkforward.md` |
| **W · walkforward @ h=10** *(NEW 2026-08-20)* | `results/walkforward_h10/` + 10 run folders | Sharpe@30 **+2.531**, 236 periods, z = +18.58 — §6-0-bis-3. ⚠️ `--out` is REQUIRED or it overwrites the row above |

### ⚠️ 6-0-a. FOUR THINGS CLOSED ON 2026-08-19, AND WHAT EACH ONE SETTLED

| | verdict |
|---|---|
| **PRF-1** — is it one lucky split? | **No.** 10 folds, 118 periods, z = +12.3. ⚠️ Sharpe@30 DOES decay: slope −0.100/fold, first five +2.775 → last five +1.564. But 2023/24/25 are **+2.64 / +0.90 / +1.39**, all above their market; **2022 is the only bad fold and it is bad for everyone** (the universe itself ran −0.94 that year) |
| **PRF-7** — were the 13 channels fitted to the test folds? | **Bounded, and MILD.** Re-running the identical selection on dates < 2017 keeps **51 of 61** channels (Jaccard 0.761, **5.8 sd** above chance), shortlists 8 of 13 against a chance of 2.17, picks the same top two. ⚠️ It bounds the bias, it does not remove it |
| **PRF-8** — would a different architecture do better? | **Not at h=20.** 205,441 params, **2,033 params** and a **1,400-node GBT** all tie on the identical folds — paired \|t\| < 1 at every cost level. §6-0-ter. ⚠️ **RE-QUALIFIED 2026-08-21 when `P1-9` shipped**: that `\|t\|` tests MEAN RETURN, not Sharpe, and h=20 has **not** been re-scored. At h=10 the Sharpe test says **one** arm loses (`cnn`, p = 0.001), not three, and `gbt` GAINS at a nominal p = 0.044 that does not survive six arms (§6-0-ter-2) |
| **PRF-0** — is it buying names nobody could sell? | **Marginally, and removing it HELPS.** The model picks ceiling names 1.33× as often as chance (against 2.14× for a 5-day screen); excluding them takes test +1.484 → **+1.551**. ✅ Now applied BY DEFAULT in `backtest.build_panel`, not by a probe |

### ⚠️ 6-0-b. ✅ AND IT BEATS "PREDICT NO CHANGE" — the first thing here that does

`P4-12` fixed 2026-08-19: block B (`mase`, `rmsse`, `skill_score`, `beats_naive`) was
computed in `metrics.evaluate` only, so **every cross-sectional run carried `test_mase =
NaN`** — an absence, never a pass (§5 rule 2). `evaluate_panel` now calls a panel-aware
version and the column is filled:

| run | grain | `naive_kind` | `mase` | beats naive? |
|---|---|---|---|---|
| `lstm__vcb__close_adjust_5day` | series | `lag_h` | **21.36** | ❌ 21× worse than a random walk |
| `lstm__vcb__return_5day` | series | `zero` | **1.068** | ❌ — `P2-3`'s *"line to quote"* |
| **`lstm__all__rank_20day`** | panel | `zero` | **0.9937** | ✅ **the first in this repo** |
| the 30 PRF-8 fold runs | panel | `zero` | mean 0.988-0.991 | ✅ `lstm_small` and `gbt` in **10/10** folds, `lstm` in 9/10 |

⚠️ **AND THE MARGIN IS 0.6 %, WHICH IS THE POINT.** `mase = 0.9937` means the model's mean
absolute error is six parts in a thousand below "always predict the mean rank". That is
what an R² of **+0.0003** looks like from the other side. **It clears the line and the
size of the clearance confirms that the magnitudes carry nothing — the result is the
ORDER.** ⚠️ I wrote in TODO P4-12, before measuring, that it would *not* beat the naive on
magnitude. That was wrong, and the wrong prediction is left in the register.

⚠️ **RE-SCORING A PANEL RUN TAKES TWO COMMANDS.** `--rescore` rewrites each run FOLDER's
`results/metrics.{csv,json}`; `index.csv` is written **only** by `--rebuild-index`
(~8 min each over 33 runs). A run scored before 2026-08-19 and not put through **both**
carries no `mase` and, if scored before 2026-08-18, an `ic_t` overstated by `√h`
(`ICT-1`).

### ⚠️ 6-0-bis-2. PRF-2 — THE CHAIN AT h=10, AND IT ANSWERS THE REGIME QUESTION

Run 2026-08-19: the same chain, same universe, same architecture, **only the horizon
moved**. Selection cleared at **z = +13.78** (`ic_mean` +0.1201, bar +0.0355, null max
below observed, `n_eff`/fold **76.6** against h=20's 38.1). Test window 2023-11 → 2026-07,
63 periods, top-20 of 150, buyable only:

| h=10, one panel | CAGR@30 | Sharpe@30 | null z |
|---|---|---|---|
| **the model** | **+43.8 %** | **+2.442** (se 0.251) | **+8.99** ✅ |
| the 3-channel HAND rule | −5.1 % | **−0.263** | −1.72 ❌ |
| equal-weight universe | +4.0 % | +0.329 | — |

Paired (ρ 0.74): **ΔSharpe +2.71, `t` = +5.94**. Model run: IC +0.1393, `ic_t` +8.19,
85.8 % of days positive, `mase` **0.9874** ✅.

⚠️ **THE BREAK AFTER 2022 IS IN THE FEATURES, NOT IN THE MARKET.** `.claude/context/model.md` §11
and `backtest` §8g both found the edge dying post-2022 and read it as a regime wall. Hold
the window, the universe and the horizon fixed and move only the FEATURE SET: 19 selected
channels return +2.44 where three hand-picked ones return −0.26. **The market did not stop
being predictable; those three columns stopped predicting it.** That is `PRF-3`'s
hypothesis (2), and it re-opens §2d's ladder as the answer rather than a longer training
window. ⚠️ The hand rule's −0.26 is not a contradiction of its own +0.652 — that figure is
over 2018-2026, and §8g itself measured +0.011 for 2022-2026.

⚠️ **h=10 BEATS h=20 WHILE PAYING DOUBLE THE FEES** (8.8 %/yr against 4.4 % at τ=0.70,
50 bps) — +2.442 against +1.441 on the same universe, architecture and window. ⚠️ **One
split each**, `se_sharpe` ~0.25 — ✅ **and the walk-forward below settled it 2026-08-20.**

### ⚠️ 6-0-bis-3. THE h=10 WALK-FORWARD — 2026-08-20, 10 folds, and it holds

The run §6-0-bis-2 called for. Same geometry as `PRF-1`: top 150 by pre-2014 turnover,
`--first-test 2017-01-01`, expanding 12-month folds `oos2017…oos2026`, top-20, ceiling
screen. **33m 26s** sweep + **8m 59s** scoring, local RTX 3050, no Kaggle quota.

| pooled, top-20, buyable only | **h=10 (NEW)** | h=20 (`PRF-1`) |
|---|---|---|
| periods | **236** | 118 |
| `se_sharpe` | **0.128** | 0.155 |
| Sharpe @20/30/50 bps | **+2.601 / +2.531 / +2.391** | +2.026 / +1.991 / +1.921 |
| CAGR@30 | **+74.0 %** vs market +13.9 % | +47.5 % vs +14.6 % |
| daily IC | **+0.1412**, `ic_t` **+16.05**, 86.5 % of days positive | +0.1097, +6.90, 80.6 % |
| null, 200 within-date shuffles | **z = +18.42 / +18.58 / +18.86**, null MAX below observed at all three | +12.18 / +12.28 / +12.46 |
| shape | IC positive **10 of 10** folds; beats the universe **10 of 10** on Sharpe AND CAGR | IC positive 9/10 |

⚠️ **THE DECAY IS THE SAME AT BOTH HORIZONS, AND THE SLOPE ALONE SAYS OTHERWISE.** h=10's
Sharpe@30 slope is **−0.219/fold** against h=20's −0.100 — 2.2× steeper — but the
proportional fall is **−45.8 %** against **−43.6 %** (first five folds vs last five). The
absolute slope is steeper only because the level is higher. **Both horizons lose ~45 % of
their Sharpe across the sweep**; that is a shared property, not an h=10 defect.

⚠️ **THE TWO HORIZONS ARE NOT PAIRED AND CANNOT BE.** `walkforward.compare` pairs ARMS
inside one sweep — same dates, same panel, ρ = 0.88, which is what makes a paired `t`
meaningful. Two horizons give **236 and 118 periods over different holding intervals**, so
no period-wise correspondence exists. The +0.54 gap is two independent estimates with `se`
~0.13-0.16 each: **suggestive, and not the paired test §6-0-ter insisted on**. §5c is why
that matters — eleven architectures once spread IC over 0.227 and the whole spread was one
error bar.

✅ **No mechanical leak.** Restricting the h=10 track to the single split's own test window
(`2023-11-28 → 2026-07-24`, read from the dataset's `metadata.json`) gives **63 periods on
both sides**, IC +0.1307 against `PRF-2`'s +0.1393, Sharpe@30 **+2.257 against +2.442** —
a gap of **0.8 SE**. ⚠️ Its SIGN is opposite to the h=20 check's and at 0.8 SE that is not
resolvable; both horizons agree with their single split within noise.

✅ **THE SELECTION LOOK-AHEAD IS BOUNDED AT h=10 TOO — measured 2026-08-20, 9m 46s on a
T4.** Re-running the identical selection on dates < 2017 keeps **51 of 61** channels
(Jaccard **0.750**, chance 39.3 ± 2.1 → **+5.48 sd**), shortlists **7** against a chance of
1.90 (**+4.37 sd**), and puts **`drv_order_vol_imb` at #1 in both** — the same channel h=20's
probe put first. **10 of the 12 shortlist misses are in the early KEPT set**; only
`drv_close_z_21` and `n_sell_orders` are absent outright, and `n_sell_orders` was one of
h=20's misses too. ⚠️ The early run shortlists **9 against 19** because `n_eff_per_fold` is
**28.9 against 76.6** — that is POWER, not the market. ⚠️ **It bounds the optimism, it does
not remove it**: these 19 channels were still chosen over 2009-2026 including every test
fold. `.claude/context/walkforward.md` §9e.
⚠️ Survivorship protects `z = +18.6` and **not** +74.0 %/yr (§2c). `.claude/context/walkforward.md` §9.

⚠️ **`--out` IS LOAD-BEARING AND OMITTING IT DESTROYS `PRF-1`.** `walkforward`'s
`DEFAULT_OUT` is `results/walkforward/`, which holds the h=20 track; every artefact is
written by basename, so the documented command run at h=10 **silently overwrites it**. The
h=10 track lives in `results/walkforward_h10/`.

⚠️ **`PRB-1`, found and fixed in the same session**: two Kaggle PROBE runs had been merged
into the CHAIN's report root, where `final_features` groups them with the real runs — the
`PRF-7` window probe **silently** (the data window is not a `SETUP_KEY`) and the `FNM-1`
representation probe as a hard collision that blocked planning entirely. Probes now write
to `reports/feature_selection_probes/`. **A run that measures the SELECTION is not a run
that feeds the CHAIN, and only the ROOT separates them.**

### ⚠️ 6-0-ter-2. SEVEN ARCHITECTURES AT h=10 — 2026-08-21, and §6-0-ter does NOT reproduce

The same test as `PRF-8`, one horizon down, **224× of capacity** (1,398 decision nodes to
313,153 parameters) against §6-0-ter's 101×. Four arms were written for it (`bilstm`,
`cnnlstm`, `tcn`, `transformer`); all seven trained on ONE build of each of 10 folds.
**2h 48m sweep + 22m scoring, 0 errors.**

⚠️ **THE TABLE BELOW WAS RE-SCORED ON 2026-08-21 WHEN `P1-9` SHIPPED, AND THE VERDICT
COLUMN SPLIT IN TWO.** `t_ret` is the old `t_paired` — a test of the mean period-RETURN
gap. `d_sharpe` now carries its own block-bootstrap interval, and **the two disagree about
three of the six arms.** Both are paired against `lstm` over the same 236 periods (ρ
0.91-0.94); at 30 bps, 2,000 circular block draws:

| arm | capacity | Sharpe@30 | IC | `t_ret` (MEAN) | **`d_sharpe` [95 % CI]** | `p_sharpe` | null z |
|---|---|---|---|---|---|---|---|
| **`gbt`** | **1,398 nodes** | **+2.891** | **+0.1460** | −1.02 | **+0.360 [+0.013, +0.721]** | **0.044** ⚠️ | +22.57 |
| `transformer` | 68,417 | +2.622 | +0.1433 | −0.33 | +0.091 [−0.171, +0.385] | 0.537 | +20.08 |
| `tcn` | 18,113 | +2.622 | +0.1426 | −0.20 | +0.091 [−0.119, +0.339] | 0.406 | +20.25 |
| `lstm` *(ref)* | 208,769 | +2.531 | +0.1412 | — | — | — | +18.58 |
| `bilstm` | 313,153 | +2.474 | +0.1419 | **−2.09** ❌ | −0.058 [−0.279, +0.161] | **0.612** ✅ tie | +17.55 |
| `cnnlstm` | 30,369 | +2.367 | +0.1308 | **−2.15** ❌ | −0.164 [−0.472, +0.157] | **0.295** ✅ tie | +16.80 |
| **`cnn`** | 5,185 | +2.133 | +0.1171 | **−3.37** ❌ | **−0.398 [−0.678, −0.135]** | **0.001** ❌ | +15.37 |

⚠️ **"THREE LOSE SIGNIFICANTLY" WAS A CLAIM ABOUT MEAN RETURN. ON SHARPE, ONE DOES.**
`bilstm` and `cnnlstm` earn less per period at *lower volatility*, so their risk-adjusted
gap is indistinguishable from zero (p = 0.61 and 0.30). Only `cnn` loses on both.

⚠️ **AND `gbt` BEATS THE LSTM ON SHARPE AT A NOMINAL p = 0.044, WHICH DOES NOT SURVIVE THE
SIX ARMS THAT WERE TRIED.** Bonferroni over one reference and six challengers is
**0.05/6 = 0.0083**: `cnn`'s 0.001 clears it, `gbt`'s 0.044 does not. That is `NUL-1` one
level up and the same shape §6-1 point 3 records for the five-ticker search — so the honest
statement is *"`gbt` is the best arm measured and its advantage is not established"*, never
*"`gbt` wins"*. ⚠️ Its `p_sharpe` also **rises with cost** (0.040 → 0.044 → 0.051 at
20/30/50 bps), which is `backtest` §3's identity showing through: `gbt` trades more.

⚠️ **SO THE SENTENCE THAT SURVIVES IS NARROWER THAN BOTH EARLIER ONES.** Not *"the
architecture is worth nothing"* (§6-0-ter, h=20, and also read off the MEAN column) and not
*"three lose"*. It is: **one architecture is measurably worse risk-adjusted (`cnn`, which
pools the sequence away), the rest tie, and the best-looking one cannot be separated from
the reference once the search is priced.**

✅ **AND THE SEED FLOOR UNDER THIS WHOLE TABLE WAS MEASURED 2026-08-21 — it is
`|d_sharpe| ≈ 0.09`.** Every config in the repo is `seed: 42` (32 of 32) and every row above
is ONE fit per arm per fold, so five `gbt` arms differing only in the seed were run over the
identical folds (13m 16s; `.claude/context/walkforward.md` §15). Pooled Sharpe@30 came back
**2.845 … 2.979, sd 0.054**, max paired `d_sharpe` **0.088**. Reading the table against it:

| arm | `d_sharpe` | × the seed floor | verdict |
|---|---|---|---|
| `cnn` | −0.398 | **4.5×** | ✅ real |
| `gbt` | +0.360 | **4.1×** | ✅ **not seed luck** — and still not established (Bonferroni) |
| `cnnlstm` | −0.164 | 1.9× | tie |
| `transformer` / `tcn` | +0.091 | **1.0×** | ⚠️ **the whole gap is one seed** |
| `bilstm` | −0.058 | **0.7×** | ⚠️ **inside** the floor |

⚠️ **SO THE ORDERING OF THE FOUR MIDDLE ARMS IS NOT AN ORDERING.** *"Tied"* and *"the
measured gap is the size of a reseed"* are different statements and only the second closes
it. ⚠️ **A PER-FOLD CELL IS 4.4× NOISIER STILL** — mean per-fold Sharpe range over five
seeds **0.593** against the pooled **0.134**, worst fold **1.079**. Never compare two arms
in one fold. ✅ **The DECAY is not a seed artefact** (slope −0.308 ± 0.027, the first-to-second
half fall −55 % in all five). ⚠️ **One architecture, and the cheapest one**: `gbt` resamples
only rows and columns, while an LSTM also varies its initialisation — the reference arm's
floor could be larger and is unmeasured.

⚠️ **`ac1` IS THE REASON THE BOOTSTRAP IS TRUSTED HERE**: the lag-1 autocorrelation of every
arm's difference series is **−0.09 … +0.06**, so the periods really are near-independent and
`block=2` is not doing hidden work. Measured, not assumed — it is printed per row.

⚠️ **AND IT IS NOT A CAPACITY STORY.** The best arm is the SMALLEST (1,398 nodes); the
largest (313 k) sits *below* the 209 k reference; the second-worst is 5,185 parameters.
What separates them is inductive bias — `cnn` pools the sequence away and loses 0.40
Sharpe, while the two arms keeping a per-timestep view of the whole window tie. §6-0-ter's
reading that **the sequence inside the lookback is worth nothing** reproduces and is the
better explanation of the whole table.

✅ **`P1-9` SHIPPED 2026-08-21 AND THIS IS WHAT IT MEASURED.** `compare.paired()` computed
its `t` on the mean period-RETURN difference while the table printed `d_sharpe` beside it
bare, so both this section and §6-0-ter were read off the wrong column. It now reports
**both estimands**, each with its own interval, by reusing `walkforward.pair`'s
`block_bootstrap_diff` rather than a second implementation. ✅ **The h=20 `PRF-8` sweep was
re-scored the same day and ITS ties hold on both** (§6-0-ter) — so the disagreement is a
property of h=10, not of the fix.

⚠️ **AND `gbt` CHANGES SIGN BETWEEN THE HORIZONS — measured 2026-08-21, and it is the
strongest argument against promoting it.** Same tool, same `k`, same universe, same
reference arm: `d_sharpe` vs `lstm` is **+0.360 [+0.013, +0.721]** at h=10 and **−0.016
[−0.299, +0.291]** at h=20 (`p_sharpe` 0.044 against **0.941**). Two estimates that
disagree in SIGN across a neighbouring horizon are what a null effect looks like. ⚠️ It is
**not** a paired test across horizons — only `walkforward.pair` can do that and it has not
been run on the arms — so read it as two independent estimates.

⚠️ **AND "BEST EPOCH IS 1" IS AN LSTM PROPERTY, NOT A PROPERTY OF THE PROBLEM.** That
sentence has been quoted four times here as evidence capacity is worthless. Across these 70
runs only **43** stop by epoch 2: `cnn` averages **7.7** (max 20) and `tcn` **5.7** (max 13).
Attach it to an architecture from now on. `.claude/context/walkforward.md` §11.

### ⚠️ 6-0-ter-3. AND NEITHER DO THE DATASET SETTINGS — 2026-08-21

Six full walk-forward tracks at h=10, one per setting, `gbt` throughout, scored **paired**
against a baseline that reproduces §6-0-ter-2's `gbt` row to every digit.

| setting | Sharpe@30 | Δ | paired `t` | ρ |
|---|---|---|---|---|
| validation 12 → **6** months | +2.9655 | +0.075 | +0.33 | 0.972 |
| validation 12 → **24** months | +2.7562 | −0.135 | −1.32 | 0.946 |
| refold every **6** months (**20 folds**) | +2.8977 | +0.007 | −0.09 | 0.989 |
| `scale_target` off | +2.8910 | **0.0000** | **NaN** | **1.0000** |

**Every setting ties; no `|t|` reaches 1.4.** ⚠️ **`step6` is the one to read**: retraining
**twice as often** moves nothing (ρ 0.989), so the ~45 % Sharpe decay across the sweep
(§6-0-bis-3) is **not staleness** — refitting does not arrest it.

⚠️ **`scale_target` is bit-identical because trees are scale-invariant, not because the
knob is inert.** Splits depend on the ORDERING of `y`, standardising is affine, and
`engine._write_predictions` inverse-transforms — end to end, the identity. **For a neural
net it would not be**, and choosing `gbt` for speed made that one setting unanswerable for
the family that uses it. ⚠️ `rank_min_width` was REFUSED by `compare` as a different
experiment (349,371 rows against 349,581) — it moves the LABEL, not the split.
⚠️ **`lookback` was never swept and is the one that would matter**: `d` comes from the
source TABLE NAME, so each value needs its own selection run. `.claude/context/walkforward.md` §12.

### ⚠️ 6-0-quater. PRF-9 — MORE FEATURES DO NOT PAY EITHER, AND THAT CLOSES THE SECOND LEVER

Run 2026-08-19. `pool__ta` is the only widening available at all — `PRF-9`'s survey found
**71 of 76 gold tables are date-only**, and a column identical for every ticker on a date
has a constant within-date rank, so ~4,500 channels are *structurally* incapable of ranking
a cross-section. `pool__ta`'s 711 numeric channels are the exception.

**They were offered, and they did not pay.** 120-channel selection (90 `pool__basic` + 30
`pool__ta`, the latter chosen LABEL-FREE by `feature_selection.prune`), 22 shortlisted of
which 6 from `pool__ta`, built into `rank_20day__final__d20_h20__wide` and trained with the
architecture, schedule, seed and universe copied unchanged. Priced against the narrow chain
on the **intersection** of their rows (646 dates, 32 periods, top-15):

| | daily IC, same rows | Sharpe@30 | null z |
|---|---|---|---|
| wide (22 ch) | **+0.1053** | +1.496 | +4.53 |
| narrow (13 ch) | +0.0927 | **+1.623** | +5.42 |

Paired, ρ **0.90**: ΔSharpe **−0.126**, `t` = **−0.29**. **The extra channels moved the
shortlist and not the money.**

⚠️ **REPRODUCED AT h=10, 2026-08-21, AND IT HOLDS.** 162 candidates (90 `pool__basic`
+ 72 pruned `pool__ta`) against `PRF-9`'s 120, a GBT instead of an LSTM, selection on a T4
in 44m 12s: **21 shortlisted, 18 `pool__basic` and 3 `pool__ta`**, top NINE all
`pool__basic`, `drv_order_vol_imb` #1 again. Priced downstream on the 340,183 rows the two
chains SHARE — daily IC **+0.1520 vs +0.1484**, Sharpe@30 **+2.8136 vs +2.8910**, paired
over 236 periods at ρ 0.943: **`t` = +0.46**. ⚠️ **IC up, Sharpe down — the same split
`PRF-9` found**, and the paired test separates neither from zero.
`.claude/context/walkforward.md` §13.

⚠️ **WITH `PRF-8`, TWO OF THE THREE OBVIOUS LEVERS ARE NOW CLOSED BY MEASUREMENT.** A model
101× smaller ties; 30 more candidate channels tie. **The 13 original channels are the
result.** What remains is honest execution (`PRF-4`/`PRF-5`) and NEW INFORMATION (`PRF-6`,
§2d) — not a better model and not more of this data.

⚠️ **`VRM-1` bounds how much of `pool__ta` could be tried.** Only **30 of 405** pruned
channels were offered, because the width ceiling on a T4 is **VRAM** — `xgb_shap`'s SHAP
contributions, `(n_rows, channels × 6 + 1)` — and not host RAM, which survived 24.5 GB at
140 channels. So this is *"these 30 did not pay"*, never *"`pool__ta` is useless"*. ⚠️ All
three of `MEM-1`, `P3-2` and `PRF-9` predicted the host wall and all three were wrong about
which one binds.

### ⚠️ 6-0-c. What the headline still does NOT say

**(1)** ⚠️ **It ranks, it does not price.** R² test **+0.0003**, RMSE 0.29065 against a
constant-predictor 0.29070. Only the ORDER carries — see §6-0-b.
**(2)** `long_short = +0.0635` is a **rank** spread, not money — the label is a rank.
**(3)** `NUL-1` — no null anywhere in this chain prices in the feature selection, the
architecture search, the early stopping, or the choice of `h=20`, `k=20` and the universe.
**(4)** ✅ `FNM-1` **MEASURED 2026-08-19 and it holds.** The selection scored those 13
channels under `feature_normalize=cs_rank` while the dataset feeds them globally
standardised — two representations, and §5 rule 1 says a bar computed for one says nothing
about another. Re-running the identical selection under `feature_normalize=none` (22m 04s
on a T4; every other setup key including `env_fingerprint` unchanged) keeps **12 of the 13**,
kept-set overlap **53 of 61**, Jaccard 0.779, **+5.90 sd** above chance, and the **same
channel at #1** (`drv_order_vol_imb`). ⚠️ **What that establishes is narrower than "it
passed": the CHANNEL SET is representation-invariant, but the BAR does not transfer** — the
`none` run carries no null, so **`z = +9.09` remains a `cs_rank` number**. ⚠️ And `none`
scores *higher* on the same folds (`ic_mean` **+0.1215** vs +0.1075), which says the
within-date feature ranking is not what is doing the work — read it as a channel-set fact,
not as a result, because it has no bar. TODO P1-6.
**(5)** **Survivorship protects the `z` and not the CAGR** (§2c). Every shuffled draw picks
from the same survivor basket, so +12.3 stands and **+47.5 %/yr does not**.
**(6)** **No slippage, no ADV cap, no floor-day exclusion on the SELL side** — `PRF-4`.

### ⚠️ 6-0-bis. AND STAGE 9 ANSWERED THE TRADABILITY QUESTION — portfolio yes, ONE STOCK no

`src/backtest/` (2026-08-18) is the first thing here that charges costs. Two results, and
the second one matters more for anyone who wants a buy/sell signal on a single name.

**The portfolio works, at this horizon, after costs.** Top-15 of 150, rebalanced every 20
sessions, 50 bps, long-only (no shorting — HOSE does not offer it), clearing a 200-draw
within-date shuffle null at **z = +4.29** (test) and **+6.10** (val), null MAX below
observed in all four cells.

⚠️ **THE NUMBERS MOVED UP ON 2026-08-19 AND THE REASON IS `PRF-0`, NOT A BETTER MODEL.**
`backtest.build_panel` now drops names sitting at their exchange's daily ceiling on the
entry date — they have no sellers, so buying them was fiction — and the stage prints how
many it dropped. It used to be a probe a reader had to remember to run:

| 50 bps, top-15 | as first reported | **screened, the default now** | rows dropped |
|---|---|---|---|
| test | +1.4845 / CAGR +30.5 % | **+1.5512** / **+32.3 %** | 1,708 (1.83 %) |
| val | +1.7367 / CAGR +69.9 % | **+1.7385** / **+70.0 %** | 3,437 (3.76 %) |

⚠️ Both reproduce §8h's probe EXACTLY, which is how the change was verified rather than
assumed. ⚠️ **And it is the walk-forward, not this split, that should be quoted** — §6-0. `k` is not a
knife-edge — Sharpe decays monotonically 1.53 (k=10) → 0.81 (k=75).

⚠️ **This CONTRADICTS §11's regime wall in the direction §2a-bis predicts.** §11 measured
net@20bps **−0.51 in 2022-26** and called the recent regime unlearnable; this window IS
2023-26 and returns +1.48 at **50** bps. The two studies differ in the HORIZON — 5 days
against 20 — which is the variable §2a-bis says nobody controlled for.

⚠️ **VCB, SPECIFICALLY: ZERO TRADES IN 33 PERIODS.** Its median percentile among the 150
is **0.273**; the maximum it ever reached is **0.826**, so it never touches the 0.90 entry
band. That is a correct call, not a failure — VCB returned **+1.45 % CAGR** over the test
window while the universe made 5.96 % and VNINDEX **20.2 %**. Lowering the band until it
trades gives Sharpe −0.65…+0.38 on 1-12 periods, and none of it beats holding the stock.
**A correct "do not hold this" is not a tradable signal for that stock.**

⚠️ **THE HORIZON IS ALSO THE COST VARIABLE, and this had never been written down.** At
turnover 0.70 and 50 bps the annual fee drag is **17.6 % at h=5**, 8.8 % at h=10, **4.4 %
at h=20** — against a top-100 benchmark CAGR of 9.75 % (§2a-bis). **At h=5 the fees alone
exceed the market's entire return.** Four single-stock defeats at `h=5` were never going
to be rescued by a better model. `.claude/context/backtest.md` §3.

⚠️ **What stage 9 does NOT establish**: `NUL-1` in full force (the null prices in the
universe, cost, schedule and `k` — never the feature selection, the architecture search or
the choice of window); **32 periods**, `se_sharpe` 0.256; survivorship protects the z but
**not** the +30.5 % (§2c); `val` chose the early-stopping epoch; one split, no
walk-forward.

⚠️ **It cannot give you a price for one stock, and that is structural.** The label removes
the market factor by construction, so inverting it needs a 20-day market forecast plus a
cross-sectional dispersion forecast — the two things §2 has failed at four times. What it
answers is *"where will VCB sit among these 150 over the next 20 sessions"*, and reading
that requires scoring all 150 on the same date.

### ⚠️ 6-0-ter. AND THE ARCHITECTURE IS WORTH NOTHING — PRF-8, 2026-08-19

`src/walkforward/` runs the h=20 chain over **ten expanding folds** (PRF-1, same day), and
PRF-8 then ran three architectures over the **identical folds and one build of each fold's
tensors** — 15m 03s. Pooled over 118 non-overlapping periods, top-20 of 150, buyable only:

| arm | capacity | IC | `ic_t` | Sharpe@30 | `z` (200 draws) |
|---|---|---|---|---|---|
| `lstm` | **205,441 params** | +0.1097 | 6.90 | **+1.991** | +12.28 ✅ |
| **`lstm_small`** | **2,033 params — 101×** | +0.1239 | 9.49 | **+1.997** | +12.43 ✅ |
| `gbt` | **1,400 decision nodes** | +0.1249 | 8.90 | **+1.975** | +11.88 ✅ |

⚠️ **PAIRED, because the arms share the market factor** (ρ = 0.88 between their period
returns, so `se_sharpe = 0.155` is the error bar on the wrong quantity): `lstm_small`
`t = +0.87…+0.88`, `gbt` `t = +0.42…+0.47` at 20/30/50 bps. **Every |t| < 1.**

✅ **RE-SCORED 2026-08-21 THROUGH THE FIXED `compare`, AND THE TIES HOLD ON BOTH
ESTIMANDS.** That `t` tested the MEAN RETURN (`P1-9`); the run that settles it took
**1m 29s** and reproduced §6-0-ter to every digit — Sharpe@30 **1.9913 / 1.9970 /
1.9750**, `se_sharpe` 0.1553 — while adding the risk-adjusted column: `lstm_small`
**`d_sharpe` +0.006 [−0.289, +0.381], p = 0.903**; `gbt` **−0.016 [−0.299, +0.291],
p = 0.941**. ⚠️ **So *"a 101× capacity span ties at h=20"* is now a risk-adjusted result
and not only a return one, and NO caveat is left on this row.** ⚠️ At h=10 the same code
found the two columns disagreeing about three of six arms (§6-0-ter-2), so the fix is not
biased toward finding disagreement — it finds it where it is.

⚠️ **THE RESULT LIVES IN THE 13 CHANNELS, NOT IN THE ARCHITECTURE**, and this is the fourth
independent measurement pointing that way — after §5c's eleven architectures inside one
error bar, `P2-3`'s *"best epoch 1 of 21"*, and PRF-1's nine-of-ten folds stopping at epoch
1. It is the first that moved capacity DELIBERATELY. ⚠️ **The sequence inside the lookback
is worth nothing either**: `model.gbt` compresses each (20, 13) window to **78 window
statistics where the LSTM sees 260 numbers**, and it ties.

⚠️ **So "try a bigger model" is closed as an answer to anything in this repo** — ⚠️ **but
"any model will do" is NOT, and 2026-08-21 measured the difference** (§6-0-ter-2): at h=10 a
CNN loses **0.40 Sharpe** to the LSTM, and it is the ONE arm that loses on the
risk-adjusted test as well as on mean return (**p = 0.001**, the only one surviving a
correction for six arms). The bidirectional LSTM's `t` = −2.09 is a MEAN-RETURN loss whose
Sharpe gap is a tie (p = 0.61). Bigger buys nothing; the WRONG INDUCTIVE BIAS costs. What is
left is FEATURES (TODO `PRF-9`, 90 → 800 candidates), the HORIZON (`PRF-2`), honest
EXECUTION (`PRF-4`/`PRF-5`) and new DATA (`PRF-6`). ⚠️ It also makes `PRF-7`'s bounded
selection look-ahead close to the WHOLE story about where this Sharpe comes from rather
than part of it — the only other candidate has been ruled out. ⚠️ **Not a claim that the
small model should replace the big one**: nothing was re-tuned for it, and a tie under one
schedule is not an optimum. `.claude/context/walkforward.md` §8.

⚠️ **TWO CONCURRENT `walkforward` SWEEPS SILENTLY CORRUPT EACH OTHER** (measured on this
run, and it cost the first attempt). Every fold writes
`train_test_set/<ticker>__<table>__…__<tag>` — a name derived from the DATA with no term
for which process built it — saved with `replace=True` and deleted once its arms are done.
The loud half is a `FileNotFoundError` in the second sweep; **the silent half is the first
sweep reading tensors the second is mid-`np.save` on**. `run.namespace_lock` refuses the
second sweep now, by pid, taking over a lock whose holder is dead.
