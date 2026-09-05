# walkforward — CONTEXT (PRF-1, created 2026-08-19)

> **The question**: every backtested number in this repo came from ONE train/val/test
> split whose test window happened to be a **+20.2 %/yr VNINDEX bull market**. A single
> split cannot tell *"the edge decayed"* from *"this split was lucky"*.
> `model/CONTEXT.md` §11 used 28 expanding folds; this brings the current chain to that
> standard. Read `backtest/CONTEXT.md` first — this file is the walk-forward version of
> its §4.

---

## 1. Run it

```powershell
cd src
python -m walkforward --ticker all --table rank_20day__final__d20_h20 `
    --config lstm__all__rank_20day__final__d20_h20.yaml --first-test 2017-01-01
#   measured 2026-08-19: 10 folds, ~35 min end to end on an RTX 3050
python -m walkforward.evaluate --top-k 20 --draws 200 --horizon 20 --universe all
```

Writes `results/walkforward/{folds,per_fold,predictions_oos}.csv` and one run folder per
fold, named `…__oos<YEAR>__<timestamp>`.

---

## 2. The geometry, and the one look-ahead that remains

```
|<--------------- train (expanding) --------------->|<-- val 12m -->|<-- test 12m -->|
                                                   gap             gap
```

Ten folds, test = calendar 2017 … 2026. `val` exists only to choose the early-stopping
epoch. **The `d + h − 1 = 39`-row purge is applied at both interior boundaries** by
`TrainTestCreator` — verified on fold 0, whose train ends **2015-11-06** and not
2015-12-31.

⚠️ **The scaler, the imputation median and the coverage screen are REFIT per fold.**
Re-slicing one split's tensors would leak later statistics into earlier folds.

⚠️ **WHAT IS STILL LOOK-AHEAD, AND IT IS NOT SMALL:** the **13 channels were selected on
the WHOLE sample** (2009-2026) against the label. Re-running the selection per fold is
~6 GPU-hours each on a T4. So **the walk-forward removes the MODEL's look-ahead, not the
SELECTION's** — every fold's LEVEL is optimistic. What stays honest is the **SHAPE**: every
fold carries the identical advantage, so a decaying fold series is still evidence of decay.
§11 made the same trade.

⚠️ **`null_draws = 0` per fold.** The verdict here is the backtest against Buy&Hold, not
`ic_clears` (`NUL-3`).

---

## 3. ⚠️ THE RESULT — 10 folds, 2017-2026, top-20 of 150, held 20 sessions, 30 bps

Buyable names only (ceiling rows dropped, `PRF-0`): **9,236 of 348,081 = 2.65 %**.

| fold | IC | `ic_t` | days IC>0 | **Sharpe@30** | market | CAGR@30 | market |
|---|---|---|---|---|---|---|---|
| 2017 | +0.164 | 6.41 | 96.8 % | **+3.09** | +2.72 | +73.6 % | +35.9 % |
| 2018 | +0.120 | 3.46 | 81.6 % | **+1.52** | −0.80 | +23.3 % | −9.9 % |
| 2019 | +0.123 | 4.72 | 92.0 % | **+2.92** | +0.63 | +50.8 % | +5.4 % |
| 2020 | +0.144 | 5.50 | 90.9 % | **+2.88** | +1.74 | +101.6 % | +60.7 % |
| 2021 | +0.056 | 1.30 | 60.4 % | **+3.46** | +2.51 | +156.9 % | +98.0 % |
| **2022** | +0.180 | 2.50 | 74.3 % | **−0.07** | −0.94 | −3.6 % | −37.1 % |
| 2023 | +0.051 | 1.25 | 65.5 % | **+2.64** | +1.57 | +44.2 % | +24.2 % |
| 2024 | +0.146 | 5.41 | 91.2 % | **+0.90** | +0.35 | +13.3 % | +4.0 % |
| 2025 | +0.104 | 2.82 | 77.4 % | **+1.39** | +0.94 | +33.2 % | +17.2 % |
| 2026 * | −0.090 | −0.58 | 70.4 % | +2.95 | −1.99 | +48.3 % | −19.0 % |

\* 125 dates / **5 periods** — noise, `se_sharpe` 1.04.

**Pooled, as one walk-forward track — 2,373 dates, 118 non-overlapping periods:**

| | IC | `ic_t` | days>0 | Sharpe@20 | @30 | @50 | `se_sharpe` | market |
|---|---|---|---|---|---|---|---|---|
| | **+0.1097** | **+6.90** | 80.6 % | **+2.026** | **+1.991** | **+1.921** | 0.155 | +0.737 |
| CAGR | | | | +48.6 % | +47.5 % | +45.4 % | | +14.6 % |

**The null** (`y_pred` shuffled within each date, 200 draws, top-20):

| bps | observed | null mean | p95 bar | null MAX | **z** | |
|---|---|---|---|---|---|---|
| 20 | +2.026 | +0.640 | +0.829 | +0.977 | **+12.18** | ✅ |
| 30 | +1.991 | +0.593 | +0.784 | +0.929 | **+12.28** | ✅ |
| 50 | +1.921 | +0.500 | +0.692 | +0.833 | **+12.46** | ✅ |

Null MAX below observed at all three, so §5 rule 3 does not fire.

⚠️ **IC is positive in 9 of 10 folds, and the strategy beats the equal-weight universe in
10 of 10.** That is the strongest thing in this file and the reason the single-split result
now looks like a sample of a pattern rather than a lucky window.

---

## 4. ⚠️ MY RECORDED PREDICTION WAS HALF WRONG, AND THE WRONG HALF IS THE INTERESTING ONE

Written into TODO **PRF-1** before the run, verbatim: *"it decays, and the post-2022 folds
straddle zero."*

- **Decay: RIGHT.** Sharpe@30 slope **−0.100/fold**; first five folds mean **+2.775**,
  last five **+1.564** — a 44 % fall.
- **Post-2022 straddling zero: WRONG.** 2022 is −0.07, but 2023/2024/2025 are **+2.64 /
  +0.90 / +1.39**, all clearly positive and all above their market.

⚠️ **This CONTRADICTS the h=5 and h=10 hand-built screens**, which were *negative* through
2022-2026 (`backtest/CONTEXT.md` §8g), and it contradicts §11's regime wall. The variable
that differs is once again the **horizon** — plus 13 selected channels against 3 hand-picked
ones. **2022 is the only bad fold, and it is bad for everyone**: the market itself ran
Sharpe −0.94 that year, and the strategy lost 3.6 % where the universe lost 37.1 %.

---

## 5. The check that says there is no mechanical leak

A per-fold Sharpe of 2-3 is high enough that a leak is the first hypothesis. **Restricting
the walk-forward track to the single split's own test window** and scoring it identically:

| 2023-11-15 → 2026-07-10, k=15, 50 bps | IC | `ic_t` | Sharpe | CAGR | n |
|---|---|---|---|---|---|
| walk-forward | **+0.0849** | +2.26 | +1.844 | +37.1 % | 32 |
| single split (`backtest` §4) | **+0.0863** | +3.47 | +1.484 | +30.5 % | 32 |

The ICs agree to the third decimal. The Sharpe is higher for the walk-forward, which is the
expected direction — its models are retrained up to each test year — and the gap is ~1.4 SE.
**The pooled Sharpe of ~2.0 is higher than the single split's 1.484 because 2017-2021 was a
better period, not because the folds leak.**

---

## 6. What it still does NOT establish

1. ⚠️ **The selection look-ahead is untouched** (§2) — but it has since been **MEASURED
   and found MILD** (TODO `PRF-7`, 2026-08-19). Re-running the identical selection on
   dates < 2017-01-01 alone — exactly what fold 0 could have seen — keeps **51 of 61**
   channels (Jaccard 0.761, **5.8 sd above chance**), shortlists **8 of 13** against a
   chance of 2.17, picks the **same top two**, and reaches `ic_mean` **+0.0973** against
   +0.1075 on 44 % of the data. All five shortlist misses have a family twin in the early
   kept set. So the channel set is **not period-fitted** and the levels below roughly
   stand. ⚠️ It bounds the bias rather than removing it, and the early run is noisier by
   construction (`n_eff_per_fold` 14.3 against 38.1).
2. **13 periods per fold**, `se_sharpe` 0.28-1.04. Individual folds are noisy; the pooled
   118-period figure is the one to quote.
3. **`NUL-1`** — the null prices in the universe, cost, schedule and `k`, never the feature
   selection, the architecture or the choice of `h=20`.
4. **Survivorship** (§2c) protects the z and not the CAGR.
5. **One architecture, one `k`, one horizon.** §5c measured eleven architectures inside one
   error bar, so this is not an architecture claim.
6. **No slippage, no ADV cap, no floor-day exclusion on the SELL side** — `PRF-4`.

---

## 7. ⚠️ Two things the run measured about the MODEL, not the market

**Nine of ten folds stopped at epoch 1** (fold 2024 at epoch 2), val loss 0.975-1.021 —
i.e. within ~2 % of the variance of a standardised label. The LSTM extracts what it can in
one pass and overfits from the second. `P2-3` recorded the same on VCB (*"best epoch 1 of
21, val loss rose from the first epoch on"*), and §5c measured eleven architectures spanning
0 to 276 k parameters inside one error bar. **Taken together: capacity is not the binding
constraint, and a smaller/cheaper model is the sensible next test, not a bigger one.**

**The fold-tag bug, kept as a lesson.** The first per-fold table labelled every fold by the
MINUTE it was trained (`20260819-023033`) because `RunDir.create` appends its own
`__<timestamp>` and `basename.split("__")[-1]` picked that up. It made the table unreadable
without changing a single number. The tag is passed explicitly now.

---

## 8. ⚠️ PRF-8 — THE ARCHITECTURE IS WORTH NOTHING. A 2,033-PARAMETER MODEL TIES IT.

Run 2026-08-19, **15m 03s** for ten folds × two arms. §7 above said capacity was not the
binding constraint; this measured it. `python -m walkforward --arm <package>:<config>`
trains every arm on **ONE build of each fold's tensors**, so "same data, different model"
is true by construction rather than by the builder being deterministic twice.

**Pooled over the same 118 non-overlapping periods, top-20 of 150, buyable only:**

| arm | capacity | IC | `ic_t` | days>0 | Sharpe@20 | @30 | @50 | CAGR@30 | `se_sharpe` |
|---|---|---|---|---|---|---|---|---|---|
| `lstm` (PRF-1) | **205,441 params** | +0.1097 | 6.90 | 80.6 % | 2.026 | **1.991** | 1.921 | +47.5 % | 0.155 |
| **`lstm_small`** | **2,033 params — 101×** | **+0.1239** | **9.49** | **86.3 %** | 2.031 | **1.997** | 1.929 | +51.5 % | 0.156 |
| `gbt` | **1,400 decision nodes** | +0.1249 | 8.90 | 82.8 % | 2.007 | **1.975** | 1.911 | +49.4 % | 0.155 |

**The null, 200 within-date shuffles, 30 bps** — all three clear, null MAX below observed
in all three:

| arm | observed | null mean | p95 bar | null MAX | **z** |
|---|---|---|---|---|---|
| `lstm` | +1.991 | +0.593 | +0.784 | +0.929 | **+12.28** |
| `lstm_small` | +1.997 | +0.581 | +0.773 | +0.997 | **+12.43** |
| `gbt` | +1.975 | +0.596 | +0.810 | +0.925 | **+11.88** |

### 8a. The comparison is PAIRED, and it had to be

Every arm trades the same rebalance dates out of the same panel, so their period returns
correlate at **ρ = 0.88** and `se_sharpe ≈ 0.155` is the error bar on the wrong quantity —
unpaired, it cannot resolve a 0.3 gap in either direction. `walkforward.compare` reports
`t` on the difference series `net_A − net_B`:

| arm vs `lstm` | bps | ΔSharpe | ρ | Δ return, annualised | **`t_paired`** |
|---|---|---|---|---|---|
| `lstm_small` | 20 / 30 / 50 | +0.005 / +0.006 / +0.008 | 0.881 | +3.07 / +3.05 / +3.02 pp | **+0.88 / +0.88 / +0.87** |
| `gbt` | 20 / 30 / 50 | −0.020 / −0.016 / −0.010 | 0.878 | +1.47 / +1.52 / +1.62 pp | **+0.42 / +0.44 / +0.47** |

⚠️ **`t_paired` IS ON THE RETURN DIFFERENCE, NOT ON THE SHARPE DIFFERENCE**, and for the
GBT the two disagree in SIGN — it earns slightly more per period at slightly more
volatility. Read them together: every |t| < 1, and every ΔSharpe is ~0.02 against an
`se_sharpe` of 0.155. **Under either statistic this is a tie.**

### 8a-bis. ✅ RE-SCORED 2026-08-21 THROUGH THE FIXED `compare` — AND THE TIES HOLD ON **BOTH** ESTIMANDS

```powershell
cd src
python -m walkforward.compare --top-k 20 --draws 0 --universe all `
    lstm=../results/walkforward `
    lstm_small=../results/walkforward/prf8/lstm_small `
    gbt=../results/walkforward/prf8/gbt          # 1m 29s, no GPU
```

`P1-9` shipped after this sweep was scored, so §8a's `t_paired` was a test of the MEAN
period-RETURN gap with `d_sharpe` printed beside it bare. At h=10 that distinction flipped
the verdict on **three of six** arms (§11c). **Here it flips nothing.** 118 periods,
ρ 0.878-0.881, 2,000 circular block draws, `block=2`, at 30 bps against `lstm`:

| arm | `t_ret` (MEAN) | **`d_sharpe` [95 % CI]** | `p_sharpe` | `ac1` | verdict |
|---|---|---|---|---|---|
| `lstm_small` | +0.88 | **+0.006 [−0.289, +0.381]** | **0.903** | −0.049 | ✅ **tie on both** |
| `gbt` | +0.44 | **−0.016 [−0.299, +0.291]** | **0.941** | −0.089 | ✅ **tie on both** |

✅ **§8a REPRODUCES TO EVERY DIGIT, WHICH IS WHAT LICENSES READING THE NEW COLUMN.** The
pooled Sharpe@30 came back **1.9913 / 1.9970 / 1.9750** against the published
+1.991 / +1.997 / +1.975; `se_sharpe` **0.1553** against 0.155; mean per-fold IC
**+0.0996 / +0.1178 / +0.1199** against §8b's +0.0996 / +0.1178 / +0.1199. And `t_ret`
returned **+0.88 / +0.88 / +0.87** and **+0.42 / +0.44 / +0.47** — *the same numbers §8a
published as `t_paired`*, confirming the column was renamed and not recomputed.

⚠️ **SO PRF-8's HEADLINE IS NOW ESTABLISHED ON THE ESTIMAND IT WAS ALWAYS READ AS.** *"A
101× capacity span ties at h=20"* was a mean-return result for two days; it is a
risk-adjusted result as well. **No caveat is left on §8.**

⚠️ **AND THE FIX IS NOT BIASED TOWARD FINDING DISAGREEMENT.** That was the live worry after
§11c: a second estimand with its own interval could just be a second chance to find
something. It finds disagreement at h=10 and agreement at h=20 **on the same code**, which
is the behaviour a real instrument has.

### 8a-ter. ⚠️ `gbt` CHANGES SIGN BETWEEN THE HORIZONS, AND THAT IS NEW

Put the two re-scored tables beside each other — same tool, same `k`, same universe, same
reference arm — and the best-measured arm at one horizon is a (statistically
indistinguishable) LOSER at the other:

| | h=10 (§11c) | **h=20 (above)** |
|---|---|---|
| `gbt` `d_sharpe` vs `lstm` | **+0.360** [+0.013, +0.721] | **−0.016** [−0.299, +0.291] |
| `p_sharpe` | 0.044 *(nominal; Bonferroni 0.0083)* | **0.941** |
| capacity span in the sweep | 224× | 101× |

⚠️ **THIS IS THE STRONGEST AVAILABLE ARGUMENT AGAINST PROMOTING `gbt` ON ITS h=10 ROW.**
§11d already said *"best measured, not established"* because the nominal p does not survive
six arms. This adds an independent reason: **the advantage does not reproduce at the
neighbouring horizon**, where the same arm sits fractionally below the reference. Two
estimates that disagree in sign across a horizon are what a null effect looks like.
⚠️ It is **not** a paired test between the horizons — only `walkforward.pair` can do that
(§10) and it has not been run on the arms. Read it as two independent estimates, which is
exactly the weakness §9c records for the horizon comparison itself.

### 8b. What it means, and it is not "we saved some GPU"

⚠️ **THE RESULT LIVES IN THE 13 CHANNELS, NOT IN THE ARCHITECTURE.** A model 101× smaller
reproduces the pooled +1.991, and so does a shallow tree ensemble. `PRF-7`'s selection
look-ahead is therefore not *part* of the story about where this Sharpe comes from — it is
close to the whole of it, since the only other candidate has just been ruled out.

⚠️ **AND THE SEQUENCE INSIDE THE LOOKBACK IS WORTH NOTHING EITHER.** This was the way the
prediction expected to be wrong and it was not: `model.gbt` compresses each (20, 13) window
to **six statistics per channel — 78 numbers where the LSTM sees 260** — and it ties. What
the recurrent layers extract from the 20-session path, over and above last/mean/slope/sd/
min/max, does not show up in either the IC or the Sharpe.

⚠️ **THE BIG MODEL IS THE ONLY ONE THAT GOES NEGATIVE IN THE LAST FOLD.** Per-fold IC
2026: `lstm` **−0.0902** against `lstm_small` +0.0016 and `gbt` +0.0254. Mean per-fold IC
is +0.0996 / **+0.1178** / **+0.1199**. ⚠️ **Do not read this as "smaller is better"** —
2026 is a 5-period, 125-date stub (`se_sharpe` 1.04), and the portfolio difference is
inside the paired error bar. The defensible statement is that the small model is **not
worse**, which is the whole claim PRF-8 was built to test.

⚠️ **This is the fourth independent measurement pointing one way**, and the first that
moves capacity deliberately: §7's nine-of-ten epoch-1 stops, `P2-3`'s *"best epoch 1 of
21"* on VCB, CLAUDE.md §5c's eleven architectures inside one error bar — and now a 101×
cut that changes nothing. **The next model question worth asking is not which architecture.
It is which FEATURES**, which is `PRF-9`.

### 8c. ⚠️ The concurrency trap this run found — two sweeps silently corrupt each other

The first attempt was launched twice by mistake and the two runs raced. Every fold writes
`train_test_set/<ticker>__<table>__…__<tag>` — **a name derived from the DATA, with no term
for which process built it** — and `build_fold` saves with `replace=True` while `main`
deletes the folder once its arms are done. So the second sweep rebuilt the same directory
*while the first was training out of it*, then deleted it underneath.

⚠️ **The visible symptom was the harmless half.** The second sweep died with
`FileNotFoundError: dataset … not found`, which is loud and unmissable. The dangerous half
is silent: the surviving sweep read tensors another process was mid-`np.save` on, and would
have reported a Sharpe with nothing to indicate it. **Every artefact of both runs was
deleted and the sweep re-run from scratch** rather than kept — a number whose provenance
cannot be reconstructed is worth nothing here.

`run.namespace_lock` now claims the fold-dataset namespace for the length of a sweep and
refuses a second one by pid, taking over a lock whose holder is dead so a killed sweep does
not block every later one. ⚠️ **Re-entry within ONE process is refused too** — two sweeps in
one process share the namespace exactly as two processes do. Three tests in
`test_arms.py` (20 in the package).

### 8d. What PRF-8 does NOT establish

1. **A tie says the two models extract the same thing from these 13 channels.** It does not
   say either extracts everything there is — a third architecture could still differ.
2. **`NUL-1` in full force.** No null here prices in the feature selection that chose the
   channels all three arms read, nor the choice of `h=20`, `k=20` or the universe.
3. **One `k`, one horizon, one universe**, and §6's caveats all still apply unchanged —
   survivorship protects the `z` and not the CAGR, and the levels carry `PRF-7`'s bounded
   but non-zero selection look-ahead.
4. ⚠️ **It is not a claim that the small model should REPLACE the big one in production.**
   It is a claim about where the signal lives. Nothing here was re-tuned for the small
   model, and a tie under one schedule is not an optimum.


---

## 9. ⚠️ THE SAME SWEEP AT h=10 — 2026-08-20, higher on every LEVEL

⚠️ **Read §10 before quoting any comparison in this section.** The levels below are all
above h=20's, and `P2-4` then paired the two tracks on the calendar: the MEAN gap is
significant (+17.0 pp/yr, p < 0.001) and the **SHARPE gap is NOT** (95 % CI
[−0.079, +1.041]). This heading originally read *"and it BEATS h=20 on every cost
level"*, which is true of the levels and false of the test that matters.

```powershell
cd src
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --config lstm__all__rank_10day__final__d20_h10.yaml --first-test 2017-01-01 `
    --out ../results/walkforward_h10                              # 33m 26s
python -m walkforward.evaluate --top-k 20 --draws 200 --horizon 10 --universe all `
    --out ../results/walkforward_h10                              # 8m 59s
```

⚠️ **`--out` IS NOT OPTIONAL HERE AND IT IS DESTRUCTIVE TO OMIT.** `DEFAULT_OUT` is
`results/walkforward/`, which holds §3's h=20 track — `predictions_oos.csv`, `folds.csv`
and `per_fold.csv` are all written by basename, so the documented §1 command run at h=10
**silently overwrites PRF-1**. The two horizons are two experiments and they need two
directories.

**Why it was run**: `PRF-2` measured h=10 at Sharpe@30 **+2.442** against h=20's +1.441 —
**on one split each**, `se_sharpe` ~0.25. h=20 had ten folds behind it and h=10 had none,
so the horizon was explicitly NOT promoted. This is the run that settles it. Identical
geometry to §3: same universe (top 150 by pre-2014 turnover), same `--first-test`, same
expanding 12-month folds `oos2017…oos2026`, same top-20, same ceiling screen.

Buyable names only (`PRF-0`): **9,259 of 349,581 = 2.65 %** — the same share as h=20's
2.65 %, which is what it should be, since the screen is a property of the panel and not of
the label.

| fold | IC | `ic_t` | days IC>0 | **Sharpe@30** | market | CAGR@30 | market |
|---|---|---|---|---|---|---|---|
| 2017 | +0.175 | 9.66 | 98.0 % | **+3.84** | +2.38 | +87.9 % | +32.7 % |
| 2018 | +0.157 | 7.56 | 94.0 % | **+3.25** | −0.61 | +59.5 % | −9.3 % |
| 2019 | +0.189 | 10.45 | 97.2 % | **+3.93** | +0.92 | +77.0 % | +7.1 % |
| 2020 | +0.152 | 5.61 | 84.9 % | **+3.52** | +2.02 | +150.9 % | +57.2 % |
| 2021 | +0.091 | 2.76 | 69.6 % | **+5.00** | +2.95 | +240.6 % | +117.4 % |
| **2022** | +0.151 | 4.43 | 83.1 % | **+0.37** | −1.26 | +7.3 % | −40.1 % |
| 2023 | +0.102 | 4.04 | 80.3 % | **+2.77** | +1.33 | +58.3 % | +24.9 % |
| 2024 | +0.171 | 9.41 | 96.0 % | **+3.68** | +0.48 | +49.7 % | +5.7 % |
| 2025 | +0.107 | 4.19 | 79.4 % | **+2.39** | +1.04 | +41.2 % | +17.6 % |
| 2026 * | +0.094 | 1.67 | 77.8 % | +1.38 | −1.42 | +22.9 % | −18.4 % |

\* 135 dates / **11 periods** — partial year, `se_sharpe` 0.400.

**Pooled, as one walk-forward track — 2,383 dates, 236 non-overlapping periods:**

| | IC | `ic_t` | days>0 | Sharpe@20 | @30 | @50 | `se_sharpe` | market |
|---|---|---|---|---|---|---|---|---|
| | **+0.1412** | **+16.05** | 86.5 % | **+2.601** | **+2.531** | **+2.391** | 0.128 | +0.715 |
| CAGR | | | | +76.8 % | +74.0 % | +68.5 % | | +13.9 % |

**The null** (`y_pred` shuffled within each date, 200 draws, top-20):

| bps | observed | null mean | p95 bar | null MAX | **z** | |
|---|---|---|---|---|---|---|
| 20 | +2.601 | +0.505 | +0.676 | +0.812 | **+18.42** | ✅ |
| 30 | +2.531 | +0.409 | +0.578 | +0.719 | **+18.58** | ✅ |
| 50 | +2.390 | +0.218 | +0.389 | +0.534 | **+18.86** | ✅ |

Null MAX below observed at all three, so §5 rule 3 does not fire.

⚠️ **IC is positive in 10 of 10 folds and the strategy beats the equal-weight universe in
10 of 10, on both Sharpe and CAGR.** h=20 managed 9 of 10 on IC. Every h=10 fold also
clears its own market on CAGR, including 2022 (+7.3 % against −40.1 %).

### 9a. ⚠️ THE DECAY IS THE SAME, AND I MISREAD IT ONCE BEFORE WRITING IT DOWN

```
Sharpe@30bps across folds: slope -0.2192/fold   first half +3.907   second half +2.119
```

The slope is **2.2× h=20's −0.100/fold**, and reading only the slope says h=10 decays twice
as fast. **It does not.** In proportional terms the two are the same decay:

| | first half | second half | fall | slope |
|---|---|---|---|---|
| h=20 (§3) | +2.775 | +1.564 | **−43.6 %** | −0.100/fold |
| **h=10** | +3.907 | +2.119 | **−45.8 %** | −0.219/fold |

⚠️ **The absolute slope is steeper only because the level is higher.** A ratio and a
difference disagree here, and the ratio is the one that compares two tracks with different
means. What the pair actually says is that **both horizons lose ~45 % of their Sharpe
between the first five folds and the last five** — a shared decay, not a property of h=10.

### 9b. The no-mechanical-leak check, run the same way §5 was

Restricting the h=10 walk-forward track to the single split's OWN test window — read from
the dataset's `metadata.json`, `2023-11-28 → 2026-07-24` — and scoring it identically:

| k=20, 30 bps | IC | `ic_t` | Sharpe@30 | CAGR@30 | n |
|---|---|---|---|---|---|
| walk-forward, restricted | **+0.1307** | +7.61 | **+2.257** | +43.2 % | **63** |
| single split (`PRF-2`) | **+0.1393** | +8.19 | **+2.442** | +43.8 % | **63** |

**63 periods on both sides** — the windows are the same window, which is the check working.
The ICs agree to ~0.009 and the Sharpe gap is **0.185 against `se_sharpe` 0.224, i.e. 0.8
SE**.

⚠️ **The sign of that gap is OPPOSITE to §5's and it must not be read as a finding.** At
h=20 the walk-forward scored *above* the single split and §5 called that the expected
direction (its models are retrained up to each test year); here it scores *below*. At
0.8 SE neither direction is resolvable, and the honest statement is that the two agree
within noise at both horizons.

### 9c. ⚠️ h=10 vs h=20 IS STILL AN UNPAIRED COMPARISON

| | h=10 | h=20 |
|---|---|---|
| Sharpe@30 | **+2.531** | +1.991 |
| periods | **236** | 118 |
| `se_sharpe` | 0.128 | 0.155 |

✅ **PAIRED 2026-08-20 — see §10, and the answer is split.** `walkforward.pair` pairs the
two on the CALENDAR (both hold a book every session), and finds the MEAN gap significant
and the SHARPE gap not. The paragraph below stands as the reason `compare` could not do
it, which is why a second tool exists.

⚠️ **`walkforward.compare` CANNOT PAIR THESE, and the reason is structural rather than a
missing flag.** It pairs ARMS within one sweep — arms that trade the same dates out of the
same panel, which is what makes `ρ = 0.88` and a paired `t` meaningful (§8a). Two horizons
produce **236 and 118 periods over different holding intervals**, so there is no period-wise
correspondence to difference. The +0.54 gap is therefore a comparison of two independent
estimates, each with `se` ~0.13-0.16 — **suggestive, and not the paired test §8a insisted on
for a difference this size**. §5c is the standing warning: eleven architectures once spread
IC across 0.227 and the whole spread was one error bar.

⚠️ **AND THE COST IDENTITY CUTS THE OTHER WAY.** `backtest/CONTEXT.md` §3: at turnover 0.70
and 50 bps the annual fee drag is **8.8 % at h=10 against 4.4 % at h=20**. Every figure above
is already net of that, so h=10 wins *after* paying double — but it also means h=10's edge is
the more fragile of the two to any cost the backtest still does not charge (`PRF-4`: ADV cap,
floor days on the sell side, slippage).

### 9d. What §9 does NOT establish

1. ✅ **The selection look-ahead is now BOUNDED at h=10 too — see §9e.** It is still a bound
   and not a removal: the 19 channels were chosen over 2009-2026 including every test fold,
   so every LEVEL here carries an optimism this probe constrains rather than subtracts.
2. **`NUL-1` in full force** — the within-date shuffle prices the universe, `k`, the cost and
   the schedule. It prices neither the feature selection, nor the architecture, nor the
   choice of `h=10` itself, which was made *using* `PRF-2`'s result.
3. **Survivorship protects the `z` and not the CAGR** (§6, CLAUDE.md §2c). `z = +18.6` stands;
   **+74.0 %/yr does not**, and it is the more extreme claim of the two by a wide margin.
4. **One `k`, one universe, one architecture.** `PRF-8` ruled the architecture out at h=20;
   nothing has re-run those arms here.


### 9e. ✅ PRF-7 AT h=10 — the channel set is NOT period-fitted here either

`cross-sectional-h10-early`, a Kaggle T4, **9m 46s**, merged into
`reports/feature_selection_probes/`. Identical to the run behind §9 in every respect except
the DATA WINDOW: dates < 2017-01-01, exactly what walk-forward fold 0 could have seen. Panel
**273,367 × 104 over 1,995 dates** — the same panel `PRF-7` used at h=20, so the two probes
differ only in the label. `RUN_NULL=false`: the kept SET is the measurement, not its bar.

| | full sample | pre-2017 | |
|---|---|---|---|
| candidates | 90 | 90 | identical pools, so the null needs no restriction |
| kept | 61 | 58 | **overlap 51**, Jaccard **0.750**, chance 39.3 ± 2.1 → **+5.48 sd** |
| shortlisted | **19** | **9** | **overlap 7**, chance 1.90 ± 1.17 → **+4.37 sd** |
| #1 channel | `drv_order_vol_imb` | `drv_order_vol_imb` | ✅ the same, and it is h=20's #1 too |
| `ic_mean` | +0.1201 | **+0.1260** | ⚠️ HIGHER on 44 % of the data — see below |
| `n_eff_per_fold` | 76.6 | **28.9** | `se_ic_per_fold` 0.115 → **0.189** |

⚠️ **AND 10 OF THE 12 SHORTLIST "MISSES" ARE IN THE EARLY KEPT SET**, so 7-of-19 understates
the agreement exactly as it did at h=20. Only **two** are absent from the early run outright:
`drv_close_z_21` and **`n_sell_orders`** — and `n_sell_orders` was one of h=20's misses too,
which makes it the one channel that is period-dependent at BOTH horizons.

⚠️ **THE ASYMMETRY IS THE OTHER WAY ROUND FROM h=20, AND IT IS SAMPLE SIZE.** At h=20 the
early run shortlisted MORE than the full one (15 vs 13); here it shortlists **half as many**
(9 vs 19). `n_eff_per_fold` is **28.9 against 76.6** and `se_ic_per_fold` **0.189 against
0.115**, so fewer channels clear the FDR cut on a quarter of the effective sample. **Read the
shortlist counts as a statement about power, not about the market.**

⚠️ **`ic_mean` going UP (+0.126 vs +0.120) is NOT evidence the early window is better.** At
`se_ic_per_fold` 0.189 the difference is ~0.03 SE. It also runs opposite to h=20's probe,
which scored −9.5 %. Both are inside noise; neither direction means anything.

**Reading — the same as `PRF-7`'s, at a second horizon:** a walk-forward that re-ran the
selection per fold would have picked substantially the same channels, so §9's levels stand as
levels-with-a-bounded-bias rather than artefacts. ⚠️ **A stable channel SET still does not
make the measured IC unbiased** — it bounds the problem and does not remove it, and the
~60 GPU-h per-fold version remains unrun at both horizons.

---

## 10. ⚠️ P2-4 — h=10 vs h=20, PAIRED AT LAST, and the two estimands disagree

```powershell
cd src
python -m walkforward.pair --top-k 20 --universe all --draws 2000 `
    h10=../results/walkforward_h10:10 h20=../results/walkforward:20   # 48 s
```

§9c said no tool in this repo could test the +0.54 Sharpe gap, because `compare` pairs
period by period and two horizons have no period-wise correspondence. **`walkforward.pair`
pairs on the CALENDAR instead**: both strategies hold a book on every one of the same 2,360
sessions, so both have a DAILY net-return series and those pair date by date. Correlation
**0.723** — high enough that pairing is worth the trouble, exactly as it was for the arms.

| bps | corr | **Δ mean/yr** | NW t | NW p | boot 95 % CI | boot p | **Δ Sharpe** | boot 95 % CI | boot p |
|---|---|---|---|---|---|---|---|---|---|
| 20 | 0.723 | **+0.1786** | +3.71 | 0.0002 | [+0.095, +0.266] | 0.0000 | +0.4782 | [−0.048, +1.080] | 0.0670 |
| 30 | 0.723 | **+0.1697** | +3.53 | 0.0004 | [+0.086, +0.257] | 0.0000 | +0.4437 | [−0.079, +1.041] | 0.0870 |
| 50 | 0.723 | **+0.1518** | +3.16 | 0.0016 | [+0.068, +0.240] | 0.0030 | +0.3730 | [−0.140, +0.959] | 0.1410 |

### 10a. The answer, in two sentences that must be quoted together

✅ **h=10 earns significantly more per unit of TIME.** +17.0 pp/yr at 30 bps, CI
[+8.6, +25.7], and **Newey-West and the block bootstrap agree** to three decimal places on
p — two methods with different assumptions, one estimand, one answer.

❌ **Its RISK-ADJUSTED advantage is NOT established.** ΔSharpe +0.44 at 30 bps with a 95 %
CI of **[−0.079, +1.041]** — zero is inside it at every cost level, and the p rises from
0.067 to 0.141 as costs do. **h=10 is a higher-return, higher-VOLATILITY track**, and the
gap that looked like a clean +0.54 is not resolvable at 2,360 sessions.

⚠️ **SO §9's HEADING WAS TOO STRONG AND IS CORRECTED HERE**: *"it BEATS h=20 on every cost
level"* is true of the levels and of the mean, and **not** of the risk-adjusted difference,
which is the quantity a horizon decision should turn on. The horizon stays at h=20 by
default — not because h=10 lost, but because it has not won the test that matters.

### 10b. ⚠️ The two tests looked like they disagreed, and the first version of this module was the reason

The first run reported Newey-West p = 0.0002 beside a bootstrap p = 0.067 and it read as a
method disagreement. **It was not.** The NW test was on the MEAN difference and the
bootstrap on the SHARPE difference — a linear functional against a ratio whose denominator
also moves. Bootstrapping BOTH estimands made them agree wherever they are comparable.
**Two tests are only a cross-check when they test the same thing**, which is the same error
`walkforward.compare` §8a exists to prevent one level up.

### 10c. What the reconciliation caught on the way — two real defects

The module refuses to report until its daily construction reproduces `long_only_top_k`'s
own period returns. It did not, twice, and both causes were real:

| | |
|---|---|
| **the return matrix was pivoted from the TRACK** | a track holds one row per SCORED `(date, ticker)`, and every one of the 150 names is missing some (median 2,339 of 2,373 sessions, min 258) — **2.21 % of cells**. That booked a 0 % return on any day the model did not score a name **we were still holding**. Fixed: returns come from `pool__basic` over the full calendar, scores from the track |
| **`BKT-1`** | `return_{h}day` steps `h` ROWS of the ticker; the book is held `h` SESSIONS. Verified: the stored column IS the `h`-row shift, to **8.9e-16 over 2.37 M rows**. Immaterial at portfolio level (−0.015 Sharpe at h=20, −0.038 at h=10) and it makes every published figure slightly CONSERVATIVE |

✅ **After both, the check passes at machine precision**: on names trading every session the
daily reconstruction reproduces `return_{h}day` to **1.22e-15**, and the period-based Sharpe
rebuilt from this module's own picks is **+1.9913** at h=20 and **+2.5310** at h=10 —
the published +1.991 and +2.531. That equality is what licenses reading anything above.

### 10d. What §10 does NOT establish

1. **It prices the difference between two REALISED tracks.** `NUL-1` unchanged: nothing here
   prices either track's feature selection, nor the choice to compare these two horizons.
2. ⚠️ **A non-significant ΔSharpe is not evidence of equality.** The CI reaches +1.04 — a
   real and large h=10 advantage is entirely consistent with this data. **Underpowered, not
   settled**, and the honest next move is more OOS sessions rather than a third test.
3. **Costs are the one thing that moves the answer directionally**: ΔSharpe falls 0.478 →
   0.373 from 20 to 50 bps and its p nearly doubles, which is `backtest/CONTEXT.md` §3's
   identity showing up — h=10 pays double the fee drag. Any cost `PRF-4` adds hurts h=10
   more.
4. **One `k`, one universe, one architecture, two horizons.** h=5 was never run.

---

## 11. ⚠️ SEVEN ARCHITECTURES AT h=10 — and PRF-8's "they all tie" does NOT reproduce

```powershell
cd src
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --first-test 2017-01-01 --out ../results/walkforward_h10_arch `
    --arm lstm:lstm__all__rank_10day__final__d20_h10.yaml `
    --arm bilstm:bilstm__... --arm cnn:cnn__... --arm cnnlstm:cnnlstm__... `
    --arm tcn:tcn__... --arm transformer:transformer__... --arm gbt:gbt__...
#   measured 2026-08-21: 70 arm-folds, 2h 48m 47s, 0 errors
python -m walkforward.compare --top-k 20 --horizon 10 --universe all --draws 200 `
    lstm=../results/walkforward_h10_arch/lstm  ...          # 22m 25s
```

**Capacity spans 224×** — 1,398 decision nodes to 313,153 parameters — against `PRF-8`'s
101×, and this is the first architecture test at h=10. Four of the seven arms
(`bilstm`, `cnnlstm`, `tcn`, `transformer`) were written for this run.

| arm | capacity | Sharpe@30 | IC | **paired `t` vs `lstm`** | null z |
|---|---|---|---|---|---|
| **`gbt`** | **1,398 nodes** | **+2.891** | **+0.1460** | −1.02 | **+22.57** |
| `transformer` | 68,417 | +2.622 | +0.1433 | −0.33 | +20.08 |
| `tcn` | 18,113 | +2.622 | +0.1426 | −0.20 | +20.25 |
| `lstm` *(reference)* | 208,769 | +2.531 | +0.1412 | — | +18.58 |
| `bilstm` | 313,153 | +2.474 | +0.1419 | **−2.09** ❌ | +17.55 |
| `cnnlstm` | 30,369 | +2.367 | +0.1308 | **−2.15** ❌ | +16.80 |
| `cnn` | 5,185 | +2.133 | +0.1171 | **−3.37** ❌ | +15.37 |

All seven clear the within-date shuffle null with the null MAX below the observed, so
§5 rule 3 fires for none of them.

### 11a. ⚠️ THE FINDING: architecture matters, but only DOWNWARD

**No arm beats the LSTM significantly. Three lose to it significantly.** That is not
`PRF-8`'s result — §8 found a 101× capacity span tying at h=20 with every |t| < 1 — and
it is not a contradiction of it either. The honest statement is narrower than both:

> **Choosing the wrong architecture costs money; choosing a better one buys nothing.**

⚠️ **AND IT IS NOT A CAPACITY STORY.** The best arm is the SMALLEST (1,398 decision nodes)
and the worst-but-one is also small (`cnn`, 5,185 parameters), while the largest
(`bilstm`, 313 k) sits below the 209 k `lstm`. Ranking the seven by parameter count gives
a Spearman of essentially nothing against their Sharpe. What separates them is the
**inductive bias**: `cnn` pools the sequence away and loses 0.40 Sharpe; the two arms that
keep a per-timestep view of the whole window (`tcn`, `transformer`) tie the LSTM; the tree
that sees only 78 window statistics beats it. §8b's reading — *"the sequence inside the
lookback is worth nothing"* — reproduces at h=10 and is now the better explanation of the
whole table.

### 11b. ⚠️ TWO RECORDED PREDICTIONS, AND BOTH WERE WRONG

Written into TODO.md at 00:20 with 24 of 70 arm-folds done and no result visible:

| prediction | outcome |
|---|---|
| *"They all tie, paired \|t\| < 1.5, at every cost level"* | ❌ **WRONG.** `bilstm` −2.09, `cnnlstm` −2.15, `cnn` −3.37 |
| *"`best_epoch` will be 0-2 for every arm in nearly every fold"* | ❌ **PARTLY WRONG.** 43 of 70. `cnn` averages **7.7** (max 20) and `tcn` **5.7** (max 13) |
| *"If anything wins, I expect `gbt` or `tcn`"* | ✅ right in direction — `gbt` leads on Sharpe AND IC |
| *"`transformer` winning would surprise me"* | it placed 2nd-equal and did not win |

⚠️ **The second one matters beyond bookkeeping.** *"Best epoch is 1"* has been quoted four
times in this repo as evidence that capacity is worthless. It is an **LSTM and GBT**
property: the convolutional arms genuinely train for 6-20 epochs. The sentence must be
attached to an architecture from now on, not to the problem.

### 11c. ✅ `P1-9` SHIPPED 2026-08-21 — AND THE SHARPE TEST DISAGREES ABOUT THREE OF SIX ARMS

`compare.paired()` computed its `t` on the mean period-RETURN difference while the table
printed `d_sharpe` beside it bare, so §11a above and `PRF-8` were both read off the wrong
column. It now returns **both estimands**, each with its own interval, by reusing
`pair.block_bootstrap_diff` rather than writing a second one — the same blocks are drawn
from both arms, so the pairing survives the resampling.

**Re-scored on the identical tracks, 3m 23s, 236 periods, ρ 0.91-0.94, 2,000 draws,
`block=2`. At 30 bps, against `lstm`:**

| arm | `t_ret` (MEAN) | verdict on RETURN | **`d_sharpe` [95 % CI]** | `p_sharpe` | verdict on SHARPE |
|---|---|---|---|---|---|
| `gbt` | −1.02 | tie | **+0.360 [+0.013, +0.721]** | **0.044** | ⚠️ **GAINS** (nominal) |
| `transformer` | −0.33 | tie | +0.091 [−0.171, +0.385] | 0.537 | tie |
| `tcn` | −0.20 | tie | +0.091 [−0.119, +0.339] | 0.406 | tie |
| `bilstm` | **−2.09** | ❌ loses | −0.058 [−0.279, +0.161] | **0.612** | ✅ **tie** |
| `cnnlstm` | **−2.15** | ❌ loses | −0.164 [−0.472, +0.157] | **0.295** | ✅ **tie** |
| `cnn` | **−3.37** | ❌ loses | **−0.398 [−0.678, −0.135]** | **0.001** | ❌ **loses** |

⚠️ **"THREE LOSE SIGNIFICANTLY" IS A MEAN-RETURN CLAIM. ON SHARPE, ONE DOES.** `bilstm`
and `cnnlstm` earn less per period at *lower volatility*; the risk-adjusted gap is
indistinguishable from zero. That is not a contradiction and it reads like one, which is
precisely why the two columns had to be separated.

⚠️ **AND `gbt`'s GAIN DOES NOT SURVIVE THE SIX ARMS THAT WERE TRIED.** Bonferroni over
one reference and six challengers is **0.05/6 = 0.0083**: `cnn` (0.001) clears it, `gbt`
(0.044) does not. `NUL-1` one level up — the same shape CLAUDE.md §6-1 point 3 records for
the five-ticker search. **Quote it as "the best arm measured, advantage not established".**

⚠️ **`gbt`'s `p_sharpe` RISES WITH COST** — 0.040 / 0.044 / 0.051 at 20/30/50 bps, and the
CI's lower bound crosses zero between 30 and 50. `backtest` §3's identity showing through:
`gbt` trades more, so the advantage is partly a gross-return one that fees eat.

⚠️ **`ac1` IS WHY THE BOOTSTRAP IS TRUSTED HERE, and it is printed per row.** The lag-1
autocorrelation of each arm's difference series is **−0.09 … +0.06**, so the periods really
are near-independent and `block=2` is doing no hidden work. The block default is a fact
about the DESIGN — `rebalance_dates` takes every `h`-th date, so periods do not overlap and
`pair`'s `2 × horizon` (which exists for DAILY returns) has no analogue beyond 2.

✅ **THE h=20 `PRF-8` SWEEP WAS RE-SCORED 2026-08-21 — §8a-bis.** It took **1m 29s** and
**its ties hold on BOTH estimands** (`lstm_small` `p_sharpe` 0.903, `gbt` 0.941), so §8's
headline carries no caveat any more. ⚠️ **And `gbt`'s advantage does NOT reproduce there**:
`d_sharpe` **+0.360** at h=10 against **−0.016** at h=20 — §8a-ter.

⚠️ **A DEFECT IN THE PORTED CODE WAS FOUND BY THE TEST THAT COMPARES AN ARM WITH
ITSELF** — `pair.summarise`'s two-sided p was `2 × min(P(x≤0), P(x≥0))`, which returns
**2.0** when every draw is exactly 0. Clipped at 1.0; it never fired on §10's published
numbers because two different strategies never tie exactly. `BOO-1` in ISSUES.md.

### 11d. What §11 does NOT establish

1. **Nothing was tuned per arm.** Every arm inherited the LSTM's optimiser schedule, batch
   size, patience and seed — which is what makes them comparable and also means a LOSS may
   be a schedule mismatch rather than an architecture verdict. `cnn` wanting 20 epochs
   under a patience of 15 is the visible case.
2. **`NUL-1` in full force** — no null here prices the feature selection that chose the 19
   channels all seven arms read.
3. **One `k`, one universe, one horizon.** `PRF-8`'s h=20 arms were not re-run here, so
   "h=20 ties and h=10 does not" is a comparison across two sweeps, not a paired test.
4. ⚠️ **`gbt` leading is STILL not a recommendation, and §11c changed the reason.** The
   Sharpe test now calls its gap a nominal WIN (p = 0.044) rather than a tie — but that p
   does not survive the six arms that were tried (Bonferroni 0.0083), and the arm is
   untuned. *"Best measured, not established"* is the whole claim.

---

## 12. ⚠️ THE DATASET AND SPLIT SETTINGS ARE WORTH NOTHING — 2026-08-21

```powershell
cd src
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --first-test 2017-01-01 --model gbt --config gbt__all__rank_10day__final__d20_h10.yaml `
    --out ../results/settings_h10/<tag> [--val-months N] [--step-months N] `
    [--no-scale-target] [--rank-min-width N]
python -m walkforward.compare --top-k 20 --horizon 10 --universe all --draws 0 `
    baseline=../results/settings_h10/baseline val6=… val24=… step6=… noscale=…
```

Six full walk-forward tracks, one per setting, scored paired against `baseline`. The arm
is `gbt` throughout — the fastest and best-scoring architecture measured in §11 — because
the question is the SETTING and the model has to be a constant.

✅ **`baseline` reproduces §11's `gbt` row to every digit** (Sharpe@30 **2.8910**, IC
**0.1460**, 236 periods). The walk-forward is deterministic given its settings, which is
what licenses reading any difference below as the setting rather than run-to-run noise.

| setting | what moved | Sharpe@30 | **Δ vs baseline** | **paired `t`** | ρ |
|---|---|---|---|---|---|
| `baseline` | — | +2.8910 | — | — | — |
| `val6` | validation 12 → **6** months | **+2.9655** | +0.0746 | **+0.33** | 0.972 |
| `val24` | validation 12 → **24** months | +2.7562 | −0.1348 | **−1.32** | 0.946 |
| `step6` | refold every **6** months — **20 folds** | +2.8977 | +0.0067 | **−0.09** | 0.989 |
| `noscale` | `scale_target` off | +2.8910 | **0.0000** | **NaN** | **1.0000** |

**Every setting ties. No `|t|` reaches 1.4.** The only directional hint is `val24` at
−1.32 — a longer validation window is weakly worse, which is what stealing training data
should look like — and it does not clear.

⚠️ **`step6` IS THE STRONGEST OF THE FIVE RESULTS AND THE EASIEST TO MISS.** It retrains
**twice as often** — 20 folds against 10 — for `t = −0.09` and ρ = 0.989. Doubling the
retraining frequency changes essentially nothing, which says the model is not chasing a
moving target between folds. Read it beside §9a's decay: the Sharpe falls ~45 % across the
sweep at BOTH horizons, and refitting twice as often does not arrest it. **Whatever decays
is not staleness.**

### 12a. ⚠️ `noscale` is BIT-IDENTICAL, and that is a fact about TREES, not about the problem

ρ = **1.0000**, `d_sharpe` exactly **0.0000**, `t` = **NaN** because the difference series
has zero variance. The flag reached the builder — the run's own banner reads
`scale_target=False` — so this is not a plumbing failure.

**Why**: `engine._write_predictions` inverse-transforms a regression prediction back to the
target's scale, and a decision tree's splits depend only on the ORDERING of `y` within a
node. Standardising the label is an affine map, orderings are affine-invariant, and the
inverse-transform undoes the rest. End to end it is the identity.

⚠️ **IT WOULD NOT BE THE IDENTITY FOR A NEURAL NET**, where the loss scale interacts with
the learning rate and the initialisation. **I chose `gbt` for speed, and that choice made
one of the six settings unanswerable for the family that actually uses it.** Re-run
`noscale` with an `lstm` arm before quoting this for anything but trees.

### 12b. ⚠️ `minw10` was REFUSED by `compare`, and the refusal is the measurement

`walkforward.compare` raised in 1.6 s:

> *arm 'minw10' covers **349,371** (date, ticker) rows against the reference arm's
> **349,581** … These are two different experiments and their Sharpes are not comparable.*

`rank_min_width` sets how many names must trade on a date for that date to contribute a
rank, so raising it 5 → 10 **drops dates from the panel** — it moves the LABEL, not the
split. It is also supposed to match the `min_ic_width` the SELECTION ran with (5), so a
`minw10` track trains on channels chosen against a different label. The flag's own
docstring says all of this; the guard proved it rather than trusting it.

### 12c. What §12 does NOT establish

1. **One arm.** `gbt` is scale-invariant and shallow; a setting worth nothing to it may be
   worth something to a 200 k-parameter recurrent net. §12a is the concrete case.
2. **One horizon, one universe, one `k`.** And `NUL-1`: no null here prices the feature
   selection every track inherits.
3. ⚠️ **A tie is not "the default is optimal"** — it is "these five knobs do not move the
   result at this width". The knobs that DO move it, on the evidence in this file, are the
   feature set (§11a) and the horizon (§9c), neither of which is a dataset setting.
4. **`lookback` was never swept**, and it is the one dataset knob that would matter: `d`
   comes from the source TABLE NAME and `engine._verify` asserts it, so changing it needs
   a fresh selection run per value — not a flag.

---

## 13. ⚠️ THE WIDENED CHAIN AT h=10 — `pool__ta` changes the SHORTLIST and not the MONEY, again

`PRF-9` measured this at h=20 with 120 candidates and an LSTM. This is h=10, **162
candidates**, a GBT, and a selection that ran only after four Kaggle failures and three
memory fixes (§13a). It reproduces.

**Selection** (`cross-sectional-wide-h10`, T4, 44m 12s, no null): 162 candidates → 133 kept
→ **21 shortlisted, 18 from `pool__basic` and 3 from `pool__ta`**. The top NINE are all
`pool__basic`; `drv_order_vol_imb` is #1 and `drv_clv` #2, as in every previous run. The
best technical channel is `close_wma_7_slope` at rank 10.

**Downstream**, priced on the 340,183 rows the two chains SHARE (the wide chain joins
`pool__ta` and loses 140 `(date, ticker)` rows to its coverage, so `walkforward.compare`
refuses them as two experiments — correctly; this is the intersection `backtest.head2head`
exists to take):

| | narrow, 19 ch | **wide, 21 ch** |
|---|---|---|
| daily IC | +0.1484 | **+0.1520** |
| `ic_t` | 17.36 | **17.64** |
| **Sharpe@30** | **+2.8910** | +2.8136 |
| CAGR@30 | +69.8 % | **+71.0 %** |

**Paired over 236 periods, ρ 0.943: `d_cagr_ann` +0.0100, `t` = +0.46 at 20, 30 and 50 bps.**

⚠️ **IC UP, SHARPE DOWN — the same split PRF-9 found** (+0.1053 vs +0.0927 IC against
ΔSharpe −0.126 there). Adding technical channels makes the ranking marginally better and
the portfolio marginally more volatile, and the paired test cannot separate either from zero.

⚠️ **The three cost levels give an IDENTICAL `t`, and that is correct rather than a bug.**
Both chains hold top-20 of 150 rebalanced every 10 sessions, so their turnover is the same
and the cost term cancels exactly in `a − b`. A paired difference removes anything the two
arms share, and here that includes the entire fee.

⚠️ **`ic_mean +0.1495` from the selection is NOT comparable to the narrow chain's +0.1201.**
This run carries no null, and a selection over more candidates always reports a higher
selected IC because it picks the best of more. That is exactly why the question was taken
downstream.

### 13a. ⚠️ FOUR ATTEMPTS, FOUR WALLS, AND THE PHASE PROFILE IS WHAT DISTINGUISHED THEM

The 233-channel version of this run never completed. Each attempt moved the wall:

| attempt | fixed before it | `window design` peak | died |
|---|---|---|---|
| 1 | — | *never exported* | host RAM on the laptop, at export |
| 2 | export ticker filter (SQL, in `reader.read`) | 26.2 G | phase 4 |
| 3 | `window_design` cube row-blocking | **26.1 G — no change** | phase 4 |
| 4 | `panel_window_design` preallocation | **21.7 G** | phase 4 |

✅ **VRAM sat at 6.1 of 14.9 GB throughout** — `gpu.tree_shap`'s row-blocking held from the
first attempt, and `VRM-1`'s VRAM half is genuinely resolved.

⚠️ **ATTEMPT 3 IS THE INSTRUCTIVE ONE.** Row-blocking `window_design` was a real fix for a
real 23.3 GB allocation and it moved the peak by 0.1 GB, because the CROSS-SECTIONAL path
does not call that function down the frame — `panel_window_design` windows each ticker and
then did `pd.concat(blocks).sort_index()`, holding **three** full copies of the design.
`rss` even went UP (13.7 → 19.3 G) because the design became preallocated. **Only `peak`
beside `rss` told the difference**, and `selector._tick` reports both for this reason.

⚠️ **Running at 162 rather than 233 is a SCOPE DECISION, not a discovery.** What remains is
the ranker ensemble's own copies of a 1,398-column design, and `PRF-9` had already settled
that the money question is downstream — so the marginal value of 233 over 162 did not
justify a fifth wall. 162 is still 35 % wider than the previous best.

---

## 14. ✅ `WFO-1` CLOSED 2026-08-21 — one directory, one experiment, and a refusal

`run.DEFAULT_OUT` is a single fixed path and all three artefacts are written by BASENAME,
so the documented command run at a second horizon overwrote the first
horizon's whole OOS track — silently. It was caught on 2026-08-20 by reading `DEFAULT_OUT`
before pressing enter, which is not a control.

### 14a. The fix is a REFUSAL, and the rename was rejected on purpose

Two candidates were on the table and only one survives contact with the repo as it is:

| candidate | verdict |
|---|---|
| derive the leaf — `results/walkforward/<ticker>__<table>/` | ❌ **rejected.** Five tracks already exist under hand-chosen names (`walkforward`, `walkforward_h10`, `_h10_arch`, `_h10_wide`, `settings_h10/*`) that CLAUDE.md and this file cite BY PATH. Moving them trades a live citation for a guarantee the refusal gives anyway |
| **refuse a mismatched directory** | ✅ **shipped.** `walkforward/manifest.py` |

`manifest.claim(out_dir, ident)` runs **before a single fold is built**, so a refusal costs
seconds rather than half an hour of training. The identity is everything that changes the
TENSORS a fold is built from:

```
ticker  table  first_test  step_months  val_months  scale_target  rank_min_width
```

`table` carries the target, the lookback and the horizon; `arm` and `config` are recorded
as **provenance and deliberately NOT identity**, or two arms of one sweep would read as two
experiments and `compare` could never be run on them.

### 14b. ⚠️ THE FIVE LEGACY TRACKS ARE COVERED — BUT ONLY ON THE TABLE

They carry no manifest, so `claim` falls back to recovering the table from `folds.csv`'s
run names (`<model>__<ticker>__<table>__<fold tag>__<stamp>`, so the table comes off by
position). Verified against every track on disk, including the scoped
`rank_10day__final__d20_h10__wide10`.

⚠️ **`folds.csv` records NO KNOBS, and they are not inferred** (§5 rule 2 — an absent
measurement is absent). So a legacy directory is guarded against the horizon collision that
actually happened and **not** against a knob-only one. That limit is pinned by a test named
after it, rather than left to be discovered. Re-running any legacy track once writes its
manifest and closes the gap.

### 14c. The half that misstates a number rather than destroying one

`evaluate --horizon` and `compare --horizon` both **defaulted to 20**, and the horizon sets
BOTH the interval the periods are cut at AND the `return_{h}day` column scored — so an h=10
track scored without the flag silently scored the wrong label against the right
predictions, with nothing in the output saying so.

Both now **derive** it from the track (manifest, else `folds.csv`, via
`train_test_creator.FINAL_TABLE` — the one parser that owns the name, per `TGT-1`) and
**raise** when an explicit `--horizon` disagrees. `compare` additionally refuses arms built
at different horizons and points at `walkforward.pair`, which is the only tool that can
compare two (§10).

### 14d. ⚠️ `RPR-1`'s half, fixed in the same commit — the tracks were UNTRACKED too

`git ls-files results/` returned **nothing**: `.gitignore`'s blanket `*.csv` takes every
artefact and `results/` — unlike `reports/`, which has negations — was never given any. So
the repo's strongest evidence was simultaneously untracked and one omitted flag from being
overwritten. Either half alone is survivable.

**Measured before deciding, not after:**

| artefact | count | total |
|---|---|---|
| `folds.csv` | 18 | **19 KB** |
| `per_fold.csv` | 8 | **22 KB** |
| `predictions_oos.csv` | 18 | **323 MB** |

So `folds.csv` + `per_fold.csv` are negated — **26 files, 41 KB**, and `per_fold.csv` is
where the per-fold Sharpe/IC/CAGR series every register quotes actually lives.
`predictions_oos.csv` stays ignored: 323 MB in git to save the ~33 GPU-minutes that
regenerate one is a bad trade, and recording it here makes it `RPR-1` **accepted on
purpose** rather than discovered later. `manifest.json` needs no rule — no blanket `*.json`
exists — so the identity that makes a track re-identifiable is tracked automatically.

### 14e. What §14 does NOT establish

1. **It is a guard, not a naming scheme.** Two sweeps of the SAME experiment still
   overwrite each other, by design — a redo is legitimate.
2. ⚠️ **A track swept before 2026-08-21 and never re-run carries no knobs**, so §14b's
   limit applies to it until it is.
3. **Nothing here re-derives a lost artefact.** `predictions_oos.csv` remains untracked and
   the run folders it is assembled from are gitignored (`RPR-1`).

---

## 15. ⚠️ THE SEED FLOOR — how big is an arm gap that means NOTHING? 2026-08-21

```powershell
cd src
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --first-test 2017-01-01 --out ../results/seeds_h10 `
    --arm gbt:gbtseed42__all__rank_10day__final__d20_h10.yaml `
    --arm gbt:gbtseed01__... --arm gbt:gbtseed07__... `
    --arm gbt:gbtseed13__... --arm gbt:gbtseed21__...      # 13m 16s, 50/50 arm-folds
python -m walkforward.compare --top-k 20 --horizon 10 --universe all --draws 0 `
    gbtseed42=../results/seeds_h10/gbtseed42 gbtseed01=… gbtseed07=… `
    gbtseed13=… gbtseed21=… archgbt=../results/walkforward_h10_arch/gbt   # 3m 34s
```

**Why it had to be run.** `grep '^seed:' src/model/*/configs/*.yaml` returned **`seed: 42`
32 times out of 32**, and `runs/index.csv` carries no seed column — so every architecture
verdict in §11 rests on **one fit per arm per fold**. `pair.block_bootstrap_diff` resamples
the realised RETURN series, which prices the market's sampling noise and **nothing about
the variance of the FIT**. Five arms differing only in the seed measure that variance
directly, with the same statistic, on the same folds.

⚠️ **THE SEED THAT MOVES A GBT IS `model.random_state`, NOT the top-level `seed`.**
`engine.set_seed(config["seed"])` seeds numpy and torch; XGBoost draws its `subsample` /
`colsample_bytree` rows and columns from its own `random_state`. Both are moved together in
the five configs. ⚠️ **Checked before the sweep was spent**: four seeds on 60,000 real h=10
windows gave predictions correlating with seed 42 at Pearson **0.956-0.968**, not 1.0, and
node counts 1,397-1,400. The seed genuinely moves the fit.

### 15a. ✅ THE DETERMINISM CHECK PASSED EXACTLY, AND IT IS WHAT LICENSES THE REST

`archgbt` — §11's `gbt` arm, from a **different sweep, alongside six neural arms** — was
scored as a sixth arm here. Against `gbtseed42` it returns **corr 1.0000, `d_sharpe`
0.0000, `t_ret` NaN** (the difference series has zero variance) and a **bit-identical
per-fold column**. Two independent sweeps, different companion arms, same numbers: the fold
builds are deterministic and arms do not contaminate each other. It also confirms
`gbtseed42` **is** §11's `gbt` row — Sharpe@30 **2.8910**, IC **0.1460**.

### 15b. THE FLOOR

| pooled over 236 periods, top-20, 30 bps | |
|---|---|
| Sharpe@30 by seed | 2.8910 · 2.8871 · 2.8451 · 2.8488 · **2.9791** |
| **sd** | **0.0540** |
| range | **0.1340** (half-width ±0.067) |
| **max paired `d_sharpe` vs seed 42** | **0.0882** |
| every seed's `p_sharpe` | **0.308 … 0.964** — all tie, as they must |
| pooled IC by seed | +0.1420 … +0.1449, **sd 0.00073** |

**So the floor on the statistic §11c reports is `|d_sharpe| ≈ 0.09`.**

### 15c. ⚠️ APPLYING IT TO §11c — TWO ARMS SURVIVE, THREE ARE AT OR INSIDE THE FLOOR

| arm | `d_sharpe` vs `lstm` | **× the seed floor (0.088)** | `p_sharpe` | reading |
|---|---|---|---|---|
| `cnn` | **−0.398** | **4.5×** | 0.001 | ✅ real, and the only one clearing Bonferroni |
| `gbt` | **+0.360** | **4.1×** | 0.044 | ✅ **not seed luck** — see below |
| `cnnlstm` | −0.164 | 1.9× | 0.295 | above the floor, still a tie |
| `transformer` | +0.091 | **1.0×** | 0.537 | ⚠️ **its whole advantage is one seed** |
| `tcn` | +0.091 | **1.0×** | 0.406 | ⚠️ same |
| `bilstm` | −0.058 | **0.7×** | 0.612 | ⚠️ **inside** the floor |

⚠️ **WHAT THIS DOES AND DOES NOT DO FOR `gbt`.** It removes ONE alternative explanation —
its +0.360 is 4.1× what a reseed produces, so it is not seed luck. It does **nothing** to
the multiple-comparison problem: `p_sharpe` 0.044 against Bonferroni **0.0083** for six
challengers is unchanged. §11d's *"best measured, not established"* stands, now for a
narrower reason. ⚠️ And §8a-ter is the other half: `gbt`'s advantage **changes sign at
h=20** (`d_sharpe` −0.016, p = 0.941).

⚠️ **`transformer` AND `tcn` ARE THE NEW INFORMATION HERE.** Their bootstrap `p` already
called them ties, but *"tied"* and *"the entire measured gap is the size of a random seed"*
are different sentences, and only the second one closes the question. §11a's ranking of the
seven arms below `cnn` should not be read as a ranking at all.

### 15d. ⚠️ A PER-FOLD SHARPE IS **4.4× MORE SEED-SENSITIVE** THAN THE POOLED ONE

| | mean per-fold range over 5 seeds | pooled range |
|---|---|---|
| Sharpe@30 | **0.593** | **0.134** |

Worst folds: **oos2019 spans 4.818 … 5.897 (1.079)**, oos2018 0.923, oos2020 0.926,
oos2026 0.750. Best: oos2022 at 0.213 — the bad fold is the stable one, because everything
is near zero there.

⚠️ **SO A CELL IN §3's OR §9's PER-FOLD TABLE IS NOT A STABLE NUMBER AND MUST NOT BE READ AS
ONE.** A one-point difference between two arms in one fold is inside a reseed. Pooling over
236 periods cuts the noise 4.4×, which is why the pooled row is the one every register
quotes — that convention is now measured rather than asserted.

### 15e. ✅ BUT THE DECAY IS **NOT** A SEED ARTEFACT

The obvious worry after §15d: §9a's decay is fitted on exactly those unstable per-fold
cells. It survives.

| per seed | Sharpe@30 slope | first-half → second-half fall |
|---|---|---|
| 42 / 01 / 07 / 13 / 21 | −0.329 · −0.279 · −0.280 · −0.319 · −0.332 | −55 % · −54 % · −54 % · −57 % · −57 % |
| **mean ± sd** | **−0.308 ± 0.027** | **−55.4 %** |

**Five seeds, five slopes inside 0.05 of each other, and the proportional fall lands in a
3-point band.** The decay is a property of the track, not of the fit. ⚠️ These are the
**`gbt`** arm's numbers; §9a's −0.219/fold and −45.8 % are the **`lstm`** track's, so the
levels are not comparable — what transfers is that the SHAPE is seed-stable.

### 15f. ⚠️ IC IS 4× MORE SEED-STABLE THAN SHARPE, IN RELATIVE TERMS

| | mean | sd over seeds | relative |
|---|---|---|---|
| pooled IC | 0.1465 | 0.00073 | **0.5 %** |
| pooled Sharpe@30 | 2.8902 | 0.0540 | **1.9 %** |

**The ranking barely moves; the money does.** `long_only_top_k` takes the top 20 of ~150,
which is a THRESHOLD — a hair's change in the ranking swaps which names are held, and the
portfolio inherits a discretisation the IC never sees. ⚠️ Practical consequence: **when two
arms differ, check the IC before believing the Sharpe.** In §11 `cnn`'s IC (+0.1171) is
genuinely below the field's +0.14 and its Sharpe loss is corroborated; `gbt`'s IC (+0.1460)
is 0.005 above `lstm`'s, which is **7× the IC seed floor** and does corroborate its
direction — but on a quantity whose whole spread across the seven arms is 0.03.

### 15g. THE RECORDED PREDICTION, AND HOW IT SCORED

> Written into TODO before the run: *"the seed spread on `gbt` Sharpe@30 is ±0.10-0.15, so
> `cnn`'s −0.398 survives comfortably and `gbt`'s +0.360 does not become significant. If the
> spread is ≥ 0.3, everything in §11 below `cnn` is noise."*

| clause | outcome |
|---|---|
| *"`cnn` survives comfortably"* | ✅ **RIGHT** — 4.5× the floor |
| *"`gbt` does not become significant"* | ✅ **RIGHT**, and for a reason the prediction did not state: the floor cannot touch `p_sharpe` at all. It removes seed luck and leaves the six-arm correction untouched |
| *"spread ±0.10-0.15"* | ⚠️ **AMBIGUOUS, AND THAT IS THE LESSON.** As a RANGE, 0.134 is dead centre. As a HALF-WIDTH, ±0.067 is half of what I said. **A prediction that can be scored two ways is half a prediction** — state the estimator next time |
| *"if ≥ 0.3, everything below `cnn` is noise"* | did not fire at 0.3, but a weaker form DID: at 0.088, **`transformer`, `tcn` and `bilstm` are at or inside the floor** |
| **unpredicted** | §15d (per-fold cells are 4.4× noisier) and §15f (IC is 4× more stable than Sharpe). Neither was anticipated and both change how the existing tables are read |

⚠️ **THE COST ESTIMATE WAS 7.5× TOO HIGH, AND THE ERROR IS INSTRUCTIVE.** TODO said
**~1 h 40 m**, anchored on §12's *"~20 min per `gbt` track"* × 5. It took **13m 16s**,
because `--arm` builds each fold's tensors **once** and every arm trains off that build —
the expensive half is the fold build, not the fit. **A five-arm sweep is not five tracks**,
and the same arithmetic error would have over-costed every future arm sweep on this page.

### 15h. What §15 does NOT establish

1. ⚠️ **ONE ARCHITECTURE, AND IT IS THE CHEAPEST ONE — this is the main limitation.**
   `gbt` is shallow, scale-invariant and resamples only rows and columns. **A
   205 k-parameter recurrent net has a second noise source this sweep cannot see** —
   initialisation and batch order — so the `lstm` floor could be larger, and `lstm` is
   §11's REFERENCE arm. The obvious follow-up is five `lstm` seeds, which by §15g's
   corrected arithmetic is ~2 h 45 m, not 13 min.
2. **Five seeds.** `sd = 0.054` on 4 degrees of freedom, so the floor is itself estimated
   to roughly ±30 %.
3. **One horizon, one `k`, one universe** — and `NUL-1` unchanged: no null here prices the
   feature selection every arm reads.
4. ⚠️ **It measures the SEED, not the SCHEDULE.** §11d's caveat stands: nothing was tuned
   per arm, and a loss may still be a schedule mismatch rather than an architecture verdict.
