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

## 9. ⚠️ THE SAME SWEEP AT h=10 — 2026-08-20, and it BEATS h=20 on every cost level

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
