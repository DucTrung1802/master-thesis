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
