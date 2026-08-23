# pipeline.md — the end-to-end chain, stated as **"which ticker, on which date"**

> Written 2026-08-21. Every figure was **measured off the artefacts on disk that day**
> using the repo's own `backtest.portfolio` rules — not re-implemented, so these are the
> same numbers the published Sharpe was computed with.
>
> ### Which pipeline document is which
>
> | file | question it answers |
> |---|---|
> | [PIPELINE_h10_CAGR74.md](PIPELINE_h10_CAGR74.md) | *how is the **+74 %/yr** number made?* — provenance, nulls, caveats |
> | **pipeline.md** *(this file)* | *what does the chain **OUTPUT**, in what shape, and can it name a ticker and a date?* |
> | [RUNBOOK.md](RUNBOOK.md) | *how do I RUN it?* — commands are that file's job |
>
> ## ⚠️ THE ONE-LINE ANSWER
>
> **The pipeline outputs `(date, ticker, weight)` — 4,720 picks across 236 dated books —
> and it CANNOT produce one for today.** ⚠️ **And §9d is the number that
> matters most: under a tradability gate the CAGR falls from +181 % to +36.5 %.** The mechanism is complete and measured; the
> *live* end is missing for three specific reasons in §6. Read §5 before treating any
> single book as a recommendation.

---

## 1. What goes in, what comes out

```
INPUT                                                    OUTPUT
─────                                                    ──────
781 VN tickers, daily bars + order counts + flow   →     (date, ticker, y_pred)   349,581 rows
2009-01-02 … 2026-08-07                                  ↓ rank within each date
                                                         ↓ take top 20
                                                   →     (rebalance_date, ticker, 5 %)
                                                         236 books × 20 names = 4,720 picks
```

**One row of the final output is**: *"on 2026-06-10, hold DCS at 5 % of the book for the
next 10 sessions."* That is the whole product. There is no price target and no per-stock
signal — see §5.4.

---

## 2. The stages, with the row count at every hop

| # | stage | writes | rows × cols | measured cost |
|---|---|---|---|---|
| ① | scrape | `raw_data/` | TradingView · CafeF · Simplize · GICS | hours |
| ② | bronze | `bronze_schema`, 25 tables | raw-faithful | minutes |
| ③ | silver | `silver_schema` | `stocks_basic` 38 cols | minutes |
| ④ | gold | `gold_schema` | + TA, returns, vol | minutes |
| ⑤ | unified | `unified_schema_all.pool__basic` | **2,388,975 × 101** | 11 m 08 s |
| ⑥ | panel + label | *(in memory)* | **624,448 × 104**, 150 names, 4,388 sessions | 2 m 04 s |
| ⑦ | selection | `reports/feature_selection/<run>/` | 90 → 61 kept → **19** | **6 h 02 m** (T4) |
| ⑧ | final_features | `unified_schema_all.rank_10day__final__d20_h10` | **624,448 × 23** | ~6 s |
| ⑨ | train_test_creator | `src/train_test_set/<name>/` | 424,776 / 93,268 / 93,383 × 20 × 19 | ~11 s |
| ⑩ | model | `src/model/runs/<run_id>/` | 208,769 params → `predictions_*.csv` | 4 m 31 s |
| ⑪ | backtest | `<run>/results/backtest_*.csv` | the book, costed | 1 m 14 s |
| Ⓦ | **walkforward** | **`results/walkforward_h10/`** | ⑨-⑪ **× 10 folds** | **33 m 26 s** + 8 m 59 s |

⚠️ **Stages ⑨-⑪ run ten times.** The published output is the ten folds' out-of-sample
predictions concatenated — `results/walkforward_h10/predictions_oos.csv`.

---

## 3. The output artefact

`results/walkforward_h10/predictions_oos.csv`

```
date,ticker,y_true,y_pred,fold
2017-01-03,AAA,0.15986393,-0.02345232,oos2017
```

| | |
|---|---|
| rows | **349,581** |
| tickers | **150** |
| sessions | **2,383** · 2017-01-03 → 2026-07-24 |
| `y_pred` | predicted `cs_rank_10day` ∈ [−0.5, +0.5] — position in *that date's* cross-section |
| `y_true` | the realised rank, for scoring only |

**The universe spans all three boards:**

| exchange | names |
|---|---|
| HOSE | **109** |
| HNX | **26** |
| UPCOM | **15** |

---

## 4. From ranking to a dated pick list — the rule, and its statistics

`backtest.portfolio.long_only_top_k`, unchanged:

| rule | value |
|---|---|
| rebalance | every **10 sessions** → **239** dates |
| selection | **top 20** by `y_pred` |
| weights | **equal, 5 % each** |
| direction | **long only** — HOSE offers no shorting |
| screen | names at their daily **price ceiling** on the entry date are dropped (no sellers) — **9,259 of 349,581 rows, 2.65 %** |
| cost | `round_trip × ½ × Σ\|Δw\|`, 50 bps round trip |

### 4a. Measured output statistics

| | measured |
|---|---|
| rebalance dates | **239** |
| ⚠️ **skipped** for width `< k` | **3** — 2026-06-24, 2026-07-08, 2026-07-22 |
| **usable books** | **236** ✅ *matches the published `n_periods` exactly* |
| **total picks** | **4,720** = 236 × 20 |
| distinct names ever picked | **149 of 150** |
| median appearances per name | **26** of 236 books · max **108** (`DCT`) |
| top 10 names' share of all picks | **17.9 %** |

### 4b. Turnover — what the cost actually charges

| | |
|---|---|
| names replaced per rebalance | mean **65.1 %**, median 65.0 %, range 20 % – 90 % |
| implied fee at 50 bps × 25.2 rebalances/yr | **8.2 %/yr** |

✅ This **confirms the `τ = 0.70` assumption** in `backtest/CONTEXT.md` §3 from the data —
measured 0.651, assumed 0.70, predicted drag 8.8 %/yr against a measured 8.2 %.

### 4c. ⚠️ Where the picks come from — the model prefers the boards you can least trade

| exchange | names | share of scored rows | **share of PICKS** | vs. its share |
|---|---|---|---|---|
| HOSE | 109 | 72.7 % | 55.4 % | **0.76×** |
| HNX | 26 | 17.4 % | 22.8 % | **1.31×** |
| **UPCOM** | **15** | **9.9 %** | **21.7 %** | **2.20×** |

⚠️ **UPCOM is over-picked 2.2×** and it is the least liquid board. The most-selected names
are `DCT` (108 books), `DCS` (106), `EFI` (87), `PVV` (84); the liquid blue chips are rare —
`ACB` 51, `VCB` 30, `FPT` 28, `SSI` 17, `HPG` 12, `VNM` 12.

**This is the single most important tradability finding in this file.** The backtest charges
30-50 bps and models **no ADV cap and no slippage**, on a book that concentrates in small
UPCOM names where a real position would move the price against itself. See TODO `P12` (⚠️ this line cited `P8` until 2026-08-22 and that was already the wrong item).

### 4d. A worked example — the last book with a real cross-section

**2026-06-10**, top 20 of 147 buyable names, 5 % each, held 10 sessions:

| ticker | board | `y_pred` | realised |
|---|---|---|---|
| DCS | UPCOM | +0.2807 | +0.1409 |
| LAS | HNX | +0.2350 | −0.1644 |
| PFL | UPCOM | +0.2283 | −0.3658 |
| DXP | HNX | +0.2180 | +0.2584 |
| BCC | HNX | +0.2110 | +0.2919 |
| TNG | HNX | +0.2081 | +0.3322 |
| DIC | UPCOM | +0.2081 | +0.1409 |
| NTP | HNX | +0.2032 | +0.3121 |
| SDD | UPCOM | +0.2012 | +0.1409 |
| KKC | HNX | +0.1952 | −0.2785 |
| PVE | UPCOM | +0.1942 | −0.3087 |
| VGS | HNX | +0.1876 | −0.0436 |
| ITQ | HNX | +0.1737 | +0.1409 |
| MPC | UPCOM | +0.1714 | +0.1409 |
| EBS | HNX | +0.1697 | −0.4530 |
| VST | UPCOM | +0.1690 | +0.1409 |
| HLD | HNX | +0.1423 | −0.2919 |
| VHG | UPCOM | +0.1406 | −0.3859 |
| PGS | HNX | +0.1363 | +0.0772 |
| PVS | HNX | +0.1352 | +0.3054 |

**Outcome: mean realised rank +0.0065, 12 of 20 in the top half — statistically a wash.**
⚠️ **Zero HOSE names in this book.** That is not a bug; it is §4c in one snapshot.

---

## 5. ⚠️ How to read a book — four things measured

### 5.1 One book is close to a coin flip; the edge is in the aggregate

| over 236 books | |
|---|---|
| mean realised rank per book | **+0.0688** (sd 0.0755) |
| books with a positive mean | **193 of 236 = 81.8 %** |
| `t` of the per-book mean | **+14.01** |
| individual picks finishing in the top half | **60.2 %** of 4,720 |
| worst book | **−0.1111** (2023-03-20) |
| best book | **+0.3051** (2021-01-14) |

**The +74 %/yr is a 60/40 edge compounding over 236 periods.** A single book carries almost
no information, and one bad book is entirely normal — 43 of 236 were negative.

### 5.2 ⚠️ The width guard protects the number and HIDES the freeze

`long_only_top_k` does `if len(day) < k: continue`. In 2026 that silently skipped **three**
rebalance dates where only 7 of 150 names had data. ✅ **The published +74 % is therefore
clean** — 239 − 3 = 236, and no period was scored on a 7-name cross-section.

⚠️ **But nothing raised, and nothing said so.** A data freeze shortens the track instead of
failing it — the same family as *"a green asset is not evidence of fresh data"* (CLAUDE.md
§5 rule 10). **Always check the width per rebalance date before reading a recent book.**

### 5.3 The cross-section must be scored whole

`cs_rank` is a rank *within a date*. You cannot ask *"is VCB a buy?"* — you must score all
150 names on that date and read VCB's position among them. With 7 names there is nothing to
rank.

### 5.4 ⚠️ It ranks, it does not price

R² ≈ 0, `mase` 0.974-0.997 — the model beats *"predict the mean rank"* by 0.3-2.6 %. It
tells you **where a name sits among these 150 over the next 10 sessions**, never what it
will be worth. Inverting that into a price needs a 10-day market forecast *and* a
dispersion forecast, which is exactly what this repo has failed at five times.

⚠️ **For one specific name the honest answer is often "no trade":** `VCB` appears in **30 of
236** books, `HPG` and `VNM` in **12**. At h=20 `VCB` took **zero trades in 33 periods** —
a correct call, and not a tradable signal for that stock.

---

## 6. ⚠️ WHY THERE IS NO BOOK FOR TODAY — three concrete blockers

### 6.1 `FRZ-1` — the data is frozen, and it is the binding blocker

Names scored and buyable per rebalance date:

| date | names |
|---|---|
| 2026-05-27 | 143 |
| **2026-06-10** | **147** ← the last real book |
| 2026-06-24 | **7** |
| 2026-07-08 | **7** |
| 2026-07-22 | **7** |

Mean names scored per session, by year: **145-147 for 2017-2025**, and **113.3 in 2026** —
the average hides a cliff. ⚠️ `MAX(date)` on `pool__basic` reports **2026-08-07** and
conceals this completely.

### ⚠️ 6.1-bis. CORRECTED 2026-08-22 — THE CLIFF AT 2026-06-11 IS IN THE **LABEL**, NOT IN THE PRICE

The sentence this section used to carry — *"after 2026-06-11 only seven names carry fresh
data"* — had the DATE right and the MECHANISM wrong, and the difference decides what a live
chain could do today. Measured on the 150-name universe:

| session | rows with a `close` | with `return_5day` | with `return_10day` | with `return_20day` |
|---|---|---|---|---|
| 2026-06-11 | **150** | 150 | **150** | 7 |
| 2026-06-12 | **150** | 150 | **98** | 7 |
| 2026-06-18 | **150** | **150** | 7 | 7 |
| 2026-06-19 | **150** | 98 | 7 | 7 |
| 2026-06-25 | **150** | 7 | 7 | 7 |
| 2026-06-26 | 98 | 7 | 7 | 7 |
| 2026-06-29 | **7** | 7 | 7 | 7 |

⚠️ **All 150 names carry a PRICE through 2026-06-25.** The cliff in the price data is
2026-06-26 → 06-29 (150 → 98 → 7). What ends on 2026-06-11 is the **forward return**, because
`return_10day` at date `t` needs a close at `t+10` and that runs off the end of the data.

Three consequences, and each one changes a decision:

1. ✅ **The 2026-06-10 book's realised returns are REAL prices**, not a carried-forward stub.
   Its label needs 2026-06-24, which all 150 names have.
2. **The horizon decides where the usable track ends**: last session with ≥100 names labelled
   is **2026-06-18** at h=5, **2026-06-11** at h=10, **2026-05-28** at h=20.
3. ⚠️ **A live-scoring module (`P8`) has ~10 more sessions of usable FEATURES than the old
   sentence implied.** You can rank the cross-section on 2026-06-25; you simply cannot score
   the outcome yet. *Scoring a book and evaluating a book fail on different dates.*

**Fix: re-scrape the 143 frozen tickers.** Until then the chain cannot emit a book at all,
because a 7-name cross-section is not a cross-section. ✅ **FIXED 2026-08-23** — the universe re-scrape put **771 names** back on the last session (CLAUDE.md §6-2-bis), so the live-scoring module (TODO **`P8`**) is no longer blocked.

### 6.2 There is no live-scoring entry point

Every stage writes predictions for a **dataset's test split**. Nothing in the repo loads a
trained fold, windows the last 20 sessions for all 150 names on today's date, and emits a
ranking. It is a small module — but it does not exist. TODO **`P8`**.

### 6.3 Execution realism is unpriced, and §4c makes it urgent

No ADV cap, no slippage, and the ceiling screen covers **entry only** — a name at its floor
on the exit date cannot be sold either, which is precisely when a loser is. On a book
**2.2× overweight UPCOM**, this is not a refinement; it is the question of whether the
strategy is tradable at any size. TODO **`P12`**.

---

## 7. What would make it emit a real pick list

| # | step | ⏱ | TODO |
|---|---|---|---|
| 1 | ~~re-scrape the frozen tickers, verify **per-ticker** max date~~ | ✅ **DONE 2026-08-23**, 1h 05m | — |
| 2 | rebuild `pool__basic` → `rank_10day__final__d20_h10` | ~12 min | — |
| 3 | write the live-scoring module (load fold, window 20 sessions × 150 names, rank) | ~½ day | **`P8`** |
| 4 | ADV cap + sell-side floor screen **before** trusting any level | ~1 day | **`P12`** |

⚠️ **Steps 1-3 give you a list. Step 4 tells you whether it is worth anything at your
size**, and on the evidence in §4c that is the one most likely to change the answer.

---

## 8. ⚠️ And the standing caveats do not go away

Restated because a pick list invites forgetting them — the full versions are in
[PIPELINE_h10_CAGR74.md](PIPELINE_h10_CAGR74.md) §12:

- **Survivorship protects the `z` and NOT the CAGR.** `silver.stocks_basic` holds no
  delisted name. `z = +18.58` stands; **+74 %/yr does not**.
- **`NUL-1`** — no null prices the feature selection, the architecture search, or the choice
  of `h=10`.
- **The universe is not point-in-time** — top-150 by pre-2014 turnover, and **358 of 781
  tickers could never enter it**.
- **`FNM-1`** — the model is fed globally standardised features while the selection scored
  within-date ranks.

**This is a research backtest. It is not a recommendation to buy any security.**

---

## 9. ⚠️ HOW MANY NAMES TO BUY — `k`, and what lowering it actually buys

Measured 2026-08-22 on the published `results/walkforward_h10/` track. **No new parameter was
needed: `--top-k` IS the cap on how many stocks may be bought** (`backtest.portfolio.
long_only_top_k`, equal weight `1/k`), and it is already wired through `backtest`,
`walkforward.evaluate` and `walkforward.compare`. Nothing upstream of stage ⑪ moves with it —
`python -m pipeline` reported all ten stages `up to date` — so re-scoring the existing track
is the whole experiment.

✅ The harness was checked against the artefact first: at k=20 it reproduces the published
row to every digit (CAGR@30 **0.7398**, Sharpe **2.5310**, 236 periods).

### 9a. The k ladder, 30 bps, on the 236 comparable books

| k | CAGR@30 | Sharpe@30 | max DD | cost drag |
|---|---|---|---|---|
| 3 | **+217.9 %** | +3.134 | −29.1 % | 6.2 % |
| **5** | **+181.6 %** | **+3.163** | −33.5 % | 6.0 % |
| 10 | +113.3 % | +2.979 | −36.3 % | 5.5 % |
| 15 | +91.1 % | +2.735 | −37.7 % | 5.2 % |
| **20** *(published)* | **+74.0 %** | +2.531 | −39.9 % | 4.9 % |
| 30 | +56.9 % | +2.191 | −45.8 % | 4.4 % |
| 50 | +40.8 % | +1.738 | −45.2 % | 3.6 % |

Monotone in both directions — the shape a real cross-sectional ranking makes. The equal-weight
universe over the same books is **+13.5 %**. At k=5 the null still clears easily: **z = +15.01**
at 30 bps (200 within-date shuffles, null MAX +0.748 below the observed +3.155), against k=20's
+18.58.

### 9b. ⚠️ AT k=5 THE WIDTH GUARD STOPS PROTECTING THE TRACK

`long_only_top_k` skips a date with `if len(day) < k`. At k=20 that silently excluded the three
frozen 2026 rebalance dates (§6.1); **at k=5 the guard passes them, because 7 ≥ 5** — the track
becomes **239 periods, not 236**, and those three books hold five HOSE banks drawn from a
seven-name panel. Worth only ~0.35 pp of CAGR, measured both ways — ⚠️ **but on a LIVE book it
would print five bank tickers as if they were a recommendation.** The guard was doing work
nobody had specified, and lowering `k` removed it.

### 9c. ⚠️ AND THIS IS WHAT LOWERING `k` REALLY BUYS: LESS TRADABLE NAMES

§4c measured the model over-picking UPCOM at k=20. Lowering `k` makes it monotonically worse.
Median matched turnover **of a picked row**, against a universe median of **2.22 bn VND/day**:

| k | median turnover of a pick | picks under 0.1 bn/day | UPCOM share | HOSE share |
|---|---|---|---|---|
| 3 | **0.02 bn** | **65.8 %** | 42.9 % | 35.6 % |
| **5** | **0.03 bn** | **61.4 %** | 38.1 % | 39.2 % |
| 10 | 0.09 bn | 50.2 % | 29.6 % | 47.3 % |
| **20** | 0.30 bn | 38.6 % | 21.7 % | 55.4 % |
| 50 | 1.21 bn | 26.7 % | 13.7 % | 67.0 % |

The names carrying k=5 are `DCT` (61 books), `DCS` (54), `VST` (40), `EFI` (39), `PVV`/`SD7`
(37) — **every one with a median matched turnover of 0.00-0.02 bn VND/day.** Turnover per
rebalance also rises, 65.1 % → **78.7 %**.

⚠️ **So the ladder in §9a is not a menu.** The extra return at low `k` is earned in names that
cannot absorb a position, and the backtest models no ADV cap and no slippage (`P12`).

### 9d. ⚠️ THE DECISIVE TEST — a tradability gate, and the CAGR collapses

Gate on **trailing 60-session median `value_matched`, `shift(1)`** — known at entry, so no
look-ahead. Applied to the same track and the same predictions, k=5, 30 bps:

| gate | daily IC h=10 | CAGR h=5 | CAGR h=10 | Sharpe h=10 | max DD |
|---|---|---|---|---|---|
| **none** | 0.1412 | **+249.0 %** | **+181.3 %** | 3.16 | −33.5 % |
| ADV60 ≥ 1 bn | 0.0816 | **+39.4 %** | **+36.5 %** | 1.08 | −50.7 % |
| ADV60 ≥ 5 bn | 0.0667 | +25.2 % | +19.9 % | 0.72 | −64.0 % |
| ADV60 ≥ 20 bn | 0.0569 | +14.4 % | +10.2 % | 0.47 | — |

✅ **All eight gated cells still CLEAR a 200-draw within-date null, MAX below observed in every
one** — ADV ≥ 1 bn: h=10 k=5 **z = +5.23**, k=20 **+7.64**; h=5 k=5 **+7.40**, k=20 **+12.32**.
ADV ≥ 5 bn: h=10 k=5 **+3.28**, k=20 **+5.60**; h=5 k=5 **+6.42**, k=20 **+10.58**.

⚠️ **`k=20` HAS A HIGHER `z` THAN `k=5` IN ALL EIGHT GATED CELLS**, and at ADV ≥ 5 bn their
CAGR is within a point of each other (+19.9 % vs +19.0 %) while `z` is 3.28 against 5.60. **On a
basket you can actually buy, cutting `k` to 5 weakens the evidence and adds almost no return.**
⚠️ The gated null MEAN is **+0.22 … +0.34**, not zero, so the excess at h=10 is ~0.75 Sharpe.

⚠️ **This is a post-hoc filter on a model trained over all 150 names.** A properly screened
chain re-selects and retrains on the screened basket — that is `plan.md`'s `screen_schema`, and
these numbers are the first measurement of what it is worth.

### 9e. ⚠️ THE SIGNAL DOES NOT DECAY WITH THE HORIZON — and that is a warning, not a feature

The same `y_pred`, scored against every forward horizon:

| h | daily IC | `ic_t` | CAGR@30 k=5 | Sharpe |
|---|---|---|---|---|
| 1 | +0.1403 | +52.6 | **+1416.9 %** | 7.59 |
| 3 | +0.1523 | +32.7 | +416.8 % | 5.06 |
| 5 | +0.1478 | +24.1 | +249.0 % | 4.05 |
| **10** *(the trained label)* | **+0.1412** | +16.1 | **+181.3 %** | 3.16 |
| 20 | +0.1347 | +10.4 | +87.6 % | 1.82 |
| 30 | +0.1328 | +9.1 | +63.7 % | 1.41 |

⚠️ **A genuine 10-day forecast should peak near h=10 and fall away. This one is FLAT from h=1
to h=30.** When the IC does not decay, `CAGR ∝ 1/√h` is pure arithmetic — the same edge
collected 252 times a year instead of 25. **The ladder is not evidence that trading faster is
better; it is evidence that the model is ranking a persistent property rather than forecasting.**

**And the property is measurable — prices that do not move.** Share of rows whose forward
return is **exactly zero**:

| | all rows | ADV60 < 0.1 bn | ADV60 ≥ 1 bn |
|---|---|---|---|
| h=1 | 21.9 % | **51.2 %** | 10.3 % |
| h=5 | 8.9 % | 23.1 % | 3.3 % |
| h=10 | 6.3 % | 16.5 % | 2.2 % |

Half of the illiquid rows do not move in a session. A frozen price returns 0 at *every*
horizon, and in a falling cross-section 0 is a good rank — which is exactly what a flat IC
looks like. ✅ **Under the ADV ≥ 1 bn gate the horizon ladder flattens too** (h=1 +44.4 % against
h=10 +36.5 %), so nearly the whole advantage of trading fast lived in the unbuyable names.

---

## 10. Two named books, read end to end — 2026-08-22

Both are read with the chain's own rules (ceiling screen on, `y_pred` from
`predictions_oos.csv`).

**2026-06-10** — on the 10-session rebalance grid, 150 scored, 3 at ceiling, **147 buyable**:

| # | ticker | board | `y_pred` | ADV60 | matched that day | sessions with NO trade since 06-01 |
|---|---|---|---|---|---|---|
| 1 | **DCS** | UPCOM | +0.2807 | **0.00 bn** | **0.00** | ⚠️ **16 of 18** |
| 2 | LAS | HNX | +0.2350 | 5.94 bn | 1.45 | 0 |
| 3 | PFL | UPCOM | +0.2283 | 0.11 bn | 0.06 | 0 |
| 4 | DXP | HNX | +0.2180 | 4.71 bn | 2.29 | 0 |
| 5 | BCC | HNX | +0.2110 | 0.26 bn | 0.46 | 0 |

⚠️ **The #1 pick is unbuyable** — DCS trades at 700 VND and did not trade at all on the entry
date. Under ADV ≥ 1 bn the book is **LAS, DXP, TNG, NTP, VGS**.

**2026-06-01** — ⚠️ **NOT on the rebalance grid** (seven sessions before 06-10, so a 10-session
hold overlaps the next book; no published statistic covers trading this way). 150 scored, 4 at
ceiling, 146 buyable:

| # | ticker | board | `y_pred` | matched that day | price |
|---|---|---|---|---|---|
| 1 | **DCS** | UPCOM | +0.3158 | **0.00** | 700 đ |
| 2 | **DCT** | UPCOM | +0.2980 | **0.00** | 500 đ |
| 3 | **PGS** | HNX | +0.2697 | **0.00** | 48,800 đ |
| 4 | **DIC** | UPCOM | +0.2623 | **0.00** | 900 đ |
| 5 | TNG | HNX | +0.2589 | 5.13 | 18,900 đ |

⚠️ **Four of the five did not trade a single dong on the entry date.** Under ADV ≥ 1 bn the book
is **TNG, LAS, NBC, VGS, DXP**; under ADV ≥ 5 bn, **TNG, LAS, VGS, DXP, PVC**.

**Realised over the following 10 sessions** — recorded because it shows how little one book
says, not as evidence:

| book | mean realised |
|---|---|
| 06-01 raw | **+6.58 %** — ⚠️ *all of it is `DCT` +40 %, a 500 đ stock that never traded* |
| 06-01 ADV ≥ 1 bn | **−2.07 %** |
| 06-10 raw | **−0.97 %** |
| 06-10 ADV ≥ 1 bn | **+0.29 %** |

⚠️ §5.1 already measured why these four numbers carry nothing: 43 of 236 books were negative and
only 60.2 % of picks land in the top half. **The edge is 236 books deep, and it is not in any
one of them.**
