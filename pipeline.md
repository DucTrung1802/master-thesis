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
> and it CANNOT produce one for today.** The mechanism is complete and measured; the
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
UPCOM names where a real position would move the price against itself. See TODO `P10`.

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
the average hides a cliff. After **2026-06-11** only seven names, all banks, carry fresh
data. ⚠️ `MAX(date)` on `pool__basic` reports **2026-08-07** and conceals this completely.

**Fix: re-scrape the 143 frozen tickers.** Until then the chain cannot emit a book at all,
because a 7-name cross-section is not a cross-section. TODO **`P1`**.

### 6.2 There is no live-scoring entry point

Every stage writes predictions for a **dataset's test split**. Nothing in the repo loads a
trained fold, windows the last 20 sessions for all 150 names on today's date, and emits a
ranking. It is a small module — but it does not exist. TODO **`P2`**.

### 6.3 Execution realism is unpriced, and §4c makes it urgent

No ADV cap, no slippage, and the ceiling screen covers **entry only** — a name at its floor
on the exit date cannot be sold either, which is precisely when a loser is. On a book
**2.2× overweight UPCOM**, this is not a refinement; it is the question of whether the
strategy is tradable at any size. TODO **`P12`**.

---

## 7. What would make it emit a real pick list

| # | step | ⏱ | TODO |
|---|---|---|---|
| 1 | re-scrape the 143 frozen tickers, verify **per-ticker** max date | ~1 h + scrape | **`P1`** |
| 2 | rebuild `pool__basic` → `rank_10day__final__d20_h10` | ~12 min | — |
| 3 | write the live-scoring module (load fold, window 20 sessions × 150 names, rank) | ~½ day | **`P2`** |
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
