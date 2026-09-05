# Workflow — quote a number safely

> **Goal:** decide whether a number may go into a document, a thesis chapter, a commit message or
> a sentence to the user — and what must be said beside it. **Cost: minutes.**
>
> ⚠️ **Run this before every number you publish, including one this repo produced itself.** The
> whole convention of this project is that *a number without a null is descriptive, not evidence*.
> Several numbers in the registers were corrected only because someone ran these checks late.

---

## 1. Is the thing under it still there? — **O1**, **O3**

`python -m pipeline` must read `up to date` for every stage **below** the one you are quoting. A
green model run on a stale table is a number about a table that no longer exists.

⚠️ **And check the row count, not just the date.** One chain's `MAX(date)` read fresh while the
last three rebalance dates carried **7 names of 150**. A data freeze SHORTENS a track silently
instead of failing it.

## 2. What kind of number is it?

| kind | the gate |
|---|---|
| a **selection IC** | §3 below |
| a **model metric** | §4 |
| a **Sharpe / CAGR** | §5 |
| a **fundamental** | §6 |
| a **freshness or coverage** figure | it must be a DISTRIBUTION, never a scalar — see [refresh-the-data.md](refresh-the-data.md) §1 |

## 3. A selection IC

- [ ] quote the **null MEAN** beside it. It is often not zero — one headline +0.1075 sat on a null
      mean of +0.0291, so the excess over chance was **+0.078, not +0.1075**. Quoting the raw IC
      overstated it by **37 %**.
- [ ] quote the **null MAX**. ⚠️ **When the max exceeds the observed, `clears_bar` is the wrong
      summary** — say `cleared_p95_not_a_pass`.
- [ ] quote **`z`, not `p`.** `p` is pinned at the `1/(n+1)` floor until a draw beats the observed.
- [ ] say **how many draws**. 10 to fail, 20 to pass.
- [ ] say **how many experiments were tried.** `NUL-1`: the null prices the feature search inside
      one run and nothing else — not the universe, the horizon, the target, or the fact that this
      was the fifth thread. Measured once: `P(z > 1.83)` is 0.0336 for one name and **0.157 for at
      least one of five.**
- [ ] check the **IC trend**. A mean built from decaying folds is not a signal; a **rising** trend
      on a ragged pool is **data arrival** (rule 23).

## 4. A model metric

- [ ] ⚠️ **On a PANEL, quote the daily-IC t-stat, NEVER `ic_clears`.** `NUL-3`: the evaluator's
      panel null is not label-neutral — its centre moved with the MODEL and it got both ends
      wrong, manufacturing a clear for the weakest model and failing the strongest.
- [ ] ⚠️ **Was the run scored before 2026-08-18?** Then its `ic_t` is overstated by **exactly
      `√h`** (`ICT-1`) until re-scored — and re-scoring takes **two** commands (**C8**).
- [ ] ⚠️ **A metric that CANNOT FAIL is not a pass.** `hit_rate` is 1.0 by construction on a price
      LEVEL target, because every label is positive. One run reported `+1.0000` beside
      `ic_mean −0.1638`.
- [ ] does it **beat the naive predictor**? `mase < 1`. One panel run reached **0.9937** — six
      parts in a thousand — and that clearance's *size* is the finding: **the result is the ORDER,
      not the magnitude.** ⚠️ `mase` is **NaN** on runs scored before that fix; do not read a
      blank as a pass.
- [ ] ⚠️ **Is the whole spread one error bar?** Eleven architectures once ranged IC −0.100 to
      +0.126 — a span of 0.227 against `SE(IC)` of 0.197. The largest `|t|` on the board was
      +1.42. **Ranking those architectures was reading noise.**
- [ ] ⚠️ **Is the gap bigger than a reseed?** The measured seed floor is `|d_sharpe| ≈ 0.09`.
      Below it, *"tied"* is the wrong word — say **the measured gap is the size of a reseed**.
- [ ] ⚠️ **Never compare two arms in ONE fold** — a per-fold Sharpe is **4.4× more seed-sensitive**
      than the pooled one.

## 5. A Sharpe or a CAGR

- [ ] ⚠️ **Survivorship protects the `z` and NOT the CAGR.** Every shuffled draw picks from the
      same survivor basket, so a null-based `z` stands while a CAGR read off that universe does
      not. `silver.stocks_basic` holds **no delisted name**.
- [ ] ⚠️ **Quote `k` beside any CAGR.** CAGR rises monotonically as `k` falls — +74.0 % at k=20 to
      +217.9 % at k=3 — and so does concentration into names nobody can buy: median matched
      turnover of a picked row goes 0.30 bn → **0.03 bn**.
- [ ] ⚠️ **Under a tradability gate the levels collapse** — one k=5 track went **+181.3 % to
      +36.5 %** (ADV ≥ 1 bn) and **+19.9 %** (≥ 5 bn), with max drawdown worsening to −64.0 %.
- [ ] ⚠️ **Say which estimand.** The highest-Sharpe arm is not the highest-CAGR arm: one earned
      **4.2 pp/yr less** while scoring **0.36 more** Sharpe. *"The best model"* is not a
      well-formed question without one.
- [ ] ⚠️ **Check the IC decays with the horizon before believing a CAGR ladder.** One model's IC
      was **FLAT from h=1 to h=30** when it should peak at its own label — with a constant IC,
      `CAGR ∝ 1/√h` is arithmetic, not skill, and the same predictions "earned" +1416.9 %/yr
      rebalanced daily. The cause was measured: **51.2 %** of thin rows have a forward 1-day
      return of **exactly zero**, and a frozen price ranks the same at every horizon.
- [ ] is it **one split**? Say so, with `se_sharpe`. 32 periods gave 0.256; 118 gave 0.155.

## 6. A fundamental

- [ ] **D6** — check `source`. ⚠️ **Anything but `pdf` or `missing` is a defect**, and CafeF's
      "not reported" sentinel is a literal `-1`, which reads as −1 dong in a column of billions.
- [ ] ⚠️ **BID cash flows: read `CFB-1` first.** 7 quarters carry the 1-Jan opening in the CLOSING
      slot; all were accepted at a STRICT layer where `verify_cash` is off, so the identity never
      ran.
- [ ] ⚠️ **Non-bank templates: read `TPL-1` / `CRP-1` first.** Nothing from one may be quoted.

## 7. Say what it does NOT establish

Every headline in this repo carries this, and it is the reason the headlines have survived. For
the current cross-sectional result the list is: **it ranks, it does not price** (R² +0.0003);
`long_short` is a spread of RANKS, not money; `NUL-1` in full force; **survivorship protects the
`z` and not the CAGR**; and **no slippage, no ADV cap, no floor-day exclusion on the sell side**.

---

## Done when

- [ ] the number carries its **date**
- [ ] it carries its **null**, or an explicit `no_null` — ⚠️ **an absent null is recorded as
      absent, never implied to be a pass**
- [ ] it carries what it does **not** establish
- [ ] any register you changed is consistent — see [record-a-finding.md](record-a-finding.md)

## Traps

⚠️ **A cleared bar is not a result.** *A run that fails it is dead; a run that clears it is not
yet alive.*

⚠️ **A cleared selection bar has never survived downstream in this repo.**

⚠️ **`evidence=no_null` is an UNKNOWN; `failed_null` is a MEASUREMENT.** They are not two shades
of the same thing.

⚠️ **`"unrecorded"` is deliberately not read as "the six rankers"**, even though those runs used
six. §5 rule 2: an absent measurement is absent, not inferred.
