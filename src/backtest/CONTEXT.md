# backtest — CONTEXT (stage 9, created 2026-08-18)

> The stage that answers **"does the ranking pay for its own trading?"**, which no other
> stage in this repo has ever asked. `result_evaluator` answers *does the score rank?* —
> a different question, and the only one the chain could answer until today.
>
> ⚠️ Every number below was **measured on 2026-08-18** against
> `lstm__all__rank_20day__final__d20_h20__20260818-195738`. Read [CLAUDE.md](../../CLAUDE.md)
> §6-0 first; this file assumes it.

---

## 1. Run it

```powershell
cd src
python -m backtest --run lstm__all__rank_20day__final__d20_h20__20260818-195738 `
    --ticker VCB --top-k 15 --draws 200            # measured 1m 14s (200-draw null)
python -m backtest --run <run_id> --split val      # the second window
```

Writes `results/backtest_<split>.csv` and `results/backtest_null_<split>.csv` into the
run folder — the same re-scoring semantics `result_evaluator --rescore` has, so a run
folder stays the source of truth and nothing else is mutated.

⚠️ **A PANEL RUN ONLY.** `predictions_*.csv` must carry a `ticker` column with more than
one name; a single-series run has no cross-section to rank and this stage refuses it.

---

## 2. ⚠️ Three things that are structural, not configuration

**2a. The run folder is not enough — this stage reads the database.** On a `cs_rank_*`
run `y_true` is a RANK: the top-150 h=20 run's spans exactly `[-0.5, +0.5]`. A rank has
no PnL, so the realised forward return is joined back from
`unified_schema_<universe>.pool__targets`, keyed `(date, ticker)` and `validate=
"one_to_one"`. This is the same fact CLAUDE.md §6-0 note 2 states about `long_short`.

**2b. `return_{h}day` is what you can TRADE; `return_rel_{h}day` is what the model
PREDICTS — and the relative one is not tradable here.** Realising it needs a short leg
against VNINDEX. `silver.stock_market` carries **six SPOT indices and no futures**
(VNINDEX, VN30INDEX, VN100, HNX, HNX30, UPCOM), and `experiment_3` records that
single-stock shorting is effectively unavailable on HOSE. Both columns are reported and
they are **not interchangeable**: the relative one measures the SIGNAL, the absolute one
measures the TRADE.

**2c. Non-overlapping, always.** At `h=20` every date carries a forward 20-day return, so
trading every date holds 20 overlapping tranches and multiplies the apparent sample by 20
without adding one independent observation — §5 rule 7 at the portfolio level. Positions
are taken every `h`-th session. **A 2.6-year test window is therefore ~32 periods**, and
`se_sharpe` is in every table for that reason.

---

## 3. The cost convention, and why it is the whole story

`cost = round_trip × ½ × Σ|Δw|`. One name entered and later exited pays `½ + ½ = 1` round
trip; a book replacing a fraction `τ` pays `½ × 2τ = τ`. `ROUND_TRIP_COST = 0.005` — the
**same constant** `sentiment/weekly_xsec.py` uses, deliberately, because two costed
backtests in one repo disagreeing about the cost is a defect and not a study.

⚠️ **`model/CONTEXT.md` §11 measured the cross-sectional strategy DEAD AT 40 bps, which
is below this.** That is a finding, not a parameter to tune away.

The identity that decides the horizon question, pinned in `test_portfolio.py`:

| rebalance | per year | annual drag at `τ=0.70`, 50 bps |
|---|---|---|
| **h=5** | 50.4 | **17.6 %** |
| **h=10** | 25.2 | **8.8 %** |
| **h=20** | 12.6 | **4.4 %** |

Top-100 benchmark CAGR is 9.75 % (§2a-bis). ⚠️ **At h=5 the fees alone exceed the market's
entire return.** That is arithmetic, not a model failure, and it is why §2's four
single-stock defeats at `h=5` were never going to be rescued by a better model.

---

## 4. ⚠️ WHAT IT MEASURED — the first result in this repo that survives a COSTED null

Long-only **top-15 of 150**, rebalanced every 20 sessions, **50 bps**, no shorting.
The null shuffles `y_pred` **within each date**, 200 draws, and re-runs the whole
strategy on it.

| split | window | target | Sharpe | `se_sharpe` | null mean | p95 BAR | null MAX | **z** | |
|---|---|---|---|---|---|---|---|---|---|
| **test** | 2023-11 → 2026-07 | absolute | **+1.484** | 0.256 | +0.189 | +0.686 | +1.134 | **+4.29** | ✅ |
| **test** | | relative | **+0.581** | 0.191 | −1.221 | −0.492 | +0.033 | **+4.21** | ✅ |
| val | 2021-04 → 2023-09 | absolute | +1.737 | 0.284 | +0.397 | +0.744 | +0.923 | **+6.10** | ✅ |
| val | | relative | +2.253 | 0.338 | +0.677 | +1.235 | +1.555 | **+4.37** | ✅ |

**In all four the null MAX is below the observed, so §5 rule 3 does not fire.**

Returns at 50 bps, test split: top-15 **CAGR +30.5 %**, cost drag 3.4 %/yr, max DD −7.6 %,
against an **equal-weight universe of +5.96 %** and VNINDEX **+20.2 %** (verified from
`pool__stock_market`: 1122.5 → 1828.0 over 2.65 yr). The relative version earns **+9.4 %**
where a random 15 of the same 150 earns **−16.6 %**.

⚠️ **`k` is not a knife-edge**, which is the first thing to check when one number looks
good. Absolute Sharpe at 50 bps: k=5 **1.24**, k=10 **1.53**, k=15 **1.48**, k=25 **1.32**,
k=50 **1.06**, k=75 **0.81** — monotone decay away from the top of the ranking, the shape
a real cross-sectional signal makes.

⚠️ **AND IT CONTRADICTS §11's REGIME WALL, IN THE DIRECTION §2a-bis PREDICTS.** §11
measured net@20bps **+1.46 (2017-20) vs −0.51 (2022-26)** and concluded the recent regime
had little learnable signal. This test window **is** 2023-26 and returns +1.48 at 50 bps.
The difference between the two studies is the HORIZON — §11 traded 5 days, this trades 20
— which is exactly what §2a-bis says the uncontrolled variable was.

### 4a. ⚠️ What this does NOT establish

1. **`NUL-1` in full force.** The null prices in the universe, the cost, the schedule and
   `k`. It does **not** price in the feature selection, the architecture search, or the
   choice of this window. **A cleared bar is a floor, not a result.**
2. **32 periods.** `se_sharpe` 0.256. The gap to the equal-weight universe (1.48 − 0.43)
   is ~3 SE of the difference — suggestive, not decisive.
3. **Survivorship cuts one way and not the other.** The 150 names come from
   `silver.stocks_basic`, which holds no delisted name (§2c). ⚠️ **The z-score is
   protected** — every shuffled draw picks from the same survivor basket — but the
   headline **+30.5 % CAGR is not**.
4. **`val` is not clean out-of-sample**: it chose the early-stopping epoch.
5. **`FNM-1` still applies** to the shortlist underneath.
6. **One model, one training window, no walk-forward.** §11's regime finding was made
   with 28 expanding folds; this is one split.

---

## 5. ⚠️ THE SINGLE-STOCK ANSWER, AND IT IS "NO TRADE"

Asked for a tradable VCB signal, the stage returns **zero trades in 33 periods** at §11's
measured band (enter 0.90 / exit 0.75), and that is a measurement rather than a bug:

| VCB's percentile among the 150, 33 rebalance dates | |
|---|---|
| median | **0.273** |
| max ever reached | **0.826** — never touches the 0.90 entry |
| periods at ≥0.90 / ≥0.75 / ≥0.50 | **0 / 1 / 3** |

Lowering the band until it trades produces noise, and never beats holding the stock:

| band | periods held | CAGR | Sharpe |
|---|---|---|---|
| 0.90 / 0.75 | **0** / 33 | — | — |
| 0.80 / 0.60 | 1 | −1.96 % | −0.65 |
| 0.60 / 0.40 | 4 | +2.17 % | +0.38 |
| 0.50 / 0.30 | 12 | −0.87 % | −0.04 |
| **VCB buy & hold** | 33 | **+0.77 %** | +0.14 |

⚠️ **The model was RIGHT about VCB and that is the point.** It ranked VCB in the bottom
third all through 2023-26, and VCB returned **+1.45 % CAGR** (58,240 → 60,500) while the
equal-weight universe made 5.96 % and VNINDEX 20.2 %. A correct "do not hold this" is not
a tradable buy/sell signal for that stock — it is portfolio advice about 150 names, which
is §11's conclusion reproduced with costs attached: *"the cross-sectional edge is a weak
per-name average that only becomes money spread across 20-40 names."*

⚠️ **Trading ONE name still requires scoring ALL 150 on every rebalance date.** The
percentile does not exist until every peer is scored. There is no cheaper form of this.

---

## 6. What the tests pin (14, `test_portfolio.py`)

Each names a way a backtest flatters itself:

- **overlapping windows** — `rebalance_dates` steps `h` sessions, verified on gaps
- **a cost charged once instead of twice** — enter pays ½, exit pays ½, sum is exactly 1
- **the τ identity** — 70 % of a book replaced costs `0.70 × rt`, and `50.4 × that` is
  the 17.6 %/yr in §3, asserted to 1e-4
- **a benchmark of zero** — `buy_and_hold` pays nothing ongoing and is always invested
- **a Sharpe with no `n`** — `se_sharpe` is `sqrt((1 + S²/2)/n)` and must exceed 0.15 at
  n=33
- **an empty track returning a flattering 0.0** — it returns NaN (§5 rule 2)
- hysteresis costs no more than a bare threshold; an inverted band raises

---

## 7. Known gaps

| gap | why it is not fixed here |
|---|---|
| **no walk-forward** — one train/val/test split | retraining per fold is a model-stage change, not a backtest one |
| **no slippage or ADV cap** | top-150 by turnover is liquid, but a 15-name book at size would move it. `pool__basic` carries `value_matched`, so this is buildable |
| **`h=5` / `h=10` unmeasured** | needs the selection and model stages re-run at those horizons — the cost table in §3 says what they must overcome |
| **no `mase` equivalent** | `result_evaluator` block B is absent on panels (`P4-12`); here the naive is Buy&Hold, which is the better benchmark anyway |
| **VN30F futures absent** | would make the relative column tradable. Not in this database — §2b |

---

## 8. ⚠️ THE "+5 % IN 5 DAYS" SCREEN — measured 2026-08-19

> ⚠️ **§8a-8e BELOW ASSUME 50 bps AND MODEL NO PRICE LIMIT. §8f SUPERSEDES THEM** with the
> real market rules (fee 0.2 %, T+3 settlement, HOSE 7 % / HNX 10 % / UPCOM 15 % bands),
> supplied 2026-08-19. The verdict changes at 5 days from "no" to "no, but 10 days yes".

Asked whether the market can be screened for names about to gain ≥5 % in 5 sessions.
Measured with these primitives on `unified_schema_all`, **top liquidity quintile**
(~137 names/date, `ntile(5)` on that date's `value_matched`, so point-in-time),
2018-01 → 2026-07, **421 non-overlapping periods** — an order of magnitude more sample
than §4's 32.

**The base rate first**, because a lift means nothing without it. Over all 781 tickers and
2.39 M rows: **P(`return_5day` ≥ +5 %) = 16.26 %**, P(≤ −5 %) = 14.46 %. By year it runs
22.78 % (2021) down to **11.65 % (2026)** — ⚠️ the target is getting rarer, so a model
trained on 2009-21 is miscalibrated for today. Flat across liquidity tiers (15.5 % → 16.7 %).

⚠️ **`return_5day` carries extreme outliers — sd 1.84 over the whole universe.** One row
in the liquid tier exceeds |100 %| in 5 sessions. Screened out rather than winsorised.

### 8a. Univariate probes — the signal is REAL

Decile of the channel, most-liquid tier, → forward P(≥+5 %):

| decile | `drv_order_vol_imb_5` | trailing 5-day return |
|---|---|---|
| 1 (lowest) | 13.78 % | 17.21 % |
| 10 (highest) | **20.13 %** | **23.23 %** |
| mean fwd return, d10 | **+0.79 %** | +0.63 % |

The order-flow column is **monotone across all ten deciles** — the shape a real signal
makes. ⚠️ And the trailing-return column is **momentum at the liquid end**, not the
reversal CLAUDE.md records at `t = −18.60` all-names: that reversal is a small-cap
phenomenon and it has already decayed to `t = −1.96` by the top 100.

### 8b. ⚠️ AND THE ONE-SESSION EXECUTION LAG DESTROYS THREE QUARTERS OF IT

Screen = mean within-date rank of (order-flow imbalance 5d, trailing 5d return, distance
from 63-day high). Top-10, held 5 sessions, 50 bps:

| variant | CAGR | Sharpe | `se` |
|---|---|---|---|
| same-close entry *(impossible)* | **+24.4 %** | **+0.880** | 0.057 |
| **t+1 entry (realistic)** | **+5.6 %** | **+0.333** | 0.050 |
| **equal-weight liquid tier, NO trading** | **+8.9 %** | **+0.466** | 0.051 |

⚠️ **The honest version LOSES TO DOING NOTHING.** The signal is built from day `t`'s
closing order counts, so it cannot be traded before `t+1`; that single day costs ~19 pp of
CAGR. **A signal that decays inside one session is not a screen, it is a latency trade.**

⚠️ **AND IT STILL CLEARS ITS NULL — which is why the null is not the test.** Within-date
shuffle, 200 draws: observed +0.333 against a null mean of **−0.383**, p95 bar −0.171,
MAX −0.056, **z = +5.55, CLEARS**. The null is a random 10-name basket paying the same
20.5 %/yr in fees, so clearing it only means "better than churning at random". **The
benchmark that decides is Buy&Hold, and it loses to that** — `experiment_10`'s finding
that not one of 23 papers reports a naive baseline, reproduced from the inside.

### 8c. Cost and regime — both reproduce §11 independently

| bps | 0 | 20 | 30 | **40** | 50 | 70 |
|---|---|---|---|---|---|---|
| CAGR | +29.7 % | +19.5 % | +14.7 % | **+10.0 %** | +5.6 % | −2.7 % |
| Sharpe | 0.99 | 0.73 | 0.60 | **0.465** | 0.33 | 0.07 |

⚠️ **It ties the market at exactly 40 bps and loses above it** — `model/CONTEXT.md` §11
said "dead at 40 bps" from a different study, a different feature set and a different
period. Two independent measurements, one threshold.

| t+1 entry, k=10, 50 bps | screen | market | |
|---|---|---|---|
| **2018-2021** (n=200) | +40.3 % / **1.24** | +27.0 % / 1.14 | ✅ marginal |
| **2022-2026** (n=222) | **−11.6 % / −0.25** | −3.8 % / 0.02 | ❌ |

§11 measured net@20bps **+1.46 (2017-20) vs −0.51 (2022-26)**. Same break, same sign,
different method.

### 8d. A longer hold does not rescue it

Same screen, t+1 entry, 50 bps, Sharpe (market in brackets):

| hold | k=10 | k=20 | fee drag |
|---|---|---|---|
| 5d | 0.300 (0.426) | 0.308 (0.426) | 20.6 % |
| **10d** | 0.394 (0.429) | **0.535** (0.429) | 9.6 % |
| 20d | 0.176 (0.446) | 0.376 (0.446) | 5.2 % |

The best cell is 10d/k=20 at **+0.11 Sharpe over the market with `se` ≈ 0.074 each** —
about one SE of the difference. **Not a result.** And in 2022-26 the screen is negative at
both 5d and 20d.

### 8e. What this settles, and what it does not

**Settled**: the ≥5 %/5d screen finds names **1.25× more likely** to make the move
(20.85 % against a 16.71 % base), that lift is real (z = +5.55), and it is **not enough to
pay for the trading** once you enter at a price you could actually get.

**Not settled**: this is a hand-built 3-channel rank, not a fitted model — a model over the
full `pool__basic` might lift 1.25× to something larger; the ceiling is set by the 40 bps
line and by the one-day decay, not by the ranker. ⚠️ **Survivorship cuts the wrong way
here** (§2c): a screen that buys recent winners is the strategy most flattered by a
universe with no delisted names, so the 2018-21 half is likelier overstated than the
2022-26 half is understated.


### 8f. ⚠️ UNDER THE REAL MARKET RULES — and the price band is the part nobody modelled

Supplied 2026-08-19: **fee 0.2 %**, **T+3 settlement**, **daily price bands HOSE 7 % /
HNX 10 % / UPCOM 15 %**. All three are now in the measurement.

**THE BAND IS THE FINDING.** A name at its ceiling has no sellers, so a backtest that buys
it buys a price that was never offered — and a momentum screen walks straight into them:

| at the ceiling on the entry day | |
|---|---|
| whole liquid tier | **3.37 %** (HNX 3.74 %, HOSE 3.52 %, UPCOM 0.68 %) |
| **the screen's own top-10 picks** | **7.12 %** |

⚠️ **The screen is 2.14× more likely than chance to pick a name it cannot buy**, and
excluding them is expensive — 5-day hold, k=10, 20 bps: **+19.3 % CAGR → +7.2 %**.
Any backtest of a momentum screen on VN that does not exclude ceiling days is fiction.

⚠️ **T+3 is NOT binding.** It floors the hold at 3 sessions, and 3 is the *worst* horizon
tested because turnover is highest. Longer is better across the range measured.

⚠️ **The fee has two readings and they do not agree.** "0.2 %" round trip is 20 bps;
0.2 %/side plus the mandatory **0.1 % sell tax** is 50 bps. The tax is not negotiable, so
the floor for any VN retail account is **30 bps**. All three are reported.

**Sharpe, buyable names only, t+1 entry, 2018-2026:**

| hold | k | 20 bps | 30 bps | 50 bps | market | `se` |
|---|---|---|---|---|---|---|
| 3d | 10 | 0.495 | 0.277 | −0.157 | 0.243 | 0.038 |
| 5d | 20 | 0.477 | 0.337 | 0.057 | 0.310 | 0.050 |
| **10d** | **20** | **0.722** | **0.652** | **0.512** | 0.404 | 0.077 |

⚠️ **THE USER'S STATED HORIZON IS THE ONE THAT DOES NOT WORK.** At 5 days and 30 bps the
screen ties the market; at 50 bps it loses. **At 10 days it beats the market at every cost
level tested**, and that is the cell to carry forward.

**The 10-day cell against its own null** (within-date shuffle, 200 draws, k=20, 211 periods):

| bps | observed | null mean | p95 bar | null MAX | **z** | vs market |
|---|---|---|---|---|---|---|
| 20 | **+0.722** ± 0.077 · CAGR +17.1 % | +0.290 | +0.448 | +0.532 | **+4.65** | ✅ beats 0.404 |
| 30 | +0.652 ± 0.076 · CAGR +14.9 % | +0.213 | +0.375 | +0.454 | **+4.72** | ✅ |
| 50 | +0.512 ± 0.073 · CAGR +10.7 % | +0.060 | +0.219 | +0.297 | **+4.87** | ✅ |

Null MAX below observed in all three, so §5 rule 3 does not fire. `k` is flat from 5 to 60
(0.57–0.65 at 30 bps) — not a knife-edge.

### 8g. ⚠️ AND THE EDGE IS STILL ALMOST ENTIRELY PRE-2022

| hold 10d, k=20 | screen | market | |
|---|---|---|---|
| **2018-2021** (n=100), 30 bps | +47.9 % / **+1.671** ± 0.155 | +24.0 % / +0.995 | ✅ |
| **2022-2026** (n=112), 30 bps | **−3.5 % / +0.011** ± 0.094 | −4.8 % / −0.049 | ⚠️ tie |
| 2022-2026, 50 bps | −7.2 % / −0.131 | −4.8 % / −0.049 | ❌ |

⚠️ **In the recent regime the screen is INDISTINGUISHABLE FROM THE MARKET, and both are
flat.** +0.011 against −0.049 is a gap of 0.06 with an SE of difference ~0.13. The full-
period Sharpe of 0.652 is an average over a regime that worked and one that does not.

⚠️ **Max drawdown is −55 % to −58 % at every `k`.** A concentrated long-only VN book
through 2022. Statistically tradable is not the same as holdable.

**The rule, stated so it can be implemented or refuted:** universe = top liquidity
quintile by that date's `value_matched`; **drop any name at its exchange's ceiling**;
score = mean within-date percentile rank of `drv_order_vol_imb_5`, trailing 5-day return,
`drv_dist_from_high_63`; signal from the close of `t`, buy at the close of `t+1`;
equal-weight the top 20; hold 10 sessions.

⚠️ It is a hand-built 3-channel rank, **not a fitted model** — the ceiling on what a model
could add is untested, and `NUL-1` applies to the choice of channels, horizon and `k`.

### 8h. ✅ AND THE BAND DOES **NOT** BITE THE h=20 MODEL — checked 2026-08-19

§8f's 2.14× ceiling bias made §4's headline suspect, since that run applied no exclusion.
Re-measured on `lstm__all__rank_20day__final__d20_h20`:

| | universe at ceiling | model's top-15 | ratio | as reported | **buyable only** |
|---|---|---|---|---|---|
| val | 3.76 % | 4.95 % | 1.32× | +1.7367 | **+1.7385** |
| test | 1.83 % | 2.46 % | 1.34× | +1.4845 | **+1.5512** |

⚠️ **The bias is real but small, and removing it IMPROVES the number** (test +1.484 →
+1.551), so §4 stands as written and is if anything conservative. Two reasons, and both
are the same point: a 20-day model is not chasing one-day spikes the way a 5-day momentum
rank is (1.33× against 2.14×), and at k=15 of 150 a ~2 % ceiling rate touches ~0.4 names
per rebalance. **The price band is a 5-day problem, not a 20-day one** — one more instance
of the horizon being the variable that decides.

⚠️ **The exclusion is still applied by a PROBE and not by the stage.** `build_panel` does
not carry `exchange`, so `long_only_top_k` cannot see a ceiling. TODO **PRF-0**'s
remainder: fold it in so it is the default rather than something a reader must remember.
