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

### 8i. ✅ AND IT IS THE DEFAULT NOW — shipped 2026-08-19, `PRF-0`'s remainder

`build_panel` joins `exchange` from `pool__basic`, computes the flag, and returns a panel
with the ceiling rows already dropped. The stage prints the count on every run, because
**an exclusion nobody can see in the output is one a later reader cannot tell was applied**
— which is precisely why §8f and §8h each had to re-measure it from scratch.

| 50 bps, top-15 | as first reported | screened (the default) | dropped |
|---|---|---|---|
| test | +1.4845 | **+1.5512** | 1,708 rows, 1.83 % |
| val | +1.7367 | **+1.7385** | 3,437 rows, 3.76 % |

⚠️ **Both reproduce §8h's probe to four decimals**, which is how the change was verified
rather than asserted.

⚠️ **ONE RULE, ONE PLACE.** `BANDS` + `mark_ceiling` + `drop_ceiling` live in
`backtest.portfolio`; `walkforward.evaluate` and `walkforward.compare` import them instead
of carrying a second copy, the way `ROUND_TRIP_COST` is shared. Two implementations of a
rule that decides whether a trade was executable is a defect, not a study.

⚠️ **`drop_ceiling` RAISES on a panel with no `at_ceiling` column** rather than passing it
through. A backtest that silently skips the screen reports a number the market would not
have given you, **and its output is identical to one that applied it** — so a panel that
cannot say what was buyable refuses to be traded.

⚠️ **NaN → False, and that is right for an ENTRY screen only.** An unknown exchange or a
missing daily return keeps the row. The SELL side needs the opposite default — a name at
its FLOOR on the exit date cannot be sold, and a loser is exactly when that happens — which
is why `PRF-4` lists floor days as a separate unbuilt item rather than something
`mark_ceiling` quietly half-covers.

---

## 9. ⚠️ PRF-2 — THE CHAIN AT h=10, AND THE MODEL BEATS THREE RANKED COLUMNS BY 2.7 SHARPE

Run 2026-08-19. §8g measured a hand-built 3-channel rank at h=10 and §4 a fitted LSTM at
h=20; **nobody had put the two on one panel**, so *how much a fitted model adds over three
ranked columns* was unknown at every horizon. `backtest.handscreen` closes that by scoring
the hand rule as a `y_pred` the normal machinery prices — same dates, same universe, same
costs, same `k`, same ceiling screen.

**Test window 2023-11-28 → 2026-07-24, 63 non-overlapping periods, top-20 of 150, buyable
only:**

| h=10, same panel | CAGR@30 | **Sharpe@30** | `se_sharpe` | max_dd | null z @30 |
|---|---|---|---|---|---|
| **the model** | **+43.8 %** | **+2.442** | 0.251 | −7.2 % | **+8.99** ✅ |
| the 3-channel hand rule | −5.1 % | **−0.263** | 0.128 | −21.8 % | **−1.72** ❌ |
| equal-weight universe | +4.0 % | +0.329 | 0.126 | −18.2 % | — |

**Paired** (they trade the same dates, ρ = 0.74): ΔSharpe **+2.71**, `t` = **+5.94** at
30 bps, +5.87/+6.08 at 20/50. The model's own run: test IC **+0.1393**, `ic_t` +8.19,
**85.8 % of days positive**, `mase` **0.9874** (beats "predict no change"), R² +0.011.

### 9a. ⚠️ The hand rule scoring −0.26 is NOT a contradiction of §8g

§8g's **+0.652** is over **2018-2026** on the top liquidity quintile of the whole market.
This window is 2023-11 onward — squarely inside the regime §8g itself measured at
**+0.011 (2022-2026)** and −0.131 at 50 bps. **So the hand rule is doing here exactly what
§8g said it does after 2022: nothing.** Two further differences, both stated so the number
is not over-read: the universe is the model's fixed 150 rather than a dynamic quintile
(deliberate — otherwise this compares two UNIVERSES), and both sides trade same-close
rather than t+1 (deliberate — otherwise the model gets a free session).

### 9b. ⚠️ AND THAT ANSWERS `PRF-3`: THE BREAK IS IN THE FEATURES, NOT THE HORIZON

`PRF-3` had two hypotheses for the post-2022 collapse. **(1) the market changed**, so no
feature set works after 2022; **(2) the FEATURES decayed** — order-flow imbalance from daily
order counts is a crowded signal by 2022 — so a different feature set still works.

PRF-2 holds the feature pipeline fixed and moves only the horizon, and this table holds the
horizon fixed and moves only the feature set. **On the same window, the same universe and
the same `h=10`, a selected 19-channel model returns +2.44 while the 3 hand-picked channels
return −0.26.** That is hypothesis **(2)**: the market did not stop being predictable after
2022, those three columns stopped predicting it.

⚠️ **What it does not license.** The 19 channels were selected on the whole sample
(`PRF-7` bounds that bias as mild but does not remove it), the hand rule was never re-fitted
for this window, and `NUL-1` applies to both. The claim is about THESE two feature sets on
THIS window, not about hand rules in general.

### 9c. ⚠️ h=10 beats h=20 *despite* paying double the fees, which was not the expectation

§3's cost identity: at turnover 0.70 and 50 bps the annual drag is **4.4 % at h=20 and
8.8 % at h=10**. So h=10 has to earn ~4.4 pp/yr more just to break even — and it does, by
far more than that. Same universe, same architecture, same test window, top-20:

| | Sharpe@30, test | selection z | `n_eff`/fold |
|---|---|---|---|
| h=20 | +1.441 | +9.09 | 38.1 |
| **h=10** | **+2.442** | **+13.78** | **76.6** |

⚠️ **Do not read this as "h=10 is the better horizon" yet.** It is ONE split at each
horizon, `se_sharpe` 0.24-0.25, and the h=20 figure has a 10-fold walk-forward behind it
(`walkforward/CONTEXT.md`, pooled +1.991 over 118 periods) while this one does not. **The
walk-forward at h=10 is the run that would settle it.** What is already solid is the
comparison inside this table — model vs hand, paired, on one panel.

---

## 10. ⚠️ THE INDICATOR SURVEY — 29 channels, 3 horizons, 2 grains, and only 3 survive

Measured 2026-08-20. Data and both scripts are in `reports/indicator_survey_*` so the
numbers below can be recomputed rather than trusted. Universe: **top liquidity quintile by
that date's own `value_matched`** (point-in-time, ~114 names/date), 480,457 rows,
2009-2026, `|return_h| > 100 %` screened to NaN rather than winsorised.

**Each channel measured TWICE, and the pair is the finding**: Spearman *within each date*
across the liquid names (can I rank a GROUP today?) and Spearman *within each ticker* down
its own history (does this predict ONE stock's future?). Averaging the t-statistic over
h=5/10/20 splits them into three families that do not transfer.

| family | channels | group `t` | one-stock `t` |
|---|---|---|---|
| **A · works at BOTH** | `drv_order_vol_imb` | +9.2 | +14.4 |
| | `drv_log_order_size_ratio` | +7.8 | +13.0 |
| | `drv_order_vol_imb_5` | +5.8 | +6.8 |
| **B · GROUP only, sign FLIPS** | `drv_dist_from_high_252` | **+5.0** | **−3.0** |
| | `drv_close_pos_252` | +4.8 | −1.0 |
| | `drv_foreign_participation` | +2.7 | −2.1 |
| **C · one-stock only, SUSPECT** | `drv_amihud_63` | −2.5 | +12.2 |
| | `avg_vol_per_sell_order` | −0.9 | +8.5 |

⚠️ **FAMILY B IS THE PRACTICAL RESULT AND IT IS THE ONE A TRADER GETS BACKWARDS.**
*"Buy this stock because it is near its own 252-day high"* measures **t = −3.0**. *"Buy the
stocks nearest their highs relative to every other name today"* measures **t = +5.0**.
Distance-from-high is a **relative** signal, not an absolute timing rule, and the same
number points opposite ways depending on what it is compared against.

⚠️ **FAMILY C IS NOT REPORTED AS A RESULT.** The per-ticker `t` measures CONSISTENCY ACROSS
NAMES, not significance within a series — it prices no autocorrelation. Both leaders are
**trending series**, and correlating a trend with a trending price is the classic spurious
regression; the cross-sectional column is immune because ranking within a date removes any
common trend. CLAUDE.md §6-1 agrees from the other direction: single-stock selection at
h=10 on these same channels did **not** clear its null.

### 10a. ⚠️ FILTER ONE — the session you cannot trade costs the best channel two thirds of itself

Every channel is built from day `t`'s closing order counts, so the earliest entry is the
close of `t+1`. Re-measuring against the return earned FROM `t+1` reorders the table
(h=10, cross-sectional `t`):

| channel | same close | t+1 | kept | reading |
|---|---|---|---|---|
| `drv_order_vol_imb` | +8.82 | +3.07 | **35 %** | fast — mostly a same-session effect |
| `drv_order_vol_imb_5` | +5.80 | +3.60 | 62 % | fast |
| **`drv_log_order_size_ratio`** | +8.05 | **+5.97** | 75 % | the slow half of the order-flow family |
| `drv_dist_from_high_252` | +5.08 | **+4.96** | 97 % | slow — the lag costs nothing |
| `drv_rogers_satchell_21` | −4.12 | −4.16 | 101 % | slow · low-volatility premium |
| `drv_order_count_imb_5` | −3.43 | −4.14 | 120 % | strengthens with the lag |
| `drv_foreign_flow_ratio_21` | −0.19 | +0.03 | — | nothing, at either lag |

⚠️ **This reproduces §8b's portfolio-level finding channel by channel and NAMES THE CULPRIT**:
`drv_order_vol_imb`, the single most important input to every working model in this repo,
is largely a **latency trade**. The channels a daily trader can actually hold are the SLOW
ones, precisely because a one-day lag costs them nothing.

⚠️ **`drv_foreign_flow_ratio_21` measures ~ZERO at either lag**, which CONTRADICTS
`model/CONTEXT.md` §11's *"foreign flow is the one signal that survives"*. The two measure
different things — §11 used `gold_schema.stocks.foreign_net_value` through a classifier,
this is `pool__basic`'s ratio univariately — and **which is right is unresolved**. Do not
quote either without the other.

### 10b. ⚠️ FILTER TWO — 2022 removes the other half, and it names WHICH half

h=10, t+1 entry, cross-sectional `t`, split at 2022-01-01:

| channel | 2018-2021 | 2022-2026 | |
|---|---|---|---|
| `drv_log_order_size_ratio` | +4.38 | **+3.19** | ✅ survives |
| `drv_order_count_imb_5` | −2.91 | **−2.97** | ✅ survives |
| `drv_rogers_satchell_21` | −1.24 | **−2.39** | ✅ strengthens |
| `drv_order_vol_imb` | +2.71 | +1.37 | ⚠️ halved |
| `drv_dist_from_high_252` | +2.45 | **+0.62** | ❌ gone |
| `drv_close_pos_252` | +2.73 | **+0.46** | ❌ gone |
| `drv_dist_from_high_63` | +2.09 | +0.72 | ❌ gone |

⚠️ **§9b established that the post-2022 break lives in the FEATURES rather than the market;
this says WHICH features.** The entire position-within-range family went from `t ≈ +2.5` to
`t ≈ +0.5`. Order SIZE and order COUNT imbalance did not.

### 10c. The three that pass all three filters

| channel | measures | h=5 | h=10 | h=20 | sign |
|---|---|---|---|---|---|
| `drv_log_order_size_ratio` | log(mean buy-order size ÷ sell-order size) | +3.16 | **+3.19** | +2.41 | buy LARGER tickets |
| `drv_order_count_imb_5` | 5-day imbalance of buy vs sell order COUNTS | — | **−2.97** | −2.78 | fade the crowd |
| `drv_rogers_satchell_21` | 21-day Rogers-Satchell range volatility | −2.34 | **−2.39** | −2.13 | buy the CALM |

⚠️ **THE TOP PAIR IS ONE ECONOMIC IDEA WITH ITS HALVES DELIBERATELY OPPOSED.** Order SIZE
up predicts positively; order COUNT up predicts negatively — the institutional-versus-retail
read of a tape, and the first time this repo has separated the two. They were always bundled
inside one "order flow" family, and the pair survives 2022 while raw volume imbalance does not.

⚠️ **NONE OF THIS IS A STRATEGY.** An IC of 0.04 at `t ≈ 3` is a RANKING edge over ~114
names, not a trade, and it must clear §3's turnover: **8.8 %/yr at h=10, 17.6 % at h=5**
against a benchmark returning 9.75 %. Every number here is univariate, unpenalised for the
29 channels searched, and has no walk-forward behind it — unlike the fitted chain, which does.

### 10d. ⚠️ The gap map — every h=10 and h=20 test in this repo has run on ONE pool

| pool | channels | tested at | |
|---|---|---|---|
| `pool__basic` | 90-101 | h=5, 10, 20 · every target | ✅ the only one |
| `pool__ta` | 711 numeric | one run each at h=5, 10, 20 | ⚠️ barely |
| 19 × `pool__economy_*` | ~1,400 | price-LEVEL target only, **0 null draws** | ❌ |
| 47 × `pool__forex_*` | 3,129 series | — | ❌ **never once** |
| `pool__fa` | — | h=5 only, 1 run | ❌ — and see below |
| `pool__bonds` / `stock_market` / `news_daily` / `market_breadth` | — | h=5 only, 1 run each | ❌ |

⚠️ **So *"nothing else predicts"* has never been shown. What has been shown is that nothing
else has been ASKED the right question** — everything outside `pool__basic` was tested at
h=5, the one horizon §3's fee arithmetic already rules out, or against a price-level target
with no null.

⚠️ **Two structural exceptions bound how much of that gap is fillable.** A date-only series
(macro, forex, bond yields) has a **constant within-date rank** and therefore cannot rank a
cross-section at all — valid for one stock, dead for a group. And **`pool__fa` holds 2
tickers on `unified_schema_all`** (VCB and ACB, 8,265 rows), so fundamentals are not
available at panel grain in this database.
