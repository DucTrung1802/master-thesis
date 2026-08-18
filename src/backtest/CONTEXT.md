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
