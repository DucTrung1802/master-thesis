# Experiment 3 — Does the signal actually trade? (walk-forward backtests + target search)

> **New-session context file.** Read this to pick up the full thread without the
> prior chat. It summarises *why* experiment_3 exists, what each script does, the
> headline results, and the open next steps. Sibling context: `../CONTEXT.md`
> (index across all experiments) and `../experiment_1/README.md`.

## Where this sits in the story

- **experiment_1** discovered a signal for "next 5 trading days up ≥ 5%": a
  volatility/momentum regime. Best single-stock model = **gradient boosting on the
  full `unified_vcb` feature set, AUC ≈ 0.77** (point-in-time; DL never beat it).
- **experiment_2** tested windowed (20-day × 1053) inputs: best model is
  stock-specific (VCB → point-in-time, VIC → sequence, VNM → unpredictable); short
  lookback wins for VCB.
- **experiment_3 (here)** asks the real question: **does AUC 0.77 mean money?**
  Answer, after costed walk-forward backtests: **no — it's a volatility detector,
  not tradable alpha.** And then: **what target *would* be tradable, and what data
  is missing?**

## Common setup

- DB `database_main_v2`; features from `unified_schema.unified_<ticker>` (VN30,
  ~1053 cols) and `gold_schema.stocks` (621-ticker panel, 910 cols, used for VN30
  pooling). `.env` at repo root holds `POSTGRES_*`. CSV/PNG outputs are produced by
  the scripts (CSVs gitignored, PNGs tracked).
- Label family: forward 5-day (and 10-day) returns; chronological / walk-forward,
  purged, no look-ahead. Costs charged per side (base 15 bps).
- **VN constraint that matters: single-stock short selling is effectively
  unavailable on HOSE → long-short is academic; only long-only is real.**

## Scripts & what they showed

### `vcb_walkforward_backtest.py` — single-stock VCB timing
Walk-forward (expanding, retrain/126d, 28 folds, OOS 2012–2026). Top-decile signal
(causal trailing-252d 90th pct), long/flat 5-day hold, vs Buy&Hold and a 20-day
momentum rule. **Result @15bps:** ML Sharpe **0.67** ≈ Buy&Hold **0.66** (CAGR
10.8% vs 15.9%), momentum 0.59. → timing one trending stock ties just holding it;
**no alpha.** Fig: `figures/A_equity_curve.png`.

### `vn30_xsec_longshort.py` — cross-sectional VN30 long-short
Pooled walk-forward (yearly), rank VN30 by predicted P(5d≥+5%), long top-6 / short
bottom-6, net 15bps. **Result: −12% CAGR, Sharpe −0.53, −88% DD** vs market +16.4%
/ 0.85. → the signal ranks stocks by **volatility**; longing "most likely to jump"
(high-vol) and shorting calm names **loses** (low-vol anomaly in reverse). Reversed
bet ≈ +0.53 but still < market and shorting isn't allowed. Fig:
`figures/B_longshort_equity.png`.

### `target_comparison.py` — which short-horizon TARGET is tradable?
Pooled VN30 walk-forward; 6 targets scored by rank-IC and a **long-only top-6**
portfolio vs the equal-weight market. Targets: `ret5/ret10` (raw fwd return),
`rel5/rel10` (market-RELATIVE = fwd return − cross-sectional mean), `volscaled5`
(Sharpe-like), `bin5` (the old 5d≥+5%). **Result (`target_comparison_results.csv`):**

| target | rank-IC | top6 Sharpe | excess vs market |
|---|---|---|---|
| rel5 (market-relative 5d) | **0.052 (best IC)** | 0.25 | −0.58 |
| rel10 | 0.050 | 0.18 | −0.65 |
| bin5 | 0.044 | 0.64 | −0.19 (least bad) |
| ret5 | 0.044 | 0.50 | −0.33 |

→ **`rel5` is the most *predictable* and conceptually correct (beta-neutral) target,
but NO target's long-only portfolio beats the market net of costs.** IC ≈ 0.05 is
near the noise floor — too weak for 30 names, long-only, one signal. **The binding
constraint is the DATA, not the target or model.**

## Conclusions carried forward

1. AUC 0.77 is real ranking skill but a **volatility-regime detector, not tradable
   alpha.** Neither single-stock timing nor cross-sectional selection beats simple
   benchmarks after costs.
2. **Best target to pursue: `rel5`** — the market-relative ~1-week forward return
   (highest IC, beta-neutral, long-only implementable). But it is **not yet
   tradable on current price/TA data.**
3. **More tuning won't help** — the problem is signal *content*, not model quality.

## Open next steps (suggested)

- **Add orthogonal data** (the real lever), priority for VN short-term:
  1) **foreign net flows** per stock/day, 2) **earnings calendar + surprises** and
  **analyst EPS revisions**, 3) **fundamentals/valuation** (P/E, P/B, ROE, growth),
  then sector, liquidity/float, news-sentiment, index-reconstitution dates.
- **Widen the universe** (VN100 / VNAllShare) for cross-sectional breadth; rerun
  `target_comparison.py` on VN100 to see if breadth lifts excess-Sharpe positive.
- **Stack multiple uncorrelated signals** rather than one; keep `rel5` as the label
  and evaluate by rank-IC + long-only top-quintile net of costs.
- Reproduce: run the three scripts above (need repo-root `.env`).
