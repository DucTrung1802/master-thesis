# Experiments Summary — Breakout detection & the "5-day +5%" signal

Single-document summary of **all methods and all results** across `experiment_1`
(signal discovery) and `experiment_2` (windowed-input model study).

## Common setup

- **Target (everywhere):** `y[t] = 1 if close[t+5]/close[t] - 1 >= 0.05` — i.e. the
  next 5 trading days rise ≥ 5%. Binary classification.
- **Evaluation:** chronological (no look-ahead) splits; metric = test **ROC-AUC**
  (+ PR-AUC, top-decile precision, lift). Train-only standardization / feature
  selection where applicable.
- **Data (PostgreSQL `database_main_v2`):**
  - `unified_schema.unified_<ticker>` — 30 VN30 tables, ~1053 features each (TA + macro + calendar).
  - `gold_schema.stocks` — 621-ticker panel, 910 TA features (VN100 universe).
  - `gold_schema.indices`, `economy_*`, `bonds_*` — index state & macro context.

---

# Experiment 1 — Signal discovery

### 1.1 Breakout event catalogue (VCB) — `breakout_events/detect_breakout_events.py`
Swing-high apex catalogue (apex = highest close within ±5 days), filtered by gain
threshold → monotonic event sets. Window = `[peak−N−2, peak+2]`; predictable/decision
day = `peak−N`.

| Filter | Events |
|---|---|
| gain10d ≥ 15% | 17 |
| gain10d ≥ 10% | 41 |
| gain10d ≥ 5% | 113 |
| gain5d ≥ 5% | 98 |

VCB's Jan-2026 move is the all-time record: **+33% in 10 days** (57,100 → 76,000).

### 1.2 Univariate signal search (VCB) — `breakout_events/signal_search_5d5pct.py`
Base rate **11.5%**. Strongest single features (ROC-AUC): `natr_14` / `atr_normalized`
0.64, `volatility_21` 0.63, `close_bb_20_bandwidth` 0.61, `ppo_12_26_9` 0.60.
Joint model (GBM, all features, chrono 80/20): **AUC 0.762**, top-decile precision
16.7% (3.1× lift). → The signal is a **volatility/momentum regime**.

### 1.3 Multi-period TA sweep (VCB) — `breakout_events/ta_period_sweep_vcb.py`
Tuning the indicator period barely helps; univariate AUC saturates ≈ **0.63–0.65**.

| Family | Best period | AUC |
|---|---|---|
| NATR / ATR | 7 (≈14) | 0.646 |
| realized vol | 20 | 0.631 |
| Bollinger bandwidth | 40 | 0.629 |
| ROC / RSI | 7–20 | 0.54 |

### 1.4 Best period per family from `unified_vcb` — `breakout_events/vcb_best_period_per_family.py`
560 period-bearing features, 80 families. Best stored periods:

| Family | Period | AUC | | Family | Period | AUC |
|---|---|---|---|---|---|---|
| ATR / NATR | 14 | **0.643** | | PPO | 12_26_9 | 0.604 |
| realized vol | 21 | 0.629 | | ROC | 10 | 0.601 |
| TRIX | 15 | 0.609 | | RSI / CMO | 14 | 0.575 |
| Bollinger bandwidth | 20 | 0.608 | | ADX | 14 | 0.575 |

Price-level MA families (`close_dema/ema/sma_100`…) score ~0.40 `low→up` — a
non-stationarity **artifact**, not signal.

### 1.5 VN30 per-ticker + pooled — `vn30_signal/vn30_signal_5d5pct.py`
Per-ticker GBM (chrono 80/20). Predictability varies widely:

| Tier | Tickers (test AUC) |
|---|---|
| Strong | **VCB 0.767**, BCM 0.717, FPT 0.647, VPB 0.643, BVH 0.629, MBB 0.626, ACB 0.620 |
| Weak | HDB 0.413, TPB 0.409, VRE 0.408 |

**Pooled VN30** (90,861 stock-days): general signal AUC **0.653**, top-decile 21.9%
(1.9× lift). Top general features again volatility + momentum.

### 1.6 DL shoot-out on VCB alone — `dl_signal/dl_model_comparison_vcb.py`
| Model | test AUC | | Model | test AUC |
|---|---|---|---|---|
| **GBM-full** | **0.770** | | CNN1D | 0.456 |
| LSTM | 0.558 | | MLP | 0.445 |
| Transformer | 0.482 | | GRU | 0.430 |

→ On a single stock (~2.9k rows) deep learning loses heavily to gradient boosting.

### 1.7 Pooled VN100 DL — `dl_signal/dl_vn100_pooled.py`
VN100 (95/100 tickers in `gold.stocks`), **266,848 stock-days**:

| Model | test AUC | | Model | test AUC |
|---|---|---|---|---|
| **GBM-full** | **0.625** | | GRU | 0.596 |
| MLP | 0.615 | | LSTM | 0.594 |
| CNN1D / Ensemble | 0.609 | | Transformer | 0.581 |

60× more data closed the DL gap (−0.21 → −0.01) but did **not** overtake GBM. Plain
MLP ≈ GBM > sequence nets → signal is point-in-time, not temporal.

### 1.8 VN100 + macro / cross-sectional / index features — `dl_signal/dl_vn100_xsec_macro.py`
Added `economy_*`/`bonds_*`, VN100/VNINDEX state, and per-date cross-sectional rank:

| Model | base | + context |
|---|---|---|
| GBM-full | 0.625 | 0.619 |
| GRU | 0.596 | **0.628** |

No robust gain (within seed noise). Useful new features: **index volatility** and
**cross-sectional volatility rank** — reinforcing the volatility-regime story.

### 1.9 VCB feature importance & trading meaning — `breakout_events/vcb_importance_and_trading.py`
**Which features:** the 0.77 is *not* reducible — leak-free top-K AUC: top-50 → 0.650,
top-300 → 0.697, **all 1053 → 0.762**. Skill comes from aggregating hundreds of weak features.
**Input/output:** X = 2-D matrix `(n_days, 1053)`; output = scalar `P(next-5d ≥ +5%)`.
**Trading meaning** (test period, actual forward 5d returns):

| Day group | mean fwd-5d | win-rate | ≥+5% rate |
|---|---|---|---|
| all days | +0.20% | 45% | 5% |
| **signal top 10%** | **+2.16%** | 62% | 17% |
| signal top 20% | +1.76% | 56% | 14% |

Real ranking edge, but not a precise timer (top-decile hits +5% only 17% of the time).

---

# Experiment 2 — Windowed input (1 sample = W-day × ~1053 matrix → scalar)

### 2.1 VCB, 20-day matrix, all models — `experiment_2/vcb_seq20x1053_models.py`
| Model | input/sample | test AUC |
|---|---|---|
| **GBM (last-day, ref)** | (1053,) | **0.770** |
| GRU | (20, 1053) | 0.695 |
| Ensemble | (20, 1053) | 0.659 |
| LSTM | (20, 1053) | 0.653 |
| CNN1D | (20, 1053) | 0.636 |
| MLP (flatten) | (21060,) | 0.609 |
| GBM (flatten) | (21060,) | 0.551 |
| Transformer | (20, 1053) | 0.540 |

The 20-day matrix does **not** beat the point-in-time GBM; flattening hurts most.

### 2.2 Other stocks (VNM, VIC) — same script, `vnm/vic_seq20x1053_results.csv`
Best model is **stock-specific**:

| Stock | Best model | Best AUC | Last-day GBM | History helps? |
|---|---|---|---|---|
| VCB | GBM (last-day) | 0.770 | 0.770 | No |
| VNM | GBM (last-day) | 0.581 | 0.581 | No (barely predictable) |
| **VIC** | **GRU (20-day)** | **0.694** | 0.509 | **Yes** (+0.18 AUC; top-decile prec 47%) |

### 2.3 VCB lookback sweep (1→20 days) — `experiment_2/vcb_lookback_sweep.py`
Best model & AUC per lookback (`vcb_lookback_auc.csv` has the full grid):

| Lookback | 1 | 2 | 3 | 5 | 8 | 10 | 12 | 15 | 18 | 20 |
|---|---|---|---|---|---|---|---|---|---|---|
| **best AUC** | 0.756 | 0.743 | 0.757 | 0.755 | 0.746 | 0.717 | 0.686 | 0.665 | 0.673 | 0.688 |
| **best model** | GBM | GBM | GBM | GRU | GRU | GBM | LSTM | GRU | GRU | GRU |

Short lookback wins. GBM degrades steadily with W (0.756 → **0.548** at W20); GRU is
the best DL model, peaking at W = 5–8 (~0.75) but never beating short-window GBM.

---

# Overall conclusions

1. **One universal signal:** a near-term 5d+5% up-move is preceded by **volatility /
   range expansion + momentum strength** — confirmed at stock, VN30, and VN100 level,
   and at three scales (own volatility, peer-relative rank, index volatility).
2. **Gradient boosting on full point-in-time features is the model to beat.** Deep
   learning only becomes competitive with the large pooled panel, and never clearly
   wins; the plain MLP ≈ GBM, so the signal is in current feature *values*, not the
   temporal trajectory.
3. **Predictability & best model are stock-specific:** VCB → point-in-time (AUC ~0.77),
   VIC → sequence models over a 20-day window (~0.69), VNM → essentially unpredictable.
4. **Lookback:** short is best for VCB; longer windows add noise (and cripple GBM).
5. **Ceiling:** ≈ **0.76** single-stock (VCB), ≈ **0.62–0.65** pooled — robust to
   richer features and deeper models. The remaining lever is the **target definition**
   (continuous / vol-scaled forward return), not the architecture.
6. **Trading:** a real ranking edge (top-decile days average +2.16% fwd-5d vs +0.20%),
   usable as a regime/timing/sizing filter — but not yet a costed, walk-forward backtest.

> All AUCs are single chronological-split point estimates (small positive counts →
> ±0.03–0.05 variance). CSV outputs are gitignored and regenerated by the scripts.

## File index
- `experiment_1/README.md` — experiment 1 detail
- `experiment_1/breakout_events/` — events, signal search, TA sweeps, importance/trading
- `experiment_1/vn30_signal/` — VN30 per-ticker + pooled
- `experiment_1/dl_signal/` — DL shoot-outs (VCB, VN100, VN100+context)
- `experiment_2/vcb_seq20x1053_models.py` — windowed model zoo (VCB/VNM/VIC)
- `experiment_2/vcb_lookback_sweep.py` — VCB lookback sweep
