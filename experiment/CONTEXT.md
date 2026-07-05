# Experiment context — read this first

Orientation doc for a fresh AI session. Covers **all methods and results** across
`experiment_1` (signal discovery), `experiment_2` (windowed-input study) and
`experiment_3` (walk-forward backtests), **plus the current database state** — which
has changed since most of these scripts were written, so read the next two sections
before running anything.

---

## ⚠️ Current database state (changed — many scripts are stale)

The database was **restructured**. `dbname="database_main_v2"` still exists, but the
flat per-ticker tables the older scripts read are **gone**.

**Schemas now in `database_main_v2`:**
- `bronze_schema` — raw scrapes (`cafef_stocks`, `simplize_stocks`, `trading_view_*`, `gics`, …)
- `silver_schema` / `gold_schema` — cleaned + feature-engineered layers.
  - `gold_schema.stocks` — **777-ticker panel**, all ~905 TA features + OHLC/volume/foreign, 2000→2026. **This table still exists and works.**
  - `gold_schema.economy` / `indices` / `bonds` / `forex` / `funds` — long-format context.
- `unified_schema_vcb` — **VCB only**, 140 tables. Feature *pools* keyed on `date`:
  - `pool__basic` (30: OHLC/volume/foreign/sector), `pool__calendar` (19), `pool__macro` (149), `pool__ta` (905), `pool__targets` (5: `return_5day`, `return_rel_5day`, `direction_5day`, `probability_gain_5pct_5day` — **forward-looking labels, never use as features**).
  - Plus per-target × per-lookback tables `{target}__lb{N}__{group}__{count}` and `__final`.
  - `pool__macro` / `pool__calendar` are **ticker-independent** (date-keyed) → reusable for any stock.

**The old flat tables `unified_schema.unified_<ticker>` (~1053 features) NO LONGER EXIST.**
The `.env` also now points `BRONZE/SILVER/GOLD_POSTGRES_DATABASE` at DBs `bronze/silver/gold`
that are **not created yet** — the live data is all inside `database_main_v2`.

### Script status against the live DB
| Reads… | Status | Scripts |
|---|---|---|
| `unified_schema_vcb.pool__*` | ✅ current | `breakout_events/vcb_gbm_auc.py`, `vn30_signal/vn30_gbm_auc.py` |
| `gold_schema.stocks` | ✅ still runs | `dl_signal/dl_vn100_pooled.py`, `dl_signal/dl_vn100_xsec_macro.py`, `experiment_3/vn30_xsec_longshort.py` |
| `unified_schema.unified_<ticker>` | ❌ **broken** (schema deleted) | all `breakout_events/*` except `vcb_gbm_auc.py`; `vn30_signal/vn30_signal_5d5pct.py`; all `experiment_2/*`; `experiment_3/vcb_walkforward_backtest.py`, `target_comparison.py`; `dl_signal/dl_model_comparison_vcb.py` |

To migrate a broken script: rebuild the feature matrix by joining
`unified_schema_vcb.pool__calendar + pool__macro + pool__ta` on `date` (and pull `close`
from `pool__basic` for the label) — see `vcb_gbm_auc.py` for the pattern. Historical
result numbers below were computed on the old flat schema; they are indicative but not
reproducible until the script is migrated.

---

## ⚠️ Headline caveat: VCB's ~0.77 AUC is a lucky single-stock outlier

The most-cited result — VCB test ROC-AUC ~0.77–0.78 for the "5-day +5%" signal — **does
not generalize and should not be trusted as evidence the move is predictable.**
`vn30_signal/vn30_gbm_auc.py` re-runs the *exact* VCB recipe on all 30 VN30 stocks:

| | value |
|---|---|
| VCB | **0.780** ← lone outlier |
| next best | VNM 0.622, TCB 0.602, FPT 0.600 |
| VN30 mean / median AUC | **0.531 / 0.535** (coin flip) |
| stocks ≥ 0.70 | **1 / 30** (only VCB) |
| stocks **< 0.50** (worse than random) | 8 / 30 |

VCB sits ~4σ above the pack — the signature of luck when an overfit-prone recipe is run
on 30 names, not a general signal. Consistent with the project's own conclusion that
single-stock direction is unpredictable and the tradeable target is **cross-sectional**.

**Related fragility of the VCB 0.78 (from `vcb_gbm_auc.py` diagnostics):** deterministic
across seeds, and 5-day-label train/test overlap is *not* the cause (a 20-day purge barely
moves it) — but it swings **0.64–0.78 across split fractions** (0.80 sits near the peak),
rests on only ~18 independent breakout episodes in the test tail (±0.15-ish CI), and the
feature-group choice was made *because* it maxes the test score (selection bias).

**Macro leakage (open data-hygiene issue, but NOT the cause of 0.78):**
`bronze_schema.trading_view_economy` has only a reference-period `date`, no release date —
so forward-filled macro values can be visible before publication (look-ahead). However the
`vn30_gbm_auc.py` ablation shows dropping the macro block *raises* AUC by +0.025 on average
(VCB: 0.780→0.785), so macro is not what produces the VCB number. Two separate problems;
don't conflate them.

---

## Common setup (all experiments)

- **Target (everywhere):** `y[t] = 1 if close[t+5]/close[t] - 1 >= 0.05` — next 5 trading
  days rise ≥ 5%. Binary classification. (`pool__targets.probability_gain_5pct_5day` is the
  precomputed version; base rate differs slightly, ~7% vs ~5% from raw close.)
- **Model to beat:** `HistGradientBoostingClassifier` on full point-in-time features.
- **Evaluation:** chronological (no shuffle) splits; metric = test **ROC-AUC** (+ PR-AUC,
  top-decile precision, lift). All AUCs are single-split point estimates with small positive
  counts → **±0.03–0.15 variance**; treat any single number with skepticism.

---

# Experiment 1 — Signal discovery
*(numbers below were computed on the old `unified_schema.unified_*` flat tables)*

### 1.1 Breakout event catalogue (VCB) — `breakout_events/detect_breakout_events.py`
Swing-high apex catalogue (apex = highest close within ±5 days), filtered by gain threshold.
Window `[peak−N−2, peak+2]`; decision day `peak−N`.

| Filter | Events | | Filter | Events |
|---|---|---|---|---|
| gain10d ≥ 15% | 17 | | gain10d ≥ 5% | 113 |
| gain10d ≥ 10% | 41 | | gain5d ≥ 5% | 98 |

VCB's Jan-2026 move is the all-time record: **+33% in 10 days** (57,100 → 76,000).

### 1.2 Univariate signal search (VCB) — `breakout_events/signal_search_5d5pct.py`
Base rate 11.5%. Strongest single features (ROC-AUC): `natr_14` / `atr_normalized` 0.64,
`volatility_21` 0.63, `close_bb_20_bandwidth` 0.61, `ppo_12_26_9` 0.60. Joint GBM (chrono
80/20): AUC ~0.76. → the signal is a **volatility/momentum regime**.

### 1.3–1.4 TA period sweeps (VCB) — `ta_period_sweep_vcb.py`, `vcb_best_period_per_family.py`
Tuning the indicator period barely helps; univariate AUC saturates ≈ **0.63–0.65**
(ATR/NATR 14 → 0.646, realized vol 20 → 0.63, Bollinger bandwidth → 0.61). Price-level MA
families score ~0.40 `low→up` — a non-stationarity **artifact**, not signal.

### 1.5 VN30 per-ticker + pooled — `vn30_signal/vn30_signal_5d5pct.py` (stale schema)
Superseded by **`vn30_signal/vn30_gbm_auc.py`** (current) — see headline caveat above.
Old pooled VN30 general-signal AUC was ~0.65; per-ticker predictability varies widely.

### 1.6–1.8 DL shoot-outs — `dl_signal/`
- `dl_model_comparison_vcb.py` (stale schema): on one stock (~2.9k rows) GBM 0.77 beats all
  DL heavily (LSTM 0.56, Transformer 0.48, CNN/MLP/GRU < 0.46).
- `dl_vn100_pooled.py` (**runs — `gold.stocks`**): VN100, 266k stock-days. GBM 0.625 ≈ MLP
  0.615 > sequence nets. 60× more data closed the DL gap but DL never overtakes GBM → signal
  is point-in-time, not temporal.
- `dl_vn100_xsec_macro.py` (**runs — `gold.stocks`**): adding macro / index / cross-sectional
  rank gives no robust gain; useful new features are **index volatility** + **cross-sectional
  volatility rank**, reinforcing the volatility-regime story.

### 1.9 VCB importance & trading meaning — `breakout_events/vcb_importance_and_trading.py` (stale schema)
0.77 is not reducible to a few features (top-50 → 0.65, all ~1053 → 0.76): skill comes from
aggregating hundreds of weak features. Test-period trading read: signal top-decile days
averaged **+2.16% fwd-5d vs +0.20%** all-days (win-rate 62% vs 45%) — a real ranking edge but
not a precise timer (top-decile hits +5% only 17% of the time). *(Re-evaluate under the
VN30-luck caveat before citing.)*

---

# Experiment 2 — Windowed input (1 sample = W-day × ~1053 matrix → scalar)
*(all scripts use the old flat schema → currently broken; results are historical)*

### 2.1 VCB, 20-day matrix — `vcb_seq20x1053_models.py`
GBM last-day (1053,) **0.770** beats every windowed model: GRU 0.695, Ensemble 0.659,
LSTM 0.653, CNN1D 0.636, MLP-flatten 0.609, GBM-flatten 0.551, Transformer 0.540. The 20-day
matrix does **not** beat point-in-time GBM; flattening hurts most.

### 2.2 Best model is stock-specific — `vnm/vic_seq20x1053_results.csv`
VCB → GBM last-day 0.770; VNM → GBM 0.581 (barely predictable); **VIC → GRU 20-day 0.694**
vs 0.509 last-day (+0.18; history helps for VIC only).

### 2.3 VCB lookback sweep (1→20) — `vcb_lookback_sweep.py`
Short lookback wins: best AUC 0.756 (lb1) → 0.688 (lb20). GBM degrades steadily with W
(0.756 → 0.548 at W20); GRU is the best DL model (peaks lb5–8 ~0.75) but never beats
short-window GBM.

---

# Experiment 3 — Walk-forward backtests & target study
See `experiment_3/README.md` for detail.
- `vcb_walkforward_backtest.py` (**stale schema**) — VCB walk-forward backtest metrics.
- `vn30_xsec_longshort.py` (**runs — `gold.stocks`**) — VN30 cross-sectional long/short:
  the project's shift toward a **cross-sectional relative-return** target (single-stock
  direction being unpredictable).
- `target_comparison.py` (**stale schema**) — compares target definitions
  (`return_5day` / `return_rel_5day` / `direction_5day` / `probability_gain_5pct_5day`).

---

# Overall conclusions (updated)

1. **VCB's ~0.77 is not a general signal — it's a lucky single-stock outlier** (VN30 median
   AUC 0.535, only VCB ≥ 0.70). Single-stock 5-day direction is essentially unpredictable.
2. **One weak-but-real universal regime:** near-term up-moves are preceded by **volatility /
   range expansion + momentum** — visible at stock, VN30, VN100 and index scale, but only
   worth ~0.62–0.65 pooled AUC.
3. **Gradient boosting on full point-in-time features is the model to beat;** plain MLP ≈ GBM
   and sequence nets never clearly win → the signal is in current feature *values*, not the
   temporal trajectory. (VIC is the lone stock where a 20-day window helps.)
4. **Short lookback wins;** longer windows add noise and cripple GBM.
5. **The real lever is the target,** not the architecture — hence the move to a
   **cross-sectional relative-return** target (experiment_3).
6. **Open issues before any result is publishable:** migrate stale scripts to the pool /
   gold schema; fix macro release-date alignment; replace single-split ROC-AUC with
   walk-forward + embargo and report PR-AUC / precision; lock the feature set before scoring.

---

## File index
- `experiment_1/README.md` — experiment 1 detail
- `experiment_1/breakout_events/` — events, signal search, TA sweeps, importance/trading; **`vcb_gbm_auc.py` = current pool-based VCB reproduction (~0.78)**
- `experiment_1/vn30_signal/` — **`vn30_gbm_auc.py` = current VN30 luck cross-check** (+ stale `vn30_signal_5d5pct.py`)
- `experiment_1/dl_signal/` — DL shoot-outs (`dl_vn100_*` run on `gold.stocks`; `dl_model_comparison_vcb.py` stale)
- `experiment_2/` — windowed model zoo + lookback sweep (VCB/VNM/VIC) — **stale schema**
- `experiment_3/` — walk-forward backtests + target study (`vn30_xsec_longshort.py` runs; others stale)

> CSV outputs are regenerated by the scripts. `vn30_gbm_auc.csv` holds the full VN30 table.
