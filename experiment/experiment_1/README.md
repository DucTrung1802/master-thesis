# Experiment 1 — Breakout detection & the "5-day +5%" signal (VCB → VN30 → VN100)

**Goal.** Characterise the dramatic upward price moves in Vietnamese equities
(the kind seen in VCB in Jan-2026), turn them into a labelled event catalogue,
and test how well the move can be *predicted* — first with classical ML, then
with deep learning, across single-stock and pooled-universe settings.

**Data.** PostgreSQL `database_main_v2`:
- `unified_schema.unified_<ticker>` — 30 VN30 tables, ~1058 engineered features each.
- `gold_schema.stocks` — 621-ticker panel, 910 TA features (used for VN100).
- `gold_schema.indices`, `unified_*` macro columns — index state, `economy_*`, `bonds_*`.

Label throughout: `y[t] = 1 if close[t+5]/close[t] - 1 >= 5%` (next 5 trading days up ≥5%).
All evaluation uses chronological (no look-ahead) splits.

---

## Sub-experiments & artefacts

| Folder | What it does | Key files |
|---|---|---|
| [`breakout_events/`](breakout_events/) | Swing-high breakout catalogue + univariate signal search + multi-period TA sweep | `detect_breakout_events.py`, `breakout_events_gain*pct.csv`, `signal_search_5d5pct.py`, `ta_period_sweep_vcb.py` |
| [`vn30_signal/`](vn30_signal/) | Per-ticker signal test for all 30 VN30 names + pooled "general signal" | `vn30_signal_5d5pct.py`, `vn30_signal_per_ticker.csv`, `vn30_general_signal_ranking.csv` |
| [`dl_signal/`](dl_signal/) | DL shoot-out (MLP/LSTM/GRU/CNN/Transformer) on VCB, pooled VN100, and VN100 + macro/cross-sectional/index features | `dl_model_comparison_vcb.py`, `dl_vn100_pooled.py`, `dl_vn100_xsec_macro.py` + `*_results.csv` |

---

## Key findings

**1. Breakout catalogue.** A fixed swing-high catalogue (apex = highest close within
±5 days) filtered by gain threshold gives monotonic event sets. VCB's Jan-2026 move
is the all-time record: **+33% in 10 days** (close 57,100 → 76,000). Event files:
`gain10d ≥ 15% (17) ⊂ ≥ 10% (41) ⊂ ≥ 5% (113)`, and `gain5d ≥ 5% (98)`.
Each event window is `[peak−N−2, peak+2]`; the **predictable/decision day is `peak−N`**
(the launch day), not the peak.

**2. The universal signal is a volatility regime.** Across VCB, all VN30, and VN100,
the strongest predictors of a 5d+5% move are **volatility/range expansion**
(`natr`, `atr_normalized`, `volatility_21`, `close_bb_20_bandwidth`) plus
**momentum strength** (`ppo_strength`, `trix_strength`). Raw price levels and the
broad macro block (`economy_*`, `bonds_*`) wash out. Tuning the indicator period
does **not** help — univariate AUC saturates ≈ 0.63–0.65.

**3. Classical ML vs deep learning.**

| Setting | Train size | Best classical (GBM) | Best DL | Verdict |
|---|---|---|---|---|
| VCB alone | ~2.9k | **0.770** | LSTM 0.558 | GBM wins by a mile |
| Pooled VN100 (per-stock TA) | ~191k | **0.625** | MLP 0.615 | DL competitive, GBM still best |
| Pooled VN100 + macro/xs/index | ~191k | 0.619 | GRU 0.628 | within noise; no robust gain |

- More data (60× via pooling) shrank the DL gap from −0.21 to ~0, but **did not
  overturn gradient boosting**.
- The **plain MLP ≈ GBM** while LSTM/GRU/Transformer lag → the signal lives in
  point-in-time feature *values*, not temporal sequence shape.
- Adding macro/cross-sectional/index-state features promoted **index volatility**
  and **cross-sectional volatility rank** into the top-64, but the headline AUC
  did not move beyond seed/architecture noise.

**4. Ceiling.** The 5d+5% binary signal has a genuine ceiling of **AUC ≈ 0.62–0.65**
on the pooled universe, robust to richer features and deeper models. The remaining
lever is the **target definition** (continuous / volatility-scaled forward return),
not the model.

---

## Reproduce

```bash
# breakout catalogue
python experiment/experiment_1/breakout_events/detect_breakout_events.py
# single-stock signal study
python experiment/experiment_1/breakout_events/signal_search_5d5pct.py
python experiment/experiment_1/breakout_events/ta_period_sweep_vcb.py
# VN30 universe
python experiment/experiment_1/vn30_signal/vn30_signal_5d5pct.py
# deep learning
python experiment/experiment_1/dl_signal/dl_model_comparison_vcb.py
python experiment/experiment_1/dl_signal/dl_vn100_pooled.py
python experiment/experiment_1/dl_signal/dl_vn100_xsec_macro.py
```

Requires `.env` with `POSTGRES_*` credentials; deps: `psycopg2`, `pandas`,
`scikit-learn`, `TA-Lib`, `torch` (CUDA optional).
