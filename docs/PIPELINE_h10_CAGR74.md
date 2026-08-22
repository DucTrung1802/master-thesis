# PIPELINE — the h=10 cross-sectional chain that returns **CAGR +74.0 %/yr**

> Written 2026-08-21. **Every number below was read from disk on that date**, not quoted
> from another register — the pooled row was re-derived with
> `walkforward.evaluate --draws 0` (26.1 s, no GPU) and the provenance came out of the
> artefacts' own `metadata.json`.
>
> | file | answers |
> |---|---|
> | [CLAUDE.md](../CLAUDE.md) | *what is this, and what has it PROVED?* |
> | [RUNBOOK.md](RUNBOOK.md) | *how do I RUN it?* |
> | [ISSUES.md](ISSUES.md) | *what is BROKEN?* |
> | [TODO.md](TODO.md) | *what is NEXT?* |
> | **PIPELINE_h10_CAGR74.md** | ***how does ONE number get made, end to end?*** |
>
> ⚠️ **This file explains ONE result. It is not a second RUNBOOK** — where a command is
> given, RUNBOOK is the authority and this file is quoting it.
>
> ## ⚠️ READ §12 BEFORE QUOTING THE 74 %
>
> The headline is **real, out of sample, after costs, over ten folds** — and it is
> **not** a claim that this strategy would have earned 74 %/yr. Four measured reasons are
> in §12, and the shortest one is: **the universe contains no delisted company.**

---

## 1. The headline, as it comes off disk

```
POOLED — the whole walk-forward as ONE track
    ic    ic_t  n_dates  days_pos  mkt_sharpe  mkt_cagr  sharpe@30  cagr@30  n_periods  se_sharpe
0.1412 16.0479     2383    0.8645      0.7149    0.1387     2.5310   0.7398        236     0.1278
```

| | value |
|---|---|
| **CAGR, 30 bps round trip** | **+73.98 %/yr** |
| CAGR at 20 / 50 bps | +76.80 % / +68.49 % |
| Sharpe at 20 / 30 / 50 bps | +2.601 / **+2.531** / +2.391 |
| the market it is measured against | equal-weight universe, **Sharpe +0.715, CAGR +13.87 %** |
| daily rank IC | **+0.1412**, `ic_t` **+16.05**, positive on **86.5 %** of 2,383 sessions |
| non-overlapping periods | **236** · `se_sharpe` **0.128** |
| null, 200 within-date shuffles | **z = +18.58** at 30 bps, null MAX below observed |
| IC positive | **10 of 10 folds**; beats the universe **10 of 10** on Sharpe *and* CAGR |

**Which model.** ⚠️ The 74 % is the **`lstm`** arm. A `gbt` arm on the identical folds
scores a *higher Sharpe* (**+2.891**) and a *lower CAGR* (**+69.8 %**) — RUNBOOK §7c:
*"the best model" is not well-formed without an estimand.* This document follows the CAGR,
so it follows the LSTM.

**Where it lives**: `results/walkforward_h10/` — `folds.csv`, `per_fold.csv`,
`predictions_oos.csv`, plus ten run folders under `src/model/runs/` (⚠️ gitignored,
`RPR-1`).

---

## 2. The chain in one picture

```
  ①  raw_data/            Selenium + HTTP scrapers        TradingView · CafeF · Simplize · GICS
        │
  ②  bronze_schema        20 assets → 25 tables           raw-faithful, one table per source/tab
        │
  ③  silver_schema        20 assets                       canonical, cross-source merged, GICS attached
        │                                                 └─ silver.stocks_basic  (38 columns)
  ④  gold_schema          11 assets                       + features, returns, volatility, as-of macro
        │
  ⑤  unified_schema_all   pool__basic  2,388,975 × 101    38 silver + 63 derived drv_*
        │                 pool__targets                   return_{h}day for every h
        │
  ⑥  THE PANEL            624,448 × 104 · 150 names       top-150 by pre-2014 turnover
        │                 + cs_rank_10day derived          ← the label is made HERE
        │
  ⑦  feature_selection    90 candidates → 61 kept → 19    Kaggle T4, 6 h 02 m, 20-draw null, z = +13.78
        │
  ⑧  final_features       rank_10day__final__d20_h10      624,448 × 23 (19 channels + keys + label)
        │
  ⑨  train_test_creator   424,776 / 93,268 / 93,383       × 20 × 19 windows, purge 29
        │
  ⑩  model.lstm           208,769 parameters              predicts cs_rank_10day
        │
  ⑪  backtest             top-20 of 150, 30 bps           long-only, ceiling-screened
        │
  Ⓦ  walkforward          10 expanding folds              ← ⚠️ THE 74 % IS PRODUCED HERE, NOT AT ⑪
```

⚠️ **Stages ⑨-⑪ run TEN TIMES, once per fold.** A single train/val/test split of the same
chain exists and gives a *different* number (§11). The 74 % is the ten folds pooled.

---

## 3. ① — ④ · Where the data comes from

**Sources.** TradingView supplies the universe and OHLCV (Selenium, because the endpoint is
browser-gated); **CafeF** supplies the matched/negotiated turnover split, foreign and
proprietary flow, news and financial-statement PDFs; **Simplize** supplies adjusted daily
OHLC and foreign flow through a plain JSON endpoint; **MSCI GICS** supplies the industry
tree that `drv_cs_ret_vs_industry` needs.

**Orchestration** is Dagster, 83 assets, `src/orchestration/` — see
[src/orchestration/CONTEXT.md](../src/orchestration/CONTEXT.md). Nothing in this chain is a
manual step.

⚠️ **A green asset is not evidence of fresh data** (CLAUDE.md §5 rule 10). `landed()` asks
*"is this folder empty?"*, not *"did this run fetch anything?"* — the honest freshness check
is the per-series `MAX(date)`, and `FRZ-1` records the case where 755 of 781 tickers were
37 sessions stale while the table-level max date looked current.

⚠️ **`OUT-1` — one corrupt source cell manufactured a finding, and the screen that catches
it is upstream of everything here.** `silver.stocks_basic` carried
`prop_buy_val = 4.001e17` for VCB on 2026-01-05 against that day's entire turnover of
2.06e11. That single cell drove a **+0.266** correlation against the forward 5-day return.
`_helper_screen_flow_outliers` now NULLs such pairs on three rules and removes
**611 of 2,388,975 rows (0.0256 %)**.

---

## 4. ⑤ · `pool__basic` — the only pool this chain reads

`unified_schema_all.pool__basic` is **2,388,975 rows × 101 columns**: every column of
`silver.stocks_basic` (38) plus **63 derived `drv_*` channels** computed in SQL in the same
`CREATE TABLE AS`.

The derived block is seven families, chosen *against* `gold.stocks_ta`'s 935 columns so as
not to duplicate them:

| family | n | examples that end up mattering |
|---|---|---|
| bar shape | 7 | `drv_clv`, `drv_lower_shadow` |
| range volatility | 9 | `drv_parkinson_5`, `drv_rogers_satchell_5`, `drv_range_hl_pct` |
| normalisation | 13 | `drv_close_z_21`, `drv_dist_from_high_252`, `drv_volume_pos_63` |
| **order flow** | 10 | **`drv_order_vol_imb`**, `_5`, `_21`, `drv_order_count_imb_z21`, `drv_log_order_size_ratio` |
| foreign / prop | 8 | — *(none survive selection at h=10)* |
| liquidity | 7 | `drv_vwap_raw`, `drv_close_vs_vwap` |
| **cross-sectional** | 5 | **`drv_cs_pct_ret_1d`**, **`drv_cs_ret_vs_industry`** *(universe partitions only)* |

✅ **Verified**: 20 channels against an independent pandas recomputation (16 at ≤9.5e-13),
**0 name collisions** with `pool__ta`/`pool__fa`, **0 history bleed** across all 781 series,
and a causality test — the block rebuilt on data truncated at 2026-06-15 reproduces all
derived columns on 4,227 shared rows at **max abs diff exactly 0.0**.

⚠️ **Five traps were measured building it** and each one had already produced a wrong
number once: silver's `open/high/low` are **RAW** and must be split-adjusted before the bar
is used; `value_matched` is in **billions** of VND while `foreign_*_value` is in plain VND;
`bigint/bigint` is integer division; `STDDEV_SAMP` over bigint returns `numeric` and trips
the `Decimal`→`object` trap; and **PostgreSQL computes PARTIAL window frames by default**,
so a 252-day channel was silently a 10-day channel for every series' first year.

⚠️ **`pool__ta` is deliberately NOT joined.** It was offered to this chain twice — 120
candidates at h=20 and 162 at h=10 — and tied both times (paired `t` = −0.29 and +0.46).
It also carries `STA-1`'s 13 legacy column names and stops at 2026-06-26, so joining it
would truncate the panel.

---

## 5. ⑥ · The panel and the label — **the most important design decision in the chain**

`feature_selection.cross_sectional.read_universe_panel` builds
`pool__basic ⋈ pool__targets` in one server-side statement and derives the label locally.

| | |
|---|---|
| panel | **624,448 rows × 104 columns**, **4,388 sessions**, 2009-01-02 → 2026-08-07 |
| universe | **150 names**, ranked by **median `value_matched` before 2014-01-01** — sha1 `301aeb491d` |
| label | **`cs_rank_10day`** = `(rank − 1)/(n − 1) − 0.5`, average ranks, **within each date** |
| derived from | `return_10day` · labelled 622,948 · unlabelled 1,500 (the incomplete tail) |
| label stats | mean 7.4e-20, sd 0.2907, min −0.5, max +0.5 |

### Why a within-date RANK and not a return

Three things follow, and each fixes a specific failure of the single-stock studies:

1. **The market factor is gone by construction.** Subtracting VNINDEX still leaves each
   stock's residual beta; a within-date rank removes anything common to the cross-section
   whatever its beta.
2. **Outliers cannot dominate.** `unified_schema_all.return_5day` reaches **−781** (VNX, a
   negative `close_adjust` for 968 sessions). One such row would set an MSE loss. A rank is
   invariant to it.
3. **It is stationary.** There is no era to identify, which is what every earlier
   representation was groping at.

⚠️ **`liquidity_before` IS REQUIRED AND HAS NO DEFAULT.** Ranking turnover over the whole
sample picks the names that *turned out* to be liquid — look-ahead of exactly the kind §12
is about. The exporter **raises** rather than defaulting silently. **358 of 781 tickers
could never enter this universe**, and it is *not* point-in-time.

⚠️ **The label is NOT STORED** (`RNK-1`). A rank depends on which other names are in the
panel, not on the row, so `pool__targets` stores `return_10day` and
`train_test_creator._label` re-ranks at dataset build. The universe travels in the table's
`COMMENT` so two different universes cannot be unioned into one table.

---

## 6. ⑦ · Feature selection — 90 → 61 → 19

Ran on a **Kaggle T4** (`kgpu`, panel mode), **6 h 02 m 20 s**, run id
`2026-08-19_093403__all__basic__cs_rank_10day`.

⚠️ **The join happens locally, not on the worker** (`CSP-1`): `read_universe_panel` reaches
for a database cursor and a Kaggle worker has none, so one finished `panel.parquet` ships
with `cs_rank` already derived.

| setup key | value |
|---|---|
| lookback `d` / horizon `h` | 20 / 10 |
| purge gap | **29** rows = `d + h − 1` |
| CV | 5 purged expanding folds, `min_train` 500 |
| **`feature_normalize`** | **`cs_rank`** — every channel ranked within its date ⚠️ see §12.5 |
| rankers | **`spearman`, `xgb_shap`, `permutation`** |
| window stats | last, mean, slope, sd, min, max → 90 channels = **540 design columns** |
| `design_dtype` | `float64` ⚠️ `float32` does **not** reproduce it (52 % change in `ic_mean`) |
| `env_fingerprint` | `b899d1bd4ec0` (numpy 2.0.2 / sklearn 1.6.1 / xgboost 3.2.0) |

**The cut**: 90 candidates → **61 kept** (29 dropped as redundant at `|ρ| ≥ 0.9`, 0 constant)
→ **19 shortlisted** by a Benjamini-Hochberg consensus at `fdr_q = 0.1`.

### The 19 channels, in the order the selection ranked them

| # | channel | ensemble | `consensus_p` | coverage |
|---|---|---|---|---|
| 1 | **`drv_order_vol_imb`** | 1.00 | 0.000003 | 0.925 ⚠️ |
| 2 | `drv_clv` | 2.67 | 0.000078 | 0.899 ⚠️ |
| 3 | `drv_order_vol_imb_5` | 6.00 | 0.0011 | 0.956 |
| 4 | `drv_close_vs_vwap` | 7.00 | 0.0018 | 0.923 ⚠️ |
| 5 | `drv_log_order_size_ratio` | 7.33 | 0.0021 | 0.908 ⚠️ |
| 6 | `drv_rogers_satchell_5` | 10.00 | 0.0056 | 0.999 |
| 7 | `drv_range_hl_pct` | 10.67 | 0.0069 | 1.000 |
| 8 | `drv_parkinson_5` | 13.33 | 0.0135 | 0.999 |
| 9 | `drv_cs_pct_ret_1d` | 14.67 | 0.0180 | 1.000 |
| 10 | `drv_lower_shadow` | 16.00 | 0.0237 | 0.899 ⚠️ |
| 11 | `drv_order_count_imb_z21` | 16.67 | 0.0268 | 0.924 ⚠️ |
| 12 | `drv_dist_from_high_252` | 17.00 | 0.0284 | 0.940 ⚠️ |
| 13 | `drv_vwap_raw` | 19.00 | 0.0397 | 0.951 |
| 14 | `drv_cs_ret_vs_industry` | 20.00 | 0.0464 | 1.000 |
| 15 | `drv_close_z_21` | 22.00 | 0.0625 | 0.991 |
| 16 | `drv_volume_pos_63` | 23.33 | 0.0746 | 0.984 |
| 17 | `n_sell_orders` | 34.00 | 0.235 | 0.955 |
| 18 | `drv_order_vol_imb_21` | 38.00 | 0.322 | 0.957 |
| 19 | `close_adjust` | 46.67 | 0.534 | 1.000 |

⚠️ **Eight rows are flagged `PARTIAL` coverage** (`COV-1`) — below the 0.95 line.
⚠️ **`close_adjust` is a raw VND PRICE LEVEL** and it enters at rank 19 with
`consensus_p` 0.534. It was chosen *as its within-date rank*; §12.5 is what happens next.

**Order flow dominates**: five of the top eleven are order-book imbalance or order-size
channels, and `drv_order_vol_imb` is #1 here, #1 at h=20, and #1 again in the pre-2017
look-ahead probe. **No `prop_*` or `foreign_*` channel survives.**

### The null — 20 draws, the whole selection re-run inside each

| | |
|---|---|
| observed `ic_mean` | **+0.1201** (folds +0.054 … +0.162, `ic_trend_per_fold` +0.0115) |
| null mean / sd | +0.0233 / 0.0070 |
| p95 BAR | +0.0355 |
| **null MAX** | **+0.0357** — *below* the observed, so §5 rule 3 does not fire |
| **z** | **+13.78** · p = 0.0476 (the 1/21 floor — read `z`) · 0 failed draws |
| `n_eff` per fold | **76.6** (`n_dates/h`, not `n_rows/h`) · `se_ic_per_fold` 0.115 |

⚠️ **The null's mean is +0.0233, not zero.** The procedure earns +0.023 on a shuffled
label, so the excess over chance is **+0.097, not +0.120**.

---

## 7. ⑧ · `final_features` — the only stage that writes the database

```powershell
python -m final_features --apply          # ~6 s
```

Builds `unified_schema_all.rank_10day__final__d20_h10` — **624,448 × 23** (19 channels +
3 keys + the stored label). Its `COMMENT` carries the full setup, the shortlist fingerprint
`033165fe12ed`, the universe sha1, the source run id and the evidence string
`cleared_p95_not_a_pass=1`, so a downstream reader cannot lose the provenance.

⚠️ **`final_features` groups on `(schema, target, setup)` — there is no term for WHICH
POOLS a run saw**, so two experiments can silently union into one table. `--scope` separates
names; `--root` separates groups (`PRB-1`).

---

## 8. ⑨ · `train_test_creator` — tensors

```powershell
python -m train_test_creator --ticker all --table rank_10day__final__d20_h10 --save
```

| | train | val | test |
|---|---|---|---|
| windows | **424,776** | 93,268 | 93,383 |
| shape | `× 20 × 19` | | |
| dates *(single-split build)* | 2009-02-05 → 2021-03-03 | 2021-04-14 → 2023-10-17 | 2023-11-28 → 2026-07-24 |

**The four steps that keep it honest:**

1. **The purge is `d + h − 1 = 29` rows**, not `h`. Purging only `h` would leave 19 rows of
   the test sample's own input window in training.
2. **The cut is on the DATE axis**, never the row index — a row-index cut would put one
   date in train for one ticker and in val for another.
3. **Imputation is the TRAIN-slice median**, never `ffill().bfill()` (a `bfill` fills a
   leading gap with a *future* observation). The "train slice" is the rows a train *sample*
   reaches — the cut minus the purge gap.
4. **The scaler is fit on the train slice only**, and the label is re-ranked here (`RNK-1`).

---

## 9. ⑩ · The model

`python -m model.lstm --config configs/lstm__all__rank_10day__final__d20_h10.yaml`

| | |
|---|---|
| architecture | LSTM, `hidden_size` 128, `num_layers` 2, `dropout` 0.2 |
| parameters | **208,769** |
| loss | **`nn.MSELoss`** on the standardised rank |
| optimiser | batch 512, `lr` 1e-3, `weight_decay` 1e-5, `grad_clip` 1.0, ReduceLROnPlateau (0.5, patience 5) |
| early stopping | `max_epochs` 100, `patience` 15 — **val chooses the epoch** |
| seed | 42 |

**Per-fold outcome, all ten folds** (`n_params` identical throughout):

| fold | `best_epoch` | `best_val_loss` | test IC | `ic_t` | `mase` |
|---|---|---|---|---|---|
| oos2017 | 1 | 0.9697 | +0.1742 | 9.59 | 0.9774 |
| oos2018 | 1 | 0.9708 | +0.1559 | 7.64 | 0.9797 |
| oos2019 | 2 | 0.9788 | +0.1903 | 10.67 | 0.9737 |
| oos2020 | 3 | 0.9558 | +0.1535 | 5.76 | 0.9801 |
| oos2021 | 1 | 0.9657 | +0.0875 | 2.72 | 0.9922 |
| oos2022 | 1 | 0.9795 | +0.1555 | 4.62 | 0.9863 |
| oos2023 | 2 | 0.9928 | +0.1031 | 4.11 | 0.9929 |
| oos2024 | 1 | 1.0181 | +0.1727 | 9.55 | 0.9808 |
| oos2025 | 2 | 0.9672 | +0.1081 | 4.19 | 0.9972 |
| oos2026 | 3 | 1.0066 | +0.0944 | 1.68 | 0.9808 |

⚠️ **`best_epoch` is 1-3 in every fold and `best_val_loss` never leaves 0.956-1.018** — i.e.
within ~4 % of the variance of a standardised label. The LSTM takes what it can in one or
two passes and overfits after. **Capacity is not the binding constraint here**, and that has
now been measured deliberately four times.

⚠️ **`mase` 0.974-0.997 — it beats "predict the mean rank" by 0.3-2.6 %.** That is what an
R² near zero looks like from the other side. **The result is the ORDER; the magnitudes carry
nothing.**

⚠️ **The architecture is nearly irrelevant.** On these identical folds a **1,398-node GBT**
scores a *higher* Sharpe (+2.891 vs +2.531) while reducing each `(20, 19)` window to **114
window statistics where the LSTM sees 380**. Of seven arms spanning **224× of capacity**,
only `cnn` loses significantly on Sharpe. **⚠️ And an arm gap below `|d_sharpe| ≈ 0.09` is a
reseed, not a result** (measured 2026-08-21 on five seeds).

---

## 10. ⑪ · From a ranking to money

`backtest.long_only_top_k`, and every rule in it is forced by the Ho Chi Minh exchange:

| rule | value | why |
|---|---|---|
| positions | **top 20 of ~150** | Sharpe decays monotonically 1.53 (k=10) → 0.81 (k=75); k=20 is not a knife-edge |
| direction | **long only** | HOSE offers no shorting |
| weights | equal, `1/k` | ⚠️ never varied — TODO `P13` |
| rebalance | every **10 sessions**, non-overlapping | so periods do not overlap and `n_eff` is honest |
| **ceiling screen** | **ON by default** | a name at its daily price ceiling on the entry date has **no sellers**; buying it is fiction. Drops **9,259 of 349,581 rows = 2.65 %** |
| cost | `round_trip × ½ × Σ\|Δw\|`, `ROUND_TRIP_COST` = 50 bps | the same constant the sentiment study uses |

### ⚠️ The cost identity that makes h=10 the right horizon

At turnover `τ = 0.70` and 50 bps:

| rebalance | per year | annual fee drag |
|---|---|---|
| h=5 | 50.4 | **17.6 %** |
| **h=10** | **25.2** | **8.8 %** |
| h=20 | 12.6 | 4.4 % |

Top-100 benchmark CAGR is 9.75 %. ⚠️ **At h=5 the fees alone exceed the market's entire
return** — which is arithmetic, not a model failure, and it is why four earlier single-stock
defeats at h=5 were never going to be rescued by a better model.

---

## 11. Ⓦ · The walk-forward — **where the 74 % is actually produced**

```
|<--------------- train (expanding) --------------->|<-- val 12m -->|<-- test 12m -->|
                                                   gap             gap
```

Ten folds, test = calendar 2017 … 2026, step 12 months. **Every fold refits everything** —
the model, the scaler, the imputation median and the coverage screen — so no later
statistic leaks into an earlier fold.

| fold | train | val | test | IC | Sharpe@30 | market | CAGR@30 | market |
|---|---|---|---|---|---|---|---|---|
| oos2017 | 229,098 | 32,777 | 37,021 | +0.175 | **+3.84** | +2.38 | +87.9 % | +32.7 % |
| oos2018 | 266,167 | 32,700 | 37,020 | +0.157 | **+3.25** | −0.61 | +59.5 % | −9.3 % |
| oos2019 | 303,188 | 32,699 | 37,211 | +0.189 | **+3.93** | +0.92 | +77.0 % | +7.1 % |
| oos2020 | 340,208 | 32,890 | 37,440 | +0.152 | **+3.52** | +2.02 | +150.9 % | +57.2 % |
| oos2021 | 377,419 | 33,119 | 37,077 | +0.091 | **+5.00** | +2.95 | +240.6 % | +117.4 % |
| **oos2022** | 414,859 | 32,756 | 37,027 | +0.151 | **+0.37** | −1.26 | +7.3 % | **−40.1 %** |
| oos2023 | 451,936 | 32,706 | 36,978 | +0.102 | **+2.77** | +1.33 | +58.3 % | +24.9 % |
| oos2024 | 488,963 | 32,657 | 37,127 | +0.171 | **+3.68** | +0.48 | +49.7 % | +5.7 % |
| oos2025 | 525,941 | 32,806 | 37,035 | +0.107 | **+2.39** | +1.04 | +41.2 % | +17.6 % |
| oos2026 \* | 563,068 | 32,685 | 15,645 | +0.094 | +1.38 | −1.42 | +22.9 % | −18.4 % |

\* partial year — 135 dates, **11 periods**, `se_sharpe` 0.400.

⚠️ **The Sharpe decays: slope −0.219/fold, first half +3.907 → second half +2.119, a
−45.8 % fall.** In proportional terms that is the *same* decay the h=20 track shows
(−43.6 %) — the absolute slope is steeper only because the level is higher.

✅ **The decay is not a seed artefact** — measured on five seeds, slope −0.308 ± 0.027 with
a −55 % fall in every one. ✅ **And it is not staleness**: retraining **twice as often**
(20 folds instead of 10) gives paired `t` = −0.09, ρ = 0.989.

⚠️ **A per-fold cell is 4.4× noisier than the pooled row** (mean per-fold Sharpe range over
five seeds **0.593** against **0.134** pooled). **Never compare two arms in one fold.**

### The null

`y_pred` shuffled **within each date**, 200 draws — this destroys the ranking while keeping
the universe, the calendar, the cost and the rebalance schedule intact.

| bps | observed | null mean | p95 bar | null MAX | **z** |
|---|---|---|---|---|---|
| 20 | +2.601 | +0.505 | +0.676 | +0.812 | **+18.42** |
| 30 | +2.531 | +0.409 | +0.578 | +0.719 | **+18.58** |
| 50 | +2.390 | +0.218 | +0.389 | +0.534 | **+18.86** |

Null MAX below observed at all three, so §5 rule 3 does not fire.

### Two checks that were run because a Sharpe of 2.5 should be disbelieved first

✅ **No mechanical leak.** Restricting the track to the single split's *own* test window
(2023-11-28 → 2026-07-24, read from the dataset's `metadata.json`) gives **63 periods on
both sides**, IC +0.1307 against the single split's +0.1393, Sharpe@30 **+2.257 against
+2.442** — a gap of **0.8 SE**. The two agree within noise.

✅ **The selection look-ahead is bounded.** Re-running the identical selection on dates
**< 2017** — exactly what fold 0 could have seen — keeps **51 of 61** channels
(Jaccard 0.750, chance 39.3 ± 2.1 → **+5.48 sd**), shortlists **7** against a chance of
1.90, and puts **`drv_order_vol_imb` at #1 in both**. ⚠️ **It bounds the optimism; it does
not remove it.**

---

## 12. ⚠️ WHAT THE 74 % DOES **NOT** MEAN

**This is the section that matters.** The `z = +18.58` is well protected. The **+74 %/yr is
not**, and the two must never be quoted as if they carried the same weight.

### 12.1 ⚠️ Survivorship protects the `z` and NOT the CAGR

`silver.stocks_basic` holds **no delisted name**. Every within-date shuffle draws from the
same survivor basket, so the null is unaffected and `z = +18.58` stands. **The CAGR is
computed on a basket of companies that we know did not go to zero**, and a screen that buys
recent winners is the strategy most flattered by that.

### 12.2 ⚠️ `NUL-1` — no null here prices the SEARCH

The within-date shuffle prices the universe, `k`, the cost and the schedule. It prices
**none** of: the feature selection that chose the 19 channels, the architecture search, the
early-stopping epoch, or the choice of `h=10` — which was itself made *using* a prior
result. Every one of those is a decision taken with the data in hand.

### 12.3 ⚠️ Execution is still kinder than the market

| gap | status |
|---|---|
| ADV / size cap | ❌ not modelled — a 20-name book at real size moves a VN mid-cap |
| **floor days on the SELL side** | ❌ the ceiling screen covers **ENTRY only**. A name at its floor on the exit date cannot be sold — and that is exactly when a loser is |
| slippage | ❌ none |
| the ATC auction | ❌ signals from full-day order counts settle only after the close |
| max drawdown | ⚠️ **−55 to −58 %** at every `k` on the related h=10 screen. *Statistically tradable ≠ holdable* |

### 12.4 ⚠️ The universe is not point-in-time

Top-150 by turnover **before 2014-01-01** avoids ranking on the future, but it still fixes
one basket for the whole sample. **358 of 781 tickers could never enter it.** Before 2014 it
is a forward-looking bet on which names would be liquid.

### 12.5 ⚠️ `FNM-1` — the model is fed a representation the selection did not score

The selection ran with **`feature_normalize=cs_rank`** — every channel ranked within its
date. `train_test_creator` does no such thing: the dataset records **19 scaled columns, 0
bounded**, standardised **globally** by one `StandardScaler`.

So `close_adjust` — a raw VND price — and `drv_vwap_raw` reach the LSTM as levels. That
dataset's own `drift.csv`:

| channel | test rows beyond 5 train-sigmas | test mean z |
|---|---|---|
| `drv_order_vol_imb_21` | **7.65 %** | +0.50 |
| `close_adjust` | **5.48 %** | **+1.10** |

✅ The *channel set* is representation-invariant (re-running under `feature_normalize=none`
keeps 12 of 13 at h=20, +5.90 sd above chance). ⚠️ **But the BAR does not transfer**, so
`z = +13.78` is a `cs_rank` number describing a model that does not use `cs_rank` features.
**This is TODO `P11`.**

### 12.6 It ranks, it does not price

R² is ≈ 0 and `mase` is 0.974-0.997. **The chain cannot tell you what a stock will be
worth** — it tells you where a stock will sit among these 150 over the next 10 sessions,
and reading that requires scoring all 150 on the same date.

⚠️ **For one specific stock the honest answer is often "no trade".** VCB took **zero
trades in 33 periods** of the h=20 test window: its median percentile among the 150 is
0.273 and its maximum ever reached is 0.826, so it never touches the entry band. That was
the correct call — VCB returned +1.45 %/yr while the universe made 5.96 % — but *a correct
"do not hold this" is not a tradable signal for that stock.*

---

## 13. Reproduce it

```powershell
.\mt_env\Scripts\Activate.ps1
$env:DAGSTER_HOME = "D:\GIT\master-thesis\.dagster"
cd src

# ⑦ selection — MANUAL, Kaggle T4, 6 h 02 m. The run folder is already merged in.
cd kaggle_gpu
python -m kgpu export   cross-sectional     # DB -> ONE panel.parquet   ~2 min / 477 MB
python -m kgpu rehearse cross-sectional     # the WORKER path, locally, NO QUOTA
python -m kgpu run      cross-sectional
cd ..

# ⑧ the final table
python -m final_features --apply                                        # ~6 s

# ⑨ tensors   ⚠️ --ticker all is NOT optional (the default is vcb)
python -m train_test_creator --ticker all --table rank_10day__final__d20_h10 --save

# Ⓦ the ten folds   ⚠️ --out is LOAD-BEARING: the default overwrites the h=20 track
python -m walkforward --ticker all --table rank_10day__final__d20_h10 `
    --config lstm__all__rank_10day__final__d20_h10.yaml --first-test 2017-01-01 `
    --out ../results/walkforward_h10                                    # 33m 26s

# the pooled row + the 200-draw null
python -m walkforward.evaluate --top-k 20 --draws 200 --universe all `
    --out ../results/walkforward_h10                                    # 8m 59s
```

**To re-read the pooled row without re-running anything** — the pooled line is printed and
never stored, so this is how you check a quoted number against the artefact:

```powershell
python -m walkforward.evaluate --top-k 20 --draws 0 --universe all `
    --out ../results/walkforward_h10                                    # 26.1 s, no GPU
```

⚠️ **`--draws 200` is what costs the ~9 minutes**, not the scoring. Drop it when re-reading
a level; keep it when claiming the level clears a bar.

### Measured runtimes

| stage | cost | where |
|---|---|---|
| ⑦ selection, 90 channels, 20-draw null | **6 h 02 m 20 s** | Kaggle T4 |
| ⑧ `final_features` | ~6 s | local |
| ⑨ `train_test_creator` | ~11 s | local |
| Ⓦ 10-fold sweep | **33 m 26 s** | RTX 3050 |
| Ⓦ scoring + 200-draw null | **8 m 59 s** | local, no GPU |
| Ⓦ re-read, no null | **26.1 s** | local, no GPU |

---

## 14. Provenance

| artefact | identifier |
|---|---|
| selection run | `reports/feature_selection/2026-08-19_093403__all__basic__cs_rank_10day/` |
| git commit at selection | `e270c9d3+dirty` |
| shortlist fingerprint | `033165fe12ed` over 19 channels |
| universe sha1 | `301aeb491d` (150 names) |
| env fingerprint | `b899d1bd4ec0` |
| final table | `unified_schema_all.rank_10day__final__d20_h10` — 624,448 × 23 |
| dataset | `all__rank_10day__final__d20_h10__tr70_val15_test15__std` |
| track | `results/walkforward_h10/` — `folds.csv`, `per_fold.csv`, `predictions_oos.csv` |
| the ten run folders | `src/model/runs/lstm__all__rank_10day__final__d20_h10__oos20XX__*` ⚠️ **gitignored** (`RPR-1`) |
| evidence string | `cleared_p95_not_a_pass=1` |

⚠️ **`RPR-1`: `src/model/runs/*/` and `src/train_test_set/` are git-ignored**, so a fresh
clone has the track CSVs and the selection report but **not** the ten run folders. They are
re-derivable from §13 in about 35 minutes.

---

## 15. Where to read more

| open this | for |
|---|---|
| [src/walkforward/CONTEXT.md](../src/walkforward/CONTEXT.md) | §9 the h=10 track · §9b the leak check · §9e the look-ahead probe · §11 seven architectures · **§15 the seed floor** |
| [src/backtest/CONTEXT.md](../src/backtest/CONTEXT.md) | §3 the cost identity · §5 the single-stock answer · §10 the indicator survey |
| [src/feature_selection/CONTEXT.md](../src/feature_selection/CONTEXT.md) | the selector, the nulls, the ranker comparison |
| [src/orchestration/CONTEXT.md](../src/orchestration/CONTEXT.md) | every asset, pool and source table above |
| [ISSUES.md](ISSUES.md) | `NUL-1`, `FNM-1`, `COV-1`, `DRF-1`, `RPR-1`, `STA-1`, `CSP-1` — all cited above |
| [TODO.md](TODO.md) | ⚠️ **re-prioritised 2026-08-22 — DATA FIRST.** `P1` (`FRZ-1`, the output blocker), `P2` (carry it up to gold), `P6`-`P9` (fundamentals: the JSON gate, then OCR on Kaggle), `P10` (live scoring), `P11` (the `FNM-1` fix), `P13` (portfolio construction), `P14` (execution realism) |
