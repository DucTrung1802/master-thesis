# Master Thesis — Progress Report, January → July 2026

> ⚠️ **Deliverable write-up, not an operating document.** For how to run the pipeline see
> [RUNBOOK.md](../RUNBOOK.md); for what it has proved, [CLAUDE.md](../../CLAUDE.md); for what is
> broken, [ISSUES.md](../ISSUES.md); for what is next, [TODO.md](../TODO.md).

**Repository:** `master-thesis` · **Author:** DucTrung1802
**Period covered:** 2026-01-01 → 2026-07-27 (last commit at time of writing)
**Source:** reconstructed from the git history (`git log`), commit bodies, and the `CONTEXT.md` handoff docs.

| Metric | Value |
| --- | --- |
| Commits in period | 517 |
| Pull requests merged | 54 (`#137` → `#190`) |
| Integration branch line | `main_v2` → `main_v3` (Mar) → `main_v4` (Apr–May) → `main_v5` (Jun) → back onto `main_v2` (from 30 Jun, current) |
| Feature branches in play | `web_scraper(_v2)`, `data_preprocessor(_v2)`, `data_postprocessor`, `database_driver`, `train_test_creator(_v2)`, `model(_v2)`, `evaluator`, `experiment`, `thread_manager` |

### Commits per month

| Month | Commits | Dominant theme |
| --- | --- | --- |
| Jan 2026 | 3 | Carry-over: exchange-rate data source |
| Feb 2026 | 17 | tsfresh → first CNN on VN-Index |
| Mar 2026 | 78 | CNN training infrastructure, TensorBoard, run sweeps |
| Apr 2026 | 93 | Full technical-analysis library + feature selection |
| May 2026 | 125 | Medallion DB pipeline, train/test creator, first positive test R² |
| Jun 2026 | 98 | Multi-source scrapers, bronze/silver/gold rebuild, LSTM, cross-sectional pivot |
| Jul 2026 | 103 | Fundamentals from filing PDFs (OCR), sentiment, silver FA tables |

---

## Executive summary

The year splits cleanly into three phases.

1. **Jan–Apr — build the machinery.** A CNN was stood up on the VN-Index, then the effort moved to the substrate underneath it: a complete technical-analysis feature library (~60 indicator functions written from scratch), a threaded PostgreSQL driver, and a repeatable train/test creator.
2. **May–Jun — build the data platform, then discover the model has nothing to learn.** A bronze/silver/gold medallion warehouse was built over three scraped sources (TradingView, CafeF, Simplize). Once models could be run cleanly and repeatedly, sweep after sweep returned the same verdict: **single-stock short-horizon return/direction is not predictable from price + technicals.** Out-of-sample R² ≈ 0; direction ROC-AUC ≈ 0.52.
3. **Jun–Jul — accept the finding and change the input, not the model.** The conclusion drawn was that the binding constraint is the *data*, not the architecture. The remainder of the period is a sustained data-acquisition campaign: corporate disclosure calendars, point-in-time share counts, a full news corpus, and — the largest single body of work — reading quarterly financial statements directly out of scanned Vietnamese filing PDFs via OCR, to a 100 % parse rate on the two tickers attempted.

The most important intellectual result of 2026 is a **negative one, established rigorously and documented**: no robustly tradable 5-day edge exists in the current regime on price + foreign flow alone, and text sentiment adds *negative* incremental value. Everything after that finding is an attempt to source genuinely orthogonal information.

---

## January 2026 — carry-over, exchange rates

**What changed.** Only 3 commits; a continuation of December work. The `EXCHANGE_RATE` macro series was re-pointed to a different source (`1275cd3`), and the `data_preprocessor` branch merged (PR #137).

**Issue.** The previous exchange-rate source was unsatisfactory as a model input.

**Solution.** Swapped the source and rewrote the scraping path (154 insertions / 82 deletions).

---

## February 2026 — from tsfresh to a CNN on the VN-Index

**What changed.**
- Started with a `tsfresh` notebook for automated time-series feature extraction (`a1b8881`), built `X`/`y`, then the train/test split and a feature-selected `X_train`.
- Pivoted mid-month: **"UPDATE: change to CNN, re-evaluate the whole flow"** (`ad5b889`) — a 6,491-line deletion against 723 insertions. tsfresh-style wide feature extraction was abandoned in favour of a convolutional model over the raw sequence.
- `cnn_vn_index_1` and `cnn_vn_index_2` notebooks; a live prediction was produced (`e52a772`: VN-Index close for 27/02/2026 = 1843.247).
- End of month: `REBUILD: scrape macroeconomics_gdp` — the scraper stack starts being rebuilt underneath the model.

**Issues.**
- Dataloader shapes were wrong — an explicit `CHECKPOINT: need to ensure the shape of dataloader` commit (`5e92781`) marks the blocker.
- The tsfresh pipeline produced an unwieldy feature matrix (single commits of 133k and 233k inserted lines are notebook outputs of that scale).

**Solutions.** Rebuilt the flow end-to-end around a CNN with an explicitly validated tensor shape, and re-scoped the feature story to something the model could actually consume.

---

## March 2026 — training infrastructure and disciplined run sweeps

The month where experiments became reproducible rather than ad hoc.

**What changed.**
- **Scraper/ingest for market indices:** rebuilt `_scrape_data_stock_market_vn_hnx_index_price()`, added `_hnx_index_order()`, fixed the VN-Index price/order scrapers, then completed the matching `_ingest_*`, `_clean_*` and `_transform_stock_market_vn_index()` steps.
- **Database driver:** `join_clause` support landed in `PostgreSQLDriver` (PR #138).
- **TensorBoard end-to-end** (`6834477`), then systematic run sweeps: `run_version_0` … `run_version_27`, later restarted as `version_0` … `version_53`, across notebooks `cnn_vn_index_1` … `cnn_vn_index_9`.
- Quality-of-life: `get_weekends()` utility, Windows notification on training completion (`f63274b`), pinned library versions in `requirements.txt`.

**Issues and how they were solved.**

| Issue | Resolution |
| --- | --- |
| Runs were not comparable — insufficient metadata logged | `REWORK: add information for log and train from start` (`f75534e`) — 152 files touched, 872 lines deleted; every prior run discarded and re-run under the new logging |
| Scaling was being validated at the wrong point | Two deliberate variants: `cnn_vn_index_1 - Validate after scaling` vs `cnn_vn_index_2 - Validate before scaling`, run side by side |
| Training runs overshooting | Dedicated `cnn_vn_index_2_overshoot` notebook + `cnn_vn_index_1 - handle overshoot` |
| Environment drift between runs | Pinned every library version; `REWORK: start training from the beginning` (`7a64a00`) deleted 168 files of stale run artefacts |
| Long training runs with no feedback | Windows toast notification on completion |

**Note on method.** The recurring pattern this month — *throw away every result and re-run from scratch once the logging changes* — is the right instinct and recurs later in the year.

---

## April 2026 — the technical-analysis library, then feature selection

The single most mechanically productive month: a complete TA-Lib-equivalent feature library, written in-project.

**What changed.**
- **~60 indicator functions implemented**, grouped by TA-Lib category and each committed individually:
  - *Overlap studies:* SMA, EMA, DEMA, TEMA, TRIMA, WMA, KAMA, T3, BBANDS, MIDPOINT, MIDPRICE, SAR
  - *Momentum:* ADX, AROON, BOP, CCI, CMO, MACD, MFI, MOM, PPO, ROC, RSI, STOCH, STOCHRSI, TRIX, ULTOSC, WILLR
  - *Volume:* AD, ADOSC, OBV
  - *Cycle:* HT_DCPERIOD, HT_DCPHASE, HT_PHASOR, HT_SINE, HT_TRENDMODE
  - *Price transform:* AVGPRICE, MEDPRICE, TYPPRICE, WCLPRICE
  - *Volatility:* ATR, NATR, TRANGE
  - Two were investigated and explicitly **rejected** (`NO ADD: add_hilbert_transform()`, `NO ADD: add_mesa_adaptive_moving_average()`) — a negative result recorded rather than silently dropped.
- A `REWORK:` pass over every function already written, standardising signatures (`add_sma`, `add_ema`, `add_bbands`, … all reworked on 21 Apr), followed by `UPDATE: clean old functions` removing 1,514 + 75 lines of superseded code.
- **`feature_selection_vn_index`** notebook; XGBoost-regressor-driven ranking; completed 28–30 Apr across 303 files.
- **Scraper:** VN-Index price scraping switched to page-by-page with a `_wait_until_text_not_equals` helper; `SwitchHandler` class introduced to turn individual scrape tasks on/off.

**Issues and how they were solved.**

| Issue | Resolution |
| --- | --- |
| Inconsistent function signatures across ~40 indicators written over two weeks | A dedicated single-day `REWORK:` sweep normalising all of them, then deleting the old implementations |
| `ta_functions.py` too slow with all indicators enabled | Two consecutive performance passes (`77eb3f2`, `993e883`) — the second removed 2,318 lines net |
| Scrape of the index price table was incomplete / timed out | Paginated scraping with an explicit text-change wait rather than a fixed sleep |
| Scrape tasks were all-or-nothing | `SwitchHandler` + `switch_config.json` — any single task re-runnable in isolation |

---

## May 2026 — the medallion warehouse, and the first positive test R²

**What changed.**
- **Web scraper generalised** from index-only to the whole market: `add_stock_market_data_scraping_tasks()`, then *all* HOSE stock prices (`863dfc3`, `147f758`), then enterprise-level data.
- **Medallion schema stood up:** `_create_tables()` for BRONZE, SILVER and GOLD; the per-layer `_ingest_ / _clean_ / _transform_` methods for indices and for enterprise daily price. Later in the month **all 3,427 lines of `create_table` functions were deleted** in favour of inferred schemas.
- **`PostgreSQLDriver` hardened:** multi-column joins, `IS / IS NOT NULL`, `IN / NOT IN` lists, and two rounds of threading work (PRs #144–#150).
- **`ThreadManager`** introduced and wired into ingestion.
- **`train_test_creator` v1 → v2:** windowed tensors, standardised `y`, `STRIDE` parameter, corrected `train_range`/`val_range`/`test_range`, saved train+val+test tensors and scalers.
- **`data_evaluator` v1** and **`result_evaluator`** — the evaluation half of the loop.
- **`0081ebf` — "UPDATE: first positive R2 on test set."** The first genuinely encouraging model result of the year.
- Macroeconomic scraper reworked at month end (USD/VND exchange rate, Vietnam interbank rate), standardised on TradingView.

**Issues and how they were solved.**

| Issue | Resolution |
| --- | --- |
| Ingestion of per-stock daily price was too slow to be practical | `ThreadManager` + multi-threaded `PostgreSQLDriver`; `_ingest_enterprise_daily_price()` rewritten to use it |
| Hand-written `CREATE TABLE` per table did not scale to a market-wide universe | Removed all of them (3,427 lines) and inferred schema from the dataframe |
| Columns with a single unique value polluted the feature matrix | `Drop columns with only one unique value` added to the post-processor |
| Train/val/test ranges were wrong | Explicitly corrected (`fff1cf2`), 316,437 lines of invalid generated data deleted |
| Notebooks unreviewable in diffs | `ipynb_to_txt.py` added so notebook content could be inspected as text |

---

## June 2026 — three data sources, a rebuilt warehouse, and the pivot

The turning-point month. Two things happen in parallel: the data platform is rebuilt properly, and the modelling effort runs out of road.

### 6.1 Scraper rebuild (`web_scraper_v2`)

- **TradingView, exhaustively:** link discovery across Stocks, Funds, Futures, Forex, Bonds, Economy (Crypto and Indices deliberately skipped), then data scraping for each.
- **Architecture:** `BaseScraper(ABC)` + a `@register_scraper` registry/factory. `WebScraper` → `TradingViewScraper`. A new source now needs only a subclass. Raw data reorganised under `raw_data/<source>/`.
- **Dividend adjustment** (`87518f7`): TradingView's *"Adjust data for dividends"* toggle is now clicked before scraping, so prices are back-adjusted.
- **Three new sources:** `CafeFScraper`, `GicsScraper` (official MSCI 2023 GICS taxonomy: 11 sectors / 25 groups / 74 industries / 163 sub-industries), and `SimplizeScraper` (one JSON endpoint, no browser, fully adjusted OHLC + true volume + foreign flow back to 2009).

### 6.2 Warehouse rebuild (`data_preprocessor_v2`)

- Bronze → silver → gold rebuilt with explicit casting, per-table switch guards, and a **`unified_schema` layer**: one table per ticker joining the gold stock rows to forward-filled macro context, datetime features and a supervised target.
- **Source-quality decision, VN30-validated:** Simplize is primary (fully adjusted OHLC, true volume, foreign flow); CafeF contributes matched/negotiated split and ownership; TradingView is OHLC fallback only.

### 6.3 Modelling (`model_v2`, `experiment`)

- PyTorch Lightning **LSTM** framework for VCB; R² added to the report; then a regularised **bidirectional-attention LSTM** with Huber loss.
- `experiment_history.csv` and `feature_groups.md` introduced — every config tried is logged with its metrics.
- **`experiment_1` – `experiment_3`:** breakout-event study, windowed-input model zoo (GBM/MLP/LSTM/GRU/CNN1D/Transformer), and walk-forward backtests.

### 6.4 Issues and how they were solved

| Issue | Root cause | Resolution |
| --- | --- | --- |
| Gold ingest took ~68 min for stocks | Profiling showed **88 %** of time in the insert, and within it `pandas.to_csv` on 910 columns — *not* the TA compute (~12 %) | `COPY FROM STDIN` fast path serialised via `pyarrow.write_csv`; **~4.5× speed-up, 68 → 15 min** |
| Silver/gold forex was entirely null | The scraper stores forex in a `value` column (like bonds/economy), but `_ingest_silver_forex` used the OHLC path and dropped it | Switched to the value path with a numeric cast |
| Unified tables exceeded **PostgreSQL's 1600-column limit** for short-history tickers | `_helper_macro_wide` joined all **621** tickers wide, producing 600+ macro columns | Scope the peer join to **same-GICS-sector peers only** (~7–49 tickers). Column counts fell to ~1,050–1,075 — and the context became semantically meaningful (sector peers, not the whole market) |
| Gold `COPY` aborted on a few exploding TA ratios | ±inf and out-of-range magnitudes vs PostgreSQL `REAL` | Sanitise: `>3.4e38` → NaN, subnormals `<1e-37` → 0 |
| Sector was being dropped from scraped stock data, flattening the output tree | Scraper read only one link sub-type | Read all three generically; output mirrors the sector tree and gains a `sector` column |
| **Model overfitting on VCB** | Too much capacity for the signal present | Top-40 features, hidden 48, dropout 0.4, weight decay 1e-3, attention pooling, Huber loss. **Overfitting resolved — but out-of-sample R² stayed ≈ 0** |
| A "multi-task direction head" was conceptually wrong | There is only one target (5-day return); direction is *derived* from it | Removed the head; `dir_acc`/AUC now computed from the return prediction. Deprecated rows marked in `experiment_history.csv` |

### 6.5 The pivot

`afa53fc` reframes the target from single-stock return to **cross-sectional relative return** — *which VN30 names beat the universe over the next h days*. Ridge, expanding walk-forward, turnover-based cost model.

> **Result:** rank IC ≈ +0.03, stable across a 10-year walk-forward; net-positive after 40 bps costs at monthly rebalancing (L/S ≈ +6.6 %/yr, long-only ≈ +3.9 %/yr).

And `experiment_3`'s verdict on the single-stock work:

> AUC 0.77 on the breakout signal is a **volatility-regime detector, not tradable alpha**. Walk-forward ML Sharpe 0.67 ≈ buy-and-hold 0.66. Cross-sectional VN30 long/short: **−0.53 Sharpe, −88 % drawdown** (the signal ranks by volatility, not direction). Across six short-horizon targets, none beats the market net of costs. **The binding constraint is the DATA, not the target or the model.**

---

## July 2026 — sourcing orthogonal data (fundamentals, news, OCR)

Having established that more model tuning would not help, the whole month goes into acquiring information the models did not previously have.

### 7.1 Closing out the modelling stage

- `src/model/common/`: reusable framework — dataset-by-content-hash, immutable run folders, GPU trainer, metrics (incl. zero-baseline and IC), and an append-only `runs/index.csv` leaderboard.
- **Three full lookback sweeps (1 → 30)**, 27 runs total:
  - `return_5day` regression — **no lookback beat the zero-baseline RMSE of 0.0357**
  - `direction_5day` classification — test `dir_auc` mean **0.519** (range 0.47–0.55)
  - `probability_gain_5pct_5day` — test ROC-AUC mean **0.545**, but val and test AUC are *anti-correlated* (lb15 = worst val, best test) → noise, not edge
- **`025956a` — the tradability ceiling, documented.** Foreign flow is the only signal surviving post-2021 (price factors reversed); best OOS classifier plateaus at AUC ≈ 0.52–0.53; gross L/S Sharpe 1.3–1.6 is destroyed by 65–78 %/leg weekly turnover. Turnover control flips net@20bps to +0.46 overall — but that is +1.46 (2017-20) vs **−0.51 (2022-26)**. Verdict: no robustly tradable 5-day edge in the current regime.

### 7.2 The data campaign

| Experiment | What it produced | Notable difficulty |
| --- | --- | --- |
| **experiment_4** | VCB financial-report **publish dates** 2009→now, from a priority chain: manual > in-PDF signing date > Vietstock news > CafeF filename | Q4 quarterly vs audited annual must be separated; assurance levels ordered Unaudited < Reviewed < Audited |
| **experiment_5** | VCB **point-in-time shares outstanding** | **Method was wrong and had to be rebuilt.** See below. |
| **experiment_6** | VCB **news corpus** — 1,629 unique headlines 2008→2026 with article bodies | Titles read from anchor inner text (the `title=""` attribute breaks on legacy embedded quotes) |
| **experiment_7** | **Quarterly financial statements**, any ticker, from CafeF's BCTC JSON API + a PDF layer | The bulk of the month's work — see §7.3 |
| **experiments 8 & 9** | Vietnamese OCR bake-off: PaddleOCR+VietOCR vs DeepDoc-ONNX+VietOCR | ONNX ~**10× faster** (1.4 vs 13.9 s/page) at equal accuracy → chosen for production |

**The experiment_5 correction — the month's most important self-catch.** The original method anchored on today's share count and walked CafeF's corporate-action log backwards. Cross-checked against VCB's own filed balance sheets, that log turned out to be **incomplete** — it omits three 2010–2012 capital increases. The series overstated mid-2011 by **31.8 %** and the pre-2010 base by **44 %**, silently inflating any pre-2013 market cap. It was correct only from mid-2014 onward, which is exactly why an earlier spot-check against post-2016 filings had passed.
*Resolution:* the authoritative source became the company's own filed charter capital (balance-sheet code 411) ÷ 10,000 VND par — complete and filing-backed across all 65 filed quarters. The action log is still fetched, but only to *date and label* each step.

### 7.3 Reading scanned Vietnamese filings — the OCR pipeline

The largest engineering effort of the year. CafeF's JSON API has gaps (VCB was missing 20 statement-quarters); the filing PDFs are the only place those figures exist — and **~90 % of VCB's filings are page scans with no text layer**, including recent ones (Q1-2026 is 53 pages of image).

**Built:** `cafef_pdf_scraper` (downloads the archive), `cafef_schema.py` (canonical chart of accounts), `cafef_pdf_parser.py` (one filing → statements), `cafef_financials.py` (archive → CSV panel), plus `onnx_ocr.py` and a vendored `_deepdoc` detector.

**Design decisions that mattered:**
- **Four charts of accounts, not two** — bank (TCTD), corp (DN), securities (CTCK), insurance (DNBH). They share *no* line items: every one has a "code 1" and it means something different in each. Template is detected by **fingerprinting** the ticker's line-item counts, not by classifying its business — GICS says what a company *is*, the chart of accounts says what the *filing looks like*, and they disagree (HVA sits in the securities industry group and files on the corporate template).
- **Template is a folder, not a column**, so every directory is schema-homogeneous.
- **`publish_date` read from inside the filing.** VCB's Q4-2025 covers the quarter ending 31 Dec 2025 but was not published until **27 Mar 2026** — joining fundamentals on the period end hands a model twelve weeks of look-ahead, every year.

**The hard problems, and their fixes:**

| Problem | Cause | Fix |
| --- | --- | --- |
| Statements produced 332 columns against a 90-column chart of accounts — nothing lined up in time | Columns keyed on *what OCR read* | Map parsed rows onto the canonical schema |
| Fuzzy match threshold could not simply be lowered | A shorter accounting name is a subsequence of a longer one more often than expected — "TỔNG VỐN CHỦ SỞ HỮU" scores 0.75 against "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU". At 0.72, **48 of 69 balance sheets** were rejected | Threshold fixed at **0.80**, plus containment guarding the *shorter* string |
| Statements vanished entirely | OCR mangles the statutory form code ("Mẫu B02" → "BU2"/"Bữ2"/"BUT") | **Four** independent signals: form code → header title (fuzzy, header-confined) → page contiguity → statement order |
| Scans stored `/Rotate 180` produced mirrored word boxes ("label left, values right" inverted) | PyMuPDF rasterises upright for OCR but returns boxes in unrotated space | Handle the mirroring explicitly; **do not** pre-rotate for ONNX (`get_pixmap` already applies `/Rotate`) |
| ACB 2013-15 cash flows vanished | A broken-CMap legacy-font text layer returns *long but garbage* strings, so the length-only gate skipped OCR | Two mojibake detectors: ≤2-char-token fraction (real VN ≈ 0.23 vs mojibake ≈ 0.45) and a diacritic-ratio gate for substitution mojibake |
| VietOCR invented a leading digit: `96.922.247` → `196.922.247`, stable across 200–600 dpi | Not a recogniser limit — the **detector box hugs the glyphs** | `CROP_PAD_PT = 2.0`. A bake-off (vgg_seq2seq, vgg_transformer, EasyOCR, Tesseract) showed all four read it correctly from a looser crop. *Check the crop before reaching for another model.* |
| Whole pages never OCR'd | A page whose entire text layer is a **signature stamp** clears the minimum-text gate and trips neither mojibake test | `_page_content_text` — 27 pages changed archive-wide |

**Three error classes that reconciliation *cannot* catch** (all of them still balance internally, and all were hit):
1. **Units** — most reports are triệu VND, but the 2009 ones are plain đồng: a 10⁶ error that reconciles perfectly.
2. **Cumulative vs standalone** — a semi-annual filing prints only the Jan–Jun column. Taking it printed VCB's Q2-2024 PBT as 20,835 bn instead of the true 10,116 bn. Derived as 6M − Q1 instead. Same for audited annuals: Q4 = FY − (Q1+Q2+Q3).
3. **Signs** — CafeF stores expenses as positive magnitudes; filings print them in parentheses.
Only a **magnitude check against neighbouring quarters** catches 1 and 2; that guard now runs before any write.

**Coverage achieved.** Scored against the era in which filings actually exist (a quarter before a ticker's first filing can never be `pdf` — it exists only in CafeF's tabs):

| Ticker | Span | Cells | From PDF |
| --- | --- | --- | --- |
| VCB | Q3-2008 → Q1-2026, 71 quarters | 213 | **213/213 (100 %)**, 210 dated |
| ACB | Q1-2010 → Q1-2026, 65 quarters | 195 | **195/195 (100 %)** |

The ACB figure went `98 → 161 → 186 → 189 → 193 → 195` across five successive hardening passes, each fix traced to a specific cause and scoped so it could not reach a quarter that already parsed — validated by re-running the statements that already worked.

### 7.4 Warehouse and downstream, July

- CafeF bronze split into one raw-faithful table per scraper folder; **every key split from `EXCHANGE:TICKER` into separate `exchange` + `ticker` columns**; column names standardised end-to-end (`close_adj` → `close_adjust`, `f_*_vol` → `foreign_*_volume`, …).
- Silver rebuilt as a four-way CafeF daily join (2,388,368 rows, no fan-out) + the full GICS tree (99.7 % of rows classified); renamed `stocks` → `stocks_basic`.
- **`silver.stocks_basic_financials_bank`** — daily price × quarterly financials via `merge_asof` **on `publish_date`**, so every price day carries the most-recently-*published* quarter: zero look-ahead by construction.
- **`silver.stocks_basic_financials_bank_fa`** — 26 fundamental indicators (P/E, P/B, ROE, ROA, EPS, NIM, CIR, LDR, growth…). Verified against VCB's latest: P/E 14.13, P/B 2.56, ROE 22.2 %, NIM 2.69 %.
- **`src/sentiment`** — PhoBERT 3-class Vietnamese scorer over the news corpus, plus price-prediction experiments with a *purged, embargoed* walk-forward splitter.

**July's other headline negative result.** Text sentiment is learnable only when the label is itself derived from text (QWK 0.61). Every price-grounded target fails: `close[N+5]` beat a random walk 0/7 times, direction ≈ 0.49, P(≥5 % jump) AUC ≈ 0.5. And the incremental ablation is worse than neutral — **adding sentiment on top of price/TA makes it worse** (direction AUC 0.543 → 0.534; 5-level QWK 0.175 → 0.045, as the 768-dim text embedding swamps 14 price features).

**Infrastructure bug worth recording.** `ThreadManager` took only a `power` percentage, and the formula `cpu * power/100 * 0.4` produced a *fractional* worker count (2.4 on a 20-core box) — so the pool ran ~2 threads regardless of the machine, and the full-universe CafeF scrape crawled. Fixed with an explicit `max_workers` (default 16), guarded by `max(1, int(...))`; a latent `AttributeError` on invalid power was fixed at the same time. A follow-up caught that `CafeFNewsScraper`/`CafeFPdfScraper` overrode `__init__` without forwarding `max_workers`, raising `TypeError` the moment it was passed.

---

## Cross-cutting themes

**1. Recurring failure modes.** Three classes of bug appear repeatedly and are worth naming:
- *Silent correctness failures that pass their own checks* — the share-count reconstruction, the cumulative-column reads, the units errors, the sign convention. Each reconciled perfectly while being wrong. The countermeasure adopted — **a magnitude check against neighbours, plus an independent cross-source confirmation** — was arrived at the hard way.
- *Configuration that quietly degrades* — the fractional thread count, the `LIKE` wildcard where `_` matched any character (`lb2__%` also matched `lb20`), silver columns round-tripping to VARCHAR. All produced working-but-wrong systems.
- *Scale limits met late* — PostgreSQL's 1600-column ceiling, `to_csv` on 910 columns, ~97 GB of PDFs.

**2. Discipline that paid off.** Deleting every result when the logging changed. Committing rejected indicators as `NO ADD:` rather than dropping them. Tracking `experiment_history.csv` and `runs/index.csv` in git. Writing `CONTEXT.md` handoff docs (five of them, 183 KB total) that record *why*, not just *what*. Nothing is written to a financial statement unless it reconciles against the filing's own printed subtotals.

**3. The intellectual arc.** Model → better model → better features → better data → *the data is the constraint* → go get different data. Each step was forced by evidence rather than assumed, and each negative result was documented rather than buried. That is the defensible core of the thesis.

---

## Open items as of 27 July 2026

1. **Unfinished regression on the OCR pipeline (flagged in-repo).** Fixes 8 (`CROP_PAD_PT`) and 9 (`Y_TOL 3.0 → 4.0`) change the crop and line grouping on **every** ONNX page for **every** ticker, and their regression run was started three times and lost each time. `regress_cf` (16 accepted cash flows, expect 16/0) and `verify_cascade` must be re-run — ~50–60 min — before the parser is trusted beyond ACB. Instructions are in `src/web_scraper/CONTEXT.md`.
2. **Parse gates prove subtotals, not every line.** A PDF-sourced row can be thinner than the CafeF row it replaces (28 items vs 47), and an interior line can be wrong while the statement reconciles. Consumers needing a minor line item should cross-check `from_api`.
3. **Coverage is two tickers.** VCB and ACB are complete; the bank template is the only one parsed. Three templates (corp, securities, insurance) have schemas but no parsed tickers. Downstream, `silver.*_financials_bank_fa` currently holds VCB only.
4. **Sentiment corpus is thin at the ticker level** for conclusions — news exists for the full 777-ticker universe now (~405 k rows), but the sentiment experiments ran on 3 tickers.
5. **The tradability question stands unresolved in the affirmative direction.** The cross-sectional VN30 strategy is the only net-positive result, and its edge is portfolio-level, decays post-2021, and is turnover-sensitive.
6. **Uncommitted working changes** at time of writing: `src/web_scraper/cafef_financials.py`, `src/web_scraper/cafef_pdf_parser.py`.

---

*Generated 2026-07-28 from the repository's git history.*
