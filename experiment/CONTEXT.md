# Experiments Context — Signal discovery → tradability → point-in-time data (VCB/VN30)

Single-document summary of **all methods and all results** across the seven
experiments:

- **experiment_1** — signal discovery (does a "next-5d ≥ +5%" signal exist?).
- **experiment_2** — windowed-input model study (does history/sequence help?).
- **experiment_3** — does the signal actually *trade*? (costed walk-forward
  backtests) and *which target* is tradable.
- **experiment_4** — VCB financial-report **publish/disclosure dates** (2009→now),
  scraped for point-in-time / look-ahead-safe modelling.
- **experiment_5** — VCB **shares-outstanding (KLCP) history** (2009→now), from **filed
  charter capital**, for point-in-time market cap.
- **experiment_6** — VCB **company-news / disclosure headlines** (2008→now),
  categorised, scraped from CafeF's event feed for a point-in-time event stream.
- **experiment_7** — **financial statements** (balance sheet / income statement /
  cash flow), full quarterly line-item history 2008→now; works for **any ticker**.

Each experiment folder has its own `README.md` with the full detail; this file is
the index across all of them.

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

# Experiment 3 — Does the signal actually trade? (costed walk-forward + target search)

The real test of the AUC-0.77 signal: **does it make money after costs?** Walk-forward,
purged, no look-ahead; costs charged per side (base 15 bps). VN reality: single-stock
shorting is effectively unavailable on HOSE → only long-only is real.

### 3.1 Single-stock VCB timing — `experiment_3/vcb_walkforward_backtest.py`
Expanding walk-forward (retrain/126d, 28 folds, OOS 2012–2026); top-decile signal,
long/flat 5-day hold, vs Buy&Hold and a 20-day momentum rule.

| Strategy @15bps | Sharpe | CAGR |
|---|---|---|
| ML timing | 0.67 | 10.8% |
| Buy & Hold | 0.66 | 15.9% |
| 20-day momentum | 0.59 | — |

→ Timing one trending stock **ties just holding it** — no alpha.

### 3.2 Cross-sectional VN30 long-short — `experiment_3/vn30_xsec_longshort.py`
Pooled walk-forward, rank VN30 by P(5d≥+5%), long top-6 / short bottom-6, net 15bps:
**−12% CAGR, Sharpe −0.53, −88% DD** vs market +16.4% / 0.85. The signal ranks stocks by
**volatility**; longing high-vol / shorting calm names **loses** (and shorting isn't
allowed anyway).

### 3.3 Which TARGET is tradable? — `experiment_3/target_comparison.py`
Pooled VN30 walk-forward; 6 targets by rank-IC + long-only top-6 vs equal-weight market:

| target | rank-IC | top6 Sharpe | excess vs market |
|---|---|---|---|
| **rel5** (market-relative 5d) | **0.052 (best)** | 0.25 | −0.58 |
| rel10 | 0.050 | 0.18 | −0.65 |
| bin5 (old 5d≥+5%) | 0.044 | 0.64 | −0.19 (least bad) |
| ret5 | 0.044 | 0.50 | −0.33 |

→ **`rel5`** (beta-neutral ~1-week relative return) is the most predictable and correct
target, but **no target's long-only portfolio beats the market net of costs**; IC ≈ 0.05
is near the noise floor. **The binding constraint is the DATA, not the target or model.**

---

# Experiment 4 — VCB financial-report publish (disclosure) dates

Motivated by experiment_3's conclusion that **orthogonal data is the lever** (esp. an
earnings/disclosure calendar). Recovers VCB's financial-statement **publish dates,
2009 → present**, so fundamentals can be joined point-in-time (no look-ahead).

- **Single script** `experiment_4/scrape_vcb_publish_dates.py` (stdlib + PyMuPDF).
  Output: **6-column** `vcb_quarter_publish_dates.csv` = `year, Q1, Q2, Q3, Q4,
  final_year` (+ a long detail CSV with assurance/confidence/source/evidence).
- **Columns = distinct reports** (Unaudited < Reviewed < Audited): Q1/Q3/Q4 unaudited
  quarterly, **Q2** semi-annual *soát xét* (Reviewed), **final_year** whole-year
  *kiểm toán* (Audited). Q4 quarterly (~late Jan) and the audited annual (~late Mar)
  are separate documents/columns.
- **Sources, in priority order:** manual overrides → in-PDF signing date (read from
  the report PDF, tolerant of legacy TCVN3 font) → Vietstock news (HOSE disclosure
  article date) → CafeF filename date → Vietstock upload date. The file/upload APIs
  only keep bulk re-upload dates for old years, so news + in-PDF recover 2009–2012.
- **Status:** 79/90 cells high-confidence, 4 approximate (early-2010s earnings-news),
  3 unavailable (2009 Q3 + 2009/2010 audited annuals are scanned image PDFs → need OCR
  or manual entry via `vcb_manual_overrides.csv`). Cadence: Q1 ≈ late Apr, Q2 ≈ mid-Aug,
  Q3 ≈ late Oct, Q4 ≈ late Jan, final_year ≈ late Mar / Apr.

---

# Experiment 5 — VCB shares-outstanding (KLCP) point-in-time history

Second orthogonal-data piece: the **listed / outstanding share count**, 2009 → now, so raw
price can be joined to market cap / turnover / free-float without look-ahead.

**⚠️ The original method was wrong.** It anchored on today's count and walked CafeF's
corporate-action log (`LichSuKien.ashx`) backwards. **That log is incomplete** — it omits
three of VCB's 2010-2012 capital increases. Cross-checked against VCB's own filed balance
sheets (experiment_7), the old series overstated mid-2011 by **+31.8%** (2,317,388,397 vs the
filed 1,758,754,000) and the pre-2010 base by **+44%** (1.74bn vs 1.21bn) — silently inflating
any pre-2013 market cap. It was right only from mid-2014 on, which is why a spot-check against
post-2016 filings had passed.

**The method now — filed charter capital.** The authoritative source is the company's own
"Vốn điều lệ" (balance-sheet code `411`), read from the quarterly statements via the same
CafeF BCTC API experiment_7 uses: `shares = charter_capital / 10,000` (10,000 VND par).
Complete and filing-backed for all 65 filed quarters. The action log is still fetched, but
**only to date and label** the steps: if an action's factor matches the observed jump and its
ex-date is within ~15 months (charter capital registers only once shares are issued, which
lags the ex-date), use the exact **ex-date**; otherwise fall back to the **quarter-end** on
which the new charter capital first appears (conservative — no look-ahead).

**Result** — 11 steps: 1,210,086,026 (Q1-2009 baseline) → 1,322,371,500 (Q3-2010, *unlogged*)
→ 1,758,754,000 (2010-12-13 rights 100:33) → 1,969,804,500 (Q3-2011, *unlogged*) →
2,317,417,100 (Q1-2012, *unlogged* — the Mizuho placement) → 2,665,020,300 (2014 bonus) →
3,597,768,600 (2016 bonus) → 3,708,877,400 (2019 GIC/Mizuho) → 4,732,516,600 (2021 stock div)
→ 5,589,091,300 (2023 stock div) → **8,355,675,094** (2025 stock div). The three
`unlogged_capital_increase` rows are exactly what CafeF's action log is missing — real (the
filings prove them), just undated beyond their quarter.
`vcb_shares_milestones.csv` pins exact counts (charter capital is filed in millions, so
`/10,000` is only good to ~±50 shares).

**Point-in-time:** `shares(d)` = last row with `effective_date ≤ d`;
`market_cap(d) = raw_close(d) × shares(d)` (**raw** close — the count carries the dilution).

---

# Experiment 6 — VCB company-news / disclosure headlines (categorised)

Third orthogonal-data piece. Scrapes VCB's full **company-news & disclosure headline
stream** from CafeF, tagged by category, 2008 → now — a point-in-time event feed
(headline counts, event flags, sentiment, announcement dates) joinable without look-ahead.

- **Source:** `cafef.vn/du-lieu/tin-doanh-nghiep/vcb/event.chn`. All category tabs
  (`#a0..#a5`) hit one AJAX endpoint returning an HTML fragment:
  `Ajax/Events_RelatedNews_New.aspx?symbol=VCB&floorID=0&configID=<0-5>&PageIndex=n&PageSize=30&Type=2`.
  PageSize caps at 30 → paginate until empty. Title from anchor **inner text** (the
  `title=""` attr breaks on legacy embedded quotes).
- **Categories (configID):** 0 all · 1 SXKD & analysis · 2 dividends/record-date ·
  3 personnel · 4 capital increase/treasury · 5 major & insider shareholder txns.
  Method: scrape 1..5 (true category), then backfill category 0 (uncategorised), dedup by URL.
- **Result** (`experiment_6/scrape_vcb_news.py`, one stdlib script, two stages: list
  headlines → fetch each article's content): **1,629 rows, 2008-01 → 2026-07** — 896
  editorial, 727 disclosure, 6 dead-link errors; 702 carry a filing `pdf_url`. Categories:
  business_results_and_analysis 770, general_uncategorized 561, major_and_insider 101,
  personnel_changes 88, capital_increase_and_treasury 71, dividends_and_record_date 38.
  Categories 2/4 cross-validate experiment_5's corporate actions and experiment_4's cadence.
- Output `vcb_news.csv` (order, timestamp, **type**, headline, **category**, **content**,
  url, pdf_url). `type` (editorial/disclosure/error) = provenance; `category` = topic —
  orthogonal, both kept. `symbol=VCB` is the only ticker-specific bit → extends to any code.
  PDFs referenced by URL only (download via PyMuPDF like experiment_4 if filing text needed).

---

# Experiment 7 — Financial statements (quarterly, any ticker)

Fourth orthogonal-data piece: **the fundamentals themselves** — every line item of all three
statements, quarterly, 2008 → now, for **any listed code**. Joined to experiment_4's publish
dates they become point-in-time safe.

### Naming (Vietnamese → standard accounting English)
`CDKT` Cân đối kế toán → **`balance_sheet`** (sections `assets`/TN, `liabilities_and_equity`/NV);
`KQKD` Kết quả kinh doanh → **`income_statement`** (P&L); `LCTT` Lưu chuyển tiền tệ →
**`cash_flow`** (sections `operating`/HDKD, `investing`/HDDT, `financing`/HDTC).
`HDKD` — the id in the `#table_HDKD` pager xpath — is the cash-flow *operating section*, **not**
a separate report.

### All history in one call
The page's tabs load via `Ajax/FinancialAjax.aspx?tab=<candoi|ketqua|luuchuyen>`, which calls a
JSON API: `apiweb.cafef.vn/api/v2/BCTC/GetReportCDKT` (TN|NV), `…/v1/BCTC/GetReportDetail`
(KQKD), `…/v1/BCTC/GetReportLCTT` (HDKD|HDDT|HDTC), with `&pageSize=<count>&TypeTime=QUY`.
`value.count` = quarters available for that ticker (VCB 70, FPT 77) → request exactly that
many; the period-pager button is just pagination. Two JSON shapes (nested for CDKT/LCTT, flat
for KQKD) are normalised.

### Result — `experiment_7/scrape_financials.py`, one stdlib script, **6 generic files**
The ticker is a **column, not a filename**: `balance_sheet.csv`, `income_statement.csv`,
`cash_flow.csv` + a `<report>_manual.csv` each. One WIDE table per report (each statement has
its own line items). Columns: `symbol, exchange, period, year, quarter, source`, then one column
per line item named `<item_code>__<slug>` (e.g. `110__tien_mat_vang_bac_da_quy`).
VCB: **71 quarter rows**, **Q3-2008 → Q1-2026**, oldest first — a **contiguous grid** (gaps are
`source=missing` rows, never skipped or zero-filled); 85 / 26 / 47 line items.
Validated: 2025 identities hold (NII 58,771 = 105,216 − 46,445; PBT 44,020; PAT 35,198 bn VND)
and the 4 quarterly income statements sum exactly to the annual figure.

**Any ticker, any sector.** `--symbol FPT` **accumulates** — replaces only that symbol's rows,
leaves others' alone, idempotent — so a multi-ticker panel builds up one run at a time.
Endpoints/sub-types are identical for banks and non-banks; only line items differ, read from
each ticker's own template (VCB: bank, 85 BS items, *direct* cash flow, starts with interest
income; FPT: non-bank, 132 items, *indirect*, starts with revenue, 81 quarters from Q1-2006).
**Line items are keyed by the full `<code>__<slug>` name, never the bare code** — code `1` is
interest income for a bank but revenue for a non-bank, so they stay separate columns and each
row is blank outside its sector (VCB+FPT → 152 rows × 213 items, sparse by design).
**Exchange** comes from CafeF's master list (`Search/company.json`, 2,556 codes) via `CenterId`:
1=HOSE, 2=HNX, 8=OTC, 9=UPCOM.

### Three layers: manual > pdf > scraped
Values merge **cell by cell**; the `source` column records which layer won
(`scraped`/`pdf`/`manual`/`missing`).

| layer | file | what it is |
|---|---|---|
| scraped | — (CafeF BCTC API) | the base |
| **pdf** | `<report>_pdf.csv` | read off the company's **actual filing** by `read_pdf.py` |
| **manual** | `<report>_manual.csv` | hand-entered; **beats everything** |

**`read_pdf.py`** (`--period Q2-2014 [--render|--list]`) finds the consolidated ("hợp nhất")
report on CafeF, locates each statement and parses it. Rows are rebuilt from **word
coordinates**, not the raw text stream — PyMuPDF emits label fragments out of order, and a
line-based read silently put *"Chi phí hoạt động khác"* into *"Chi phí hoạt động"*; the period
columns are found by clustering the x-positions of every number. **Nothing is written unless it
reconciles** against the statement's own printed subtotals. ~2/3 of VCB's older filings are
**scanned images** with no text layer; `--render` rasterises the pages so the figures can be
transcribed by eye/OCR into `<report>_pdf.csv`.

**Filled so far** (each reconciled against the filing's own totals): balance_sheet **Q2-2011,
Q2-2014**; income_statement **Q4-2010, Q1-2011, Q2-2011**; cash_flow **Q2-2011, Q2-2014**.
Q2-2011 cross-validated experiment_5 and exposed its 31.8% share-count error; Q2-2014's filed
charter capital (23,174,171 mn → 2,317,417,100 shares) independently confirms exp_5's Q1-2012
step. Remaining VCB gaps: BS 3 (Q3/Q4-2008, **Q2-2024**), IS 4 (Q2-2009, Q1/Q3-2010,
**Q2-2024**), CF 15 — Q3/Q4-2008 have **no document at all**; the rest are scanned PDFs.

### ⚠️ Caveats (verified — read before modelling)
1. **Quarterly cash flow is cumulative YTD** — it resets each January and Q4 = the full year
   (2023: 26,870 → 56,751 → 83,233 → 108,116). **Difference consecutive quarters** for
   standalone values. The income statement **is** already standalone (quarters sum to annual)
   and the balance sheet is a stock — neither needs this.
2. **Gaps are real source gaps, not parse bugs.** CafeF omits **Q2-2024** entirely (market-wide
   — FPT too) and returns literal all-zero rows elsewhere. Both are emitted as blank
   `source=missing` rows, **never as 0**. Dense usable history effectively starts ~**2012**.
3. **Two error classes reconciliation CANNOT catch** — both balance internally, so only a
   **magnitude check against neighbouring quarters** finds them (`read_pdf.py` runs one before
   writing; sanity-check any hand-fill the same way):
   **(a) cumulative vs standalone** — a Q2/Q3/Q4 report prints both the quarter and the YTD
   column; the income statement needs the *standalone quarter*, cash flow the *cumulative* one.
   Take the wrong one and the number reconciles perfectly and is still wrong — this is why
   Q2-2009 was **rejected rather than written**.
   **(b) units** — most reports are Triệu VNĐ (×10⁶), but VCB's 2009 ones are plain đồng.

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
6. **Trading (experiment_3):** the ranking edge is real but **not tradable alpha** — it's
   a volatility-regime detector. Costed walk-forward: single-stock timing ties Buy&Hold;
   cross-sectional long-short loses; no long-only target beats the market net of costs.
7. **The binding constraint is DATA, not model/target.** Best label to pursue = **`rel5`**
   (market-relative ~1-week return). The lever is **orthogonal data** — foreign flows,
   earnings/disclosure calendar + surprises, fundamentals/valuation.
8. **experiments 4–7 build the orthogonal-data pieces:** a point-in-time **disclosure
   calendar** (exp_4) so fundamentals align to when they became public, a point-in-time
   **shares-outstanding series** (exp_5) so raw price → market cap / turnover / free-float
   are computable without look-ahead, a categorised **news/event stream** (exp_6) for
   headline-count / event-flag / sentiment features, and the **financial statements**
   themselves (exp_7) — which, joined to exp_4's publish dates, give look-ahead-safe
   fundamentals/valuation.

> AUCs are single chronological-split point estimates (small positive counts →
> ±0.03–0.05 variance). Most CSV outputs are gitignored and regenerated by the scripts;
> experiment_4's dated CSVs are tracked.

## File index
- `experiment_1/README.md` — signal discovery detail
  - `breakout_events/` — events, signal search, TA sweeps, importance/trading
  - `vn30_signal/` — VN30 per-ticker + pooled ; `dl_signal/` — DL shoot-outs
- `experiment_2/vcb_seq20x1053_models.py` — windowed model zoo (VCB/VNM/VIC)
- `experiment_2/vcb_lookback_sweep.py` — VCB lookback sweep
- `experiment_3/README.md` — backtests + target search detail
  - `vcb_walkforward_backtest.py`, `vn30_xsec_longshort.py`, `target_comparison.py`
- `experiment_4/README.md` — disclosure-date scraper detail
  - `scrape_vcb_publish_dates.py` (one script) → `vcb_quarter_publish_dates.csv`
    (+ `_detail.csv`); `vcb_manual_overrides.csv` for hand-entered dates
- `experiment_5/README.md` — shares-outstanding (KLCP) reconstruction detail
  - `scrape_vcb_shares_outstanding.py` (one script) → `vcb_shares_outstanding.csv`
    (+ `vcb_corporate_actions.csv`); `vcb_shares_milestones.csv` for exact filed counts
- `experiment_6/README.md` — categorised company-news / disclosure feed **with content**
  - `scrape_vcb_news.py` (one script: list headlines + fetch article content) →
    `vcb_news.csv` (order, timestamp, type, headline, category, content, url, pdf_url)
- `experiment_7/README.md` — financial statements (balance sheet / income statement / cash flow)
  - `scrape_financials.py` (one script, `--symbol <TICKER>`, all history via the BCTC JSON
    API) → three wide CSVs shared by all tickers: `balance_sheet.csv`, `income_statement.csv`,
    `cash_flow.csv` (rows = symbol × quarter, columns = line items `<item_code>__<slug>`)
    + a `<report>_manual.csv` hand-fill template per report (manual beats scraped)
