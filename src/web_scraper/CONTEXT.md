# Context — `src/web_scraper` (raw-data acquisition layer)

> Handoff notes for a new session. Describes the web-scraping subsystem: how the
> data sources are structured, what each pulls, how they are driven, and where
> output lands. This is the **bronze-input** stage — it writes CSV/xlsx/PDF under
> `raw_data/<source>/`, which `src/data_preprocessor` then ingests into
> `bronze_schema`. Verify anything before acting on it — the code and
> `src/switch_config.json` are the sources of truth.

## 1. Big picture / pipeline

```
src/switch_config.json  ──(feature flags)──┐
                                            ▼
src/main.py  ──►  TradingViewScraper.scrape()   ← universe authority (link CSVs)
                       │                            + OHLCV per symbol (Selenium)
                       ▼
                  CafeFScraper.scrape()         ← per-stock fields TV lacks (requests)
                  CafeFPdfScraper.scrape()      ← the filing PDFs (requests)
                  CafeFNewsScraper.scrape()     ← company-news / disclosure feed (requests)
                  SimplizeScraper.scrape()      ← validated daily-panel backbone (requests)
                  GicsScraper.scrape()          ← MSCI GICS taxonomy (independent)
                       │
                       ▼
                  raw_data/<source>/...  (CSV + PDFs + the raw GICS .xlsx)
                       │
                       ▼
             src/data_preprocessor → bronze_schema (see its own ingest code)
```

- **TradingView is the universe authority.** CafeF and Simplize both derive their
  `(exchange, symbol)` list from the TradingView **stock link CSVs**
  (`get_stock_symbols()` reads `raw_data/trading_view/links/stocks/**/*.csv`),
  so TV must run first; the other two enrich the same universe. `main.py` runs
  them in that order deliberately.
- **Each source writes one CSV per stock** (except GICS = one taxonomy CSV, and
  TV links = one CSV per filter leaf). All writers use **temp-file + atomic
  `os.replace`** so an interrupted run never leaves a partial file that
  `skip_existing` would treat as complete.
- Everything is **flag-gated** by `SwitchHandler` reading `src/switch_config.json`
  (see §5). Note the top-level `"web_scraper": false` in the committed config — the
  whole scraper stage is currently OFF by default; individual asset-class subtrees
  are pre-wired to `true` for when it is turned on.

### What is under `raw_data/cafef/` and what writes it

Every folder here has exactly one scraper that produces it — nothing is orphaned. If a
folder appears that is not on this list, its producer is gone and the data cannot be
regenerated; delete it or restore the scraper rather than modelling on it.

| folder | written by | scope so far |
|---|---|---|
| `price/`, `foreign/` | `cafef_scraper.py` | 781 tickers (HOSE+HNX+UPCOM) |
| `order_stats/` | `cafef_scraper.py` | 777 tickers (HOSE+HNX+UPCOM) — full universe |
| `prop_trading/` | `cafef_scraper.py` | 777 tickers queried (full universe); 431 have data, 350 have no prop-desk trades (history from ~2023) |
| `insider_txn/` | `cafef_scraper.py` | VN100 (HOSE) |
| `news/` | `cafef_news_scraper.py` | 777 tickers (full universe); ~405k rows, ~495 MB |
| `pdfs/` | `cafef_pdf_scraper.py` | all VN100 (100 tickers, all years) + 8 non-VN100 leftovers = 108 folders, ~97 GB |
| `financials/` | `cafef_financials.py` (§3a) | the 12 schemas + `templates.csv`; statements per template — **the only part of `raw_data/` that is TRACKED in git** (it is 0.3 MB and costs hours of OCR to rebuild) |

**⚠️ EVERY TICKER HAS A "PDF ERA", AND COVERAGE MUST BE READ AGAINST IT, NOT AGAINST THE GRID.**
The written grid spans every quarter ATTEMPTED, which reaches back further than CafeF's document
list does — **ACB's earliest filing is Q1-2010, VCB's is Q3-2008.** Quarters before a ticker's
first filing can never be `pdf` no matter how good the parser gets; they exist only in CafeF's
tabs. Scored on the raw grid ACB reads 88% and VCB 91% and both look broken; scored on the span
where filings actually exist, **both are 100%**. Use `source == 'missing'` to find real gaps —
`cafef` on a pre-archive quarter is the system working as designed.

**Where the statement parser stands — VCB, Q3-2008 → Q1-2026 (71 quarters, its whole PDF era).**
Re-parsed 2026-07-29 with the four new relaxations, 238 min:

| report | quarters | pdf | cafef | was pdf | gain |
|---|---|---|---|---|---|
| balance_sheet | 71 | **68** | 3 | 57 | +11 |
| income_statement | 71 | **67** | 4 | 46 | +21 |
| cash_flow | 71 | **69** | 2 | 61 | +8 |

**204 of 213 read from the filings, +40, and NOTHING was lost** — no quarter that read from a
filing before stopped doing so. All 6 formerly HOLLOW cash flows (a `pdf` row with an empty
closing balance) now carry one that is checked. The 11 remaining are filled from CafeF's tabs, so
every quarter still has data.

### ⚠️ VCB'S COVERAGE IS GOOD AND ITS Q4 CLOSING BALANCES ARE NOT — verify before use

The same checks that gave ACB 57/58 and 48/54 give VCB **45/62 on the opening/closing chain** and
**32/54 against CafeF's tab**. That is not a rounding difference, and two Q4s carry a specific,
diagnosable signature — **a closing balance identical to another quarter's OPENING**:

| quarter | our closing | equals | CafeF says |
|---|---|---|---|
| Q4-2017 | 157,564,955 | Q3-2017's **opening** | 258,262,431 |
| Q4-2023 | 412,235,294 | Q1-2023's **opening** | 371,827,129 |

Neither agrees with the following year's openings either (2018 reads 305,534,247 and 2024
372,818,730 across all four quarters, which is what a 1-January opening should equal). A closing
that lands on an opening row is a wrong-row read, not OCR noise. **Do not trust a VCB Q4/annual
closing balance until this is diagnosed** — the coverage is sound, the annual cash position is
not. ACB shows no such pattern, so it is VCB's annual layout, not the cascade.

Some VCB-vs-CafeF differences are CafeF's fault, already confirmed: Q4-2011 (ours 124,705,018),
Q2-2019 (ours 209,368,161), Q4-2025 (ours 541,688,802, which the Q1-2026 filing prints as its
opening). Others are ours. **The comparison alone does not say which side is wrong** — render the
page.

**⚠️ Some VCB filings have a DAMAGED PAGE TREE** ("non-page object in page tree"). `scan()` already
loads pages by index to survive it, but ad-hoc tooling that iterates the document will silently
stop at the bad node.

**The 213/213 this section used to claim was measured differently and is not comparable:** The written grid starts at Q4-2006
(an FY-2006 annual report sits in the archive) and those 7 pre-listing quarters are blank:
VCB IPO'd Dec-2007 and listed Jun-2009, and neither the filings nor CafeF hold anything before
Q3-2008. On the raw 78-quarter grid that reads 91%; there is no data to be had.

Two things got it from 81% to complete, and both are structural rather than tuning:
- **Q4 is taken from the AUDITED ANNUAL report** (CafeF files it under quarter 5). Same period,
  far better-produced document. Sound as-is for the balance sheet (31 Dec *is* Q4) and the cash
  flow (cumulative either way) — but the P&L covers the WHOLE YEAR, so Q4 = FY − (Q1+Q2+Q3),
  and the document is tagged `annual` so `_decumulate` does that. Without it every Q4 income
  statement would be a full-year figure in a quarterly row.
- **CafeF's tabs are the fallback** (`from_api`). For a quarter whose scan is unreadable this is
  the BETTER source, not the lesser one: the tabs are keyed by the same item CODES the schema
  was built from, so a value lands on its canonical column *exactly* — no OCR, no fuzzy match.
  The PDF is still read first (CafeF transcribes, has gaps, rounds), and every row records which
  it was in `source`: `pdf` (164 quarters, 77%) or `cafef` (49, 23%).

**⚠️ BUT CAFEF'S QUARTERLY TABS ARE NOT UNIFORMLY RIGHT, AND Q4 IS THE WEAK ONE.** For VCB
2011-13, 2015 and 2020 the four quarterly figures **do not sum to CafeF's own ANNUAL tab**, and
the entire shortfall is in Q4 — 2013 reads Q4 = 1,752,672 where the annual implies ~3,216,740.
Anything still filled from `from_api` for a Q4 may carry this. Eight wrong CafeF values are now
confirmed against the filings: ACB Q4-2012 PBT (−215,386 vs **−374,433**), ACB
Q1-2019 closing cash (30,335,949 vs **36,335,949**), **ACB Q3-2023 closing cash (112,599,304 vs
112,718,456** — the filing prints VII = 112.718.456, its four components sum to it, and
103,510,228 + 9,332,621 − 124,393 closes on it), VCB Q4-2011 (124,304,308 vs **124,705,018**),
VCB Q2-2019 (207,056,920 vs **209,368,161**), VCB Q4-2025 (540,799,468 vs the **541,688,802** the
Q1-2026 filing prints as its opening), VCB Q2-2021 PBT, and the Q4-vs-annual class above. Prefer
`annual − (Q1+Q2+Q3)` for a Q4 whenever the annual and all three quarters came from the PDF —
which is what `_decumulate` already does, since it runs BEFORE the fallback and can only subtract
PDF-parsed quarters.

**Two more places CafeF is the lesser source:** it stores a literal **0** for lines the filing
prints as a dash (46 of the 89 field-level differences on ACB Q1-2022 are this), and it stores
expenses **positive** where the filing prints them negative. Anything summing a column across
`pdf` and `cafef` rows will get the wrong answer for the expense lines.

**ACB re-parsed through a PARSE-LAYER pipeline on GPU — all 65 filings, Q1-2010 → Q1-2026, which
is every ACB filing CafeF has.** ACB is the stress case: ~2/3 of its filings are scans, many
carrying the SUBSTITUTION-mojibake text layer. Each way
of reading a filing — an OCR engine + its settings + optional matching relaxations — is a
`ParseLayer`, and `build` tries them per statement, in order, until each reconciles, then the
CafeF-tab fallback (see `_parse_cascaded`). `FinancialsBuilder.LAYERS`:

  onnx@200 → onnx@300 → onnx@400 → tesseract@200 → onnx@200+relax → onnx@300+relax
  → tesseract@400+relax → onnx@200+components → onnx@300+components
  → onnx@200+relax+components → onnx@200+pad6+components → onnx@200+pad6+relax+components
  → onnx@300+split → onnx@300+split+components
  → onnx@200+join+components → onnx@200+join+relax+components
  → onnx@200+title → onnx@200+title+relax

**The eleven `+components` / `+pad6` / `+split` / `+join` / `+title` layers are appended, never inserted**, so a
statement that reconciles today cannot reach them — `_parse_cascaded` skips a report once
accepted. They add two relaxations, each traced to a specific cause and each recovering quarters
whose figures were already correct:

- **`relax_components` — a bank's fifth cash equivalent is a TREASURY BILL.** `CASH_COMPONENT`
  knows cash, deposits, securities and gold, so a filing listing `tín phiếu` beneath its closing
  balance had that line dropped from the sum and was refused for a discrepancy it did not have.
  Six quarters, each closing EXACTLY once counted: Q4-2010, Q1-2012, Q1-2013, Q3-2018, Q3-2019,
  Q1-2020. Widened per layer, never globally — a marker that is too narrow makes the sum fall
  short (recoverable, a later layer still gets it), one too wide makes it OVERSHOOT and can
  refuse a quarter that passes today.
- **`crop_pad` — the detector box starts INSIDE the number.** The recogniser cannot read pixels
  it was never shown, so raising the DPI does nothing: ACB's Q3-2023 reads its 93.261.018 deposit
  line as 261.018 at 200/300/400 dpi and on tesseract, and correctly at pad 6, after which the
  breakdown closes to the đồng. This is the MIRROR of the phantom-leading-digit bug that set
  `CROP_PAD_PT = 2` — too tight invents a digit, too tight in the other direction loses one.
- **`relax_split_tail` — the balance line's label WRAPPED and took its figure with it.** ACB's
  Q3-2017 reads `…tương đương tiền tại ngày` with an empty current-period cell and `thang_9` on
  the next row holding **15,044,850**; `_first_value` then falls through to the comparative
  column and returns 13,316,705, which is Q3-2016's closing balance — a wrong answer of exactly
  the right shape, and what the old committed data stored. The continuation is a bare date
  fragment (no account is named "tháng 9"), which is what makes it safe to splice back. At 300
  dpi the same filing also reads `tiền mặt` as 4,080,492 rather than 492, and
  4,080,492 + 7,411,264 + 3,553,094 = 15,044,850 — so the breakdown CONFIRMS the spliced figure
  instead of merely permitting it.
- **`join_digits` — a thousands separator read as a SPACE.** `_split_number_runs` exists for one
  box holding TWO period figures; the same box can instead hold ONE whose separator was lost, and
  splitting it keeps only the tail. ACB's Q2-2012 reads `'3 396.864'` for a printed 3.396.864 and
  came out short by exactly 3,000,000. The two are told apart by the FIRST part: a lost separator
  leaves a BARE 1-3 digit group in front, which cannot be a period figure of its own (they are all
  4-9 digits), and every part after it must continue the grouping exactly — so
  `'135.272.610 126.501.216'` still splits. Regressed on Q1-2025, the very filing that motivated
  the splitter.
- **`title_over_form` — THE FILING MIS-STAMPED ITS OWN FORM CODE.** VCB's Q2-2014 interim report
  prints `Mẫu B04a/TCTD-HN` on BOTH its income statement (page 9) and its cash flow (page 11).
  `B04` maps to the cash flow and a form code is trusted absolutely, so the income statement was
  claimed as a cash flow and never reached the row builder; its balance sheet is correctly `B02a`,
  so the document is mis-stamped, not garbled. **All sixteen earlier layers failed IDENTICALLY —
  both engines, four DPIs, every crop setting — which is the tell: OCR variation cannot produce
  identical results unless the failure is upstream of OCR.** No parse layer can fix a page the
  classifier never offers. A VERBATIM title match (score exactly 1.0, not the usual 0.80) now
  overrules a code that names a different statement; exact containment is what stops the
  auditor's report, which NAMES every statement, from overruling a sound code.
- **⚠️ The parse cache key is `(engine, dpi, crop_pad, join_digits)`.** Keyed on `(engine, dpi)` alone the
  wider-crop layer is handed the narrow crop's cached parse — the one that just failed — and the
  layer silently does nothing.

The first four are STRICT (higher resolution, then a different engine, for a filing whose scan
merges lines or misreads a digit). The last two set `relax_totals`: they recover the balance
sheet's grand-total columns from label variants the strict fuzzy match rejects — a filing that
prints "TỔNG CỘNG TÀI SẢN" where the schema expects "TỔNG TÀI SẢN", or that OCR-merges the
grand-total label into the line above (ACB Q1-2019, a `/Rotate 270` scan whose total assets
otherwise mapped to a garbage −1.8T row). They run ONLY after the strict layers fail and only add
the two total columns, still gated by reconcile + `sane`, so no other quarter is touched. The OCR
of each `(engine, dpi)` is cached within a filing, so the relaxed layers re-map without a second
OCR pass. The winning layer name is recorded per statement (`ocr_config`).

### ⚠️ THE PDF ARCHIVE BEGINS AT Q1-2010 — that boundary, not the parser, sets the coverage

**CafeF holds 65 consolidated ACB filings and the earliest is Q1-2010.** Nothing exists for
2008-2009, so those quarters can only ever come from CafeF's tabs however good the parser gets.
Judge the parser on Q1-2010 → Q1-2026 and nothing else; measured against the raw 74-quarter grid
it will always read ~88% and look broken when it is not.

**Result over the PDF era, Q1-2010 → Q1-2026 (65 quarters, 195 cells): 195 of 195.**
balance_sheet 65/65, income_statement 65/65, cash_flow 65/65 — every statement of every quarter
that has a filing, read from the filing.

**⚠️ THIS IS NOT THE OLD "195 of 195" RESTORED — IT IS THE FIRST ONE THAT MEANS WHAT IT SAYS.**
The figure this file carried before 2026-07-29 counted **23 cash flows with NO closing balance**,
written as `pdf` while the one column the statement is probed on was blank. **Every one of the 65
now carries a closing balance checked against the components printed beneath it.** Score coverage
on the closing balance, never on the row count — the two agreed only by accident.

On the full written grid (Q1-2008 → Q2-2026, 74 quarters, 222 cells): `pdf` **195**, `cafef` 27,
**0 missing**. All 27 are structural and none is a parse failure:

| | quarters | why |
|---|---|---|
| Q1-2008 … Q4-2009 | 8 × 3 = 24 | **no filing in the archive** — ACB listed on HNX in Nov-2006 but CafeF's document list starts 2010 |
| Q2-2026 | 1 × 3 = 3 | not filed yet at the time of the run |

### ⚠️ THE OLD "195 of 195" COUNTED 23 HOLLOW CASH FLOWS — coverage was never what it claimed

Nine cash flows fall to the CafeF tabs that used to read `pdf`: **Q4-2010, Q1-2012, Q2-2012,
Q1-2013, Q3-2017, Q3-2018, Q3-2019, Q1-2020, Q3-2023**. That looks like a regression and mostly
is not. **Score the closing balance, not the row**, and the old data reads far worse:

| | pdf rows | with NO closing balance |
|---|---|---|
| before 2026-07-29 | 65 | **23** |
| after | 56 | **0** |

Six of the nine (Q1-2012, Q1-2013, Q3-2018, Q3-2019, Q1-2020, Q3-2023) are hollow rows of
exactly the kind described below — written as `pdf` while the one column the statement is probed
on is blank, because `reconcile` accepted a cash flow on IV alone. Refusing them is the gate
working. **On verified content the re-parse is 56 against 42**, and 17 previously-hollow quarters
now carry a checked closing balance.

Only **three were real regressions** — Q4-2010, Q2-2012, Q3-2017 — and two of those exposed a
WRONG committed figure rather than a lost one: Q4-2010 is stored as 33,310,887 where the filing
prints **38,310,887** (its components sum to it, and opening 40,311,008 − 1,772,378 − 227,743
gives it exactly), and Q3-2017's stored 13,316,705 is the COMPARATIVE column, i.e. Q3-2016's.

**All nine are fixed and WRITTEN — cash_flow 65/65, every row verified.**

- **It is deterministic** — two independent full runs produced the same nine, no more and no
  fewer, so it is diagnosable one quarter at a time rather than being OCR flakiness.
- **Only the cash flow is affected.** Both other statements read 65/65, and the PDF rows are
  RICHER than the ones they replace (584 balance-sheet, 292 income-statement and 447 cash-flow
  cells populated where the old data was blank).
- **The `_closing_breakdown` "gồm có" fix recovers Q3-2010 only.** Its comment claims five
  quarters (Q3-2010, Q4-2010, Q1-2012, Q2-2012, Q1-2013); the other four needed the treasury-bill
  component below, and Q2-2012 is still unread.

### ⚠️ WHEN THE ARITHMETIC WON'T SAY WHICH FIGURE IS WRONG, LOOK AT THE PAGE

Q2-2012 was diagnosed three times from its sums and wrongly each time. Its breakdown was short by
exactly 3,000,000 and the components read IDENTICALLY at crop pads 2/6/10 and at 200/400 dpi, so
the conclusion was that no OCR configuration could recover it — twice it was written up as
unreadable, once with an argument for why refusing it was correct.

**Rendering the page settled it in one look.** The filing prints `3.396.864` for the NHNN deposit
line, and the recogniser returns the box as `'3 396.864'` — the thousands separator read as a
SPACE. Nothing was clipped, which is exactly why more pixels never helped; `_split_number_runs`
was throwing the leading group away. See `join_digits`. **A cheap `page.get_pixmap()` of the
region beats another sweep** whenever the arithmetic can only tell you that SOMETHING is wrong.

Whole run **~3.7 h** on an RTX 3050 (221 min for ACB's 65 filings, GPU otherwise idle; the
earlier ~2.6 h figure predates the `tesseract@400+relax` layer). A single quarter re-parsed
alone costs ~4 min when it fails (it runs every layer, including the CPU-only tesseract ones)
and ~6-7 min when a relaxed layer carries it.

**Re-running a SUBSET now MERGES** (`build(periods=[…])`, `merge` defaults on whenever `periods`
is given). It upserts: only the quarters the run actually produced are rewritten, the rest keep
what is on disk, and the file stays in quarter order — so the seven recoveries above cost 47 min
rather than a 3.7 h full re-parse. Before this, a subset run rebuilt the grid from what it held
in memory and every quarter it did not parse lost its `pdf` row to the CafeF tabs.

- **A quarter it attempted and FAILED is left alone**, not overwritten with a blank `missing`
  row — failing to re-read a statement is not evidence the statement is unreadable, and the row
  on disk may be a good one.
- **`open_ref` is read back from the file** when this run has no previous Q4 of its own,
  so a re-parsed Q1 is not judged more harshly than the same quarter in a full run.
- **`sane` fails open** in a subset run — it has no neighbouring quarters to judge magnitude
  against. Reconcile and the cash-flow breakdown are unaffected.
- **⚠️ A MERGING RUN DOES NOT SNAPSHOT.** The per-quarter snapshot is a PROGRESS VIEW whose
  income statements are still cumulative, and merging one into the authoritative file cannot be
  taken back: `_decumulate` drops the cumulative row from `data`, the final write then sees the
  quarter as "not produced" and leaves the file alone — leaving the snapshot. Q4-2010's income
  statement came out holding the FULL-YEAR PBT 3,102,248 (Q1..Q3 1,422,302 + the true Q4
  1,679,946) that way. A subset run is minutes, so it writes once, at the end, after
  de-cumulation. Any post-loop transform of `data` has the same hazard, not only `_decumulate`.

### ✅ HOW THE 195/195 WAS VERIFIED — and what it still does not claim (2026-07-29)

Four independent checks, none of which is the gate that accepted the rows in the first place:

| check | result |
|---|---|
| era recount, all three reports | 65/65 `pdf` each |
| the PROBE column populated on every parsed row | **0 blank** (the old data had 23) |
| a year's opening = the previous Q4's closing | **57 of 58 exact**; Q1-2023 off by 3 (in millions — one digit, 3e-8) |
| cash-flow closing ≥ the balance sheet's own cash on hand | 62/62 consistent, 0 impossible |
| vs CafeF's code-keyed tab | 48 match, 6 differ, 11 CafeF has no value |

Of the six disagreements with CafeF, **two are settled and both go to the parser** (ACB Q1-2019
and Q3-2023, above). **Four are unverified and nobody should assume either side**: Q4-2012
(70,232), Q2-2014 (4,811), Q1-2017 (15,165), Q4-2013 (1 — rounding). Opening those four filings
is the cheapest remaining accuracy work on this ticker.

**⚠️ A NOTES PAGE IS BEING SWEPT INTO THE CASH-FLOW RUN.** Seen on both filings rendered during
this work — Q2-2012 reports `pages [10, 11]` and Q3-2023 `[6, 7]`, and in both the second page is
prose (board members, accounting policies), not a statement. Neither quarter is corrupted by it,
but the page classifier is one page greedy at the tail and that is where a stray figure would come
from. Not yet diagnosed.

**⚠️ WHAT THIS DOES AND DOES NOT GUARANTEE.** The gates prove a statement's SUBTOTALS, not every
line on it. Every accepted cash flow now closes `V + IV + VI = VII` to the đồng and its closing
balance equals the components printed beneath it — two independent checks. Below that level the
rows are as good as the OCR: ACB's Q1-2023 carries `hdkd_13` = 96 where the filing prints
(438.096), and one investing line took the comparative column. Two systematic quirks are corpus-
wide and not defects to chase: a `hdkd_nhung_thay_doi_ve_*` SECTION HEADER holds the first line
of its section (OCR merges the two), and a PDF row is thinner than the CafeF row it replaces —
Q1-2023 is 28 items against CafeF's 47, because CafeF's tabs enumerate every code including the
nil ones. **If a consumer needs a specific minor line item rather than the subtotals, check it
against `from_api` rather than trusting the PDF row.**

- **Adding a way to parse a stubborn filing is adding a `ParseLayer` to `LAYERS`** — a new engine,
  a new DPI, or a new matching relaxation. The strict-first / relaxed-last ordering is what keeps
  a relaxation from ever loosening a quarter the strict layers already read correctly.

### ⚠️ A STATEMENT CAN BE `pdf` AND STILL HAVE A HOLE — the cash flow's closing balance

`reconcile` used to accept a cash flow on **either** IV or the closing balance, so a statement
that mapped IV alone passed at the FIRST layer and never escalated to one that could read the
balance lines. The result was the worst of both: the grid claims a parsed row and the one column
the statement is probed on is blank, so it reads as neither a gap nor a value. **27 quarters were
written that way** (ACB 24, VCB 3), and ACB's are every Q1 and Q3 — its unaudited quarterlies,
which print the balance lines DATED ("tại ngày 31 tháng 3") where the schema names the period, so
only the relaxed layers match them.

The closing balance is now **required**, and — this is the part that matters — **checked on every
layer** by `_closing_breakdown`. Requiring it without checking it merely trades one failure for a
worse one: ACB's Q3-2012 and Q1-2019 stopped failing for an absent figure and started passing with
the COMPARATIVE column's (54,560,217 and 22,356,020, each its own prior-year quarter, internally
consistent and contradicted by nothing else). The components printed beneath the closing balance
state it a second time and are the only thing that tells them apart; the check fails open when the
filing prints no breakdown.

**The identity stays relax-only, deliberately.** It tests the whole statement at once and so
cannot say WHICH term is wrong — and on a strict layer the wrong one is usually not the closing
balance. ACB's FY-2013 reads its closing 9,762,451 and opening 16,668,138 correctly at nearly
every layer, but IV maps to −6,905,687, which is exactly `closing − opening`: a figure that
already absorbs the FX line. Adding the mapped fx of −445,111 on top double-counts, the identity
misses by precisely that, and a sound quarter is thrown away. Running it everywhere cost 1 of 18
regression quarters; scoping it back to relaxed layers restored it.

Three supporting rules came out of the same work:
- **`_cash_balance_rows`** — a fallback for a second mangling: OCR merges the PREVIOUS row's
  trailing words onto the front of the balance line, so VCB's FY-2011 reads
  `tien_va_cac_khoan_tuong_duong_tien_v` and
  `tai_thoi_diem_dau_nam_vii_tien_va_cac_khoan_tuong_duong_tien` — the date has moved off one line
  onto the other and neither contains "…tiền TẠI…". Runs only when the dated scan found fewer than
  two rows, since the undated phrase also matches the "gồm có" breakdown header.
- **A missing FX line may stand as zero**, but only when the identity then closes EXACTLY — no
  tolerance. VCB's FY-2011 gives 96,678,346 + 28,026,672 = 124,705,018 with nothing left over.
- **The opening is read STRICTLY from the current-period column**, and REMOVED if that cell is
  empty (the ordered pass has already written `_first_value` into it). It is the one field where
  the comparative column is indistinguishable from a correct answer — an opening balance simply IS
  a prior period's closing — so a fall-through returns a number of exactly the right kind and the
  identity "verifies" against it. Unmapped, `_cash_flow_identity` substitutes the previous year's
  Q4 closing (`open_ref`, threaded from `build`) and requires an exact close: ACB Q1-2022 gives
  82,601,567 − 8,595,083 + 14,889 = 74,021,373, the figure its own printed components confirm.

### ⚠️ IN A TWO-COLUMN STATEMENT, INDEX 0 IS THE CURRENT PERIOD — do not fall through

`Statement._first_value` returns the first POPULATED column, which is right when OCR
over-segments (a spurious note column pushing the real value to index 1 — that needs three or
more columns by definition). With exactly two columns there is nowhere for a figure to hide, so
falling through returns **last year's number for a line the filing printed as a dash**. ACB's
Q1-2022 did this four times, every one silently plausible: `hdkd_20` read 9,009,073 and
`hddt_mua_sam_bat_dong_san_dau_tu` 148,453, both 2021 figures against a blank 2022 column.

It also splits a **wrapped label across two rows**, leaving the first holding only the comparative
and the continuation holding the real figure — `…uy_thac_dau_tu_cho_vay_ma_tctd` `[., -8,456]`
then `chiu_rui_ro` `[-6,890, .]`. Returning None for the first is what lets the second be found.
The opening balance was never "lost" either: it sits on the next row (`thang_1`, 82,601,567).

**Seven hardenings took ACB from 189 to 193, and every one is scoped so it CANNOT reach a quarter
that already parses** — that is the design rule here, not a nicety: a change that improves five
quarters and quietly breaks a sixtieth is a net loss, and the only way to know which you have is
to re-run the ones that already work. Each was traced to a specific cause in the pixels.

| # | fix | reaches only |
|---|---|---|
| 1 | `_page_content_text` — a page whose whole text layer is a SIGNATURE STAMP has none | pages previously classified `None` |
| 2 | `MIN_CONTAINS_FRAGMENT` — containment must guard the SHORTER string | tightens a false match |
| 3 | `_anchor` assigns competitively — one row answers one anchor | needs #4 |
| 4 | `sane` rejects a probe EXACTLY equal to an accepted quarter | comparative-column reads |
| 5 | `_split_number_runs` — one detection box holding TWO period figures | tokens that parse to nothing today |
| 6 | orphan label — a label whose box starts BELOW its figures | rows discarded for an empty key |
| 7 | `_cash_flow_identity` — closing = opening + movement + FX | relaxed layers only |
| 8 | **`CROP_PAD_PT`** — pad a detected box before RECOGNISING it | every crop (see warning below) |
| 9 | **`Y_TOL` 3.0 → 4.0** — a label's box starts higher than its digits | every page (see warning) |
| 10 | IV recovered by POSITION (the row above the opening balance) | relaxed layers only |
| 11 | `_is_cash_tail` matches by CONTAINMENT, not prefix | relaxed layers only |
| 12 | a relaxed cash flow must be VERIFIABLE, not merely un-contradicted | relaxed layers only |

- **#1 the signature stamp.** A scanned page that was e-signed carries a text layer holding only
  the signature appearance (~350 chars of real Vietnamese and English), which clears MIN_PAGE_TEXT
  and trips neither mojibake test — so the page is never OCR'd and the parser reads the stamp. It
  usually lands on a cover page and costs nothing; on ACB's Q1-2023 it landed on the BALANCE
  SHEET'S SECOND PAGE and the statement stopped at TỔNG TÀI SẢN, unreconcilable at any DPI or
  engine. Across all 137 ACB+VCB filings this changes **27 pages, every one previously `None`**,
  26 of them covers or trailers; exactly one sits inside a statement. **No VCB page changes.**
- **#5 the merged number box.** The onnx engine detects text LINES, so on some rows it boxes both
  period columns together (`'135.272.610 126.501.216'`). That parses as no number at all, so the
  row loses every value and is dropped — which is how Q1-2025 and Q1-2026 lost IV, V *and* VII,
  the entire basis for reconciling a cash flow, while the rows either side read perfectly.
  Apportioning the box by character offset lands each right edge within a point of where the
  neighbouring rows report it.
- **#7 is the one that earns its keep.** `_closing_breakdown` proves ONE figure, and that turned
  out not to be enough: Q1-2024 recovered a closing balance agreeing with its components to the
  đồng while eleven figures around it came from the comparative column. It reconciled, and it was
  written, and it was wrong — it had to be reverted off disk. Tying closing back to opening
  through the flows makes the interior hold together too.
- **Validated by re-running what already worked**, not by inspecting the diff: 16 accepted cash
  flows re-parsed at two DPIs — **16 reconcile, 0 fail** — plus VCB Q1-2023 / Q3-2024 and ACB
  Q4-2021 / Q4-2022 / Q2-2023 on the balance sheet. An earlier, unscoped version of #3 was caught
  this way (it moved Q4-2021's closing to the comparative column and failed 5 of 16), which is why
  the cash-flow relaxations are gated behind `relax_totals`.
- **Corrections these produced in passing:** VCB Q1-2023 total liabilities 1,846,431→**1,701,773**
  and equity 62,168→**144,658** (they now sum to total assets exactly); VCB Q3-2024 equity
  36,293→**190,297**; ACB Q4-2022 closing cash −10,817,313→**103,510,228**.
- **#8 the crop padding — the one to remember, because it was misdiagnosed twice.** The detector
  returns a box hugging the glyphs and VietOCR misreads a crop that tight, INVENTING a leading
  digit at the clipped edge: 96.922.247 → **1**96.922.247, 6.654.779 → **1**6.654.779. It looks
  exactly like a bad recogniser, and it survives 200, 300, 400, 500 and 600 dpi, so it looks like
  a hard limit too. It is neither. A bake-off on the same cells showed **vgg_seq2seq (the one in
  use), vgg_transformer, EasyOCR and Tesseract all read them correctly** from a slightly looser
  crop — the padding sweep flips the answer at **1pt** and it stays stable to 6. Before reaching
  for another Vietnamese OCR model, check the crop. This one bug was almost certainly shaving
  digits elsewhere in the archive too.
- **#11 the stray character.** ACB's Q1-2023 opening balance parses as
  `t_tien_va_cac_khoan_tuong_duong_tien_tai_ngay_1_thang_1` — OCR keeps a fragment of the section
  numeral. One leading character made a `startswith` test miss the line, leaving the statement
  with no opening balance and refusing it as unverifiable although every figure had been read
  correctly. Containment still does not over-match: the breakdown header reads "…tương đương tiền
  GỒM CÓ", which contains no "…tiền TẠI…".
- **⚠️ Pre-existing corruption these gates now CATCH but have not yet fixed** (only a full re-parse
  will): ACB's committed **Q1-2024 income statement** carries Q1-2023's PBT 5,156,497 — it read the
  comparative column; the true figure is ~4,892bn. **Q1-2015** repeats Q1-2014's 318,253. Gate #4
  is what detects this class, so re-running is worth it for more than the two open cells.

> ### ✅ RESOLVED: #8 AND #9 CARRY THEIR OWN WEIGHT (2026-07-27)
>
> The regression this block asked for was superseded by something far larger: a **full ACB + VCB
> re-parse** with both constants live — 137 filings, 411 statements, 5.2 h. Two independent
> references were then asked which version is right, and **neither favours the pre-#8/#9 output
> anywhere**:
>
> - **CafeF's ANNUAL tab** (code-keyed, no OCR, independent of any single quarter): of 31
>   ticker-years, the re-parse wins 6 and ties 25. **Zero favour the old data.** VCB 2020 goes
>   13,162 → **23,050 bn** and 2013 4,279 → **5,743 bn**, both landing on the annual figure
>   exactly; ACB 2012 was unreadable before and now sums to 1,043 bn.
> - **The opening/closing chain** (a Q1 opening must equal the prior Q4 closing): ACB matches
>   **7 of 7** against 5 of 7 before; VCB 8 against 5.
>
> Both **known corruptions are fixed and confirmed**: ACB Q1-2024 PBT 5,156,497 → **4,892,313**
> (the ~4,892bn predicted above) and Q1-2015 318,253 → **359,265**, each matching CafeF's
> quarterly tab to the million. The phantom-leading-digit signature of #8 unwinds cleanly through
> de-cumulation — Q1-2022 loses a fabricated leading 1 (14,114,005 → 4,114,005) and Q2-2022 moves
> by exactly the offsetting −10,000,000, leaving the annual total untouched.
>
> **What this still is not:** a controlled A/B (many things changed at once), and both tickers are
> `bank`-template. Nothing here speaks to `corp` / `securities` / `insurance` — and it cannot,
> since ACB and VCB are the only tickers with committed statements to regress against. That
> becomes testable the first time a corp ticker is built.
>
> Coverage over the whole re-parse: **360 → 393** statement-quarters read from the filing. VCB
> gains 34 (balance sheet 57→67, income statement 46→65, cash flow 62→67); ACB was already
> complete.
- **Detection runs on the GPU** (`onnxruntime-gpu`, CUDAExecutionProvider): ~0.25 vs ~1.8 s/page
  for the CPU wheel. **⚠️ onnxruntime-gpu's version must match the machine's CUDA** — the current
  1.28 wheel needs CUDA 13 and silently falls back to CPU here (CUDA 12.1); the **1.20.x** line is
  the CUDA-12 / cuDNN-9 match. `onnx_ocr._enable_cuda_dlls` adds torch's bundled CUDA/cuDNN to the
  DLL path so no separate system CUDA is needed. If the provider cannot load it degrades to CPU,
  correct but slow.
- Regenerate with `CAFEF_OCR_ENGINE=onnx` and `FinancialsBuilder.build("HOSE","ACB")`.

- **⚠️ `publish_date` — the day the figures became PUBLIC. Join on this, never on the period
  end.** VCB's Q4-2025 covers the quarter ending 31 Dec 2025 and was not published until
  **27 Mar 2026**. Joining fundamentals to prices on the period end hands a model twelve weeks
  of look-ahead, every year, on the audited annuals — which are the worst offenders precisely
  because they are the best documents. 210 of 213 rows are dated (99%).
  - It is read from INSIDE the filing (the signing line *"ngày DD tháng MM năm YYYY"*), taking
    the **latest** date that falls after the period end. The first date in a filing is the
    period itself ("tại ngày 31 tháng 12 năm 2024"), and an unbounded maximum picks up a
    comparative period from years earlier; a report is signed after every date it reports on.
  - The date CafeF embeds in the filename is only a fallback — those exist from 2022 only.
    Where both exist they agree exactly, which is a useful independent check.
  - **It belongs to the QUARTER'S DOCUMENT, not to a statement.** One filing produced all
    three, so they share it — including a row that had to come from CafeF's tabs because its
    own statement would not parse. Keeping it per statement is what left VCB's Q1-2009 undated
    even though the very document it failed to parse prints "Hà Nội, ngày 27 tháng 04 năm 2009"
    on page 4.
  - **The older filings do not sign under the statements at all** — they approve the accounts
    in the last note ("28. Phê duyệt báo cáo tài chính giữa niên độ … ngày 20 tháng 10 năm
    2009"). The page scan stops at the notes for speed, so `_tail_date()` reads the end of the
    document when the statement pages yield nothing.
  - Only **Q3-2008** is undated: it exists in CafeF's tabs alone, with no filing to read.

## 2. Directory layout & the Strategy/registry pattern

```
src/web_scraper/
├── CONTEXT.md                ← this file
├── base_scraper.py           BaseScraper ABC + SCRAPER_REGISTRY + @register_scraper + build_scraper
├── trading_view_scraper.py   SOURCE_NAME="trading_view"  (Selenium/Chrome + BS4, ~1600 lines)
├── cafef_scraper.py          SOURCE_NAME="cafef"         (requests → CafeF AJAX; the 5 daily tabs)
├── cafef_pdf_scraper.py      SOURCE_NAME="cafef_pdf"     (requests → the filing PDFs themselves)
├── cafef_news_scraper.py     SOURCE_NAME="cafef_news"    (requests → company-news / disclosure feed)
├── cafef_schema.py           ── the PDF-reading pipeline: canonical chart of accounts
├── cafef_pdf_parser.py       ──   one filing PDF → statements (Tesseract OCR for the scans)
├── cafef_financials.py       ──   the local PDF archive → raw_data/cafef/financials/
├── simplize_scraper.py       SOURCE_NAME="simplize"      (requests → api.simplize.vn JSON)
└── gics_scraper.py           SOURCE_NAME="gics"          (requests + openpyxl → MSCI xlsx)
```

**Two different kinds of module live here.** The `*_scraper.py` files fetch from the network
and register a `SOURCE_NAME`; the three bare `cafef_*.py` files are NOT scrapers — they are
an offline pipeline that reads the PDFs already on disk (§3a). Fetching is cheap and
one-shot, parsing is expensive and iterative, so they are kept apart: the parser can be
re-run over the 2.4 GB archive as often as it takes without touching the network.

The CafeF scrapers are separate sources, not one: `cafef_scraper` pulls the daily price/flow
tabs, `cafef_pdf_scraper` downloads the filings, `cafef_news_scraper` pulls the event stream.
Each registers its own `SOURCE_NAME` and writes its own folder under `raw_data/cafef/`.

**Pattern = Strategy + registry/factory** (`base_scraper.py`):
- `BaseScraper(ABC)` holds shared infra: `Logger`, optional `SwitchHandler`, a
  `ThreadManager` (parallelism via `power` %), and the retry policy
  (`retry_attempts` / `retry_delay`). Subclasses set `SOURCE_NAME`, implement
  `scrape()`, and decorate with `@register_scraper`.
- `SCRAPER_REGISTRY: dict[str, type]` + `build_scraper(source_name, *args)` let a
  caller build any source by name. Adding a source is open-for-extension: new
  subclass, no change to `main.py` or the others.
- **Note:** `main.py` currently instantiates the four classes *directly* (not via
  `build_scraper`), so the registry/factory is available but not the live path.

## 3. The four sources — what each pulls & its output

### TradingView — `trading_view_scraper.py` (the universe + OHLCV; Selenium)
- **Two-phase `scrape()`:** (1) scrape symbol **links** per enabled asset class →
  (2) `aggregate_trading_view_links()` dedups them into one CSV →
  (3) open each link and extract **price data**. Each phase is gated by its own
  switch (`.../links`, `.../collected_links`, `.../data`).
- **9 asset classes**, each with its own filter dimensions:
  `stocks` (country/stock_type/sector), `funds` (country/fund_type),
  `futures`, `forex` (source), `crypto` (source/type/exchange — data step is a
  no-op, not yet implemented), `indices`, `bonds`, `economy`, `options`
  (links only; `_add_options_data_tasks` returns 0).
- **How data is extracted:** navigates the chart, optionally toggles **ADJ**
  (`_helper_toggle_adj_dividends` — dividend adjustment, chart defaults ADJ **off**),
  sets a custom date range (`SCRAPER_START_DATE`=2000-01-01 → today), waits for the
  bar count to stabilize, then reads bars straight out of the in-memory chart widget
  (`window._exposed_chartWidgetCollection…`) via injected JS. A **two-layer OHLC
  detector** (structural slot-count + semantic OHLC invariants) decides OHLCV vs a
  single `value` series.
- **Concurrency hardening:** `Semaphore(SCRAPER_MAX_CONCURRENT_BROWSERS=8)` caps
  Chrome instances; a `_nav_time_lock` staggers navigations by
  `SCRAPER_NAV_STAGGER=8s`; random pre-acquire sleeps desync threads; a background
  `_dialog_remover_loop` thread kills pop-ups. Chrome runs with images+CSS disabled,
  JS on.
- **Output:**
  - links → `raw_data/trading_view/links/<asset>/<dims…>/trading_view_links_<YYYY-MM-DD>.csv`
  - aggregated → `raw_data/trading_view/collected_links/all_links_<date>.csv`
  - data → `raw_data/trading_view/data/<asset>/<dims…>/<SYMBOL>_<start>_<end>.csv`
  - Link CSV schema = `TRADING_VIEW_TABLE_SCHEMA` (constants.py):
    `scrape_main_type, sub_type_name/value_{1,2,3}, url`.

### Simplize — `simplize_scraper.py` (validated daily-panel backbone; requests)
- Pure `requests` against `api.simplize.vn`. The **primary** source: fully
  dividend-adjusted OHLC, true total volume, and foreign buy/sell/net flow
  (volume+value) + room — one endpoint feeds both the price and foreign-investor
  tabs.
  `GET /api/historical/quote/prices/<SYMBOL>?page&size` (newest-first, paginated,
  server caps `size`=1000; `date` is a unix UTC-midnight timestamp).
- Also scrapes **per-ticker GICS-based industry** via
  `/api/company/summary/<TICKER>` (`scrape_all_industries()`, `ThreadPoolExecutor`)
  → `raw_data/simplize/industry.csv` (10 economic sectors / 50 industry groups —
  the ticker→industry source that the `SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY`
  crosswalk in constants.py maps onto official GICS leaves).
- **Output:** `raw_data/simplize/stocks/<EXCHANGE>_<SYMBOL>.csv` (17 cols:
  date/exchange/symbol, OHLC, net/pct change, volume, foreign_room, f_buy/sell/net
  vol+val).

### CafeF — `cafef_scraper.py` (fills fields TV lacks; requests)
- `requests` against CafeF `du-lieu` AJAX endpoints. Each numbered tab of the
  `lich-su-giao-dich/<exchange>/<sym>-<n>.chn` page maps to one `.ashx` endpoint,
  and **each link is written to its own folder** (one folder per link):

  | Tab (`<sym>-<n>.chn`) | Endpoint | Content | Output folder | History |
  |---|---|---|---|---|
  | -1 | `PriceHistory.ashx` | OHLC, raw+adj close, matched/negotiated vol | `price/` | 2009 |
  | -2 | `ThongKeDL.ashx` | **order-placement stats** — # + vol of buy vs sell orders | `order_stats/` | 2010 |
  | -3 | `GDKhoiNgoai.ashx` | foreign buy/sell/net flow, room, own% | `foreign/` | 2012 |
  | -4 | `GDTuDoanh.ashx` | **proprietary-desk trades** — firms' own-account buy/sell vol+val | `prop_trading/` | 2023 |
  | -6 | `GDCoDong.ashx` | **insider / major-shareholder transactions** — who, plan vs real buy/sell, holdings | `insider_txn/` | 2008 |

- Tabs -1/-2/-3/-4 are **daily series** (one row per trading day); tab -6 is
  **event-based** (one row per transaction). Each folder is ingested **one-to-one**
  into its own bronze table by `data_preprocessor._ingest_bronze_cafef_*`
  (`cafef_price`, `cafef_foreign`, `cafef_order_stats`, `cafef_prop_trading`,
  `cafef_insider_shareholder_transactions` ← from the `insider_txn/` folder);
  `price/` + `foreign/` are re-merged on (symbol, date) later, in **silver**
  (`_ingest_silver_stocks`), not bronze.
- **Quirks handled:** `StartDate/EndDate` are **MM/dd/yyyy (US)**; a query is capped
  at ~63 rows and `PageSize` 20, so history is fetched in overlapping ~2-month
  windows and paginated; `ExchangeType` (HOSE/HNX/UPCOM, UPPERCASE) is **required**
  for HNX/UPCOM or CafeF silently returns nothing; **prices are '000 VND** so `_mul`
  ×1000 (applies to the price tab only — order-stats/prop volumes+values are already
  raw). The row **date key differs by tab** (`Ngay` for price/foreign, `Date` for
  order-stats/prop) and `GDTuDoanh` **nests its list** under `Data.Data.ListDataTudoanh`
  — both handled by `_collect(..., list_key=, date_key=)`. The insider tab
  (`GDCoDong`) is **event-based**: `_collect_paged` paginates the whole history
  (no date-window/dedup), and its dates arrive as .NET `/Date(ms)/` (parsed to ISO
  VN-local by `_net_date`).
- **Output:** `raw_data/cafef/{price,foreign,order_stats,prop_trading,insider_txn}/<EXCHANGE>_<SYMBOL>.csv`
  (price 12 cols, foreign 11, order_stats 9, prop_trading 7, insider_txn 21). `scrape()`
  runs all five batch drivers (`scrape_all_price/_foreign/_order_stats/_prop_trading/
  _insider_txn`) over the universe via the shared `_scrape_all(label, fn, …)`.
- **Universe = all three VN exchanges** (`VN_EXCHANGES = HOSE, HNX, UPCOM`; ~777 unique
  tickers). `get_stock_symbols(exchanges=…)` reads the TradingView stock links and
  filters to those exchanges (default = all three); `scrape()` and every `scrape_all_*`
  take an `exchanges=` passthrough, so `scrape()` covers the full HOSE+HNX+UPCOM set and
  `scrape(exchanges=("HOSE",))` scopes to one. `skip_existing=True` means a full run only
  scrapes what each tab is still missing (price/foreign/order_stats/prop_trading done
  across the full 777-ticker universe; insider_txn still VN100-only → ~681 tickers
  remaining for that one). Note prop_trading only queried the full universe: 431 tickers
  have data and 350 have no prop-desk trades (nothing written for those — history ~2023).

### CafeF PDFs — `cafef_pdf_scraper.py` (the filings themselves; requests, no PDF library)
- **Why:** the filings are the PRIMARY source — CafeF's JSON financial API is a
  transcription of them, and where the API has a gap (VCB is missing 20 statement-quarters)
  the PDF is the only place those figures exist. This scraper only **fetches**; it never
  opens, parses or OCRs a document, so it needs no PDF library at all.
- **Source:** one endpoint, the same one the disclosure-date work reads —
  `cafef.vn/du-lieu/Ajax/PageNew/FileBCTC.ashx?Symbol=<sym>&Type=1&Year=0`. It lists every
  report for the code (VCB 206, VIC 210, back to 2002) with a link to the PDF.
- **Output:**
  - `raw_data/cafef/pdfs/index/<EXCHANGE>_<SYMBOL>.csv` — one row per document
  - `raw_data/cafef/pdfs/files/<EXCHANGE>_<SYMBOL>/*.pdf` — the documents
  - Index and binaries are split so the archive stays navigable at VN100 scale (a flat
    folder would interleave 100 CSVs with 100 directories); `index/` then mirrors every
    other CafeF folder — one `<EXCHANGE>_<SYMBOL>.csv` per stock.
- **The index says what each document IS** — a caller must know this before trusting its
  numbers, and all three of these are traps that have already bitten:
  - `consolidated` — "hợp nhất". The parent-company ("công ty mẹ") report covers a
    **different entity** with different figures; CafeF lists both, roughly 50/50.
  - `assurance` — audited (`kiểm toán`) > reviewed (`soát xét`) > unaudited. A quarter is
    often filed twice, and the later document restates the earlier.
  - `half_year` — the semi-annual report prints **only the cumulative Jan-Jun column**, so
    its income statement is NOT the standalone quarter (VCB Q2-2024 prints PBT 20,835bn
    where the quarter is 10,116bn). It cannot be read from the title: CafeF calls it
    "…quý 2 năm 2024 (đã soát xét)". A **reviewed Q2 filing is the semi-annual by
    definition**, which is how it is detected.
- **Quirks handled:**
  - **Two CDN hosts.** The API advertises `cafefnew.mediacdn.vn`, but older files live only
    on `cafef1.mediacdn.vn` — which 404s **entire years of VIC** (all of 2020-21). Both are
    tried, and a **4xx is not retried**: re-asking five times with a delay only postpones the
    host that has the file.
  - **Truncated downloads.** A short read does not raise — it yields a PDF that still opens
    and still reports its true `page_count`, with the missing pages failing to load one by
    one. That silently reduced VCB's Q2-2014 filing to 14 of 58 pages and looked exactly
    like file corruption. Every download is verified against `Content-Length`.
  - **Duplicate titles.** CafeF lists a re-uploaded filing under an *identical* name (VCB has
    two "BCTC hợp nhất quý 4 năm 2023"), which slugged to one filename and silently
    overwrote a document. Colliding names take a URL-hash suffix; unique names are untouched.
- **Size:** highly ticker-dependent — VCB is ~1.7 GB (90% of its filings are page scans),
  VIC ~0.65 GB, but across the full VN100 the average is closer to ~1.0 GB/ticker. The whole
  VN100 archive (all 100 tickers, all years) is **~97 GB** on disk. **Estimate before you
  download:** the document-list API carries no size field, so sum each PDF's `Content-Length`
  via a HEAD (or 1-byte ranged GET) — that predicted the VN100 pull to within 0.2% (47.6 GB
  actual vs 47.7 GB estimated) without fetching a single file.

### CafeF news — `cafef_news_scraper.py` (company-news / disclosure event stream; requests)
- A **point-in-time event feed**: headline counts, event flags, announcement dates and
  article text, joinable to prices without look-ahead.
- **Source:** every category tab of `cafef.vn/du-lieu/tin-doanh-nghiep/<sym>/event.chn` hits
  one AJAX endpoint (`Ajax/Events_RelatedNews_New.aspx`) with a different `configID`.
  `PageSize` caps at **30** → paginate until a page repeats or runs short. Categories **1..5
  are scraped first** (they give the TRUE category), then **0 backfills** what is left
  uncategorised; rows dedup by URL. Then each article is fetched for its body.
- **Two orthogonal labels, both kept:** `type` (editorial | disclosure | error) is
  *provenance* — a disclosure is a filing CafeF republished, and is where `pdf_url` comes
  from; `category` is *topic* (business results, dividends, personnel, capital increase,
  insider transactions, uncategorised).
- **Output:** `raw_data/cafef/news/<EXCHANGE>_<SYMBOL>.csv`
  (`order, timestamp, type, headline, category, content, url, pdf_url`).
  Now scraped for the **full 777-ticker universe** — ~405k rows, ~495 MB, spanning 2007 →
  2026. Row count is very ticker-dependent: blue-chips run ~1,500-2,300 (VCB 1,629 / PNJ
  1,715 / FPT 2,255), the small-cap tail ~100-800 (mean ~490).
- **Gotchas:**
  - **`order` is numbered from the article's own `datePublished`, not from the headline
    feed's listing date.** The two disagree, and numbering by the listing left `order`
    claiming a chronology the timestamps did not have — which leaks look-ahead into anything
    keyed on it.
  - Article bodies are the expensive stage (blue-chips ~1,600-2,300 per ticker) → fetched in
    parallel with a polite per-worker delay. The cost is TIME, not disk: the whole 777-ticker
    universe ran in ~2 hours at **8 ticker-threads × 8 article-workers** (`max_workers=8`,
    `ARTICLE_WORKERS=8`). Storage is tiny beside the PDFs — ~495 MB of text vs ~97 GB.
  - A dead link keeps its headline as `type=error` rather than dropping the row.
  - Headline text comes from the anchor's **inner text**; the `title=""` attribute breaks on
    the embedded quotes in legacy headlines.

## 3a. Reading the PDFs — `cafef_schema.py` / `cafef_pdf_parser.py` / `cafef_financials.py`

Not scrapers. They read the archive `cafef_pdf_scraper.py` has already downloaded and build
quarterly financial statements from it:

```
raw_data/cafef/pdfs/                 (in)   the filings
        └─ cafef_financials.FinancialsBuilder.build("HOSE", "VCB")

raw_data/cafef/financials/           (out)
├── schema/<template>_<report>.csv          the 4 charts of accounts x 3 statements
├── statements/<template>/<report>/<bs|is|cf>_<EXCHANGE>_<SYMBOL>.csv
└── templates.csv                           ticker -> template + cash-flow method
```

**The TEMPLATE is a folder, not a column**, because the four charts of accounts share no line
items. A directory is then schema-homogeneous: every file under `statements/bank/` has the
same 90 columns and they mean the same thing. As a column in one shared table, the columns
would depend on which *row* you were reading.

**`templates.csv` is the map that makes the folders navigable** — without it a consumer holding
a ticker cannot tell which of the four folders to look in. It carries the fingerprinted
template beside the GICS sector and industry group, so where the two disagree it is visible in
the data rather than buried: **HVA** is filed under `chung-khoan-va-ngan-hang-dau-tu`
(securities) and has `template=corp`.

- **`cafef_schema.py` — the canonical chart of accounts.** Fetched from the three tabs of
  `cafef.vn/du-lieu/<exchange>/<sym>-tai-chinh.chn` (Cân đối kế toán / Kết quả KD / Lưu
  chuyển tiền tệ), whose JSON `templace` block IS the line-item template. `save()` writes
  `schema_<template>_<report>.csv`.
- **⚠️ A SCHEMA IS PER ACCOUNTING TEMPLATE, NOT PER TICKER.** Vietnam has **four** charts of
  accounts among listed companies, and they share no line items — every one has a "code 1",
  and it means a different thing in each, so their columns must never meet in one table:

  | template | opens its P&L with | columns (BS/IS/CF) | who |
  |---|---|---|---|
  | **bank** (TCTD) | *Thu nhập lãi…* | 90 / 26 / 47 | 20 tickers — industry group `551010` |
  | **corp** (DN) | *Doanh thu bán hàng…* | 133 / 24 / 44 | ~735 — the 9 non-financial sectors |
  | **securities** (CTCK) | *I. DOANH THU HOẠT ĐỘNG* | 132 / 81 / 72 | 14 — group `551020` |
  | **insurance** (DNBH) | *Doanh thu phí bảo hiểm* | 95 / 54 / 44 | 2 — group `553010` |

  All 12 files exist under `raw_data/cafef/financials/schema/<template>_<report>.csv`, built by
  `cafef_schema.save()` from a reference ticker per template (VCB / FPT / SSI / BVH).

- **⚠️ FINGERPRINT THE TICKER — DO NOT CLASSIFY IT.** `detect_template(symbol)` reads CafeF's
  own chart of accounts and matches it against `FINGERPRINTS` (the line-item count of each
  section). GICS says what the business *is*; the chart of accounts says what the *filing*
  looks like, and only the second is what a parser needs. **They disagree:** `HVA` sits in the
  securities industry group and files on the **corporate** template. Sector is worse still —
  *Tài chính* spans banks, brokers AND insurers, which share nothing. Use the industry group
  as a sanity check on the fingerprint, never as the source of truth.

- **⚠️ THE CASH-FLOW METHOD IS A COMPANY'S CHOICE, read per filing — never inferred.**
  Indirect (*"start from profit before tax and adjust"*) vs direct (*"receipts and payments"*).
  It is not a property of the sector, nor even of the template: ANV and DIG file **direct**
  while their sector-mates FPT/VNM/VIC file **indirect**; BLI files direct where BVH files
  indirect; banks and securities file direct and indirect respectively.
  - The test is one line: the operating section opens with **"Lợi nhuận trước thuế"** ⇒
    indirect, anything else ⇒ direct. The rule is defined on the indirect side deliberately —
    direct's opening line is worded differently by every template and half the tickers (VCB
    *"Thu nhập lãi… nhận được"*, ANV *"Tiền thu **từ** bán hàng"*, BLI *"Tiền thu bán hàng"* —
    no *từ*), and enumerating those is a losing game.
  - `cafef_schema.method_of()` applies it to an API template; `Statement.cash_flow_method`
    applies the same test to a parsed PDF, since both print the same words.
  - **ONE cash-flow schema holds BOTH methods.** They are near-disjoint — of 19 and 8 operating
    lines they share exactly one, the subtotal both converge on — and investing/financing are
    the same lines either way. So the table carries both branches (`hdkd_indirect_*`,
    `hdkd_direct_*`) plus that one shared, untagged subtotal; a filing fills the branch it used
    and leaves the other **blank, not zero** (the company did not report those lines). The
    statement then reconciles the same way whichever method it used — which is why there are
    12 schemas and not 16.
- **Column names** are `<index>_<sub>_<subsub>_<account>`, lowercase `a-z0-9_`, taken from the
  numbering the filing itself prints. The three statements are numbered on *different*
  principles, so each has its own rule:
  - **balance sheet — hierarchical.** Roman = section, digit = child, letter = grandchild, all
    kept verbatim: `vii_hoat_dong_mua_no` → `vii_1_mua_no`. Keeping the letter is what
    separates the three identically-named *Hao mòn tài sản cố định* lines: `x_1_b`, `x_2_b`,
    `x_3_b`.
  - **income statement — flat.** Arabic digits are component lines and Roman numerals are the
    SUBTOTALS of the lines above them; they are siblings, not parent and child. Both are kept
    as printed, which also keeps them in separate namespaces so "1." and "I." cannot collide:
    `1_thu_nhap_lai…`, `2_chi_phi_lai…`, `i_thu_nhap_lai_thuan` (= 1 − 2).
  - **cash flow — flat, section-prefixed.** The digits RESTART in every section (HDKD 1..22,
    HDTC 1..6) and HDDT prints none at all, so the section is part of the name:
    `hdkd_1_…`, `hddt_mua_sam_tai_san_co_dinh`, `hdtc_vii_tien_va_cac_khoan_tuong_duong_tien…`.
  - Account names are capped at **120 chars**, which is not a round number: CafeF's longest
    line is 113 chars, and it agrees with the line below it for its first ~100 — a shorter cap
    would truncate two different cash-flow lines onto one column name.
- **`cafef_pdf_parser.py` — one filing → statements.** Two front-ends feed one row-builder:
  the PDF's own text layer, or **OCR** (`vie`) for the documents that have none —
  **90% of VCB's filings are page scans, and not only the old ones** (its Q1-2026 report is 53
  pages of image). Both return word boxes in the same coordinate system, via the single
  `_ocr_page(page, native)` seam.
  - **OCR engine is pluggable** (`OCR_ENGINE` / `CAFEF_OCR_ENGINE` env): `tesseract` (default,
    CPU, the engine every reconciliation threshold was tuned against), **`onnx`** (DeepDoc DB
    detection + VietOCR — see below), or `easyocr` (CUDA/GPU, `Reader(['vi'])`, a lazy singleton;
    built but NOT adopted — its detection-box tokens fragment differently from Tesseract's
    layout-aware word boxes, so the tuned row-clustering mis-groups dense statements).
  - **`onnx` — the engine to re-OCR the archive with (`onnx_ocr.py`).** DeepDoc's 4.7 MB DB text
    detector under onnxruntime + VietOCR (`vgg_seq2seq`) recognition, validated in experiments
    8-9: accuracy-tied with a PaddleOCR-server stack and **~10× faster** (1.4 vs 13.9 s/page),
    which is what makes a whole-ticker re-parse practical. The detection operators + DB
    post-processing are vendored verbatim (Apache-2.0) in `_deepdoc/`; the model auto-downloads
    from HuggingFace (`InfiniFlow/deepdoc`) if the bundled `models/deepdoc_det.onnx` is absent.
    Recognition is batched on the GPU. It returns the same visual-space word boxes as the other
    engines, so the row-builder is engine-blind.
    - **⚠️ Do NOT `prerotate(page.rotation)` when rasterising.** `page.get_pixmap()` ALREADY
      applies the page's `/Rotate`, so a plain scale matrix gives an upright raster. Rotating
      again turns a `/Rotate 180` scan (every reviewed/annual filing) upside-down and OCR returns
      pure garbage ("Các thuyết minh…" → "NYHI Y NYHD…"). This bit the first ACB run — the
      rotation-180 quarters came out blank — and was invisible in experiments 8-9 because their
      test filings are all `/Rotate 0`.
  - **A page is OCR'd when its native text is too SHORT *or* GARBLED.** The length-only gate
    ("< MIN_PAGE_TEXT chars ⇒ image ⇒ OCR") misses two mojibake classes ACB's 2013-2015 filings
    embed, so `_native_garbled` catches both:
    - **SUBSTITUTION** — a TCVN3/VNI legacy font maps every accented letter to an ASCII one
      ("Bảng cân đối kế toán" → "Bine can ddi k6loAn"). Words stay medium-length, so the token
      test cannot see it, but the **diacritic-per-letter ratio collapses to ~0.00** where genuine
      Vietnamese runs ~0.10-0.13 (threshold 0.02). This is the tell that recovered ACB's balance
      sheets, whose short-token fraction (0.24-0.38) sat *below* the 0.40 gate.
    - **SHREDDING** — a broken CMap that fragments every diacritic word ("LƯU CHUYỂN TIỀN" →
      "llfu chuyttn t n"), spiking the ≤2-char-token fraction (real ~0.23, mojibake ~0.45+;
      threshold 0.40).
    Genuine Vietnamese text trips neither, so VCB's clean-text pages are unaffected.
  - **Finding a statement's pages takes FOUR signals, because any one of them fails.** OCR
    mangles the form code's DIGITS — VCB's Q4-2021 balance sheet prints `Mẫu BU2/TCTD-HN`,
    `Mẫu Bữ2/TCTD-HN` and `Mẫu BUT/TCTD-HN` across its three pages (`0`→`U`/`ữ`, `5`→`S`,
    `B`→`H`), so all three failed a `B\d{2}` match and the balance sheet vanished outright:
    1. the **form code** (`Mẫu B02/TCTD-HN`), when its digits survive;
    2. else the **statement title** in the same header — it OCRs far better, but must be
       matched *fuzzily* ("Bảng cân đối kế toán" on one page, "Hãng cần đải kếtoán" on the
       next) and *only within the header block*, since the auditor's report NAMES every
       statement in its prose;
    3. **contiguity** — a table page whose header OCR destroyed entirely belongs to the
       statement running through it;
    4. **order** — a filing prints balance sheet, then income statement, then cash flow, so a
       "cash flow" appearing before the balance sheet is not one. That, plus discarding
       title-only pages *detached* from the form-coded run, is what keeps the auditor's report
       out. Those audit pages carry no "Triệu VNĐ" header, so sweeping them in also made the
       unit come out **×1 instead of ×10⁶** — a uniform 10⁶ error that reconciles perfectly.
       `unit_of()` therefore consults every page of the statement, never just the first.
  - Rows are rebuilt from **word coordinates**, and the period columns found by clustering the
    numbers' **right edges** (figures are right-aligned; clustering centres lets a wide number
    bridge two columns). The left of the page holds the section numbering and the *Thuyết
    minh* note reference — which are numbers too, and counted as a period column they drag the
    label boundary left of the labels and every label parses as empty.
  - **Three page-classification hardenings, found by the onnx re-parse of ACB** (line-level
    detection surfaces failures word-level OCR hid). All engine-agnostic:
    - **A table of CONTENTS is not a statement.** A filing's "NỘI DUNG" page lists every
      statement WITH its form code; a real statement page carries exactly ONE. `_page_kind`
      rejects a page bearing two or more distinct form codes (else it was classified as the first
      statement it named, anchoring the run pages early and feeding its page numbers into the
      column clustering).
    - **The BEST-matching title wins, not the first to clear the threshold.** A form code with an
      OCR-mangled digit ("Mẫu BO4/TCTD-HN", letter O for 0) falls through to title matching, and
      page boilerplate can score above threshold for the wrong statement in dict order — ACB's
      cash flow was lost that way though "lưu chuyển tiền tệ" is in its header verbatim.
    - **Drop the note column by MAGNITUDE, not position.** A line-level detector emits one tight
      "Thuyết minh" column inside the value zone; it becomes column 1 and `_first_value` reads
      every line's note number as its figure. A period column's numbers are 4-9 digits, a note
      reference 1-2, so `value_columns` drops any surviving column whose median is ≤ 2 digits.
  - **The scans are stored `/Rotate 180`.** PyMuPDF rasterises them upright to OCR — so the
    text is correct — but hands back word boxes in *unrotated* space, mirrored. Left and right
    swap and the parser's whole premise inverts. Clearing the rotation is not a fix (OCR then
    reads an upside-down image); the boxes are mapped through the rotation matrix instead.
- **`cafef_financials.py` — the archive → CSVs.** Picks the *consolidated* filing per quarter
  (preferring reviewed/audited), maps its rows onto the canonical schema, gates it, and writes
  a contiguous quarter grid — a quarter it could not read is a blank `source=missing` row,
  never zero-filled.
  - **The file name carries its report** (`bs_` / `is_` / `cf_`), formed in ONE place —
    `statement_path()`. The directory is still the authority on which statement a file holds;
    the prefix is for the file once it has left the directory, where three tabs all reading
    `HOSE_ACB.csv` say nothing. Readers call the helper rather than rebuilding the name.
  - **`method` records WHICH PARSE LAYER read each statement** (`onnx@200`, `tesseract@200`,
    `onnx@300+relax`), blank on a `cafef` / `missing` row. The cascade always knew it and used
    to discard it at the CSV boundary. It is what makes the layer mix visible: on ACB the
    income statement never left `onnx@200`, while **over 40% of cash flows needed a relaxed
    layer** — and it predicts a re-run's cost, since a tesseract quarter is minutes where an
    `onnx@200` one is seconds.
  - **⚠️ A COLUMN ADDED HERE MUST BE ADDED TO `CAFEF_FINANCIAL_META_COLS`** in
    `data_preprocessor.py`. That list is defined by exclusion — a line item is "any column
    that is not meta" — so an unlisted text column is fed to a decimal cast and the bronze
    ingest fails.
  - **⚠️ A FAILED CafeF TAB VOIDS THE WHOLE REPORT'S FALLBACK, by design.** The tabs come in
    SECTIONS ("NV" is every liability and equity line, "HDTC" every financing line), and
    filling a quarter from only the sections that answered writes a row that reads as complete
    while missing half its accounts — ACB's 2008-09 balance sheets came out with 54 of 107
    columns and a blank `tong_no_phai_tra` that way, from three timeouts in one run. `_get`
    now retries 3× with backoff, and a section that still fails drops that report's fill
    entirely.
  - **Snapshots to disk after EACH quarter** (atomic temp+rename), so a long OCR run's progress
    is visible on disk and survives an interrupt. The mid-run snapshots are *progress views
    only*: income statements are still cumulative (de-cumulated at the end) and CafeF-tab
    fallback quarters are still absent, so **only the completed run's output is authoritative**.
- **⚠️ ROWS ARE MAPPED ONTO THE SCHEMA, NOT KEYED ON OCR TEXT** (`map_to_schema`). Keyed on
  what OCR read, the same printed line becomes a *different column every quarter*: VCB's
  balance sheet produced **332 columns against a 90-column chart of accounts**, so nothing
  lined up in time and the panel was unusable. Mapped, a line is the same column in every
  quarter and across every ticker on the template.
  - Matching walks the schema and the parsed rows together **in statement order**, and is
    fuzzy because OCR damages the names ("TỔNG NỢ PHẢI TRẢ" → `tong_nuphai_tra`). Order is
    what keeps a fuzzy match honest.
  - **The threshold is 0.80 and must not be lowered.** A shorter accounting name is a
    subsequence of a longer one far more often than it looks: *TỔNG VỐN CHỦ SỞ HỮU* scores
    **0.75** against *TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU* — every character of the first
    appears, in order, inside the second. At 0.72 total equity captured the grand total's
    column, the real grand total had nowhere to go, and **48 of 69 balance sheets** were
    rejected for "assets != liabilities + equity".
  - **The current-period figure is the first POPULATED column, not literally `values[0]`**
    (`Statement._first_value`, used by `find` / `map_to_schema` / `_anchor`). OCR sometimes
    **over-segments the columns** — a left-hand note-reference number clusters as its own
    spurious column — pushing the real value into index 1. ACB's Q2-2010 grand total parsed as
    `[None, 176999825…, None, 167881047…, None]` (5 columns where there are 2); a strict
    `values[0]` read it as empty and rejected the balance sheet for "no total assets" even
    though the figure was correct. Taking the first non-None value is purely additive — when
    column 0 is populated (the normal case) it returns exactly what `values[0]` did — and
    recovered **+18 ACB balance sheets**.
  - **The anchor re-match tolerates a heavily damaged label ONLY when it is near full length**
    (`ANCHOR_MATCH_LONG` 0.80 gated on `ANCHOR_LEN_RATIO` 0.85, below the strict `ANCHOR_MATCH`
    0.86). ACB's Q4-2014 grand total reads `tong_ng_pha_tra_va_von_chu_sd_hoij` (nợ→ng, phải→pha,
    sở→sd, hữu→hoij), scoring 0.808 against the target — right, but under 0.86. The length gate
    is what keeps this safe from the 0.80-threshold trap above: the false match (*TỔNG VỐN CHỦ
    SỞ HỮU*) is **short** (58% of the target length, 0.73 ratio) and stays rejected, while the
    real damaged total is **full length** (26/26 chars) and is accepted. Validated with zero
    regression across VCB/ACB, but net effect on the full run is small — the balance sheets it
    recovers in isolation often hit the magnitude guard (`sane`) instead.
  - **⚠️ MATCHING IS A MONOTONIC ALIGNMENT, NOT A GREEDY WALK** (`_align`, 2026-07-28). The
    ordered walk it replaces let whichever row asked FIRST take an account, and never went back —
    and accounting names NEST, so the row that asks first is very often the wrong one. ACB's
    Q1-2022 prints the parent *Tiền gửi VÀ cho vay các TCTD khác* (54,337,806) above its two
    children; the parent's name CONTAINS *cho vay các TCTD khác*, so it scored the containment
    0.95 against `iii_2` and took it, the real `iii_2` (2,960,720) found the cursor past it and
    settled for `iii_3`, and a provision line ended up holding a loan balance. Scoring the whole
    (row × account) grid and maximising the TOTAL fixes it with no new threshold, because the
    right answer is worth more — `parent→iii_, child→iii_1, child→iii_2` scores 0.70+1.00+1.00
    against the greedy 0.95+0.95. Order is still what keeps a fuzzy match honest; it is now a
    property of the alignment rather than of the cursor. Short damaged fragments keep working
    (`hoanlai` → line 8, `chiu_rui_ro` → `hdkd_19`) since each only has to beat what competes for
    that line. Costs ~4× the `_label_score` calls — tens of ms, irrelevant beside OCR.
  - **ONE PRINTED LINE FILLS ONE COLUMN** (`_claim`). Provenance is tracked through the
    alignment, `_anchor` and `_recover_totals`, so claiming a line RELEASES whatever else that row
    had been given. ACB's Q1-2022 reads *Dự phòng rủi ro khác* and *TỔNG NỢ PHẢI TRẢ* as one row:
    the ordered pass gave its 480,433,095 to `vii_3_du_phong_rui_ro_khac` and `_anchor` gave the
    same figure to `tong_no_phai_tra`. The second is right; the first was a provision line holding
    total liabilities, and nothing downstream could tell, because both were merely "mapped".
  - **A MERGED ROW IS SPLIT AT ITS LINE MARKER** (`_split_merged`). OCR joins a section header to
    the line beneath it, and the header then wins the match on containment — so a *title* holds a
    figure it cannot have while the real line comes out empty. The seam is the marker the filing
    prints: a two-digit number (`09.`, `15.`) or a roman numeral (`XII.`). Single-letter numerals
    are excluded, because `…_v_1` is a note reference at the END of an ordinary label.
    - **⚠️ LOOK FOR THE SEAM IN THE FULL LABEL, NOT THE KEY.** `PdfParser.slug` caps a row key at
      **60 characters** — ample for a real line, and a merged row is long BY DEFINITION, so the
      cap throws away exactly the marker needed to split it. ACB's Q1-2022 reads three printed
      lines as one row carrying 44,323,457; the key stops at `…cac_khoan_no_chinh_phu` and the
      `II` that owns the figure is gone. Re-slugging the label uncapped recovers it. When no seam
      is found in either, the key is returned byte-identical, so ordinary rows are untouched —
      raising the cap globally would instead shift every long label's score.
  - **THE FILINGS ABBREVIATE WHERE THE SCHEMA SPELLS OUT** (`ABBREV` / `_expand`). *vay các TCTD
    khác* against *vay các tổ chức tín dụng khác* shares almost no characters and scores ~0.70, so
    the line is simply lost — which is how `ii_tien_gui_va_vay_cac_tctd_khac` came to hold its own
    child's figure: neither child could reach its own account, so one won the PARENT's slot on
    containment instead. Expanded on both sides before scoring, each child matches exactly and the
    parent is left alone. Currently TCTD / NHNN / TSCĐ / TNDN / BĐSĐT.
  - **Reconciliation reads its subtotals from the CANONICAL columns**, not by searching OCR
    text. Searching the text is what most rejections actually were — the row was parsed, its
    figure correct, and the lookup simply could not recognise the name OCR had mangled.
  - The output carries **every column the schema defines, in its order** — a line the filings
    never reported is an empty column, not an absent one, so every ticker on a template
    produces a table of the same shape.
  - **Nothing is written unless it reconciles** against the statement's own printed subtotals
    AND is of a sane magnitude beside its neighbours. A wrong figure is worse than a gap.
  - **Three ways to be wrong that no single check catches:** *units* (most filings are Triệu
    VNĐ, VCB's 2009 ones plain đồng — out by 10⁶ and still reconciling); *cumulative* (a
    semi-annual filing prints ONLY the Jan-Jun column, so its income statement is not the
    standalone quarter — VCB Q2-2024 prints PBT 20,835bn where the quarter is 10,116bn, and
    the cumulative figures balance perfectly against each other); *OCR* (a misread digit).
    The half-year case is handled by de-cumulating, YTD − the quarters already accepted.
  - **⚠️ …BUT SOME INTERIM FILINGS PRINT THE STANDALONE QUARTER TOO, and then de-cumulating
    REMOVES IT TWICE** (`Statement.quarter_column`, 2026-07-30). VCB's Q2-2014 prints four
    columns — "Quý II" (this year, last year) AND "Lũy kế từ đầu năm" (this year, last year) — so
    column 0 is already the quarter. Subtracting Q1 turned interest income 6,928,272 into 226,646
    and gave **PBT −154,988 for a bank that earned 1,345,661**, and it was WRITTEN: a quarter
    column reconciles against itself perfectly, and `sane` fails open in a subset run for want of
    neighbours. The index says a filing is cumulative; only the STATEMENT says whether it is, so
    the parser reads the column HEADINGS and `build` clears `half_year` when both "quý" and
    "lũy kế" appear.
    - **Detected from the header WORDS, not from `n_columns == 4`** — four columns can equally be
      a note reference plus an over-segmented pair, which is the very thing `_first_value` exists
      to survive. The words are the filing stating outright what it prints.
    - **NOT layer-scoped, unlike the relaxations**: a wrong de-cumulation is wrong at every
      layer, so it cannot be a fallback. Verified in both directions — ACB Q2/Q4-2022 and VCB
      Q2-2024 / Q4-2023 all still report `quarter_column=False`, `n_columns=2` and keep
      de-cumulating (VCB Q2-2024 is the filing that prints PBT 20,835bn cumulative against a
      10,116bn quarter, so it MUST).

### GICS — `gics_scraper.py` (reference taxonomy; requests + openpyxl)
- Downloads MSCI's published **"GICS structure & definitions eff. 17 Mar 2023"**
  `.xlsx` and parses it with `openpyxl` into a flat CSV. Independent of the other
  sources (does not use `SwitchHandler` or the ticker universe).
- **Cleaning:** forward-fills codes (present only on first occurrence per level);
  drops 7 `(Discontinued)` sub-industries (170→163) and 2 discontinued industries
  (76→74); strips change-annotation parentheticals (`(New Code)`, `(Definition
  Update)`…) while keeping semantic ones (`(HMOs)`, `(except bauxite)`); pulls each
  sub-industry's definition from the row directly below it. Sanity check =
  `EXPECTED_COUNTS (11, 25, 74, 163)`.
- **Output:** `raw_data/gics/gics_2023_official.csv` (one row per sub-industry, all
  four levels + code + name + snake_case + definition); the raw xlsx is kept for
  provenance.

## 4. Source specialization (why 3 price sources)

Matches the bronze-source decision (memory `project-bronze-source-per-field`):

| Field | Primary | Notes |
|---|---|---|
| OHLC / volume / foreign flow | **Simplize** | fully adjusted, true volume, most complete |
| split-only / negotiated volume, raw vs adj close | **CafeF** | matched/negotiated split, `close_raw`/`close_adjust`, '000 VND |
| universe (which tickers exist) | **TradingView** | the link CSVs everyone else reads |
| fundamentals as filed (the source of truth) | **CafeF PDFs** | the statements CafeF's own API transcribes; the only place its gaps exist |
| news / disclosure events | **CafeF** | headline + body + filing PDF link, categorised |

- TradingView prices are **split/stock-div adjusted but NOT cash-div adjusted**
  unless the ADJ toggle fires (memory `project-vcb-price-adjustment`).
- Simplize/CafeF are pure `requests` (fast, no browser). TradingView needs Selenium
  because the data only exists in the client-side chart widget.
- **CafeF is the sole source** of the order-flow tabs that neither Simplize nor TV
  expose — order-placement stats (`order_stats/`), proprietary-desk trades
  (`prop_trading/`), and insider/major-shareholder transactions (`insider_txn/`); see
  §3. These are orthogonal signals worth noting for modelling (cf. `src/model/CONTEXT.md`).

## 5. How it's driven — SwitchHandler + `src/switch_config.json`

- `SwitchHandler` (`src/utils/switch_handler.py`) reads a **flat JSON of
  slash-path → bool**. A path is enabled only when **every prefix is explicitly
  true** (disabling a parent disables the whole subtree); missing key = false; keys
  starting `//` are inline comments.
- Two APIs: `is_enabled("a","b","c")` (all-ancestors check) and
  `get_enabled_paths(*prefix)` (returns enabled **leaf** paths — used by the TV task
  adders to enumerate exactly which `(country, stock_type, sector)` etc. to scrape).
- Config hierarchy for TV:
  `web_scraper/trading_view/{links|collected_links|data}/{asset}/{dim1}/{dim2?}/{dim3?}`.
  CafeF/Simplize/GICS are **not** switch-gated at the asset level — they run
  wholesale when `main.py` calls them (they take `switch_handler` but CafeF/Simplize
  use it only via the shared base; GICS ignores it entirely).
- **Current committed state:** `"web_scraper": false` (master off). When enabling,
  set `web_scraper` + the `trading_view/links` (or `/data`) subtree you want.

## 6. Shared infra it depends on (outside this dir)

- `src/utils/constants.py` — all `SCRAPER_*` knobs (`SCRAPER_START_DATE` 2000-01-01,
  `SCRAPER_END_DATE` 2026-04-30, retries 5×5s, 8 browsers, 8s nav stagger,
  `SCRAPER_MAX_WORKERS` 16), `*_RAW_DATA_DIR` paths, `TRADING_VIEW_TABLE_SCHEMA`, and
  `SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY`.
- `src/thread_manager/thread_manager.py` + `dtos/thread_manager_dtos/task.py` —
  `Task(name, func, *args)` queued and run by `ThreadManager`;
  `task.run()` calls `func(*args)` directly (no lambda wrapper).
  - **Thread-pool sizing.** `ThreadManager` takes an explicit `max_workers` that
    pins the pool to exactly that many threads, falling back to the CPU-proportional
    `power` formula only when `max_workers is None`. Every `BaseScraper` subclass
    defaults `max_workers=SCRAPER_MAX_WORKERS` (16) — the right knob for these
    I/O-bound (`requests`) scrapers, where the count is not tied to CPU cores. Pass
    `CafeFScraper(logger, max_workers=N)` (or any scraper) to override per run.
    NB the `power` path alone previously yielded a *fractional* worker count
    (`cpu*power/100*0.4` → e.g. 2.4), so the pool ran ~2 threads regardless of the
    machine; the explicit `max_workers` is what makes the thread count real and
    predictable. TradingView is still separately capped at
    `SCRAPER_MAX_CONCURRENT_BROWSERS` (8) browsers by its own semaphore, so a wider
    pool there only deepens the task queue, it does not open more Chrome instances.
    - Every scraper that overrides `__init__` (`CafeFScraper`, `CafeFPdfScraper`,
      `CafeFNewsScraper`) now takes `max_workers` and forwards it to `super().__init__`
      — omitting it there raised `TypeError: unexpected keyword argument 'max_workers'`
      the moment a caller passed one (it bit the news run).
    - **`CafeFNewsScraper` is TWO nested pools:** `max_workers` sets how many *tickers*
      run at once, and a separate per-ticker `ThreadPoolExecutor(ARTICLE_WORKERS=8)`
      fetches article bodies. Peak concurrent HTTP ≈ `max_workers × ARTICLE_WORKERS`
      (e.g. 8×8 = 64), so raise the ticker pool cautiously — it multiplies.
- `src/utils/enums.py`, `src/utils/utils.py` — the `Country`/`StockType`/`Sector`/…
  enums and the `build_*_link_scrape_actions` / `format_key_for_{path,name}` helpers
  that TV imports via `from utils.* import *`.
- `src/logger/logger.py` — `Logger` (all sources log to `logs/app.log`; truncate it
  before a run per memory `feedback-clean-app-log-before-run`).

## 7. Gotchas

- **Order matters:** CafeF/Simplize read TV's link CSVs for their universe — run TV
  (at least the links phase) first, or they scrape nothing. The two newer CafeF scrapers do
  the same, but both take a `symbols=[(exchange, symbol), …]` override, which is how a run
  is scoped to VN30/VN100 instead of all ~777 codes.
- **Nothing added since `CafeFScraper` is wired into `main.py` yet.** `CafeFPdfScraper`,
  `CafeFNewsScraper` and the whole PDF-reading pipeline (§3a) are registered and importable,
  but nothing drives them — `main.py` still runs only TradingView / CafeF / Simplize / GICS.
  Note `CafeFPdfScraper` must NOT be pointed at the full 777-ticker universe by default: the
  archive averages ~1.0 GB *per ticker* (VN100 alone is ~97 GB), so it takes a `symbols=`
  override — it is currently run for VN100 (all 100 tickers on disk).
- **There are FOUR financial-statement schemas, not one** (bank / corp / securities /
  insurance) and they share no line items. Pick one by **fingerprinting the ticker**
  (`cafef_schema.detect_template`), never by its GICS sector or industry group — HVA sits in
  the securities group and files corporate, and *Tài chính* spans all three financial
  templates (§3a).
- **The cash-flow method (direct/indirect) is chosen by the COMPANY**, so it must be read from
  the filing, not inferred from the sector or the template (§3a).
- **Some companies file NO consolidated report** — a single entity with no subsidiaries only
  ever produces the parent-company one (NT2, PPC, IMP, AGP among the 50 sampled). Code that
  filters to `consolidated == True` returns nothing for them and they look like tickers with
  no data; it needs a fallback to the parent report.
- **CafeF serves two CDN hosts and only one of them has everything.** `cafefnew.mediacdn.vn`
  404s on entire years of some tickers (all of VIC 2020-21); those files exist only on
  `cafef1.mediacdn.vn`. Any code fetching a CafeF-hosted file must try both — and must not
  retry a 4xx, which only delays reaching the host that works.
- **A truncated download does not raise.** It yields a PDF that opens and reports its true
  page count, with the missing pages failing individually — indistinguishable from a corrupt
  file. Verify `Content-Length`.
- **`web_scraper` master switch is `false`** in the committed config; TV's
  `scrape()` will no-op every phase until it (and the desired subtree) is enabled.
- **TV `close()` vs `quit()`:** the data path calls `web_driver.close()` in its
  `finally` (the links path uses `quit()`). `close()` can leak the driver process if
  it were the last window — harmless here because each data task uses its own driver,
  but worth knowing.
- **CafeF needs UPPERCASE `ExchangeType`** for HNX/UPCOM tickers or it silently
  defaults to HOSE and returns empty.
- **CafeF prices are '000 VND** — `_mul` ×1000 is applied to OHLC + both closes but
  NOT to volumes/values (those come pre-scaled).
- **`skip_existing=True`** on the CafeF/Simplize per-stock scrapes means re-running
  skips any ticker whose CSV already exists — delete the file (or pass `False`) to
  refresh. (CafeF now has one scrape per tab: `scrape_price/_foreign/_order_stats/
  _prop_trading/_insider_txn`, each guarding its own folder.)
- **GICS URL is version-pinned** to the Mar-2023 xlsx (GUID + `?t=` token required);
  if MSCI revises the structure the `EXPECTED_COUNTS` check logs a warning rather
  than failing.
- **`raw_data/` is the handoff to `src/data_preprocessor`** — schema/column names
  here are the contract its bronze ingest expects; changing an `OUTPUT_COLUMNS` list
  ripples downstream.
- **CafeF folders are coupled to the preprocessor:** each folder now lands as its
  own bronze table — `data_preprocessor._ingest_bronze_cafef_*` (`_price`, `_foreign`,
  `_order_stats`, `_prop_trading`, and `_insider_shareholder_transactions` from the
  `insider_txn/` folder — all five tabs are ingested), so renaming a folder or its
  columns requires updating the matching method. Note the `insider_txn/` folder maps
  to the `cafef_insider_shareholder_transactions` table (folder name ≠ table name).
  `price/` + `foreign/` are re-merged on (symbol, date) in **silver**
  (`_ingest_silver_stocks`), not bronze. `order_stats/` / `prop_trading/` /
  `insider_txn/` reach bronze but are **not yet consumed by silver/gold** — turning
  them into signals is future preprocessor work.

## 8. Index-membership reference files — `vn30.csv` / `vn100.csv` (repo root)

Two small static lookup CSVs listing the VN30 and VN100 constituents with basic
info. **Not produced by the scrapers** — assembled by joining a hardcoded
membership list to `raw_data/simplize/industry.csv` (§3, Simplize industry). All
constituents are HOSE-listed.

- **Columns:** `no, ticker, exchange, economic_sector_name, industry_group_slug,
  industry_group_code, industry_activity` (the last four = Simplize's GICS-based VN
  taxonomy; UTF-8 **with BOM** so the Vietnamese names render in Excel).
- **Membership sources:**
  - VN30 = `UNIFIED_TICKERS` in `src/utils/constants.py` (30 tickers).
  - VN100 = the hardcoded `VN100` list in
    `experiment/experiment_1/dl_signal/dl_vn100_pooled.py` (100 tickers).
- **"Basic information" = ticker + exchange + industry only.** No company full-name
  field exists anywhere in the repo (no scraper captures it), so names are not
  included.
- **Coverage caveat:** VN30 is fully populated; in `vn100.csv` **4 tickers
  (`DSE, KOS, SIP, VPI`)** are absent from `industry.csv` (not in the
  TradingView-derived universe at last scrape) so their industry columns are blank.
- **Regenerate:** re-run the join if `industry.csv` or either membership list
  changes (the generator lived in session scratch, not the repo — recreate from the
  two sources above).
