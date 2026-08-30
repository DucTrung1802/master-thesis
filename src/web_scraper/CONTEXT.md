# Context — `src/web_scraper` (raw-data acquisition layer)

> 🗺️ **Project hub: [CLAUDE.md](../../CLAUDE.md)** — the whole project in ~5k tokens
> (verdict, chain, standing rules, routing). Read that first; this file is the depth
> behind one stage.

> Handoff notes for a new session. Describes the web-scraping subsystem: how the
> data sources are structured, what each pulls, how they are driven, and where
> output lands. This is the **bronze-input** stage — it writes CSV/xlsx/PDF under
> `raw_data/<source>/`, which `orchestration/preprocessor` then ingests into
> `bronze_schema`. Verify anything before acting on it — the code is the source of
> truth.
>
> ## ⚠️ THE DRIVER CHANGED — `main.py` AND `switch_config.json` ARE DELETED
>
> **Nothing here is run by editing a config file any more.** `src/main.py` and
> `src/switch_config.json` were deleted on 2026-08-05/06 (orchestration phase 5), and
> so was `src/data_preprocessor` — its contents moved to
> [`src/orchestration/preprocessor/`](../orchestration/preprocessor/CONTEXT.md).
> **Every scraper is now a Dagster asset and `--select` is the run plan**; see
> [`src/orchestration/CONTEXT.md`](../orchestration/CONTEXT.md).
>
> **Not one line of the scrapers changed**, which is why the rest of this file is still
> accurate. What changed is who calls them. The two places that described the old
> driver are marked as history: the diagram below, and §5.

## 1. Big picture / pipeline

```
dagster asset materialize -f src/orchestration/definitions.py --select "…"
                       │
                       ▼   (19 landing assets, assets/scrape.py)
                  TradingViewScraper            ← universe authority (link CSVs)
                       │                            + OHLCV per symbol (Selenium)
                       ▼
                  CafeFScraper                  ← per-stock fields TV lacks (requests)
                  CafeFIndexScraper             ← the same 4 tabs for the 6 MARKET INDICES
                                                   (requests; needs no TV links)
                  CafeFNewsScraper              ← company-news / disclosure feed (requests)
                  CafeFPdfScraper               ← the filing PDFs (requests)
                  FinancialsBuilder.build_all() ← OCRs those PDFs → statement CSVs (LOCAL,
                                                   no network; must follow the line above)
                  SimplizeScraper               ← validated daily-panel backbone (requests)
                  GicsScraper                   ← MSCI GICS taxonomy (independent)
                       │
                       ▼
                  raw_data/<source>/...  (CSV + PDFs + the raw GICS .xlsx)
                       │
                       ▼
             orchestration/preprocessor → bronze_schema
```

> **Historical, and it is why the ordering below is worth keeping.** This diagram used
> to read `src/switch_config.json ──(feature flags)──► src/main.py ──► …scrape()`, with
> the note that the committed config had `"web_scraper": false` so the whole stage was
> OFF by default. Both files are gone; the dependency ORDER they enforced by hand is now
> declared as asset edges, and Dagster walks it.

- **TradingView is the universe authority.** CafeF and Simplize both derive their
  `(exchange, symbol)` list from the TradingView **stock link CSVs**
  (`get_stock_symbols()` reads `raw_data/trading_view/links/stocks/**/*.csv`),
  so TV must run first; the other two enrich the same universe. ⚠️ **This is now a
  declared Dagster edge on the `stocks` PARTITION only**
  (`SpecificPartitionsPartitionMapping(["stocks"])`) — CafeF and Simplize do not need
  the other eight asset classes. `orchestration/CONTEXT.md` §2 has the full audit,
  including three edges the prose here originally got wrong.
  - **`CafeFIndexScraper` is the exception — it needs no links at all.** An index has
    no TradingView link CSV, so its universe is a fixed six-entry list on the class
    (`INDEXES`). It can therefore run standalone, before TV has ever been run.
  - **`CafeFPdfScraper` is the other exception** — alone among the CafeF scrapers it
    never calls `get_stock_symbols()`; its universe is `vn100.csv`.
- **Each source writes one CSV per stock** (except GICS = one taxonomy CSV, and
  TV links = one CSV per filter leaf). All writers use **temp-file + atomic
  `os.replace`** so an interrupted run never leaves a partial file that
  `skip_existing` would treat as complete.
- **What each scraper ENUMERATES is still a flag tree**, and that part survives — see
  §5. What is gone is the tree deciding whether the stage RUNS.

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
| `index_price/`, `index_order_stats/`, `index_foreign/`, `index_prop_trading/` | `cafef_index_scraper.py` | the 6 market indices, full history (§3, *CafeF indices*) |
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
| income_statement | 71 | **69** | 2 | 46 | +23 |
| cash_flow | 71 | **69** | 2 | 61 | +8 |

**206 of 213 read from the filings, +42, and NOTHING was lost** — no quarter that read from a
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
- ⚠️ **CafeF's tabs WERE the fallback (`from_api`) — FORBIDDEN AS A SOURCE SINCE 2026-08-24.**
  **CLAUDE.md §5 rule 24: a financial statement value may come from the filing PDF and from
  nothing else. A quarter no readable PDF can produce is `missing`, and `missing` is the
  correct answer.** ⚠️ *This bullet used to read: "For a quarter whose scan is unreadable this
  is the BETTER source, not the lesser one: the tabs are keyed by the same item CODES the
  schema was built from, so a value lands on its canonical column exactly — no OCR, no fuzzy
  match."* That argument is kept because it is the reason the fallback was built and it is
  **not wrong about the mechanism** — it is overruled on a different ground: **a transcription
  is somebody else's parse of the document**, and once it is in the table nothing downstream
  can tell it from the filing. The two paragraphs below this one are the evidence for the
  overrule — eight CafeF values are now confirmed WRONG against the filings, and its
  "not reported" sentinel is a literal `-1`.
- ⚠️ **THE CODE HAS NOT CAUGHT UP AND THE DEFAULT IS STILL ON**: `use_api: bool = True`
  (`cafef_financials.py:485`, `:1629`), and the fallback fires on any absent period **without
  checking whether a PDF exists**. Measured 2026-08-24 from `bronze.cafef_financial_reports`:
  **ACB 195 `pdf` / 27 `cafef` / 0 `missing`; VCB 209 / 7 / 18**. ⚠️ **Only FOUR of the 34
  can be retried from a document on disk, and all four are VCB** — `documents()` above keeps
  `consolidated == "True"` only, and **ACB filed no consolidated statement before 2010**
  (2007-09 are parent-only), so its 27 rows are unreachable without changing that rule. That
  is `FIN-1`. **Read the `source` column
  before quoting any fundamental.**

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
  → onnx@200+title → onnx@200+title+relax → onnx@200+loose → onnx@200+loose+relax

⚠️ **THAT ARROW LIST IS A SNAPSHOT AND HAS NOT BEEN RE-SYNCED — `LAYERS` holds 47 entries as of
2026-08-27.** Everything appended since (`+realign`, `+notes`, `+seam`, `+tail`, `+unit`,
`+annual`, `+extra`) is absent from it. **The list in the code is the list**; read `LAYERS` and
its comments, and treat the arrows above as history rather than as an index.

**The thirteen `+components` / `+pad6` / `+split` / `+join` / `+title` / `+loose` layers are appended, never inserted**, so a
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

  ⚠️ **AND THE BOX CAN END INSIDE THE NUMBER TOO — measured 2026-08-27 on BID's FY-2016
  consolidated cash flow, each config run twice on the page itself.** The filing prints
  `6.711.633`; the default crop loses the last digit at every resolution and `parse_num` turns
  each corpse into a plausible number:

  | | what OCR returns | `parse_num` |
  |---|---|---|
  | `onnx@200` | `6.711.6.3` | **671,163** — 10× low |
  | `onnx@300` | `6.711.610` | **6,711,610** — off by 23 |
  | `onnx@400` | `6.711.63)` | **671,163** |
  | **`onnx@200+pad6`** | **`6.711.633`** | **6,711,633** ✅ |

  ⚠️ **Three of the four are WELL-FORMED thousands groups, so no grouping check can catch them**
  — and the 300 dpi read is wrong by 23 đồng in 6.7 million, which is why the identity that
  judges such a figure is held to EXACT equality and not to `_equal`'s tolerance. ⚠️ It also
  corrects a claim `CLAUDE.md` §6-2-tervicies carried: *"one line is misread at every DPI"* is
  true of the default crop and false at pad 6, which that session never tried.
- **`cash_extra_terms` — the statement has a FOURTH term and the chart of accounts has no column
  for it.** `_cash_flow_identity` tests `closing = opening + movement + fx`, and a bank that
  ABSORBS another bank gains cash that is none of the three. BID prints such a line in three
  separate years (MHB 1,477,340 in 2015 and 3,004,011 in 2016, LienVietPostBank 1,540,994 in
  2017), and its FY-2016 cash flow prints TWO at once, one per column. Both columns then close
  only with the extra term — 55,806,145 + 6,711,633 + 3,004,011 = 65,521,789 — so the quarter
  was refused for `fx not mapped` while every figure on the page was right.

  The flag sums what the filing printed BETWEEN its two balance rows and lets that stand in for
  `fx`, which it already contains. Three properties carry the design:
  - ⚠️ **COUNTED, NEVER WRITTEN.** Claiming the row as the FX adjustment puts merger cash in
    `hdtc_vi_…_ty_gia`, and the identity then CONFIRMS the wrong account because the arithmetic
    is right (`CLAUDE.md` §6-2-vicies measured exactly this on FY-2015). So the figure is
    admitted to the CHECK and the column is left empty — §5 rule 2.
  - ⚠️ **AND THE MATCHING GUARD IN `_recover_totals` IS NOT THIS FLAG'S TO GIVE — corrected
    2026-08-27, `P39`.** This bullet used to end *"for the same reason the flag stops
    `_recover_totals`' positional FX guess claiming a row whose own label does not say FX"*, and
    that wiring WAS the defect: the guard was live on the three layers carrying
    `cash_extra_terms` and absent on the other forty-four, `onnx@200+relax` — **layer 5 of 47**
    — among them. Read off `cf_HOSE_BID.csv` afterwards, the unguarded claim had already written
    merger cash into the FX column twice, from two different documents, and the identity
    confirmed both to the đồng: **Q4-2015** 50,202,708 + 4,288,806 + **1,477,340** = 55,968,854
    (MHB, FY-2015 audited annual) and **Q2-2017** 65,521,789 + 2,648,425 + **1,540,994** =
    69,711,208 (LienVietPostBank, Q2-2017 reviewed quarterly). The guard is unconditional now
    and `_recover_totals` takes no parameter that could switch it off. ⚠️ **A knob that decides
    whether a guard applies is a knob that turns a guard off.**
  - ⚠️ **THE CURRENT-PERIOD CELL ONLY, never `_first_value`.** BID's 2016 column leaves the MHB
    line blank and prints 1,477,340 beside it in the 2015 comparative; the fall-through would add
    a prior-year figure to this year's identity and break a sum that closes exactly without it.
  - ⚠️ **POSITION IS THE WHOLE DEFINITION.** Matching by label would mean guessing which words
    name a reconciling item, and filings word them differently every time. Between the two
    balances a cash flow prints nothing else, so the span needs no vocabulary — and whatever it
    returns is tested to the đồng immediately, so a span that swept in a wrong row is rejected
    rather than written.
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
- **`loose_form_code` — OCR APPENDS A STRAY DIGIT AND THE PAGE LOSES ITS ANCHOR.** VCB's Q1-2009
  prints `Mẫu số: B040/TCTD-HN`; the strict pattern tolerates a junk LETTER after the two digits
  but not a junk DIGIT, so nothing matches. That matters far beyond the code itself, because
  `_drop_islands` prunes BY ANCHOR: with no form-coded page, every notes page that fuzzy-matches a
  statement title is kept. Q1-2009's income statement came out as pages **[5, 14, 28, 29, 30] —
  57 rows of which 2 mapped** — and was refused for "no profit before tax" while its own page 5
  read perfectly. Note 15, *"Lãi/lỗ thuần từ hoạt động kinh doanh (mua bán) chứng khoán"*, clears
  the 0.80 title threshold against *BÁO CÁO KẾT QUẢ HOẠT ĐỘNG KINH DOANH*. Allows up to two junk
  characters of either kind — codes are B01..B09, so a third character is always noise. The same
  defect was silently costing VCB's Q2-2014 balance sheet its anchor (`B020`), which survived only
  by falling back to the title.
  - **One fix, two quarters, via de-cumulation.** Q4-2009's income statement always parsed
    (PBT 5,004,374, reconciled); it was dropped because the FY annual is genuinely cumulative and
    Q1-2009 was missing. **Fixing a Q1/Q2 pays for its Q4 as well** — the same shape as Q2-2014
    unlocking Q4-2014.
  - **Q4-2009 is where the de-cumulated figure BEATS CafeF.** Ours reads 697,896; CafeF says
    1,395,082. Our four quarters sum to 5,004,372 against the audited FY PBT of **5,004,374**;
    CafeF's sum to 5,701,558, too much by 697,184. This is the Q4-vs-annual class above.
- **⚠️ The parse cache key is every flag that changes the OCR** — `(engine, dpi, crop_pad,
  join_digits, title_over_form, loose_form_code, realign_rows, notes_boundary,
  tail_continuation, label_wrap, unit_from_document)`, and `_parse_cascaded` is where it lives.
  Keyed on `(engine, dpi)` alone the wider-crop layer is handed the narrow crop's cached parse —
  the one that just failed — and the layer silently does nothing. ⚠️ **`relax_merged_seam`,
  `annual_tail` and `cash_extra_terms` are deliberately ABSENT**: each re-MAPS an existing parse
  or changes a GATE, so layers differing only in them share one OCR pass. That is what makes
  `onnx@200+pad6+annual+extra` cost no OCR at all — `onnx@200+pad6+components` has already
  rendered those pages.

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

> ⚠️ **AND THE RULE HAS ONE MEASURED EXCEPTION SINCE 2026-08-28 — `_split_fx_from_balance`,
> which runs on EVERY layer including the strict ones.** "Scope it to a relaxed layer" works only
> when the defect makes a statement FAIL, because that is what escalates the cascade. VIC's
> Q1-2026 cash flow was **ACCEPTED at `onnx@200`, layer 1 of 47**, carrying its closing balance
> in the FX column — so a late layer was unreachable by construction, and a scoped fix would have
> been dead code that looked like a repair. `PGB-1` and CLAUDE.md §6-2-unvicies each recorded the
> same trap from the other side (a half-right layer that passes the gates ends the cascade). The
> generalisation: **when the gates cannot SEE the defect, the repair cannot be an escalation.**
>
> What replaces the scoping is a narrow precondition plus a real regression. It fires only when
> the closing column is EMPTY, the row holding FX begins with the FX account's own wording, and
> what follows that wording matches the closing account — and **15 statements across 5 filings
> of ACB, VCB and BID re-map identically under all 6 mapping-flag combinations the 47 layers
> use: 90 of 90 mappings** with it in. ⚠️ **Re-map at every combination, not just the strict
> default** — a filing that escalates takes a path a strict-only check never touches. ⚠️ **That regression cost minutes
> rather than hours because the ROWS were replayed, not re-parsed**: a mapping change cannot
> alter what the OCR read, so re-mapping a stored `row_dump` (CLAUDE.md §6-2-tricies) or a
> single-layer probe measures the blast radius exactly. Reach for that before booking a re-parse.
> CLAUDE.md §6-2-untricies; `ISSUES.md` `TPL-1` / `CRP-1`.

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

### ⚠️ THE FIRST CORP TICKER WAS BUILT — VIC, 27 of 72 quarters, 2026-08-29

The paragraph above says the corp/securities/insurance case *"becomes testable the first time a
corp ticker is built"*. It has been, partly: `raw/cafef_financials` partition `HOSE_VIC`,
`skip_existing: false` `allow_parent: true`, no `periods` — **stopped by hand after 12 h at 27 of
72 consolidated quarters** (Q2-2008 … Q4-2014). `_write` snapshots per quarter on a full run, so
the three CSVs under `statements/corp/` hold 27 complete rows each.

| | balance sheet | income statement | cash flow |
|---|---|---|---|
| `pdf` / `missing` | **13 / 14** | **21 / 6** | **20 / 7** |
| line items | 66 | 22 | 34 |

**0 rows from any HTML tab** (`use_api` defaults to `False` now), 8 alternate-filing retries of
which 4 recovered, and the winning layers are cheap — `onnx@200` took 42 of the 54 accepted
statements.

⚠️ **THE BALANCE SHEET FAILS ON SELF-PREPARED QUARTERLIES AND NOWHERE ELSE**, and the other two
statements are the control that makes that a finding rather than an impression:

| parse rate | audited / reviewed | unaudited quarterly |
|---|---|---|
| **balance sheet** | **12 / 13** | **1 / 14** |
| income statement | 10 / 13 | 11 / 14 |
| cash flow | 7 / 9 | 13 / 18 |

⚠️ **The mechanism is indicated as the COMPARATIVE COLUMN, and `sane` is the only gate that sees
it.** The magnitude guard refused Q1-2009, Q3-2009 and Q4-2009 on the *same* probe (6.02e+12) and
Q1-2010 / Q3-2010 on another (1.43e+13) — a quarterly balance sheet prints the prior year-end
beside the current period, and that prior figure is a quarter the run had already accepted.
`reconcile` cannot catch it: a comparative column balances against itself. The rest of the
refusals (11 × `assets != liabilities + equity`, 3 × `no total assets`) fit two columns being
mixed. ⚠️ **Not verified against the PDF's own columns** — what is measured is the assurance
split, the repeated probes and the refusal mix.

⚠️ **A resumed run is not available.** `skip_existing: true` or `periods` makes it a subset run
and flips `sane` to failing open — four measured downgrades in this repo — so the next VIC attempt
is the same 72-quarter run from scratch, and it should wait for `P5`'s remaining half.
CLAUDE.md §6-2-duotricies; `ISSUES.md` `CRP-1`.
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
├── cafef_index_scraper.py    SOURCE_NAME="cafef_index"   (subclasses CafeFScraper; 4 tabs × 6 indices)
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
tabs, `cafef_index_scraper` pulls those same tabs for the market indices,
`cafef_pdf_scraper` downloads the filings, `cafef_news_scraper` pulls the event stream.
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
- **⚠️ How LINKS are extracted, and the one selector that broke (2026-07-31).** The
  symbol list is **virtualised** — only visible rows exist in the DOM — so
  `_helper_extract_trading_view_links` scrolls a container and unions what renders. Two
  selectors, and they are NOT equally trustworthy:
  `div[data-role="list-item"][data-symbol-name]` is semantic markup and has been stable;
  `.scrollContainer-<hash>` carries a **build hash that rotates on every TradingView
  redeploy**. It rotated (`FSX6AatX` → `Q9nrHY0X`) and **all 140 links tasks collected 0
  symbols, wrote header-only CSVs and reported SUCCESS** — the rows were on the page the
  whole time (50 rendered for `stocks/vietnam/finance`, `HOSE:VCB` first); the old JS did
  `if (!container) return { symbols: [] }` and threw them away. Three fixes, all in place:
  - the container is matched by **hash-free prefix** (`[class*="scrollContainer"]`), with
    the first scrollable ancestor of a list item as a fallback — and symbols are returned
    even when the lookup fails;
  - **progress** is "container moved **OR** a new symbol appeared", not scroll position
    alone. Scroll-only was what let a dead container end the loop after 3 no-op passes;
  - it **raises `PipelineError`** when rows rendered but nothing was read, or when
    symbols were found with no scrollable container (a first-screen-only result). A
    genuinely empty filter still returns `[]` — `futures/vietnam/*` and
    `economy/*/health` have always been empty, so 0 cannot be an error by itself. The
    20-second wait for list items is what tells the two apart.
  - The CSV is now replaced **after** a successful scrape. The delete used to run before
    the browser even opened, so a broken run overwrote good data with a header.
  - **The lesson:** anchor on `data-*` attributes, never on a CSS-module class. This was
    the only hashed selector in the repo; the `TradingViewXpath` locators use semantic
    attributes and were unaffected.

  Verified after the fix, one leaf per asset class through the real task adders:
  stocks 20, funds 21, forex 48, indices 50, economy 32 links; `futures/vietnam/agriculture`
  and `bonds/vietnam/corporate` **0 and legitimately so** (header-only in June too, and
  the empty path no longer raises); crypto and options queue no tasks (see below).
- **⚠️ THE FOREX BROKER FILTER SILENTLY FAILS FOR 19 OF 47 BROKERS — issue `FLT-1`
  (measured 2026-08-14).** `build_forex_link_scrape_actions` injects the source into
  `selectedSearchSources["forex"]` as an UPPERCASE string. For **27 brokers it works**:
  the links CSV holds exactly one exchange and the folder name matches it. For **19 it
  does not** — the CSV comes back holding **49 exchanges** dominated by `FX_IDC`, which
  is TradingView's unfiltered default list. `ibroker` enumerates 0.

  It fails OPEN, which is why nothing raised for months: a broker whose filter did not
  apply still returns thousands of rows, so every count looks healthy and
  `_helper_extract_trading_view_links`' emptiness guards never fire.

  The signature is unmistakable once counted — six brokers returned **byte-identical
  ~16,700-symbol lists** (`velocity_trade` and `wh_selfinvest` identical at 16,748;
  `interactive_brokers` 16,727; `osmanli_fx` 16,718; `phillip_nova` 16,716;
  `trade_nation` 16,674), and eight more shared one 8,48x-row list, against a normal
  broker's 48–169.

  ⚠️ **The consequence lands in `data/`, not `links/`.** `_add_generic_link_data_tasks`
  fetches whatever that broker's links CSV lists, so a contaminated list makes the DATA
  folder disagree with its own name: before 2026-08-14, `capital_com/` held
  `ACTIVTRADES_*` and `SAXO_*` files, and 8 folders held one identical 50-file set.
  **The folder name tells you nothing; the filename does** — every file is named
  `<EXCHANGE>_<SYMBOL>_<start>_<end>.csv` and the EXCHANGE in it is correct.
  ⚠️ It also means a re-scrape of a contaminated broker is a multi-day job for data that
  is not that broker's book: `parameters.data_only` (see `orchestration/CONTEXT.md`)
  exists to keep the fetch on the 27 that work while links still enumerate all 47.

- **⚠️ `_add_generic_link_data_tasks` READS ONE LINKS CSV PER LEAF** —
  `sorted(csv_files, reverse=True)[0]`, the newest by filename. A leaf accumulates one
  dated CSV per run (forex brokers hold 5), so the fetch plan is the LATEST snapshot,
  not the union. Measured 2026-08-14 across the 10 fetched brokers: newest 897 symbols
  vs union 898 — one symbol (`tastyfx:eurdkk`) exists only in an older snapshot, so the
  cost here is ~0. It is worth knowing anyway: a run that came back short leaves a
  short newest CSV, and the next data phase inherits it silently.

- **⚠️ `crypto` and `options` are the two asset classes whose switch node is a LEAF** —
  no countries/sources configured beneath them — so `build_unblocked` forcing the
  run-plan ancestor true makes the NODE ITSELF an enabled path, and the adder gets a
  4-part path where it expects 5-7. `_add_crypto_links_tasks` guarded against this;
  `_add_options_links_tasks` did not, and died on `IndexError: list index out of range`
  before queueing anything — i.e. Dagster's `trading_view_links` partition `options`
  could never run. Both now warn and skip. Neither class has ever produced links
  (`raw_data/trading_view/links/` has no `crypto/` or `options/` folder).
- **How data is extracted:** navigates the chart, optionally toggles **ADJ**
  (`_helper_toggle_adj_dividends` — dividend adjustment, chart defaults ADJ **off**),
  sets a custom date range (`SCRAPER_START_DATE`=2000-01-01 → today), waits for the
  bar count to stabilize, then reads bars straight out of the in-memory chart widget
  (`window._exposed_chartWidgetCollection…`) via injected JS. A **two-layer OHLC
  detector** (structural slot-count + semantic OHLC invariants) decides OHLCV vs a
  single `value` series.
- **Concurrency hardening:** `_browser_slot()` — a `Semaphore(max_browsers)` plus a
  live/peak counter — caps Chrome instances in **both** phases,
  `_scrape_data_trading_view_link_attempt` and, since 2026-07-31,
  `_scrape_links_attempt`. ⚠️ The links path used to open a driver *outside* the
  semaphore, so the real cap there was the thread pool. The permit is taken before
  `webdriver.Chrome()` and held until `quit()`, and `_browser_slot` **raises** if a
  driver is ever born outside it.
  > ⚠️ **The cap and the POOL are one number now (2026-08-05).**
  > `TradingViewScraper(max_browsers=…)` defaults to
  > `SCRAPER_MAX_CONCURRENT_BROWSERS` (**12** since 2026-08-22, was 4, and
  > `os.getenv`-overridable) and sizes
  > `ThreadManager(max_workers=…)` from the same value. It had to: the cap was 8 in
  > the prose and 1 in `constants.py`, against a pool of `SCRAPER_MAX_WORKERS=2` — so
  > the effective concurrency was **1**, and a wider cap alone could never have been
  > reached. A pool wider than the cap buys only threads blocked on a semaphore; a
  > pool narrower than it makes the documented cap fiction. Verified with a fake
  > driver: 40 tasks → **peak exactly 4**, and **2** with the env var set to 2.
  A `_nav_time_lock` staggers navigations by
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
### ⚠️ THREE SCRAPE MODES SINCE 2026-08-23, AND `incremental` IS THE ONE FOR A REFRESH

Added for `P1`/`FRZ-1`, where 757 of 781 tickers had gone stale and the only refresh the
scraper could do was a full refetch from 2009. **They are three modes, not points on one
scale**, and picking the wrong one is how the price universe froze for two months:

| mode | fetches | cost per ticker, MEASURED 2026-08-22/23 | right for |
|---|---|---|---|
| `skip_existing=True` *(default)* | only symbols **absent** from disk | ~0 s, and **refreshes nothing** | resuming an interrupted run |
| **`incremental=True`** | each CSV resumed from **its own last date**, then merged | **2.9-5.2 s** for a ticker ~40 sessions behind | ⭐ **a refresh** |
| neither | the whole history from `start_year` | **615 s** for all 4 tabs (price 200.5 / order_stats 157.7 / foreign 138.3 / prop 118.9) | the authoritative rebuild |

⚠️ **THE FULL REFETCH IS 38× DEARER THAN THE RESUME AND BUYS NOTHING WHEN NOTHING WAS
RESTATED** — measured on PNJ: 198.9 s against 5.2 s, and the resumed CSV reproduced the
full one **cell for cell**, all 4,344 rows × 12 columns. At the universe scale that is
the difference between **~67 h and well under an hour**.

#### ⚠️ Why the resume needs a RESTATEMENT CHECK, and what it would corrupt without one

`close_adjust` is **not a fact about a day; it is a fact about a day as seen from today.**
When a stock splits or pays a dividend, CafeF re-bases the **whole history**. So appending
fresh rows to stored ones splices two different bases into one series — a step change at
the join that **looks exactly like a real price move**, and that no freshness check can
see: the row count is right, the date range is right, the last date is today.

So `_scrape_tab` refetches an overlap (`INCREMENTAL_OVERLAP_DAYS = 45`, ~30 sessions)
*behind* the last stored date and **compares it cell-by-cell against what is stored**. Any
disagreement means the basis moved, and the ticker falls back to the full refetch. A
restatement is a **WARNING in `logs/app.log`**, never a silent repair.

⚠️ **THE LAST STORED DATE IS REFETCHED, OVERWRITTEN, AND NEVER JUDGED.** A row captured
intraday holds a partial volume that legitimately differs from its settled value; counting
that as a restatement would send every ticker down the slow path and quietly undo the whole
optimisation.

✅ **Verified 2026-08-23, four checks, on a throwaway folder:** *equivalence* — a truncated
CSV resumed incrementally reproduces the full scrape exactly (4,344 × 12, zero differing
cells); *cheapness* — 198.9 s → 5.2 s, **38.3×**; *restatement* — halving `close_adjust`
across all 4,304 stored rows (what a 2:1 split does) **is detected**, triggers the full
refetch, and repairs both the overlap and history far outside it (2015-01-08); *no false
positive* — an honest stale CSV resumes in 2.9 s without falling back.

⚠️ **`insider_txn` ACCEPTS `incremental` AND IGNORES IT, deliberately.** `_scrape_all`
queues one uniform task signature for all five tabs, but that tab is paginated by event
index with no date to resume from — and a row is **amended in place** upstream (a
registered transaction later gains its executed volume), so "rows after date X" is not a
well-defined increment. It is not part of the `P1` refresh for that reason.

⚠️ **`incremental` HELPS ONLY WHERE A CSV ALREADY EXISTS**, which is why `prop_trading` is
the slow tab in a universe run: **350 of 781 tickers have no prop-desk history at all**, so
those correctly take the full path every time.

- **Universe = all three VN exchanges** (`VN_EXCHANGES = HOSE, HNX, UPCOM`; ~777 unique
  tickers). `get_stock_symbols(exchanges=…)` reads the TradingView stock links and
  filters to those exchanges (default = all three); `scrape()` and every `scrape_all_*`
  take an `exchanges=` passthrough, so `scrape()` covers the full HOSE+HNX+UPCOM set and
  `scrape(exchanges=("HOSE",))` scopes to one. `skip_existing=True` means a full run only
  scrapes what each tab is still missing (price/foreign/order_stats/prop_trading done
  across the full 777-ticker universe; insider_txn still VN100-only → ~681 tickers
  remaining for that one). Note prop_trading only queried the full universe: 431 tickers
  have data and 350 have no prop-desk trades (nothing written for those — history ~2023).

### CafeF indices — `cafef_index_scraper.py` (the 6 market indices; requests)

The same four daily tabs as above, for the **market indices** rather than a stock.
`CafeFIndexScraper` **subclasses `CafeFScraper`** because an index is served by the
identical `.ashx` endpoints with the identical JSON keys — the windowing, pagination,
retry and all four row builders are inherited unchanged, and only the universe, the
unit scaling and the output folders are overridden. There is no index analogue of the
insider tab (-6), so it is not scraped.

- **The universe is a fixed 6-entry list** (`INDEXES`), not TradingView-derived — an
  index has no link CSV, so **this scraper can run before TV ever has**. The code is the
  URL slug of its history page, uppercased, which is what the rest of the repo calls it
  (cf. `MarketIndexConfig(index_code="VNINDEX")` in `main.py`).
- **Output:** `raw_data/cafef/index_{price,order_stats,foreign,prop_trading}/<EXCHANGE>_<INDEX>.csv`,
  **column-identical to the matching per-stock folder**, so the same reader handles both.
  A full run from scratch is **~18 min** for all 6 × 4 (2026-07-30, 16 workers).

| index | exchange | price rows | span | order_stats | foreign | prop |
|---|---|---|---|---|---|---|
| `VNINDEX` | HOSE | **6,328** | 2000-07-28 → today | 4,568 | 4,267 | 931 |
| `VN30INDEX` | HOSE | 3,595 | 2012-02-06 → | 3,553 | 3,032 | 86 |
| `VN100-INDEX` | HOSE | 2,273 | 2014-02-07 → **2025-04-29** | 3,088 | 2,522 | 1 |
| `HNX-INDEX` | HNX | 5,122 | 2005-07-14 → | 4,588 | 4,248 | 200 |
| `HNX30-INDEX` | HNX | 3,440 | 2012-07-09 → | 2,888 | 2,865 | 78 |
| `UPCOM-INDEX` | UPCOM | 4,204 | 2009-06-24 → | 4,178 | 3,613 | 198 |

VNINDEX starts on **HOSE's opening day** and its first close is the index base, 100.00.
Each tab starts at the later of the index's inception and that tab's own earliest data
(`TAB_START_FLOOR`: order_stats/foreign 2007, prop 2022), so no window is requested
where neither could hold anything.

- **⚠️ AN INDEX VALUE IS A POINT, NOT '000 VND — `_mul` is overridden to identity.**
  The per-stock builder multiplies OHLC and both closes by 1000 because CafeF quotes a
  share price in thousands of đồng. An index level is a pure number: ×1000 would store
  VNINDEX as 1,824,090 instead of 1824.09 — internally consistent, plots fine, wrong by
  10³. Neutralising `_mul` (rather than copying `_build_price_rows`) keeps the scaling
  decision in exactly one place per class and stops the two paths drifting.
- **⚠️ `VN100-INDEX`'s PRICE TAB STOPS AT 2025-04-29** while its order_stats and foreign
  run to today. CafeF stopped serving that one series; it is not a scrape failure and no
  symbol spelling recovers it (`vn100index` / `vn100` / `VN100` all return nothing).
  It also has **129 rows in 2015 with `open`/`high`/`low` = 0** and only a close.
- **⚠️ `prop_trading` IS EFFECTIVELY AN EXCHANGE-LEVEL SERIES.** VNINDEX, HNX-INDEX and
  UPCOM-INDEX carry history from late 2022; the three sub-indices (VN30/VN100/HNX30)
  hold only a 2026 trickle — VN100 exactly **one row**. A prop desk's trades are reported
  per exchange, not per index basket. All six are queried anyway, so the files are the
  honest record of what CafeF serves.
- **⚠️ `order_stats` IS PARTLY ZERO-FILLED BY CAFEF.** VN30INDEX/VN100-INDEX return rows
  whose every count is literally 0, and the HNX/UPCOM indices report `n_sell_orders`
  while leaving `sell_order_vol` at 0. A zero here is CafeF's and is indistinguishable
  from a real zero — check before treating these as a breadth signal.
- **⚠️ `foreign` HAS REAL HOLES IN THE OLDER YEARS, and they are CafeF's.** Whole 2-month
  windows return nothing while the price tab answers for the same index and the same
  dates — deterministically, on repeated attempts, at every page and at 1-month
  granularity. `foreign_room_left` / `foreign_own` are always 0 (an index has no foreign
  ownership limit); the columns are kept for layout parity.
- **The last row can be an UNFINISHED SESSION** — VNINDEX on the run date carried
  `open`/`high`/`value_matched` = 0 with a live close. Same behaviour as the per-stock
  price tab; drop the current day if a complete bar matters.
- Both closes are kept although CafeF derives no dividend adjustment for an index. On
  HOSE they are identical, but on **HNX/UPCOM `close_raw` carries full precision**
  (HNX-INDEX 235.1647) where `close_adjust` is rounded to 2dp (235.16) — so the columns
  are not redundant and `close_raw` is the better series for those three.

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

> ### ⚠️ THE ONE RULE THAT GOVERNS THIS WHOLE SECTION (2026-08-24)
>
> **A financial statement value comes from the filing PDF and from nothing else.** No HTML
> tab, no JSON endpoint, no web table, no transcription — not as a fallback, not "for the
> quarters OCR cannot read", not to close a gap. **A quarter no readable PDF can produce is
> `missing`, and `missing` is the correct answer.** CLAUDE.md §5 rule 24.
>
> ⚠️ **The code still defaults `use_api=True` and 34 rows on disk came from the web tabs
> (`FIN-1`).** Everything below about `from_api` — the Q4 weakness, the eight confirmed wrong
> values, the literal `-1` sentinel, the hollow cash flows — is now the EVIDENCE for the rule
> rather than the tuning notes for a fallback. Keep it; it explains why the rule exists.

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

- **⚠️ THE 12 SCHEMA CSVs ARE A GIT-TRACKED INPUT WITH NO PRODUCER IN THE PIPELINE — and a
  missing one used to cost 2.4 h of OCR and report itself as something else.** Nothing in
  `src/` calls `cafef_schema.save()`; the files are committed (`raw_data/cafef/financials/`
  is the single exception in `.gitignore`), so "absent" always means deleted or moved, never
  "a run has not produced it yet". `FinancialsBuilder.schema_of` read them behind an
  `if os.path.exists(path)` and returned an **empty list** when absent — an empty chart of
  accounts, against which nothing matches — so `map_to_schema` mapped no line, `reconcile`
  found no subtotal, and **every statement of every quarter was rejected**, after the full
  parse, looking exactly like a hard OCR problem. The same guard in `_from_api` silently
  dropped that report's CafeF-tab fallback, turning recoverable quarters into permanent
  `source='missing'` rows.
  - Both now go through `utils.inputs.require_file`, which raises naming the file, the
    consequence and the fix.
  - **`FinancialsBuilder.preflight(exchange, symbol)` checks every input up front** — the
    template, the three charts of accounts, the PDF index and the archive — and `build()`
    calls it as its first statement, so `main.py`, a notebook and the orchestrator are all
    covered. A ~2.4 h parse must never start on inputs that can be validated in milliseconds.

- **⚠️ `build_templates_index` also reads `raw_data/simplize/industry.csv`** — an OPTIONAL
  input, and the only place the dependency is written down. The template itself is
  fingerprinted over the network, so absence stops nothing; it silently BLANKS the
  `sector` / `industry_group_code` / `industry_group_slug` columns, which are the only
  reason a GICS-vs-fingerprint disagreement (HVA: securities group, corporate template) is
  visible in the data. A blank column reads like "no disagreement", so it now goes through
  `utils.inputs.optional_file`, which logs a WARNING naming exactly what was blanked.

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
  - **⚠️ `months` RECORDS THE SPAN A FLOW ROW COVERS** — 3 for a standalone quarter, 6/9/12
    for a year-to-date figure that could not be split, blank for a balance sheet (a STOCK at
    a date, so a span would be a category error) and for a `missing` row. Added 2026-08-30,
    and it is the same move `consolidated` made: **two different things in one column with
    nothing saying which is which**. Two consequences worth knowing before reading any flow
    column:
    - **the cash flow has ALWAYS been cumulative from 1 January**, so a Q3 row is nine
      months. That was true before the column and unsaid; `statement_months` only writes it
      down. ⚠️ **Read it before summing or diffing two rows of one column.**
    - **it is what lets `_decumulate` KEEP a row it used to drop.** A cumulative Q2/Q4
      income statement needs Q1..Q(q-1) subtracted from it; where those quarters were never
      FILED — BSR before H2-2018, BID before 2012, VCB Q4-2008, **9 quarters measured** —
      nothing will ever subtract them, so dropping is permanent loss rather than a deferral.
      Where they WERE filed the drop stands, because a full `build()` can still do it
      properly. ⚠️ **The set of filed periods must be the TICKER'S, never the RUN'S**, or
      every subset run reads its own narrowing as "never filed"; `build()` captures it before
      the `periods` filter and `pdf_ocr_merge._unfiled_priors` reads the PDF index at
      `allow_parent=True`, the widest set there is.
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

### ⚠️ THE COLUMN SET IS TWO DEFECTS DEEP ON A CORPORATE FILING — `MSO-1` and `SPL-1` (2026-08-29)

Both found in one quarter, VIC Q3-2014, and **both write wrong figures rather than refusing.**
`value_columns` is the whole story: it clusters right edges in the right 60 % of the page, keeps
a cluster holding ≥35 % of the biggest, and drops a "Thuyết minh" note column **by median digit
count** — *"a period column's figures are 4-9 digits, a note reference 1-2"*.

- ⚠️ **`MSO-1` — the VAS `Mã số` column is 3 digits and sits in the overlap that rule cannot
  cover.** Form B01-DN prints `Chỉ tiêu | Mã số | Thuyết minh | Số cuối kỳ | Số đầu năm`, and the
  item codes are 100, 110, 270, 300, 440, sometimes 4-digit (3131, 3161). On VIC Q3-2014 that
  column clusters at **x=279.7 of a 595pt page — 47 %, inside the value zone** — 86 numbers, and
  it became column 0, so `_first_value` returned every row's ITEM CODE as its figure: assets 270
  against resources 440, refused *"assets != liabilities + equity"* on all 45 layers.
  ⚠️ **A balance sheet is the only statement whose gates catch that**: an income statement only
  has to present a PBT line and `50` is one, a cash flow only a closing balance and `70` is one,
  and `sane` fails open on a ticker with no accepted history — which is every non-bank ticker on
  its first run. VIC **Q1-2011** is on disk with `a_tai_san_ngan_han = 100` for exactly that
  reason. ✅ **Fixed by `_code_column`, which reads the HEADING** — the same thing this file
  already does for the quarter column instead of counting columns. Three conditions, each failing
  SAFE: a whole word box normalising to *maso*, a detected column under it, and that column being
  the **leftmost** (the form's layout, so a mis-read heading cannot take a figure column).
  ✅ Blast radius measured from cached word boxes: **19 of 22 statements re-detect the identical
  column set**, all 12 bank statements among them; the 3 that move are VIC balance sheets, and
  every number in each dropped column is item numbering.

- ⚠️ **`SPL-1` — one printed figure comes back as TWO detector boxes, and both halves are
  plausible.** `'5.209.108'` ending at x=405.7 and `'954.978'` starting at x=409.5 is one printed
  5.209.108.954.978, **3.8pt apart**. The left half lands on no column and is dropped, so the row
  keeps `954.978`; where enough left halves line up they instead form a **spurious column**
  (x=498.2, n=34) kept as a period of its own. **60 figures of that balance sheet and 27 of the
  income statement**, while BOTH GRAND TOTALS survive whole — so `reconcile` passes and `sane`
  probes a correct total. `SLD-1`'s shape a fourth time.
  ⚠️ **It is resolution-dependent**: the identical document at **onnx@300 splits NOTHING**, two
  clean columns, identical totals — so accepting at layer 1 is `PGB-1`'s *"a half-right layer that
  passes the gates ends the cascade"* for the third time. ✅ **Fixed by REFUSING, not repairing**:
  `split_figures` counts the pairs from geometry and text alone — deliberately **not** keyed on
  the detected columns, since the fragments create their own — and `reconcile` escalates the
  cascade. ⚠️ **The 4.5pt gap is measured, not chosen**: 0 hits across 12 statements of VCB
  Q1-2021, VCB Q1-2026, ACB Q1-2024 and BID Q4-2016, all of which parse today; at 6pt it starts
  picking up one or two per bank statement.
  ✅ **AND THE REPAIR SHIPS BESIDE THE REFUSAL** — `_merge_split_figures`, at the OCR seam,
  re-joining a pair when the gap is under 4.5pt, the right box begins with a FULL three-digit
  group and the join is a well-formed figure, confined to the value zone. Held back for an hour
  as too wide a change on four bank filings; the base was widened to **19 filings / 53
  statements** and the argument did not survive it — **47 untouched**, the 6 that move all VIC,
  and across **21 bank statements it fires once and changes no mapped cell**. ✅ Scored against
  onnx@300 (which splits nothing) on VIC Q3-2014: agreement rises 29 → **43** of 45 balance-sheet
  cells, **17 repaired and 0 broken** across the three statements. ⚠️ It does NOT retire the gate:
  6 unmergeable three-way splits remain at 200 dpi, which is why that quarter still escalates.
  ⚠️ **MERGE FIRST, THEN SPLIT** — `_split_number_runs` apportions by character offset, leaving a
  gap of `width / len(text)`: 5.7pt on the measured box, **4.3pt at a 100pt box**, inside
  `MERGE_MAX_GAP`, where a merge running afterwards would undo the splitter.

**Result**: VIC Q3-2014 parses in **3m 42s** (against 23 min refusing), balance sheet at
`onnx@300` with 45 items, and the income statement and cash flow already on disk **REPRODUCE**
16/16 and 19/19 cells at their own layers. Five internal identities close to the đồng, none of
them the one `reconcile` tested. `test_cafef_code_column.py` (8) and `test_cafef_split_figures.py`
(7) pin both without a PDF, a network or an engine.

### ⚠️ ONE QUARTER, FOUR DEFECTS — `ISL-1`, `TCG-1`, `MEN-1`, `DEC-1` (TCB Q3-2013, 2026-08-29)

All four were found by asking one question — *why is TCB Q3-2013 `missing`?* — and three of them
put a wrong figure on disk rather than refusing. The recorded reason for that quarter was
*"the filing declares no unit anywhere the parser can find"*; **the filing declares it on page 2,
and page 2 was thrown away.**

- ⚠️ **`ISL-1` — `_drop_islands` measured the gap in a ±1 WINDOW, not along the statement's own
  run.** A form code has to survive OCR to anchor a statement, and on a 2013 scan usually only
  one page keeps one: this balance sheet runs pages 2-4, all three classified `balance_sheet` by
  title, and only page 4 kept `B02a/TCTD-HN` (page 2 OCRs as `B020/TCID-HN`, page 3 as
  `B022/TCTD-HN`). Page 2 measured two pages from that single anchor and was dropped —
  **while page 3, sitting between them, was kept**, which is not what a gap looks like. Page 2 is
  the statement's FIRST page and the only place in the whole filing that prints
  `Đơn vị tính: triệu đồng`: the income statement prints no unit line at all and the cash flow's
  two pages do not repeat one, so `document_unit` had nothing to offer either and all three
  statements were read in đồng. ✅ The walk now expands through pages the same report already
  owns before the ±1 tolerance applies. VCB Q2-2023 — the case the pruner exists for — is still
  pruned, because page 8 there belongs to no report and the walk stops.

- ⚠️ **`TCG-1` — "TỔNG CỘNG" is "TỔNG", and one syllable cost two cells.** "TỔNG CỘNG TÀI SẢN CÓ"
  scores **0.769** against the chart's "TỔNG TÀI SẢN" (no containment: the syllable is in the
  middle), so **total assets did not map**; and "TỔNG CỘNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" scored
  0.929 for its own anchor against a flat **0.95** awarded to EQUITY by containment, so equity
  took the grand total — 165,878,786 mn against a real 13,857,834. ✅ `ABBREV` normalises
  `tongcong` → `tong` on both sides; the row then scores **1.000** for its own account and the
  anchors settle it with no new threshold. **0 new account collisions** across all 12 charts.

- ⚠️ **`MEN-1` — an anchor may not take a row where its account is only MENTIONED.** With the
  grand total gone to its own anchor, equity fell to the row where `table_rows` had glued the
  section header "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU" onto "II. Tiền gửi và vay các TCTD khác", and
  read 24,686,177 mn of interbank deposits — with `_claim` releasing the deposits line the walk
  had placed correctly. ✅ In `header + line` the header is a PREFIX and the item a SUFFIX, so an
  account that is neither is a mention; containment no longer awards the flat score there.
  ⚠️ **ANCHORS ONLY**: gating the ordered walk the same way changed **23 of 228** archived
  statements and lost sound cells, against **4** confined to `_anchor`, all repairs.

- ⚠️ **`DEC-1` — `1,630,428.99` was read as 163,042,899.** TCB's 2012 filings print the
  international convention (comma thousands, dot decimal) and `parse_num` stripped both. The
  error is uniform, so the statement reconciles perfectly against itself: **TCB Q2-2012's PBT is
  on disk as 163,042,899 mn** — 163 tn for a bank with 180 tn of assets — and Q1-2013's cash flow
  closes at 2,144 tn. ⚠️ **AND ONE SUCH ROW REFUSED A CORRECT QUARTER**: `sane` bands on the
  MEDIAN of the quarters already accepted, and Q3-2013's own income statement (97,315 mn, right)
  came out 23× under a median one 100× row had dragged up. ✅ A separator followed by ONE OR TWO
  digits at the end of a token is a decimal point in either convention — a thousands group is
  always three digits — and the character itself cannot be trusted, because OCR confuses `.` and
  `,` ("1,234.567" must stay 1,234,567).

✅ **THE QUARTER IS ON DISK, 2026-08-30** — `--symbol TCB --quarters 2013-Q3 --merge`, 39.1 min,
and every layer it uses is one of the fixes: balance sheet at **`onnx@200`** (with page 2 back,
the filing declares its own unit and nothing has to be repaired), income statement at
**`onnx@300+unit+tail`**, cash flow at **`onnx@300+unit`**. Seven independent checks pass,
three of them arithmetic the parser never tested: `assets == resources` = 165,878,786 mn, the
cash identity `22,621,969 + 2,989,205 = 25,611,174` to the đồng, and the comparative column
reproducing Q4-2012's stored total assets (179,933,598 mn). PBT is the QUARTER column
(97,315 mn), not the 9M cumulative (749,886). ⚠️ `viii_von_chu_so_huu` is deliberately absent:
the filing prints "Vốn và các quỹ", which this chart cannot name.

⚠️ **AND THE BAND HAD TO BE REPAIRED FIRST — fixing `parse_num` does not move a figure already
written.** `sane` bands on the median of the accepted quarters, and one of TCB's seven was the
163 tn row: `[0.01, 0.397, 1.018, 2.253, 2.744, 4.221, 163.043]` gives a floor of 0.113 tn
against a probe of 0.097. Q2-2012 and Q1-2013 were re-parsed (88.8 min) and merged first —
**3 periods changed, every figure ÷100, and Q1-2013's income statement REPRODUCED and was
skipped** — after which the median is 1.630 tn, the floor 0.082, and the quarter passes.

⚠️ **AND THE UNIT BLOCK WAS 200 dpi ONLY, WHICH IS NOT WHERE THIS FILING IS READABLE.** Its
income statement returns **7 split figures** at 200 dpi (`SPL-1`, refused) and none at 300; its
cash flow reads the net movement as **205** at 200 dpi where the page prints 2,989,205 — the
crop defect, invisible to both gates because the two balances are right and the probe is the
closing one. Two layers were added (`onnx@300+unit+tail`, `onnx@300+unit`) and
`unit_from_document` now demands the cash identity the way `relax_totals` does: **a layer that
multiplies every figure by a million may not also be the layer that skips the arithmetic**
(§6-2-tervicies drew the same conclusion for `annual_tail`). The 200-dpi reading then fails
`22,621,969 + 205 != 25,611,174` and the cascade escalates.

### ⚠️ `PAR-1` — A NEGATIVE FIGURE CUT IN HALF, AND THE POSITIVE HALF WRITTEN (BID Q4-2016)

Found by the regression for the four defects above, on the one filing in it that had not been
re-parsed since `SPL-1` shipped. The detector boxes text LINES, and on some rows a thousands
separator comes back as a SPACE **inside a parenthesised figure**: `'(1.029 827)'` for a printed
(1.029.827). `_split_number_runs` cut it on the space and the row kept the RIGHT half as a
POSITIVE number — **BID Q4-2016 is on disk with `hddt_mua_sam_tai_san_co_dinh` = 616 mn for a
printed (2.298.616) and dividends paid of 383 mn for a printed (2.940.383)**.

⚠️ Neither existing repair reaches it: `_merge_split_figures` needs TWO boxes and there is one,
and `_join_split_number` wants a bare 1-3 digit head where this has "(1.029". ⚠️ **And since
`SPL-1` shipped it costs the whole statement**: the splitter's own pieces sit
`box_width / len(text)` = **4.1pt** apart, under `SPLIT_MAX_GAP`, so `split_figures` counts them
and `reconcile` refuses the reading as fragmented — this is the case the *"MERGE FIRST, THEN
SPLIT"* comment predicted, arriving through the guard instead of through the merge.

✅ **A box the parentheses SPAN is one figure** — two figures boxed together each close their
own, so `'(135.272.610) (126.501.216)'` is still split — and the pieces are re-joined before the
split, **in the default path**, because the alternative is not a wrong figure but a lost
statement. ⚠️ **Proven pre-existing, not caused by the four fixes**: the identical three pairs
appear at the same gaps on stashed HEAD.

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

## 3b. Running that parse somewhere else — `pdf_ocr_job.py` / `RUN__pdf_ocr.ipynb`

Added 2026-08-28. §3a's cascade is expensive and its expensive half is OCR: DeepDoc DB
detection under onnxruntime and VietOCR recognition under torch, both of which want a GPU. This
machine has **4 GiB**; a Kaggle T4 has **15**. So the machine became a parameter, and nothing
else did.

```powershell
# the notebook is the entry point: ONE parameter cell, ENVIRONMENT = LOCAL | KAGGLE
jupyter lab src\kaggle_gpu\RUN__pdf_ocr_control.ipynb

# the same thing without a notebook
cd src ; python -m web_scraper.pdf_ocr_job --symbol VCB --quarters 2026-Q1 --merge
cd src\kaggle_gpu ; python -m kgpu rehearse pdf-ocr ; python -m kgpu run pdf-ocr
```

| function | what it is |
|---|---|
| `use_data_root()` / `use_models()` | re-point `cafef_financials`' path globals and the two model env vars. ⚠️ The globals are read at CALL time precisely so a harness can do this — `statement_path`'s docstring has said so since the experiment harnesses needed it |
| `plan()` | the filings, from `FinancialsBuilder.documents()`, filtered by `periods` and/or `quarters`. ⚠️ **The choice is never re-implemented**: it carries a measured guard against an annual report changing the ENTITY of a Q4 row |
| `partition_by_disk()` | splits those into "a re-parse could still win something" and "already `pdf` in all three statements". Applied by `prepare()` unless `overwrite` |
| `seed_history()` | the magnitude band `sane` compares against, rebuilt from the `pdf` rows on disk |
| `run_document()` | ONE filing through **`FinancialsBuilder._parse_cascaded`** — the real 47-layer cascade, its parse cache, its `reconcile`-then-`sane` short-circuit and its refusal report |
| `compare()` | every parsed cell against the statement CSV on disk |
| `engine_report()` | which device each HALF of the OCR actually ran on — see `ORT-1` below |

⚠️ **IT WRITES NO STATEMENT CSV BY DEFAULT, AND THAT IS STILL THE POINT.** The output is a
run folder (`reports/pdf_ocr/<run_id>/`: `metadata.json`, `summary.csv`, one JSON per filing
written BEFORE the next one starts). CLAUDE.md records **four** builds in which a `periods` run
silently DOWNGRADED a quarter it was given only for history, while the log said `RUN_SUCCESS`
— a run whose output is an artefact cannot do that.

⚠️ **`merge_into_csv=True` IS THE OPT-IN THAT GIVES THE OTHER HALF OF THAT TRADE**, added
2026-08-29 by request, and it does not weaken the argument above because it goes through
`pdf_ocr_merge` and its three refusals. See "`OVERWRITE`, and the per-quarter upsert" below.

### `quarters` — the batch filter, `YYYY-QQ` (was `years` until 2026-08-29)

`plan()`, `JobSpec`, the CLI (`--quarters 2014-Q3 2014-Q4`), `kgpu`'s
`data.documents.quarters`, both notebooks' `QUARTERS` and the payload/parameter cross-check all
carry it. **Empty or absent means every quarter the ticker files**, and that answer is
`documents()`'s own rather than a list any of them computes — the default is the ABSENCE of a
filter, not a re-derivation of one.

| | |
|---|---|
| the unit | ⚠️ **a QUARTER since 2026-08-29; it was a YEAR before.** The year came from the unit the statement BUILD skips in (`orchestration` §2a — `_decumulate` needs Q1..Q(q-1) of the same year, so a partial skip deletes the very quarter a run exists to fix). ⚠️ **That argument never bound this module**: nothing here de-cumulates and nothing here writes, so the wider unit only ever bought extra OCR. The hazard still exists where the WRITE is, and `pdf_ocr_merge` already refuses a cumulative income statement a one-document run cannot de-cumulate |
| the form | ⚠️ **`YYYY-QQ` in one of TWO spellings — `2014-Q3` and the zero-padded `2014-03`** — folded onto the first by `normalise_quarter` at the edge, because `cfg.name`, the payload directory and the Kaggle kernel slug are all derived from this list and two spellings reaching those would be two runs racing for one slug. ⚠️ **`2014-3` is refused although it is unambiguous**: one digit is a keystroke from a MONTH. ⚠️ **And the repo-native `QQ-YYYY` is REFUSED rather than accepted.** They name the same quarter and only one of them SORTS. A lenient parser would be easy — and then a caller who used the wrong form would never find out, while a caller who made a TYPO gets "files no document for [...]" and goes looking at CafeF for a filing that is sitting there. `as_quarter()` converts; the form is checked BEFORE the corpus is |
| the quarter of a document | `_quarter_of` reads it from the **PERIOD**, never from the index's `year` column. `documents()` folds a quarter-5 annual onto that year's Q4 and rewrites `period`, so `period` is the normalised key everything else compares on — and CafeF files 10 of 84,076 documents with a `Year` of `0`, `202` or `203` |
| with `periods` | they **INTERSECT**. `quarters=["2014-Q3"]` + `periods=["Q3-2014"]` is Q3-2014, and each filter is checked against what survives the one before it, so the error names the filter that emptied the plan |
| matching nothing | **raises**, exactly as `periods` already did — a filter that matches nothing is a run that parses nothing and reports success |

⚠️ **`kgpu` VALIDATES THE TWO COPIES AGAINST EACH OTHER.** `data.documents` decides which
filings are UPLOADED and `parameters` decides which the worker OPENS; a job naming different
quarters in the two ships one set and parses another, and the worker reports the shortfall as
`missing` — which is what a genuinely unreadable filing reports too. `config._validate` refuses
the mismatch, and imports `QUARTER_RE` from `plan`'s own module rather than re-writing the form.
Free here; the run that discovers it costs a Kaggle round trip.

⚠️ **IT EARNED ITS KEEP TWICE ON ITS FIRST USE.** Asked for VIC's 17 quarters that still hold a
`missing` cell, `plan` **refused 2008-Q3** — VIC files no document for it at all, so its three
`missing` cells are the correct answer, where a year filter would silently have opened Q2 and Q4
of 2008 instead. And the first rehearsal of the first `quarters` job exposed **`RHS-1`**:
`rehearse._rehearse_documents` passed only `periods_requested` to `plan`, never the batch filter,
so it compared a filtered shipment against an UNFILTERED `documents()` and tripped its own
assertion. ⚠️ **`years` had the identical defect for as long as it existed** and nothing ever
exercised it, because every documents job rehearsed until then was narrowed by `periods` alone.

### ⚠️ `OVERWRITE`, and the per-quarter upsert (2026-08-29)

Two knobs on `JobSpec`, one question each, and the notebook exposes both in the same cell.

**`overwrite`** decides what happens to a quarter that is already on disk, in BOTH places it
can be decided — before the OCR and at the write:

| | `overwrite=False` (the default) | `overwrite=True` |
|---|---|---|
| a quarter already `pdf` in all three statements | **dropped by `partition_by_disk` before any OCR**, and — on Kaggle — before it is uploaded | re-parsed |
| a figure that DIFFERS from a good `pdf` row | refused by `pdf_ocr_merge` | written (`force_differs`) |

⚠️ **THE SKIP IS AT QUARTER GRAIN HERE AND AT YEAR GRAIN IN `build()`, AND THE DIFFERENCE IS
MEASURED RATHER THAN STYLISTIC.** `_skippable_years` keeps a YEAR whole because `_decumulate`
turns a cumulative income statement into a standalone quarter using Q1..Q(q-1) of THAT run — so
dropping Q1..Q3 while keeping Q4 would delete the very quarter the run exists to fix. **Nothing
in this module de-cumulates**: `run_document` writes what the cascade accepted, `pdf_ocr_merge`
refuses a cumulative income statement outright, and both `seed_history` and `open_reference`
read from DISK. No quarter here depends on another quarter of the same run, so the finer grain
is safe — and at 4-18 min a document, a year held whole for one missing cash flow costs three
filings that had nothing left to win.

⚠️ **A QUARTER IS "COMPLETE" ONLY WHEN ALL THREE STATEMENTS READ `pdf`.** One filing produces
all three, so a quarter missing its cash flow re-opens the document, and the two statements that
come back with it are judged on their own merits — identical is SKIPPED, different is REFUSED
unless overwrite was asked for. ⚠️ A `cafef` row is not evidence a quarter is done either: §5
rule 24 makes it exactly the row a re-parse exists to replace.

**`merge_into_csv`** upserts each quarter into the statement CSVs **as it finishes**, before the
next document is opened. That is the whole point of doing it per quarter:
`FinancialsBuilder._write` renders to a `.tmp` and `os.replace`s it, and only the quarters a
merge PRODUCED are rewritten — so **a 12-hour run stopped at hour 6 keeps every quarter that
finished and can lose at most the one in flight.** The backup is taken ONCE, by the first
quarter that actually writes something (`merge_run(backup=False)` exists for this one caller);
seventy timestamped copies of three CSVs answer *"what did this run change?"* worse than one.

⚠️ **A WORKER MAY NOT DO IT, AND `run()` REFUSES RATHER THAN TRUSTING THE CALLER.** On Kaggle
`CAFEF_DATA_ROOT` is an unpacked payload that dies with the kernel, so an upsert there would
edit a copy and report success; `run()` compares the resolved data root against the repo's own
and turns the flag off with a line in the log. The write belongs to whoever holds the real
`raw_data/` — `kgpu pull`, which merges after the folder comes home.

⚠️ **`OVERWRITE` REACHES THE MERGE, NOT ONLY THE PARSE.** Without that, a re-parse asked for
explicitly would come home and be refused by `force_differs` — the run would do the work and
disk would keep the old figure, which is the worst of both answers. `kgpu`'s `merge_statements`
and `merge_latest` both read it off `cfg.parameters["OVERWRITE"]`, and `config._validate`
refuses a job whose `data.documents.overwrite` disagrees with it, the same way it already
refuses a quarters mismatch: one decides what is UPLOADED, the other what is OPENED.

### ⚠️ WHICH TOOL STARTS A TICKER — `pdf_ocr_job` does NOT (`BND-1`, measured 2026-08-29)

`seed_history` rebuilds `sane`'s magnitude band from the `pdf` rows **on disk** and `run()`
re-seeds it **per document**; `build()` does the opposite, appending to `history` after every
quarter it accepts. So on a ticker with no statement CSV the band is EMPTY for every document and
**`sane` fails open for the whole run** — and `pdf_ocr_merge` then refuses every empty-band
statement, so nothing is written and the band stays empty. The loop closes on itself.

**TCB paid 5h 21m to demonstrate it.** 59 filings, **169 of 177 cells parsed = 95.5 %** — and two
screens over the finished artefact, doing by hand what the band would have done, convicted
**9 of the 169**:

| screen | found |
|---|---|
| a statement whose `unit` is the MINORITY for its report | **8 statements at `unit=1`** against the ticker norm of 1,000,000 — TCB Q1-2014 PBT read 673,136 for a company that earned **673 tỷ** |
| total assets, quarter on quarter | **Q1-2013 = 17,586,290,323 tr** against ~178,000 tr either side, the equity line holding the same figure |

✅ **Five of the nine were repaired in 8m 58s** by restricting the cascade to the three layers
carrying `unit_from_document` — each came back at **exactly ×10⁶**, asserted as a RATIO because a
genuine re-read would not divide cleanly (`UNT-1`). The other four are `missing` on disk and that
is the right answer.

⚠️ **So: `pdf_ocr_job` REPAIRS a quarter on a ticker that already has history; a NEW ticker's
authoritative path is a full Dagster `raw/cafef_financials` run.** A 95.5 % headline concealed a
5.3 % wrong-figure rate, and every one of the nine was the shape `sane` exists to catch. TODO
`P47` ships the two screens as code, which is worth more than the warning — they also run where
`sane` is on.

### `pdf_ocr_merge` — the write the job refuses, made explicit (2026-08-29)

`pdf_ocr_job` still writes no statement CSV. What was added is a SECOND module that takes a
finished run folder and upserts it, so the two decisions stay separate: parsing is one act,
putting a figure into `raw_data/` is another.

```powershell
cd src\kaggle_gpu
python -m kgpu merge <job>              # WRITES, after a backup
python -m kgpu merge <job> --dry-run    # every decision printed, nothing touched
```

⚠️ **WRITING IS THE DEFAULT since 2026-08-29, by request** — in the library (`merge_run`), on
the job (`merge_statements`), in the notebook (`MERGE_INTO_CSV`) and in the CLI. What keeps it
honest is the three refusals and the pre-merge backup, not a second command.

⚠️ **THE MERGE RUNS HERE AND COULD NOT RUN ANYWHERE ELSE.** A Kaggle kernel writes
`/kaggle/working` and exits; the statement CSVs are on this disk. "The Kaggle run upserts the
CSV" is necessarily "the pull does" — which is what makes a pre-merge backup and a printed diff
possible at all.

⚠️ **IT DOES NOT WRITE THE CSV ITSELF** — it calls `FinancialsBuilder._write(merge=True)`, the
same upsert `build()` uses, so only the quarters this run PRODUCED are rewritten and every
other row keeps what the file holds. A second CSV writer would be a second place for the column
contract to be wrong.

**Four refusals, each from a measurement, each with a `force_*` escape:**

| refused | the measurement |
|---|---|
| a **cumulative income statement whose priors WERE filed** | an annual filing prints the year to date; the column holds the quarter, and a one-document run has no Q1..Q(q-1) to de-cumulate with. ⚠️ **Where those priors were NEVER FILED it is written instead**, carrying `months = 6`/`12` — nothing can ever split it, so refusing would be permanent loss rather than a deferral (BSR Q4-2016) |
| an **empty `sane` band** | with no band the magnitude guard fails open — the documented way a subset run writes a wrong figure (§6-2-octodecies) |
| a figure that **DIFFERS from a good `pdf` row** | `compare()` already scored it; two runs disagreeing is not resolved by preferring the newer |
| a document whose parse **RAISED** (`VCR-1`, 2026-08-29) | a refusal measures the FILING, an exception measures the MACHINE — `vocr.vn`'s certificate expired, every `onnx@*` layer raised, and `tesseract@200` rewrote 13 columns of a filing that had reproduced 98 of 98 cells, both gates passing |

⚠️ **AND THE SECOND REFUSAL HAS A FIELD CASE FROM THE DAY IT SHIPPED.** VIC Q3-2014,
2026-08-29: the Kaggle worker ACCEPTED an income statement at `onnx@300` that the full local run
had REFUSED with `sane: probe exactly equals an already-accepted quarter`. **Nothing about the
machine differed** — the cash flow reproduced bit for bit at the same layer and the balance
sheet was refused for the same reason on both. What differed is the population the gate compares
against: `seed_history` rebuilds the band from the `pdf` rows on DISK (12 income-statement
probes for VIC) while a full run accumulates it IN THE RUN, over more quarters and over
pre-de-cumulation figures. **A statement a worker accepts is not a statement a full run would
accept**, and no code here can tell the difference — which is why every decision and every
changed cell is printed rather than summarised, and why a backup is taken first.

**19 tests**, no PDF, no network, no OCR engine.

⚠️ **`seed_history` IS A RECONSTRUCTION OF A FULL RUN'S BAND, NOT THE RUN'S OWN.** It applies
three of `build()`'s rules — `source == 'pdf'` only, `MIN_ITEMS_FOR_HISTORY` withheld, split
per ENTITY — and restricts to periods BEFORE the target, because a full run judges a quarter
against what it has already accepted and never against its own future. It holds what DISK
records, so it diverges the moment disk is not a full run's output. ⚠️ An EMPTY band is what
makes `sane` fail open, which is how a subset run writes a wrong figure, so `kgpu rehearse`
warns on one.

⚠️ **Three things `build()` does that this does not**: the alternate-filing retry,
de-cumulation, and the CafeF-tab fallback (which §5 rule 24 forbids anyway). The first two need
state a one-document run has not got — so `compare()` **refuses** to score a cumulative income
statement against a de-cumulated row rather than reporting every cell as changed.

### ⚠️ `ORT-1` — a green GPU run can be half on the CPU

Measured on the first Kaggle run, 2026-08-28. `pip install onnxruntime-gpu` resolves to
**1.29.0**, which needs **cuDNN 9 with CUDA 13**; Kaggle's image is **CUDA 12.8**.
`ort.get_available_providers()` still listed `CUDAExecutionProvider`, `InferenceSession` then
failed to create it, and **detection ran on the worker's CPU while VietOCR ran on the T4** —
correct output, **21 % slower**, one warning inside a wall of ANSI-coloured onnxruntime noise.

⚠️ **`get_available_providers()` is an ADVERTISEMENT; `session.get_providers()` is the
MEASUREMENT**, and `_DbTextDetector` had been choosing from the advertisement since it was
written. `onnx_ocr` now calls `ort.preload_dlls()` where it exists (1.21+ stopped adding the
`nvidia-*` wheels to the loader path itself), `engine_report()` records the session's real
providers into every `metadata.json`, and the notebook pins the LINE `onnxruntime-gpu>=1.19,
<1.23` — ⚠️ a `==1.20.1` pin was tried first and **failed the install**, because this repo's
own version is not published for Kaggle's cp312 Linux.

### What VCB Q1-2026 measured

The test case, chosen because all three of its statements already read `pdf` at `onnx@200` on
this machine — so the run has an exact baseline. **98 of 98 cells identical on every run**,
same layer, same unit, same `publish_date`:

| | card | detection | parse |
|---|---|---|---|
| local | RTX 3050 | CUDA | 100.6 s / 113.3 s (two runs) |
| Kaggle, `onnxruntime-gpu` 1.29 | Tesla T4 | ⚠️ CPU | 83.8 s |
| **Kaggle, 1.22** | **Tesla T4** | **CUDA** | **69.0 s** |

⚠️ The local spread is **12.7 s (12 %)** across two runs of the same file, so ~1.5× is one
measurement each, not a benchmark.

### And BID Q4-2016 — the HARD document, which inverts the speedup

The hardest filing on disk: FY-2016 audited consolidated annual, 5.0 MB, cash flow at
**layer 45 of 47** (`onnx@200+pad6+annual+extra`). `_parse_cascaded` breaks only when all three
statements are accepted, so one unresolvable statement makes the whole document pay the cascade.

| | local RTX 3050 | Tesla T4 | |
|---|---|---|---|
| parse | **32.9 min** | **26.4 min** | **1.24×** — against 1.55× on the easy document |
| balance sheet | `onnx@200`, 52 items | same | REPRODUCED |
| cash flow | `onnx@200+pad6+annual+extra`, 24 items | **the same layer 45** | REPRODUCED |
| income statement | — | — | ⏭ refused: the filing is cumulative, the row on disk de-cumulated |

✅ **Reproducing the LAYER is the stronger claim**: the document must lose 44 layers and win on
the 45th, so `reconcile`, `sane`, the parse cache and the escalation order all behaved
identically across two torch majors.

### ⚠️ BOTH SPEEDUPS ARE WITHDRAWN — the local clock moves 2.25× on its own

Four runs of VCB Q1-2026 on this machine, same document, same code: **100.6 s, 113.3 s** (01:04
and 01:26) and **50.8 s, 50.3 s** (06:46 and 06:48). Two tight clusters five hours apart, and
the T4's 69.0 s sits BETWEEN them. Nothing recorded distinguishes the clusters — same torch,
same onnxruntime, same providers, same `vram_free_mb`, same 12 of 53 pages read.

**So no cross-machine timing number here is established**, including the 1.24× on the hard
document. **To compare two machines, INTERLEAVE the runs.** What survives is the correctness,
which is deterministic and was reproduced four times.

### The three contracts — input, log, output

| | |
|---|---|
| **input** | ONE frozen `JobSpec`, built identically by the CLI, the notebook and `kgpu`. `prepare()` resolves root / models / TEMPLATE / documents and RAISES on any of them, before a page is rendered |
| **log** | ⚠️ **ONE LINE, FOUR SEGMENTS, LEADING WITH THE OVERALL % (2026-08-30):** ` 33.7% - doc 2/3 HOSE_TCB Q3-2013 - layer 12/47 onnx@300 - page 40/96  ~76 s left`. Formatted by `utils.progress`, which the Kaggle CONTROL side uses for its own six steps, so the two cannot drift. The three denominators are still named, in the segments (`doc 2/3`, `layer 12/47`, `page 40/96` — only the last predicts time), and the overall % is `documents finished + this document's place in the cascade`: **a position in the plan, a LOWER BOUND on real progress, never a fraction of the time**. Rate-limited to a 10-point step or 15 s |
| **output** | `metadata.json` (`schema_version`, resolved spec, `template_how`, `environment.ocr`), `summary.csv`, `documents/<key>.json`, and `run.log` written line-buffered as it goes |

⚠️ **ANYTHING THAT PARSED `run.log` MUST BE RE-READ.** A filter anchored at the start of
a line (`startswith("WRITE ")`) matches nothing now — the line starts with the
percentage. `progress.detail_of(line)` is the segment that used to BE the line, and the
control notebook's own "what was refused" cell was the first thing this broke.

⚠️ **`resolve_template` REPLACED A SILENT `or "bank"`.** 761 of 781 listed names are not banks;
the default would have mapped a corporate filing against the bank chart of accounts and
rejected every statement hours later as a parse failure. It now raises, and records WHICH
route answered — `templates.csv` and "CafeF's line-item fingerprint, over the network" are not
the same claim.

⚠️ **THE PAGE HOOK REPORTED NOTHING ON ITS FIRST VERSION AND LOOKED FINE.** It was set on
`builder._parsers.values()`, which on a fresh builder holds the env-default parser only — the
onnx parser is built lazily by `_parser_for` on the first layer that needs it. Zero lines, no
error. The builder owns it now. **A progress reporter that reports nothing fails exactly like a
fast run.**

⚠️ **New OCR weights knob:** `CAFEF_ONNX_VIETOCR_WEIGHTS` points VietOCR at a LOCAL checkpoint.
`download_weights` returns any non-`http` value unchanged, which is what makes it work; empty
restores the URL, i.e. the behaviour every run had before. An unreadable path RAISES rather
than falling back to the download — on a worker with no internet that fallback is a connection
error minutes into the first page.


### The OCR stack — `requirements-ocr.txt`, and why it is not all `==`

One file, installed into `mt_env` by hand and onto a Kaggle worker by the notebook. Every line
in it changes pixels, boxes or characters, so a drift between the two machines is a drift in
what the OCR reads.

⚠️ **PINNING BOTH MACHINES TO ONE onnxruntime VERSION BROKE THE WORKER** (`ORT-2`, measured
2026-08-28 over four T4 runs with opencv varied to exonerate it): `1.22.0` reproduces VCB
Q1-2026's 98 cells at `onnx@200`, `1.20.2` makes that layer FAIL, and the `onnx@300` fallback
writes a row-slid income statement that `reconcile` and `sane` both accept. Both versions are
inside the CUDA 12 / cuDNN 9 line — **supported is not equivalent**.

✅ **AND THEN ALIGNED PROPERLY, 9 OF 10** — because the range-pin above was an answer drawn
from ONE direction. Only the DOWNWARD move had been tried; moving THIS machine UP to Kaggle's
**1.22.0** reproduces 98/98 here too, so a single version holds. The same treatment aligned
**both** opencv distributions to 4.13.0.92 (Kaggle ships both, either can win the `cv2` import,
so pinning one is a collision). Every pin is guarded by a VCB Q1-2026 re-parse.

| | |
|---|---|
| pinned to ONE version, both machines | `onnxruntime-gpu==1.22.0`, `pymupdf`, `vietocr`, `opencv-python`, `opencv-python-headless`, `shapely`, `pyclipper`, `numpy`, `einops` |
| ❌ cannot be aligned | `torch` — measured: pip succeeds, the next cell still prints the OLD version because a running interpreter cannot be handed a new torch, and the kernel then DIES. `requirements-ocr-torch.txt` is kept OFF so nobody spends a run re-learning it |
| unchoosable | Python patch (3.12.10 / 3.12.13), OS, and the **GPU architecture** — sm_86 against sm_75, which selects different cuDNN kernels and would survive a byte-identical library set |

⚠️ **VERSION IDENTITY IS NEITHER NECESSARY NOR SUFFICIENT FOR OUTPUT IDENTITY, and one
measurement of each exists**: a MISMATCHED pair (1.20.1/1.22.0) reproduced 98/98, and an
IDENTICAL pair (1.20.2 both) diverged. Alignment narrows where to look when something moves;
**the proof is `compare()` against the cells on disk.**

⚠️ **"Same version" is not the goal; "same OUTPUT" is.** `engine_report()` records the whole
stack, a 12-char `stack_fingerprint` and `pin_violations` into every `metadata.json` —
**reported, never enforced**, because a worker that could not honour a pin has still done work
worth collecting. Two runs whose fingerprints differ may be compared on correctness and on
nothing else.

⚠️ **`source.zip` had to learn `.txt`** or the pins file never reached the worker; and the
install cell had to stop reconstructing `==` pins from the file, because the day one became a
RANGE it was silently never installed and the run blamed a missing internet connection it had.


### Is the output identical on both machines? One diacritic, and no figure

Measured 2026-08-28. `compare()` scores a run against the CSV on disk, so two `REPRODUCED`
runs agree on the MAPPED cells only. `rows_sha` + `row_dump` record every line the OCR read:
VCB Q1-2026's balance sheet (72 rows) and income statement (29) are identical on both
machines; the cash flow's 32 rows differ in **one label** — `khoản` locally, `khoàn` on the
worker — and **no `values` entry differs anywhere**.

✅ It cannot propagate: a row is matched on `slug(label)`, accent-stripped ASCII, and both slug
to `cac_khoan_tien_gui_cua_khach_hang`. **A tone-mark misread cannot move a figure.**

⚠️ It is a RECOGNITION difference — VietOCR under `torch`, the one library that cannot be
aligned, on different silicon (sm_86 / sm_75). Which of the two is unestablished.

### ⚠️ `CWD-1` — the merge read the statement CSVs through a RELATIVE path

Measured 2026-08-30 while repairing BID Q4-2016 (`PAR-1`'s last open cell). `statement_path()`
reads `fin.STATEMENTS_DIR` **at call time**, and the module default is relative
(`raw_data/cafef/financials/statements`). `pdf_ocr_job.use_data_root` re-points it from the
resolved data root — which is why `compare()` finds disk from any cwd — and
**`pdf_ocr_merge.plan_merge` never did**.

| the identical `merge_run(..., apply=False)` | `on_disk` | refusal 3 | plans |
|---|---|---|---|
| from `src/` | **`absent`** | **could not run** | **2 writes** |
| from the repo root | `pdf` | ran | **0** — DIFFERS, refused correctly |

⚠️ **`absent` IS A LEGITIMATE ANSWER, WHICH IS WHY THIS WAS SILENT.** A ticker with no CSV yet
reads exactly the same (`BND-1`, and BSR was bootstrapped that way on 2026-08-30), so a
mislocated root and a genuinely new ticker are indistinguishable from the reason string. The
guard that stands between a merge and a wrong figure on disk was skipped, and `_write` would
have put the row under the wrong root as well. ⚠️ **`kgpu merge` runs from `src/kaggle_gpu/`** —
the same cwd that mislocated `BACKUP_ROOT` on 2026-08-29, which was anchored then while this
was not.

✅ **Anchored to `pdf_ocr_job.DEFAULT_DATA_ROOT`, and ONLY while the path is still relative.**
That predicate is the defect's own definition rather than a proxy: a relative `STATEMENTS_DIR`
is exactly one that resolves against the cwd, and the module default is the only relative value
there is. Anything absolute was set deliberately — by `pdf_ocr_job.run`, by an experiment
harness (`statement_path`'s docstring records that contract) or by a test fixture — and
overruling it would move the WRITE, not just the read.

⚠️ **The first version was unconditional, and it was worse than the defect**: the `root` fixture
in `test_pdf_ocr_merge.py` monkeypatches `STATEMENTS_DIR` into a `tmp_path`, so every
`apply=True` test would have written into the real `raw_data/`. Caught by reading the fixture
before running the suite. **2 tests, one per direction.**

⚠️ **`DEFAULT_DATA_ROOT`, never `data_root()`** — the merge upserts into THIS repo by definition
(`pdf_ocr_job.run` refuses to merge at all when the root is a payload), so honouring
`$CAFEF_DATA_ROOT` could only point it at a copy that dies with a kernel.

### ⚠️ 3c. THE CASCADE READ EVERY PAGE 24 TIMES TO GET 7 ANSWERS — `P41` + `P42`, 2026-08-30

Two cost defects, measured together and fixed together. **Nothing about the parse, the gates,
the layer order or the 49 layers moved**, and that is the claim the regression below has to
carry: a change to the path that writes fundamentals is only worth having if the fundamentals
do not move.

| document | before | after | |
|---|---|---|---|
| **BID Q4-2016** — the hardest filing on disk, cash flow at layer 47 of 49 | **64.6 min** | **9.5 min** | **6.8x** |
| **VIC Q1-2026** — `corp`, all three statements refused | 34.8 min | 5.2 min | **6.7x** |
| **TCB Q3-2013** — three different layers, two at 300 dpi | 39.1 min | 7.9 min | **5.0x** |
| VCB Q1-2026 — accepted at layer 1, ONE OCR pass | 1.4 min | 1.4 min | 1.0x |

⚠️ **THE EASY DOCUMENT IS THE CONTROL AND IT IS SUPPOSED TO BE FLAT.** A filing accepted at
layer 1 pays one OCR pass and one note scan; there is no repetition to remove. Everything
below is about what a filing pays when it does NOT stop at layer 1 — which is ~17 % of
quarters (§6-2-quindecies' three-ticker failure rate) and all of the tail of the cost
distribution.

#### `P42` · the parse cache keys on eleven fields; the OCR depends on three

`_parse_cascaded` caches a whole parse under `parse_key`, which carries every `ParseLayer`
flag because every one of them can change the ROWS. **Not one of them can change a recognised
character**: `join_digits`, `title_over_form`, `loose_form_code`, `realign_rows`,
`notes_boundary`, `tail_continuation`, `label_wrap` and `unit_from_document` all run AFTER
`scan` has read the page, in `_page_kind`, `_fill_continuations`, `table_rows` or `parse`.
Counted over the 49 layers: **24 distinct `parse_key` against 7 distinct `ocr_key`**.

So `PdfParser._ocr_cache` memoises `(text, words)` per page under `(engine, dpi, crop_pad)`,
scoped to one document (`_use_document`, cleared when `parse` is handed a different path — a
parser instance is reused for a whole run by `_parser_for`, and page 3 of one filing is not
page 3 of the next). ⚠️ **`_split_number_runs` stays OUTSIDE the cache**, because
`join_split_digits` is per-LAYER: freezing it into the stored words would hand
`onnx@200+join+components` the un-joined words of `onnx@200` and the flag would do nothing at
all. A test drives exactly that.

#### `P41` · the capital-note scan was 69-77 % of a parse, and invisible

`share_capital` walks from the last statement page to the **END** of the filing, OCR-ing every
page until it meets `SHARE_NOTE_ANCHOR`. It calls `_ocr_page` directly rather than through
`scan`, so **the page-progress hook never saw it and it has never appeared in a run log** —
which is why 23 passes of ETA-inverted page rates summed to 16 min against a 64.6 min run.
Profiled per phase 2026-08-30:

| filing | `scan` | `share_capital` | found |
|---|---|---|---|
| BID FY-2016 (bank, 62 pp) | 14 pages, 37.6 s, 30.5 % | **50 pages, 84.8 s, 68.8 %** | nothing |
| VIC Q1-2026 (corp, 71 pp) | 14 pages, 24.5 s, 22.9 % | **58 pages, 81.9 s, 76.6 %** | nothing |

`parse()` gained `want_shares`, and `_parse_cascaded` passes `not facts["publish_date"]` —
**the same condition the block below the call reads the counts under**. `facts["shares"]` is
assigned inside `if not facts["publish_date"]`, so once a layer has produced a signing date no
later layer's counts can be looked at, while `parse()` went on paying for them once per parse
key. `facts` cannot change between the two lines, so this is provably output-identical.

⚠️ **IT IS REDUCED, NOT CLOSED, AND THE RESIDUE HAS A SHAPE.** A filing whose pages carry no
signing date leaves `facts` open, so every layer still asks — TCB Q3-2013 is exactly that. The
page cache makes the repeats cheap (the scan now costs once per OCR **configuration**, not
once per parse key), which is why that document still came down 5.0x. Two further reductions
are measured-available and deliberately NOT taken: `SHARE_NOTE_ANCHOR` is
*"phat hanh cua ngan hang"*, so on `corp`/`securities`/`insurance` it can never match
(**0 of 91 `corp` rows on disk carry a share count, against 201 of 753 `bank` rows**), and the
walk has no page budget. Both are behaviour changes rather than cost changes, and neither was
needed to get the 6.8x.

#### The recogniser was bucketing its crops AFTER chunking them

`_BatchedVietOcr.__call__` sorted crops by ASPECT RATIO and chunked by `batch_size`;
`predict_batch` then re-grouped each chunk by the EXACT padded width `process_input` produces,
because a vietocr batch is one autoregressive decode and must share a width. Measured on BID's
FY-2016 filing: **542 crops over 44 distinct widths**, so a 24-crop chunk fragmented into a
dozen decode loops of one or two images. Grouping by width FIRST and chunking each group is
**1.11-1.22x on recognition, over four interleaved pairs, with 0 of 542 crops changed**;
`REC_BATCH` went 24 -> 64 with it, because a chunk is now one real batch rather than a cap on
how badly the crops can be regrouped.

⚠️ **A FIRST MEASUREMENT OF THIS SAID 1.37x AND IT WAS MEASURING A DIFFERENT THING.** The
benchmark pre-computed each crop's tensor once and handed it straight to `translate`, which
skips the convert + LANCZOS-resize + normalise that `predict_batch` does internally — a saving
the shipped code does not take. The shipped version asks vietocr's own `resize` for the WIDTH
(pure arithmetic on `(w, h)`, verified to agree with `process_input` on all 542 crops) and lets
`predict_batch` build the tensors as it always has.

⚠️ **AND THE BIG NUMBER IS NOT AVAILABLE FROM HERE.** Bucketing across the WHOLE document
rather than within a page is **2.35x**, also at 0 of 542 mismatches — 68 crops per page spread
over 44 widths is a thin bucket however it is chunked. Taking it means recognising pages in
blocks, and `scan` reads each page's TEXT to decide whether to read the next one (it stops at
the notes boundary once all three statements are behind it). Deferring recognition would change
which pages are read, which is a change to the parse and not to its cost.

⚠️ **Recognition is 85 % of an OCR pass** — profiled at 200 dpi: render 1.2 %, detection
12.7 %, cropping 1.1 %, **recognition 85.1 %**. Detection already runs on the GPU and the DB
detector downsamples to `DET_SIDE_LEN` whatever the render DPI, so neither rasterising nor
detecting is where the money is.

#### ⚠️ TWO FASTER THINGS WERE MEASURED AND REJECTED

1. **Padding every crop in a chunk to a common width** — 2.0x further, and **70 of 542 crops
   came back different** (`'Deloitte'` -> `'Deloitte.'`, `'ĐÃ ĐƯỢC KIỂM TOÁN TH'` ->
   `'ĐÃ ĐƯỢC KIỂM TOÁN TRUNG'`). The recogniser is width-sensitive; a fuller batch bought a
   different answer. **That is why the change that shipped is bucketing and not padding**, and
   it is why the 0-of-542 figure is quoted beside it.
2. **A rewritten greedy decode** — no per-step `.to('cpu')`, no `topk(5)` for a top-1, no
   O(steps^2) numpy rebuild of the whole token history. It is **not faster** (3.00x against
   the bucketing's 3.07x on the same crops) and it changed one crop. The decode is bound by
   the RNN step, not by the host work around it.

#### What the change was verified against

⚠️ **`rows_sha`, not the mapped cells.** `compare()` scores the cells that map to a chart of
accounts — 76 for BID Q4-2016 — and says nothing about the rest of the statement. `rows_sha`
digests **every row the OCR read**: the label, the filing's own numbering and every figure of
every parsed row.

| | |
|---|---|
| BID Q4-2016 | all three statements **IDENTICAL `rows_sha`**, same winning layer `onnx@200+pad6+annual+extra` |
| TCB Q3-2013 | all three **IDENTICAL**, layers `onnx@200` / `onnx@300+unit+tail` / `onnx@300+unit` |
| VCB Q1-2026 | all three **IDENTICAL**, and 98 of 98 cells `REPRODUCED` |
| VIC Q1-2026 | all three refused — **identical refusal reasons, layer for layer** |

⚠️ **AND VIC's REFUSAL WAS PROVEN PRE-EXISTING RATHER THAN ASSUMED.** VIC Q1-2026 parsed at
`onnx@200` on 2026-08-28 and is refused now, which reads like a regression. It is not: HEAD
was stashed back in and re-run against today's disk (34.8 min) and produced **the same three
absences with the same reasons**. The cause is `sane` — every VIC balance sheet on disk is
2008-2014, and six more small quarters (2011-2014) were merged on 2026-08-29, pulling the
band's median to 5.11e13 while Q1-2026 is 1.18e15. **A stash-and-re-run is what separates "my
change did this" from "the disk moved underneath it", and nothing cheaper does.**

#### ⚠️ AND THE PROGRESS DENOMINATOR HAD TO MOVE WITH IT

`run()` counted `parse_key` to size the overall %, so the first run under the cache printed
*"OCR pass 23/24"* on a document that read its pages **7** times — a denominator naming work
that no longer happens. `fin.ocr_key` is that denominator now, and `_parse_cascaded`'s
`on_layer(cached=...)` means *"this layer reads no pixels"*, which folds three states into the
two the bar can show: a repeated `parse_key` returns in milliseconds, a new `parse_key` whose
`ocr_key` has already run re-maps cached pages, and only a new `ocr_key` costs a pass.

**`test_cafef_ocr_cache.py` — 15 tests, no PDF, no network, no OCR engine.**

## 4. Source specialization (why 3 price sources)

Matches the bronze-source decision (memory `project-bronze-source-per-field`):

| Field | Primary | Notes |
|---|---|---|
| OHLC / volume / foreign flow | **Simplize** | fully adjusted, true volume, most complete |
| split-only / negotiated volume, raw vs adj close | **CafeF** | matched/negotiated split, `close_raw`/`close_adjust`, '000 VND |
| market-index level + breadth | **CafeF** | the only source here for index-level order flow / prop / foreign (§3, *CafeF indices*) |
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

## 5. How it's driven — ⚠️ REWRITTEN 2026-08-10: the run plan is `--select`

**HOW TO RUN ANYTHING HERE:**

```powershell
dagster asset materialize -f src/orchestration/definitions.py --select "group:cafef"
dagster asset materialize -f src/orchestration/definitions.py `
    --select "raw/cafef_financials" --partition "HOSE_VCB"     # ~2.4 h — read §3a first
```

Truncate `logs/app.log` first; it is still the record of what the scraper itself did
(Dagster's own per-run logs are separate, in `.dagster/`).

> ### ⚠️ THIS SECTION USED TO SAY SOMETHING ELSE, AND IT WAS DELETED-FILE ADVICE
>
> It opened: *"HOW TO RUN ANYTHING HERE: edit `src/switch_config.json`, then
> `python src\main.py` from the repo root. There is no per-scraper runner and no CLI
> flag — `main.py` calls every scraper unconditionally and each one no-ops unless its
> leaf is on, so the config IS the run plan."*
>
> **Every file in that sentence is gone** — `main.py` deleted 2026-08-05,
> `switch_config.json` deleted 2026-08-06. Following it would have edited a file that
> does not exist and run a script that does not exist. Kept here as history because it
> explains the shape of the flag tree below, which DOES survive.

### 5a. The flag tree survives — as PARAMETERS, not as a run plan

The distinction is the whole point: the tree no longer decides **whether** a scrape
runs (that is `--select`), only **what a running scrape enumerates** — which countries,
sectors and categories. Its 295 leaves live in
[`orchestration/config.json`](../orchestration/config.json)'s `parameters` section, and
`orchestration/enabled.py::trading_view_switches()` rebuilds the flat
`web_scraper/trading_view/<phase>/…` paths from it and hands them to an ordinary
`SwitchHandler`. **Not one line of the scrapers changed** — the fifteen
`get_enabled_paths` call sites in `trading_view_scraper.py` still index positionally
(`parts[4]` is the country, `parts[6]` the sector). Verified by golden test against the
deleted file: 17 of 19 (phase × asset class) selections byte-identical.

- ⚠️ **`SwitchHandler` has NO DEFAULT PATH any more.** It takes an explicit `switches`
  dict, an explicit file, or neither. "Neither" is what CafeF, Simplize and
  `DataPreprocessor` get — they all require a handler in their constructor and none of
  them ever calls it. **A resurrected `src/switch_config.json` now RAISES.**
- The format it reads is unchanged: a **flat JSON of
  slash-path → bool**. A path is enabled only when **every prefix is explicitly
  true** (disabling a parent disables the whole subtree); missing key = false; keys
  starting `//` are inline comments.
- Two APIs: `is_enabled("a","b","c")` (all-ancestors check) and
  `get_enabled_paths(*prefix)` (returns enabled **leaf** paths — used by the TV task
  adders to enumerate exactly which `(country, stock_type, sector)` etc. to scrape).
- **⚠️ A BRANCH KEY MUST NOT END IN `/`.** `is_enabled("web_scraper","cafef","price")`
  looks up the prefix `web_scraper/cafef`, so the key `"web_scraper/cafef/"` never
  matches and the branch is unreachable *no matter what its leaves say*. That exact typo
  disabled the whole CafeF **and** Simplize branch until it was found on 2026-07-30 —
  and it fails silently, because a false switch is a normal outcome. Same trap applies
  to a **BOM**: `_load_config` swallows a read error and returns `{}`, i.e. every switch
  false and `main.py` a complete no-op (now read as `utf-8-sig`, so only a JSON typo
  does it). When a run does nothing, check the log line
  `Switch config loaded: N switches (M enabled)`.
- **The leaves, by source:**

  | Leaf | Drives | Cost |
  |---|---|---|
  | `web_scraper/trading_view/{links,collected_links,data}/…` | TV, gated per `(asset, country, sector)` | varies |
  | `web_scraper/cafef/{price,order_stats,foreign,prop_trading,insider_txn}` | `CafeFScraper` daily tabs | whole universe |
  | `web_scraper/cafef_index/{price,order_stats,foreign,prop_trading}` | `CafeFIndexScraper` | 6 indices — **minutes**, all four tabs |
  | `web_scraper/cafef/news` | `CafeFNewsScraper` | whole universe (already on disk) |
  | `web_scraper/cafef/pdfs` | `CafeFPdfScraper` | **~1.0-1.7 GB/ticker**, `CAFEF_PDF_TICKERS` = VN100 |
  | `web_scraper/cafef/financials` | `FinancialsBuilder.build_all` | **~2.4 h/ticker**, `CAFEF_FINANCIALS_TICKERS` = VCB+ACB |
  | `web_scraper/simplize/{stocks,industry}` | `SimplizeScraper` | stocks = 2.6 M rows; industry = minutes |
  | `web_scraper/gics/structure` | `GicsScraper` | one file |

- **⚠️ The two per-FILING pipelines are scoped by an explicit LIST, not by the universe** —
  `CAFEF_PDF_TICKERS` (VN100, read from the repo-root `vn100.csv`; ~97 GB, matching the 108
  folders on disk) and `CAFEF_FINANCIALS_TICKERS` (VCB + ACB — only what has actually been
  parsed) in `src/utils/constants.py`. They cost orders of magnitude more per ticker than
  the daily tabs, so the full 777 codes would be a terabyte-scale download / a multi-week
  parse, not a longer version of the same job. **Add a ticker by editing that list.**
  Every `scrape()` still takes a `symbols=` override for a one-off.
- The remaining scrapers (TV, the CafeF daily tabs, **news**, Simplize, GICS) keep the
  **full universe** as their default — they are per-ticker-cheap and `skip_existing=True`
  makes a re-run skip what is already on disk.
- **Current committed state:** `"web_scraper": false` (master off), and the three heavy
  leaves (`pdfs`, `financials`, `news`) plus `simplize/industry` ship `false` — opt in.
  The daily-tab leaves ship `true`, so enabling the master alone starts a full-universe
  CafeF + Simplize price run.

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
    predictable. ⚠️ **TradingView is the exception since 2026-08-05: it does NOT take
    `SCRAPER_MAX_WORKERS`.** Every task it runs opens a browser, so its pool is sized
    to `max_browsers` (`SCRAPER_MAX_CONCURRENT_BROWSERS`, 4) and the two can no longer
    disagree — which they did, at 2 workers against a cap of 1 documented as 8.
    (The permit was taken in the DATA phase only until 2026-07-31 — the LINKS phase
    took none and ran the whole pool. Fixed; this paragraph now describes both.)
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
  (at least the links phase) first, or they scrape nothing. (`CafeFIndexScraper` is the
  one exception — its universe is a fixed list, so it is order-independent.) The two newer CafeF scrapers do
  the same, but both take a `symbols=[(exchange, symbol), …]` override, which is how a run
  is scoped to VN30/VN100 instead of all ~777 codes.
- **Everything is wired into `main.py` as of 2026-07-30.** `CafeFNewsScraper`,
  `CafeFPdfScraper` and the PDF-reading pipeline (§3a) used to be registered and importable
  with nothing driving them — they were run by hand-editing `test.py`. They now each have a
  switch leaf and run from `main.py` like the rest (§5). The parse pipeline is not a
  `BaseScraper` (it reads the LOCAL archive, nothing is scraped), so it is driven by
  `FinancialsBuilder.build_all(logger, switch_handler)` rather than `scrape()`.
  `CafeFPdfScraper` still must NOT be pointed at the full 777-ticker universe: the archive
  averages ~1.0-1.7 GB *per ticker* (VN100 alone is ~97 GB), which is why its `scrape()`
  defaults to `CAFEF_PDF_TICKERS` rather than `get_stock_symbols()`.
- **⚠️ `build_templates_index` REWRITES `templates.csv` from exactly the symbols handed to
  it — it does not upsert.** Calling it per ticker therefore leaves a ONE-ROW file naming
  only the last ticker parsed: that is how VCB lost its row when ACB was parsed alone on
  2026-07-24, leaving a `templates.csv` that mapped no ticker to the statement CSVs sitting
  right beside it. `build_all` closes this by building the index **once, for the whole
  `CAFEF_FINANCIALS_TICKERS` list, before any parsing** — so the file is correct regardless
  of which subset actually parses, or of one ticker failing. Never call
  `build_templates_index` with a subset directly.
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
- **⚠️ A SHORT PAGE FROM CAFEF IS NOT THE LAST PAGE — fixed 2026-07-30, and the
  PER-STOCK CSVs ON DISK PREDATE THE FIX.** `_collect` / `_collect_paged` used to stop
  paginating a window on `len(rec) < PageSize`, which is wrong: for HNX-INDEX over
  `05/01/2026..07/06/2026` the price tab returns **19 rows on page 1** and then 20 and 6
  on pages 2-3, so the short first page was read as end-of-data and 19 of 45 rows were
  kept. It cost the HNX, HNX30 and UPCOM index price series **23 trading days each**,
  recovered by the re-run (5,099 → 5,122 / 3,417 → 3,440 / 4,181 → 4,204).
  Pagination now ends only on an EMPTY page or one contributing no new date.
  - **It is silent, which is what makes it dangerous:** a truncated window is
    indistinguishable from a market holiday once the CSV is written, and nothing in the
    log fires — no request failed. It was found only by noticing that `prop_trading`
    held 22 dates the price series lacked.
  - **The same bug applied to every per-stock tab**, so `price/`, `foreign/`,
    `order_stats/`, `prop_trading/` and `insider_txn/` on disk may each be missing rows
    wherever CafeF served a short non-final page. They were scraped before the fix and
    `skip_existing=True` will not refresh them — a re-scrape with `skip_existing=False`
    is the only way to close it, and has NOT been done.
- **CafeF needs UPPERCASE `ExchangeType`** for HNX/UPCOM tickers or it silently
  defaults to HOSE and returns empty.
- **CafeF prices are '000 VND** — `_mul` ×1000 is applied to OHLC + both closes but
  NOT to volumes/values (those come pre-scaled). **For an INDEX there is no ×1000 at
  all** — the value is a point, not a price (§3, *CafeF indices*).
- **⚠️ `value_matched` AND `value_negotiated` ARE IN DIFFERENT UNITS, in `price/` and
  `index_price/` alike.** CafeF's `GiaTriKhopLenh` is **billions of VND** (VCB 319.09
  for 4,951,400 shares at 64,400 đ; VNINDEX 19,127.05) while `GtThoaThuan` is **raw VND**
  (1,235,389,240,000). Neither is scaled by the scraper, so the two columns of the same
  row differ by 10⁹. This is pre-existing behaviour that bronze already ingests, so the
  index folders mirror it deliberately rather than diverge — but anything comparing or
  summing the two must convert first.
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
- **The four `index_*/` folders REACH BRONZE as of 2026-07-30** —
  `_ingest_bronze_cafef_index_{price,foreign,order_stats,prop_trading}` →
  `bronze.cafef_index_*`, PK `(exchange, ticker, date)`, gated by
  `data_preprocessor/data_quality_bronze/cafef_index_*`. Because this scraper subclasses
  the stock one and reuses its column constants, the ingests are thin wrappers on the
  same `_ingest_bronze_cafef_daily` helper with the same cast lists.
  **⚠️ THEY ARE SEPARATE TABLES AND MUST STAY THAT WAY.** `ticker` holds an INDEX code,
  not a company, so unioning them into `cafef_price` would put six phantom stocks into
  `silver.stocks_basic` with NULL GICS classes and carry them into every cross-sectional
  model. An index is a different GRAIN, not another ticker. Nothing in silver or gold
  reads them yet — that is the open work.

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
