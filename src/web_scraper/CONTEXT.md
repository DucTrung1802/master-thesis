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

**Where the statement parser stands — VCB, Q3-2008 → Q1-2026 (71 quarters):**

| report | quarters | pdf | cafef | missing | coverage | dated |
|---|---|---|---|---|---|---|
| balance_sheet | 71 | 57 | 14 | 0 | **100%** | 70/71 |
| income_statement | 71 | 46 | 25 | 0 | **100%** | 70/71 |
| cash_flow | 71 | 61 | 10 | 0 | **100%** | 70/71 |

**213 / 213 — every quarter VCB financial data exists for.** The written grid starts at Q4-2006
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
├── statements/<template>/<report>/<EXCHANGE>_<SYMBOL>.csv
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
  the PDF's own text layer, or **Tesseract OCR** (`vie`) for the documents that have none —
  **90% of VCB's filings are page scans, and not only the old ones** (its Q1-2026 report is 53
  pages of image). Both return word boxes in the same coordinate system.
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
  - **The scans are stored `/Rotate 180`.** PyMuPDF rasterises them upright to OCR — so the
    text is correct — but hands back word boxes in *unrotated* space, mirrored. Left and right
    swap and the parser's whole premise inverts. Clearing the rotation is not a fix (OCR then
    reads an upside-down image); the boxes are mapped through the rotation matrix instead.
- **`cafef_financials.py` — the archive → CSVs.** Picks the *consolidated* filing per quarter
  (preferring reviewed/audited), maps its rows onto the canonical schema, gates it, and writes
  a contiguous quarter grid — a quarter it could not read is a blank `source=missing` row,
  never zero-filled.
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
