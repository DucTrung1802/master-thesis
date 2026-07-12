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

## 2. Directory layout & the Strategy/registry pattern

```
src/web_scraper/
├── CONTEXT.md                ← this file
├── base_scraper.py           BaseScraper ABC + SCRAPER_REGISTRY + @register_scraper + build_scraper
├── trading_view_scraper.py   SOURCE_NAME="trading_view"  (Selenium/Chrome + BS4, ~1600 lines)
├── cafef_scraper.py          SOURCE_NAME="cafef"         (requests → CafeF AJAX; the 5 daily tabs)
├── cafef_pdf_scraper.py      SOURCE_NAME="cafef_pdf"     (requests → the filing PDFs themselves)
├── cafef_news_scraper.py     SOURCE_NAME="cafef_news"    (requests → company-news / disclosure feed)
├── simplize_scraper.py       SOURCE_NAME="simplize"      (requests → api.simplize.vn JSON)
└── gics_scraper.py           SOURCE_NAME="gics"          (requests + openpyxl → MSCI xlsx)
```

The three CafeF modules are separate sources, not one: `cafef_scraper` pulls the daily
price/flow tabs, `cafef_pdf_scraper` downloads the filings, `cafef_news_scraper` pulls the
event stream. Each registers its own `SOURCE_NAME` and writes its own folder under
`raw_data/cafef/`.

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
  scrapes what each tab is still missing (price/foreign done; order_stats/prop/insider
  currently VN100-only → ~681 tickers/tab remaining).

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
- **Size:** ~1.7 GB for VCB (90% of its filings are page scans), ~0.65 GB for VIC. Budget
  accordingly before pointing this at VN100.

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
  VCB 1,629 rows / PNJ 1,715 / FPT 2,255, spanning 2007 → 2026.
- **Gotchas:**
  - **`order` is numbered from the article's own `datePublished`, not from the headline
    feed's listing date.** The two disagree, and numbering by the listing left `order`
    claiming a chronology the timestamps did not have — which leaks look-ahead into anything
    keyed on it.
  - Article bodies are the expensive stage (~1,600-2,300 per ticker) → fetched in parallel
    with a polite per-worker delay. ~2 min/ticker, so VN100 is hours, not days.
  - A dead link keeps its headline as `type=error` rather than dropping the row.
  - Headline text comes from the anchor's **inner text**; the `title=""` attribute breaks on
    the embedded quotes in legacy headlines.

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
| split-only / negotiated volume, raw vs adj close | **CafeF** | matched/negotiated split, `close_raw`/`close_adj`, '000 VND |
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
  `SCRAPER_END_DATE` 2026-04-30, retries 5×5s, 8 browsers, 8s nav stagger),
  `*_RAW_DATA_DIR` paths, `TRADING_VIEW_TABLE_SCHEMA`, and
  `SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY`.
- `src/thread_manager/thread_manager.py` + `dtos/thread_manager_dtos/task.py` —
  `Task(name, func, *args)` queued and run by `ThreadManager(power=%)`;
  `task.run()` calls `func(*args)` directly (no lambda wrapper).
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
- **`CafeFPdfScraper` and `CafeFNewsScraper` are NOT called from `main.py` yet** — they are
  registered and importable, but nothing drives them in the pipeline. Add them alongside
  `CafeFScraper` when you want them in the main run.
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
