# Context — `src/web_scraper` (raw-data acquisition layer)

> Handoff notes for a new session. Describes the web-scraping subsystem: how the
> four data sources are structured, what each pulls, how they are driven, and where
> output lands. This is the **bronze-input** stage — it writes CSV/xlsx under
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
                  SimplizeScraper.scrape()      ← validated daily-panel backbone (requests)
                  GicsScraper.scrape()          ← MSCI GICS taxonomy (independent)
                       │
                       ▼
                  raw_data/<source>/...  (CSV + the raw GICS .xlsx)
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
├── cafef_scraper.py          SOURCE_NAME="cafef"         (requests → CafeF AJAX)
├── simplize_scraper.py       SOURCE_NAME="simplize"      (requests → api.simplize.vn JSON)
└── gics_scraper.py           SOURCE_NAME="gics"          (requests + openpyxl → MSCI xlsx)
```

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
- `requests` against CafeF `du-lieu` AJAX endpoints (`PriceHistory.ashx`,
  `GDKhoiNgoai.ashx`). Provides **raw + adjusted close** (`close_raw`/`close_adj`),
  **matched & negotiated (block) volume**, and foreign flow.
- **Quirks handled:** `StartDate/EndDate` are **MM/dd/yyyy (US)**; a query is capped
  at ~63 rows and `PageSize` 20, so history is fetched in overlapping ~2-month
  windows and paginated; `ExchangeType` (HOSE/HNX/UPCOM, UPPERCASE) is **required**
  for HNX/UPCOM or CafeF silently returns nothing; prices are in '000 VND so `_mul`
  multiplies OHLC + close by 1000. Price history from 2009, foreign flow from 2012.
- **Output:** `raw_data/cafef/stocks/<EXCHANGE>_<SYMBOL>.csv` (20 cols).

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

- TradingView prices are **split/stock-div adjusted but NOT cash-div adjusted**
  unless the ADJ toggle fires (memory `project-vcb-price-adjustment`).
- Simplize/CafeF are pure `requests` (fast, no browser). TradingView needs Selenium
  because the data only exists in the client-side chart widget.

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
  (at least the links phase) first, or they scrape nothing.
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
- **`skip_existing=True`** on CafeF/Simplize `scrape_stock` means re-running skips
  any ticker whose CSV already exists — delete the file (or pass `False`) to refresh.
- **GICS URL is version-pinned** to the Mar-2023 xlsx (GUID + `?t=` token required);
  if MSCI revises the structure the `EXPECTED_COUNTS` check logs a warning rather
  than failing.
- **`raw_data/` is the handoff to `src/data_preprocessor`** — schema/column names
  here are the contract its bronze ingest expects; changing an `OUTPUT_COLUMNS` list
  ripples downstream.
```
