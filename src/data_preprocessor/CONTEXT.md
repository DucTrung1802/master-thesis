# Context — `src/data_preprocessor` (bronze → silver → gold ETL)

> Handoff notes for a new session. Describes the medallion ETL that turns the raw
> CSV/xlsx written by `src/web_scraper` (under `raw_data/<source>/`) into three
> PostgreSQL schemas — **bronze → silver → gold** — inside one database
> (`DATABASE_MAIN_V2`). This is the stage *after* scraping. Verify anything before
> acting on it — the code and `src/switch_config.json` are the sources of truth.

## 1. Big picture / pipeline

```
raw_data/<source>/*.csv,*.xlsx           (produced by src/web_scraper)
        │
        ▼   ingest_bronze_data()   ← load CSVs as-is, one bronze table per source/tab
  bronze_schema:  trading_view_{bonds,economy,forex,funds,indices,stocks},
                  cafef_{price,foreign,order_stats,prop_trading},
                  cafef_index_{price,foreign,order_stats,prop_trading}  (6 MARKET INDICES),
                  cafef_insider_shareholder_transactions, cafef_news,
                  cafef_financial_{templates,schema,reports},
                  cafef_financials_<template>_<report>   (4 templates x 3 reports),
                  simplize_stocks, simplize_industry, gics
        │
        ▼   ingest_silver_data()   ← split symbol, merge sources, GICS classify
  silver_schema:  {bonds,economy,forex,funds,indices},
                  stocks_basic (CafeF four-way join + GICS tree),
                  gics, cafef_{price,foreign,order_stats,prop_trading},
                  cafef_insider_shareholder_transactions,
                  cafef_financials_<template>_<report>
        │
        ▼   ingest_gold_data()     ← feature engineering (TA + returns/vol/rolling)
  gold_schema:    {bonds,economy,forex,funds,indices,stocks}   (stocks ← silver.stocks_basic)
```

- **One file, one class.** [data_preprocessor.py](data_preprocessor.py) holds the
  whole ETL as `DataPreprocessor` (~1920 lines). The three public entry points
  (`ingest_bronze_data` / `ingest_silver_data` / `ingest_gold_data`) each
  connect, `CREATE DATABASE`/`CREATE SCHEMA` if needed, run the flag-gated
  per-asset ingests, and `disconnect()` in a `finally`.
- **`src/main.py` calls the three entry points in order.** Everything is
  **flag-gated** by `SwitchHandler` reading `src/switch_config.json` under the
  `data_preprocessor` subtree (see §5).
- **Medallion contract:** bronze = raw-faithful per source (key normalised, types
  cast, dedup'd); silver = one canonical cross-source table per asset with the
  GICS tree attached; gold = silver + engineered features (TA indicators, returns,
  volatility, rolling stats, microstructure ratios).

## 2. The DB driver it sits on (`src/tabular_database_driver`)

`DataPreprocessor` owns a `PostgreSQLDriver` (psycopg2). Two files:

- [tabular_database_driver_interface.py](../tabular_database_driver/tabular_database_driver_interface.py)
  — `TabularDatabaseDriverInterface(ABC)`: `connect/disconnect`, `create/drop_database`,
  `create/drop_schema`, `create/drop_table`, `insert/update/delete/select`. The
  Strategy contract so a different backend could be dropped in.
- [postgre_sql_driver.py](../tabular_database_driver/postgre_sql_driver.py) — the
  psycopg2 implementation. Key design points a new session must know:
  - **Per-cursor concurrency.** `_cursor_ctx()` is a `@contextmanager` that opens a
    **brand-new cursor per DML/DDL call** and closes it on exit, so the preprocessor
    can fan out chunked upserts across threads safely. The legacy shared-`self._cursor`
    path (`execute_query`/`fetch_result`) is **not** thread-safe — kept for
    back-compat only.
  - **Auto-create on connect.** `connect()` catches `"does not exist"` and creates
    the database, then reconnects. `create_table` uses `IF NOT EXISTS`. All
    connections run `autocommit = True`.
  - **Column cache.** `_get_table_columns` caches each table's column set (guarded
    by `_cache_lock`); `create_table`/`drop_table` invalidate it. Used to detect
    `create_date` / `update_date` / `delete_date` columns.
  - **`upsert()`** returns `(status, inserted, updated)` via an `INSERT … ON
    CONFLICT … RETURNING xmax` trick (xmax=0 → inserted). `select()` returns a
    pandas DataFrame.

## 3. The reusable helper toolkit (inside `DataPreprocessor`)

The per-asset ingests are thin; almost all logic lives in `_helper_*` methods:

| Helper | Role |
|---|---|
| `_helper_connect_to_database` | connect to bronze/silver/gold DB by `DataQuality` (note: entry points actually connect via `"postgres"` + `create_database`) |
| `_helper_select` | thin pass-through to `driver.select` → DataFrame |
| `_helper_clean` | applies a list of `CleanLayer` ops (drop-null, drop-all-null, order-by, drop-column, drop-dup-columns) |
| `_helper_cast_columns` | string-clean + cast to nullable `Float64` / `Int64` (strips commas, maps `""/nan/NULL/N/A` → `<NA>`) |
| `_helper_remove_duplicates` | dedup on PKs **only if duplicates exist**, with optional filter/sort/keep-first |
| `_helper_infer_sql_type` | pandas dtype → SQL type string |
| `_helper_ensure_table_exists` | build `Column` list (+ `dtype_overrides`) and `create_table` |
| `_helper_save_pandas_table_to_database` | **the workhorse writer** — see below |
| `_helper_copy_insert_to_database` | fast `COPY FROM STDIN` (pyarrow CSV, no upsert) for known-new rows |
| `_helper_build_upsert_sql` | the `ON CONFLICT … RETURNING xmax` upsert SQL |
| `_helper_load_csvs` | concat all CSVs in a folder, dropping empty/all-NA frames |
| `_helper_transform` | apply `TransformLayer` list per `(exchange, ticker)` group, sorted by date; optional checkpoint flushing |
| `_helper_build_feature_layers` | pick standard gold features by price shape (OHLC vs single `value`) |
| `_helper_build_gics_classification` | per-ticker full GICS tree from `simplize_industry` × `gics` |

**`_helper_save_pandas_table_to_database`** is the single save path: drops all-NA
rows, sanitizes values bound for `REAL` columns (±inf → NaN, subnormals → 0.0,
> REAL max → NaN), ensures the table exists, then either:
- **`use_copy=True`** → `COPY` bulk insert (gold path — freshly created table, no
  conflicts), or
- **default** → chunked (`chunk_size=5000`) parallel **upsert** via
  `ThreadPoolExecutor`, one cursor per chunk, counting inserted vs updated.

DTO helpers come from
[dtos/tabular_database_driver_dtos](../dtos/tabular_database_driver_dtos/tabular_database_driver_dtos.py):
`DataType` (classmethods returning SQL type strings), `Column`, `Condition`,
`JoinModel`, `Record`. `CleanLayer` / `TransformLayer` / `CleanAction` /
`TransformAction` live in `utils.enums` (imported via `from utils.enums import *`).

## 4. Layer-by-layer — what each ingest does

### Bronze (`_ingest_bronze_*`) — raw-faithful, one table per source
- **⚠️ THE KEY IS `(exchange, ticker)`, SPLIT — the `"<EXCHANGE>:<TICKER>"` colon key
  is GONE from bronze.** As of 2026-07-16 no bronze table has a `symbol` column; every
  price/daily table is PK'd on `(exchange, ticker, date)` and the event tables carry
  `exchange`/`ticker` beside their surrogate keys. Two split paths, by raw shape:
  - **TradingView** stores only the colon `symbol` → `_helper_split_symbol_column`
    splits it on the FIRST `:`, **on read** (2026-08-01; it used to run just before
    save). Note this splits a **data-provider prefix**, not always a bourse:
    `ECONOMICS:CN14RRR`, `B2PRIME:AUDCAD`, `TVC:VN01` → `exchange` is the vendor
    namespace for the non-stock assets.
  - **CafeF & Simplize** keep `exchange` and the ticker apart in the raw CSV already,
    so they just `rename(symbol → ticker)`.
  - ✅ **`symbol` now exists in exactly one place: `_helper_split_symbol_column`,
    called on read (2026-08-01).** Before that it was the layer's working key —
    the six TradingView ingests cleaned, ordered and deduped on `symbol` and split it
    only at the end, and `_ingest_bronze_cafef_daily` had a `split_key=False` branch
    that FOLDED CafeF's `(exchange, symbol)` into `"HOSE:VCB"` just to split it apart
    again. **All 8 callers passed `split_key=True`**, so that round-trip was dead code
    — but it kept `symbol` looking like a bronze concept, which is exactly how five
    silver ingests came to split a column no bronze table has ever stored. The dead
    branch and `_helper_normalise_cafef_symbol` are deleted; clean/order/dedupe now key
    on `(exchange, ticker, date)` everywhere.
    > **Verified by re-ingesting all 20 bronze leaves** (2026-08-01, via Dagster
    > `--select group:bronze`, 20/20 green): **22 of 25 tables reproduced their row
    > count exactly**. The three that changed — `cafef_news` 5,599 → 405,320,
    > `cafef_order_stats` 351,373 → 2,523,196, `cafef_prop_trading` 64,139 → 73,810 —
    > are **stale bronze catching up with raw data** (those scrapers ran 2026-07-23/24
    > over the full 777-781 ticker universe; news had been 3 tickers). Each now matches
    > its raw folder exactly: 2,523,196 = 2,523,196, 73,810 = 73,810; news drops 2 rows
    > of 405,322 on the null-key clean.
- **Simple TradingView asset classes** (`bonds/economy/forex/funds/indices` +
  `trading_view_stocks`): glob `raw_data/trading_view/data/<asset>/**/*.csv`, concat,
  **split `symbol` → `(exchange, ticker)` immediately**, clean (drop rows null on
  `exchange`/`ticker`/`date`/`value|close`), cast, `date → date`, dedup on
  `(exchange, ticker, date)`, save. Same PK.
- **CafeF — one bronze table per scraper link-folder** (mirrors the scraper's
  one-folder-per-link design; the former single merged `cafef_stocks` is gone —
  the price+foreign merge moved to silver). All share the `_helper_load_cafef_folder`
  helper; the four daily ones go through the generic `_ingest_bronze_cafef_daily`
  (PK `(exchange, ticker, date)`):
  - `cafef_price` — OHLC (`close_raw`/`close_adjust`) + matched/negotiated vol/val. PK `(exchange, ticker, date)`.
  - `cafef_foreign` — foreign buy/sell/net flow (vol+val), `foreign_room_left`, `foreign_own`. PK `(exchange, ticker, date)`.
  - `cafef_order_stats` — buy/sell order counts, volume, avg vol/order. PK `(exchange, ticker, date)`.
  - `cafef_prop_trading` — proprietary-desk buy/sell vol+val. PK `(exchange, ticker, date)`.
- **CafeF MARKET INDICES — the same four tabs, four more tables** (added 2026-07-30):
  `cafef_index_{price,foreign,order_stats,prop_trading}`, from the `index_*/` folders
  `CafeFIndexScraper` writes. That scraper **subclasses the stock one and reuses its
  column constants**, so the folders are column-identical to their per-stock twins and
  these ingests are thin wrappers on the SAME `_ingest_bronze_cafef_daily` with the same
  cast lists and `split_key=True`. PK `(exchange, ticker, date)`. Six indices: `VNINDEX`,
  `VN30INDEX`, `VN100-INDEX` (HOSE), `HNX-INDEX`, `HNX30-INDEX` (HNX), `UPCOM-INDEX`.
  - **⚠️ SEPARATE TABLES ON PURPOSE — NEVER UNION THESE INTO THE STOCK ONES.** `ticker`
    holds an INDEX CODE, not a company. Appended to `cafef_price` the six would surface
    as phantom stocks in `silver.stocks_basic`, pick up NULL GICS classes, and flow into
    every downstream cross-sectional model as if they were tradeable names. An index is a
    different GRAIN, not another ticker. (Verified after the ingest: 0 index-coded rows in
    any of the four stock tables.)
  - **⚠️ The values are ALREADY correctly scaled — do not add a `_mul`.** The scraper
    neutralises its ×1000 because an index level is a POINT, not '000 VND; VNINDEX's first
    row is the base `100.0` on 2000-07-28, HOSE's opening day. Storing ×1000 would give
    1,824,090 for a 1824.09 close — internally consistent, plots fine, wrong by 10³.
  - **⚠️ Three of the four carry holes that are CAFEF'S, and bronze is faithful to them**
    (see `web_scraper/CONTEXT.md §3`): `VN100-INDEX`'s price stops dead at **2025-04-29**;
    `order_stats` is **literally zero-filled** for VN30INDEX/VN100-INDEX and leaves
    `sell_order_vol` 0 on the HNX/UPCOM indices; `prop_trading` is effectively
    **exchange-level** (VN100-INDEX has ONE row) because a prop desk reports per exchange,
    not per index basket. A consumer reading those zeros as data gets a breadth signal made
    of CafeF's padding. `foreign_room_left`/`foreign_own` are always 0 — an index has no
    ownership limit; the columns exist for layout parity.
  - Not read by silver or gold yet.
  - `cafef_insider_shareholder_transactions` — registered vs executed buy/sell by
    insiders, related persons and major shareholders (from the `insider_txn/`
    folder). **Event-based** (no natural date key) → deterministic **md5 `row_id`
    surrogate PK** (hash of the full raw row, so re-ingests are idempotent); five
    date columns overridden to `DATE`, long text columns to `TEXT`. Carries
    `exchange`/`ticker` (split), not a colon `symbol`.
  - `cafef_news` — the company-news / disclosure feed (from the `news/` folder):
    headline, body, `type` (editorial|disclosure|error), `category` (topic), and the
    filing `pdf_url` for disclosures. **Event-based**, and it breaks the shared CafeF
    helpers three ways, so it has its own ingest (`_ingest_bronze_cafef_news`) rather
    than going through `_helper_load_cafef_folder` / `_ingest_bronze_cafef_daily`:
    - **the CSV has no exchange/ticker column** — they exist only in the filename
      (`<EXCHANGE>_<SYMBOL>.csv`), so the key is rebuilt from the path;
    - the key is stored **split as `(exchange, ticker)`**, not folded into the
      `"<EXCHANGE>:<TICKER>"` colon key. That convention exists so the three price
      sources merge uniformly in silver; news has nothing to merge with, and
      `simplize_industry` keys the same way;
    - **`order` is a reserved SQL word** → stored as `news_order`.

    PK = md5 `row_id` of `(exchange, ticker, url)` — **not** of the whole row as the
    insider table does. The URL is the article's identity (the scraper already dedups
    on it), so a re-scrape that fills in a body which was previously a `type=error`
    stub UPDATES the row instead of writing a second copy of the same article.
    `content` reaches ~19 KB → `TEXT` is required, not `VARCHAR`.
- **CafeF financials — 15 tables at full coverage** (`_ingest_bronze_cafef_financials`),
  from `raw_data/cafef/financials/`, which is built OFFLINE by the PDF-reading
  pipeline in `src/web_scraper` (not by a network scraper). **The template is a
  FOLDER, not a column**: Vietnam has four charts of accounts among listed companies
  (bank / corp / securities / insurance) and they share no line items — each has a
  "code 1" and it means something different in each — so their columns must never
  meet in one table. Hence **12 statement tables (4 templates × 3 reports)** + **3
  reference tables**. Only parsed templates get a table; today that is `bank`
  (**VCB + ACB**, 152 rows each as of 2026-07-30), so 3 of the 12 exist.
  ⚠️ **A second ticker on the same template does NOT widen the schema** — ACB and VCB
  produced byte-identical column names *and* order, because both are mapped onto the
  same `schema/bank_<report>.csv` chart of accounts, so adding ACB was a pure upsert.
  A ticker on a *new* template mints new tables instead; it never joins these.
  - `cafef_financials_<template>_<report>` — the figures. PK
    `(exchange, ticker, year, quarter)`; a contiguous quarter grid where an unreadable
    quarter is a **blank `source='missing'` row, never zero-filled** (so the null-drop
    layers gate on the KEY columns only, never the line items). Figures are already
    **absolute VND** — the parser applied the filing's unit — so `unit` is provenance,
    not a scale factor to re-apply.
    - ⚠️ **Share counts** (added 2026-07-20): `shares_authorized` / `shares_issued`
      (published) / `shares_outstanding` are appended to EACH of the 3 statement tables
      as **BIGINT** columns. They are a per-DOCUMENT fact read from the filing's
      **"Vốn cổ phần" note** (`PdfParser.share_capital()`), NOT from any statement — the
      statement parser stops before the notes — so all three statements of a quarter
      carry the SAME value, like `publish_date`. They ride through the ingest as "line
      columns" but are listed in `CAFEF_FINANCIAL_SHARE_COLS` so they cast bigint (whole
      shares), not decimal. VCB Q4-2019 = **3,708,877,448** (all three), coverage 62/78
      quarters. This is the true share count for P/E, P/B — replaces the
      `viii_1_von_dieu_le / 10_000` par-value estimate. ⚠️ Reading them costs OCR of the
      notes pages (the scan runs from the last statement to EOF until it finds the note),
      which roughly DOUBLED the VCB rebuild to ~2.4h; not yet bounded. OCR misreads in a
      handful of quarters were repaired offline (see `web_scraper/CONTEXT.md`), so the
      panel is monotone.
  - `cafef_financial_reports` — the 14 per-DOCUMENT metadata columns (+`report`), split
    off because they describe the filing, not the accounts. ⚠️ **The parser gained a
    `method` column** (the OCR layer that finally read the filing — `onnx@200`,
    `tesseract@200`, `onnx@200+relax+components`, …), which the live table predated; since
    `create_table` is `IF NOT EXISTS` the upsert failed with `column "method" does not
    exist` until the table was **dropped and rebuilt** (2026-07-30). It is regenerated in
    full from the statement CSVs in the same pass, so dropping it loses nothing — do that
    whenever `CAFEF_FINANCIAL_META_COLS` changes. PK
    `(exchange, ticker, report, year, quarter)` — **per report, not per quarter**: the
    three statements of one quarter often come from different documents (36 of VCB's
    78 quarters). (The share counts are ALSO a per-document fact but are kept on the
    statement tables, not here, so a consumer of one statement has them without a join.)
    Home of **`publish_date`**, ⚠️ the column downstream MUST join on:
    it is the day the figures became public, not the period end (VCB's Q4-2025 covers
    the quarter ending 31 Dec 2025 but was published 27 Mar 2026 — joining on the
    period end hands a model twelve weeks of look-ahead every year).
  - `cafef_financial_schema` — the 12 charts of accounts concatenated into ONE
    dictionary. This is the only place the four templates may meet, because here a
    line item is a **row** (a fact about the template), not a column. PK
    `(template, report, line_id)`; maps a column back to the Vietnamese line the
    filing printed (`as_printed`) and to CafeF's item code.
  - `cafef_financial_templates` — `templates.csv`: ticker → **which of the 12 tables
    holds it** + the cash-flow method. Load-bearing, not a convenience.
- **`simplize_stocks`** — the validated daily backbone: adjusted OHLC, true volume,
  net/pct change, foreign vol+val + room. Key kept split; PK `(exchange, ticker, date)`.
- **`simplize_industry`** — per-ticker VN GICS-based industry, loaded as-is;
  PK `(exchange, ticker)`.
- **`gics`** — official MSCI GICS taxonomy CSV, one row per sub-industry;
  PK `sub_industry_code`; `sub_industry_definition` overridden to `TEXT`.

### Silver (`_ingest_silver_*`) — canonical, cross-source merged
- **Simple assets** (`bonds/economy/forex/funds/indices`): select the canonical columns
  from bronze, cast, save. PK `(exchange, ticker, date)`.
  > ✅ **Fixed 2026-08-01.** All five used to re-derive the key with
  > `df["exchange"] = df["symbol"].str.split(":")…` — against a frame that has no
  > `symbol`, because bronze splits it on read. All five raised `KeyError('symbol')`
  > (confirmed empirically on the live tables, not by reading). The two lines are gone;
  > `exchange` and `ticker` come straight out of bronze. See §`symbol` below.
- **`economy` + `economy_series` — a FACT table and its DIMENSION (2026-08-01).**
  - `silver.economy` — LONG, PK `(exchange, ticker, date)`, **579,459 rows = the bronze
    row count exactly, 0 nulls by construction.** Same grain as its four siblings and as
    `_ingest_gold_economy`.
  - `silver.economy_series` — one row per series (1,034), PK `(exchange, ticker)`:
    `country`, `scrape_main_type`, `category`, plus a **derived `frequency`** (500
    monthly, 226 quarterly, 206 annual, 66 daily, 32 weekly, 4 irregular), the
    observation count and the first/last date. TradingView publishes no frequency field —
    it is the median gap between consecutive observations.
  - ⚠️ **The dimensions are NOT columns on the fact table**, and not for tidiness:
    `_ingest_gold_table` coerces every column outside `{exchange, ticker, date, GICS}`
    with `pd.to_numeric`, so carrying `country`/`category` on `silver.economy` would wipe
    all three to NaN in `gold.economy`.
  > **`silver.economy` was WIDE for one afternoon** (one row per date × 1,034 columns)
  > before the shape review moved that panel to gold. It measured **5.8% filled**, but
  > the nulls were the least of it — a NULL costs ~1 bit, so 9.4 M of them was ~1.2 MB.
  > The deciding arguments: a column-per-series table makes the **SCHEMA a function of
  > the DATA** (every new series is a DDL change; it also sat at 65% of PostgreSQL's
  > 1,600-column ceiling), it **mixed frequencies on one calendar** (67 daily series hold
  > 63% of all observations and imposed a 9,719-day grid on 500 monthly and 226 quarterly
  > ones — on their own grids those buckets are 76-93% filled, so the sparsity was an
  > artefact of the shape), and it silently invited **look-ahead bias** (see below).
- **`stock_market` — the four CafeF INDEX tabs joined into ONE table (2026-08-01).**
  `bronze.cafef_index_{price,order_stats,foreign,prop_trading}` →
  `silver.stock_market`, PK `(exchange, ticker, date)`. **25,935 index-days × 30
  columns**, 6 indices, 2000-07-28 → 2026-07-30. The four tabs are four MEASURES of the
  same entity (index × day), split across tables only because the scraper writes one
  folder per CafeF tab; no measure name collides across them, so the merge needs no
  suffixes.
  - ⚠️ **`ticker` IS AN INDEX CODE, NOT A COMPANY** — `VNINDEX`, `VN30INDEX`,
    `VN100-INDEX`, `HNX-INDEX`, `HNX30-INDEX`, `UPCOM-INDEX`. Never union this into
    `stocks_basic`; that separation is the whole reason it is its own table.
  - ⚠️ **OUTER join, a deliberate divergence from `stocks_basic`'s left-join-on-price.**
    Measured: the key union is **25,935** against price's **24,962**, so a left join
    would silently drop **973 index-days** that have data in another tab but no price
    row — 930 with order stats, 539 with foreign flow, 6 with prop trading, and 842 of
    them VN100-INDEX (2014-04 → 2026-07). For six indices, discarding a thousand days of
    real observations to keep a convention is the wrong trade.
  - The tabs have very different histories — price from 2000-07, foreign 2007-01, order
    stats 2007-11, prop trading only 2022-11 — so **NULL in a measure means "that tab has
    no record for this index-day", never zero**. Coverage: 24,962 price / 22,863 order
    stats / 20,547 foreign / 1,494 prop, each exactly its bronze row count.
  - An invariant check **raises** if the join's row count differs from the union of the
    four key sets, which is how a duplicate key in any input would otherwise fan out
    silently.
  - **Verified**: all four sources matched row-for-row, **532,188 cells compared, 0
    mismatches**.
- **Per-source CafeF carry-ups** (`_ingest_silver_cafef_*`, added 2026-07-18) — a
  source-named lift of the bronze CafeF tables into silver, one-to-one, NOT the
  canonical asset merge. Each **selects the bronze table, applies a basic clean pass
  only** (drop rows null on the key or all columns, order by key), **drops its old
  silver table first** (so a schema change re-materialises past the driver's
  `IF NOT EXISTS`), and saves under the SAME name:
  - `silver.cafef_price` / `cafef_order_stats` / `cafef_foreign` / `cafef_prop_trading`
    — daily, PK `(exchange, ticker, date)`. The three latter share the
    `_ingest_silver_cafef_daily(table_name)` helper; `cafef_price` predates it.
  - `silver.cafef_insider_shareholder_transactions` — EVENT-based, keeps bronze's md5
    `row_id` PK (no date key); the five date columns + long free-text columns keep
    their bronze type overrides (a default `VARCHAR(255)` would truncate
    `note`/`profile_url`).
  - ⚠️ **These are basic-clean-ONLY — no cast.** `_helper_select` reads bronze's
    `numeric` columns back as `Decimal` → pandas `object`, and the save path then
    infers **`VARCHAR`** for them (only `bigint` columns stay numeric). So the decimal
    price/value columns land as TEXT in silver — values correct and aligned, type
    degraded. Add a `_helper_cast_columns` call before save (as bronze does) to fix.
  - Wired into `ingest_silver_data` under the silver **`cafef_carry_ups`** leaf (was the
    shared `stocks` leaf until 2026-07-30), ahead of `_ingest_silver_stocks_basic`.
    Verified: each silver table row count == its current bronze count.
- **`_ingest_silver_gics`** (added 2026-07-19) — the same basic-clean carry-up
  pattern applied to the bronze `gics` reference table (not a CafeF source, so it
  has its own method, not `_helper_load_cafef_folder`/`_ingest_silver_cafef_daily`).
  Select bronze `gics` → drop rows null on `sub_industry_code` → drop-all-null →
  order by key → drop the old silver table → save. PK stays `sub_industry_code`
  (not `(exchange, ticker, date)` — this table is one row per GICS sub-industry,
  not per ticker); `sub_industry_definition` keeps its bronze `TEXT` override.
  Wired into `ingest_silver_data` under its own new switch leaf,
  `data_preprocessor/data_quality_silver/gics` (sits alongside `bonds`/`economy`/
  `forex`/`funds`/`indices`/`stocks_basic`, independent of the `cafef_carry_ups` leaf
  that gates the CafeF carry-ups above).
- **CafeF financials → silver** (added 2026-07-19), gated by a new
  `data_preprocessor/data_quality_silver/financials` switch leaf. Two steps:
  - **`_ingest_silver_cafef_financials`** — carry every bronze
    `cafef_financials_<template>_<report>` STATEMENT table up to silver one-to-one
    (same name, PK `(exchange, ticker, year, quarter)`). Discovered from
    `information_schema` by the `cafef_financials_` prefix (trailing `s`), which
    **excludes** the 3 metadata tables (`cafef_financial_reports`/`_schema`/`_templates`
    — they describe the filings / chart of accounts, not the figures, so they are NOT
    carried). Basic clean + cast, with the financials rule: **null-drop gates on the
    KEY columns only, never the line items**, and `REMOVE_IF_ALL_COLUMNS_ARE_NULL` is
    omitted — a `source='missing'` quarter is a legitimate blank-but-keyed row that
    must survive. Today: `bank` × {balance_sheet, income_statement, cash_flow} = 3
    tables (100 / 36 / 57 cols — incl. the 3 share columns — 78 rows each).
  - **`_ingest_silver_cafef_financials_template(template)`** (+ thin
    `_ingest_silver_cafef_financials_bank` wrapper) — combine ONE template's three
    per-report silver tables into one wide **`cafef_financials_<template>`**,
    **OUTER-joined on `(exchange, ticker, year, quarter)`**. Every NON-KEY column is
    **prefixed by its report** (`balance_sheet_…` / `income_statement_…` /
    `cash_flow_…`) so line items never collide and provenance is explicit — including
    the shared meta (`*_template` / `*_period` / `*_source`), since a quarter can be
    `source='missing'` in one statement but present in another. Plus a single
    **unprefixed `publish_date`** column joined from `bronze.cafef_financial_reports`:
    the 3 reports of a quarter publish on the same day (verified: 0 of the 72 dated
    quarters disagree), so one column suffices — collapsed per key via `max()`
    (== the shared value; NULLs from a missing report drop out), placed right after the
    keys, `DATE`-typed. ⚠️ `publish_date` is the day the figures became **public**, NOT
    the period end — join fundamentals→prices on it, never on the quarter end (avoids
    ~12 weeks of look-ahead/year). The **share counts** (`shares_authorized` /
    `shares_issued` / `shares_outstanding`) get the same treatment as `publish_date`: a
    per-document fact identical across the 3 statements, so they are NOT report-prefixed
    (that would mint 9 duplicate columns) — they are dropped from each report's prefixing,
    re-attached ONCE unprefixed (from the balance_sheet statement) right after
    `publish_date`, and cast BIGINT. `silver.cafef_financials_bank`: **180 cols** (4 keys
    + publish_date + 3 share cols + 93 `balance_sheet_*` + 29 `income_statement_*`
    + 50 `cash_flow_*`), 78 rows, publish_date on 72/78, shares on 62/78. The join is 1:1
    in practice (all 3 share the same contiguous quarter grid). Generic by `template`, so
    `corp`/`securities`/`insurance` combine the same way once parsed.
- **`silver.stocks_basic_financials_bank`** (added 2026-07-21,
  `_ingest_silver_stocks_basic_financials_bank`) — the **daily** `silver.stocks_basic`
  panel joined to the **quarterly** `silver.cafef_financials_bank`, keeping **ALL columns
  of both** (a straight join — **no computed indicators**). Gated by its own new switch
  leaf `data_preprocessor/data_quality_silver/stocks_financials`, wired into
  `ingest_silver_data` AFTER `_ingest_silver_stocks_basic` (it reads both silver tables).
  - ⚠️ **The join is an as-of merge on `publish_date`, NOT the period end**
    (`pandas.merge_asof`, `direction="backward"`, `by=(exchange, ticker)`,
    `left_on=date` ↔ `right_on=publish_date`): each price day carries the
    most-recently-*published* quarter's figures, so every financials column **steps** on
    its `publish_date` and holds flat until the next filing drops — **zero look-ahead**
    (verified: 0 rows where `publish_date > date`). Financials rows with a NULL
    `publish_date` (the 6 earliest un-dated quarters) are **excluded from the as-of key** —
    a fact with no public date can't be pinned to a day.
  - **INNER-scoped**, two ways: (1) the price side is inner-joined to the set of tickers
    that have financials, so it does NOT carry 620 tickers of NULL fundamentals; (2)
    price days *before* a ticker's first `publish_date` are dropped (no quarter was public
    yet). → the table is **dense**, and grows automatically as more bank tickers are parsed.
    Today: **HOSE:VCB only → 4,235 rows** (2009-06-30 … 2026-06-25).
  - The two tables **share no non-key column name**, so the merge needs **no suffixes**:
    every row is a `stocks_basic` day (38 price/GICS/flow cols) + the as-of quarter's 177
    financials cols (`cafef_financials_bank`'s own `exchange`/`ticker` fold into the join
    keys). **216 cols** total = 38 + 180 − 2 shared keys; layout = `(exchange, ticker,
    date, publish_date)` then the stocks_basic block then the financials block.
    PK `(exchange, ticker, date)` (unique, verified).
  - **Numeric types are rebuilt from each source's live `information_schema`** via the new
    `_helper_column_types(schema, table)` helper (a column is BIGINT iff bigint in its
    source, else `numeric`→Float64; keys/date/publish_date/text pass through). This avoids
    the degraded-VARCHAR trap the skip-the-cast carry-ups fall into (the driver reads
    `numeric` back as `Decimal`→pandas `object`). ⚠️ Note `driver.select` still returns the
    stored `numeric` columns as Python `Decimal` (pandas `object` dtype) on read-back — the
    STORAGE is correct (178 numeric / 17 bigint / 19 varchar / 2 date), only the round-trip
    dtype looks like object. Old table is dropped first (schema change re-materialises past
    the driver's IF NOT EXISTS).
  - **This is the raw-columns base**; the computed **fundamental indicators** live one
    step downstream in `silver.stocks_basic_financials_bank_fa` (next bullet), which reads
    THIS table and appends the ratio catalog. (Both realise the as-of mechanic the PLANNED
    `stocks_fundamental` describes — this one the raw join, the `_fa` one the ratios.)
- **`silver.stocks_basic_financials_bank_fa`** (added 2026-07-21,
  `_ingest_silver_stocks_basic_financials_bank_fa`) — `stocks_basic_financials_bank` (the
  bullet above) with the **full fundamental indicator catalog** (FUNDAMENTAL_INDICATORS.md
  §1) appended: it keeps **ALL 216 source columns PLUS 26 indicators = 242 cols**, same
  4,235 VCB rows, PK `(exchange, ticker, date)`. Wired into `ingest_silver_data` under the
  SAME `stocks_financials` leaf, run immediately AFTER the plain-join step (it reads the
  table that step just wrote). Old table dropped first.
  - **The as-of merge is already baked into the source** (every day carries its
    most-recently-published quarter, `publish_date ≤ date`), so the `_fa` step does **no
    re-join** — it just computes ratios and preserves the source's **zero look-ahead**
    (verified: 0 rows `publish_date > date`; indicators are constant within a publish
    window — 0 of 55 windows show >1 distinct `eps_ttm`).
  - **Two-grain compute** (`_helper_build_bank_fundamental_indicators`): the
    price-INDEPENDENT indicators are computed once on the **quarterly grain** — the distinct
    `(exchange, ticker, year, quarter)` rows of the source (69 for VCB; `(year,quarter)` ↔
    `publish_date` is 1:1), the only grain where **trailing-4-quarter TTM sums** and
    **year-ago-balance averages** are well defined — then mapped back onto every day of the
    window by `(exchange, ticker, year, quarter)`. The price-DEPENDENT valuation ratios are
    then computed **row-wise** from `close_adjust` × those as-of fundamentals.
  - **The 26 indicators** (constants `BANK_FA_QUARTERLY_COLS` + `BANK_FA_VALUATION_COLS`):
    21 quarterly — `shares_used`, `ttm_net_income`, `ttm_op_income`, `eps_ttm`, `bvps`,
    `roe`, `roa`, `nim`, `net_profit_margin`, `pretax_margin`, `effective_tax_rate`,
    `cost_to_income`, `equity_multiplier`, `equity_to_assets`, `ldr`, `loans_to_assets`,
    `deposits_to_assets`, + `{earnings,opincome,equity,asset}_growth_yoy` — and 5 valuation:
    `market_cap`, `pe_ttm`, `pb`, `ps_ttm`, `earnings_yield`. Formulas map 1:1 to
    FUNDAMENTAL_INDICATORS.md §1.
  - **Shares**: prefer scanned `shares_outstanding` → published `shares_issued` →
    par-value estimate `balance_sheet_viii_1_a_von_dieu_le / 10_000` (`VN_PAR_VALUE`).
    **TTM requires all 4 quarters** (a gap makes the window NULL, never wrong);
    ROE/ROA/NIM average this-quarter and year-ago balances; ±inf from a zero denominator
    (early sparse quarters) → NaN. **Coverage** (VCB, /4,235 days): `pb`/`bvps`/`market_cap`
    full (need only current equity+shares); `pe_ttm`/`eps_ttm` 3,383, `roe` 3,362 (need the
    4-quarter TTM / year-ago balance); `nim` 4,025, `ldr` 3,910. P/S, CIR and the margins
    stay thin — `tong_thu_nhap_hoat_dong` is only 25/78 quarters. **Verified latest VCB
    (2026-06-25): P/E 14.13, P/B 2.56, ROE 22.2%, NIM 2.69%, mkt cap 508 T₫, 8.36 bn shares.**
  - **Types**: source numeric cols re-cast from the source's live `information_schema` (via
    `_helper_column_types`), every computed indicator → nullable `Float64` (NaN → SQL NULL).
    Bank-template only; `corp`/`securities`/`insurance` would each get an analogous
    `…_<template>_fa` once parsed.
- **PLANNED: `silver.stocks_fundamental`** — ⚠️ **the `bank` slice of this is now BUILT as
  `silver.stocks_basic_financials_bank_fa`** (two bullets up); this remaining PLANNED entry
  is the *cross-template / universal* generalization (one table spanning all templates, or
  the per-template `…_<template>_fa` set once `corp`/`securities`/`insurance` parse). The
  fundamental-analysis
  indicators (P/E, P/B, ROE, ROA, EPS, market cap, leverage, growth, + bank NIM/CIR/LDR)
  computed on a **daily** panel = `stocks_basic` joined to `cafef_financials_<template>`.
  ⚠️ The join is an **as-of merge on `publish_date`** (`merge_asof` backward, by
  `(exchange, ticker)`): each price day gets the most-recently-*published* quarter, so
  a ratio steps on its `publish_date` and holds flat — zero look-ahead, never joined on
  the period end. **Shares outstanding is now stored** (added 2026-07-20): the
  `shares_issued`/`shares_outstanding` columns on the financials tables are the TRUE
  count read from the "Vốn cổ phần" note (see the bronze/silver financials sections
  above), so P/E and P/B use the real figure — no longer the
  `viii_1_a_von_dieu_le / 10_000` par-value estimate (which the earlier plan used;
  cross-checked = VCB's ~8.36 bn shares, consistent with the scanned count). Coverage is
  62/78 VCB quarters, so keep the par-value derivation as a fallback where the scanned
  count is null. Full indicator catalog — every ratio computable TODAY with its formula
  (mapped to our report-prefixed line ids), per-quarter coverage, the reliable
  high-coverage subset vs the op-income-limited ones (`tong_thu_nhap_hoat_dong` is only
  25/78, so P/S, CIR and the bank margins are thin), the as-of build sketch, and the open
  decisions live in [FUNDAMENTAL_INDICATORS.md](FUNDAMENTAL_INDICATORS.md) (refreshed
  2026-07-20 for the stored share count).
- **`_ingest_silver_stocks_basic`** → writes **`silver.stocks_basic`** (renamed from
  `silver.stocks` on 2026-07-19). **REWRITTEN 2026-07-19: a CafeF-only four-way join,
  no longer the Simplize-primary canonical spine.** `bronze.cafef_price` is the base
  (spine), LEFT-joined to the other three daily CafeF tables on the FULL
  `(exchange, ticker, date)` key:

  ```
  cafef_price
    LEFT JOIN cafef_order_stats  ON (exchange, ticker, date)
    LEFT JOIN cafef_foreign      ON (exchange, ticker, date)
    LEFT JOIN cafef_prop_trading ON (exchange, ticker, date)
  ```
  - All four are already keyed `(exchange, ticker, date)` in bronze and **share no
    non-key column name**, so it is a clean left-merge with **no suffixes**: every
    output row is a `cafef_price` day, with the other sources' columns filled where
    they have that day and NULL where they don't (their history is shorter —
    `foreign_own` from 2012, order_stats from 2010, **prop_trading from 2023**, so old
    days carry NULL prop columns).
  - **Join on the FULL key, not `(ticker, date)`** — a ticker can list on more than
    one exchange, and dropping `exchange` would fan the base rows out. Verified: output
    row count == base `cafef_price` count exactly (2,388,368), no fan-out.
  - **Basic clean + cast**, then **drop the old silver table first** (so a schema
    change re-materialises past the driver's `IF NOT EXISTS`), then save. **Unlike the
    per-source carry-ups above it DOES cast**, so its columns land correctly typed
    (`numeric` for prices/values, `bigint` for volumes/counts) — not the degraded
    VARCHAR those skip-the-cast ones get.
  - **Attaches the full GICS tree** (added 2026-07-19): after the cast it left-joins
    `_helper_build_gics_classification()` (bronze `simplize_industry` × `gics`, constant
    per ticker) on `(exchange, ticker)` and places the 8 `GICS_CLASS_COLS` right after
    the keys. `sector` is populated on ~99.7% of rows (the rest are tickers outside the
    `SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY` crosswalk). GICS columns store as VARCHAR.
  - **38 columns** = 3 keys + 8 GICS class cols + 27 non-key columns of the four CafeF
    tables. **No price/volume/foreign source fallback, no Simplize/TradingView** — the
    old canonical merge kept only its GICS-attach step, not its source-priority logic.
  - ⚠️ **Name divergence silver→gold:** the gold stocks table is still `gold.stocks`
    (unchanged); `_ingest_gold_stocks` reads `silver.stocks_basic` via
    `_ingest_gold_table(table_name="stocks", silver_table_name="stocks_basic")`.

### Gold (`_ingest_gold_*`) — feature engineering
- **`stock_market` — the WIDE index panel (2026-08-01).** `silver.stock_market` →
  `gold.stock_market`: **one row per TRADING DAY** (PK `date`), one column per
  index × measure named `{exchange}__{ticker}__{measure}` —
  `hose__vnindex__close_adjust`, `hnx__hnx_index__n_buy_orders`. **6,339 days ×
  162 columns** (6 indices × 27 measures), 2000-07-28 → 2026-07-30.
  - ⚠️ **THE TICKERS CONTAIN HYPHENS AND POSTGRESQL CANNOT.** `HNX-INDEX`,
    `VN100-INDEX`, `HNX30-INDEX`, `UPCOM-INDEX` are real index codes, but `hnx-index`
    unquoted parses as `hnx MINUS index` — and `_helper_build_upsert_sql` interpolates
    column names **unquoted**. Hyphens become underscores, and the result is checked for
    collisions: sanitising two indices into one column name would merge them silently.
    Verified distinct: `hnx_index`, `hnx30_index`, `upcom_index`, `vn100_index`,
    `vn30index`, `vnindex`.
  - ⚠️ **NO as-of fill, unlike `gold.economy`** — and the difference is the source, not
    a preference. Macro series are published on a lag and are stale-but-valid between
    releases. An index either traded that day or it did not: a gap means VN100-INDEX did
    not exist yet (it starts 2014-02) or that tab has no record, so filling it would
    invent prices for days the market was shut. NULL stays NULL, which is why the panel
    is 34-71% filled per index — each index simply starts on a different day.
  - **DECIMAL, not REAL.** `gold.economy` is REAL because 1,034 float8 columns would
    exceed PostgreSQL's ~8 kB row limit; at 162 columns there is no such pressure, and
    `value_matched` reaches ~1e12 where REAL's ~7 significant digits lose thousands.
    So this panel round-trips EXACTLY.
  - The calendar is the distinct dates in silver, not a synthetic `bdate_range` —
    Vietnamese exchange holidays are not weekends.
  - An invariant check **raises** if the pivot's non-null cell count differs from the
    observation count going in.
  - **Verified**: 532,188 observations compared, **0 missing, 0 value mismatches**, and
    gold holds exactly 532,188 cells. Max column name 42 bytes.

- **`economy` — the WIDE macro panel (2026-08-01).** `silver.economy` +
  `silver.economy_series` → `gold.economy`: **one row per BUSINESS DAY** (PK
  `date`), one column per series named
  `{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`. 6,935 days ×
  1,034 series, **88.6% filled** (the long form is 5.8% of a date × series grid).
  It lives in GOLD because every step that makes it usable is a modelling decision.
  ⚠️ **It is the ONLY gold economy table.** `gold.economy` used to be the generic
  `_ingest_gold_table("economy")` output — the LONG grain with per-series TA features
  (579,459 × 16: returns, volatility, rolling stats) — with the panel beside it as
  `economy_panel`. Two gold tables for one asset is one too many, so the panel took the
  name and the feature table was dropped (2026-08-01). Restoring it is one line
  (`self._ingest_gold_table("economy")`); the generic builder is untouched and still
  drives bonds/forex/funds/indices/stocks.
  - ⚠️ **PUBLICATION LAG — this is the look-ahead guard.** The source `date` is the
    REFERENCE period, not the release date: Vietnam's Q1 GDP is dated 2026-03-31 and
    published in April, so a panel joined on `date` hands a model a figure ~a week
    before it existed. Each observation becomes visible at
    `date + ECONOMY_PUBLICATION_LAG_DAYS[frequency]`. **Verified**: VNGDPYY's Q1-2026
    value (7.83) first appears on 2026-05-15 = ref + 45d, and the panel still shows the
    Q4-2025 figure (8.46) up to 2026-05-14.
  - ⚠️ **ROLL FORWARD TO THE NEXT BUSINESS DAY, and this one bites.** 2025-12-31 + 45
    days is **Saturday** 2026-02-14; the panel is indexed by business day, so a plain
    `reindex` DROPPED that observation outright — the series jumped Q3 → Q1 and Vietnam's
    Q4-2025 GDP vanished. Availability dates are now rolled forward, and an invariant
    check **raises** if the reindex loses any observation.
  - **As-of carry**, bounded by `ECONOMY_MAX_STALENESS_BDAYS[frequency]`, so a series
    that stopped reporting (e.g. JPLTUR, last observation 2014) is NULL today rather than
    carrying a 12-year-old value forever.
  - ⚠️ **The calendar ENDS TODAY.** 47 series carry projections dated to 2036; on a
    business-day calendar those were 2,685 rows at 2.3% filled whose only real effect was
    to make a look-ahead join possible. They stay in bronze/silver as raw data (132
    observations excluded from the panel).
  - ⚠️ **Both lag tables are ASSUMPTIONS, not data** — TradingView gives no release
    dates. Tighten them if release dates are ever scraped.
  - ⚠️ **`REAL`, not `DOUBLE PRECISION`**: 1,034 float8 columns is 8,272 bytes against
    PostgreSQL's ~8,160-byte row limit; float4 halves it to ~4.1 kB. Measured cost:
    **max relative error 1.16e-7** (float32 epsilon) over 565,171 cells, 0 above 1e-6.
  - **Verified**: 565,171 observations in range, **0 missing from the panel**, 0
    relative-error outliers, staleness confirmed on a dead series.

- All routed through **`_ingest_gold_table`**: read the silver table, coerce numeric
  columns to float (GICS class columns passed through untouched), apply
  `_helper_build_feature_layers` (returns / intraday range / return-vol / rolling
  stats — chosen by OHLC vs single `value`) **plus** any table-specific TA layers,
  then **checkpoint-save** in 100k-row chunks via `COPY` (`use_copy=True`).
- **All float columns are stored as `REAL`** (4-byte) in gold to stay under
  PostgreSQL's 8160-byte row limit given the very wide TA feature set.
- **`_ingest_gold_stocks`** adds the full **TA-Lib battery** (~40 indicators:
  overlap studies, momentum, volume, cycle, price-transform, volatility) + three
  microstructure features (foreign buy pressure, foreign net-val ratio, negotiated
  vol ratio). TA functions come from `ta.ta_functions` and are mapped by
  `_build_transform_func_map()` (module-level so it survives process-pool re-import).

## 5. How it's driven — SwitchHandler + `src/switch_config.json`

**HOW TO RUN ANYTHING HERE: edit two lines of `src/switch_config.json`, then
`python src\main.py` from the repo root.** There is no per-module runner and no CLI
flag; `main.py` calls all three entry points unconditionally and every ingest inside
them is switch-gated, so the config IS the run plan. Truncate `logs/app.log` first —
that log is the only record of what actually ran.

```jsonc
// re-ingest ONLY the CafeF financial statements (~1 s), touching nothing else
"data_preprocessor/data_quality_bronze": true,
"data_preprocessor/data_quality_bronze/cafef_financials": true,
```

- `SwitchHandler` reads a **flat JSON of slash-path → bool**; a path is enabled only
  when **every ancestor is explicitly true**, so a master flag off disables its whole
  subtree without touching the leaves. The three entry points each gate on
  `data_preprocessor/data_quality_{bronze|silver|gold}` and then on a per-table leaf.
  Keys beginning `//` are comments and are ignored — the heavy leaves carry one giving
  their cost, so you can read the price of a run before starting it.
- ⚠️ **The config is read as `utf-8-sig`, and a branch key must NOT end in `/`.** Two
  ways this file silently does nothing, both hit in practice:
  - a **BOM** (PowerShell 5.1's `Out-File -Encoding utf8` writes one, as do several
    Windows editors) used to throw inside `_load_config`, which swallows the error and
    returns `{}` — *every* switch false, `main.py` running to completion doing nothing,
    one ERROR line in the log as the only clue. Fixed 2026-07-30 by reading `utf-8-sig`;
    a malformed-JSON typo still fails the same quiet way, so check that log line
    (`Switch config loaded: N switches (M enabled)`) when a run does nothing.
  - `"web_scraper/cafef/"` (trailing slash) never matches, because `is_enabled` looks
    up the prefix `web_scraper/cafef`. That typo made the whole CafeF + Simplize
    scraper branch unreachable until 2026-07-30.
- **⚠️ BRONZE IS ONE LEAF PER SOURCE TABLE** (since 2026-07-30) — `trading_view_stocks`,
  `cafef_{price,foreign,order_stats,prop_trading,insider_txn,news,financials}`,
  `cafef_index_{price,foreign,order_stats,prop_trading}`,
  `simplize_{stocks,industry}`, `gics`. The **single `.../bronze/stocks` leaf is GONE**:
  it fired all ten ingests, so re-reading the financials CSVs (~1 s) also meant
  re-reading 2.4 M CafeF price rows and 2.7 M Simplize rows. Bronze has **no
  cross-table dependency** — each ingest reads its own `raw_data/` folder and writes its
  own table — so **any subset is a valid run**; the order in `bronze_ingests` is
  convention only (universe → daily → event → reference). All ship `false`: opt in.
- **Silver leaves** are `bonds`/`economy`/`forex`/`funds`/`indices`/`gics`/
  `cafef_carry_ups`/`stocks_basic`/`financials`/`stocks_financials`/`news_sentiment`.
  The old `.../silver/stocks` leaf was **split in two** on 2026-07-30: `cafef_carry_ups`
  (the five one-to-one bronze→silver lifts) and `stocks_basic` (the four-way join).
  They are not each other's inputs — `stocks_basic` joins the **bronze** tables directly —
  so rebuilding the 2.4 M-row panel to refresh a carry-up was pure cost.
- `gics` is a **bronze + silver** leaf (`.../bronze/gics`, `.../silver/gics`) — the
  silver copy is a straight reference-table carry-up; there is still no `gics` gold
  table. `bronze.gics` also feeds `silver.stocks_basic`'s GICS tree (via
  `_helper_build_gics_classification`) regardless of the silver `gics` leaf.
- The silver-only leaf `.../silver/stocks_financials` (added 2026-07-21) gates **two
  chained ingests in order**: `_ingest_silver_stocks_basic_financials_bank` (the raw
  price×financials as-of join → `stocks_basic_financials_bank`) then
  `_ingest_silver_stocks_basic_financials_bank_fa` (that table + the indicator catalog →
  `stocks_basic_financials_bank_fa`). It reads `silver.stocks_basic` +
  `silver.cafef_financials_bank`, so it runs after both — but note `stocks_basic` is
  itself under the separate `.../silver/stocks_basic` leaf, so if you flip
  `stocks_financials` on while that is off it reuses whatever is already materialised.
- **Order matters:** silver reads bronze tables; gold reads silver tables. Run the
  layers in bronze → silver → gold order (main.py does). Within bronze, order is free.

## 6. Shared infra it depends on (outside this dir)

- `src/utils/constants.py` — `*_RAW_DATA_DIR` paths, schema names
  (`BRONZE_SCHEMA`/`SILVER_SCHEMA`/`GOLD_SCHEMA`), `DATABASE_MAIN_V2`,
  `SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY`, `THREAD_MANAGER_POWER`.
- `src/utils/enums.py` — `DataQuality`, `CleanAction`/`CleanLayer`,
  `TransformAction`/`TransformLayer`, SQL enums.
- `src/ta/ta_functions.py` — every `add_*` TA / feature function used in gold.
- `src/thread_manager` + `dtos/thread_manager_dtos/task.py` — the `ThreadManager`
  (constructed but the heavy fan-out actually uses a local `ThreadPoolExecutor`).
- Connection env vars: `POSTGRES_HOST/USER/PASSWORD/PORT` and
  `{BRONZE,SILVER,GOLD}_POSTGRES_DATABASE` (loaded via `.env` / `load_dotenv`).
- `src/logger/logger.py` — `Logger` → `logs/app.log` (truncate before a run per
  memory `feedback-clean-app-log-before-run`).

## 7. Gotchas

- **Bronze keys are SPLIT `(exchange, ticker)` — the colon `symbol` is gone** (as of
  2026-07-16; see §4 Bronze). Every daily/price table is PK `(exchange, ticker, date)`;
  event tables carry `exchange`/`ticker` beside their surrogate key. TradingView is
  split from its colon `symbol` via `_helper_split_symbol_column`; CafeF/Simplize keep
  the raw CSV's already-split columns. (Historically bronze used the colon key and
  silver split it — that is now done one stage earlier, in bronze.)
- **Not every `raw_data/` folder reaches bronze.** As of 2026-07-14 the only gap left
  is `cafef/pdfs/` (the 6.7 GB filing archive + its per-ticker `index/` CSVs) — the
  binaries the financials pipeline reads; nothing ingests them. `trading_view/links/`
  + `collected_links/` are *by design* not ingested: they are the ticker universe the
  scrapers read, not data.
- **⚠️ THE DRIVER DOES NOT QUOTE IDENTIFIERS**, and 133 of the 753 financial line
  items are named with the flat ARABIC numbering the filing prints
  (`1_thu_nhap_lai…`) — which PostgreSQL accepts only as a *quoted* identifier. So
  those columns take an **`n` prefix** on ingest (`n1_thu_nhap_lai…`) and only those;
  every other name is left exactly as the parser produced it. The mapping is injective
  and recorded in `cafef_financial_schema` as `sql_column` beside the untouched
  `line_id`, so the printed line is always recoverable — join on `sql_column`, not
  `line_id`, when going from a statement column to its meaning. The alternative (make
  `postgre_sql_driver` quote every identifier) is the *correct* fix but touches
  `create_table` / `upsert` / `COPY` / `select` for every table in all three schemas.
- **The financial statements do not perfectly reconcile, and that is the SOURCE**, not
  the ingest: VCB's Q1-2020 balance sheet is out by 5,000,000 VND on 1,144 *trillion*
  (5 units in the filing's "triệu VNĐ" denomination — 0.0000004%). The same gap is in
  the raw CSV. Bronze is faithful to disk; don't "fix" it here.
- **`cafef_news` ordering:** its `timestamp` is often **date-only** (midnight), so
  same-day articles cannot be separated by it — order by
  `(exchange, ticker, timestamp, news_order)` for a deterministic chronology. And
  note `news_order` is numbered from the article's own `datePublished`, not from
  CafeF's listing order (see `web_scraper/CONTEXT.md §3`).
- **CafeF is one bronze table per scraper folder** (`cafef_price`, `cafef_foreign`,
  `cafef_order_stats`, `cafef_prop_trading`, `cafef_insider_shareholder_transactions`
  ← from the `insider_txn/` folder, `cafef_news`) — the folder/column names are the contract, so
  renaming them upstream breaks the ingest (mirrors the note in
  `web_scraper/CONTEXT.md §7`). **As of the 2026-07-19 rewrite, `silver.stocks_basic`
  IS the four-way join of the daily CafeF tables** (`cafef_price` base LEFT JOIN
  order_stats/foreign/prop_trading on `(exchange, ticker, date)` — 38 cols incl. the
  GICS tree, base row count preserved; see §4 Silver). So `order_stats` / `prop_trading`
  are now consumed by silver; only **insider-shareholder txns / news** remain
  bronze-only (event-based, do **not** 1:1-join onto a daily row — wiring them into a
  signal is future work). Coverage of the appended CafeF columns tapers with source
  history (foreign_own from 2012, order_stats from 2010, prop_trading from 2023→ so old
  days carry NULLs there). (The current join is CafeF-only; the earlier prototype that
  put **Simplize** as the backbone and appended CafeF's unique columns — 33 cols, or 41
  with the GICS tree — is NOT what shipped. `silver.stocks_basic` carries the GICS tree
  but no Simplize / TradingView.)
- **Source-quality ranking (research finding — Simplize > CafeF for the daily panel).**
  ⚠️ Note the *current* `silver.stocks_basic` is CafeF-only and does NOT use Simplize (see the
  2026-07-19 rewrite above); this bullet is the validation behind treating Simplize as
  the backbone whenever the canonical merge is rebuilt, plus memory
  `project-bronze-source-per-field`. TV is only ever an OHLC fallback; its volume/sector
  are never trusted. Re-validated across the whole **VN30** (2026-07-09, on this bronze): vs CafeF,
  Simplize wins on precision (CafeF rounds price to 10-VND ticks, volume to ~100
  shares), history depth, and adjustment — **CafeF `close_adjust` is split/stock-div
  adjusted but under-accounts for CASH dividends** (adjusted-price levels diverge up
  to ~38% in deep history for high-payout names like MWG/ACB), while Simplize is
  fully total-return adjusted. Both anchor to the same recent traded price, and
  **daily returns agree on 99.8% of days** — so for returns the two are
  interchangeable; the gap is a price-*level* offset (never splice the two into one
  series). Foreign volume: Simplize folds in **block/put-through** trades, CafeF's
  foreign tab is matched-market only. → Simplize backbone is correct; CafeF's real
  value is its unique columns (`close_raw`, matched/negotiated split, `foreign_own`).
- **Gold uses `REAL`, not `DOUBLE`** — the writer sanitizes out-of-range/subnormal
  floats to avoid PostgreSQL REAL rejections; the 8160-byte row limit is the reason
  the stocks TA panel must stay `REAL`.
- **Gold `COPY` path assumes a fresh/empty table** (plain insert, no conflict
  handling). Re-running gold on an existing table will duplicate/conflict — drop the
  gold table first if re-ingesting.
- **`_helper_connect_to_database` (per-quality DBs) is not the live path** — the
  three entry points connect to `"postgres"` and `create_database(DATABASE_MAIN_V2)`,
  putting all three schemas in **one** database rather than separate bronze/silver/gold
  databases. The per-quality helper + `{BRONZE,SILVER,GOLD}_POSTGRES_DATABASE` env
  vars are vestigial for the current single-DB layout.
- **TA compute is only ~12% of gold wall-time** (the DB write dominates), so
  `_helper_transform` stays a simple sequential per-ticker loop — don't parallelize
  it without cause. FeatureSelector cost note: memory `project-feature-selection-ta-cost`.

## 8. Current materialized state (snapshot — 2026-07-16, financials refreshed 2026-07-30)

> Row counts below are unchanged by the 2026-07-16 key split — that reshape moved the
> `symbol` colon key to split `(exchange, ticker)` columns without adding or dropping
> a single row (verified: every table re-ingested to the same count).
>
> **2026-07-30:** the 4 financials tables were re-ingested from
> `raw_data/cafef/financials/statements` and now carry **ACB beside VCB** (152 rows per
> statement table, 456 reports). Verified cell-by-cell against the raw CSVs — 152 rows ×
> 89/28/39 numeric columns, **0 mismatches**, 0 duplicate keys, `source='missing'`
> blank-but-keyed rows intact. Every other bronze table is untouched by that run.
>
> **2026-07-30, later:** the four **`cafef_index_*`** tables were added (69,866 rows over
> the 6 market indices). Verified against the raw CSVs — row counts equal, **0 mismatches**
> across every numeric column, 0 duplicate keys, index levels confirmed unscaled (VNINDEX
> opens at the base 100.0 on HOSE's opening day), and **0 index-coded rows in any of the
> four per-stock tables**.

`bronze_schema` in `database_main_v2` — **25 tables** (counts below re-read live from
`information_schema` on 2026-07-30; 34 once all four financials templates are parsed):

| Table | Rows | Notes |
|---|---:|---|
| `trading_view_bonds` | 66,100 | |
| `trading_view_economy` | 579,459 | |
| `trading_view_forex` | 1,324,940 | |
| `trading_view_funds` | 18,662 | |
| `trading_view_indices` | 24,095 | |
| `trading_view_stocks` | 1,312,523 | universe + sector fallback |
| `cafef_price` | 2,388,368 | daily `(exchange, ticker, date)` |
| `cafef_foreign` | 1,772,666 | daily `(exchange, ticker, date)` |
| `cafef_order_stats` | 2,523,196 | daily; joined into silver.stocks_basic. **2026-08-01: 351,373 → 2,523,196** — bronze had lagged the 781-ticker scrape |
| `cafef_prop_trading` | 73,810 | daily; joined into silver.stocks_basic. **2026-08-01: 64,139 → 73,810** (431 tickers) |
| `cafef_index_price` | 24,962 | **6 MARKET INDICES**, 2000-07-28 → today; VN100-INDEX stops 2025-04-29 |
| `cafef_index_order_stats` | 22,863 | 6 indices; ⚠️ zero-filled for VN30/VN100 |
| `cafef_index_foreign` | 20,547 | 6 indices; ⚠️ real holes in the older years (CafeF's) |
| `cafef_index_prop_trading` | 1,494 | 6 indices; ⚠️ effectively exchange-level (VN100-INDEX = 1 row) |
| `cafef_insider_shareholder_transactions` | 13,607 | event-based, `row_id` PK; carried 1:1 to silver (own method) |
| `cafef_news` | 405,320 | event-based, `row_id` PK, `(exchange, ticker)` key. **2026-08-01: 5,599 → 405,320** — the scraper has since run the FULL 777-ticker universe (was VCB/PNJ/FPT only); not yet in silver |
| `cafef_financials_bank_balance_sheet` | 152 | 100 cols (incl. 3 share cols); VCB 78 (Q4-2006→Q1-2026) + ACB 74 (Q1-2008→Q2-2026) |
| `cafef_financials_bank_income_statement` | 152 | 36 cols (incl. 3 share cols); VCB 78 + ACB 74 |
| `cafef_financials_bank_cash_flow` | 152 | 57 cols (incl. 3 share cols); VCB 78 + ACB 74 |
| `cafef_financial_reports` | 456 | 152 quarters × 3 reports; **15 cols** (gained `method`, the OCR layer that read the filing); `publish_date` on 408/456 |
| `cafef_financial_schema` | 842 | all 12 charts of accounts (753 distinct line ids) |
| `cafef_financial_templates` | 2 | VCB + ACB → `bank` / direct; grows with the parse. ⚠️ the on-disk `templates.csv` holds only ACB (a subset parse rewrote it); the TABLE has both because the ingest upserts |
| `simplize_stocks` | 2,658,773 | PRIMARY daily backbone |
| `simplize_industry` | 777 | per-ticker GICS industry |
| `gics` | 163 | official sub-industry taxonomy |

- **silver is (re)built off this bronze; GOLD is NOT.** As of 2026-07-19 the silver
  CafeF carry-ups, `silver.gics`, the per-report `silver.cafef_financials_<template>_<report>`
  tables + the combined **`silver.cafef_financials_bank`** (180 cols: report-prefixed
  line items + one `publish_date` + 3 unprefixed share cols; 78 rows), and the rewritten
  **`silver.stocks_basic`**
  (CafeF four-way join + GICS tree, 2,388,368 rows) have all been materialised against
  the current bronze. Added 2026-07-21: **`silver.stocks_basic_financials_bank`** — the
  plain as-of join of `stocks_basic` × `cafef_financials_bank` on `publish_date`, all 216
  cols of both, no indicators; **HOSE:VCB only, 4,235 rows** (2009-06-30…2026-06-25) — and
  **`silver.stocks_basic_financials_bank_fa`** — that table + the 26-indicator fundamental
  catalog (242 cols, same 4,235 rows).
  **`gold.stocks` is still
  stale** — it reflects the old canonical silver stocks schema (Simplize-primary OHLC
  spine), which the rewrite has now replaced, so gold will not find several columns it
  expects. `_ingest_gold_stocks` now reads **`silver.stocks_basic`** (writes
  `gold.stocks`), so re-run `ingest_gold_data()` (drop the gold tables first — the gold
  `COPY` path assumes empty) and expect `_ingest_gold_stocks` /
  `_helper_build_feature_layers` to need updating for the CafeF-only column set (no
  single `close` / Simplize OHLC spine; `close_adjust` is the adjusted close).
- Regenerate this whole layer with a bronze drop + re-ingest (schema is fully
  derivable from `raw_data/`); counts will grow as the scrapers add history/tickers.
