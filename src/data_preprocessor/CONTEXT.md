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
    splits it on the FIRST `:` (applied just before save, so dedup still runs on the
    intact key). Note this splits a **data-provider prefix**, not always a bourse:
    `ECONOMICS:CN14RRR`, `B2PRIME:AUDCAD`, `TVC:VN01` → `exchange` is the vendor
    namespace for the non-stock assets.
  - **CafeF & Simplize** keep `exchange` and the ticker apart in the raw CSV already,
    so they just `rename(symbol → ticker)` — they no longer fold-then-split.
  - `_helper_normalise_cafef_symbol` (the old fold-to-colon helper) is now UNUSED by
    any live ingest; kept only for reference.
- **Simple TradingView asset classes** (`bonds/economy/forex/funds/indices` +
  `trading_view_stocks`): glob `raw_data/trading_view/data/<asset>/**/*.csv`, concat,
  clean (drop rows null on `symbol`/`date`/`value|close`), cast, `date → date`, dedup
  on `(symbol, date)`, **split symbol → `(exchange, ticker)`**, save. PK
  `(exchange, ticker, date)`.
- **CafeF — one bronze table per scraper link-folder** (mirrors the scraper's
  one-folder-per-link design; the former single merged `cafef_stocks` is gone —
  the price+foreign merge moved to silver). All share the `_helper_load_cafef_folder`
  helper; the four daily ones go through the generic `_ingest_bronze_cafef_daily`
  (with `split_key=True` → PK `(exchange, ticker, date)`):
  - `cafef_price` — OHLC (`close_raw`/`close_adjust`) + matched/negotiated vol/val. PK `(exchange, ticker, date)`.
  - `cafef_foreign` — foreign buy/sell/net flow (vol+val), `foreign_room_left`, `foreign_own`. PK `(exchange, ticker, date)`.
  - `cafef_order_stats` — buy/sell order counts, volume, avg vol/order. PK `(exchange, ticker, date)`.
  - `cafef_prop_trading` — proprietary-desk buy/sell vol+val. PK `(exchange, ticker, date)`.
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
  reference tables**. Only parsed templates get a table; today that is `bank` (VCB),
  so 3 of the 12 exist.
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
  - `cafef_financial_reports` — the 13 per-DOCUMENT metadata columns, split off
    because they describe the filing, not the accounts. PK
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
- **Simple assets** (`bonds/economy/forex/funds/indices`): split `symbol` →
  `(exchange, ticker)`, select the canonical columns, cast, save. PK
  `(exchange, ticker, date)`.
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
  - Wired into `ingest_silver_data` under the silver `stocks` switch, ahead of
    `_ingest_silver_stocks_basic`. Verified: each silver table row count == its current
    bronze count.
- **`_ingest_silver_gics`** (added 2026-07-19) — the same basic-clean carry-up
  pattern applied to the bronze `gics` reference table (not a CafeF source, so it
  has its own method, not `_helper_load_cafef_folder`/`_ingest_silver_cafef_daily`).
  Select bronze `gics` → drop rows null on `sub_industry_code` → drop-all-null →
  order by key → drop the old silver table → save. PK stays `sub_industry_code`
  (not `(exchange, ticker, date)` — this table is one row per GICS sub-industry,
  not per ticker); `sub_industry_definition` keeps its bronze `TEXT` override.
  Wired into `ingest_silver_data` under its own new switch leaf,
  `data_preprocessor/data_quality_silver/gics` (sits alongside `bonds`/`economy`/
  `forex`/`funds`/`indices`/`stocks`, independent of the `stocks` switch that gates
  the CafeF carry-ups above).
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
- **PLANNED: `silver.stocks_fundamental`** (not built yet) — the fundamental-analysis
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

- `SwitchHandler` reads a **flat JSON of slash-path → bool**; a path is enabled only
  when **every ancestor is explicitly true**. The three entry points each gate on
  `data_preprocessor/data_quality_{bronze|silver|gold}` and then on a per-asset leaf
  (`.../bronze/stocks`, `.../silver/bonds`, `.../gold/economy`, …). `gics` is a
  **bronze + silver** leaf (`.../bronze/gics`, `.../silver/gics`) — the silver copy
  is a straight reference-table carry-up; there is still no `gics` gold table.
  `bronze.gics` also feeds `silver.stocks_basic`'s GICS tree (via
  `_helper_build_gics_classification`) regardless of the silver `gics` leaf.
- **Order matters:** silver reads bronze tables; gold reads silver tables. Run the
  layers in bronze → silver → gold order (main.py does).

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

## 8. Current materialized state (snapshot — 2026-07-16)

> Row counts below are unchanged by the 2026-07-16 key split — that reshape moved the
> `symbol` colon key to split `(exchange, ticker)` columns without adding or dropping
> a single row (verified: every table re-ingested to the same count).

`bronze_schema` in `database_main_v2` — **21 tables** (15 + the 6 financials tables
that exist so far; 30 once all four templates are parsed):

| Table | Rows | Notes |
|---|---:|---|
| `trading_view_bonds` | 66,100 | |
| `trading_view_economy` | 579,459 | |
| `trading_view_forex` | 1,324,940 | |
| `trading_view_funds` | 18,662 | |
| `trading_view_indices` | 24,095 | |
| `trading_view_stocks` | 1,312,523 | universe + sector fallback |
| `cafef_price` | 2,388,368 | daily `(symbol, date)` |
| `cafef_foreign` | 1,772,666 | daily `(symbol, date)` |
| `cafef_order_stats` | 320,838 | daily; now joined into silver.stocks_basic |
| `cafef_prop_trading` | 64,139 | daily; now joined into silver.stocks_basic |
| `cafef_insider_shareholder_transactions` | 13,607 | event-based, `row_id` PK; carried 1:1 to silver (own method) |
| `cafef_news` | 5,599 | event-based, `row_id` PK, `(exchange, ticker)` key; **VCB/PNJ/FPT only** (1,629 / 1,715 / 2,255) — the scraper has run on 3 tickers; not yet in silver |
| `cafef_financials_bank_balance_sheet` | 78 | 100 cols (incl. 3 share cols); VCB only, Q4-2006 → Q1-2026 |
| `cafef_financials_bank_income_statement` | 78 | 36 cols (incl. 3 share cols); VCB only |
| `cafef_financials_bank_cash_flow` | 78 | 57 cols (incl. 3 share cols); VCB only |
| `cafef_financial_reports` | 234 | 78 quarters × 3 reports; 71/78 readable per report, `publish_date` on 210/213 |
| `cafef_financial_schema` | 842 | all 12 charts of accounts (753 distinct line ids) |
| `cafef_financial_templates` | 1 | VCB → `bank` / direct; grows with the parse |
| `simplize_stocks` | 2,658,773 | PRIMARY daily backbone |
| `simplize_industry` | 777 | per-ticker GICS industry |
| `gics` | 163 | official sub-industry taxonomy |

- **silver is (re)built off this bronze; GOLD is NOT.** As of 2026-07-19 the silver
  CafeF carry-ups, `silver.gics`, the per-report `silver.cafef_financials_<template>_<report>`
  tables + the combined **`silver.cafef_financials_bank`** (180 cols: report-prefixed
  line items + one `publish_date` + 3 unprefixed share cols; 78 rows), and the rewritten
  **`silver.stocks_basic`**
  (CafeF four-way join + GICS tree, 2,388,368 rows) have all been materialised against
  the current bronze. **`gold.stocks` is still
  stale** — it reflects the old canonical silver stocks schema (Simplize-primary OHLC
  spine), which the rewrite has now replaced, so gold will not find several columns it
  expects. `_ingest_gold_stocks` now reads **`silver.stocks_basic`** (writes
  `gold.stocks`), so re-run `ingest_gold_data()` (drop the gold tables first — the gold
  `COPY` path assumes empty) and expect `_ingest_gold_stocks` /
  `_helper_build_feature_layers` to need updating for the CafeF-only column set (no
  single `close` / Simplize OHLC spine; `close_adjust` is the adjusted close).
- Regenerate this whole layer with a bronze drop + re-ingest (schema is fully
  derivable from `raw_data/`); counts will grow as the scrapers add history/tickers.
