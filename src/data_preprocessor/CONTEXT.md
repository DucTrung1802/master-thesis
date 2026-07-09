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
                  cafef_{price,foreign,order_stats,prop_trading,insider_txn},
                  simplize_stocks, simplize_industry, gics
        │
        ▼   ingest_silver_data()   ← split symbol, merge sources, GICS classify
  silver_schema:  {bonds,economy,forex,funds,indices,stocks}
        │
        ▼   ingest_gold_data()     ← feature engineering (TA + returns/vol/rolling)
  gold_schema:    {bonds,economy,forex,funds,indices,stocks}
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
- **Simple TradingView asset classes** (`bonds/economy/forex/funds/indices` +
  `trading_view_stocks`): glob `raw_data/trading_view/data/<asset>/**/*.csv`, concat,
  clean (drop rows null on `symbol`/`date`/`value|close`), cast, `date → date`, dedup
  on `(symbol, date)`, save. Key stays TradingView's `"<EXCHANGE>:<TICKER>"`.
- **CafeF — one bronze table per scraper link-folder** (mirrors the scraper's
  one-folder-per-link design; the former single merged `cafef_stocks` is gone —
  the price+foreign merge moved to silver). All share the
  `_helper_load_cafef_folder` + `_helper_normalise_cafef_symbol` (key →
  `"<EXCHANGE>:<TICKER>"`) helpers; the four daily ones go through the generic
  `_ingest_bronze_cafef_daily`:
  - `cafef_price` — OHLC (`close_raw`/`close_adj`) + matched/negotiated vol/val. PK `(symbol, date)`.
  - `cafef_foreign` — foreign buy/sell/net flow (vol+val), `room_left`, `own_pct`. PK `(symbol, date)`.
  - `cafef_order_stats` — buy/sell order counts, volume, avg vol/order. PK `(symbol, date)`.
  - `cafef_prop_trading` — proprietary-desk buy/sell vol+val. PK `(symbol, date)`.
  - `cafef_insider_txn` — insider / major-shareholder transactions. **Event-based**
    (no natural date key) → deterministic **md5 `row_id` surrogate PK** (hash of the
    full raw row, so re-ingests are idempotent); five date columns overridden to
    `DATE`, long text columns to `TEXT`.
- **`simplize_stocks`** — the validated daily backbone: adjusted OHLC, true volume,
  net/pct change, foreign vol+val + room. Key normalised to `"<EXCHANGE>:<TICKER>"`.
- **`simplize_industry`** — per-ticker VN GICS-based industry, loaded as-is;
  PK `(exchange, ticker)`.
- **`gics`** — official MSCI GICS taxonomy CSV, one row per sub-industry;
  PK `sub_industry_code`; `sub_industry_definition` overridden to `TEXT`.

### Silver (`_ingest_silver_*`) — canonical, cross-source merged
- **Simple assets** (`bonds/economy/forex/funds/indices`): split `symbol` →
  `(exchange, ticker)`, select the canonical columns, cast, save. PK
  `(exchange, ticker, date)`.
- **`_ingest_silver_stocks`** — the important one. First **reconstructs the CafeF
  frame** by merging bronze `cafef_price` + `cafef_foreign` on `(symbol, date)`
  (they are separate bronze tables now), then **OUTER-joins Simplize (PRIMARY)
  + CafeF + TradingView** on `(exchange, ticker, date)`:
  - Price: Simplize → TradingView (adjusted) → CafeF adjusted close. **Never** uses
    CafeF raw OHL; `close_raw` is dropped.
  - Volume: Simplize total → CafeF (matched + negotiated). TV volume is
    split-inflated → never a fallback.
  - Foreign flow/room: Simplize → CafeF. CafeF also uniquely supplies
    matched/negotiated split + `own_pct`.
  - Attaches the **full GICS tree** (`_helper_build_gics_classification`): each
    ticker's Simplize industry group → a GICS sub-industry leaf via
    `SIMPLIZE_GROUP_TO_GICS_SUB_INDUSTRY`, joined to `bronze.gics` to yield
    sector/industry_group/industry/sub_industry (+ codes), English snake_case.
    Columns listed in `GICS_CLASS_COLS`.

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
  bronze-only leaf; there is no `gics` silver/gold table (it feeds silver.stocks).
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

- **Bronze keys are `"<EXCHANGE>:<TICKER>"`; silver splits them** back into
  `exchange` + `ticker`. CafeF/Simplize store the two split in the raw CSV and the
  bronze ingest re-joins them into the TV-style colon key so all three merge
  uniformly in silver.
- **CafeF is one bronze table per scraper folder** (`cafef_price`, `cafef_foreign`,
  `cafef_order_stats`, `cafef_prop_trading`, `cafef_insider_txn`) — the folder/column
  names are the contract, so renaming them upstream breaks the ingest (mirrors the
  note in `web_scraper/CONTEXT.md §7`). The price+foreign merge that used to build
  `cafef_stocks` in bronze now happens in `_ingest_silver_stocks`. `order_stats` /
  `prop_trading` / `insider_txn` are ingested to bronze but **not yet consumed by
  silver/gold** — wiring them into a signal is future work.
- **Simplize is PRIMARY in silver.stocks**; TV is an OHLC fallback only and its
  volume/sector are never trusted (memory `project-bronze-source-per-field`).
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

## 8. Current materialized state (snapshot — 2026-07-09)

`bronze_schema` in `database_main_v2` after a full drop + re-ingest — **14 tables**:

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
| `cafef_order_stats` | 320,838 | daily; not yet in silver |
| `cafef_prop_trading` | 64,139 | daily; not yet in silver |
| `cafef_insider_txn` | 13,607 | event-based, `row_id` PK; not yet in silver |
| `simplize_stocks` | 2,658,773 | PRIMARY daily backbone |
| `simplize_industry` | 777 | per-ticker GICS industry |
| `gics` | 163 | official sub-industry taxonomy |

- **silver / gold are NOT yet rebuilt** against this bronze — `silver.stocks` /
  `gold.stocks` still reflect the pre-rework schema (the old `cafef_stocks`). Re-run
  `ingest_silver_data()` → `ingest_gold_data()` to refresh them off the new CafeF
  tables.
- Regenerate this whole layer with a bronze drop + re-ingest (schema is fully
  derivable from `raw_data/`); counts will grow as the scrapers add history/tickers.
