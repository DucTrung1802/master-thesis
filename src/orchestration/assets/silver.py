# src\orchestration\assets\silver.py
"""The SILVER layer — `bronze_schema` → `silver_schema`.

Seven assets. Two are a FACT table plus its DIMENSION:

* `silver/economy` — long, one row per series per date, PK `(exchange, ticker, date)`.
  The same grain as `bonds`/`forex`/`funds`/`indices`, and the grain
  `_ingest_gold_economy` reads.
* `silver/economy_series` — one row per series (1,034), PK `(exchange, ticker)`, holding
  `country`/`scrape_main_type`/`category` plus a DERIVED `frequency`.

⚠️ **Why the dimensions are not columns on the fact table.** `_ingest_gold_table` coerces
every column outside `{exchange, ticker, date, GICS}` with `pd.to_numeric`, so carrying
`country`/`category` on `silver.economy` would wipe all three to NaN in `gold.economy`.
Splitting them out also keeps the fact table identical in shape to its four siblings.

> **`silver.economy` was briefly WIDE** (one row per date × 1,034 columns, 2026-08-01)
> before the shape review moved that panel to gold — see `gold/economy_panel`. It
> measured 5.8% filled, but the deciding argument was not the nulls (≈1 bit each): a
> column-per-series table makes the SCHEMA a function of the DATA, so every new series
> becomes a DDL change. Long form takes new series as rows.

The other two are the same idea at two different granularities — several bronze tables
folded into one per-entity-per-day panel:

* `silver/stock_market` — the four `bronze.cafef_index_*` tables joined into ONE, PK
  `(exchange, ticker, date)`. ⚠️ `ticker` here is an INDEX CODE (`VNINDEX`, `VN30INDEX`,
  …), never a company — this must not be unioned into `stocks_basic`.
* `silver/stocks_basic` — the PER-STOCK equivalent: **six** bronze tables, four on the
  day key with `cafef_price` as the spine, plus `simplize_industry × gics` on
  `(exchange, ticker)` for the GICS tree.

The last three are the FINANCIALS chain, and it is the only silver→silver chain in the
layer — each step reads the table the one before it wrote:

```
bronze/cafef_financials ─► silver/cafef_financials_bank        (quarterly, 180 cols)
                                     └─┬─► silver/stocks_basic_financials_bank  (daily)
silver/stocks_basic ─────────────────┘         └─► …_fa  (+ 26 indicators) ─► gold
```

⚠️ **`publish_date` is what makes the daily join honest.** A quarter's figures are
attached to a price day only from the day they were PUBLISHED (`publish_date <= date`),
never from the period end — a period-end join would hand a model VCB's Q1 balance sheet
in March, weeks before it existed. The asset asserts it: it counts rows with
`publish_date > date` and raises if any exist.

All are thin wrappers — the logic lives in `DataPreprocessor`, so `main.py`, a notebook
and Dagster all build the same tables (`src/orchestration/CONTEXT.md` §3).
"""

from typing import Callable, List

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration.resources import PreprocessorResource

BRONZE_ECONOMY = AssetKey(["bronze", "trading_view_economy"])
INDEX_TABS = [
    "cafef_index_price",
    "cafef_index_order_stats",
    "cafef_index_foreign",
    "cafef_index_prop_trading",
]
# The six bronze tables `_ingest_silver_stocks_basic` opens: four daily CafeF tabs keyed
# `(exchange, ticker, date)` with `cafef_price` as the spine, plus the two the GICS
# crosswalk needs, keyed `(exchange, ticker)`.
STOCKS_BASIC_SOURCES = [
    "cafef_price",
    "cafef_order_stats",
    "cafef_foreign",
    "cafef_prop_trading",
    "simplize_industry",
    "gics",
]


@asset(
    name="economy",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[BRONZE_ECONOMY],
    description=(
        "bronze.trading_view_economy → silver.economy, PK (exchange, ticker, date) — "
        "LONG, one row per series per date, no nulls by construction. ⚠️ This ingest "
        "raised KeyError('symbol') on every run until 2026-08-01: it re-derived the key "
        "from a column bronze splits on read and has never stored."
    ),
)
def silver_economy(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        # Called directly, so an exception FAILS the asset — `ingest_silver_data()`
        # would swallow it per leaf and report a summary instead (main.py's shim).
        prep._ingest_silver_economy()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM silver_schema.economy")
            rows = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(DISTINCT ticker) FROM silver_schema.economy")
            tickers = int(cur.fetchone()[0])
            cur.execute("SELECT MIN(date), MAX(date) FROM silver_schema.economy")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM bronze_schema.trading_view_economy")
            bronze_rows = int(cur.fetchone()[0])

    context.log.info(f"silver.economy: {rows} rows / {tickers} series")
    return MaterializeResult(
        metadata={
            "rows": rows,
            "series": tickers,
            "bronze_rows": bronze_rows,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("silver_schema.economy"),
        }
    )


@asset(
    name="economy_series",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[BRONZE_ECONOMY],
    description=(
        "bronze.trading_view_economy → silver.economy_series, PK (exchange, ticker) — "
        "the dimension table: country / scrape_main_type / category, plus a DERIVED "
        "`frequency` (TradingView publishes none) that sets the publication lag in "
        "gold/economy_panel."
    ),
)
def silver_economy_series(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        prep._ingest_silver_economy_series()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM silver_schema.economy_series")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT frequency, COUNT(*) FROM silver_schema.economy_series "
                "GROUP BY 1 ORDER BY 2 DESC"
            )
            by_freq = {f: int(n) for f, n in cur.fetchall()}

    context.log.info(f"silver.economy_series: {rows} series — {by_freq}")
    return MaterializeResult(
        metadata={
            "rows": rows,
            "frequencies": MetadataValue.json(by_freq),
            "table": MetadataValue.text("silver_schema.economy_series"),
        }
    )


@asset(
    name="stock_market",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[AssetKey(["bronze", t]) for t in INDEX_TABS],
    description=(
        "The four bronze.cafef_index_* tabs (price, order_stats, foreign, prop_trading) "
        "→ ONE silver.stock_market, PK (exchange, ticker, date). 6 market indices. "
        "⚠️ OUTER join, unlike stocks_basic's left-join-on-price: the key union is "
        "25,935 against price's 24,962, so a left join would drop 973 index-days that "
        "have order/foreign/prop data but no price row."
    ),
)
def silver_stock_market(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        prep._ingest_silver_stock_market()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM silver_schema.stock_market")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'silver_schema' AND table_name = 'stock_market'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT ticker, COUNT(*) FROM silver_schema.stock_market "
                "GROUP BY 1 ORDER BY 2 DESC"
            )
            by_index = {t: int(n) for t, n in cur.fetchall()}
            cur.execute("SELECT MIN(date), MAX(date) FROM silver_schema.stock_market")
            first, last = cur.fetchone()

    context.log.info(f"silver.stock_market: {rows} index-days × {columns} columns")
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "indices": MetadataValue.json(by_index),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("silver_schema.stock_market"),
        }
    )


@asset(
    name="stocks_basic",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[AssetKey(["bronze", t]) for t in STOCKS_BASIC_SOURCES],
    description=(
        "SIX bronze tables → silver.stocks_basic, PK (exchange, ticker, date): "
        "cafef_price as the spine, LEFT JOIN cafef_{order_stats,foreign,prop_trading} "
        "on the full day key, plus the GICS tree from simplize_industry × gics on "
        "(exchange, ticker). ⚠️ A CafeF-faithful merge — simplize_stocks is NOT a "
        "source here despite being the bigger daily table."
    ),
)
def silver_stocks_basic(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        prep._ingest_silver_stocks_basic()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM silver_schema.stocks_basic")
            rows = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(DISTINCT ticker) FROM silver_schema.stocks_basic")
            tickers = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'silver_schema' AND table_name = 'stocks_basic'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute("SELECT MIN(date), MAX(date) FROM silver_schema.stocks_basic")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM bronze_schema.cafef_price")
            spine_rows = int(cur.fetchone()[0])
            # Coverage per joined block — a left join fills what each source has.
            cur.execute(
                "SELECT COUNT(n_buy_orders), COUNT(foreign_buy_volume), "
                "COUNT(prop_buy_vol), COUNT(sector) FROM silver_schema.stocks_basic"
            )
            order_stats, foreign, prop, gics = (int(x) for x in cur.fetchone())

    context.log.info(
        f"silver.stocks_basic: {rows} stock-days × {columns} columns "
        f"(spine bronze.cafef_price has {spine_rows})"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "tickers": tickers,
            "spine_rows": spine_rows,
            "rows with order_stats": order_stats,
            "rows with foreign": foreign,
            "rows with prop_trading": prop,
            "rows with GICS": gics,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("silver_schema.stocks_basic"),
        }
    )


@asset(
    name="cafef_financials_bank",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[AssetKey(["bronze", "cafef_financials"])],
    description=(
        "The bronze cafef_financials_<template>_<report> STATEMENT tables carried up "
        "one-to-one, then the three `bank` reports OUTER-joined on "
        "(exchange, ticker, year, quarter) into silver.cafef_financials_bank — 180 "
        "columns, report-prefixed, plus ONE publish_date joined from "
        "bronze.cafef_financial_reports. ⚠️ The join is OUTER because a quarter can "
        "have a balance sheet and no cash flow."
    ),
)
def silver_cafef_financials_bank(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        # Both halves of main.py's `financials` leaf, in order: the per-report
        # carry-ups first, then the wide per-template join that reads them.
        prep._ingest_silver_cafef_financials()
        prep._ingest_silver_cafef_financials_bank()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT ticker), COUNT(publish_date) "
                "FROM silver_schema.cafef_financials_bank"
            )
            rows, tickers, dated = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'silver_schema' AND table_name = 'cafef_financials_bank'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT MIN(year || '-Q' || quarter), MAX(year || '-Q' || quarter) "
                "FROM silver_schema.cafef_financials_bank"
            )
            first, last = cur.fetchone()

    context.log.info(
        f"silver.cafef_financials_bank: {rows} quarters × {columns} columns "
        f"({dated} with a publish_date)"
    )
    return MaterializeResult(
        metadata={
            "rows": int(rows),
            "columns": columns,
            "tickers": int(tickers),
            "quarters with publish_date": int(dated),
            "period_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("silver_schema.cafef_financials_bank"),
        }
    )


@asset(
    name="stocks_basic_financials_bank",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[
        AssetKey(["silver", "stocks_basic"]),
        AssetKey(["silver", "cafef_financials_bank"]),
    ],
    description=(
        "silver.stocks_basic (DAILY) × silver.cafef_financials_bank (QUARTERLY), "
        "as-of on publish_date — every price day carries the most recently PUBLISHED "
        "quarter, so a figure steps on its release date and holds flat. PK "
        "(exchange, ticker, date). ⚠️ INNER scope: only tickers that have financials, "
        "only days on/after their first publish."
    ),
)
def silver_stocks_basic_financials_bank(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        prep._ingest_silver_stocks_basic_financials_bank()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                "FROM silver_schema.stocks_basic_financials_bank"
            )
            rows, tickers, first, last = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'silver_schema' AND "
                "table_name = 'stocks_basic_financials_bank'"
            )
            columns = int(cur.fetchone()[0])
            # The look-ahead invariant: no day may carry a quarter published after it.
            cur.execute(
                "SELECT COUNT(*) FROM silver_schema.stocks_basic_financials_bank "
                "WHERE publish_date > date"
            )
            look_ahead = int(cur.fetchone()[0])

    if look_ahead:
        raise ValueError(
            f"{look_ahead} rows have publish_date > date — the as-of join leaked "
            f"future financials into the panel."
        )

    context.log.info(
        f"silver.stocks_basic_financials_bank: {rows} stock-days × {columns} columns"
    )
    return MaterializeResult(
        metadata={
            "rows": int(rows),
            "columns": columns,
            "tickers": int(tickers),
            "rows_with_look_ahead": look_ahead,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("silver_schema.stocks_basic_financials_bank"),
        }
    )


@asset(
    name="stocks_basic_financials_bank_fa",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stocks_basic_financials_bank"])],
    description=(
        "silver.stocks_basic_financials_bank + the 26 fundamental indicators "
        "(EPS/BVPS/ROE/ROA/NIM/LDR/CIR, the YoY growths, and the price-dependent "
        "P/E, P/B, P/S, market cap, earnings yield). Keeps all source columns. "
        "⚠️ No re-join: the as-of merge is already baked into the source, so the "
        "indicators inherit its zero look-ahead."
    ),
)
def silver_stocks_basic_financials_bank_fa(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="silver_schema") as prep:
        prep._ingest_silver_stocks_basic_financials_bank_fa()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                "FROM silver_schema.stocks_basic_financials_bank_fa"
            )
            rows, tickers, first, last = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'silver_schema' AND "
                "table_name = 'stocks_basic_financials_bank_fa'"
            )
            columns = int(cur.fetchone()[0])
            # Coverage of the indicators that need a full 4-quarter TTM window vs the
            # ones that need only the current balance sheet.
            cur.execute(
                "SELECT COUNT(pe_ttm), COUNT(pb), COUNT(roe), COUNT(nim) "
                "FROM silver_schema.stocks_basic_financials_bank_fa"
            )
            pe, pb, roe, nim = (int(x) for x in cur.fetchone())

    context.log.info(
        f"silver.stocks_basic_financials_bank_fa: {rows} stock-days × {columns} columns"
    )
    return MaterializeResult(
        metadata={
            "rows": int(rows),
            "columns": columns,
            "tickers": int(tickers),
            "rows with pe_ttm": pe,
            "rows with pb": pb,
            "rows with roe": roe,
            "rows with nim": nim,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(
                "silver_schema.stocks_basic_financials_bank_fa"
            ),
        }
    )


assets: List[Callable] = [
    silver_economy,
    silver_economy_series,
    silver_stock_market,
    silver_stocks_basic,
    silver_cafef_financials_bank,
    silver_stocks_basic_financials_bank,
    silver_stocks_basic_financials_bank_fa,
]
