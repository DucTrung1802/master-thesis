# orchestration\assets\silver.py
"""The SILVER layer — `bronze_schema` → `silver_schema`.

Two assets, and they are a FACT table plus its DIMENSION:

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

Both are thin wrappers — the logic lives in `DataPreprocessor`, so `main.py`, a notebook
and Dagster all build the same tables (`orchestration/CONTEXT.md` §3).
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


assets: List[Callable] = [silver_economy, silver_economy_series]
