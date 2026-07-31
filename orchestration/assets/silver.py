# orchestration\assets\silver.py
"""The SILVER layer — `bronze_schema` → `silver_schema`.

One asset so far: `silver/economy`. Like `assets/bronze.py` this is a thin wrapper —
the ingest lives in `DataPreprocessor._ingest_silver_economy`, so `main.py`, a notebook
and Dagster all build the same table (`orchestration/CONTEXT.md` §3).

Unlike the bronze assets, silver edges are **table → table**, and this one is real: the
ingest selects `bronze_schema.trading_view_economy`. That dep is only expressible now
that every bronze leaf is an asset — an earlier version of this module had to run
without one and say so.

> **What was here before:** `silver/trading_view_economy`, the same bronze table pivoted
> to one row per DATE and one column per ticker (9,719 × 1,034, 5.8% filled). Built and
> verified 2026-07-31 — non-null cells matched the bronze row count exactly — then
> retired 2026-08-01 in favour of the canonical long grain below. It is in git history
> (`orchestration/assets/silver.py` at `fa74ad3`) if the wide macro panel is wanted
> again; rebuilding it means restoring `_ingest_silver_trading_view_economy` too.
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


@asset(
    name="economy",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    deps=[AssetKey(["bronze", "trading_view_economy"])],
    description=(
        "bronze.trading_view_economy → silver.economy, PK (exchange, ticker, date) — "
        "the canonical LONG panel, one row per series per date. ⚠️ This ingest raised "
        "KeyError('symbol') on every run until 2026-08-01: it re-derived the key from a "
        "column bronze splits on read and has never stored."
    ),
)
def silver_economy(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    table = "economy"

    with preprocessor.session(schema="silver_schema") as prep:
        # Called directly, so an exception FAILS the asset — `ingest_silver_data()`
        # would swallow it per leaf and report a summary instead (main.py's shim).
        prep._ingest_silver_economy()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(f"SELECT COUNT(*) FROM silver_schema.{table}")
            rows = int(cur.fetchone()[0])
            cur.execute(f"SELECT COUNT(DISTINCT ticker) FROM silver_schema.{table}")
            tickers = int(cur.fetchone()[0])
            cur.execute(f"SELECT MIN(date), MAX(date) FROM silver_schema.{table}")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM bronze_schema.trading_view_economy")
            bronze_rows = int(cur.fetchone()[0])

    context.log.info(
        f"silver.{table}: {rows} rows / {tickers} tickers "
        f"(bronze had {bronze_rows})"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "tickers": tickers,
            "bronze_rows": bronze_rows,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(f"silver_schema.{table}"),
        }
    )


assets: List[Callable] = [silver_economy]
