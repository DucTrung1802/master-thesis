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
        "bronze.trading_view_economy → silver.economy, PIVOTED to ONE ROW PER DATE "
        "(PK `date`), one column per series named "
        "`{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`. "
        "9,719 dates × 1,034 series. ⚠️ ~94% NULL by construction — each series keeps "
        "its own calendar — and the table is DROPPED and rebuilt each run because the "
        "grain changed."
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
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'silver_schema' AND table_name = %s",
                (table,),
            )
            columns = int(cur.fetchone()[0])
            cur.execute(f"SELECT MIN(date), MAX(date) FROM silver_schema.{table}")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM bronze_schema.trading_view_economy")
            bronze_rows = int(cur.fetchone()[0])

    context.log.info(
        f"silver.{table}: {rows} dates × {columns - 1} series "
        f"(bronze had {bronze_rows} observations)"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "series": columns - 1,
            "bronze_rows": bronze_rows,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(f"silver_schema.{table}"),
        }
    )


assets: List[Callable] = [silver_economy]
