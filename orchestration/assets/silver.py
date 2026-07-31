# orchestration\assets\silver.py
"""The SILVER layer — `bronze_schema` → `silver_schema`.

One asset so far: the pivoted economy panel. Like `assets/bronze.py` this is a thin
wrapper — the reshape and the clean layer live in
`DataPreprocessor._ingest_silver_trading_view_economy`, so `main.py`, a notebook and
Dagster all get the same table (`orchestration/CONTEXT.md` §3).

⚠️ NO UPSTREAM ASSET, ON PURPOSE. Its input is the bronze TABLE, and bronze is only
partly migrated — `assets/bronze.py` covers the four `cafef_index_*` tables, not
`trading_view_economy`. Declaring a dep on an asset that does not exist would invent an
edge; the real precondition is enforced where it belongs, in the ingest, which raises
`MissingSourceDataError` if the bronze table is empty. Phase 1 adds the 20 bronze leaves
as assets, and this `deps=` gets filled in then.
"""

from typing import Callable, List

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration.resources import PreprocessorResource


@asset(
    name="trading_view_economy",
    key_prefix=["silver"],
    group_name="silver",
    compute_kind="postgres",
    description=(
        "bronze.trading_view_economy → silver.trading_view_economy, PIVOTED to one row "
        "per DATE with one column per ticker (PK `date`). The long EAV table becomes the "
        "wide macro panel that joins on date alone. ⚠️ ~94% NULL by construction — each "
        "series keeps its own calendar (VNINBR daily, VNGDPYY quarterly), so a date only "
        "carries the series that reported on it. Forward-filling is gold's decision."
    ),
)
def silver_trading_view_economy(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    table = "trading_view_economy"

    with preprocessor.session(schema="silver_schema") as prep:
        # Called directly, so an exception FAILS the asset — `ingest_silver_data()`
        # would swallow it per leaf and report a summary instead (main.py's shim).
        prep._ingest_silver_trading_view_economy()

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

    context.log.info(f"silver.{table}: {rows} dates × {columns - 1} tickers")
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "tickers": columns - 1,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(f"silver_schema.{table}"),
        }
    )


assets: List[Callable] = [silver_trading_view_economy]
