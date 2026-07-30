# orchestration\assets\bronze.py
"""The FIRST database layer — `raw_data/` → `bronze_schema`.

Deliberately small: only the four market-index tables. This module is the proof that
the landing layer in `assets/scrape.py` connects to PostgreSQL cleanly, not the bronze
layer itself — the other 16 bronze tables are Phase 1 of the migration
(`orchestration/CONTEXT.md` §4.2).

Everything here is downstream of `assets/scrape.py`, and the edge is real: each ingest
reads exactly the `raw_data/cafef/index_*/` folder the matching scrape asset writes.
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

# (tab, bronze ingest method) — the folder is `index_<tab>` and the table
# `cafef_index_<tab>`, matching the scrape assets one-for-one.
TABS = [
    ("price", "_ingest_bronze_cafef_index_price"),
    ("order_stats", "_ingest_bronze_cafef_index_order_stats"),
    ("foreign", "_ingest_bronze_cafef_index_foreign"),
    ("prop_trading", "_ingest_bronze_cafef_index_prop_trading"),
]


def _build_bronze_asset(tab: str, ingest: str):
    @asset(
        name=f"cafef_index_{tab}",
        key_prefix=["bronze"],
        group_name="bronze",
        compute_kind="postgres",
        deps=[AssetKey(["raw", f"cafef_index_{tab}"])],
        description=(
            f"raw_data/cafef/index_{tab}/ → bronze.cafef_index_{tab}, PK "
            f"(exchange, ticker, date). Replaces switch leaf "
            f"`data_preprocessor/data_quality_bronze/cafef_index_{tab}`."
        ),
    )
    def _bronze(
        context: AssetExecutionContext, preprocessor: PreprocessorResource
    ) -> MaterializeResult:
        table = f"cafef_index_{tab}"
        with preprocessor.session(schema="bronze_schema") as prep:
            # Called directly, so an exception propagates and FAILS the asset. Going
            # through `ingest_bronze_data()` would catch it per leaf and report a
            # summary instead — that path is main.py's shim, not this one.
            getattr(prep, ingest)()

            # A raw cursor rather than `driver.select`: a COUNT is one less layer
            # between the asset's metadata and the database.
            with prep._database_driver._cursor_ctx() as cur:
                cur.execute(f"SELECT COUNT(*) FROM bronze_schema.{table}")
                rows = int(cur.fetchone()[0])

        context.log.info(f"bronze.{table}: {rows} rows")
        return MaterializeResult(
            metadata={
                "rows": rows,
                "table": MetadataValue.text(f"bronze_schema.{table}"),
            }
        )

    return _bronze


assets: List[Callable] = [_build_bronze_asset(tab, ingest) for tab, ingest in TABS]
