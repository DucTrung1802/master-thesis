# src\orchestration\assets\unified.py
"""The UNIFIED layer — `silver_schema` → `unified_schema_<ticker>`.

The fourth layer, and the first one that is scoped to a SINGLE TICKER. Where bronze /
silver / gold each hold the whole universe in one table, `unified_schema_vcb` holds one
company cut into the FEATURE GROUPS a model consumes:

    pool__basic      the price/flow panel            ← this module
    pool__ta         the technical block
    pool__macro      the macro series
    pool__calendar   calendar features
    pool__targets    the labels

The point of the split is feature SELECTION: a run can be scoped to one group, which is
why `train_test_creator/unified_schema_creator.ipynb` (the only builder until now) also
writes `<target>__lb<N>__<group>__<n>` tables holding each group's surviving columns.

⚠️ **THE SCHEMA IS CREATED BY THE ASSET, not by hand.** `PreprocessorResource.session`
already issues `CREATE SCHEMA IF NOT EXISTS` for whatever schema it is handed — the same
preamble `ingest_bronze_data` runs — so naming `unified_schema_vcb` there is what brings
the schema into existence. `_ingest_unified_pool_basic` also creates it, so the method is
correct when called from a notebook or `main.py` with no Dagster around it.

⚠️ **The ticker is an IDENTIFIER here, not a value.** It is interpolated into a schema
NAME, which cannot be a bound parameter — `DataPreprocessor._helper_unified_schema`
validates it against `UNIFIED_TICKER_PATTERN` and raises rather than interpolating
anything else. `UNIFIED_TICKER` below is the one place this module names it.
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

# The ticker this code location builds a unified schema for.
#
# ⚠️ Deliberately a CONSTANT and not a partition. A partition would suggest the other
# `pool__*` tables are per-ticker assets too, and they are not assets at all yet — the
# notebook still builds them. When the rest of the schema moves here, this becomes a
# `StaticPartitionsDefinition` over the tickers actually wanted, and the asset key stops
# carrying the ticker in its name.
UNIFIED_TICKER = "VCB"
UNIFIED_SCHEMA_NAME = f"unified_schema_{UNIFIED_TICKER.lower()}"


@asset(
    name="pool__basic",
    key_prefix=["unified_vcb"],
    group_name="unified_vcb",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stocks_basic"])],
    description=(
        f"silver.stocks_basic (ticker {UNIFIED_TICKER} only) → "
        f"{UNIFIED_SCHEMA_NAME}.pool__basic: EVERY column of the silver table, with "
        f"silver's own types, PK (exchange, ticker, date). ⚠️ Built with CREATE TABLE "
        f"AS, not a pandas round-trip: psycopg2 returns `numeric` as Decimal, which a "
        f"DataFrame carries as dtype `object` and the writer maps to VARCHAR — a "
        f"round-trip would silently turn every price column into TEXT. ⚠️ Creates the "
        f"schema if absent and REPLACES the table; sibling pool__* tables are untouched."
    ),
)
def unified_vcb_pool_basic(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    # ⚠️ `session(schema=...)` is what CREATES the schema — see the module docstring.
    with preprocessor.session(schema=UNIFIED_SCHEMA_NAME) as prep:
        prep._ingest_unified_pool_basic(UNIFIED_TICKER)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                f"FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
            )
            rows, tickers, first, last = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'pool__basic'",
                (UNIFIED_SCHEMA_NAME,),
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'silver_schema' AND table_name = 'stocks_basic'"
            )
            silver_columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM silver_schema.stocks_basic WHERE ticker = %s",
                (UNIFIED_TICKER,),
            )
            silver_rows = int(cur.fetchone()[0])
            # ⚠️ The column set is the ASSERTION, not a metric. "All columns of
            # silver.stocks_basic" is this asset's contract, and a CTAS that silently
            # dropped or gained one would still produce a plausible-looking table.
            cur.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'silver_schema' AND table_name = 'stocks_basic'
                   EXCEPT
                   SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = 'pool__basic'""",
                (UNIFIED_SCHEMA_NAME,),
            )
            missing = [r[0] for r in cur.fetchall()]

    if missing:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__basic is missing {len(missing)} column(s) of "
            f"silver.stocks_basic: {sorted(missing)}"
        )
    if rows != silver_rows:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__basic has {rows} rows but silver holds "
            f"{silver_rows} for {UNIFIED_TICKER}."
        )
    if tickers != 1:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__basic holds {tickers} tickers — a unified "
            f"schema is one company by definition."
        )

    context.log.info(
        f"{UNIFIED_SCHEMA_NAME}.pool__basic: {rows} rows × {columns} columns "
        f"({first} → {last}), every column of silver.stocks_basic present"
    )
    return MaterializeResult(
        metadata={
            "rows": int(rows),
            "columns": columns,
            "silver_columns": silver_columns,
            "ticker": MetadataValue.text(UNIFIED_TICKER),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(f"{UNIFIED_SCHEMA_NAME}.pool__basic"),
        }
    )


assets: List[Callable] = [unified_vcb_pool_basic]
