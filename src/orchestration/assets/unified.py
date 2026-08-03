# src\orchestration\assets\unified.py
"""The UNIFIED layer — `silver_schema` → `unified_schema_<ticker>`.

The fourth layer, and the first one that is scoped to a SINGLE TICKER. Where bronze /
silver / gold each hold the whole universe in one table, `unified_schema_vcb` holds one
company cut into the FEATURE GROUPS a model consumes:

    pool__basic      the price/flow panel            ← this module
    pool__targets    the labels                      ← this module
    pool__ta         the technical block
    pool__macro      the macro series
    pool__calendar   calendar features

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


@asset(
    name="pool__targets",
    key_prefix=["unified_vcb"],
    group_name="unified_vcb",
    compute_kind="postgres",
    deps=[AssetKey(["unified_vcb", "pool__basic"])],
    description=(
        f"{UNIFIED_SCHEMA_NAME}.pool__basic → {UNIFIED_SCHEMA_NAME}.pool__targets: "
        f"`date` (PK) plus ONE COLUMN PER HORIZON in "
        f"DataPreprocessor.UNIFIED_TARGET_HORIZONS — `return_5day`, `return_10day` — "
        f"each the forward simple return close[t+h]/close[t]-1 on the SPLIT-ADJUSTED "
        f"close. ⚠️ Sourced from pool__basic, not gold.stocks, so the labels and the "
        f"features share one calendar by construction — the dropped version had 4,242 "
        f"rows against pool__basic's 4,235 for exactly that reason. ⚠️ Each column's "
        f"last h rows are NULL (their future does not exist yet) and are kept so the "
        f"table still joins; the two horizons therefore have DIFFERENT usable ranges."
    ),
)
def unified_vcb_pool_targets(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema=UNIFIED_SCHEMA_NAME) as prep:
        prep._ingest_unified_pool_targets(UNIFIED_TICKER)
        horizons = tuple(prep.UNIFIED_TARGET_HORIZONS)
        target_cols = {h: f"return_{h}day" for h in horizons}

        with prep._database_driver._cursor_ctx() as cur:
            stats = {}
            for h, target_col in target_cols.items():
                cur.execute(
                    f"SELECT COUNT({target_col}), MIN({target_col}), "
                    f"       MAX({target_col}), AVG({target_col}) "
                    f"FROM {UNIFIED_SCHEMA_NAME}.pool__targets"
                )
                stats[h] = cur.fetchone()
            cur.execute(
                f"SELECT COUNT(*), MIN(date), MAX(date) "
                f"FROM {UNIFIED_SCHEMA_NAME}.pool__targets"
            )
            rows, first, last = cur.fetchone()
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'pool__targets' "
                "ORDER BY ordinal_position",
                (UNIFIED_SCHEMA_NAME,),
            )
            columns = [r[0] for r in cur.fetchall()]
            cur.execute(
                f"SELECT COUNT(*) FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
            )
            basic_rows = int(cur.fetchone()[0])
            # ⚠️ The join to the feature pools is the whole reason this table exists,
            # so it is checked rather than assumed: every target date must be a
            # pool__basic date and vice versa.
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT date FROM {UNIFIED_SCHEMA_NAME}.pool__targets"
                f"  EXCEPT SELECT date FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
                f"  UNION ALL"
                f"  SELECT date FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
                f"  EXCEPT SELECT date FROM {UNIFIED_SCHEMA_NAME}.pool__targets"
                f") d"
            )
            unaligned = int(cur.fetchone()[0])

    expected = ["date"] + list(target_cols.values())
    if columns != expected:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__targets should hold exactly {expected}, "
            f"got {columns}."
        )
    if unaligned:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__targets and pool__basic disagree on "
            f"{unaligned} date(s) — they must share one calendar to join on `date`."
        )
    if rows != basic_rows:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__targets has {rows} rows against "
            f"pool__basic's {basic_rows}."
        )
    # ⚠️ Asserted per horizon: each column's unlabelled tail must be exactly its OWN
    # h. A shared check against the longest would let a hole in `return_5day` pass.
    for h, target_col in target_cols.items():
        tail = rows - int(stats[h][0])
        if tail != h:
            raise ValueError(
                f"{UNIFIED_SCHEMA_NAME}.pool__targets.{target_col} has a {tail}-row "
                f"unlabelled tail; exactly {h} were expected."
            )

    context.log.info(
        f"{UNIFIED_SCHEMA_NAME}.pool__targets: {rows} rows ({first} → {last}) — "
        + "; ".join(
            f"{target_cols[h]} {int(stats[h][0])} labelled + {rows - int(stats[h][0])} "
            f"tail, range {float(stats[h][1]):.4f} → {float(stats[h][2]):.4f}, "
            f"mean {float(stats[h][3]):.5f}"
            for h in horizons
        )
    )
    metadata = {
        "rows": int(rows),
        "horizons": MetadataValue.text(", ".join(str(h) for h in horizons)),
        "targets": MetadataValue.text(", ".join(target_cols.values())),
        "date_range": MetadataValue.text(f"{first} → {last}"),
        "table": MetadataValue.text(f"{UNIFIED_SCHEMA_NAME}.pool__targets"),
    }
    for h, target_col in target_cols.items():
        labelled, lo, hi, mean = stats[h]
        metadata[f"{target_col}__labelled"] = int(labelled)
        metadata[f"{target_col}__unlabelled_tail"] = int(rows - int(labelled))
        metadata[f"{target_col}__min"] = MetadataValue.float(round(float(lo), 6))
        metadata[f"{target_col}__max"] = MetadataValue.float(round(float(hi), 6))
        metadata[f"{target_col}__mean"] = MetadataValue.float(round(float(mean), 6))
    return MaterializeResult(metadata=metadata)


assets: List[Callable] = [unified_vcb_pool_basic, unified_vcb_pool_targets]
