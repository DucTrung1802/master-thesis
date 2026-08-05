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


def _primary_key(cur, table: str) -> tuple:
    """The table's primary key columns, IN INDEX ORDER.

    ⚠️ `information_schema.key_column_usage` reports `ordinal_position`, which for a
    PK is the position within the CONSTRAINT — and PostgreSQL does not guarantee that
    matches the index's own column order. `pg_index.indkey` is the index order, which
    is the thing that decides what a range scan can use, so that is what is read.
    """
    cur.execute(
        """SELECT a.attname
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
           JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
           WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s
           ORDER BY k.ord""",
        (UNIFIED_SCHEMA_NAME, table),
    )
    return tuple(row[0] for row in cur.fetchall())


@asset(
    name="pool__basic",
    key_prefix=["unified_vcb"],
    group_name="unified_vcb",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stocks_basic"])],
    description=(
        f"silver.stocks_basic (ticker {UNIFIED_TICKER} only) → "
        f"{UNIFIED_SCHEMA_NAME}.pool__basic: EVERY column of the silver table, with "
        f"silver's own types, PK (date, exchange, ticker) — DataPreprocessor."
        f"UNIFIED_PRIMARY_KEY, and the order is asserted. ⚠️ Built with CREATE TABLE "
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
            primary_key = _primary_key(cur, "pool__basic")
            expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

    # ⚠️ The key ORDER is asserted, not just its membership. `ADD PRIMARY KEY`
    # accepts any ordering of the same three columns, and only a leading `date`
    # lets the index serve the time-range scans every consumer here does.
    if primary_key != expected_key:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__basic primary key is {primary_key}, "
            f"expected {expected_key} — order is part of the contract."
        )
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
            "primary_key": MetadataValue.text(", ".join(primary_key)),
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
        f"PK (date, exchange, ticker) — the same key as every other pool, so a join "
        f"needs no special case — plus TWO COLUMNS PER HORIZON in "
        f"DataPreprocessor.UNIFIED_TARGET_HORIZONS — `return_{{h}}day`, the forward "
        f"simple return close[t+h]/close[t]-1 on the SPLIT-ADJUSTED close, and "
        f"`return_rel_{{h}}day`, the same minus the VNINDEX return over the same "
        f"window (gold_schema.stock_market.hose__vnindex__close_adjust). ⚠️ The "
        f"relative target exists because a single stock's ABSOLUTE forward return is "
        f"dominated by the market factor, which no company-level feature predicts; "
        f"subtracting the index leaves the part a stock-specific feature could "
        f"explain. ⚠️ It reads gold.stock_market, NOT the retired gold.indices the "
        f"old notebook used. ⚠️ Sourced from pool__basic, not gold.stocks, so the "
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
        relative_cols = {h: f"return_rel_{h}day" for h in horizons}
        # Keyed so absolute and relative never collide: `h` for absolute, `-h` for
        # the relative twin.
        all_targets = {**target_cols, **{-h: c for h, c in relative_cols.items()}}
        prep_benchmark_table = prep.UNIFIED_BENCHMARK_TABLE
        prep_benchmark_column = prep.UNIFIED_BENCHMARK_COLUMN

        with prep._database_driver._cursor_ctx() as cur:
            stats = {}
            for h, target_col in all_targets.items():
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
            primary_key = _primary_key(cur, "pool__targets")
            expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

    if primary_key != expected_key:
        raise ValueError(
            f"{UNIFIED_SCHEMA_NAME}.pool__targets primary key is {primary_key}, "
            f"expected {expected_key} — order is part of the contract."
        )
    expected = list(expected_key) + list(target_cols.values()) + list(
        relative_cols.values()
    )
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
            f"{col} {int(stats[key][0])} labelled + {rows - int(stats[key][0])} null, "
            f"range {float(stats[key][1]):.4f} → {float(stats[key][2]):.4f}, "
            f"mean {float(stats[key][3]):.5f}"
            for key, col in all_targets.items()
        )
    )
    metadata = {
        "rows": int(rows),
        "horizons": MetadataValue.text(", ".join(str(h) for h in horizons)),
        "targets": MetadataValue.text(", ".join(expected[len(expected_key):])),
        "primary_key": MetadataValue.text(", ".join(primary_key)),
        "benchmark": MetadataValue.text(
            f"{prep_benchmark_table}.{prep_benchmark_column}"
        ),
        "date_range": MetadataValue.text(f"{first} → {last}"),
        "table": MetadataValue.text(f"{UNIFIED_SCHEMA_NAME}.pool__targets"),
    }
    for key, target_col in all_targets.items():
        labelled, lo, hi, mean = stats[key]
        metadata[f"{target_col}__labelled"] = int(labelled)
        metadata[f"{target_col}__unlabelled_tail"] = int(rows - int(labelled))
        metadata[f"{target_col}__min"] = MetadataValue.float(round(float(lo), 6))
        metadata[f"{target_col}__max"] = MetadataValue.float(round(float(hi), 6))
        metadata[f"{target_col}__mean"] = MetadataValue.float(round(float(mean), 6))
    return MaterializeResult(metadata=metadata)


# ── The two FEATURE pools: pool__ta and pool__fa ─────────────────────────────────
#
# Both are one gold table sliced to this ticker and re-keyed onto `pool__basic`'s
# calendar, so both take TWO deps: the gold source, and `pool__basic` for the
# calendar they must land on.
#
# ⚠️ Neither existed as an asset until 2026-08-05 — `unified_schema_creator.ipynb`
# built them, which is why the whole schema was lost when it was dropped on
# 2026-08-03 and only `pool__basic` came back. A notebook is not a run plan.
#
# ⚠️ THE EXCLUSIONS ARE THE INTERESTING PART, and they live in the methods, not here.
# `gold.stocks_financials_bank_fa` IS the FA block merged onto the TA one — 906 of
# its 1,150 columns are `gold.stocks_ta` columns — so `pool__fa` excludes the TA
# names by INTERSECTION rather than a prefix guess. Without that the two pools would
# be 906-way duplicates of each other and the correlation prune would spend its
# entire budget rediscovering that.
#
# (asset name, gold source asset, what it is)
FEATURE_POOLS: list[tuple[str, str, str]] = [
    (
        "pool__ta",
        "stocks_ta",
        "~920 technical indicator columns — Bollinger/MACD/RSI/Hilbert families plus "
        "their slopes, crossings and boolean flags. ⚠️ ~207 are BOOLEAN and "
        "FeatureSelector._prepare excludes bool dtypes, so they are stored but not "
        "scored until someone decides how to encode them.",
    ),
    (
        "pool__fa",
        "stocks_financials_bank_fa",
        "~204 fundamental columns — balance sheet (93), cash flow (50), income "
        "statement (29), share counts, eps/bvps/ttm_*, forward-filled to a DAILY "
        "grain. ⚠️ `publish_date` is the only thing stopping this being a time "
        "machine: a quarter is attached to a price day only once PUBLISHED, a median "
        "54 days after the period ends.",
    ),
]


def _build_unified_feature_pool(name: str, gold_source: str, what: str):
    @asset(
        name=name,
        key_prefix=["unified_vcb"],
        group_name="unified_vcb",
        compute_kind="postgres",
        deps=[
            AssetKey(["gold", gold_source]),
            AssetKey(["unified_vcb", "pool__basic"]),
        ],
        description=(
            f"gold.{gold_source} → {UNIFIED_SCHEMA_NAME}.{name}, PK "
            f"(date, exchange, ticker), on pool__basic's calendar. {what}"
        ),
    )
    def _feature_pool(
        context: AssetExecutionContext, preprocessor: PreprocessorResource
    ) -> MaterializeResult:
        with preprocessor.session(schema=UNIFIED_SCHEMA_NAME) as prep:
            getattr(prep, f"_ingest_unified_{name.replace('__', '_')}")(UNIFIED_TICKER)
            expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

            with prep._database_driver._cursor_ctx() as cur:
                cur.execute(
                    f"SELECT COUNT(*), MIN(date), MAX(date) "
                    f"FROM {UNIFIED_SCHEMA_NAME}.{name}"
                )
                rows, first, last = cur.fetchone()
                rows = int(rows)
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (UNIFIED_SCHEMA_NAME, name),
                )
                columns = int(cur.fetchone()[0])
                cur.execute(
                    f"SELECT COUNT(*) FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
                )
                basic_rows = int(cur.fetchone()[0])
                # The pools are joined on the key by every downstream selection
                # table, so a shared calendar is checked in BOTH directions rather
                # than inferred from a matching row count.
                cur.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"  SELECT date FROM {UNIFIED_SCHEMA_NAME}.{name}"
                    f"  EXCEPT SELECT date FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
                    f"  UNION ALL"
                    f"  SELECT date FROM {UNIFIED_SCHEMA_NAME}.pool__basic"
                    f"  EXCEPT SELECT date FROM {UNIFIED_SCHEMA_NAME}.{name}"
                    f") d"
                )
                unaligned = int(cur.fetchone()[0])
                cur.execute(
                    f"SELECT COUNT(DISTINCT ticker) FROM {UNIFIED_SCHEMA_NAME}.{name}"
                )
                tickers = int(cur.fetchone()[0])
                primary_key = _primary_key(cur, name)

        if primary_key != expected_key:
            raise ValueError(
                f"{UNIFIED_SCHEMA_NAME}.{name} primary key is {primary_key}, expected "
                f"{expected_key} — order is part of the contract."
            )
        if unaligned:
            raise ValueError(
                f"{UNIFIED_SCHEMA_NAME}.{name} and pool__basic disagree on "
                f"{unaligned} date(s) — every pool must share one calendar to join."
            )
        if rows != basic_rows:
            raise ValueError(
                f"{UNIFIED_SCHEMA_NAME}.{name} has {rows} rows against pool__basic's "
                f"{basic_rows}. A feature pool adds columns, never rows."
            )
        if tickers != 1:
            raise ValueError(
                f"{UNIFIED_SCHEMA_NAME}.{name} holds {tickers} tickers; a "
                f"single-company schema must hold exactly 1."
            )

        context.log.info(
            f"{UNIFIED_SCHEMA_NAME}.{name}: {rows} rows × {columns} columns "
            f"({first} → {last})"
        )
        return MaterializeResult(
            metadata={
                "rows": rows,
                "columns": columns,
                "features": columns - len(expected_key),
                "primary_key": MetadataValue.text(", ".join(primary_key)),
                "source": MetadataValue.text(f"gold_schema.{gold_source}"),
                "date_range": MetadataValue.text(f"{first} → {last}"),
                "table": MetadataValue.text(f"{UNIFIED_SCHEMA_NAME}.{name}"),
            }
        )

    return _feature_pool


feature_pool_assets: List[Callable] = [
    _build_unified_feature_pool(*spec) for spec in FEATURE_POOLS
]


assets: List[Callable] = [
    unified_vcb_pool_basic,
    unified_vcb_pool_targets,
    *feature_pool_assets,
]
