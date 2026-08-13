# src\orchestration\assets\unified.py
"""The UNIFIED layer — `silver_schema` / `gold_schema` → `unified_schema_<universe>`.

The fourth layer, and the only PARTITIONED one in the database half of the pipeline.
Where bronze / silver / gold each hold the whole market in one table, a unified schema
holds ONE UNIVERSE cut into the FEATURE GROUPS a model selects over:

    pool__basic      the price/flow panel, every column of silver.stocks_basic
    pool__targets    the labels — forward returns (absolute and index-relative) and
                     the forward adjusted close itself
    pool__economy    all country macro panels, already causal in gold
    pool__forex      357 broker-quoted FX pairs, one `value` column each
    pool__ta         the ~920-column technical block
    pool__fa         the ~200-column fundamental block

⚠️ **TWO JOIN SHAPES, and which one a pool uses is decided by its SOURCE's key.**
`pool__ta` / `pool__fa` come from tables already keyed `(date, exchange, ticker)` and
INNER JOIN the spine on all three. `pool__economy` and `pool__forex` come from tables
keyed on `date` ALONE, so they LEFT JOIN on `date` and BROADCAST one row across every
ticker of that day — which is why their row count must EQUAL the spine's, while a
per-ticker pool is allowed to cover a subset.

⚠️ **THE PARTITION IS THE UNIVERSE, and all six pools share it** (2026-08-05). Until
then this module was hard-coded to `UNIFIED_TICKER = "VCB"` and the asset keys carried
the ticker (`unified_vcb/pool__basic`), which meant `unified_schema_all` and
`unified_schema_bank` — 4.9 M rows between them — existed in the database but were
built by calling the methods by hand and appeared nowhere in the graph. The comment
that used to sit on that constant said this became a partition "when the rest of the
schema moves here". It has.

    VCB    one company                            4,235 rows
    ALL    every ticker silver holds              2,388,368 rows, 781 tickers
    BANK   GICS industry_code 401010              53,921 rows, 20 tickers

⚠️ **`ALL` and `BANK` are SENTINELS, not tickers**, and the library already knew it:
`DataPreprocessor.UNIFIED_MEMBER_FILTERS` maps each to the `silver.stocks_basic`
predicate that selects its members, `None` meaning no predicate at all. Membership is
DERIVED — `BANK` reads the GICS classification silver already carries — so a bank
listing or being reclassified is picked up by a rebuild rather than by editing a list.
Adding a universe is one entry in that dict plus one partition key here.

⚠️ **The single-company assertions are GATED, not deleted.** `COUNT(DISTINCT ticker) =
1` is right for `VCB` and wrong for `ALL`, so every such check asks
`_helper_unified_is_universe` first. That is the change CONTEXT flagged as the
blocker to partitioning: an assertion that is correct for one partition and false for
another has to know which it is looking at.

⚠️ **THE SCHEMA IS CREATED BY THE ASSET, not by hand.** `PreprocessorResource.session`
issues `CREATE SCHEMA IF NOT EXISTS` for whatever schema it is handed, so naming
`unified_schema_<universe>` there is what brings it into existence.

⚠️ **The universe is an IDENTIFIER here, not a value.** It is interpolated into a
schema NAME, which cannot be a bound parameter — `_helper_unified_schema` validates it
against `UNIFIED_TICKER_PATTERN` and raises rather than interpolating anything else.
A Dagster partition key is exactly the "arrives from somewhere this class does not
control" case that validation exists for.
"""

from typing import Callable, List

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration import enabled
from orchestration.resources import PreprocessorResource

# ⚠️ The two sentinels here must stay in step with
# `DataPreprocessor.UNIFIED_MEMBER_FILTERS`; everything else is an ordinary ticker.
# Kept as literals rather than read off the class because a partition set has to be
# known at DEFINITION time, before any database connection exists.
#
# One universe at a time can be switched off from `config.json` —
# `"partitions": {"unified": {"ALL": false}}` — under the GROUP name `unified`, because all four `pool__*`
# assets share this object and a pool offered for a universe its spine does not have
# would be a broken edge.
UNIFIED_PARTITIONS = StaticPartitionsDefinition(
    enabled.register("unified", ["VCB", "ALL", "BANK"])
)


def _schema_of(universe: str) -> str:
    """`unified_schema_vcb` for `VCB`. Mirrors `_helper_unified_schema`, which is what
    actually validates the name — this is only for building SQL in the asset body."""
    return f"unified_schema_{universe.lower()}"


def _primary_key(cur, schema: str, table: str) -> tuple:
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
        (schema, table),
    )
    return tuple(row[0] for row in cur.fetchall())


def _member_count(prep, cur, universe: str) -> int:
    """Rows of `silver.stocks_basic` this universe should produce.

    Built from `_helper_unified_member_filter` rather than a hard-coded
    `ticker = %s`, so it is the SAME predicate the builder used — a count derived
    independently would only be checking this function against itself.
    """
    predicate, params = prep._helper_unified_member_filter(universe)
    where = f"WHERE {predicate}" if predicate else ""
    cur.execute(f"SELECT COUNT(*) FROM silver_schema.stocks_basic {where}", params)
    return int(cur.fetchone()[0])


@asset(
    name="pool__basic",
    key_prefix=["unified"],
    group_name="unified",
    compute_kind="postgres",
    partitions_def=UNIFIED_PARTITIONS,
    deps=[AssetKey(["silver", "stocks_basic"])],
    description=(
        "silver.stocks_basic (this partition's universe) → "
        "unified_schema_<universe>.pool__basic: EVERY column of the silver table, with "
        "silver's own types, PK (date, exchange, ticker) — DataPreprocessor."
        "UNIFIED_PRIMARY_KEY, and the ORDER is asserted. ⚠️ Built with CREATE TABLE "
        "AS, not a pandas round-trip: psycopg2 returns `numeric` as Decimal, which a "
        "DataFrame carries as dtype `object` and the writer maps to VARCHAR — a "
        "round-trip would silently turn every price column into TEXT. ⚠️ Creates the "
        "schema if absent and REPLACES the table; sibling pool__* tables are untouched."
    ),
)
def unified_pool_basic(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    universe = context.partition_key
    schema = _schema_of(universe)

    # ⚠️ `session(schema=...)` is what CREATES the schema — see the module docstring.
    with preprocessor.session(schema=schema) as prep:
        prep._ingest_unified_pool_basic(universe)
        is_universe = prep._helper_unified_is_universe(universe)
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                f"FROM {schema}.pool__basic"
            )
            rows, tickers, first, last = cur.fetchone()
            rows, tickers = int(rows), int(tickers)
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'pool__basic'",
                (schema,),
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'silver_schema' AND table_name = 'stocks_basic'"
            )
            silver_columns = int(cur.fetchone()[0])
            silver_rows = _member_count(prep, cur, universe)
            # ⚠️ The column set is the ASSERTION, not a metric. "All columns of
            # silver.stocks_basic" is this asset's contract, and a CTAS that silently
            # dropped or gained one would still produce a plausible-looking table.
            cur.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'silver_schema' AND table_name = 'stocks_basic'
                   EXCEPT
                   SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = 'pool__basic'""",
                (schema,),
            )
            missing = [r[0] for r in cur.fetchall()]
            primary_key = _primary_key(cur, schema, "pool__basic")

    # ⚠️ The key ORDER is asserted, not just its membership. `ADD PRIMARY KEY`
    # accepts any ordering of the same three columns, and only a leading `date`
    # lets the index serve the time-range scans every consumer here does.
    if primary_key != expected_key:
        raise ValueError(
            f"{schema}.pool__basic primary key is {primary_key}, expected "
            f"{expected_key} — order is part of the contract."
        )
    if missing:
        raise ValueError(
            f"{schema}.pool__basic is missing {len(missing)} column(s) of "
            f"silver.stocks_basic: {sorted(missing)}"
        )
    if rows != silver_rows:
        raise ValueError(
            f"{schema}.pool__basic has {rows} rows but silver holds {silver_rows} for "
            f"universe {universe!r}."
        )
    # ⚠️ GATED, not dropped. One company is the contract for a ticker partition and
    # flatly wrong for `ALL` — this is the assertion that had to learn which partition
    # it was looking at before the module could be partitioned at all.
    if not is_universe and tickers != 1:
        raise ValueError(
            f"{schema}.pool__basic holds {tickers} tickers — a single-company unified "
            f"schema is one company by definition."
        )
    if is_universe and tickers < 2:
        raise ValueError(
            f"{schema}.pool__basic holds {tickers} ticker(s), but {universe!r} names a "
            f"SET of companies. A sentinel that resolves to one series means its "
            f"member filter matched almost nothing."
        )

    context.log.info(
        f"{schema}.pool__basic: {rows} rows × {columns} columns, {tickers} ticker(s) "
        f"({first} → {last}), every column of silver.stocks_basic present"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "tickers": tickers,
            "silver_columns": silver_columns,
            "universe": MetadataValue.text(universe),
            "primary_key": MetadataValue.text(", ".join(primary_key)),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(f"{schema}.pool__basic"),
        }
    )


@asset(
    name="pool__targets",
    key_prefix=["unified"],
    group_name="unified",
    compute_kind="postgres",
    partitions_def=UNIFIED_PARTITIONS,
    deps=[AssetKey(["unified", "pool__basic"])],
    description=(
        "pool__basic → pool__targets: PK (date, exchange, ticker) — the same key as "
        "every other pool, so a join needs no special case — plus THREE COLUMNS PER "
        "HORIZON in DataPreprocessor.UNIFIED_TARGET_HORIZONS: `return_{h}day`, the "
        "forward simple return close[t+h]/close[t]-1 on the SPLIT-ADJUSTED close, "
        "`return_rel_{h}day`, the same minus the VNINDEX return over that window "
        "(gold_schema.stock_market.hose__vnindex__close_adjust), and "
        "`close_adjust_{h}day`, the forward adjusted close close[t+h] itself. ⚠️ The "
        "relative target exists because a single stock's ABSOLUTE forward return is "
        "dominated by the market factor, which no company-level feature predicts. "
        "⚠️ `close_adjust_{h}day` IS A LABEL — it is LEAD(close_adjust, h), the answer "
        "in price units — and it reads like a pool__basic column, so anything joining "
        "pool__basic ⋈ pool__targets must exclude it from its features. ⚠️ Sourced "
        "from pool__basic, not gold.stocks, so features and labels share one calendar "
        "by construction. ⚠️ Each column's last h rows are NULL (their future does not "
        "exist yet) and are KEPT so the table still joins."
    ),
)
def unified_pool_targets(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    universe = context.partition_key
    schema = _schema_of(universe)

    with preprocessor.session(schema=schema) as prep:
        prep._ingest_unified_pool_targets(universe)
        is_universe = prep._helper_unified_is_universe(universe)
        horizons = tuple(prep.UNIFIED_TARGET_HORIZONS)
        target_cols = {h: f"return_{h}day" for h in horizons}
        relative_cols = {h: f"return_rel_{h}day" for h in horizons}
        price_cols = {h: f"close_adjust_{h}day" for h in horizons}
        # ⚠️ Keyed by COLUMN NAME, in the builder's own emission order. It used to be
        # keyed by horizon with the relative twin under `-h`, which worked for exactly
        # two families and had no room for a third — `close_adjust_{h}day` would have
        # needed a second escape convention to avoid colliding with `return_{h}day`.
        all_targets = {
            c: h
            for group in (target_cols, relative_cols, price_cols)
            for h, c in group.items()
        }
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)
        benchmark = f"{prep.UNIFIED_BENCHMARK_TABLE}.{prep.UNIFIED_BENCHMARK_COLUMN}"

        with prep._database_driver._cursor_ctx() as cur:
            stats = {}
            for target_col in all_targets:
                cur.execute(
                    f"SELECT COUNT({target_col}), MIN({target_col}), "
                    f"       MAX({target_col}), AVG({target_col}) "
                    f"FROM {schema}.pool__targets"
                )
                stats[target_col] = cur.fetchone()
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                f"FROM {schema}.pool__targets"
            )
            rows, tickers, first, last = cur.fetchone()
            rows, tickers = int(rows), int(tickers)
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'pool__targets' "
                "ORDER BY ordinal_position",
                (schema,),
            )
            columns = [r[0] for r in cur.fetchall()]
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            basic_rows = int(cur.fetchone()[0])
            # ⚠️ The join to the feature pools is the whole reason this table exists,
            # so it is checked rather than assumed: every target date must be a
            # pool__basic date and vice versa.
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT date FROM {schema}.pool__targets"
                f"  EXCEPT SELECT date FROM {schema}.pool__basic"
                f"  UNION ALL"
                f"  SELECT date FROM {schema}.pool__basic"
                f"  EXCEPT SELECT date FROM {schema}.pool__targets"
                f") d"
            )
            unaligned = int(cur.fetchone()[0])
            primary_key = _primary_key(cur, schema, "pool__targets")

    if primary_key != expected_key:
        raise ValueError(
            f"{schema}.pool__targets primary key is {primary_key}, expected "
            f"{expected_key} — order is part of the contract."
        )
    # ⚠️ The ORDER is the builder's `CREATE TABLE AS` select list, not an alphabetical
    # or a per-horizon grouping: keys, then every `return_{h}day`, then every
    # `return_rel_{h}day`, then every `close_adjust_{h}day`. This is an equality check,
    # so a family appended in a different place here fails a correct table.
    expected = list(expected_key) + list(all_targets)
    if columns != expected:
        raise ValueError(
            f"{schema}.pool__targets should hold exactly {expected}, got {columns}."
        )
    if unaligned:
        raise ValueError(
            f"{schema}.pool__targets and pool__basic disagree on {unaligned} date(s) — "
            f"they must share one calendar to join."
        )
    if rows != basic_rows:
        raise ValueError(
            f"{schema}.pool__targets has {rows} rows against pool__basic's "
            f"{basic_rows}."
        )
    # ⚠️ The unlabelled tail is PER SERIES, so on a multi-ticker universe the total is
    # h × the number of tickers — a single check against `h` would fail on `ALL` for a
    # table that is perfectly correct. On one company the two are the same number,
    # which is exactly why the old check looked right.
    #
    # ⚠️ `close_adjust_{h}day` is held to the SAME exact tail. It is `return_{h}day`'s
    # numerator with no denominator to guard, so any NULL outside the tail means the
    # forward price itself is missing — and a model predicting a LEVEL would train on
    # whatever the imputer put there.
    for h in horizons:
        for target_col in (target_cols[h], price_cols[h]):
            tail = rows - int(stats[target_col][0])
            expected_tail = h * tickers
            if tail != expected_tail:
                raise ValueError(
                    f"{schema}.pool__targets.{target_col} has a {tail}-row unlabelled "
                    f"tail; expected {expected_tail} ({h} per series × {tickers} "
                    f"series). More would mean NULL or zero closes putting silent "
                    f"holes in the labels."
                )

    context.log.info(
        f"{schema}.pool__targets: {rows} rows / {tickers} ticker(s) ({first} → {last})"
    )
    metadata: dict = {
        "rows": rows,
        "tickers": tickers,
        "universe": MetadataValue.text(universe),
        "horizons": MetadataValue.text(", ".join(str(h) for h in horizons)),
        "targets": MetadataValue.text(", ".join(expected[len(expected_key):])),
        "primary_key": MetadataValue.text(", ".join(primary_key)),
        "benchmark": MetadataValue.text(benchmark),
        "date_range": MetadataValue.text(f"{first} → {last}"),
        "table": MetadataValue.text(f"{schema}.pool__targets"),
    }
    for target_col in all_targets:
        labelled, lo, hi, mean = stats[target_col]
        metadata[f"{target_col}__labelled"] = int(labelled)
        metadata[f"{target_col}__unlabelled_tail"] = rows - int(labelled)
        if labelled:
            metadata[f"{target_col}__min"] = MetadataValue.float(round(float(lo), 6))
            metadata[f"{target_col}__max"] = MetadataValue.float(round(float(hi), 6))
            metadata[f"{target_col}__mean"] = MetadataValue.float(round(float(mean), 6))
    return MaterializeResult(metadata=metadata)


@asset(
    name="pool__economy",
    key_prefix=["unified"],
    group_name="unified",
    compute_kind="postgres",
    partitions_def=UNIFIED_PARTITIONS,
    deps=[
        AssetKey(["gold", "economy"]),
        AssetKey(["unified", "pool__basic"]),
    ],
    description=(
        "gold.economy_<country> → unified_schema_<universe>.pool__economy_<country>: "
        "one table per country, each on pool__basic's (date, exchange, ticker) spine. "
        "A single combined pool would exceed PostgreSQL's 1,600-column limit, so this "
        "asset preserves gold's 19-panel storage shape. It does NOT forward-fill a "
        "second time: gold owns publication lags and staleness caps."
    ),
)
def unified_pool_economy(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    universe = context.partition_key
    schema = _schema_of(universe)

    with preprocessor.session(schema=schema) as prep:
        panels = prep._ingest_unified_pool_economy(universe)
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "AND LEFT(table_name, CHAR_LENGTH(%s)) = %s ORDER BY table_name",
                (schema, "pool__economy_", "pool__economy_"),
            )
            actual_tables = [str(row[0]) for row in cur.fetchall()]
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            basic_rows = int(cur.fetchone()[0])
            for panel in panels:
                table = panel["table"]
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                rows = int(cur.fetchone()[0])
                if rows != basic_rows:
                    raise ValueError(
                        f"{schema}.{table} has {rows} rows against pool__basic's "
                        f"{basic_rows}. A macro pool adds columns, never rows."
                    )
                primary_key = _primary_key(cur, schema, table)
                if primary_key != expected_key:
                    raise ValueError(
                        f"{schema}.{table} primary key is {primary_key}, expected "
                        f"{expected_key} — order is part of the contract."
                    )

    expected_tables = sorted(panel["table"] for panel in panels)
    if actual_tables != expected_tables:
        raise ValueError(
            f"{schema}.pool__economy_* tables are {actual_tables}, expected "
            f"{expected_tables}. A stale or missing country panel is not a complete "
            "macro pool."
        )
    total_features = sum(panel["features"] for panel in panels)
    populated_rows = sum(panel["populated_rows"] for panel in panels)
    total_rows = len(panels) * basic_rows
    coverage = 100.0 * populated_rows / max(total_rows, 1)
    context.log.info(
        f"{schema}.pool__economy_*: {len(panels)} country panels, {total_features} "
        f"macro series total; {coverage:.1f}% panel-row coverage on pool__basic's spine"
    )
    return MaterializeResult(
        metadata={
            "country_panels": len(panels),
            "rows_per_panel": basic_rows,
            "features_total": total_features,
            "populated_panel_rows": populated_rows,
            "panel_row_coverage_pct": MetadataValue.float(round(coverage, 2)),
            "universe": MetadataValue.text(universe),
            "primary_key": MetadataValue.text(", ".join(expected_key)),
            "source": MetadataValue.text("gold_schema.economy_<country>"),
            "tables": MetadataValue.text(", ".join(expected_tables)),
        }
    )


@asset(
    name="pool__forex",
    key_prefix=["unified"],
    group_name="unified",
    compute_kind="postgres",
    partitions_def=UNIFIED_PARTITIONS,
    deps=[
        AssetKey(["gold", "forex"]),
        AssetKey(["unified", "pool__basic"]),
    ],
    description=(
        "gold.forex → unified_schema_<universe>.pool__forex: 357 broker-quoted FX "
        "pairs, ONE `value` column each named {exchange}__{ticker} (e.g. "
        "`saxo__eurusd`), on pool__basic's (date, exchange, ticker) spine. ⚠️ The "
        "source is keyed on `date` ALONE, so the join BROADCASTS one FX row across "
        "every ticker of the day — the pool__economy shape, not the pool__ta one. "
        "⚠️ NOT forward-filled: a NULL means that broker did not quote that pair that "
        "day, and filling it would invent a price. ⚠️ THE 9 EXCHANGES ARE 9 BROKERS "
        "and must not be collapsed — SAXO and JFX disagree on 160,781 of 161,816 "
        "shared ticker-days. ⚠️ 328 of 357 series stop at 2026-06-08/09 (a "
        "skip_existing=True scrape), so the recent tail is mostly NULL however healthy "
        "MAX(date) looks."
    ),
)
def unified_pool_forex(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    universe = context.partition_key
    schema = _schema_of(universe)

    with preprocessor.session(schema=schema) as prep:
        panel = prep._ingest_unified_pool_forex(universe)
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                f"FROM {schema}.pool__forex"
            )
            rows, tickers, first, last = cur.fetchone()
            rows, tickers = int(rows), int(tickers)
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            basic_rows = int(cur.fetchone()[0])
            primary_key = _primary_key(cur, schema, "pool__forex")
            # ⚠️ The LAST date carrying a quote, not MAX(date) — those are different
            # numbers here and the difference is the whole staleness story. A pool
            # built on a spine that runs 2 months past its source looks perfectly
            # healthy on MAX(date) and is NULL for every one of those rows.
            cur.execute(
                f"SELECT MAX(date) FROM {schema}.pool__forex f "
                f"WHERE EXISTS (SELECT 1 FROM {prep.UNIFIED_FOREX_SOURCE} g "
                f"              WHERE g.date = f.date)"
            )
            last_quoted = cur.fetchone()[0]

    if primary_key != expected_key:
        raise ValueError(
            f"{schema}.pool__forex primary key is {primary_key}, expected "
            f"{expected_key} — order is part of the contract."
        )
    if rows != basic_rows:
        raise ValueError(
            f"{schema}.pool__forex has {rows} rows against pool__basic's "
            f"{basic_rows}. An FX pool adds columns, never rows."
        )

    coverage = 100.0 * panel["populated_rows"] / max(rows, 1)
    context.log.info(
        f"{schema}.pool__forex: {rows} rows × {panel['features']} FX series, "
        f"{tickers} ticker(s) ({first} → {last}) — {coverage:.1f}% of rows carry at "
        f"least one quote; last spine date with any FX row is {last_quoted}"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": panel["features"] + len(expected_key),
            "features": panel["features"],
            "tickers": tickers,
            "pool__basic_rows": basic_rows,
            "populated_rows": panel["populated_rows"],
            "row_coverage_pct": MetadataValue.float(round(coverage, 2)),
            "universe": MetadataValue.text(universe),
            "primary_key": MetadataValue.text(", ".join(primary_key)),
            "source": MetadataValue.text(panel["source"]),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "last_date_with_fx_row": MetadataValue.text(str(last_quoted)),
            "table": MetadataValue.text(f"{schema}.pool__forex"),
        }
    )


# ── The two FEATURE pools: pool__ta and pool__fa ─────────────────────────────────
#
# Both are one gold table sliced to this universe and re-keyed onto `pool__basic`'s
# calendar, so both take TWO deps: the gold source, and `pool__basic`.
#
# ⚠️ Neither existed as an asset until 2026-08-05 — `unified_schema_creator.ipynb`
# built them, which is why the whole schema was lost when it was dropped on
# 2026-08-03 and only `pool__basic` came back. A notebook is not a run plan.
#
# ⚠️ THE EXCLUSIONS ARE THE INTERESTING PART, and they live in the methods, not here.
# `gold.stocks_financials_bank_fa` IS the FA block merged onto the TA one — 906 of
# its 1,150 columns are `gold.stocks_ta` columns — so `pool__fa` excludes the TA
# names by INTERSECTION rather than a prefix guess. Without that the two pools would
# be 906-way duplicates and the correlation prune would spend its whole budget
# rediscovering that.
#
# ⚠️ `pool__fa` IS BANK-ONLY BY CONSTRUCTION. `gold.stocks_financials_bank_fa` is
# built from the CafeF *bank* chart of accounts and holds VCB and ACB alone, so on
# the `ALL` partition this pool covers a small fraction of the universe's tickers.
# That is the source's shape, not a defect here — hence the row check below asserts
# alignment with the pool it joins, never with `pool__basic`'s row count.
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
        "54 days after the period ends. ⚠️ BANK TICKERS ONLY — the source is the "
        "CafeF bank chart of accounts.",
    ),
]


def _build_unified_feature_pool(name: str, gold_source: str, what: str):
    @asset(
        name=name,
        key_prefix=["unified"],
        group_name="unified",
        compute_kind="postgres",
        partitions_def=UNIFIED_PARTITIONS,
        deps=[
            AssetKey(["gold", gold_source]),
            AssetKey(["unified", "pool__basic"]),
        ],
        description=(
            f"gold.{gold_source} → unified_schema_<universe>.{name}, PK "
            f"(date, exchange, ticker), on pool__basic's calendar. {what}"
        ),
    )
    def _feature_pool(
        context: AssetExecutionContext, preprocessor: PreprocessorResource
    ) -> MaterializeResult:
        universe = context.partition_key
        schema = _schema_of(universe)

        with preprocessor.session(schema=schema) as prep:
            getattr(prep, f"_ingest_unified_{name.replace('__', '_')}")(universe)
            is_universe = prep._helper_unified_is_universe(universe)
            expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

            with prep._database_driver._cursor_ctx() as cur:
                cur.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                    f"FROM {schema}.{name}"
                )
                rows, tickers, first, last = cur.fetchone()
                rows, tickers = int(rows), int(tickers)
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, name),
                )
                columns = int(cur.fetchone()[0])
                cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
                basic_rows = int(cur.fetchone()[0])
                # ⚠️ Checked in BOTH directions and ONE-SIDED on purpose: every row of
                # this pool must correspond to a pool__basic row, but NOT the reverse.
                # `pool__fa` legitimately covers a subset of tickers (bank chart of
                # accounts), so requiring the reverse would fail a correct table.
                cur.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"  SELECT date, exchange, ticker FROM {schema}.{name}"
                    f"  EXCEPT"
                    f"  SELECT date, exchange, ticker FROM {schema}.pool__basic"
                    f") d"
                )
                orphaned = int(cur.fetchone()[0])
                primary_key = _primary_key(cur, schema, name)

        if primary_key != expected_key:
            raise ValueError(
                f"{schema}.{name} primary key is {primary_key}, expected "
                f"{expected_key} — order is part of the contract."
            )
        if orphaned:
            raise ValueError(
                f"{schema}.{name} has {orphaned} row(s) whose (date, exchange, ticker) "
                f"is not in pool__basic — every pool must sit on the spine's calendar "
                f"to join."
            )
        if rows == 0:
            raise ValueError(
                f"{schema}.{name} is EMPTY against pool__basic's {basic_rows} rows."
            )
        if rows > basic_rows:
            raise ValueError(
                f"{schema}.{name} has {rows} rows against pool__basic's {basic_rows}. "
                f"A feature pool adds columns, never rows."
            )
        if not is_universe and tickers != 1:
            raise ValueError(
                f"{schema}.{name} holds {tickers} tickers; a single-company schema "
                f"must hold exactly 1."
            )
        # ⚠️ On a SINGLE-COMPANY schema the pool must still cover the whole spine.
        # The subset allowance exists because a source covers fewer TICKERS than the
        # universe; it is not a licence to lose days for a company the source has.
        if not is_universe and rows != basic_rows:
            raise ValueError(
                f"{schema}.{name} has {rows} rows against pool__basic's {basic_rows}. "
                f"On a one-company schema the source either covers that company or it "
                f"does not — a partial calendar means days went missing."
            )

        # ⚠️ COVERAGE IS REPORTED, NOT ASSERTED, and that is the 2026-08-05 decision.
        # A feature pool is as wide as its SOURCE: `gold.stocks_financials_bank_fa`
        # holds VCB and ACB alone, so `pool__fa` covers 15% of a BANK spine and 0.3%
        # of ALL. Demanding equality made the table unbuildable rather than safe. The
        # number goes in the metadata so a pool that silently shrank is VISIBLE.
        coverage = 100.0 * rows / max(basic_rows, 1)
        context.log.info(
            f"{schema}.{name}: {rows} rows × {columns} columns, {tickers} ticker(s) "
            f"({first} → {last}) — {coverage:.1f}% of pool__basic's {basic_rows} rows"
        )
        return MaterializeResult(
            metadata={
                "rows": rows,
                "columns": columns,
                "features": columns - len(expected_key),
                "tickers": tickers,
                "pool__basic_rows": basic_rows,
                "spine_coverage_pct": MetadataValue.float(round(coverage, 2)),
                "universe": MetadataValue.text(universe),
                "primary_key": MetadataValue.text(", ".join(primary_key)),
                "source": MetadataValue.text(f"gold_schema.{gold_source}"),
                "date_range": MetadataValue.text(f"{first} → {last}"),
                "table": MetadataValue.text(f"{schema}.{name}"),
            }
        )

    return _feature_pool


feature_pool_assets: List[Callable] = [
    _build_unified_feature_pool(*spec) for spec in FEATURE_POOLS
]


assets: List[Callable] = [
    unified_pool_basic,
    unified_pool_targets,
    unified_pool_economy,
    unified_pool_forex,
    *feature_pool_assets,
]
