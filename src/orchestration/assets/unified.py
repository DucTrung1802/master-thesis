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
    pool__funds      21 HOSE ETFs × up to 19 measures
    pool__bonds      9 VN government tenors × 13 measures — the yield curve
    pool__stock_market  6 VN indices × 27 measures — price, order flow, foreign, prop
    pool__basic_bank    20 GICS-401010 banks × 27 measures, PIVOTED to peer channels
    pool__ta         the ~920-column technical block
    pool__fa         the ~200-column fundamental block

⚠️ **TWO JOIN SHAPES, and which one a pool uses is decided by its SOURCE's key.**
`pool__ta` / `pool__fa` come from tables already keyed `(date, exchange, ticker)` and
INNER JOIN the spine on all three. `pool__economy`, `pool__forex`, `pool__funds`,
`pool__bonds` and `pool__stock_market` come from tables keyed on `date` ALONE, so they
LEFT JOIN on `date` and BROADCAST one row across every ticker of that day — which is why
their row count must EQUAL the spine's, while a per-ticker pool is allowed a subset.

⚠️ **THE PARTITION IS THE UNIVERSE, and all nine pools share it** (2026-08-05). Until
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
    enabled.register(
        "unified",
        # ⚠️ The sentinels first, then one schema per company for the SINGLE-STOCK
        # track. They need no `UNIFIED_MEMBER_FILTERS` entry — an unlisted key falls
        # through to `ticker = %s`, which is exactly right for one company.
        # ⚠️ VCB is a sentinel-adjacent single name AND a VN30 constituent; the list
        # must stay deduplicated or StaticPartitionsDefinition raises on it.
        [
            "VCB", "ALL", "BANK", "VN30",
            "ACB", "BCM", "BID", "BVH", "CTG", "FPT",
            "GAS", "GVR", "HDB", "HPG", "MBB", "MSN",
            "MWG", "PLX", "POW", "SAB", "SHB", "SSB",
            "SSI", "STB", "TCB", "TPB", "VHM", "VIB",
            "VIC", "VJC", "VNM", "VPB", "VRE",
        ],
    )
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
        "UNIFIED_PRIMARY_KEY, and the ORDER is asserted — PLUS the drv_* derived "
        "block. ⚠️ IT STOPPED BEING A FAITHFUL COPY ON 2026-08-16: ~58 trailing "
        "derived channels are now computed in SQL beside the silver columns (bar "
        "shape, range-based volatility, trailing normalisation, order-flow imbalance, "
        "foreign/prop flow, liquidity), plus 5 cross-sectional ones on a universe "
        "partition only — PERCENT_RANK over one ticker is 0.0 forever, and a column "
        "known to be constant before it is written is worse than a missing one. The "
        "contract that survives is the SUBSET one, and it is still asserted: every "
        "silver column present, silver's type, silver's value. ⚠️ EVERY WINDOW IS "
        "PARTITION BY exchange, ticker AND EVERY FRAME ENDS AT CURRENT ROW — there is "
        "no FOLLOWING in the block, and on ALL a missing partition would corrupt 780 "
        "series boundaries per column. ⚠️ THE BAR IS SPLIT-ADJUSTED FIRST: silver's "
        "open/high/low are RAW and track close_raw (VCB 2009-06-30 is open=high=low="
        "close_raw=60,000 against close_adjust=9,130), so every expression reads the "
        "OHLC set rebuilt with close_adjust/close_raw. ⚠️ Built with CREATE TABLE "
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
        panel = prep._ingest_unified_pool_basic(universe)
        is_universe = prep._helper_unified_is_universe(universe)
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)
        derived_expected = list(panel["derived_columns"])
        prefix = prep.UNIFIED_DERIVED_PREFIX

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
            # ⚠️ The derived set is checked in BOTH directions, unlike the silver one.
            # Silver is a SUBSET contract ("every silver column survives"); the derived
            # block is an EQUALITY contract, because a stray column here is a CTE
            # helper (`_o`, `_gk_var`, `_m3_63`) that leaked past the explicit select
            # list — an internal of the SQL becoming a candidate feature.
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'pool__basic' "
                "AND column_name NOT IN ("
                "  SELECT column_name FROM information_schema.columns "
                "  WHERE table_schema = 'silver_schema' AND table_name = 'stocks_basic'"
                ") ORDER BY ordinal_position",
                (schema,),
            )
            derived_actual = [r[0] for r in cur.fetchall()]

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
    # ⚠️ EQUALITY, and ordered. A missing name means a derived expression silently
    # failed to reach the select list; an extra one means a CTE helper leaked and is
    # about to be offered to a selector as a feature.
    if derived_actual != derived_expected:
        raise ValueError(
            f"{schema}.pool__basic derived columns are {derived_actual}, expected "
            f"{derived_expected}. Extra names are CTE helpers that escaped the "
            f"explicit select list; missing ones are expressions that never landed."
        )
    # ⚠️ EVERY DERIVED NAME CARRIES THE PREFIX, checked rather than trusted. It is what
    # keeps this block from colliding with pool__ta's 935 columns when
    # UnifiedSchemaReader.join puts the two side by side — a collision there produces
    # _x/_y and one copy is dropped without a word.
    unprefixed = [c for c in derived_actual if not c.startswith(prefix)]
    if unprefixed:
        raise ValueError(
            f"{schema}.pool__basic has {len(unprefixed)} derived column(s) without the "
            f"{prefix!r} prefix: {unprefixed}. The prefix is what stops a join against "
            f"pool__ta from silently dropping a column."
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

    # ⚠️ COVERAGE PER DERIVED CHANNEL, not one scalar for the block. Rule 22: a scalar
    # cannot tell a late starter from a dead source, and this block spans four source
    # blocks with wildly different histories — OHLCV from 2009, order stats from 2010,
    # foreign from 2012, prop from 2023 at 3.1% market-wide. The worst three are named
    # in the metadata so a thin channel is visible without querying the table.
    populated = panel["derived_populated"]
    thinnest = sorted(populated.items(), key=lambda kv: kv[1])[:3]
    context.log.info(
        f"{schema}.pool__basic: {rows} rows × {columns} columns "
        f"({silver_columns} from silver + {len(derived_actual)} derived"
        + (", cross-sectional included" if is_universe else ", no cross-sectional block")
        + f"), {tickers} ticker(s) ({first} → {last}); thinnest derived channels "
        + ", ".join(f"{c} {100.0 * n / max(rows, 1):.1f}%" for c, n in thinnest)
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "tickers": tickers,
            "silver_columns": silver_columns,
            "derived_columns": len(derived_actual),
            "derived_empty": len(panel["derived_empty"]),
            "cross_sectional_block": MetadataValue.bool(bool(panel["cross_sectional"])),
            "derived_min_coverage_pct": MetadataValue.float(
                round(100.0 * min(populated.values()) / max(rows, 1), 2)
            ),
            "derived_thinnest": MetadataValue.text(
                ", ".join(
                    f"{c} ({100.0 * n / max(rows, 1):.1f}%)" for c, n in thinnest
                )
            ),
            "derived_empty_channels": MetadataValue.text(
                ", ".join(panel["derived_empty"]) or "none"
            ),
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
    #
    # ⚠️ **AND IT IS `min(h, n)` PER SERIES, not `h × tickers` (fixed 2026-08-17).** A
    # series SHORTER than `h` has no future for any of its rows, so it contributes `n`
    # NULLs rather than `h`. Adding the 4-week horizon exposed this: on
    # `unified_schema_all` exactly one series of 781 — `SDA`, 19 rows — makes the two
    # formulas differ by 1, and `h × tickers` failed a table that was perfectly correct.
    # ⚠️ The same rule lives in `_ingest_unified_pool_targets`; both had to be fixed,
    # which is the cost of stating one invariant in two places.
    with preprocessor.session(schema=schema) as prep:
        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.pool__basic GROUP BY exchange, ticker"
            )
            series_rows = [int(row[0]) for row in cur.fetchall()]
    for h in horizons:
        expected_tail = sum(min(h, n) for n in series_rows)
        for target_col in (target_cols[h], price_cols[h]):
            tail = rows - int(stats[target_col][0])
            if tail != expected_tail:
                raise ValueError(
                    f"{schema}.pool__targets.{target_col} has a {tail}-row unlabelled "
                    f"tail; expected {expected_tail} (sum of min({h}, series length) "
                    f"over {tickers} series). More would mean NULL or zero closes "
                    f"putting silent holes in the labels."
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


# ── The DATE-BROADCAST pools: forex, funds, bonds, stock_market ──────────────────
#
# One wide, `date`-keyed gold panel each, LEFT JOINed onto `pool__basic` and broadcast
# across its tickers. They are a spec table rather than four copies for the reason
# `bronze.py` and `gold.py` give — near-identical assets drift — and because the
# differences that matter (the source, and the ⚠️ each one carries) are visible here in
# a row instead of buried in four near-identical bodies. Three mirror the wide
# TradingView panels gold already builds from one spec table; the fourth is the CafeF
# index chain.
#
# ⚠️ THE ROW ASSERTION IS EQUALITY, NOT THE SUBSET `pool__ta`/`pool__fa` GET. Those are
# limited by their source's TICKER coverage, which is legitimately narrow. These join
# from the spine outward, so every spine row must survive — fewer means the join went
# wrong, not that the source is thin. A source that covers only part of the CALENDAR
# (gold.funds starts 2014, the VCB spine starts 2009) produces NULLs, never lost rows.
#
# ⚠️ `last_date_with_data` IS REPORTED BESIDE `date_range`, and it is the metric that
# matters on both. A pool built on a spine running months past its source looks
# perfectly healthy on MAX(date) and is NULL for every one of those rows — which is
# exactly the state `gold.forex` is in (328 of 357 series stopped 2026-06-08/09).
#
# (asset name, gold source asset, entity noun, what it is)
DATE_SPINE_POOLS: list[tuple[str, str, str, str]] = [
    (
        "pool__funds",
        "funds",
        "fund series",
        "21 HOSE-listed ETFs × up to 19 measures = 389 columns, named "
        "{exchange}__{ticker}__{measure} (e.g. `hose__e1vfvn30__close`). The measure "
        "suffix is present here and absent on pool__forex because gold.funds carries "
        "19 measures per fund and gold.forex carries one; 21 × 19 = 399 minus 10 "
        "never written, because FUEBFVND's 3 rows cannot fill a 5- or 21-day window. "
        "⚠️ EVERY MEASURE IS TRAILING — pct_change / shift(1) / bare rolling(w), "
        "verified in ta/ta_functions.py, so nothing here is a label wearing a "
        "feature's name. ⚠️ The source starts 2014-10-06 against a VCB spine starting "
        "2009-06-30, so 1,351 of 4,266 rows (31.7%) are NULL by construction and "
        "median column coverage is 17.7%. ⚠️ E1VFVN30 IS THE VN30 INDEX wearing a "
        "ticker — an ETF close is the market factor that return_rel_{h}day subtracts.",
    ),
    (
        "pool__bonds",
        "bonds",
        "tenor series",
        "9 VN government tenors × 13 measures = 117 columns, named "
        "{exchange}__{ticker}__{measure} (e.g. `tvc__vn10y__value`). ⚠️ THE SLOPE IS "
        "THE SIGNAL AND IT IS NOT A COLUMN — vn10y minus vn02y is the 10s2s slope, "
        "and the slope is what carries macro information, not any single tenor's "
        "level. The wide shape makes that a subtraction; computing it is the "
        "consumer's job. ⚠️ 9 tenors from 18 TradingView spellings, collapsed in gold "
        "after asserting the twins agree (0 differing values, 2026-08-05). ⚠️ THE "
        "WHOLE SOURCE STOPS 2026-06-08 UNIFORMLY — the scrape queued 0 bond data "
        "tasks, so the last 43 spine rows are entirely NULL. ⚠️ The 15y/20y/30y "
        "tenors start in 2018 and gold.bonds has no row for 1,017 of 4,266 VCB spine "
        "dates, so coverage is min 37.1% / median 75.9% / max 76.1%.",
    ),
    (
        "pool__market_breadth",
        "market_breadth",
        "market series",
        "8 date-keyed channels compressing the WHOLE 781-name cross-section to one row "
        "per session — mkt_xs_disp5 / mkt_xs_skew5 / mkt_xs_kurt5 / mkt_xs_mean5 "
        "(dispersion), mkt_hhi_turnover (concentration), mkt_log_turnover / "
        "mkt_turnover_z (flow), mkt_n_names (the width it was computed over). ⚠️ THIS "
        "IS THE ANSWER TO \"PUT THE WHOLE MARKET IN\" THAT ACTUALLY FITS. Pivoting 781 "
        "tickers × 27 measures is 21,087 columns against PostgreSQL's 1,600 limit "
        "(WID-1), and VCB has n_eff = 852 independent observations — §5c measured 202 "
        "channels at test IC −0.011 against 724 at −0.072 on the same ticker and "
        "splits, so width is the enemy here, not the goal. ⚠️ THE CHANNEL SET WAS "
        "CHOSEN BY MEASUREMENT (2026-08-16, 826 non-overlapping observations against "
        "VCB's forward 5-day return): the dispersion/flow family is kept (xs_skew5 "
        "t = −2.29, xs_disp5 t = +1.64, turnover_z t = −1.46) and the BREADTH family "
        "was dropped — breadth_pos5 t = +0.21, above_ma20 t = +0.29, n_active "
        "t = +0.34, all ≈ 0 and all near-duplicates of the index level pool__stock_market "
        "already carries. ⚠️ NOT ONE CLEARS MULTIPLE TESTING: seven tests puts the "
        "Bonferroni bar at |t| > 2.69 and the best is −2.29. They are here as the "
        "least-bad candidates at a cost of 8 columns, not as a demonstrated signal — "
        "and the row of that table that matters most is VCB's OWN past 5-day return at "
        "t = −0.31. ⚠️ Survivorship: silver holds no delisted name, so early dispersion "
        "is computed over the companies that survived to 2026 and is biased downward.",
    ),
    (
        "pool__stock_market",
        "stock_market",
        "index series",
        "6 VN indices × 27 measures = 162 columns, named {exchange}__{index}__"
        "{measure} — vnindex / vn30index / vn100_index / hnx_index / hnx30_index / "
        "upcom_index. ⚠️ THE PIVOT ALREADY HAPPENED IN GOLD: gold.stock_market is the "
        "four CafeF index tabs joined and pivoted to one row per date, so every index "
        "is already its own set of channels. ⚠️ hose__vnindex__close_adjust IS "
        "UNIFIED_BENCHMARK_COLUMN — the series pool__targets subtracts to build "
        "return_rel_{h}day. This pool carries bm[t] and its trailing history, NEVER "
        "bm[t+h], so there is no leakage; what is true is that the target's own "
        "denominator is a feature. ⚠️ For the ABSOLUTE target the index close is the "
        "market factor itself — the level-predicts-level trap in its purest form. The "
        "order-flow and foreign-flow measures are the half of this pool that is not "
        "that, and the closest anything in this database gets to §2d's top lever. "
        "⚠️ Widest coverage of the date-broadcast family, median 83.1%, but the four "
        "prop_* measures cover only 5.8% and the source ends 2026-07-30 (6 NULL rows "
        "at the tail).",
    ),
]


def _build_unified_date_spine_pool(name: str, gold_source: str, noun: str, what: str):
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
            f"(date, exchange, ticker), on pool__basic's calendar. ⚠️ The source is "
            f"keyed on `date` ALONE, so the join BROADCASTS one row across every "
            f"ticker of that day — the pool__economy shape, not the pool__ta one. "
            f"{what}"
        ),
    )
    def _date_spine_pool(
        context: AssetExecutionContext, preprocessor: PreprocessorResource
    ) -> MaterializeResult:
        universe = context.partition_key
        schema = _schema_of(universe)

        with preprocessor.session(schema=schema) as prep:
            panel = getattr(prep, f"_ingest_unified_{name.replace('__', '_')}")(
                universe
            )
            expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)
            source = panel["source"]

            with prep._database_driver._cursor_ctx() as cur:
                cur.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                    f"FROM {schema}.{name}"
                )
                rows, tickers, first, last = cur.fetchone()
                rows, tickers = int(rows), int(tickers)
                cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
                basic_rows = int(cur.fetchone()[0])
                primary_key = _primary_key(cur, schema, name)
                # ⚠️ The last spine date the SOURCE actually has a row for, not
                # MAX(date). See the block comment above: these are different numbers
                # and the difference is the whole staleness story.
                cur.execute(
                    f"SELECT MAX(date) FROM {schema}.{name} p "
                    f"WHERE EXISTS (SELECT 1 FROM {source} g WHERE g.date = p.date)"
                )
                last_with_data = cur.fetchone()[0]

        if primary_key != expected_key:
            raise ValueError(
                f"{schema}.{name} primary key is {primary_key}, expected "
                f"{expected_key} — order is part of the contract."
            )
        if rows != basic_rows:
            raise ValueError(
                f"{schema}.{name} has {rows} rows against pool__basic's "
                f"{basic_rows}. A date-keyed panel adds columns, never rows."
            )

        coverage = 100.0 * panel["populated_rows"] / max(rows, 1)
        context.log.info(
            f"{schema}.{name}: {rows} rows × {panel['features']} {noun} columns, "
            f"{tickers} ticker(s) ({first} → {last}) — {coverage:.1f}% of rows carry "
            f"at least one value; last spine date present in {source} is "
            f"{last_with_data}"
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
                "source": MetadataValue.text(source),
                "date_range": MetadataValue.text(f"{first} → {last}"),
                "last_date_with_data": MetadataValue.text(str(last_with_data)),
                "table": MetadataValue.text(f"{schema}.{name}"),
            }
        )

    return _date_spine_pool


date_spine_pool_assets: List[Callable] = [
    _build_unified_date_spine_pool(*spec) for spec in DATE_SPINE_POOLS
]


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
        "gold.forex_<exchange> → unified_schema_<universe>.pool__forex_<exchange>: "
        "ONE POOL PER EXCHANGE, each on pool__basic's (date, exchange, ticker) spine, "
        "columns named {exchange}__{ticker} with no measure suffix. ⚠️ IT WAS ONE "
        "TABLE UNTIL 2026-08-14 — the re-scrape took forex from 357 series to 3,074, "
        "gold split per exchange to stay under PostgreSQL's 1,600 columns (WID-1), and "
        "this followed, exactly as pool__economy_<country> follows "
        "gold.economy_<country>. A caller still naming `pool__forex` is naming a table "
        "that no longer exists. ⚠️ The source is keyed on `date` ALONE, so each join "
        "BROADCASTS one row across every ticker of that day and the row count must "
        "EQUAL the spine's. ⚠️ NOT forward-filled: a NULL means that broker did not "
        "quote that pair that day. ⚠️ The exchanges are BROKERS and must not be "
        "collapsed — SAXO and JFX disagree on 160,781 of 161,816 shared ticker-days."
    ),
)
def unified_pool_forex(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    universe = context.partition_key
    schema = _schema_of(universe)

    with preprocessor.session(schema=schema) as prep:
        panels = prep._ingest_unified_pool_forex(universe)
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "AND LEFT(table_name, CHAR_LENGTH(%s)) = %s ORDER BY table_name",
                (schema, "pool__forex_", "pool__forex_"),
            )
            actual_tables = [str(row[0]) for row in cur.fetchall()]
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            basic_rows = int(cur.fetchone()[0])
            for panel in panels:
                primary_key = _primary_key(cur, schema, panel["table"])
                if primary_key != expected_key:
                    raise ValueError(
                        f"{schema}.{panel['table']} primary key is {primary_key}, "
                        f"expected {expected_key} — order is part of the contract."
                    )
                if panel["rows"] != basic_rows:
                    raise ValueError(
                        f"{schema}.{panel['table']} has {panel['rows']} rows against "
                        f"pool__basic's {basic_rows}. An FX pool adds columns, never "
                        f"rows."
                    )

    expected_tables = sorted(panel["table"] for panel in panels)
    if actual_tables != expected_tables:
        raise ValueError(
            f"{schema}.pool__forex_* tables are {actual_tables}, expected "
            f"{expected_tables}. A stale or missing exchange panel is not a complete "
            f"FX pool."
        )

    total_pairs = sum(panel["features"] for panel in panels)
    populated = sum(panel["populated_rows"] for panel in panels)
    coverage = 100.0 * populated / max(len(panels) * basic_rows, 1)
    widest = max(panels, key=lambda p: p["features"])
    context.log.info(
        f"{schema}.pool__forex_*: {len(panels)} exchange panels, {total_pairs} pairs "
        f"total on {basic_rows} spine rows each; widest {widest['table']} at "
        f"{widest['features']} pairs; {coverage:.1f}% panel-row coverage"
    )
    return MaterializeResult(
        metadata={
            "exchange_panels": len(panels),
            "rows_per_panel": basic_rows,
            "pairs_total": total_pairs,
            "populated_panel_rows": populated,
            "panel_row_coverage_pct": MetadataValue.float(round(coverage, 2)),
            "universe": MetadataValue.text(universe),
            "primary_key": MetadataValue.text(", ".join(expected_key)),
            "source": MetadataValue.text("gold_schema.forex_<exchange>"),
            "widest_panel": MetadataValue.text(
                f"{widest['table']} ({widest['features']} pairs)"
            ),
            "tables": MetadataValue.text(
                ", ".join(expected_tables[:8])
                + (f", +{len(expected_tables) - 8} more" if len(panels) > 8 else "")
            ),
        }
    )


@asset(
    name="pool__basic_bank",
    key_prefix=["unified"],
    group_name="unified",
    compute_kind="postgres",
    partitions_def=UNIFIED_PARTITIONS,
    deps=[
        AssetKey(["silver", "stocks_basic"]),
        AssetKey(["unified", "pool__basic"]),
    ],
    description=(
        "silver.stocks_basic (GICS industry_code 401010) PIVOTED to peer channels → "
        "unified_schema_<universe>.pool__basic_bank: 20 banks × 15 measures = 300 "
        "columns named {exchange}__{ticker}__{measure}, one row per date, broadcast "
        "onto pool__basic's spine. ⚠️ THE ONLY DATE-BROADCAST POOL WITH NO TABLE "
        "BEHIND IT — there is no gold.stocks_bank_wide, so the pivot is built on the "
        "fly with one MAX(CASE WHEN ticker = …) per channel. ⚠️ Membership is DERIVED "
        "from the same GICS predicate unified_schema_bank uses, and is NOT "
        "point-in-time: silver.stocks_basic carries today's code on every historical "
        "row and holds no delisted name, so these are the banks that SURVIVED to 2026 "
        "carried back to 2009. ⚠️ THE SCHEMA'S OWN TICKER IS ONE OF THE CHANNELS — on "
        "unified_schema_vcb, hose__vcb__close_adjust IS pool__basic.close_adjust "
        "(verified, 0 mismatches on all 15 measures), so a consumer joining both holds "
        "each VCB measure twice. ⚠️ The 8 GICS identity columns are excluded: constant "
        "per ticker, so pivoting them would write 160 constant strings."
    ),
)
def unified_pool_basic_bank(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    universe = context.partition_key
    schema = _schema_of(universe)

    with preprocessor.session(schema=schema) as prep:
        panel = prep._ingest_unified_pool_basic_bank(universe)
        expected_key = tuple(prep.UNIFIED_PRIMARY_KEY)

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                f"FROM {schema}.pool__basic_bank"
            )
            rows, tickers, first, last = cur.fetchone()
            rows, tickers = int(rows), int(tickers)
            primary_key = _primary_key(cur, schema, "pool__basic_bank")
            cur.execute(f"SELECT COUNT(*) FROM {schema}.pool__basic")
            basic_rows = int(cur.fetchone()[0])
            # ⚠️ THE SELF-DUPLICATION IS MEASURED, NOT ASSUMED, and only where it is
            # meaningful: on a single-company schema the spine's own ticker is one of
            # the peers, so its channels must equal pool__basic's own columns exactly.
            # If that ever stops holding, the pivot is reading the wrong row.
            self_duplicated = None
            if not prep._helper_unified_is_universe(universe):
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'pool__basic' "
                    "AND column_name NOT IN ('date', 'exchange', 'ticker')",
                    (schema,),
                )
                own = [r[0] for r in cur.fetchall()]
                cur.execute(
                    f"SELECT LOWER(exchange), LOWER(ticker) FROM {schema}.pool__basic "
                    f"LIMIT 1"
                )
                ex, tk = cur.fetchone()
                mirrored = [
                    (c, f"{ex}__{tk}__{c}")
                    for c in own
                    if f"{ex}__{tk}__{c}" in panel["channels"]
                ]
                mismatches = 0
                for own_col, peer_col in mirrored:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {schema}.pool__basic b "
                        f"JOIN {schema}.pool__basic_bank p USING (date, exchange, ticker) "
                        f'WHERE b."{own_col}" IS DISTINCT FROM p."{peer_col}"'
                    )
                    mismatches += int(cur.fetchone()[0])
                self_duplicated = (len(mirrored), mismatches)

    if primary_key != expected_key:
        raise ValueError(
            f"{schema}.pool__basic_bank primary key is {primary_key}, expected "
            f"{expected_key} — order is part of the contract."
        )
    if rows != basic_rows:
        raise ValueError(
            f"{schema}.pool__basic_bank has {rows} rows against pool__basic's "
            f"{basic_rows}. A peer pool adds columns, never rows."
        )
    if self_duplicated and self_duplicated[1]:
        raise ValueError(
            f"{schema}.pool__basic_bank: {self_duplicated[1]} value(s) of the spine's "
            f"OWN ticker disagree with pool__basic across {self_duplicated[0]} mirrored "
            f"measure(s). The pivot is reading the wrong row."
        )

    coverage = 100.0 * panel["populated_rows"] / max(rows, 1)
    context.log.info(
        f"{schema}.pool__basic_bank: {rows} rows × {panel['features']} channels "
        f"({panel['members']} members × {panel['measures']} measures), {tickers} "
        f"ticker(s) ({first} → {last}) — {coverage:.1f}% of rows carry at least one "
        f"value"
        + (
            f"; own ticker mirrored on {self_duplicated[0]} measures, 0 mismatches"
            if self_duplicated
            else ""
        )
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": panel["features"] + len(expected_key),
            "channels": panel["features"],
            "members": panel["members"],
            "measures_per_member": panel["measures"],
            "tickers": tickers,
            "pool__basic_rows": basic_rows,
            "populated_rows": panel["populated_rows"],
            "row_coverage_pct": MetadataValue.float(round(coverage, 2)),
            "universe": MetadataValue.text(universe),
            "primary_key": MetadataValue.text(", ".join(primary_key)),
            "source": MetadataValue.text(panel["source"]),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "own_ticker_mirrored_measures": (
                self_duplicated[0] if self_duplicated else 0
            ),
            "table": MetadataValue.text(f"{schema}.pool__basic_bank"),
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
        "pool__news_daily",
        "news_daily_panel",
        "~18 news and disclosure EVENT columns at a daily grain — n_docs_{5,10}d, "
        "n_editorial_*, n_docs_named_*, n_earnings_*, relevance_max_*, if_news_*, "
        "if_editorial_*. ⚠️ THIS IS NOT THE SENTIMENT THREAD. CLAUDE.md §2a records "
        "that news SENTIMENT found nothing and made price/TA models worse (QWK 0.175 "
        "→ 0.045); this pool carries no sentiment at all, only counts of what happened "
        "and when it was disclosed, which §2d lists separately as the third-ranked "
        "remaining lever. ⚠️ NINE PRICE COLUMNS ARE DROPPED (UNIFIED_NEWS_PRICE_DUPES) "
        "— the panel re-derives its own returns and turnover to stand alone, and one "
        "of them is a second close_adjust in a panel whose label comes from the first. "
        "⚠️ ret_5d is TRAILING (verified 2026-08-16: corr +1.000000 with the trailing "
        "5-day return, −0.006 with the forward one), so it is excluded as a DUPLICATE, "
        "not as a leak. ⚠️ The corpus starts 2013-01-02 against a 2009 spine and ends "
        "2026-07-08, ~16 sessions behind it — VCB has 3,355 rows against 4,266, so "
        "read coverage AND trailing_null_sessions.",
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
    *date_spine_pool_assets,
    unified_pool_basic_bank,
    *feature_pool_assets,
]
