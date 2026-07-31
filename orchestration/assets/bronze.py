# orchestration\assets\bronze.py
"""THE BRONZE LAYER — `raw_data/` → `bronze_schema`. All 20 ingest leaves.

One asset per bronze table, generated from the `INGESTS` spec table rather than
copy-pasted, and every one a thin wrapper over the `_ingest_bronze_*` method that
already exists in `src/` (`orchestration/CONTEXT.md` §3).

**Bronze has no cross-table dependency** — each ingest reads its own `raw_data/` folder
and nothing else — so these 20 are a FLAT layer. All the edges point upward, to the
landing asset that writes the folder each one reads:

```
raw/trading_view_data[economy]  ─►  bronze/trading_view_economy
raw/cafef_price                 ─►  bronze/cafef_price
raw/simplize_stocks             ─►  bronze/simplize_stocks
…
```

⚠️ **The TradingView edges are PER PARTITION.** `_ingest_bronze_economy` globs
`raw_data/trading_view/data/economy/**` and is indifferent to the other eight asset
classes, so each of those six deps uses `SpecificPartitionsPartitionMapping([<class>])`.
The default for an unpartitioned asset depending on a partitioned one is ALL partitions,
which would claim the economy table needs forex and crypto data to build.

⚠️ **`cafef_financials` writes SIX tables from one ingest** (three bank statements plus
`cafef_financial_reports` / `_schema` / `_templates`), so its asset reports a row count
per table rather than one number. It stays a single asset because it is a single
method — splitting it here would invent a granularity `src/` does not have.
"""

from typing import Callable, List, Optional, Sequence

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    asset,
)

from orchestration._bootstrap import bootstrap

bootstrap()

from orchestration.resources import PreprocessorResource

TV_DATA = AssetKey(["raw", "trading_view_data"])


def _tv_dep(asset_class: str) -> AssetDep:
    """The `trading_view_data` partition this bronze table actually reads."""
    return AssetDep(
        TV_DATA,
        partition_mapping=SpecificPartitionsPartitionMapping([asset_class]),
    )


def _raw(name: str) -> AssetDep:
    return AssetDep(AssetKey(["raw", name]))


# (asset name, ingest method, tables it writes, upstream landing asset)
#
# The asset name IS the bronze table name for every single-table leaf, so
# `bronze/cafef_price` reads as `bronze_schema.cafef_price`. The four `cafef_index_*`
# assets keep the keys they had as the original prototype slice — they have run history.
INGESTS: list[tuple[str, str, Sequence[str], Optional[AssetDep]]] = [
    # ── TradingView: one table per asset class, each from its own data partition ──
    ("trading_view_bonds", "_ingest_bronze_bonds", ["trading_view_bonds"], _tv_dep("bonds")),
    ("trading_view_economy", "_ingest_bronze_economy", ["trading_view_economy"], _tv_dep("economy")),
    ("trading_view_forex", "_ingest_bronze_forex", ["trading_view_forex"], _tv_dep("forex")),
    ("trading_view_funds", "_ingest_bronze_funds", ["trading_view_funds"], _tv_dep("funds")),
    ("trading_view_indices", "_ingest_bronze_indices", ["trading_view_indices"], _tv_dep("indices")),
    ("trading_view_stocks", "_ingest_bronze_stocks_trading_view", ["trading_view_stocks"], _tv_dep("stocks")),
    # ── CafeF per-stock tabs ──────────────────────────────────────────────────────
    ("cafef_price", "_ingest_bronze_cafef_price", ["cafef_price"], _raw("cafef_price")),
    ("cafef_foreign", "_ingest_bronze_cafef_foreign", ["cafef_foreign"], _raw("cafef_foreign")),
    ("cafef_order_stats", "_ingest_bronze_cafef_order_stats", ["cafef_order_stats"], _raw("cafef_order_stats")),
    ("cafef_prop_trading", "_ingest_bronze_cafef_prop_trading", ["cafef_prop_trading"], _raw("cafef_prop_trading")),
    # ── CafeF market indices (the original prototype slice) ───────────────────────
    ("cafef_index_price", "_ingest_bronze_cafef_index_price", ["cafef_index_price"], _raw("cafef_index_price")),
    ("cafef_index_foreign", "_ingest_bronze_cafef_index_foreign", ["cafef_index_foreign"], _raw("cafef_index_foreign")),
    ("cafef_index_order_stats", "_ingest_bronze_cafef_index_order_stats", ["cafef_index_order_stats"], _raw("cafef_index_order_stats")),
    ("cafef_index_prop_trading", "_ingest_bronze_cafef_index_prop_trading", ["cafef_index_prop_trading"], _raw("cafef_index_prop_trading")),
    # ── CafeF event streams + filings ─────────────────────────────────────────────
    (
        "cafef_insider_shareholder_transactions",
        "_ingest_bronze_cafef_insider_shareholder_transactions",
        ["cafef_insider_shareholder_transactions"],
        _raw("cafef_insider_txn"),
    ),
    ("cafef_news", "_ingest_bronze_cafef_news", ["cafef_news"], _raw("cafef_news")),
    (
        "cafef_financials",
        "_ingest_bronze_cafef_financials",
        [
            "cafef_financials_bank_balance_sheet",
            "cafef_financials_bank_income_statement",
            "cafef_financials_bank_cash_flow",
            "cafef_financial_reports",
            "cafef_financial_schema",
            "cafef_financial_templates",
        ],
        _raw("cafef_financials"),
    ),
    # ── Simplize + GICS ───────────────────────────────────────────────────────────
    ("simplize_stocks", "_ingest_bronze_stocks_simplize", ["simplize_stocks"], _raw("simplize_stocks")),
    ("simplize_industry", "_ingest_bronze_simplize_industry", ["simplize_industry"], _raw("simplize_industry")),
    ("gics", "_ingest_bronze_gics", ["gics"], _raw("gics_structure")),
]


def _build_bronze_asset(
    name: str, ingest: str, tables: Sequence[str], upstream: Optional[AssetDep]
):
    @asset(
        name=name,
        key_prefix=["bronze"],
        group_name="bronze",
        compute_kind="postgres",
        deps=[upstream] if upstream else [],
        description=(
            f"→ bronze_schema.{', '.join(tables)}. "
            f"Wraps `DataPreprocessor.{ingest}`; PK (exchange, ticker, date) unless the "
            f"table is event-based. Replaces the switch leaf under "
            f"`data_preprocessor/data_quality_bronze/`."
        ),
    )
    def _bronze(
        context: AssetExecutionContext, preprocessor: PreprocessorResource
    ) -> MaterializeResult:
        with preprocessor.session(schema="bronze_schema") as prep:
            # Called directly, so an exception propagates and FAILS the asset. Going
            # through `ingest_bronze_data()` would catch it per leaf and report a
            # summary instead — that path is main.py's shim, not this one.
            getattr(prep, ingest)()

            # A raw cursor rather than `driver.select`: a COUNT is one less layer
            # between the asset's metadata and the database.
            counts: dict[str, int] = {}
            with prep._database_driver._cursor_ctx() as cur:
                for table in tables:
                    cur.execute(f"SELECT COUNT(*) FROM bronze_schema.{table}")
                    counts[table] = int(cur.fetchone()[0])

        for table, rows in counts.items():
            context.log.info(f"bronze.{table}: {rows} rows")

        metadata: dict = {"rows": sum(counts.values())}
        if len(tables) > 1:
            metadata |= {f"rows: {t}": n for t, n in counts.items()}
        metadata["table"] = MetadataValue.text(
            ", ".join(f"bronze_schema.{t}" for t in tables)
        )
        return MaterializeResult(metadata=metadata)

    return _bronze


assets: List[Callable] = [_build_bronze_asset(*spec) for spec in INGESTS]
