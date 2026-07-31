# orchestration\assets\gold.py
"""The GOLD layer — `silver_schema` → `gold_schema`.

One asset: `gold/economy_panel`, the WIDE macro panel — one row per business day, one
column per series, named
`{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`.

⚠️ **This is the layer where the wide shape belongs, and the reason is not aesthetics.**
Every step that makes the panel usable is a MODELLING decision, and gold is the layer
allowed to make them:

* **publication lag** — the source `date` is the REFERENCE period, not the release date.
  Vietnam's Q1 GDP is dated 2026-03-31 and published in April, so a panel joined on
  `date` hands a model a number a week before it existed. Each observation is shifted
  forward by its frequency's typical lag.
* **as-of carry** — between releases, the last published value IS the current known
  value; carrying it forward takes the panel from 5.8% filled to ~91%.
* **staleness cap** — but bounded per frequency, so a series that stopped reporting in
  2010 does not read as live data in 2026.

Silver keeps the raw-faithful long table and none of these assumptions
(`assets/silver.py`). Both lag tables are ASSUMPTIONS, documented as such in
`DataPreprocessor.ECONOMY_PUBLICATION_LAG_DAYS` / `ECONOMY_MAX_STALENESS_BDAYS` — they
are imposed, not read off the source, because TradingView publishes no release dates.
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
    name="economy_panel",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[
        AssetKey(["silver", "economy"]),
        AssetKey(["silver", "economy_series"]),
    ],
    description=(
        "silver.economy + silver.economy_series → gold.economy_panel: ONE ROW PER "
        "BUSINESS DAY (PK `date`), one column per series named "
        "`{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`. As-of "
        "filled with a per-frequency publication lag and staleness cap. Columns are "
        "REAL, not DOUBLE — 1,034 float8 would exceed PostgreSQL's ~8 kB row limit."
    ),
)
def gold_economy_panel(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_economy_panel()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_schema.economy_panel")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'gold_schema' AND table_name = 'economy_panel'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute("SELECT MIN(date), MAX(date) FROM gold_schema.economy_panel")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM silver_schema.economy")
            silver_rows = int(cur.fetchone()[0])

    context.log.info(
        f"gold.economy_panel: {rows} business days × {columns - 1} series "
        f"(silver.economy holds {silver_rows} observations)"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "series": columns - 1,
            "silver_observations": silver_rows,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("gold_schema.economy_panel"),
        }
    )


assets: List[Callable] = [gold_economy_panel]
