# src\orchestration\assets\gold.py
"""The GOLD layer — `silver_schema` → `gold_schema`.

Two assets are WIDE — one row per date, one column per (entity × measure):

* `gold/economy` — the macro panel, `{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`,
  one row per BUSINESS DAY, as-of filled.
* `gold/stock_market` — the six market indices, `{exchange}__{ticker}__{measure}`, one
  row per TRADING DAY, **not** filled.

The rest are the other kind of gold table — same grain as their silver source, PK
`(exchange, ticker, date)`, columns added and never rows:

* `gold/stocks` — `silver.stocks_basic` with a split-adjusted OHLC set and **nothing
  else**. The plain price/flow panel.
* `gold/stocks_ta` — the same panel plus the ~900-column TA block. ⚠️ The pair replaced
  a single 935-column `gold.stocks` on 2026-08-03; splitting them makes "prices without
  technicals" a ~200 MB read instead of an ~11 GB one.
* `gold/stocks_financials_bank_fa` — `silver.stocks_basic_financials_bank_fa` (price ×
  as-of bank financials × 26 fundamental indicators) plus that same TA battery.

⚠️ **None of the three is built from either of the others.** Each recomputes its base
from silver, so two gold tables cannot disagree about a stock-day while looking
identical. The shared `_helper_stock_ta_layers` is what stops the feature sets drifting.

⚠️ **Why one is filled and the other is not.** Macro series are published on a lag and
are stale-but-valid between releases, so carrying them forward is what a reader would
actually know. An index either traded that day or it did not — a gap means VN100-INDEX
did not exist yet (it starts 2014), and filling it would invent prices for days the
market was shut.

The macro panel below:

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

⚠️ **`gold.economy` used to be something else** — the generic `_ingest_gold_table`
output, i.e. the LONG grain with per-series TA features (579,459 x 16: returns,
volatility, rolling stats), with this panel beside it as `gold.economy_panel`. Two gold
tables for one asset is one too many, so the wide panel took the name (2026-08-01).
Restoring the feature table is one line in `_ingest_gold_economy`; the generic builder is
untouched and still drives bonds/forex/funds/indices/stocks.

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
    name="economy",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[
        AssetKey(["silver", "economy"]),
        AssetKey(["silver", "economy_series"]),
    ],
    description=(
        "silver.economy + silver.economy_series → gold.economy: ONE ROW PER "
        "BUSINESS DAY (PK `date`), one column per series named "
        "`{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`. As-of "
        "filled with a per-frequency publication lag and staleness cap. Columns are "
        "REAL, not DOUBLE — 1,034 float8 would exceed PostgreSQL's ~8 kB row limit."
    ),
)
def gold_economy(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_economy()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_schema.economy")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'gold_schema' AND table_name = 'economy'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute("SELECT MIN(date), MAX(date) FROM gold_schema.economy")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM silver_schema.economy")
            silver_rows = int(cur.fetchone()[0])

    context.log.info(
        f"gold.economy: {rows} business days × {columns - 1} series "
        f"(silver.economy holds {silver_rows} observations)"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "series": columns - 1,
            "silver_observations": silver_rows,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("gold_schema.economy"),
        }
    )


@asset(
    name="stock_market",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stock_market"])],
    description=(
        "silver.stock_market → gold.stock_market: ONE ROW PER TRADING DAY (PK `date`), "
        "one column per index × measure named `{exchange}__{ticker}__{measure}` — "
        "6 indices × 27 measures. ⚠️ Hyphenated tickers (HNX-INDEX, VN100-INDEX) are "
        "sanitised to underscores because PostgreSQL cannot take a hyphen in an "
        "unquoted identifier; collisions are checked. No as-of fill: a gap means the "
        "index did not trade, not that the value is stale."
    ),
)
def gold_stock_market(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_stock_market()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_schema.stock_market")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'gold_schema' AND table_name = 'stock_market'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute("SELECT MIN(date), MAX(date) FROM gold_schema.stock_market")
            first, last = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM silver_schema.stock_market")
            silver_rows = int(cur.fetchone()[0])

    context.log.info(
        f"gold.stock_market: {rows} trading days × {columns - 1} columns "
        f"(silver holds {silver_rows} index-days)"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns - 1,
            "silver_index_days": silver_rows,
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("gold_schema.stock_market"),
        }
    )


@asset(
    name="stocks_financials_bank_fa",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stocks_basic_financials_bank_fa"])],
    description=(
        "silver.stocks_basic_financials_bank_fa → gold.stocks_financials_bank_fa: the "
        "daily price × as-of bank financials × 26 fundamental indicators panel, plus "
        "the same TA battery gold.stocks gets (~40 TA-Lib indicators + the three "
        "microstructure features) and the standard return/volatility/rolling layers. "
        "Same grain and row count as its source — this adds columns, never rows. "
        "⚠️ The carried financial lines are DOUBLE PRECISION, not gold's usual REAL: "
        "VND figures reach ~1e15-1e17, where REAL rounds to the nearest ~1e8."
    ),
)
def gold_stocks_financials_bank_fa(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_stocks_financials_bank_fa()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                "FROM gold_schema.stocks_financials_bank_fa"
            )
            rows, tickers, first, last = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'gold_schema' AND "
                "table_name = 'stocks_financials_bank_fa'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM silver_schema.stocks_basic_financials_bank_fa"
            )
            silver_rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'silver_schema' AND "
                "table_name = 'stocks_basic_financials_bank_fa'"
            )
            silver_columns = int(cur.fetchone()[0])

    # The grain must survive the feature build: gold adds columns to every silver row.
    if rows != silver_rows:
        raise ValueError(
            f"gold.stocks_financials_bank_fa has {rows} rows but silver has "
            f"{silver_rows} — the feature build changed the grain."
        )

    context.log.info(
        f"gold.stocks_financials_bank_fa: {rows} stock-days × {columns} columns "
        f"(silver: {silver_rows} × {silver_columns})"
    )
    return MaterializeResult(
        metadata={
            "rows": int(rows),
            "columns": columns,
            "silver_columns": silver_columns,
            "features_added": columns - silver_columns,
            "tickers": int(tickers),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("gold_schema.stocks_financials_bank_fa"),
        }
    )


@asset(
    name="news_weekly_panel",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[
        AssetKey(["silver", "cafef_news"]),
        AssetKey(["silver", "stocks_basic"]),
    ],
    description=(
        "silver.cafef_news × silver.stocks_basic → gold.news_weekly_panel, PK "
        "(exchange, ticker, week_start). The MINIMAL panel: news and event COUNTS, no "
        "sentiment — it exists so the costed walk-forward can be run on if_news/n_docs "
        "before any NLP work. ⚠️ WEEKLY: paper 57 measures daily news predicting 1-2 days "
        "against weekly news predicting 13 weeks, and on this corpus editorials cover "
        "1.6% of ticker-DAYS but 8.7% of ticker-WEEKS. ⚠️ The spine is PRICE, so "
        "if_news=0 rows are KEPT — that dummy is paper 57's publication effect, not "
        "missing data."
    ),
)
def gold_news_weekly_panel(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_news_weekly_panel()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_schema.news_weekly_panel")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'gold_schema' AND table_name = 'news_weekly_panel'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(DISTINCT ticker), MIN(week_start), MAX(week_start) "
                "FROM gold_schema.news_weekly_panel"
            )
            tickers, first, last = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE if_news = 1), "
                "       COUNT(*) FILTER (WHERE n_editorial > 0), "
                "       COUNT(*) FILTER (WHERE if_earnings_week = 1) "
                "FROM gold_schema.news_weekly_panel"
            )
            with_news, with_editorial, earnings_weeks = (int(x) for x in cur.fetchone())
            # ⚠️ The grain, re-checked against what landed rather than against the frame.
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT 1 FROM gold_schema.news_weekly_panel "
                "GROUP BY exchange, ticker, week_start HAVING COUNT(*) > 1) d"
            )
            duplicates = int(cur.fetchone()[0])

    if duplicates:
        raise ValueError(
            f"gold.news_weekly_panel: {duplicates} duplicate (exchange, ticker, "
            f"week_start) groups — the grain is one row per ticker-week."
        )

    context.log.info(
        f"gold.news_weekly_panel: {rows} ticker-weeks × {columns} columns, "
        f"{with_news} with news ({with_news / max(rows, 1):.1%}), "
        f"{with_editorial} with an editorial ({with_editorial / max(rows, 1):.1%})"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "tickers": int(tickers),
            "ticker_weeks_with_news": with_news,
            "ticker_weeks_with_editorial": with_editorial,
            "earnings_weeks": earnings_weeks,
            "news_coverage": MetadataValue.float(round(with_news / max(rows, 1), 4)),
            "editorial_coverage": MetadataValue.float(
                round(with_editorial / max(rows, 1), 4)
            ),
            "week_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("gold_schema.news_weekly_panel"),
        }
    )


@asset(
    name="news_daily_panel",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[
        AssetKey(["silver", "cafef_news"]),
        AssetKey(["silver", "stocks_basic"]),
    ],
    description=(
        "The DAILY-formation twin of gold/news_weekly_panel, PK (exchange, ticker, date). "
        "News enters as trailing 5- and 10-session windows, matched to the rel5/rel10 "
        "horizons experiment_3.3 settled on. ⚠️ The trailing window includes the formation "
        "day and that is not a leak — trading_date is already the first session whose OPEN "
        "follows the article. ⚠️ Daily formation OVERLAPS: consecutive rows share h-1 days "
        "of forward path, so the row count is ~5x the weekly panel's without 5x the "
        "information, and a random split would be fatal."
    ),
)
def gold_news_daily_panel(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_news_daily_panel()

        with prep._database_driver._cursor_ctx() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_schema.news_daily_panel")
            rows = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE "
                "table_schema = 'gold_schema' AND table_name = 'news_daily_panel'"
            )
            columns = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COUNT(DISTINCT ticker), MIN(date), MAX(date) "
                "FROM gold_schema.news_daily_panel"
            )
            tickers, first, last = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE if_news_5d = 1), "
                "       COUNT(*) FILTER (WHERE if_editorial_5d = 1), "
                "       COUNT(*) FILTER (WHERE if_news_10d = 1) "
                "FROM gold_schema.news_daily_panel"
            )
            news_5d, ed_5d, news_10d = (int(x) for x in cur.fetchone())
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT 1 FROM gold_schema.news_daily_panel "
                "GROUP BY exchange, ticker, date HAVING COUNT(*) > 1) d"
            )
            duplicates = int(cur.fetchone()[0])

    if duplicates:
        raise ValueError(
            f"gold.news_daily_panel: {duplicates} duplicate (exchange, ticker, date) "
            f"groups — the grain is one row per stock-day."
        )

    context.log.info(
        f"gold.news_daily_panel: {rows} stock-days × {columns} columns, "
        f"{news_5d} with news in the trailing 5 sessions ({news_5d / max(rows, 1):.1%}), "
        f"{ed_5d} with an editorial ({ed_5d / max(rows, 1):.1%})"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "columns": columns,
            "tickers": int(tickers),
            "stock_days_with_news_5d": news_5d,
            "stock_days_with_editorial_5d": ed_5d,
            "stock_days_with_news_10d": news_10d,
            "news_coverage_5d": MetadataValue.float(round(news_5d / max(rows, 1), 4)),
            "editorial_coverage_5d": MetadataValue.float(round(ed_5d / max(rows, 1), 4)),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text("gold_schema.news_daily_panel"),
        }
    )


def _stocks_panel_result(
    context: AssetExecutionContext, prep, table: str
) -> MaterializeResult:
    """Shared verification + metadata for the `stocks` / `stocks_ta` pair.

    ⚠️ The row-count assertion is the point. Both tables are the same grain as
    `silver.stocks_basic` — one row per stock-day — so a build that changed the row
    count either dropped stock-days or duplicated them, and a bigger table would
    otherwise read as a better one.
    """
    with prep._database_driver._cursor_ctx() as cur:
        cur.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) "
            f"FROM gold_schema.{table}"
        )
        rows, tickers, first, last = cur.fetchone()
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE "
            "table_schema = 'gold_schema' AND table_name = %s",
            (table,),
        )
        columns = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM silver_schema.stocks_basic")
        silver_rows = int(cur.fetchone()[0])
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE "
            "table_schema = 'silver_schema' AND table_name = 'stocks_basic'"
        )
        silver_columns = int(cur.fetchone()[0])
        cur.execute(
            f"SELECT pg_size_pretty(pg_total_relation_size('gold_schema.{table}'))"
        )
        size = cur.fetchone()[0]

    if rows != silver_rows:
        raise ValueError(
            f"gold.{table} has {rows} rows but silver.stocks_basic has {silver_rows} "
            f"— the build changed the grain. Both are one row per stock-day."
        )

    context.log.info(
        f"gold.{table}: {rows} stock-days × {columns} columns, {tickers} tickers, {size} "
        f"(silver.stocks_basic: {silver_rows} × {silver_columns})"
    )
    return MaterializeResult(
        metadata={
            "rows": int(rows),
            "columns": columns,
            "silver_columns": silver_columns,
            "columns_added": columns - silver_columns,
            "tickers": int(tickers),
            "size": MetadataValue.text(size),
            "date_range": MetadataValue.text(f"{first} → {last}"),
            "table": MetadataValue.text(f"gold_schema.{table}"),
        }
    )


@asset(
    name="stocks",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stocks_basic"])],
    description=(
        "silver.stocks_basic → gold.stocks: the per-stock price/flow panel, PK "
        "(exchange, ticker, date), with a SPLIT-ADJUSTED OHLC set and NO derived "
        "columns — the technicals are gold/stocks_ta. ⚠️ `open`/`high`/`low`/`close` "
        "are ADJUSTED and are NOT silver's: CafeF ships raw legs beside "
        "close_raw/close_adjust, which is two price scales in one row (VCB 2009-06-30: "
        "raw 60,000, adjusted 9,130). The source values are kept as "
        "open_raw/high_raw/low_raw/close_raw, so nothing is lost. ⚠️ Carried numerics "
        "are DOUBLE PRECISION, not gold's usual REAL — at ~42 columns there is no "
        "row-size pressure, and value_matched reaches ~1e12 where REAL loses hundreds "
        "of thousands of dong."
    ),
)
def gold_stocks(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_stocks()
        return _stocks_panel_result(context, prep, "stocks")


@asset(
    name="stocks_ta",
    key_prefix=["gold"],
    group_name="gold",
    compute_kind="postgres",
    deps=[AssetKey(["silver", "stocks_basic"])],
    description=(
        "silver.stocks_basic → gold.stocks_ta: everything gold/stocks carries plus the "
        "full per-stock TA battery (~40 TA-Lib indicators + the three microstructure "
        "features) and the standard return/volatility/rolling layers. ~900 added "
        "columns over ~2.4 M rows. ⚠️ HEAVY — hours, and ~11 GB on disk. ⚠️ It is NOT "
        "built from gold/stocks: the base is recomputed from silver so the two cannot "
        "disagree about a stock-day while looking identical. ⚠️ The table in the "
        "database predates this asset — it is the 2026-08-03 rename of the "
        "pre-2026-07-19 gold.stocks and still carries that era's column names; the "
        "first materialisation REPLACES it."
    ),
)
def gold_stocks_ta(
    context: AssetExecutionContext, preprocessor: PreprocessorResource
) -> MaterializeResult:
    with preprocessor.session(schema="gold_schema") as prep:
        prep._ingest_gold_stocks_ta()
        return _stocks_panel_result(context, prep, "stocks_ta")


# ── The three WIDE TradingView panels: bonds, funds, forex ───────────────────────
#
# One silver source each, pivoted to ONE ROW PER TRADING DAY with a column per
# (entity × measure). They are one spec table rather than three copies for the reason
# `bronze.py` gives — and because the ASSERTIONS below are the valuable part, so
# every panel must get all of them, not the ones whoever wrote it remembered.
#
# ⚠️ NONE of the three is as-of filled, unlike `gold/economy`. A macro series is
# stale-but-valid between releases; a quote is not. A gap means that tenor / ETF /
# broker pair did not quote that day, and filling would invent a price.
#
# ⚠️ THE MEASURE SET SHRINKS AS THE ENTITY COUNT GROWS, and that is the ceiling
# talking, not taste: PostgreSQL allows 1,600 columns per table. 9 tenors and 19
# ETFs carry the full 13-measure feature block; forex's 328 series would need 4,264
# columns to do the same, so it carries `value` alone — the identical trade
# `gold.economy` makes at 1,034 series.
#
# (name, entity noun, one-line shape description for the asset's UI description)
WIDE_PANELS: list[tuple[str, str, str]] = [
    (
        "bonds",
        "tenor-days",
        "9 government tenors × 13 measures named `{exchange}__{ticker}__{measure}`. "
        "⚠️ Every tenor is published TWICE (`VN01` and `VN01Y`); the builder asserts "
        "the twins agree on every shared date before dropping the plain spelling.",
    ),
    (
        "funds",
        "fund-days",
        "19 HOSE ETFs × up to 19 measures named `{exchange}__{ticker}__{measure}` — "
        "351 columns rather than 361, because FUEBFVND's 3 rows cannot fill a 5- or "
        "21-day window and an all-NULL column is not written. ⚠️ REPLACES the long "
        "18,662 × 22 feature table the generic builder used to write.",
    ),
    (
        "forex",
        "quote-days",
        "328 broker-quoted pairs, ONE `value` column each, named `{exchange}__"
        "{ticker}` with no measure suffix. ⚠️ No features: 328 × 13 measures is 4,264 "
        "columns against PostgreSQL's 1,600 ceiling, so the measure set gives way — "
        "the same trade `gold.economy` makes. ⚠️ The 9 exchanges are 9 BROKERS and "
        "disagree on 95-99.9% of shared ticker-days, so they are not duplicates.",
    ),
]


def _build_gold_wide_panel(name: str, noun: str, shape: str):
    @asset(
        name=name,
        key_prefix=["gold"],
        group_name="gold",
        compute_kind="postgres",
        deps=[AssetKey(["silver", name])],
        description=(
            f"silver.{name} → gold.{name}: ONE ROW PER TRADING DAY (PK `date`). "
            f"{shape} NOT as-of filled — a gap means it did not quote that day."
        ),
    )
    def _gold_panel(
        context: AssetExecutionContext, preprocessor: PreprocessorResource
    ) -> MaterializeResult:
        with preprocessor.session(schema="gold_schema") as prep:
            getattr(prep, f"_ingest_gold_{name}")()

            with prep._database_driver._cursor_ctx() as cur:
                cur.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT date), MIN(date), MAX(date) "
                    f"FROM gold_schema.{name}"
                )
                rows, dates, first, last = cur.fetchone()
                rows, dates = int(rows), int(dates)
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = 'gold_schema' AND table_name = %s",
                    (name,),
                )
                columns = int(cur.fetchone()[0])
                cur.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT date) FROM silver_schema.{name}"
                )
                silver_rows, silver_dates = (int(v) for v in cur.fetchone())

        # ⚠️ THE GRAIN IS THE POINT OF THESE TABLES, so it is asserted rather than
        # described. A PK on `date` already makes a duplicate impossible at the server,
        # but this also catches the opposite slip — a pivot that LOST dates — which no
        # constraint can, and which would look like a perfectly healthy smaller table.
        if rows != dates:
            raise ValueError(
                f"gold.{name} is not one row per date: {rows} rows over {dates} "
                f"distinct dates."
            )
        if dates != silver_dates:
            raise ValueError(
                f"gold.{name} covers {dates} dates but silver.{name} has "
                f"{silver_dates}. The panel's calendar IS the set of distinct dates in "
                f"silver, so a difference means the pivot dropped a trading day."
            )

        # The true fill percentage is logged by the builder, which has the frame in
        # hand; counting non-nulls across hundreds of columns here would be a second,
        # worse answer to a question already answered.
        context.log.info(
            f"gold.{name}: {rows} trading days × {columns - 1} columns, from "
            f"{silver_rows} silver {noun}"
        )
        return MaterializeResult(
            metadata={
                "rows": rows,
                "columns": columns - 1,
                f"silver_{noun}": silver_rows,
                "date_range": MetadataValue.text(f"{first} → {last}"),
                "table": MetadataValue.text(f"gold_schema.{name}"),
            }
        )

    return _gold_panel


wide_panel_assets: List[Callable] = [
    _build_gold_wide_panel(*spec) for spec in WIDE_PANELS
]


assets: List[Callable] = [
    gold_economy,
    *wide_panel_assets,
    gold_stock_market,
    gold_stocks,
    gold_stocks_ta,
    gold_stocks_financials_bank_fa,
    gold_news_weekly_panel,
    gold_news_daily_panel,
]
