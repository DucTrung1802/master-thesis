# src\orchestration\assets\gold.py
"""The GOLD layer — `silver_schema` → `gold_schema`.

Three assets. Two are WIDE — one row per date, one column per (entity × measure):

* `gold/economy` — the macro panel, `{country}__{scrape_main_type}__{category}__{exchange}__{ticker}`,
  one row per BUSINESS DAY, as-of filled.
* `gold/stock_market` — the six market indices, `{exchange}__{ticker}__{measure}`, one
  row per TRADING DAY, **not** filled.

The third is the other kind of gold table — the FEATURE panel, same grain as its silver
source with columns added:

* `gold/stocks_financials_bank_fa` — `silver.stocks_basic_financials_bank_fa` (price ×
  as-of bank financials × 26 fundamental indicators) plus the full per-stock TA battery.
  One row per stock-day, PK `(exchange, ticker, date)` — the same shape `gold.stocks`
  has, which is still built through `main.py` rather than as an asset.

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


assets: List[Callable] = [
    gold_economy,
    gold_stock_market,
    gold_stocks_financials_bank_fa,
    gold_news_weekly_panel,
    gold_news_daily_panel,
]
