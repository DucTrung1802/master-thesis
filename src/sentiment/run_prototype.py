"""End-to-end prototype: news sentiment → predict 5-day-ahead close price & direction.

Chains the two models built in this package:
  Model 1  (sentiment_functions)  news text → per-article sentiment score
  features (sentiment_features)    → per-(ticker, date) sentiment + price/TA panel
  Model 2  (price_predictor)       → close[N+5] regression + up/down classification,
                                     scored out-of-sample on a purged walk-forward.

Run:  python -m sentiment.run_prototype            # uses stored sentiment scores
      python -m sentiment.run_prototype --rescore  # re-run Model 1 over the news first

⚠️ Honest expectation baked into the report: forecasting an absolute price 5 days out is
a random walk, so the price head is judged against `close[t+H]=close[t]` and the direction
head against the majority class / 0.5. On the current 3-ticker news set this chain does
NOT beat those baselines — the prototype exists to measure that rigorously, not to imply
the signal is tradeable. See `sentiment/CONTEXT.md`.
"""

from __future__ import annotations

import argparse
import io
import os
import sys

# Windows consoles default to cp1252; force UTF-8 so Vietnamese text / any symbol prints.
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import pandas as pd
from dotenv import load_dotenv

from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Condition,
    DataType,
)
from logger.logger import LogType, Logger
from utils.enums import SqlOperator
from sentiment.price_predictor import evaluate, format_report
from sentiment.sentiment_features import build_event_panel
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import BRONZE_SCHEMA, DATABASE_MAIN_V2, LOG_FILE_BASE, SILVER_SCHEMA

# The stocks_basic columns the price/TA/foreign features need.
_PX_COLS = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close_adjust",
    "volume_matched",
    "value_matched",
    "n_buy_orders",
    "n_sell_orders",
    "foreign_net_value",
    "foreign_buy_value",
    "foreign_sell_value",
    "foreign_own",
]


def _connect(logger: Logger) -> PostgreSQLDriver:
    load_dotenv()
    driver = PostgreSQLDriver(logger=logger)
    driver.connect(
        PostgreSQLConnectionDto(
            logger=logger,
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT"),
            database=DATABASE_MAIN_V2,
        )
    )
    return driver


def _load_sentiment(driver: PostgreSQLDriver, rescore: bool) -> pd.DataFrame:
    """Per-article sentiment scores. Reads the stored `silver.cafef_news_sentiment`,
    or re-runs Model 1 over `bronze.cafef_news` when `--rescore` is passed."""
    if not rescore:
        sent = driver.select(
            schema_name=SILVER_SCHEMA, table_name="cafef_news_sentiment"
        )
        if not sent.empty:
            return sent[["ticker", "timestamp", "sentiment_score"]]

    # Re-score from bronze news via Model 1.
    from sentiment.sentiment_functions import score_news_frame

    news = driver.select(schema_name=BRONZE_SCHEMA, table_name="cafef_news")
    scored = score_news_frame(news)
    out = news[["ticker", "timestamp"]].reset_index(drop=True)
    out["sentiment_score"] = scored["sentiment_score"].reset_index(drop=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rescore",
        action="store_true",
        help="Re-run Model 1 over bronze.cafef_news instead of reading stored scores.",
    )
    parser.add_argument("--horizon", type=int, default=5, help="Forward horizon (days).")
    args = parser.parse_args()

    logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    driver = _connect(logger)
    try:
        sent = _load_sentiment(driver, args.rescore)
        tickers = sorted(sent["ticker"].dropna().unique().tolist())
        # Restrict prices to the news tickers server-side (only those rows are usable;
        # stocks_basic is ~2.4M rows, so filtering in SQL, not pandas, is essential).
        px = driver.select(
            schema_name=SILVER_SCHEMA,
            table_name="stocks_basic",
            columns=_PX_COLS,
            conditions=[
                Condition(
                    column="ticker",
                    operator=SqlOperator.IN,
                    value=tickers,
                    data_type=DataType.VARCHAR(),
                )
            ],
        )

        panel = build_event_panel(sent, px, horizon=args.horizon)
        print(
            f"\npanel: {len(panel)} (ticker,date) rows over {panel['ticker'].nunique()} "
            f"tickers, {panel['date'].min().date()}…{panel['date'].max().date()}"
        )

        results = evaluate(panel, horizon=args.horizon)
        print("\n" + format_report(results) + "\n")

        # One-line verdict.
        allf = next((r for r in results if "all" in r.feature_set), results[-1])
        beat_price = allf.price_beats_rw
        beat_dir = allf.dir_beats_half
        print(
            f"VERDICT (all features): price beats random-walk in "
            f"{beat_price}/{allf.n_folds} folds; direction beats 0.5 in "
            f"{beat_dir}/{allf.n_folds} folds."
        )
    finally:
        driver.disconnect()


if __name__ == "__main__":
    main()
