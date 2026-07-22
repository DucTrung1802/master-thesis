"""End-to-end: train a news-TEXT model to predict the 5-level PRICE-REACTION sentiment.

Sentiment is (re)defined by the market: a news is VERY_POSITIVE…VERY_NEGATIVE by its
forward price move (exchange-aware bands), NOT by a general-domain language model. This
runner:
  1. loads bronze.cafef_news (text) + silver.stocks_basic (prices),
  2. labels each news by its H-day forward return (price_reaction_labels),
  3. embeds headline+content with PhoBERT (frozen),
  4. trains Logistic + Gradient Boosting to predict the label,
  5. reports out-of-sample macro-F1 + QWK on a purged walk-forward.

Run:  python -m sentiment.run_reaction_prototype                 # H=5, HOSE ±7% / ±2%
      python -m sentiment.run_reaction_prototype --horizon 10 --neutral 0.015
"""

from __future__ import annotations

import argparse
import io
import os
import sys

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from dotenv import load_dotenv

from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    Condition,
    DataType,
)
from logger.logger import LogType, Logger
from sentiment.price_reaction_labels import (
    DEFAULT_HORIZON,
    DEFAULT_NEUTRAL_BAND,
    build_price_reaction_labels,
    label_distribution,
)
from sentiment.text_reaction_model import build_text, embed_texts, evaluate, format_report
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import BRONZE_SCHEMA, DATABASE_MAIN_V2, LOG_FILE_BASE, SILVER_SCHEMA
from utils.enums import SqlOperator


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument(
        "--neutral", type=float, default=DEFAULT_NEUTRAL_BAND, help="Neutral dead-band."
    )
    args = parser.parse_args()

    logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    driver = _connect(logger)
    try:
        news = driver.select(schema_name=BRONZE_SCHEMA, table_name="cafef_news")
        news = news[news["headline"].notna()]
        news = news[news["headline"].astype(str).str.strip() != ""].reset_index(drop=True)
        tickers = sorted(news["ticker"].dropna().unique().tolist())

        px = driver.select(
            schema_name=SILVER_SCHEMA,
            table_name="stocks_basic",
            columns=["ticker", "date", "close_adjust"],
            conditions=[
                Condition(
                    column="ticker",
                    operator=SqlOperator.IN,
                    value=tickers,
                    data_type=DataType.VARCHAR(),
                )
            ],
        )

        ev = build_price_reaction_labels(
            news, px, horizon=args.horizon, neutral_band=args.neutral
        )
        dist = label_distribution(ev)
        print(
            f"\nlabelled news events: {len(ev)} over {ev['ticker'].nunique()} tickers, "
            f"{ev['event_date'].min().date()}…{ev['event_date'].max().date()}\n"
            f"target: 5-level PRICE reaction over {args.horizon}d "
            f"(NEUTRAL ±{args.neutral:.0%}, VERY = exchange daily limit)\n"
            f"label distribution:"
        )
        for level, count in dist.items():
            print(f"   {level:14s}: {count:5d}  ({count/len(ev):5.1%})")

        print("\nembedding headline+content with PhoBERT (frozen)…")
        texts = build_text(ev["headline"], ev["content"])
        X = embed_texts(texts)
        print(f"embedded {len(X)} texts, dim={X.shape[1]}")

        results = evaluate(ev, X, horizon=args.horizon)
        target_desc = f"5-level, {args.horizon}d, NEU±{args.neutral:.0%}"
        print("\n" + format_report(results, target_desc) + "\n")

        best = max(results, key=lambda r: r.avg_qwk)
        print(
            f"VERDICT: best model = {best.model_name}, macro-F1 {best.avg_macro_f1:.3f} "
            f"(baseline {best.avg_macro_f1_baseline:.3f}), QWK {best.avg_qwk:.3f} "
            f"(baseline {best.avg_qwk_baseline:.3f})."
        )
    finally:
        driver.disconnect()


if __name__ == "__main__":
    main()
