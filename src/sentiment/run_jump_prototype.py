"""End-to-end: news sentiment → probability of a ≥X% close jump within H days.

Chains: Model 1 sentiment scores (stored, or `--rescore`) → per-(ticker, date) sentiment
panel with the `jump_fwd` target → Logistic + Gradient Boosting classifiers →
out-of-sample ROC-AUC / PR-AUC / Brier / lift on a purged walk-forward.

Run:  python -m sentiment.run_jump_prototype                       # H=5, thr=5%
      python -m sentiment.run_jump_prototype --threshold 0.05 --horizon 5
      python -m sentiment.run_jump_prototype --rescore             # re-run Model 1 first

Sentiment-only features by request. Target base rate ~11% (VCB/FPT/PNJ), so metrics are
ranking/calibration, not accuracy. See `sentiment/CONTEXT.md`.
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
from sentiment.jump_predictor import evaluate, format_report
from sentiment.sentiment_features import SENTIMENT_FEATURES, build_event_panel
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import BRONZE_SCHEMA, DATABASE_MAIN_V2, LOG_FILE_BASE, SILVER_SCHEMA
from utils.enums import SqlOperator

# stocks_basic columns needed to build close_fwd/jump_fwd (price only — features are
# sentiment-only, but we still need close_adjust to compute the target).
_PX_COLS = ["ticker", "date", "open", "high", "low", "close_adjust", "volume_matched"]


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


def _load_sentiment(driver: PostgreSQLDriver, rescore: bool):
    if not rescore:
        sent = driver.select(
            schema_name=SILVER_SCHEMA, table_name="cafef_news_sentiment"
        )
        if not sent.empty:
            return sent[["ticker", "timestamp", "sentiment_score"]]
    from sentiment.sentiment_functions import score_news_frame

    news = driver.select(schema_name=BRONZE_SCHEMA, table_name="cafef_news")
    scored = score_news_frame(news)
    out = news[["ticker", "timestamp"]].reset_index(drop=True)
    out["sentiment_score"] = scored["sentiment_score"].reset_index(drop=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threshold", type=float, default=0.05, help="Jump size (0.05 = 5%).")
    parser.add_argument("--horizon", type=int, default=5, help="Forward horizon (days).")
    parser.add_argument("--rescore", action="store_true", help="Re-run Model 1 first.")
    args = parser.parse_args()

    logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    driver = _connect(logger)
    try:
        sent = _load_sentiment(driver, args.rescore)
        tickers = sorted(sent["ticker"].dropna().unique().tolist())
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

        panel = build_event_panel(
            sent, px, horizon=args.horizon, jump_threshold=args.threshold
        )
        pos = int(panel["jump_fwd"].sum())
        print(
            f"\npanel: {len(panel)} (ticker,date) rows over {panel['ticker'].nunique()} "
            f"tickers, {panel['date'].min().date()}…{panel['date'].max().date()}\n"
            f"target: P(close rises ≥ {args.threshold:.0%} within {args.horizon}d) — "
            f"{pos} positives / {len(panel)} = base rate {pos/len(panel):.1%}\n"
            f"features (sentiment-only, {len(SENTIMENT_FEATURES)}): {SENTIMENT_FEATURES}"
        )

        target_desc = f"close rises ≥{args.threshold:.0%} in {args.horizon}d"
        results = evaluate(
            panel, horizon=args.horizon, features=SENTIMENT_FEATURES
        )
        print("\n" + format_report(results, target_desc) + "\n")

        best = max(results, key=lambda r: r.avg_roc_auc)
        print(
            f"VERDICT: best model = {best.model_name}, avg ROC-AUC {best.avg_roc_auc:.3f} "
            f"(0.5 = no signal), AUC>0.5 in {best.folds_auc_above_half}/{best.n_folds} "
            f"folds, top-decile lift {best.avg_lift:.2f}x base rate."
        )
    finally:
        driver.disconnect()


if __name__ == "__main__":
    main()
