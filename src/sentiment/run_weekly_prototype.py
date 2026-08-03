"""guidance.md §8 step 6 — the costed walk-forward on NEWS COUNTS ONLY, no sentiment.

Reads `gold.news_weekly_panel`, builds the cross-sectional quantile labels, and runs a
purged walk-forward over three feature sets. **The ablation is the finding:**

    controls          momentum / turnover / sessions      ← the baseline to beat
    news              if_news, counts, event categories   ← paper 57's publication effect
    controls + news   both                                ← does news ADD anything?

Run:  python -m sentiment.run_weekly_prototype
      python -m sentiment.run_weekly_prototype --horizons 1,4,13 --cost 0.005

⚠️ Expectation, set from the literature before the numbers arrive: paper 51 — the only
properly-run study in `experiment/experiment_10` — reports **MCC 0.069 before costs** on
8.5M articles, and traces its whole outperformance to one month in 2011. Paper 63 got
50.4% out-of-sample on a balanced binary task with Reuters text and tick-level labels.
A large positive here would be evidence of a bug, not of a signal.
"""

from __future__ import annotations

import argparse
import io
import os
import sys

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import pandas as pd
from dotenv import load_dotenv

from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from logger.logger import LogType, Logger
from sentiment.weekly_xsec import (
    CONTROL_FEATURES,
    EDITORIAL_FEATURES,
    NEWS_FEATURES,
    ROUND_TRIP_COST,
    build_labels,
    evaluate,
    format_report,
)
from tabular_database_driver.postgre_sql_driver import PostgreSQLDriver
from utils.constants import DATABASE_MAIN_V2, GOLD_SCHEMA, LOG_FILE_BASE


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
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--horizons", default="1,2,4,8,13", help="forward horizons in WEEKS")
    ap.add_argument("--cost", type=float, default=ROUND_TRIP_COST)
    ap.add_argument("--folds", type=int, default=6)
    ap.add_argument(
        "--top-tickers",
        type=int,
        default=0,
        help="restrict to the N best-covered tickers by editorial count (0 = all). "
        "⚠️ The decisive test: if tone cannot help where coverage is HIGHEST, it "
        "cannot help anywhere.",
    )
    args = ap.parse_args()

    logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    driver = _connect(logger)
    panel = driver.select(schema_name=GOLD_SCHEMA, table_name="news_weekly_panel")
    panel["week_start"] = pd.to_datetime(panel["week_start"])
    print(
        f"gold.news_weekly_panel: {len(panel):,} ticker-weeks, "
        f"{panel['ticker'].nunique()} tickers, "
        f"{panel['week_start'].min():%Y-%m-%d} → {panel['week_start'].max():%Y-%m-%d}"
    )
    print("  (the 2012-06→11 scrape hole is cut in the PANEL now — see news_panel.PANEL_START)")

    if args.top_tickers:
        rank = (
            panel.groupby("ticker")["n_editorial"].sum().sort_values(ascending=False)
        )
        keep = rank.head(args.top_tickers).index
        panel = panel[panel["ticker"].isin(keep)]
        print(
            f"  top-{args.top_tickers} by editorial count → {len(panel):,} ticker-weeks: "
            f"{', '.join(list(keep)[:12])}…"
        )

    results = []
    for horizon in [int(h) for h in args.horizons.split(",")]:
        labelled = build_labels(panel, horizon=horizon, cost=args.cost)
        uni = labelled[labelled["in_universe"] & labelled["label"].notna()]
        tradeable = labelled.drop_duplicates("widx")["tradeable_week"]
        print(
            f"\nh={horizon:>2}w  labelled rows {len(uni):>7,}  "
            f"weeks {uni['widx'].nunique():>4}  "
            f"news coverage {uni['if_news'].mean():.1%}  "
            f"editorial {uni['if_editorial'].mean():.1%}  "
            f"weeks whose q75 clears cost: {tradeable.mean():.1%}"
        )
        for name, feats in [
            ("controls", CONTROL_FEATURES),
            ("news (all)", NEWS_FEATURES),
            ("news (editorial)", EDITORIAL_FEATURES),
            ("controls + news", CONTROL_FEATURES + NEWS_FEATURES),
            ("controls + editorial", CONTROL_FEATURES + EDITORIAL_FEATURES),
        ]:
            results.append(
                evaluate(labelled, feats, name, horizon, n_folds=args.folds, cost=args.cost)
            )

    print(format_report(results, cost=args.cost))
    print(
        "\nVERDICT — read the PAIRED table, not the levels.\n"
        "  ΔMCC is 'controls + news' minus 'controls' on the SAME folds. If |t| < 2 and\n"
        "  the fold count is near half, the news block adds nothing measurable, and\n"
        "  steps 7-13 (annotation, fine-tune, LIME) buy nothing on this data.\n"
        "  Reference points: paper 51 MCC 0.069 (8.5M articles, done properly);\n"
        "  paper 63 50.4% out-of-sample on a balanced binary task with tick-level labels."
    )


if __name__ == "__main__":
    main()
