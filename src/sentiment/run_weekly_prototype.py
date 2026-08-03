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
from sentiment.news_panel import DAILY_CONTROL_FEATURES, DAILY_NEWS_FEATURES
from sentiment.weekly_xsec import (
    CONTROL_FEATURES,
    EDITORIAL_FEATURES,
    NEWS_FEATURES,
    ROUND_TRIP_COST,
    build_labels,
    evaluate,
    format_report,
)

#: `--grain` presets. The daily one is `rel5`/`rel10` — the target experiment_3.3 settled
#: on — and it exists because weekly formation answers a slightly different question:
#: it rebalances once a week, where `rel5` forms every session.
GRAINS = {
    "weekly": dict(
        table="news_weekly_panel", date_col="week_start", close_col="close_last",
        value_col="value_w", horizons="1,2", per_year=52.0, lookback=12, min_train=104,
        rank_col="n_editorial",
    ),
    "daily": dict(
        table="news_daily_panel", date_col="date", close_col="close_adjust",
        value_col="value_matched", horizons="5,10", per_year=252.0, lookback=60,
        min_train=504, rank_col="n_editorial_5d",
    ),
}
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
    ap.add_argument(
        "--grain",
        choices=list(GRAINS),
        default="weekly",
        help="'daily' = rel5/rel10, the experiment_3.3 target; 'weekly' = paper 57's design",
    )
    ap.add_argument("--horizons", default=None, help="forward horizons, in the grain's unit")
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

    cfg = GRAINS[args.grain]
    unit = "d" if args.grain == "daily" else "w"

    logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    driver = _connect(logger)

    # ⚠️ Raw cursor with explicit float casts, not driver.select: the daily panel is 2.1M
    # rows and `numeric` comes back as Decimal→object, which is the thing that has stalled
    # runs on stocks_basic before (sentiment/CONTEXT.md §5).
    with driver._cursor_ctx() as cur:
        cur.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{GOLD_SCHEMA}' AND table_name = '{cfg['table']}' "
            f"ORDER BY ordinal_position"
        )
        cols = cur.fetchall()
        select = ", ".join(
            f"{c}::double precision AS {c}" if t in ("numeric", "bigint") else c
            for c, t in cols
        )
        cur.execute(f"SELECT {select} FROM {GOLD_SCHEMA}.{cfg['table']}")
        panel = pd.DataFrame(cur.fetchall(), columns=[c for c, _ in cols])

    date_col = cfg["date_col"]
    panel[date_col] = pd.to_datetime(panel[date_col])
    print(
        f"gold.{cfg['table']} [{args.grain}]: {len(panel):,} rows, "
        f"{panel['ticker'].nunique()} tickers, "
        f"{panel[date_col].min():%Y-%m-%d} → {panel[date_col].max():%Y-%m-%d}"
    )
    print("  (the 2012-06→11 scrape hole is cut in the PANEL — see news_panel.PANEL_START)")

    if args.top_tickers:
        rank = panel.groupby("ticker")[cfg["rank_col"]].sum().sort_values(ascending=False)
        keep = rank.head(args.top_tickers).index
        panel = panel[panel["ticker"].isin(keep)]
        print(
            f"  top-{args.top_tickers} by editorial count → {len(panel):,} rows: "
            f"{', '.join(list(keep)[:12])}…"
        )

    if args.grain == "daily":
        sets = [
            ("controls", DAILY_CONTROL_FEATURES),
            ("news (all)", DAILY_NEWS_FEATURES),
            ("controls + news", DAILY_CONTROL_FEATURES + DAILY_NEWS_FEATURES),
        ]
        news_flag, ed_flag = "if_news_5d", "if_editorial_5d"
    else:
        sets = [
            ("controls", CONTROL_FEATURES),
            ("news (all)", NEWS_FEATURES),
            ("news (editorial)", EDITORIAL_FEATURES),
            ("controls + news", CONTROL_FEATURES + NEWS_FEATURES),
            ("controls + editorial", CONTROL_FEATURES + EDITORIAL_FEATURES),
        ]
        news_flag, ed_flag = "if_news", "if_editorial"

    results = []
    for horizon in [int(h) for h in (args.horizons or cfg["horizons"]).split(",")]:
        labelled = build_labels(
            panel, horizon=horizon, cost=args.cost, lookback=cfg["lookback"],
            date_col=date_col, close_col=cfg["close_col"], value_col=cfg["value_col"],
        )
        uni = labelled[labelled["in_universe"] & labelled["label"].notna()]
        tradeable = labelled.drop_duplicates("widx")["tradeable_week"]
        print(
            f"\nh={horizon:>2}{unit}  labelled rows {len(uni):>8,}  "
            f"periods {uni['widx'].nunique():>5}  "
            f"news coverage {uni[news_flag].mean():.1%}  "
            f"editorial {uni[ed_flag].mean():.1%}  "
            f"periods whose q75 clears cost: {tradeable.mean():.1%}"
        )
        for name, feats in sets:
            results.append(
                evaluate(
                    labelled, feats, name, horizon, n_folds=args.folds, cost=args.cost,
                    periods_per_year=cfg["per_year"], min_train=cfg["min_train"],
                )
            )

    print(
        format_report(
            results,
            cost=args.cost,
            unit="trading session(s)" if args.grain == "daily" else "week(s)",
        )
    )
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
