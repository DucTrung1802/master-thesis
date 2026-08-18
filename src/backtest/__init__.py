# src\backtest\__init__.py
"""Stage 9 — the costed backtest. `python -m backtest --run <run_id>`.

`result_evaluator` answers *does the score rank?*; this answers *does the ranking pay
for its own trading?* — the two are different questions and the repo has only ever had
the first. See `portfolio.py` for the cost convention and why every table carries
`se_sharpe`.
"""

from backtest.portfolio import (
    COST_SWEEP,
    ENTER_PERCENTILE,
    EXIT_PERCENTILE,
    ROUND_TRIP_COST,
    SESSIONS_PER_YEAR,
    Track,
    buy_and_hold,
    long_flat_single,
    long_only_top_k,
    rebalance_dates,
    stats,
    turnover_cost,
)

__all__ = [
    "COST_SWEEP", "ENTER_PERCENTILE", "EXIT_PERCENTILE", "ROUND_TRIP_COST",
    "SESSIONS_PER_YEAR", "Track", "buy_and_hold", "long_flat_single",
    "long_only_top_k", "rebalance_dates", "stats", "turnover_cost",
]
