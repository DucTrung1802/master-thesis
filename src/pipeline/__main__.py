# src\pipeline\__main__.py
"""`python -m pipeline [--apply] [--from STAGE] [--only STAGE] [--ticker T] [--table T]`.

    python -m pipeline                          # what is stale; writes nothing
    python -m pipeline --apply                  # run the stale stages
    python -m pipeline --apply --rescrape       # ⚠️ and re-fetch from the network first

    # the pool__basic prototype — one shared report root, its own scoped table
    python -m pipeline --apply \
        --scope basic \
        --table return_5day__final__d20_h5__basic \
        --config vcb__return_5day__final__d20_h5__basic.yaml
"""

import sys

import pandas as pd

from pipeline.stages import (
    DEFAULT_CONFIG,
    DEFAULT_ROOT,
    DEFAULT_SCOPE,
    DEFAULT_TABLE,
    DEFAULT_TICKER,
    run,
)
from utils import runtime


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default):
        return argv[argv.index(flag) + 1] if flag in argv else default

    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 60)

    # ⚠️ `show_gpu` is ON here, unlike the SQL and numpy stages, because this driver
    # calls `apply_model` IN-PROCESS — the chain's one CUDA stage runs inside this
    # process, so the card in the box is a fact about this run. The per-stage clock
    # is in `stages.run`; this is the total, and the two disagree by exactly the
    # `status()` calls, which is the point of printing both.
    with runtime.RunTimer(
        f"pipeline  {option('--ticker', DEFAULT_TICKER)} / "
        f"{option('--table', DEFAULT_TABLE)}"
        f"{'  --apply' if '--apply' in argv else '  (plan only)'}"
        f"{'  --rescrape' if '--rescrape' in argv else ''}",
    ):
        return _main(argv, option)


def _main(argv, option):
    frame = run(
        apply="--apply" in argv,
        start=option("--from", None),
        only=option("--only", None),
        ticker=option("--ticker", DEFAULT_TICKER),
        table=option("--table", DEFAULT_TABLE),
        config=option("--config", DEFAULT_CONFIG),
        root=option("--root", DEFAULT_ROOT),
        scope=option("--scope", DEFAULT_SCOPE),
        # ⚠️ Off unless asked. The scrape hits someone else's servers; every stage
        # below it is local and idempotent. See `stages.apply_data`.
        rescrape="--rescrape" in argv,
    )
    print(f"\n{'=' * 78}")
    print(frame.to_string(index=False))
    if "--apply" not in argv:
        print("\nplan only — pass --apply to run the stages that are not ready")
    return frame


if __name__ == "__main__":
    main()
