"""The ONE place the model chain's current experiment is named.

⚠️ **Why this module exists.** On 2026-08-16 three stages of the same six-stage
chain carried three independently-edited defaults:

| module | defaulted to |
|---|---|
| `pipeline/stages.py` | `close_adjust_5day__final__d20_h5` |
| `train_test_creator/dataset.py` | `return_5day__final__d20_h5` |
| `model/lstm/train.py` | `lstm__vcb__return_5day__final__d20_h5.yaml` |

So `python -m pipeline` planned one experiment and `python -m train_test_creator
--save` — the command `pipeline` itself prints as the next step — went looking for
a different table and died with `does not exist`. Nothing was wrong with either
module in isolation; they simply disagreed, and a chain that disagrees with itself
about what it is building has no default worth having.

The names below are the chain's CURRENT experiment. Change them here and every
stage moves together; pass an explicit `--table` / `--config` / `--target` to run
anything else. Nothing here is a research claim — see CLAUDE.md §2 for what the
current target is actually worth (`close_adjust_5day` is a price LEVEL whose
selection failed its own null on 2026-08-16, and it is the default because it is
what the chain is wired to today, not because it works).

⚠️ **`d` and `h` still come from the TABLE NAME downstream, never from here.**
`train_test_creator` parses them out of `…__final__d20_h5` and asserts them against
the dataset; this module only spells the name in the first place. Two sources for
one fact is the bug this module was written to remove, so do not add a third by
reading `DEFAULT_LOOKBACK` where a table name is already in hand.
"""

from __future__ import annotations

# The chain's current experiment. One ticker, one target, one window.
DEFAULT_TICKER = "vcb"
DEFAULT_TARGET = "close_adjust_5day"
DEFAULT_LOOKBACK = 20
DEFAULT_HORIZON = 5

# `final_features` strips a `cs_` prefix when it names the table (its CONTEXT §5),
# so `cs_rank_5day` becomes `rank_5day__final__…`. Mirrored here so a cross-sectional
# default spells the same table name the builder would create.
CROSS_SECTIONAL_PREFIX = "cs_"

__all__ = [
    "CROSS_SECTIONAL_PREFIX",
    "DEFAULT_HORIZON",
    "DEFAULT_LOOKBACK",
    "DEFAULT_TARGET",
    "DEFAULT_TICKER",
    "config_name",
    "dataset_name",
    "final_table",
    "schema",
]


def _target(target: str | None) -> str:
    name = DEFAULT_TARGET if target is None else target
    return name[len(CROSS_SECTIONAL_PREFIX):] if name.startswith(CROSS_SECTIONAL_PREFIX) else name


def final_table(
    target: str | None = None,
    lookback: int = DEFAULT_LOOKBACK,
    horizon: int = DEFAULT_HORIZON,
) -> str:
    """`<target>__final__d<d>_h<h>` — what `final_features` creates and everything reads."""
    return f"{_target(target)}__final__d{lookback}_h{horizon}"


def config_name(
    model: str = "lstm",
    ticker: str | None = None,
    target: str | None = None,
    lookback: int = DEFAULT_LOOKBACK,
    horizon: int = DEFAULT_HORIZON,
) -> str:
    """`<model>__<ticker>__<table>.yaml` under `model/<model>/configs/`.

    ⚠️ The `<model>__` prefix is load-bearing and `RUN__lstm.ipynb` omitted it until
    2026-08-16, which left the notebook's default pointing at a file that does not
    exist. Build the name; do not spell it.
    """
    who = DEFAULT_TICKER if ticker is None else ticker
    return f"{model}__{who}__{final_table(target, lookback, horizon)}.yaml"


def dataset_name(
    ticker: str | None = None,
    target: str | None = None,
    lookback: int = DEFAULT_LOOKBACK,
    horizon: int = DEFAULT_HORIZON,
    train: float = 0.70,
    val: float = 0.15,
    scaled: bool = True,
) -> str:
    """The folder `train_test_creator --save` writes under `src/train_test_set/`.

    ⚠️ Derived here for a CALLER that wants to predict the name. `TrainTestCreator`
    still builds its own from the ratios it was actually given — this must not become
    the second place a dataset can be named.
    """
    who = DEFAULT_TICKER if ticker is None else ticker
    test = 1.0 - train - val
    tail = "std" if scaled else "raw"
    return (
        f"{who}__{final_table(target, lookback, horizon)}"
        f"__tr{round(train * 100):d}_val{round(val * 100):d}_test{round(test * 100):d}__{tail}"
    )


def schema(ticker: str | None = None) -> str:
    """`unified_schema_<ticker>`, lower-cased the way the assets create it."""
    return f"unified_schema_{(DEFAULT_TICKER if ticker is None else ticker).lower()}"
