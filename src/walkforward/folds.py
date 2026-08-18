# src\walkforward\folds.py
"""Fold geometry for an expanding walk-forward, and the builder that honours it.

⚠️ **WHY THIS EXISTS.** Every backtested number in this repo — including the
`Sharpe +1.484` in `backtest/CONTEXT.md` §4 — comes from ONE train/val/test split whose
test window happens to be a +20.2 %/yr VNINDEX bull market. A single split cannot tell
*"the edge decayed"* from *"this split was lucky"*, and the 2022-2026 rows in
`backtest/CONTEXT.md` §8g are exactly the case where those two readings disagree.
`model/CONTEXT.md` §11 used 28 expanding folds; this brings the current chain to the
same standard. TODO **PRF-1**.

**The geometry.** For a fold whose test block is `[t_i, t_{i+1})`:

```
|<--------------- train --------------->|<-- val -->|<-- test -->|
                                       gap         gap
```

train expands from the start of the sample; `val` is a fixed window immediately before
the test block and exists only to choose the early-stopping epoch; `test` is untouched.
The `d + h − 1` purge is applied at BOTH interior boundaries by `TrainTestCreator` itself —
this module only moves the boundaries.

⚠️ **THE SCALER, THE IMPUTATION MEDIAN AND THE FEATURE SCREEN ARE REFIT PER FOLD**,
because `TrainTestCreator` computes all three from the rows a train SAMPLE carries. Reusing
one split's tensors and re-slicing them would leak later statistics into earlier folds —
mild, but it is exactly the kind of leak this exercise exists to remove.

⚠️ **WHAT IS STILL LOOK-AHEAD, AND IT MUST BE SAID:** the 13 channels were selected on
the WHOLE sample against the label. Re-running the selection per fold is ~6 GPU-hours per
fold on a T4 and is not affordable here (§15c). So the LEVEL of every fold is optimistic;
what stays honest is the SHAPE — the comparison across folds, since every fold carries the
identical advantage. A decaying fold series is therefore still evidence of decay.
`model/CONTEXT.md` §11 made the same trade.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

import pandas as pd

from train_test_creator.dataset import SplitBounds, TrainTestCreator


@dataclass(frozen=True)
class Fold:
    """One expanding-window fold, named by the period it is out of sample for."""

    index: int
    tag: str
    train_end: pd.Timestamp
    val_end: pd.Timestamp
    test_end: pd.Timestamp

    def describe(self) -> str:
        return (
            f"fold {self.index:2d} {self.tag}  train < {self.train_end.date()}  "
            f"val < {self.val_end.date()}  test < {self.test_end.date()}"
        )


def make_folds(
    dates: Sequence,
    first_test: str,
    step_months: int = 12,
    val_months: int = 12,
    last_test: Optional[str] = None,
) -> List[Fold]:
    """Expanding folds, one per `step_months` block from `first_test` onward.

    ⚠️ A fold is emitted only when its test block is non-empty AND the sample actually
    reaches into it — a trailing partial block is kept (it is real out-of-sample data)
    but an empty one is not, because an empty test block would raise deep inside the
    builder with a message about ratios rather than about dates.
    """
    unique = pd.DatetimeIndex(sorted(pd.unique(pd.DatetimeIndex(dates))))
    if unique.empty:
        raise ValueError("no dates")
    start = pd.Timestamp(first_test)
    stop = pd.Timestamp(last_test) if last_test else unique[-1] + pd.Timedelta(days=1)
    if start <= unique[0]:
        raise ValueError(f"first_test {start.date()} is at or before the sample start")

    folds: List[Fold] = []
    edge = start
    while edge < stop:
        test_end = min(edge + pd.DateOffset(months=step_months), stop)
        if not ((unique >= edge) & (unique < test_end)).any():
            edge = test_end
            continue
        val_start = edge - pd.DateOffset(months=val_months)
        if not ((unique >= val_start) & (unique < edge)).any():
            raise ValueError(f"fold at {edge.date()} has an empty val window")
        folds.append(
            Fold(
                index=len(folds),
                tag=f"oos{edge.year}" if step_months == 12 else f"oos{edge.date()}",
                train_end=val_start,
                val_end=edge,
                test_end=test_end,
            )
        )
        edge = test_end
    if not folds:
        raise ValueError(f"no non-empty test block from {start.date()}")
    return folds


class FoldBuilder(TrainTestCreator):
    """`TrainTestCreator` with the split boundaries given as DATES, not as ratios.

    ⚠️ The base class cuts at `int(n_dates × ratio)`, which cannot express "test is
    calendar 2019". Overriding `_bounds` is the whole change — `_fit_mask`,
    `_sample_ranges` and the purge all read `SplitBounds`, so they follow for free and
    none of the leak-prevention logic is reimplemented here.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fold: Optional[Fold] = None

    def use(self, fold: Fold) -> "FoldBuilder":
        self._fold = fold
        return self

    @property
    def name(self) -> str:
        base = super().name
        return base if self._fold is None else f"{base}__{self._fold.tag}"

    def _bounds(self, dates: pd.Series) -> SplitBounds:
        if self._fold is None:
            return super()._bounds(dates)
        unique = pd.DatetimeIndex(sorted(pd.unique(pd.DatetimeIndex(dates))))
        n_train = int((unique < self._fold.train_end).sum())
        n_val = int(
            ((unique >= self._fold.train_end) & (unique < self._fold.val_end)).sum()
        )
        n_test = int(
            ((unique >= self._fold.val_end) & (unique < self._fold.test_end)).sum()
        )
        if not (n_train and n_val and n_test):
            raise ValueError(
                f"{self._fold.tag}: empty block — train {n_train} val {n_val} "
                f"test {n_test}. The frame must be sliced to `date < test_end` first."
            )
        return SplitBounds(
            train_end_date=self._fold.train_end,
            val_end_date=self._fold.val_end,
            n_dates=n_train + n_val + n_test,
            train_dates=n_train,
            val_dates=n_val,
            test_dates=n_test,
        )
