# src\feature_selection\prune.py
"""Choose which channels of a wide pool are even OFFERED to a selection — `PRF-9`.

    python -m feature_selection.prune --ticker ALL --pool pool__ta --budget 50

⚠️ **THIS EXISTS BECAUSE OF `MEM-1`, NOT BECAUSE OF ANY BELIEF ABOUT THE CHANNELS.**
`pool__ta` holds **711 numeric channels** that can rank a cross-section, against
`pool__basic`'s 90 — `PRF-9`'s survey found 71 of 76 gold tables are date-only and
therefore *structurally* incapable of ranking (a column identical for every ticker on a
date has a constant within-date rank). So `pool__ta` is the only real widening available.
But the selection design is `rows × channels × 6 window stats × float64`, and on
624,448 rows a 90-channel run already peaked at **16.3 GB** of host RAM against a Kaggle
box's ~29-30 GB. The whole pool cannot be offered at once.

⚠️ **EVERY CRITERION HERE IS LABEL-FREE, AND THAT IS THE POINT.** Ranking channels by
their correlation with the TARGET and keeping the best would build `PRF-7`'s selection
look-ahead *into the candidate set* — before the selection ran, before any null could
see it, and in a way no downstream bar could price. A channel is kept or dropped here on
**coverage** and on **redundancy against other channels**, never on the label.

## What it does, in order

1. **Coverage screen.** A channel present on fewer than `min_coverage` of rows is
   dropped. ⚠️ Read `COV-1`: coverage is a scalar and a scalar cannot see a frozen
   source — a late starter and a channel dead since June score alike.
2. **Correlation prune.** Among channels correlating above `corr_threshold`, keep one
   representative. This is the same operation `FeatureSelector` already performs inside
   a run; doing it EARLIER changes nothing about which family wins, only which member
   of a redundant family survives to be offered.
3. **Budget cut, if one is given.** ⚠️ **This is the arbitrary step and it is labelled
   as such.** When the pruned set still exceeds what memory allows, the survivors are
   cut to `budget` — deterministically, by coverage then name. It is not a claim that
   the kept ones are better; it is a claim that the box is finite. The dropped list is
   returned so the next run can offer a different slice.

⚠️ **The representative is chosen by COVERAGE, then alphabetically.** Both are label-free
and both are deterministic, which matters because this list becomes part of an
experiment's definition — a prune that shuffles between runs would make two selections
incomparable for a reason nobody recorded.

⚠️ **The correlation is measured on a DATE SAMPLE, not on every row.** Redundancy between
two channels is a structural fact and does not need 624 k rows to see; reading them all
as float64 is ~3.5 GB and this step is supposed to be the cheap one.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

#: Rows a channel must be present on to be offered at all.
MIN_COVERAGE = 0.95

#: Above this |correlation| two channels are treated as one family.
CORR_THRESHOLD = 0.90

#: Keep every Nth day-of-year when sampling for the correlation estimate.
SAMPLE_EVERY = 8


@dataclass
class PruneResult:
    """What survived, what did not, and why — the artefact the export records."""

    pool: str
    kept: List[str]
    n_candidates: int
    dropped_coverage: List[str] = field(default_factory=list)
    dropped_redundant: Dict[str, str] = field(default_factory=dict)
    dropped_budget: List[str] = field(default_factory=list)
    coverage: Dict[str, float] = field(default_factory=dict)
    sample_rows: int = 0
    sample_dates: int = 0

    def describe(self) -> str:
        return (
            f"{self.pool}: {self.n_candidates} candidates -> {len(self.kept)} offered\n"
            f"  dropped {len(self.dropped_coverage)} on coverage < threshold\n"
            f"  dropped {len(self.dropped_redundant)} as redundant (kept a representative)\n"
            f"  dropped {len(self.dropped_budget)} to fit the memory budget "
            f"(⚠️ arbitrary — see the module docstring)\n"
            f"  measured on {self.sample_rows:,} sampled rows over {self.sample_dates} dates"
        )


def numeric_channels(column_types: Dict[str, str], keys: Sequence[str]) -> List[str]:
    """The channels a selection could rank. Booleans and keys are not among them.

    ⚠️ `pool__ta` carries **208 boolean flags** beside its 711 numeric channels
    (`close_gt_ema_50`, `close_bb_20_above_upper`, …). They are excluded here: a window
    design takes mean/sd/slope/min/max of each channel, and those statistics over a 0/1
    flag are a different kind of feature than the pool's numeric ones. Offering them is
    a separate decision, not a side effect of widening.
    """
    numeric_kinds = ("int", "numeric", "double", "real", "float")
    return [
        c
        for c, kind in column_types.items()
        if c not in set(keys) and any(k in str(kind).lower() for k in numeric_kinds)
    ]


def prune_frame(
    frame: pd.DataFrame,
    channels: Sequence[str],
    pool: str = "pool",
    min_coverage: float = MIN_COVERAGE,
    corr_threshold: float = CORR_THRESHOLD,
    budget: Optional[int] = None,
) -> PruneResult:
    """The pure half: given a sampled frame, decide which channels are offered."""
    channels = list(channels)
    block = frame[channels]
    coverage = block.notna().mean()

    survivors = [c for c in channels if coverage[c] >= min_coverage]
    result = PruneResult(
        pool=pool,
        kept=[],
        n_candidates=len(channels),
        dropped_coverage=[c for c in channels if coverage[c] < min_coverage],
        coverage={c: float(coverage[c]) for c in channels},
        sample_rows=len(frame),
        sample_dates=int(frame["date"].nunique()) if "date" in frame else 0,
    )

    # ⚠️ A constant channel correlates with nothing and would survive every prune while
    # carrying no information at all. Dropped as redundant with itself.
    constant = [c for c in survivors if block[c].nunique(dropna=True) <= 1]
    for c in constant:
        result.dropped_redundant[c] = "constant"
    survivors = [c for c in survivors if c not in set(constant)]

    if survivors:
        # RANK first: this is |Spearman|, which is the right notion of redundancy for
        # channels the selection will rank anyway, and it is immune to the outliers
        # `cross_sectional.py` §3 records (a return of −781 exists in this database).
        matrix = block[survivors].rank().corr().abs().to_numpy()
        index = {c: i for i, c in enumerate(survivors)}
        order = sorted(survivors, key=lambda c: (-coverage[c], c))
        kept: List[str] = []
        for candidate in order:
            i = index[candidate]
            twin = next(
                (k for k in kept if matrix[i, index[k]] >= corr_threshold), None
            )
            if twin is None:
                kept.append(candidate)
            else:
                result.dropped_redundant[candidate] = twin
    else:
        kept = []

    kept = sorted(kept, key=lambda c: (-coverage[c], c))
    if budget is not None and len(kept) > budget:
        result.dropped_budget = kept[budget:]
        kept = kept[:budget]

    result.kept = sorted(kept)
    return result


def prune_pool(
    ticker: str,
    pool: str,
    tickers: Optional[Sequence[str]] = None,
    min_coverage: float = MIN_COVERAGE,
    corr_threshold: float = CORR_THRESHOLD,
    budget: Optional[int] = None,
    sample_every: int = SAMPLE_EVERY,
) -> PruneResult:
    """Read a date sample of `pool` and prune it. The database half."""
    from feature_selection.unified_reader import KEY_COLS, UnifiedSchemaReader

    with UnifiedSchemaReader(ticker) as reader:
        schema = reader.schema
        channels = numeric_channels(reader.column_types(pool), KEY_COLS)
        # ⚠️ float8 cast in SQL, not a pandas round trip: psycopg2 returns `numeric` as
        # `Decimal`, which lands as dtype `object` and makes `.rank()` a Python-level
        # sort over 711 columns (CLAUDE.md §5 rule 15, one consequence further on).
        selected = ", ".join(f'"{c}"::float8 AS "{c}"' for c in channels)
        where = ["TRUE"]
        params: List = []
        if tickers is not None:
            where.append("ticker = ANY(%s)")
            params.append(list(tickers))
        if sample_every > 1:
            where.append(f"EXTRACT(DOY FROM date)::int %% {int(sample_every)} = 0")
        with reader.driver._cursor_ctx() as cur:
            cur.execute(
                f'SELECT "date", "ticker", {selected} FROM {schema}.{pool} '
                f"WHERE {' AND '.join(where)}",
                tuple(params),
            )
            rows = cur.fetchall()
            head = [d[0] for d in cur.description]

    frame = pd.DataFrame(rows, columns=head)
    return prune_frame(
        frame, channels, pool=pool, min_coverage=min_coverage,
        corr_threshold=corr_threshold, budget=budget,
    )


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    import json

    ticker = str(option("--ticker", "ALL"))
    pool = str(option("--pool", "pool__ta"))
    budget = option("--budget")
    budget = int(budget) if budget is not None else None
    corr = float(option("--corr", CORR_THRESHOLD))
    universe = option("--universe-from")

    names = None
    if universe:
        from feature_selection.unified_reader import UnifiedSchemaReader

        with UnifiedSchemaReader(ticker) as reader:
            names = sorted(
                reader.read(universe, columns=["ticker"])["ticker"]
                .astype(str).str.upper().unique()
            )
        print(f"universe: {len(names)} names from {universe}")

    result = prune_pool(ticker, pool, tickers=names, corr_threshold=corr, budget=budget)
    print(result.describe())
    out = option("--out")
    if out:
        with open(out, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "pool": result.pool, "kept": result.kept,
                    "n_candidates": result.n_candidates,
                    "corr_threshold": corr, "budget": budget,
                    "min_coverage": MIN_COVERAGE,
                    "n_dropped_coverage": len(result.dropped_coverage),
                    "n_dropped_redundant": len(result.dropped_redundant),
                    "n_dropped_budget": len(result.dropped_budget),
                    "criteria": "LABEL-FREE: coverage, then |Spearman| redundancy, then "
                                "a deterministic budget cut. Never the target.",
                },
                handle, indent=2, ensure_ascii=False,
            )
        print(f"wrote {out}")
    return result


if __name__ == "__main__":
    main()
