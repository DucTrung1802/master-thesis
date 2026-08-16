# src\feature_selection\__init__.py
"""Feature selection over a per-ticker `unified_schema_<ticker>` schema.

⚠️ **This docstring listed SIX modules until 2026-08-16 and there were fourteen** —
`run.py`, the actual entry point, was not among them. A package reads as sprawl when
it has no map, so here is the whole of it, in the order data moves:

    read        unified_reader.py   the `pool__*` tables, JOINed on `(exchange, ticker, date)` ∩
                contract.py         ⚠️ the interface to `final_features` — filenames, keys, checks
    shape       windows.py          daily panel → windowed samples; scoring CHANNELS, not columns
    rank        selector.py         rankers → ensemble → correlation prune → purged walk-forward
                gpu.py              the CUDA paths, and which steps measured SLOWER on the card
                gpu_rankers.py      the two rankers sklearn owns, reimplemented for the GPU
    judge       evaluation.py       the BAR — the shuffled-label null, `n_eff`, the IC summary
                selection_cut.py    how many channels a run supports (replaced `max_features=12`)
    panels      cross_sectional.py  N × T — per-date target and IC, date-grouped CV, panel null
    emit        report.py           one run → one self-describing folder
                plots.py            the figures — one theme, one palette
                outstanding.py      one run → its final feature list, each mapped back to its pool
    drive       run.py              `python -m feature_selection.run` — the scripted entry point
    meta        ranker_eval.py      what each ranker is WORTH (the module behind §19)

⚠️ **NINE of those fourteen are imported by name from OUTSIDE this package**, so their
module paths are API and cannot be renamed without touching six other packages
(measured 2026-08-16): `unified_reader`, `report`, `outstanding` and `contract` by
`final_features` / `pipeline` / `train_test_creator` / `kaggle_gpu`; `evaluation` and
`plots` by `result_evaluator`; `selector` and `windows` by `kaggle_gpu`; `run` by
`orchestration`. The other five — `gpu`, `gpu_rankers`, `cross_sectional`,
`selection_cut`, `ranker_eval` — are internal and free to move.

`RUN__feature_importance_report.ipynb` is the ONLY notebook meant to be run — set its
parameter cell, Run All, get an archived report folder. The modules hold nothing
notebook-specific, so the same run can be scripted; `run.py` is that script.

Three subfolders, all added 2026-08-16 and none of them importable API:

    tests/      the 8 test modules (85 tests). ⚠️ `tests/__init__.py` is load-bearing
    studies/    the four finished `study_*.ipynb` write-ups — the record, not entry points
    docs/       NULL_DRAWS.md, NULL_DRAWS_VI.md, RANKER_COMPARISON.md

⚠️ `CONTEXT.md` stays at the top level — every package in this repo keeps its
CONTEXT.md at its root, and CLAUDE.md §7 links them all by that path.
"""

from feature_selection.unified_reader import (
    KEY_COLS,
    UnifiedSchemaReader,
    unified_schema_name,
)
from feature_selection.selector import (
    FeatureSelector,
    PurgedWalkForward,
    SelectionResult,
)
from feature_selection.gpu import cuda_available, device_report, resolve_device
from feature_selection.windows import (
    NORMALIZATIONS,
    WINDOW_STATS,
    usable_sample_count,
    window_design,
)
from feature_selection.evaluation import (
    NullResult,
    block_shuffle,
    effective_sample,
    ic_summary,
    null_distribution,
)

__all__ = [
    "NORMALIZATIONS",
    "WINDOW_STATS",
    "usable_sample_count",
    "window_design",
    "NullResult",
    "block_shuffle",
    "effective_sample",
    "ic_summary",
    "null_distribution",
    "KEY_COLS",
    "UnifiedSchemaReader",
    "unified_schema_name",
    "FeatureSelector",
    "PurgedWalkForward",
    "SelectionResult",
    "cuda_available",
    "device_report",
    "resolve_device",
]
