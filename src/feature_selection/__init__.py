# src\feature_selection\__init__.py
"""Feature selection over a per-ticker `unified_schema_<ticker>` schema.

Three pieces, deliberately separate:

    unified_reader.py   read the `pool__*` tables and JOIN them on their shared keys
    windows.py          daily panel → windowed samples; scoring CHANNELS not columns
    selector.py         rank the joined features against one target, then prune
    evaluation.py       the BAR a selection has to clear — the shuffled-label null
    gpu.py              the CUDA paths — and which steps do not have one
    plots.py            the figures — one theme, one palette, no per-chart styling

`RUN__feature_importance_report.ipynb` is the ONLY notebook meant to be run — set
its parameter cell, Run All, get an archived report folder. The four `study_*.ipynb`
notebooks are finished write-ups kept for the record, not entry points. The modules
hold nothing notebook-specific, so the same run can be scripted.
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
