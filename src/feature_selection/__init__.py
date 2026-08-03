# src\feature_selection\__init__.py
"""Feature selection over a per-ticker `unified_schema_<ticker>` schema.

Three pieces, deliberately separate:

    unified_reader.py   read the `pool__*` tables and JOIN them on their shared keys
    selector.py         rank the joined features against one target, then prune
    gpu.py              the CUDA paths — and which steps do not have one
    plots.py            the figures — one theme, one palette, no per-chart styling

`feature_selection.ipynb` is the entry point; the modules hold nothing notebook-
specific so the same run can be scripted.
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

__all__ = [
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
