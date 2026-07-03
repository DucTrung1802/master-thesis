"""Shared model-stage framework: referenced datasets, run folders, training,
metrics, and a run registry. Used by the per-model notebooks under src/model/*.
"""

from .data import Dataset, load_dataset, hash_dataset, resolve_dataset_dir
from .metrics import regression_metrics
from .registry import append_run
from .run_dir import RunDir
from .trainer import Trainer, TrainConfig, set_seed, resolve_device, to_loaders

__all__ = [
    "Dataset", "load_dataset", "hash_dataset", "resolve_dataset_dir",
    "regression_metrics", "append_run", "RunDir",
    "Trainer", "TrainConfig", "set_seed", "resolve_device", "to_loaders",
]
