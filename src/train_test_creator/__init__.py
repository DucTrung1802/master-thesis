# src\train_test_creator\__init__.py
"""Final feature table → windowed train/val/test tensors.

    dataset.py      read `<target>__final__d<d>_h<h>`, purge, impute, scale, window

One module, because the stage is one pipeline with no branch: the table already
holds exactly the channels chosen upstream, so there is nothing to select, tune or
join here. `RUN__train_test_creator.ipynb` is the notebook meant to be run — set its
parameter cell, Run All, get a folder under `src/train_test_set/`.

The output folder is a CONTRACT with `model/common/data.py`, which loads
`X_/y_{train,val,test}.npy`, `dates_*.npy`, the two scalers and `metadata.json` by
name and hashes the six tensors.
"""

from train_test_creator.dataset import (
    DEFAULT_OUTPUT_ROOT,
    FINAL_TABLE,
    SplitBounds,
    TrainTestCreator,
    WindowedDataset,
    dataset_name,
    parse_final_table,
)

__all__ = [
    "DEFAULT_OUTPUT_ROOT",
    "FINAL_TABLE",
    "SplitBounds",
    "TrainTestCreator",
    "WindowedDataset",
    "dataset_name",
    "parse_final_table",
]
