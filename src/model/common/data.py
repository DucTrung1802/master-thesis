"""Dataset loading for the model stage.

The model stage **references** a materialized dataset produced by
`train_test_creator.ipynb` under `src/train_test_set/<dataset_name>/` — it does not
clone the tensors into the run folder. A content hash of the six tensor files is
recorded in the run so the exact data used is verifiable/reproducible.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Optional

import joblib
import numpy as np

# src/train_test_set (this file is src/model/common/data.py -> up 2 -> src)
_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TRAIN_TEST_SET_ROOT = os.path.join(_SRC, "train_test_set")

_TENSOR_FILES = [
    "X_train.npy", "y_train.npy",
    "X_val.npy", "y_val.npy",
    "X_test.npy", "y_test.npy",
]


@dataclass
class Dataset:
    """A referenced train/val/test dataset (tensors stay on disk in `dir`)."""

    name: str
    dir: str
    hash: str
    meta: dict
    X_train: np.ndarray
    y_train: np.ndarray
    X_val: np.ndarray
    y_val: np.ndarray
    X_test: np.ndarray
    y_test: np.ndarray
    dates_train: Optional[np.ndarray] = None
    dates_val: Optional[np.ndarray] = None
    dates_test: Optional[np.ndarray] = None
    feature_scaler: object = field(default=None, repr=False)
    target_scaler: object = field(default=None, repr=False)

    @property
    def n_features(self) -> int:
        return int(self.X_train.shape[2])

    @property
    def lookback(self) -> int:
        return int(self.X_train.shape[1])

    def reference(self) -> dict:
        """Small pointer dict to embed in a run's metadata (no tensors)."""
        return {
            "dataset_name": self.name,
            "dataset_dir": os.path.relpath(self.dir, _SRC).replace("\\", "/"),
            "dataset_hash": self.hash,
            "n_features": self.n_features,
            "lookback": self.lookback,
            "shapes": {
                "X_train": list(self.X_train.shape),
                "X_val": list(self.X_val.shape),
                "X_test": list(self.X_test.shape),
            },
        }

    def inverse_target(self, y: np.ndarray) -> np.ndarray:
        """Map (possibly scaled) targets back to the original return scale."""
        if self.target_scaler is None:
            return np.asarray(y).ravel()
        return self.target_scaler.inverse_transform(
            np.asarray(y).reshape(-1, 1)
        ).ravel()


def hash_dataset(dataset_dir: str) -> str:
    """Deterministic content hash of the six tensor files (order-fixed)."""
    h = hashlib.sha256()
    for fn in _TENSOR_FILES:
        p = os.path.join(dataset_dir, fn)
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    return h.hexdigest()[:16]


def resolve_dataset_dir(dataset: str) -> str:
    """Accept an absolute path, a path relative to src/, or a bare dataset name
    under src/train_test_set/."""
    for cand in (dataset,
                 os.path.join(_SRC, dataset),
                 os.path.join(TRAIN_TEST_SET_ROOT, dataset)):
        if os.path.isdir(cand):
            return os.path.abspath(cand)
    raise FileNotFoundError(
        f"dataset '{dataset}' not found (looked under {TRAIN_TEST_SET_ROOT})."
    )


def load_dataset(dataset: str, expected_hash: Optional[str] = None) -> Dataset:
    """Load a referenced dataset by name/path. If `expected_hash` is given, verify
    the on-disk tensors still match it (guards against silently-changed data)."""
    d = resolve_dataset_dir(dataset)

    def _load(fn):
        p = os.path.join(d, fn)
        return np.load(p, allow_pickle=False) if os.path.exists(p) else None

    digest = hash_dataset(d)
    if expected_hash is not None and expected_hash != digest:
        raise ValueError(
            f"dataset hash mismatch for {d}: expected {expected_hash}, got {digest}"
        )

    meta = {}
    meta_p = os.path.join(d, "metadata.json")
    if os.path.exists(meta_p):
        # ⚠️ encoding is explicit: `open()` defaults to cp1252 on Windows, and a
        # metadata.json holding a non-ASCII byte — the ⚠️ in a provenance comment
        # copied from the source table's COMMENT — raises UnicodeDecodeError there.
        # Every other metadata.json reader in the repo already passes utf-8.
        with open(meta_p, encoding="utf-8") as f:
            meta = json.load(f)

    fs_p = os.path.join(d, "feature_scaler.pkl")
    ts_p = os.path.join(d, "target_scaler.pkl")

    return Dataset(
        name=os.path.basename(d.rstrip("/\\")),
        dir=d,
        hash=digest,
        meta=meta,
        X_train=_load("X_train.npy"), y_train=_load("y_train.npy"),
        X_val=_load("X_val.npy"), y_val=_load("y_val.npy"),
        X_test=_load("X_test.npy"), y_test=_load("y_test.npy"),
        dates_train=_load("dates_train.npy"),
        dates_val=_load("dates_val.npy"),
        dates_test=_load("dates_test.npy"),
        feature_scaler=joblib.load(fs_p) if os.path.exists(fs_p) else None,
        target_scaler=joblib.load(ts_p) if os.path.exists(ts_p) else None,
    )
