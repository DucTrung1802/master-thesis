# src\walkforward\__init__.py
"""PRF-1 — expanding walk-forward over the model chain.

One model per fold, each trained only on data before its own test block, the OOS
predictions concatenated into a single track the backtest can price. See `folds.py`
for the geometry and for the one look-ahead that remains (the feature selection).
"""

from walkforward.folds import Fold, FoldBuilder, make_folds

__all__ = ["Fold", "FoldBuilder", "make_folds"]
