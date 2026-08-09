# src\model\lstm\__init__.py
"""LSTM over a `(lookback, n_features)` window.

    model.py   LSTMRegressor + build_model + arch_dict — the architecture only
    train.py   config → run folder → trained model → scored result
    configs/   one YAML per run; the dataset is referenced by folder name

    python -m model.lstm --config configs/vcb__return_5day__final__d20_h5.yaml

⚠️ The head emits ONE scalar for both tasks: the return for regression, the logit for
classification. Only the loss and the evaluation transform differ, so a classifier
needs no change here.
"""

from model.lstm.model import LSTMRegressor, arch_dict, build_model

__all__ = ["LSTMRegressor", "arch_dict", "build_model"]
