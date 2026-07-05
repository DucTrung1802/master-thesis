"""LSTM regressor for windowed return prediction.

`build_model(n_features, **kwargs)` rebuilds the module from a config/arch dict, and
`arch_dict(...)` serializes the architecture into `model/arch.json` so a run is
reproducible from disk.
"""

from __future__ import annotations

import torch.nn as nn


class LSTMRegressor(nn.Module):
    """LSTM over a (lookback, n_features) window; predicts a scalar from the last
    timestep's hidden state (LayerNorm + Dropout + Linear head)."""

    def __init__(self, n_features: int, hidden_size: int = 128, num_layers: int = 2,
                 dropout: float = 0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.head = nn.Sequential(
            nn.LayerNorm(hidden_size),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x):
        out, _ = self.lstm(x)                     # (batch, lookback, hidden)
        return self.head(out[:, -1, :]).squeeze(-1)   # last timestep -> scalar


def build_model(n_features: int, hidden_size: int = 128, num_layers: int = 2,
                dropout: float = 0.2) -> LSTMRegressor:
    return LSTMRegressor(n_features, hidden_size, num_layers, dropout)


def arch_dict(n_features: int, hidden_size: int, num_layers: int, dropout: float) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "LSTMRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "hidden_size": int(hidden_size),
            "num_layers": int(num_layers),
            "dropout": float(dropout),
        },
    }
