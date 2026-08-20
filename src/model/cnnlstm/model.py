"""Conv1d feature extractor over time, then an LSTM over the convolved sequence.

The argument for the hybrid: the CNN learns short local shapes (a 3-5 day pattern) at
every offset with shared weights, and the LSTM carries what it found across the whole
window. An LSTM alone must learn the local shape separately at each position; a CNN
alone pools it away.

⚠️ **THE CONVOLUTION IS CAUSAL-BY-CONSTRUCTION ONLY BECAUSE THE WINDOW IS.** `padding`
is symmetric here, so a kernel centred at `t-1` reads `t`. That is fine — every
timestep in the tensor is on or before `t`, and the label is at `t` — but it means this
module must never be reused on a tensor that extends past the label. `model.tcn` is the
strictly causal variant.

⚠️ `Conv1d` wants `(batch, channels, time)` and the dataset is `(batch, time, features)`,
so the transpose is load-bearing: without it the convolution runs across FEATURES and
the layer silently learns a per-timestep feature mixer instead of a temporal filter.
"""

from __future__ import annotations

import torch.nn as nn


class CNNLSTMRegressor(nn.Module):
    """Conv1d stack -> LSTM -> scalar."""

    def __init__(self, n_features: int, channels: int = 32, kernel_size: int = 3,
                 conv_layers: int = 2, hidden_size: int = 64, num_layers: int = 1,
                 dropout: float = 0.2):
        super().__init__()
        blocks = []
        in_ch = n_features
        for _ in range(conv_layers):
            blocks += [
                nn.Conv1d(in_ch, channels, kernel_size, padding=kernel_size // 2),
                nn.BatchNorm1d(channels),
                nn.ReLU(),
            ]
            in_ch = channels
        self.features = nn.Sequential(*blocks)
        self.lstm = nn.LSTM(
            input_size=channels,
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
        z = self.features(x.transpose(1, 2))       # (batch, channels, lookback)
        out, _ = self.lstm(z.transpose(1, 2))      # (batch, lookback, hidden)
        return self.head(out[:, -1, :]).squeeze(-1)


def build_model(n_features: int, channels: int = 32, kernel_size: int = 3,
                conv_layers: int = 2, hidden_size: int = 64, num_layers: int = 1,
                dropout: float = 0.2) -> CNNLSTMRegressor:
    return CNNLSTMRegressor(n_features, channels, kernel_size, conv_layers,
                            hidden_size, num_layers, dropout)


def arch_dict(n_features: int, channels: int = 32, kernel_size: int = 3,
              conv_layers: int = 2, hidden_size: int = 64, num_layers: int = 1,
              dropout: float = 0.2) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "CNNLSTMRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "channels": int(channels),
            "kernel_size": int(kernel_size),
            "conv_layers": int(conv_layers),
            "hidden_size": int(hidden_size),
            "num_layers": int(num_layers),
            "dropout": float(dropout),
        },
    }
