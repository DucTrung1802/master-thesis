"""Bidirectional LSTM over a (lookback, n_features) window.

⚠️ **BIDIRECTIONAL IS NOT LEAKAGE HERE, AND THE REASON IS THE WINDOW, NOT THE LAYER.**
One sample is the window `t-d+1 … t` and the label is the target AT day `t`
(`model/CONTEXT.md` §1). A backward pass reads that same window right-to-left; every
timestep it touches is still on or before `t`. Nothing after `t` is in the tensor at
all, so there is nothing for the backward direction to leak from.

What it CAN do is read the window's *shape* — a move at `t-15` is seen with knowledge
of what followed it inside the window — which is exactly the argument for trying it.

⚠️ The head reads the CONCATENATED final states of both directions, not `out[:, -1, :]`.
For a bidirectional LSTM the last timestep of the output holds the forward direction's
final state and the backward direction's FIRST state (i.e. its view of `t-d+1`), so
slicing the last timestep silently discards half the backward pass. `h_n` carries both
final states and is the correct read.
"""

from __future__ import annotations

import torch
import torch.nn as nn


class BiLSTMRegressor(nn.Module):
    """Bidirectional LSTM; predicts a scalar from both directions' final hidden states."""

    def __init__(self, n_features: int, hidden_size: int = 96, num_layers: int = 2,
                 dropout: float = 0.2):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
            bidirectional=True,
        )
        self.head = nn.Sequential(
            nn.LayerNorm(hidden_size * 2),
            nn.Dropout(dropout),
            nn.Linear(hidden_size * 2, 1),
        )

    def forward(self, x):
        _, (h_n, _) = self.lstm(x)                 # h_n: (layers*2, batch, hidden)
        h_n = h_n.view(self.num_layers, 2, x.size(0), self.hidden_size)
        last = torch.cat((h_n[-1, 0], h_n[-1, 1]), dim=-1)   # (batch, hidden*2)
        return self.head(last).squeeze(-1)


def build_model(n_features: int, hidden_size: int = 96, num_layers: int = 2,
                dropout: float = 0.2) -> BiLSTMRegressor:
    return BiLSTMRegressor(n_features, hidden_size, num_layers, dropout)


def arch_dict(n_features: int, hidden_size: int = 96, num_layers: int = 2,
              dropout: float = 0.2) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "BiLSTMRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "hidden_size": int(hidden_size),
            "num_layers": int(num_layers),
            "dropout": float(dropout),
        },
    }
