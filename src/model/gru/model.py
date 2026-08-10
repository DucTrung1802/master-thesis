"""GRU regressor for windowed return prediction — Tier 2.

Same shape as `model/lstm/model.py`: consumes the window sequentially and predicts from
the LAST timestep's hidden state. The difference is the cell — a GRU has **three** gates
to an LSTM's four and no separate cell state, so at the same `hidden_size` it carries
about 25% fewer parameters.

⚠️ **It is here as a third point on the capacity ladder, not because it is expected to
differ.** `model/CONTEXT.md` §13 already measured an LSTM and a CNN — genuinely
different inductive biases — converging on test IC ≈ −0.033. A GRU is the LSTM's near
neighbour; if it lands anywhere else, that is evidence about run-to-run variance rather
than about architecture, which is itself worth having on a board where every result so
far is inside its own null.
"""

from __future__ import annotations

import torch.nn as nn


class GRURegressor(nn.Module):
    """GRU over a (lookback, n_features) window; scalar from the last timestep."""

    def __init__(self, n_features: int, hidden_size: int = 16, num_layers: int = 1,
                 dropout: float = 0.1):
        super().__init__()
        self.gru = nn.GRU(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            # ⚠️ torch applies `dropout` BETWEEN layers only, so it is a no-op at
            # `num_layers=1` and torch warns about it. Zeroed explicitly; the head's
            # dropout below is the one that acts.
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.head = nn.Sequential(
            nn.LayerNorm(hidden_size),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x):
        out, _ = self.gru(x)                          # (batch, lookback, hidden)
        return self.head(out[:, -1, :]).squeeze(-1)   # last timestep -> scalar


def build_model(n_features: int, hidden_size: int = 16, num_layers: int = 1,
                dropout: float = 0.1) -> GRURegressor:
    return GRURegressor(n_features, hidden_size, num_layers, dropout)


def arch_dict(n_features: int, hidden_size: int = 16, num_layers: int = 1,
              dropout: float = 0.1) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "GRURegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "hidden_size": int(hidden_size),
            "num_layers": int(num_layers),
            "dropout": float(dropout),
        },
    }
