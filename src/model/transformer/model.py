"""Transformer encoder over a (lookback, n_features) window.

Self-attention lets any day in the window read any other directly, rather than through
`d` recurrent steps. On a 20-day window that is a modest claim — an LSTM's path length
is already short — which is exactly why it is worth measuring rather than assuming.

⚠️ **POSITIONAL ENCODING IS REQUIRED, NOT OPTIONAL.** Attention is permutation-invariant:
without it the encoder cannot tell day `t` from day `t-19` and the model degenerates to a
set function over the window. Sinusoidal encoding is used (no parameters, and it
generalises to a lookback the model never trained on, which matters because `d` comes
from the source TABLE NAME and can change between datasets).

⚠️ **NO CAUSAL MASK, DELIBERATELY.** Every timestep in the tensor is on or before the
label's day, so a full-attention encoder reads only the past — the same argument as
`model.bilstm`. A causal mask would make position `t-19` blind to the rest of the window
for no gain, since only the final position feeds the head.

⚠️ `d_model` must be divisible by `nhead` or `nn.MultiheadAttention` raises. The input
projection exists to guarantee that regardless of `n_features`, which is set by the
selection and is whatever it is.
"""

from __future__ import annotations

import math

import torch
import torch.nn as nn


class _SinusoidalPositions(nn.Module):
    """Fixed sinusoidal positional encoding, added to the projected input."""

    def __init__(self, d_model: int, max_len: int = 512):
        super().__init__()
        pe = torch.zeros(max_len, d_model)
        pos = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div = torch.exp(torch.arange(0, d_model, 2).float()
                        * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(pos * div)
        pe[:, 1::2] = torch.cos(pos * div[: pe[:, 1::2].size(1)])
        self.register_buffer("pe", pe.unsqueeze(0))    # (1, max_len, d_model)

    def forward(self, x):
        return x + self.pe[:, : x.size(1), :]


class TransformerRegressor(nn.Module):
    """Input projection -> positional encoding -> encoder stack -> last-position head."""

    def __init__(self, n_features: int, d_model: int = 64, nhead: int = 4,
                 num_layers: int = 2, dim_feedforward: int = 128,
                 dropout: float = 0.2):
        super().__init__()
        if d_model % nhead:
            raise ValueError(
                f"d_model={d_model} must be divisible by nhead={nhead}"
            )
        self.project = nn.Linear(n_features, d_model)
        self.positions = _SinusoidalPositions(d_model)
        layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=nhead, dim_feedforward=dim_feedforward,
            dropout=dropout, batch_first=True, norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=num_layers)
        self.head = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Dropout(dropout),
            nn.Linear(d_model, 1),
        )

    def forward(self, x):
        z = self.positions(self.project(x))         # (batch, lookback, d_model)
        z = self.encoder(z)
        return self.head(z[:, -1, :]).squeeze(-1)   # the label's own day


def build_model(n_features: int, d_model: int = 64, nhead: int = 4,
                num_layers: int = 2, dim_feedforward: int = 128,
                dropout: float = 0.2) -> TransformerRegressor:
    return TransformerRegressor(n_features, d_model, nhead, num_layers,
                                dim_feedforward, dropout)


def arch_dict(n_features: int, d_model: int = 64, nhead: int = 4,
              num_layers: int = 2, dim_feedforward: int = 128,
              dropout: float = 0.2) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "TransformerRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "d_model": int(d_model),
            "nhead": int(nhead),
            "num_layers": int(num_layers),
            "dim_feedforward": int(dim_feedforward),
            "dropout": float(dropout),
        },
    }
