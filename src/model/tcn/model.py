"""Temporal Convolutional Network — dilated CAUSAL convolutions with residual blocks.

Bai, Kolter & Koltun (2018): a stack of dilated causal convolutions matches or beats
recurrent nets on sequence tasks while training in parallel over time. Dilation `2^i`
gives a receptive field of `1 + 2·(k-1)·(2^L − 1)`, so a 20-day window is covered by
3 layers at `k=3` (field 29) rather than by 20 sequential recurrent steps.

⚠️ **CAUSAL PADDING IS THE WHOLE POINT AND IT IS EASY TO GET WRONG.** `nn.Conv1d` pads
BOTH ends. Padding only the left — `pad = (k-1)·dilation` then chopping the same number
off the right — is what makes output `t` a function of inputs `≤ t` only. Symmetric
padding would let position `t` read `t+1`, which inside this window is not a leak of the
future (the label is at the window's end) but IS a different model from the one named
here, and the difference would be invisible in the metrics.

⚠️ The residual path needs a 1×1 convolution whenever `in_ch != out_ch`, otherwise the
skip cannot be added. Without it the first block silently drops its residual.
"""

from __future__ import annotations

import torch.nn as nn


class _Chomp(nn.Module):
    """Remove the right-hand padding that keeps the convolution causal."""

    def __init__(self, size: int):
        super().__init__()
        self.size = size

    def forward(self, x):
        return x[:, :, :-self.size].contiguous() if self.size > 0 else x


class _Block(nn.Module):
    """Dilated causal conv -> chomp -> ReLU -> dropout, twice, plus a residual."""

    def __init__(self, in_ch: int, out_ch: int, kernel_size: int, dilation: int,
                 dropout: float):
        super().__init__()
        pad = (kernel_size - 1) * dilation
        self.net = nn.Sequential(
            nn.Conv1d(in_ch, out_ch, kernel_size, padding=pad, dilation=dilation),
            _Chomp(pad), nn.ReLU(), nn.Dropout(dropout),
            nn.Conv1d(out_ch, out_ch, kernel_size, padding=pad, dilation=dilation),
            _Chomp(pad), nn.ReLU(), nn.Dropout(dropout),
        )
        self.down = nn.Conv1d(in_ch, out_ch, 1) if in_ch != out_ch else None
        self.relu = nn.ReLU()

    def forward(self, x):
        out = self.net(x)
        res = x if self.down is None else self.down(x)
        return self.relu(out + res)


class TCNRegressor(nn.Module):
    """Dilated causal conv stack; predicts a scalar from the LAST timestep."""

    def __init__(self, n_features: int, channels: int = 32, kernel_size: int = 3,
                 num_layers: int = 3, dropout: float = 0.2):
        super().__init__()
        blocks = []
        in_ch = n_features
        for i in range(num_layers):
            blocks.append(_Block(in_ch, channels, kernel_size, 2 ** i, dropout))
            in_ch = channels
        self.tcn = nn.Sequential(*blocks)
        self.head = nn.Sequential(
            nn.LayerNorm(channels),
            nn.Dropout(dropout),
            nn.Linear(channels, 1),
        )

    def forward(self, x):
        z = self.tcn(x.transpose(1, 2))            # (batch, channels, lookback)
        # ⚠️ the LAST timestep, which under causal padding is the only position that has
        # seen the whole window.
        return self.head(z[:, :, -1]).squeeze(-1)


def build_model(n_features: int, channels: int = 32, kernel_size: int = 3,
                num_layers: int = 3, dropout: float = 0.2) -> TCNRegressor:
    return TCNRegressor(n_features, channels, kernel_size, num_layers, dropout)


def arch_dict(n_features: int, channels: int = 32, kernel_size: int = 3,
              num_layers: int = 3, dropout: float = 0.2) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "TCNRegressor",
        "module": "model",
        "builder": "build_model",
        "kwargs": {
            "n_features": int(n_features),
            "channels": int(channels),
            "kernel_size": int(kernel_size),
            "num_layers": int(num_layers),
            "dropout": float(dropout),
        },
    }
