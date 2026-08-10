"""1-D CNN regressor for windowed return prediction.

`build_model(n_features, **kwargs)` rebuilds the module from a config/arch dict, and
`arch_dict(...)` serializes the architecture into `model/arch.json` so a run is
reproducible from disk. Same contract as `model/lstm/model.py`: an `nn.Module` mapping
`(batch, lookback, n_features) → (batch,)`.

## What this reads that the LSTM does not

The LSTM consumes the window sequentially and predicts from the LAST timestep's hidden
state. This convolves over the TIME axis, so each filter is a learned shape detector of
width `kernel_size` — a slope, a spike, a reversal — applied at every position, then
pooled. The two are different inductive biases over the same tensor, which is the point
of running both: `feature_selection` already reports which window statistic carries each
channel (`last`, `slope`, `sd`, `min`, `max`), and on this dataset three of the four
channels are carried by `slope`/`mean` rather than `last`. A shape detector is the
architecture that matches that description.

## ⚠️ The convolution is NOT causal, and that is not leakage

`padding="same"` lets position `t` inside the window see `t+1`. That is fine here and
worth stating, because it looks wrong at a glance: one sample is rows `N-d+1 … N` and
its label is `return_5day[N]`, computed from `close[N+5]`. **Every row in the window
precedes the label**, so a filter reading forward *within the window* is reading the
past, not the future. The leak that matters in this repo is across SAMPLES, and it is
handled where it belongs — the `d + h - 1` purge at each split boundary
(`train_test_creator/CONTEXT.md` §3).

## ⚠️ Global average pooling, not the last position

`AdaptiveAvgPool1d(1)` collapses the time axis by averaging. Taking the last position
instead would make this an LSTM with a different cell, and would inherit the failure
mode §6b names: on this ticker the level at day `N` acts as an ERA PROXY — a
seventeen-year price trend lets a model identify the year rather than predict the
return, and `feature_selection/CONTEXT.md` §6c measured that removing the level removes
the apparent signal. Averaging over the window is the weaker assumption, and `close_adjust`
is one of the four channels.
"""

from __future__ import annotations

import torch.nn as nn


class CNNRegressor(nn.Module):
    """1-D CNN over a (lookback, n_features) window; predicts a scalar.

    Channels are the FEATURES and the convolution runs along TIME, so the input is
    transposed to `(batch, n_features, lookback)` — `nn.Conv1d` expects
    `(batch, channels, length)` and handing it the window as-is would convolve across
    unrelated feature columns instead of across days.
    """

    def __init__(self, n_features: int, channels: int = 32, kernel_size: int = 3,
                 num_layers: int = 2, dropout: float = 0.1):
        super().__init__()
        blocks = []
        in_channels = n_features
        for _ in range(num_layers):
            blocks += [
                nn.Conv1d(
                    in_channels,
                    channels,
                    kernel_size=kernel_size,
                    padding="same",
                ),
                # BatchNorm over channels, then ReLU. Normalising here rather than in
                # the head is what keeps a 2-layer stack trainable at this width.
                nn.BatchNorm1d(channels),
                nn.ReLU(),
                nn.Dropout(dropout),
            ]
            in_channels = channels
        self.features = nn.Sequential(*blocks)
        self.pool = nn.AdaptiveAvgPool1d(1)
        self.head = nn.Sequential(
            nn.LayerNorm(channels),
            nn.Dropout(dropout),
            nn.Linear(channels, 1),
        )

    def forward(self, x):
        x = x.transpose(1, 2)                      # (batch, n_features, lookback)
        out = self.features(x)                     # (batch, channels, lookback)
        pooled = self.pool(out).squeeze(-1)        # (batch, channels)
        return self.head(pooled).squeeze(-1)       # -> scalar


def build_model(n_features: int, channels: int = 32, kernel_size: int = 3,
                num_layers: int = 2, dropout: float = 0.1) -> CNNRegressor:
    return CNNRegressor(n_features, channels, kernel_size, num_layers, dropout)


def arch_dict(n_features: int, channels: int = 32, kernel_size: int = 3,
              num_layers: int = 2, dropout: float = 0.1) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "CNNRegressor",
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
