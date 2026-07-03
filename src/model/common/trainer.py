"""Model-agnostic training loop with TensorBoard logging + checkpointing.

`Trainer` takes any `nn.Module` that maps a `(batch, lookback, n_features)` tensor
to a `(batch,)` prediction, trains with early stopping on validation loss, streams
`train`/`val` loss + lr to TensorBoard, and writes checkpoints into the run's
`model/` dir:

  - `best.pt` : best-val `state_dict` (for inference)
  - `last.pt` : `state_dict` + optimizer + scheduler + epoch + RNG (exact resume)
  - `arch.json`: class + kwargs to rebuild the module from config
"""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from torch.utils.tensorboard import SummaryWriter


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def resolve_device(device: str = "auto") -> torch.device:
    if device == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return torch.device(device)


def to_loaders(dataset, batch_size: int, device: torch.device):
    """Build train/val/test DataLoaders from a `common.data.Dataset`."""
    pin = device.type == "cuda"

    def _loader(X, y, shuffle):
        ds = TensorDataset(torch.from_numpy(X), torch.from_numpy(y))
        return DataLoader(ds, batch_size=batch_size, shuffle=shuffle, pin_memory=pin)

    return (
        _loader(dataset.X_train, dataset.y_train, True),
        _loader(dataset.X_val, dataset.y_val, False),
        _loader(dataset.X_test, dataset.y_test, False),
    )


@dataclass
class TrainConfig:
    batch_size: int = 64
    lr: float = 1e-3
    weight_decay: float = 1e-5
    max_epochs: int = 150
    patience: int = 20
    grad_clip: float = 1.0
    lr_factor: float = 0.5
    lr_patience: int = 5
    log_every: int = 10

    @classmethod
    def from_dict(cls, d: dict) -> "TrainConfig":
        return cls(**{k: v for k, v in (d or {}).items() if k in cls.__annotations__})


class Trainer:
    def __init__(self, model: nn.Module, cfg: TrainConfig, run, device,
                 arch: dict | None = None):
        self.model = model.to(device)
        self.cfg = cfg
        self.run = run
        self.device = device
        self.criterion = nn.MSELoss()
        self.optimizer = torch.optim.Adam(
            model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay
        )
        self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer, factor=cfg.lr_factor, patience=cfg.lr_patience
        )
        self.writer = SummaryWriter(log_dir=run.tb_dir)
        self.history = {"train": [], "val": []}
        self.best_val = float("inf")
        self.best_epoch = -1
        if arch is not None:
            with open(os.path.join(run.model_dir, "arch.json"), "w") as f:
                json.dump(arch, f, indent=2)

    # ---- one pass ---------------------------------------------------------
    def _run_epoch(self, loader, train: bool) -> float:
        self.model.train(train)
        total, count = 0.0, 0
        with torch.set_grad_enabled(train):
            for xb, yb in loader:
                xb = xb.to(self.device, non_blocking=True)
                yb = yb.to(self.device, non_blocking=True)
                pred = self.model(xb)
                loss = self.criterion(pred, yb)
                if train:
                    self.optimizer.zero_grad()
                    loss.backward()
                    nn.utils.clip_grad_norm_(self.model.parameters(), self.cfg.grad_clip)
                    self.optimizer.step()
                total += loss.item() * len(yb)
                count += len(yb)
        return total / max(count, 1)

    # ---- fit --------------------------------------------------------------
    def fit(self, train_loader, val_loader) -> dict:
        best_state = None
        for epoch in range(1, self.cfg.max_epochs + 1):
            tr = self._run_epoch(train_loader, train=True)
            va = self._run_epoch(val_loader, train=False)
            self.scheduler.step(va)
            lr_now = self.optimizer.param_groups[0]["lr"]

            self.history["train"].append(tr)
            self.history["val"].append(va)
            self.writer.add_scalar("Loss/train", tr, epoch)
            self.writer.add_scalar("Loss/val", va, epoch)
            self.writer.add_scalar("lr", lr_now, epoch)

            if va < self.best_val:
                self.best_val, self.best_epoch = va, epoch
                best_state = {k: v.detach().cpu().clone()
                              for k, v in self.model.state_dict().items()}
                torch.save(best_state, os.path.join(self.run.model_dir, "best.pt"))

            self._save_last(epoch)

            if epoch == 1 or epoch % self.cfg.log_every == 0:
                print(f"epoch {epoch:3d}  train {tr:.4f}  val {va:.4f}"
                      f"  (best {self.best_val:.4f} @ {self.best_epoch})  lr {lr_now:.2e}")
            if epoch - self.best_epoch >= self.cfg.patience:
                print(f"early stop at epoch {epoch} "
                      f"(no val improvement for {self.cfg.patience} epochs)")
                break

        if best_state is not None:
            self.model.load_state_dict(best_state)
        print(f"restored best epoch {self.best_epoch}: val MSE {self.best_val:.4f}")
        return self.history

    def _save_last(self, epoch: int) -> None:
        torch.save(
            {
                "epoch": epoch,
                "model": self.model.state_dict(),
                "optimizer": self.optimizer.state_dict(),
                "scheduler": self.scheduler.state_dict(),
                "rng": {
                    "torch": torch.get_rng_state(),
                    "numpy": np.random.get_state(),
                    "python": random.getstate(),
                },
                "best_val": self.best_val,
                "best_epoch": self.best_epoch,
            },
            os.path.join(self.run.model_dir, "last.pt"),
        )

    # ---- inference / logging ---------------------------------------------
    @torch.no_grad()
    def predict(self, loader) -> np.ndarray:
        self.model.eval()
        preds = []
        for xb, _ in loader:
            preds.append(self.model(xb.to(self.device)).cpu().numpy())
        return np.concatenate(preds)

    def log_hparams(self, hparams: dict, metrics: dict) -> None:
        """Populate the TensorBoard HPARAMS tab (config vs metrics per run)."""
        flat_h = {k: v for k, v in hparams.items()
                  if isinstance(v, (int, float, str, bool))}
        flat_m = {f"hparam/{k}": float(v) for k, v in metrics.items()
                  if isinstance(v, (int, float)) and not isinstance(v, bool)}
        self.writer.add_hparams(flat_h, flat_m)

    def close(self) -> None:
        self.writer.flush()
        self.writer.close()
