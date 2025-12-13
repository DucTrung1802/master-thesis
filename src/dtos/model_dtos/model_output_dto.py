from dataclasses import dataclass
from typing import Any, List
import numpy as np
import torch
import torch.nn as nn

from dtos.model_dtos.model_config_dto import ModelConfigDto


@dataclass
class ModelOutputDto:
    model: nn.Module
    model_state_dict: dict[str, Any]
    model_config: ModelConfigDto
    train_loss_history: List[float]
    final_train_loss: float
    validation_loss_history: List[float] | None
    final_validation_loss: float | None
    test_loss: float

    # Enforced as lists
    y_pred: List[float]
    y_pred_denorm: List[float]
    y_true: List[float]

    input_size: int
    forecast_size: int
    training_time: Any
    mape: float

    def __post_init__(self):
        """Ensure predictions and targets are stored as Python lists."""
        self.y_pred = self._to_list(self.y_pred)
        self.y_pred_denorm = self._to_list(self.y_pred_denorm)
        self.y_true = self._to_list(self.y_true)

    @staticmethod
    def _to_list(value):
        if isinstance(value, np.ndarray):
            return value.tolist()
        if torch.is_tensor(value):
            return value.detach().cpu().tolist()
        return list(value)

    def to_dict(self) -> dict:
        """Return model output metadata and results as a serializable dictionary."""
        return {
            # --- Metadata ---
            "model_config": self.model_config.to_dict(),
            "input_size": self.input_size,
            "forecast_size": self.forecast_size,
            "training_time": float(self.training_time),
            # --- Training metrics ---
            "train_loss_history": self.train_loss_history,
            "final_train_loss": float(self.final_train_loss),
            "validation_loss_history": self.validation_loss_history,
            "final_validation_loss": self.final_validation_loss,
            # --- Test metrics ---
            "test_loss": float(self.test_loss),
            "mape": float(self.mape),
            # --- Predictions ---
            "y_pred": self.y_pred,
            "y_pred_denorm": self.y_pred_denorm,
            "y_true": self.y_true,
        }

    def format_output(self):
        """Pretty-print the model output summary as formatted JSON."""
        import json

        print(json.dumps(self.to_dict(), indent=4))
