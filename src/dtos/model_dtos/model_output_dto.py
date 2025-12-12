from dataclasses import dataclass
from typing import Any
import numpy as np
import torch.nn as nn

from dtos.model_dtos.model_config_dto import ModelConfigDto


@dataclass
class ModelOutputDto:
    model: nn.Module
    model_state_dict: dict[str, Any]
    model_config: ModelConfigDto
    train_loss_history: list
    final_train_loss: float
    validation_loss_history: list
    final_validation_loss: float
    test_loss: float
    y_pred: np.ndarray
    y_pred_denorm: np.ndarray
    y_true: np.ndarray
    input_size: int
    forecast_size: int
    training_time: Any
    mape: float

    def to_dict(self) -> dict:
        """Return model output metadata and results as a serializable dictionary."""
        return {
            # --- Metadata ---
            "model_config": self.model_config.to_dict(),
            "input_size": self.input_size,
            "forecast_size": self.forecast_size,
            "training_time": float(self.training_time),
            # --- Training metrics ---
            "train_loss_history": list(map(float, self.train_loss_history)),
            "final_train_loss": float(self.final_train_loss),
            "validation_loss_history": (
                list(map(float, self.validation_loss_history))
                if self.validation_loss_history is not None
                else None
            ),
            "final_validation_loss": (
                float(self.final_validation_loss)
                if self.final_validation_loss is not None
                else None
            ),
            # --- Test metrics ---
            "test_loss": float(self.test_loss),
            "mape": float(self.mape),
            # --- Predictions ---
            "y_pred": (
                self.y_pred.tolist()
                if isinstance(self.y_pred, np.ndarray)
                else self.y_pred
            ),
            "y_pred_denorm": (
                self.y_pred_denorm.tolist()
                if isinstance(self.y_pred_denorm, np.ndarray)
                else self.y_pred_denorm
            ),
            "y_true": (
                self.y_true.tolist()
                if isinstance(self.y_true, np.ndarray)
                else self.y_true
            ),
        }

    def format_output(self):
        """Pretty-print the model output summary as formatted JSON."""
        import json

        print(json.dumps(self.to_dict(), indent=4))
