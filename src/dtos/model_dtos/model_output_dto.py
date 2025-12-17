from dataclasses import dataclass
from typing import Any, List
import numpy as np
import torch
import torch.nn as nn

from dtos.model_dtos.model_config_dto import ModelConfigDto


@dataclass
class ModelOutputDto:
    # Dataset metadata
    dataset_name: str
    data_set_size: int
    train_set_size: int
    val_set_size: int
    test_set_size: int
    number_of_train_window: int
    number_of_val_window: int
    number_of_test_window: int
    input_size: int
    forecast_size: int

    # Model metadata
    model_name: str
    model_config: ModelConfigDto
    training_time: float

    # Training history
    train_loss_history: List[float]
    validation_loss_history: List[float] | None
    final_train_loss: float
    final_validation_loss: float | None
    test_loss: float

    # Result
    y_pred: List[float]
    y_pred_denorm: List[float]
    y_true: List[float]

    # Metrics
    mae: float | None
    mape: float | None
    mase: float | None
    rmse: float | None
    r2: float | None

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
        """Return model output metadata, metrics, and results as a serializable dictionary."""
        return {
            # --- Metadata ---
            "model_name": self.model_name,
            "model_config": (
                self.model_config.to_dict()
                if hasattr(self.model_config, "to_dict")
                else self.model_config
            ),
            "training_time": self.training_time,
            # --- Training history ---
            "train_loss_history": self.train_loss_history,
            "validation_loss_history": self.validation_loss_history,
            "final_train_loss": (
                float(self.final_train_loss)
                if self.final_train_loss is not None
                else None
            ),
            "final_validation_loss": (
                float(self.final_validation_loss)
                if self.final_validation_loss is not None
                else None
            ),
            # --- Test ---
            "test_loss": float(self.test_loss) if self.test_loss is not None else None,
            # --- Predictions ---
            "y_pred": self.y_pred,
            "y_pred_denorm": self.y_pred_denorm,
            "y_true": self.y_true,
            # --- Metrics ---
            "mae": float(self.mae) if self.mae is not None else None,
            "mape": float(self.mape) if self.mape is not None else None,
            "mase": float(self.mase) if self.mase is not None else None,
            "rmse": float(self.rmse) if self.rmse is not None else None,
            "r2": float(self.r2) if self.r2 is not None else None,
        }

    def format_output(self):
        """Pretty-print the model output summary as formatted JSON."""
        import json

        print(json.dumps(self.to_dict(), indent=4))
