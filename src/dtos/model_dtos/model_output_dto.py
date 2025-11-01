from dataclasses import dataclass
from typing import Any
import numpy as np
from rpds import List
import torch.nn as nn

from dtos.model_dtos.model_config_dto import ModelConfigDto


@dataclass
class ModelOutputDto:
    model: nn.Module
    model_state_dict: dict[str, Any]
    model_config: ModelConfigDto
    train_loss_history: List
    final_train_loss: float
    test_loss: float
    y_pred: np.ndarray
    y_pred_denorm: np.ndarray
    y_true: np.ndarray
    y_true_denorm: np.ndarray
    input_window_size: int
    horizon_size: int
    training_time: Any
