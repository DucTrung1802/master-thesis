from dataclasses import dataclass
from typing import Dict, List

from utils.enums import (
    LossFunctionType,
    ModelAchitectureType,
    OptimizerType,
    ScalerType,
)


@dataclass
class ConfigDto:
    # Data
    notebook_name: str
    feature_groups: List[str]
    stock_code: str
    train_start_date: str
    train_end_date: str
    validation_start_date: str
    validation_end_date: str
    test_start_date: str
    test_end_date: str

    # Data Hyperparameters
    random_seed: int
    lookback_window_size: int
    forecast_window_size: int
    stride: int
    scaler_type: ScalerType

    # Model
    model_architecture: ModelAchitectureType
    model_additional_params: Dict

    # Training Hyperparameters
    epochs: int
    learning_rate: float
    batch_size: int
    optimizer: OptimizerType
    loss_fn: LossFunctionType
    patience: int
