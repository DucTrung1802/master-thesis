from dataclasses import dataclass
import json

from utils.constants import RANDOM_SEED
from utils.enums import (
    AchitectureType,
    LossFunctionType,
    MetricType,
    OptimizerType,
    ScalerType,
    WindowType,
)


@dataclass
class ModelConfigDto:
    # Metadata
    entity: str
    project: str
    architecture: AchitectureType
    stock_code: str

    # Windowing
    window_type: WindowType
    input_size: int
    forecast_size: int

    # Training
    epochs: int
    learning_rate: float
    batch_size: int
    optimizer: OptimizerType
    loss_fn: LossFunctionType
    patience: int

    # Data & metrics
    scaler_type: ScalerType
    metric: MetricType

    # Reproducibility & runtime
    device: str
    seed: int = RANDOM_SEED

    # Configurations with default value
    lr_scheduler: str | None = None

    def to_dict(self) -> dict:
        """Return the model configuration as a Python dictionary."""
        return {
            # Metadata
            "entity": self.entity,
            "project": self.project,
            "architecture": self.architecture.value,
            "stock_code": self.stock_code,
            # Windowing
            "window_type": self.window_type.value,
            "input_size": self.input_size,
            "forecast_size": self.forecast_size,
            # Training
            "epochs": self.epochs,
            "learning_rate": self.learning_rate,
            "batch_size": self.batch_size,
            "optimizer": self.optimizer.value,
            "loss_fn": self.loss_fn.value,
            "patience": self.patience,
            # Data & metrics
            "scaler_type": self.scaler_type.value,
            "metric": self.metric.value,
            # Reproducibility & runtime
            "device": str(self.device),
            "seed": self.seed,
            # Configurations with default value
            "lr_scheduler": self.lr_scheduler,
        }

    def format_config(self):
        """Pretty-print the model configuration as formatted JSON."""
        print(json.dumps(self.to_dict(), indent=4))
