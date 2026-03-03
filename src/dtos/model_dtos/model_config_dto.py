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
    train_window_size: int
    validation_window_size: int
    test_window_size: int

    # Training
    epochs: int
    learning_rate: float
    batch_size: int
    optimizer: OptimizerType
    loss_fn: LossFunctionType

    # Data & metrics
    scaler_type: ScalerType
    metric: MetricType

    # Output scale
    output_range_scale: float
    output_range: tuple[float, float]

    # Reproducibility & runtime
    device: str
    seed: int = RANDOM_SEED

    # Configurations with default value
    lr_scheduler: str | None = None

    def to_dict(self) -> dict:
        """Return the model configuration as a Python dictionary."""
        return {
            "entity": self.entity,
            "project": self.project,
            "architecture": self.architecture.value,
            "stock_code": self.stock_code,
            "window_type": self.window_type.value,
            "train_window_size": self.train_window_size,
            "validation_window_size": self.validation_window_size,
            "test_window_size": self.test_window_size,
            "epochs": self.epochs,
            "learning_rate": self.learning_rate,
            "batch_size": self.batch_size,
            "optimizer": self.optimizer.value,
            "loss_fn": self.loss_fn.value,
            "scaler_type": self.scaler_type.value,
            "metric": self.metric.value,
            "device": str(self.device),
            "output_range_scale": self.output_range_scale,
            "output_range": self.output_range,
            "seed": self.seed,
            "lr_scheduler": self.lr_scheduler,
        }

    def format_config(self):
        """Pretty-print the model configuration as formatted JSON."""
        print(json.dumps(self.to_dict(), indent=4))
