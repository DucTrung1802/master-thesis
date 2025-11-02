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
    input_window_size: int
    forecast_horizon_size: int

    # Training
    epochs: int
    learning_rate: float
    batch_size: int
    optimizer: OptimizerType
    loss_fn: LossFunctionType

    # Data & metrics
    scaler_type: ScalerType
    metric: MetricType

    # Reproducibility & runtime
    device: str
    seed: int = RANDOM_SEED

    def format_config(self):
        """Pretty-print the model configuration as formatted JSON."""
        data = {
            "entity": self.entity,
            "project": self.project,
            "architecture": self.architecture.value,
            "stock_code": self.stock_code,
            "window_type": self.window_type.value,
            "input_window_size": int(self.input_window_size[0]),
            "forecast_horizon_size": int(self.forecast_horizon_size[0]),
            "epochs": self.epochs,
            "learning_rate": self.learning_rate,
            "batch_size": self.batch_size,
            "optimizer": self.optimizer.value,
            "loss_fn": self.loss_fn.value,
            "scaler_type": self.scaler_type.value,
            "metric": self.metric.value,
            "device": str(self.device),
            "seed": self.seed,
        }
        print(json.dumps(data, indent=4))
