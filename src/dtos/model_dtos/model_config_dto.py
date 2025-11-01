from dataclasses import dataclass


@dataclass
class ModelConfigDto:
    epochs: int
    learning_rate: float
    batch_size: int
