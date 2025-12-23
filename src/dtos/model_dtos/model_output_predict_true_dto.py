from dataclasses import dataclass
from typing import List


@dataclass
class ModelOutputPredictTrueDto:
    index: int
    history_data: List[float]
    y_pred_denorm: List[float]
    y_true: List[float]

    def to_dict(self) -> dict:
        """Return prediction vs true output as a serializable dictionary."""
        return {
            "index": self.index,
            "history_data": self.history_data,
            "y_pred_denorm": self.y_pred_denorm,
            "y_true": self.y_true,
        }
