from dataclasses import dataclass
from typing import List


@dataclass
class ModelPredictTrueWindowDto:
    """
    Data Transfer Object (DTO) representing a single prediction window produced
    by a time-series forecasting model.

    This class encapsulates all information required to analyze and visualize
    model predictions against ground-truth values for a specific window index.
    It is primarily designed for downstream tasks such as plotting, evaluation,
    serialization, and debugging.

    Attributes
    ----------
    index : int
        Sequential index of the prediction window within the full dataset.
        This index determines the temporal order of the window and is especially
        important when concatenating multiple windows into a continuous timeline.

    history_data : List[float]
        The historical input time-series values used by the model to generate
        the prediction. This represents the context window preceding the
        predicted values and is typically plotted first.

    y_pred_denorm : List[float]
        The model's predicted output values after denormalization. These values
        correspond temporally to `y_true` and are used for direct comparison
        against the ground truth. Denormalization ensures the predictions are
        expressed in the original data scale.

    y_true : List[float]
        The true (ground-truth) output values for the prediction horizon. These
        values are aligned with `y_pred_denorm` and serve as the reference for
        evaluating model accuracy.

    Visualization Usage
    -------------------
    This DTO supports two primary plotting strategies:

    1. Per-window plotting:
       - Each instance is plotted independently.
       - `history_data` is shown first, followed by `y_true` and
         `y_pred_denorm`.
       - Useful for inspecting individual prediction windows and performing
         localized error analysis.

    2. Concatenated (continuous) plotting:
       - Windows are plotted sequentially on a shared timeline.
       - Window with `index == 0` contributes only `history_data` to initialize
         the timeline.
       - Windows with `index >= 1` contribute `y_true` and `y_pred_denorm`,
         which are appended to the timeline.
       - Useful for visualizing long-term model performance and temporal trends.

    Methods
    -------
    to_dict() -> dict
        Convert the DTO into a JSON-serializable dictionary containing the
        window index, history data, denormalized predictions, and true values.
        This is useful for logging, persistence, or API responses.

    Notes
    -----
    This class is intentionally lightweight and free of business logic. All
    computation, evaluation, and visualization logic should be handled by
    external components to maintain separation of concerns.
    """

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
