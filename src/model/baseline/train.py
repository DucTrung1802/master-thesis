# src\model\baseline\train.py
"""Config → run folder → fitted baseline → scored result.

    python -m model.baseline --config baseline_zero__vcb__return_5day__final__d20_h5__basic.yaml
    python -m model.baseline --config <path> --dry-run

⚠️ **These are scored by exactly the same code as the LSTM and the CNN.** The engine's
estimator path shares `_verify`, `_write_predictions`, `evaluate_run` and the `index.csv`
row with the torch path — the only difference is that there is no training loop. A
baseline computed anywhere else could not be put beside a network row and read.

⚠️ **`model_type` comes from the config's `kind`**, not from a constant here: this one
package holds five estimators (`zero`, `mean`, `ridge_stats`, `ridge_flat`, `ar`) and
they must be distinguishable in the shared leaderboard. `kind` genuinely SELECTS the
model, which is why it is in the YAML where an architecture's `type` is not.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Sequence

from model.baseline import model as baseline_model
from model.common import engine
from model.common.engine import RUNS_DIR, load_config  # re-exported

_HERE = os.path.dirname(os.path.abspath(__file__))

CONFIG_DIR = os.path.join(_HERE, "configs")
DEFAULT_CONFIG = os.path.join(
    CONFIG_DIR, "baseline_zero__vcb__return_5day__final__d20_h5__basic.yaml"
)

__all__ = ["CONFIG_DIR", "DEFAULT_CONFIG", "RUNS_DIR", "load_config", "main", "train"]


def model_type(config: Dict) -> str:
    """`BASELINE_ZERO`, `BASELINE_RIDGE_STATS`, … — one per `kind`."""
    kind = str(config["model"].get("kind", "")).strip()
    if kind not in baseline_model.KINDS:
        raise ValueError(
            f"config model.kind must be one of {baseline_model.KINDS}, got {kind!r}"
        )
    return f"BASELINE_{kind.upper()}"


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    """Fit one baseline config end to end and return `(run_dir, metrics_table)`."""
    return engine.train_estimator(
        config,
        model_module=baseline_model,
        model_type=model_type(config),
        runs_dir=runs_dir,
        dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)
    path = argv[argv.index("--config") + 1] if "--config" in argv else DEFAULT_CONFIG
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, os.path.basename(path))
    return train(load_config(path), dry_run="--dry-run" in argv)


if __name__ == "__main__":
    main()
