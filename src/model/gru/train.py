# src\model\gru\train.py
"""Config → run folder → trained GRU → scored result.

    python -m model.gru --config gru__vcb__return_5day__final__d20_h5__basic.yaml
    python -m model.gru --config <path> --dry-run

Holds no training logic: the engine is `model/common/engine.py`, shared with every other
model, so `_verify`, the prediction writer, the lineage block and the `index.csv` row are
one implementation (issue **DUP-2**). This module names the model module, the
`model_type` string and its own `configs/`.
"""

from __future__ import annotations

import os
from typing import Dict, Optional, Sequence

from model.common import engine
from model.common.engine import CRITERIA, RUNS_DIR, load_config  # re-exported
from model.gru import model as gru_model

_HERE = os.path.dirname(os.path.abspath(__file__))

CONFIG_DIR = os.path.join(_HERE, "configs")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "gru__vcb__return_5day__final__d20_h5__basic.yaml")

# What lands in `index.csv`'s `model_type`. ⚠️ Passed by this module, never read from
# `config["model"]["type"]` — the YAML field is a label a person edits.
MODEL_TYPE = "GRU"

__all__ = ["CONFIG_DIR", "CRITERIA", "DEFAULT_CONFIG", "MODEL_TYPE", "RUNS_DIR",
           "load_config", "main", "train"]


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    """Train one GRU config end to end and return `(run_dir, metrics_table)`."""
    return engine.train(
        config,
        model_module=gru_model,
        model_type=MODEL_TYPE,
        runs_dir=runs_dir,
        dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    return engine.run_cli(
        model_module=gru_model,
        model_type=MODEL_TYPE,
        config_dir=CONFIG_DIR,
        default_config=DEFAULT_CONFIG,
        argv=argv,
    )


if __name__ == "__main__":
    main()
