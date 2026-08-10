# src\model\lstm\train.py
"""Config → run folder → trained LSTM → scored result. One command, no notebook.

    python -m model.lstm                                     # default config
    python -m model.lstm --config configs/vcb__return_5day__final__d20_h5.yaml
    python -m model.lstm --config <path> --dry-run           # print the plan only

`RUN__lstm.ipynb` is the same thing with figures. Both call `train()` — the notebook
holds no logic the script does not, so a sweep is a shell loop rather than nine edited
copies of a notebook.

## ⚠️ The engine moved to `model/common/engine.py` (2026-08-10)

Everything this file used to do — `_verify`, `_write_predictions`, `_registry_row`, the
lineage block, the `evaluate_run` call — is now in `model.common.engine`, unchanged and
shared with `model/cnn`. This module is the LSTM BINDING: it names the model module, the
`model_type` string that lands in `index.csv`, and its own `configs/` directory.

⚠️ **The public names are deliberately unchanged.** `pipeline/stages.py` imports
`CONFIG_DIR`, `RUNS_DIR`, `load_config` and `train` from here, and
`RUN__lstm.ipynb` calls `train(load_config(path))` — so `train` keeps its original
signature and binds the model itself, rather than growing the argument the engine takes.

⚠️ **`model_type` is passed by this module, not read from the YAML.** `config["model"]
["type"]` is a label a person edits; what lands in the shared `index.csv` should not be.
"""

from __future__ import annotations

import os
from typing import Dict, Optional, Sequence

from model.common import engine
from model.common.engine import CRITERIA, RUNS_DIR, load_config  # re-exported
from model.lstm import model as lstm_model

_HERE = os.path.dirname(os.path.abspath(__file__))

CONFIG_DIR = os.path.join(_HERE, "configs")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "lstm__vcb__return_5day__final__d20_h5.yaml")

# What lands in `index.csv`'s `model_type` and in the run's `metadata.json`.
MODEL_TYPE = "LSTM"

__all__ = [
    "CONFIG_DIR",
    "CRITERIA",
    "DEFAULT_CONFIG",
    "MODEL_TYPE",
    "RUNS_DIR",
    "load_config",
    "main",
    "train",
]


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    """Train one LSTM config end to end and return `(run_dir, metrics_table)`."""
    return engine.train(
        config,
        model_module=lstm_model,
        model_type=MODEL_TYPE,
        runs_dir=runs_dir,
        dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    return engine.run_cli(
        model_module=lstm_model,
        model_type=MODEL_TYPE,
        config_dir=CONFIG_DIR,
        default_config=DEFAULT_CONFIG,
        argv=argv,
    )


if __name__ == "__main__":
    main()
