# src\model\cnnlstm	rain.py
"""Config -> run folder -> trained Conv1d feature extractor followed by an LSTM -> scored result.

    python -m model.cnnlstm
    python -m model.cnnlstm --config cnnlstm__all__rank_10day__final__d20_h10.yaml
    python -m model.cnnlstm --config <path> --dry-run

⚠️ **No training logic lives here, deliberately** — `model/common/engine.py` holds
`_verify`, the prediction writer (which inverse-transforms the target and inserts the
`ticker` column that tells `result_evaluator` a run is a panel), the lineage block and
the registry row. A CNNLSTM run and an LSTM run therefore land in `index.csv`
scored by identical code, which is the only reason the two rows may be read against
each other. `model/CONTEXT.md` §7.

This module names three things: the model module, the `model_type` string, and its own
`configs/` directory.
"""

from __future__ import annotations

import os
from typing import Dict, Optional, Sequence

from model.cnnlstm import model as cnnlstm_model
from model.common import engine
from model.common.engine import CRITERIA, RUNS_DIR, load_config  # re-exported

_HERE = os.path.dirname(os.path.abspath(__file__))

CONFIG_DIR = os.path.join(_HERE, "configs")
# ⚠️ The filename is prefixed with the model. `pipeline._config_path` resolves a bare
# `--config` name across `model/*/configs/`, so a name shared with another package is
# ambiguous — `model/CONTEXT.md` §2 (issue CFG-1).
DEFAULT_CONFIG = os.path.join(
    CONFIG_DIR, "cnnlstm__all__rank_10day__final__d20_h10.yaml"
)

# What lands in `index.csv`'s `model_type` and in the run's `metadata.json`.
MODEL_TYPE = "CNNLSTM"

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
    """Train one CNNLSTM config end to end and return `(run_dir, metrics_table)`."""
    return engine.train(
        config,
        model_module=cnnlstm_model,
        model_type=MODEL_TYPE,
        runs_dir=runs_dir,
        dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    return engine.run_cli(
        model_module=cnnlstm_model,
        model_type=MODEL_TYPE,
        config_dir=CONFIG_DIR,
        default_config=DEFAULT_CONFIG,
        argv=argv,
    )


if __name__ == "__main__":
    main()
