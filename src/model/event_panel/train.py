"""Config → run folder → peer-panel event classifier → scored result.

    python -m model.event_panel --config event_panel_xgb_d2__vcb__up_5pct_5day__final__d1_h5__tab.yaml
    python -m model.event_panel --config <path> --dry-run

No training logic here — `model/common/engine.py`'s estimator path. `model_type` is
`EVENT_PANEL_XGB`. ⚠️ The peer universe's `pool__*` tables must exist (RUNBOOK G6).
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Sequence

from model.common import engine
from model.common.engine import RUNS_DIR, load_config  # re-exported
from model.event_panel import model as event_panel_model

_HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(_HERE, "configs")
MODEL_TYPE = "EVENT_PANEL_XGB"

__all__ = ["CONFIG_DIR", "MODEL_TYPE", "RUNS_DIR", "load_config", "main", "train"]


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    return engine.train_estimator(
        config, model_module=event_panel_model, model_type=MODEL_TYPE,
        runs_dir=runs_dir, dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--config" not in argv:
        raise SystemExit("python -m model.event_panel --config <name>.yaml [--dry-run]")
    path = argv[argv.index("--config") + 1]
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, os.path.basename(path))
    return train(load_config(path), dry_run="--dry-run" in argv)


if __name__ == "__main__":
    main()
