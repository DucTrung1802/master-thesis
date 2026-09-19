"""Config → run folder → fitted forest → scored result.

    python -m model.forest --config forest_rf200_d12__liquid__uphold_5pct_5day__final__d10_h5__bsk.yaml
    python -m model.forest --config <path> --dry-run

No training logic here — `model/common/engine.py`'s estimator path, shared with
`model.gbt` and `model.baseline`. `model_type` comes from `model.kind` (`FOREST_ET`,
`FOREST_RF`) so the two families are distinguishable in `index.csv`.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Sequence

from model.common import engine
from model.common.engine import RUNS_DIR, load_config  # re-exported
from model.forest import model as forest_model

_HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(_HERE, "configs")

__all__ = ["CONFIG_DIR", "RUNS_DIR", "load_config", "main", "train"]


def model_type(config: Dict) -> str:
    kind = str(config["model"].get("kind", "et")).strip()
    if kind not in forest_model.KINDS:
        raise ValueError(f"model.kind must be one of {forest_model.KINDS}, got {kind!r}")
    return f"FOREST_{kind.upper()}"


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    return engine.train_estimator(
        config, model_module=forest_model, model_type=model_type(config),
        runs_dir=runs_dir, dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--config" not in argv:
        raise SystemExit("python -m model.forest --config <name>.yaml [--dry-run]")
    path = argv[argv.index("--config") + 1]
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, os.path.basename(path))
    return train(load_config(path), dry_run="--dry-run" in argv)


if __name__ == "__main__":
    main()
