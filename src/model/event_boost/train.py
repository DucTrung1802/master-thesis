"""Config → run folder → fitted boosted-tree event estimator → scored result.

    python -m model.event_boost --config event_boost_xgb_d8__liquid__up_5pct_5day__final__d1_h5__bsk.yaml
    python -m model.event_boost --config <path> --dry-run

No training logic here — `model/common/engine.py`'s estimator path, shared with
`model.event_linear`, `model.gbt`, `model.forest` and `model.baseline`. `model_type` comes from
`model.kind` (`EVENT_BOOST_XGB`, `EVENT_BOOST_RANK`).
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Sequence

from model.common import engine
from model.common.engine import RUNS_DIR, load_config  # re-exported
from model.event_boost import model as event_boost_model

_HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(_HERE, "configs")

__all__ = ["CONFIG_DIR", "RUNS_DIR", "load_config", "main", "train"]


def model_type(config: Dict) -> str:
    kind = str(config["model"].get("kind", "xgb")).strip()
    if kind not in event_boost_model.KINDS:
        raise ValueError(f"model.kind must be one of {event_boost_model.KINDS}, got {kind!r}")
    return f"EVENT_BOOST_{kind.upper()}"


def train(config: Dict, runs_dir: str = RUNS_DIR, dry_run: bool = False):
    return engine.train_estimator(
        config, model_module=event_boost_model, model_type=model_type(config),
        runs_dir=runs_dir, dry_run=dry_run,
    )


def main(argv: Optional[Sequence[str]] = None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--config" not in argv:
        raise SystemExit("python -m model.event_boost --config <name>.yaml [--dry-run]")
    path = argv[argv.index("--config") + 1]
    if not os.path.isabs(path) and not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, os.path.basename(path))
    return train(load_config(path), dry_run="--dry-run" in argv)


if __name__ == "__main__":
    main()
