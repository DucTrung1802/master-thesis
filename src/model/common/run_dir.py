"""RunDir — one immutable experiment run = one self-contained folder.

Layout (data is *referenced*, never cloned):

    runs/<run_name>__<YYYYmmdd-HHMMSS>/
      config.yaml       # the exact input config for this run
      metadata.json     # resolved facts: git sha, env, device, dataset ref+hash, timings, metrics
      model/            # best.pt, last.pt, arch.json  (reproduce / resume)
      tensorboard/      # TB event files (scalars, hparams)
      results/          # metrics.json, predictions_*.csv, plots
      logs/
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
from datetime import datetime

import yaml


def _git_sha(repo_dir: str) -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo_dir,
            capture_output=True, text=True, timeout=5,
        )
        sha = out.stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], cwd=repo_dir,
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        return sha + ("-dirty" if dirty else "") if sha else "unknown"
    except Exception:
        return "unknown"


def _lib_versions() -> dict:
    vers = {"python": sys.version.split()[0], "platform": platform.platform()}
    for mod in ("numpy", "pandas", "torch", "sklearn"):
        try:
            m = __import__(mod)
            vers[mod] = getattr(m, "__version__", "?")
        except Exception:
            vers[mod] = "not-installed"
    try:
        import torch
        if torch.cuda.is_available():
            vers["cuda_device"] = torch.cuda.get_device_name(0)
            vers["cuda"] = torch.version.cuda
    except Exception:
        pass
    return vers


class RunDir:
    """Creates + owns a single run folder and writes its config/metadata."""

    def __init__(self, root: str, run_id: str, config: dict):
        self.root = os.path.abspath(root)
        self.run_id = run_id
        self.dir = os.path.join(self.root, run_id)
        self.config = config
        self._metadata: dict = {}

    # ---- construction -----------------------------------------------------
    @classmethod
    def create(cls, base_dir: str, run_name: str, config: dict,
               repo_dir: str | None = None) -> "RunDir":
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        run_id = f"{run_name}__{ts}"
        self = cls(base_dir, run_id, config)
        for sub in ("", "model", "tensorboard", "results", "logs"):
            os.makedirs(os.path.join(self.dir, sub), exist_ok=True)

        repo_dir = repo_dir or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..")
        )
        with open(self.path("config.yaml"), "w") as f:
            yaml.safe_dump(config, f, sort_keys=False)
        self._metadata = {
            "run_id": run_id,
            "run_name": run_name,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "git_sha": _git_sha(repo_dir),
            "env": _lib_versions(),
            "config": config,
        }
        self.save_metadata()
        return self

    # ---- paths ------------------------------------------------------------
    def path(self, *parts) -> str:
        return os.path.join(self.dir, *parts)

    @property
    def model_dir(self) -> str:
        return self.path("model")

    @property
    def tb_dir(self) -> str:
        return self.path("tensorboard")

    @property
    def results_dir(self) -> str:
        return self.path("results")

    @property
    def logs_dir(self) -> str:
        return self.path("logs")

    # ---- metadata ---------------------------------------------------------
    def update_metadata(self, **kwargs) -> None:
        self._metadata.update(kwargs)
        self.save_metadata()

    def save_metadata(self) -> None:
        with open(self.path("metadata.json"), "w") as f:
            json.dump(self._metadata, f, indent=2, default=str)

    @property
    def metadata(self) -> dict:
        return self._metadata
