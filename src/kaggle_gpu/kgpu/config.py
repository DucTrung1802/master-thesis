# src\kaggle_gpu\kgpu\config.py
"""Job configuration — one `kaggle_config.json`, many jobs, all repo-relative.

⚠️ **THIS FILE USED TO DESCRIBE ONE NOTEBOOK IN ITS OWN FOLDER.** It now describes
JOBS in the master-thesis repo: a job names a notebook anywhere under the repo, the
source directories that notebook imports, and the unified-schema tables it reads.
That is the whole difference between a demo and an integration — a Kaggle worker
cannot reach `database_main_v2`, so the tables have to travel with the notebook.

Three objects:

    JobConfig     one kernel: which notebook, which accelerator, which parameters
    DataConfig    the payload dataset: which schema, which tables, which source dirs
    load_job()    read `kaggle_config.json`, merge `defaults`, validate hard

⚠️ **EVERY UNKNOWN KEY RAISES.** A silently-ignored key in a config that decides
what runs on a paid GPU is the same failure mode as Kaggle silently ignoring an
unrecognised `accelerator` — you get a run, it just is not the run you asked for.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# `<repo>/src/kaggle_gpu/kgpu/config.py` -> the package root, then the repo root.
PKG_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = PKG_ROOT.parent.parent


def repo_src_on_path() -> None:
    """Put the repo's `src` on `sys.path`.

    ⚠️ **`python -m kgpu` runs from `src/kaggle_gpu/`, so the repo's `src` is NOT
    importable by default** — this tool ships repo modules to a worker and reads the
    artefacts they write, so it needs `feature_selection`, `utils` and friends. It
    lived in `export.py` as `_repo_src_on_path` and three other modules now want it;
    one definition beside `REPO_ROOT` is where it belongs.
    """
    src = str(REPO_ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)

CONFIG_PATH = PKG_ROOT / "kaggle_config.json"
BUILD_DIR = PKG_ROOT / ".build"  # the notebook staged for upload
PAYLOAD_DIR = PKG_ROOT / ".payload"  # the dataset staged for upload
RESULTS_DIR = PKG_ROOT / "results"  # raw download of /kaggle/working

# The label table is always joined by the selection notebooks, whether or not the
# job names it. Shipping the pools without it produces a KeyError on the GPU,
# twenty minutes and one queue after the mistake was made.
TARGETS_TABLE = "pool__targets"

# Kaggle silently ignores an unrecognised machine_shape and hands you the default
# accelerator instead, so a typo here costs a whole run. Validate locally. The
# values are case-sensitive.
ACCELERATORS = {
    "NvidiaTeslaT4": "2x Tesla T4, 15 GiB each (sm_75) - the safe default",
    "NvidiaTeslaP100": "1x Tesla P100 (sm_60) - BROKEN on Kaggle's default "
    "image; its PyTorch build has no sm_60 kernels",
    "Tpu1VmV38": "TPU VM v3-8 - requires a TPU-specific notebook, not CUDA",
}


# The single artefact a panel job ships, and the key it takes in the manifest.
PANEL_TABLE = "panel"


@dataclass(frozen=True)
class DataConfig:
    """The payload dataset: repo source + unified-schema tables as parquet.

    `tables` is derived from the job's `POOLS` parameter when it is not given —
    which is the point: the pools the notebook will join are the pools that must
    be shipped, and stating them twice is one place for them to drift.
    """

    id: str  # <kaggle-username>/<dataset-slug>
    title: str = ""
    ticker: str = "VCB"
    tables: List[str] = field(default_factory=list)
    source_dirs: List[str] = field(default_factory=lambda: ["src/feature_selection"])
    # ⚠️ **PANEL MODE — the only way a CROSS-SECTIONAL target can run on Kaggle.**
    # `read_universe_panel` joins `pool__basic ⋈ pool__targets` with ONE hand-written
    # SQL statement and derives `cs_rank_{h}day` from it, so it reaches for
    # `reader.driver._cursor_ctx()` — and `ParquetSchemaReader.driver` raises
    # "there is no database on a Kaggle worker". No `--pools` value and no notebook
    # parameter can route around that: the cross-sectional read bypasses every
    # abstraction the payload replaces. This is `CSP-1` in its second form.
    #
    # So the join happens HERE, where the database is, and the worker receives the
    # finished panel — `cs_rank` already derived, so the within-date rank is computed
    # on the full universe before anything is shipped.
    #
    # {"top_n": 300, "liquidity_before": "2014-01-01", "horizons": [20], "min_width": 5}
    panel: Optional[dict] = None

    @property
    def slug(self) -> str:
        return self.id.split("/", 1)[1]

    @property
    def is_panel(self) -> bool:
        return bool(self.panel)

    def resolved_tables(self, pools: Optional[List[str]]) -> List[str]:
        """The tables to export: explicit `tables`, else the job's POOLS, + targets."""
        if self.is_panel:
            # A panel job ships ONE artefact, already joined. Naming pools here would
            # promise a shape the worker never sees.
            return [PANEL_TABLE]
        chosen = list(self.tables) or list(pools or [])
        if not chosen:
            raise ValueError(
                f"data.id {self.id!r} names no tables and the job sets no POOLS "
                f"parameter, so nothing would be exported."
            )
        if TARGETS_TABLE not in chosen:
            chosen.append(TARGETS_TABLE)
        return list(dict.fromkeys(chosen))


@dataclass(frozen=True)
class JobConfig:
    """One Kaggle kernel job."""

    name: str
    id: str  # <kaggle-username>/<kernel-slug>
    title: str
    notebook: str  # RELATIVE TO THE REPO ROOT, not to this package
    accelerator: Optional[str] = "NvidiaTeslaT4"
    enable_gpu: bool = True
    is_private: bool = True
    enable_internet: bool = False
    timeout_seconds: int = 0
    poll_seconds: int = 15
    max_wait_minutes: int = 720  # Kaggle's own GPU session cap
    parameters: Dict[str, Any] = field(default_factory=dict)
    data: Optional[DataConfig] = None
    results_into: Optional[str] = None  # repo-relative dir to merge run folders into
    dataset_sources: List[str] = field(default_factory=list)
    competition_sources: List[str] = field(default_factory=list)
    model_sources: List[str] = field(default_factory=list)
    kernel_sources: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ paths

    @property
    def notebook_path(self) -> Path:
        return REPO_ROOT / self.notebook

    @property
    def built_notebook(self) -> Path:
        return BUILD_DIR / Path(self.notebook).name

    @property
    def payload_dir(self) -> Path:
        """One payload folder per job — two jobs must not overwrite each other."""
        return PAYLOAD_DIR / self.name

    @property
    def url(self) -> str:
        return f"https://www.kaggle.com/code/{self.id}"

    @property
    def pools(self) -> Optional[List[str]]:
        value = self.parameters.get("POOLS")
        return list(value) if isinstance(value, list) else None

    @property
    def ticker(self) -> str:
        """The job's ticker, from the data block or the notebook's own parameter."""
        if self.data is not None:
            return self.data.ticker
        return str(self.parameters.get("TICKER", "VCB"))

    def tables(self) -> List[str]:
        if self.data is None:
            return []
        return self.data.resolved_tables(self.pools)

    # --------------------------------------------------------------- metadata

    def kernel_metadata(self) -> dict:
        """The kernel-metadata.json the Kaggle API expects beside the code file."""
        sources = list(self.dataset_sources)
        if self.data is not None and self.data.id not in sources:
            sources.append(self.data.id)
        return {
            "id": self.id,
            "title": self.title,
            "code_file": self.built_notebook.name,
            "language": "python",
            "kernel_type": "notebook",
            "is_private": self.is_private,
            "enable_gpu": self.enable_gpu,
            "enable_tpu": False,
            "enable_internet": self.enable_internet,
            "dataset_sources": sources,
            "competition_sources": self.competition_sources,
            "model_sources": self.model_sources,
            "kernel_sources": self.kernel_sources,
        }


# ------------------------------------------------------------------- loading


def kaggle_slug(title: str) -> str:
    """Kaggle's own slug rule: lower-case, every other character to `-`, collapsed.

    Verified 2026-08-17 against three live kernels — `MT feature selection` →
    `mt-feature-selection`, `Local GPU Training` → `local-gpu-training`, and
    `MT cross-sectional selection (top-300 panel)` →
    `mt-cross-sectional-selection-top-300-panel`.
    """
    slug = "".join(c if c.isalnum() else "-" for c in title.lower())
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def _validate(cfg: JobConfig) -> JobConfig:
    if cfg.id.count("/") != 1 or "YOUR_" in cfg.id:
        raise ValueError(
            f"job {cfg.name!r}: 'id' must be '<kaggle-username>/<kernel-slug>', "
            f"got {cfg.id!r}"
        )
    # ⚠️ **KAGGLE DERIVES THE KERNEL SLUG FROM THE TITLE AND IGNORES A DISAGREEING
    # `id` — IT WARNS IN PROSE AND PUSHES ANYWAY.** Measured 2026-08-17: title
    # `MT cross-sectional selection (top-300 panel)` with id `.../mt-cross-sectional`
    # created `.../mt-cross-sectional-selection-top-300-panel`, so the push reported
    # success, the kernel ran on a T4 — and `status`, `wait` and `pull` all raised
    # "Kaggle has no kernel you can read" against the id that was asked for. The run
    # is not lost, but nothing can reach it until the id is corrected. This is the
    # accelerator trap's exact shape: Kaggle silently substitutes and carries on.
    expected = kaggle_slug(cfg.title)
    if cfg.id.split("/", 1)[1] != expected:
        raise ValueError(
            f"job {cfg.name!r}: Kaggle will create the kernel at "
            f"'{cfg.id.split('/', 1)[0]}/{expected}' — the slug of the TITLE — not at "
            f"{cfg.id!r}, and it will only warn.\n"
            f"  Fix EITHER side: set 'id' to that slug, or rename 'title' so it "
            f"slugs to the id you want."
        )
    if not cfg.notebook_path.exists():
        raise FileNotFoundError(
            f"job {cfg.name!r}: notebook not found: {cfg.notebook_path}\n"
            f"  'notebook' is relative to the REPO ROOT ({REPO_ROOT})."
        )
    if cfg.accelerator is not None and cfg.accelerator not in ACCELERATORS:
        options = "\n".join(f"    {k:<16} {v}" for k, v in ACCELERATORS.items())
        near = [k for k in ACCELERATORS if k.lower() == cfg.accelerator.lower()]
        hint = (
            f"\n  Did you mean {near[0]!r}? Values are case-sensitive." if near else ""
        )
        raise ValueError(
            f"job {cfg.name!r}: unknown accelerator {cfg.accelerator!r}.{hint}\n"
            f"  Kaggle ignores unrecognised values and silently gives you the\n"
            f"  default accelerator, so this is rejected here instead.\n"
            f"  Valid values (or null for the default GPU):\n{options}"
        )
    # ⚠️ **AND KAGGLE ENFORCES THE SAME 50-CHARACTER CAP ON A KERNEL TITLE, which this
    # guard did not check until 2026-08-19.** The dataset half was validated here and the
    # kernel half was not, so a 54-character title passed `plan`, passed `export`, passed
    # `rehearse`, passed a **4m 41s dataset upload**, and then died on `kernels_push` with
    # a bare `HTTPError: 400 Bad Request` naming no field. Measured on the PRF-7 job.
    # Same argument as the dataset check below: anything Kaggle validates server-side is
    # validated here, where it is free and where the message can say which field.
    if not 5 <= len(cfg.title) <= 50:
        raise ValueError(
            f"job {cfg.name!r}: title is {len(cfg.title)} characters ({cfg.title!r}); "
            f"Kaggle caps a kernel title at 50 and rejects it with a bare 400 from "
            f"kernels_push — after the dataset has already been uploaded."
        )
    if cfg.data is not None:
        if cfg.data.id.count("/") != 1 or "YOUR_" in cfg.data.id:
            raise ValueError(
                f"job {cfg.name!r}: data.id must be "
                f"'<kaggle-username>/<dataset-slug>', got {cfg.data.id!r}"
            )
        # ⚠️ **KAGGLE ENFORCES 6-50 CHARACTERS ON A DATASET TITLE AND SAYS SO ONLY AFTER
        # THE UPLOAD CALL.** Measured 2026-08-17: a 62-character title cost a full
        # `kgpu data` — 1m 51s of export and parquet write — before
        # `dataset_create_version` raised. Same class as the accelerator check above:
        # anything Kaggle validates server-side is validated here, where it is free.
        title = cfg.data.title or f"{cfg.title} payload"
        if not 6 <= len(title) <= 50:
            raise ValueError(
                f"job {cfg.name!r}: data.title is {len(title)} characters "
                f"({title!r}); Kaggle requires 6-50 and rejects it only after the "
                f"whole payload has been exported and uploaded."
            )
        for rel in cfg.data.source_dirs:
            if not (REPO_ROOT / rel).is_dir():
                raise FileNotFoundError(
                    f"job {cfg.name!r}: source_dirs entry {rel!r} is not a directory "
                    f"under {REPO_ROOT}"
                )
    return cfg


def _known(cls) -> set:
    return {f.name for f in fields(cls)}


def load_config(path: Path = CONFIG_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    unknown = set(raw) - {"defaults", "jobs"}
    if unknown:
        raise ValueError(f"Unknown top-level keys in {path.name}: {sorted(unknown)}")
    if not raw.get("jobs"):
        raise ValueError(f"{path.name} defines no jobs.")
    return raw


def job_names(path: Path = CONFIG_PATH) -> List[str]:
    return list(load_config(path)["jobs"])


def load_job(name: Optional[str] = None, path: Path = CONFIG_PATH) -> JobConfig:
    """One job by name, `defaults` merged underneath it. No name = the first job."""
    raw = load_config(path)
    jobs = raw["jobs"]
    if name is None:
        name = next(iter(jobs))
    if name not in jobs:
        raise ValueError(
            f"no job {name!r} in {path.name}. Available: {sorted(jobs)}"
        )

    merged: Dict[str, Any] = dict(raw.get("defaults", {}))
    merged.update(jobs[name])
    merged["name"] = name

    data_raw = merged.pop("data", None)
    if data_raw is not None:
        unknown = set(data_raw) - _known(DataConfig)
        if unknown:
            raise ValueError(f"job {name!r}: unknown data keys {sorted(unknown)}")
        merged["data"] = DataConfig(**data_raw)

    unknown = set(merged) - _known(JobConfig)
    if unknown:
        raise ValueError(
            f"job {name!r}: unknown keys {sorted(unknown)}. A key this loader does "
            f"not know is a key that changes nothing — which is worse than an error."
        )
    return _validate(JobConfig(**merged))


# --------------------------------------------------------------- credentials


def load_credentials() -> None:
    """Put Kaggle credentials into the environment before the Kaggle API imports.

    ⚠️ Reads BOTH `.env` files — this package's own and the repo root's. The repo
    root already carries `POSTGRES_*` for the export step, so keeping the Kaggle
    token beside it is the one-file option; keeping it here is the isolated
    option. Both work, and neither silently wins: the package `.env` is loaded
    first and `override=False` keeps it authoritative.
    """
    load_dotenv(PKG_ROOT / ".env")
    load_dotenv(REPO_ROOT / ".env")

    has_token = bool(os.environ.get("KAGGLE_API_TOKEN"))
    has_legacy = bool(
        os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")
    )
    home = Path.home() / ".kaggle"
    has_file = (home / "access_token").exists() or (home / "kaggle.json").exists()

    if not (has_token or has_legacy or has_file):
        raise RuntimeError(
            "No Kaggle credentials found.\n"
            "  Create a token at https://www.kaggle.com/settings/api, then either\n"
            f"  put KAGGLE_API_TOKEN=... in {PKG_ROOT / '.env'}, or run: "
            "kaggle auth login"
        )
