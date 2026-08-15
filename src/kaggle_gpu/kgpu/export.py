# src\kaggle_gpu\kgpu\export.py
"""Stage the payload dataset: PostgreSQL -> parquet, repo source -> zip.

⚠️ **A KAGGLE WORKER CANNOT REACH `database_main_v2`.** That single fact is why
this module exists and why running a notebook on a Kaggle GPU is not "point the
kernel at the repo". The pools the notebook joins are read here, on the machine
that can see the database, typed by `UnifiedSchemaReader` exactly as the notebook
would have typed them, and written to parquet — so the frame that arrives on the
GPU is the frame the local run would have built.

What travels, and why each piece is needed:

| file | why |
|---|---|
| `<pool>.parquet` | the data. Typed at export, so `numeric` never arrives as `object` |
| `manifest.json` | the `information_schema` types, the row counts, the date spans, the full schema overview — everything `UnifiedSchemaReader` gets from SQL |
| `source.zip` | `src/feature_selection/*.py` and friends. The Kaggle image has torch/xgboost/sklearn; it does not have this repo |
| `kgpu_bootstrap.py` | the first cell's entry point, on the remote side |
| `kgpu_remote_reader.py` | `UnifiedSchemaReader` with the SQL swapped for parquet — **subclassed, so `join()` is inherited rather than re-implemented** |

⚠️ **THE PAYLOAD IS FLAT ON PURPOSE.** `dataset_create_version(dir_mode="skip")`
silently ignores subdirectories, so a payload with a `data/` folder uploads as a
dataset holding nothing but the loose files, and the run fails on the GPU with a
missing-file error that says nothing about why. Every file here is at the top
level; the only nesting is inside `source.zip`, which the remote side unpacks.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from .config import REPO_ROOT, JobConfig, repo_src_on_path

# The remote-side files, shipped flat beside the parquet.
REMOTE_DIR = Path(__file__).resolve().parent / "remote"
REMOTE_FILES = ("kgpu_bootstrap.py", "kgpu_remote_reader.py")

# What never travels: bytecode, checkpoints, and the notebooks themselves. The
# notebook being run is uploaded as the KERNEL; the four `study_*.ipynb` files
# beside it are 1.4 MB each of finished write-up and would be dead weight in
# every dataset version.
SOURCE_INCLUDE = (".py",)
SOURCE_EXCLUDE_DIRS = {"__pycache__", ".ipynb_checkpoints", ".git", "runs"}

MANIFEST = "manifest.json"


# Moved to `config.py` (2026-08-16) — `runner` and `__main__` want it too, and three
# copies of one `sys.path.insert` is three places for the repo layout to be wrong.
_repo_src_on_path = repo_src_on_path


def git_commit() -> str | None:
    """The commit the payload was cut at — `report._git_commit`'s answer, locally.

    ⚠️ Recorded and replayed on the worker. `feature_selection.report` shells out
    to git for the provenance line in every `metadata.json`, and a Kaggle worker
    has no repo — so without this the whole point of that field is lost on exactly
    the runs that are hardest to reproduce.
    """
    try:
        head = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=10,
        )
        if head.returncode != 0:
            return None
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=10,
        )
        return head.stdout.strip() + ("+dirty" if dirty.stdout.strip() else "")
    except Exception:  # noqa: BLE001 - a missing git is not a failed export
        return None


def _zip_sources(dirs: List[str], out: Path) -> Dict[str, int]:
    """Zip the given repo-relative directories, paths preserved from the repo root."""
    counts: Dict[str, int] = {}
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for rel in dirs:
            base = REPO_ROOT / rel
            n = 0
            for path in sorted(base.rglob("*")):
                if path.is_dir():
                    continue
                if SOURCE_EXCLUDE_DIRS & set(path.relative_to(base).parts[:-1]):
                    continue
                if path.suffix not in SOURCE_INCLUDE:
                    continue
                archive.write(path, path.relative_to(REPO_ROOT).as_posix())
                n += 1
            if n == 0:
                raise ValueError(
                    f"source_dirs entry {rel!r} contributed 0 files — only "
                    f"{SOURCE_INCLUDE} are shipped, and a package with no .py in it "
                    f"will fail to import on the worker."
                )
            counts[rel] = n
    return counts


def _dataset_metadata(cfg: JobConfig) -> dict:
    data = cfg.data
    assert data is not None
    return {
        "title": data.title or f"{cfg.title} payload",
        "id": data.id,
        "licenses": [{"name": "other"}],
    }


def _payload_hash(folder: Path) -> str:
    """Content hash of the staged payload — what `run` compares against uploads.

    ⚠️ `uploaded.json` is excluded: it is written INTO the payload after the hash
    is taken, so including it would make every payload stale the instant it was
    uploaded — a staleness check that always fires is a staleness check nobody reads.
    """
    digest = hashlib.sha256()
    for path in sorted(folder.rglob("*")):
        if path.is_file() and path.name != "uploaded.json":
            digest.update(path.relative_to(folder).as_posix().encode())
            digest.update(str(path.stat().st_size).encode())
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()[:16]


def export(cfg: JobConfig, quiet: bool = False) -> Path:
    """Read the job's tables out of PostgreSQL and stage the whole payload.

    Returns the payload folder. Raises if the job declares no `data` block —
    a notebook that needs no database needs no payload and should say so by
    omitting it.
    """
    if cfg.data is None:
        raise ValueError(
            f"job {cfg.name!r} has no 'data' block, so there is nothing to export. "
            f"Add one naming the dataset id and the ticker, or run the job as-is."
        )

    _repo_src_on_path()
    # ⚠️ Explicit path, per the repo's standing rule: this module can be invoked
    # from anywhere, and `find_dotenv()` from the wrong CWD gives an empty
    # password and a connection error that blames PostgreSQL.
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")

    from feature_selection.unified_reader import UnifiedSchemaReader

    tables = cfg.tables()
    folder = cfg.payload_dir
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)

    manifest: dict = {
        "kgpu_payload_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_commit": git_commit(),
        "ticker": cfg.data.ticker,
        "job": cfg.name,
        "tables": {},
    }

    with UnifiedSchemaReader(cfg.data.ticker) as reader:
        manifest["schema"] = reader.schema
        manifest["database"] = reader.database

        available = set(reader.pools())
        missing = [t for t in tables if t not in available and not t.startswith("_")]
        if missing:
            raise ValueError(
                f"{reader.schema} has no table(s) {missing}. Materialise them first:\n"
                f'  dagster asset materialize -f src/orchestration/definitions.py '
                f'--select "group:unified" --partition {cfg.data.ticker}'
            )

        # ⚠️ The FULL schema overview travels, not just the shipped tables'. The
        # notebook prints it as its orientation table, and an overview showing
        # only what was shipped would hide that 74 other pools exist — the exact
        # information a reader uses to notice a pool was left behind.
        overview = reader.overview()
        overview["shipped"] = overview["table"].isin(tables)
        manifest["overview"] = json.loads(
            overview.to_json(orient="records", date_format="iso")
        )

        for index, table in enumerate(tables, start=1):
            frame = reader.read(table)
            types = reader.column_types(table)
            path = folder / f"{table}.parquet"
            frame.to_parquet(path, index=False)

            manifest["tables"][table] = {
                "file": path.name,
                "rows": int(len(frame)),
                "columns": int(frame.shape[1]),
                "column_types": types,
                "first_date": str(frame["date"].min().date()),
                "last_date": str(frame["date"].max().date()),
                "bytes": path.stat().st_size,
            }
            if not quiet:
                # A count of TABLES, not of bytes — one wide pool can be 100x
                # another, so this is a position in the list, not a time estimate.
                print(
                    f"  [{index}/{len(tables)} {index / len(tables):>4.0%}] "
                    f"{table:<40} {len(frame):>7,} x {frame.shape[1]:>5}  "
                    f"{path.stat().st_size / 1024**2:>7.1f} MB  "
                    f"-> {frame['date'].max():%Y-%m-%d}",
                    flush=True,
                )

    # ⚠️ THE CALENDAR CHECK, HERE AS WELL AS IN THE NOTEBOOK. The notebook's guard
    # cell raises on the worker — after the queue, the upload and the startup. The
    # same assertion costs nothing here and fails before anything is spent.
    ends = {t: m["last_date"] for t, m in manifest["tables"].items()}
    if len(set(ends.values())) != 1:
        raise ValueError(
            f"the pools end on different dates {ends}\n"
            f"  The notebook's INNER join would truncate the panel to the earliest "
            f"and every number in the report would silently describe the shorter "
            f"window. Re-materialise the lagging pool before exporting."
        )

    source_counts = _zip_sources(cfg.data.source_dirs, folder / "source.zip")
    manifest["source_dirs"] = source_counts

    for name in REMOTE_FILES:
        shutil.copy2(REMOTE_DIR / name, folder / name)

    (folder / MANIFEST).write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    (folder / "dataset-metadata.json").write_text(
        json.dumps(_dataset_metadata(cfg), indent=2), encoding="utf-8"
    )

    total = sum(p.stat().st_size for p in folder.iterdir() if p.is_file())
    if not quiet:
        print(
            f"  source.zip: {sum(source_counts.values())} .py from "
            f"{', '.join(source_counts)}"
        )
        print(f"staged payload -> {folder}  ({total / 1024**2:.1f} MB total)")
    return folder


def load_manifest(cfg: JobConfig) -> dict:
    path = cfg.payload_dir / MANIFEST
    if not path.exists():
        raise FileNotFoundError(
            f"no staged payload for job {cfg.name!r} ({path}).\n"
            f"  Run: python -m kgpu data {cfg.name}"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def payload_hash(cfg: JobConfig) -> str:
    return _payload_hash(cfg.payload_dir)


def upload_record(cfg: JobConfig) -> dict | None:
    """What `kgpu data` recorded about the last upload, if there was one."""
    path = cfg.payload_dir / "uploaded.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_upload_record(cfg: JobConfig, version: str | int | None) -> dict:
    record = {
        "dataset": cfg.data.id if cfg.data else None,
        "version": version,
        "payload_hash": payload_hash(cfg),
        "tables": cfg.tables(),
        "uploaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (cfg.payload_dir / "uploaded.json").write_text(
        json.dumps(record, indent=2), encoding="utf-8"
    )
    return record
