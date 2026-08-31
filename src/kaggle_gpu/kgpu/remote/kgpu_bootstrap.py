# src\kaggle_gpu\kgpu\remote\kgpu_bootstrap.py
"""The first cell, on the worker: make a Kaggle kernel look like the repo.

`kgpu` injects one cell at the top of every notebook it uploads, and that cell
calls `setup()`. Everything below it — the notebook you actually wrote, unedited —
then runs as if it were sitting in the repo with the database up.

Four things happen, in this order, and each one exists because the notebook below
would otherwise fail on a specific line:

1. **The source is unpacked to `/kaggle/working/src`.** Not to a library folder:
   `RUN__feature_importance_report.ipynb` finds the repo by walking UP from the CWD
   looking for `src/feature_selection`, and `feature_selection.report` anchors a
   relative `REPORT_ROOT` to *its own* three-levels-up. Put the package anywhere
   else and the run folder is written somewhere Kaggle does not collect.

2. **The database modules are stubbed.** `feature_selection/__init__.py` imports
   `unified_reader`, which imports `psycopg2` through three repo packages that are
   not shipped. The stubs let the module import; they are never called, because —

3. **`UnifiedSchemaReader` is replaced by `ParquetSchemaReader`**, which subclasses
   it and overrides only the four methods that spoke SQL. `join()` is the local
   one. The patch lands on both `feature_selection.unified_reader` and the package
   re-export, before the notebook's own import cell runs.

4. **`report._git_commit` is pinned to the commit the payload was cut at.** The
   worker has no repo, so provenance would otherwise be `null` on precisely the
   runs that are hardest to reproduce.

⚠️ **THIS FILE RUNS ON KAGGLE AND NOWHERE ELSE.** It imports nothing from `kgpu`,
because `kgpu` is not shipped — it travels flat in the payload dataset.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import types
import zipfile
from pathlib import Path

# ⚠️ **THE ONE SEAM, AND IT IS NOT A CONVENIENCE.** Both bugs this integration has
# had were in payload DISCOVERY and UNPACKING — the mount is
# `/kaggle/input/datasets/<owner>/<slug>/` and Kaggle deletes `source.zip` after
# extracting it — and neither was reachable from a local test while these paths
# were hard-coded. `python -m kgpu rehearse` sets these two variables and then runs
# the notebook's own injected cell, so the code under test is the code that ships.
INPUT_ENV = "KGPU_INPUT_DIR"
WORK_ENV = "KGPU_WORK_DIR"

# ⚠️ **THE SEAM FOR DOCUMENTS MODE, AND IT IS A STRING IN TWO FILES ON PURPOSE.** This module
# runs BEFORE the repo source is on `sys.path` — that is its whole job — so it cannot import
# `web_scraper.pdf_ocr_job` to ask what the variable is called. The other end is
# `pdf_ocr_job.DATA_ROOT_ENV` / `MODELS_DIR_ENV`, and both sides carry this note.
DATA_ROOT_ENV = "CAFEF_DATA_ROOT"
MODELS_DIR_ENV = "CAFEF_MODELS_DIR"
DOCUMENTS_ARCHIVE = "documents.zip"


def input_dir() -> Path:
    return Path(os.environ.get(INPUT_ENV, "/kaggle/input"))


def default_work_dir() -> Path:
    return Path(os.environ.get(WORK_ENV, "/kaggle/working"))

# The repo modules `feature_selection.unified_reader` imports at module level.
# ⚠️ Stubbed, not shipped: shipping them means shipping psycopg2's import chain
# and a connection attempt that can only time out. Every attribute below is
# referenced at import time and never used, because the reader is replaced.
_STUBS = {
    "dtos": {},
    "dtos.tabular_database_driver_dtos": {},
    "dtos.tabular_database_driver_dtos.postgre_sql_connection_dto": {
        "PostgreSQLConnectionDto": type("PostgreSQLConnectionDto", (), {}),
    },
    "logger": {},
    "logger.logger": {"Logger": type("Logger", (), {})},
    "tabular_database_driver": {},
    "tabular_database_driver.postgre_sql_driver": {
        "PostgreSQLDriver": type("PostgreSQLDriver", (), {}),
    },
    "utils": {},
    "utils.constants": {"DATABASE_MAIN_V2": "database_main_v2"},
}


def on_kaggle() -> bool:
    """On a worker, or standing in for one (`kgpu rehearse`)."""
    return Path("/kaggle/working").exists() or bool(os.environ.get(INPUT_ENV))


# How deep under /kaggle/input a payload may sit. ⚠️ **KAGGLE MOUNTS AT
# `/kaggle/input/datasets/<owner>/<slug>/`, NOT `/kaggle/input/<slug>/`** —
# measured 2026-08-15, and it cost the second run of this integration. Every
# tutorial, and this package's own first version, assumes the flat layout. The
# search is bounded rather than a bare rglob because a co-attached dataset can
# hold hundreds of thousands of files.
MAX_INPUT_DEPTH = 4


def find_payload() -> Path | None:
    """The mounted payload dataset — located by its manifest, not by its slug.

    ⚠️ Found by CONTENT, at whatever depth Kaggle chose. Renaming the dataset,
    Kaggle lower-casing the slug, or Kaggle moving the mount point again must not
    break the run; a directory holding a `manifest.json` beside the reader we
    shipped is the definition of the payload.
    """
    root = input_dir()
    if not root.exists():
        return None

    level = [root]
    for _ in range(MAX_INPUT_DEPTH):
        nxt = []
        for folder in level:
            if (folder / "manifest.json").is_file() and (
                folder / "kgpu_remote_reader.py"
            ).is_file():
                return folder
            try:
                nxt += sorted(c for c in folder.iterdir() if c.is_dir())
            except OSError:
                continue
        if not nxt:
            break
        level = nxt
    return None


def _importable(name: str) -> bool:
    """Is the REAL module reachable — i.e. did it travel in `source.zip`?

    ⚠️ **A STUB THAT SHADOWS A SHIPPED PACKAGE IS WORSE THAN A MISSING ONE**, because
    it fails at the import that needs it rather than at the import that is absent.
    Measured 2026-08-17 by `kgpu rehearse feature-selection`, in 3.6 s: `utils` is on
    `_STUBS` for `utils.constants.DATABASE_MAIN_V2` alone, and the stub's `__path__ =
    []` made `from utils import runtime` — added to `feature_selection/report.py` on
    2026-08-15, after this integration's only green round trip — raise *"cannot import
    name 'runtime' from 'utils' (unknown location)"*. The fix is to ship `src/utils`
    AND to stop the stub winning over it.

    `find_spec` is used rather than an import so nothing is executed here, and a
    ModuleNotFoundError from an absent PARENT (`logger.logger` under a stubbed
    `logger`) is the answer "no", not an error.
    """
    import importlib.util

    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _install_stubs() -> list:
    installed = []
    for name, attrs in _STUBS.items():
        if name in sys.modules:
            continue
        if _importable(name):  # the real one travelled — never shadow it
            continue
        module = types.ModuleType(name)
        module.__kgpu_stub__ = True
        for key, value in attrs.items():
            setattr(module, key, value)
        if "." in name:  # a submodule must be reachable as an attribute too
            parent, _, leaf = name.rpartition(".")
            module.__package__ = parent
            if parent in sys.modules:
                setattr(sys.modules[parent], leaf, module)
        else:
            module.__path__ = []  # make it a package, so submodules resolve
        sys.modules[name] = module
        installed.append(name)

    # python-dotenv is on the Kaggle image today; a stub costs nothing and turns
    # a hypothetical ImportError into a no-op.
    if "dotenv" not in sys.modules:
        try:
            import dotenv  # noqa: F401
        except ImportError:
            stub = types.ModuleType("dotenv")
            stub.load_dotenv = lambda *a, **k: False
            stub.find_dotenv = lambda *a, **k: ""
            stub.__kgpu_stub__ = True
            sys.modules["dotenv"] = stub
            installed.append("dotenv")
    return installed


def _unpack_source(payload: Path, work_dir: Path) -> int:
    """Put the repo source under `work_dir` — from source.zip, or already-extracted.

    ⚠️ **KAGGLE EXTRACTS THE ARCHIVE FOR YOU AND THE ARCHIVE IS THEN GONE**
    (measured 2026-08-15). `source.zip` is uploaded flat; what the worker mounts is
    `<payload>/source/src/feature_selection/*.py` — the zip unpacked into a folder
    named after it, with no `source.zip` left to find. The uploaded payload and the
    mounted payload are therefore DIFFERENT SHAPES, which is why both are handled
    here and why the rehearsal runs against both. A bootstrap that only knew the
    zip passed every local check and would have failed on the worker.
    """
    archive = payload / "source.zip"
    if archive.is_file():
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(work_dir)
            return len(zf.namelist())

    # `<payload>/src` (hand-staged) or `<payload>/<anything>/src` (Kaggle's unpack).
    candidates = [payload / "src"] + sorted(
        child / "src" for child in payload.iterdir() if child.is_dir()
    )
    found = [c for c in candidates if c.is_dir()]
    if not found:
        raise FileNotFoundError(
            f"no source.zip and no src/ under {payload} (looked at "
            f"{[str(c) for c in candidates]}) — the payload is incomplete. "
            f"Re-run `python -m kgpu data <job>` locally."
        )

    target = work_dir / "src"
    for source in found:
        shutil.copytree(source, target, dirs_exist_ok=True)
    return sum(1 for _ in target.rglob("*.py"))


def _documents_base(payload: Path) -> Path:
    """Where the shipped filings ended up — mounted already-unpacked, or extracted here.

    ⚠️ **THE UPLOADED PAYLOAD AND THE MOUNTED PAYLOAD ARE DIFFERENT SHAPES**, the same
    measured fact `_unpack_source` exists for: Kaggle extracts a zip into a folder named after
    it and deletes the archive, so `documents.zip` arrives as `documents/`. Both are handled,
    and `kgpu rehearse` drives both.

    ⚠️ **AN EXTRACTION GOES TO THE TEMP DIR, NEVER TO `/kaggle/working`.** That directory is
    what Kaggle collects and `kgpu pull` downloads, and this payload is ~100 MB of filings and
    OCR checkpoints. Unpacking it there would carry every byte home again for nothing.
    """
    ready = payload / "documents" / "data" / "cafef"
    if ready.is_dir():
        return payload / "documents"

    archive = payload / DOCUMENTS_ARCHIVE
    if not archive.is_file():
        raise FileNotFoundError(
            f"the manifest says mode=documents but {payload} holds neither "
            f"{DOCUMENTS_ARCHIVE} nor documents/data/cafef. Re-stage it: "
            f"python -m kgpu data <job>"
        )
    import hashlib
    import tempfile

    # Keyed on the payload path so two jobs rehearsed on one machine cannot land in the same
    # folder, and so the same payload extracts to the same place every time.
    key = hashlib.sha256(str(payload).encode()).hexdigest()[:8]
    base = Path(tempfile.gettempdir()) / "kgpu_documents" / key
    base.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zf:
        zf.extractall(base)
    return base


def _setup_documents(payload: Path, work: Path, manifest: dict) -> dict:
    """Point the CafeF PDF parse at the shipped files. Returns what it set, for the log.

    Four environment variables and nothing else — no import, no monkey-patch. `pdf_ocr_job`
    reads them (`use_data_root`, `use_models`) and the parse then behaves exactly as it does on
    a machine with the repo's own `raw_data/cafef`.

    ⚠️ `CAFEF_OCR_ENGINE` is set so a default-constructed `PdfParser` is the onnx one. Every
    `ParseLayer` names its engine explicitly, so this changes no layer's behaviour.

    ⚠️ **`TESSDATA_PREFIX` IS THE ONE THAT MAKES THE CASCADE THE SAME LENGTH ON BOTH
    MACHINES.** `cafef_pdf_parser.TESSDATA_DIR` is evaluated at IMPORT time from that variable,
    falling back to `%LOCALAPPDATA%\\tessdata` — a Windows shape that answers False on a Linux
    worker — so `_parse_cascaded` dropped `tesseract@200` (layer 4 of 55) and
    `tesseract@400+relax` (layer 7) with a bare `continue`, and a T4 ran 53 layers where this
    repo's machine runs 55 (CLAUDE.md §6-2-quinquagies). ⚠️ It is ASSIGNED and not
    `setdefault`: the payload's copy is the one this repo parses with, and an image that
    happens to export its own `TESSDATA_PREFIX` would point the parse at a DIFFERENT build of
    the language model — a different reading under the same version number.
    """
    base = _documents_base(payload)
    data_root = base / "data" / "cafef"
    models = base / "models"
    if not (data_root / "financials" / "schema").is_dir():
        raise FileNotFoundError(
            f"{data_root} carries no financials/schema — without the charts of accounts "
            f"`schema_of` raises AFTER the OCR, and every statement is rejected as "
            f"unreconcilable."
        )
    os.environ[DATA_ROOT_ENV] = str(data_root)
    os.environ[MODELS_DIR_ENV] = str(models)
    os.environ["TESSDATA_PREFIX"] = str(models)
    os.environ.setdefault("CAFEF_OCR_ENGINE", "onnx")

    spec = manifest.get("documents", {})
    return {
        "mode": "documents",
        "data_root": str(data_root),
        "models": sorted(p.name for p in models.glob("*")) if models.is_dir() else [],
        "filings": [f["file"] for f in spec.get("filings", [])],
    }


def setup(
    payload_dir: str | None = None,
    work_dir: str | None = None,
    git_commit: str | None = None,
    quiet: bool = False,
) -> dict:
    """Make the worker look like the repo. Returns what it did, for the log.

    A no-op off Kaggle: the injected cell is left in the notebook so the built
    copy still runs locally, where the real reader and the real database are
    already there.
    """
    if not on_kaggle():
        if not quiet:
            print("kgpu: not on Kaggle — leaving the local environment alone")
        return {"on_kaggle": False}

    work = Path(work_dir) if work_dir else default_work_dir()
    payload = Path(payload_dir) if payload_dir else find_payload()
    report: dict = {
        "on_kaggle": on_kaggle(),
        "payload": str(payload) if payload else None,
    }

    if payload is None:
        raise FileNotFoundError(
            "no kgpu payload dataset is mounted under /kaggle/input.\n"
            "  The kernel's dataset_sources must include the job's data.id. Run "
            "`python -m kgpu data <job>` locally, then push again."
        )

    manifest = json.loads((payload / "manifest.json").read_text(encoding="utf-8"))
    report["manifest"] = {
        k: manifest[k] for k in ("exported_at", "git_commit", "ticker", "schema")
        if k in manifest
    }

    n_files = _unpack_source(payload, work)
    src = str(work / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    report["source_files"] = n_files
    report["source_path"] = src

    report["stubs"] = _install_stubs()

    commit = git_commit or manifest.get("git_commit")

    # ⚠️ **DOCUMENTS MODE STOPS HERE, AND THE THREE STEPS BELOW MUST NOT RUN.** A PDF-parse
    # payload ships no parquet, no `feature_selection` and no database to replace, so
    # `import feature_selection` would be an ImportError on a job that needs nothing from it.
    # The mode is read from the manifest rather than inferred from a missing key: a branch that
    # tests `if manifest.get("panel")` reads a THIRD mode as the FIRST one.
    if manifest.get("mode") == "documents":
        report.update(_setup_documents(payload, work, manifest))
        if commit:
            # `pdf_ocr_job` records this into its own metadata.json. Same purpose as pinning
            # `report._git_commit` below — the worker has no repo, and a run whose provenance
            # is `null` is the run hardest to reproduce.
            os.environ["KGPU_GIT_COMMIT"] = commit
            report["git_commit"] = commit
        if not quiet:
            _print_summary(report, manifest)
        report["gpu"] = _check_gpu(quiet=quiet)
        return report

    # The payload directory holds `kgpu_remote_reader.py` flat beside the parquet.
    if str(payload) not in sys.path:
        sys.path.insert(0, str(payload))

    import kgpu_remote_reader

    kgpu_remote_reader.DEFAULT_DATA_DIR = payload

    import feature_selection
    from feature_selection import report as fs_report
    from feature_selection import unified_reader

    unified_reader.UnifiedSchemaReader = kgpu_remote_reader.ParquetSchemaReader
    feature_selection.UnifiedSchemaReader = kgpu_remote_reader.ParquetSchemaReader
    report["reader"] = "ParquetSchemaReader"

    if commit:
        fs_report._git_commit = lambda _c=commit: _c
        report["git_commit"] = commit

    report["report_root"] = fs_report.DEFAULT_REPORT_ROOT

    if not quiet:
        _print_summary(report, manifest)
    report["gpu"] = _check_gpu(quiet=quiet)
    return report


def _check_gpu(quiet: bool = False) -> str | None:
    """Report the device, and fail fast on an accelerator torch cannot use.

    ⚠️ Kaggle's default image ships a torch with no kernels for the P100 (sm_60).
    Without this the run dies somewhere inside the selection with a bare "no
    kernel image is available for execution on the device", an hour after the
    accelerator was chosen.
    """
    try:
        import torch
    except ImportError:
        if not quiet:
            print("  gpu          : torch not importable")
        return None

    if not torch.cuda.is_available():
        if not quiet:
            print("  gpu          : NONE — the job will run on the worker's CPU")
        return None

    props = torch.cuda.get_device_properties(0)
    arch = f"sm_{props.major}{props.minor}"
    if not quiet:
        print(
            f"  gpu          : {props.name} x{torch.cuda.device_count()}  "
            f"{props.total_memory / 1024**3:.1f} GiB  {arch}"
        )
    supported = torch.cuda.get_arch_list()
    if arch not in supported:
        raise RuntimeError(
            f"{props.name} is {arch}; this torch build has kernels only for "
            f"{[a for a in supported if a.startswith('sm_')]}. Set "
            f'"accelerator": "NvidiaTeslaT4" in kaggle_config.json.'
        )
    return props.name


def _print_summary(report: dict, manifest: dict) -> None:
    print("kgpu bootstrap")
    print(f"  payload      : {report['payload']}")
    print(
        f"  exported     : {manifest.get('exported_at')}  "
        f"@ {manifest.get('git_commit')}"
    )
    print(f"  source       : {report['source_files']} files -> {report['source_path']}")
    if report.get("mode") == "documents":
        spec = manifest.get("documents", {})
        models = ", ".join(report["models"]) or "NONE — the engine would try to download"
        print(f"  data root    : {report['data_root']}")
        print(f"  models       : {models}")
        print(f"  filings      : {len(report['filings'])} of "
              f"{spec.get('exchange')}_{spec.get('symbol')} "
              f"({', '.join(f['period'] for f in spec.get('filings', []))})")
        return
    print(f"  reader       : {report['reader']} (parquet, join() inherited)")
    print(f"  report root  : {report['report_root']}")
    for name, entry in manifest.get("tables", {}).items():
        print(
            f"    {name:<44} {entry['rows']:>7,} x {entry['columns']:>5}  "
            f"-> {entry['last_date']}"
        )
