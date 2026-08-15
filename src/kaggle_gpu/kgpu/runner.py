# src\kaggle_gpu\kgpu\runner.py
"""Push a notebook to Kaggle, run it on a GPU, bring the run folder back into the repo.

Kaggle has no public endpoint that lets a local Jupyter client attach to a remote
GPU kernel. The supported path is the Kernels API: upload the notebook as a kernel
version, Kaggle schedules it on a GPU worker, runs it top to bottom, and exposes
whatever the run wrote to `/kaggle/working`.

⚠️ **`pull` DOES TWO THINGS AND ONLY ONE OF THEM DELETES.** The raw download into
`results/` wipes that folder every time — it is a scratch mirror of
`/kaggle/working` and nothing may be kept there. The MERGE step then copies each
`<run_id>/` it finds into the repo's real report root and **never overwrites**: an
archived run folder is immutable (repo convention), so a collision is an error to
read, not a file to replace.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import List

from .config import (
    BUILD_DIR,
    PKG_ROOT,
    REPO_ROOT,
    RESULTS_DIR,
    JobConfig,
    load_credentials,
)

# Statuses that mean the worker is no longer going to make progress.
TERMINAL = {"COMPLETE", "ERROR", "CANCEL_REQUESTED", "CANCEL_ACKNOWLEDGED"}


def _api():
    """Authenticate lazily so credentials are in the environment first."""
    load_credentials()
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    return api


def _status_name(status) -> str:
    """kernels_status() returns an enum on success and occasionally a bare string."""
    return getattr(status, "name", str(status)).upper()


def _fetch_status(api, cfg: JobConfig):
    """kernels_status(), with Kaggle's 403-for-everything error made actionable."""
    try:
        return api.kernels_status(cfg.id)
    except ValueError as exc:
        if "denied" in str(exc).lower():
            raise RuntimeError(
                f"Kaggle has no kernel '{cfg.id}' you can read.\n"
                f"  If you have not pushed it yet, run: python -m kgpu push {cfg.name}\n"
                "  Otherwise check the job's 'id' in kaggle_config.json is "
                "<your-kaggle-username>/<kernel-slug>."
            ) from exc
        raise


def build(cfg: JobConfig, quiet: bool = False) -> Path:
    """Stage the patched notebook + kernel-metadata.json in a clean upload folder."""
    from . import notebook as nbbuild
    from .export import git_commit

    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True)

    path = nbbuild.build_notebook(cfg, git_commit=git_commit())

    (BUILD_DIR / "kernel-metadata.json").write_text(
        json.dumps(cfg.kernel_metadata(), indent=2) + "\n", encoding="utf-8"
    )

    if not quiet:
        size = path.stat().st_size / 1024
        print(f"staged {path.name} ({size:.0f} KiB) -> {BUILD_DIR}")
    return BUILD_DIR


def push(cfg: JobConfig) -> None:
    """Upload a new version of the kernel and start it."""
    from .dataset import check_uploaded

    check_uploaded(cfg)  # refuse to run a kernel against a payload that is not up
    folder = build(cfg)
    api = _api()

    accel = cfg.accelerator or ("(default GPU)" if cfg.enable_gpu else "CPU")
    print(f"pushing {cfg.id} | accelerator: {accel}")
    if cfg.data is not None:
        print(f"        data: {cfg.data.id}")

    result = api.kernels_push(str(folder), cfg.timeout_seconds or None, cfg.accelerator)

    if result is None:
        raise RuntimeError("Push failed; see output above.")
    if getattr(result, "error", None):
        raise RuntimeError(f"Push rejected by Kaggle: {result.error}")

    # ⚠️ **KAGGLE ACCEPTS A PUSH THAT SILENTLY DROPPED YOUR DATA SOURCES.** The
    # response carries `invalidDatasetSources` and the push otherwise succeeds —
    # the kernel starts, runs, and finds an empty /kaggle/input. Only the CLI
    # printed this; the API path used here has to check it explicitly.
    for field, label in (
        ("DatasetSources", "dataset"),
        ("CompetitionSources", "competition"),
        ("ModelSources", "model"),
        ("KernelSources", "kernel"),
    ):
        invalid = getattr(result, f"invalid{field}", None) or getattr(
            result, f"invalid_{label}_sources", None
        )
        if invalid:
            raise RuntimeError(
                f"Kaggle rejected {label} source(s) {list(invalid)} and pushed the "
                f"kernel anyway. The run would start with nothing mounted.\n"
                f"  Check the slug, and that the {label} exists and is READY."
            )

    print(f"pushed version {result.versionNumber}")
    print(f"watch: {cfg.url}")


def status(cfg: JobConfig) -> str:
    api = _api()
    response = _fetch_status(api, cfg)
    name = _status_name(response.status)
    if response.failure_message:
        print(f"{name}: {response.failure_message}")
    else:
        print(name)
    return name


def wait(cfg: JobConfig) -> str:
    """Poll until the run reaches a terminal state. Returns the final status.

    ⚠️ Bounded by `max_wait_minutes`. An unbounded poll on a hung worker is a
    process that never returns and a quota that keeps draining.
    """
    api = _api()
    start = time.perf_counter()
    deadline = start + cfg.max_wait_minutes * 60
    last = None

    while True:
        response = _fetch_status(api, cfg)
        name = _status_name(response.status)
        elapsed = time.perf_counter() - start

        if name != last:
            print(f"\n[{elapsed / 60:5.1f} min] {name}", end="", flush=True)
            last = name
        else:
            print(".", end="", flush=True)

        if name in TERMINAL:
            print()
            if response.failure_message:
                print(f"failure: {response.failure_message}")
            return name

        if time.perf_counter() > deadline:
            print()
            raise TimeoutError(
                f"still {name} after {cfg.max_wait_minutes} min. The Kaggle run is "
                f"NOT cancelled — check {cfg.url} or run: "
                f"python -m kgpu status {cfg.name}"
            )
        time.sleep(cfg.poll_seconds)


def download(cfg: JobConfig) -> List[str]:
    """Raw download of /kaggle/working into `results/`. ⚠️ Wipes it first."""
    api = _api()

    if RESULTS_DIR.exists():
        shutil.rmtree(RESULTS_DIR)
    RESULTS_DIR.mkdir(parents=True)

    files, _ = api.kernels_output(cfg.id, str(RESULTS_DIR), force=True, quiet=True)

    if not files:
        print("no output files (the run may have written nothing to /kaggle/working)")
        return []

    print(f"downloaded {len(files)} file(s) to {RESULTS_DIR}:")
    for path in sorted(files)[:20]:
        size = Path(path).stat().st_size / 1024
        print(f"  {str(Path(path).relative_to(RESULTS_DIR)):<52} {size:>9.1f} KiB")
    if len(files) > 20:
        print(f"  … and {len(files) - 20} more")
    return files


def merge_results(cfg: JobConfig, force: bool = False) -> List[Path]:
    """Copy each downloaded run folder into the repo's real report root.

    A run folder is identified the way the rest of the repo identifies one: a
    directory holding `metadata.json`. Existing folders are never overwritten —
    `--force` is the only way, and it is for a re-download of the same run, not
    for a second run that happened to pick the same id.
    """
    if not cfg.results_into:
        return []

    source_root = RESULTS_DIR / cfg.results_into
    target_root = REPO_ROOT / cfg.results_into
    if not source_root.is_dir():
        print(
            f"⚠️ nothing under results/{cfg.results_into} — the run wrote no report "
            f"folder there. Check the notebook's REPORT_ROOT."
        )
        return []

    target_root.mkdir(parents=True, exist_ok=True)
    merged: List[Path] = []
    for run in sorted(p for p in source_root.iterdir() if p.is_dir()):
        if not (run / "metadata.json").exists():
            continue
        target = target_root / run.name
        if target.exists() and not force:
            print(f"  ⚠️ {run.name} already exists in {cfg.results_into} — skipped")
            continue
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(run, target)
        merged.append(target)
        print(f"  merged {run.name} -> {cfg.results_into}/")

    if merged:
        print(
            f"\n{len(merged)} run folder(s) are now in the repo's report root — "
            f"`python -m feature_selection.outstanding` and `final_features` "
            f"will see them like any local run."
        )
    return merged


def pull(cfg: JobConfig, force: bool = False) -> List[str]:
    files = download(cfg)
    if files:
        merge_results(cfg, force=force)
    return files


def logs(cfg: JobConfig) -> None:
    api = _api()
    print(api.kernels_logs(cfg.id))


def run(cfg: JobConfig, refresh_data: bool = False, force: bool = False) -> int:
    """(export+upload) -> push -> wait -> pull -> merge. Returns a process exit code."""
    started = time.perf_counter()

    if refresh_data:
        from . import dataset, export

        export.export(cfg)
        dataset.upload(cfg)

    push(cfg)
    final = wait(cfg)

    if final != "COMPLETE":
        print(f"\nrun ended as {final}; fetching logs\n")
        logs(cfg)
        return 1

    pull(cfg, force=force)
    print(f"\ndone in {(time.perf_counter() - started) / 60:.1f} min")
    return 0


def plan(cfg: JobConfig) -> None:
    """What a `run` would do, without doing any of it."""
    from . import notebook as nbbuild
    from .export import upload_record

    print(f"job          : {cfg.name}")
    print(f"kernel       : {cfg.id}   [{cfg.accelerator or 'default GPU'}]")
    print(f"notebook     : {cfg.notebook}")
    if cfg.data is not None:
        record = upload_record(cfg)
        print(f"dataset      : {cfg.data.id}")
        print(f"  ticker     : {cfg.data.ticker}")
        print(f"  tables     : {', '.join(cfg.tables())}")
        print(f"  source     : {', '.join(cfg.data.source_dirs)}")
        print(
            f"  uploaded   : {record['uploaded_at']} (version {record['version']})"
            if record
            else "  uploaded   : ⚠️ never — run `python -m kgpu data` first"
        )
    else:
        print("dataset      : none (this job ships no data)")
    print(f"results into : {cfg.results_into or '(results/ only)'}")

    patches = nbbuild.describe_patches(cfg)
    print(f"parameters   : {len(patches)} patched in place")
    for name, line in patches.items():
        print(f"  {line}")


def rehearse(cfg: JobConfig) -> int:
    """Run `kgpu/rehearse.py` in a clean subprocess against the staged payload.

    ⚠️ A SUBPROCESS, with the CWD set to the rehearsal folder — see that module's
    docstring. Rehearsing in-process would import the repo's own
    `feature_selection` and prove nothing about the copy that travels.
    """
    import subprocess
    import sys as _sys

    from .export import REMOTE_DIR, REMOTE_FILES, load_manifest

    load_manifest(cfg)  # raises with the fix if nothing is staged

    # ⚠️ The payload is a SNAPSHOT of the remote-side files, not a link to them.
    # Editing `kgpu/remote/*.py` and rehearsing without re-exporting rehearses the
    # old copy — and passes, on code that is not the code that would be uploaded.
    stale = [
        name
        for name in REMOTE_FILES
        if (REMOTE_DIR / name).read_bytes() != (cfg.payload_dir / name).read_bytes()
    ]
    if stale:
        raise RuntimeError(
            f"the staged payload's {', '.join(stale)} differ from kgpu/remote/ — "
            f"it would rehearse code that is not what would be uploaded.\n"
            f"  Run: python -m kgpu export {cfg.name}"
        )
    work = PKG_ROOT / ".rehearsal" / cfg.name
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    # The notebook's own injected cell is what the rehearsal executes, so it has
    # to exist — and building it here also checks the parameter patches.
    built = build(cfg, quiet=True)
    notebook = cfg.built_notebook

    # ⚠️ TWO LAYOUTS, because the payload that is UPLOADED and the payload that is
    # MOUNTED are not the same shape or the same PLACE: Kaggle unpacks
    # `source.zip` into `source/`, removes the archive, and mounts the whole thing
    # at `/kaggle/input/datasets/<owner>/<slug>/` — both measured 2026-08-15, each
    # after a wasted run. Rehearsing one shape tests a branch the worker may never
    # take.
    owner, slug = cfg.data.id.split("/") if cfg.data else ("owner", "payload")
    layouts = [
        ("flat mount, source.zip", work / "flat", Path(slug), False),
        (
            f"datasets/{owner}/{slug} mount, source/ unpacked",
            work / "nested",
            Path("datasets") / owner / slug,
            True,
        ),
    ]

    for label, base, relative, extract in layouts:
        input_root = base / "input"
        run_dir = base / "work"
        run_dir.mkdir(parents=True, exist_ok=True)
        _stage_layout(cfg, input_root / relative, extract=extract)

        print(f"\nrehearsing {cfg.name} — {label}")
        result = subprocess.run(
            [
                _sys.executable,
                str(Path(__file__).with_name("rehearse.py")),
                str(input_root),
                str(run_dir),
                str(notebook),
            ],
            cwd=str(run_dir),
        )
        if result.returncode != 0:
            return result.returncode
    print(f"\nboth mount layouts pass; the built notebook is staged in {built}")
    return 0


def _stage_layout(cfg: JobConfig, mount: Path, extract: bool) -> Path:
    """Copy the payload into `mount`, optionally in Kaggle's post-extraction shape."""
    import zipfile

    if mount.exists():
        shutil.rmtree(mount)
    mount.mkdir(parents=True)
    for path in cfg.payload_dir.iterdir():
        if not path.is_file() or path.name == "uploaded.json":
            continue
        if path.name == "source.zip" and extract:
            with zipfile.ZipFile(path) as zf:
                zf.extractall(mount / "source")
        else:
            shutil.copy2(path, mount / path.name)
    return mount


def quota() -> int:
    api = _api()
    response = api.quota_view()
    if response.quota_refresh_time:
        print(f"resets: {response.quota_refresh_time.isoformat()}")
    for name, limit in (("GPU", response.gpu_quota), ("TPU", response.tpu_quota)):
        if limit is None:
            continue
        used = limit.time_used.total_seconds() / 3600
        total = limit.total_time_allowed.total_seconds() / 3600
        print(f"{name}: {max(0.0, total - used):.2f}h remaining of {total:.2f}h")
    return 0
