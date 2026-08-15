# src\kaggle_gpu\kgpu\dataset.py
"""Upload the payload as a private Kaggle dataset, and wait for it to be usable.

⚠️ **A DATASET IS NOT READABLE THE MOMENT THE UPLOAD RETURNS.** Kaggle processes
a new version asynchronously; a kernel pushed against a dataset still in
`processing` mounts the PREVIOUS version, runs to COMPLETE, and hands back a
report built on last week's data with nothing anywhere saying so. `wait_ready`
is not politeness — it is the only thing standing between a stale payload and a
result that looks fine.

⚠️ `dir_mode` stays at its default `"skip"`, which IGNORES subdirectories. That is
safe here only because `export.py` stages the payload FLAT. If a subdirectory ever
appears in it, its contents are dropped without a word.
"""

from __future__ import annotations

import json
import time
from typing import Optional

from .config import JobConfig
from .export import load_manifest, upload_record, write_upload_record

# Kaggle's own status strings, lowercased by the client.
READY = "ready"
FAILED = {"error", "failed"}


def _api():
    from .runner import _api as _kernel_api

    return _kernel_api()


def _exists(api, dataset_id: str) -> bool:
    try:
        api.dataset_status(dataset_id)
        return True
    except Exception:  # noqa: BLE001 - the client raises several types for "no such"
        return False


def _state(api, dataset_id: str) -> tuple:
    """`(status, current_version_number)` — both, in one call where the API allows."""
    try:
        payload = json.loads(
            api.dataset_status(dataset_id, format="json(status,current_version_number)")
        )
        return str(payload.get("status", "")).lower(), payload.get(
            "current_version_number"
        )
    except Exception as exc:  # noqa: BLE001 - a fresh dataset 404s for a moment
        return f"pending ({type(exc).__name__})", None


def current_version(api, dataset_id: str) -> Optional[int]:
    return _state(api, dataset_id)[1]


def wait_ready(
    api,
    dataset_id: str,
    poll_seconds: int = 10,
    timeout_minutes: int = 30,
    min_version: Optional[int] = None,
) -> str:
    """Poll until the NEW version is processed. Returns the final status.

    ⚠️ **`status == "ready"` ALONE IS NOT EVIDENCE THE UPLOAD LANDED.** A dataset
    that already has a processed version reports `ready` the instant a new version
    is accepted — measured 2026-08-15, where a fresh version returned `ready` on
    the first poll. A kernel pushed on that answer mounts the PREVIOUS version,
    runs to COMPLETE, and produces a report on the old data with nothing saying so.
    `min_version` is what makes the wait mean something: the version NUMBER has to
    move, not just the word.
    """
    deadline = time.perf_counter() + timeout_minutes * 60
    last: Optional[str] = None
    while True:
        status, version = _state(api, dataset_id)
        shown = f"{status} v{version}" if version is not None else status

        if shown != last:
            print(f"\n  dataset {shown}", end="", flush=True)
            last = shown
        else:
            print(".", end="", flush=True)

        arrived = min_version is None or (
            version is not None and version >= min_version
        )
        if status == READY and arrived:
            print()
            return status
        if status in FAILED:
            print()
            raise RuntimeError(
                f"Kaggle could not process the dataset ({status}). Open "
                f"https://www.kaggle.com/datasets/{dataset_id} for the reason."
            )
        if time.perf_counter() > deadline:
            print()
            raise TimeoutError(
                f"dataset {dataset_id} was still {shown!r} after "
                f"{timeout_minutes} min"
                + (f" (waiting for version >= {min_version})" if min_version else "")
                + "."
            )
        time.sleep(poll_seconds)


def upload(cfg: JobConfig, notes: str = "") -> dict:
    """Create the dataset or push a new version of it, then wait until ready."""
    if cfg.data is None:
        raise ValueError(f"job {cfg.name!r} has no 'data' block to upload.")

    manifest = load_manifest(cfg)
    folder = str(cfg.payload_dir)
    api = _api()

    notes = notes or (
        f"{manifest['schema']} · {len(manifest['tables'])} tables · "
        f"exported {manifest['exported_at']} @ {manifest.get('git_commit')}"
    )

    before = current_version(api, cfg.data.id) if _exists(api, cfg.data.id) else None

    if before is not None or _exists(api, cfg.data.id):
        print(f"versioning dataset {cfg.data.id} (currently v{before})")
        response = api.dataset_create_version(
            folder,
            version_notes=notes,
            quiet=True,
            convert_to_csv=False,
            delete_old_versions=False,
        )
    else:
        print(f"creating dataset {cfg.data.id} (private)")
        response = api.dataset_create_new(
            folder,
            public=False,
            quiet=True,
            convert_to_csv=False,
        )

    error = getattr(response, "error", None)
    if error:
        raise RuntimeError(f"Kaggle rejected the dataset upload: {error}")

    status = wait_ready(
        api,
        cfg.data.id,
        poll_seconds=10,
        min_version=(before + 1) if before is not None else None,
    )
    version = current_version(api, cfg.data.id) or getattr(
        response, "versionNumber", None
    )
    record = write_upload_record(cfg, version)
    print(
        f"dataset ready: https://www.kaggle.com/datasets/{cfg.data.id}"
        + (f" (version {version})" if version else "")
    )
    return {"status": status, **record}


def check_uploaded(cfg: JobConfig) -> None:
    """Raise unless what is staged locally is what was last uploaded.

    ⚠️ This is the `skip_existing` lesson from the data pipeline, one layer over:
    a green push proves an upload happened, never that THIS payload is the one on
    Kaggle. The content hash is the only honest check.
    """
    if cfg.data is None:
        return
    record = upload_record(cfg)
    if record is None:
        raise RuntimeError(
            f"job {cfg.name!r} needs dataset {cfg.data.id}, but nothing has been "
            f"uploaded from this payload.\n  Run: python -m kgpu data {cfg.name}"
        )

    from .export import payload_hash

    if record.get("payload_hash") != payload_hash(cfg):
        raise RuntimeError(
            f"the staged payload for {cfg.name!r} differs from what was uploaded "
            f"({record.get('uploaded_at')}).\n"
            f"  The kernel would run against the OLD data and say nothing.\n"
            f"  Run: python -m kgpu data {cfg.name}"
        )

    wanted = set(cfg.tables())
    uploaded = set(record.get("tables") or [])
    if wanted != uploaded:
        raise RuntimeError(
            f"job {cfg.name!r} now names tables {sorted(wanted)} but the uploaded "
            f"payload holds {sorted(uploaded)}.\n"
            f"  Run: python -m kgpu data {cfg.name}"
        )
