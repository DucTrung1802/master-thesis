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
import contextlib
import shutil
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from .config import (
    BUILD_DIR,
    PKG_ROOT,
    REPO_ROOT,
    RESULTS_DIR,
    JobConfig,
    load_credentials,
    repo_src_on_path,
)


def _shortlist_filename() -> str:
    """`outstanding.csv`, from the module that DEFINES the handoff.

    ⚠️ Not a literal here, and not imported at module scope. `feature_selection.contract`
    is the one place that string is spelled — a rename must not leave this checker
    looking for a file that no longer exists while reporting that everything is fine —
    but it pulls in pandas, and `python -m kgpu jobs` has no business paying for that.
    """
    repo_src_on_path()
    from feature_selection.contract import SHORTLIST_FILENAME

    return SHORTLIST_FILENAME

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


#: How long to keep retrying a status poll that failed for a NETWORK reason, and how
#: long to sleep between attempts. ⚠️ **Measured 2026-08-19: a six-hour run lost its
#: watcher at 355 min to one `ConnectionError` from `api.kaggle.com`.** The kernel was
#: fine — it was still RUNNING when checked by hand — but the local process was gone,
#: so nothing pulled the results and nothing recorded the duration. A watcher that
#: cannot outlive a transient DNS blip is the weakest link in a job whose entire point
#: is that it takes hours.
POLL_RETRY_MINUTES = 30.0
POLL_RETRY_SECONDS = 30.0


def _fetch_status(api, cfg: JobConfig, retry_minutes: float = 0.0):
    """kernels_status(), with Kaggle's 403-for-everything error made actionable.

    ⚠️ **A NETWORK failure is retried; an ANSWER is not.** A `ConnectionError` says
    nothing about the kernel, so giving up on one throws away a running job's result.
    A `ValueError` from Kaggle IS an answer — the kernel is missing or unreadable — and
    retrying it would only repeat a wrong request for half an hour.
    """
    deadline = time.perf_counter() + retry_minutes * 60
    attempt = 0
    while True:
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
        except Exception as exc:  # noqa: BLE001 — network, DNS, TLS, proxy, 5xx
            attempt += 1
            if time.perf_counter() >= deadline:
                raise RuntimeError(
                    f"lost contact with api.kaggle.com for {retry_minutes:.0f} min "
                    f"({attempt} attempts; last: {type(exc).__name__}).\n"
                    f"  ⚠️ THE KERNEL IS PROBABLY STILL RUNNING — this is the WATCHER "
                    f"giving up, not the job.\n"
                    f"  Check it with:  python -m kgpu status {cfg.name}\n"
                    f"  Resume with:    python -m kgpu wait {cfg.name}  then  "
                    f"python -m kgpu pull {cfg.name}"
                ) from exc
            print(f"  network error ({type(exc).__name__}); retrying in "
                  f"{POLL_RETRY_SECONDS:.0f}s — the kernel is unaffected", flush=True)
            time.sleep(POLL_RETRY_SECONDS)


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


def wait(cfg: JobConfig, on_poll=None) -> str:
    """Poll until the run reaches a terminal state. Returns the final status.

    ⚠️ **KAGGLE REPORTS NO PROGRESS, SO THE PERCENTAGE HERE IS AGAINST THE LAST
    COMPLETED RUN OF THIS JOB AND SAYS SO ON EVERY LINE.** There is no completion
    fraction in `kernels_status` — only QUEUED / RUNNING / COMPLETE — so anything
    presented as "42% done" would be invented. What IS knowable is how long this
    same job took last time, which is recorded on every COMPLETE. With no
    baseline yet, the elapsed time is printed and no percentage is.

    Bounded by `max_wait_minutes`: an unbounded poll on a hung worker is a process
    that never returns and a quota that keeps draining.

    `on_poll(status, elapsed_seconds, baseline_or_None)` is called once per poll, before
    the line is decided, so a caller reporting an OVERALL percentage can move it while this
    blocks. ⚠️ **It is handed the baseline it does not have, too** — `None` — because the
    honest answer for a job that has never completed is that this stage cannot report a
    fraction, and a hook that invented one would put this module's own warning above into
    the very number it warns about.
    """
    api = _api()
    start = time.perf_counter()
    deadline = start + cfg.max_wait_minutes * 60
    baseline = last_duration(cfg)
    tty = sys.stdout.isatty()
    last_state, last_bucket = None, None

    if baseline:
        print(f"baseline: last COMPLETE run took {baseline / 60:.1f} min")
    else:
        print("baseline: none yet — no percentage until this job completes once")

    while True:
        response = _fetch_status(api, cfg, retry_minutes=POLL_RETRY_MINUTES)
        name = _status_name(response.status)
        elapsed = time.perf_counter() - start
        # ⚠️ The hook MOVES A NUMBER; it does not print. The poll's own line is already the
        # one the reader sees (through `Stages.capture`), and a hook that printed too would
        # double every line of a 12-hour wait.
        if on_poll is not None:
            on_poll(name, elapsed, baseline)
        pct = f"{elapsed / baseline:>4.0%} of last" if baseline else "  no baseline"
        line = f"[{elapsed / 60:5.1f} min] {name:<9} {pct}"

        if name in TERMINAL:
            print(f"\r{line}{' ' * 12}" if tty else line, flush=True)
            if name == "COMPLETE":
                _record_duration(cfg, elapsed)
            if response.failure_message:
                print(f"failure: {response.failure_message}")
            return name

        # ⚠️ A terminal rewrites in place; a PIPE or a log file must not collect
        # 2,880 lines from a 12-hour poll, so redirected output prints only when
        # the state changes or the percentage crosses a 5-point step.
        bucket = int(elapsed / baseline * 20) if baseline else int(elapsed / 300)
        if tty:
            print(f"\r{line}", end="", flush=True)
        elif name != last_state or bucket != last_bucket:
            print(line, flush=True)
        last_state, last_bucket = name, bucket

        if time.perf_counter() > deadline:
            print()
            raise TimeoutError(
                f"still {name} after {cfg.max_wait_minutes} min. The Kaggle run is "
                f"NOT cancelled — check {cfg.url} or run: "
                f"python -m kgpu status {cfg.name}"
            )
        time.sleep(cfg.poll_seconds)


def _state_file(cfg: JobConfig) -> Path:
    return PKG_ROOT / ".state" / f"{cfg.name}.json"


def last_duration(cfg: JobConfig) -> float | None:
    """Seconds the last COMPLETE run of this job took, if one is on record."""
    path = _state_file(cfg)
    if not path.exists():
        return None
    try:
        return float(json.loads(path.read_text(encoding="utf-8"))["seconds"])
    except (ValueError, KeyError, OSError):
        return None


def _record_duration(cfg: JobConfig, seconds: float) -> None:
    path = _state_file(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"seconds": round(seconds, 1), "at": time.strftime("%Y-%m-%dT%H:%M:%S")}),
        encoding="utf-8",
    )


@contextlib.contextmanager
def _utf8_text_files():
    """Force `open()` to UTF-8 for the duration of a call into the Kaggle client.

    ⚠️ **MEASURED 2026-08-29, AND IT COST A COMPLETED RUN ITS RESULT.** The Kaggle client
    writes the kernel log with a bare `open(outfile, "w")`
    (`kaggle_api_extended.py:kernels_output`), which on Windows resolves to **cp1252** — and
    this repo's own logs carry `⚠️` and Vietnamese account names by design. A VIC run that had
    already COMPLETED on Kaggle died in `pull` with `UnicodeEncodeError: 'charmap' codec`, so
    the kernel's 24 minutes of GPU were spent and the run folder never reached the repo.

    §5 rule 18 is written for our own scripts; this is the same defect in a dependency, where
    the file mode is not ours to pass. `PYTHONUTF8=1` fixes it too but only from the *next*
    process — a running interpreter cannot change `locale.getpreferredencoding`, and this has
    to work when `kgpu` is called from a notebook that is already up.

    ⚠️ **SCOPED TO ONE CALL, and deliberately.** A process-wide `open` is not something to
    rewrite for the life of a session; every write outside this block keeps whatever encoding
    its caller chose.
    """
    import builtins

    original = builtins.open

    def utf8_open(file, mode="r", *args, **kwargs):
        if "b" not in mode and kwargs.get("encoding") is None and len(args) < 2:
            kwargs["encoding"] = "utf-8"
            kwargs.setdefault("errors", "replace")
        return original(file, mode, *args, **kwargs)

    builtins.open = utf8_open
    try:
        yield
    finally:
        builtins.open = original


def download(cfg: JobConfig) -> List[str]:
    """Raw download of /kaggle/working into `results/`. ⚠️ Wipes it first."""
    api = _api()

    if RESULTS_DIR.exists():
        shutil.rmtree(RESULTS_DIR)
    RESULTS_DIR.mkdir(parents=True)

    with _utf8_text_files():
        files, _ = api.kernels_output(cfg.id, str(RESULTS_DIR), force=True, quiet=True)

    if not files:
        print("no output files (the run may have written nothing to /kaggle/working)")
        return []

    print(f"downloaded {len(files)} file(s) to {RESULTS_DIR}:")
    for index, path in enumerate(sorted(files)[:20], start=1):
        size = Path(path).stat().st_size / 1024
        print(
            f"  [{index:>2}/{len(files)} {index / len(files):>4.0%}] "
            f"{str(Path(path).relative_to(RESULTS_DIR)):<52} {size:>9.1f} KiB"
        )
    if len(files) > 20:
        print(f"  … and {len(files) - 20} more")
    return files


def merge_results(cfg: JobConfig,
                  force: bool = False) -> Tuple[List[Path], List[Path]]:
    """`(copied, already_present)` — the downloaded run folders, by what happened to each.

    A run folder is identified the way the rest of the repo identifies one: a
    directory holding `metadata.json`. Existing folders are never overwritten —
    `--force` is the only way, and it is for a re-download of the same run, not
    for a second run that happened to pick the same id.

    ⚠️ **THE SECOND LIST IS NOT BOOKKEEPING — IT IS THE DIFFERENCE BETWEEN "NOTHING TO DO" AND
    "NOTHING WAS DONE".** `pull` merges into `raw_data/` only what THIS pull copied, so a
    folder already in the report root took the statement CSVs out of the round trip entirely,
    and until 2026-08-30 the only trace was one line about the run FOLDER. Returning it lets
    the caller say which of the two happened.
    """
    if not cfg.results_into:
        return [], []

    source_root = RESULTS_DIR / cfg.results_into
    target_root = REPO_ROOT / cfg.results_into
    if not source_root.is_dir():
        print(
            f"⚠️ nothing under results/{cfg.results_into} — the run wrote no report "
            f"folder there. Check the notebook's REPORT_ROOT."
        )
        return [], []

    target_root.mkdir(parents=True, exist_ok=True)
    merged: List[Path] = []
    present: List[Path] = []
    for run in sorted(p for p in source_root.iterdir() if p.is_dir()):
        if not (run / "metadata.json").exists():
            continue
        target = target_root / run.name
        if target.exists() and not force:
            print(f"  ⚠️ {run.name} already exists in {cfg.results_into} — skipped")
            present.append(target)
            continue
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(run, target)
        merged.append(target)
        print(f"  merged {run.name} -> {cfg.results_into}/")

    if merged:
        print(
            f"\n{len(merged)} run folder(s) are now in the repo's report root — "
            f"`final_features` will see them like any local run."
        )
        # ⚠️ **A MERGED RUN WITH NO SHORTLIST IS INVISIBLE, AND NOTHING USED TO SAY SO.**
        # `final_features.plan_from_reports` skips a folder carrying no `outstanding.csv`
        # without a word; measured 2026-08-15, the two newest runs — both produced through
        # this command, back when the notebook wrote a report and stopped — sat in exactly
        # that state while `final_features` planned 19 runs and reported no error
        # (`feature_selection/contract.py` §2). The notebook writes the shortlist itself
        # since 2026-08-16, so this is now a CHECK of what came home rather than a
        # reminder to go and do it by hand.
        # ⚠️ **ONLY A SELECTION RUN OWES A SHORTLIST.** A documents (PDF-parse) run carries
        # `summary.csv` and a JSON per filing, feeds no `final_features`, and would trip this
        # warning on every pull — a warning that always fires is a warning nobody reads, which
        # is the same argument `_payload_hash` makes about a staleness check that always fires.
        if cfg.data is not None and cfg.data.is_documents:
            return merged, present
        shortlist = _shortlist_filename()
        without = [m.name for m in merged if not (m / shortlist).exists()]
        if without:
            print(
                f"WARNING: {len(without)} merged run(s) carry no {shortlist} and "
                f"final_features cannot see them: {without}\n"
                f"WARNING: run `python -m feature_selection.outstanding` to build it from "
                f"the archived feature_importance.csv, and check the execution log for "
                f"why the notebook's own shortlist cell did not."
            )
    return merged, present


def merge_statements(cfg: JobConfig, folders: List[Path], apply: bool = True,
                     already_present: Optional[List[Path]] = None) -> int:
    """Upsert the pulled run folders' accepted statements into `raw_data/`.

    ⚠️ **THE WORKER CANNOT DO THIS AND NEVER COULD.** A Kaggle kernel writes
    `/kaggle/working` and exits; the statement CSVs live on this disk. So "the Kaggle run
    upserts the CSV" is necessarily "the pull does", and doing it here is what makes a
    pre-merge backup and a printed diff possible at all.

    ⚠️ **IT IS NOT A BLANKET WRITE.** `pdf_ocr_merge` refuses a cumulative income statement, a
    statement whose `sane` band was empty, and any figure that DIFFERS from a good `pdf` row
    already on disk. Each refusal is a measurement, not caution — the first run this was built
    for had a worker ACCEPT an income statement the full local run REFUSED, because
    `seed_history` reconstructs the magnitude band from disk while a full run accumulates it
    in the run. Same machine result, different populations, different verdict.

    Returns the number of statements actually written, so a caller can tell a merge that
    refused everything from a merge that never ran.

    ⚠️ **AN EMPTY `folders` USED TO RETURN IN SILENCE, AND THAT IS HOW A GREEN ROUND TRIP CAME
    TO WRITE NO CSV WITH NOTHING SAYING SO.** `pull` passes only what it just copied, so a run
    folder already in the report root — a re-pull, or a second push of the same job — leaves
    this list empty. The run folder is on disk, the parse is fine, and the statement CSVs were
    never opened. It now says which case it is and names the command that finishes the job.
    """
    import sys

    if not folders:
        if already_present:
            print()
            print("WARNING: NOTHING WAS MERGED INTO raw_data/.../statements/.")
            print(f"  {len(already_present)} run folder(s) were already in "
                  f"{cfg.results_into}, so this pull copied none — and only what a "
                  f"pull copies is offered to the upsert.")
            print("  The parse is on disk and the statement CSVs were NOT opened. "
                  "To finish the job:")
            print(f"      python -m kgpu merge {cfg.name}")
            print("  (or re-run with force=True to re-copy the folder and merge it "
                  "here.)")
        else:
            print()
            print("WARNING: no run folder came home, so no statement CSV was written.")
        return 0

    src = str(REPO_ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    from web_scraper import pdf_ocr_job, pdf_ocr_merge

    pdf_ocr_job.use_data_root()
    # ⚠️ **`OVERWRITE` REACHES THE MERGE, NOT ONLY THE PARSE.** Without it a re-parse asked for
    # explicitly would come home and be refused by `force_differs` — the run would do the work
    # and the disk would keep the old figure, which is the worst of both answers.
    overwrite = bool((cfg.parameters or {}).get("OVERWRITE", False))
    print("\nmerging into raw_data/.../statements/"
          + ("   (overwrite=True — a `pdf` row that DIFFERS is replaced)" if overwrite else "")
          + ("\n  force_empty_band=True: a ticker with no history on disk is bootstrapped;"
             "\n  those figures passed NO magnitude guard (`BND-1`), so screen them"
             " before quoting any." if cfg.merge_force_empty_band else ""))
    written = 0
    for folder in folders:
        if not (folder / "documents").is_dir():
            continue                     # not a pdf_ocr run folder
        report = pdf_ocr_merge.merge_run(folder, apply=apply, force_differs=overwrite,
                                         force_empty_band=cfg.merge_force_empty_band)
        # ⚠️ The worker wrote `metadata.json` and could not have known this; without writing
        # the outcome back, the artefact says `merged_into_csv: false` on every Kaggle run
        # forever, whatever the pull did with it.
        if apply:
            pdf_ocr_merge.record_merge(folder, report)
        # ⚠️ **`report.written` IS THE FILE'S TOTAL, NOT THIS MERGE'S.**
        # `_write` returns `{report: rows whose source is pdf}` over the WHOLE csv
        # after the upsert, so summing it reports a ticker's entire history as
        # though this run had produced it. What this run did is its own decisions.
        written += len(report.to_write) if report.applied else 0
    _say_what_landed(written, apply)
    return written


def _say_what_landed(written: int, apply: bool) -> None:
    """One line, at the end, saying whether anything reached `raw_data/`.

    ⚠️ **"THE RUN FINISHED" AND "THE CSV CHANGED" ARE DIFFERENT FACTS AND ONLY THE SECOND WAS
    EVER THE POINT.** A bootstrap ticker whose every statement is refused for an empty band
    prints a full page of `skip` lines and then a green finish; measured on HOSE_BSR and again
    on HOSE_CTG, both 8-hour runs that wrote nothing (`BND-1`).
    """
    print()
    if not apply:
        print("  DRY RUN — no statement CSV was touched.")
    elif written:
        print(f"{written} statement(s) written into raw_data/.../statements/.")
    else:
        print("WARNING: 0 statements were written — every one was REFUSED. "
              "The reasons are the `skip` lines above.")
        print("  The commonest on a ticker with no CSV yet is an EMPTY `sane` band: "
              "pass FORCE_EMPTY_BAND = True in the")
        print("  control notebook (`--force-empty-band` on `kgpu merge`) to bootstrap "
              "it. That lifts ONE guard and no")
        print("  other, so screen the artefact before quoting anything from it "
              "(`BND-1`).")


def merge_latest(cfg: JobConfig, apply: bool = True) -> int:
    """`kgpu merge <job>` — merge the newest run folder already in the repo.

    ⚠️ **THE NEWEST, NOT THE ONE THIS JOB LAST PUSHED**, and the two can differ: a run folder
    is named by the WORKER's clock, and a ticker may have folders from several jobs. It prints
    which one it chose before deciding anything.
    """
    import sys

    if not cfg.data or not cfg.data.is_documents:
        print(f"job {cfg.name!r} ships no documents — nothing to merge into raw_data/")
        return 1
    src = str(REPO_ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    from web_scraper import pdf_ocr_job, pdf_ocr_merge

    pdf_ocr_job.use_data_root()
    spec = cfg.data.documents or {}
    folder = pdf_ocr_merge.latest_run(
        REPO_ROOT / (cfg.results_into or "reports/pdf_ocr"),
        spec.get("exchange", "HOSE"), spec["symbol"])
    if folder is None:
        print(f"no run folder for {spec.get('exchange')}_{spec['symbol']} — pull one first")
        return 1
    print(f"merging {folder.name}")
    report = pdf_ocr_merge.merge_run(
        folder, apply=apply,
        force_differs=bool((cfg.parameters or {}).get("OVERWRITE", False)),
        force_empty_band=cfg.merge_force_empty_band)
    if apply:
        pdf_ocr_merge.record_merge(folder, report)
    _say_what_landed(len(report.to_write) if report.applied else 0, apply)
    return 0


#: The six steps of `run()`, and what share of the round trip each is worth.
#:
#: ⚠️ **THE WEIGHTS ARE NOMINAL — they say which step is the long one, and they are
#: not a measurement of any run.** The same job queues for five minutes or for fifty
#: (`kgpu` §3d: a smoke job took 8m 15s of which 5.2 min was QUEUE), and the payload upload
#: is an 86 MB push over whatever the line is doing today. Presenting them as measured would
#: be §5 rule 2 wearing a progress bar. They exist so a reader can tell a run stuck on the
#: upload from one stuck on the kernel, at a glance.
RUN_STAGES = (
    ("export", "stage payload", 10.0),
    ("upload", "upload dataset", 15.0),
    ("push", "push kernel", 5.0),
    ("wait", "wait kernel", 50.0),
    ("download", "pull results", 10.0),
    ("merge", "merge into repo", 10.0),
)


@contextlib.contextmanager
def _stage(reporter, key: str, detail: str = ""):
    """Run one step of `RUN_STAGES`, with its output re-emitted as progress lines.

    ⚠️ **`reporter=None` IS TODAY'S BEHAVIOUR, UNCHANGED, AND THAT IS DELIBERATE.** The
    percentage was added for the PDF-OCR control notebook (2026-08-30); `python -m kgpu run`
    on a feature-selection job prints exactly what it printed before, because a formatting
    change that reaches a command nobody asked to change is a change nobody consented to.
    """
    if reporter is None:
        yield
        return
    reporter.begin(key, detail)
    with reporter.capture():
        yield
    reporter.end()


def _poll_hook(reporter):
    """Move the overall % through the `wait` stage — only where a baseline exists."""
    if reporter is None:
        return None

    def hook(status: str, elapsed: float, baseline) -> None:
        if baseline:
            # ⚠️ CAPPED BELOW THE CEILING. The stage is over when Kaggle says COMPLETE,
            # not when this job's last run would have finished; a bar parked at the ceiling
            # while the kernel is still running reads as a hang.
            reporter.inside(min(elapsed / baseline, 0.99))

    return hook


def pull(cfg: JobConfig, force: bool = False, progress=None) -> List[str]:
    with _stage(progress, "download", "results/ is wiped first — it is a scratch mirror"):
        files = download(cfg)
    if not files:
        if progress is not None:
            progress.skip("merge", "nothing was downloaded, so there is nothing to merge")
        return files
    with _stage(progress, "merge", f"-> {cfg.results_into or 'results/ only'}"):
        merged, present = merge_results(cfg, force=force)
        # ⚠️ Only what THIS pull merged. A job whose folder already existed was skipped by
        # `merge_results`, and re-merging it into the CSVs would upsert a run the user did not
        # just fetch — the same class of surprise `--force` exists to make explicit.
        # ⚠️ `present` travels with it so that "there was nothing to merge" cannot be printed
        # as silence: those folders ARE the run the caller is waiting on.
        if cfg.merge_statements:
            merge_statements(cfg, merged, already_present=present)
    return files


def logs(cfg: JobConfig) -> None:
    api = _api()
    print(api.kernels_logs(cfg.id))


def run(cfg: JobConfig, refresh_data: bool = False, force: bool = False,
        progress=None) -> int:
    """(export+upload) -> push -> wait -> pull -> merge. Returns a process exit code.

    `progress` is an optional `utils.progress.Stages` built over `RUN_STAGES` — pass
    one and every line of the round trip comes out as
    `xx.x% - step i/6 <label> - <stage> - <detail>`, the same shape
    `web_scraper.pdf_ocr_job.Progress` prints on the machine that does the OCR. Pass
    None (the CLI default) and the output is what it has always been.

    ⚠️ **THE OVERALL % MOVES THROUGH `wait` ONLY WHEN THIS JOB HAS COMPLETED BEFORE.**
    `kernels_status` reports QUEUED / RUNNING / COMPLETE and no fraction, so the only
    honest clock is this job's own last duration — and a PDF-OCR job is named after its
    ticker and quarters, so its FIRST run has none: the number then holds still at the
    stage floor while the detail keeps printing the elapsed minutes. Standing still is
    the correct behaviour there; inventing a curve would not be.
    """
    started = time.perf_counter()

    if refresh_data:
        from . import dataset, export

        with _stage(progress, "export", cfg.name):
            export.export(cfg)
        with _stage(progress, "upload", cfg.data.id if cfg.data else ""):
            dataset.upload(cfg)
    elif progress is not None:
        # ⚠️ A skipped step CLAIMS its weight rather than redistributing it: the plan
        # is the plan, and "we did not have to do that" is progress through it.
        progress.skip("export", "refresh_data=False — the staged payload is reused")
        progress.skip("upload", "refresh_data=False — the dataset on Kaggle is reused")

    with _stage(progress, "push", cfg.id):
        push(cfg)
    with _stage(progress, "wait", cfg.accelerator or "default GPU"):
        final = wait(cfg, on_poll=_poll_hook(progress))

    if final != "COMPLETE":
        # ⚠️ NOT advanced to 100 %: the run did not finish, and a bar that completes
        # on a failure is the "green step that did nothing" this repo keeps measuring.
        if progress is not None:
            progress.note(f"run ended as {final}; fetching logs")
        else:
            print(f"\nrun ended as {final}; fetching logs\n")
        logs(cfg)
        return 1

    pull(cfg, force=force, progress=progress)
    minutes = (time.perf_counter() - started) / 60
    if progress is not None:
        progress.done(f"COMPLETE and pulled — round trip {minutes:.1f} min")
    else:
        print(f"\ndone in {minutes:.1f} min")
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
        # ⚠️ **KAGGLE EXTRACTS EVERY ZIP, NOT JUST `source.zip`** — into a folder named after
        # the archive, and the archive is then gone. This read `path.name == "source.zip"`
        # until documents mode shipped a second one (2026-08-28), which would have left the
        # nested layout rehearsing a shape the worker never sees.
        if path.suffix == ".zip" and extract:
            with zipfile.ZipFile(path) as zf:
                zf.extractall(mount / path.stem)
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
