# src\kaggle_gpu\kgpu\__main__.py
"""CLI: python -m kgpu <command> [job]

    python -m kgpu jobs                     what is configured
    python -m kgpu plan  feature-selection  what a run would do — touches nothing
    python -m kgpu data  feature-selection  DB -> parquet -> private Kaggle dataset
    python -m kgpu run   feature-selection  push, wait, download, merge into reports/

⚠️ **`data` IS A SEPARATE COMMAND ON PURPOSE.** Exporting a wide pool is minutes
of PostgreSQL and tens of MB of upload, and the pools change far less often than
the parameters do. `run` refuses to start when what is staged is not what was
uploaded, so separating them costs nothing and re-uploading `pool__economy_usa`
on every parameter tweak costs a lot.
"""

from __future__ import annotations

import argparse
import sys

from . import runner
from dataclasses import replace

from .config import JobConfig, job_names, load_job, repo_src_on_path

# ⚠️ **`python -m kgpu` RUNS FROM `src/kaggle_gpu/`, so the repo's `src` IS NOT ON THE
# PATH** — hence the insert before the import. The banner is shared code and has to come
# from the one module that defines it: a second copy here would be a second clock, a
# second timezone decision and a second GPU probe, all free to drift from the ones every
# other `python -m <stage>` prints.
repo_src_on_path()

from utils import runtime  # noqa: E402  — needs the path insert above

COMMANDS = {
    "run": "export? -> push -> wait -> download -> merge into the repo (default)",
    "push": "push and start the run, then return immediately",
    "wait": "poll the current run until it finishes",
    "status": "print the current run status once",
    "logs": "print the execution log of the latest session",
    "pull": "download the latest run's outputs and merge the run folders",
    "build": "stage the patched notebook locally without pushing",
    "plan": "print what a run would do; touches nothing",
    "data": "export the pools to parquet and (re)upload the payload dataset",
    "export": "export the pools to parquet only — no upload",
    "rehearse": "run the worker side locally against the staged payload — no quota",
    "merge": "upsert the newest pulled run folder's statements into raw_data/ "
             "(--dry-run to look first)",
    "jobs": "list the configured jobs",
    "quota": "show remaining weekly GPU/TPU hours",
}

# Commands that do not need a job resolved.
GLOBAL = {"quota", "jobs"}


def _jobs() -> int:
    for name in job_names():
        cfg = load_job(name)
        data = cfg.data.id if cfg.data else "—"
        print(f"  {name:<22} {cfg.notebook:<52} data: {data}")
    return 0


def _utf8_stdout() -> None:
    """⚠️ Windows/cp1252: `⚠️` and `—` in this tool's own output raise on a REDIRECTED
    stream (a console handles them; a pipe or `> file` does not). Reconfiguring is
    cheaper than owning a second, ASCII vocabulary for the same messages."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def main(argv: list[str] | None = None) -> int:
    _utf8_stdout()
    parser = argparse.ArgumentParser(
        prog="python -m kgpu",
        description="Run a repo notebook on a Kaggle GPU, with its data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="commands:\n"
        + "\n".join(f"  {name:<8} {help_}" for name, help_ in COMMANDS.items()),
    )
    parser.add_argument("command", nargs="?", default="run", choices=list(COMMANDS))
    parser.add_argument(
        "job",
        nargs="?",
        default=None,
        help="job name from kaggle_config.json (default: the first one)",
    )
    parser.add_argument(
        "--data",
        action="store_true",
        help="run: re-export and re-upload the payload before pushing",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="pull/run: overwrite a run folder that already exists in the repo",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="merge: print every decision and touch nothing. The merge WRITES by default; "
             "what keeps it honest is the three refusals and the pre-merge backup, not a "
             "second command.",
    )
    parser.add_argument(
        "--force-empty-band",
        action="store_true",
        help="merge: write a statement whose `sane` band was EMPTY. That is the only way a "
             "ticker with no statement CSV is ever bootstrapped (BND-1) — and those figures "
             "passed no magnitude guard, so screen the run folder before quoting any.",
    )
    args = parser.parse_args(argv)

    if args.command == "quota":
        return runner.quota()
    if args.command == "jobs":
        return _jobs()

    cfg: JobConfig = load_job(args.job)

    # ⚠️ **`show_gpu=False` — THE GPU THAT MATTERS IS NOT IN THIS BOX.** `runtime.
    # gpu_report()` would answer from the local `nvidia-smi` and print an RTX 3050
    # above a run that executes on a Kaggle T4, which is worse than printing nothing:
    # a T4 run is a different PROCEDURE, not the same one on faster hardware (a
    # different xgboost RNG stream and a different library stack — CLAUDE.md §3d), and
    # the run's OWN banner, from the notebook's `RunTimer`, is in the execution log and
    # in the merged `metadata.json`. What this clock measures is the ROUND TRIP —
    # export, upload, queue, execute, download — which is the number that decides
    # whether `kgpu` is worth using at all, and it is not the runtime of the selection.
    with runtime.RunTimer(
        f"kgpu {args.command}  {cfg.name}", show_gpu=False
    ):
        return _dispatch(args, cfg)


def _dispatch(args, cfg: JobConfig) -> int:
    if args.command == "run":
        return runner.run(cfg, refresh_data=args.data, force=args.force)
    if args.command == "rehearse":
        return runner.rehearse(cfg)
    if args.command == "merge":
        # ⚠️ The flag WIDENS the job's own setting and never narrows it: a job built with
        # `force_empty_band=True` already means it, and a merge that silently dropped that
        # would refuse the very statements the run was launched to obtain.
        if args.force_empty_band:
            cfg = replace(cfg, merge_force_empty_band=True)
        return runner.merge_latest(cfg, apply=not args.dry_run)
    if args.command == "wait":
        return 0 if runner.wait(cfg) == "COMPLETE" else 1
    if args.command == "push":
        runner.push(cfg)
    elif args.command == "status":
        runner.status(cfg)
    elif args.command == "logs":
        runner.logs(cfg)
    elif args.command == "pull":
        runner.pull(cfg, force=args.force)
    elif args.command == "build":
        runner.build(cfg)
    elif args.command == "plan":
        runner.plan(cfg)
    elif args.command == "export":
        from . import export

        export.export(cfg)
    elif args.command == "data":
        from . import dataset, export

        export.export(cfg)
        dataset.upload(cfg)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted (the Kaggle run keeps going; use 'status' to check)")
        sys.exit(130)
    except (RuntimeError, ValueError, FileNotFoundError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
