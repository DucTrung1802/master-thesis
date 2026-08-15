# src\utils\runtime.py
"""ONE clock and ONE GPU probe, shared by every `python -m <stage>` entry point.

Added 2026-08-15. Two things every run in this repo now reports about itself, in the
same words, from the same code:

* **when it ran, in GMT+7** — the market's timezone, and the one the operator reads
  a wall clock in;
* **what hardware it ran on, and how long it took** — because `device` is part of the
  experimental setup here (`feature_selection/gpu.py` §1: the same selection on `cuda`
  and on `cpu` keeps a *different* feature set), and because the runtime is the number
  the cost model in `feature_selection/CONTEXT.md` §15c is fitted on.

## ⚠️ Why GMT+7 is a FIXED OFFSET and not `ZoneInfo("Asia/Ho_Chi_Minh")`

Vietnam has observed UTC+07:00 with no DST since 1975, so the two agree for every
timestamp this repo will ever write — and a fixed offset needs no `tzdata` package,
which a stock Windows CPython does not ship and a Kaggle worker is not guaranteed to
have. `ZoneInfo` would turn a missing wheel into a `ZoneInfoNotFoundError` at the top
of a four-hour run. The offset is the same and it cannot fail.

⚠️ **The database already agrees.** `database_main_v2` runs `TimeZone = Asia/Bangkok`
(measured 2026-08-15), so SQL `now()` returns `+07:00` — while the Python side wrote
`datetime.now(timezone.utc)` into `create_date`. Those two were seven hours apart in
the same row. Everything Python-side now uses this module.

## ⚠️ What this module does NOT touch, on purpose

**The scrapers' epoch parsing.** `simplize_scraper._build_rows` reads its API's epoch
as UTC and `cafef_scraper._net_date` reads CafeF's as UTC+7 — those are claims about
what each *vendor* means by a number, not display choices, and they decide which
calendar day a bar is stored under. Changing one would silently shift bronze rows by a
day. They are left exactly as they are; only "what is today" and "when did this run"
moved.

## Probing the GPU without paying for torch

`gpu_report()` answers from whatever is already cheap:

| order | probe | cost | used when |
|---|---|---|---|
| 1 | `torch` from `sys.modules` | free | a stage that already imported it — and it is the authority, since it is what the run will actually dispatch on |
| 2 | `nvidia-smi` | ~0.1 s | every other stage |
| 3 | nothing | — | reported as absent, never as "cpu" |

⚠️ **It never imports torch itself.** `python -m final_features` is a SQL stage that
has no business paying a ~4 s torch import to print one banner line, and a probe that
costs more than the thing it describes is a probe that gets deleted.
"""

from __future__ import annotations

import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

# The project's wall clock. See the module docstring for why this is not ZoneInfo.
PROJECT_TZ = timezone(timedelta(hours=7), "GMT+7")
TZ_LABEL = "GMT+7"

# `2026-08-15 13:08:27` — what a human reads. `TZ_LABEL` is appended by `stamp()`.
STAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# `20260815-130827` — what a run FOLDER is named. Sorts lexicographically by time,
# which is the only property a folder name has to have.
FOLDER_FORMAT = "%Y%m%d-%H%M%S"


# ------------------------------------------------------------------- the clock


def now() -> datetime:
    """The current time, tz-aware, in GMT+7."""
    return datetime.now(PROJECT_TZ)


def to_project_tz(value: datetime) -> datetime:
    """Any datetime into GMT+7.

    ⚠️ A NAIVE datetime is assumed to be GMT+7 already rather than UTC. Everything
    this repo produces is now stamped by this module, so the naive case is either a
    literal someone typed or a value read back from a `timestamp without time zone`
    column — and both of those are local wall clock.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=PROJECT_TZ)
    return value.astimezone(PROJECT_TZ)


def stamp(fmt: str = STAMP_FORMAT, *, label: bool = True) -> str:
    """`'2026-08-15 13:08:27 GMT+7'` — the form that goes in printed output."""
    text = now().strftime(fmt)
    return f"{text} {TZ_LABEL}" if label else text


def iso(value: Optional[datetime] = None, timespec: str = "seconds") -> str:
    """`'2026-08-15T13:08:27+07:00'` — the form that goes in `metadata.json`.

    The offset is part of the string, so a reader six months from now does not have
    to know which machine wrote it.
    """
    return to_project_tz(value or now()).isoformat(timespec=timespec)


def folder_stamp(value: Optional[datetime] = None) -> str:
    """`'20260815-130827'` — the timestamp segment of a run folder's name."""
    return to_project_tz(value or now()).strftime(FOLDER_FORMAT)


def format_duration(seconds: float) -> str:
    """`'58.5 s'` / `'12m 03s'` / `'2h 14m 03s'` — three scales, one function.

    Seconds are kept to a decimal only below a minute, where a selection pass and a
    kernel launch are the things being compared; above it the tenth is noise.
    """
    seconds = float(max(seconds, 0.0))
    if seconds < 60:
        return f"{seconds:.1f} s"
    minutes, secs = divmod(int(round(seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    return f"{minutes}m {secs:02d}s"


# --------------------------------------------------------------------- the GPU


def _from_loaded_torch() -> Optional[Dict[str, Any]]:
    """The GPU as the already-imported torch sees it, or None if torch is not loaded.

    ⚠️ `sys.modules` is checked rather than `import torch`. See the module docstring:
    this must not be the reason a SQL-only stage pays a torch import.
    """
    torch = sys.modules.get("torch")
    if torch is None:
        return None
    try:
        if not torch.cuda.is_available():
            return {"available": False, "reason": "torch reports no CUDA device",
                    "probe": "torch"}
        free, total = torch.cuda.mem_get_info()
        return {
            "available": True,
            "probe": "torch",
            "name": torch.cuda.get_device_name(0),
            "count": int(torch.cuda.device_count()),
            "vram_total_mb": round(total / 1024**2),
            "vram_free_mb": round(free / 1024**2),
            "cuda": getattr(torch.version, "cuda", None),
            "torch": getattr(torch, "__version__", None),
        }
    except Exception as error:  # noqa: BLE001 — a banner must never fail a run
        return {"available": False, "reason": f"torch probe failed: {error}",
                "probe": "torch"}


def _from_nvidia_smi(timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """The GPU as the driver reports it, without importing anything heavy."""
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None
    try:
        out = subprocess.run(
            [exe, "--query-gpu=name,memory.total,memory.free,driver_version",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=timeout,
        )
    except Exception:  # noqa: BLE001 — see above
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None

    rows = [r.strip() for r in out.stdout.strip().splitlines() if r.strip()]
    fields = [f.strip() for f in rows[0].split(",")]
    if len(fields) < 4:
        return None
    name, total, free, driver = fields[:4]
    report: Dict[str, Any] = {
        "available": True, "probe": "nvidia-smi", "name": name,
        "count": len(rows), "driver": driver,
    }
    for key, raw in (("vram_total_mb", total), ("vram_free_mb", free)):
        try:
            report[key] = int(float(raw))
        except ValueError:
            pass
    return report


def gpu_report() -> Dict[str, Any]:
    """What GPU this machine has, from the cheapest probe that can answer.

    ⚠️ **This is the HARDWARE, not the device a run chose.** A stage that resolves a
    device (`feature_selection.gpu.resolve_device`, `model.common.trainer.resolve_device`)
    must report that separately — a box with a 3050 in it and a run that went to the
    host are two different facts, and conflating them is exactly how 22 archived
    selections came to say `device=cpu` while everyone believed the GPU was busy
    (`feature_selection/gpu.py` §4).
    """
    for probe in (_from_loaded_torch, _from_nvidia_smi):
        report = probe()
        if report is not None and report.get("available"):
            return report
    return {
        "available": False,
        "probe": "none",
        "reason": "no CUDA device found (torch not loaded, nvidia-smi not on PATH)",
    }


def describe_gpu(report: Optional[Dict[str, Any]] = None) -> str:
    """One line: what the card is, how much of it is free, who was asked."""
    report = gpu_report() if report is None else report
    if not report.get("available"):
        return f"none detected — {report.get('reason', 'unknown')}"

    parts = [str(report.get("name", "?"))]
    if report.get("count", 1) > 1:
        parts[0] += f" x{report['count']}"
    total, free = report.get("vram_total_mb"), report.get("vram_free_mb")
    if total is not None:
        parts.append(
            f"{total:,} MiB total"
            + (f", {free:,} MiB free" if free is not None else "")
        )
    stack = [
        f"CUDA {report['cuda']}" if report.get("cuda") else "",
        f"driver {report['driver']}" if report.get("driver") else "",
        f"torch {report['torch']}" if report.get("torch") else "",
    ]
    stack = [s for s in stack if s]
    if stack:
        parts.append(", ".join(stack))
    return "  |  ".join(parts) + f"   [{report.get('probe')}]"


def environment() -> Dict[str, Any]:
    """The `gpu` + `host` block that goes into a run's `metadata.json`."""
    return {
        "gpu": gpu_report(),
        "host": platform.node(),
        "platform": platform.platform(),
        "timezone": TZ_LABEL,
    }


# ------------------------------------------------------------------ the banner

BANNER_RULE = "=" * 78


class RunTimer:
    """Start banner, wall clock, finish banner — for one `python -m <stage>` run.

        with RunTimer("feature_selection.run", device="cuda") as timer:
            ...
        timer.seconds            # the runtime, for whatever wants to record it

    ⚠️ **The finish banner prints on the way out of a FAILURE too**, saying so. A
    stage that dies after 3 hours and prints nothing about it teaches the operator to
    budget from the successful runs only, and CLAUDE.md §5 rule 20 is the same lesson
    one level down: finish the unit, write it out, then describe it.

    ⚠️ `time.perf_counter` for the duration, `datetime` for the two wall-clock
    stamps. They answer different questions: perf_counter is monotonic and survives a
    clock adjustment mid-run, and the stamps are what a human matches against a log.
    """

    def __init__(
        self,
        label: str,
        *,
        device: Optional[str] = None,
        stream=None,
        show_gpu: bool = True,
        rule: bool = True,
    ):
        self.label = label
        self.device = device
        self.stream = stream
        self.show_gpu = show_gpu
        self.rule = rule
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.seconds: float = 0.0
        self._counter: Optional[float] = None

    # ---- lifecycle --------------------------------------------------------
    def start(self) -> "RunTimer":
        self.started_at = now()
        self._counter = time.perf_counter()
        self._emit_start()
        return self

    def stop(self, ok: bool = True, error: Optional[BaseException] = None) -> float:
        self.seconds = self.elapsed
        self.finished_at = now()
        self._emit_finish(ok=ok, error=error)
        return self.seconds

    @property
    def elapsed(self) -> float:
        """Seconds so far — live while running, frozen once `stop()` has been called.

        ⚠️ `summary()` reads THIS, not `self.seconds`, so a caller can record the
        execution block before the run is over. `feature_selection.run` does exactly
        that: the block goes into `metadata.json`, and `write_report` is what writes
        `metadata.json`, so waiting for `stop()` would mean recording a zero.
        """
        if self.finished_at is not None or self._counter is None:
            return self.seconds
        return time.perf_counter() - self._counter

    def __enter__(self) -> "RunTimer":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.stop(ok=exc_type is None, error=exc)
        return False

    # ---- what a caller records --------------------------------------------
    def summary(self) -> Dict[str, Any]:
        """The block a `metadata.json` should carry about this run's execution.

        Safe to call mid-run: `elapsed` is live until `stop()`, and `finished_at` is
        None until then rather than a guess.
        """
        seconds = self.elapsed
        return {
            "started_at": iso(self.started_at) if self.started_at else None,
            "finished_at": iso(self.finished_at) if self.finished_at else None,
            "timezone": TZ_LABEL,
            "runtime_seconds": round(seconds, 2),
            "runtime": format_duration(seconds),
            "device": self.device,
            **environment(),
        }

    def set_device(self, device: Optional[str]) -> None:
        """Record the device once the stage has actually resolved one."""
        self.device = device

    # ---- printing ---------------------------------------------------------
    def _print(self, text: str = "") -> None:
        print(text, file=self.stream or sys.stdout, flush=True)

    def _emit_start(self) -> None:
        if self.rule:
            self._print(BANNER_RULE)
        self._print(f"{self.label}")
        self._print(f"started   {stamp()}")
        if self.device:
            self._print(f"device    {self.device}")
        if self.show_gpu:
            self._print(f"gpu       {describe_gpu()}")
        if self.rule:
            self._print(BANNER_RULE)

    def _emit_finish(self, ok: bool, error: Optional[BaseException]) -> None:
        self._print()
        if self.rule:
            self._print(BANNER_RULE)
        verb = "finished" if ok else "FAILED"
        line = f"{verb:<9} {stamp()}   runtime {format_duration(self.seconds)}"
        if self.seconds >= 60:
            line += f" ({self.seconds:.1f} s)"
        if self.device:
            line += f"   device {self.device}"
        self._print(line)
        if not ok and error is not None:
            # The traceback still propagates; this is the one-line version that sits
            # next to the runtime, so a log skim shows what died and how long it took.
            self._print(f"          {type(error).__name__}: {error}")
        if self.rule:
            self._print(BANNER_RULE)
