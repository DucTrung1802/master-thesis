# src\utils\progress.py
"""ONE progress line, and every stage that reports progress prints exactly it.

    ` 33.7% - doc 2/3 HOSE_TCB Q3-2013 - layer 12/47 onnx@300 - page 40/96  ~76 s left`
     ^^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^^^
     overall  task                       sub-task               detail

Added 2026-08-30, on request, because the OCR run printed its progress as THREE nested
percentages on three differently-indented lines — documents, cascade positions, pages —
and a reader had to hold the nesting in their head to answer the one question a progress
readout exists to answer: **how far through the whole thing am I?** The three fractions
are still there, and they are still the honest ones; they moved into the segments, where
each names its own denominator (`doc 2/3`, `layer 12/47`, `page 40/96`), and the number
at the front is the answer.

## ⚠️ EVERY PROGRESS LINE IN THIS REPO IS THIS SHAPE — leading with the OVERALL %

A standing convention since 2026-09-04, and it is one rule: **anything that reports progress
prints `xx.x% - task - sub-task - detail`, formatted HERE, and the number at the front is the
fraction of the WHOLE thing the reader started.** Not of the current file, not of the current
step — a per-step percentage is what the three nested readouts already were.

Two mechanisms exist so that a caller never has to break the rule to compose a plan out of
parts that each already report:

* **`capture(nested=True)`** for output that already leads with a percentage of its own
  denominator — the inner number is dropped and its segments are kept, so ONE line carries
  ONE number (`split_line`).
* **`Stages(..., final=False)`** for a plan whose stages are driven partly by a routine that
  owns only a segment of them, so that routine's `done()` cannot advance the bar to 100 %
  while the session runs on.

⚠️ **A SECOND FORMATTER IS THE FAILURE THIS PREVENTS.** The rule is worth nothing if a caller
builds the line itself: two writers drift, and then a reader parsing the log gets one shape
from one stage and another from the next.

## ⚠️ THE OVERALL % IS A POSITION IN A PLAN. IT IS NOT A FRACTION OF THE TIME LEFT

Nothing in this repo can promise otherwise and the difference is measured, not
theoretical: one filing accepted at layer 1 of 47 costs ~1 min and one that defeats the
whole cascade cost 33 min (CLAUDE.md §6-2-quindecies, §6-2-noviesdecies), so "half the
documents" is not "half the time" and never was. What the number IS good for is the thing
a nested readout could not do at all — telling a run that is 3 % in from one that is 96 %
in, at a glance, in a log that is being tailed.

⚠️ **IT IS MONOTONE BY CONSTRUCTION** (`advance()` takes a `max`). A percentage that goes
backwards is read as a bug in the run rather than as a bug in the reporting, and the
reader then stops trusting the whole line.

## ⚠️ WHY THE CAPTURE EXISTS, AND WHAT IT COSTS

`Stages.capture()` redirects `sys.stdout` and re-emits each finished line as the DETAIL of
the current position. It is how a stage whose work is somebody else's `print()` — the
Kaggle client, `pdf_ocr_merge` — reports in this shape without every one of those call
sites being rewritten. Two properties make it safe rather than clever:

* **it streams.** The shim emits on each newline, so a 30-minute poll is not silent until
  it returns. Buffering the whole stage and printing it at the end would be the exact
  failure §5 rule 20 is written about.
* **it never recurses.** While captured, this module writes to the stdout it saved on the
  way in, so a line emitted from inside the capture cannot be captured again.

⚠️ **What it costs is the ORIGINAL SHAPE of that output.** Indentation is stripped, blank
lines are dropped and a carriage-return-rewritten line becomes a new line — so anything
that PARSES this output must read the last ` - ` segment (`detail_of`) rather than the
start of the line.
"""

from __future__ import annotations

import contextlib
import io
import re
import sys
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple, Union

#: What separates the four segments. One string, because a reader that splits on it and a
#: writer that joins with it are two halves of one contract.
SEPARATOR = " - "

#: ⚠️ ONE DECIMAL, ALWAYS, AND RIGHT-ALIGNED TO 5. `33.7%` says the number moved since the
#: last line where `34%` would have said nothing for a whole document, and a fixed width
#: keeps the task titles in a column when a log is read as a block.
PERCENT_FORMAT = "{:>5.1f}%"


def clamp(fraction: float) -> float:
    """`fraction` into [0, 1]. A percentage outside it is a bug in the caller's arithmetic,
    and printing `-12.0%` would advertise it while printing `0.0%` merely hides it — but a
    progress line is not the place to raise, so it is clamped and the caller keeps running."""
    value = float(fraction)
    return 0.0 if value < 0.0 else 1.0 if value > 1.0 else value


def percent(fraction: float) -> str:
    return PERCENT_FORMAT.format(100.0 * clamp(fraction))


def format_line(fraction: float, task: str = "", sub: str = "", detail: str = "") -> str:
    """`' 33.7% - task - sub - detail'`, with EMPTY SEGMENTS DROPPED.

    ⚠️ Dropped, not printed as blanks: `' 33.7% -  -  - up'` reads as three things that
    failed to be named, and a line whose shape depends on what is known is easier to read
    than one padded to a fixed skeleton.
    """
    parts = [percent(fraction)]
    parts.extend(text for text in (str(task).strip(), str(sub).strip(), str(detail).strip())
                 if text)
    return SEPARATOR.join(parts)


def detail_of(line: str) -> str:
    """The DETAIL segment of a formatted line — what a reader that used to match on the
    start of the line must now match on. Returns the whole line if it is not one of ours."""
    return line.split(SEPARATOR)[-1].strip() if SEPARATOR in line else line.strip()


#: What a percentage this module wrote looks like, and the ONLY way a line is recognised as
#: ours. ⚠️ Anchored and one-decimal, so a caller's own text beginning "50% - done" is NOT
#: mistaken for a formatted line and stripped of a segment it never had.
PERCENT_RE = re.compile(r"^\s*\d{1,3}\.\d%$")


def split_line(text: str) -> Optional[Tuple[str, str]]:
    """`(sub, detail)` of a line THIS MODULE formatted, or `None` for anything else.

    ⚠️ **THIS IS HOW ONE LINE KEEPS ONE NUMBER.** An entry point that already prints this
    shape — `pdf_ocr_job.Progress`, a nested `Stages` — has its own overall %, and its own
    denominator; driven from inside a LARGER plan, printing it verbatim puts two percentages
    on one line, which is the exact nesting confusion this module was written to remove. So
    `capture(nested=True)` splits the inner line here, drops the inner percentage and keeps
    the segments that still mean something.

    ⚠️ **THE INNER TASK IS DROPPED WITH THE PERCENTAGE, AND THAT IS THE TRADE.** It named the
    inner denominator (`doc 2/3`), which is a fact about a plan the reader is no longer being
    shown; the outer task names the one they are. Where that matters — a multi-document run —
    the outer label is what must carry the identity.

    ⚠️ Like `detail_of`, it splits on `SEPARATOR` and takes the LAST segment as the detail, so
    a detail that itself contains `" - "` loses its head. Same property, same reason: the
    writer joined with this string and nothing escapes it.
    """
    parts = str(text).split(SEPARATOR)
    if len(parts) < 2 or not PERCENT_RE.match(parts[0]):
        return None
    return (parts[-2].strip() if len(parts) >= 3 else "", parts[-1].strip())


class Overall:
    """A monotone overall fraction, and the one line that reports it."""

    def __init__(self, *, task: str = "", emit: Optional[Callable[[str], None]] = None):
        self.task = task
        self._fraction = 0.0
        self._emit = emit
        # The stdout to write to. Set only while `capture()` holds the real one, so a line
        # emitted from inside a capture is never fed back into it.
        self._out = None

    # ---- the number -------------------------------------------------------
    @property
    def fraction(self) -> float:
        return self._fraction

    def advance(self, fraction: float) -> float:
        """Move the overall fraction FORWARD. Never backwards — see the module docstring."""
        self._fraction = max(self._fraction, clamp(fraction))
        return self._fraction

    # ---- the line ---------------------------------------------------------
    def write(self, text: str) -> None:
        """The only place this module emits. Everything else formats and calls this."""
        if self._emit is not None:
            self._emit(text)
        else:
            print(text, file=self._out or sys.stdout, flush=True)

    def say(self, sub: str = "", detail: str = "", *, fraction: Optional[float] = None,
            task: Optional[str] = None) -> str:
        if fraction is not None:
            self.advance(fraction)
        line = format_line(self._fraction, self.task if task is None else task, sub, detail)
        self.write(line)
        return line


@dataclass(frozen=True)
class Stage:
    """One named step of a plan, and what share of the whole it is worth."""

    key: str
    title: str
    weight: float = 1.0


StageLike = Union[Stage, Tuple[str, str, float], Tuple[str, str]]


class Stages(Overall):
    """The overall fraction across a FIXED list of weighted stages.

    ⚠️ **THE WEIGHTS ARE NOMINAL AND THE CALLER MUST SAY SO.** They are a statement about
    which step is the long one, not a measurement of any particular run — the same run can
    queue on Kaggle for five minutes or for fifty. A weight that pretended to be measured
    would be this repo's most-repeated defect (§5 rule 2) wearing a progress bar.

    ⚠️ **A STAGE IS NAMED, AND AN UNKNOWN NAME RAISES.** A `begin("dowload")` that silently
    did nothing would leave the percentage frozen while the work ran — which looks exactly
    like a hung stage, and is the one failure a progress readout must not manufacture.
    """

    def __init__(self, stages: Sequence[StageLike], *, label: str = "",
                 emit: Optional[Callable[[str], None]] = None, final: bool = True):
        super().__init__(emit=emit)
        self.stages: List[Stage] = [s if isinstance(s, Stage) else Stage(*s) for s in stages]
        if not self.stages:
            raise ValueError("a plan with no stages has no denominator")
        keys = [s.key for s in self.stages]
        if len(set(keys)) != len(keys):
            raise ValueError(f"stage keys must be unique: {keys}")
        if any(s.weight <= 0 for s in self.stages):
            raise ValueError("a stage weighing 0 can never be reported as started")
        self.label = label
        # ⚠️ **`final=False` IS FOR A PLAN THAT OUTLIVES THE CODE DRIVING PART OF IT**, and it
        # exists because `done()` means two different things to the two callers. A plan whose
        # stages are driven partly by a routine that owns only a SEGMENT of them — the PDF-OCR
        # control notebook embeds `kgpu.runner.RUN_STAGES` as six of its fifteen, and
        # `runner.run` ends by calling `done()` — would otherwise be advanced to 100 % by that
        # routine finishing, and every line after it would read `100.0%` while the session ran
        # on. See `done()`.
        self.final = bool(final)
        self._index = -1

    # ---- geometry ---------------------------------------------------------
    @property
    def total_weight(self) -> float:
        return sum(s.weight for s in self.stages)

    def _floor(self, index: int) -> float:
        return sum(s.weight for s in self.stages[:index]) / self.total_weight

    def _ceiling(self, index: int) -> float:
        return self._floor(index) + self.stages[index].weight / self.total_weight

    def _find(self, key: str) -> int:
        for index, stage in enumerate(self.stages):
            if stage.key == key:
                return index
        raise KeyError(f"no stage named {key!r}; the plan is {[s.key for s in self.stages]}")

    # ---- position ---------------------------------------------------------
    @property
    def stage(self) -> Optional[Stage]:
        return self.stages[self._index] if 0 <= self._index < len(self.stages) else None

    @property
    def task(self) -> str:                                     # type: ignore[override]
        """`step 4/6 HOSE_TCB 2013-Q3` — the position in the plan, and what it is about."""
        position = f"step {max(self._index, 0) + 1}/{len(self.stages)}"
        return f"{position} {self.label}".strip()

    @task.setter
    def task(self, value: str) -> None:
        # `Overall.__init__` assigns `self.task`; the label is what a caller means by it.
        self.label = value or getattr(self, "label", "")

    @property
    def sub(self) -> str:
        stage = self.stage
        return stage.title if stage is not None else ""

    # ---- driving ----------------------------------------------------------
    def begin(self, key: str, detail: str = "") -> None:
        self._index = self._find(key)
        self.advance(self._floor(self._index))
        self.say(self.sub, detail)

    def inside(self, ratio: float, detail: str = "") -> None:
        """Move to `ratio` (0..1) THROUGH the current stage. Silent unless given a detail —
        a poll that both advanced the number and printed a line would print twice, because
        the poll's own output is already coming back through `capture()`."""
        if self.stage is None:
            raise RuntimeError("inside() before begin(): there is no current stage")
        floor, ceiling = self._floor(self._index), self._ceiling(self._index)
        self.advance(floor + clamp(ratio) * (ceiling - floor))
        if detail:
            self.say(self.sub, detail)

    def note(self, detail: str, sub: Optional[str] = None) -> None:
        """A line at the current position — what `capture()` turns other people's output into."""
        if detail.strip():
            self.say(self.sub if sub is None else sub, detail)

    def end(self, detail: str = "") -> None:
        if self.stage is None:
            raise RuntimeError("end() before begin(): there is no current stage")
        self.advance(self._ceiling(self._index))
        if detail:
            self.say(self.sub, detail)

    def skip(self, key: str, detail: str = "not requested") -> None:
        """A stage that will not run. ⚠️ Its weight is CLAIMED, not silently redistributed:
        the plan is what it is, and a skipped step is progress through it."""
        self._index = self._find(key)
        self.advance(self._ceiling(self._index))
        self.say(self.sub, detail)

    def done(self, detail: str = "") -> None:
        """The plan is finished. ⚠️ **UNLESS `final=False`, when it means "the stages I was
        driving are finished" and degrades to `end()`** — the fraction reaches the ceiling of
        the CURRENT stage and no further, so the caller that owns the rest of the plan keeps
        its remaining share. A `done()` on the last stage is `end()` anyway, so a plan driven
        end to end reads the same either way."""
        if not self.final:
            self.end(detail)
            return
        self._index = len(self.stages) - 1
        self.advance(1.0)
        self.say(self.sub, detail)

    # ---- somebody else's print() ------------------------------------------
    @contextlib.contextmanager
    def capture(self, sub: Optional[str] = None, *, nested: bool = False):
        """Re-emit everything written to stdout as the DETAIL of the current position.

        ⚠️ **`nested=True` WHEN THE CAPTURED CODE ALREADY PRINTS THIS SHAPE.** `job.run` and
        any inner `Stages` lead with an overall % of their OWN denominator; re-emitted whole,
        the outer line carries two percentages and the reader is back to holding a nesting in
        their head. With `nested`, such a line is split by `split_line`, its percentage and its
        task are dropped, and its sub-task and detail become this position's — so one line has
        one number. Text that is NOT one of ours passes through unchanged, which is why the
        flag is safe on a stage whose output is mixed.
        """

        def relay(text: str) -> None:
            inner = split_line(text) if nested else None
            if inner is None:
                self.note(text, sub)
            else:
                self.note(inner[1], inner[0] or (self.sub if sub is None else sub))

        real = sys.stdout
        sink = _LineSink(relay)
        self._out = real
        try:
            with contextlib.redirect_stdout(sink):
                yield
        finally:
            sink.flush()
            self._out = None


class _LineSink(io.TextIOBase):
    """A stdout stand-in that hands each COMPLETED line to `emit`.

    ⚠️ **`isatty()` IS FALSE AND THAT IS LOAD-BEARING.** `kgpu.runner.wait` rewrites one
    line in place on a terminal and prints on a state change or a 5-point step otherwise —
    the second is the branch a captured stream must take, or a 12-hour poll becomes 2,880
    formatted lines.
    """

    def __init__(self, emit: Callable[[str], None]):
        self._emit = emit
        self._buffer = ""

    # `TextIOBase.encoding` is None; something that reads it (a Kaggle client, a warning
    # writer) must not be told this stream has no encoding.
    @property
    def encoding(self) -> str:
        return "utf-8"

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def write(self, text: str) -> int:
        # ⚠️ A CARRIAGE RETURN ENDS A LINE HERE. A caller that rewrites in place is
        # producing successive states of one line; captured, each state is its own event,
        # and the alternative is to hold the last one until an unrelated newline arrives.
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._emit(line)
        return len(text)

    def flush(self) -> None:
        if self._buffer.strip():
            self._emit(self._buffer)
        self._buffer = ""
