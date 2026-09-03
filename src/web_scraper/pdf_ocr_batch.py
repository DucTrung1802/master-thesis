"""Drive MANY filings — across many tickers — through the OCR cascade, ONE PER PROCESS.

⚠️ **WHY A SEPARATE DRIVER AND NOT A LONGER `pdf_ocr_job.run()`.** Measured 2026-09-02 on this
machine's RTX 3050: an 18-document CTG run inside ONE process cleared three filings and then
**every `onnx@*` layer raised `CUDA failure 2: out of memory`** — 294 of them. The cascade did
what a cascade does: it went on, the layers behind the raised one re-mapped an EMPTY cached
parse, and the run recorded *"no such statement on any page of this filing"* for statements it
had simply been unable to read. A machine failure written down as a fact about the FILING.

`gpu_lock` stops a SECOND run from doing that to a first. It cannot stop a single long run from
doing it to itself, because the growth is inside the process: the onnxruntime CUDA arena and
torch's caching allocator both grow with the largest page they have seen, and
`torch.cuda.empty_cache()` returns only torch's half. **A fresh process per document is the only
thing that resets both.**

⚠️ **AND IT CHANGES NO SEMANTICS — that is what makes it a legitimate substitute rather than a
different procedure.** Two properties of `pdf_ocr_job` make a document independent of its
neighbours already, and both are load-bearing here:

  * `seed_history` rebuilds `sane`'s magnitude band from the `pdf` rows **on disk** and re-seeds
    it per document. A run does NOT accumulate its own band (that is `build()`, and `BND-1`
    records the difference), so splitting the run cannot change any gate's verdict.
  * `PdfParser._ocr_cache` is keyed on the pdf path and cleared when it changes, so the page
    cache never spans two filings and nothing is lost by ending the process between them.

What IS paid is model load — ~10-20 s per document — and that is the whole cost.

⚠️ **THE MERGE IS TWO-PASS, OLDEST FIRST, AND UNFORCED.** `merge_run` plans a folder against
disk and writes afterwards, so a `months` span recorded for Q3 reaches Q4's planner only in the
NEXT call. `SPN-1` is exactly that dependency, so a batch that re-parses a span operand and the
Q4 it unblocks MUST merge them in separate calls, oldest first. `force_differs` is never passed:
two runs disagreeing is not settled by preferring the newer one.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job

# ⚠️ How much free VRAM one document wants before it is allowed to start. Measured
# 2026-09-02: a single CTG filing through the 53-layer onnx cascade peaked at **2.9-3.2 GiB**
# on a 4 GiB card, and the runs that died had ~0.9 GiB free because three notebook kernels
# were holding CUDA contexts. This is a FLOOR on free memory, not a budget for the run —
# a document that starts with less does not fail cleanly, it falls through to whatever layer
# did not raise (`GPU-1`).
VRAM_FLOOR_MB = 2600

# How long to wait for the card to come free before giving up. A browser tab or a notebook
# kernel releasing its context is a matter of seconds; anything longer is a person's problem
# and should be reported rather than waited on.
VRAM_WAIT_SECONDS = 120

# ⚠️ **A DOCUMENT WHOSE LAYERS RAISED IS RETRIED, SMALLER, RATHER THAN LOST.** A pre-flight VRAM
# check cannot prevent a spike DURING a document — measured 2026-09-02, when a single CTG filing
# started with 3.8 GiB free and still raised on 4 of its 53 layers at 300/400 dpi. The retry
# halves `CAFEF_ONNX_REC_BATCH` each time, which is the one VRAM lever that does not change what
# is read (`onnx_ocr.REC_BATCH` carries the measurement: 64 vs 12 gives the IDENTICAL `rows_sha`
# on all three statements of CTG Q3-2019), and asks torch for expandable segments so the
# allocator stops fragmenting. ✅ On the filing that failed — CTG Q1-2009, 4 of 53 layers raised
# with 3,303 MiB free — `REC_BATCH=12` returned **0 engine errors** and the same accepted
# statement at the same layer. ⚠️ It retries only on an ENGINE ERROR, never on a refusal:
# a refusal is a measurement of the filing and repeating it would return the same answer at the
# same cost.
RETRIES = 2
RETRY_REC_BATCH = (32, 12)


@dataclass
class TickerPlan:
    """One ticker's share of a batch — resolved from disk, before anything is spent."""

    exchange: str
    symbol: str
    template: str
    template_how: str
    quarters: List[str] = field(default_factory=list)      # YYYY-QQ, sorted
    operands: List[str] = field(default_factory=list)      # the subset added by `SPN-1`
    settled: Dict[str, List[str]] = field(default_factory=dict)   # quarter -> reports
    filed: int = 0
    complete: int = 0

    @property
    def key(self) -> str:
        return f"{self.exchange}_{self.symbol}"


def free_vram_mb() -> Optional[int]:
    """Free VRAM in MiB, or `None` when it cannot be measured.

    ⚠️ **`nvidia-smi`, NOT `torch.cuda.mem_get_info()`.** This runs in the PARENT, which does no
    OCR — and touching torch's CUDA API here would create a context in a process that has no use
    for one, taking ~300 MB of the very thing it is trying to protect.

    ⚠️ **`None` is "cannot tell", and the caller must not read it as "plenty".** A machine with
    no `nvidia-smi` (a CPU-only box, a Kaggle worker) proceeds; that is right, because there is
    nothing here to protect it from.
    """
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=15, check=True).stdout
    except Exception:                       # noqa: BLE001 — no GPU, no driver, no answer
        return None
    values = [int(line.strip()) for line in out.splitlines() if line.strip().isdigit()]
    return max(values) if values else None


def wait_for_vram(floor_mb: int = VRAM_FLOOR_MB, timeout: int = VRAM_WAIT_SECONDS,
                  log: Optional[Callable[[str], None]] = None) -> Optional[int]:
    """Block until the card has `floor_mb` free, or give up and say so.

    Returns the free MiB last measured (`None` if unmeasurable). It does NOT raise on timeout:
    a document that starts short of memory is a document whose layers may raise, and
    `pdf_ocr_merge` refuses such a document whole — so the honest behaviour is to go ahead and
    make the shortfall visible in the log and in the artefact, not to end a 20-document batch on
    a browser tab.
    """
    say = log or (lambda _s: None)
    deadline = time.time() + timeout
    free = free_vram_mb()
    if free is None or free >= floor_mb:
        return free
    say(f"⚠️ only {free} MiB of VRAM free, waiting for {floor_mb} MiB "
        f"(another CUDA process is holding it — a notebook kernel, a browser)")
    while time.time() < deadline:
        time.sleep(5)
        free = free_vram_mb()
        if free is None or free >= floor_mb:
            say(f"   {free} MiB free — going ahead")
            return free
    say(f"⚠️ STILL {free} MiB free after {timeout}s — starting anyway. If layers RAISE, that is "
        f"why, and `pdf_ocr_merge` will refuse those documents (`GPU-1`).")
    return free


def plan_batch(tickers: Sequence[str], *, exchange: str = "HOSE",
               reports_root: os.PathLike | str,
               allow_parent: bool = True,
               span_operands: bool = True,
               template: Optional[str] = None,
               builder: Optional[fin.FinancialsBuilder] = None) -> List[TickerPlan]:
    """What each ticker still owes, resolved from the statement CSVs and the PDF index.

    ⚠️ **IT IS NOT A SECOND RULE.** The quarters come from `documents()` through `job.plan()` —
    the same call the run makes — "already done" is `job.parsed_reports()`, which is `pdf` and
    nothing else, and a cell a past run PROVED unproducible is dropped by `settled_absences`.

    ⚠️ **THE SPAN OPERANDS ARE PART OF THE PLAN, NOT AN AFTERTHOUGHT** (`SPN-1`). A Q4 income
    statement is the YEAR and the quarter is `FY - (Q1+Q2+Q3)`; the merge subtracts only a prior
    whose span is a KNOWN three months, and most of the corpus predates that column. So a Q4 can
    parse perfectly and be unwritable because of a blank column in ANOTHER row — and nothing in
    an outstanding list says so. Re-parsing a prior moves no figure.
    """
    builder = builder or fin.FinancialsBuilder(logger=None)
    plans: List[TickerPlan] = []
    for symbol in tickers:
        symbol = symbol.upper()
        tpl, how = ((template, "given") if template
                    else job.resolve_template(builder, symbol))
        filed = job.plan(builder, exchange, symbol, allow_parent=allow_parent, template=tpl)
        settled = job.settled_absences(reports_root, exchange, symbol)
        outstanding, settled_here, complete = [], {}, 0
        for task in filed:
            quarter = job.as_quarter(task.period)
            done = set(job.parsed_reports(builder, task))
            gap = [r for r in job.REPORTS if r not in done]
            if not gap:
                complete += 1
                continue
            known = settled.get(quarter, {})
            if any(r in known for r in gap):
                settled_here[quarter] = [r for r in gap if r in known]
            if any(r not in known for r in gap):
                outstanding.append(quarter)
        operands = (job.span_operands(builder, exchange, symbol, tpl, outstanding)
                    if span_operands and outstanding else [])
        plans.append(TickerPlan(
            exchange=exchange, symbol=symbol, template=tpl, template_how=how,
            quarters=sorted(set(outstanding) | set(operands)), operands=operands,
            settled=settled_here, filed=len(filed), complete=complete))
    return plans


def run_batch(plans: Sequence[TickerPlan], *, layers: Optional[Sequence[str]] = None,
              out_root: Optional[os.PathLike | str] = None,
              allow_parent: bool = True, overwrite: bool = True,
              compare: bool = True, notes: str = "",
              vram_floor_mb: int = VRAM_FLOOR_MB, retries: int = RETRIES,
              log: Optional[Callable[[str], None]] = None, progress=None) -> List[Path]:
    """Parse every document of every plan, ONE PER PROCESS. Returns the run folders, in order.

    ⚠️ **NOTHING IS MERGED HERE.** `merge_batch` is a separate, deliberate act, because the
    merge is where a wrong figure would reach disk — and this repo has measured four builds in
    which an automatic per-quarter write silently downgraded a quarter it had been given only
    for history.

    `progress` is an optional `utils.progress.Stages`, positioned on the stage this batch IS.
    Given one, every line here comes out as `xx.x% - <task> - <sub> - <detail>` and the overall
    fraction moves one document at a time through that stage — which is the only reason a
    caller's bar does not stand still through the longest thing it does. ⚠️ `progress=None`
    (the CLI default) prints exactly what it printed before: a formatting change that reaches a
    command nobody asked to change is a change nobody consented to.

    ⚠️ **DOCUMENTS ARE COUNTED AS EQUAL AND THEY ARE NOT** — one accepted at layer 1 is ~1 min
    and one that defeats the cascade was 33 (§6-2-noviesdecies). The number is a position in
    the plan, the same lower bound every other one in this repo is.

    ⚠️ **A CHILD'S OWN PROGRESS LINES DO NOT COME THROUGH HERE.** Each document is a
    subprocess that INHERITS stdout, so its `xx.x%` lines go to the terminal's file descriptor
    and never through `say` — in a notebook they land in the kernel log, not the cell. What
    this reports is the batch's own position, and the child reports its own in its `run.log`.
    """
    say = log or (progress.note if progress is not None else print)
    out_root = Path(out_root or job.DEFAULT_OUT_ROOT)
    total = sum(len(p.quarters) for p in plans)
    folders: List[Path] = []
    raised: List[str] = []
    done = 0
    for plan in plans:
        for quarter in plan.quarters:
            done += 1
            # ⚠️ The FLOOR of this document, not its ceiling: it has not been read yet, and a
            # bar that credits work before it happens is the one thing a progress readout must
            # not do. `end()` is the caller's, once the batch returns.
            if progress is not None:
                progress.inside((done - 1) / max(1, total))
            wait_for_vram(vram_floor_mb, log=say)
            cmd = [sys.executable, "-m", "web_scraper.pdf_ocr_job",
                   "--exchange", plan.exchange, "--symbol", plan.symbol,
                   "--quarters", quarter, "--template", plan.template,
                   "--out", str(out_root),
                   "--notes", notes or f"{plan.key} {quarter} — batch, one process per document"]
            if overwrite:
                cmd.append("--overwrite")
            if allow_parent:
                cmd.append("--allow-parent")
            if not compare:
                cmd.append("--no-compare")
            if layers:
                cmd += ["--layers", *layers]
            say(f"── {done}/{total}  {plan.key} {quarter} " + "─" * 30)
            folder = None
            for attempt in range(retries + 1):
                env = dict(os.environ)
                if attempt:
                    # ⚠️ SMALLER, AND SAID. The first attempt is the shipped configuration; a
                    # retry is a DIFFERENT one, and a run folder that does not record which
                    # produced it is a run folder that cannot be reproduced.
                    size = RETRY_REC_BATCH[min(attempt, len(RETRY_REC_BATCH)) - 1]
                    env["CAFEF_ONNX_REC_BATCH"] = str(size)
                    env["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
                    say(f"   retry {attempt}/{retries} with CAFEF_ONNX_REC_BATCH={size} "
                        f"(same reading, fewer crops per decode)")
                    wait_for_vram(vram_floor_mb, log=say)
                started = time.time()
                code = subprocess.call(cmd, cwd=str(Path(job.__file__).resolve().parents[1]),
                                       env=env)
                folder = _newest_folder(out_root, plan.exchange, plan.symbol, since=started)
                mins = (time.time() - started) / 60
                if folder is None:
                    say(f"   ⚠️ exit {code} and NO run folder — nothing for {quarter}")
                    break
                errs = _engine_errors(folder)
                say(f"   exit {code}   {mins:.1f} min   {folder.name}"
                    + (f"   ⚠️ {errs} layer(s) RAISED" if errs else ""))
                if not errs:
                    break
            if folder is None:
                continue
            folders.append(folder)
            if progress is not None:
                progress.inside(done / max(1, total))
            if _engine_errors(folder):
                raised.append(f"{plan.key} {quarter}")
                say(f"   ⚠️ STILL raising after {retries} retr(ies) — whatever won this "
                    f"document won BY DEFAULT, and the merge refuses it whole (`GPU-1`)")
    if raised:
        # ⚠️ Said ONCE at the end as well as per document: a raised layer is invisible in the
        # verdict table, which reports `pdf` with a real layer and a real item count.
        say("")
        say(f"⚠️ {len(raised)} document(s) had a layer RAISE: {', '.join(raised[:8])}"
            + (" …" if len(raised) > 8 else ""))
        say("   If the cause is `out of memory`, another CUDA process held the card. "
            "Nothing from those documents may be merged.")
    return folders


def merge_batch(folders: Sequence[os.PathLike | str], *, apply: bool = False,
                force_empty_band: bool = False,
                reports: Optional[Sequence[str]] = None,
                log: Optional[Callable[[str], None]] = None) -> Dict[str, int]:
    """Upsert the batch's run folders — ONE PERIOD PER CALL, OLDEST FIRST, UNFORCED.

    ⚠️ **THE ORDER IS THE WHOLE POINT** and it is per TICKER: `merge_run` plans against disk and
    writes afterwards, so a span recorded for Q3 reaches Q4's planner only in the following call.
    A batch that re-parsed a span operand and the Q4 it unblocks gets both only in this order.

    ⚠️ **ONE BACKUP PER TICKER**, taken by the first call that actually writes — seventy
    timestamped copies of three CSVs answer "what did this change?" worse than one.
    """
    from web_scraper import pdf_ocr_merge

    say = log or print
    tasks = []
    for folder in folders:
        folder = Path(folder)
        meta = folder / "metadata.json"
        if not meta.is_file():
            say(f"skip {folder.name} — no metadata.json (interrupted run)")
            continue
        data = json.loads(meta.read_text(encoding="utf-8"))
        ticker = f"{data['inputs']['exchange']}_{data['inputs']['symbol']}"
        for period in {r["period"] for r in data.get("results", [])}:
            tasks.append((ticker, fin._period_key(period), period, folder))
    tasks.sort(key=lambda t: (t[0], t[1]))

    say(f"{'APPLY' if apply else 'PLAN'} — {len(tasks)} (ticker, period) pass(es), oldest first")
    written = skipped = 0
    backed: set = set()
    for ticker, _order, period, folder in tasks:
        report = pdf_ocr_merge.merge_run(
            folder, apply=apply, periods=[period], reports=reports,
            force_empty_band=force_empty_band, backup=ticker not in backed, quiet=True)
        for line in report.lines()[1:]:
            say("  " + line.strip())
        if getattr(report, "backup", None):
            backed.add(ticker)
            say(f"  backup: {report.backup}")
        # ⚠️ COUNT THE DECISIONS, NOT `report.written` — that field is a
        # `{csv: pdf rows on disk AFTER the upsert}` map, so summing it reports a ticker's
        # whole history as this batch's work. `MRG-1` records the same mistake being made
        # once already.
        written += len(report.to_write)
        skipped += len(report.decisions) - len(report.to_write)
        if apply:
            pdf_ocr_merge.record_merge(folder, report)
    say("")
    say(f"-> {written} statement(s) written, {skipped} refused"
        + ("" if apply else "   (nothing was written — this was a PLAN)"))
    return {"written": written, "skipped": skipped, "passes": len(tasks)}


def _newest_folder(out_root: Path, exchange: str, symbol: str,
                   since: float) -> Optional[Path]:
    """The run folder this child just made — matched on the ticker AND on being new.

    ⚠️ `since` is not decoration: a ticker accumulates one folder per document, so taking the
    newest by name would happily return the PREVIOUS document's folder when a child died before
    creating one, and the batch would then merge a folder it did not produce.
    """
    pattern = f"*__{exchange.lower()}_{symbol.lower()}__pdf_ocr"
    candidates = [f for f in out_root.glob(pattern) if f.stat().st_mtime >= since - 5]
    return max(candidates, key=lambda f: f.name) if candidates else None


def _engine_errors(folder: Path) -> int:
    total = 0
    for doc in (folder / "documents").glob("*.json"):
        try:
            data = json.loads(doc.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        total += len(data.get("engine_errors") or [])
    return total
