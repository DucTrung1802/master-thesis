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

⚠️ **AND SINCE 2026-09-06 `run_batch(merge_each=True)` DOES THAT WRITE AS THE RUN PROCEEDS —
one quarter at a time, the moment its filing has produced ALL THREE statements.** It is the
same `merge_run` call `merge_batch` makes, with `force_differs` still never passed and three
of the four refusals untouched; what changes is WHEN, and that is the whole point:

  * **A run that is interrupted keeps what it has already read.** ⚠️ The measurement is
    HOSE_FPT, 2026-09-04, and its CAUSE was a knob rather than an interrupt: a 185-minute T4
    round trip over 71 filings accepted 128 of 213 statements, the sweep planned **96 WRITEs**
    and **0** of them reached disk, because `MERGE_APPLY` was off and two knobs had to be
    flipped by hand afterwards. What it measures for this flag is the SHAPE both share —
    **the parse is durable in the run folder and the CSVs are not touched until a later step
    that may never run** (`BND-1`). A per-quarter write cannot lose more than the document in
    flight: `_write` renders to a `.tmp` and `os.replace`s it.
  * **It is the ORDER `SPN-1` asks for, for free.** `TickerPlan.quarters` is sorted, and
    `YYYY-QQ` sorts chronologically, so a span operand is parsed AND written before the Q4 it
    exists to unblock is planned. The two-pass sweep reproduces that order deliberately; here
    it falls out of the loop.
  * ⚠️ **THE GATE IS ALL THREE STATEMENTS, NOT "SOMETHING WAS ACCEPTED"** (`complete_periods`).
    A filing that produced two of three is left to the sweep at the end of the run, where a
    person is reading the refusals — the CSVs move together or they do not move.

⚠️ **AND IT LIFTS EXACTLY ONE GUARD, DELIBERATELY, FOR EXACTLY THAT GATE (2026-09-06, by
request).** A complete quarter is written whether or not `sane` had a magnitude band to judge
it by. Refusal 2 — *"the band was EMPTY, so `sane` failed open and this figure passed no
guard"* — is real, but on a ticker with nothing on disk it CLOSES A LOOP: no `pdf` row means no
band, no band means every statement refused, and every statement refused means there is still
no `pdf` row and no CSV. `BND-1`. The three CSVs are created by `FinancialsBuilder._write` the
moment something clears the refusals, so a ticker bootstraps itself now instead of waiting for
a person to notice and set a flag after the parse has already been spent.

⚠️ **WHAT STANDS IN THAT GUARD'S PLACE IS THE GATE AND THE RECORD.** The gate is all three
statements off one filing with no layer raised; the record is `Decision.band == 0`, printed
beside the WRITE and carried into the run folder's `merge` block, and it says in as many words
that **the figure passed no magnitude guard and must be screened by arithmetic before being
quoted.** The other three refusals are untouched: a cumulative income statement whose priors
are unavailable, a figure that DIFFERS from a `pdf` row on disk, and a document any of whose
layers RAISED are all still refused, here as in the sweep.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

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


# ⚠️ **THE REFUSAL TAXONOMY — a SUMMARY of `absent_reasons`, never a replacement for it.**
# The artefact records the exact sentence each layer gave and that stays the evidence; this
# folds the sentences onto the DEFECT they name so 428 of them can be ranked. The bucket is
# chosen by the first substring that matches, so order is significance, and anything unmatched
# comes back as its own text rather than as "other" — a bucket called "other" is where a new
# failure mode goes to be invisible.
#
# ⚠️ **MEASURED 2026-09-10 over every run since 2026-09-07** — 428 absent statements, and the
# number that matters is that **236 of them (55 %) give ONE reason from EVERY layer of the
# cascade**, i.e. one defect fixed wins the whole cell. Ranked: `no such statement on any page`
# 48, `is: operating profit` 44, `bs: assets != liab+equity` 30, `cash: no closing balance` 26,
# `fragmented reading` 26, `is: no profit before tax` 18. That ranking is what a session
# attacking the parse RATE should read before opening a filing.
REFUSAL_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    ("split across two boxes", "fragmented reading"),
    ("no such statement", "no such statement on any page"),
    ("fx not mapped", "cash: fx not mapped"),
    ("no closing cash balance", "cash: no closing balance"),
    ("closing cash balance", "cash: closing != balance sheet"),
    ("!= liabilities + equity", "bs: assets != liab+equity"),
    ("assets != liabilities", "bs: assets != liab+equity"),
    ("section sum does not close", "bs: section sum does not close"),
    ("no total assets", "bs: no total assets"),
    ("no total to balance", "no total to balance against"),
    ("operating profit does not close", "is: operating profit does not close"),
    ("no profit before tax", "is: no profit before tax"),
    ("rows parsed", "too few rows parsed"),
    ("sane:", "sane (magnitude guard)"),
)


def refusal_category(reason: str) -> str:
    """One refusal sentence -> the DEFECT it names. Unmatched text is returned as itself."""
    low = (reason or "").lower()
    for needle, label in REFUSAL_CATEGORIES:
        if needle in low:
            return label
    return (reason or "").strip()[:60] or "(no reason recorded)"


def parse_scorecard(folders: Sequence[os.PathLike | str]) -> Dict[str, object]:
    """What fraction of what these run folders OPENED came back `pdf`.

    ⚠️ **TWO DENOMINATORS AND THEY ANSWER DIFFERENT QUESTIONS.** `cells` is (period, report)
    pairs — the statement-level rate, and the one a ">95 % of PDFs parse" target is naturally
    read against. `documents` is quarters, scored all-or-nothing on all three statements,
    which is the rate that decides whether a quarter can be WRITTEN at all: the per-quarter
    writer's gate is the FILING, not the statement. Measured 2026-09-10 across every run since
    2026-09-07 they are **86.9 %** and **68.9 %** — so quoting one for the other misstates the
    state of the corpus by 18 points in whichever direction flatters it.

    ⚠️ **IT SCORES WHAT WAS OPENED AND NOT WHAT EXISTS.** A quarter no run has ever attempted
    is outside every number here; `fill_grid` and the statement CSVs are where the corpus-wide
    denominator lives. A scorecard over the run folders of a gap-filling batch is a rate over
    the HARD quarters by construction, and reads lower than the ticker's own coverage.
    """
    cells = ok = 0
    per_doc: Dict[Tuple[str, str], int] = {}
    seconds: Dict[Tuple[str, str], float] = {}
    for folder in folders:
        meta = Path(folder) / "metadata.json"
        if not meta.is_file():
            continue
        try:
            m = json.loads(meta.read_text(encoding="utf-8"))
        except Exception:                                 # noqa: BLE001 — an interrupted child
            continue
        run = str(folder)
        for r in m.get("results") or []:
            cells += 1
            good = r.get("status") == "pdf"
            ok += good
            key = (run, r.get("period", ""))
            per_doc[key] = per_doc.get(key, 0) + int(good)
            seconds[key] = max(seconds.get(key, 0.0), float(r.get("seconds") or 0))
    docs = len(per_doc)
    whole = sum(1 for n in per_doc.values() if n == len(fin.REPORTS))
    return {"cells": cells, "cells_pdf": ok,
            "cell_rate": (ok / cells) if cells else None,
            "documents": docs, "documents_complete": whole,
            "document_rate": (whole / docs) if docs else None,
            "minutes": sum(seconds.values()) / 60,
            "minutes_incomplete": sum(s for k, s in seconds.items()
                                      if per_doc[k] < len(fin.REPORTS)) / 60}


def refusal_histogram(folders: Sequence[os.PathLike | str]) -> Tuple[Dict[str, int],
                                                                    Dict[str, int]]:
    """`(every category seen, the categories that were the ONLY one)` over these run folders.

    ⚠️ **THE SECOND DICT IS THE ACTIONABLE ONE.** A statement whose every layer gave the same
    reason has one defect between it and a parse; a statement with four reasons has been
    refused by four different gates and fixing any one of them changes nothing. Over the corpus
    the split is 236 of 428 — so a majority of what is still missing is single-cause.
    """
    every: Dict[str, int] = {}
    sole: Dict[str, int] = {}
    for folder in folders:
        for doc in sorted((Path(folder) / "documents").glob("*.json")):
            try:
                d = json.loads(doc.read_text(encoding="utf-8"))
            except Exception:                             # noqa: BLE001
                continue
            for tried in (d.get("absent_reasons") or {}).values():
                cats = {refusal_category(why) for _layer, why in tried}
                for c in cats:
                    every[c] = every.get(c, 0) + 1
                if len(cats) == 1:
                    c = next(iter(cats))
                    sole[c] = sole.get(c, 0) + 1
    return every, sole


def fill_grid(plan: TickerPlan, *, builder: Optional[fin.FinancialsBuilder] = None,
              allow_parent: bool = True, apply: bool = True,
              log: Optional[Callable[[str], None]] = None) -> Dict[str, dict]:
    """Every quarter from this ticker's FIRST filing to its LAST gets a row in all three
    statement CSVs — `source='missing'` for the ones nothing was written for.

    ⚠️ **THE HOLE IS NOT A DETAIL OF PRESENTATION, IT IS A CLAIM THE FILE DECLINES TO MAKE.**
    A quarter an OCR run attempted and failed on leaves NO row at all: `pdf_ocr_merge` builds
    `_write`'s quarter grid from *exactly the periods being written*, deliberately, so that a
    merge cannot manufacture blanks for quarters it never opened. The cost is that the finished
    file cannot tell "we read this and it is not there" from "we never looked" — measured
    2026-09-10 across the 24 parsed tickers: **1,014 quarter-cells with no row of any kind**,
    SHB's balance sheet holding 34 rows over a 70-quarter span. `build()` has never had this
    hole (`_write`'s own docstring: *"A grid built from the parsed periods hides its own
    failures"*), and this is the OCR path being brought level with it.

    ⚠️ **IT IS A SEPARATE STEP AND NOT A WIDER `attempted`.** Widening the merge's grid would
    put the blank-manufacturing back inside the writer, where it would fire on every partial
    merge and depend on the ordering of a dict for its safety. Here the intent is explicit, the
    range is derived from `documents()` — the same source `plan()` and every count in this
    module read — and `apply=False` prints what it would do.

    ⚠️ **ANCHORED ON THE FILINGS, NEVER ON THE CALENDAR.** The grid runs first-filed to
    last-filed, so it reaches the current quarter exactly when CafeF has published one: BID
    reaches Q2-2026 and BSR stops at Q4-2020, because that is where each ticker's filing chain
    stops. A grid run to `date.today()` would write `missing` rows asserting a company failed to
    file quarters nobody has filed yet.
    """
    say = log or (lambda _s: None)
    builder = builder or fin.FinancialsBuilder(logger=None)
    periods = [t.period for t in job.plan(builder, plan.exchange, plan.symbol,
                                          allow_parent=allow_parent, template=plan.template)]
    if not periods:
        say(f"   {plan.key}: no filing to anchor a grid on — nothing filled")
        return {}
    out = builder.fill_period_grid(plan.exchange, plan.symbol, plan.template, periods,
                                   apply=apply)
    added = sum(v["added"] for v in out.values())
    span = next((v["span"] for v in out.values() if v["span"]), None)
    if added:
        say(f"   {plan.key}: {added} `missing` row(s) added across three statements"
            + (f", grid {span[0]}..{span[1]}" if span else "")
            + ("" if apply else "   [PLAN ONLY]"))
    elif any(v["rows"] for v in out.values()):
        say(f"   {plan.key}: the grid is already contiguous"
            + (f" ({span[0]}..{span[1]})" if span else ""))
    else:
        say(f"   {plan.key}: no statement CSV on disk yet — the first write creates it")
    return out


def complete_periods(folder: os.PathLike | str) -> List[str]:
    """The periods this run folder read in ALL THREE statements, oldest first.

    ⚠️ **THIS IS THE GATE FOR THE IMMEDIATE WRITE, AND IT IS STRICTER THAN THE MERGE'S ON
    PURPOSE.** `merge_batch` asks of each STATEMENT whether it cleared the refusals; this asks
    of the FILING whether it produced the whole quarter. A document that accepted two of
    three is not written as the run proceeds — it is held for the sweep at the end, where a
    person is reading the refusals and can see which statement is missing and why. The three
    CSVs of a quarter move together or they do not move.

    ⚠️ **A DOCUMENT WHOSE LAYERS RAISED IS NOT COMPLETE, WHATEVER ITS `accepted` BLOCK SAYS**
    (`VCR-1`, and `GPU-1` for how it happens): an exception measures the MACHINE, so whatever
    won the cascade won by default and the block looks exactly like a good one — a real layer,
    a real item count. `plan_merge` refuses such a document whole as well; refusing it HERE is
    what puts the reason in the batch log, beside the document it belongs to, rather than
    hours later in a merge nobody was watching.

    ⚠️ **AN INTERRUPTED CHILD IS SILENTLY NOT COMPLETE.** A subprocess killed before it wrote
    its `documents/*.json` leaves a folder with nothing to read, and this returns `[]` rather
    than raising — the batch's job is to go on to the next filing (`wait_for_vram` takes the
    same line), and `merge_batch` will report the folder at the end.
    """
    ready: List[str] = []
    for doc in sorted((Path(folder) / "documents").glob("*.json")):
        try:
            data = json.loads(doc.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if data.get("error") or data.get("engine_errors"):
            continue
        accepted = data.get("accepted") or {}
        if all(name in accepted for name in fin.REPORTS):
            ready.append(data["period"])
    return sorted(ready, key=fin._period_key)


def held_periods(folder: os.PathLike | str) -> Dict[str, str]:
    """`{period: why it was not written as the run proceeded}` — the complement of the above.

    ⚠️ **SAID, NEVER SILENT.** A quarter the immediate write passes over is a quarter whose
    statements are on disk in the run folder and NOT in the CSVs, which is `BND-1`'s shape
    exactly: the work is done, the file does not have it, and a green run says nothing about
    which. One line per held quarter here, and `merge_batch` is what picks them up.
    """
    held: Dict[str, str] = {}
    for doc in sorted((Path(folder) / "documents").glob("*.json")):
        try:
            data = json.loads(doc.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        period = data.get("period") or doc.stem
        if data.get("error"):
            held[period] = f"the run errored: {data['error']}"
            continue
        if data.get("engine_errors"):
            held[period] = (f"{len(data['engine_errors'])} layer(s) RAISED — whatever won this "
                            f"document won BY DEFAULT (`VCR-1`)")
            continue
        accepted = data.get("accepted") or {}
        missing = [name for name in fin.REPORTS if name not in accepted]
        if missing:
            held[period] = (f"{len(accepted)} of {len(fin.REPORTS)} statement(s) accepted — "
                            f"no {', '.join(missing)}")
    return held


def _merge_finished_quarters(folder: Path, plan: TickerPlan, *, apply: bool,
                             reports: Optional[Sequence[str]], backed: set,
                             say: Callable[[str], None]) -> Tuple[int, int]:
    """Upsert the quarters this ONE document finished, right now. `(written, unguarded)`.

    ⚠️ **ONE PERIOD PER CALL, exactly as `merge_batch` does it** — same function, same
    refusals, `force_differs` never passed. The only difference is that disk is read a
    document later than it was in the sweep, which is the direction `SPN-1` wants.

    ⚠️ **REFUSAL 2 IS LIFTED HERE, WITHOUT A KNOB, AND THAT IS A DECISION rather than an
    oversight (2026-09-06, by request).** A quarter whose filing produced ALL THREE statements
    is written whether or not `sane` had a magnitude band to judge it by. The refusal exists
    for a real reason — an empty band means `sane` FAILED OPEN, so the figure passed no guard
    — but refusing on it CLOSES A LOOP rather than opening one: a ticker with nothing on disk
    has no band, so every statement is refused, so there is still nothing on disk and the CSV
    is never created. That is `BND-1`, and the old way out was a person setting
    `FORCE_EMPTY_BAND` after a whole-ticker parse had already been spent for nothing.

    ⚠️ **WHAT STANDS IN THE GUARD'S PLACE IS THE GATE AND THE RECORD, and neither is the same
    thing as the guard.** The gate is `complete_periods` — all three statements off ONE
    filing, no layer having raised — which is a real screen, and is why the lift is scoped to
    it rather than applied to every accepted statement. The record is `Decision.band == 0`,
    carried into the run folder's `merge` block and printed beside every such WRITE: **those
    figures passed no magnitude guard and must be screened by arithmetic before being
    quoted.** The other three refusals are untouched — a cumulative income statement whose
    priors are unavailable, a figure that DIFFERS from a `pdf` row on disk, and a document any
    of whose layers RAISED are all still refused.

    ⚠️ **A MERGE THAT RAISES MUST NOT END THE BATCH.** The parse is the expensive half — 71
    filings were 185 minutes on a T4 (HOSE_FPT, 2026-09-04) — and every accepted statement is
    already durable in the run folder, so a merge that blows up costs one sweep at the end and
    not a re-parse. It is reported at the document it happened on, not swallowed.
    """
    from web_scraper import pdf_ocr_merge

    written = unguarded = 0
    for period in complete_periods(folder):
        try:
            report = pdf_ocr_merge.merge_run(
                folder, apply=apply, periods=[period], reports=reports,
                force_empty_band=True, backup=plan.key not in backed, quiet=True)
        except Exception as exc:                # noqa: BLE001 — see the docstring
            say(f"   ⚠️ the merge of {period} RAISED: {exc}")
            say(f"      nothing was written for it; the sweep at the end of the run is what "
                f"picks it up.")
            continue
        for line in report.lines()[1:]:         # [0] repeats the ticker header
            say("   " + line.strip())
        # ⚠️ RECORDED, NOT RE-PRINTED: `MergeReport.lines()` already prints the backup path
        # (and `— (taken earlier in this run)` once one exists), so saying it again here would
        # put the same path on two lines of the same document's log.
        if getattr(report, "backup", None):
            backed.add(plan.key)
        if apply:
            pdf_ocr_merge.record_merge(folder, report)
        # ⚠️ THE DECISIONS, NOT `report.written` — that field is `{csv: pdf rows on disk AFTER
        # the upsert}`, so summing it reports the ticker's whole history as this run's work
        # (`MRG-1`, and `merge_batch` carries the same warning).
        written += len(report.to_write) if apply else 0
        # ⚠️ COUNTED, not merely printed. "How many rows of this ticker did `sane` never
        # judge?" is the question the lift above creates, and a question nobody totals is a
        # question nobody asks. `band == 0` is the fact; the note beside the WRITE is its
        # sentence, and both reach the run folder through `merge_event`.
        unguarded += sum(1 for d in report.to_write if not d.band) if apply else 0
    for period, why in sorted(held_periods(folder).items(), key=lambda kv: fin._period_key(kv[0])):
        say(f"   {period} HELD — {why}")
    return written, unguarded


def run_batch(plans: Sequence[TickerPlan], *, layers: Optional[Sequence[str]] = None,
              out_root: Optional[os.PathLike | str] = None,
              allow_parent: bool = True, overwrite: bool = True,
              compare: bool = True, notes: str = "",
              vram_floor_mb: int = VRAM_FLOOR_MB, retries: int = RETRIES,
              merge_each: bool = False, merge_apply: bool = True,
              merge_reports: Optional[Sequence[str]] = None, fill_gaps: bool = True,
              log: Optional[Callable[[str], None]] = None, progress=None) -> List[Path]:
    """Parse every document of every plan, ONE PER PROCESS. Returns the run folders, in order.

    ⚠️ **NOTHING IS MERGED HERE UNLESS `merge_each` SAYS SO — IT IS OFF BY DEFAULT.** The
    merge is where a wrong figure would reach disk, and this repo has measured four builds in
    which an automatic per-quarter write silently DOWNGRADED a quarter it had been given only
    for history (CLAUDE.md §6-2-vicies, §6-2-unvicies, §6-2-quatervicies, §6-2-quinvicies). So
    `merge_batch` stays the deliberate sweep, and the default of this driver is still to write
    nothing.

    ⚠️ **WHAT MAKES `merge_each=True` A DIFFERENT PROPOSITION FROM THOSE FOUR IS WHAT IT
    STILL REFUSES.** `force_differs` is not passed here and cannot be from this signature, so
    a figure that disagrees with a `pdf` row on disk is refused exactly as it is in the sweep,
    and so are a cumulative income statement whose priors are unavailable and a document any
    of whose layers RAISED. A backup of the three CSVs is taken by the first call that writes.
    The gate is `complete_periods` — all three statements off ONE filing, no layer having
    raised. What it buys is that an interrupted run keeps what it has already read, which
    `BND-1` and the 185-minute HOSE_FPT round trip of 2026-09-04 are the standing argument for.

    ⚠️ **THE ONE REFUSAL IT DOES LIFT IS THE EMPTY MAGNITUDE BAND, and there is no knob for
    it.** A quarter that clears the gate is written even where `sane` had nothing to judge it
    by — see `_merge_finished_quarters` for why refusing there is a loop and not a guard — and
    every such row is counted, printed and recorded as unguarded. This is why `run_batch` has
    no `force_empty_band` argument: on this path it would have nothing left to decide.

    ⚠️ **`merge_each` DOES NOT REPLACE THE SWEEP.** Quarters it holds back — two statements of
    three, a document whose layers raised — are on disk in the run folder and outside the
    CSVs until a `merge_batch` picks them up. Each held quarter is named in the log as it
    happens (`held_periods`), because a quarter parsed and not written is `BND-1`'s exact
    shape and silence is how it survives.

    ⚠️ **SO THIS FUNCTION NOW RUNS THAT SWEEP ITSELF, BEFORE IT RETURNS** (2026-09-08, by
    request) — `merge_each and merge_apply` only, so the write-nothing default is untouched.
    It was the operator's job and it was being forgotten; the code at the end of the loop
    carries the measurement. A caller may still call `merge_batch` afterwards: everything
    comes back `identical to the row already on disk`, which is a check that costs nothing.

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
    # ⚠️ ONE BACKUP PER TICKER, not one per quarter — `merge_batch` and
    # `pdf_ocr_job._upsert_period` both take this line, and for the same reason: seventy
    # timestamped copies of three CSVs answer "what did this run change?" worse than one.
    backed: set = set()
    merged = unguarded = 0
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
            # ⚠️ **HERE, BEFORE THE NEXT DOCUMENT IS OPENED — that is the whole point of the
            # flag.** `_write` renders to a `.tmp` and `os.replace`s it, so an interrupt can
            # lose the quarter in flight and never one already on disk; and the next filing's
            # `plan_merge` reads a disk this one has already updated, which is the ordering
            # `SPN-1` needs and the reason the sweep has to be two-pass to imitate it.
            if merge_each:
                _wrote, _unguarded = _merge_finished_quarters(
                    folder, plan, apply=merge_apply, reports=merge_reports,
                    backed=backed, say=say)
                merged += _wrote
                unguarded += _unguarded
    if raised:
        # ⚠️ Said ONCE at the end as well as per document: a raised layer is invisible in the
        # verdict table, which reports `pdf` with a real layer and a real item count.
        say("")
        say(f"⚠️ {len(raised)} document(s) had a layer RAISE: {', '.join(raised[:8])}"
            + (" …" if len(raised) > 8 else ""))
        say("   If the cause is `out of memory`, another CUDA process held the card. "
            "Nothing from those documents may be merged.")
    if merge_each:
        # ⚠️ **THE STATEMENTS THAT REACHED `raw_data/`, NOT THE ONES THAT PARSED** — the two
        # came apart on a run that finished green and created no CSV at all (`BND-1`), and
        # only the second number was ever the point. `0` here on a run that accepted plenty
        # means every write was refused: the sweep prints the reasons, most common first.
        say("")
        say(f"-> {merged} statement(s) reached the CSVs as the run proceeded"
            + ("" if merge_apply else "   (nothing was written — merge_apply=False)"))
        if unguarded:
            # ⚠️ SAID AGAIN AT THE END, because it is the one thing about such a run a reader
            # must carry away: those rows are on disk and `sane` never judged them.
            say(f"⚠️ {unguarded} of them passed NO MAGNITUDE GUARD — this ticker had no `pdf` "
                f"history for `seed_history` to rebuild a band from, so `sane` failed open "
                f"(`BND-1`).")
            say("   Screen those figures by arithmetic — two statements agreeing on one "
                "figure, a printed subtotal closing — before quoting any of them.")
    if merge_each and merge_apply and folders:
        # ⚠️ **THE SWEEP RUNS HERE, AT THE END OF THE RUN, AND IT IS NOT AN EXTRA STEP THE
        # OPERATOR MAY FORGET** (added 2026-09-08, by request). `merge_each` writes a quarter
        # the moment its filing has produced all three statements and HOLDS every other one —
        # a filing that produced two of three, a quarter whose span operand landed later —
        # and those held quarters were reaching the CSVs only if somebody afterwards ran §9
        # of the control notebook by hand. That is `BND-1` wearing its third face: the parse
        # is durable in the run folder, the CSV was never opened, and a green run says nothing
        # about which. HOSE_MBB, 2026-09-08: a 158-minute T4 round trip accepted 176 of 186
        # cells and wrote 0, because the process holding the notebook died after the pull.
        # ⚠️ It is the SAME call §9 makes and it changes no verdict: `force_differs` is not
        # passed, so what `merge_each` already wrote comes back `identical to the row already
        # on disk` — a re-plan against disk, which is a CHECK — and what it held is what this
        # writes. `force_empty_band` is not passed either: `merge_batch` lifts the band only
        # for a quarter whose filing produced all three statements, which is the gate
        # `merge_each` itself applied, so the two writers cannot disagree about one quarter.
        say("")
        say("sweep — the quarters `merge_each` HELD, one period at a time, oldest first")
        _tally = merge_batch(folders, apply=True, log=say)
        if _tally["skipped"]:
            say(f"⚠️ {_tally['skipped']} statement(s) are still REFUSED and stay outside the "
                f"CSVs — the reasons are the `skip` lines above, and each is a judgement "
                f"about THAT filing.")
    # ⚠️ **LAST, AND AFTER THE SWEEP, BECAUSE A QUARTER THE SWEEP WROTE MUST NOT BE FILLED IN
    # AS `missing` FIRST AND THEN OVERWRITTEN.** The fill only ever ADDS a row for a period the
    # file does not carry, so running it before the writes would still be correct — but it
    # would leave the log reading `+1 missing` for a quarter that landed thirty seconds later,
    # which is a readout that has to be un-learned. It runs whenever this driver is allowed to
    # write at all (`merge_apply`), including on a run that wrote nothing: a run whose every
    # statement was refused is EXACTLY the run whose CSV most needs to say the quarters were
    # looked at (`BND-1`).
    if fill_gaps and merge_apply:
        say("")
        say("grid — every quarter from the first filing to the last, `missing` where nothing "
            "was written")
        for plan in plans:
            fill_grid(plan, allow_parent=allow_parent, apply=True, log=say)
    return folders


def _screen_folders(folders: Sequence[os.PathLike | str],
                    log: Callable[[str], None]) -> Dict[tuple, list]:
    """`{(ticker, period, report): [why]}` — the arithmetic screens, per ticker.

    ⚠️ **PER TICKER, BECAUSE `screen_run` KEYS ON `(period, report)` AND NOTHING ELSE.** Its
    continuity check is a per-quarter rate over total assets, which is only meaningful within
    one company; handing it two tickers' folders at once would both collide their keys and
    compare one company's balance sheet against another's.
    """
    from web_scraper import statement_screens as screens

    by_ticker: Dict[str, list] = {}
    for folder in folders:
        meta = Path(folder) / "metadata.json"
        if not meta.is_file():
            continue
        try:
            inp = json.loads(meta.read_text(encoding="utf-8"))["inputs"]
        except Exception:                                 # noqa: BLE001 — an interrupted child
            continue
        by_ticker.setdefault(f"{inp['exchange']}_{inp['symbol']}", []).append(folder)
    out: Dict[tuple, list] = {}
    for ticker, group in sorted(by_ticker.items()):
        try:
            flagged = screens.screen_run(group)
        except Exception as e:                            # noqa: BLE001
            log(f"⚠️ the screens raised on {ticker} ({type(e).__name__}: {e}) — nothing is "
                f"withheld on their account, and that is the honest reading of a check that "
                f"did not run (§5 rule 2)")
            continue
        for (period, report), why in flagged.items():
            out[(ticker, period, report)] = why
    return out


def merge_batch(folders: Sequence[os.PathLike | str], *, apply: bool = False,
                force_empty_band: bool = False, force_differs: bool = False,
                reports: Optional[Sequence[str]] = None, screen: bool = True,
                log: Optional[Callable[[str], None]] = None) -> Dict[str, int]:
    """Upsert the batch's run folders — ONE PERIOD PER CALL, OLDEST FIRST, UNFORCED.

    ⚠️ **THE ORDER IS THE WHOLE POINT** and it is per TICKER: `merge_run` plans against disk and
    writes afterwards, so a span recorded for Q3 reaches Q4's planner only in the following call.
    A batch that re-parsed a span operand and the Q4 it unblocks gets both only in this order.

    ⚠️ **AFTER A `merge_each=True` RUN THIS IS THE SWEEP, AND IT IS STILL WORTH RUNNING.** What
    the run already wrote comes back `identical to the row already on disk` — a re-plan against
    disk, which is a check and not a second write — and what it HELD BACK is what this picks
    up: a filing that produced two statements of three, a document whose layers raised and was
    re-run since, a quarter whose span operand only landed later in the run. A quarter parsed
    and never merged is `BND-1`, so the sweep is the thing that closes it.

    ⚠️ **ONE BACKUP PER TICKER**, taken by the first call that actually writes — seventy
    timestamped copies of three CSVs answer "what did this change?" worse than one.

    ⚠️ **`force_differs` DEFAULTS OFF AND EVERY AUTOMATIC CALLER LEAVES IT OFF** (added
    2026-09-08, when `kgpu.runner.merge_statements` was rewritten onto this function). It
    exists so the one deliberate caller — `kgpu merge --overwrite`, an operator who has read
    the DIFFERS report and decided against the FILING — has a way past it. ⚠️ **It is NOT
    wired to a run's `OVERWRITE`**: that flag says which quarters to PARSE, and replacing a
    good `pdf` row already on disk is a different judgement with its own scoped tool
    (`REPAIR`). The control notebook says so in as many words, and the pull used to disagree.
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
    # ⚠️ **THE SCREENS RUN WHERE THE GUARD IS LIFTED, AND THAT IS THE WHOLE OF THEIR SCOPE.**
    # A quarter whose filing produced all three statements is written band or no band — refusing
    # it is `BND-1`'s loop rather than a guard — and `force_empty_band` lifts the rest. Where
    # the band is lifted `sane` judged NOTHING, and the guide has said since 2026-09-04 that
    # *"the arithmetic screens are what replaces it"*. Until now that was advice: a person had
    # to call `screens.screen_run` and hold the flagged pairs out by hand, and the 147 cells
    # measured 2026-09-10 sitting parsed-and-unwritten across six tickers are what advice
    # nobody executes looks like.
    # ⚠️ **THEY DO NOT OVERRULE `sane`, AND THEY ARE NOT CONSULTED WHERE IT SPOKE.** A
    # statement judged against a real band keeps that verdict however the identities read —
    # they check different things (`sane` compares magnitudes against accepted quarters, these
    # are the filing's own arithmetic) and a screen that could veto a guarded row would be a
    # second gate nobody measured. Flagged-and-guarded rows are still PRINTED.
    # ⚠️ **AND A FLAG IS NOT A VERDICT ON THE FIGURE.** `screen_run`'s continuity check is a
    # per-quarter rate and a batch parses the OUTSTANDING quarters, so an honest 1.79x between
    # two quarters a year apart is flagged (FPT's Q2-2009/Q2-2010). What withholding buys is
    # that an unguarded row reaches disk only when the filing's own identities close — which
    # is strictly more than the nothing that guarded it before.
    flagged = _screen_folders(folders, say) if screen else {}
    if flagged:
        say(f"screens: {len(flagged)} statement(s) fail an identity the filing asserts about "
            f"itself — withheld only where `sane` had no band")
    written = skipped = already = withheld = failed = 0
    backed: set = set()
    # ⚠️ **ONE FOLDER IS READ ONCE.** The one-process path writes every document of a run into
    # a single folder, so asking `complete_periods` per (ticker, period) task would re-parse
    # 70 documents 70 times — each of which carries a `row_dump` and is 100-200 KB
    # (`_documents` records the same trap).
    complete: Dict[Path, List[str]] = {}
    for ticker, _order, period, folder in tasks:
        # ⚠️ **A QUARTER WHOSE FILING PRODUCED ALL THREE STATEMENTS IS WRITTEN WHETHER OR NOT
        # `sane` HAD A BAND — the same rule `run_batch(merge_each=True)` applies, so the two
        # writers cannot disagree about the same quarter (2026-09-06, by request).** Refusing
        # it is `BND-1`'s loop: no `pdf` row, no band, every statement refused, still no `pdf`
        # row. `force_empty_band` still governs everything this gate does NOT cover — a filing
        # that produced two statements of three is the operator's call, and stays one.
        if folder not in complete:
            complete[folder] = complete_periods(folder)
        lift = force_empty_band or period in complete[folder]
        # ⚠️ **WITHHELD THROUGH `merge_run`'s OWN `reports` FILTER, never by editing the
        # artefact** — the run folder is immutable and is the evidence. An EMPTY list would be
        # falsy and `merge_run` reads that as "all three", so a period whose every statement is
        # flagged is skipped outright rather than merged wide open.
        allowed = list(reports or fin.REPORTS)
        if lift and flagged:
            held = [r for r in allowed if (ticker, period, r) in flagged]
            for r in held:
                say(f"  hold   {period:9} {r:18} unguarded and fails an identity: "
                    f"{'; '.join(flagged[(ticker, period, r)])[:90]}")
            withheld += len(held)
            allowed = [r for r in allowed if r not in held]
            if not allowed:
                skipped += len(held)
                continue
        # ⚠️ **AN OS-LEVEL WRITE FAILURE IS A FACT ABOUT THE MACHINE, NOT ABOUT THE FILING, AND
        # IT MUST NOT END THE SWEEP.** Measured 2026-09-10: a corpus-wide sweep died on
        # `PermissionError: [WinError 5]` at `os.replace(bs_HOSE_BID.csv.tmp, ...)` — a Windows
        # lock, an editor or a scanner holding the CSV open for a moment — with ten tickers
        # still unmerged behind it, and it left the orphaned `.tmp` beside the intact original.
        # `_write` renders to that `.tmp` and replaces atomically, so the file on disk is either
        # the old one or the new one and never a half (the run that hit this was verified: 72
        # lines, every figure column identical, the grid still contiguous). What was lost was
        # the REST OF THE BATCH, which is the same shape as `GPU-1` — one document's machine
        # trouble ending work that had nothing to do with it.
        # ⚠️ It is SAID and COUNTED, never swallowed: a ticker skipped for a lock is a ticker
        # whose statements are still in the run folder and not on disk (`BND-1`), and the retry
        # is free — re-running the sweep finds everything already written `identical`.
        try:
            report = pdf_ocr_merge.merge_run(
                folder, apply=apply, periods=[period],
                reports=allowed if (reports or (lift and flagged)) else None,
                force_empty_band=lift,
                force_differs=force_differs,
                backup=ticker not in backed, quiet=True)
        except OSError as e:
            failed += 1
            say(f"⚠️ {ticker} {period}: the WRITE failed — {type(e).__name__}: {e}")
            say("   That is the machine, not the filing (a file lock, a full disk). Nothing "
                "of this period reached disk; re-run the sweep.")
            continue
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
        # ⚠️ **"ALREADY ON DISK" IS NOT A REFUSAL, AND AFTER A `merge_each=True` RUN IT IS
        # MOST OF WHAT THIS PASS SEES.** Counting the two together reported a clean sweep of
        # a fully-written ticker as *"0 written, 12 refused"*, which reads as a failure and
        # is the opposite of what happened. The predicate is `pdf_ocr_merge.IDENTICAL`, the
        # module's own constant, so it cannot drift from the reason it counts.
        _same = sum(1 for d in report.decisions if d.reason == pdf_ocr_merge.IDENTICAL)
        already += _same
        skipped += len(report.decisions) - len(report.to_write) - _same
        if apply:
            pdf_ocr_merge.record_merge(folder, report)
    say("")
    say(f"-> {written} statement(s) written, {already} already on disk unchanged, "
        f"{skipped} refused"
        + (f", {withheld} withheld by the screens" if withheld else "")
        + (f", {failed} could not be WRITTEN" if failed else "")
        + ("" if apply else "   (nothing was written — this was a PLAN)"))
    if withheld:
        say(f"⚠️ the {withheld} withheld passed NO magnitude guard AND fail an identity the "
            f"filing asserts about itself. They are in the run folder, not on disk; read the "
            f"figures against the filing before deciding any of them.")
    if failed:
        say(f"⚠️ {failed} period(s) could not be written at all — the machine refused the file, "
            f"not the merge. Their statements are in the run folder and NOT on disk; re-run "
            f"this sweep once whatever held the file has let go.")
    return {"written": written, "skipped": skipped, "already": already,
            "withheld": withheld, "failed": failed, "passes": len(tasks)}


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
