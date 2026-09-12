"""Drive a WHOLE UNIVERSE of tickers across every GPU this project can reach, at once.

⚠️ **THE CONTROL NOTEBOOK IS ONE TICKER ON ONE MACHINE, AND THAT IS WHY VN30 TOOK A DAY OF
HAND-DRIVING.** Measured 2026-09-12: one RTX 3050 plus four Kaggle T4 kernels (2 accounts x 2
sessions, a hard API limit) took VN30 **56.4 % -> 74.3 %** — and the fleet was a person cloning
`RUN__pdf_ocr_control.ipynb` per ticker, picking an account by hand, and watching five logs.
Every lane below is one of those clones, run by a subprocess instead of by a person.

⚠️ **WHY SUBPROCESSES AND NOT THREADS — AND IT IS NOT A PERFORMANCE ARGUMENT.**
`kgpu.accounts.activate` copies the chosen token into the process-wide `KAGGLE_API_TOKEN`,
because `kagglesdk` reads that variable and nothing else (`ACC-1`). **Two Kaggle lanes on two
accounts therefore cannot share a process**: the second `activate` silently repoints the first,
and the symptom is a 403 *after* a payload has been uploaded. One process per lane is the only
shape in which two accounts can run at the same time.

⚠️ **TWO LANES MUST NEVER HOLD THE SAME TICKER, AND THE COST IS NOT A RACE BUT A SILENT
OVERWRITE.** A pdf-ocr job's name derives from its symbol and scope, and the payload directory,
the rehearsal directory and the kernel slug all come off that name — *"the second overwrites the
first's payload, and its kernel REPLACES the first"*. `assign` therefore returns a PARTITION and
asserts it is one.

⚠️ **THE LOCAL LANE COUNT DEFAULTS TO 1 AND RAISING IT NEEDS A VRAM MEASUREMENT.** One document
measured **2,030 MB RSS with the card at 2,313 MiB of 4,096** on this machine, so two documents
do not fit — and two that do not fit *do not fail loudly*, they raise layers, and the cascade
then records a machine failure as a fact about the FILING (`GPU-1`). `gpu_lease` is the mutex for
exactly this, and it admits `slots` documents at a time; the honest companion knob is
`CAFEF_ONNX_REC_BATCH`, the one VRAM lever measured not to change what is read. ⚠️ **And the
lease spans a whole document's cascade**, so `slots=1` serialises two local lanes almost
entirely: the 20 idle cores the parse leaves (GPU median **0 %**, 96 % of ONE core of 20) are not
reachable by adding lanes alone.

⚠️ **EVERY LANE ENDS WITH `release_batch`, AND THAT IS NOT TIDYING.** `HLD-1`: a filing that
produced two statements of three keeps the empty-band refusal, so its two good statements sit in
the run folder unwritten and the next run wins them again for nothing. 358 of VN30's 779 open
cells were in that state on 2026-09-12.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .config import REPO_ROOT, repo_src_on_path


def anchor() -> Path:
    """Put the repo's `src` on the path and ANCHOR the CafeF data root. Returns the root.

    ⚠️ **`CWD-2`, AND IT FIRED ON THE FIRST FLEET PLAN EVER RUN.** `cafef_financials`'
    `DEFAULT_DATA_ROOT` is the RELATIVE `raw_data/cafef`, and every `kgpu` command runs from
    `src/kaggle_gpu/` — so the plan asked for `src/kaggle_gpu/raw_data/cafef/pdfs/index/
    HOSE_ACB.csv` and reported *"run CafeFPdfScraper for ACB first"* about a ticker whose index
    has been on disk for weeks. A relative root names as many directories as there are working
    directories, and the same defect once made MSN's whole run merge nothing while saying only
    `exit 0 and NO run folder`.

    ⚠️ **IT IS CALLED BY EVERY ENTRY POINT IN THIS MODULE AND NOT ONCE AT IMPORT.** A lane is a
    subprocess with its own interpreter, and `use_data_root` re-points module globals in the
    process that calls it — so importing this module is not what makes the root right.
    """
    repo_src_on_path()
    from web_scraper import pdf_ocr_job as job

    return job.use_data_root(REPO_ROOT / "raw_data" / "cafef")

# ⚠️ A universe file lives at the REPO ROOT because it is DATA, not documentation, and it is
# **current membership and never point-in-time** — which is fine here: this decides which
# filings to PARSE, and a parse is not a backtest.
UNIVERSE_FILES: Dict[str, str] = {"VN30": "vn30.csv", "VN100": "vn100.csv"}

# The Kaggle API allows 2 concurrent GPU sessions per account, so two accounts are FOUR lanes.
# ⚠️ It buys WALL-CLOCK and not quota: the 30 GPU-h/week is spent twice as fast.
SESSIONS_PER_ACCOUNT = 2


def universe(name: str) -> List[Tuple[str, str]]:
    """`[(ticker, exchange)]` for a named universe, in the file's own order.

    ⚠️ **THE FILE'S ORDER IS LIQUIDITY-DESCENDING AND IS NOT A COLUMN** — `vn30.csv` carries a
    `no` column and no turnover, so re-sorting it loses the only thing that ranking is. The
    fleet does not rely on it (it balances by document count), but a caller reading the list
    should not assume it is alphabetical.
    """
    key = name.strip().upper()
    if key not in UNIVERSE_FILES:
        raise ValueError(f"unknown universe {name!r}; known: {sorted(UNIVERSE_FILES)}")
    path = REPO_ROOT / UNIVERSE_FILES[key]
    if not path.is_file():
        raise FileNotFoundError(f"{path} is missing — the universe files live at the repo root")
    out: List[Tuple[str, str]] = []
    # ⚠️ `utf-8-sig`: these files carry a BOM, and without it the first header reads `﻿no`
    # and every lookup of the first column misses (§5 rule 18's other half).
    with path.open(encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            ticker = (row.get("ticker") or "").strip().upper()
            if ticker:
                out.append((ticker, (row.get("exchange") or "HOSE").strip().upper()))
    return out


@dataclass
class Lane:
    """One GPU's worth of work — a disjoint slice of the universe, and where it runs."""

    name: str                       # "local", "kaggle:lyductrung#1" — the log prefix
    kind: str                       # "local" | "kaggle"
    tickers: List[str] = field(default_factory=list)
    account: Optional[str] = None   # a Kaggle label, i.e. the lower-cased username
    exchange: str = "HOSE"
    documents: int = 0              # the plan's own count, for the balance readout

    @property
    def spec(self) -> str:
        return f"{self.kind}:{self.account or '-'}:{','.join(self.tickers)}"


def winnable(tickers: Sequence[str], *, exchange: str = "HOSE",
             reports_root: Optional[os.PathLike | str] = None,
             skip_exhausted: bool = True,
             log: Optional[Callable[[str], None]] = None) -> List["object"]:
    """The `TickerPlan`s that still have something a run could WIN, richest first.

    ⚠️ **IT IS `plan_batch` AND NOT A SECOND RULE** — same quarters, same `settled_absences`,
    same `ASK-1` skip and same `OPB-1` operand test. What this adds is ordering and the
    dropping of plans with nothing left, because a lane handed an empty plan spends a process
    launch to be told so (`ONLY_MISSING`'s retirement measured 70 of those in one run).
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_batch as batch

    say = log or (lambda _line: None)
    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    builder = fin.FinancialsBuilder(logger=None)
    plans = batch.plan_batch(list(tickers), exchange=exchange, reports_root=root,
                             skip_exhausted_on=skip_exhausted, builder=builder)
    keep = []
    for plan in plans:
        if plan.quarters:
            keep.append(plan)
        else:
            why = []
            if plan.exhausted:
                why.append(f"{len(plan.exhausted)} quarter(s) already met this cascade")
            if plan.blocked:
                why.append(f"{len(plan.blocked)} blocked on a de-cumulation operand")
            if plan.settled:
                why.append(f"{len(plan.settled)} settled")
            say(f"   {plan.symbol:5s} nothing to win"
                + (f" — {'; '.join(why)}" if why else f" — {plan.complete}/{plan.filed} complete"))
    keep.sort(key=lambda p: (-len(p.quarters), p.symbol))
    return keep


def assign(plans: Sequence["object"], *, local_lanes: int = 1, kaggle_accounts: Sequence[str] = (),
           sessions_per_account: int = SESSIONS_PER_ACCOUNT,
           exchange: str = "HOSE") -> List[Lane]:
    """Partition the plans across the lanes — longest-first, and it IS a partition.

    ⚠️ **LONGEST-PROCESSING-TIME-FIRST, BECAUSE THE LANES MUST FINISH TOGETHER.** A 54-document
    ticker beside a 6-document one is nine hours against one, and round-robin over a
    liquidity-ordered list produces exactly that. LPT (put the biggest remaining ticker on the
    emptiest lane) is the standard greedy answer and is within 4/3 of optimal; the cost measure
    is the plan's own document count, which is a COUNT and not a duration — *"a long document is
    a document that is losing"*, and nothing here knows which will lose.

    ⚠️ **A TICKER LANDS ON EXACTLY ONE LANE, ASSERTED AND NOT ASSUMED.** Two lanes sharing a
    ticker share its job name, and therefore its payload directory, its rehearsal directory and
    its kernel slug — the second overwrites the first's payload and its kernel replaces the
    first's.
    """
    lanes: List[Lane] = [Lane(name="local" if local_lanes == 1 else f"local#{i + 1}",
                              kind="local", exchange=exchange)
                         for i in range(max(0, local_lanes))]
    for label in kaggle_accounts:
        for session in range(max(1, sessions_per_account)):
            lanes.append(Lane(name=f"kaggle:{label}#{session + 1}", kind="kaggle",
                              account=label, exchange=exchange))
    if not lanes:
        raise ValueError("no lanes — ask for at least one local lane or one Kaggle account")
    # ⚠️ **THE TIE-BREAK PREFERS THE LOCAL LANE, AND THAT IS ECONOMICS RATHER THAN TASTE.**
    # Local GPU costs nothing and Kaggle's 30 GPU-h/week is metered, does **not pool** between
    # accounts and resets weekly — so an hour left unspent locally is an hour of quota burnt
    # that a longer ticker may need later. Sorting on the name alone put a lone ticker on
    # `kaggle:one#1` purely because it sorts before `local`.
    for plan in plans:
        lane = min(lanes, key=lambda l: (l.documents, 0 if l.kind == "local" else 1, l.name))
        lane.tickers.append(plan.symbol)
        lane.documents += len(plan.quarters)
    seen: Dict[str, str] = {}
    for lane in lanes:
        for ticker in lane.tickers:
            if ticker in seen:
                raise AssertionError(
                    f"{ticker} is on two lanes ({seen[ticker]} and {lane.name}) — they would "
                    f"share one job name, one payload directory and one kernel slug")
            seen[ticker] = lane.name
    return [lane for lane in lanes if lane.tickers]


# ── what the universe looks like, and whether it is ONE BAND ─────────────────

def coverage(names: Sequence[Tuple[str, str]], *,
             reports_root: Optional[os.PathLike | str] = None) -> Dict[str, object]:
    """Measure a universe: cells, quarters, and **whether the parsed quarters are one band.**

    ⚠️ **THREE NUMBERS, AND NONE OF THEM IS THE OTHER** — the same trap `PDF_OCR.md` §4 names
    for one run, one level up:

    | | counts |
    |---|---|
    | `cells` | every (quarter, statement) pair — **the finest and the most forgiving** |
    | `quarters` | quarters whose filing produced ALL THREE, which is what the coverage table calls `ocred` |
    | `holes` | non-solid quarters sitting BETWEEN two solid ones |

    ⚠️ **RELEASING A 2-OF-3 FILING'S TWO GOOD STATEMENTS RAISES `cells` AND NOT `quarters`, AND
    THAT IS NOT A DEFECT IN EITHER.** Measured 2026-09-12: +243 cells bought +29 quarters. A
    reader who quotes one rate for the other is out by 14 points.

    ⚠️ **`holes` IS THE ONE THAT ANSWERS *"is the series usable?"*, AND IT IS NOT `1 - rate`.**
    A ticker can read 90 % of its cells with every gap in one block at the start (a clean
    series that simply begins later) or scattered through the middle (a series nothing can
    difference or compound across). `band` is the longest unbroken run of solid quarters; a
    ticker with `holes == 0` is **one band**, whatever its rate.

    ⚠️ **THE GRID IS THE FILING CHAIN AND NOT THE CALENDAR** — `job.plan`, i.e. `documents()`,
    one filing per quarter. A quarter the company never filed is not a hole and no run can
    change it (§5 rule 24); `fill_grid` writes its `missing` row and `corpus_ceiling` extends
    the top edge, and neither invents a cell this counts.
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_job as job

    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    builder = fin.FinancialsBuilder(logger=None)
    per: Dict[str, Dict[str, object]] = {}
    for symbol, exchange in names:
        template = job.resolve_template(builder, symbol)[0]
        solid: List[bool] = []
        quarters: List[str] = []
        open_cells = 0
        for task in job.plan(builder, exchange, symbol, allow_parent=True, template=template):
            done = set(job.parsed_reports(builder, task))
            gap = [r for r in job.REPORTS if r not in done]
            open_cells += len(gap)
            quarters.append(job.as_quarter(task.period))
            solid.append(not gap)
        per[symbol] = {
            "exchange": exchange, "template": template,
            "filed": len(solid), "complete": sum(solid),
            "cells": 3 * len(solid), "open_cells": open_cells,
            "band": _longest_run(solid), "holes": _holes(solid),
            "first_solid": next((q for q, s in zip(quarters, solid) if s), ""),
            "last_solid": next((q for q, s in zip(reversed(quarters), reversed(solid)) if s), ""),
        }
    return {
        "per_ticker": per,
        "filed": sum(int(d["filed"]) for d in per.values()),
        "complete": sum(int(d["complete"]) for d in per.values()),
        "cells": sum(int(d["cells"]) for d in per.values()),
        "open_cells": sum(int(d["open_cells"]) for d in per.values()),
        "holes": sum(int(d["holes"]) for d in per.values()),
        "one_band": sum(1 for d in per.values() if not d["holes"]),
        "tickers": len(per),
        "root": str(root),
    }


def _longest_run(flags: Sequence[bool]) -> int:
    best = run = 0
    for flag in flags:
        run = run + 1 if flag else 0
        best = max(best, run)
    return best


def _holes(flags: Sequence[bool]) -> int:
    """Non-solid quarters BETWEEN two solid ones — the gaps a difference cannot cross.

    ⚠️ **A GAP AT EITHER END IS NOT A HOLE, AND CONFLATING THEM WOULD MAKE A LATE LISTING LOOK
    LIKE A PARSE FAILURE.** VHM's 2017-Q1/Q2/Q3 predate its listing and BSR stops filing in
    2020; neither is something a run can close, and `GRD-2` is the same distinction at the top
    edge — *"a STALE SCRAPE and a company that stopped filing were one sentence"*.
    """
    if True not in flags:
        return 0
    first = flags.index(True)
    last = len(flags) - 1 - list(reversed(flags)).index(True)
    return sum(1 for flag in flags[first:last + 1] if not flag)


def coverage_line(label: str, snap: Dict[str, object]) -> str:
    cells, open_cells = int(snap["cells"]), int(snap["open_cells"])
    filed, complete = int(snap["filed"]), int(snap["complete"])
    return (f"{label}: cells {cells - open_cells:,}/{cells:,} = "
            f"{100 * (cells - open_cells) / max(cells, 1):5.1f}%   "
            f"quarters {complete:,}/{filed:,} = {100 * complete / max(filed, 1):5.1f}%   "
            f"holes {int(snap['holes'])}   one band {int(snap['one_band'])}/"
            f"{int(snap['tickers'])} ticker(s)")


def coverage_delta(before: Dict[str, object], after: Dict[str, object]) -> str:
    def _d(key: str) -> int:
        return int(after[key]) - int(before[key])

    return (f"delta : cells {-_d('open_cells'):+d}   quarters {_d('complete'):+d}   "
            f"holes {_d('holes'):+d}   one band {_d('one_band'):+d}")


def unasked(names: Sequence[Tuple[str, str]], *,
            reports_root: Optional[os.PathLike | str] = None) -> Dict[str, Dict[str, List[str]]]:
    """`{ticker: {quarter: [report]}}` for open cells **no run has ever opened.**

    ⚠️ **THIS IS THE ONE HONEST "IS MORE GPU WORTH IT" TEST, AND IT MUST BE ASKED BEFORE A
    FLEET IS STARTED.** `exhausted_quarters` may reuse a refusal only when the parser is the
    same file, and for 1,214 of VN30's 1,472 run folders it cannot — they were written at a
    dirty tree — so the gap plan proposed **311 documents** while **414 of the 536 open cells
    had already met the full 115-layer cascade and lost.** This asks the strictly weaker
    question that needs no such assumption, and on VN30 the answer was **1**.
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_batch as batch

    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    builder = fin.FinancialsBuilder(logger=None)
    out: Dict[str, Dict[str, List[str]]] = {}
    for symbol, exchange in names:
        found = batch.unasked_quarters(root, exchange, symbol, builder=builder)
        if found:
            out[symbol] = found
    return out


# ── the ONE block where GPU still buys cells: the unasked alternate (`ALT-2`) ─

def alternate_quarters(names: Sequence[Tuple[str, str]], *,
                       reports_root: Optional[os.PathLike | str] = None,
                       log: Optional[Callable[[str], None]] = None,
                       ) -> Dict[str, Dict[str, object]]:
    """`{ticker: {exchange, template, quarters, cells}}` — open cells whose quarter has an
    alternate filing **on disk that no run has ever asked**.

    ⚠️ **NO QUARTER-LEVEL CENSUS CAN FIND THESE, WHICH IS WHY THE FUNCTION EXISTS.** Every one
    of these quarters HAS been opened — `unasked_quarters` counts it as asked and `ASK-1`'s
    skip drops it — but it was opened on the CHOSEN filing only. `documents()` returns one
    filing per period, and **asking a DIFFERENT filing of the same period is a new question**
    (`ALT-2`). A plan built with `skip_exhausted_on=True` therefore proposes none of this work
    and reports the universe as having nothing left to win.

    ⚠️ **"NEVER ASKED" IS READ OFF THE RUN FOLDER'S LOG AND NOT OFF A FIELD**, because the
    field does not exist: `_alternate_retry` announces itself with `retrying on the …` and
    records the recovery in `origin`, and until 2026-09-12 it could reach neither on a worker.
    So the test is *did any run folder for this ticker log the retry on this period* — which
    over-counts if a run retried one alternate of a period that has two, and that is the
    conservative direction: it can only make this plan SMALLER.

    ⚠️ **AND THE ALTERNATE MUST BE ON DISK HERE.** The index is what CafeF advertises; a
    filing that was never scraped is not a question this machine can ask, and counting it
    would put a document on a lane to be skipped with a warning (`ALT-2`'s other half).
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_job as job

    say = log or (lambda _line: None)
    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    builder = fin.FinancialsBuilder(logger=None)
    out: Dict[str, Dict[str, object]] = {}
    for symbol, exchange in names:
        template = job.resolve_template(builder, symbol)[0]
        tried = _retried_periods(root, exchange, symbol)
        quarters: List[str] = []
        cells = 0
        on_disk = 0
        for task in job.plan(builder, exchange, symbol, allow_parent=True, template=template):
            gap = [r for r in job.REPORTS if r not in set(job.parsed_reports(builder, task))]
            if not gap or not task.index_row:
                continue
            alts = [a for a in builder.alternates(exchange, symbol, task.index_row)
                    if os.path.exists(os.path.join(fin.PDFS_DIR,
                                                   a["path"].replace("/", os.sep)))]
            if not alts:
                continue
            on_disk += len(gap)
            if job.as_quarter(task.period) in tried:
                continue
            quarters.append(job.as_quarter(task.period))
            cells += len(gap)
        if quarters:
            out[symbol] = {"exchange": exchange, "template": template,
                           "quarters": sorted(quarters), "cells": cells,
                           "on_disk_cells": on_disk}
            say(f"   {symbol:5s} {len(quarters):3d} document(s), {cells:3d} cell(s) "
                f"with an alternate NO run has asked")
    return out


def _retried_periods(root: Path, exchange: str, symbol: str) -> set:
    """The periods some run folder of this ticker logged an alternate retry on."""
    import json

    tried = set()
    repo_src_on_path()
    from web_scraper import pdf_ocr_job as job

    for folder in sorted(root.glob(f"*__{exchange.lower()}_{symbol.lower()}__pdf_ocr")):
        for path in sorted((folder / "documents").glob("*.json")):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if doc.get("period") and "retrying on the" in " ".join(doc.get("log") or []):
                tried.add(job.as_quarter(doc["period"]))
    return tried


def alternate_plans(names: Sequence[Tuple[str, str]], *,
                    reports_root: Optional[os.PathLike | str] = None,
                    log: Optional[Callable[[str], None]] = None) -> List["object"]:
    """`alternate_quarters` as `TickerPlan`s, richest first — what a lane consumes.

    ⚠️ **THE PERIODS ARE HANDED OVER WHOLE AND `ASK-1` IS NOT CONSULTED.** That skip is
    correct about the chosen filing and wrong about this work by construction, so the plan is
    built from the census rather than filtered by `plan_batch` — and `run_batch` must be given
    `overwrite=True`'s effect the same way the Kaggle side already does it, i.e. the plan has
    already decided.
    """
    anchor()
    from web_scraper import pdf_ocr_batch as batch

    census = alternate_quarters(names, reports_root=reports_root, log=log)
    plans = []
    for symbol, row in census.items():
        plans.append(batch.TickerPlan(
            exchange=str(row["exchange"]), symbol=symbol, template=str(row["template"]),
            template_how="resolved", quarters=list(row["quarters"])))
    plans.sort(key=lambda p: (-len(p.quarters), p.symbol))
    return plans


def release_and_fill(names: Sequence[Tuple[str, str]], *, apply: bool = True,
                     fill: bool = True,
                     reports_root: Optional[os.PathLike | str] = None,
                     log: Optional[Callable[[str], None]] = None) -> Dict[str, object]:
    """`release_batch` then `fill_grid`, over a whole universe. **No OCR, no GPU, no network.**

    ⚠️ **THE ORDER IS LOAD-BEARING AND IT IS THE ONLY REASON THIS IS ONE FUNCTION.** The
    release WRITES `pdf` rows; `fill_grid` then writes a blank `source='missing'` row for every
    quarter of the chain that still has none, so filling first would leave the grid describing
    a coverage the release then changed. `fill_grid` only ever ADDS — a period already on disk
    keeps its row byte for byte — so running it afterwards cannot undo the release.
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_batch as batch

    say = log or print
    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    builder = fin.FinancialsBuilder(logger=None)
    by_exchange: Dict[str, List[str]] = {}
    for symbol, exchange in names:
        by_exchange.setdefault(exchange, []).append(symbol)
    out: Dict[str, object] = {}
    for exchange, group in sorted(by_exchange.items()):
        out[exchange] = batch.release_batch(group, exchange=exchange, reports_root=root,
                                            apply=apply, screen=True, builder=builder, log=say)
        if not fill:
            continue
        filled = 0
        for plan in batch.plan_batch(group, exchange=exchange, reports_root=root,
                                     span_operands=False, builder=builder):
            # ⚠️ `fill_grid` reports `already contiguous` and is idempotent, so this is safe to
            # re-run; what it can never do is reshape a table or manufacture a figure.
            report = batch.fill_grid(plan, builder=builder, apply=apply, log=None)
            filled += sum(int(v.get("added", 0)) for v in report.values()
                          if isinstance(v, dict))
        say(f"{exchange}: grid fill added {filled} `missing` row(s) "
            + ("" if apply else "(PLAN — nothing was written)"))
        out[f"{exchange}_grid_rows"] = filled
    return out


# ── one lane, in this process ─────────────────────────────────────────────────

def run_local(tickers: Sequence[str], *, exchange: str = "HOSE",
              reports_root: Optional[os.PathLike | str] = None,
              layers: Optional[Sequence[str]] = None,
              engines: Optional[Sequence[str]] = None,
              vram_floor_mb: Optional[int] = None,
              apply: bool = True,
              mode: str = "open",
              log: Optional[Callable[[str], None]] = None) -> int:
    """Parse these tickers HERE — `run_batch`, then the release, then the grid.

    ⚠️ **`mode="alternates"` IS A DIFFERENT QUESTION AND NOT A NARROWER ONE** (`ALT-2`): it
    re-opens periods `ASK-1` has correctly retired, to ask a filing nothing has read. See
    `alternate_plans` for why no quarter-level census can propose that work.

    ⚠️ **`merge_each=True` IS THE INTERRUPTION GUARANTEE AND IS WHY A LANE CAN BE KILLED.**
    Each finished quarter is upserted the moment its filing has produced all three statements,
    so stopping a nine-hour lane at hour six keeps every quarter that finished and loses at most
    the document in flight.
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_batch as batch

    say = log or print
    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    names = [(t, exchange) for t in tickers]
    plans = (alternate_plans(names, reports_root=root, log=say) if mode == "alternates"
             else winnable(tickers, exchange=exchange, reports_root=root, log=say))
    if not plans:
        say(f"nothing to parse on this lane (mode={mode}) — every ticker is done or exhausted")
        return 0
    say(f"{len(plans)} ticker(s), {sum(len(p.quarters) for p in plans)} document(s)")
    kwargs: Dict[str, object] = {}
    if vram_floor_mb is not None:
        kwargs["vram_floor_mb"] = vram_floor_mb
    batch.run_batch(plans, layers=_cascade(layers, engines), out_root=root,
                    merge_each=apply, merge_apply=apply, fill_gaps=apply,
                    log=say, **kwargs)
    # ⚠️ THE SWEEP IS NOT THE SAME WRITE. `merge_each` gates on the FILING (all three
    # statements); this picks up what that held — `HLD-1`.
    batch.release_batch([p.symbol for p in plans], exchange=exchange, reports_root=root,
                        apply=apply, screen=True, builder=fin.FinancialsBuilder(logger=None),
                        log=say)
    return 0


def run_kaggle(tickers: Sequence[str], *, account: str, exchange: str = "HOSE",
               reports_root: Optional[os.PathLike | str] = None,
               layers: Optional[Sequence[str]] = None,
               engines: Optional[Sequence[str]] = None,
               apply: bool = True,
               rehearse: bool = False,
               mode: str = "open",
               log: Optional[Callable[[str], None]] = None) -> int:
    """Ship these tickers to ONE Kaggle session, one ticker at a time, in this process.

    ⚠️ **`mode="alternates"` NEEDS `with_alternates` ON THE PAYLOAD AND IT IS ON BY DEFAULT**
    (`ALT-2`) — without it the worker skips every alternate with a warning, which is exactly
    the silent failure this mode exists to undo.

    ⚠️ **THE ACCOUNT IS FORCED, NEVER CHOSEN.** The fleet has already decided which lane this
    is, so letting `accounts.select` re-rank by quota here would let two lanes converge on one
    account — and the tightest-fit rule exists to answer *"which balance covers this run"*, a
    question the fleet answered when it built the lanes. `force_label` is the operator override
    and the fleet IS the operator.

    ⚠️ **A TICKER THAT FAILS DOES NOT END THE LANE.** A round trip is the expensive half and
    every accepted statement is durable in the run folder, so a 403, a queue timeout or a
    kernel that ends as `ERROR` costs one ticker. The lane's exit code counts the failures.
    """
    anchor()
    from web_scraper import cafef_financials as fin
    from web_scraper import pdf_ocr_batch as batch

    from . import accounts, pdf_ocr, runner

    say = log or print
    root = Path(reports_root) if reports_root else REPO_ROOT / "reports" / "pdf_ocr"
    names = [(t, exchange) for t in tickers]
    plans = (alternate_plans(names, reports_root=root, log=say) if mode == "alternates"
             else winnable(tickers, exchange=exchange, reports_root=root, log=say))
    if not plans:
        say(f"nothing to ship on this lane (mode={mode}) — every ticker is done or exhausted")
        return 0

    statuses = accounts.survey()
    failures = 0
    for index, plan in enumerate(plans, start=1):
        head = f"[{index}/{len(plans)}] {plan.symbol} — {len(plan.quarters)} document(s)"
        say(head)
        try:
            need = accounts.estimate_hours(len(plan.quarters))
            choice = accounts.select(need_hours=need, force_label=account, statuses=statuses)
            for line in choice.lines():
                say("   " + line)
            cfg = pdf_ocr.job(
                plan.symbol, exchange=exchange, quarters=plan.quarters,
                # ⚠️ `overwrite=True` because the PLAN has already chosen: `partition_by_disk`
                # would otherwise drop this plan's span operands, which are by definition
                # quarters already `pdf` in all three. It does NOT reach the merge — the pull
                # never passes `force_differs`.
                allow_parent=True, overwrite=True, template=plan.template,
                layers=_cascade(layers, engines),
                notes=(f"fleet lane {account} — {plan.symbol} gap run, "
                       f"{len(plan.quarters)} of {plan.filed} filed quarters"),
            )
            if rehearse:
                from . import export

                export.export(cfg)
                if runner.rehearse(cfg) != 0:
                    say(f"   ⚠️ the rehearsal FAILED for {plan.symbol} — nothing was pushed")
                    failures += 1
                    continue
            code = runner.run(cfg, refresh_data=True)
            if code != 0:
                say(f"   ⚠️ {plan.symbol} ended non-zero ({code}); the lane goes on")
                failures += 1
        except Exception as exc:                       # noqa: BLE001 — see the docstring
            say(f"   ⚠️ {plan.symbol} RAISED: {type(exc).__name__}: {exc}")
            say("      the lane goes on; that ticker's statements are wherever the trip "
                "reached, and the release below picks up anything that landed")
            failures += 1
    batch.release_batch([p.symbol for p in plans], exchange=exchange, reports_root=root,
                        apply=apply, screen=True, builder=fin.FinancialsBuilder(logger=None),
                        log=say)
    return 1 if failures else 0


def _cascade(layers: Optional[Sequence[str]], engines: Optional[Sequence[str]],
             *, onnx_only: bool = True) -> Optional[List[str]]:
    """An explicit layer list wins; otherwise `pdf_ocr_job.layers_for_engines` decides.

    ⚠️ **THE RULE IS NOT RE-IMPLEMENTED HERE.** It lived only inside the control notebook until
    2026-09-12 and this would have been its second copy — a filter that did not RAISE on an
    unknown engine would silently run the whole cascade for a typo.

    ⚠️ **`onnx_only` DEFAULTS TRUE BECAUSE A FLEET SPANS TWO MACHINES** (`TSS-1`).
    `tesseract@200` is a real layer here and does not exist on a Kaggle worker, so leaving it in
    would have the local lane and the T4 lanes running DIFFERENT cascades — and then
    `exhausted_quarters`, which skips a cell only when a past run brought *at least this*
    cascade to it, can no longer compare the two lanes' verdicts.
    """
    if layers:
        return list(layers)
    repo_src_on_path()
    from web_scraper import pdf_ocr_job as job

    return job.layers_for_engines(list(engines) if engines else None, onnx_only=onnx_only)


# ── the fleet, as processes ───────────────────────────────────────────────────

def _lane_command(lane: Lane, *, apply: bool, rehearse: bool,
                  engines: Sequence[str] = (), layers: Sequence[str] = (),
                  vram_floor_mb: Optional[int] = None, mode: str = "open") -> List[str]:
    argv = [sys.executable, "-m", "kgpu.fleet", "lane",
            "--kind", lane.kind, "--exchange", lane.exchange,
            "--tickers", ",".join(lane.tickers), "--name", lane.name, "--mode", mode]
    if lane.account:
        argv += ["--account", lane.account]
    if not apply:
        argv.append("--dry-run")
    if rehearse:
        argv.append("--rehearse")
    for engine in engines:
        argv += ["--engine", engine]
    for layer in layers:
        argv += ["--layer", layer]
    if vram_floor_mb is not None:
        argv += ["--vram-floor-mb", str(vram_floor_mb)]
    return argv


def run_fleet(lanes: Sequence[Lane], *, apply: bool = True, rehearse: bool = False,
              engines: Sequence[str] = (), layers: Sequence[str] = (),
              vram_floor_mb: Optional[int] = None,
              mode: str = "open",
              log_dir: Optional[os.PathLike | str] = None,
              poll_seconds: float = 20.0,
              log: Optional[Callable[[str], None]] = None) -> Dict[str, int]:
    """Start every lane as a subprocess, stream them, and wait. `{lane name: exit code}`.

    ⚠️ **EACH LANE'S OUTPUT GOES TO ITS OWN FILE AND THE PARENT PRINTS A HEARTBEAT.** Five
    lanes interleaving hours of per-page lines into one terminal is a log nobody can read, and
    the per-lane file is what a session comes back to. The heartbeat says which lane is on which
    ticker and how long it has been there — §5 rule 20's reason, one level up: a unit of work is
    written to disk as it finishes.

    ⚠️ **`PYTHONUTF8=1` IS SET FOR EVERY CHILD.** The parse logs Vietnamese account labels and
    this machine is cp1252, so a child without it dies on its own progress line (§5 rule 18).

    ⚠️ **CTRL-C KILLS THE CHILDREN AND NOT THE KAGGLE KERNELS.** A pushed kernel runs to
    completion on Kaggle whatever happens here; `python -m kgpu wait <job>` / `pull <job>`
    reattaches, and the release then writes whatever came home.
    """
    say = log or print
    root = Path(log_dir) if log_dir else REPO_ROOT / "logs" / "fleet"
    root.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")

    running: Dict[str, Tuple[subprocess.Popen, Path]] = {}
    for lane in lanes:
        path = root / f"{stamp}__{lane.name.replace(':', '_').replace('#', '')}.log"
        handle = path.open("w", encoding="utf-8", buffering=1)
        argv = _lane_command(lane, apply=apply, rehearse=rehearse, engines=engines,
                             layers=layers, vram_floor_mb=vram_floor_mb, mode=mode)
        handle.write(f"=== {lane.name} · {lane.kind} · {lane.account or '-'} · "
                     f"{len(lane.tickers)} ticker(s), {lane.documents} document(s) ===\n")
        handle.write(" ".join(argv) + "\n\n")
        handle.flush()
        proc = subprocess.Popen(argv, cwd=str(REPO_ROOT / "src" / "kaggle_gpu"),
                                stdout=handle, stderr=subprocess.STDOUT, env=env)
        running[lane.name] = (proc, path)
        say(f"started {lane.name:22s} pid {proc.pid:6d}  {len(lane.tickers)} ticker(s), "
            f"{lane.documents} document(s)  -> {path.name}")
        say(f"          {', '.join(lane.tickers)}")

    codes: Dict[str, int] = {}
    started = time.time()
    try:
        while len(codes) < len(running):
            time.sleep(poll_seconds)
            for name, (proc, path) in running.items():
                if name in codes:
                    continue
                code = proc.poll()
                if code is not None:
                    codes[name] = code
                    say(f"{'✅' if code == 0 else '⚠️'} {name} finished, exit {code} "
                        f"after {(time.time() - started) / 60:.1f} min")
            if len(codes) < len(running):
                say(f"[{(time.time() - started) / 60:6.1f} min] "
                    + " | ".join(f"{name}: {_tail(path)}"
                                 for name, (_p, path) in running.items()
                                 if name not in codes))
    except KeyboardInterrupt:
        say("")
        say("⚠️ interrupted — killing the lanes. A KAGGLE KERNEL ALREADY PUSHED KEEPS RUNNING: "
            "reattach with `python -m kgpu wait <job>` then `pull <job>`.")
        for name, (proc, _path) in running.items():
            if name not in codes:
                proc.kill()
                codes[name] = 130
    say("")
    say(f"-> {sum(1 for c in codes.values() if c == 0)} of {len(codes)} lane(s) clean; "
        f"logs in {root}")
    return codes


def _tail(path: Path, width: int = 58) -> str:
    """The last non-blank line of a lane's log, trimmed to its DETAIL segment.

    ⚠️ `progress.detail_of` and not the whole line: every progress line now opens with the
    overall percentage, so the informative half is what follows it (`RUN__pdf_ocr_control`'s own
    refusal cell was the first reader this broke).
    """
    try:
        lines = [l.rstrip() for l in path.read_text(encoding="utf-8", errors="replace")
                 .splitlines() if l.strip()]
    except OSError:
        return "(no log yet)"
    if not lines:
        return "(no log yet)"
    repo_src_on_path()
    try:
        from utils import progress

        text = progress.detail_of(lines[-1]) or lines[-1]
    except Exception:                                  # noqa: BLE001
        text = lines[-1]
    return text[:width]


def plan_fleet(tickers: Sequence[Tuple[str, str]], *, local_lanes: int = 1,
               kaggle_accounts: Sequence[str] = (),
               sessions_per_account: int = SESSIONS_PER_ACCOUNT,
               skip_exhausted: bool = True,
               mode: str = "open",
               log: Optional[Callable[[str], None]] = None) -> List[Lane]:
    """Resolve the whole plan and print it — **spends nothing, opens no PDF, no network.**

    ⚠️ Tickers may span exchanges, and a lane is per exchange because `plan_batch` is. Today
    every VN30 name is HOSE; the grouping is here so a HNX name does not silently get parsed as
    a HOSE one.
    """
    say = log or print
    by_exchange: Dict[str, List[str]] = {}
    for ticker, exchange in tickers:
        by_exchange.setdefault(exchange, []).append(ticker)
    lanes: List[Lane] = []
    for exchange, group in sorted(by_exchange.items()):
        if mode == "alternates":
            say(f"{exchange}: {len(group)} ticker(s) — resolving the UNASKED ALTERNATES "
                f"(`ALT-2`), the one block where GPU still buys cells")
            plans = alternate_plans([(t, exchange) for t in group], log=say)
        else:
            say(f"{exchange}: {len(group)} ticker(s) — resolving what is still winnable")
            plans = winnable(group, exchange=exchange, skip_exhausted=skip_exhausted, log=say)
        say(f"   {len(plans)} ticker(s) with something to win, "
            f"{sum(len(p.quarters) for p in plans)} document(s)")
        lanes += assign(plans, local_lanes=local_lanes, kaggle_accounts=kaggle_accounts,
                        sessions_per_account=sessions_per_account, exchange=exchange)
    say("")
    for lane in lanes:
        say(f"{lane.name:22s} {lane.documents:4d} document(s)  {', '.join(lane.tickers)}")
    if lanes:
        widest = max(l.documents for l in lanes)
        say("")
        say(f"the long pole is {widest} document(s) — at ~2.6 min each that is "
            f"~{widest * 2.6 / 60:.1f} h of GPU on that lane")
        say("⚠️ NOMINAL. `accounts.estimate_hours`' per-document figure sizes the quota "
            "question and predicts no runtime: half the GPU hours go to the documents that "
            "FAIL, and a document that is long is a document that is losing.")
    return lanes


# ── CLI ───────────────────────────────────────────────────────────────────────

def _main(argv: Optional[Sequence[str]] = None) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(prog="python -m kgpu.fleet", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    for name, helptext in (("plan", "resolve the lanes and print them; spends nothing"),
                           ("run", "resolve the lanes and START them, one process each")):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--universe", default="VN30", help="VN30 | VN100")
        p.add_argument("--tickers", default="", help="a comma-separated list, instead of a universe")
        p.add_argument("--exchange", default="HOSE")
        p.add_argument("--local-lanes", type=int, default=1,
                       help="⚠️ >1 needs a VRAM measurement — see the module docstring")
        p.add_argument("--accounts", default="",
                       help="comma-separated Kaggle labels; empty = every account in .env")
        p.add_argument("--sessions", type=int, default=SESSIONS_PER_ACCOUNT,
                       help="concurrent GPU kernels per account (Kaggle allows 2)")
        p.add_argument("--no-skip-exhausted", action="store_true",
                       help="re-ask cells this cascade has already lost (`ASK-1`)")
        p.add_argument("--engine", action="append", default=[],
                       help="name the cascade by engine, e.g. --engine easyocr")
        p.add_argument("--layer", action="append", default=[], help="an explicit layer list")
        p.add_argument("--vram-floor-mb", type=int, default=None)
        p.add_argument("--mode", default="open", choices=("open", "alternates"),
                       help="open = every winnable document; alternates = only the periods "
                            "holding a filing NO run has read (`ALT-2`), which `ASK-1` "
                            "correctly retires and which is the one GPU-shaped block left")
        if name == "run":
            p.add_argument("--dry-run", action="store_true",
                           help="parse but write no CSV — the run folders are still produced")
            p.add_argument("--rehearse", action="store_true",
                           help="KAGGLE lanes: run the worker side locally first, no quota")

    lane = sub.add_parser("lane", help="ONE lane, in this process — what `run` spawns")
    lane.add_argument("--kind", required=True, choices=("local", "kaggle"))
    lane.add_argument("--tickers", required=True)
    lane.add_argument("--exchange", default="HOSE")
    lane.add_argument("--account", default=None)
    lane.add_argument("--name", default="lane")
    lane.add_argument("--dry-run", action="store_true")
    lane.add_argument("--rehearse", action="store_true")
    lane.add_argument("--engine", action="append", default=[])
    lane.add_argument("--layer", action="append", default=[])
    lane.add_argument("--vram-floor-mb", type=int, default=None)
    lane.add_argument("--mode", default="open", choices=("open", "alternates"))

    args = ap.parse_args(argv)

    if args.command == "lane":
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if args.kind == "local":
            return run_local(tickers, exchange=args.exchange, layers=args.layer or None,
                             engines=args.engine or None, vram_floor_mb=args.vram_floor_mb,
                             apply=not args.dry_run, mode=args.mode)
        if not args.account:
            raise SystemExit("a kaggle lane needs --account <label>; the fleet never lets a "
                             "lane re-rank by quota (two lanes would converge on one account)")
        return run_kaggle(tickers, account=args.account, exchange=args.exchange,
                          layers=args.layer or None, engines=args.engine or None,
                          apply=not args.dry_run, rehearse=args.rehearse, mode=args.mode)

    if args.tickers:
        names = [(t.strip().upper(), args.exchange)
                 for t in args.tickers.split(",") if t.strip()]
    else:
        names = universe(args.universe)

    labels = [l.strip() for l in args.accounts.split(",") if l.strip()]
    if not labels and args.command == "run" or (not labels and args.accounts == ""):
        from . import accounts as acc

        labels = [a.label for a in acc.discover()]
    lanes = plan_fleet(names, local_lanes=args.local_lanes, kaggle_accounts=labels,
                       sessions_per_account=args.sessions,
                       skip_exhausted=not args.no_skip_exhausted, mode=args.mode)
    if args.command == "plan":
        return 0
    if not lanes:
        print("nothing to run — every ticker is done or exhausted")
        return 0
    codes = run_fleet(lanes, apply=not args.dry_run, rehearse=args.rehearse,
                      engines=args.engine, layers=args.layer,
                      vram_floor_mb=args.vram_floor_mb, mode=args.mode)
    return 0 if all(c == 0 for c in codes.values()) else 1


if __name__ == "__main__":
    raise SystemExit(_main())
