# src\web_scraper\pdf_ocr_job.py
"""Run the CafeF filing parse on WHATEVER MACHINE HAS THE GPU — the OCR, without the write.

`FinancialsBuilder.build()` is the production path: it chooses the documents, escalates the
OCR cascade, de-cumulates, falls back, and WRITES the three statement CSVs. That last verb is
why it cannot be the thing a Kaggle worker runs. This module is the same parse with the write
removed and the machine made a parameter:

    plan()          which filings to open, via `FinancialsBuilder.documents()` — not re-chosen
    seed_history()  the magnitude band `sane` would have had in a full run, read off disk
    run_document()  ONE filing through `FinancialsBuilder._parse_cascaded` — the real cascade
    run()           a list of them, one JSON per document written AS IT FINISHES
    compare()       the parsed cells against the statement CSV already on disk

⚠️ **NOTHING HERE WRITES INTO `raw_data/.../statements/`, AND THAT IS THE DESIGN.** The repo
has measured the cost of a partial write four times (CLAUDE.md §6-2-vicies, §6-2-unvicies,
§6-2-quatervicies, §6-2-quinvicies): a `periods` run silently DOWNGRADES the quarters it was
given only for history, and the log says `RUN_SUCCESS` either way. A run whose output is an
artefact cannot do that. Merging a recovered quarter back into the CSVs stays a separate,
deliberate act through Dagster, with a pre-run backup and a diff of every column.

⚠️ **AND `sane` IS THE REASON THIS MODULE SEEDS A HISTORY AT ALL.** `_parse_cascaded` takes
`history` and uses it for the magnitude gate; hand it an empty one and the gate FAILS OPEN,
which is how a `periods` run wrote 115,110 mn as BID's total assets twice in one session
(§6-2-octodecies). `seed_history` reconstructs the band from the quarters already accepted on
disk — ⚠️ **a RECONSTRUCTION of a full run's history, not the run's own**: it holds what disk
records, in the entity split `build()` uses, restricted to periods before the target, and it
cannot know a quarter a full run would have accepted and disk does not carry.

## Why it exists: the machine is a parameter and the parse is not

The expensive half of a parse is OCR, and the onnx engine is two models — DeepDoc DB detection
under onnxruntime and VietOCR recognition under torch — both of which run on a GPU when there
is one. The machine that has this repo has 4 GiB; a Kaggle T4 has 15. Nothing else about the
parse should change with the machine, so nothing else is re-implemented here: the document
choice, the 47-layer cascade, `reconcile`, `sane`, `map_to_schema` and the schema files are the
production ones, reached by import.

⚠️ **A run on another machine is a different PROCEDURE, not the same one faster.** Kaggle ships
its own torch and onnxruntime, and detection under `CUDAExecutionProvider` is not bit-identical
to detection on the CPU wheel. So the artefact carries `environment()` and the run is scored by
`compare()` against what disk already holds — cell for cell — rather than being assumed to
reproduce it. That comparison is the measurement, and the reason this module ships one.

## Paths

Everything the parse reads lives under one root — `raw_data/cafef/` here, an unpacked payload
on a worker. `use_data_root()` re-points `cafef_financials`' three path globals, which those
functions read at CALL time precisely so a harness can do this (`statement_path`'s docstring
has said so since the experiment harnesses needed it). The models are separate: the det model
and the VietOCR weights are found through the env vars `onnx_ocr` reads, so an offline worker
never reaches for HuggingFace or vocr.vn.
"""

from __future__ import annotations

# ===== Standard Library =====
import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

# ===== Local / Custom Modules =====
from web_scraper import cafef_financials as fin
from web_scraper.cafef_financials import FinancialsBuilder, ParseLayer
from web_scraper.cafef_pdf_parser import REPORTS

# ⚠️ **THE ENV VAR IS THE SEAM BETWEEN THIS MODULE AND `kgpu_bootstrap`, WHICH CANNOT IMPORT
# IT.** The bootstrap runs before the repo source is on `sys.path` — that is its whole job — so
# it hands the unpacked data root over as an environment variable and this module reads it. The
# name is duplicated in `kgpu/remote/kgpu_bootstrap.py`; both sides say so.
DATA_ROOT_ENV = "CAFEF_DATA_ROOT"
MODELS_DIR_ENV = "CAFEF_MODELS_DIR"

# The repo default, resolved from this file rather than from the CWD: `python -m` runs from
# `src\`, a notebook runs from wherever it was opened, and a worker runs from `/kaggle/working`.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DATA_ROOT = REPO_ROOT / "raw_data" / "cafef"
DEFAULT_MODELS_DIR = Path(__file__).resolve().parent / "models"
DEFAULT_OUT_ROOT = REPO_ROOT / "reports" / "pdf_ocr"

# The columns of a statement CSV that describe the ROW rather than the statement. Taken from
# `cafef_financials.DATA_COLS` rather than re-listed, so a new provenance column cannot start
# reading as a line item here while the writer treats it as metadata.
META_COLS = set(fin.DATA_COLS)


# ──────────────────────────────────────────────────────────────────────────────
# Where the inputs live
# ──────────────────────────────────────────────────────────────────────────────


def data_root() -> Path:
    """The CafeF data root in force — `$CAFEF_DATA_ROOT`, else the repo's own."""
    value = os.environ.get(DATA_ROOT_ENV)
    return Path(value) if value else DEFAULT_DATA_ROOT


def use_data_root(root: Optional[os.PathLike] = None) -> Path:
    """Point `cafef_financials` at `root` and verify what the parse actually needs.

    ⚠️ **RE-POINTS MODULE GLOBALS, WHICH IS THE SUPPORTED WAY HERE.** `documents()` reads
    `PDFS_DIR`, `schema_of()` reads `SCHEMA_DIR` and `statement_path()` reads `STATEMENTS_DIR`
    — each at call time, and `statement_path`'s docstring records why. Rebinding them is
    therefore a redirect and not a monkey-patch of behaviour.

    ⚠️ **IT CHECKS THE SCHEMA DIRECTORY BECAUSE AN ABSENT CHART OF ACCOUNTS IS SILENT.**
    `schema_of` already raises through `require_file` — after the OCR. `utils/inputs.py` is
    named for the 2.4 hours that cost once; checking here makes it free.
    """
    root = Path(root) if root is not None else data_root()
    root = root.resolve()
    pdfs = root / "pdfs"
    financials = root / "financials"
    missing = [str(p) for p in (pdfs, financials / "schema") if not p.is_dir()]
    if missing:
        raise FileNotFoundError(
            f"{root} is not a CafeF data root — missing {missing}.\n"
            f"  It must hold pdfs/{{index,files}} and financials/schema/*.csv. Set "
            f"{DATA_ROOT_ENV} or pass --data-root."
        )
    fin.PDFS_DIR = str(pdfs)
    fin.FIN_DIR = str(financials)
    fin.SCHEMA_DIR = str(financials / "schema")
    fin.STATEMENTS_DIR = str(financials / "statements")
    fin.TEMPLATES_INDEX = str(financials / "templates.csv")
    os.environ[DATA_ROOT_ENV] = str(root)
    return root


def use_models(models_dir: Optional[os.PathLike] = None) -> Dict[str, Optional[str]]:
    """Point the onnx engine at LOCAL model files, so an offline worker downloads nothing.

    Two models and two different failure modes without this. `ensure_det_model` fetches
    `det.onnx` from HuggingFace when the bundled copy is absent, and VietOCR's
    `download_weights` fetches `vgg_seq2seq.pth` from vocr.vn on first use — both need
    internet, and a Kaggle kernel with `enable_internet: false` has none.

    ⚠️ **THE ENV VARS ARE READ AT `onnx_ocr` IMPORT TIME**, so they are set here AND written
    onto the module when it is already imported. Setting only the environment works exactly
    until something has imported the engine first, which is the kind of ordering bug that shows
    up as a download attempt three minutes into a run.
    """
    models = Path(models_dir) if models_dir is not None else Path(
        os.environ.get(MODELS_DIR_ENV, DEFAULT_MODELS_DIR))
    chosen: Dict[str, Optional[str]] = {"det": None, "vietocr": None}

    det = models / "deepdoc_det.onnx"
    if det.is_file():
        os.environ["CAFEF_ONNX_DET"] = str(det)
        chosen["det"] = str(det)
    # ⚠️ The payload's `models/` first, then wherever this machine already has one. Both are a
    # LOCAL file, which is the whole point — vietocr's fall-through is a 90 MB download, and
    # reporting "no checkpoint" on a machine that has had one cached since the first parse
    # would be a warning that is simply untrue.
    weights = models / "vgg_seq2seq.pth"
    weights = weights if weights.is_file() else find_vietocr_weights()
    if weights is not None:
        os.environ["CAFEF_ONNX_VIETOCR_WEIGHTS"] = str(weights)
        chosen["vietocr"] = str(weights)

    engine = sys.modules.get("web_scraper.onnx_ocr")
    if engine is not None:
        if chosen["det"]:
            engine.DET_MODEL = chosen["det"]
        if chosen["vietocr"]:
            engine.VIETOCR_WEIGHTS = chosen["vietocr"]
    os.environ[MODELS_DIR_ENV] = str(models)
    return chosen


# Where a VietOCR checkpoint may already be on this machine, best first. ⚠️ **The last entry is
# vietocr's OWN cache and is the one that usually hits**: `vietocr.tool.utils.download` writes
# `vgg_seq2seq.pth` into `tempfile.gettempdir()` and re-uses it, so any machine that has ever
# run the onnx engine has a copy there and nothing needs downloading to build a payload.
def vietocr_weight_candidates() -> List[Path]:
    import tempfile

    env = os.environ.get("CAFEF_ONNX_VIETOCR_WEIGHTS", "")
    return [p for p in (
        Path(env) if env else None,
        DEFAULT_MODELS_DIR / "vgg_seq2seq.pth",
        (REPO_ROOT / "experiment" / "experiment_9" / "vendor" / "deepdoc_vietocr"
         / "vietocr" / "weight" / "vgg_seq2seq.pth"),
        Path(tempfile.gettempdir()) / "vgg_seq2seq.pth",
    ) if p is not None]


def find_vietocr_weights() -> Optional[Path]:
    """The VietOCR checkpoint to SHIP, or None. Never downloads — the caller decides that."""
    return next((p for p in vietocr_weight_candidates() if p.is_file()), None)


# ──────────────────────────────────────────────────────────────────────────────
# What to parse
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DocumentTask:
    """One filing to open: the document `documents()` chose, plus where it is on disk."""

    exchange: str
    symbol: str
    period: str
    template: str
    path: str
    file: str
    consolidated: str          # "True" | "False" — the ENTITY, and `sane` bands per entity
    assurance: str
    # ⚠️ A CUMULATIVE FILING'S INCOME STATEMENT IS THE YEAR TO DATE, NOT THE QUARTER.
    # `build()` de-cumulates it afterwards; this module does not, because de-cumulation needs
    # Q1..Q(q-1) of the same year and a one-document run does not have them. Recorded so
    # `compare()` refuses to read a cumulative P&L against a de-cumulated row on disk.
    cumulative: bool

    @property
    def key(self) -> str:
        return f"{self.exchange}_{self.symbol}__{self.period}"


def plan(builder: FinancialsBuilder, exchange: str, symbol: str,
         periods: Optional[Sequence[str]] = None,
         allow_parent: bool = False,
         period_min: Optional[str] = fin.FINANCIALS_PERIOD_MIN) -> List[DocumentTask]:
    """The filings to open, oldest first — `documents()` chooses them, this only filters.

    ⚠️ **THE CHOICE IS NOT RE-IMPLEMENTED.** Consolidated beats standalone, the audited annual
    stands in for Q4, and neither rule may be re-stated here: `documents()` carries a measured
    guard against an annual changing the ENTITY of a Q4 row (86 of 13,912 periods moved before
    it existed), and a second copy of that logic is a second place for it to be wrong.

    A `periods` filter naming a period the ticker does not file RAISES — a filter that matches
    nothing is a run that parses nothing and reports success.
    """
    docs = builder.documents(exchange, symbol, allow_parent=allow_parent,
                             period_min=period_min)
    if periods:
        wanted = list(dict.fromkeys(periods))
        have = {d["period"] for d in docs}
        unknown = [p for p in wanted if p not in have]
        if unknown:
            raise ValueError(
                f"{exchange}_{symbol} files no document for {unknown} "
                f"(allow_parent={allow_parent}, period_min={period_min!r}).\n"
                f"  Periods available: {sorted(have)[:8]}"
                f"{' …' if len(have) > 8 else ''}"
            )
        docs = [d for d in docs if d["period"] in wanted]

    template = builder.template_of(symbol) or "bank"
    tasks: List[DocumentTask] = []
    for d in docs:
        path = os.path.join(fin.PDFS_DIR, d["path"].replace("/", os.sep))
        tasks.append(DocumentTask(
            exchange=exchange, symbol=symbol, period=d["period"], template=template,
            path=path, file=d["file"], consolidated=d.get("consolidated", "True"),
            assurance=d.get("assurance", ""),
            cumulative=(d.get("half_year") == "True" or d.get("annual") == "True"),
        ))
    return tasks


# ──────────────────────────────────────────────────────────────────────────────
# The magnitude band `sane` needs
# ──────────────────────────────────────────────────────────────────────────────


def _line_items(row: dict) -> Dict[str, int]:
    """The statement's own columns of a CSV row — everything `DATA_COLS` does not name."""
    out: Dict[str, int] = {}
    for col, value in row.items():
        if not col or col in META_COLS or value in ("", None):
            continue
        try:
            out[col] = int(value)
        except (TypeError, ValueError):
            continue
    return out


def seed_history(builder: FinancialsBuilder, exchange: str, symbol: str, template: str,
                 before: Optional[str] = None) -> Dict[str, Dict[str, List[int]]]:
    """`{report: {entity: [probe]}}` reconstructed from the statement CSVs on disk.

    This is the band `sane` compares a candidate statement against, in the shape `build()`
    keeps it: per report AND per entity, because a standalone company is legitimately smaller
    than the consolidated group and pooling the two makes the band meaningless in both
    directions (`SAN-1`).

    Three rules are copied from `build()` deliberately, and each one changes the verdict:

      * only `source == 'pdf'` rows contribute — a `cafef` or `missing` row was never accepted
        by the gates, so it was never in the run's history either;
      * a row with fewer than `MIN_ITEMS_FOR_HISTORY` mapped items is EXCLUDED, exactly as an
        accepted-but-thin statement is withheld in `build()`. That rule exists because one
        2-item statement became the whole reference population and silently rejected every
        correct quarter after it;
      * `before` restricts to earlier periods, because a full run judges a quarter against the
        quarters it has already accepted — never against its own future.

    ⚠️ **IT IS A RECONSTRUCTION AND CANNOT BE THE RUN'S OWN HISTORY.** A full run's band holds
    what it accepted in that run; this holds what disk records. They agree when disk is the
    output of a full run and diverge the moment it is not. Read a verdict from a subset run
    with that in mind — and never WRITE one (see the module docstring).
    """
    history: Dict[str, Dict[str, List[int]]] = {
        r: {"True": [], "False": []} for r in REPORTS}
    probe_columns = {
        fin.BALANCE_SHEET: builder.C_ASSETS,
        fin.INCOME_STATEMENT: builder.C_PBT,
        fin.CASH_FLOW: builder.C_CASH_CLOSE,
    }
    floor = fin._period_key(before) if before else None
    for report in REPORTS:
        for period, row in builder._existing(exchange, symbol, template, report).items():
            if row.get("source") != "pdf":
                continue
            if floor is not None and fin._period_key(period) >= floor:
                continue
            items = _line_items(row)
            if len(items) < builder.MIN_ITEMS_FOR_HISTORY:
                continue
            probe = next((items[c] for c in probe_columns[report] if c in items), None)
            if probe is None:
                continue
            entity = row.get("consolidated") or "True"
            history[report].setdefault(entity, []).append(probe)
    return history


def open_reference(builder: FinancialsBuilder, exchange: str, symbol: str, template: str,
                   period: str) -> Optional[int]:
    """The previous year's Q4 closing cash, off disk — what `build()` hands a subset run.

    "Đầu kỳ" is 1 January, so every quarter of a year opens on the same figure. `build()` reads
    it back from the CSV when the run itself did not parse that Q4, and without it a re-parsed
    Q1 is judged more harshly than the same quarter in a full run.
    """
    prev = f"Q4-{int(period.split('-')[1]) - 1}"
    row = builder._existing(exchange, symbol, template, fin.CASH_FLOW).get(prev, {})
    if row.get("source") != "pdf":
        return None
    items = _line_items(row)
    return next((items[c] for c in builder.C_CASH_CLOSE if c in items), None)


# ──────────────────────────────────────────────────────────────────────────────
# Running one filing
# ──────────────────────────────────────────────────────────────────────────────


class CollectingLogger:
    """A `Logger`-shaped sink that KEEPS the lines and echoes them, flushed.

    Two reasons, both measured. `_parse_cascaded` computes a refusal reason per layer and
    reports it through `_warn` only — so an artefact that does not capture the log cannot say
    why a statement is absent, and recovering one such reason afterwards took four probe runs
    (§6-2-quindecies). And a long parse prints nothing for over an hour when stdout is block
    buffered in a subprocess (§5 rule 20, §6-2-noviesdecies), which on a batch worker is the
    difference between a progress signal and a black box.
    """

    def __init__(self, echo: bool = True):
        self.lines: List[str] = []
        self.echo = echo

    def _put(self, level: str, message: str) -> None:
        self.lines.append(message if level == "INFO" else f"{level}: {message}")
        if self.echo:
            print(message, flush=True)

    def log_info(self, message: str) -> None:
        self._put("INFO", message)

    def log_warning(self, message: str) -> None:
        self._put("WARNING", message)

    def log_error(self, message: str) -> None:
        self._put("ERROR", message)

    def log_debug(self, message: str) -> None:  # never echoed; the cascade is chatty enough
        self.lines.append(f"DEBUG: {message}")

    def take(self) -> List[str]:
        lines, self.lines = self.lines, []
        return lines


def select_layers(names: Optional[Sequence[str]]) -> List[ParseLayer]:
    """The cascade, or the named subset of it, in the cascade's OWN order.

    ⚠️ **ORDER IS PRESERVED AND A NAME THAT MATCHES NOTHING RAISES.** Layer order is
    load-bearing — a half-right layer that passes the gates ENDS the cascade, which is why
    `+notes+seam` runs before bare `+notes` and `+unit+tail` before bare `+unit`
    (`PGB-1`, §6-2-unvicies) — so a subset may drop layers but never re-order them. And a
    misspelt layer name would silently widen the run to a cascade the caller did not ask for.
    """
    if not names:
        return list(FinancialsBuilder.LAYERS)
    known = {layer.name for layer in FinancialsBuilder.LAYERS}
    unknown = [n for n in names if n not in known]
    if unknown:
        raise ValueError(
            f"no parse layer(s) named {unknown}. A name that matches nothing is a silent "
            f"widening of the cascade, so it raises.\n  Known: {sorted(known)[:10]} …")
    wanted = set(names)
    return [layer for layer in FinancialsBuilder.LAYERS if layer.name in wanted]


@dataclass
class DocumentResult:
    """What one filing produced — the artefact row, and what `compare()` reads."""

    task: DocumentTask
    seconds: float
    accepted: Dict[str, dict] = field(default_factory=dict)   # report -> {layer, items, values}
    absent: List[str] = field(default_factory=list)
    facts: dict = field(default_factory=dict)
    log: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_json(self) -> dict:
        return {
            "exchange": self.task.exchange,
            "symbol": self.task.symbol,
            "period": self.task.period,
            "template": self.task.template,
            "document": self.task.file,
            "consolidated": self.task.consolidated,
            "assurance": self.task.assurance,
            "cumulative": self.task.cumulative,
            "seconds": round(self.seconds, 2),
            "accepted": self.accepted,
            "absent": self.absent,
            "facts": self.facts,
            "error": self.error,
            "log": self.log,
        }


def run_document(builder: FinancialsBuilder, task: DocumentTask,
                 history: Dict[str, Dict[str, List[int]]],
                 open_ref: Optional[int] = None,
                 logger: Optional[CollectingLogger] = None) -> DocumentResult:
    """One filing through the production cascade. Returns what was accepted and why not.

    ⚠️ **IT CALLS `_parse_cascaded` RATHER THAN REPRODUCING IT.** That function is where the
    layer order, the per-(engine, dpi, crop) parse cache, the `reconcile`-then-`sane`
    short-circuit and the refusal report all live, and every one of them has been the subject
    of a measured defect. A second implementation would be a second thing to keep correct.
    """
    logger = logger or CollectingLogger()
    band = {r: history[r].get(task.consolidated, []) for r in REPORTS}
    started = time.perf_counter()
    result = DocumentResult(task=task, seconds=0.0)
    try:
        accepted, facts = builder._parse_cascaded(
            task.path, builder._period_end(task.period), task.template, band, open_ref)
    except Exception as exc:                    # noqa: BLE001 — one bad filing is not a run
        result.seconds = time.perf_counter() - started
        result.error = f"{type(exc).__name__}: {exc}"
        result.absent = list(REPORTS)
        result.log = logger.take()
        return result

    result.seconds = time.perf_counter() - started
    for report in REPORTS:
        got = accepted.get(report)
        if got is None:
            result.absent.append(report)
            continue
        row, statement, layer = got
        result.accepted[report] = {
            "layer": layer,
            "items": len(row),
            "pages": statement.pages,
            "unit": statement.unit,
            "n_columns": statement.n_columns,
            "cash_flow_method": statement.cash_flow_method or "",
            "quarter_column": bool(statement.quarter_column),
            "values": {k: int(v) for k, v in row.items()},
        }
    result.facts = {
        "publish_date": facts.get("publish_date", ""),
        **{k: v for k, v in (facts.get("shares") or {}).items()},
    }
    result.log = logger.take()
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Scoring a run against what is already on disk
# ──────────────────────────────────────────────────────────────────────────────


def compare(builder: FinancialsBuilder, result: DocumentResult) -> Dict[str, dict]:
    """Cell-by-cell against the statement CSV on disk — the run's own measurement.

    ⚠️ **DIFF EVERY COLUMN, NOT THE FIGURES.** A run that changed one `publish_date` and
    nothing else read as clean to a figures-only diff (§6-2-quatervicies), so the layer, the
    unit and the item count are compared beside the values.

    ⚠️ **A CUMULATIVE INCOME STATEMENT IS NOT COMPARABLE AND IS REFUSED, NOT SCORED.** An
    annual or semi-annual filing prints the year to date; the row on disk has been through
    `_decumulate`. Comparing them would report every cell as changed and mean nothing.
    """
    out: Dict[str, dict] = {}
    for report in REPORTS:
        got = result.accepted.get(report)
        disk = builder._existing(result.task.exchange, result.task.symbol,
                                 result.task.template, report).get(result.task.period)
        entry: Dict[str, object] = {
            "on_disk": (disk or {}).get("source", "absent"),
            "disk_layer": (disk or {}).get("method", ""),
            "run_layer": (got or {}).get("layer", ""),
        }
        if got is None:
            entry["verdict"] = "absent in this run"
            out[report] = entry
            continue
        if disk is None or disk.get("source") != "pdf":
            entry["verdict"] = "no pdf row on disk to compare against"
            out[report] = entry
            continue
        if report == fin.INCOME_STATEMENT and result.task.cumulative:
            entry["verdict"] = (
                "skipped — the filing is cumulative and the row on disk is de-cumulated")
            out[report] = entry
            continue

        disk_values = _line_items(disk)
        run_values = {k: int(v) for k, v in got["values"].items()}
        identical = [c for c in run_values
                     if c in disk_values and run_values[c] == disk_values[c]]
        changed = {c: [disk_values[c], run_values[c]] for c in run_values
                   if c in disk_values and run_values[c] != disk_values[c]}
        entry.update({
            "cells_disk": len(disk_values),
            "cells_run": len(run_values),
            "identical": len(identical),
            "changed": changed,
            "only_on_disk": sorted(set(disk_values) - set(run_values)),
            "only_in_run": sorted(set(run_values) - set(disk_values)),
            "same_layer": got["layer"] == disk.get("method"),
            "same_unit": str(got["unit"]) == str(disk.get("unit", "")),
            "same_publish_date": (result.facts.get("publish_date", "")
                                  == disk.get("publish_date", "")),
        })
        entry["verdict"] = "REPRODUCED" if all((
            not changed, not entry["only_on_disk"], not entry["only_in_run"],
            entry["same_layer"], entry["same_unit"], entry["same_publish_date"],
        )) else "DIFFERS"
        out[report] = entry
    return out


# ──────────────────────────────────────────────────────────────────────────────
# The run
# ──────────────────────────────────────────────────────────────────────────────


def run(tasks: Sequence[DocumentTask], out_root: os.PathLike = DEFAULT_OUT_ROOT,
        run_id: Optional[str] = None, layers: Optional[Sequence[str]] = None,
        notes: str = "", git_commit: Optional[str] = None,
        compare_with_disk: bool = True) -> Path:
    """Parse every task, writing each document's JSON AS IT FINISHES. Returns the run folder.

    ⚠️ **ONE FILE PER DOCUMENT, WRITTEN BEFORE THE NEXT ONE STARTS** — §5 rule 20. A four-hour
    run that keeps its results in memory loses all of them to the first crash, and this stage
    has documents that cost 73 minutes each (§6-2-noviesdecies).

    The folder holds a `metadata.json`, which is also what makes it a RUN FOLDER to
    `kgpu pull` — that merge copies any directory carrying one, so nothing extra is needed to
    bring a Kaggle run home.
    """
    from utils import runtime

    if not tasks:
        raise ValueError("nothing to parse — `plan()` returned no documents.")

    builder = FinancialsBuilder(logger=None)
    chosen = select_layers(layers)
    builder.LAYERS = chosen                      # instance attribute; the class list is intact

    symbols = sorted({f"{t.exchange}_{t.symbol}" for t in tasks})
    run_id = run_id or f"{runtime.folder_stamp()}__{'_'.join(symbols).lower()}__pdf_ocr"
    folder = Path(out_root) / run_id
    (folder / "documents").mkdir(parents=True, exist_ok=True)

    timer = runtime.RunTimer("web_scraper.pdf_ocr_job", device=_ocr_device()).start()
    rows: List[dict] = []
    try:
        for index, task in enumerate(tasks, start=1):
            logger = CollectingLogger()
            builder._logger = logger
            for parser in builder._parsers.values():
                parser._logger = logger
            print(f"[{index}/{len(tasks)}] {task.key}  {task.file}", flush=True)

            history = seed_history(builder, task.exchange, task.symbol, task.template,
                                   before=task.period)
            open_ref = open_reference(builder, task.exchange, task.symbol, task.template,
                                      task.period)
            result = run_document(builder, task, history, open_ref, logger=logger)
            payload = result.to_json()
            payload["history_sizes"] = {r: {e: len(v) for e, v in history[r].items()}
                                        for r in REPORTS}
            payload["open_ref"] = open_ref
            if compare_with_disk:
                payload["compare"] = compare(builder, result)
            (folder / "documents" / f"{task.key}.json").write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

            for report in REPORTS:
                got = result.accepted.get(report)
                rows.append({
                    "period": task.period,
                    "report": report,
                    "layer": (got or {}).get("layer", ""),
                    "items": (got or {}).get("items", 0),
                    "status": "pdf" if got else "absent",
                    "verdict": payload.get("compare", {}).get(report, {}).get("verdict", ""),
                    "seconds": round(result.seconds, 2),
                })
            print("    " + task.period + ": "
                  + "; ".join(f"{r}={result.accepted[r]['items']} items "
                              f"[{result.accepted[r]['layer']}]"
                              for r in REPORTS if r in result.accepted)
                  + (f"; absent {result.absent}" if result.absent else "")
                  + f"  ({result.seconds / 60:.1f} min)", flush=True)
    finally:
        timer.stop(ok=True)

    with (folder / "summary.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=list(rows[0]) if rows else ["period", "report", "status"])
        writer.writeheader()
        writer.writerows(rows)

    metadata = {
        "run_id": run_id,
        "stage": "web_scraper.pdf_ocr_job",
        "created": runtime.iso(),
        "git_commit": git_commit or os.environ.get("KGPU_GIT_COMMIT") or _git_commit(),
        "notes": notes,
        "inputs": {
            "data_root": str(data_root()),
            "models": use_models(),
            "documents": [t.key for t in tasks],
            "layers": [layer.name for layer in chosen],
            "layers_are_the_full_cascade": len(chosen) == len(FinancialsBuilder.LAYERS),
        },
        # ⚠️ `runtime.environment()` names the CARD; `engine_report()` names what each half of
        # the OCR actually ran on, which is not the same question — the first Kaggle run had
        # VietOCR on a T4 and the DB detector on the worker's CPU.
        "environment": {**runtime.environment(), "ocr": engine_report()},
        "execution": timer.summary(),
        "results": rows,
    }
    (folder / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nrun folder -> {folder}", flush=True)
    return folder


def _ocr_device() -> str:
    """What the recogniser will actually run on — reported, never chosen here."""
    try:
        import torch

        return "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:  # noqa: BLE001
        return "unknown"


def engine_report() -> Dict[str, object]:
    """Which device each HALF of the OCR actually ran on. Cheap: it loads the 4.7 MB detector.

    ⚠️ **`ort.get_available_providers()` IS AN ADVERTISEMENT, NOT A MEASUREMENT — and it lied
    on the first Kaggle run** (2026-08-28). It listed `CUDAExecutionProvider` while
    `InferenceSession` could not create one: Kaggle's image is CUDA 12.8 and the
    `onnxruntime-gpu` pip resolves to needs cuDNN 9.* with CUDA **13**.*, so the session fell
    back to CPU, printed a warning into a wall of ANSI-coloured onnxruntime noise, and produced
    a perfectly correct — and slower — run. `session.get_providers()` is what the session
    ACTUALLY holds, so that is what goes in the artefact.

    ⚠️ **The two halves fail independently and only one of them is torch's.** Detection is
    onnxruntime and recognition is torch, so "the GPU is being used" is two questions: a run
    can have VietOCR on a T4 and the DB detector on the worker's CPU, which is exactly what the
    first Kaggle run did.
    """
    out: Dict[str, object] = {"det_providers": None, "recognizer_device": _ocr_device()}
    try:
        import onnxruntime as ort

        out["onnxruntime"] = ort.__version__
        out["onnxruntime_advertises"] = list(ort.get_available_providers())
    except Exception as exc:  # noqa: BLE001
        out["onnxruntime"] = f"unavailable: {type(exc).__name__}"
        return out
    try:
        from web_scraper.onnx_ocr import OnnxOcr

        out["det_providers"] = list(OnnxOcr().detector.session.get_providers())
    except Exception as exc:  # noqa: BLE001 — a probe must not take down a run
        out["det_providers"] = f"unavailable: {type(exc).__name__}: {exc}"
    return out


def _git_commit() -> Optional[str]:
    import subprocess

    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT,
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except Exception:  # noqa: BLE001 — a worker has no repo, and that is not a failure
        return None


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> int:
    """`cd src && python -m web_scraper.pdf_ocr_job --symbol VCB --periods Q1-2026`"""
    parser = argparse.ArgumentParser(
        description="OCR + parse CafeF filings into a run folder. Writes NO statement CSV.")
    parser.add_argument("--exchange", default="HOSE")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--periods", nargs="*", default=None,
                        help="e.g. Q1-2026. Omit for every period the ticker files.")
    parser.add_argument("--allow-parent", action="store_true",
                        help="fall back to the STANDALONE filing where no consolidated one "
                             "exists (documents(); consolidated still wins).")
    parser.add_argument("--layers", nargs="*", default=None,
                        help="restrict the cascade to these layer names, in cascade order.")
    parser.add_argument("--data-root", default=None)
    parser.add_argument("--models", default=None)
    parser.add_argument("--out", default=str(DEFAULT_OUT_ROOT))
    parser.add_argument("--notes", default="")
    parser.add_argument("--no-compare", action="store_true")
    args = parser.parse_args(argv)

    try:                                    # ⚠️ §5 rule 18 — the cascade logs Vietnamese
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    use_data_root(args.data_root)
    use_models(args.models)
    builder = FinancialsBuilder(logger=None)
    tasks = plan(builder, args.exchange.upper(), args.symbol.upper(),
                 periods=args.periods, allow_parent=args.allow_parent)
    run(tasks, out_root=args.out, layers=args.layers, notes=args.notes,
        compare_with_disk=not args.no_compare)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
