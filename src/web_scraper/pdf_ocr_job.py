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

## ⚠️ THE THREE CONTRACTS — input, log, output (standardised 2026-08-28)

### INPUT is ONE object, `JobSpec`, and it is validated before anything is spent

The CLI, the notebook and `kgpu` all build the same frozen `JobSpec` and hand it to `run()`.
There is no second way in. `JobSpec.validate()` resolves the data root, the models and the
TEMPLATE, and raises on anything it cannot answer — before a page is rendered.

⚠️ **THE TEMPLATE IS RESOLVED, NEVER DEFAULTED.** This module's first version ended
`builder.template_of(symbol) or "bank"`, which is a silent wrong answer for the 761 of 781
listed names that are not banks: it would map a corporate filing against the bank chart of
accounts and reject every statement as unreconcilable, hours later, reported as a parse
failure. `resolve_template()` tries the explicit override, then `templates.csv`, then
CafeF's own fingerprint — and **raises** if all three are silent. Which route answered is
recorded in the artefact, because "read off templates.csv" and "guessed from a line-item
count" are not the same claim.

### LOG is one line per event, flushed, and every percentage names its DENOMINATOR

`kaggle_gpu/README.md` §3 already records that this repo's three progress readouts have three
different denominators and only one of them predicts time. The same rule is applied here, and
the label is printed rather than assumed:

| line | denominator | predicts time? |
|---|---|---|
| `[doc 2/3   67%]` | **documents** | ❌ 4.2 min against 18.2 for a failing one |
| `[layer 12/47  26%]` | **positions in the cascade** | ❌ one layer re-OCRs 96 pages, the next re-maps a cache in ms |
| `[ocr onnx@200  page 40/96  42%  ~49 s left]` | **pages of one OCR pass** | ✅ **the only one** — 0.87 s/page, measured |

⚠️ **A LONG RUN USED TO PRINT NOTHING FOR 73 MINUTES** (§6-2-noviesdecies): the only live
signal was the `LastAccessTime` of the PDF. Page progress is what replaces that, and it is
rate-limited to a 10-point step or 15 s so a 96-page document over 10 passes does not emit 960
lines.

### OUTPUT is a run folder with a declared schema, written INCREMENTALLY

    <out_root>/<run_id>/
        metadata.json        schema_version, the resolved JobSpec, environment (incl. `ocr`),
                             execution, and the per-statement results table
        summary.csv          one row per (period, report) — the file a reader opens first
        run.log              every log line, written as it happens (§5 rule 20)
        documents/<EX>_<SYM>__<period>.json   accepted values, refusal reasons, compare verdict

⚠️ **NO STATEMENT CSV IS EVER WRITTEN — see the top of this docstring.**
"""

from __future__ import annotations

# ===== Standard Library =====
import argparse
import csv
import json
import os
import re
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

# ⚠️ **THE FILES A MODELS DIRECTORY MUST HOLD, NAMED ONCE.** `use_models` looks these up and
# `kgpu.export._export_documents` ships them into the payload under exactly these names — two
# readers of one fact, and they were two hand-written lists until 2026-08-29, when the
# recogniser's CONFIG was added to the first and not the second. Every documents payload built
# that day was one file short of what the worker needs, and the only thing that said so was the
# rehearsal's own assertion (`VCR-1`).
MODEL_FILES = {
    "det": "deepdoc_det.onnx",              # the DeepDoc DB detector, gitignored (4.7 MB)
    "vietocr": "vgg_seq2seq.pth",           # the recogniser's checkpoint, gitignored (90 MB)
    "vietocr_config": "vietocr_vgg_seq2seq.yml",   # its config — TRACKED, 3 KB, see VCR-1
}

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
    chosen: Dict[str, Optional[str]] = {k: None for k in MODEL_FILES}

    det = models / MODEL_FILES["det"]
    if det.is_file():
        os.environ["CAFEF_ONNX_DET"] = str(det)
        chosen["det"] = str(det)
    # ⚠️ The payload's `models/` first, then wherever this machine already has one. Both are a
    # LOCAL file, which is the whole point — vietocr's fall-through is a 90 MB download, and
    # reporting "no checkpoint" on a machine that has had one cached since the first parse
    # would be a warning that is simply untrue.
    weights = models / MODEL_FILES["vietocr"]
    weights = weights if weights.is_file() else find_vietocr_weights()
    if weights is not None:
        os.environ["CAFEF_ONNX_VIETOCR_WEIGHTS"] = str(weights)
        chosen["vietocr"] = str(weights)

    # ⚠️ **THE RECOGNISER'S CONFIG IS A THIRD FILE, AND A NETWORK CALL WITHOUT IT.**
    # `Cfg.load_config_from_name` fetches base.yml + <arch>.yml from vocr.vn on every Predictor
    # build and caches neither, so the two model files alone never made the engine offline —
    # measured 2026-08-29, when that host's certificate expired and the cascade fell silently
    # through to tesseract.
    config = models / MODEL_FILES["vietocr_config"]
    if config.is_file():
        os.environ["CAFEF_ONNX_VIETOCR_CONFIG"] = str(config)
        chosen["vietocr_config"] = str(config)

    engine = sys.modules.get("web_scraper.onnx_ocr")
    if engine is not None:
        if chosen["det"]:
            engine.DET_MODEL = chosen["det"]
        if chosen["vietocr"]:
            engine.VIETOCR_WEIGHTS = chosen["vietocr"]
        if chosen["vietocr_config"]:
            engine.VIETOCR_CONFIG = chosen["vietocr_config"]
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
# Which chart of accounts
# ──────────────────────────────────────────────────────────────────────────────

TEMPLATES = ("bank", "corp", "securities", "insurance")


def resolve_template(builder: FinancialsBuilder, symbol: str,
                     override: Optional[str] = None) -> tuple:
    """`(template, how)` — and it RAISES rather than defaulting to anything.

    ⚠️ **THE FIRST VERSION OF THIS MODULE ENDED `or "bank"` AND THAT IS A SILENT WRONG
    ANSWER.** 761 of 781 listed names are not banks; mapping a corporate filing against the
    bank chart of accounts rejects every statement as unreconcilable, hours later, and reports
    it as a parse failure rather than as the wrong schema. `utils/inputs.py` is named for
    exactly this shape.

    Three routes, best first, and `how` records which one answered because they are not the
    same claim:

      * `override` — the caller stated it, and it is checked against the four that exist;
      * `templates.csv` — written by `build_templates_index` from the tickers actually parsed;
      * `detect_template` — CafeF's own line-item fingerprint. ⚠️ **THIS ONE IS A NETWORK
        CALL**, so on a Kaggle worker it either fails or costs a round trip; `kgpu` resolves
        the template at EXPORT time and ships the answer in the manifest for that reason.

    ⚠️ **A resolved template is not a safe one.** `TPL-1`: seven reconcile anchors are
    bank-shaped, and on `corp` and `insurance` the cash-flow one fuzzy-matches the OPENING
    balance and returns it as the closing one. Resolving the template correctly is necessary
    and nowhere near sufficient.
    """
    if override:
        if override not in TEMPLATES:
            raise ValueError(
                f"unknown template {override!r} — the four that exist are {TEMPLATES}.")
        return override, "override"

    if os.path.exists(fin.TEMPLATES_INDEX):
        with open(fin.TEMPLATES_INDEX, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("symbol") == symbol and row.get("template"):
                    return row["template"], "templates.csv"

    try:
        from web_scraper.cafef_schema import detect_template

        guess = detect_template(symbol)
    except Exception as exc:  # noqa: BLE001 — an unreachable network is an unknown, not a crash
        guess = None
        why = f"{type(exc).__name__}: {exc}"
    else:
        why = "returned None"
    if guess:
        return guess, "detect_template (CafeF fingerprint, over the network)"

    raise ValueError(
        f"cannot resolve the accounting template for {symbol!r}: it is not in "
        f"{fin.TEMPLATES_INDEX}, and detect_template {why}.\n"
        f"  Pass one explicitly — {TEMPLATES} — but read ISSUES.md `TPL-1` first: two of the "
        f"seven reconcile anchors are bank-shaped, and on corp/insurance the cash-flow one "
        f"returns the OPENING balance as the closing one."
    )


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


# ⚠️ **TWO SPELLINGS OF ONE QUARTER, AND NOTHING ELSE.** `2014-Q3` is the canonical form and
# `2014-03` the zero-padded one; both SORT, both are unambiguous, and `normalise_quarter` folds
# the second onto the first before anything compares — so two spellings can never become two
# job names, two payload directories or two run folders for one quarter.
#
# ⚠️ **EVERY OTHER SHAPE IS REFUSED, THE REPO-NATIVE `Q3-2014` INCLUDED.** It names the same
# quarter, so a lenient parser would be easy to write — and then a caller who used the wrong
# form would never find out, while a caller who made a TYPO would get "files no document for
# [...]" and go looking at CafeF for a filing that is sitting there. `2014-3` is refused in the
# other direction: one digit is a keystroke away from a MONTH, and the unit here is a quarter.
QUARTER_RE = re.compile(r"^(\d{4})-(?:Q([1-4])|0([1-4]))$")

# The sentence every error about this form has to say, written once.
QUARTER_FORM = ("quarters are written YYYY-QQ — '2014-Q3', or the zero-padded '2014-03'. The "
                "repo-native period key is the other way round ('Q3-2014') and is what "
                "`periods` takes")


def normalise_quarter(text: str) -> str:
    """`2014-Q3` / `2014-03` -> `2014-Q3`. Raises on anything else.

    ⚠️ **THE CANONICAL FORM IS THE ONE WITH THE `Q`**, because that is what `_quarter_of`
    builds from a document's own period and what every comparison here is against. The fold
    happens once, at the edge, so nothing downstream has to know there were two spellings.
    """
    match = QUARTER_RE.match(str(text).strip())
    if not match:
        raise ValueError(f"{text!r} is not a quarter: {QUARTER_FORM}.")
    return f"{match.group(1)}-Q{match.group(2) or match.group(3)}"


def canonical_quarters(quarters: Optional[Sequence[str]]) -> Optional[List[str]]:
    """A sorted, de-duplicated, canonical quarter list — or None for "every quarter".

    ⚠️ **EMPTY AND ABSENT ARE ONE ANSWER AND IT IS `None`.** `[]` is falsy everywhere it is
    read, so it already behaved this way by accident; returning None makes it the contract.
    """
    if not quarters:
        return None
    return sorted({normalise_quarter(q) for q in quarters})


def _quarter_of(doc: dict) -> str:
    """The `YYYY-QQ` quarter a document contributes — from its PERIOD, never the index column.

    ⚠️ **THE TWO DISAGREE AND `documents()` IS WHY.** CafeF files an annual report under
    quarter 5; `documents()` folds it onto that year's Q4 and rewrites `period`, so `period`
    is the normalised key every other comparison here uses (`_period_key`, `period_min`, the
    `periods` filter). The raw column is also not always a year — CafeF files 10 of the 84,076
    documents with a `Year` of `0`, `202` or `203` (CLAUDE.md §6-2-septies) — and reading it
    would file those under whatever `int()` makes of them.

    ⚠️ **`YYYY-QQ`, NOT THE REPO-NATIVE `QQ-YYYY`.** They name the same thing and only one of
    them SORTS: `["Q1-2014", "Q3-2009"]` orders the 2014 quarter first, which is why every
    ordering in this package goes through `_period_key` rather than through the string. A batch
    filter is written and read by a person, so it takes the form that reads in order.
    `as_quarter` converts, and `plan` refuses the other form outright rather than silently
    matching nothing.
    """
    y, q = fin._period_key(doc["period"])
    return f"{y}-Q{q}"


def as_quarter(period: str) -> str:
    """`Q3-2014` -> `2014-Q3`. The repo-native period key, in the sortable form."""
    y, q = fin._period_key(period)
    return f"{y}-Q{q}"


def plan(builder: FinancialsBuilder, exchange: str, symbol: str,
         periods: Optional[Sequence[str]] = None,
         quarters: Optional[Sequence[str]] = None,
         allow_parent: bool = False,
         period_min: Optional[str] = fin.FINANCIALS_PERIOD_MIN,
         template: Optional[str] = None) -> List[DocumentTask]:
    """The filings to open, oldest first — `documents()` chooses them, this only filters.

    ⚠️ **THE CHOICE IS NOT RE-IMPLEMENTED.** Consolidated beats standalone, the audited annual
    stands in for Q4, and neither rule may be re-stated here: `documents()` carries a measured
    guard against an annual changing the ENTITY of a Q4 row (86 of 13,912 periods moved before
    it existed), and a second copy of that logic is a second place for it to be wrong.

    A `periods` or `quarters` filter matching NOTHING raises — a filter that matches nothing is
    a run that parses nothing and reports success.

    ⚠️ **`quarters` IS THE BATCH FILTER, IN `YYYY-QQ`, AND IT WAS `years` UNTIL 2026-08-29.** The
    unit was a YEAR on one argument: `orchestration` §2a records that the statement BUILD skips
    complete years rather than quarters, because `_decumulate` needs Q1..Q(q-1) of the same year
    and a partial skip deletes the very quarter a run exists to fix. ⚠️ **That argument never
    bound THIS module, and the wider unit cost real GPU time.** Nothing here de-cumulates and
    nothing here writes — the artefact is a run folder — so a quarter is the honest grain, and
    asking for 17 quarters no longer opens the 27 filings of the six years they fall in.
    ⚠️ **The de-cumulation hazard did not vanish; it sits where the WRITE is**, and
    `pdf_ocr_merge` already refuses a cumulative income statement that a one-document run has no
    priors to de-cumulate against.

    **Empty or absent means every quarter the ticker files**, which is exactly what
    `documents()` already returns: the default is not re-derived, it is the absence of a filter.

    `quarters` and `periods` INTERSECT — `quarters=["2014-Q3"]` with `periods=["Q3-2014"]` is
    Q3-2014 and not a contradiction. Each is checked against what survives the one before it, so
    the error names the filter that emptied the plan rather than the first one written.
    """
    docs = builder.documents(exchange, symbol, allow_parent=allow_parent,
                             period_min=period_min)
    if quarters:
        # ⚠️ THE REPO-NATIVE `Q3-2014` IS REFUSED, NOT ACCEPTED QUIETLY. Both forms name the same
        # quarter, so a lenient parser would be easy to write — and then a caller who used the
        # wrong one would never find out, while a caller who made a TYPO gets "files no document
        # for [...]" and goes looking at CafeF for a filing that is sitting there. The form is
        # part of the contract, so it is checked before the corpus is.
        bad = [q for q in quarters if not QUARTER_RE.match(str(q).strip())]
        if bad:
            raise ValueError(
                f"quarters must be written YYYY-QQ, got {bad}.\n"
                f"  {QUARTER_FORM}; `quarters` SORTS, which is why it is the batch form."
            )
        wanted_q = canonical_quarters(quarters) or []
        have_q = sorted({_quarter_of(d) for d in docs})
        unknown_q = [q for q in wanted_q if q not in have_q]
        if unknown_q:
            raise ValueError(
                f"{exchange}_{symbol} files no document for quarter(s) {unknown_q} "
                f"(allow_parent={allow_parent}, period_min={period_min!r}).\n"
                f"  Quarters available: {have_q}"
            )
        docs = [d for d in docs if _quarter_of(d) in set(wanted_q)]
    if periods:
        wanted = list(dict.fromkeys(periods))
        have = {d["period"] for d in docs}
        unknown = [p for p in wanted if p not in have]
        if unknown:
            raise ValueError(
                f"{exchange}_{symbol} files no document for {unknown} "
                f"(allow_parent={allow_parent}, period_min={period_min!r}"
                f"{', quarters=' + str(sorted(set(quarters))) if quarters else ''}).\n"
                f"  Periods available: {sorted(have)[:8]}"
                f"{' …' if len(have) > 8 else ''}"
            )
        docs = [d for d in docs if d["period"] in wanted]

    # ⚠️ RESOLVED, NEVER DEFAULTED — see `resolve_template`. This line read
    # `builder.template_of(symbol) or "bank"` until 2026-08-28, which is a silent wrong answer
    # for every non-bank ticker and would have been one for VIC.
    template = template or resolve_template(builder, symbol)[0]
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


def parsed_reports(builder: FinancialsBuilder, task: DocumentTask) -> List[str]:
    """Which of the three statements this quarter ALREADY reads `pdf` on disk.

    `pdf` and nothing else: a `missing` row is a quarter nothing was written for, and rule 24
    leaves no third answer — a `cafef` row would be a transcription, which is forbidden as a
    source and is therefore not evidence that a quarter is done either.
    """
    return [report for report in REPORTS
            if (builder._existing(task.exchange, task.symbol, task.template, report)
                .get(task.period, {}).get("source") == "pdf")]


def partition_by_disk(builder: FinancialsBuilder,
                      tasks: Sequence[DocumentTask]) -> tuple:
    """`(to_parse, already_complete)` — the quarters a re-parse could still win, and the rest.

    ⚠️ **THE UNIT IS A QUARTER HERE AND A YEAR IN `build()`, AND THE DIFFERENCE IS MEASURED
    RATHER THAN STYLISTIC.** `FinancialsBuilder._skippable_years` skips whole YEARS because
    `_decumulate` turns a cumulative income statement into a standalone quarter as
    `YTD − (Q1..Q(q-1))`, taking those priors from THAT run — so dropping Q1..Q3 while keeping
    Q4 would delete the very quarter the run exists to fix.

    ⚠️ **NOTHING IN THIS MODULE DE-CUMULATES**, which is what makes the finer grain safe:
    `run_document` writes what the cascade accepted and no more, `pdf_ocr_merge` REFUSES a
    cumulative income statement outright, `seed_history` rebuilds `sane`'s band from DISK, and
    a Q1's `open_ref` comes from disk too. No quarter here depends on another quarter of the
    same run. The grain is worth having: at 4-18 min a document, a year held whole for one
    missing cash flow costs three filings that had nothing left to win.

    ⚠️ **A QUARTER IS "COMPLETE" ONLY WHEN ALL THREE STATEMENTS READ `pdf`** — one filing
    produces all three, so a quarter missing its cash flow must re-open the document, and the
    two statements that come back with it are then judged by `pdf_ocr_merge` on their own
    merits (identical -> skipped, different -> refused unless overwrite was asked for).
    """
    to_parse: List[DocumentTask] = []
    complete: List[DocumentTask] = []
    for task in tasks:
        (complete if len(parsed_reports(builder, task)) == len(REPORTS)
         else to_parse).append(task)
    return to_parse, complete


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

    ⚠️ **`line()` IS THE ONLY EMIT POINT, WHICH IS WHAT MAKES `Progress` A SUBCLASS.** The two
    sinks each re-implemented the four `log_*` methods until 2026-08-29, so a change to how a
    warning is written had two places to be made and one of them would have been missed.
    """

    def __init__(self, echo: bool = True):
        self.lines: List[str] = []
        self.echo = echo

    def line(self, text: str) -> None:
        self.lines.append(text)
        if self.echo:
            print(text, flush=True)

    # ---- the `Logger` shape the parser expects ----------------------------
    def log_info(self, message: str) -> None:
        self.line(message)

    def log_warning(self, message: str) -> None:
        self.line(f"WARNING: {message}")

    def log_error(self, message: str) -> None:
        self.line(f"ERROR: {message}")

    def log_debug(self, message: str) -> None:
        # Kept, never echoed and never written to the log file: the cascade emits a DEBUG line
        # per row, which would bury the refusal reasons this log exists to carry.
        self.lines.append(f"DEBUG: {message}")

    def take(self) -> List[str]:
        lines, self.lines = self.lines, []
        return lines


class Progress(CollectingLogger):
    """The run log. One line per event, flushed, and every percentage NAMES its denominator.

    ⚠️ **THREE DENOMINATORS, ONE OF WHICH PREDICTS TIME**, and the line says which:

      * `[doc i/N]` — DOCUMENTS. A clean filing costs 4.2 min and a failing one 18.2
        (§6-2-quindecies), so this is a position in a list, never a time estimate.
      * `[layer k/47]` — POSITIONS IN THE CASCADE. One layer re-OCRs every page; the next
        re-maps a cached parse in milliseconds. `cached` is printed for exactly that reason.
      * `[ocr <layer> page p/P]` — **PAGES OF ONE OCR PASS, and this one is real.** Pages of
        one document cost about the same (0.87 s/page at onnx@200), so the fraction is a
        fraction of the work and the ETA is an extrapolation rather than a guess.

    ⚠️ **RATE-LIMITED, because a 96-page document over ~10 passes is 960 page events.** A page
    line is emitted on a 10-point step or after 15 s, whichever comes first — the same shape
    `kgpu wait` uses so a long poll does not fill a log with lines nobody reads.

    ⚠️ **THE PAGE ETA IS AN UPPER BOUND, and the denominator is why.** It extrapolates over
    `doc.page_count`, while `scan` STOPS at the notes boundary once all three statements are
    behind it — VCB Q1-2026 reads 12 of 53 pages and finishes while the last line printed said
    *"~61 s left"*. That is the honest direction for a progress estimate to be wrong in, and it
    is stated rather than hidden: the alternative is to not know the denominator until the page
    that ends the scan, which is one page before the end.
    """

    PAGE_STEP = 10          # percentage points
    PAGE_SECONDS = 15.0

    def __init__(self, total_documents: int, log_path: Optional[Path] = None,
                 echo: bool = True):
        super().__init__(echo=echo)
        self.total_documents = total_documents
        self._handle = None
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            # ⚠️ Line-buffered and utf-8: §5 rule 20 (a 4-hour run lost to a re-buffering
            # wrapper) and §5 rule 18 (the cascade logs Vietnamese account labels).
            self._handle = log_path.open("w", encoding="utf-8", buffering=1)
        self._doc = ""
        self._page_at = 0.0
        self._page_pct = -100
        self._page_started = 0.0

    # ---- emitting ---------------------------------------------------------
    def line(self, text: str) -> None:
        if self._handle is not None:
            self._handle.write(text + "\n")
        super().line(text)

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    # ---- the three denominators -------------------------------------------
    def document(self, index: int, task: "DocumentTask", size_mb: float) -> None:
        self._doc = task.period
        self.line(f"[doc {index}/{self.total_documents} "
                  f"{index / self.total_documents:>4.0%} of DOCUMENTS, not of time] "
                  f"{task.key}  {task.file}  {size_mb:.1f} MB  "
                  f"{task.template}/{'consolidated' if task.consolidated == 'True' else 'parent'}"
                  f"{'  CUMULATIVE' if task.cumulative else ''}")

    def layer(self, index: int, total: int, layer, cached: bool) -> None:
        self._page_pct = -100
        self._page_started = time.perf_counter()
        self.line(f"  [layer {index}/{total} {index / total:>4.0%} of POSITIONS] "
                  f"{layer.name}" + ("   (cached parse, re-map only)" if cached else ""))

    def page(self, index: int, total: int) -> None:
        pct = int(100 * (index + 1) / max(1, total))
        now = time.perf_counter()
        if pct - self._page_pct < self.PAGE_STEP and now - self._page_at < self.PAGE_SECONDS:
            return
        self._page_at, self._page_pct = now, pct
        elapsed = now - self._page_started
        eta = ""
        if index >= 2 and elapsed > 0:
            left = (total - index - 1) * elapsed / (index + 1)
            eta = f"  ~{left:.0f} s left"
        self.line(f"    [ocr page {index + 1}/{total} {pct:>3}% of PAGES — the only "
                  f"fraction here that predicts time]{eta}")


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
    # ⚠️ `(layer, exception)` for every layer whose PARSE RAISED — an engine failure, not a
    # verdict on the filing. A run with any of these was decided by whichever layer the broken
    # tool did not reach, so `pdf_ocr_merge` refuses to write from it.
    engine_errors: List[tuple] = field(default_factory=list)

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
            "engine_errors": [list(e) for e in self.engine_errors],
            "facts": self.facts,
            "error": self.error,
            "log": self.log,
        }


def _rows_digest(statement) -> str:
    """12 hex characters over EVERY parsed row — label, numbering and figures.

    ⚠️ **THE MAPPED CELLS ARE THE MINORITY, AND THEY ARE ALL THE ARTEFACT USED TO CARRY.** A
    statement's `rows` are what the OCR read; `map_to_schema` then places some of them on chart
    columns and drops the rest. Two runs agreeing on the mapped cells therefore agree on the
    part that reconciles and say NOTHING about the rest — a label read differently on an
    unmapped line, or a figure on a line the chart has no column for, is invisible. This is the
    comparison that is not.

    ⚠️ It is a DIGEST, so it answers "identical or not" and never "how do they differ". That is
    the right trade for an artefact that must stay small; a run that disagrees is re-read with
    both statements in hand.
    """
    import hashlib

    blob = "\n".join(
        f"{r.number}|{r.key}|{r.label}|" + ",".join("" if v is None else str(v) for v in r.values)
        for r in statement.rows
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


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

    # ⚠️ Read off the builder rather than scraped out of the log: a WARNING is prose, and the
    # decision downstream ("was this parse decided by a broken tool?") must not depend on
    # matching a sentence.
    result.engine_errors = list(getattr(builder, "layer_errors", []))
    if result.engine_errors:
        engines = sorted({name.split("@")[0] for name, _ in result.engine_errors})
        logger.log_warning(
            f"{task.period}: {len(result.engine_errors)} layer(s) RAISED rather than refusing "
            f"({', '.join(engines)}) — whatever won did so because those layers could not run. "
            f"The upsert refuses this document; see `engine_errors` in its JSON.")
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
            # ⚠️ **EVERY ROW THE PARSER READ, NOT ONLY THE ONES THAT MAPPED.** `values` below
            # holds the mapped cells — 98 for VCB Q1-2026 — and two runs agreeing on those says
            # nothing about the lines that mapped to no column, which is most of a statement.
            # This digest covers the label, the filing's own numbering and every figure of every
            # parsed row, so two runs can be compared on what the OCR actually READ rather than
            # on what survived the mapping. Added 2026-08-28 to answer exactly that question.
            "rows": len(statement.rows),
            "rows_sha": _rows_digest(statement),
            # ⚠️ **THE ROWS THEMSELVES, BECAUSE A DIGEST THAT DISAGREES IS USELESS ON ITS OWN.**
            # The first cross-machine comparison at row level found the cash flow's digest
            # differing while all 98 mapped cells matched — and the artefact could not say
            # WHERE, so locating it needed another run on each machine. That is §6-2-quindecies'
            # lesson exactly: the parser computed the answer and threw it away. ~10 KB per
            # document, against an artefact that is already ~9 KB and a re-run that costs
            # minutes on two machines.
            "row_dump": [
                [r.number, r.key, r.label,
                 [None if v is None else int(v) for v in r.values]]
                for r in statement.rows
            ],
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
# The run — one input object, one log, one folder
# ──────────────────────────────────────────────────────────────────────────────

# ⚠️ Bump when the run folder's SHAPE changes, so a reader of an old artefact is not left
# guessing whether a missing key means "absent" or "this run predates it" (§5 rule 2).
#   1 -> 2 (2026-08-29): documents carry `engine_errors`, and `inputs` carries `overwrite`,
#           `skipped_already_parsed`, `merged_into_csv` and `merge_backup`. ⚠️ A v1 folder has
#           no `engine_errors` key and `pdf_ocr_merge` reads that as "none recorded", which is
#           the only reading available — it CANNOT mean "none happened", and the version is
#           what lets a reader tell those apart.
SCHEMA_VERSION = 2


@dataclass(frozen=True)
class JobSpec:
    """THE input. The CLI, the notebook and `kgpu` all build this and nothing else.

    ⚠️ **ONE OBJECT, AND IT IS VALIDATED BEFORE ANYTHING IS SPENT.** Three callers with three
    argument lists is three places for a default to differ, and the defaults here decide which
    chart of accounts a filing is read against — a wrong one costs the whole OCR and reports
    itself as a parse failure. `prepare()` resolves the root, the models, the template and the
    document list, and raises on any of them.
    """

    exchange: str = "HOSE"
    symbol: str = "VCB"
    periods: Optional[Sequence[str]] = None      # None = every period the ticker files
    # ⚠️ EMPTY OR ABSENT = EVERY QUARTER, and that answer is `documents()`'s rather than a list
    # this class computes. ⚠️ **This was `years: Sequence[int]` until 2026-08-29**, on the
    # argument that the statement BUILD skips whole years (`orchestration` §2a: `_decumulate`
    # needs Q1..Q(q-1) of the same year). That is a fact about the WRITE, and this module does
    # not write — so the wider unit only ever bought extra OCR. `plan` refuses anything that is
    # not `YYYY-QQ`.
    quarters: Optional[Sequence[str]] = None     # None/[] = every quarter the ticker files
    allow_parent: bool = False
    # ⚠️ **OVERWRITE DECIDES TWO THINGS AND THEY ARE THE SAME QUESTION.** False FILLS THE GAPS:
    # a quarter already reading `pdf` in all three statements is dropped before any OCR
    # (`partition_by_disk`), and the upsert below still refuses to replace a `pdf` row that
    # disagrees with this run. True parses every selected quarter and lets the upsert replace
    # what disk holds (`pdf_ocr_merge`'s `force_differs`). ⚠️ Re-parsing is not free and it is
    # not obviously better: a quarter on disk was written by a run whose `sane` band was the
    # RUN'S OWN, and this module reconstructs the band from disk instead (`seed_history`), so
    # the two gates look at different populations. Overwrite when the PARSER changed.
    overwrite: bool = False
    period_min: Optional[str] = fin.FINANCIALS_PERIOD_MIN
    # ⚠️ None means RESOLVE it (templates.csv, then CafeF's fingerprint), never "assume bank".
    template: Optional[str] = None
    layers: Optional[Sequence[str]] = None       # None = the full cascade, in cascade order
    data_root: Optional[str] = None
    models_dir: Optional[str] = None
    out_root: Optional[str] = None
    compare_with_disk: bool = True
    # ⚠️ **UPSERT EACH QUARTER INTO THE STATEMENT CSVs AS IT FINISHES — OFF BY DEFAULT.** The
    # module docstring says why the default is off: the artefact is the product, and a run that
    # writes nothing cannot silently downgrade a quarter. What turning it ON buys is the other
    # half of that trade — a 12-hour run that is stopped at hour 6 keeps every quarter it
    # finished, because `_write` is an upsert onto a `.tmp` file replaced atomically, so an
    # interrupt can lose the quarter in flight and nothing else.
    # ⚠️ It goes through `pdf_ocr_merge`, so all three refusals are in force per quarter — a
    # cumulative income statement, an empty `sane` band, and a figure that DIFFERS from a good
    # `pdf` row are skipped and SAID, not written.
    merge_into_csv: bool = False
    notes: str = ""
    run_id: Optional[str] = None

    def prepare(self) -> "PreparedJob":
        """Resolve every input and RAISE on anything unanswerable. Cheap: no OCR, no PDF."""
        root = use_data_root(self.data_root)
        models = use_models(self.models_dir)
        builder = FinancialsBuilder(logger=None)
        template, how = (
            (self.template, "override") if self.template
            else resolve_template(builder, self.symbol.upper()))
        tasks = plan(builder, self.exchange.upper(), self.symbol.upper(),
                     periods=self.periods, quarters=self.quarters,
                     allow_parent=self.allow_parent,
                     period_min=self.period_min, template=template)
        # ⚠️ THE SKIP IS APPLIED HERE, NOT IN `plan`, so that the quarters it dropped survive
        # into the artefact. "Nothing was selected" and "everything selected was already done"
        # are different facts and a reader of the run folder must be able to tell them apart.
        skipped: List[DocumentTask] = []
        if not self.overwrite:
            tasks, skipped = partition_by_disk(builder, tasks)
        if not tasks:
            if skipped:
                raise ValueError(
                    f"{self.exchange.upper()}_{self.symbol.upper()}: all {len(skipped)} "
                    f"selected quarter(s) already read `pdf` in all three statements, so a "
                    f"re-parse has nothing left to win — "
                    f"{', '.join(as_quarter(t.period) for t in skipped[:8])}"
                    f"{' …' if len(skipped) > 8 else ''}.\n"
                    f"  Pass overwrite=True (OVERWRITE in the notebook) to parse them anyway.")
            raise ValueError(
                f"{self.exchange}_{self.symbol} has no filing to parse — a run that parses "
                f"nothing and reports success is the failure this raises to prevent.")
        return PreparedJob(spec=self, builder=builder, tasks=tasks, template=template,
                           template_how=how, data_root=root, models=models,
                           layers=select_layers(self.layers), skipped=skipped)

    def to_json(self) -> dict:
        return {
            "exchange": self.exchange, "symbol": self.symbol,
            "periods": list(self.periods) if self.periods else None,
            "quarters": canonical_quarters(self.quarters),
            "allow_parent": self.allow_parent, "period_min": self.period_min,
            "overwrite": self.overwrite, "merge_into_csv": self.merge_into_csv,
            "template_requested": self.template,
            "layers_requested": list(self.layers) if self.layers else None,
            "compare_with_disk": self.compare_with_disk, "notes": self.notes,
        }


@dataclass(frozen=True)
class PreparedJob:
    """A `JobSpec` with every input resolved — what `run()` actually consumes."""

    spec: JobSpec
    builder: FinancialsBuilder
    tasks: List[DocumentTask]
    template: str
    template_how: str
    data_root: Path
    models: Dict[str, Optional[str]]
    layers: List[ParseLayer]
    # Selected, then dropped because disk already holds all three statements as `pdf`. Empty
    # whenever `overwrite` is on, because nothing is dropped then.
    skipped: List[DocumentTask] = field(default_factory=list)

    def describe(self) -> List[str]:
        """The header every run prints — the resolved inputs, before any of them is used."""
        return [
            f"symbol       : {self.spec.exchange.upper()}_{self.spec.symbol.upper()}",
            f"template     : {self.template}   ({self.template_how})",
            f"documents    : {len(self.tasks)}  "
            f"({', '.join(t.period for t in self.tasks[:8])}"
            f"{' …' if len(self.tasks) > 8 else ''})",
            f"quarters     : {canonical_quarters(self.spec.quarters) or 'all'}"
            f"   selected {sorted(as_quarter(t.period) for t in self.tasks)}",
            f"skipped      : {len(self.skipped)} quarter(s) already `pdf` in all three "
            f"statements"
            + (f" ({', '.join(as_quarter(t.period) for t in self.skipped[:8])}"
               f"{' …' if len(self.skipped) > 8 else ''})" if self.skipped else "")
            + ("   [overwrite=True — nothing is skipped]" if self.spec.overwrite else ""),
            f"cascade      : {len(self.layers)} of {len(FinancialsBuilder.LAYERS)} layers",
            f"data root    : {self.data_root}",
            f"models       : det={_name(self.models.get('det'))} "
            f"vietocr={_name(self.models.get('vietocr'))}",
            # ⚠️ The recogniser's CONFIG is a third file and a network call without it.
            f"vietocr cfg  : {self.models.get('vietocr_config') or NO_VIETOCR_CONFIG}",
        ]


NO_VIETOCR_CONFIG = ("⚠️ NONE — vietocr will FETCH base.yml from vocr.vn on every "
                     "Predictor build, and a failure there falls the cascade through to "
                     "tesseract")


def _name(path: Optional[str]) -> str:
    return Path(path).name if path else "⚠️ NONE — the engine would try to download one"


def _upsert_period(folder: Path, task: DocumentTask, *, overwrite: bool,
                   log: "Progress", backup: bool) -> Optional[Path]:
    """Upsert ONE finished quarter into the statement CSVs, through `pdf_ocr_merge`.

    ⚠️ **IT IS THE SAME MODULE `kgpu pull` USES, SCOPED TO ONE PERIOD** — so the three
    refusals (a cumulative income statement, an empty `sane` band, a figure that DIFFERS from a
    good `pdf` row) are in force here exactly as they are there, and a quarter that clears them
    is written by `FinancialsBuilder._write(merge=True)`, the upsert `build()` itself uses.

    ⚠️ **THE BACKUP IS TAKEN ONCE PER RUN, NOT ONCE PER QUARTER.** A backup exists to answer
    "what did this run change?", and 70 timestamped copies of three CSVs answer it worse than
    one taken before the first write.
    """
    from web_scraper import pdf_ocr_merge

    result = pdf_ocr_merge.merge_run(folder, apply=True, periods=[task.period],
                                     force_differs=overwrite, backup=backup, quiet=True)
    for line in result.lines()[1:]:            # [0] repeats the ticker header
        log.line("  " + line.strip() if line.strip() else line)
    return result.backup


def run(spec: JobSpec, git_commit: Optional[str] = None) -> Path:
    """Parse every planned filing, writing each document's JSON AS IT FINISHES.

    ⚠️ **ONE FILE PER DOCUMENT, WRITTEN BEFORE THE NEXT ONE STARTS** — §5 rule 20. This stage
    has single documents that cost 33 minutes, and a run that keeps its results in memory
    loses all of them to the first crash.

    The folder holds a `metadata.json`, which is also what makes it a RUN FOLDER to
    `kgpu pull` — that merge copies any directory carrying one.
    """
    from utils import runtime

    prepared = spec.prepare()
    builder, tasks = prepared.builder, prepared.tasks

    symbols = sorted({f"{t.exchange}_{t.symbol}" for t in tasks})
    run_id = spec.run_id or (f"{runtime.folder_stamp()}__"
                             f"{'_'.join(symbols).lower()}__pdf_ocr")
    folder = Path(spec.out_root or DEFAULT_OUT_ROOT) / run_id
    (folder / "documents").mkdir(parents=True, exist_ok=True)

    log = Progress(len(tasks), log_path=folder / "run.log")
    # ⚠️ **A WORKER MAY NOT WRITE THE STATEMENT CSVs, AND THE ROOT IS HOW WE KNOW.** On Kaggle
    # `CAFEF_DATA_ROOT` points at the unpacked payload, so an upsert there would edit a copy
    # that is deleted with the kernel — and report success. The write belongs to whoever holds
    # the real `raw_data/`, which is what `kgpu pull` does after the run folder comes home.
    merge_into_csv = spec.merge_into_csv
    if merge_into_csv and prepared.data_root != DEFAULT_DATA_ROOT.resolve():
        merge_into_csv = False
    timer = runtime.RunTimer("web_scraper.pdf_ocr_job", device=_ocr_device()).start()
    builder.LAYERS = prepared.layers          # instance attribute; the class list is intact
    rows: List[dict] = []
    # ⚠️ ONE backup per RUN, taken by the FIRST quarter that actually writes something. Stays
    # None while every quarter is refused or identical, which is correct: nothing was changed,
    # so there is nothing to be able to restore.
    merge_backup: Optional[Path] = None
    try:
        for line in prepared.describe():
            log.line(line)
        if spec.merge_into_csv:
            log.line("upsert       : " + (
                "ON — each finished quarter is upserted into the statement CSVs"
                if merge_into_csv else
                "REFUSED — this data root is a payload, not the repo's own raw_data/. A "
                "worker writes a copy that dies with the kernel; `kgpu pull` writes here."))
        log.line("")

        for index, task in enumerate(tasks, start=1):
            # ⚠️ Set on the BUILDER, which hands them to every parser it creates — including
            # the onnx one it builds lazily on the first layer that needs it. Setting them on
            # `builder._parsers` instead reaches whatever happens to exist already, which on a
            # fresh builder is the env-default parser and not the one that does the work.
            builder._logger = log
            builder.on_layer = log.layer
            builder.on_page = log.page
            log.document(index, task, os.path.getsize(task.path) / 1024 ** 2)

            history = seed_history(builder, task.exchange, task.symbol, task.template,
                                   before=task.period)
            open_ref = open_reference(builder, task.exchange, task.symbol, task.template,
                                      task.period)
            sizes = {r: len(v.get(task.consolidated, [])) for r, v in history.items()}
            if not any(sizes.values()):
                log.log_warning(
                    f"{task.period}: the magnitude band is EMPTY, so `sane` will FAIL OPEN — "
                    f"this ticker has no accepted quarters on disk to reconstruct one from.")
            else:
                log.line("  band: " + ", ".join(f"{r.split('_')[0]} {n}"
                                                 for r, n in sizes.items())
                         + f"   open_ref={open_ref if open_ref is not None else '—'}")

            result = run_document(builder, task, history, open_ref, logger=log)
            payload = result.to_json()
            payload["history_sizes"] = {r: {e: len(v) for e, v in history[r].items()}
                                        for r in REPORTS}
            payload["open_ref"] = open_ref
            payload["template_how"] = prepared.template_how
            if spec.compare_with_disk:
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
            log.line("  " + task.period + ": "
                     + "; ".join(f"{r}={result.accepted[r]['items']} items "
                                 f"[{result.accepted[r]['layer']}]"
                                 for r in REPORTS if r in result.accepted)
                     + (f"; absent {result.absent}" if result.absent else "")
                     + f"  ({result.seconds / 60:.1f} min)")

            # ⚠️ **UPSERTED HERE, BEFORE THE NEXT DOCUMENT IS OPENED.** That is the whole
            # point of doing it per quarter rather than at the end: `_write` renders to a
            # `.tmp` and `os.replace`s it, so an interrupt can lose the quarter in flight and
            # never a quarter already on disk.
            if merge_into_csv:
                merge_backup = _upsert_period(
                    folder, task, overwrite=spec.overwrite, log=log,
                    backup=merge_backup is None) or merge_backup
    finally:
        timer.stop(ok=True)

    with (folder / "summary.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=list(rows[0]) if rows else ["period", "report", "status"])
        writer.writeheader()
        writer.writerows(rows)

    metadata = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "stage": "web_scraper.pdf_ocr_job",
        "created": runtime.iso(),
        "git_commit": git_commit or os.environ.get("KGPU_GIT_COMMIT") or _git_commit(),
        "notes": spec.notes,
        "inputs": {
            **spec.to_json(),
            "template": prepared.template,
            # ⚠️ "read off templates.csv" and "guessed from a line-item count" are not the
            # same claim, and a reader of this artefact must be able to tell them apart.
            "template_how": prepared.template_how,
            "data_root": str(prepared.data_root),
            "models": prepared.models,
            "documents": [t.key for t in tasks],
            "layers": [layer.name for layer in prepared.layers],
            "layers_are_the_full_cascade":
                len(prepared.layers) == len(FinancialsBuilder.LAYERS),
            # ⚠️ §5 rule 2 at the artefact: "no quarter was skipped" and "the skip was off"
            # are different facts, and only one of them says anything about the corpus.
            "skipped_already_parsed": [t.period for t in prepared.skipped],
            "merged_into_csv": merge_into_csv,
            "merge_backup": str(merge_backup) if merge_backup else None,
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
    log.line(f"\nrun folder -> {folder}")
    log.close()
    return folder


def _ocr_device() -> str:
    """What the recogniser will actually run on — reported, never chosen here."""
    try:
        import torch

        return "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:  # noqa: BLE001
        return "unknown"


# ⚠️ **THE STACK THAT DECIDES WHAT THE OCR READS.** Every one of these changes pixels, boxes
# or characters — `requirements-ocr.txt` says which does what — so two runs that differ in any
# of them are two PROCEDURES, not one repeated. `torch` is on the list and is NOT pinnable
# (Kaggle ships its own), which is exactly why the list is RECORDED rather than merely pinned.
# The same shape as `feature_selection.report.FINGERPRINTED_LIBRARIES`, one stage over.
FINGERPRINTED_LIBRARIES = (
    "onnxruntime-gpu", "pymupdf", "vietocr", "opencv-python-headless", "opencv-python",
    "shapely", "pyclipper", "numpy", "einops", "torch",
)

REQUIREMENTS = Path(__file__).resolve().parent / "requirements-ocr.txt"


def ocr_stack() -> Dict[str, Optional[str]]:
    """`{package: version or None}` for everything that decides what the OCR reads."""
    import importlib.metadata as md

    out: Dict[str, Optional[str]] = {}
    for name in FINGERPRINTED_LIBRARIES:
        try:
            out[name] = md.version(name)
        except Exception:  # noqa: BLE001 — absent is an answer, and it is `None`
            out[name] = None
    return out


def stack_fingerprint(stack: Optional[Dict[str, Optional[str]]] = None) -> str:
    """12 hex characters over the whole stack — what makes a drift comparable, not invisible.

    ⚠️ **A DIFFERENT FINGERPRINT MEANS TWO RUNS MAY NOT BE COMPARED ON ANYTHING BUT
    CORRECTNESS.** This machine and a Kaggle worker cannot be brought to one stack — Kaggle's
    torch is preinstalled and this repo's modelling is validated against ours — so the residue
    is real and permanent. Recording it is the only honest option: an unrecorded difference is
    not an absent one (§5 rule 2).
    """
    import hashlib

    stack = ocr_stack() if stack is None else stack
    blob = ";".join(f"{k}=={v}" for k, v in sorted(stack.items()))
    return hashlib.sha256(blob.encode()).hexdigest()[:12]


def requirement_pins() -> Dict[str, str]:
    """`{package: pinned version}` read from `requirements-ocr.txt` — the single source."""
    pins: Dict[str, str] = {}
    if not REQUIREMENTS.is_file():
        return pins
    for raw in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if "==" in line:
            name, _, version = line.partition("==")
            pins[name.strip()] = version.strip()
    return pins


def pin_violations() -> Dict[str, str]:
    """Which pinned packages are NOT at the pinned version here. Empty = aligned.

    ⚠️ **REPORTED, NEVER ENFORCED.** A worker that could not honour a pin has still done work
    worth collecting, and refusing the run would throw it away; what must not happen is the
    mismatch going unnoticed. So it lands in `metadata.json` and is printed.
    """
    installed = ocr_stack()
    return {name: f"pinned {want}, installed {installed.get(name) or 'ABSENT'}"
            for name, want in requirement_pins().items()
            if installed.get(name) != want}


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
    stack = ocr_stack()
    out: Dict[str, object] = {
        "det_providers": None,
        "recognizer_device": _ocr_device(),
        # ⚠️ The whole stack and its fingerprint, so a later reader can tell two runs apart
        # without diffing version lists — and so the part that CANNOT be pinned (torch, the
        # Python patch level, the OS) is visible rather than merely absent.
        "stack": stack,
        "stack_fingerprint": stack_fingerprint(stack),
        "pin_violations": pin_violations(),
    }
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
    """`cd src && python -m web_scraper.pdf_ocr_job --symbol VCB --periods Q1-2026`

    ⚠️ `--quarters 2014-Q3 2014-Q4` is the batch form; `--quarters` with no value is every
    quarter, which is the same as omitting it. Both filters may be given and they intersect.
    ⚠️ **`YYYY-QQ`, and the repo-native `Q3-2014` is REFUSED rather than accepted** — see `plan`.
    """
    parser = argparse.ArgumentParser(
        description="OCR + parse CafeF filings into a run folder. Writes NO statement CSV.")
    parser.add_argument("--exchange", default="HOSE")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--periods", nargs="*", default=None,
                        help="e.g. Q1-2026. Omit for every period the ticker files.")
    parser.add_argument("--quarters", nargs="*", default=None,
                        help="e.g. --quarters 2014-Q3 2014-Q4 (YYYY-QQ, the sortable form; "
                             "the repo-native 'Q3-2014' is refused). Omit or pass none for "
                             "every quarter the ticker files. Intersects with --periods.")
    parser.add_argument("--overwrite", action="store_true",
                        help="re-parse quarters that already read `pdf` in all three "
                             "statements, and let --merge replace what disk holds. Without "
                             "it those quarters are dropped before any OCR.")
    parser.add_argument("--merge", action="store_true",
                        help="upsert each quarter into raw_data/.../statements/ AS IT "
                             "FINISHES, through pdf_ocr_merge and its three refusals. Off by "
                             "default: the run folder is this module's product.")
    parser.add_argument("--allow-parent", action="store_true",
                        help="fall back to the STANDALONE filing where no consolidated one "
                             "exists (documents(); consolidated still wins).")
    parser.add_argument("--layers", nargs="*", default=None,
                        help="restrict the cascade to these layer names, in cascade order.")
    parser.add_argument("--template", default=None, choices=TEMPLATES,
                        help="override the chart of accounts. ⚠️ Read ISSUES.md TPL-1 first: "
                             "two of the seven reconcile anchors are bank-shaped.")
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

    # ⚠️ ONE input object, the same one the notebook and `kgpu` build. The CLI parses
    # arguments and constructs it; it does not resolve anything itself.
    run(JobSpec(
        exchange=args.exchange, symbol=args.symbol, periods=args.periods,
        quarters=args.quarters, overwrite=args.overwrite,
        allow_parent=args.allow_parent, layers=args.layers, template=args.template,
        data_root=args.data_root, models_dir=args.models, out_root=args.out,
        compare_with_disk=not args.no_compare, merge_into_csv=args.merge, notes=args.notes,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
