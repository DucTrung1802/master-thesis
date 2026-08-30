"""Merge a `pdf_ocr_job` run folder INTO the statement CSVs — the write `pdf_ocr_job` refuses.

⚠️ **THIS IS THE ONE PLACE A KAGGLE RUN CAN REACH `raw_data/`, AND IT RUNS HERE.** A worker
executes on Kaggle, writes `/kaggle/working` and exits; there is no path from it to this disk
except `kgpu pull`. So "the Kaggle run upserts the CSV" is necessarily "the pull does", and
that is what this module implements — after the artefact is on disk, where a backup and a diff
are possible.

⚠️ **IT DOES NOT WRITE THE CSV ITSELF.** It calls `FinancialsBuilder._write(merge=True)`, the
same upsert `build()` uses: only the quarters this run PRODUCED are rewritten, every other row
keeps what the file already holds, and columns the file already carries survive. A second CSV
writer would be a second place for the column contract to be wrong.

---

## ⚠️ Why the refusals below exist, and why they are ON by default

`pdf_ocr_job` was built to write no CSV, on four measured occasions where a `periods` run
silently DOWNGRADED a quarter it was given only for history (CLAUDE.md §6-2-vicies,
§6-2-unvicies, §6-2-quatervicies, §6-2-quinvicies). Merging automatically gives that risk back,
in a NEW shape, and the shape is not hypothetical — it was measured on the first run this
module was written for:

**VIC Q3-2014, 2026-08-29.** The worker ACCEPTED an income statement at `onnx@300` that the
full local run had REFUSED, and the refusal was `sane: probe exactly equals an already-accepted
quarter`. Nothing about the machine differed — the cash flow reproduced bit for bit at the same
layer, and the balance sheet was refused for the same reason on both. What differed is the
MAGNITUDE BAND: `seed_history` reconstructs it from the `pdf` rows on DISK (12 income-statement
probes for VIC), while a full run accumulates it IN THE RUN, over more quarters and over
pre-de-cumulation figures. **The gate that decides is looking at two different populations.**

So a statement accepted on a worker is not a statement a full run would accept, and a merge
that ignores that is `SAN-1` with the guard removed. Three refusals follow from it, and each
one is a `force_*` argument rather than a silent default:

1. **A cumulative income statement is REFUSED.** An annual or half-year filing prints the year
   to date; the CSV column holds the standalone quarter. `pdf_ocr_job` does not de-cumulate —
   it cannot, a one-document run has no Q1..Q(q-1) — so writing its figures would put a
   9-month total in a 3-month column, and nothing downstream could tell.
2. **A statement whose magnitude band was EMPTY is REFUSED.** With no band `sane` fails open,
   which is the documented way a run writes a wrong figure. A ticker with nothing on disk yet
   has no band at all, so its first run is unguarded by construction.
3. **A quarter that DIFFERS from a good row on disk is REFUSED.** `compare()` already scored
   it; a `DIFFERS` verdict means two runs disagree about a figure, and picking the newer one
   by default is a coin toss dressed as an upsert.
4. ⚠️ **A document whose parse had an ENGINE ERROR is REFUSED WHOLE.** A layer that REFUSES has
   measured the filing; a layer that RAISES has measured the machine. When the second happens
   the cascade's answer comes from whichever layer the broken tool did not reach, and both
   gates pass on it — so the result is not a worse parse, it is a parse of a different
   procedure. **Measured 2026-08-29**: `vocr.vn`'s TLS certificate expired, and since
   `Cfg.load_config_from_name` fetches `base.yml` from that host on EVERY predictor
   construction, all three `onnx@*` layers raised and `tesseract@200` won VCB Q1-2026 — a
   filing that had read `onnx@200` with 98 of 98 cells reproducing. With `force_differs` on it
   rewrote 13 columns of three good statements before the backup put them back.

⚠️ **`apply=True` IS THE DEFAULT SINCE 2026-08-29, BY REQUEST — the refusals are what keeps it
honest, not the extra command.** Every merge still prints each decision and each changed cell,
and still backs the three CSVs up first, so a merge remains reversible and readable. What is
gone is the second step, not the checks: `apply=False` gives the dry run, `plan_merge` gives
the decisions with no I/O at all.
"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job
from web_scraper.cafef_financials import FinancialsBuilder, REPORTS, statement_path

# Where a pre-merge backup goes: under `raw_data/_backup/`, timestamped so a second merge
# cannot overwrite the evidence of the first.
#
# ⚠️ **ANCHORED TO THE REPO, NOT TO THE CWD.** A relative path put the backup wherever the
# caller happened to be standing — `kgpu merge` runs from `src/kaggle_gpu`, so the one thing
# that makes a merge reversible would have landed in a directory nobody looks in. Found
# 2026-08-29 on the first real merge, by looking for the backup and not finding it where the
# tool said it was.
BACKUP_ROOT = Path(__file__).resolve().parents[2] / "raw_data" / "_backup" / "statements"


@dataclass
class Decision:
    """What this module decided about one (period, report), and why."""

    period: str
    report: str
    action: str                  # "write" | "skip"
    reason: str
    layer: str = ""
    items: int = 0
    on_disk: str = ""            # the row's `source` before the merge
    changed: Dict[str, Tuple[Optional[int], Optional[int]]] = field(default_factory=dict)
    # How many months of activity this row covers, when it is anything other than a quarter.
    # Set only where refusal 1 let a year-to-date income statement through; `None` everywhere
    # else, because the span is then the ordinary one and `_write` takes it from the run.
    months: Optional[int] = None
    # A caveat printed beside a WRITE. A write with a note is still a write — what the note
    # says is what a reader would otherwise have to reconstruct from the PDF index.
    note: str = ""

    @property
    def writing(self) -> bool:
        return self.action == "write"


@dataclass
class MergeReport:
    folder: Path
    exchange: str
    symbol: str
    template: str
    decisions: List[Decision] = field(default_factory=list)
    applied: bool = False
    backup: Optional[Path] = None
    written: Dict[str, int] = field(default_factory=dict)

    @property
    def to_write(self) -> List[Decision]:
        return [d for d in self.decisions if d.writing]

    def lines(self) -> List[str]:
        out = [f"{self.exchange}_{self.symbol}  template={self.template}  "
               f"run={self.folder.name}"]
        if not self.decisions:
            out.append("  nothing accepted in this run — nothing to merge")
            return out
        for d in sorted(self.decisions, key=lambda d: (d.period, d.report)):
            mark = "WRITE " if d.writing else "skip  "
            detail = f"[{d.layer}] {d.items} items" if d.layer else ""
            out.append(f"  {mark} {d.period:9} {d.report:18} {detail:32} {d.reason}")
            # ⚠️ A CAVEAT ON A WRITE IS LOUDER THAN A REFUSAL, because a refusal stops and a
            # write does not. The one that exists today says a row is 6 or 12 months. It is
            # printed only for a WRITE: refusal 1 sets it before refusals 2-4 have had their
            # say, and "written with months=12" beside the word `skip` would contradict itself.
            if d.note and d.writing:
                out.append(f"           ⚠️  {d.note}")
            for column, (was, now) in sorted(d.changed.items())[:8]:
                out.append(f"           {column:56} {was} -> {now}")
            if len(d.changed) > 8:
                out.append(f"           … and {len(d.changed) - 8} more columns")
        if self.applied:
            out.append(f"  backup: {self.backup or '— (taken earlier in this run)'}")
            out.append(f"  written: {self.written}")
        else:
            out.append(f"  DRY RUN — {len(self.to_write)} statement(s) would be written. "
                       f"Pass apply=True to write them.")
        return out


def _documents(folder: Path, periods: Optional[Sequence[str]] = None) -> List[dict]:
    """Every per-filing record in a run folder, oldest period first.

    ⚠️ **`periods` FILTERS BY FILENAME, BEFORE THE JSON IS PARSED**, and that is not a
    micro-optimisation. Each record carries a `row_dump` — every row the OCR read, mapped or
    not — so a document is 100-200 KB, and merging one quarter at a time as a 70-quarter run
    proceeds would otherwise re-parse the whole folder 70 times. The name is
    `<EXCHANGE>_<SYMBOL>__<period>.json`, built by `DocumentTask.key`.
    """
    files = sorted((folder / "documents").glob("*.json"))
    if periods is not None:
        wanted = {f"__{p}.json" for p in periods}
        files = [p for p in files if any(p.name.endswith(w) for w in wanted)]
    docs = [json.loads(p.read_text(encoding="utf-8")) for p in files]
    return sorted(docs, key=lambda d: fin._period_key(d["period"]))


def _backup(exchange: str, symbol: str, template: str, stamp: str) -> Path:
    """Copy the three CSVs somewhere a diff can reach them, BEFORE anything is written.

    ⚠️ `SAN-1` was found by diffing a run against a backup, not by reading a log — the log said
    `RUN_SUCCESS` and named a layer while a quarter had been silently downgraded. A backup is
    the only thing that makes "what did this change?" answerable afterwards.
    """
    target = BACKUP_ROOT / f"{stamp}__{exchange}_{symbol}"
    target.mkdir(parents=True, exist_ok=True)
    for report in REPORTS:
        path = Path(statement_path(template, report, exchange, symbol))
        if path.is_file():
            shutil.copy2(path, target / path.name)
    return target


def _unfiled_priors(builder: FinancialsBuilder, exchange: str, symbol: str,
                    period: str) -> List[str]:
    """Which of `period`'s earlier quarters this ticker never filed a report for.

    De-cumulating a Q2/Q4 income statement means subtracting Q1..Q(q-1) of the same year, so
    an empty list is "a full `build()` could do it" and a non-empty one is "no run ever will".
    That distinction is the whole of refusal 1: the same YTD figure is a bad write in the
    first case and the only write available in the second.

    ⚠️ **`allow_parent=True` — THE WIDEST POSSIBLE SET, DELIBERATELY.** This answers "does a
    filing exist AT ALL", and it is used to justify writing a 12-month figure into a quarterly
    row. A prior quarter reachable only through the standalone report still makes that claim
    false, so it must count — the conservative direction is to keep refusing, and the operator
    who knows better has `force_cumulative`.

    ⚠️ It reads the PDF INDEX, not the statement CSVs: "was it filed" and "did we parse it"
    are different questions, and only the first one is permanent.
    """
    year, quarter = int(period.split("-")[1]), int(period[1])
    try:
        filed = {d["period"] for d in
                 builder.documents(exchange, symbol, allow_parent=True, period_min=None)}
    except FileNotFoundError:
        # No index on disk — we cannot show a prior quarter was never filed, so we must not
        # claim it. An empty list keeps refusal 1 refusing, which is the safe direction.
        return []
    return [p for p in (f"Q{i}-{year}" for i in range(1, quarter)) if p not in filed]


def plan_merge(folder: os.PathLike | str,
               *,
               reports: Optional[Sequence[str]] = None,
               periods: Optional[Sequence[str]] = None,
               force_cumulative: bool = False,
               force_empty_band: bool = False,
               force_differs: bool = False,
               force_engine_errors: bool = False) -> MergeReport:
    """Decide, without writing anything, what this run folder would put on disk."""
    folder = Path(folder)
    docs = _documents(folder, periods)
    if not docs:
        raise ValueError(
            f"{folder} holds no documents/*.json"
            + (f" for {list(periods)}" if periods else "") + " — nothing to merge")

    exchange = docs[0]["exchange"]
    symbol = docs[0]["symbol"]
    template = docs[0]["template"]

    # ⚠️ **THE DISK THIS COMPARES AGAINST IS THE REPO'S, RESOLVED FROM THIS FILE AND NEVER
    # FROM THE CWD.** `statement_path()` reads `fin.STATEMENTS_DIR` at call time and its
    # module default is RELATIVE, so a caller running from anywhere but the repo root read an
    # empty directory and every quarter came back `on_disk="absent"` — **which is a legitimate
    # state for a ticker being bootstrapped (`BND-1`)**, so nothing looked wrong. Refusal 3
    # then could not fire, and a figure that DIFFERS from a good `pdf` row would have been
    # written unguarded; `_write` would have put it under the wrong root too.
    #
    # Measured 2026-08-30 on the BID Q4-2016 repair: the identical call planned **2 writes**
    # from `src\` and **0** from the repo root, where refusal 3 correctly refused. ⚠️ `kgpu
    # merge` runs from `src\kaggle_gpu\` — the same cwd that mislocated `BACKUP_ROOT` on
    # 2026-08-29, which was anchored then while this was not.
    #
    # ⚠️ **`DEFAULT_DATA_ROOT`, NOT `data_root()`** — the merge upserts into THIS repo by
    # definition (`pdf_ocr_job.run` refuses to merge at all when the root is a payload), so
    # honouring `$CAFEF_DATA_ROOT` could only ever point it at a copy that dies with a kernel.
    #
    # ⚠️ **ONLY WHEN THE PATH IS STILL RELATIVE, AND THAT PREDICATE IS THE DEFECT'S OWN
    # DEFINITION rather than a proxy for it**: a relative `STATEMENTS_DIR` is exactly one that
    # resolves against the CWD, and the module default (`raw_data/cafef/financials/statements`)
    # is the only relative value there is. Anything absolute was put there deliberately — by
    # `pdf_ocr_job.run`, by an experiment harness (`statement_path`'s docstring records that
    # contract) or by a test fixture — and overriding a deliberate root would point the write
    # itself somewhere the caller did not ask for. ⚠️ An unconditional call here made every
    # `apply=True` test in `test_pdf_ocr_merge.py` write into the real `raw_data/`.
    if not os.path.isabs(fin.STATEMENTS_DIR):
        job.use_data_root(job.DEFAULT_DATA_ROOT)

    builder = FinancialsBuilder(logger=None)
    report = MergeReport(folder=folder, exchange=exchange, symbol=symbol, template=template)

    wanted_reports = set(reports) if reports else set(REPORTS)

    for doc in docs:
        period = doc["period"]
        # ⚠️ A run that raised mid-document may still have written an `accepted` block for the
        # statements it got through. Refuse the whole filing rather than reasoning about which
        # half survived.
        if doc.get("error"):
            for name in sorted(wanted_reports):
                report.decisions.append(Decision(
                    period, name, "skip", f"the run errored: {doc['error']}"))
            continue
        # ⚠️ REFUSAL 4, and it is refused WHOLE. A raising layer did not judge the filing, so
        # the layer that won did so by default — see the module docstring for the day this was
        # measured. Not a per-statement decision: the broken tool was broken for all three.
        # ⚠️ A run folder written before schema v2 has no such key, and absent is read as
        # "none recorded" — the only reading available, and NOT the same claim as "none
        # happened" (§5 rule 2). `metadata.json`'s `schema_version` is what tells them apart.
        engine_errors = doc.get("engine_errors") or []
        if engine_errors and not force_engine_errors:
            names = ", ".join(sorted({str(e[0]) for e in engine_errors}))
            for name in sorted(wanted_reports):
                report.decisions.append(Decision(
                    period, name, "skip",
                    f"{len(engine_errors)} layer(s) RAISED rather than refusing ({names}) — "
                    f"whatever won did so because those could not run"))
            continue

        bands = doc.get("history_sizes") or {}
        for name in REPORTS:
            if name not in wanted_reports:
                continue
            got = (doc.get("accepted") or {}).get(name)
            if got is None:
                report.decisions.append(Decision(
                    period, name, "skip", "absent in this run"))
                continue

            disk = builder._existing(exchange, symbol, template, name).get(period) or {}
            on_disk = disk.get("source", "absent")
            decision = Decision(period, name, "write", "", layer=got.get("layer", ""),
                                items=got.get("items", 0), on_disk=on_disk)

            # ── refusal 1: a cumulative P&L is not a quarter — UNLESS it can never be one ─
            #
            # ⚠️ **THE SPAN COMES FROM THE RUN, NOT FROM THE INDEX'S `cumulative` FLAG.**
            # `pdf_ocr_job` decided it with the filing's own column headings in hand, and a
            # half-year report that prints "Quý II" BESIDE "Lũy kế" already IS the quarter —
            # `months = 3`, and refusing it on the index's flag alone was an over-refusal.
            # A run folder written before the field records no span, and the flag is then the
            # only thing available: refuse, as before.
            months = got.get("months")
            cumulative = months > 3 if months is not None else bool(doc.get("cumulative"))
            if name == fin.INCOME_STATEMENT and cumulative and not force_cumulative:
                unfiled = _unfiled_priors(builder, exchange, symbol, period)
                if not unfiled:
                    # Every prior quarter WAS filed, so an authoritative `build()` over the
                    # whole ticker can subtract them and produce the real quarter. Writing the
                    # year-to-date figure now would pre-empt a better answer with a worse one.
                    decision.action = "skip"
                    decision.reason = (
                        "cumulative income statement — this module does not de-cumulate, and "
                        f"Q1..Q{int(period[1]) - 1}-{period.split('-')[1]} WERE filed, so a "
                        f"full `build()` can subtract them")
                    report.decisions.append(decision)
                    continue
                # ⚠️ NOTHING WILL EVER SUBTRACT QUARTERS THAT WERE NEVER REPORTED, so the
                # choice here is not "cumulative now or a quarter later" — it is "cumulative
                # now or nothing, ever". The row is written with `months` saying what it is,
                # which is the whole reason that column exists; see `DATA_COLS`.
                decision.months = months if months is not None else 3 * int(period[1])
                decision.note = (f"{decision.months}-month figure — {', '.join(unfiled)} "
                                 f"{'was' if len(unfiled) == 1 else 'were'} never filed, so "
                                 f"no run can ever split it. Written with `months="
                                 f"{decision.months}`")

            # ── refusal 2: `sane` had no band, so it could not have refused anything ──
            band = (bands.get(name) or {}).get(doc.get("consolidated", "True"), 0)
            if not band and not force_empty_band:
                decision.action = "skip"
                decision.reason = ("the magnitude band was EMPTY — `sane` failed open, so "
                                   "this figure passed no guard")
                report.decisions.append(decision)
                continue

            # ── refusal 3: two runs disagree about a figure already on disk ───────────
            values = {k: int(v) for k, v in (got.get("values") or {}).items()}
            if on_disk == "pdf":
                # ⚠️ **THE LINE-ITEM RULE IS `pdf_ocr_job`'s, IMPORTED RATHER THAN RESTATED.**
                # `compare()` scored this very quarter with it, so a merge reading the row by a
                # slightly different rule could refuse what compare called REPRODUCED, or
                # write what it called DIFFERS. It also refuses to `int()` a cell that is not a
                # number instead of raising halfway through a decision.
                disk_values = job._line_items(disk)
                decision.changed = {
                    column: (disk_values.get(column), values.get(column))
                    for column in sorted(set(values) | set(disk_values))
                    if disk_values.get(column) != values.get(column)}
                changed = decision.changed
                # ⚠️ **A ROW WHOSE FIGURES MATCH BUT WHOSE SPAN DISK DOES NOT RECORD IS NOT
                # "IDENTICAL" — IT IS INCOMPLETE.** `months` arrived after most of the corpus
                # was written, so every row parsed before it carries a blank there while the
                # run in hand knows the answer. Rewriting fills it in and moves no figure.
                # ⚠️ Only ever in that direction: when THIS run has no span (a folder written
                # before the field) disk's value stands, because a blank overwriting a known
                # 12 would destroy the one thing the column exists to say.
                span_known = "" if got.get("months") is None else str(got["months"])
                span_on_disk = str(disk.get("months", "")).strip()
                fills_span = bool(span_known) and span_on_disk != span_known
                if not changed and disk.get("method") == got.get("layer") and not fills_span:
                    decision.action = "skip"
                    decision.reason = "identical to the row already on disk"
                    report.decisions.append(decision)
                    continue
                if not changed and fills_span:
                    decision.reason = (
                        f"same figures, same layer — recording the span this row covers "
                        f"(`months`: {span_on_disk or 'unrecorded'} -> {span_known})")
                    report.decisions.append(decision)
                    continue
                if changed and not force_differs:
                    decision.action = "skip"
                    decision.reason = (f"DIFFERS from a `pdf` row on disk in "
                                       f"{len(changed)} column(s) — two runs disagree, and "
                                       f"the newer one is not automatically the right one")
                    report.decisions.append(decision)
                    continue

            decision.reason = ("recovers a quarter disk records as "
                               f"`{on_disk}`" if on_disk != "pdf" else "re-writes a `pdf` row")
            report.decisions.append(decision)

    return report


def merge_run(folder: os.PathLike | str,
              *,
              apply: bool = True,
              reports: Optional[Sequence[str]] = None,
              periods: Optional[Sequence[str]] = None,
              force_cumulative: bool = False,
              force_empty_band: bool = False,
              force_differs: bool = False,
              force_engine_errors: bool = False,
              backup: bool = True,
              quiet: bool = False) -> MergeReport:
    """Upsert a run folder's accepted statements into the ticker's CSVs.

    ⚠️ **`apply=True` BY DEFAULT since 2026-08-29, by request.** The three refusals below are
    what stands between an automatic merge and a wrong figure on disk, so they stay on; what
    changed is that a run which clears them is written without a second command. Pass
    `apply=False` for the dry run, or call `plan_merge` for the decisions alone.

    ⚠️ **A BACKUP IS TAKEN EVERY TIME BY DEFAULT.** It is the only thing that makes "what did
    this change?" answerable afterwards, and `SAN-1` was found that way rather than from a log.
    `backup=False` exists for ONE caller — `pdf_ocr_job.run`, which upserts quarter by quarter
    as a long parse proceeds and takes its single backup before the first of them. Seventy
    timestamped copies of three CSVs answer "what did this run change?" worse than one.
    """
    result = plan_merge(folder, reports=reports, periods=periods,
                        force_cumulative=force_cumulative,
                        force_empty_band=force_empty_band,
                        force_differs=force_differs,
                        force_engine_errors=force_engine_errors)

    if apply and result.to_write:
        if backup:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            result.backup = _backup(result.exchange, result.symbol, result.template, stamp)

        builder = FinancialsBuilder(logger=None)
        docs = {d["period"]: d for d in _documents(Path(folder), periods)}
        writing = {(d.period, d.report) for d in result.to_write}
        periods_written = sorted({d.period for d in result.to_write}, key=fin._period_key)

        data: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        items: Dict[str, List[str]] = {r: [] for r in REPORTS}
        meta: Dict[str, Dict[str, dict]] = {r: {} for r in REPORTS}
        published: Dict[str, str] = {}
        assurance: Dict[str, str] = {}
        shares: Dict[str, Dict[str, Optional[int]]] = {}

        for period in periods_written:
            doc = docs[period]
            facts = doc.get("facts") or {}
            published[period] = facts.get("publish_date", "") or ""
            assurance[period] = doc.get("assurance", "") or ""
            shares[period] = {k: facts.get(k) for k in
                              ("shares_authorized", "shares_issued", "shares_outstanding")}
            for name in REPORTS:
                if (period, name) not in writing:
                    continue
                got = doc["accepted"][name]
                values = {k: int(v) for k, v in (got.get("values") or {}).items()}
                data[name][period] = values
                items[name] += [c for c in values if c not in items[name]]
                # ⚠️ The provenance `_write` reads. `source` is `pdf` and nothing else: rule 24
                # forbids any other origin, and this module has no other origin to offer.
                meta[name][period] = {
                    "ocr_config": got.get("layer", ""),
                    "source": "pdf",
                    "consolidated": doc.get("consolidated", ""),
                    "cash_flow_method": got.get("cash_flow_method", "") or "",
                    "unit": got.get("unit", ""),
                    "n_columns": got.get("n_columns", ""),
                    # ⚠️ **THE SPAN, TAKEN FROM THE RUN AND NEVER RE-DERIVED HERE.**
                    # `pdf_ocr_job` decided it with both terms in hand (the index's
                    # cumulative flag, and the filing's own "Quý N | Lũy kế" heading that
                    # overrules it). A run folder written before this field simply has no
                    # span recorded, and a blank is the only honest reading of that — §5
                    # rule 2, never a default of 3.
                    "months": got.get("months"),
                    "document": doc.get("document", ""),
                    "assurance": doc.get("assurance", "") or "",
                }

        # ⚠️ `attempted` is EXACTLY the periods being written. `_write` builds its quarter grid
        # from `attempted ∪ rows`, and a wider grid would manufacture blank `missing` rows for
        # quarters this run never opened — which `merge=True` would then keep out of the file,
        # but only by accident of ordering. Keeping it narrow makes the intent explicit.
        attempted = [(int(p.split("-")[1]), int(p[1])) for p in periods_written]
        result.written = builder._write(
            result.exchange, result.symbol, data, items, meta, attempted,
            result.template, published, assurance, shares, merge=True)
        result.applied = True

    if not quiet:
        print("\n".join(result.lines()))
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Recording the outcome back into the run folder
# ──────────────────────────────────────────────────────────────────────────────


def merge_event(report: MergeReport) -> dict:
    """One merge, as the run folder records it — JSON, no Path objects, no I/O."""
    return {
        "at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "applied": report.applied,
        "backup": str(report.backup) if report.backup else None,
        # ⚠️ NAMED FOR WHAT IT IS. `_write` returns the number of `pdf` rows in the WHOLE csv
        # after the upsert, not the number this merge produced — summing it as "statements
        # written" reports a ticker's entire history as this run's work. `statements_written`
        # in the block below counts the DECISIONS instead.
        "pdf_rows_on_disk": dict(report.written),
        "decisions": [
            {"period": d.period, "report": d.report, "action": d.action,
             "reason": d.reason, "layer": d.layer, "items": d.items,
             "on_disk": d.on_disk, "months": d.months, "note": d.note,
             "columns_changed": len(d.changed)}
            for d in sorted(report.decisions, key=lambda d: (d.period, d.report))
        ],
    }


def merge_block(events: Sequence[dict]) -> dict:
    """The `merge` block of a run folder's `metadata.json` — events plus their union.

    ⚠️ **EVENTS, NOT A FLAG, because a folder is legitimately merged more than once.** The CTG
    bootstrap took three calls — one per report, each carrying only the periods that survived
    an external screen — and a single overwritten block would have kept the last and lost the
    other two. A LOCAL run appends one event per QUARTER, for the same reason.

    The union is keyed by `(period, report)` so a later event supersedes an earlier one for
    the same statement, which is what a reader asking "is this quarter on disk?" needs.
    """
    latest: Dict[Tuple[str, str], dict] = {}
    for ev in events:
        for d in ev["decisions"]:
            latest[(d["period"], d["report"])] = {**d, "applied": ev["applied"]}
    written = [d for d in latest.values() if d["action"] == "write" and d["applied"]]
    return {
        "events": list(events),
        "statements_written": len(written),
        "statements_skipped": len(latest) - len(written),
        "periods_written": sorted({d["period"] for d in written}, key=fin._period_key),
    }


def record_merge(folder: os.PathLike | str, report: MergeReport) -> Optional[Path]:
    """Write what this merge DID into the run folder's `metadata.json`.

    ⚠️ **THE ARTEFACT WAS SAYING `merged_into_csv: false` ON EVERY KAGGLE RUN, FOREVER, AND IT
    WAS NOT A STALE FLAG — NOTHING COULD EVER HAVE SET IT.** `metadata.json` is written by the
    process that PARSES, and on a Kaggle round trip that process is a worker which cannot
    reach this disk (`pdf_ocr_job.run` turns its own upsert off for exactly that reason). The
    merge happens later, here, on the pull — and until 2026-08-30 it wrote nothing back. So a
    run that upserted 126 statements and a run that upserted none carried the identical
    artefact, and the control notebook printed `upserted : False` for both.

    That is §5 rule 10 at the artefact: the field recorded what was INTENDED at parse time,
    and was read as what HAPPENED.

    ⚠️ **`inputs.merged_into_csv` BECOMES TRUE ONLY IF SOMETHING WAS ACTUALLY WRITTEN.** An
    applied merge that every refusal turned away is not a merge, and recording it as one would
    re-create the defect one level down.

    Returns the metadata path, or `None` when the folder carries no `metadata.json` — a LOCAL
    run writes its metadata once, at the end, and builds the same block itself from the
    reports it collected on the way.
    """
    path = Path(folder) / "metadata.json"
    if not path.is_file():
        return None
    meta = json.loads(path.read_text(encoding="utf-8"))
    events = list((meta.get("merge") or {}).get("events") or []) + [merge_event(report)]
    meta["merge"] = merge_block(events)
    inputs = meta.setdefault("inputs", {})
    inputs["merged_into_csv"] = (bool(inputs.get("merged_into_csv"))
                                 or bool(meta["merge"]["statements_written"]))
    if report.backup:
        inputs["merge_backup"] = str(report.backup)
    path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def latest_run(reports_root: os.PathLike | str, exchange: str, symbol: str) -> Optional[Path]:
    """The newest run folder for this ticker — run ids sort chronologically by construction."""
    root = Path(reports_root)
    folders = sorted(root.glob(f"*__{exchange.lower()}_{symbol.lower()}__pdf_ocr"),
                     key=lambda p: p.name)
    return folders[-1] if folders else None
