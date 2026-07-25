"""
Parse MANY quarters of one ticker with an OCR engine, and score each against the production
reference. Shared by experiment_8 (`paddle`) and experiment_9 (`onnx`), same as `ocr_pipeline`.

The single-document driver in `ocr_pipeline.run` proves an engine can read one hard scan. This
proves it across a RANGE — ACB Q1-2014 … Q4-2016 — which is the question that decides adoption:
not "can it read the worst page" but "does it hold up over a dozen filings of three different
shapes" (plain quarterlies, semi-annual reviews, audited annuals).

WHAT COUNTS AS RIGHT changes with the filing, and getting it wrong looks exactly like an OCR
error. The parser always reads the filing's FIRST populated period column — the current period as
printed — so:

  * balance sheet — a stock. The 31-Mar / 30-Jun / 30-Sep / 31-Dec figure stands alone; compared
    directly to the reference row for that quarter.
  * income statement — the printed column depends on the document. A plain quarterly (Q1, Q3)
    prints the STANDALONE quarter; a semi-annual review (Q2) prints only CUMULATIVE Jan-Jun; an
    audited annual (Q4) prints the WHOLE YEAR. The reference stores standalone quarters, so the
    expected value is Q1 for Q1, Q1+Q2 for Q2, Q3 for Q3, Q1+Q2+Q3+Q4 for Q4.
  * cash flow — cumulative year-to-date in every filing (Q2 = H1, Q4 = full year), and the
    reference stores it the same way, so it compares 1:1 by quarter.

The reference is `raw_data/cafef/financials/statements/bank/<report>/HOSE_ACB.csv` — the
production pipeline's accepted output, whatever source each cell came from. The question is
therefore "does this engine reproduce the figures the project already trusts?".
"""

# ===== Standard Library =====
import csv
import os
import time
from typing import Dict, List, Optional, Tuple

# ===== Local / Custom Modules =====
from ocr_pipeline import (
    BALANCE_SHEET, CASH_FLOW, INCOME_STATEMENT, REPORTS, STATEMENTS_DIR, CachingEngine,
    FinancialsBuilder, cafef_financials, compare, parse_statements, schema_labels, to_canonical,
    write_csv,
)

PDFS_DIR = os.path.join(cafef_financials.PDFS_DIR)


# ──────────────────────────────────────────────────────────────────────────────
# Which filing to read per quarter
# ──────────────────────────────────────────────────────────────────────────────

def documents_for(exchange: str, symbol: str, periods: List[str]) -> List[dict]:
    """The consolidated filing the production pipeline would pick for each requested period.

    Reuses `FinancialsBuilder.documents`, so a Q2 resolves to the semi-annual review and a Q4 to
    the audited annual — exactly the documents whose cumulative columns the scoring above has to
    account for. A period with no filing on the index is simply skipped (reported as missing).
    """
    fb = FinancialsBuilder()
    by_period = {d["period"]: d for d in fb.documents(exchange, symbol)}
    return [by_period[p] for p in periods if p in by_period]


# ──────────────────────────────────────────────────────────────────────────────
# The reference figures, per quarter, with the right cumulative arithmetic
# ──────────────────────────────────────────────────────────────────────────────

def _reference_rows(report: str, symbol: str, exchange: str,
                    template: str) -> Dict[str, Dict[str, float]]:
    """{period: {canonical column: value}} from the tracked production output."""
    path = os.path.join(STATEMENTS_DIR, template, report, f"{exchange}_{symbol}.csv")
    if not os.path.exists(path):
        return {}
    cols_of = {c for c, _ in FinancialsBuilder().schema_of(template, report)}
    out: Dict[str, Dict[str, float]] = {}
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            out[r["period"]] = {c: float(r[c]) for c in cols_of
                                if c in r and r[c] not in (None, "")}
    return out


def reference_value(rows: Dict[str, Dict[str, float]], report: str, period: str,
                    column: str) -> Optional[float]:
    """The figure a correct parse of `period` should print for `column`.

    Balance sheet and cash flow are the quarter's own reference row. The income statement is
    reconstructed to the CUMULATIVE basis the filing actually prints (see the module docstring):
    a semi-annual filing's Jan-Jun total is Q1+Q2 of the standalone reference, an annual filing's
    year is Q1..Q4.
    """
    q, year = int(period[1]), period.split("-")[1]

    if report != INCOME_STATEMENT or q in (1, 3):
        row = rows.get(period)
        return row.get(column) if row else None

    # income statement, Q2 (cumulative H1) or Q4 (cumulative full year)
    upto = 2 if q == 2 else 4
    vals = [rows[f"Q{i}-{year}"][column] for i in range(1, upto + 1)
            if f"Q{i}-{year}" in rows and column in rows[f"Q{i}-{year}"]]
    return sum(vals) if len(vals) == upto else None


# ──────────────────────────────────────────────────────────────────────────────
# One engine over the whole range
# ──────────────────────────────────────────────────────────────────────────────

def run_batch(engine, engine_name: str, symbol: str, exchange: str, periods: List[str],
              out_dir: str, template: str = "bank", max_pages: int = 32,
              cache: bool = True, verbose: bool = False) -> dict:
    """Parse every period with `engine`, score each statement, write per-cell + summary CSVs.

    Returns the summary dict (also written to `summary.json` and `report.md`). The OCR of each
    filing is cached separately under `out_dir/cache/`, so re-scoring after a change to the table
    logic does not re-read a single page.
    """
    import json

    os.makedirs(out_dir, exist_ok=True)
    docs = documents_for(exchange, symbol, periods)
    labels = {r: schema_labels(template, r) for r in REPORTS}
    ref = {r: _reference_rows(r, symbol, exchange, template) for r in REPORTS}

    cells: List[dict] = []          # one row per (period, report): the scorecard
    detail: List[dict] = []         # one row per (period, report, column): every figure
    total_ocr_seconds, total_ocr_pages = 0.0, 0

    for d in docs:
        period = d["period"]
        pdf = os.path.join(PDFS_DIR, d["path"].replace("/", os.sep))
        if not os.path.exists(pdf):
            print(f"  {period}: FILE MISSING {d['path']}", flush=True)
            continue

        eng = engine
        if cache:
            eng = CachingEngine(
                engine, os.path.join(out_dir, "cache", f"{engine_name}_{period}.json"),
                key=f"{engine_name}|{d['file']}|dpi=200")
        t0 = time.time()
        statements, parser, pages = parse_statements(eng, pdf, max_pages, verbose)
        secs = time.time() - t0
        total_ocr_seconds += getattr(eng, "ocr_seconds", secs)
        total_ocr_pages += getattr(eng, "ocr_pages", 0)

        line = [f"{period}"]
        for report in REPORTS:
            st = statements.get(report)
            if st is None:
                cells.append({"period": period, "report": report, "found": False,
                              "reconciles": False, "why": "not found", "rows": 0, "mapped": 0,
                              "agree": 0, "sign_only": 0, "differ": 0,
                              "parsed_only": 0, "truth_only": 0})
                line.append(f"{report.split('_')[0][:3]}:—")
                continue

            mapped, why = to_canonical(st, template)
            truth = {c: reference_value(ref[report], report, period, c)
                     for c in ref[report].get(period, {})}
            truth = {c: v for c, v in truth.items() if v is not None}
            rows, stats = compare(mapped, truth)
            for r in rows:
                r.update({"period": period, "report": report,
                          "account": labels[report].get(r["column"], "")})
            detail += rows
            cells.append({"period": period, "report": report, "found": True,
                          "reconciles": why is None, "why": why or "",
                          "rows": len(st.rows), "mapped": len(mapped), **stats})
            tag = report.split("_")[0][:3]
            # OCR-correct = magnitude agrees (agree + sign_only); comparable = those + differ
            ok = stats["agree"] + stats["sign_only"]
            line.append(f"{tag}:{'OK' if why is None else 'x'} "
                        f"{ok}/{ok + stats['differ']}")
        print(f"  {period}  {'   '.join(line[1:])}   ({secs:.0f}s)", flush=True)

    write_csv(os.path.join(out_dir, "cells.csv"), cells,
              ["period", "report", "found", "reconciles", "why", "rows", "mapped",
               "agree", "sign_only", "differ", "parsed_only", "truth_only"])
    write_csv(os.path.join(out_dir, "detail.csv"), detail,
              ["period", "report", "column", "account", "parsed", "cafef", "verdict"])

    summary = _summarise(engine_name, symbol, cells, total_ocr_pages, total_ocr_seconds)
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    _write_report(os.path.join(out_dir, "report.md"), summary, cells)
    speed = (f"{summary['ocr_seconds']:.0f}s OCR ({summary['sec_per_page']:.1f}s/page)"
             if summary["sec_per_page"] else "all pages from cache")
    print(f"\n{engine_name}: {summary['reconciled']}/{summary['statements']} statements "
          f"reconcile, {summary['agree']}/{summary['comparable']} figures agree, {speed}",
          flush=True)
    return summary


def _summarise(engine_name: str, symbol: str, cells: List[dict],
               ocr_pages: int, ocr_seconds: float) -> dict:
    found = [c for c in cells if c["found"]]
    # OCR-correct = magnitude agrees (agree + sign_only, see compare); comparable adds differ
    agree = sum(c["agree"] + c["sign_only"] for c in found)
    differ = sum(c["differ"] for c in found)
    per_report = {}
    for report in REPORTS:
        rc = [c for c in cells if c["report"] == report]
        fc = [c for c in rc if c["found"]]
        per_report[report] = {
            "found": len(fc), "quarters": len(rc),
            "reconciled": sum(c["reconciles"] for c in rc),
            "agree": sum(c["agree"] + c["sign_only"] for c in fc),
            "strict_agree": sum(c["agree"] for c in fc),
            "sign_only": sum(c["sign_only"] for c in fc),
            "differ": sum(c["differ"] for c in fc),
            "missed": sum(c["truth_only"] for c in fc),
        }
    return {
        "engine": engine_name, "symbol": symbol,
        "statements": len(cells), "found": len(found),
        "reconciled": sum(c["reconciles"] for c in cells),
        "comparable": agree + differ, "agree": agree, "differ": differ,
        "strict_agree": sum(c["agree"] for c in found),
        "sign_only": sum(c["sign_only"] for c in found),
        "accuracy": round(agree / (agree + differ), 4) if (agree + differ) else None,
        "ocr_pages": ocr_pages, "ocr_seconds": round(ocr_seconds, 1),
        "sec_per_page": round(ocr_seconds / ocr_pages, 2) if ocr_pages else None,
        "per_report": per_report,
    }


def _write_report(path: str, summary: dict, cells: List[dict]) -> None:
    s = summary
    lines = [f"# {s['engine']} — {s['symbol']} batch", "",
             f"- statements reconciled: **{s['reconciled']}/{s['statements']}**",
             f"- figures matching the production reference (magnitude): "
             f"**{s['agree']}/{s['comparable']}** ({s['accuracy']}) — of which "
             f"{s['sign_only']} agree in magnitude but flip sign (income-statement expense "
             f"convention, not an OCR error)",
             f"- OCR: {s['ocr_pages']} pages in {s['ocr_seconds']} s "
             f"({s['sec_per_page']} s/page)", "",
             "## Per statement", "",
             "| statement | found | reconciled | match | (of which sign-only) | differ | missed |",
             "|---|---|---|---|---|---|---|"]
    for report, r in s["per_report"].items():
        lines.append(f"| {report} | {r['found']}/{r['quarters']} | {r['reconciled']} | "
                     f"{r['agree']} | {r['sign_only']} | {r['differ']} | {r['missed']} |")
    lines += ["", "## Per quarter", "",
              "| period | balance_sheet | income_statement | cash_flow |",
              "|---|---|---|---|"]
    by_period: Dict[str, Dict[str, dict]] = {}
    for c in cells:
        by_period.setdefault(c["period"], {})[c["report"]] = c
    for period in sorted(by_period):
        row = [period]
        for report in REPORTS:
            c = by_period[period].get(report)
            if not c or not c["found"]:
                row.append("—")
            else:
                ok = "OK" if c["reconciles"] else "REJECT"
                match = c["agree"] + c["sign_only"]
                row.append(f"{ok} · {match}/{match + c['differ']}")
        lines.append("| " + " | ".join(row) + " |")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
