"""
Head-to-head: the two OCR models over ACB Q1-2014 … Q4-2016.

    python compare_models.py

Reads what each batch run wrote —

    experiment_8/out_batch/   model "paddle"  (PaddleOCR-DB + VietOCR)
    experiment_9/out_batch/   model "onnx"    (DeepDoc-ONNX + VietOCR)

— and prints a side-by-side plus writes `model_comparison.md` and `model_comparison.csv`. Runs in
either venv (no OCR, no model imports); run the two `run_batch_acb.py` first.
"""

# ===== Standard Library =====
import csv
import json
import os
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
REPORTS = ["balance_sheet", "income_statement", "cash_flow"]
RUNS = {"paddle": os.path.join(HERE, "experiment_8", "out_batch"),
        "onnx": os.path.join(HERE, "experiment_9", "out_batch")}


def load(run_dir: str) -> Optional[dict]:
    sp = os.path.join(run_dir, "summary.json")
    cp = os.path.join(run_dir, "cells.csv")
    if not (os.path.exists(sp) and os.path.exists(cp)):
        return None
    with open(sp, encoding="utf-8") as f:
        summary = json.load(f)
    with open(cp, encoding="utf-8-sig") as f:
        cells = list(csv.DictReader(f))
    return {"summary": summary, "cells": cells}


def cell(cells: List[dict], period: str, report: str) -> Optional[dict]:
    return next((c for c in cells if c["period"] == period and c["report"] == report), None)


def fmt(c: Optional[dict]) -> str:
    if not c or c["found"] != "True":
        return "—"
    ok = "OK" if c["reconciles"] == "True" else "REJECT"
    match = int(c["agree"]) + int(c.get("sign_only", 0))
    return f"{ok} {match}/{match + int(c['differ'])}"


def main() -> None:
    runs = {name: load(d) for name, d in RUNS.items()}
    missing = [n for n, r in runs.items() if r is None]
    if missing:
        print(f"no results for: {', '.join(missing)} — run experiment_"
              f"{'8' if 'paddle' in missing else '9'}/run_batch_acb.py first")
        if all(r is None for r in runs.values()):
            return

    have = {n: r for n, r in runs.items() if r}
    periods = sorted({c["period"] for r in have.values() for c in r["cells"]},
                     key=lambda p: (p.split("-")[1], p[1]))

    lines = ["# Two OCR models on ACB Q1-2014 … Q4-2016", "",
             "`paddle` = PaddleOCR-DB + VietOCR (experiment_8); "
             "`onnx` = DeepDoc-ONNX + VietOCR (experiment_9). "
             "Both feed the SAME statement parser, so the difference is the OCR alone. The score "
             "is figures matching the production reference by MAGNITUDE (`match / comparable`) — "
             "income-statement expense lines that agree in magnitude but flip sign are counted as "
             "matches, because the sign is a CafeF-vs-filing storage convention, not an OCR "
             "error.", "",
             "## Overall", "",
             "| metric | " + " | ".join(have) + " |",
             "|---|" + "---|" * len(have)]

    def row(label, fn):
        lines.append(f"| {label} | " + " | ".join(str(fn(r["summary"])) for r in have.values())
                     + " |")

    row("statements found", lambda s: f"{s['found']}/{s['statements']}")
    row("statements reconciled", lambda s: f"{s['reconciled']}/{s['statements']}")
    row("figures match (magnitude)", lambda s: f"{s['agree']}/{s['comparable']}")
    row("accuracy", lambda s: s["accuracy"])
    row("(sign-only, not counted as error)", lambda s: s.get("sign_only", "—"))
    row("OCR pages", lambda s: s["ocr_pages"])
    row("OCR seconds", lambda s: s["ocr_seconds"])
    row("sec / page", lambda s: s["sec_per_page"])

    # speed-up line if both present
    if "paddle" in have and "onnx" in have:
        p, o = have["paddle"]["summary"], have["onnx"]["summary"]
        if o["sec_per_page"]:
            lines += ["", f"**onnx is {p['sec_per_page'] / o['sec_per_page']:.1f}× faster per "
                      f"page** ({o['sec_per_page']} vs {p['sec_per_page']} s), at "
                      f"{o['accuracy']} vs {p['accuracy']} accuracy."]

    lines += ["", "## Per statement (match / comparable, reconciled quarters)", "",
              "| statement | " + " | ".join(have) + " |",
              "|---|" + "---|" * len(have)]
    for report in REPORTS:
        cellstr = []
        for r in have.values():
            pr = r["summary"]["per_report"][report]
            cellstr.append(f"{pr['agree']}/{pr['agree'] + pr['differ']} "
                           f"({pr['reconciled']}/{pr['quarters']} reconcile)")
        lines.append(f"| {report} | " + " | ".join(cellstr) + " |")

    lines += ["", "## Per quarter × statement (reconcile · agree/comparable)", ""]
    header = ["period"]
    for report in REPORTS:
        header += [f"{report.split('_')[0]}·{n}" for n in have]
    lines += ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    csv_rows = []
    for period in periods:
        row_cells = [period]
        rec = {"period": period}
        for report in REPORTS:
            for n, r in have.items():
                c = cell(r["cells"], period, report)
                row_cells.append(fmt(c))
                rec[f"{report}__{n}"] = fmt(c)
        lines.append("| " + " | ".join(row_cells) + " |")
        csv_rows.append(rec)

    out_md = os.path.join(HERE, "model_comparison.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    if csv_rows:
        with open(os.path.join(HERE, "model_comparison.csv"), "w",
                  encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(csv_rows[0]))
            w.writeheader()
            w.writerows(csv_rows)

    print("\n".join(lines))
    print(f"\nwritten: {out_md}")


if __name__ == "__main__":
    main()
