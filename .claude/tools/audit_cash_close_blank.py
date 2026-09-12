"""⚠️ A `pdf` CASH-FLOW ROW WITH NO CLOSING BALANCE IN ANY CLOSING COLUMN — `CCB-1`.

Read-only, no OCR, no database. Reads the statement CSVs and counts rows that `source='pdf'`
marks as parsed while every closing-cash column is blank.

⚠️ **THIS IS THE HAZARD `GCW-1` AND `replay_cash_close.py` BOTH ARGUE ABOUT, MEASURED IN THE
DATA RATHER THAN PREDICTED.** `reconcile` REQUIRES a closing balance before it accepts a cash
flow, and it can satisfy that requirement on a term that fills no schema column — so the gate
passes, the row is written `pdf`, and the single most important figure on the statement is
absent. `replay_cash_close.py` reverted a `CASH_CLOSE` change for exactly this reason
(*"a needle satisfies reconcile's GATE and fills NO COLUMN"*); this tool says how many rows
already carry it.

⚠️ **IT QUALIFIES EVERY COVERAGE NUMBER TAKEN OFF THESE FILES** (§5 rule 21 — a gate opening is
not a cell): these rows count as successes in the `pdf`-row rate and hand a downstream reader
NULL. Measured 2026-09-13: **113 of 1,702 = 6.6 %**, concentrated on BVH 41, FPT 13, VIC 13,
BSR 12, PLX 10.

⚠️ **A BLANK IS NOT A ZERO AND MUST NOT BE FILLED WITH ONE.** The fix is the schema ALIAS route
(`GCW-1`'s `cash_wording`), which puts the printed figure in the column; inventing a value from
`opening + net + fx` would be a transcription, which §5 rule 24 forbids as a source.
"""
import collections
import csv
import glob
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

ROOT = "raw_data/cafef/financials/statements"
bad = collections.Counter()
blanks = []
total = 0
no_column = []

for path in sorted(glob.glob(os.path.join(ROOT, "*", "cash_flow", "cf_*.csv"))):
    with open(path, encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        continue
    # every column that could hold a CLOSING cash balance on this template's chart
    closing = [c for c in rows[0] if "cuoi" in c and ("tien" in c or "ton" in c)]
    symbol = os.path.basename(path)[3:-4]
    if not closing:
        no_column.append(symbol)
        continue
    for row in rows:
        if row.get("source") != "pdf":
            continue
        total += 1
        if all(not (row.get(c) or "").strip() for c in closing):
            bad[symbol] += 1
            blanks.append((symbol, row.get("period", "?")))

print(f"{total} `pdf` cash-flow row(s) on disk")
print(f"{sum(bad.values())} carry NO closing-cash figure in any closing column "
      f"({100 * sum(bad.values()) / max(total, 1):.1f}%)\n")
for symbol, n in bad.most_common():
    print(f"  {symbol:14s} {n}")
if no_column:
    print(f"\n⚠️ {len(no_column)} file(s) have no closing column in their header at all — "
          f"a different chart, not a blank: {', '.join(no_column[:6])}")
print(f"\nfirst 12: {blanks[:12]}")
