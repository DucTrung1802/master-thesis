"""⚠️ HOLLOW ROWS — a cell that counts as PARSED and hands the reader almost nothing.

`CCB-1` (cash flow, no closing balance) and `HOL-1` (a statement of a handful of figures).

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


# ── `HOL-1` — a statement written `pdf` with a handful of figures ─────────────

def _numeric_columns(rows):
    """The columns that hold a FIGURE somewhere, so metadata is not counted as content.

    ⚠️ **COUNTING METADATA AS A FILLED COLUMN IS HOW THIS MEASUREMENT FIRST CAME BACK CLEAN**
    (2026-09-13). A naive filled-column count reported `1 row with <=5 filled` for the income
    statement; `symbol`, `method`, `n_columns` and `document` are always populated, so every
    hollow row scored four higher than its content. The honest answer is **64**.
    """
    out = []
    for column in rows[0]:
        for row in rows:
            value = (row.get(column) or "").strip().replace("-", "").replace(".", "")
            if value and value.isdigit():
                out.append(column)
                break
    return [c for c in out if c not in {"period", "months", "year", "quarter",
                                        "n_columns", "unit"}]


def hollow_rows(threshold: int = 6) -> None:
    """How many `pdf` rows of each statement carry `threshold` figures or fewer, by QUARTER.

    ⚠️ **THE QUARTER SPLIT IS THE FINDING, NOT A BREAKDOWN.** Measured 2026-09-13:

    | quarter | thin income statements | rate |
    |---|---|---|
    | Q1 | 1 of 425 | **0.2 %** |
    | Q2 | 21 of 418 | **5.0 %** |
    | Q3 | 2 of 388 | **0.5 %** |
    | Q4 | 40 of 436 | **9.2 %** |

    Q2 and Q4 are exactly the CUMULATIVE quarters, **59 of the 64 carry `months=3`**, and the
    balance sheet — which is never de-cumulated — has **0** thin rows out of 1,679 at a median
    of 58 figures. So the mechanism is `_decumulate`: `FY - (Q1+Q2+Q3)` fills a column only
    where the year-to-date figure AND every prior held a value, so **one sparse prior
    propagates its sparsity into the derived quarter**, and Q4 is worst because it subtracts
    three. The run log says so per document (*"14 of 18 columns"*) and nothing aggregates it.
    """
    for sub in ("income_statement", "balance_sheet", "cash_flow"):
        dist = collections.Counter()
        by_quarter = collections.Counter()
        all_quarter = collections.Counter()
        months = collections.Counter()
        thin = []
        for path in sorted(glob.glob(os.path.join(ROOT, "*", sub, "*.csv"))):
            with open(path, encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            if not rows:
                continue
            columns = _numeric_columns(rows)
            for row in rows:
                if row.get("source") != "pdf":
                    continue
                quarter = (row.get("period") or "??")[:2]
                all_quarter[quarter] += 1
                n = sum(1 for c in columns if (row.get(c) or "").strip())
                dist[n] += 1
                if n <= threshold:
                    by_quarter[quarter] += 1
                    months[row.get("months", "?")] += 1
                    thin.append((os.path.basename(path)[3:-4], row.get("period"), n))
        counts = sorted(dist.elements())
        if not counts:
            continue
        median = counts[len(counts) // 2]
        print("")
        print(f"{sub}: {len(counts)} `pdf` row(s), median {median} figures, "
              f"min {min(counts)} - {len(thin)} with <= {threshold}")
        for quarter in sorted(all_quarter):
            n, d = by_quarter[quarter], all_quarter[quarter]
            print(f"    {quarter}: {n:3d} of {d:4d} = {100 * n / max(d, 1):4.1f}%")
        if thin:
            print(f"    their `months`: {dict(months)}")
            print(f"    first 6: {thin[:6]}")


if __name__ == "__main__":
    print("=" * 70)
    hollow_rows()
