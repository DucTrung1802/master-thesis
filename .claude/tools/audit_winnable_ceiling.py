"""⚠️ **WHAT FRACTION OF THE GRID CAN EVER BE PARSED? — the denominator a coverage goal needs.**

Read-only, no OCR, no GPU, no database. Walks every open cell of a universe and sorts it into
PERMANENT (the correct answer is `missing`) or WINNABLE (parser or GPU work), then reports the
rate against both denominators.

⚠️ **THE GRID IS NOT THE CEILING, AND CONFUSING THEM IS WHY A TARGET CAN LOOK MISSED WHEN IT IS
MET.** `GRD-1` made a hole admit it was a hole and `GRD-2` extended the grid to the newest ended
quarter; both made the DENOMINATOR honest and neither made every cell reachable. A cell is
permanently `missing` when the filing does not contain the statement (§5 rule 24) — and three
measured ways that happens are:

  * **`OPB-2`** — a cumulative Q2/Q4 whose Q1..Q(q-1) operand is not a filing of this issuer at
    all. SSB filed no Q1 from 2008 to 2015, so eight Q4s can never be de-cumulated.
  * **`BND-3`** — a short filing with a text layer and no wording for the statement the cell
    needs. FPT Q1-2009 and Q3-2010 are two-page filings with no cash flow in them.
  * **`TRC-1`** — a truncated download: 0 pages, non-zero size, opens without raising. **10 of
    4,763 VN30 filings**, 7 of them an exact KiB multiple. ⚠️ These are **re-scrapable**, so they
    count as WINNABLE here and are reported separately: the work is a Dagster run, not a parser.

⚠️ **THE TEST FOR A SCAN IS ASYMMETRIC AND THIS TOOL OBEYS THAT** (`SET-2`, `BND-3`): finding the
wording proves the statement is present; NOT finding it proves nothing when there is no text
layer. A scan is therefore never called permanent, which is the direction that keeps the ceiling
HONEST — it can only overstate what is winnable.
"""
import collections
import csv
import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
from dotenv import load_dotenv

load_dotenv(str(REPO / ".env"), override=True)

import fitz

from kaggle_gpu.kgpu import fleet

fleet.anchor()
from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job
from web_scraper import pdf_ocr_merge as merge

CONDENSED_PAGES = 10
WORDING = {
    fin.CASH_FLOW: ("lưu chuyển tiền tệ", "luu chuyen tien te"),
    fin.BALANCE_SHEET: ("cân đối kế toán", "can doi ke toan", "tình hình tài chính"),
    fin.INCOME_STATEMENT: ("kết quả hoạt động kinh doanh", "ket qua hoat dong kinh doanh"),
}


def main(universe: str = "VN30") -> None:
    builder = fin.FinancialsBuilder(logger=None)
    names = fleet.universe(universe)
    kinds: collections.Counter = collections.Counter()
    per_ticker: collections.Counter = collections.Counter()
    for symbol, exchange in names:
        template = job.resolve_template(builder, symbol)[0]
        tasks = list(job.plan(builder, exchange, symbol, allow_parent=True,
                              template=template))
        filed = {job.as_quarter(t.period) for t in tasks}
        for task in tasks:
            done = set(job.parsed_reports(builder, task))
            gap = [r for r in job.REPORTS if r not in done]
            if not gap:
                continue
            # ⚠️ **`OPB-2` IS A VERDICT ON ONE REPORT AND NOT ON THE QUARTER**, which is how
            # this first measured 5 where the block is 16: gating it on
            # `gap == {INCOME_STATEMENT}` skipped every quarter whose balance sheet or cash
            # flow is ALSO open. A cumulative quarter whose operand was never filed can never
            # produce its income statement whatever the other two do.
            if fin.INCOME_STATEMENT in gap and task.cumulative:
                _priors, why = merge._quarter_priors(
                    builder, exchange, symbol, template, task.period, {}, filed=filed)
                if why and "NEVER FILED" in why:
                    kinds["PERMANENT — de-cumulation operand never filed (`OPB-2`)"] += 1
                    per_ticker[symbol] += 1
                    gap = [r for r in gap if r != fin.INCOME_STATEMENT]
                    if not gap:
                        continue
            if not task.path or not os.path.exists(task.path):
                kinds["WINNABLE — the filing is not on disk (scrape it)"] += len(gap)
                continue
            try:
                with fitz.open(task.path) as doc:
                    pages = doc.page_count
                    text = "\n".join((doc[p].get_text() or "")
                                     for p in range(pages)).lower()
                    with_text = sum(1 for p in range(pages)
                                    if (doc[p].get_text() or "").strip())
            except Exception:                                  # noqa: BLE001
                kinds["WINNABLE — the filing does not open (re-scrape it)"] += len(gap)
                continue
            if pages == 0:
                # ⚠️ `TRC-1` — a truncated download, and RE-SCRAPABLE: not permanent.
                kinds["WINNABLE — truncated download, 0 pages (`TRC-1`, re-scrape)"] += len(gap)
                continue
            for report in gap:
                if any(w in text for w in WORDING[report]):
                    kinds["WINNABLE — the wording is in the text layer (parser)"] += 1
                elif with_text == 0:
                    # ⚠️ asymmetric: a scan can never be called permanent (`SET-2`)
                    kinds["WINNABLE — a SCAN, absence of wording is not evidence"] += 1
                elif pages <= CONDENSED_PAGES:
                    kinds[f"PERMANENT — <={CONDENSED_PAGES}-page filing, text layer, no "
                          f"wording (`BND-3`)"] += 1
                    per_ticker[symbol] += 1
                else:
                    kinds["WINNABLE — text layer, wording not found, long filing"] += 1

    snap = fleet.coverage(names)
    # ⚠️ **`cells` IS THE GRID TOTAL AND `open_cells` IS WHAT IS LEFT** — the first version of
    # this tool read `cells` as the parsed count and printed nothing, which is the safer way to
    # be wrong about it.
    grid = int(snap.get("cells") or 0)
    have = grid - int(snap.get("open_cells") or 0)
    permanent = sum(n for k, n in kinds.items() if k.startswith("PERMANENT"))
    winnable_open = sum(n for k, n in kinds.items() if k.startswith("WINNABLE"))

    print(fleet.coverage_line(universe, snap))
    print(f"\n{permanent + winnable_open} open cell(s) sorted:\n")
    for kind, n in sorted(kinds.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4d}  {kind}")
    if not grid:
        print("\n⚠️ the coverage snapshot carries no grid total — rate against it not computed")
        return
    ceiling = grid - permanent
    print(f"\n  grid                     {grid:5d}")
    print(f"  permanently `missing`    {permanent:5d}   (§5 rule 24 — the correct answer)")
    print(f"  honest ceiling           {ceiling:5d}")
    print(f"  parsed                   {have:5d}")
    print(f"\n  {have}/{grid} = {100 * have / grid:.1f}% of the GRID")
    print(f"  {have}/{ceiling} = {100 * have / max(ceiling, 1):.1f}% of what is WINNABLE")
    print(f"\n  +{max(0, round(0.95 * grid) - have)} cell(s) to 95% of the grid, "
          f"+{max(0, round(0.95 * ceiling) - have)} to 95% of the ceiling")
    if per_ticker:
        print(f"\n  permanent cells by ticker: {dict(per_ticker.most_common(10))}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "VN30")
