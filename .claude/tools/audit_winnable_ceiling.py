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

#: Characters of extracted text per PAGE below which a filing is a SCAN WITH A COVER PAGE and
#: not a text-layer document. ⚠️ **MEASURED 2026-09-13 AND IT MOVED 97 CELLS**: `with_text == 0`
#: was the only scan test, so a filing whose title page carries a few words counted as text and
#: its missing wording counted as evidence. **97 of the 157 cells in the `long filing, wording
#: not found` bucket run at 3-8 chars/page**, i.e. a title and nothing else, against 1,700-3,000
#: for a real text layer. Calling those "text" is `SET-2`'s mistake wearing a different hat.
THIN_CHARS_PER_PAGE = 120
WORDING = {
    fin.CASH_FLOW: ("lưu chuyển tiền tệ", "luu chuyen tien te"),
    fin.BALANCE_SHEET: ("cân đối kế toán", "can doi ke toan", "tình hình tài chính"),
    fin.INCOME_STATEMENT: ("kết quả hoạt động kinh doanh", "ket qua hoat dong kinh doanh"),
}


WIDER = {
    fin.CASH_FLOW: ("lưu chuyển tiền tệ", "luru chuyen", "lim chuyen", "b03", "b 03",
                    "tiền và tương đương tiền", "tiền thuần"),
    fin.BALANCE_SHEET: ("bảng cân đối", "b01", "b 01", "tổng cộng tài sản",
                        "tong cong tai san", "nguồn vốn"),
    fin.INCOME_STATEMENT: ("b02", "b 02", "doanh thu thuần", "lợi nhuận sau thuế",
                           "báo cáo thu nhập", "thu nhập lãi"),
}


def _period_has_it(path: str, report) -> bool:
    """Does ANOTHER filing OF THE SAME PERIOD contain the statement's wording?

    ⚠️ **`missing` IS ONLY CORRECT IF NO FILING OF THE PERIOD HAS IT** — a quarter usually has
    several (parent and consolidated, reviewed and unaudited) and `ALT-2` is the finding that a
    payload ships one of them. ⚠️ **THE FIRST VERSION OF THIS GLOBBED THE WHOLE DIRECTORY AND
    ANSWERED `100 % winnable`**, which is what a broken test looks like (§5 rule 21): the folder
    holds EVERY filing of the ticker, so GAS 2024-Q1 matched a `Q2-2022` document and HDB
    2013-Q2 matched `FY-2006`. Over a 70-filing issuer some document always contains the words.
    **The filename's period prefix is what makes an alternate an alternate**, and restricting to
    it takes the answer from 100 % to 10.2 %.
    """
    import glob as _glob

    here = os.path.basename(path)
    prefix = here.split("_", 1)[0]
    for sibling in _glob.glob(os.path.join(os.path.dirname(path), f"{prefix}_*.pdf")):
        if os.path.basename(sibling) == here:
            continue
        try:
            with fitz.open(sibling) as doc:
                if not doc.page_count:
                    continue
                other = "\n".join((doc[i].get_text() or "")
                                   for i in range(doc.page_count)).lower()
        except Exception:                                      # noqa: BLE001
            continue
        if any(w in other for w in WORDING[report] + WIDER[report]):
            return True
    return False


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
                density = len(text) / max(pages, 1)
                if any(w in text for w in WORDING[report] + WIDER[report]):
                    kinds["WINNABLE — the wording is in the text layer (parser)"] += 1
                elif with_text == 0 or density < THIN_CHARS_PER_PAGE:
                    # ⚠️ asymmetric: a scan can never be called permanent (`SET-2`), and a
                    # title page is not a text layer — see `THIN_CHARS_PER_PAGE`.
                    kinds["WINNABLE — a SCAN, absence of wording is not evidence"] += 1
                elif pages <= CONDENSED_PAGES:
                    kinds[f"PERMANENT — <={CONDENSED_PAGES}-page filing, text layer, no "
                          f"wording (`BND-3`)"] += 1
                    per_ticker[symbol] += 1
                elif _period_has_it(task.path, report):
                    kinds["WINNABLE — ANOTHER filing of the period has it (`ALT-2`)"] += 1
                else:
                    # ⚠️ **RICH TEXT, LONG FILING, AND NO FILING OF THE PERIOD CARRIES THE
                    # STATEMENT — THIS IS `missing` AND IT USED TO BE COUNTED AS WORK.**
                    kinds["PERMANENT — long text-rich filing, no filing of the period has "
                          "it"] += 1
                    per_ticker[symbol] += 1

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
