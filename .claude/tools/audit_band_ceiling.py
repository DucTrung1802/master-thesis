"""⚠️ WHICH VN30 TICKERS CAN EVER BE ONE UNBROKEN BAND — and which holes are permanent.

Read-only, page tree only, nothing rendered. For every hole on a ticker within reach of whole,
it asks the one question that decides whether the hole is parser work or a fact about the
filing: **does the document physically contain the statement the hole needs?**

⚠️ **A HOLE ON A TWO-PAGE FILING IS NOT A DEFECT** (§5 rule 24 — `missing` is the correct
answer). Measured 2026-09-13: FPT Q1-2009 and Q3-2010 are **two-page** filings with a text
layer and no cash-flow wording anywhere in them, so **FPT can never be a single band** however
good the parser becomes. A contiguity target that counts those holes is unreachable by
construction, and that is the number this tool exists to produce.

⚠️ **THE TEST IS ASYMMETRIC AND ONLY ONE DIRECTION IS EVIDENCE.** Finding the wording proves
the statement is there; NOT finding it proves nothing on a SCAN, because there is no text to
search. So a scan is reported as `unknown`, never as `absent` — the mistake `SET-2` is about,
one level up.
"""
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "src" / "kaggle_gpu"))

from dotenv import load_dotenv

load_dotenv(str(REPO / ".env"), override=True)

import fitz
from kgpu import fleet

fleet.anchor()
from web_scraper import cafef_financials as fin
from web_scraper import pdf_ocr_job as job

WORDING = {
    fin.CASH_FLOW: ("lưu chuyển tiền", "luu chuyen tien", "ngân lưu", "cash flow"),
    fin.BALANCE_SHEET: ("cân đối kế toán", "can doi ke toan", "balance sheet",
                        "tình hình tài chính"),
    fin.INCOME_STATEMENT: ("kết quả hoạt động", "ket qua hoat dong", "kết quả kinh doanh",
                           "income statement", "báo cáo lãi"),
}
CONDENSED_PAGES = 10


def _holes(solid):
    first = next((i for i, s in enumerate(solid) if s), None)
    last = next((i for i in range(len(solid) - 1, -1, -1) if solid[i]), None)
    if first is None:
        return []
    return [i for i in range(first + 1, last) if not solid[i]]


def main(universe: str = "VN30", within: int = 4) -> None:
    builder = fin.FinancialsBuilder(logger=None)
    verdicts = {}
    for symbol, exchange in fleet.universe(universe):
        template = job.resolve_template(builder, symbol)[0]
        tasks, solid = [], []
        for task in job.plan(builder, exchange, symbol, allow_parent=True, template=template):
            gap = [r for r in job.REPORTS
                   if r not in set(job.parsed_reports(builder, task))]
            tasks.append((task, gap))
            solid.append(not gap)
        idx = _holes(solid)
        if not idx or len(idx) > within:
            continue
        rows = []
        for i in idx:
            task, gap = tasks[i]
            try:
                with fitz.open(task.path) as doc:
                    pages = doc.page_count
                    text = "\n".join((doc[p].get_text() or "") for p in range(pages)).lower()
                    with_text = sum(1 for p in range(pages)
                                    if (doc[p].get_text() or "").strip())
            except Exception as exc:                      # noqa: BLE001
                rows.append((job.as_quarter(task.period), gap, "?", f"unreadable: {exc}"))
                continue
            for report in gap:
                found = any(w in text for w in WORDING[report])
                if found:
                    verdict = "PARSER — the wording is in the text layer"
                elif with_text == 0:
                    verdict = "unknown — a SCAN, so absence of wording is not evidence"
                elif pages <= CONDENSED_PAGES:
                    verdict = (f"PERMANENT — {pages}-page filing with a text layer and no "
                               f"wording: `missing` is correct (§5 rule 24)")
                else:
                    verdict = "PARSER — text layer, wording not found, but the filing is long"
                rows.append((job.as_quarter(task.period), report, pages, verdict))
        verdicts[symbol] = rows

    print(f"{universe}: tickers within {within} holes of ONE BAND\n")
    reachable = []
    for symbol, rows in sorted(verdicts.items()):
        permanent = [r for r in rows if r[3].startswith("PERMANENT")]
        print(f"{symbol}  ({len({r[0] for r in rows})} hole(s), {len(rows)} open cell(s))"
              + ("   ⚠️ CAN NEVER BE ONE BAND" if permanent else "   reachable in principle"))
        for period, report, pages, verdict in rows:
            print(f"    {period}  {str(report)[:16]:16s} {str(pages):>4s}p  {verdict}")
        if not permanent:
            reachable.append(symbol)
    print(f"\n{len(reachable)} of {len(verdicts)} near-band ticker(s) are reachable in "
          f"principle: {', '.join(reachable) or 'none'}")
    print("⚠️ `reachable in principle` means no hole is provably impossible — each still needs "
          "its own parser fix.")


if __name__ == "__main__":
    main()
