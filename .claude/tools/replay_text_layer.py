"""⚠️ TODAY'S PARSER ON A TEXT-LAYER FILING, WITH NO GPU — the one replay that can verify a DETECTOR.

`data-state.md` (MSN, 2026-09-07): *"a replay over recorded rows can verify a figure and can never
verify a detector"* — a run folder stores rows, not word boxes. **A TEXT-LAYER filing is the
exception**: `PdfParser._read_page` reads a page from the PDF's own words whenever its text is long
enough and not `_native_garbled`, so with every OCR engine switched off those pages hand the parser
exactly the words a real run hands it. This drives `_parse_cascaded` itself — the real layer order,
the real gates — over any subset of layers, on CPU, with no lock, no writes and no GPU.

⚠️ **BUILT FOR `MSC-1` AND IT CHANGED THE ANSWER THREE TIMES IN ONE DAY** (2026-09-13): a stored-row
scan said VNM's code column was defeated by the header date `31`; this replay showed the detector
ALSO stopping at VIC's off-balance appendix (`001` after `440`), at a consolidated sheet's `490 →
440`, at VNM Q1-2015's twice-printed `240`, and — once the column dropped — at a Mẫu CBTT-03 summary
merged into the full balance sheet. None of those is visible in `absent_rows`.

⚠️ **ITS NULL IS A MONKEYPATCH, NOT A BRANCH**: set the changed class attribute back (e.g.
`PdfParser.CODE_HEADER_BAND = 0`) in a second process and replay the same cells. A fix that wins
under both is not the fix (TPB Q1-2010 won under both — it was a screen-held cell, not a parser one).

⚠️ **`sane` HAS NO BAND HERE**, so a WON is a CANDIDATE for a run and never a banked cell; the
continuity screens and the merge still decide. A filing whose text pages are mostly `_native_garbled`
is SKIPPED under `--open`, because a real run OCRs those pages and this would read words it never sees.

    python .claude/tools/replay_text_layer.py VNM:Q3-2012 VIC:Q1-2009
    python .claude/tools/replay_text_layer.py --flag code_column_by_value --flag notes_boundary VIC:Q1-2009
    python .claude/tools/replay_text_layer.py --open VNM VIC
"""
import argparse
import os
import sys
from pathlib import Path

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
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
from web_scraper.cafef_pdf_parser import PdfParser

# ⚠️ NO OCR MODEL IS EVER BUILT: with CUDA hidden the onnx provider and VietOCR's torch model both
# crash natively (access violation), and a text-layer page never needed them.
PdfParser._init_ocr = lambda self: False


def _clean_text_layer(parser: PdfParser, path: str) -> bool:
    with fitz.open(path) as doc:
        n = doc.page_count
        textful = garbled = 0
        for i in range(n):
            content = parser._page_content_text(doc[i], doc[i].get_text() or "")
            if len(content.strip()) < parser.MIN_PAGE_TEXT:
                continue
            textful += 1
            garbled += parser._native_garbled(content)
    return bool(n) and textful >= max(2, 0.5 * n) and garbled < 0.5 * textful


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("targets", nargs="+", help="SYMBOL:PERIOD cells, or tickers with --open")
    ap.add_argument("--flag", action="append", default=[],
                    help="keep only onnx layers carrying ANY of these ParseLayer flags")
    ap.add_argument("--open", action="store_true",
                    help="targets are tickers: replay every open cell on a clean text layer")
    args = ap.parse_args()

    layers = [l for l in fin.FinancialsBuilder.LAYERS if l.engine == "onnx"
              and (not args.flag or any(getattr(l, f) for f in args.flag))]
    print(f"{len(layers)} layer(s)", flush=True)
    exchanges = dict(fleet.universe("VN30"))
    probe = PdfParser()
    wanted = {}
    for t in args.targets:
        symbol, _, period = t.partition(":")
        wanted.setdefault(symbol, set()).update({period} if period else set())
    for symbol, periods in wanted.items():
        b = fin.FinancialsBuilder(logger=None)
        template = job.resolve_template(b, symbol)[0]
        for task in job.plan(b, exchanges.get(symbol, "HOSE"), symbol, allow_parent=True,
                             template=template):
            if not args.open and task.period not in periods:
                continue
            gap = [r for r in job.REPORTS if r not in set(job.parsed_reports(b, task))]
            if args.open and (not gap or not _clean_text_layer(probe, str(task.path))):
                continue
            rb = fin.FinancialsBuilder(logger=None)
            rb.LAYERS = layers
            accepted, _ = rb._parse_cascaded(str(task.path), rb._period_end(task.period),
                                             task.template, {r: [] for r in job.REPORTS})
            for r in (gap if args.open else job.REPORTS):
                if r in accepted:
                    items = sum(1 for v in accepted[r][0].values() if isinstance(v, (int, float)))
                    print(f"WON {symbol} {task.period} {r} [{accepted[r][2]}] {items} items",
                          flush=True)
                else:
                    reasons = list(dict.fromkeys(w for _, w in (rb.refusals or {}).get(r, [])))
                    print(f"REF {symbol} {task.period} {r} :: "
                          + " || ".join(w[:110] for w in reasons[-3:]), flush=True)


if __name__ == "__main__":
    main()
