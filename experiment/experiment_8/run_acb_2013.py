"""
experiment_8 — read ACB's FY-2013 audited consolidated filing with PaddleOCR-DB + VietOCR.

    ../../ocr_env8/Scripts/python.exe run_acb_2013.py
    ../../ocr_env8/Scripts/python.exe run_acb_2013.py --dpi 300 --box-thresh 0.4

Writes to `out/`: the three statements (raw rows, canonical columns, and a column-by-column
comparison against CafeF's own transcription of the same filing), the OCR text of every page it
read, and `report.md`.

Why this filing: `raw_data/cafef/financials/statements/bank/*/HOSE_ACB.csv` records all three of
its FY-2013 statements as `source=cafef` — the production Tesseract parser could not read the
document, so the figures had to be taken from CafeF's tabs instead. It is a page scan carrying a
legacy-font text layer that is neither usable nor short enough to be ignored, which makes it the
hardest case in the archive and the honest test for a better OCR stack.
"""

# ===== Standard Library =====
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ===== Local / Custom Modules =====
import vietnamese_ocr
from ocr_pipeline import REPO_ROOT, run

PDF = os.path.join(REPO_ROOT, "raw_data", "cafef", "pdfs", "files", "HOSE_ACB",
                   "FY-2013_bao_cao_tai_chinh_hop_nhat_nam_2013_da_kiem_toan.pdf")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf", default=PDF)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--year", type=int, default=2013)
    ap.add_argument("--symbol", default="ACB")
    ap.add_argument("--exchange", default="HOSE")
    ap.add_argument("--template", default="bank",
                    help="chart of accounts to map onto (bank/corp/securities/insurance)")
    ap.add_argument("--max-pages", type=int, default=24,
                    help="hard cap on how far into the filing to read")
    ap.add_argument("--dpi", type=int, default=200)
    ap.add_argument("--padding", type=int, default=4, help="px added around each detected box")
    ap.add_argument("--batch-size", type=int, default=24)
    ap.add_argument("--min-prob", type=float, default=0.25,
                    help="drop recognitions below this confidence")
    ap.add_argument("--det-side-len", type=int, default=1600)
    ap.add_argument("--box-thresh", type=float, default=0.5)
    ap.add_argument("--unclip-ratio", type=float, default=1.8)
    ap.add_argument("--beamsearch", action="store_true",
                    help="VietOCR beam search — slower, occasionally cleaner")
    ap.add_argument("--weights", default="vgg_transformer",
                    help="VietOCR config name: vgg_transformer | vgg_seq2seq")
    ap.add_argument("--no-cache", action="store_true",
                    help="re-OCR instead of reusing out/ocr_cache.json")
    args = ap.parse_args()

    engine = vietnamese_ocr.build(
        dpi=args.dpi, padding=args.padding, batch_size=args.batch_size,
        min_prob=args.min_prob, det_side_len=args.det_side_len, box_thresh=args.box_thresh,
        unclip_ratio=args.unclip_ratio, beamsearch=args.beamsearch, weights=args.weights)

    run(engine, f"experiment_8 — {vietnamese_ocr.ENGINE_NAME}", args.pdf, args.out,
        year=args.year, symbol=args.symbol, exchange=args.exchange, template=args.template,
        max_pages=args.max_pages, dpi=args.dpi, cache=not args.no_cache)


if __name__ == "__main__":
    main()
