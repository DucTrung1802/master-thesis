"""
experiment_8 (model = "paddle") — parse ACB Q1-2014 … Q4-2016 with PaddleOCR-DB + VietOCR.

    ../../ocr_env8/Scripts/python.exe run_batch_acb.py

Writes to `out_batch/`: `cells.csv` (a scorecard per quarter × statement), `detail.csv` (every
figure vs the production reference), `report.md`, `summary.json`. The OCR of each filing is cached
under `out_batch/cache/`, so re-scoring is instant.
"""

# ===== Standard Library =====
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ===== Local / Custom Modules =====
import vietnamese_ocr
from batch import run_batch

PERIODS = [f"Q{q}-{y}" for y in (2014, 2015, 2016) for q in (1, 2, 3, 4)]
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out_batch")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", default="ACB")
    ap.add_argument("--exchange", default="HOSE")
    ap.add_argument("--template", default="bank")
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--max-pages", type=int, default=32)
    ap.add_argument("--dpi", type=int, default=200)
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()

    engine = vietnamese_ocr.build(dpi=args.dpi, verbose=False)
    run_batch(engine, "paddle", args.symbol, args.exchange, PERIODS, args.out,
              template=args.template, max_pages=args.max_pages, cache=not args.no_cache)


if __name__ == "__main__":
    main()
