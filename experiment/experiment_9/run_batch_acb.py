"""
experiment_9 (model = "onnx") — parse ACB Q1-2014 … Q4-2016 with DeepDoc-ONNX + VietOCR.

    ../../ocr_env9/Scripts/python.exe run_batch_acb.py

Same batch, same outputs, same reference as experiment_8's `run_batch_acb.py` — the engine is the
only difference, so `out_batch/` here is directly comparable. Writes to `out_batch/`.
"""

# ===== Standard Library =====
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "experiment_8")))

# ===== Local / Custom Modules =====
import deepdoc_vietocr_engine as engine_mod
from batch import run_batch

PERIODS = [f"Q{q}-{y}" for y in (2014, 2015, 2016) for q in (1, 2, 3, 4)]
OUT = os.path.join(HERE, "out_batch")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", default="ACB")
    ap.add_argument("--exchange", default="HOSE")
    ap.add_argument("--template", default="bank")
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--max-pages", type=int, default=32)
    ap.add_argument("--dpi", type=int, default=200)
    ap.add_argument("--det-side-len", type=int, default=1600)
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()

    engine = engine_mod.build(dpi=args.dpi, det_side_len=args.det_side_len, verbose=False)
    run_batch(engine, "onnx", args.symbol, args.exchange, PERIODS, args.out,
              template=args.template, max_pages=args.max_pages, cache=not args.no_cache)


if __name__ == "__main__":
    main()
