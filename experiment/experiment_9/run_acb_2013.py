"""
experiment_9 — read ACB's FY-2013 audited consolidated filing with DeepDoc + VietOCR.

    ../../ocr_env9/Scripts/python.exe setup_vendor.py      # once: checkout + ONNX models
    ../../ocr_env9/Scripts/python.exe run_acb_2013.py
    ../../ocr_env9/Scripts/python.exe run_acb_2013.py --det-side-len 960   # deepdoc's own default

Same filing, same downstream, same outputs as experiment_8 — the engine is the only difference,
so `out/report.md` and `out/comparison.csv` here are directly comparable with experiment_8's.
"""

# ===== Standard Library =====
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ===== Local / Custom Modules =====
import deepdoc_vietocr_engine as engine_mod
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
    ap.add_argument("--template", default="bank")
    ap.add_argument("--max-pages", type=int, default=24)
    ap.add_argument("--dpi", type=int, default=200)
    ap.add_argument("--batch-size", type=int, default=24)
    ap.add_argument("--min-score", type=float, default=0.25)
    ap.add_argument("--det-side-len", type=int, default=1600,
                    help="detector input cap; deepdoc ships 960 (≈85 dpi on an A4 scan)")
    ap.add_argument("--device", default=None, help="torch device for VietOCR, e.g. cpu")
    ap.add_argument("--config-name", default="vgg_seq2seq",
                    help="VietOCR architecture matching the vendored checkpoint")
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()

    ocr = engine_mod.build(dpi=args.dpi, device=args.device, batch_size=args.batch_size,
                           min_score=args.min_score, det_side_len=args.det_side_len,
                           config_name=args.config_name)

    run(ocr, f"experiment_9 — {engine_mod.ENGINE_NAME}", args.pdf, args.out,
        year=args.year, symbol=args.symbol, exchange=args.exchange, template=args.template,
        max_pages=args.max_pages, dpi=args.dpi, cache=not args.no_cache)


if __name__ == "__main__":
    main()
