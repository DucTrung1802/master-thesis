"""
experiment_9's second half — DeepDoc's LAYOUT and TABLE-STRUCTURE models on the same pages.

    ../../ocr_env9/Scripts/python.exe table_structure.py                 # balance-sheet pages
    ../../ocr_env9/Scripts/python.exe table_structure.py --pages 11 12   # income statement

`run_acb_2013.py` uses only DeepDoc's text detection, because that is the part that competes
with experiment_8. But detection+recognition is not what the repo is FOR: its selling point is
the two document models beside it —

    layout.onnx   YOLOv10 over 10 page-element classes (text, title, table, figure, header, …)
    tsr.onnx      table structure: table column / row / column header / row header / spanning cell

— which answer a question the current parser answers geometrically, by clustering the right
edges of numbers: where are this table's rows and columns? A model that says so directly would
be a different way to build the statement, and this script is what shows whether it can: it
writes every region each model found, plus DeepDoc's own reconstruction of the table as markdown
(`construct_table`, via the fork's `get_table_markdown`).

Nothing here feeds the statements in `out/` — it is evidence for whether it SHOULD.
"""

# ===== Standard Library =====
import argparse
import csv
import os
import sys

# ===== Local / Custom Modules =====
import deepdoc_vietocr_engine as engine_mod
from deepdoc_vietocr_engine import VENDOR, _shim_ragflow_utils
from ocr_pipeline import REPO_ROOT, page_raster

PDF = os.path.join(REPO_ROOT, "raw_data", "cafef", "pdfs", "files", "HOSE_ACB",
                   "FY-2013_bao_cao_tai_chinh_hop_nhat_nam_2013_da_kiem_toan.pdf")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")

# 1-based, as printed by `run_acb_2013.py`: the three pages of the consolidated balance sheet.
DEFAULT_PAGES = [8, 9, 10]


def vendor_modules():
    """Import the fork's layout/TSR stack and its markdown helper.

    `t_recognizer` is imported for `get_table_markdown` — 40 lines that tie OCR boxes to the
    detected rows/columns/headers and hand them to `construct_table`. Reimplementing it here
    would be copying their code with a different indentation, so it is imported instead, at the
    cost of two workarounds: the module REBINDS `sys.stdout`/`sys.stderr` to a log file at import
    time (it is written as a script, not a library), and the fork resolves several paths relative
    to the working directory.
    """
    _shim_ragflow_utils()
    if VENDOR not in sys.path:
        sys.path.insert(0, VENDOR)

    cwd, out, err = os.getcwd(), sys.stdout, sys.stderr
    os.chdir(VENDOR)
    try:
        from module import LayoutRecognizer, TableStructureRecognizer
        import t_recognizer
        return LayoutRecognizer, TableStructureRecognizer, t_recognizer.get_table_markdown
    finally:
        os.chdir(cwd)
        sys.stdout, sys.stderr = out, err


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf", default=PDF)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--pages", type=int, nargs="+", default=DEFAULT_PAGES,
                    help="1-based page numbers")
    ap.add_argument("--dpi", type=int, default=200)
    ap.add_argument("--threshold", type=float, default=0.2)
    ap.add_argument("--det-side-len", type=int, default=1600)
    ap.add_argument("--device", default=None)
    args = ap.parse_args()

    import fitz
    import numpy as np
    from PIL import Image

    LayoutRecognizer, TableStructureRecognizer, get_table_markdown = vendor_modules()
    os.makedirs(args.out, exist_ok=True)

    doc = fitz.open(args.pdf)
    images, numbers = [], []
    for n in args.pages:
        img, _ = page_raster(doc[n - 1], args.dpi)
        images.append(Image.fromarray(img))
        numbers.append(n)
    doc.close()

    print(f"layout + tsr on pages {numbers} @ {args.dpi} dpi", flush=True)
    layout = LayoutRecognizer("layout").forward(images, thr=args.threshold)
    tsr = TableStructureRecognizer()(images, thr=args.threshold)

    rows = []
    for kind, per_page in (("layout", layout), ("tsr", tsr)):
        for n, regions in zip(numbers, per_page):
            for r in regions:
                rows.append({"model": kind, "page": n,
                             "label": r.get("type") or r.get("label", ""),
                             "score": round(float(r.get("score", 0)), 3),
                             "x0": round(float(r.get("x0", 0)), 1),
                             "top": round(float(r.get("top", 0)), 1),
                             "x1": round(float(r.get("x1", 0)), 1),
                             "bottom": round(float(r.get("bottom", 0)), 1)})
    path = os.path.join(args.out, "layout_tsr_regions.csv")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["model", "page", "label", "score",
                                          "x0", "top", "x1", "bottom"])
        w.writeheader()
        w.writerows(rows)
    print(f"  {len(rows)} regions -> {path}", flush=True)

    ocr = engine_mod.build(dpi=args.dpi, device=args.device,
                           det_side_len=args.det_side_len).ocr
    for n, img, cpns in zip(numbers, images, tsr):
        md = get_table_markdown(np.array(img), cpns, ocr)
        path = os.path.join(args.out, f"tsr_page{n}.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"  page {n}: {len(cpns)} components -> {path}", flush=True)


if __name__ == "__main__":
    main()
