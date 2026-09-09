"""Can `_resolve_duplicate_identity` arbitrate the contradicted cells? Replay, no OCR."""
import json, os, glob, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath("src"))
from web_scraper.cafef_pdf_parser import PdfParser as P
from web_scraper.cafef_financials import FinancialsBuilder as F

OPI = F.OP_IDENTITY
terms = set()
for col, (plus, minus, opt) in OPI.items():
    terms |= {col} | set(plus) | set(minus) | set(opt)

latest = {}
for f in glob.glob("reports/pdf_ocr/*__pdf_ocr/documents/*.json"):
    run = f.split(os.sep)[-3][:15]; key = os.path.basename(f)
    if key not in latest or run > latest[key][0]:
        latest[key] = (run, f)

tot_bad = arb = out = 0
lines = []
for run, f in sorted(latest.values()):
    try: d = json.load(open(f, encoding="utf-8"))
    except Exception: continue
    a = (d.get("accepted") or {}).get("income_statement")
    per = d.get("period", "")
    if not a or not per.startswith("Q1-") or not a.get("quarter_column"): continue
    rd = [r for r in a["row_dump"] if isinstance(r, list) and len(r) > 3
          and isinstance(r[3], list)]
    width = max((len(r[3]) for r in rd), default=0)
    k = None
    for cand in range(2, width):
        ag = df = 0
        for r in rd:
            v = r[3]
            if len(v) > cand and v[0] is not None and v[cand] is not None:
                ag += (v[0] == v[cand]); df += (v[0] != v[cand])
        if ag >= P.DUP_MIN_AGREE_ROWS and ag >= P.DUP_MIN_AGREE_RATIO * (ag + df):
            k = cand; break
    if k is None: continue
    bad = [(r[1], r[3][0], r[3][k]) for r in rd
           if len(r[3]) > k and r[3][0] is not None and r[3][k] is not None
           and r[3][0] != r[3][k]]
    if not bad: continue
    # which of the contradicted rows map onto a term the identity can weigh?
    vals = a["values"]
    on_id = [lbl for lbl, v0, vk in bad
             if any(t in vals and (vals[t] == v0 or vals[t] == vk) for t in terms)]
    tot_bad += len(bad); arb += len(on_id); out += len(bad) - len(on_id)
    lines.append(f"  {d.get('symbol')} {per}: {len(bad)} contradicted, "
                 f"{len(on_id)} on an OP_IDENTITY term")
print(f"contradicted cells across the 17 quarters : {tot_bad}")
print(f"  ON a term `_resolve_duplicate_identity` weighs : {arb}")
print(f"  OUTSIDE it (no arbitration possible)          : {out}")
print()
for l in lines: print(l)
