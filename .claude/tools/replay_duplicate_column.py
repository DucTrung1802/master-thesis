"""Replay `_duplicate_period` over every accepted income statement on disk — no OCR."""
import json, os, glob, sys, collections
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath("src"))
from web_scraper.cafef_pdf_parser import PdfParser as P

latest = {}
for f in glob.glob("reports/pdf_ocr/*__pdf_ocr/documents/*.json"):
    run = f.split(os.sep)[-3][:15]
    key = os.path.basename(f)
    if key not in latest or run > latest[key][0]:
        latest[key] = (run, f)

tot = q1 = fires = 0
drops = collections.Counter()
rows = []
for run, f in sorted(latest.values()):
    try: d = json.load(open(f, encoding="utf-8"))
    except Exception: continue
    a = (d.get("accepted") or {}).get("income_statement")
    if not a: continue
    tot += 1
    per = d.get("period", "")
    if not (per.startswith("Q1-") and a.get("quarter_column")): continue
    q1 += 1
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
    fires += 1
    bad = [r[1] for r in rd if len(r[3]) > k and r[3][0] is not None
           and r[3][k] is not None and r[3][0] != r[3][k]]
    drops[len(bad)] += 1
    if bad:
        rows.append((d.get("symbol"), per, a["layer"], len(rd), len(bad), bad[:3]))

print(f"accepted income statements on disk : {tot}")
print(f"  of them Q1 with a quarter column : {q1}")
print(f"  where `_duplicate_period` FIRES  : {fires}")
print(f"  cells it would DROP, by count    : {dict(sorted(drops.items()))}")
print()
for s, per, lay, n, nb, ex in rows:
    print(f"  {s} {per} [{lay}] {n} rows -> {nb} cell(s) dropped")
    for lbl in ex: print(f"        {str(lbl)[:70]}")
