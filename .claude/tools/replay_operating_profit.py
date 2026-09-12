"""⚠️ THE TOOL THAT RULED OUT THE SECOND-LARGEST REFUSAL BUCKET AS A SCHEMA FIX, AND IT IS
KEPT FOR THAT. Replays `_operating_profit_identity`'s residual over every income statement
whose EVERY deepest-layer reason is `operating profit does not close` — read-only, no OCR.

**The hypothesis it tested, and the reason it is worth re-running after any parser change:**
if the residual equals a figure the reading PRODUCED and did not map, the defect is a missing
schema ALIAS and the row names the label to add (`GCW-1`'s shape, which won 67 statements).
Half the residual would mean a mapped term sits on the wrong SIDE of the identity.

**Measured 2026-09-13 over VN30's run folders: 24 such cells, and NEITHER holds.** 0 of 24
are explained by one row, in either direction. The residuals are small and spread — 16 cells
inside 1 % of the printed operating profit, 2 inside 0.1 % — so the identity very nearly
closes and no single line accounts for the difference.

⚠️ **THAT IS A RESULT AND NOT A DEAD END: it says this bucket is not one defect.** A schema
alias, which is what the ranking's size invites, would win none of it. And ⚠️ **LOOSENING
`OP_IDENTITY_TOL` IS THE TRAP `SET-3` NAMES** — 1 % of VNM Q1-2022's printed figure is 58
billion dong, a number the filing means, and a gate widened to pass it would raise the
measured rate by retiring cells it cannot read (§5 rule 21: a metric that cannot fail is not
a pass). ⚠️ **VNM Q1-2015 AND Q1-2016 SHARE A RESIDUAL TO SIX FIGURES (5.57e+09)**, which is
where to start: an identical gap across two filings is structural, not OCR noise.
"""
import collections, json, glob, os, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.abspath("src"))

latest = {}
for f in glob.glob("reports/pdf_ocr/*__pdf_ocr/documents/*.json"):
    run = f.split(os.sep)[-3][:15]
    key = os.path.basename(f)
    if key not in latest or run > latest[key][0]:
        latest[key] = (run, f)

hits = collections.Counter()
labels = collections.Counter()
n = matched = 0
for run, f in sorted(latest.values()):
    try:
        d = json.load(open(f, encoding="utf-8"))
    except Exception:
        continue
    deep = (d.get("absent_deepest") or {}).get("income_statement") or []
    reasons = [r[1] for r in deep if isinstance(r, list) and len(r) > 1]
    if not reasons or not all("operating profit does not close" in r for r in reasons):
        continue
    n += 1
    rows = ((d.get("absent_rows") or {}).get("income_statement") or {}).get("rows") or []
    best = None
    for r in reasons:
        try:
            give = float(r.split("components give ")[1].split(" ")[0])
            exp = float(r.split("(or ")[1].split(" ")[0])
            printed = float(r.split("against a printed ")[1].split()[0].rstrip(")"))
        except Exception:
            continue
        for cand in (give, exp):
            gap = abs(cand - printed)
            if best is None or gap < best[0]:
                best = (gap, printed)
    if best is None:
        continue
    gap = best[0]
    # every figure this reading produced, in any column
    figs = []
    for r in rows:
        # ⚠️ **A ROW IS A DICT, NOT A LIST, AND READING IT AS A LIST IS HOW THIS TOOL FIRST
        # ANSWERED `0 of 24`** (2026-09-13). `absent_rows` stores `{key, label, values}`;
        # `row_dump` on the ACCEPTED side stores lists, and the two were confused. The wrong
        # answer was the confident kind: every figure list came back empty, so nothing could
        # match, and the conclusion "the residual is spread" was a measurement of a typo.
        vals = r.get("values") or [] if isinstance(r, dict) else []
        label = (r.get("label") or r.get("key") or "") if isinstance(r, dict) else ""
        for v in vals:
            if isinstance(v, (int, float)) and v:
                figs.append((abs(float(v)), label))
    hit = [(v, l) for v, l in figs if abs(v - gap) <= max(4.0, gap * 0.002)]
    # or HALF the gap, which is a term on the wrong SIDE of the identity (sign flip)
    half = [(v, l) for v, l in figs if abs(v - gap / 2.0) <= max(4.0, gap * 0.002)]
    if hit:
        hits["the residual IS a row on the page — a schema ALIAS is missing"] += 1
        matched += 1
        labels[hit[0][1][:44]] += 1
    elif half:
        hits["HALF the residual is a row — that term is on the WRONG SIDE (sign)"] += 1
        matched += 1
        labels["[sign] " + half[0][1][:36]] += 1
    else:
        hits["no single row matches — the residual is spread"] += 1

print(f"{n} single-reason `operating profit` cell(s)\n")
for k, v in hits.most_common():
    print(f"  {v:4d}  {k}")
print(f"\n{matched} of {n} explained by ONE row. The labels that row carries:")
for k, v in labels.most_common(14):
    print(f"  {v:3d}  {k}")
